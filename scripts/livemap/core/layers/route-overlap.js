// livemap/core/layers/route-overlap.js
// -----------------------------------------------------------------------------
// Alternating-colour route striping — the TransLoc-patented technique
// (US7920967B1): where two routes follow the same path, draw that stretch as a
// repeating cycle of every sharing route's colour instead of one colour winning.
//
// Ported from testmap.js's `OverlapRouteRenderer` (proven on the same TransLoc
// `EncodedPolyline` data), with two deliberate changes for livemap:
//
//   1. Detection runs in a LOCAL METRES projection, zoom-independent, and is
//      memoised on the route set. testmap re-detected in pixel space on every
//      `zoomend`; keeping detection zoom-stable removes a flicker class and the
//      per-zoom cost. Only the dash slicing (a screen-space thing) is redone
//      per zoom.
//   2. Render is GEOMETRY-LEVEL slicing, not Leaflet `dashArray` + `dashOffset`
//      (MapLibre's `line-dasharray` is per-layer with no phase offset, so it
//      can't stagger one route's dashes into another's gaps).
//
// `stripeRoutes(routes, zoom, centerLat)` -> { features }  for the
// `livemap-routes` GeoJSON source. `features` carry `kind: 'casing' | 'line'`
// and `color`; route-style.js filters the two layers on `kind`.
// -----------------------------------------------------------------------------

import { simplifyPath } from '../util.js';

// Tuning — metres for detection, screen px for the dash cadence.
const SIMPLIFY_TOL_M = 3; // drop polyline noise below this
const SAMPLE_STEP_M = 14; // resample spacing
const MATCH_TOL_M = 12; // two routes this close (and same heading) = same corridor
const HEADING_TOL_RAD = (20 * Math.PI) / 180;
const DASH_PX = 16; // one colour's run length on screen
const MIN_DASH_PX = 0.5;
const TILE = 512;

// --- projection (local equirectangular, metres) --------------------------------

function makeProjection(routes) {
  let sLat = 0;
  let n = 0;
  for (const r of routes) {
    for (const c of r.coords) {
      sLat += c[1];
      n++;
    }
  }
  const lat0 = n ? sLat / n : 38;
  const mPerLat = 111_320;
  const mPerLng = 111_320 * Math.cos((lat0 * Math.PI) / 180);
  return {
    lat0,
    toM: ([lng, lat]) => [lng * mPerLng, lat * mPerLat],
    toLngLat: ([x, y]) => [x / mPerLng, y / mPerLat],
  };
}

/** Metres of ground per screen pixel at this zoom / latitude. */
function metresPerPx(zoom, lat) {
  return (40_075_016.686 * Math.cos((lat * Math.PI) / 180)) / (TILE * 2 ** zoom);
}

// --- resample + segment build (metres space) ---------------------------------

function dist(a, b) {
  const dx = a[0] - b[0];
  const dy = a[1] - b[1];
  return Math.sqrt(dx * dx + dy * dy);
}

function resample(pts, step) {
  if (pts.length < 2) return [];
  const out = [{ p: pts[0], cum: 0 }];
  let cum = 0;
  let carry = 0;
  for (let i = 1; i < pts.length; i++) {
    const a = pts[i - 1];
    const b = pts[i];
    const segLen = dist(a, b);
    if (segLen === 0) continue;
    let consumed = 0;
    while (carry + (segLen - consumed) >= step) {
      const need = step - carry;
      consumed += need;
      cum += need;
      const t = consumed / segLen;
      out.push({ p: [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t], cum });
      carry = 0;
    }
    const left = segLen - consumed;
    cum += left;
    carry += left;
  }
  const last = pts[pts.length - 1];
  if (dist(out[out.length - 1].p, last) > 1e-6) out.push({ p: last, cum });
  return out;
}

function buildSegments(key, samples) {
  const segs = [];
  for (let i = 0; i < samples.length - 1; i++) {
    const a = samples[i].p;
    const b = samples[i + 1].p;
    const len = dist(a, b);
    if (!(len > 0)) continue;
    segs.push({
      key,
      cum: samples[i].cum,
      a,
      b,
      mid: [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2],
      heading: Math.atan2(b[1] - a[1], b[0] - a[0]),
      sharedWith: new Set([key]),
    });
  }
  return segs;
}

function headingClose(h1, h2) {
  let d = Math.abs(h1 - h2) % (Math.PI * 2);
  if (d > Math.PI) d = Math.PI * 2 - d;
  // same direction OR opposite direction (same street, other way) both count
  return d <= HEADING_TOL_RAD || Math.abs(Math.PI - d) <= HEADING_TOL_RAD;
}

function segmentsOverlap(a, b) {
  if (dist(a.mid, b.mid) > MATCH_TOL_M) return false;
  if (!headingClose(a.heading, b.heading)) return false;
  const m = Math.min(
    dist(a.a, b.a),
    dist(a.b, b.b),
    dist(a.a, b.b),
    dist(a.b, b.a),
  );
  return m <= MATCH_TOL_M * 2;
}

// --- shared-corridor detection (zoom-independent) ---------------------------

function detectGroups(routes, proj) {
  const segsByKey = new Map();
  const cell = MATCH_TOL_M * 2;
  const grid = new Map(); // "gx,gy" -> segment[]
  const cellKey = (p) => `${Math.floor(p[0] / cell)},${Math.floor(p[1] / cell)}`;

  for (const r of routes) {
    const ptsM = r.coords.map(proj.toM);
    const simp = simplifyPath(ptsM, SIMPLIFY_TOL_M);
    const segs = buildSegments(r.key, resample(simp, SAMPLE_STEP_M));
    if (!segs.length) continue;
    segsByKey.set(r.key, segs);
    for (const s of segs) {
      const k = cellKey(s.mid);
      let bucket = grid.get(k);
      if (!bucket) grid.set(k, (bucket = []));
      bucket.push(s);
    }
  }

  const seenPair = new Set();
  for (const segs of segsByKey.values()) {
    for (const s of segs) {
      const gx = Math.floor(s.mid[0] / cell);
      const gy = Math.floor(s.mid[1] / cell);
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          const bucket = grid.get(`${gx + dx},${gy + dy}`);
          if (!bucket) continue;
          for (const o of bucket) {
            if (o.key === s.key) continue;
            const pk =
              s.key < o.key
                ? `${s.key}#${s.cum}|${o.key}#${o.cum}`
                : `${o.key}#${o.cum}|${s.key}#${s.cum}`;
            if (seenPair.has(pk)) continue;
            seenPair.add(pk);
            if (!segmentsOverlap(s, o)) continue;
            s.sharedWith.add(o.key);
            o.sharedWith.add(s.key);
          }
        }
      }
    }
  }

  // Walk each route's segments in order; a run with the same sharedWith set is
  // one group, owned by the lowest key in the set so it's emitted once.
  const groups = [];
  for (const [key, segs] of segsByKey) {
    const ordered = segs.slice().sort((a, b) => a.cum - b.cum);
    let cur = null;
    const flush = () => {
      if (cur && cur.pts.length >= 2) {
        let len = 0;
        for (let i = 1; i < cur.pts.length; i++) len += dist(cur.pts[i - 1], cur.pts[i]);
        groups.push({ keys: cur.keys, ptsM: cur.pts, lenM: len });
      }
      cur = null;
    };
    for (const s of ordered) {
      const set = [...s.sharedWith].sort();
      if (set[0] !== key) {
        flush(); // this stretch belongs to a lower-keyed owner; break our run
        continue;
      }
      const sameSet = cur && cur.keys.length === set.length && cur.keys.every((k, i) => k === set[i]);
      const contiguous = cur && dist(cur.pts[cur.pts.length - 1], s.a) <= SAMPLE_STEP_M * 1.5;
      if (!sameSet || !contiguous) {
        flush();
        cur = { keys: set, pts: [s.a, s.b] };
      } else {
        cur.pts.push(s.b);
      }
    }
    flush();
  }
  return groups;
}

// --- slice a metres polyline into fixed-length arc pieces ------------------

function sliceByArc(ptsM, pieceLen) {
  const pieces = [];
  let piece = [ptsM[0]];
  let acc = 0;
  for (let i = 1; i < ptsM.length; i++) {
    let a = ptsM[i - 1];
    const b = ptsM[i];
    let segLen = dist(a, b);
    while (acc + segLen >= pieceLen) {
      const need = pieceLen - acc;
      const t = need / segLen;
      const cut = [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
      piece.push(cut);
      pieces.push(piece);
      piece = [cut];
      a = cut;
      segLen = dist(a, b);
      acc = 0;
    }
    acc += segLen;
    piece.push(b);
  }
  if (piece.length >= 2) pieces.push(piece);
  return pieces;
}

// --- public ---------------------------------------------------------------

let _cache = null; // { sig, proj, groups }

function routesSig(routes) {
  return routes.map((r) => `${r.key}:${r.coords.length}:${r.coords[0]?.join(',')}`).join('|');
}

/**
 * @param {{ key:string, color:string, coords:[number,number][] }[]} routes
 *   one representative polyline per visible line (name-group), lng/lat order.
 * @param {number} zoom  current map zoom
 * @param {number} centerLat  for the metres-per-pixel conversion
 * @returns {{ features: object[] }}
 */
export function stripeRoutes(routes, zoom, centerLat) {
  const valid = (routes || []).filter((r) => Array.isArray(r.coords) && r.coords.length >= 2);
  if (!valid.length) return { features: [] };

  const colorOf = new Map(valid.map((r) => [r.key, r.color]));

  const sig = routesSig(valid);
  if (!_cache || _cache.sig !== sig) {
    const proj = makeProjection(valid);
    _cache = { sig, proj, groups: detectGroups(valid, proj) };
  }
  const { proj, groups } = _cache;

  const dashM = Math.max(MIN_DASH_PX, DASH_PX) * metresPerPx(zoom, centerLat);

  const features = [];

  // Casing: one continuous halo per visible route (full raw shape).
  for (const r of valid) {
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: r.coords },
      properties: { kind: 'casing', key: r.key },
    });
  }

  // Colour lines from the detected groups.
  for (const g of groups) {
    const keys = g.keys;
    const toLL = (pts) => pts.map(proj.toLngLat);
    if (keys.length === 1) {
      features.push(lineFeat(toLL(g.ptsM), colorOf.get(keys[0]), keys[0]));
      continue;
    }
    const n = keys.length;
    if (g.lenM < dashM * n) {
      // too short for a full cycle — solid, owner's colour
      features.push(lineFeat(toLL(g.ptsM), colorOf.get(keys[0]), keys[0]));
      continue;
    }
    // one MultiLineString per colour: all the dash pieces that land on it
    const byColor = keys.map(() => []);
    sliceByArc(g.ptsM, dashM).forEach((piece, i) => {
      byColor[i % n].push(toLL(piece));
    });
    keys.forEach((k, i) => {
      if (!byColor[i].length) return;
      features.push({
        type: 'Feature',
        geometry: { type: 'MultiLineString', coordinates: byColor[i] },
        properties: { kind: 'line', color: colorOf.get(k), key: k },
      });
    });
  }

  return { features };
}

function lineFeat(coords, color, key) {
  return {
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: coords },
    properties: { kind: 'line', color, key },
  };
}

/** Plain fallback — one solid full line per route, no striping. */
export function plainRouteFeatures(routes) {
  const out = [];
  for (const r of routes || []) {
    if (!Array.isArray(r.coords) || r.coords.length < 2) continue;
    out.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: r.coords },
      properties: { kind: 'casing', key: r.key },
    });
    out.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: r.coords },
      properties: { kind: 'line', color: r.color, key: r.key },
    });
  }
  return out;
}
