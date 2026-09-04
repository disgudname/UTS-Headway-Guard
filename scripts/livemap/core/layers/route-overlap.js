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
// Two routes closer than this (and roughly aligned) = one corridor. Opposite
// directions get a looser bound: that's the divided-road case (two carriageways
// either side of a planted median, e.g. JPA at Observatory Ave), where the
// polyline centrelines are genuinely ~15-20 m apart but riders read it as one
// street. Same direction stays tight so parallel nearby streets don't merge.
const MATCH_TOL_PARALLEL_M = 18;
const MATCH_TOL_ANTIPARALLEL_M = 28;
const MATCH_TOL_MAX_M = Math.max(MATCH_TOL_PARALLEL_M, MATCH_TOL_ANTIPARALLEL_M);
const HEADING_TOL_RAD = (20 * Math.PI) / 180;
const DASH_PX = 16; // one colour's run length on screen
const MIN_DASH_PX = 0.5;
// Two polylines this close here = genuinely coincident: safe for one to borrow
// the other's stripe. Beyond it (up to MATCH_TOL) they still count as the same
// corridor for detection, but each draws its own path — otherwise a gap opens
// where they fork apart. Sized to clear TransLoc shape noise (~2-5 m) but split
// a divided road / a widening fork.
const TIGHT_M = 8;
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
      // Routes whose polyline is right next to this one here (overlap test +
      // ≤ TIGHT_M). A run of this route's segments is drawn once, striped
      // through its tight set, owned by the lowest key in it.
      tightWith: new Set([key]),
    });
  }
  return segs;
}

/** 0 = not aligned, 1 = same direction, -1 = opposite direction. */
function alignment(h1, h2) {
  let d = Math.abs(h1 - h2) % (Math.PI * 2);
  if (d > Math.PI) d = Math.PI * 2 - d;
  if (d <= HEADING_TOL_RAD) return 1;
  if (Math.abs(Math.PI - d) <= HEADING_TOL_RAD) return -1;
  return 0;
}

function segmentsOverlap(a, b) {
  const align = alignment(a.heading, b.heading);
  if (align === 0) return false;
  const tol = align < 0 ? MATCH_TOL_ANTIPARALLEL_M : MATCH_TOL_PARALLEL_M;
  if (dist(a.mid, b.mid) > tol) return false;
  const m = Math.min(
    dist(a.a, b.a),
    dist(a.b, b.b),
    dist(a.a, b.b),
    dist(a.b, b.a),
  );
  return m <= tol * 2;
}

// --- shared-corridor detection (zoom-independent) ---------------------------

function detectGroups(routes, proj) {
  const segsByKey = new Map();
  const cell = MATCH_TOL_MAX_M * 2;
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
            if (dist(s.mid, o.mid) <= TIGHT_M) {
              s.tightWith.add(o.key);
              o.tightWith.add(s.key);
            }
          }
        }
      }
    }
  }

  // Smooth single-sample dropouts: where the tight test flickers off for a
  // sample or two (a kink, resample phase), relabel the short run to match its
  // agreeing neighbours so it doesn't leave a stray crumb beside the stripe.
  for (const segs of segsByKey.values()) {
    smoothSets(segs.slice().sort((a, b) => a.cum - b.cum), 'tightWith');
  }

  // Walk each route's own segments. It renders a run of them unless a TIGHT
  // lower-keyed route is right beside it there (safe to borrow that route's
  // stripe). "Tight lower-keyed" — not just "lower-keyed" — is the whole fix:
  // at a fork the owner's polyline has drifted 10-18 m off, so it's no longer
  // tight, so this route stops borrowing and draws its own path → no gap.
  // A rendered run's `keys` is the set of routes tight here, so the stripe
  // reflects who's actually coincident, and it's owned by the lowest of those.
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
      const tight = [...s.tightWith].sort();
      // Borrow another route's stripe only if that route is BOTH lower-keyed
      // AND tight to us here.
      if (tight[0] !== key) {
        flush();
        continue;
      }
      const sameSet = cur && cur.keys.length === tight.length && cur.keys.every((k, i) => k === tight[i]);
      const contiguous = cur && dist(cur.pts[cur.pts.length - 1], s.a) <= SAMPLE_STEP_M * 1.5;
      if (!sameSet || !contiguous) {
        flush();
        cur = { keys: tight, pts: [s.a, s.b] };
      } else {
        cur.pts.push(s.b);
      }
    }
    flush();
  }

  return groups;
}

const CRUMB_MAX_SEGS = 4; // a solo run this short (~56 m), sandwiched between two
                          // shared runs of the same set, is a detection dropout —
                          // relabel it so it stripes with its neighbours

function setSig(set) {
  return [...set].sort().join('|');
}

function smoothSets(ordered, field) {
  if (ordered.length < 3) return;
  // collapse into runs of identical set signature for `field`
  const runs = [];
  for (const s of ordered) {
    const sig = setSig(s[field]);
    if (runs.length && runs[runs.length - 1].sig === sig) runs[runs.length - 1].segs.push(s);
    else runs.push({ sig, segs: [s] });
  }
  for (let i = 1; i < runs.length - 1; i++) {
    const run = runs[i];
    if (
      run.segs.length <= CRUMB_MAX_SEGS &&
      runs[i - 1].sig === runs[i + 1].sig &&
      run.sig !== runs[i - 1].sig
    ) {
      const donor = runs[i - 1].segs[0][field];
      for (const s of run.segs) s[field] = new Set(donor);
      run.sig = runs[i - 1].sig;
    }
  }
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

  for (const g of groups) {
    const keys = g.keys;
    const n = keys.length;
    const path = g.ptsM.map(proj.toLngLat);

    // Casing follows the GROUP geometry, not each raw route — where a shared
    // corridor is collapsed onto the owner's alignment, the other route has no
    // group there and so contributes no casing. (Emitting casing per raw route
    // left a bare halo line running down the empty side of a merged divided
    // road — invisible in day mode, an obvious dark stripe at night.)
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: path },
      properties: { kind: 'casing', key: keys[0] },
    });

    if (n === 1 || g.lenM < dashM * n) {
      // solo run, or too short for one full colour cycle → solid owner colour
      features.push(lineFeat(path, colorOf.get(keys[0]), keys[0]));
      continue;
    }

    // one MultiLineString per colour: all the dash pieces that land on it
    const byColor = keys.map(() => []);
    sliceByArc(g.ptsM, dashM).forEach((piece, i) => {
      byColor[i % n].push(piece.map(proj.toLngLat));
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
