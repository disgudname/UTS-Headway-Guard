// livemap/core/layers/route-overlap.js
// -----------------------------------------------------------------------------
// Alternating-colour route striping — the TransLoc-patented technique
// (US7920967B1): where two routes follow the same path, draw that stretch as a
// repeating cycle of every sharing route's colour instead of one colour winning.
//
// Model (after several failed "owner borrows geometry" variants that always left
// a gap where routes split apart): EVERY route draws its OWN complete path.
// Detection tags each ~14 m segment with the set of routes sharing it; a route's
// path is then split into runs by that set. A run shared by N routes renders as
// this route's phase-slot of a dash cycle (piece k is "on" for rank == k mod N),
// so on a coincident corridor N routes' runs interleave into an A-B-C stripe and
// where they diverge each simply traces its own path. Gap-free by construction.
//
// Two deliberate differences from testmap.js's `OverlapRouteRenderer`:
//   1. Detection runs in a LOCAL METRES projection, zoom-independent, memoised
//      on the route set. Only the dash slicing (screen-space) reruns per zoom.
//   2. Render is GEOMETRY-LEVEL slicing, not Leaflet `dashArray`/`dashOffset`
//      (MapLibre's `line-dasharray` is per-layer with no phase offset).
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
            s.sharedWith.add(o.key);
            o.sharedWith.add(s.key);
          }
        }
      }
    }
  }

  // Smooth single-sample dropouts: a shared corridor where the overlap test
  // flickers off for one or two samples (a kink, resample phase) would
  // otherwise leave a stray solid crumb of one route's colour sitting beside
  // the striped line. If a short run's shared set differs from both its
  // neighbours and they agree, adopt theirs.
  for (const segs of segsByKey.values()) {
    smoothSharedSets(segs.slice().sort((a, b) => a.cum - b.cum));
  }

  // EVERY route emits its OWN complete path, split into runs by shared-set. No
  // "owner" borrows geometry from another — that model always left a gap where
  // routes split apart (one line's segments deferred to an owner whose polyline
  // had turned away). A run shared by N routes is rendered as this route's Nth
  // phase-slot of the dash cycle, so N routes' runs interleave into a stripe on
  // a coincident corridor and each traces its own path where they diverge.
  const runs = [];
  for (const [key, segs] of segsByKey) {
    const ordered = segs.slice().sort((a, b) => a.cum - b.cum);
    let cur = null;
    const flush = () => {
      if (cur && cur.pts.length >= 2) {
        let len = 0;
        for (let i = 1; i < cur.pts.length; i++) len += dist(cur.pts[i - 1], cur.pts[i]);
        runs.push({ thisKey: key, sharedKeys: cur.keys, ptsM: cur.pts, lenM: len });
      }
      cur = null;
    };
    for (const s of ordered) {
      const set = [...s.sharedWith].sort();
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

  return runs;
}

const CRUMB_MAX_SEGS = 4; // a solo run this short (~56 m), sandwiched between two
                          // shared runs of the same set, is a detection dropout —
                          // relabel it so it stripes with its neighbours

function setSig(set) {
  return [...set].sort().join('|');
}

function smoothSharedSets(ordered) {
  if (ordered.length < 3) return;
  // collapse into runs of identical shared-set signature
  const runs = [];
  for (const s of ordered) {
    const sig = setSig(s.sharedWith);
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
      const donor = runs[i - 1].segs[0].sharedWith;
      for (const s of run.segs) s.sharedWith = new Set(donor);
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

let _cache = null; // { sig, proj, runs }

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
    _cache = { sig, proj, runs: detectGroups(valid, proj) };
  }
  const { proj, runs } = _cache;

  const dashM = Math.max(MIN_DASH_PX, DASH_PX) * metresPerPx(zoom, centerLat);

  const features = [];

  for (const run of runs) {
    const { thisKey, sharedKeys, ptsM } = run;
    const n = sharedKeys.length;
    const path = ptsM.map(proj.toLngLat);
    const color = colorOf.get(thisKey);

    // Casing along THIS route's own path for every run — so the white halo is
    // continuous under every route everywhere, including where it diverges.
    features.push({
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: path },
      properties: { kind: 'casing', key: thisKey },
    });

    if (n === 1 || run.lenM < dashM * 1.5) {
      // alone here (or too short to cycle) → solid, this route's colour
      features.push(lineFeat(path, color, thisKey));
      continue;
    }

    // Shared: keep this route's phase slot of the dash cycle. Rank = position in
    // the sorted shared-key set. Piece k is "on" for the route whose rank ==
    // k mod n, so N routes' runs interleave into an even A-B-C-A-B-C stripe.
    const rank = sharedKeys.indexOf(thisKey);
    const pieces = sliceByArc(ptsM, dashM);
    const mine = [];
    for (let k = 0; k < pieces.length; k++) {
      if (k % n === rank) mine.push(pieces[k].map(proj.toLngLat));
    }
    if (mine.length) {
      features.push({
        type: 'Feature',
        geometry: { type: 'MultiLineString', coordinates: mine },
        properties: { kind: 'line', color, key: thisKey },
      });
    } else {
      // never got a slot (very short run) → a thin solid so it isn't a gap
      features.push(lineFeat(path, color, thisKey));
    }
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
