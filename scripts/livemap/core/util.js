// livemap/core/util.js
// -----------------------------------------------------------------------------
// Tiny dependency-free helpers. Keep this file small and boring.
// -----------------------------------------------------------------------------

/** Read a URL query param, returning `fallback` when absent/blank. */
export function param(name, fallback = null) {
  const v = new URLSearchParams(location.search).get(name);
  return v == null || v === '' ? fallback : v;
}

/** URL param parsed as a boolean. `?x`, `?x=1`, `?x=true` are all true. */
export function paramBool(name, fallback = false) {
  const raw = new URLSearchParams(location.search).get(name);
  if (raw == null) return fallback;
  if (raw === '') return true;
  return /^(1|true|yes|on)$/i.test(raw);
}

/** URL param parsed as a finite number, or `fallback`. */
export function paramNum(name, fallback = null) {
  const n = Number.parseFloat(new URLSearchParams(location.search).get(name));
  return Number.isFinite(n) ? n : fallback;
}

/** localStorage read that never throws (private mode, disabled storage, ...). */
export function lsGet(key, fallback = null) {
  try {
    const v = localStorage.getItem(key);
    return v == null ? fallback : v;
  } catch {
    return fallback;
  }
}

/** localStorage write that never throws. */
export function lsSet(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* ignore */
  }
}

/** Minimal event emitter — `on` returns an unsubscribe function. */
export function emitter() {
  const map = new Map(); // event -> Set<fn>
  return {
    on(event, fn) {
      if (!map.has(event)) map.set(event, new Set());
      map.get(event).add(fn);
      return () => map.get(event)?.delete(fn);
    },
    emit(event, ...args) {
      map.get(event)?.forEach((fn) => {
        try {
          fn(...args);
        } catch (err) {
          console.error(`[livemap] listener for "${event}" threw`, err);
        }
      });
    },
  };
}

/** Promise that resolves after `ms`. */
export const wait = (ms) => new Promise((r) => setTimeout(r, ms));

/** Clamp `n` into [min, max]. */
export const clamp = (n, min, max) => Math.min(max, Math.max(min, n));

/** Trailing-edge debounce. */
export function debounce(fn, ms) {
  let t = 0;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

// --- colour helpers (used by the dark basemap treatment) --------------------

/** Parse "#rgb", "#rrggbb", or "rgb()/rgba()" into {r,g,b,a} 0-255 / 0-1. */
export function parseColor(input) {
  if (typeof input !== 'string') return null;
  const s = input.trim();
  let m = /^#([0-9a-f]{3})$/i.exec(s);
  if (m) {
    const [r, g, b] = m[1].split('').map((c) => parseInt(c + c, 16));
    return { r, g, b, a: 1 };
  }
  m = /^#([0-9a-f]{6})$/i.exec(s);
  if (m) {
    const n = parseInt(m[1], 16);
    return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255, a: 1 };
  }
  m = /^rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+))?\s*\)$/i.exec(s);
  if (m) {
    return { r: +m[1], g: +m[2], b: +m[3], a: m[4] == null ? 1 : +m[4] };
  }
  return null;
}

/** {r,g,b,a} -> css string. */
export function colorToCss({ r, g, b, a = 1 }) {
  const to = (x) => clamp(Math.round(x), 0, 255);
  return a >= 1
    ? `#${[to(r), to(g), to(b)].map((x) => x.toString(16).padStart(2, '0')).join('')}`
    : `rgba(${to(r)},${to(g)},${to(b)},${+a.toFixed(3)})`;
}

/** Perceived luminance 0-1 (Rec. 601). */
export function luminance({ r, g, b }) {
  return (0.299 * r + 0.587 * g + 0.114 * b) / 255;
}

/** Mix two {r,g,b,a} colours, `t` from 0 (a) to 1 (b). */
export function mix(a, b, t) {
  return {
    r: a.r + (b.r - a.r) * t,
    g: a.g + (b.g - a.g) * t,
    b: a.b + (b.b - a.b) * t,
    a: (a.a ?? 1) + ((b.a ?? 1) - (a.a ?? 1)) * t,
  };
}

// --- geometry --------------------------------------------------------------

/**
 * Decode a Google "encoded polyline" string into `[[lng, lat], ...]` (GeoJSON
 * coordinate order). TransLoc hands us route shapes this way in
 * `EncodedPolyline`. Precision 5 is the standard.
 */
export function decodePolyline(str, precision = 5) {
  if (typeof str !== 'string' || !str) return [];
  const factor = Math.pow(10, precision);
  const coords = [];
  let index = 0;
  let lat = 0;
  let lng = 0;

  while (index < str.length) {
    let result = 0;
    let shift = 0;
    let b;
    do {
      b = str.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    lat += result & 1 ? ~(result >> 1) : result >> 1;

    result = 0;
    shift = 0;
    do {
      b = str.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    lng += result & 1 ? ~(result >> 1) : result >> 1;

    coords.push([lng / factor, lat / factor]);
  }
  return coords;
}

/**
 * Douglas–Peucker simplify of a pixel-space path `[[x,y], ...]`, dropping points
 * that sit within `tol` px of the line between the points that survive. Used by
 * the route-overlap renderer before resampling. Returns a new array.
 */
export function simplifyPath(points, tol) {
  if (!Array.isArray(points) || points.length < 3 || !(tol > 0)) {
    return Array.isArray(points) ? points.slice() : [];
  }
  const tol2 = tol * tol;
  const keep = new Uint8Array(points.length);
  keep[0] = 1;
  keep[points.length - 1] = 1;

  const stack = [[0, points.length - 1]];
  while (stack.length) {
    const [lo, hi] = stack.pop();
    if (hi - lo < 2) continue;
    const [ax, ay] = points[lo];
    const [bx, by] = points[hi];
    const dx = bx - ax;
    const dy = by - ay;
    const len2 = dx * dx + dy * dy;
    let far = -1;
    let farD = tol2;
    for (let i = lo + 1; i < hi; i++) {
      const [px, py] = points[i];
      let d2;
      if (len2 === 0) {
        d2 = (px - ax) * (px - ax) + (py - ay) * (py - ay);
      } else {
        let t = ((px - ax) * dx + (py - ay) * dy) / len2;
        t = t < 0 ? 0 : t > 1 ? 1 : t;
        const ex = px - (ax + t * dx);
        const ey = py - (ay + t * dy);
        d2 = ex * ex + ey * ey;
      }
      if (d2 > farD) {
        farD = d2;
        far = i;
      }
    }
    if (far !== -1) {
      keep[far] = 1;
      stack.push([lo, far], [far, hi]);
    }
  }

  const out = [];
  for (let i = 0; i < points.length; i++) if (keep[i]) out.push(points[i]);
  return out;
}
