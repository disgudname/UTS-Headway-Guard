// livemap/core/data/transloc.js
// -----------------------------------------------------------------------------
// The UTS (TransLoc) live feed: vehicle positions over SSE with a polling
// fallback, plus route metadata (colours + names) fetched once and refreshed
// occasionally.
//
// Consumers:
//   onVehicles(fn)  -> fn(normalizedVehicle[])   every tick
//   onMetadata(fn)  -> fn()                        when route colours change
//   onStatus(fn)    -> fn('live' | 'polling' | 'down')
//   getRouteColor(routeId) / getRouteName(routeId)
// -----------------------------------------------------------------------------

import { API_BASE, NO_ROUTE_COLOR } from '../config.js';
import { emitter, paramBool, decodePolyline } from '../util.js';

const SSE_URL = `${API_BASE}/v1/stream/testmap/vehicles`;
const VEHICLES_URL = `${API_BASE}/v1/testmap/transloc/vehicles`;
const METADATA_URL = `${API_BASE}/v1/testmap/transloc/metadata`;

const POLL_MS = 10_000;
const SSE_GAP_MS = 16_000; // if SSE goes this long without a message, poll
const METADATA_REFRESH_MS = 10 * 60_000;

const bus = emitter();
export const onVehicles = (fn) => bus.on('vehicles', fn);
export const onMetadata = (fn) => bus.on('metadata', fn);
/** Replays the current status immediately (a late subscriber still learns it). */
export const onStatus = (fn) => {
  if (status) {
    try { fn(status); } catch (err) { console.error('[livemap] status listener threw', err); }
  }
  return bus.on('status', fn);
};
export const onRoutes = (fn) => bus.on('routes', fn);
export const onStops = (fn) => bus.on('stops', fn);

const routeColor = new Map(); // RouteID(str) -> "#rrggbb"
const routeName = new Map(); // RouteID(str) -> "Gold Line"
const routeShape = new Map(); // RouteID(str) -> { id, name, color, coords: [[lng,lat],...] }
let routeShapeSig = '';

// One entry per *physical* stop (metadata lists a separate route-stop per route
// serving a shelter; we fold those on rounded lat/lon). Shape:
//   { key, lat, lng, name, routeIds:[str], members:[{ routeStopId:str, routeId:str }] }
let stopGroups = [];
let stopSig = '';

/** Every physical UTS stop currently known. */
export function getStops() {
  return stopGroups;
}

export function getRouteColor(routeId) {
  return routeColor.get(String(routeId)) || NO_ROUTE_COLOR;
}
export function getRouteName(routeId) {
  return routeName.get(String(routeId)) || '';
}
/** Every route that currently has a drawable shape. */
export function getRoutes() {
  return [...routeShape.values()];
}

let started = false;
let lastMessageAt = 0;
let status = '';
let source = null;
let pollTimer = 0;
let metaTimer = 0;

function setStatus(next) {
  if (next === status) return;
  status = next;
  bus.emit('status', next);
}

/** Raw TransLoc vehicle -> the shape the rest of livemap uses. */
function normalize(v) {
  const routeId = v.RouteID ?? v.routeID ?? 0;
  const pct = typeof v.percentage === 'number' ? v.percentage : null;
  return {
    id: String(v.VehicleID),
    routeId: String(routeId),
    lng: Number(v.Longitude),
    lat: Number(v.Latitude),
    heading: Number(v.Heading) || 0,
    speedMph: Number(v.GroundSpeed) || 0,
    name: String(v.Name || v.VehicleID),
    ageS: Number(v.SecondsSinceReport) || 0,
    stale: !!v.IsStale,
    veryStale: !!v.IsVeryStale,
    routeName: v.RouteName || getRouteName(routeId),
    capacity: Number(v.capacity) || null,
    onboard: Number.isFinite(v.current_occupation) ? v.current_occupation : null,
    occupancy: pct, // 0..1 or null
  };
}

function emitVehicles(list) {
  const out = [];
  for (const v of Array.isArray(list) ? list : []) {
    const n = normalize(v);
    if (Number.isFinite(n.lng) && Number.isFinite(n.lat) && !n.veryStale) out.push(n);
  }
  bus.emit('vehicles', out);
}

// --- metadata ------------------------------------------------------------------

async function loadMetadata() {
  try {
    const r = await fetch(METADATA_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`metadata HTTP ${r.status}`);
    const data = await r.json();
    let changed = false;
    const shapes = new Map();
    for (const route of data.routes || []) {
      const id = String(route.RouteID);
      const color = normalizeHex(route.MapLineColor);
      const name = route.Description || route.RouteName || '';
      if (color && routeColor.get(id) !== color) {
        routeColor.set(id, color);
        changed = true;
      }
      if (name && routeName.get(id) !== name) {
        routeName.set(id, name);
        changed = true;
      }
      const coords = decodePolyline(route.EncodedPolyline || '');
      if (coords.length >= 2) {
        shapes.set(id, {
          id,
          name: name || routeName.get(id) || `Route ${id}`,
          color: color || routeColor.get(id) || NO_ROUTE_COLOR,
          coords,
        });
      }
    }
    if (changed) bus.emit('metadata');
    commitRouteShapes(shapes);
    commitStops(data.stops || []);
  } catch (err) {
    console.warn('[livemap] route metadata load failed', err);
  }
}

/** Fold the per-route stop list into physical stops and emit on change. */
function commitStops(raw) {
  const byLoc = new Map();
  for (const s of raw) {
    const lat = Number(s.Latitude);
    const lng = Number(s.Longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    const key = `${lat.toFixed(5)},${lng.toFixed(5)}`;
    const routeId = s.RouteIds && s.RouteIds.length ? String(s.RouteIds[0]) : '';
    const routeStopId = String(s.RouteStopID ?? s.StopID ?? '');
    let g = byLoc.get(key);
    if (!g) {
      g = { key, lat, lng, name: s.Name || s.Description || 'Stop', routeIds: [], members: [] };
      byLoc.set(key, g);
    }
    g.members.push({ routeStopId, routeId });
    if (routeId && !g.routeIds.includes(routeId)) g.routeIds.push(routeId);
  }
  const groups = [...byLoc.values()];
  const sig = groups
    .map((g) => `${g.key}:${g.routeIds.slice().sort().join(',')}`)
    .sort()
    .join('|');
  if (sig === stopSig) return;
  stopSig = sig;
  stopGroups = groups;
  bus.emit('stops', stopGroups);
}

/** Swap in a fresh set of route shapes, emitting only when it actually differs. */
function commitRouteShapes(shapes) {
  const sig = [...shapes.values()]
    .map((s) => `${s.id}:${s.color}:${s.coords.length}`)
    .sort()
    .join('|');
  if (sig === routeShapeSig) return;
  routeShapeSig = sig;
  routeShape.clear();
  for (const [id, s] of shapes) routeShape.set(id, s);
  bus.emit('routes', getRoutes());
}

function normalizeHex(c) {
  if (typeof c !== 'string') return null;
  const s = c.trim();
  if (/^#[0-9a-f]{6}$/i.test(s)) return s.toLowerCase();
  if (/^[0-9a-f]{6}$/i.test(s)) return `#${s.toLowerCase()}`;
  return null;
}

// --- SSE + polling -----------------------------------------------------------

function connectSSE() {
  try {
    source = new EventSource(SSE_URL);
  } catch (err) {
    console.warn('[livemap] EventSource unavailable, polling only', err);
    startPolling();
    return;
  }
  source.onopen = () => {
    lastMessageAt = Date.now();
    setStatus('live');
    stopPolling();
  };
  source.onmessage = (evt) => {
    lastMessageAt = Date.now();
    setStatus('live');
    stopPolling();
    try {
      const payload = JSON.parse(evt.data);
      emitVehicles(payload.vehicles);
    } catch (err) {
      console.warn('[livemap] bad SSE payload', err);
    }
  };
  source.onerror = () => {
    // EventSource retries on its own; cover the gap with polling.
    if (Date.now() - lastMessageAt > SSE_GAP_MS) {
      setStatus('polling');
      startPolling();
    }
  };

  // Watchdog: silent SSE (proxy buffering, sleep/wake) also triggers polling.
  setInterval(() => {
    if (started && Date.now() - lastMessageAt > SSE_GAP_MS) {
      setStatus(source && source.readyState === 1 ? 'polling' : 'down');
      startPolling();
    }
  }, POLL_MS);
}

async function pollOnce() {
  try {
    const r = await fetch(VEHICLES_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`vehicles HTTP ${r.status}`);
    const data = await r.json();
    emitVehicles(data.vehicles);
    if (status !== 'live') setStatus('polling');
  } catch (err) {
    console.warn('[livemap] vehicle poll failed', err);
    setStatus('down');
  }
}

function startPolling() {
  if (pollTimer) return;
  pollOnce();
  pollTimer = setInterval(pollOnce, POLL_MS);
}
function stopPolling() {
  if (!pollTimer) return;
  clearInterval(pollTimer);
  pollTimer = 0;
}

/** Begin the feed. Safe to call once. */
export function startVehicleFeed() {
  if (started) return;
  started = true;

  // ?mock=1 -> synthetic moving buses. For offline dev, demos and screenshots
  // where the real TransLoc key isn't available.
  if (paramBool('mock')) {
    startMockFeed();
    return;
  }

  loadMetadata();
  metaTimer = setInterval(loadMetadata, METADATA_REFRESH_MS);
  connectSSE();
}

// --- mock feed --------------------------------------------------------------

function startMockFeed() {
  const CENTER = [-78.5098, 38.0355];
  const LAT_SQUEEZE = 0.72; // rough lon/lat scale at this latitude, keeps loops round
  const TICK_MS = 1200;
  const ROUTES = [
    { id: '901', name: 'Gold Line (mock)', color: '#f2a900' },
    { id: '902', name: 'Blue Line (mock)', color: '#0d3268' },
    { id: '903', name: 'Green Line (mock)', color: '#00843d' },
    { id: '904', name: 'Orange Line (mock)', color: '#e57200' },
  ];
  const mockShapes = new Map();
  ROUTES.forEach((r, i) => {
    routeColor.set(r.id, r.color);
    routeName.set(r.id, r.name);
    // One representative loop per route so local dev has real geometry to draw.
    const radius = 0.0052 + i * 0.0016;
    const coords = [];
    for (let a = 0; a <= 64; a++) {
      const t = (a / 64) * Math.PI * 2;
      coords.push([
        CENTER[0] + Math.cos(t) * radius,
        CENTER[1] + Math.sin(t) * radius * LAT_SQUEEZE,
      ]);
    }
    mockShapes.set(r.id, { id: r.id, name: r.name, color: r.color, coords });
  });
  bus.emit('metadata');
  commitRouteShapes(mockShapes);

  // Mock stops: 9 evenly spaced around each mock loop...
  const mockStops = [];
  ROUTES.forEach((r, i) => {
    const radius = 0.0052 + i * 0.0016;
    for (let k = 0; k < 9; k++) {
      const t = (k / 9) * Math.PI * 2;
      const id = 90000 + i * 100 + k;
      mockStops.push({
        RouteStopID: id,
        StopID: id,
        Name: `${r.name.replace(' (mock)', '')} Stop ${k + 1}`,
        Latitude: CENTER[1] + Math.sin(t) * radius * LAT_SQUEEZE,
        Longitude: CENTER[0] + Math.cos(t) * radius,
        RouteIds: [r.id],
      });
    }
  });
  // ...plus a few hand-placed shared stops so the multi-route pie is visible.
  const shared = [
    { d: 0.0016, routes: ['901', '902'] },
    { d: -0.0016, routes: ['902', '903', '904'] },
    { d: 0.0032, routes: ['901', '902', '903', '904'] },
  ];
  shared.forEach((sh, j) => {
    sh.routes.forEach((rid, n) => {
      const id = 95000 + j * 10 + n;
      mockStops.push({
        RouteStopID: id,
        StopID: id,
        Name: `Shared Stop ${j + 1}`,
        Latitude: CENTER[1] + sh.d * LAT_SQUEEZE,
        Longitude: CENTER[0] + sh.d,
        RouteIds: [rid],
      });
    });
  });
  commitStops(mockStops);
  setStatus('live');

  const fleet = [];
  for (let i = 0; i < 11; i++) {
    const route = ROUTES[i % ROUTES.length];
    fleet.push({
      id: 6100 + i,
      route,
      radius: 0.0055 + (i % 4) * 0.0016,
      angle: (i / 11) * Math.PI * 2,
      angVel: (0.22 + (i % 3) * 0.06) * (i % 2 ? 1 : -1), // rad/sec, both directions
      onboard: 6 + ((i * 7) % 38),
      dwellUntil: 0,
    });
  }

  let last = performance.now();
  const emit = () => {
    const now = performance.now();
    const dt = Math.min((now - last) / 1000, 3);
    last = now;

    const vehicles = fleet.map((b) => {
      const moving = now >= b.dwellUntil;
      if (moving && Math.random() < 0.05) b.dwellUntil = now + 5000 + Math.random() * 9000;
      if (moving) b.angle += b.angVel * dt;

      const a = b.angle;
      const lat = CENTER[1] + Math.sin(a) * b.radius * LAT_SQUEEZE;
      const lon = CENTER[0] + Math.cos(a) * b.radius;
      // Velocity vector along the ellipse, in (east, north); heading is CW from N.
      const dEast = -Math.sin(a) * Math.sign(b.angVel);
      const dNorth = Math.cos(a) * LAT_SQUEEZE * Math.sign(b.angVel);
      const heading = ((Math.atan2(dEast, dNorth) * 180) / Math.PI + 360) % 360;

      b.onboard = Math.max(0, Math.min(50, b.onboard + (Math.random() < 0.5 ? -1 : 1)));

      return {
        VehicleID: b.id,
        RouteID: b.route.id,
        Latitude: lat,
        Longitude: lon,
        Heading: heading,
        GroundSpeed: moving ? 9 + Math.random() * 11 : 0,
        Name: String(b.id),
        SecondsSinceReport: b.id % 5 === 0 ? 110 : Math.random() * 14,
        IsStale: b.id % 5 === 0,
        IsVeryStale: false,
        RouteName: b.route.name,
        capacity: 50,
        current_occupation: b.onboard,
        percentage: b.onboard / 50,
      };
    });
    emitVehicles(vehicles);
  };

  emit();
  setInterval(emit, TICK_MS);
}
