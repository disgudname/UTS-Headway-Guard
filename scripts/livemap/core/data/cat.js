// livemap/core/data/cat.js
// -----------------------------------------------------------------------------
// Charlottesville Area Transit (CAT) — the secondary agency overlay. Unlike the
// UTS feed this is entirely opt-in: nothing is fetched until the user turns the
// overlay on (persisted `livemap.cat.enabled`), and polling stops again when
// they turn it off.
//
// Consumers:
//   onCatEnabled(fn) -> fn(bool)      (replays the current state immediately)
//   onCatRoutes(fn)  -> fn(shape[])   pattern polylines, on metadata refresh
//   onCatStops(fn)   -> fn(stop[])    folded physical stops
//   onCatVehicles(fn)-> fn(vehicle[]) every ~8s while enabled
//   onCatAlerts(fn)  -> fn(alert[])
//   fetchCatStopEtas(stopId) -> Promise<eta[]>   (on demand, for a stop popup)
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter, lsGet, lsSet, paramBool, decodePolyline } from '../util.js';

const ROUTES_URL = `${API_BASE}/v1/testmap/cat/routes`;
const PATTERNS_URL = `${API_BASE}/v1/testmap/cat/patterns`;
const STOPS_URL = `${API_BASE}/v1/testmap/cat/stops`;
const VEHICLES_URL = `${API_BASE}/v1/testmap/cat/vehicles`;
const ALERTS_URL = `${API_BASE}/v1/testmap/cat/service-alerts`;
const STOP_ETAS_URL = `${API_BASE}/v1/testmap/cat/stop-etas`;

const VEHICLES_POLL_MS = 8_000;
const META_POLL_MS = 5 * 60_000;
const ALERTS_POLL_MS = 60_000;

const ENABLED_KEY = 'livemap.cat.enabled';
const HIDDEN_KEY = 'livemap.cat.routes.hidden';
// CAT tags every out-of-service unit with this synthetic route id (matches the
// legacy testmap convention). Treated as "not in service", drawn black.
const OUT_OF_SERVICE_ROUTE_ID = '777';
const CAT_NEUTRAL = '#5b6472';
const OUT_OF_SERVICE_COLOR = '#000000';

const bus = emitter();
export const onCatRoutes = (fn) => bus.on('routes', fn);
export const onCatStops = (fn) => bus.on('stops', fn);
export const onCatVehicles = (fn) => bus.on('vehicles', fn);
export const onCatAlerts = (fn) => bus.on('alerts', fn);
/** Replays the current enabled state immediately, then subscribes to changes. */
export const onCatEnabled = (fn) => {
  try { fn(enabled); } catch (err) { console.error('[livemap] cat-enabled listener threw', err); }
  return bus.on('enabled', fn);
};
/** Per-CAT-route visibility. fn(groups): [{ name, color, ids:[], hidden }]. */
export const onCatRouteVisibility = (fn) => {
  try { fn(catRouteGroups()); } catch (err) { console.error('[livemap] cat-routeVis listener threw', err); }
  return bus.on('routeVis', fn);
};

let enabled = lsGet(ENABLED_KEY, '0') === '1';
export const isCatEnabled = () => enabled;

const routeColor = new Map(); // RouteID(str) -> "#rrggbb"
const routeName = new Map(); // RouteID(str) -> "1 PVCC/Woolen Mills"
const routeAbbr = new Map(); // RouteID(str) -> "1"
const hidden = loadHidden(); // Set<RouteID str> the picker has switched off

let catRoutes = []; // [{ id, name, abbr, color }] from /routes — the picker's list
let routeShapes = []; // [{ id (patternId), routeId, name, color, coords:[[lng,lat],...] }]
let stops = []; // [{ key, lat, lng, name, stopId, routeIds:[str], members:[{routeStopId, routeId, stopId}] }]
let vehicles = []; // [{ id, lat, lng, heading, routeId, inService, color, label, routeName, routeAbbr, speed }]
let alerts = []; // [{ id, title, message, routes }]

export const getCatRoutes = () => routeShapes;
export const getCatStops = () => stops;
export const getCatVehicles = () => vehicles;
export const getCatAlerts = () => alerts;
export const catRouteColor = (id) => routeColor.get(String(id)) || CAT_NEUTRAL;
export const catRouteName = (id) => routeName.get(String(id)) || '';
export const catRouteAbbr = (id) => routeAbbr.get(String(id)) || '';
export const isCatRouteHidden = (routeId) => hidden.has(String(routeId));

let started = false;
let vehTimer = 0;
let metaTimer = 0;
let alertTimer = 0;

/** Begin the feed machinery. Safe to call once; only actually polls if enabled. */
export function startCatFeed() {
  if (started) return;
  started = true;
  if (paramBool('mock')) {
    seedMock();
    return;
  }
  if (enabled) spin();
}

export function setCatEnabled(v) {
  v = !!v;
  if (v === enabled) return;
  enabled = v;
  lsSet(ENABLED_KEY, v ? '1' : '0');
  bus.emit('enabled', enabled);
  if (paramBool('mock')) return; // mock data is already seeded
  if (enabled) spin();
  else idle();
}

// --- per-route visibility (the right-panel CAT picker) ---------------------

function loadHidden() {
  try {
    const arr = JSON.parse(lsGet(HIDDEN_KEY, '[]'));
    return new Set(Array.isArray(arr) ? arr.map(String) : []);
  } catch {
    return new Set();
  }
}
function persistHidden() {
  lsSet(HIDDEN_KEY, JSON.stringify([...hidden]));
}
function pruneHidden() {
  const live = new Set(catRoutes.map((r) => r.id));
  for (const id of [...hidden]) if (!live.has(id)) hidden.delete(id);
}

/** [{ name, color, ids:[RouteID], hidden }], one per distinct CAT route name. */
export function catRouteGroups() {
  const byName = new Map();
  for (const r of catRoutes) {
    let g = byName.get(r.name);
    if (!g) {
      g = { name: r.name, color: r.color, ids: [] };
      byName.set(r.name, g);
    }
    g.ids.push(r.id);
  }
  const out = [...byName.values()];
  for (const g of out) g.hidden = g.ids.every((id) => hidden.has(id));
  out.sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true }));
  return out;
}

export function setCatGroupHidden(name, hide) {
  let touched = false;
  for (const r of catRoutes) {
    if (r.name !== name) continue;
    if (hide && !hidden.has(r.id)) { hidden.add(r.id); touched = true; }
    else if (!hide && hidden.has(r.id)) { hidden.delete(r.id); touched = true; }
  }
  if (!touched) return;
  persistHidden();
  bus.emit('routeVis', catRouteGroups());
}

export function setCatAllHidden(hide) {
  hidden.clear();
  if (hide) for (const r of catRoutes) hidden.add(r.id);
  persistHidden();
  bus.emit('routeVis', catRouteGroups());
}

function spin() {
  loadMeta();
  loadAlerts();
  pollVehicles();
  if (!metaTimer) metaTimer = setInterval(loadMeta, META_POLL_MS);
  if (!alertTimer) alertTimer = setInterval(loadAlerts, ALERTS_POLL_MS);
  if (!vehTimer) vehTimer = setInterval(pollVehicles, VEHICLES_POLL_MS);
}

function idle() {
  clearInterval(metaTimer);
  clearInterval(alertTimer);
  clearInterval(vehTimer);
  metaTimer = alertTimer = vehTimer = 0;
  vehicles = [];
  bus.emit('vehicles', vehicles);
}

// --- metadata (routes + pattern shapes + stops) ------------------------------

async function loadMeta() {
  try {
    const [routesRes, patternsRes, stopsRes] = await Promise.all([
      fetch(ROUTES_URL, { cache: 'no-store' }),
      fetch(PATTERNS_URL, { cache: 'no-store' }),
      fetch(STOPS_URL, { cache: 'no-store' }),
    ]);
    const routesJson = routesRes.ok ? await routesRes.json() : { routes: [] };
    const patternsJson = patternsRes.ok ? await patternsRes.json() : { patterns: [] };
    const stopsJson = stopsRes.ok ? await stopsRes.json() : { stops: [] };

    routeColor.clear();
    routeName.clear();
    routeAbbr.clear();
    catRoutes = [];
    for (const r of routesJson.routes || []) {
      const id = String(r.RouteID ?? r.RouteId ?? r.id ?? '');
      if (!id) continue;
      const col = normHex(r.Color);
      if (col) routeColor.set(id, col);
      if (r.RouteName) routeName.set(id, String(r.RouteName));
      if (r.RouteAbbreviation) routeAbbr.set(id, String(r.RouteAbbreviation));
      catRoutes.push({
        id,
        name: r.RouteName ? String(r.RouteName) : `Route ${id}`,
        abbr: r.RouteAbbreviation ? String(r.RouteAbbreviation) : id,
        color: col || CAT_NEUTRAL,
      });
    }
    pruneHidden();
    bus.emit('routeVis', catRouteGroups());

    routeShapes = [];
    for (const p of patternsJson.patterns || []) {
      const enc = p.encLine || p.EncLine || p.EncodedPolyline || '';
      const coords = decodePolyline(enc);
      if (coords.length < 2) continue;
      const rid = String(
        p.RouteID ?? p.routeID ?? (Array.isArray(p.routes) ? p.routes[0] : '') ?? '',
      );
      const pid = String(p.PatternID ?? p.id ?? `${rid}-${routeShapes.length}`);
      routeShapes.push({
        id: pid,
        routeId: rid,
        name: p.name || p.extID || catRouteName(rid) || `Route ${rid}`,
        color: normHex(p.color) || catRouteColor(rid),
        coords,
      });
    }
    bus.emit('routes', routeShapes);

    stops = foldStops(stopsJson.stops || []);
    bus.emit('stops', stops);
  } catch (err) {
    console.warn('[livemap] CAT metadata load failed', err);
  }
}

/** One entry per physical stop; CAT lists a separate route-stop per route. */
function foldStops(raw) {
  const byLoc = new Map();
  for (const s of raw) {
    const lat = Number(s.Latitude);
    const lng = Number(s.Longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    const key = `${lat.toFixed(5)},${lng.toFixed(5)}`;
    const routeId = String(s.RouteID ?? '');
    const stopId = String(s.StopID ?? '');
    let g = byLoc.get(key);
    if (!g) {
      g = { key, lat, lng, name: s.StopName || s.Name || 'Stop', stopId, routeIds: [], members: [] };
      byLoc.set(key, g);
    }
    g.members.push({ routeStopId: String(s.RouteStopID ?? ''), routeId, stopId });
    if (!g.stopId && stopId) g.stopId = stopId;
    if (routeId && !g.routeIds.includes(routeId)) g.routeIds.push(routeId);
  }
  return [...byLoc.values()];
}

// --- vehicles ---------------------------------------------------------------

// The CAT feed gives no speed and a near-useless Heading, so we derive both from
// the displacement between consecutive polls: `kin` remembers each unit's last
// position + wall-clock time, and pollVehicles computes an EMA-smoothed speed
// and a movement-gated heading from that.
const kin = new Map(); // id -> { lat, lng, t, speedMph, heading }
const MOVE_MIN_M = 12; // displacement over a poll interval to count as "moving"
const SPEED_EMA = 0.5;

async function pollVehicles() {
  try {
    const r = await fetch(VEHICLES_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`CAT vehicles HTTP ${r.status}`);
    const data = await r.json();
    const list = Array.isArray(data?.vehicles) ? data.vehicles : [];
    const out = [];
    const now = Date.now();
    const seen = new Set();
    for (const v of list) {
      const lat = Number(v.Latitude);
      const lng = Number(v.Longitude);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
      const rid = String(v.RouteID ?? v.routeID ?? '');
      const inService = !!rid && rid !== OUT_OF_SERVICE_ROUTE_ID && routeColor.has(rid);
      // Out-of-service units are kept but flagged + drawn black (like a UTS
      // no-route bus). The public/authed gate for these lives in plan step 11.
      const equip = v.EquipmentID != null ? String(v.EquipmentID) : '';
      const name = String(v.Name || v.VehicleID || equip || '?');
      const id = String(v.VehicleID || equip || `${lat.toFixed(5)},${lng.toFixed(5)}`);
      seen.add(id);

      // --- derive speed + heading from movement since the last poll ---
      const prev = kin.get(id);
      let speedMph = prev ? prev.speedMph || 0 : 0;
      let heading = prev ? prev.heading : Number(v.Heading) || 0;
      if (prev) {
        const dt = (now - prev.t) / 1000;
        const d = haversineM(prev.lat, prev.lng, lat, lng);
        const moving = d > MOVE_MIN_M && dt > 0.5;
        const inst = moving ? (d / dt) * 2.2369363 : 0; // m/s -> mph
        speedMph = (prev.speedMph || 0) + (inst - (prev.speedMph || 0)) * SPEED_EMA;
        if (speedMph < 1) speedMph = 0;
        if (moving) heading = bearingDeg(prev.lat, prev.lng, lat, lng);
      }
      kin.set(id, { lat, lng, t: now, speedMph, heading });

      out.push({
        id,
        lat,
        lng,
        heading,
        routeId: inService ? rid : '',
        inService,
        color: inService ? catRouteColor(rid) : OUT_OF_SERVICE_COLOR,
        label: equip || name,
        routeName: inService ? v.RouteName || catRouteName(rid) || `Route ${rid}` : 'Not in service',
        routeAbbr: inService ? v.RouteAbbreviation || catRouteAbbr(rid) : '',
        speed: Math.round(speedMph),
        speedEstimated: true,
      });
    }
    for (const id of [...kin.keys()]) if (!seen.has(id)) kin.delete(id);
    vehicles = out;
    bus.emit('vehicles', vehicles);
  } catch (err) {
    console.warn('[livemap] CAT vehicle poll failed', err);
  }
}

// --- alerts ---------------------------------------------------------------

async function loadAlerts() {
  try {
    const r = await fetch(ALERTS_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`CAT alerts HTTP ${r.status}`);
    const data = await r.json();
    const list = Array.isArray(data?.alerts) ? data.alerts : [];
    alerts = list
      .map((a) => ({
        id: String(a.ID ?? a.Id ?? a.Title ?? a.Message ?? ''),
        title: String(a.Title || a.Name || '').trim(),
        message: String(a.Message || a.Description || '').trim(),
        routes: a.Routes || a.RouteNames || '',
      }))
      .filter((a) => a.title || a.message);
    bus.emit('alerts', alerts);
  } catch (err) {
    console.warn('[livemap] CAT alerts poll failed', err);
  }
}

// --- stop ETAs (on demand) -------------------------------------------------

/** Fetch upcoming arrivals for one CAT stop id, flattened + normalised. */
export async function fetchCatStopEtas(stopId) {
  if (!stopId) return [];
  try {
    const r = await fetch(`${STOP_ETAS_URL}?stop_id=${encodeURIComponent(stopId)}`, {
      cache: 'no-store',
    });
    if (!r.ok) return [];
    const data = await r.json();
    const groups = Array.isArray(data?.etas) ? data.etas : [];
    const out = [];
    for (const g of groups) {
      const list = Array.isArray(g.enRoute)
        ? g.enRoute
        : Array.isArray(g.ETAs)
          ? g.ETAs
          : Array.isArray(g.etas)
            ? g.etas
            : [];
      for (const e of list) {
        const rid = String(e.RouteID ?? e.routeID ?? e.route ?? '');
        const mins = Number(e.Minutes ?? e.minutes);
        out.push({
          routeId: rid,
          routeName: e.RouteName || catRouteName(rid) || `Route ${rid}`,
          color: catRouteColor(rid),
          minutes: Number.isFinite(mins) ? mins : null,
          timeText: String(e.Time || e.Text || e.text || '').trim(),
          direction: String(e.Direction || '').trim(),
        });
      }
    }
    out.sort((a, b) => {
      const am = a.minutes == null ? 1e9 : a.minutes;
      const bm = b.minutes == null ? 1e9 : b.minutes;
      return am - bm;
    });
    return out;
  } catch {
    return [];
  }
}

// --- helpers -------------------------------------------------------------

function normHex(c) {
  if (typeof c !== 'string') return null;
  const s = c.trim();
  if (/^#[0-9a-f]{6}$/i.test(s)) return s.toLowerCase();
  if (/^[0-9a-f]{6}$/i.test(s)) return `#${s.toLowerCase()}`;
  return null;
}

const R_EARTH_M = 6371000;
const toRad = (d) => (d * Math.PI) / 180;

/** Great-circle distance in metres. */
function haversineM(lat1, lon1, lat2, lon2) {
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R_EARTH_M * Math.asin(Math.min(1, Math.sqrt(a)));
}

/** Initial bearing from (lat1,lon1) to (lat2,lon2), degrees clockwise from north. */
function bearingDeg(lat1, lon1, lat2, lon2) {
  const φ1 = toRad(lat1);
  const φ2 = toRad(lat2);
  const dλ = toRad(lon2 - lon1);
  const y = Math.sin(dλ) * Math.cos(φ2);
  const x = Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(dλ);
  return (Math.atan2(y, x) * 180) / Math.PI;
}

// --- mock -----------------------------------------------------------------
// A couple of downtown routes + a small fleet so `?mock=1` shows the CAT
// overlay once it's toggled on (no CAT token in local dev).

function seedMock() {
  const CENTER = [-78.4783, 38.0293];
  const LAT_SQUEEZE = 0.79;
  const ROUTES = [
    { id: '11', name: '11 Downtown Loop (mock)', abbr: '11', color: '#2b328a' },
    { id: '13', name: '13 UVA / Downtown (mock)', abbr: '13', color: '#ed2124' },
  ];
  for (const r of ROUTES) {
    routeColor.set(r.id, r.color);
    routeName.set(r.id, r.name);
    routeAbbr.set(r.id, r.abbr);
  }
  catRoutes = ROUTES.map((r) => ({ id: r.id, name: r.name, abbr: r.abbr, color: r.color }));
  pruneHidden();
  bus.emit('routeVis', catRouteGroups());

  routeShapes = ROUTES.map((r, i) => {
    const radius = 0.006 + i * 0.0022;
    const coords = [];
    for (let a = 0; a <= 64; a++) {
      const t = (a / 64) * Math.PI * 2;
      coords.push([
        CENTER[0] + Math.cos(t) * radius,
        CENTER[1] + Math.sin(t) * radius * LAT_SQUEEZE,
      ]);
    }
    return { id: `p${r.id}`, routeId: r.id, name: r.name, color: r.color, coords };
  });

  stops = [];
  ROUTES.forEach((r, i) => {
    const radius = 0.006 + i * 0.0022;
    for (let k = 0; k < 8; k++) {
      const t = (k / 8) * Math.PI * 2;
      const lat = CENTER[1] + Math.sin(t) * radius * LAT_SQUEEZE;
      const lng = CENTER[0] + Math.cos(t) * radius;
      const stopId = `9${r.id}${k}`;
      stops.push({
        key: `${lat.toFixed(5)},${lng.toFixed(5)}`,
        lat,
        lng,
        name: `${r.abbr} · Stop ${k + 1}`,
        stopId,
        routeIds: [r.id],
        members: [{ routeStopId: stopId, routeId: r.id, stopId }],
      });
    }
  });

  const fleet = ROUTES.flatMap((r, i) =>
    [0, 1].map((n) => ({
      route: r,
      radius: 0.006 + i * 0.0022,
      angle: (n / 2) * Math.PI * 2 + i,
      angVel: (0.16 + 0.05 * n) * (n % 2 ? -1 : 1),
      label: `${10 + i * 4 + n}`,
    })),
  );

  const emit = () => {
    const now = performance.now() / 1000;
    vehicles = fleet.map((b) => {
      const a = b.angle + b.angVel * now;
      const lat = CENTER[1] + Math.sin(a) * b.radius * LAT_SQUEEZE;
      const lng = CENTER[0] + Math.cos(a) * b.radius;
      // heading = tangent to the loop, in the direction of travel
      const dir = Math.sign(b.angVel) || 1;
      const dEast = -Math.sin(a) * dir;
      const dNorth = Math.cos(a) * LAT_SQUEEZE * dir;
      const heading = ((Math.atan2(dEast, dNorth) * 180) / Math.PI + 360) % 360;
      return {
        id: `catmock-${b.label}`,
        lat,
        lng,
        heading,
        routeId: b.route.id,
        inService: true,
        color: b.route.color,
        label: b.label,
        routeName: b.route.name,
        routeAbbr: b.route.abbr,
        speed: 11 + Math.round(4 * Math.abs(Math.sin(now + b.angle))),
        speedEstimated: true,
      };
    });
    bus.emit('vehicles', vehicles);
  };

  bus.emit('routes', routeShapes);
  bus.emit('stops', stops);
  emit();
  setInterval(emit, 1200);
}
