// livemap/core/data/safety.js
// -----------------------------------------------------------------------------
// "Traffic & Incidents" data: PulsePoint emergency incidents + TomTom traffic
// incidents (both public, polled) and the TomTom traffic-flow raster (a tile
// source, no polling — the layer module just toggles it).
//
// PulsePoint is special: an incident with responders committed that sits near a
// transit route (or, while the FlexRide overlay is on, anywhere in the FlexRide
// service area) shows AUTOMATICALLY — no toggle. The "Emergency incidents"
// checkbox is an override that additionally surfaces every active incident,
// anywhere. TomTom incidents + the flow raster stay plain opt-in toggles.
//
//   onPulsePoint(fn)   -> fn(incident[])   { id, lat, lng, kind, type, address, units[], age }
//   onTrafficInc(fn)   -> fn(feature[])    GeoJSON LineString Features (+ normalised props)
//   is/​set/onSafety(key) for keys: 'pulsepoint' | 'trafficInc' | 'trafficFlow'
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter, lsGet, lsSet } from '../util.js';
import { onRoutes, getRoutes } from './transloc.js';
import { onCatRoutes, getCatRoutes } from './cat.js';
import { getMicroZone, onMicroZone } from './microtransit.js';
import { isDispatcher, onDispatcher } from './session.js';

const PULSEPOINT_URL = `${API_BASE}/v1/testmap/pulsepoint`;
const TRAFFIC_INC_URL = `${API_BASE}/api/traffic/incidents`;
const POLL_MS = 60_000;
// "Near a route" for the auto-show (matches testmap's
// INCIDENT_ROUTE_PROXIMITY_THRESHOLD_METERS) — it's a transit map, not a
// scanner feed.
const NEAR_ROUTE_M = 180;

const KEYS = ['pulsepoint', 'trafficInc', 'trafficFlow'];
const lsKey = (k) => `livemap.safety.${k}`;

const bus = emitter();
export const onPulsePoint = (fn) => bus.on('pulsepoint', fn);
export const onTrafficInc = (fn) => bus.on('trafficInc', fn);
export const onSafety = (key, fn) => {
  try { fn(enabled[key]); } catch (e) { console.error('[livemap] safety listener threw', e); }
  return bus.on(`toggle:${key}`, fn);
};

const enabled = {};
for (const k of KEYS) enabled[k] = lsGet(lsKey(k), '0') === '1';

let started = false;
let timer = 0;
let pulsePoint = [];
let trafficInc = [];

// Flat list of route polylines for the near-route filter; refreshed on change.
let routePaths = []; // [[lng,lat], ...][]
function refreshRoutePaths() {
  const paths = [];
  for (const r of getRoutes()) if (Array.isArray(r.coords) && r.coords.length > 1) paths.push(r.coords);
  for (const r of getCatRoutes()) if (Array.isArray(r.coords) && r.coords.length > 1) paths.push(r.coords);
  routePaths = paths;
}

// --- geometry helpers -----------------------------------------------------

/** Ray-cast point-in-polygon over a GeoJSON Polygon / MultiPolygon geometry. */
function pointInPolygon(lng, lat, geom) {
  if (!geom) return false;
  const polys =
    geom.type === 'Polygon' ? [geom.coordinates]
    : geom.type === 'MultiPolygon' ? geom.coordinates
    : [];
  for (const rings of polys) {
    if (!Array.isArray(rings) || !rings.length) continue;
    let inside = false;
    // outer ring only for a quick hit; holes are rare for a service area
    const ring = rings[0];
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const xi = ring[i][0];
      const yi = ring[i][1];
      const xj = ring[j][0];
      const yj = ring[j][1];
      if ((yi > lat) !== (yj > lat) && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) {
        inside = !inside;
      }
    }
    if (inside) return true;
  }
  return false;
}

/** In the FlexRide service area? (getMicroZone() is null unless FlexRide is on.) */
function inFlexZone(lat, lng) {
  const z = getMicroZone();
  return !!z && pointInPolygon(lng, lat, z);
}

// --- point ↔ route distance (local equirectangular approx, fine at ~200 m) ---
function nearAnyRoute(lat, lng) {
  if (!routePaths.length) return true; // no routes loaded yet -> don't hide everything
  const mPerLat = 111_320;
  const mPerLng = 111_320 * Math.cos((lat * Math.PI) / 180);
  const px = lng * mPerLng;
  const py = lat * mPerLat;
  const limit2 = NEAR_ROUTE_M * NEAR_ROUTE_M;
  for (const path of routePaths) {
    for (let i = 1; i < path.length; i++) {
      const ax = path[i - 1][0] * mPerLng;
      const ay = path[i - 1][1] * mPerLat;
      const bx = path[i][0] * mPerLng;
      const by = path[i][1] * mPerLat;
      const dx = bx - ax;
      const dy = by - ay;
      const len2 = dx * dx + dy * dy;
      let t = len2 ? ((px - ax) * dx + (py - ay) * dy) / len2 : 0;
      t = t < 0 ? 0 : t > 1 ? 1 : t;
      const ex = px - (ax + t * dx);
      const ey = py - (ay + t * dy);
      if (ex * ex + ey * ey <= limit2) return true;
    }
  }
  return false;
}

export const isSafetyOn = (key) => !!enabled[key];

/**
 * PulsePoint incidents to render right now. Dispatcher-only.
 *   - toggle ON  -> every active incident, anywhere (the override)
 *   - toggle OFF -> only those near a route, or (FlexRide on) in the FlexRide
 *                   service area — the automatic transit-relevant set.
 * `pulsePoint` is PulsePoint's `active` list (recent/cleared excluded).
 */
export const getPulsePoint = () => {
  if (!isDispatcher()) return [];
  if (enabled.pulsepoint) return pulsePoint;
  return pulsePoint.filter((x) => x.nearRoute || inFlexZone(x.lat, x.lng));
};
export const getTrafficInc = () => (enabled.trafficInc ? trafficInc : []);

export function setSafety(key, on) {
  on = !!on;
  if (!KEYS.includes(key) || on === enabled[key]) return;
  enabled[key] = on;
  lsSet(lsKey(key), on ? '1' : '0');
  bus.emit(`toggle:${key}`, on);
  // Re-emit current data so a layer that just turned on paints immediately.
  if (key === 'pulsepoint') bus.emit('pulsepoint', getPulsePoint());
  if (key === 'trafficInc') bus.emit('trafficInc', getTrafficInc());
  if (on && (key === 'pulsepoint' || key === 'trafficInc')) poll();
  maybeStopTimer();
}

export function startSafetyFeed() {
  if (started) return;
  started = true;
  refreshRoutePaths();
  onRoutes(() => {
    refreshRoutePaths();
    if (wantPulsePoll()) pollPulsePoint(); // re-filter with the new geometry
  });
  onCatRoutes(() => refreshRoutePaths());
  // FlexRide zone appeared/changed -> a different set may now auto-show.
  onMicroZone(() => bus.emit('pulsepoint', getPulsePoint()));
  // Became / stopped being a dispatcher -> PulsePoint (dispatcher-only) starts
  // or stops polling and showing.
  onDispatcher(() => {
    bus.emit('pulsepoint', getPulsePoint());
    maybeStopTimer();
    if (wantPulsePoll()) pollPulsePoint();
  });
  maybeStopTimer();
}

/** PulsePoint always wants to poll for an authed dispatcher — the near-route /
 *  service-area set auto-shows with no toggle. */
function wantPulsePoll() {
  return isDispatcher();
}

function maybeStopTimer() {
  const wantPoll = wantPulsePoll() || enabled.trafficInc;
  if (wantPoll && !timer) {
    poll();
    timer = setInterval(poll, POLL_MS);
  } else if (!wantPoll && timer) {
    clearInterval(timer);
    timer = 0;
  }
}

// --- PulsePoint -----------------------------------------------------------

// PulsePoint call-type prefixes -> a coarse category for colouring.
const PP_KIND = [
  [/^(TC|TCE|TCS|VEH|HR)/i, 'traffic'],
  [/^(F|SF|WFA|CMA|OA|GAS|EE|ELF|EF|HMR|BRSH|VEG|WSF|OUT)/i, 'fire'],
  [/^(ME|MCI|RES|ST|SI|CPR|FALL|SEIZ|OD|CO)/i, 'medical'],
];
function ppKind(type) {
  const t = String(type || '');
  for (const [re, kind] of PP_KIND) if (re.test(t)) return kind;
  return 'other';
}

// The short type code that keys the PulsePoint "respond icon" PNGs
// (/v1/pulsepoint/respond_icons/{code}_map_active.png) — the same markers
// testmap and vandispatch use. Ported from testmap's inferPulsePointMarkerType:
// the CallType field is sometimes a bare code, sometimes prose, sometimes an
// id, so try a run of candidate fields and coerce each to a 1-6 char code.
function ppIconType(rec) {
  const candidates = [
    rec.PulsePointIncidentCallTypePrimaryCode,
    rec.PulsePointIncidentCallTypeCode,
    rec.PulsePointIncidentCallTypeID,
    rec.PulsePointIncidentTypeCode,
    rec.PulsePointIncidentType,
    rec.CallTypeCode,
    rec.TypeCode,
    rec.CallType,
    rec.Type,
    rec.IncidentType,
    rec.PulsePointIncidentCallType,
  ];
  for (const value of candidates) {
    if (value == null) continue;
    const trimmed = String(value).trim();
    if (!trimmed) continue;
    if (/^[A-Za-z0-9]{1,6}$/.test(trimmed)) return trimmed.toLowerCase();
    const firstToken = trimmed.split(/[\s/-]+/)[0];
    if (firstToken && /^[A-Za-z0-9]{1,4}$/.test(firstToken)) return firstToken.toLowerCase();
    const words = trimmed.match(/[A-Za-z0-9]+/g);
    if (words && words.length >= 2) {
      const acronym = words.map((w) => w[0]).join('');
      if (/^[A-Za-z0-9]{1,4}$/.test(acronym)) return acronym.toLowerCase();
    }
  }
  return '';
}

async function pollPulsePoint() {
  try {
    const r = await fetch(PULSEPOINT_URL, { cache: 'no-store' });
    if (!r.ok) return;
    const data = await r.json();
    // Active incidents only — recent/cleared calls live under `incidents.recent`
    // and aren't shown (matches testmap, which renders the `active` list as-is
    // with no unit-status gate). The near-route / service-area filtering for the
    // auto-show happens at emit time in getPulsePoint().
    const active = (data && data.incidents && data.incidents.active) || [];
    const now = Date.now();
    const out = [];
    for (const inc of active) {
      const lat = Number(inc.Latitude);
      const lng = Number(inc.Longitude);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
      const type = inc.PulsePointIncidentCallType || inc.CallType || '';
      const t = Date.parse(inc.CallReceivedDateTime || '');
      const units = Array.isArray(inc.Unit)
        ? inc.Unit.map((u) => ({
            id: (u.UnitID || '').toString(),
            status: (u.PulsePointDispatchStatus || '').toString(),
            cleared: !!u.UnitClearedDateTime,
          })).filter((u) => u.id)
        : [];
      out.push({
        id: String(inc.ID || `${lat},${lng}`),
        lat,
        lng,
        nearRoute: nearAnyRoute(lat, lng),
        kind: ppKind(type),
        iconType: ppIconType(inc),
        type,
        address: (inc.FullDisplayAddress || inc.MedicalEmergencyDisplayAddress || '').toString(),
        units,
        ageMin: Number.isNaN(t) ? null : Math.max(0, Math.round((now - t) / 60_000)),
      });
    }
    pulsePoint = out;
    bus.emit('pulsepoint', getPulsePoint());
  } catch (err) {
    console.warn('[livemap] pulsepoint poll failed', err);
  }
}

// --- TomTom traffic incidents -------------------------------------------

// iconCategory -> label; magnitudeOfDelay -> colour bucket.
const TT_CAT = {
  1: 'Accident', 2: 'Fog', 3: 'Dangerous conditions', 4: 'Rain', 5: 'Ice',
  6: 'Traffic jam', 7: 'Lane closed', 8: 'Road closed', 9: 'Road works',
  10: 'Wind', 11: 'Flooding', 14: 'Broken-down vehicle',
};
function ttColor(mag) {
  const m = Number(mag);
  return m >= 4 ? '#b91c1c' : m === 3 ? '#dc2626' : m === 2 ? '#ea580c' : m === 1 ? '#f59e0b' : '#6b7280';
}

async function pollTrafficInc() {
  try {
    const r = await fetch(TRAFFIC_INC_URL, { cache: 'no-store' });
    if (!r.ok) return;
    const data = await r.json();
    const feats = (data && data.incidents) || [];
    const out = [];
    for (const f of feats) {
      if (!f || !f.geometry || f.geometry.type !== 'LineString') continue;
      const p = f.properties || {};
      const events = Array.isArray(p.events) ? p.events.map((e) => e.description).filter(Boolean) : [];
      out.push({
        type: 'Feature',
        geometry: f.geometry,
        properties: {
          cat: TT_CAT[p.iconCategory] || 'Incident',
          color: ttColor(p.magnitudeOfDelay),
          from: (p.from || '').toString(),
          to: (p.to || '').toString(),
          desc: events.join(' · '),
          delay: p.delay == null ? '' : `${Math.round(Number(p.delay))}s delay`,
        },
      });
    }
    trafficInc = out;
    bus.emit('trafficInc', getTrafficInc());
  } catch (err) {
    console.warn('[livemap] traffic incidents poll failed', err);
  }
}

function poll() {
  if (wantPulsePoll()) pollPulsePoint();
  if (enabled.trafficInc) pollTrafficInc();
}
