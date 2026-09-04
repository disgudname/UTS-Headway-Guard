// livemap/core/data/microtransit.js
// -----------------------------------------------------------------------------
// On-Demand (UTS microtransit) + Spare (paratransit) live vehicles. Both feeds
// are dispatcher-auth-only: a public visitor gets 401 and this module simply
// reports itself unavailable, so no toggle and no markers ever appear for them.
// A signed-in dispatcher gets `available` -> the left panel shows an opt-in
// "On-Demand & Paratransit" toggle, and enabling it plots the vehicles through
// the shared vehicle layer (core/layers/vehicles.js) with the same markers as
// fixed-route buses.
//
//   onMicroAvailable(fn) -> fn(bool)  replays; true once an authed fetch works
//   onMicroEnabled(fn)   -> fn(bool)  replays; the panel toggle state
//   onMicroVehicles(fn)  -> fn(vehicle[])   merged OnDemand + Spare, or [] when off
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter } from '../util.js';

const OD_URL = `${API_BASE}/api/ondemand`;
const SPARE_URL = `${API_BASE}/api/spare/vehicles`;
const SPARE_REQ_URL = `${API_BASE}/api/spare/requests`;
const SPARE_ZONE_URL = `${API_BASE}/api/spare/service-area`;
const SPARE_SSE_URL = `${API_BASE}/stream/spare`;
const POLL_MS = 10_000;
const ZONE_POLL_MS = 10 * 60_000; // the coverage polygon barely changes
const STALE_MAX_MS = 3 * 60_000; // a fix older than this = not live
const DEFAULT_COLOR = '#7c3aed';
const HEX = /^#[0-9a-f]{6}$/i;

// UVA Transportation branding: On-Demand microtransit = "UVA Ride";
// the Spare-run paratransit (incl. FlexRide via Lyft) = "UVA FlexRide".
// These toggles are deliberately session-only (NOT persisted) — a dispatcher
// opts in per visit; they must never come back on by themselves on a reload.

// Spare RequestStatus values we still want to show a pickup/dropoff for.
const LIVE_REQ_STATUSES = new Set(['processing', 'accepted', 'arriving', 'inProgress']);

const bus = emitter();
export const onMicroVehicles = (fn) => bus.on('vehicles', fn);
export const onMicroTrips = (fn) => bus.on('trips', fn); // FlexRide requests + Ride stops
export const onMicroZone = (fn) => {
  try { fn(zone); } catch (e) { console.error('[livemap] micro-zone listener threw', e); }
  return bus.on('zone', fn);
};
export const onMicroAvailable = (fn) => {
  try { fn(available); } catch (e) { console.error('[livemap] micro-available listener threw', e); }
  return bus.on('available', fn);
};
/** fn({ ride, flex }) — replays current state, then on any change. */
export const onMicroEnabled = (fn) => {
  try { fn({ ...enabled }); } catch (e) { console.error('[livemap] micro-enabled listener threw', e); }
  return bus.on('enabled', fn);
};

let available = false;
const enabled = { ride: false, flex: false }; // session-only, always start off
let started = false;
let timer = 0;
let zoneTimer = 0;
let vehicles = []; // all fetched (both sources), unfiltered
let trips = [];
let zone = null; // GeoJSON geometry (Polygon/MultiPolygon) or null

// Live Spare positions from the /stream/spare SSE (webhook-pushed). Keyed by the
// Spare vehicle id; between the 10s roster polls these keep FlexRide vans moving.
let sse = null; // EventSource | null
const livePos = new Map(); // vehicleId -> { lng, lat, heading, ts (ms) }
let reqPollTimer = 0;

export const isMicroAvailable = () => available;
export const isRideOn = () => enabled.ride;
export const isFlexOn = () => enabled.flex;
export const isMicroEnabled = () => enabled.ride || enabled.flex;

function forSources(list) {
  return (list || []).filter(
    (x) => (x.source === 'spare' && enabled.flex) || (x.source !== 'spare' && enabled.ride),
  );
}
export const getMicroVehicles = () => forSources(vehicles);
export const getMicroTrips = () => forSources(trips);
export const getMicroZone = () => (enabled.flex ? zone : null);

export function setMicroEnabled(key, on) {
  on = !!on;
  if ((key !== 'ride' && key !== 'flex') || on === enabled[key]) return;
  enabled[key] = on; // not persisted — see note by the branding constants
  bus.emit('enabled', { ...enabled });
  bus.emit('vehicles', getMicroVehicles());
  bus.emit('trips', getMicroTrips());
  bus.emit('zone', getMicroZone());
  if (on) {
    poll(); // refresh now rather than waiting for the timer
    if (key === 'flex') loadZone();
  }
  syncSpareSse();
}

export function startMicrotransitFeed() {
  if (started) return;
  started = true;
  poll();
  loadZone();
  timer = setInterval(poll, POLL_MS);
  zoneTimer = setInterval(loadZone, ZONE_POLL_MS);
}

// --- live Spare positions over SSE ------------------------------------------

/** Open the SSE only while it can do something: authed (`available`) + FlexRide
 *  toggled on. Closing it also drops the stale live-position cache. */
function syncSpareSse() {
  if (available && enabled.flex) openSpareSse();
  else closeSpareSse();
}

/** A `vehicleLocation` webhook payload -> our normalized live-position record. */
function parseSpareLoc(d) {
  if (!d || typeof d !== 'object') return null;
  const vid = d.vehicleId ?? d.vehicleID ?? d.id;
  const c = d.location && Array.isArray(d.location.coordinates) ? d.location.coordinates : null;
  if (vid == null || !c) return null;
  const lng = Number(c[0]);
  const lat = Number(c[1]);
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return null;
  const tsRaw = Number(d.latestLocationUpdatedTs ?? d.locationUpdatedTs);
  const ts = Number.isFinite(tsRaw) ? (tsRaw < 1e12 ? tsRaw * 1000 : tsRaw) : Date.now();
  const hd = Number(d.bearing);
  return { vid: String(vid), lng, lat, heading: Number.isFinite(hd) && hd >= 0 ? hd : 0, ts };
}

function openSpareSse() {
  if (sse || typeof EventSource === 'undefined') return;
  try {
    sse = new EventSource(SPARE_SSE_URL, { withCredentials: true });
  } catch {
    sse = null;
    return;
  }
  sse.onmessage = (e) => {
    let msg;
    try {
      msg = JSON.parse(e.data);
    } catch {
      return;
    }
    if (msg.type === 'initial') {
      for (const loc of msg.data?.vehicleLocations || []) {
        const p = parseSpareLoc(loc);
        if (p) livePos.set(p.vid, p);
      }
      applyLivePositions();
    } else if (msg.type === 'vehicleLocation') {
      const p = parseSpareLoc(msg.data);
      if (p) {
        livePos.set(p.vid, p);
        applyLivePositions();
      }
    } else if (msg.type === 'requestStatus' || msg.type === 'eta') {
      // Trips (pickup/drop-off points) come from the 10s /api/spare/requests
      // poll — nudge one sooner when a request changes, debounced.
      if (!reqPollTimer) {
        reqPollTimer = setTimeout(() => {
          reqPollTimer = 0;
          poll();
        }, 1500);
      }
    }
  };
  sse.onerror = () => {
    /* EventSource auto-reconnects; a public 401 just backs off and retries. */
  };
}

function closeSpareSse() {
  if (sse) {
    try {
      sse.close();
    } catch {
      /* ignore */
    }
    sse = null;
  }
  livePos.clear();
}

/** Patch the current Spare vehicles with any fresher SSE position and re-emit. */
function applyLivePositions() {
  if (!enabled.flex || !vehicles.length) return;
  const now = Date.now();
  let changed = false;
  for (const v of vehicles) {
    if (v.source !== 'spare') continue;
    const p = livePos.get(String(v.id).replace(/^sp:/, ''));
    if (!p || now - p.ts > STALE_MAX_MS) continue;
    if (v.lat !== p.lat || v.lng !== p.lng || v.heading !== p.heading) {
      v.lat = p.lat;
      v.lng = p.lng;
      v.heading = p.heading;
      changed = true;
    }
  }
  if (changed) bus.emit('vehicles', getMicroVehicles());
}

function hex(c) {
  return typeof c === 'string' && HEX.test(c.trim()) ? c.trim().toLowerCase() : null;
}

/** "BUS 241: White Karsan eJest" -> "BUS 241" (matches testmap). */
function shortName(callName) {
  if (typeof callName !== 'string') return '';
  const t = callName.trim();
  const i = t.indexOf(':');
  return i > 0 ? t.slice(0, i).trim() : t;
}

/** "BUS 241: White Karsan eJest" -> "White Karsan eJest" (the vehicle blurb). */
function callDescr(callName) {
  if (typeof callName !== 'string') return '';
  const i = callName.indexOf(':');
  return i > 0 ? callName.slice(i + 1).trim() : '';
}

async function poll() {
  let anyOk = false;
  let any401 = false;
  const out = [];
  const tripOut = [];

  try {
    const r = await fetch(OD_URL, { credentials: 'include', cache: 'no-store' });
    if (r.status === 401) any401 = true;
    else if (r.ok) {
      anyOk = true;
      const data = await r.json();
      for (const s of data.ondemandStops || []) {
        const t = onDemandStopToTrip(s);
        if (t) tripOut.push(t);
      }
      for (const v of data.vehicles || []) {
        const lat = Number(v.lat);
        const lng = Number(v.lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
        // Drop stale ghosts — the feed keeps units in the roster for days after
        // their last fix (e.g. a van "that hasn't existed in a long time"). Show
        // only vehicles the backend still considers live, and belt-and-braces
        // against an old lastUpdate.
        const updMs = Date.parse(v.lastUpdate || '');
        const ageMs = Number.isNaN(updMs) ? Infinity : Date.now() - updMs;
        if (v.stale || ageMs > STALE_MAX_MS) continue;
        const hd = Number(v.heading);
        const sp = Number(v.speed);
        out.push({
          source: 'ondemand',
          id: `od:${v.vehicleId ?? v.deviceId ?? v.deviceUuid ?? `${lat},${lng}`}`,
          lat,
          lng,
          heading: Number.isFinite(hd) && hd >= 0 ? hd : 0,
          speedMph: Number.isFinite(sp) && sp > 0 ? sp : 0,
          color: hex(v.markerColor) || DEFAULT_COLOR,
          label: shortName(v.callName) || `Vehicle ${v.vehicleId ?? ''}`.trim(),
          driver: typeof v.driverName === 'string' ? v.driverName.trim() : '',
          descr: callDescr(v.callName), // "White Karsan eJest"
          stale: !!v.stale,
        });
      }
    }
  } catch {
    /* network hiccup — leave anyOk/any401 as-is */
  }

  try {
    const r = await fetch(SPARE_URL, { credentials: 'include', cache: 'no-store' });
    if (r.status === 401) any401 = true;
    else if (r.ok) {
      anyOk = true;
      const arr = await r.json();
      for (const v of Array.isArray(arr) ? arr : []) {
        // Position: prefer a fresher SSE fix (see /stream/spare) over the
        // roster's `currentLocation`. Spare merges the webhook-pushed position
        // in as `currentLocation` ({ location:{coordinates:[lng,lat]}, bearing,
        // latestLocationUpdatedTs } — NOT `v.location`; missing this is why the
        // vans never showed on livemap).
        const live = livePos.get(String(v.id ?? v.identifier ?? ''));
        let lng;
        let lat;
        let hd;
        let fixMs;
        if (live) {
          ({ lng, lat, heading: hd } = live);
          fixMs = live.ts;
        } else {
          const cl = v.currentLocation || {};
          const coords =
            cl.location && Array.isArray(cl.location.coordinates) ? cl.location.coordinates : null;
          lng = coords ? Number(coords[0]) : NaN;
          lat = coords ? Number(coords[1]) : NaN;
          hd = Number(cl.bearing ?? v.bearing);
          const fixTs = Number(cl.latestLocationUpdatedTs || cl.locationUpdatedTs);
          fixMs = Number.isFinite(fixTs) ? fixTs * 1000 : NaN;
        }
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue; // no position pushed
        if (Number.isFinite(fixMs) && Date.now() - fixMs > STALE_MAX_MS) continue;
        const seats = Number(v.passengerSeats);
        const access = Array.isArray(v.accessibilityFeatures)
          ? v.accessibilityFeatures.filter(Boolean)
          : [];
        out.push({
          source: 'spare',
          id: `sp:${v.id ?? v.identifier ?? `${lat},${lng}`}`,
          lat,
          lng,
          heading: Number.isFinite(hd) && hd >= 0 ? hd : 0,
          speedMph: 0,
          color: hex(v.markerColor) || DEFAULT_COLOR,
          label: v.identifier || v.licensePlate || 'Van',
          driver: '',
          descr: [v.color, v.make, v.model].filter(Boolean).join(' '),
          plate: (v.licensePlate || '').toString().trim(),
          seats: Number.isFinite(seats) && seats > 0 ? seats : 0,
          access,
          stale: false,
        });
      }
    }
  } catch {
    /* network hiccup */
  }

  // Spare ride requests -> pickup/dropoff trips
  try {
    const r = await fetch(SPARE_REQ_URL, { credentials: 'include', cache: 'no-store' });
    if (r.status === 401) any401 = true;
    else if (r.ok) {
      anyOk = true;
      const arr = await r.json();
      for (const q of Array.isArray(arr) ? arr : []) {
        const t = spareRequestToTrip(q);
        if (t) tripOut.push(t);
      }
    }
  } catch {
    /* network hiccup */
  }

  if (any401 && !anyOk) {
    if (available) {
      available = false;
      bus.emit('available', false);
    }
    vehicles = [];
    trips = [];
    bus.emit('vehicles', []);
    bus.emit('trips', []);
    syncSpareSse(); // drop the SSE now that we're not authed
    return;
  }
  if (anyOk && !available) {
    available = true;
    bus.emit('available', true);
  }
  vehicles = out;
  numberTrips(tripOut, out); // attach vanColor + per-van 1..N stop sequence
  trips = tripOut;
  bus.emit('vehicles', getMicroVehicles());
  bus.emit('trips', getMicroTrips());
  syncSpareSse(); // (re)open the SSE now that we know we're authed
}

function pt(loc) {
  const c = loc && Array.isArray(loc.coordinates) ? loc.coordinates : null;
  if (!c) return null;
  const lng = Number(c[0]);
  const lat = Number(c[1]);
  return Number.isFinite(lng) && Number.isFinite(lat) ? [lng, lat] : null;
}

/** A Spare request -> a pickup/dropoff trip, or null if not worth drawing. */
function spareRequestToTrip(q) {
  if (!q || !LIVE_REQ_STATUSES.has(q.status)) return null;
  const pickup = pt(q.scheduledPickupLocation) || pt(q.requestedPickupLocation);
  const dropoff = pt(q.scheduledDropoffLocation) || pt(q.requestedDropoffLocation);
  if (!pickup && !dropoff) return null;
  const r = q.rider || {};
  const rider = [r.firstName, r.lastName ? `${String(r.lastName).charAt(0)}.` : '']
    .filter(Boolean)
    .join(' ')
    .trim();
  return {
    source: 'spare',
    id: `sp:${q.id}`,
    requestId: q.id,
    spareVehicleId: q.vehicleId || '',
    status: q.status || '',
    pickup,
    dropoff,
    rider,
    pickupAddr: q.requestedPickupAddress || '',
    dropoffAddr: q.requestedDropoffAddress || '',
    pickupEta: msTs(q.pickupEta),
    dropoffEta: msTs(q.dropoffEta),
    pickupOrder: rawTs(q.scheduledPickupTs ?? q.pickupEta ?? q.requestedPickupTs),
    dropoffOrder: rawTs(q.scheduledDropoffTs ?? q.dropoffEta ?? q.requestedDropoffTs),
    pickupDone: !!(q.pickupArrivedTs || q.pickupCompletedTs),
    dropoffDone: !!(q.dropoffCompletedTs || q.dropoffArrivedTs),
    vehicleLabel: q.dutyIdentifier || '',
    numRiders: Number(q.numRiders) || 1,
    accessibility: Array.isArray(q.accessibilityFeatures) ? q.accessibilityFeatures : [],
  };
}

/** One OnDemand plan stop -> a single-ended trip (pickup OR dropoff). */
function onDemandStopToTrip(s) {
  const lat = Number(s.lat ?? s.latitude);
  const lng = Number(s.lng ?? s.lon ?? s.longitude);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  const isDrop = String(s.stopType || s.stop_type || '').toLowerCase() === 'dropoff';
  const at = [lng, lat];
  const riders = Array.isArray(s.riders)
    ? s.riders.map((n) => (typeof n === 'string' ? n.trim() : '')).filter(Boolean)
    : [];
  const order = rawTs(s.stopTimestamp ?? s.stop_timestamp ?? s.eta ?? s.time);
  const timeLabel = msTs(s.stopTimestamp ?? s.stop_timestamp ?? s.time);
  return {
    source: 'ondemand',
    id: `od:${s.rideId ?? s.stopTimestamp ?? `${lat},${lng}`}:${isDrop ? 'd' : 'p'}`,
    status: String(s.rideStatus || '').trim(),
    pickup: isDrop ? null : at,
    dropoff: isDrop ? at : null,
    rider: riders.join(', '),
    pickupAddr: isDrop ? '' : String(s.address || '').trim(),
    dropoffAddr: isDrop ? String(s.address || '').trim() : '',
    pickupEta: isDrop ? '' : timeLabel,
    dropoffEta: isDrop ? timeLabel : '',
    pickupOrder: isDrop ? Infinity : order,
    dropoffOrder: isDrop ? order : Infinity,
    pickupDone: false,
    dropoffDone: false,
    vehicleLabel: shortName(s.callName || s.call_name || ''),
    numRiders: riders.length || 1,
    accessibility: [],
  };
}

/** A unix-seconds / ms / ISO timestamp -> ms number for ordering, or Infinity. */
function rawTs(v) {
  if (v == null) return Infinity;
  if (typeof v === 'number') return v < 1e12 ? v * 1000 : v;
  const t = Date.parse(v);
  return Number.isNaN(t) ? Infinity : t;
}

/**
 * Give every trip its van's marker colour and its 1..N position in that van's
 * time-ordered stop list — so the map points come out numbered + van-coloured,
 * matching vandispatch and the manifest list in the van popup.
 */
function numberTrips(list, veh) {
  const spColor = new Map(); // spare vehicleId -> colour
  const odColor = new Map(); // ride van label -> colour
  for (const v of veh) {
    if (v.source === 'spare') spColor.set(String(v.id).replace(/^sp:/, ''), v.color);
    else odColor.set(v.label, v.color);
  }
  const byVan = new Map();
  for (const t of list) {
    if (t.source === 'spare') {
      t.vanKey = t.spareVehicleId ? `spare:${t.spareVehicleId}` : '';
      t.vanColor = spColor.get(t.spareVehicleId) || DEFAULT_COLOR;
    } else {
      t.vanKey = t.vehicleLabel ? `ride:${t.vehicleLabel}` : '';
      t.vanColor = odColor.get(t.vehicleLabel) || DEFAULT_COLOR;
    }
    t.pickupSeq = 0;
    t.dropoffSeq = 0;
    if (!t.vanKey) continue;
    if (!byVan.has(t.vanKey)) byVan.set(t.vanKey, []);
    const g = byVan.get(t.vanKey);
    if (t.pickup) g.push({ t, kind: 'p', order: t.pickupOrder });
    if (t.dropoff) g.push({ t, kind: 'd', order: t.dropoffOrder });
  }
  for (const g of byVan.values()) {
    g.sort((a, b) => a.order - b.order);
    g.forEach((e, i) => {
      if (e.kind === 'p') e.t.pickupSeq = i + 1;
      else e.t.dropoffSeq = i + 1;
    });
  }
}

/** A Spare epoch-seconds (or ms, or ISO) timestamp -> short ET time, or ''. */
function msTs(v) {
  if (v == null) return '';
  let ms = null;
  if (typeof v === 'number') ms = v < 1e12 ? v * 1000 : v;
  else if (typeof v === 'string') {
    const t = Date.parse(v);
    if (!Number.isNaN(t)) ms = t;
  }
  if (ms == null) return '';
  try {
    return new Date(ms).toLocaleTimeString('en-US', {
      timeZone: 'America/New_York',
      hour: 'numeric',
      minute: '2-digit',
    });
  } catch {
    return '';
  }
}

// --- van manifest (ordered stop list for a van's popup, vandispatch-style) ---

/**
 * The ordered list of upcoming stops for a van — what its popup shows instead
 * of drawing route lines.
 *   agency: 'spare' -> `ref` is the Spare vehicleId
 *   agency: 'ride'  -> `ref` is the van label ("VAN 241")
 * Returns [{ kind:'Pickup'|'Drop-off', rider, addr, time, done }], time-ordered,
 * completed stops dropped.
 */
export function vanManifest(agency, ref) {
  if (!ref) return [];
  const rows = [];
  for (const t of trips) {
    const mine =
      agency === 'spare'
        ? t.source === 'spare' && t.spareVehicleId === ref
        : t.source === 'ondemand' && t.vehicleLabel === ref;
    if (!mine) continue;
    if (t.pickup && !t.pickupDone) {
      rows.push({
        n: t.pickupSeq,
        kind: 'Pickup',
        rider: t.rider || '',
        addr: t.pickupAddr || '',
        time: t.pickupEta || '',
      });
    }
    if (t.dropoff && !t.dropoffDone) {
      rows.push({
        n: t.dropoffSeq,
        kind: 'Drop-off',
        rider: t.rider || '',
        addr: t.dropoffAddr || '',
        time: t.dropoffEta || '',
      });
    }
  }
  rows.sort((a, b) => a.n - b.n);
  return rows;
}

async function loadZone() {
  try {
    const r = await fetch(SPARE_ZONE_URL, { credentials: 'include', cache: 'no-store' });
    if (!r.ok) {
      // 204 (scope lost / shapely missing) or 401 -> clear the overlay quietly
      if ((r.status === 204 || r.status === 401) && zone) {
        zone = null;
        bus.emit('zone', null);
      }
      return;
    }
    const feat = await r.json();
    const geom = feat && feat.type === 'Feature' ? feat.geometry : feat;
    if (geom && (geom.type === 'Polygon' || geom.type === 'MultiPolygon')) {
      zone = geom;
      bus.emit('zone', getMicroZone());
    }
  } catch {
    /* keep the last zone on a network hiccup */
  }
}
