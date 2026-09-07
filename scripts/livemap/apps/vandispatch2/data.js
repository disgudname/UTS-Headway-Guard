// livemap/apps/vandispatch2/data.js
// -----------------------------------------------------------------------------
// The polling data layer behind the vandispatch2 Active Trips board + Duty
// Roster. It fetches the FULL Spare / OnDemand / WhenToWork objects (rider
// names, phones, notes, accessibility, intent, lead time, break state, …) that
// the panels render — livemap's shared core/data/microtransit.js keeps only the
// map-marker subset, so this is a deliberate parallel feed for the panels.
//
// Ported from html/vandispatch.html's fetchers; the map-marker upserts and the
// Leaflet route-drawing are gone (livemap's shared vehicle / micro-trips /
// safety layers own all of that now).
//
//   onChange(fn)  -> fires after any successful poll; fn gets no args, just
//                    call the getters. Also replays once on subscribe.
// -----------------------------------------------------------------------------

import { API_BASE } from '../../core/config.js';
import { emitter } from '../../core/util.js';
import { lngLatFromGeoJson, parseOdCallName } from './helpers.js';

const U = (p) => `${API_BASE}${p}`;

const bus = emitter();
/** fn() after any successful poll (and once immediately). */
export function onChange(fn) {
  try {
    fn();
  } catch (e) {
    console.error('[vandispatch2] data listener threw', e);
  }
  return bus.on('change', fn);
}
const emit = () => bus.emit('change');

// --- state ---------------------------------------------------------------
let vanColors = {}; // { "Van 16": "#hex", … }
const vehicleData = {}; // spare vehicleId -> full Spare vehicle object
let spareTrips = []; // /api/spare/requests
let spareDuties = []; // /api/spare/duties
let w2wShifts = []; // /v1/dispatch/block-drivers, flattened
let odStops = []; // OnDemand per-van plan stops (+ vehicleId/Name/Color stamped on)
let odVehicles = []; // live OnDemand vans: { vehicleId, driverName, label, color, rideCount }
let webhookConfigured = true; // Spare vehicleLocation webhook — drives #map-no-webhooks

export const getVanColors = () => vanColors;
export const getVehicleData = () => vehicleData;
export const getSpareTrips = () => spareTrips;
export const getSpareDuties = () => spareDuties;
export const getW2WShifts = () => w2wShifts;
export const getOdStops = () => odStops;
export const getOdVehicles = () => odVehicles;
export const isWebhookConfigured = () => webhookConfigured;

export function getVanColor(displayName, fallback) {
  return (displayName && vanColors[displayName]) || fallback;
}

// --- OnDemand active-status sets (ported) ------------------------------
export const OD_ACTIVE = new Set([
  'pending',
  'in_progress',
  'inProgress',
  'scheduled',
  'accepted',
]);

// --- fetchers ---------------------------------------------------------
async function loadVanColors() {
  try {
    const r = await fetch(U('/api/van-colors'), { cache: 'no-store' });
    if (r.ok) {
      vanColors = await r.json();
      emit();
    }
  } catch {
    /* soft-fail */
  }
}

async function loadVehicles() {
  try {
    const r = await fetch(U('/api/spare/vehicles'), { credentials: 'include', cache: 'no-store' });
    if (!r.ok) return;
    const arr = await r.json();
    for (const v of Array.isArray(arr) ? arr : []) {
      if (v && v.id) vehicleData[v.id] = v;
    }
    emit();
  } catch {
    /* soft-fail */
  }
}

async function loadTrips() {
  try {
    const r = await fetch(U('/api/spare/requests'), { credentials: 'include', cache: 'no-store' });
    if (!r.ok) return;
    const arr = await r.json();
    spareTrips = Array.isArray(arr) ? arr : [];
    emit();
  } catch {
    /* soft-fail */
  }
}

async function loadDuties() {
  try {
    const r = await fetch(U('/api/spare/duties'), { credentials: 'include', cache: 'no-store' });
    if (!r.ok) return;
    const arr = await r.json();
    spareDuties = Array.isArray(arr) ? arr : [];
    emit();
  } catch {
    /* soft-fail */
  }
}

async function loadW2WShifts() {
  try {
    const r = await fetch(U('/v1/dispatch/block-drivers'), { credentials: 'include', cache: 'no-store' });
    if (r.status === 401 || r.status === 403 || !r.ok) return;
    const data = await r.json();
    const byBlock = data.assignments_by_block || {};
    const shifts = [];
    for (const blockName of ['OnDemand Driver', 'OnDemand EB', 'FlexRide Driver']) {
      const block = byBlock[blockName];
      if (!block) continue;
      for (const period of Object.values(block)) {
        for (const s of period) shifts.push({ ...s, position_name: blockName });
      }
    }
    w2wShifts = shifts;
    emit();
  } catch {
    /* soft-fail */
  }
}

async function loadOnDemand() {
  let data;
  try {
    const r = await fetch(U('/api/ondemand'), { credentials: 'include', cache: 'no-store' });
    if (r.status === 401 || r.status === 403 || !r.ok) return;
    data = await r.json();
  } catch {
    return;
  }
  const vehicles = Array.isArray(data.vehicles) ? data.vehicles : [];
  const freshStops = [];
  const freshVehicles = [];
  for (const v of vehicles) {
    if (!v.vehicleId || v.lat == null || v.lng == null) continue;
    const lastActive = v.lastActiveAt || v.last_active_at;
    if (lastActive) {
      if ((Date.now() - new Date(lastActive).getTime()) / 86400000 > 3) continue;
    } else if (v.stale) {
      continue;
    }
    const odLabel = parseOdCallName(v.callName) || 'Van';
    const odColor = getVanColor(odLabel, v.markerColor || '#ec4899');
    const rideIds = new Set();
    for (const stop of v.stops || []) {
      if (stop.rideId) rideIds.add(stop.rideId);
      freshStops.push({ ...stop, vehicleId: v.vehicleId, vehicleName: odLabel, vehicleColor: odColor });
    }
    freshVehicles.push({
      vehicleId: v.vehicleId,
      driverName: v.driverName || '',
      label: odLabel,
      color: odColor,
      rideCount: rideIds.size,
      lat: Number(v.lat),
      lng: Number(v.lng),
    });
  }
  odStops = freshStops;
  odVehicles = freshVehicles;
  emit();
}

// --- Spare stop-order numbering (ported) -------------------------------
// Spare's /requests carries no explicit per-van stop sequence, so order is
// inferred by sorting each van's remaining pickups/dropoffs by scheduled time.
// Stops that land on the same spot within a short window are merged so the map
// doesn't stack two differently-numbered pins invisibly.
const STOP_COORD_PRECISION = 5; // ~1.1 m
const STOP_SAME_VISIT_WINDOW_S = 180; // 3 min

/** { vehicleId -> [{ ts, lngLat:[lng,lat], stops:[…] }] } time-ordered. */
export function computeVehicleStopGroups() {
  const byVehicle = {};
  for (const t of spareTrips) {
    if (!t.vehicleId) continue;
    const list = byVehicle[t.vehicleId] || (byVehicle[t.vehicleId] = []);
    const name = riderNameLocal(t);
    const phone = t.rider && t.rider.phoneNumber ? String(t.rider.phoneNumber).trim() : '';
    if (t.status !== 'inProgress') {
      const p = lngLatFromGeoJson(t.scheduledPickupLocation || t.requestedPickupLocation);
      if (p)
        list.push({
          ts: t.scheduledPickupTs || t.requestedPickupTs || 0,
          lngLat: p,
          requestId: t.id,
          kind: 'Pickup',
          name,
          phone,
          address: t.scheduledPickupAddress || t.requestedPickupAddress || '',
        });
    }
    const d = lngLatFromGeoJson(t.scheduledDropoffLocation || t.requestedDropoffLocation);
    if (d)
      list.push({
        ts: t.scheduledDropoffTs || 0,
        lngLat: d,
        requestId: t.id,
        kind: 'Dropoff',
        name,
        phone,
        address: t.scheduledDropoffAddress || t.requestedDropoffAddress || '',
      });
  }
  const result = {};
  for (const vehicleId of Object.keys(byVehicle)) {
    const stops = byVehicle[vehicleId].slice().sort((a, b) => a.ts - b.ts);
    const groups = [];
    for (const stop of stops) {
      const key = stop.lngLat.map((n) => n.toFixed(STOP_COORD_PRECISION)).join(',');
      const existing = groups.find(
        (g) => g.key === key && Math.abs(g.ts - stop.ts) <= STOP_SAME_VISIT_WINDOW_S,
      );
      if (existing) {
        existing.stops.push(stop);
        existing.ts = Math.min(existing.ts, stop.ts);
      } else {
        groups.push({ key, ts: stop.ts, lngLat: stop.lngLat, stops: [stop] });
      }
    }
    groups.sort((a, b) => a.ts - b.ts);
    result[vehicleId] = groups;
  }
  return result;
}

/** `${requestId}-${kind}` -> order number, matching the map's stop pins. */
export function computeStopOrderLookup() {
  const lookup = {};
  const byVehicle = computeVehicleStopGroups();
  for (const vehicleId of Object.keys(byVehicle)) {
    byVehicle[vehicleId].forEach((group, idx) => {
      const order = idx + 1;
      group.stops.forEach((stop) => {
        lookup[`${stop.requestId}-${stop.kind}`] = order;
      });
    });
  }
  return lookup;
}

// local copy to avoid a circular import with helpers for this one use
function riderNameLocal(req) {
  const r = req.rider;
  if (!r) return 'Rider';
  return [r.firstName, r.lastName].filter(Boolean).join(' ') || 'Rider';
}

// --- SSE: nudge a trips refresh when a request changes -----------------
let sse = null;
function openSse() {
  if (sse || typeof EventSource === 'undefined') return;
  try {
    sse = new EventSource(U('/stream/spare'), { withCredentials: true });
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
      if (msg.data && msg.data.webhookConfigured === false) {
        webhookConfigured = false;
        emit();
      }
    } else if (msg.type === 'vehicleLocation') {
      if (!webhookConfigured) {
        webhookConfigured = true;
        emit();
      }
    } else if (msg.type === 'requestStatus') {
      loadTrips();
    }
  };
  sse.onerror = () => {
    try {
      sse.close();
    } catch {
      /* ignore */
    }
    sse = null;
    setTimeout(openSse, 5000);
  };
}

// --- lifecycle ------------------------------------------------------
let started = false;
const timers = [];
export function startVandispatchData() {
  if (started) return;
  started = true;

  loadVanColors().then(() => {
    loadVehicles();
    loadOnDemand();
  });
  loadTrips();
  loadDuties();
  loadW2WShifts();
  openSse();

  timers.push(
    setInterval(loadTrips, 30_000),
    setInterval(loadDuties, 60_000),
    setInterval(loadW2WShifts, 60_000),
    setInterval(loadVehicles, 120_000),
    setInterval(loadVanColors, 60_000),
    setInterval(loadOnDemand, 10_000),
  );
}
