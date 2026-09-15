// livemap/core/data/bus-eta.js
// -----------------------------------------------------------------------------
// Our own live stop-ETA predictions for UTS routes (see bus_eta.py) -- a second
// opinion alongside TransLoc's own arrival times, computed from each vehicle's
// real-time position rather than whatever TransLoc's backend does. UTS only:
// CAT has no block-schedule/hop-time-model infrastructure to predict from.
//
// One shared poll (same 25s cadence as stops.js's existing TransLoc arrivals
// poll) feeds both consumers -- ui/stops.js (per-route rows in a stop's popup)
// and core/layers/vehicles.js (a vehicle's own upcoming stops in its popup) --
// rather than each polling independently.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';

const URL = `${API_BASE}/v1/eta/uts_stop_arrivals`;
const POLL_MS = 25_000;

let byRouteStop = new Map(); // "routeId|routeStopId" -> {routeId, routeStopId, stopDescription, times:[{vehicleId, seconds, source}]}
const listeners = new Set();
let started = false;

/** Call once at boot. Idempotent -- safe to call from multiple consumers. */
export function startBusEtaFeed() {
  if (started) return;
  started = true;
  poll();
  setInterval(poll, POLL_MS);
}

/** fn(Map) -- replays the current snapshot immediately, then on every poll. */
export function onBusEta(fn) {
  listeners.add(fn);
  fn(byRouteStop);
  return () => listeners.delete(fn);
}

/** Our ETA entries for one physical route-stop (soonest first), or []. */
export function getBusEtaForStop(routeId, routeStopId) {
  const entry = byRouteStop.get(`${routeId}|${routeStopId}`);
  return entry ? entry.times : [];
}

/** This vehicle's own upcoming stops on its current route, soonest first --
 *  for a vehicle marker's popup, which cares about "where is THIS bus headed
 *  next," not the whole route's arrivals. */
export function getBusEtaForVehicle(routeId, vehicleId) {
  const rid = String(routeId);
  const vid = String(vehicleId);
  const out = [];
  for (const entry of byRouteStop.values()) {
    if (entry.routeId !== rid) continue;
    for (const t of entry.times) {
      if (t.vehicleId === vid) {
        out.push({ stopId: entry.routeStopId, stopName: entry.stopDescription, seconds: t.seconds, source: t.source });
      }
    }
  }
  out.sort((a, b) => a.seconds - b.seconds);
  return out;
}

async function poll() {
  try {
    const r = await fetch(URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const next = new Map();
    for (const a of (Array.isArray(data.arrivals) ? data.arrivals : [])) {
      const routeId = String(a.RouteId ?? '');
      const routeStopId = String(a.RouteStopId ?? '');
      if (!routeId || !routeStopId) continue;
      const times = (Array.isArray(a.Times) ? a.Times : [])
        .map((t) => ({ vehicleId: String(t.VehicleId ?? ''), seconds: Number(t.Seconds), source: t.Source || '' }))
        .filter((t) => t.vehicleId && Number.isFinite(t.seconds));
      next.set(`${routeId}|${routeStopId}`, { routeId, routeStopId, stopDescription: a.StopDescription || '', times });
    }
    byRouteStop = next;
    for (const fn of listeners) fn(byRouteStop);
  } catch (err) {
    console.warn('[livemap] bus-eta poll failed', err);
  }
}

