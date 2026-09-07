// livemap/apps/vandispatch2/selection.js
// -----------------------------------------------------------------------------
// Van <-> card selection sync, ported from html/vandispatch.html.
//
// Clicking a van marker on the map highlights its Duty Roster card and every
// Active Trips card it is serving, expands that duty card's itinerary, and
// frames the map to the van + its remaining stops. Clicking the same van again,
// another van, or empty map clears it. Clicking a trip card frames just that
// ride's pickup / drop-off.
//
// Unlike the Leaflet page this draws NO route lines between stops — livemap
// deliberately doesn't (the user was emphatic about it), so this is the
// "fit-to-bounds" fallback only.
// -----------------------------------------------------------------------------

import { emitter } from '../../core/util.js';
import { VEHICLE_PIN_LAYER } from '../../core/layers/vehicle-style.js';
import {
  getVehicleData,
  getOdVehicles,
  getOdStops,
  getSpareTrips,
  computeVehicleStopGroups,
} from './data.js';
import { lngLatFromGeoJson } from './helpers.js';

const bus = emitter();
/** fn(selected) after any selection change. selected is null or
 *  { source:'spare'|'od', id, driverNorm }. */
export function onSelectionChange(fn) {
  return bus.on('change', fn);
}

let map = null;
let selected = null; // { source, id, driverNorm }

export const getSelected = () => selected;

// --- card matching (ported vanCardMatches) --------------------------
export function vanCardMatches(el) {
  if (!selected) return false;
  if (el.dataset.vanSource !== selected.source) return false;
  if (selected.id && el.dataset.vehicleId && el.dataset.vehicleId === selected.id) return true;
  // W2W shift rows / OnDemand cards carry no vehicle id — fall back to driver name.
  if (selected.driverNorm && el.dataset.driver && el.dataset.driver === selected.driverNorm) return true;
  return false;
}

/** Toggle `.card-selected` on every trip/duty card, scrolling the first hit
 *  into view when `doScroll`. Itinerary expansion is handled by duty-roster.js
 *  reacting to onSelectionChange, so this only touches the class. */
export function applyHighlight(doScroll) {
  ['#duties-list', '#trips-list'].forEach((sel) => {
    let firstHit = null;
    document.querySelectorAll(`${sel} .duty-card, ${sel} .trip-card`).forEach((el) => {
      const hit = vanCardMatches(el);
      el.classList.toggle('card-selected', hit);
      if (hit && !firstHit) firstHit = el;
    });
    if (doScroll && firstHit) firstHit.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  });
}

// --- framing (fit-to-bounds, no route lines) ------------------------
function boundsOf(points) {
  const pts = points.filter((p) => Array.isArray(p) && Number.isFinite(p[0]) && Number.isFinite(p[1]));
  if (!pts.length) return null;
  let w = Infinity;
  let s = Infinity;
  let e = -Infinity;
  let n = -Infinity;
  for (const [lng, lat] of pts) {
    if (lng < w) w = lng;
    if (lng > e) e = lng;
    if (lat < s) s = lat;
    if (lat > n) n = lat;
  }
  return [
    [w, s],
    [e, n],
  ];
}

function fit(points) {
  const b = boundsOf(points);
  if (!b || !map) return;
  const same = b[0][0] === b[1][0] && b[0][1] === b[1][1];
  if (same) {
    map.easeTo({ center: b[0], zoom: Math.max(map.getZoom(), 14), duration: 500 });
  } else {
    map.fitBounds(b, { padding: 90, maxZoom: 16, duration: 500 });
  }
}

/** Frame the van + all its remaining pickup/drop-off points. */
function frameVan(source, id) {
  const points = [];
  if (source === 'spare') {
    const v = getVehicleData()[id] || {};
    const here = lngLatFromGeoJson(v.currentLocation && v.currentLocation.location);
    if (here) points.push(here);
    for (const g of computeVehicleStopGroups()[id] || []) points.push(g.lngLat);
  } else {
    const van = getOdVehicles().find((x) => String(x.vehicleId) === String(id));
    if (van && Number.isFinite(van.lng) && Number.isFinite(van.lat)) points.push([van.lng, van.lat]);
    for (const st of getOdStops()) {
      if (String(st.vehicleId) !== String(id)) continue;
      if (Number.isFinite(st.lng) && Number.isFinite(st.lat)) points.push([st.lng, st.lat]);
    }
  }
  fit(points);
}

/** Frame just one ride's pickup + drop-off. */
export function frameTrip(kind, ref) {
  const points = [];
  if (kind === 'spare') {
    const req = getSpareTrips().find((t) => t.id === ref);
    if (req) {
      const p = lngLatFromGeoJson(req.scheduledPickupLocation || req.requestedPickupLocation);
      const d = lngLatFromGeoJson(req.scheduledDropoffLocation || req.requestedDropoffLocation);
      if (p) points.push(p);
      if (d) points.push(d);
    }
  } else {
    // ref is the ride key used by trip-board.js to group OnDemand stop rows.
    for (const st of getOdStops()) {
      const rec = (st.rides && st.rides[0]) || {};
      const key = st.rideId || rec.rideId || `${st.vehicleId}|${(st.riders || []).join(',')}`;
      if (key !== ref) continue;
      if (Number.isFinite(st.lng) && Number.isFinite(st.lat)) points.push([st.lng, st.lat]);
    }
  }
  fit(points);
}

// --- selection ----------------------------------------------------
export function clearSelection() {
  if (!selected) return;
  selected = null;
  applyHighlight(false);
  bus.emit('change', null);
}

export function selectVan(source, id, driverNorm) {
  const normId = id != null && id !== '' ? String(id) : null;
  const norm = driverNorm || null;
  if (
    selected &&
    selected.source === source &&
    selected.id === normId &&
    selected.driverNorm === norm
  ) {
    clearSelection(); // clicked the already-selected van -> toggle off
    return;
  }
  selected = { source, id: normId, driverNorm: norm };
  applyHighlight(true);
  bus.emit('change', selected);
  if (normId) frameVan(source, normId);
}

// --- map click: which van (if any) was hit -----------------------
/** `micro:sp:<id>` / `micro:od:<id>` (from core/layers/vehicles.js feature
 *  props) -> { source, id }, or null. */
function parseVehicleFeatureId(fid) {
  const s = String(fid || '');
  if (s.startsWith('micro:sp:')) return { source: 'spare', id: s.slice('micro:sp:'.length) };
  if (s.startsWith('micro:od:')) return { source: 'od', id: s.slice('micro:od:'.length) };
  return null;
}

export function installSelection(theMap) {
  map = theMap;
  map.on('click', (e) => {
    let hits = [];
    try {
      hits = map.queryRenderedFeatures(e.point, { layers: [VEHICLE_PIN_LAYER] });
    } catch {
      hits = [];
    }
    const hit = hits.length ? parseVehicleFeatureId(hits[0].properties && hits[0].properties.id) : null;
    if (hit) selectVan(hit.source, hit.id);
    else clearSelection();
  });
}
