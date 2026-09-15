// livemap/core/trip-planner.js
// -----------------------------------------------------------------------------
// Trip planner core: owns origin/destination pins, calls /v1/trip-planner/plan,
// and draws the selected itinerary on the map. App-agnostic on purpose — nothing
// here assumes /livemap specifically, so a future page can import this module
// the same way ui/trip-planner-panel.js does, without rebuilding the map logic.
//
// State lives here, not in the panel; the panel is a thin view over
// onOrigin/onDestination/onStatus/onItineraries.
// -----------------------------------------------------------------------------

import { API_BASE } from './config.js';
import { getMap, onStyleReady } from './map.js';
import { emitter } from './util.js';
import { isCatEnabled } from './data/cat.js';
import {
  TRIP_PLANNER_SOURCE_ID,
  TRIP_PLANNER_CASING_LAYER,
  TRIP_PLANNER_RIDE_LAYER,
  TRIP_PLANNER_WALK_LAYER,
  TRIP_PLANNER_STOP_LAYER,
} from './layers/trip-planner-style.js';

const bus = emitter();
/** fn({lat,lng,label}|null) */
export const onOrigin = (fn) => bus.on('origin', fn);
/** fn({lat,lng,label}|null) */
export const onDestination = (fn) => bus.on('destination', fn);
/** fn('idle'|'loading'|'ready'|'empty'|'error') */
export const onStatus = (fn) => bus.on('status', fn);
/** fn(itineraries[]) -- the raw /v1/trip-planner/plan response's `itineraries` array */
export const onItineraries = (fn) => bus.on('itineraries', fn);
/** fn(index|null) -- which itinerary is currently drawn on the map */
export const onSelected = (fn) => bus.on('selected', fn);

let origin = null; // {lat, lng, label}
let destination = null;
let itineraries = [];
let selectedIndex = null;
let reqSeq = 0;

let originMarker = null;
let destMarker = null;

/** Call once at boot (livemap's full-chrome branch only — see apps/boot.js). */
export function installTripPlanner() {
  // The source/layers are already baked into the basemap style (empty, hidden) --
  // nothing to add here. This exists mainly so a future consumer has one obvious
  // "start using the trip planner" entry point, matching every other install*().
}

function makePin(kind) {
  const el = document.createElement('div');
  el.className = `tp-pin tp-pin--${kind}`;
  el.textContent = kind === 'origin' ? 'A' : 'B';
  return new maplibregl.Marker({ element: el, anchor: 'center' });
}

export function setOrigin(point) {
  origin = point ? { ...point } : null;
  const map = getMap();
  if (originMarker) {
    originMarker.remove();
    originMarker = null;
  }
  if (origin && map) {
    originMarker = makePin('origin').setLngLat([origin.lng, origin.lat]).addTo(map);
  }
  bus.emit('origin', origin);
  clearPlan();
}

export function setDestination(point) {
  destination = point ? { ...point } : null;
  const map = getMap();
  if (destMarker) {
    destMarker.remove();
    destMarker = null;
  }
  if (destination && map) {
    destMarker = makePin('destination').setLngLat([destination.lng, destination.lat]).addTo(map);
  }
  bus.emit('destination', destination);
  clearPlan();
}

export function getOrigin() {
  return origin;
}
export function getDestination() {
  return destination;
}

/** Drop both pins, the drawn route, and any results. Leaves the panel itself alone. */
export function clearAll() {
  setOrigin(null);
  setDestination(null);
}

function clearPlan() {
  reqSeq++; // abandon any in-flight /plan request
  itineraries = [];
  selectedIndex = null;
  bus.emit('itineraries', itineraries);
  bus.emit('selected', null);
  bus.emit('status', 'idle');
  drawItinerary(null);
}

/** Fetch itineraries for the current origin/destination. `when` is a Date, or
 *  null/omitted for "now". No-ops if either point isn't set yet. */
export async function planTrip(when) {
  if (!origin || !destination) return;
  const seq = ++reqSeq;
  selectedIndex = null;
  bus.emit('status', 'loading');

  const params = new URLSearchParams({
    from_lat: origin.lat, from_lon: origin.lng,
    to_lat: destination.lat, to_lon: destination.lng,
  });
  if (when instanceof Date) params.set('when', when.toISOString());
  // Mirrors the map's own CAT layer toggle -- a rider who hasn't turned CAT on
  // shouldn't be routed onto it as a surprise (see app.py's trip_planner_plan).
  if (isCatEnabled()) params.set('cat', 'true');

  try {
    const r = await fetch(`${API_BASE}/v1/trip-planner/plan?${params}`, { cache: 'no-store' });
    if (seq !== reqSeq) return; // superseded
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    itineraries = Array.isArray(data.itineraries) ? data.itineraries : [];
    bus.emit('itineraries', itineraries);
    if (itineraries.length) {
      bus.emit('status', 'ready');
      selectItinerary(0);
    } else {
      bus.emit('status', 'empty');
    }
  } catch (err) {
    if (seq !== reqSeq) return;
    console.warn('[livemap] trip plan request failed', err);
    itineraries = [];
    bus.emit('itineraries', itineraries);
    bus.emit('status', 'error');
  }
}

export function selectItinerary(index) {
  if (index == null || !itineraries[index]) {
    selectedIndex = null;
    bus.emit('selected', null);
    drawItinerary(null);
    return;
  }
  selectedIndex = index;
  bus.emit('selected', index);
  drawItinerary(itineraries[index]);
  fitToItinerary(itineraries[index]);
}

function fitToItinerary(itinerary) {
  const map = getMap();
  if (!map) return;
  const coords = [];
  for (const leg of itinerary.legs || []) {
    for (const [lat, lon] of leg.coordinates || []) coords.push([lon, lat]);
  }
  if (coords.length < 2) return;
  let minLng = coords[0][0], maxLng = coords[0][0], minLat = coords[0][1], maxLat = coords[0][1];
  for (const [lng, lat] of coords) {
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  }
  map.fitBounds([[minLng, minLat], [maxLng, maxLat]], { padding: 70, maxZoom: 17, duration: 700 });
}

function legFeatures(leg) {
  const coords = (leg.coordinates || []).map(([lat, lon]) => [lon, lat]);
  if (coords.length < 2) return [];
  if (leg.kind === 'walk') {
    return [{
      type: 'Feature',
      geometry: { type: 'LineString', coordinates: coords },
      properties: { kind: 'walk' },
    }];
  }
  // ride
  const props = { color: `#${String(leg.color || 'E57200').replace(/^#/, '')}` };
  return [
    { type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: { kind: 'ride-casing', ...props } },
    { type: 'Feature', geometry: { type: 'LineString', coordinates: coords }, properties: { kind: 'ride-line', ...props } },
    ...legStopFeatures(leg, props.color),
  ];
}

/** Every stop a ride leg passes through -- board, intermediate, alight -- as its
 *  own point feature, so they're still visible once trip planning hides the
 *  ambient route/stop layers (see ui/trip-planner-panel.js). Board/alight get
 *  `variant: 'end'` (drawn larger, see trip-planner-style.js) so a rider can
 *  still tell "where I get on/off" apart from stops just passed through. */
function legStopFeatures(leg, color) {
  if (leg.kind !== 'ride' || !Array.isArray(leg.stops) || !leg.stops.length) return [];
  const lastIdx = leg.stops.length - 1;
  return leg.stops.map((s, i) => ({
    type: 'Feature',
    geometry: { type: 'Point', coordinates: [s.lon, s.lat] },
    properties: { kind: 'stop', variant: i === 0 || i === lastIdx ? 'end' : 'mid', color },
  }));
}

function drawItinerary(itinerary) {
  const map = getMap();
  if (!map) return;
  const src = map.getSource(TRIP_PLANNER_SOURCE_ID);
  if (!src) return; // style not ready yet -- fine, there's nothing to show pre-boot anyway

  const features = itinerary ? (itinerary.legs || []).flatMap(legFeatures) : [];
  src.setData({ type: 'FeatureCollection', features });

  const visible = features.length > 0;
  for (const id of [TRIP_PLANNER_CASING_LAYER, TRIP_PLANNER_RIDE_LAYER, TRIP_PLANNER_WALK_LAYER, TRIP_PLANNER_STOP_LAYER]) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
  }
}

// Re-draw the currently selected itinerary after a theme swap replaces the
// style (and therefore wipes the source back to empty) -- onStyleReady replays
// every registered builder, this module's included.
onStyleReady(() => {
  if (selectedIndex != null && itineraries[selectedIndex]) {
    drawItinerary(itineraries[selectedIndex]);
  }
});
