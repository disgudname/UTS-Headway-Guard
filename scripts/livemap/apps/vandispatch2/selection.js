// livemap/apps/vandispatch2/selection.js
// -----------------------------------------------------------------------------
// Van <-> card selection sync, ported from html/vandispatch.html.
//
// Clicking a van marker on the map highlights its Duty Roster card and every
// Active Trips card it is serving, expands that duty card's itinerary, and
// (via map-overlays.js reacting to onSelectionChange) draws that van's full
// remaining route + frames it. Clicking the same van again, another van, or
// empty map clears it.
// -----------------------------------------------------------------------------

import { emitter } from '../../core/util.js';
import { VEHICLE_PIN_LAYER } from '../../core/layers/vehicle-style.js';

const bus = emitter();
/** fn(selected) after any selection change. selected is null or
 *  { source:'spare'|'od', id, driverNorm }. */
export function onSelectionChange(fn) {
  return bus.on('change', fn);
}
/** fn() on every click on empty map (no van hit) — fires even when nothing was
 *  selected, so a route drawn by a trip-card click (which doesn't go through the
 *  selection system) still gets cleared. Mirrors /vandispatch's
 *  `map.on('click', clearVanSelection)`, which always clears the route. */
export function onMapBackground(fn) {
  return bus.on('background', fn);
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
    if (hit) {
      selectVan(hit.source, hit.id);
    } else {
      clearSelection();
      bus.emit('background'); // clear any trip-card route too
    }
  });
}
