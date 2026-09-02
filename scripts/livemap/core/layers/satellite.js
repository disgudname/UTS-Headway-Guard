// livemap/core/layers/satellite.js
// -----------------------------------------------------------------------------
// The satellite ("aerial") view toggle. The raster layers themselves are baked
// into the style doc (see core/basemap-style.js, addSatelliteLayers) and start
// hidden; this module just flips their `visibility`, persists the choice, and
// re-applies after a theme swap (setStyle resets layer visibility).
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { lsGet, lsSet } from '../util.js';
import { SATELLITE_LAYER_IDS } from '../basemap-style.js';

const KEY = 'livemap.satellite';
const bus = new Set(); // change listeners: fn(bool)

let on = lsGet(KEY, '0') === '1';

export function installSatelliteLayer() {
  onStyleReady(() => apply());
}

export function isSatelliteVisible() {
  return on;
}

export function setSatelliteVisible(v) {
  on = !!v;
  lsSet(KEY, on ? '1' : '0');
  apply();
  bus.forEach((fn) => {
    try { fn(on); } catch (err) { console.error('[livemap] satellite listener threw', err); }
  });
}

/** fn(bool) on every change; fires once immediately with the current value. */
export function onSatelliteChange(fn) {
  bus.add(fn);
  fn(on);
  return () => bus.delete(fn);
}

function setVis() {
  const map = getMap();
  if (!map) return;
  const vis = on ? 'visible' : 'none';
  for (const id of SATELLITE_LAYER_IDS) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
  }
}

function apply() {
  setVis();
  // A visibility change made the instant the style becomes ready can be dropped
  // by MapLibre, and right after a theme swap the baked layers may not be
  // registered for a tick — so re-assert once the dust settles. Without this the
  // aerial view silently stays hidden after Auto->Night until you re-toggle it.
  setTimeout(setVis, 250);
}
