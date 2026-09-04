// livemap/core/layers/building-highlight.js
// -----------------------------------------------------------------------------
// The highlighted-building footprint shown when you pick a search result. The
// fill + outline layers and the GeoJSON source are baked into the style doc
// (see core/basemap-style.js); this module only setData()s it and re-applies
// after a theme swap.
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { BUILDING_SOURCE_ID as SRC } from '../basemap-style.js';

let current = null; // last GeoJSON geometry, so a theme swap can re-draw it

export function installBuildingHighlight() {
  onStyleReady(() => push(current));
}

/** Show `geometry` (a GeoJSON Polygon/MultiPolygon) as the highlighted building. */
export function highlightBuilding(geometry) {
  current = geometry || null;
  push(current);
}

export function clearBuildingHighlight() {
  current = null;
  push(null);
}

function push(geometry) {
  const map = getMap();
  const src = map && map.getSource(SRC);
  if (!src) return;
  src.setData(
    geometry
      ? { type: 'FeatureCollection', features: [{ type: 'Feature', geometry, properties: {} }] }
      : { type: 'FeatureCollection', features: [] },
  );
}
