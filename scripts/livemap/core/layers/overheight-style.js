// livemap/core/layers/overheight-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the over-height bridge alert (overheight.js):
// a translucent red disc marking the low-clearance bridge's trigger radius.
//
// Baked into the basemap style document like every other layer, and inserted
// just BELOW the vehicle layer (see core/basemap-style.js) so the disc tints the
// streets and routes but never draws over a bus marker. It stays empty until an
// over-height bus is inside the radius.
// -----------------------------------------------------------------------------

export const OVERHEIGHT_SOURCE_ID = 'livemap-overheight';
export const OVERHEIGHT_FILL_LAYER = 'livemap-overheight-fill';
export const OVERHEIGHT_LINE_LAYER = 'livemap-overheight-line';

export const OVERHEIGHT_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

export function overheightLayerDefs() {
  return [
    {
      id: OVERHEIGHT_FILL_LAYER,
      type: 'fill',
      source: OVERHEIGHT_SOURCE_ID,
      paint: { 'fill-color': '#dc2626', 'fill-opacity': 0.15 },
    },
    {
      id: OVERHEIGHT_LINE_LAYER,
      type: 'line',
      source: OVERHEIGHT_SOURCE_ID,
      paint: { 'line-color': '#dc2626', 'line-width': 2 },
    },
  ];
}
