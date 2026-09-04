// livemap/core/layers/cat-style.js
// -----------------------------------------------------------------------------
// Static MapLibre source + layer defs for the CAT overlay's routes and stops.
// Baked into the basemap style document like the UTS layers; cat.js only ever
// setData()s the sources.
//
// CAT reads as a secondary agency: route lines are dashed (UTS is solid) and
// stops are small hollow rings. CAT *vehicles* are drawn by
// core/layers/vehicles.js with the same pin + pills as UTS buses — there is no
// CAT vehicle source here. Both layers ship visibility:'none'; cat.js flips
// them when the overlay is enabled. Order: CAT routes above UTS routes, CAT
// stops above UTS stops.
// -----------------------------------------------------------------------------

export const CAT_ROUTE_SOURCE_ID = 'livemap-cat-routes';
export const CAT_ROUTE_CASING_LAYER = 'livemap-cat-routes-casing';
export const CAT_ROUTE_LINE_LAYER = 'livemap-cat-routes-line';

export const CAT_STOP_SOURCE_ID = 'livemap-cat-stops';
export const CAT_STOP_LAYER = 'livemap-cat-stops-point';

export const CAT_ROUTE_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};
export const CAT_STOP_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

/** Every CAT layer id, in draw order — cat.js toggles visibility over this. */
export const CAT_LAYER_IDS = [
  CAT_ROUTE_CASING_LAYER,
  CAT_ROUTE_LINE_LAYER,
  CAT_STOP_LAYER,
];

const CASING_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 2.4, 13, 3.6, 16, 7, 18, 11];
const LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 1.2, 13, 2.2, 16, 4, 18, 6.5];

export function catRouteLayerDefs(theme) {
  const casing = theme === 'dark' ? '#0b0f18' : '#ffffff';
  const shown = ['==', ['get', 'visible'], 1]; // the right-panel CAT picker
  return [
    {
      id: CAT_ROUTE_CASING_LAYER,
      type: 'line',
      source: CAT_ROUTE_SOURCE_ID,
      filter: shown,
      layout: { 'line-cap': 'round', 'line-join': 'round', visibility: 'none' },
      paint: { 'line-color': casing, 'line-width': CASING_WIDTH, 'line-opacity': 0.9 },
    },
    {
      id: CAT_ROUTE_LINE_LAYER,
      type: 'line',
      source: CAT_ROUTE_SOURCE_ID,
      filter: shown,
      layout: { 'line-cap': 'butt', 'line-join': 'round', visibility: 'none' },
      paint: {
        'line-color': ['get', 'color'],
        'line-width': LINE_WIDTH,
        'line-dasharray': [2, 1.6],
      },
    },
  ];
}

export function catStopLayerDef(theme) {
  const dark = theme === 'dark';
  return {
    id: CAT_STOP_LAYER,
    type: 'circle',
    source: CAT_STOP_SOURCE_ID,
    minzoom: 12.5,
    layout: { visibility: 'none' },
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 13, 2.6, 16, 4.4, 19, 6],
      'circle-color': dark ? '#11161f' : '#ffffff',
      'circle-stroke-width': 1.6,
      'circle-stroke-color': ['get', 'color'],
    },
  };
}
