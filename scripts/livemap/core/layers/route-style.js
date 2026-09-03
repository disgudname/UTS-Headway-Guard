// livemap/core/layers/route-style.js
// -----------------------------------------------------------------------------
// Static MapLibre source + layer definitions for the UTS route lines.
//
// Like the vehicle layer, the GeoJSON source is baked into the basemap style
// document (see core/basemap-style.js) rather than added at runtime — a
// runtime-added GeoJSON source was landing with "errored" tiles. routes.js only
// ever calls getSource(...).setData() on it.
//
// Two layers, bottom to top:
//   casing — a wide neutral halo that separates the coloured line from the map
//   line   — the route-coloured stroke itself (colour comes per-feature)
//
// The source is fed by route-overlap.js's striping engine: every feature carries
// `kind` ('casing' | 'line'), and the shared-corridor stretches arrive as short
// alternating-colour LineString pieces. The line layer is butt-capped so those
// pieces meet flush and read as one continuous stroke; the casing stays round so
// the halo is unbroken.
//
// Both sit above the basemap but below the vehicle symbols (basemap-style.js
// appends the vehicle layers after these).
// -----------------------------------------------------------------------------

export const ROUTE_SOURCE_ID = 'livemap-routes';
export const ROUTE_CASING_LAYER = 'livemap-routes-casing';
export const ROUTE_LINE_LAYER = 'livemap-routes-line';

export const ROUTE_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

const CASING_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 3, 13, 5, 16, 10, 18, 15];
const LINE_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 1.6, 13, 3, 16, 6, 18, 10];

/** The two route line layers, themed. `theme` is 'light' | 'dark'. */
export function routeLayerDefs(theme) {
  const casing = theme === 'dark' ? '#0b0f18' : '#ffffff';
  return [
    {
      id: ROUTE_CASING_LAYER,
      type: 'line',
      source: ROUTE_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'casing'],
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': casing,
        'line-width': CASING_WIDTH,
      },
    },
    {
      id: ROUTE_LINE_LAYER,
      type: 'line',
      source: ROUTE_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'line'],
      layout: { 'line-cap': 'butt', 'line-join': 'round' },
      paint: {
        'line-color': ['get', 'color'],
        'line-width': LINE_WIDTH,
      },
    },
  ];
}
