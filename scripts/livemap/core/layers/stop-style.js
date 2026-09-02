// livemap/core/layers/stop-style.js
// -----------------------------------------------------------------------------
// Static MapLibre source + layer defs for UTS stops. Baked into the basemap
// style doc (see core/basemap-style.js) like the vehicle + route sources;
// stops.js only ever setData()s it (and addImage()s the pie icons).
//
// A stop served by more than one route is drawn as a pie chart split evenly
// between the route colours (the legacy testmap treatment). stops.js rasterises
// one pie image per distinct colour-set and each stop feature just points at
// the right one via `icon-image`.
//
// The source clusters at low zoom so a pulled-back regional view isn't a wall
// of beads; from ~z14 individual stops show. Sits above the route lines, below
// the vehicles.
// -----------------------------------------------------------------------------

export const STOP_SOURCE_ID = 'livemap-stops';
export const STOP_CLUSTER_LAYER = 'livemap-stops-cluster';
export const STOP_CLUSTER_COUNT_LAYER = 'livemap-stops-cluster-count';
export const STOP_POINT_LAYER = 'livemap-stops-point';

export const STOP_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
  cluster: true,
  clusterMaxZoom: 13,
  clusterRadius: 38,
};

const PIE_SIZE_BY_ZOOM = ['interpolate', ['linear'], ['zoom'], 12, 0.5, 14, 0.72, 16, 0.95, 19, 1.15];
const CLUSTER_RADIUS = ['step', ['get', 'point_count'], 12, 10, 15, 25, 19];

const CLUSTER_FONT = ['Corbel Bold', 'Corbel Regular'];

/** `theme` is 'light' | 'dark'. */
export function stopLayerDefs(theme) {
  const dark = theme === 'dark';
  const clusterFill = dark ? '#33507d' : '#2b3557';
  const clusterStroke = dark ? '#0b0f18' : '#ffffff';

  return [
    {
      id: STOP_CLUSTER_LAYER,
      type: 'circle',
      source: STOP_SOURCE_ID,
      filter: ['has', 'point_count'],
      paint: {
        'circle-color': clusterFill,
        'circle-radius': CLUSTER_RADIUS,
        'circle-stroke-width': 2,
        'circle-stroke-color': clusterStroke,
      },
    },
    {
      id: STOP_CLUSTER_COUNT_LAYER,
      type: 'symbol',
      source: STOP_SOURCE_ID,
      filter: ['has', 'point_count'],
      layout: {
        'text-field': ['get', 'point_count_abbreviated'],
        'text-font': CLUSTER_FONT,
        'text-size': 12,
        'text-allow-overlap': true,
      },
      paint: {
        'text-color': '#ffffff',
      },
    },
    {
      id: STOP_POINT_LAYER,
      type: 'symbol',
      source: STOP_SOURCE_ID,
      filter: ['!', ['has', 'point_count']],
      layout: {
        'icon-image': ['get', 'pie'],
        'icon-size': PIE_SIZE_BY_ZOOM,
        'icon-allow-overlap': true,
        'icon-ignore-placement': true,
      },
    },
  ];
}
