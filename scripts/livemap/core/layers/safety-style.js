// livemap/core/layers/safety-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the "Traffic & Incidents" overlay:
//   * traffic-flow  — TomTom congestion raster (a tile source, toggled)
//   * traffic-inc   — TomTom incident LineStrings, coloured by delay magnitude
//   * pulsepoint    — emergency incidents as category-coloured dots + label
// Baked into the basemap style doc; safety.js feeds the geojson sources and
// toggles each layer independently. All ship visibility:'none'.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';

export const TRAFFIC_FLOW_SOURCE_ID = 'livemap-traffic-flow';
export const TRAFFIC_FLOW_LAYER = 'livemap-traffic-flow';

export const TRAFFIC_INC_SOURCE_ID = 'livemap-traffic-inc';
export const TRAFFIC_INC_CASING_LAYER = 'livemap-traffic-inc-casing';
export const TRAFFIC_INC_LINE_LAYER = 'livemap-traffic-inc-line';

export const PULSEPOINT_SOURCE_ID = 'livemap-pulsepoint';
export const PULSEPOINT_DOT_LAYER = 'livemap-pulsepoint-dot';
export const PULSEPOINT_LABEL_LAYER = 'livemap-pulsepoint-label';

export const TRAFFIC_FLOW_SOURCE_DEF = {
  type: 'raster',
  tiles: [`${API_BASE}/api/traffic/tile/{z}/{x}/{y}.png`],
  tileSize: 256,
  minzoom: 8,
  maxzoom: 20,
};
export const TRAFFIC_INC_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};
export const PULSEPOINT_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

/** Every safety layer id — safety.js toggles these individually. */
export const SAFETY_LAYER_IDS = [
  TRAFFIC_FLOW_LAYER,
  TRAFFIC_INC_CASING_LAYER,
  TRAFFIC_INC_LINE_LAYER,
  PULSEPOINT_DOT_LAYER,
  PULSEPOINT_LABEL_LAYER,
];

const LABEL_FONT = ['Corbel Bold', 'Corbel Regular'];
const PP_COLOR = [
  'match', ['get', 'kind'],
  'fire', '#dc2626',
  'medical', '#2563eb',
  'traffic', '#ea580c',
  /* other */ '#6b7280',
];

/** The traffic-flow raster. Sits just above the street basemap. */
export function trafficFlowLayerDef() {
  return {
    id: TRAFFIC_FLOW_LAYER,
    type: 'raster',
    source: TRAFFIC_FLOW_SOURCE_ID,
    layout: { visibility: 'none' },
    paint: { 'raster-opacity': 0.75 },
  };
}

/** TomTom incident segments — above routes/stops, below vehicles. */
export function trafficIncLayerDefs(theme) {
  const casing = theme === 'dark' ? '#0b0f18' : '#ffffff';
  return [
    {
      id: TRAFFIC_INC_CASING_LAYER,
      type: 'line',
      source: TRAFFIC_INC_SOURCE_ID,
      layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': casing,
        'line-width': ['interpolate', ['linear'], ['zoom'], 11, 4, 15, 9, 18, 14],
        'line-opacity': 0.85,
      },
    },
    {
      id: TRAFFIC_INC_LINE_LAYER,
      type: 'line',
      source: TRAFFIC_INC_SOURCE_ID,
      layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': ['get', 'color'],
        'line-width': ['interpolate', ['linear'], ['zoom'], 11, 2.5, 15, 5.5, 18, 9],
        'line-dasharray': [1, 1.4],
      },
    },
  ];
}

/** PulsePoint incident dots + a small call-type label. Above the vehicles. */
export function pulsePointLayerDefs(theme) {
  const dark = theme === 'dark';
  return [
    {
      id: PULSEPOINT_DOT_LAYER,
      type: 'circle',
      source: PULSEPOINT_SOURCE_ID,
      layout: { visibility: 'none' },
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 4, 14, 6.5, 18, 9],
        'circle-color': PP_COLOR,
        'circle-stroke-width': 2,
        'circle-stroke-color': dark ? '#0b0f18' : '#ffffff',
        'circle-opacity': 0.95,
      },
    },
    {
      id: PULSEPOINT_LABEL_LAYER,
      type: 'symbol',
      source: PULSEPOINT_SOURCE_ID,
      minzoom: 12,
      layout: {
        visibility: 'none',
        'text-field': ['get', 'type'],
        'text-font': LABEL_FONT,
        'text-size': 10,
        'text-offset': [0, 1.1],
        'text-anchor': 'top',
        'text-allow-overlap': false,
      },
      paint: {
        'text-color': dark ? '#e7ecf5' : '#1b2130',
        'text-halo-color': dark ? '#0b0f18' : '#ffffff',
        'text-halo-width': 1.4,
      },
    },
  ];
}
