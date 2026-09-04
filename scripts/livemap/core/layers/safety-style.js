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
// Kept the id `...-dot` for continuity (toggle logic, marker-menu registration),
// but it's now a symbol layer drawing the PulsePoint "respond icon" pins — the
// same PNG markers testmap and vandispatch use.
export const PULSEPOINT_DOT_LAYER = 'livemap-pulsepoint-dot';
export const PULSEPOINT_FALLBACK_IMAGE = 'livemap-pp-pin';

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

/** PulsePoint incidents as the standard "respond icon" pins (PNG teardrops, the
 *  same markers testmap / vandispatch use). Icons are lazy-loaded per type code
 *  by safety.js via `styleimagemissing`; anything without a real icon falls back
 *  to a generated neutral pin. Anchored at the tip, above the vehicles. */
export function pulsePointLayerDefs(/* theme */) {
  return [
    {
      id: PULSEPOINT_DOT_LAYER,
      type: 'symbol',
      source: PULSEPOINT_SOURCE_ID,
      layout: {
        visibility: 'none',
        // `['image', …]` lets coalesce fall through to the fallback pin while a
        // real respond-icon PNG is still lazy-loading (or the type has none).
        'icon-image': [
          'coalesce',
          ['image', ['get', 'icon']],
          ['image', PULSEPOINT_FALLBACK_IMAGE],
        ],
        // Respond icons + the fallback pin are ~180 px source art; testmap
        // renders them near 0.25 scale (~46 px). Keep close to that, with a
        // gentle zoom taper so they don't crowd the dense central view.
        'icon-size': ['interpolate', ['linear'], ['zoom'], 10, 0.2, 13, 0.24, 16, 0.3, 19, 0.36],
        'icon-anchor': 'bottom',
        'icon-allow-overlap': true,
        'icon-ignore-placement': true,
      },
    },
  ];
}
