// livemap/core/layers/safety-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the "Traffic & Incidents" overlay:
//   * traffic-flow  — TomTom congestion raster (a tile source, toggled)
//   * pulsepoint    — emergency incidents as category-coloured dots + label
// Baked into the basemap style doc; safety.js feeds the geojson sources and
// toggles each layer independently. All ship visibility:'none'.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';

export const TRAFFIC_FLOW_SOURCE_ID = 'livemap-traffic-flow';
export const TRAFFIC_FLOW_LAYER = 'livemap-traffic-flow';

export const PULSEPOINT_SOURCE_ID = 'livemap-pulsepoint';
// Kept the id `...-dot` for continuity (toggle logic, marker-menu registration),
// but it's now a symbol layer drawing the PulsePoint "respond icon" pins — the
// same PNG markers testmap and vandispatch use.
export const PULSEPOINT_DOT_LAYER = 'livemap-pulsepoint-dot';
export const PULSEPOINT_FALLBACK_IMAGE = 'livemap-pp-pin';

export const TRAFFIC_FLOW_TILE_URL = `${API_BASE}/api/traffic/tile/{z}/{x}/{y}.png`;
// The backend only has 512px tiles at z14 over the service area (TomTom
// free-tier budget); MapLibre overzooms them past 14.
export const TRAFFIC_FLOW_SOURCE_DEF = {
  type: 'raster',
  tiles: [TRAFFIC_FLOW_TILE_URL],
  tileSize: 512,
  minzoom: 14,
  maxzoom: 14,
  bounds: [-78.5419, 38.0081, -78.4872, 38.0582],
};
export const PULSEPOINT_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

/** Every safety layer id — safety.js toggles these individually. */
export const SAFETY_LAYER_IDS = [
  TRAFFIC_FLOW_LAYER,
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
