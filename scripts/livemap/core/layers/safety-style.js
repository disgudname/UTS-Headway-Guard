// livemap/core/layers/safety-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the "Traffic & Incidents" overlay:
//   * traffic-flow  — TomTom vector flow lines, coloured by speed vs free flow
//   * pulsepoint    — emergency incidents as category-coloured dots + label
// Baked into the basemap style doc; safety.js feeds the geojson sources and
// toggles each layer independently. All ship visibility:'none'.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';

export const TRAFFIC_FLOW_SOURCE_ID = 'livemap-traffic-flow';
export const TRAFFIC_FLOW_CASING_LAYER = 'livemap-traffic-flow-casing';
export const TRAFFIC_FLOW_LAYER = 'livemap-traffic-flow';

export const PULSEPOINT_SOURCE_ID = 'livemap-pulsepoint';
// Kept the id `...-dot` for continuity (toggle logic, marker-menu registration),
// but it's now a symbol layer drawing the PulsePoint "respond icon" pins — the
// same PNG markers testmap and vandispatch use.
export const PULSEPOINT_DOT_LAYER = 'livemap-pulsepoint-dot';
export const PULSEPOINT_FALLBACK_IMAGE = 'livemap-pp-pin';

// Absolute: vector tiles are fetched in a web worker, which can't resolve a
// root-relative URL (the raster/geojson sources load on the main thread).
export const TRAFFIC_FLOW_TILE_URL =
  new URL(`${API_BASE}/api/traffic/vector/`, location.href).href + '{z}/{x}/{y}.pbf';
// TomTom vector flow tiles, proxied + cached by the backend (free-tier budget):
// only z11-13 over the service area exist. z13 carries every road class, so
// MapLibre overzooms it crisply past 13.
export const TRAFFIC_FLOW_SOURCE_DEF = {
  type: 'vector',
  tiles: [TRAFFIC_FLOW_TILE_URL],
  minzoom: 11,
  maxzoom: 13,
  bounds: [-78.5419, 38.0081, -78.4872, 38.0582],
};
export const PULSEPOINT_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

/** Every safety layer id — safety.js toggles these individually. */
export const SAFETY_LAYER_IDS = [
  TRAFFIC_FLOW_CASING_LAYER,
  TRAFFIC_FLOW_LAYER,
  PULSEPOINT_DOT_LAYER,
];

// Features carry traffic_level (current speed / free-flow speed, 0-1),
// road_closure and traffic_road_coverage. The lines sit under the route lines,
// so they are sized to stick out from behind them: FLOW_HALF is half the route
// casing width (route-style.js CASING_WIDTH) plus the part that shows.
//   one_side — one direction of a two-way road: FLOW_HALF wide, offset right
//              of travel by half its width, so its inside edge is on the road
//              centreline and it shows on that direction's side only.
//   full     — the whole road: centred, FLOW_HALF showing on both sides.
// Every road class gets the same width; routes run on local roads too.
const FLOW_ONE_SIDE = ['==', ['get', 'traffic_road_coverage'], 'one_side'];
const FLOW_HALF = [[10, 3.5], [13, 5.5], [16, 9], [18, 12.5]];
const flowWidth = (extra) => ['interpolate', ['linear'], ['zoom'],
  ...FLOW_HALF.flatMap(([z, half]) => [z, ['case', FLOW_ONE_SIDE, half + extra, half * 2 + extra]])];
const FLOW_OFFSET = ['interpolate', ['linear'], ['zoom'],
  ...FLOW_HALF.flatMap(([z, half]) => [z, ['case', FLOW_ONE_SIDE, half / 2, 0]])];
const FLOW_LEVEL = ['to-number', ['get', 'traffic_level'], 1];
// Only slowdowns are drawn, in reds only: the route palette already uses
// green / orange / yellow / purple / gray, so traffic green or orange would
// read as a bus line.
const FLOW_SLOW = ['any', ['to-boolean', ['get', 'road_closure']], ['<', FLOW_LEVEL, 0.75]];
const FLOW_COLOR = ['case',
  ['to-boolean', ['get', 'road_closure']], '#4a0d12',
  ['<', FLOW_LEVEL, 0.25], '#8f1420',
  ['<', FLOW_LEVEL, 0.5], '#d7263d',
  '#f47c7c'];

/** Traffic flow lines (casing + colour). Sit just above the street basemap,
 *  under routes and stops. */
export function trafficFlowLayerDefs(theme) {
  const casing = theme === 'dark' ? '#0b0f18' : '#ffffff';
  const common = {
    type: 'line',
    source: TRAFFIC_FLOW_SOURCE_ID,
    'source-layer': 'Traffic flow',
    layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
  };
  return [
    {
      ...common,
      id: TRAFFIC_FLOW_CASING_LAYER,
      filter: FLOW_SLOW,
      paint: {
        'line-color': casing,
        'line-width': flowWidth(2),
        'line-offset': FLOW_OFFSET,
        'line-opacity': 0.8,
      },
    },
    {
      ...common,
      id: TRAFFIC_FLOW_LAYER,
      filter: FLOW_SLOW,
      paint: {
        'line-color': FLOW_COLOR,
        'line-width': flowWidth(0),
        'line-offset': FLOW_OFFSET,
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
