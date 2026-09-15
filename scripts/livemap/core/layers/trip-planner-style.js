// livemap/core/layers/trip-planner-style.js
// -----------------------------------------------------------------------------
// Static MapLibre source + layer definitions for the trip planner's itinerary
// rendering. Baked into the basemap style document (see core/basemap-style.js),
// same as route-style.js and stop-style.js — a runtime-added GeoJSON source was
// landing with "errored" tiles (see route-style.js's note), so every feature
// module bakes its source in up front and only ever calls
// getSource(...).setData() on it at runtime.
//
// This is deliberately its OWN source, not the shared livemap-routes one
// (route-style.js) — that source gets overwritten wholesale on every
// routes/vehicles tick (core/layers/routes.js's syncSource()), which would wipe
// an itinerary mid-display.
//
// Four layers, bottom to top:
//   ride-casing — white/dark halo under a ride leg, matching route-style.js's look
//   ride-line   — the route-coloured stroke for a ride leg
//   walk        — a thin dashed line for a walk leg (line-dasharray can't be a
//                 data-driven property in MapLibre, so it needs its own layer —
//                 see basemap-style.js's addVandispatchRouteLayer for the same
//                 constraint solved the same way)
//   stop        — every stop a ride leg passes through (board, intermediate,
//                 alight) -- shown while trip planning hides the ambient stop
//                 layer (see core/layers/routes.js/stops.js), so a rider isn't
//                 left with no stop markers at all once the regular routes are
//                 hidden. 'variant' distinguishes board/alight ("end", drawn
//                 larger) from stops just passed through ("mid").
// -----------------------------------------------------------------------------

export const TRIP_PLANNER_SOURCE_ID = 'livemap-trip-planner';
export const TRIP_PLANNER_CASING_LAYER = 'livemap-trip-planner-casing';
export const TRIP_PLANNER_RIDE_LAYER = 'livemap-trip-planner-ride';
export const TRIP_PLANNER_WALK_LAYER = 'livemap-trip-planner-walk';
export const TRIP_PLANNER_STOP_LAYER = 'livemap-trip-planner-stop';

export const TRIP_PLANNER_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

const CASING_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 4, 14, 6.5, 18, 12];
const RIDE_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 2, 14, 3.6, 18, 7.5];
const WALK_WIDTH = ['interpolate', ['linear'], ['zoom'], 10, 1.6, 14, 2.4, 18, 4];

/** The three itinerary layers, themed. `theme` is 'light' | 'dark'. */
export function tripPlannerLayerDefs(theme) {
  const casing = theme === 'dark' ? '#0b0f18' : '#ffffff';
  const walkColor = theme === 'dark' ? '#9aa5ba' : '#5b6472';
  return [
    {
      id: TRIP_PLANNER_CASING_LAYER,
      type: 'line',
      source: TRIP_PLANNER_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'ride-casing'],
      layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
      paint: { 'line-color': casing, 'line-width': CASING_WIDTH, 'line-opacity': 0.95 },
    },
    {
      id: TRIP_PLANNER_RIDE_LAYER,
      type: 'line',
      source: TRIP_PLANNER_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'ride-line'],
      layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': ['coalesce', ['get', 'color'], '#E57200'],
        'line-width': RIDE_WIDTH,
      },
    },
    {
      id: TRIP_PLANNER_WALK_LAYER,
      type: 'line',
      source: TRIP_PLANNER_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'walk'],
      layout: { visibility: 'none', 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': walkColor,
        'line-width': WALK_WIDTH,
        'line-dasharray': [1.6, 1.8],
        'line-opacity': 0.9,
      },
    },
    {
      id: TRIP_PLANNER_STOP_LAYER,
      type: 'circle',
      source: TRIP_PLANNER_SOURCE_ID,
      filter: ['==', ['get', 'kind'], 'stop'],
      layout: { visibility: 'none' },
      paint: {
        'circle-radius': [
          'interpolate', ['linear'], ['zoom'],
          10, ['case', ['==', ['get', 'variant'], 'end'], 4, 2.5],
          14, ['case', ['==', ['get', 'variant'], 'end'], 6, 3.5],
          18, ['case', ['==', ['get', 'variant'], 'end'], 10, 6],
        ],
        'circle-color': ['coalesce', ['get', 'color'], '#E57200'],
        'circle-stroke-width': ['case', ['==', ['get', 'variant'], 'end'], 2, 1.2],
        'circle-stroke-color': casing,
      },
    },
  ];
}
