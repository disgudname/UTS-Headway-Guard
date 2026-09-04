// livemap/core/layers/micro-trips-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the dispatcher-only microtransit trip overlay:
//   * the Spare (UVA FlexRide) coverage polygon (quiet dashed under-layer)
//   * pickup / drop-off points for live FlexRide requests and UVA Ride plan
//     stops — plain points, NO connecting lines (vandispatch-style: the van's
//     popup carries the ordered destination list instead).
// Baked into the basemap style doc like the other livemap sources.
// -----------------------------------------------------------------------------

export const MICRO_ZONE_SOURCE_ID = 'livemap-micro-zone';
export const MICRO_ZONE_FILL_LAYER = 'livemap-micro-zone-fill';
export const MICRO_ZONE_LINE_LAYER = 'livemap-micro-zone-line';

export const MICRO_TRIP_SOURCE_ID = 'livemap-micro-trips';
export const MICRO_TRIP_PT_LAYER = 'livemap-micro-trip-pt';
export const MICRO_TRIP_LABEL_LAYER = 'livemap-micro-trip-label';

export const MICRO_ZONE_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};
export const MICRO_TRIP_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

/** Every trip-overlay layer id, in draw order — micro-trips.js toggles these. */
export const MICRO_TRIP_LAYER_IDS = [
  MICRO_ZONE_FILL_LAYER,
  MICRO_ZONE_LINE_LAYER,
  MICRO_TRIP_PT_LAYER,
  MICRO_TRIP_LABEL_LAYER,
];

const MICRO = '#7c3aed';
const PICKUP = '#16a34a';
const DROPOFF = '#dc2626';

/** Coverage polygon. Sits low (just above the street basemap). */
export function microZoneLayerDefs(theme) {
  return [
    {
      id: MICRO_ZONE_FILL_LAYER,
      type: 'fill',
      source: MICRO_ZONE_SOURCE_ID,
      layout: { visibility: 'none' },
      paint: { 'fill-color': MICRO, 'fill-opacity': theme === 'dark' ? 0.07 : 0.05 },
    },
    {
      id: MICRO_ZONE_LINE_LAYER,
      type: 'line',
      source: MICRO_ZONE_SOURCE_ID,
      layout: { visibility: 'none', 'line-join': 'round' },
      paint: {
        'line-color': MICRO,
        'line-opacity': 0.55,
        'line-width': 1.5,
        'line-dasharray': [3, 3],
      },
    },
  ];
}

/** Pickup / drop-off points — van-coloured badges numbered by stop order (like
 *  vandispatch). Sits above stops, below the vehicles. A pickup badge is solid;
 *  a drop-off badge carries a darker ring so P/D still read at a glance. */
export function microTripLayerDefs(theme) {
  const dark = theme === 'dark';
  return [
    {
      id: MICRO_TRIP_PT_LAYER,
      type: 'circle',
      source: MICRO_TRIP_SOURCE_ID,
      // Individual numbered stop badges only earn their space once you're zoomed
      // in on a van; below this they just pile up over the bus markers.
      minzoom: 13.5,
      layout: { visibility: 'none' },
      paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 12, 8, 16, 11, 19, 13.5],
        'circle-color': ['get', 'color'],
        'circle-stroke-width': ['match', ['get', 'role'], 'dropoff', 3.5, 2],
        'circle-stroke-color': [
          'match',
          ['get', 'role'],
          'dropoff',
          dark ? '#e7ecf5' : '#1b2130',
          dark ? '#0b0f18' : '#ffffff',
        ],
      },
    },
    {
      // The seq digit is a pre-rendered canvas glyph (micro-trips.js), NOT a
      // MapLibre `text-field`: this style's SDF glyphs intermittently fail to
      // parse in the worker ("Unimplemented type: 3"), and the local-font
      // fallback mis-centres a single character badly. Same reason the vehicle
      // pills and stop pies bake their text.
      id: MICRO_TRIP_LABEL_LAYER,
      type: 'symbol',
      source: MICRO_TRIP_SOURCE_ID,
      minzoom: 13.5,
      layout: {
        visibility: 'none',
        'icon-image': ['get', 'numIcon'],
        'icon-size': ['interpolate', ['linear'], ['zoom'], 12, 0.7, 16, 1, 19, 1.2],
        'icon-allow-overlap': true,
        'icon-ignore-placement': true,
      },
    },
  ];
}
