// livemap/core/layers/floorplan-style.js
// -----------------------------------------------------------------------------
// Static source + layer defs for the Parking & Transit floor plan (floorplan.js):
// the 2018 plan of 1101 Millmont St, drawn inside the building once you zoom in.
//
// Baked into the basemap style document like every other layer, just above the
// basemap and below routes, stops and vehicles. The source stays empty unless a
// dispatcher is signed in. Features carry `kind`: floor | room | wall |
// partition | label (see scripts/build_pt_floorplan.py).
// -----------------------------------------------------------------------------

export const FLOORPLAN_SOURCE_ID = 'livemap-floorplan';

export const FLOORPLAN_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
  maxzoom: 20, // walls are inches thick: tile them at full zoom, not overzoomed from 18
  tolerance: 0,
};

const MIN_ZOOM = 16.5;
const LABEL_MIN_ZOOM = 18.6;

// Room colour by department, the same key as the drawing.
const PALETTES = {
  light: {
    floor: '#f4f5f5',
    wall: '#27303a',
    text: '#17212b',
    halo: 'rgba(255,255,255,0.85)',
    dept: {
      ADMIN: '#f0d3c6',
      MAINTENANCE: '#bfd6e6',
      OPERATIONS: '#f4a868',
      SUPPORT: '#dfe2cf',
      'NOT IN SCOPE': '#e9e4dc',
    },
  },
  dark: {
    floor: '#1b222b',
    wall: '#c9d1d9',
    text: '#e6ebf0',
    halo: 'rgba(12,16,22,0.85)',
    dept: {
      ADMIN: '#5a3d33',
      MAINTENANCE: '#2c4a60',
      OPERATIONS: '#8a5320',
      SUPPORT: '#434a35',
      'NOT IN SCOPE': '#35312b',
    },
  },
};

const kind = (k) => ['==', ['get', 'kind'], k];
const fadeIn = ['interpolate', ['linear'], ['zoom'], MIN_ZOOM, 0, MIN_ZOOM + 0.5, 1];

export function floorplanLayerDefs(theme) {
  const p = PALETTES[theme] || PALETTES.light;
  const deptColor = ['match', ['get', 'dept']];
  for (const [dept, color] of Object.entries(p.dept)) deptColor.push(dept, color);
  deptColor.push(p.floor);
  return [
    {
      id: 'livemap-floorplan-floor',
      type: 'fill',
      source: FLOORPLAN_SOURCE_ID,
      filter: kind('floor'),
      minzoom: MIN_ZOOM,
      paint: { 'fill-color': p.floor, 'fill-opacity': fadeIn },
    },
    {
      id: 'livemap-floorplan-room',
      type: 'fill',
      source: FLOORPLAN_SOURCE_ID,
      filter: kind('room'),
      minzoom: MIN_ZOOM,
      paint: { 'fill-color': deptColor, 'fill-opacity': fadeIn },
    },
    {
      id: 'livemap-floorplan-wall',
      type: 'fill',
      source: FLOORPLAN_SOURCE_ID,
      filter: kind('wall'),
      minzoom: MIN_ZOOM,
      paint: { 'fill-color': p.wall, 'fill-opacity': fadeIn },
    },
    {
      id: 'livemap-floorplan-partition',
      type: 'line',
      source: FLOORPLAN_SOURCE_ID,
      filter: kind('partition'),
      minzoom: MIN_ZOOM,
      paint: { 'line-color': p.wall, 'line-width': 1, 'line-opacity': fadeIn },
    },
    {
      id: 'livemap-floorplan-label',
      type: 'symbol',
      source: FLOORPLAN_SOURCE_ID,
      filter: kind('label'),
      minzoom: LABEL_MIN_ZOOM,
      layout: {
        'text-field': ['get', 'label'],
        'text-font': ['Corbel Bold', 'Corbel Regular'],
        'text-size': ['interpolate', ['linear'], ['zoom'], LABEL_MIN_ZOOM, 9, 20, 12],
        'text-line-height': 1.1,
        'text-padding': 1,
        // biggest rooms win when labels collide
        'symbol-sort-key': ['-', 0, ['get', 'sf']],
      },
      paint: { 'text-color': p.text, 'text-halo-color': p.halo, 'text-halo-width': 1.2 },
    },
  ];
}
