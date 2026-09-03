// livemap/core/basemap-style.js
// -----------------------------------------------------------------------------
// Fetches UVA GIS's stylized vector basemap once, then produces two finished
// MapLibre style documents from it: a faithful "light" (day) treatment and a
// hand-tuned "dark" (night) treatment.
//
// Why transform instead of shipping two static style files:
//   * The UVA style has 187 layers (roof materials, turf colours, pavement
//     markings). Re-deriving that by hand for night would rot the moment UVA
//     edits their style. A category-based transform tracks their updates.
//   * The dark treatment deliberately *flattens* UVA's architectural detail
//     (every roof material -> one calm building fill) so route lines and
//     vehicle markers are the brightest things on the map.
// -----------------------------------------------------------------------------

import { UVA_BASEMAP_STYLE_URL, BRAND, SATELLITE } from './config.js';
import { parseColor, colorToCss, luminance, mix } from './util.js';
import { VEHICLE_SOURCE_ID, VEHICLE_SOURCE_DEF, VEHICLE_LAYER_DEFS } from './layers/vehicle-style.js';
import { ROUTE_SOURCE_ID, ROUTE_SOURCE_DEF, routeLayerDefs } from './layers/route-style.js';
import { STOP_SOURCE_ID, STOP_SOURCE_DEF, stopLayerDefs } from './layers/stop-style.js';
import {
  CAT_ROUTE_SOURCE_ID,
  CAT_ROUTE_SOURCE_DEF,
  CAT_STOP_SOURCE_ID,
  CAT_STOP_SOURCE_DEF,
  catRouteLayerDefs,
  catStopLayerDef,
} from './layers/cat-style.js';
import {
  MICRO_ZONE_SOURCE_ID,
  MICRO_ZONE_SOURCE_DEF,
  MICRO_TRIP_SOURCE_ID,
  MICRO_TRIP_SOURCE_DEF,
  microZoneLayerDefs,
  microTripLayerDefs,
} from './layers/micro-trips-style.js';
import {
  TRAFFIC_FLOW_SOURCE_ID,
  TRAFFIC_FLOW_SOURCE_DEF,
  TRAFFIC_INC_SOURCE_ID,
  TRAFFIC_INC_SOURCE_DEF,
  PULSEPOINT_SOURCE_ID,
  PULSEPOINT_SOURCE_DEF,
  trafficFlowLayerDef,
  trafficIncLayerDefs,
  pulsePointLayerDefs,
} from './layers/safety-style.js';

// Same-origin proxy of UVA GIS's VectorTileServer (app.py). Keeps the basemap
// reachable from a locked-down signage network that can't hit tiles.arcgis.com.
// Must be an ABSOLUTE url: the style is handed to MapLibre as an object, and
// its tile/glyph workers have no base to resolve a root-relative path against.
const VTS_BASE = new URL('/v1/livemap/basemap', location.href).href;

// The UVA vector tiles carry real geometry only through LOD 16; MapLibre
// overzooms past that for z16-20.
const SOURCE_MAXZOOM = 16;

// Deep-clone for plain JSON style fragments (sources, layer defs). Hand-rolled
// rather than structuredClone(): signage players run older Chromium where that
// global is missing, and there's nothing here JSON can't round-trip.
const clone = (o) => JSON.parse(JSON.stringify(o));

let _cache = null; // Promise<{ light, dark }>

/** Fetch + build both treatments. Cached for the page's lifetime. */
export function loadBasemapTreatments() {
  if (!_cache) {
    _cache = fetch(UVA_BASEMAP_STYLE_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`UVA basemap style HTTP ${r.status}`);
        return r.json();
      })
      .then((raw) => ({
        light: buildLight(raw),
        dark: buildDark(raw),
      }))
      .catch((err) => {
        _cache = null; // allow a retry on the next call
        throw err;
      });
  }
  return _cache;
}

// --- shared plumbing -------------------------------------------------------

/**
 * MapLibre resolves a style's relative sprite/glyph/tile URLs against the style
 * URL only when you pass it a URL string. We pass an object, so we absolutise
 * everything up front and pin the source to an explicit XYZ tile template.
 */
function normalizeBase(style) {
  const s = clone(style);

  s.glyphs = `${VTS_BASE}/fonts/{fontstack}/{range}.pbf`;
  s.sprite = `${VTS_BASE}/sprites/sprite`;

  s.sources = s.sources || {};
  for (const [id, src] of Object.entries(s.sources)) {
    if (src && src.type === 'vector') {
      s.sources[id] = {
        type: 'vector',
        tiles: [`${VTS_BASE}/tile/{z}/{y}/{x}.pbf`],
        scheme: 'xyz',
        minzoom: src.minzoom ?? 11,
        maxzoom: SOURCE_MAXZOOM,
        attribution: src.attribution || 'UVA GES',
      };
    }
  }

  // Two street basemaps beneath the UVA campus layers:
  //  1. cville  — Protomaps/OSM vector extract of Albemarle. Full county
  //     coverage, themeable, and the only layer below zoom 12.
  //  2. citybasemap — City of Charlottesville GIS cartographic raster (real
  //     road/sidewalk/alley polygon shapes). Covers the city + near county,
  //     zoom 12-21, and sits ON TOP of the Protomaps vector where it exists.
  s.sources.cville = {
    type: 'vector',
    url: 'pmtiles:///livemap-vendor/albemarle.pmtiles',
    attribution:
      '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap</a>',
  };
  s.sources.citybasemap = {
    type: 'raster',
    tiles: ['/citybasemap/{z}/{x}/{y}'],
    tileSize: 256,
    minzoom: 12,
    maxzoom: 21,
    // The City service's own cached extent — keeps MapLibre from requesting
    // (and drawing blank) tiles out past Charlottesville + the near county.
    bounds: [-78.5972, 37.9788, -78.3361, 38.2123],
    attribution: 'City of Charlottesville GIS',
  };

  // Live-vehicle + route GeoJSON sources, baked in so they tile reliably (see
  // layers/vehicle-style.js, layers/route-style.js). The feature modules only
  // ever setData() them.
  s.sources[VEHICLE_SOURCE_ID] = clone(VEHICLE_SOURCE_DEF);
  s.sources[ROUTE_SOURCE_ID] = clone(ROUTE_SOURCE_DEF);
  s.sources[STOP_SOURCE_ID] = clone(STOP_SOURCE_DEF);
  s.sources[CAT_ROUTE_SOURCE_ID] = clone(CAT_ROUTE_SOURCE_DEF);
  s.sources[CAT_STOP_SOURCE_ID] = clone(CAT_STOP_SOURCE_DEF);
  s.sources[MICRO_ZONE_SOURCE_ID] = clone(MICRO_ZONE_SOURCE_DEF);
  s.sources[MICRO_TRIP_SOURCE_ID] = clone(MICRO_TRIP_SOURCE_DEF);
  s.sources[TRAFFIC_FLOW_SOURCE_ID] = clone(TRAFFIC_FLOW_SOURCE_DEF);
  s.sources[TRAFFIC_INC_SOURCE_ID] = clone(TRAFFIC_INC_SOURCE_DEF);
  s.sources[PULSEPOINT_SOURCE_ID] = clone(PULSEPOINT_SOURCE_DEF);
  s.sources[BUILDING_SOURCE_ID] = {
    type: 'geojson',
    data: { type: 'FeatureCollection', features: [] },
  };

  // Satellite view (Esri). Layers start hidden; layers/satellite.js flips them.
  s.sources['sat-imagery'] = {
    type: 'raster',
    tiles: [SATELLITE.imagery],
    tileSize: 256,
    maxzoom: 19,
    attribution: SATELLITE.attribution,
  };
  s.sources['sat-ref'] = {
    type: 'raster',
    tiles: [SATELLITE.transportation],
    tileSize: 256,
    maxzoom: 19,
  };
  s.sources['sat-places'] = {
    type: 'raster',
    tiles: [SATELLITE.places],
    tileSize: 256,
    maxzoom: 19,
  };

  return s;
}

export const BUILDING_SOURCE_ID = 'livemap-building';
const BUILDING_FILL_LAYER = 'livemap-building-fill';
const BUILDING_LINE_LAYER = 'livemap-building-outline';

/** A single highlighted building footprint (search result). Above the basemap,
 *  below routes/stops/vehicles so transit markers stay readable on top. */
function addBuildingHighlightLayers(style) {
  style.layers.push(
    {
      id: BUILDING_FILL_LAYER,
      type: 'fill',
      source: BUILDING_SOURCE_ID,
      paint: { 'fill-color': BRAND.orange, 'fill-opacity': 0.22 },
    },
    {
      id: BUILDING_LINE_LAYER,
      type: 'line',
      source: BUILDING_SOURCE_ID,
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: { 'line-color': BRAND.orange, 'line-width': 2.5 },
    },
  );
}

const SAT_IMAGERY_LAYER = 'livemap-sat-imagery';
const SAT_REF_LAYER = 'livemap-sat-ref';
const SAT_PLACES_LAYER = 'livemap-sat-places';
export const SATELLITE_LAYER_IDS = [SAT_IMAGERY_LAYER, SAT_REF_LAYER, SAT_PLACES_LAYER];

/**
 * Opaque aerial imagery (dimmed a touch so route lines / markers keep contrast)
 * with Esri's transparent road + place-label tiles back on top. Covers the
 * whole vector basemap when shown; starts hidden.
 */
function addSatelliteLayers(style) {
  style.layers.push(
    {
      id: SAT_IMAGERY_LAYER,
      type: 'raster',
      source: 'sat-imagery',
      layout: { visibility: 'none' },
      paint: {
        'raster-brightness-max': 0.72,
        'raster-saturation': -0.12,
        'raster-contrast': -0.03,
      },
    },
    {
      id: SAT_REF_LAYER,
      type: 'raster',
      source: 'sat-ref',
      layout: { visibility: 'none' },
      paint: { 'raster-opacity': 0.85 },
    },
    {
      id: SAT_PLACES_LAYER,
      type: 'raster',
      source: 'sat-places',
      layout: { visibility: 'none' },
      paint: { 'raster-opacity': 0.7 },
    },
  );
}

/** Append the live-vehicle symbol layers on top of everything. */
function addVehicleLayers(style) {
  for (const def of VEHICLE_LAYER_DEFS) style.layers.push(clone(def));
}

/** Append the route casing + line layers (above the basemap, below vehicles). */
function addRouteLayers(style, theme) {
  for (const def of routeLayerDefs(theme)) style.layers.push(clone(def));
}

/** Append the stop bead + cluster layers (above routes, below vehicles). */
function addStopLayers(style, theme) {
  for (const def of stopLayerDefs(theme)) style.layers.push(clone(def));
}

/** Append the CAT overlay route + stop layers (start hidden). CAT vehicles ride
 *  the shared UTS vehicle layers, so there's nothing for them here. */
function addCatLayers(style, theme) {
  for (const def of catRouteLayerDefs(theme)) style.layers.push(clone(def));
  style.layers.push(clone(catStopLayerDef(theme)));
}

/** Spare coverage polygon — sits just above the street basemap. */
function addMicroZoneLayers(style, theme) {
  for (const def of microZoneLayerDefs(theme)) style.layers.push(clone(def));
}

/** Microtransit pickup/drop-off point layers — above stops, below vehicles. */
function addMicroTripLayers(style, theme) {
  for (const def of microTripLayerDefs(theme)) style.layers.push(clone(def));
}

/** TomTom congestion raster — just above the street basemap. */
function addTrafficFlowLayer(style) {
  style.layers.push(clone(trafficFlowLayerDef()));
}

/** TomTom incident lines — above routes/stops, below vehicles. */
function addTrafficIncLayers(style, theme) {
  for (const def of trafficIncLayerDefs(theme)) style.layers.push(clone(def));
}

/** PulsePoint incident dots — on top of everything (safety info stays visible). */
function addPulsePointLayers(style, theme) {
  for (const def of pulsePointLayerDefs(theme)) style.layers.push(clone(def));
}

/** Splice the street-basemap layers in just above the background layer. */
function addStreetLayers(style, theme) {
  const layers = streetBasemapLayers(theme);
  const firstReal = style.layers.findIndex((l) => l.id !== 'livemap-background');
  const at = firstReal === -1 ? style.layers.length : firstReal;
  style.layers.splice(at, 0, ...layers);
}

/** Guarantee a background layer at the very bottom with the given colour. */
function ensureBackground(style, color) {
  const hasBg = style.layers.some((l) => l.type === 'background');
  if (hasBg) {
    for (const l of style.layers) {
      if (l.type === 'background') {
        l.paint = l.paint || {};
        l.paint['background-color'] = color;
      }
    }
  } else {
    style.layers.unshift({
      id: 'livemap-background',
      type: 'background',
      paint: { 'background-color': color },
    });
  }
}

// --- LIGHT (day) ---------------------------------------------------------------
// Faithful to UVA's cartography. The only change is guaranteeing a warm paper
// backdrop behind any gaps, so the map never flashes the page background.

function buildLight(raw) {
  const s = normalizeBase(raw);
  ensureBackground(s, '#f3efe6');
  addStreetLayers(s, 'light');
  addSatelliteLayers(s);
  addTrafficFlowLayer(s);
  addMicroZoneLayers(s, 'light');
  addBuildingHighlightLayers(s);
  addRouteLayers(s, 'light');
  addStopLayers(s, 'light');
  addCatLayers(s, 'light');
  addMicroTripLayers(s, 'light');
  addTrafficIncLayers(s, 'light');
  addVehicleLayers(s);
  addPulsePointLayers(s, 'light');
  s.name = 'UVA Grounds — Day';
  return s;
}

// --- STREET BASEMAP (Protomaps / OSM) --------------------------------------
// A compact road network + water + place labels for everywhere the UVA tiles
// don't cover. Two palettes; same layer set. Protomaps "basemap" v4 schema:
// source-layers water / landuse / roads / places, road `kind` in
// highway|major_road|medium_road|minor_road|other|path|rail.

const STREET_PALETTES = {
  light: {
    water: '#a9cae8',
    park: '#dcebc9',
    minor: '#ffffff',
    medium: '#ffffff',
    mediumCasing: '#e7e1d4',
    major: '#f6d6a1',
    majorCasing: '#e3bd82',
    highway: '#f7d49a',
    highwayCasing: '#dda85c',
    rail: '#c7c1b6',
    roadLabel: '#5b5346',
    roadLabelHalo: '#ffffff',
    placeLabel: '#37373a',
    placeLabelHalo: 'rgba(255,255,255,0.9)',
  },
  dark: {
    water: BRAND.nightWater,
    park: '#233021',
    minor: '#333c4b',
    medium: '#3c4555',
    mediumCasing: '#2b3340',
    major: '#49546c',
    majorCasing: '#39435a',
    highway: '#5a6684',
    highwayCasing: '#2b3341',
    rail: '#3a4250',
    roadLabel: '#aeb8c9',
    roadLabelHalo: BRAND.nightTextHalo,
    placeLabel: BRAND.nightText,
    placeLabelHalo: BRAND.nightTextHalo,
  },
};

const ROAD_LABEL_FONT = ['Corbel Regular'];
const PLACE_LABEL_FONT = ['Corbel Bold', 'Corbel Regular'];

function streetBasemapLayers(theme) {
  const p = STREET_PALETTES[theme] || STREET_PALETTES.light;
  const S = 'cville';
  // The City raster is fixed-light cartography — dimming it for night just
  // looks muddy, so at night we drop it and let the themed OSM vector carry
  // off-Grounds. (Day keeps the real City road shapes.)
  const includeCityRaster = theme !== 'dark';
  const line = (id, filter, color, widthStops, extra = {}) => ({
    id: `cv-${id}`,
    type: 'line',
    source: S,
    'source-layer': 'roads',
    filter,
    layout: { 'line-cap': 'round', 'line-join': 'round' },
    paint: {
      'line-color': color,
      'line-width': ['interpolate', ['linear'], ['zoom'], ...widthStops],
    },
    ...extra,
  });

  return [
    {
      id: 'cv-water',
      type: 'fill',
      source: S,
      'source-layer': 'water',
      // The `water` source-layer mixes polygon water bodies with river/stream
      // CENTRELINES (LineStrings). A fill layer rendering those linestrings
      // closes each path into a huge triangular wedge — the stray blue slabs
      // across the county when zoomed out. Fill polygons only.
      filter: ['==', ['geometry-type'], 'Polygon'],
      paint: { 'fill-color': p.water },
    },
    {
      // River / stream centrelines as thin lines (kept off the fill layer above).
      id: 'cv-waterway',
      type: 'line',
      source: S,
      'source-layer': 'water',
      filter: [
        'all',
        ['==', ['geometry-type'], 'LineString'],
        ['match', ['get', 'kind'], ['river', 'stream', 'canal', 'ditch', 'drain'], true, false],
      ],
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': p.water,
        'line-width': ['interpolate', ['linear'], ['zoom'], 11, 0.6, 14, 1.6, 17, 3.5],
      },
    },
    {
      id: 'cv-park',
      type: 'fill',
      source: S,
      'source-layer': 'landuse',
      filter: ['match', ['get', 'kind'], ['park', 'forest', 'grass', 'wood', 'nature_reserve', 'golf_course'], true, false],
      paint: { 'fill-color': p.park, 'fill-opacity': theme === 'dark' ? 0.5 : 0.55 },
    },
    // Casings first (so fills sit on top and joins look clean).
    line('highway-casing', ['==', ['get', 'kind'], 'highway'], p.highwayCasing, [7, 1.6, 12, 5, 16, 15]),
    line('major-casing', ['==', ['get', 'kind'], 'major_road'], p.majorCasing, [9, 1, 13, 3.5, 16, 10]),
    // Fills, thinnest class first.
    line(
      'minor',
      ['match', ['get', 'kind'], ['minor_road', 'other'], true, false],
      p.minor,
      [12, 0.4, 15, 1.8, 17, 4],
    ),
    line('medium-casing', ['==', ['get', 'kind'], 'medium_road'], p.mediumCasing, [11, 1, 14, 3.5, 16, 8]),
    line('medium', ['==', ['get', 'kind'], 'medium_road'], p.medium, [11, 0.6, 14, 2.4, 16, 6]),
    line('major', ['==', ['get', 'kind'], 'major_road'], p.major, [9, 0.7, 13, 2.6, 16, 8]),
    line('highway', ['==', ['get', 'kind'], 'highway'], p.highway, [7, 1, 12, 3.6, 16, 12]),
    {
      ...line('rail', ['==', ['get', 'kind'], 'rail'], p.rail, [11, 0.6, 16, 2]),
      paint: {
        'line-color': p.rail,
        'line-width': ['interpolate', ['linear'], ['zoom'], 11, 0.6, 16, 2],
        'line-dasharray': [2, 3],
      },
    },
    {
      id: 'cv-road-label',
      type: 'symbol',
      source: S,
      'source-layer': 'roads',
      filter: ['all', ['has', 'name'], ['match', ['get', 'kind'], ['highway', 'major_road', 'medium_road'], true, false]],
      layout: {
        'symbol-placement': 'line',
        'text-field': ['get', 'name'],
        'text-font': ROAD_LABEL_FONT,
        'text-size': ['interpolate', ['linear'], ['zoom'], 12, 10, 16, 13],
        'text-letter-spacing': 0.02,
      },
      paint: {
        'text-color': p.roadLabel,
        'text-halo-color': p.roadLabelHalo,
        'text-halo-width': 1.5,
      },
    },
    {
      id: 'cv-place-label',
      type: 'symbol',
      source: S,
      'source-layer': 'places',
      filter: ['match', ['get', 'kind'], ['locality', 'region', 'city', 'town', 'village'], true, false],
      layout: {
        'text-field': ['get', 'name'],
        'text-font': PLACE_LABEL_FONT,
        'text-size': [
          'interpolate',
          ['linear'],
          ['zoom'],
          8, ['match', ['get', 'kind'], ['region', 'city'], 13, 11],
          13, ['match', ['get', 'kind'], ['region', 'city'], 17, 14],
        ],
        'text-transform': 'none',
        'text-max-width': 7,
      },
      paint: {
        'text-color': p.placeLabel,
        'text-halo-color': p.placeLabelHalo,
        'text-halo-width': 1.7,
      },
    },
    // City cartographic raster on top of the OSM vector — real road/sidewalk
    // shapes wherever the City covers (day only; see includeCityRaster).
    ...(includeCityRaster
      ? [
          {
            id: 'cv-city-raster',
            type: 'raster',
            source: 'citybasemap',
            minzoom: 12, // matches the source; never overzoom it into a blurry hillshade
            paint: { 'raster-opacity': 1 },
          },
        ]
      : []),
  ];
}

// --- DARK (night) ------------------------------------------------------------
// Category-based recolour keyed on each layer's `source-layer`. Anything not
// matched falls through to a luminance-preserving darken so new UVA layers
// still look sane before we tune them.

const NIGHT = {
  ground: BRAND.nightGround,
  water: BRAND.nightWater,
  building: BRAND.nightBuilding,
  buildingEdge: BRAND.nightBuildingEdge,
  road: BRAND.nightRoad,
  walk: BRAND.nightWalk,
  green: BRAND.nightGreen,
  text: BRAND.nightText,
  textHalo: BRAND.nightTextHalo,
  detail: BRAND.nightDetail,
};

// Ordered: first match wins. Tested against a lower-cased source-layer name.
const CATEGORY_RULES = [
  { test: (n) => /water|stream|pond|lake/.test(n), kind: 'water' },
  { test: (n) => /building|roof|structure|facilities under construction/.test(n), kind: 'building' },
  { test: (n) => /\bwall\b|walls/.test(n), kind: 'buildingEdge' },
  { test: (n) => /sidewalk|walk|trail|steps|\bpath\b/.test(n), kind: 'walk' },
  { test: (n) => /road|drive|railroad/.test(n), kind: 'road' },
  {
    test: (n) =>
      /parking|pavement marking|pavementmarkings|sports lines|no parking|saber|sabers/.test(n),
    kind: 'detail',
  },
  {
    test: (n) =>
      /landscape|athletic field|athletic court|cemetery|sportsfield|lawn|\bfield\b|boundary|bush|tree/.test(
        n,
      ),
    kind: 'green',
  },
];

function categoryFor(sourceLayer) {
  const n = String(sourceLayer || '').toLowerCase();
  if (!n) return null;
  for (const rule of CATEGORY_RULES) if (rule.test(n)) return rule.kind;
  return null;
}

/** Recolour a single string colour for a known category. */
function nightColor(value, kind, role) {
  const c = parseColor(value);
  if (!c) return value; // expression or unparseable -> leave alone

  const targetHex =
    role === 'text' ? NIGHT.text
    : role === 'textHalo' ? NIGHT.textHalo
    : kind === 'water' ? NIGHT.water
    : kind === 'building' ? NIGHT.building
    : kind === 'buildingEdge' ? NIGHT.buildingEdge
    : kind === 'road' ? NIGHT.road
    : kind === 'walk' ? NIGHT.walk
    : kind === 'green' ? NIGHT.green
    : kind === 'detail' ? NIGHT.detail
    : null;

  if (targetHex) {
    const t = parseColor(targetHex);
    // Preserve the source alpha (many marking layers are intentionally faint).
    return colorToCss({ ...t, a: kind === 'detail' ? Math.min(c.a, 0.55) : c.a });
  }

  // Fallback: keep the hue, force it into the dark end of the value range.
  const ground = parseColor(NIGHT.ground);
  const lum = luminance(c);
  return colorToCss(mix(c, ground, lum > 0.6 ? 0.82 : 0.55));
}

function buildDark(raw) {
  const s = normalizeBase(raw);
  s.name = 'UVA Grounds — Night';
  ensureBackground(s, NIGHT.ground);

  for (const layer of s.layers) {
    if (layer.id === 'livemap-background') continue;
    const paint = (layer.paint = layer.paint || {});
    const kind = categoryFor(layer['source-layer']);

    if (layer.type === 'symbol') {
      if ('text-color' in paint || layer.layout?.['text-field'] != null) {
        paint['text-color'] = nightColor(paint['text-color'] ?? '#000000', kind, 'text');
        paint['text-halo-color'] = nightColor(
          paint['text-halo-color'] ?? '#ffffff',
          kind,
          'textHalo',
        );
        paint['text-halo-width'] = Math.max(Number(paint['text-halo-width']) || 0, 1.2);
      }
      continue; // leave icon-* untouched
    }

    for (const key of ['fill-color', 'fill-outline-color', 'line-color', 'circle-color']) {
      if (typeof paint[key] === 'string') {
        const role = key === 'fill-outline-color' ? 'edge' : 'body';
        // Outlines on buildings should use the edge tone, not the fill tone.
        const effectiveKind =
          role === 'edge' && kind === 'building' ? 'buildingEdge' : kind;
        paint[key] = nightColor(paint[key], effectiveKind, null);
      }
    }
  }

  // Added after the recolour pass — the street + route layers already carry the
  // dark palette and must not be flattened by the generic category recolour.
  addStreetLayers(s, 'dark');
  addSatelliteLayers(s);
  addTrafficFlowLayer(s);
  addMicroZoneLayers(s, 'dark');
  addBuildingHighlightLayers(s);
  addRouteLayers(s, 'dark');
  addStopLayers(s, 'dark');
  addCatLayers(s, 'dark');
  addMicroTripLayers(s, 'dark');
  addTrafficIncLayers(s, 'dark');
  addVehicleLayers(s);
  addPulsePointLayers(s, 'dark');
  return s;
}
