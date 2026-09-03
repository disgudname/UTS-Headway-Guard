// livemap/core/config.js
// -----------------------------------------------------------------------------
// Static configuration shared by every livemap shell (live / kiosk / embed).
// No behaviour here, just constants. Anything that can be tuned without a code
// change belongs in here so it is easy to find.
// -----------------------------------------------------------------------------

// UVA GIS's own stylized vector basemap (ArcGIS Online, public, no token).
// This is the "root.json" MapLibre style document; MapLibre resolves the
// relative sprite/glyph/tile URLs inside it against this URL automatically.
export const UVA_BASEMAP_STYLE_URL =
  'https://tiles.arcgis.com/tiles/lipaMyHWQlV3h6yZ/arcgis/rest/services/' +
  'VTP_UVABasemap_Stylized/VectorTileServer/resources/styles/root.json';

// Satellite view — Esri World Imagery, plus Esri's transparent reference tiles
// (roads + place labels) laid back on top so the photo isn't label-less.
// Same source set as testmap.html's satellite toggle.
const ESRI = 'https://server.arcgisonline.com/ArcGIS/rest/services';
export const SATELLITE = Object.freeze({
  imagery: `${ESRI}/World_Imagery/MapServer/tile/{z}/{y}/{x}`,
  transportation: `${ESRI}/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}`,
  places: `${ESRI}/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}`,
  attribution:
    'Imagery &copy; <a href="https://www.esri.com/" target="_blank" rel="noopener">Esri</a>, ' +
    'Maxar, Earthstar Geographics, USDA FSA, USGS &amp; the GIS User Community',
});

// The UVA campus tiles cover z11-16; the Protomaps street basemap under them
// covers the Albemarle extract. z9 lets you pull back for regional context
// (whole service area) without hitting the edge of the pmtiles bbox. Below z12
// the City raster is off (it only overzooms to a blur down there — see
// cv-city-raster's minzoom), so the low-zoom view is clean Protomaps vector.
export const MIN_ZOOM = 9;
export const MAX_ZOOM = 20;

// Where the map first looks: central Grounds. Centre matches the legacy
// testmap default exactly; the interactive view sits one notch wider than
// testmap's zoom 15.
export const DEFAULT_VIEW = Object.freeze({
  center: [-78.50981502838886, 38.03799212281404], // [lng, lat] — MapLibre order
  zoom: 14.5,
});

// Kiosk / signage first-look zoom. A lobby screen should frame Grounds the way
// testmap's kiosk does — its INITIAL_MAP_VIEW is this same centre at zoom 15.
// Still overridden by ?centerZoom=.
export const KIOSK_ZOOM = 15;

// Matches the Albemarle Protomaps extract's bounding box, so the camera can't
// pan into un-tiled void.
export const MAX_BOUNDS = [
  [-78.95, 37.65], // south-west [lng, lat]
  [-78.10, 38.40], // north-east [lng, lat]
];

// Same-origin API. Every backend call in livemap goes through here.
export const API_BASE = '';

// localStorage keys — namespaced so they never collide with the legacy testmap.
export const STORAGE = Object.freeze({
  themeMode: 'livemap.themeMode',      // 'auto' | 'light' | 'dark'
  agency: 'livemap.agency',
  agencyConsent: 'livemap.agencyConsent',
});

// Civil-twilight anchor for the "auto" theme's solar mode (central Grounds).
export const SOLAR_ANCHOR = Object.freeze({ lat: 38.0336, lon: -78.5080 });

// UVA brand palette — the shared source of truth for UI chrome and the dark
// basemap treatment. See css/livemap.css for the CSS-variable mirror.
export const BRAND = Object.freeze({
  navy: '#232D4B',
  orange: '#E57200',
  // Dark-treatment ground tones, derived from navy so the basemap and the UI
  // chrome feel like one object.
  nightGround: '#1a2233',
  nightWater: '#122f4d',
  nightBuilding: '#2b3446',
  nightBuildingEdge: '#3b465e',
  nightRoad: '#3a4250',
  nightWalk: '#2b3242',
  nightGreen: '#20291f', // low-chroma grey-green; keeps parks legible without shouting
  nightText: '#c9d2e0',
  nightTextHalo: '#141a26',
  nightDetail: '#454f63', // hairline pavement / parking markings, dimmed
});

// Fallback colour for a vehicle whose route has no colour (or RouteID 0).
export const NO_ROUTE_COLOR = '#000000';

// Vehicle position glide: how long a bus takes to ease from its last known
// spot to a freshly reported one. Long enough to read as motion, short enough
// that the marker isn't chronically "behind".
export const VEHICLE_ANIM_MS = 900;

// A vehicle whose last GPS fix is older than this is drawn dimmed.
export const VEHICLE_STALE_S = 90;
