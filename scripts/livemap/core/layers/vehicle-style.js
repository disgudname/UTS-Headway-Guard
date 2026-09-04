// livemap/core/layers/vehicle-style.js
// -----------------------------------------------------------------------------
// Static MapLibre source + the single layer definition for the live-vehicle
// layer.
//
// Baked into the basemap style document (see core/basemap-style.js): a GeoJSON
// source added after the style loads was landing with "errored" tiles and never
// rendering. vehicles.js only ever calls getSource(...).setData() on this
// source; marker images are added at runtime via map.addImage.
//
// ONE symbol layer, ONE image per vehicle. For a public / kiosk viewer that
// image is just the route-coloured teardrop from busmarker.svg (rotated to
// heading via `icon-rotate`). For a signed-in dispatcher it's a *composite* —
// the teardrop (baked pre-rotated) plus the number pill above and the speed
// pill below, all drawn onto one canvas by vehicles.js, with `icon-rotate` set
// to 0.
//
// Why one image and not three stacked layers: MapLibre draws a whole layer
// before the next, so with separate pin / speed / name layers every vehicle's
// pills drew on top of every *other* vehicle's pin — pills from bus A landing
// over bus B's marker. A single image per vehicle means `symbol-sort-key`
// orders the entire marker as a unit (southernmost on top, Leaflet-style).
// -----------------------------------------------------------------------------

export const VEHICLE_SOURCE_ID = 'livemap-vehicles';
// The one vehicle symbol layer (kept named "…-pin" for back-compat: the marker
// menu and a couple of call sites reference this id).
export const VEHICLE_PIN_LAYER = 'livemap-vehicles-pin';

export const VEHICLE_SOURCE_DEF = {
  type: 'geojson',
  data: { type: 'FeatureCollection', features: [] },
};

// Scales the marker image with zoom. The teardrop is rasterised at ~80px (2x);
// this keeps it ~25-37 screen px across the usable zoom range. The composite's
// pills are drawn proportionally and ride along.
const SIZE_BY_ZOOM = [
  'interpolate',
  ['linear'],
  ['zoom'],
  11, 0.44,
  14, 0.62,
  16, 0.78,
  18, 0.92,
];

export const VEHICLE_LAYER_DEFS = [
  {
    id: VEHICLE_PIN_LAYER,
    type: 'symbol',
    source: VEHICLE_SOURCE_ID,
    layout: {
      'icon-image': ['get', 'icon'],
      // heading for a bare teardrop; 0 for a composite (its pin is baked rotated)
      'icon-rotate': ['get', 'iconRotate'],
      'icon-rotation-alignment': 'map',
      'icon-anchor': 'center',
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
      'icon-size': SIZE_BY_ZOOM,
      // Whole-marker stacking: southernmost vehicle's image draws on top.
      'symbol-sort-key': ['get', 'sortKey'],
    },
  },
];
