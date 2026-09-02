// livemap/core/map.js
// -----------------------------------------------------------------------------
// Owns the single MapLibre GL map instance and the "style-ready" lifecycle.
//
// Swapping the basemap (light <-> dark) calls map.setStyle() with a whole new
// style document, which discards every custom source/layer we added. Feature
// modules therefore don't add their layers directly to the map once — they
// register a builder via onStyleReady(), and this module replays those builders
// after the initial load and after every theme swap.
// -----------------------------------------------------------------------------

import { DEFAULT_VIEW, MIN_ZOOM, MAX_ZOOM, MAX_BOUNDS } from './config.js';
import { paramNum } from './util.js';
import { isInteractionLocked } from './modes.js';

let map = null;
const styleReadyBuilders = new Set(); // Set<fn(map)>
let styleReadyFiredOnce = false;

// If the style hasn't reported "loaded" this long after style.load, run the
// builders anyway — addSource/addLayer still work, and a wedged tile fetch
// shouldn't hold the whole UI hostage.
const STYLE_READY_TIMEOUT_MS = 8000;

/** The live map instance (null until createMap() resolves). */
export function getMap() {
  return map;
}

/**
 * Run `cb` once the current style is genuinely ready to take custom sources and
 * layers (or after a timeout, so a stalled basemap tile can't hang the app).
 */
export function whenStyleReady(cb) {
  if (!map) return;
  if (map.isStyleLoaded()) {
    cb();
    return;
  }
  let done = false;
  const finish = () => {
    if (done) return;
    done = true;
    map.off('styledata', check);
    map.off('idle', check);
    clearTimeout(timer);
    cb();
  };
  const check = () => {
    if (map.isStyleLoaded()) finish();
  };
  const timer = setTimeout(() => {
    if (!done) console.warn('[livemap] style not "loaded" after timeout; continuing anyway');
    finish();
  }, STYLE_READY_TIMEOUT_MS);
  map.on('styledata', check);
  map.on('idle', check);
}

/**
 * Register a callback that (re)builds a feature's sources + layers. Runs once
 * the current style is ready, and again after every basemap theme swap. If the
 * style is already ready when you call this, it runs on the next tick.
 */
export function onStyleReady(builder) {
  styleReadyBuilders.add(builder);
  // If the style has already been through its first load, run this builder now
  // (next microtask). We intentionally don't gate on isStyleLoaded() — right
  // after 'style.load' it can still report false while the vector source
  // finishes, and addSource/addLayer are fine before then.
  if (styleReadyFiredOnce && map) {
    Promise.resolve().then(() => safeRun(builder));
  }
  return () => styleReadyBuilders.delete(builder);
}

function safeRun(fn) {
  try {
    fn(map);
  } catch (err) {
    console.error('[livemap] style-ready builder threw', err);
  }
}

/** Called by the theme module every time it swaps the basemap style. */
export function replayStyleBuilders() {
  styleReadyFiredOnce = true;
  styleReadyBuilders.forEach(safeRun);
}

/**
 * Boot the map with an initial style document. Resolves once the first style
 * has loaded and builders have been replayed once.
 */
let pmtilesRegistered = false;
/** Teach MapLibre the pmtiles:// scheme (used by the street basemap). */
function registerPmtilesProtocol() {
  if (pmtilesRegistered) return;
  if (typeof pmtiles === 'undefined' || !pmtiles.Protocol) {
    console.warn('[livemap] pmtiles library not loaded; street basemap disabled');
    return;
  }
  maplibregl.addProtocol('pmtiles', new pmtiles.Protocol().tile);
  pmtilesRegistered = true;
}

export function createMap(containerId, initialStyle) {
  if (map) return Promise.resolve(map);
  registerPmtilesProtocol();

  const center = [
    paramNum('centerLon', DEFAULT_VIEW.center[0]),
    paramNum('centerLat', DEFAULT_VIEW.center[1]),
  ];
  const zoom = paramNum('centerZoom', DEFAULT_VIEW.zoom);

  map = new maplibregl.Map({
    container: containerId,
    style: initialStyle,
    center,
    zoom,
    minZoom: MIN_ZOOM,
    maxZoom: MAX_ZOOM,
    maxBounds: MAX_BOUNDS,
    attributionControl: false,
    // Feels-good defaults: a touch of inertia, no rotation surprises for a
    // transit map (pitch/bearing add nothing here and cost clarity).
    dragRotate: false,
    pitchWithRotate: false,
    touchZoomRotate: true,
    fadeDuration: 120,
  });
  map.touchZoomRotate.disableRotation();
  map.on('error', (e) =>
    console.warn('[livemap] map error:', (e && e.error && (e.error.message || e.error)) || e),
  );

  map.addControl(
    // Per-source attribution ('UVA GES', OpenStreetMap) is collected automatically.
    new maplibregl.AttributionControl({ compact: true }),
    'bottom-right',
  );

  if (isInteractionLocked()) {
    // Kiosk / locked embed: a fixed camera. Kill every hand-driven gesture and
    // skip the zoom buttons — the view comes from DEFAULT_VIEW or ?centerLat/Lon.
    for (const h of [
      'dragPan', 'scrollZoom', 'boxZoom', 'dragRotate',
      'keyboard', 'doubleClickZoom', 'touchZoomRotate', 'touchPitch',
    ]) {
      map[h]?.disable?.();
    }
    map.getContainer().classList.add('livemap-map--locked');
  } else {
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
  }

  return new Promise((resolve) => {
    map.once('style.load', () => {
      whenStyleReady(() => {
        replayStyleBuilders();
        resolve(map);
      });
    });
  });
}
