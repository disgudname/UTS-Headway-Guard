// livemap/apps/boot.js
// -----------------------------------------------------------------------------
// The shared boot sequence for every livemap shell. The thin entry points
// (apps/live.js, apps/kiosk.js, apps/embed.js) set the operator mode (if any)
// and then call startLivemap() — this file is where the map, basemap, theme
// and every feature layer actually come up.
//
// What gets shown is decided by core/modes.js: a full interactive page mounts
// the panels + search box; a kiosk / embed drops them (a kiosk keeps only the
// "no bus data" status card).
// -----------------------------------------------------------------------------

import { createMap } from '../core/map.js';
import { initTheme } from '../core/theme.js';
import { getOperatorMode, isChromeHidden, isKioskLike } from '../core/modes.js';
import { startKioskSchedule } from '../core/kiosk-schedule.js';
import { RouteLegend } from '../ui/route-legend.js';
import { installMarkerMenu } from '../core/marker-menu.js';
import { installSatelliteLayer } from '../core/layers/satellite.js';
import { installBuildingHighlight } from '../core/layers/building-highlight.js';
import { installRouteLayer } from '../core/layers/routes.js';
import { installStopLayer } from '../core/layers/stops.js';
import { installVehicleLayer } from '../core/layers/vehicles.js';
import { installCatLayer } from '../core/layers/cat.js';
import { installMicroTripsLayer } from '../core/layers/micro-trips.js';
import { installSafetyLayer } from '../core/layers/safety.js';
import { installDispatcherBridge } from '../core/dispatcher-bridge.js';
import { installCoordCopy } from '../core/coord-copy.js';
import { Panels } from '../ui/panels.js';
import { SearchBox } from '../ui/search.js';
import { KioskStatus } from '../ui/kiosk-status.js';

/** MapLibre GL v5 renders only through WebGL 2. */
function hasWebGL2() {
  try {
    return (
      !!window.WebGL2RenderingContext &&
      !!document.createElement('canvas').getContext('webgl2')
    );
  } catch {
    return false;
  }
}

/** Hand a kiosk / embed shell to the Leaflet map (/map) — raster tiles, no GPU
 *  — so a display that can't do WebGL 2 still shows a live map. Carries the
 *  framing params across. Returns true once it has navigated away. */
function redirectToLeafletMap() {
  const mode = getOperatorMode();
  if (mode !== 'kiosk' && mode !== 'adminKiosk' && mode !== 'embed') return false;
  const src = new URLSearchParams(location.search);
  const dst = new URL('/map', location.origin);
  if (mode === 'adminKiosk') dst.searchParams.set('adminKioskMode', 'true');
  else if (mode === 'kiosk') dst.searchParams.set('kioskMode', 'true');
  for (const k of ['theme', 'centerLat', 'centerLon', 'centerZoom']) {
    if (src.has(k)) dst.searchParams.set(k, src.get(k));
  }
  location.replace(dst.toString());
  return true;
}

async function boot() {
  const loading = document.getElementById('loadingOverlay');

  // Let CSS and every module see which shell this is.
  document.documentElement.dataset.mode = getOperatorMode();

  // Older digital-signage players (Raspberry Pi Chromium) have no WebGL 2, so
  // MapLibre GL v5 can't run at all. Fall back to /map (Leaflet) for signage;
  // tell an interactive visitor plainly.
  if (!hasWebGL2()) {
    console.warn('[livemap] WebGL 2 unavailable — this browser cannot render the vector map');
    if (redirectToLeafletMap()) return;
    showFatal('This display can’t render the vector map (it needs WebGL 2). Open /map instead.');
    return;
  }

  let initialStyle;
  try {
    initialStyle = await initTheme();
  } catch (err) {
    console.error('[livemap] failed to load the UVA basemap style', err);
    showFatal('Could not load the base map. Check the network and reload.');
    return;
  }

  let map;
  try {
    map = await createMap('map', initialStyle);
  } catch (err) {
    console.error('[livemap] map init failed', err);
    if (redirectToLeafletMap()) return;
    showFatal('The map failed to start. Open /map instead.');
    return;
  }

  installSatelliteLayer();
  installBuildingHighlight();
  installRouteLayer();
  installStopLayer();
  installCatLayer();
  installMicroTripsLayer();
  installSafetyLayer();
  installVehicleLayer();
  installMarkerMenu();
  installDispatcherBridge();

  if (isChromeHidden()) {
    // Kiosk: no interactive chrome, but keep the read-only route legend and the
    // full-screen "no bus data" status overlay so a public screen is legible
    // and never looks broken. A plain kiosk also carries the QR footer; an
    // adminKiosk (a back-office wall display) does not.
    if (isKioskLike()) {
      new RouteLegend({ qr: getOperatorMode() === 'kiosk' }).mount();
      new KioskStatus().mount();
    }
    // A dispatcher wall display has nobody to flip overlays — run the UVA Ride
    // overlay on a clock (overnight), like testmap's admin kiosk.
    if (getOperatorMode() === 'adminKiosk') startKioskSchedule();
  } else {
    new Panels().mount();
    new SearchBox().mount();
    installCoordCopy();
  }

  loading?.classList.add('is-hidden');
  loading?.setAttribute('aria-busy', 'false');

  // Handy during development.
  window.__livemap = { map };
}

function showFatal(message) {
  const loading = document.getElementById('loadingOverlay');
  if (loading) {
    loading.classList.remove('is-hidden');
    loading.setAttribute('aria-busy', 'false');
    const text = loading.querySelector('.loading-overlay__text');
    if (text) text.textContent = message;
  }
}

/** Kick off the boot sequence once the DOM is ready. Call this from an entry
 *  point after any configureMode(). Safe to call exactly once. */
export function startLivemap() {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot, { once: true });
  } else {
    boot();
  }
}
