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
import { Panels } from '../ui/panels.js';
import { SearchBox } from '../ui/search.js';
import { KioskStatus } from '../ui/kiosk-status.js';

async function boot() {
  const loading = document.getElementById('loadingOverlay');

  // Let CSS and every module see which shell this is.
  document.documentElement.dataset.mode = getOperatorMode();

  let initialStyle;
  try {
    initialStyle = await initTheme();
  } catch (err) {
    console.error('[livemap] failed to load the UVA basemap style', err);
    showFatal('Could not load the base map. Check the network and reload.');
    return;
  }

  const map = await createMap('map', initialStyle);

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
  } else {
    new Panels().mount();
    new SearchBox().mount();
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
