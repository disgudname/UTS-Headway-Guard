// livemap/apps/vandispatch2.js
// -----------------------------------------------------------------------------
// Entry point for the MapLibre GL preview of Van Dispatch (served at
// /vandispatch2). This is a placeholder name — the page it will eventually
// replace is the live Leaflet /vandispatch, so the two run side by side until
// the swap.
//
// It reuses the shared livemap core for the map, basemap (UVA GES vector +
// day/night + Esri satellite), theme, coord-copy and the vehicle layer (van
// markers only). Everything that makes this Van Dispatch rather than the Live
// Map — the Active Trips board, the Duty Roster, the van <-> card selection,
// and the map overlays (numbered pickup/drop-off markers + popups, the Spare
// service-area outline, PulsePoint pins + halo, the route polyline) — is
// vandispatch2's own code under apps/vandispatch2/, matching the Leaflet
// /vandispatch, NOT livemap's micro-trips / safety treatment.
// -----------------------------------------------------------------------------

import { createMap } from '../core/map.js';
import { initTheme, toggleTheme, getEffectiveTheme, onThemeChange } from '../core/theme.js';
import {
  installSatelliteLayer,
  isSatelliteVisible,
  setSatelliteVisible,
  onSatelliteChange,
} from '../core/layers/satellite.js';
import { installCoordCopy } from '../core/coord-copy.js';
import { installVehicleLayer } from '../core/layers/vehicles.js';
import { installBuildingHighlight } from '../core/layers/building-highlight.js';
import { setMicroEnabled } from '../core/data/microtransit.js';
import { SearchBox } from '../ui/search.js';
import { startVandispatchPanels } from './vandispatch2/index.js';

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

function showFatal(message) {
  const loading = document.getElementById('loadingOverlay');
  if (!loading) return;
  loading.classList.remove('is-hidden');
  loading.setAttribute('aria-busy', 'false');
  const text = loading.querySelector('.loading-overlay__text');
  if (text) text.textContent = message;
}

// Small inline stroke SVGs for the round header buttons — same visual language
// as the Leaflet /vandispatch page's svgIcon() helper.
function icon(name) {
  const body = {
    moon: '<path d="M13.2 9.4A5.6 5.6 0 1 1 6.6 2.8a4.6 4.6 0 0 0 6.6 6.6z"/>',
    sun:
      '<circle cx="8" cy="8" r="3"/><path d="M8 1.2v1.6M8 13.2v1.6M1.2 8h1.6M13.2 8h1.6M3.3 3.3l1.1 1.1M11.6 11.6l1.1 1.1M12.7 3.3l-1.1 1.1M4.4 11.6l-1.1 1.1"/>',
    // globe / satellite view
    globe:
      '<circle cx="8" cy="8" r="6"/><path d="M2 8h12M8 2c2 2 2 10 0 12M8 2c-2 2-2 10 0 12"/>',
  }[name] || '';
  return `<svg viewBox="0 0 16 16" width="1em" height="1em" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${body}</svg>`;
}

/** Wire the two round header buttons to the shared livemap theme / satellite
 *  modules. */
function wireControls() {
  const themeBtn = document.getElementById('theme-toggle');
  const syncTheme = () => {
    if (themeBtn) themeBtn.innerHTML = icon(getEffectiveTheme() === 'light' ? 'sun' : 'moon');
  };
  themeBtn?.addEventListener('click', () => toggleTheme());
  syncTheme();
  onThemeChange(syncTheme);

  const satBtn = document.getElementById('satellite-toggle');
  const syncSat = (on) => {
    if (!satBtn) return;
    satBtn.innerHTML = icon('globe');
    satBtn.setAttribute('aria-pressed', on ? 'true' : 'false');
  };
  satBtn?.addEventListener('click', () => setSatelliteVisible(!isSatelliteVisible()));
  onSatelliteChange(syncSat); // fires once immediately with the current value
}

async function boot() {
  document.documentElement.dataset.mode = 'vandispatch';
  const loading = document.getElementById('loadingOverlay');

  if (!hasWebGL2()) {
    console.warn('[vandispatch2] WebGL 2 unavailable — cannot render the vector map');
    showFatal('This browser can’t render the vector map (it needs WebGL 2). Use /vandispatch instead.');
    return;
  }

  let initialStyle;
  try {
    initialStyle = await initTheme();
  } catch (err) {
    console.error('[vandispatch2] failed to load the UVA basemap style', err);
    showFatal('Could not load the base map. Check the network and reload.');
    return;
  }

  let map;
  try {
    map = await createMap('map', initialStyle);
  } catch (err) {
    console.error('[vandispatch2] map init failed', err);
    showFatal('The map failed to start. Use /vandispatch instead.');
    return;
  }

  installSatelliteLayer();
  installCoordCopy();
  installBuildingHighlight(); // footprint highlight for the search box's building picks

  // Van markers come from livemap's shared vehicle layer, restricted to the
  // microtransit feed (no fixed-route buses, no CAT) and with the shared van
  // popup turned OFF — /vandispatch has no van popup, it uses selection.
  installVehicleLayer({ feeds: ['micro'], markerMenu: false });

  // This page is always about the vans, so force both microtransit sources on
  // (they feed the shared vehicle layer). Not persisted by the module.
  setMicroEnabled('ride', true);
  setMicroEnabled('flex', true);

  // Everything else — Active Trips board, Duty Roster, van<->card selection,
  // and the map overlays (numbered pickup/drop-off markers + their popups, the
  // Spare service-area outline, PulsePoint pins + halo, the route polyline) —
  // is vandispatch2's own, matching the Leaflet /vandispatch, NOT livemap's
  // micro-trips / safety treatment.
  startVandispatchPanels(map);

  // livemap's search box, reused as-is: one field over the live vehicle index +
  // UVA building/address search (/v1/uva/facility_search). Picking a van flies
  // to + follows it; picking a building frames + highlights the footprint.
  // Mounted into the map pane so it centres over the map, not the window.
  new SearchBox().mount(document.getElementById('map-pane'));

  wireControls();

  loading?.classList.add('is-hidden');
  loading?.setAttribute('aria-busy', 'false');

  // Handy during development.
  window.__vandispatch2 = { map };
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', boot, { once: true });
} else {
  boot();
}
