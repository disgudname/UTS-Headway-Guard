// livemap/apps/kiosk.js
// -----------------------------------------------------------------------------
// Kiosk shell — a hands-off public display (lobby screen, platform sign).
// Served at /livemap/kiosk. Forces the 'kiosk' operator mode: no panels, no
// search, a locked camera, no marker menu or popups, solar (civil-twilight)
// day/night, and the large "no bus data" status card when the feed is empty.
//
// Still tweakable per-screen via the URL: ?theme=light|dark pins the theme,
// ?centerLat=&centerLon=&centerZoom= sets the framing, ?adminKiosk keeps
// dispatcher overlays, ?ui=full brings the panels back for setup.
// -----------------------------------------------------------------------------

import { configureMode } from '../core/modes.js';
import { startLivemap } from './boot.js';

configureMode('kiosk');
startLivemap();
