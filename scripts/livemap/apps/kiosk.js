// livemap/apps/kiosk.js
// -----------------------------------------------------------------------------
// Kiosk shell — a hands-off public display (lobby screen, platform sign).
// Served at /livemap/kiosk. Forces the 'kiosk' operator mode: no panels, no
// search, a locked camera, no marker menu or popups, solar (civil-twilight)
// day/night, and the large "no bus data" status card when the feed is empty.
//
// `?adminKiosk` (or `?kiosk=admin`) upgrades it to 'adminKiosk' — the
// back-office / dispatcher wall display: same hands-off framing but keeps the
// dispatcher overlays and lets you click a marker. Needs the dispatch cookie
// on that machine.
//
// Still tweakable per-screen via the URL: ?theme=light|dark pins the theme,
// ?centerLat=&centerLon=&centerZoom= sets the framing, ?ui=full brings the
// panels back for setup.
// -----------------------------------------------------------------------------

import { configureMode } from '../core/modes.js';
import { param, paramBool } from '../core/util.js';
import { startLivemap } from './boot.js';

const admin = paramBool('adminKiosk') || param('kiosk', null) === 'admin';
configureMode(admin ? 'adminKiosk' : 'kiosk');
startLivemap();
