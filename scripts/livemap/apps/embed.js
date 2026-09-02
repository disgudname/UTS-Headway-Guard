// livemap/apps/embed.js
// -----------------------------------------------------------------------------
// Embed shell — a stripped map for an <iframe> on another page. Served at
// /livemap/embed. Forces the 'embed' operator mode: no panels, no search, no
// credit line. The camera stays interactive (pan/zoom) unless ?lock is set.
//
// Common query knobs: ?bus=1234 follows a vehicle, ?centerLat=&centerLon=
// &centerZoom= sets the framing, ?theme=light|dark pins the theme, ?lock
// freezes the camera, ?ui=full brings the panels back.
// -----------------------------------------------------------------------------

import { configureMode } from '../core/modes.js';
import { startLivemap } from './boot.js';

configureMode('embed');
startLivemap();
