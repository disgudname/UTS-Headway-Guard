// livemap/apps/live.js
// -----------------------------------------------------------------------------
// Entry point for the full interactive Live Map (served at /livemap).
//
// No mode is forced here — core/modes.js reads it from the URL, so this same
// page also serves ?kiosk / ?adminKiosk / ?embed / ?lock / ?solar for quick
// testing without a dedicated shell. The real boot sequence lives in boot.js,
// shared with apps/kiosk.js and apps/embed.js.
// -----------------------------------------------------------------------------

import { startLivemap } from './boot.js';

startLivemap();
