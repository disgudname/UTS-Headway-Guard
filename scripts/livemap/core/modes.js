// livemap/core/modes.js
// -----------------------------------------------------------------------------
// The page's *operator mode* — one shared decision the rest of livemap reads to
// know how much UI to show and whether the map is a hands-on tool or a hands-off
// display. Mirrors the legacy testmap's kioskMode / adminKioskMode / adminMode
// switches, collapsed into a single place.
//
//   'public'      full interactive UI (the default; /livemap).
//   'kiosk'       public-facing display: no chrome, locked camera, no marker
//                 menu or popups, solar (civil-twilight) day/night, a large
//                 centred status line when the bus feed has nothing to show.
//   'adminKiosk'  same hands-off framing as 'kiosk' but keeps dispatcher
//                 overlays and lets you click a marker (a back-office wall
//                 display). Needs the dispatcher cookie for the overlays.
//   'embed'       stripped chrome for an <iframe>; camera stays interactive
//                 unless ?lock is set.
//
// Set it three ways (first wins):
//   1. configureMode('kiosk')  — a shell entry point calls this before boot.
//   2. ?kiosk / ?kiosk=admin / ?adminKiosk / ?embed  — URL params.
//   3. nothing -> 'public'.
//
// Individual behaviours can still be nudged per-load:
//   ?ui=none|full     force the chrome off / on
//   ?lock  / ?lock=0  force the camera lock on / off
//   ?solar / ?solar=0 force solar day/night on / off
//   ?adminMode=false  drop dispatcher-only presentation even with the cookie
// -----------------------------------------------------------------------------

import { param, paramBool } from './util.js';

const MODES = new Set(['public', 'kiosk', 'adminKiosk', 'embed']);

let forced = null; // set by configureMode(), wins over the URL

/** A shell entry point (apps/kiosk.js, apps/embed.js) calls this before boot. */
export function configureMode(name) {
  if (!MODES.has(name)) {
    console.warn(`[livemap] ignoring unknown operator mode "${name}"`);
    return;
  }
  forced = name;
}

/** 'public' | 'kiosk' | 'adminKiosk' | 'embed' */
export function getOperatorMode() {
  if (forced) return forced;
  const kiosk = param('kiosk', null);
  if (kiosk === 'admin' || paramBool('adminKiosk')) return 'adminKiosk';
  if (kiosk != null && kiosk !== 'false' && kiosk !== '0') return 'kiosk';
  if (paramBool('embed')) return 'embed';
  return 'public';
}

export const isKioskLike = () => {
  const m = getOperatorMode();
  return m === 'kiosk' || m === 'adminKiosk';
};

/** Hide the control panels, route selector, search box and credit line. */
export function isChromeHidden() {
  const ui = param('ui', null);
  if (ui === 'none') return true;
  if (ui === 'full') return false;
  const m = getOperatorMode();
  return m === 'kiosk' || m === 'adminKiosk' || m === 'embed';
}

/** Disable pan/zoom/rotate and drop the navigation control. */
export function isInteractionLocked() {
  if (param('lock', null) != null) return paramBool('lock');
  return isKioskLike();
}

/** Swallow the radial marker menu and marker popups entirely. */
export function isMenuSuppressed() {
  // adminKiosk keeps click-to-inspect; a plain kiosk is look-don't-touch.
  return getOperatorMode() === 'kiosk';
}

/** In 'auto' theme, decide day/night from the sun at Grounds, not the OS. */
export function useSolarTheme() {
  if (param('solar', null) != null) return paramBool('solar');
  return isKioskLike();
}

/** Whether dispatcher-only presentation (block pills, OOS buses, …) may show.
 *  The cookie still has to be present — this only vetoes it. A plain kiosk is
 *  always the public view (matches the legacy testmap); an adminKiosk keeps the
 *  overlays; anywhere else, `?adminMode=false` forces the public view. */
export function dispatcherOverlaysAllowed() {
  if (getOperatorMode() === 'kiosk') return false;
  return param('adminMode', null) !== 'false';
}
