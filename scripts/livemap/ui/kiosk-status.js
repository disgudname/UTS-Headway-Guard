// livemap/ui/kiosk-status.js
// -----------------------------------------------------------------------------
// The full-screen status overlay a kiosk shows when the map would otherwise
// look broken to a passer-by. Ported to match the legacy testmap kiosk exactly:
//
//   * nothing until the feed has answered once ("known") — a kiosk that just
//     booted doesn't flash "No active vehicles" before the first poll lands;
//   * feed reachable, zero vehicles anywhere (UTS + CAT + microtransit) ->
//     "No active vehicles";
//   * feed unreachable -> "network" copy if the browser reports offline,
//     otherwise the "TransLoc is failing, the dashboard is fine" copy.
//
// While buses are moving it stays fully hidden.
// -----------------------------------------------------------------------------

import { onStatus, onVehicles } from '../core/data/transloc.js';
import { onCatVehicles } from '../core/data/cat.js';
import { onMicroVehicles } from '../core/data/microtransit.js';

// Copy is verbatim from testmap's KIOSK_* constants.
const NO_VEHICLES_TEXT = 'No active vehicles';
const NETWORK_ERROR_TEXT =
  'Unable to connect to the network.\nPlease check your internet connection.';
const SERVICE_ERROR_TEXT =
  'TransLoc is currently failing to provide bus data.\nThe Operations Dashboard is functioning normally.';

export class KioskStatus {
  mount() {
    this._el = document.createElement('div');
    this._el.className = 'livemap-kiosk-status';
    this._el.setAttribute('role', 'status');
    this._el.setAttribute('aria-live', 'polite');
    this._el.setAttribute('aria-hidden', 'true');
    document.body.appendChild(this._el);

    this._feed = 'polling';
    this._known = false;
    this._uts = 0;
    this._cat = 0;
    this._micro = 0;

    this._offStatus = onStatus((s) => {
      this._feed = s;
      this._render();
    });
    this._offVehicles = onVehicles((list) => {
      this._known = true; // the feed has answered at least once
      this._uts = countActive(list);
      this._render();
    });
    this._offCat = onCatVehicles((list) => {
      this._cat = Array.isArray(list) ? list.length : 0;
      this._render();
    });
    this._offMicro = onMicroVehicles((list) => {
      this._micro = Array.isArray(list) ? list.length : 0;
      this._render();
    });
    this._render();
    return this;
  }

  unmount() {
    this._offStatus?.();
    this._offVehicles?.();
    this._offCat?.();
    this._offMicro?.();
    this._el?.remove();
  }

  _render() {
    if (!this._el) return;
    const text = this._text();
    if (text == null) {
      this._el.classList.remove('is-visible');
      this._el.setAttribute('aria-hidden', 'true');
      return;
    }
    if (this._el.textContent !== text) this._el.textContent = text;
    this._el.classList.add('is-visible');
    this._el.setAttribute('aria-hidden', 'false');
  }

  /** The overlay text, or null to stay hidden. */
  _text() {
    if (this._feed === 'down') {
      const offline = typeof navigator !== 'undefined' && navigator.onLine === false;
      return offline ? NETWORK_ERROR_TEXT : SERVICE_ERROR_TEXT;
    }
    if (!this._known) return null;
    const anyVehicles = this._uts > 0 || this._cat > 0 || this._micro > 0;
    return anyVehicles ? null : NO_VEHICLES_TEXT;
  }
}

function countActive(list) {
  let n = 0;
  for (const v of Array.isArray(list) ? list : []) {
    if (v && v.routeId && v.routeId !== '0' && !v.stale) n++;
  }
  return n;
}
