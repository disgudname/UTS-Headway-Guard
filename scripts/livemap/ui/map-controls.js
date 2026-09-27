// livemap/ui/map-controls.js
// -----------------------------------------------------------------------------
// The bottom-right control cluster: zoom in/out, "my location" (recenter + a
// persistent blue dot marker once known), the trip planner's own toggle button
// (relocated here from its old standalone bottom-centre pill -- see boot.js
// passing `mapControls.tripPlannerSlot` as TripPlannerPanel's mount parent),
// and a contextual "Navigate here" button that search.js shows/hides once a
// building/address is selected.
//
// Replaces MapLibre's own NavigationControl (which the right panel's route
// list could grow tall enough to sit on top of -- confirmed live, "zoom
// controls are hidden under the right panel") with custom buttons in one
// cohesive bar. The right panel's own max-height is capped in livemap.css to
// leave this bar permanently clear, rather than having this bar chase the
// panel's real (content-dependent) height with JS measurement.
// -----------------------------------------------------------------------------

import { getMap } from '../core/map.js';

const ICONS = {
  zoomIn:
    '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M8 2v12M2 8h12"/></svg>',
  zoomOut:
    '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M2 8h12"/></svg>',
  locate:
    '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><circle cx="8" cy="8" r="2.3"/><path d="M8 1v2.6M8 12.4V15M1 8h2.6M12.4 8H15"/></svg>',
  navigate:
    '<svg viewBox="0 0 16 16" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"><path d="M8 1.4 2.7 14.3l5.3-3 5.3 3z"/></svg>',
  install:
    '<svg viewBox="0 0 16 16" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 1.8v8.4M4.6 7l3.4 3.4L11.4 7M2.5 11.5v1.6c0 .6.5 1.1 1.1 1.1h8.8c.6 0 1.1-.5 1.1-1.1v-1.6"/></svg>',
  share:
    '<svg viewBox="0 0 16 16" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px"><path d="M8 1.5v8.5M5.2 4.3 8 1.5l2.8 2.8M5 6.5H3.8v8h8.4v-8H11"/></svg>',
};

/** The service-alert bell (scripts/push-notifications.js) is a classic script
 *  that seats itself in [data-push-bell-slot] if one exists when it runs, so it
 *  has to load after the cluster is in the DOM. It hides itself when the browser
 *  has no push support or the server has no VAPID keys. */
function installPushBell() {
  if (document.getElementById('uts-push-bell') || document.querySelector('script[data-push-bell]')) return;
  const s = document.createElement('script');
  s.src = '/scripts/push-notifications.js';
  s.dataset.pushBell = '';
  s.async = true;
  document.head.appendChild(s);
}

/** Already running as the installed app (Android/desktop standalone, or an
 *  iOS Home Screen web app)? */
function isInstalledApp() {
  return window.matchMedia?.('(display-mode: standalone)').matches || navigator.standalone === true;
}

/** iOS has no install prompt API: "Add to Home Screen" is only in the Share
 *  sheet, so the button shows instructions instead. iPadOS reports as a Mac. */
function isIOS() {
  return /iPad|iPhone|iPod/.test(navigator.userAgent) ||
    (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
}

let userLocationMarker = null; // shared across mounts -- there's only ever one map

export class MapControls {
  mount(parent = document.body) {
    const el = document.createElement('div');
    el.className = 'map-ctrl-cluster';
    el.innerHTML = `
      <button type="button" class="map-ctrl-btn map-ctrl-install" title="Install app" aria-label="Install app" hidden>${ICONS.install}</button>
      <span class="map-ctrl-push-slot" data-push-bell-slot></span>
      <button type="button" class="map-ctrl-btn map-ctrl-navhere" aria-label="Navigate here" hidden>
        ${ICONS.navigate}<span class="map-ctrl-label">Navigate here</span>
      </button>
      <span class="map-ctrl-tp-slot"></span>
      <button type="button" class="map-ctrl-btn map-ctrl-locate" title="My location" aria-label="My location">${ICONS.locate}</button>
      <span class="map-ctrl-zoom">
        <button type="button" class="map-ctrl-btn map-ctrl-zoom-out" title="Zoom out" aria-label="Zoom out">${ICONS.zoomOut}</button>
        <button type="button" class="map-ctrl-btn map-ctrl-zoom-in" title="Zoom in" aria-label="Zoom in">${ICONS.zoomIn}</button>
      </span>`;

    this._el = el;
    this._tpSlot = el.querySelector('.map-ctrl-tp-slot');
    this._navHereBtn = el.querySelector('.map-ctrl-navhere');

    el.querySelector('.map-ctrl-zoom-in').addEventListener('click', () => getMap()?.zoomIn());
    el.querySelector('.map-ctrl-zoom-out').addEventListener('click', () => getMap()?.zoomOut());
    el.querySelector('.map-ctrl-locate').addEventListener('click', () => this._locate());

    (parent || document.body).appendChild(el);
    installPushBell();
    this._setupInstall();
    return this;
  }

  /** "Install app": Chrome/Edge/Android get the browser's own install prompt
   *  (captured early in livemap.html as window.__lmInstallPrompt); iOS gets a
   *  "Share -> Add to Home Screen" hint. Hidden when already installed or when
   *  /livemap is iframed (dispatcher embed). */
  _setupInstall() {
    const btn = this._el.querySelector('.map-ctrl-install');
    if (isInstalledApp() || window.self !== window.top) return;

    const showIfPromptable = () => { if (window.__lmInstallPrompt) btn.hidden = false; };
    if (isIOS()) {
      btn.hidden = false;
    } else {
      showIfPromptable();
      window.addEventListener('lm-installable', showIfPromptable);
    }
    window.addEventListener('appinstalled', () => {
      btn.hidden = true;
      window.__lmInstallPrompt = null;
    });

    btn.addEventListener('click', async () => {
      const prompt = window.__lmInstallPrompt;
      if (prompt) {
        prompt.prompt();
        const { outcome } = await prompt.userChoice;
        // A prompt event can only be used once; Chrome fires a fresh one later
        // if the user dismissed it.
        window.__lmInstallPrompt = null;
        if (outcome === 'accepted') btn.hidden = true;
        else showIfPromptable();
        return;
      }
      if (isIOS()) this._toggleIOSInstallHint();
    });
  }

  _toggleIOSInstallHint() {
    let hint = this._el.querySelector('.map-ctrl-install-hint');
    if (hint) {
      hint.remove();
      return;
    }
    hint = document.createElement('div');
    hint.className = 'map-ctrl-install-hint';
    hint.setAttribute('role', 'dialog');
    hint.setAttribute('aria-label', 'Install app');
    hint.innerHTML = `
      <button type="button" class="map-ctrl-install-hint-close" aria-label="Close">&times;</button>
      <div class="map-ctrl-install-hint-title">Install UVATransit Map</div>
      <ol>
        <li>Tap the Share button ${ICONS.share} in Safari's toolbar</li>
        <li>Choose <strong>Add to Home Screen</strong></li>
        <li>Open the map from your Home Screen, then tap the bell for service alerts</li>
      </ol>`;
    hint.querySelector('.map-ctrl-install-hint-close').addEventListener('click', () => hint.remove());
    this._el.appendChild(hint);
  }

  /** Where TripPlannerPanel should mount its own toggle button so it renders as
   *  part of this same bar instead of its old separate floating pill -- pass
   *  this as the `parent` argument to `new TripPlannerPanel().mount(...)`. */
  get tripPlannerSlot() {
    return this._tpSlot;
  }

  _locate() {
    if (!navigator.geolocation) return;
    const map = getMap();
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        const { latitude, longitude } = pos.coords;
        this._showUserLocation(latitude, longitude);
        if (map) {
          map.flyTo({ center: [longitude, latitude], zoom: Math.max(map.getZoom(), 16), duration: 850 });
        }
      },
      () => {
        /* denied/unavailable -- silently no-op, same graceful-degrade as the
           trip planner's own "use my location" field button. */
      },
      { enableHighAccuracy: true, timeout: 8000 },
    );
  }

  _showUserLocation(lat, lng) {
    const map = getMap();
    if (!map) return;
    if (!userLocationMarker) {
      const dot = document.createElement('div');
      dot.className = 'map-ctrl-userdot';
      dot.innerHTML = '<span class="map-ctrl-userdot-pulse"></span><span class="map-ctrl-userdot-core"></span>';
      userLocationMarker = new maplibregl.Marker({ element: dot, anchor: 'center' });
    }
    userLocationMarker.setLngLat([lng, lat]).addTo(map);
  }

  /** Show/hide the contextual "Navigate here" button. Pass a click handler to
   *  show it (the handler closes over whatever point it should navigate to);
   *  pass nothing to hide it. Called by search.js once a building/address
   *  becomes the current selection, or is cleared. */
  setNavigateHere(onClick) {
    if (!onClick) {
      this._navHereBtn.hidden = true;
      this._navHereBtn.onclick = null;
      return;
    }
    this._navHereBtn.hidden = false;
    this._navHereBtn.onclick = onClick;
  }
}
