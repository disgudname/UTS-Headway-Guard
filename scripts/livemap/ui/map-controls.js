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
};

let userLocationMarker = null; // shared across mounts -- there's only ever one map

export class MapControls {
  mount(parent = document.body) {
    const el = document.createElement('div');
    el.className = 'map-ctrl-cluster';
    el.innerHTML = `
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
    return this;
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
