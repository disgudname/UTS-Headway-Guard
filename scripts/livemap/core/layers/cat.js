// livemap/core/layers/cat.js
// -----------------------------------------------------------------------------
// The CAT overlay layer controller: keeps the baked CAT route + stop sources
// fed from core/data/cat.js, toggles those layers with the overlay's enabled
// state, and owns the CAT stop popup (routed through the shared marker menu).
//
// CAT *vehicles* are NOT drawn here — they flow through core/layers/vehicles.js
// and get the exact same pin + route pill + speed pill + glide treatment as UTS
// buses (the two agencies read apart by route colour, the dashed route lines,
// and a "CAT" tag in the popup).
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { registerMarkerLayer } from '../marker-menu.js';
import {
  startCatFeed,
  isCatEnabled,
  onCatEnabled,
  onCatRoutes,
  onCatStops,
  onCatRouteVisibility,
  isCatRouteHidden,
  getCatRoutes,
  getCatStops,
  catRouteColor,
  fetchCatStopEtas,
} from '../data/cat.js';
import {
  CAT_ROUTE_SOURCE_ID,
  CAT_STOP_SOURCE_ID,
  CAT_STOP_LAYER,
  CAT_LAYER_IDS,
} from './cat-style.js';

let routes = [];
let stops = [];
let popup = null;
let popupKey = null;
let wired = false;

export function installCatLayer() {
  onStyleReady(onStyleRebuilt);

  onCatRoutes((list) => {
    routes = list;
    syncRoutes();
  });
  onCatStops((list) => {
    stops = list;
    syncStops();
  });
  onCatRouteVisibility(() => {
    syncRoutes();
    syncStops();
  });
  onCatEnabled(() => {
    applyVisibility();
    if (!isCatEnabled()) closePopup();
  });

  startCatFeed();

  // Seed from anything the feed already produced before we subscribed.
  const seedR = getCatRoutes();
  if (seedR.length) routes = seedR;
  const seedS = getCatStops();
  if (seedS.length) stops = seedS;
}

// --- style lifecycle --------------------------------------------------------

function onStyleRebuilt() {
  syncRoutes();
  syncStops();
  applyVisibility(); // setStyle resets layer visibility
  wireInteractions();
}

function applyVisibility() {
  const map = getMap();
  if (!map) return;
  const vis = isCatEnabled() ? 'visible' : 'none';
  for (const id of CAT_LAYER_IDS) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
  }
}

// --- sources --------------------------------------------------------------

function syncRoutes() {
  const src = getMap()?.getSource(CAT_ROUTE_SOURCE_ID);
  if (!src) return;
  src.setData({
    type: 'FeatureCollection',
    features: routes
      .slice()
      .sort((a, b) => a.id.localeCompare(b.id))
      .map((r) => ({
        type: 'Feature',
        geometry: { type: 'LineString', coordinates: r.coords },
        properties: {
          id: r.id,
          routeId: r.routeId,
          name: r.name,
          color: r.color,
          visible: isCatRouteHidden(r.routeId) ? 0 : 1,
        },
      })),
  });
}

/** Distinct colours for a stop's routes the CAT picker hasn't switched off. */
function visibleCatColors(stop) {
  const out = [];
  for (const rid of stop.routeIds) {
    if (isCatRouteHidden(rid)) continue;
    const c = catRouteColor(rid);
    if (!out.includes(c)) out.push(c);
  }
  return out;
}

function syncStops() {
  const src = getMap()?.getSource(CAT_STOP_SOURCE_ID);
  if (!src) return;
  const features = [];
  for (const s of stops) {
    const colors = visibleCatColors(s);
    if (!colors.length) continue; // every route here is switched off
    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [s.lng, s.lat] },
      properties: {
        key: s.key,
        name: s.name,
        stopId: s.stopId || '',
        color: colors[0],
        routeIds: s.routeIds.join(','),
      },
    });
  }
  src.setData({ type: 'FeatureCollection', features });
  if (popupKey) refreshStopPopup();
}

// --- interactions -------------------------------------------------------

function wireInteractions() {
  const map = getMap();
  if (!map || wired) return;
  wired = true;

  registerMarkerLayer({
    layer: CAT_STOP_LAYER,
    resolve: (f) => {
      if (!isCatEnabled()) return null;
      const s = stops.find((x) => x.key === f.properties.key);
      if (!s) return null;
      return {
        key: `catstop:${s.key}`,
        label: s.name,
        sublabel: 'CAT stop',
        color: catRouteColor(s.routeIds[0]),
        open: () => openStopPopup(s.key),
      };
    },
  });

  map.on('mouseenter', CAT_STOP_LAYER, () => {
    map.getCanvas().style.cursor = 'pointer';
  });
  map.on('mouseleave', CAT_STOP_LAYER, () => {
    map.getCanvas().style.cursor = '';
  });
}

// --- stop popup ------------------------------------------------------------

function openStopPopup(key) {
  const stop = stops.find((s) => s.key === key);
  if (!stop || !isCatEnabled()) return;
  closePopup();
  popupKey = key;
  popup = new maplibregl.Popup({
    offset: 12,
    closeButton: true,
    className: 'livemap-stop-popup livemap-cat-popup',
    maxWidth: '280px',
  })
    .setLngLat([stop.lng, stop.lat])
    .setHTML(stopPopupHTML(stop, null))
    .addTo(getMap());
  popup.on('close', clearPopupRef);

  fetchCatStopEtas(stop.stopId).then((etas) => {
    if (popup && popupKey === key) popup.setHTML(stopPopupHTML(stop, etas));
  });
}

function refreshStopPopup() {
  const stop = stops.find((s) => s.key === popupKey);
  if (!popup || !stop) return;
  fetchCatStopEtas(stop.stopId).then((etas) => {
    if (popup && popupKey === stop.key) popup.setHTML(stopPopupHTML(stop, etas));
  });
}

function closePopup() {
  if (popup) popup.remove();
  clearPopupRef();
}
function clearPopupRef() {
  popup = null;
  popupKey = null;
}

function stopPopupHTML(stop, etas) {
  let body;
  if (etas == null) {
    body = '<div class="ls-empty">Loading arrivals…</div>';
  } else if (!etas.length) {
    body = '<div class="ls-empty">No upcoming arrivals</div>';
  } else {
    body = etas
      .slice(0, 6)
      .map((e) => {
        const when =
          e.minutes == null
            ? escapeHTML(e.timeText || '—')
            : e.minutes <= 0
              ? 'Due'
              : `${e.minutes} min`;
        return `
      <div class="ls-row">
        <span class="ls-sw" style="background:${escapeAttr(e.color)}"></span>
        <span class="ls-route">${escapeHTML(e.routeName)}${
          e.direction ? ` <span class="ls-dir">${escapeHTML(e.direction)}</span>` : ''
        }</span>
        <span class="ls-eta">${when}</span>
      </div>`;
      })
      .join('');
  }
  return `
    <div class="ls-pop">
      <div class="ls-name">${escapeHTML(stop.name)} <span class="ls-tag">CAT</span></div>
      ${body}
    </div>`;
}

function escapeHTML(str) {
  return String(str).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[ch]);
}
function escapeAttr(str) {
  return escapeHTML(str);
}
