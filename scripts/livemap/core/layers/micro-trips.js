// livemap/core/layers/micro-trips.js
// -----------------------------------------------------------------------------
// The dispatcher-only microtransit trip overlay controller: the UVA FlexRide
// coverage polygon plus pickup / drop-off POINTS for live FlexRide requests and
// UVA Ride plan stops. No connecting lines — a van's ordered destination list
// lives in its popup (vandispatch-style). Feeds the two baked sources from
// core/data/microtransit.js, toggles them with the UVA Ride / FlexRide overlay,
// and owns the pickup/drop-off popup.
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { registerMarkerLayer } from '../marker-menu.js';
import { parseColor, luminance } from '../util.js';
import {
  startMicrotransitFeed,
  isMicroEnabled,
  onMicroEnabled,
  onMicroTrips,
  onMicroZone,
  getMicroTrips,
  getMicroZone,
} from '../data/microtransit.js';
import {
  MICRO_ZONE_SOURCE_ID,
  MICRO_TRIP_SOURCE_ID,
  MICRO_TRIP_PT_LAYER,
  MICRO_TRIP_LAYER_IDS,
} from './micro-trips-style.js';

let trips = [];
let zone = null;
let ptFeatures = []; // the Point features currently in the source (for popup lookup)
let popup = null;
let popupKey = null;
let wired = false;

export function installMicroTripsLayer() {
  onStyleReady(onRebuilt);
  onMicroTrips((list) => {
    trips = list;
    syncTrips();
  });
  onMicroZone((g) => {
    zone = g;
    syncZone();
  });
  onMicroEnabled(() => {
    applyVisibility();
    if (!isMicroEnabled()) closePopup();
  });
  startMicrotransitFeed(); // idempotent — vehicles.js may have started it already

  const t = getMicroTrips();
  if (t.length) trips = t;
  const z = getMicroZone();
  if (z) zone = z;
}

function onRebuilt() {
  syncZone();
  syncTrips();
  applyVisibility();
  wire();
}

function applyVisibility() {
  const map = getMap();
  if (!map) return;
  const vis = isMicroEnabled() ? 'visible' : 'none';
  for (const id of MICRO_TRIP_LAYER_IDS) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
  }
}

function syncZone() {
  const src = getMap()?.getSource(MICRO_ZONE_SOURCE_ID);
  if (!src) return;
  src.setData(
    zone
      ? { type: 'FeatureCollection', features: [{ type: 'Feature', geometry: zone, properties: {} }] }
      : { type: 'FeatureCollection', features: [] },
  );
}

function syncTrips() {
  const src = getMap()?.getSource(MICRO_TRIP_SOURCE_ID);
  if (!src) return;
  const pts = [];
  for (const t of trips) {
    if (t.pickup) pts.push(ptFeature(t, 'pickup', t.pickup));
    if (t.dropoff) pts.push(ptFeature(t, 'dropoff', t.dropoff));
  }
  ptFeatures = pts;
  src.setData({ type: 'FeatureCollection', features: pts });
  if (popupKey) refreshPopup();
}

function ptFeature(t, role, coord) {
  const seq = role === 'pickup' ? t.pickupSeq : t.dropoffSeq;
  const color = t.vanColor || '#7c3aed';
  return {
    type: 'Feature',
    geometry: { type: 'Point', coordinates: coord },
    properties: {
      key: `${t.id}:${role}`,
      role,
      tag: role === 'pickup' ? 'Pickup' : 'Drop-off',
      seq: seq ? String(seq) : '',
      color,
      // Readable seq digit whatever the van livery colour is (yellow/pale vans
      // washed out white-on-light with only the halo before this).
      textColor:
        luminance(parseColor(color) || { r: 124, g: 58, b: 237 }) > 0.6
          ? '#1b2130'
          : '#ffffff',
      rider: t.rider || '',
      addr: role === 'pickup' ? t.pickupAddr : t.dropoffAddr,
      eta: role === 'pickup' ? t.pickupEta : t.dropoffEta,
      status: t.status || '',
      src: t.source || '',
      veh: t.vehicleLabel || '',
      riders: t.numRiders || 1,
      access: (t.accessibility || []).join(', '),
    },
  };
}

// --- interactions --------------------------------------------------------

function wire() {
  const map = getMap();
  if (!map || wired) return;
  wired = true;

  registerMarkerLayer({
    layer: MICRO_TRIP_PT_LAYER,
    resolve: (f) => {
      if (!isMicroEnabled()) return null;
      const p = f.properties;
      return {
        key: `microtrip:${p.key}`,
        label: (p.seq ? `${p.seq}. ` : '') + p.tag + (p.rider ? ` · ${p.rider}` : ''),
        sublabel: p.src === 'spare' ? 'UVA FlexRide' : 'UVA Ride',
        color: p.color || '#7c3aed',
        open: () => openPopup(p.key, f.geometry.coordinates),
      };
    },
  });

  map.on('mouseenter', MICRO_TRIP_PT_LAYER, () => {
    map.getCanvas().style.cursor = 'pointer';
  });
  map.on('mouseleave', MICRO_TRIP_PT_LAYER, () => {
    map.getCanvas().style.cursor = '';
  });
}

// --- popup --------------------------------------------------------------

function findPt(key) {
  return ptFeatures.find((f) => f.properties.key === key) || null;
}

function openPopup(key, lngLat) {
  const f = findPt(key);
  if (!f || !isMicroEnabled()) return;
  closePopup();
  popupKey = key;
  popup = new maplibregl.Popup({
    offset: 12,
    closeButton: true,
    className: 'livemap-stop-popup livemap-cat-popup',
    maxWidth: '260px',
  })
    .setLngLat(lngLat || f.geometry.coordinates)
    .setHTML(popupHTML(f.properties))
    .addTo(getMap());
  popup.on('close', () => {
    popup = null;
    popupKey = null;
  });
}

function refreshPopup() {
  const f = findPt(popupKey);
  if (!popup || !f) return;
  popup.setHTML(popupHTML(f.properties));
}

function closePopup() {
  if (popup) popup.remove();
  popup = null;
  popupKey = null;
}

function popupHTML(p) {
  const tag = p.src === 'spare' ? 'UVA FLEXRIDE' : 'UVA RIDE';
  const rows = [];
  if (p.rider) rows.push(row('Rider', p.rider));
  if (Number(p.riders) > 1) rows.push(row('Party', `${p.riders}`));
  if (p.status) rows.push(row('Status', titleCase(p.status)));
  if (p.eta) rows.push(row(p.role === 'pickup' ? 'Pickup ETA' : 'Drop-off ETA', p.eta));
  if (p.veh) rows.push(row('Duty', p.veh));
  if (p.access) rows.push(row('Access', p.access));
  const addr = p.addr ? `<div class="ls-empty">${esc(p.addr)}</div>` : '';
  return `
    <div class="ls-pop">
      <div class="ls-name">${esc(p.tag)} <span class="ls-tag">${tag}</span></div>
      ${addr}
      ${rows.join('')}
    </div>`;
}

function row(k, v) {
  return `
    <div class="ls-row">
      <span class="ls-route">${esc(k)}</span>
      <span class="ls-eta">${esc(v)}</span>
    </div>`;
}

function titleCase(s) {
  return String(s)
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, (c) => c.toUpperCase())
    .trim();
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
