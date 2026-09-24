// livemap/core/layers/overheight.js
// -----------------------------------------------------------------------------
// Over-height bridge alert — parity with the legacy testmap's dispatcher mode.
//
// When a bus on the over-height list comes within the bridge's trigger radius:
//   * the map flies to the bridge (zoom >= 18) and its gestures are locked so
//     nobody can pan away from the warning,
//   * a red disc marks the radius (baked layers, see overheight-style.js),
//   * an "OVERHEIGHT VEHICLE" popup rides on the nearest such bus.
// When no over-height bus is in range any more, gestures come back and the map
// returns to where it was.
//
// Safety-critical, so it fires for EVERY authenticated (dispatcher-cookie) user,
// plus `?dispatcher=true` (the bus dispatch console's map iframe) and adminKiosk
// wall displays (testmap's original gates). `?adminMode=false` does not veto it.
// The public never sees it. Bridge location, radius and the bus list come from
// GET /v1/config (BRIDGE_LAT/LON/RADIUS, OVERHEIGHT_BUSES), falling back to the
// same defaults testmap ships.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { getMap, onStyleReady } from '../map.js';
import { paramBool } from '../util.js';
import { getOperatorMode } from '../modes.js';
import { isAuthed, startSession } from '../data/session.js';
import { onVehicles } from '../data/transloc.js';
import { listVehicles, getVehicleLngLat, stopFollow } from './vehicles.js';
import { OVERHEIGHT_SOURCE_ID } from './overheight-style.js';

const DEFAULTS = Object.freeze({
  lat: 38.03404931117353,
  lng: -78.4995922309842,
  radiusM: 117,
  busIds: ['25131', '25231', '25331', '25431'],
});
const MIN_FOCUS_ZOOM = 18;
// Gesture handlers we lock. `.isEnabled()` is captured first so a handler that
// was already off (a locked kiosk) is not switched back on at release.
const HANDLERS = [
  'dragPan', 'scrollZoom', 'boxZoom', 'dragRotate',
  'keyboard', 'doubleClickZoom', 'touchZoomRotate', 'touchPitch',
];

let config = null; // { lat, lng, radiusM, busIds:Set<string> }
let active = false;
let vehicleKey = null; // state key of the bus currently flagged
let popup = null;
let rafId = 0;
let saved = null; // { center, zoom, handlers:{name:bool} }

export const overheightAllowed = () =>
  isAuthed() || paramBool('dispatcher') || getOperatorMode() === 'adminKiosk';

let configRequested = false;

export function installOverheightAlert() {
  startSession(); // idempotent; makes sure the auth probe is running
  // Registered after installVehicleLayer's own listener, so by the time this
  // runs the vehicle state already reflects this report.
  onVehicles(() => evaluate());
  onStyleReady(() => syncCircle()); // a theme swap rebuilds the style, wiping the source
}

async function loadConfig() {
  const out = {
    lat: DEFAULTS.lat,
    lng: DEFAULTS.lng,
    radiusM: DEFAULTS.radiusM,
    busIds: new Set(DEFAULTS.busIds),
  };
  try {
    const r = await fetch(`${API_BASE}/v1/config`, { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    const lat = Number(d.BRIDGE_LAT);
    const lng = Number(d.BRIDGE_LON);
    const rad = Number(d.BRIDGE_RADIUS);
    if (Number.isFinite(lat)) out.lat = lat;
    if (Number.isFinite(lng)) out.lng = lng;
    if (Number.isFinite(rad) && rad > 0) out.radiusM = rad;
    if (Array.isArray(d.OVERHEIGHT_BUSES) && d.OVERHEIGHT_BUSES.length) {
      out.busIds = new Set(d.OVERHEIGHT_BUSES.map((x) => String(x).trim()).filter(Boolean));
    }
  } catch (err) {
    console.warn('[livemap] over-height config failed, using defaults', err);
  }
  return out;
}

function distanceM(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLng = (lng2 - lng1) * rad;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

/** The nearest over-height bus inside the radius, or null. Reads the vehicle
 *  layer's own state, so a bus the route picker hides is never flagged. */
function findCandidate() {
  if (!config) return null;
  let best = null;
  for (const v of listVehicles()) {
    if (v.agency !== 'uts') continue;
    const rawId = String(v.id).replace(/^[a-z]+:/, '');
    if (!config.busIds.has(rawId) && !config.busIds.has(String(v.label).trim())) continue;
    const d = distanceM(v.lat, v.lng, config.lat, config.lng);
    if (!Number.isFinite(d) || d > config.radiusM) continue;
    if (!best || d < best.d) best = { key: v.id, label: v.label, block: v.block, d };
  }
  return best;
}

function evaluate() {
  const map = getMap();
  if (!map) return;
  if (!overheightAllowed()) {
    if (active) release(); // signed out mid-alert
    return;
  }
  if (!configRequested) {
    configRequested = true; // first time we're allowed (auth probe may land late)
    loadConfig().then((c) => {
      config = c;
      evaluate();
    });
  }
  if (!config) return;
  const c = findCandidate();
  if (!c) {
    if (active) release();
    return;
  }
  engage(map, c);
}

function engage(map, c) {
  if (!active) {
    saved = {
      center: map.getCenter(),
      zoom: map.getZoom(),
      handlers: Object.fromEntries(HANDLERS.map((h) => [h, !!map[h]?.isEnabled?.()])),
    };
    stopFollow(); // a followed bus's per-frame recentre would fight the lock
    for (const h of HANDLERS) map[h]?.disable?.();
    active = true;
    map.easeTo({
      center: [config.lng, config.lat],
      zoom: Math.min(Math.max(map.getZoom(), MIN_FOCUS_ZOOM), map.getMaxZoom()),
      duration: 600,
    });
    syncCircle();
    loop();
  }
  vehicleKey = c.key;
  ensurePopup(map, c);
}

function release() {
  const map = getMap();
  active = false;
  vehicleKey = null;
  if (rafId) cancelAnimationFrame(rafId);
  rafId = 0;
  if (popup) popup.remove();
  popup = null;
  syncCircle();
  if (map && saved) {
    for (const h of HANDLERS) if (saved.handlers[h]) map[h]?.enable?.();
    map.easeTo({ center: saved.center, zoom: saved.zoom, duration: 600 });
  }
  saved = null;
}

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (ch) => ESC[ch]);

function popupHTML(c) {
  const block = String(c.block || '').trim();
  const blockText = block && !block.toLowerCase().startsWith('block ') ? `Block ${block}` : block;
  return (
    `<div class="livemap-overheight__vehicle">${esc(c.label || `Vehicle ${c.key}`)}</div>` +
    (blockText ? `<div class="livemap-overheight__block">${esc(blockText)}</div>` : '') +
    '<div class="livemap-overheight__msg">OVERHEIGHT VEHICLE</div>'
  );
}

function ensurePopup(map, c) {
  const pos = getVehicleLngLat(c.key);
  if (!pos) return;
  if (!popup) {
    popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      closeOnMove: false,
      anchor: 'bottom',
      offset: 24,
      className: 'livemap-overheight-popup',
      maxWidth: '260px',
    }).addTo(map);
  }
  popup.setLngLat(pos).setHTML(popupHTML(c));
}

/** Keep the popup glued to the (easing) bus marker between feed reports. */
function loop() {
  rafId = requestAnimationFrame(() => {
    rafId = 0;
    if (!active) return;
    if (popup && vehicleKey) {
      const pos = getVehicleLngLat(vehicleKey);
      if (pos) popup.setLngLat(pos);
    }
    loop();
  });
}

function circleFeature(lat, lng, radiusM) {
  const pts = [];
  const N = 72;
  const dLat = radiusM / 111320;
  const dLng = radiusM / (111320 * Math.cos((lat * Math.PI) / 180));
  for (let i = 0; i <= N; i++) {
    const a = (i / N) * 2 * Math.PI;
    pts.push([lng + dLng * Math.cos(a), lat + dLat * Math.sin(a)]);
  }
  return { type: 'Feature', properties: {}, geometry: { type: 'Polygon', coordinates: [pts] } };
}

function syncCircle() {
  const map = getMap();
  const src = map && map.getSource(OVERHEIGHT_SOURCE_ID);
  if (!src) return;
  const features = active && config ? [circleFeature(config.lat, config.lng, config.radiusM)] : [];
  src.setData({ type: 'FeatureCollection', features });
}
