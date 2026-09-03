// livemap/core/layers/safety.js
// -----------------------------------------------------------------------------
// "Traffic & Incidents" overlay controller: keeps the PulsePoint + TomTom
// incident sources fed, toggles the three layers (traffic flow raster, traffic
// incident lines, PulsePoint dots) independently with their panel checkboxes,
// and owns the PulsePoint / incident popups.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { getMap, onStyleReady } from '../map.js';
import { registerMarkerLayer } from '../marker-menu.js';
import {
  startSafetyFeed,
  isSafetyOn,
  onSafety,
  onPulsePoint,
  onTrafficInc,
  getPulsePoint,
  getTrafficInc,
} from '../data/safety.js';
import {
  TRAFFIC_FLOW_LAYER,
  TRAFFIC_INC_SOURCE_ID,
  TRAFFIC_INC_CASING_LAYER,
  TRAFFIC_INC_LINE_LAYER,
  PULSEPOINT_SOURCE_ID,
  PULSEPOINT_DOT_LAYER,
  PULSEPOINT_FALLBACK_IMAGE,
} from './safety-style.js';

// PulsePoint respond-icon PNGs (the real /v1/pulsepoint/respond_icons/*.png
// teardrops testmap uses). We load them EAGERLY as incident data arrives rather
// than leaning on `styleimagemissing` — the layer's icon-image wraps names in
// `['image', …]` so a missing one silently resolves to null instead of firing
// that event. `ppDone` = codes we've loaded or that 404'd; cleared on style
// rebuild since a theme swap wipes the image registry.
const PP_ICON_BASE = `${API_BASE}/v1/pulsepoint/respond_icons/`;
const ppDone = new Set();
let lastPulseFC = null;

let pulse = [];
let inc = [];
let popup = null;
let popupKey = null;
let wired = false;

export function installSafetyLayer() {
  onStyleReady(onRebuilt);
  onPulsePoint((list) => {
    pulse = list;
    syncPulse();
    applyVis(); // near-route / service-area incidents auto-show — track that here
  });
  onTrafficInc((list) => {
    inc = list;
    syncInc();
  });
  onSafety('pulsepoint', () => applyVis());
  onSafety('trafficInc', () => applyVis());
  onSafety('trafficFlow', () => applyVis());
  startSafetyFeed();

  const p = getPulsePoint();
  if (p.length) pulse = p;
  const i = getTrafficInc();
  if (i.length) inc = i;
}

function onRebuilt() {
  ppDone.clear(); // style rebuild wiped the image atlas
  ensureFallbackPin();
  syncPulse();
  syncInc();
  applyVis();
  wire();
}

/** Load one PulsePoint respond-icon PNG into the map's image registry, keyed
 *  `pp-<code>`. On success, re-push the last incident FeatureCollection so the
 *  symbol layer re-lays-out with the now-present image. Codes with no icon
 *  (the endpoint returns an HTML fallback, not a PNG) are marked done so we
 *  don't retry — those features fall back to the generated pin. */
function ensurePpIcon(code) {
  const map = getMap();
  const id = `pp-${code}`;
  if (!map || !code || ppDone.has(id) || map.hasImage(id)) return;
  if (!/^[a-z0-9]{1,6}$/.test(code)) {
    ppDone.add(id);
    return;
  }
  ppDone.add(id);
  map
    .loadImage(`${PP_ICON_BASE}${code}_map_active.png`)
    .then((res) => {
      const data = res && res.data;
      if (!data || map.hasImage(id)) return;
      map.addImage(id, data);
      if (lastPulseFC && map.getSource(PULSEPOINT_SOURCE_ID)) {
        map.getSource(PULSEPOINT_SOURCE_ID).setData(lastPulseFC);
      }
    })
    .catch(() => {
      /* no PNG for this code — the layer's coalesce uses the fallback pin */
    });
}

/** A neutral teardrop pin, generated once, for incident type codes with no
 *  PulsePoint respond icon. Drawn at the same ~180 px source scale as the real
 *  respond-icon PNGs (pixelRatio 1) so it sizes identically under `icon-size`. */
function ensureFallbackPin() {
  const map = getMap();
  if (!map || map.hasImage(PULSEPOINT_FALLBACK_IMAGE)) return;
  const w = 168;
  const h = 200;
  const r = 70;
  const cx = w / 2;
  const cy = r + 10;
  const cv = document.createElement('canvas');
  cv.width = w;
  cv.height = h;
  const g = cv.getContext('2d');
  g.beginPath();
  g.arc(cx, cy, r, Math.PI * 0.86, Math.PI * 0.14, false);
  g.lineTo(cx, h - 8); // taper to the tip
  g.closePath();
  g.fillStyle = '#6b7280';
  g.fill();
  g.lineWidth = 12;
  g.strokeStyle = '#ffffff';
  g.stroke();
  g.beginPath();
  g.arc(cx, cy, r * 0.42, 0, Math.PI * 2);
  g.fillStyle = '#ffffff';
  g.fill();
  try {
    map.addImage(PULSEPOINT_FALLBACK_IMAGE, g.getImageData(0, 0, w, h));
  } catch (err) {
    /* already added in a race — fine */
  }
}

function applyVis() {
  const map = getMap();
  if (!map) return;
  const set = (id, on) => {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', on ? 'visible' : 'none');
  };
  // PulsePoint shows whenever there's anything to show — the near-route /
  // FlexRide-zone set auto-shows; the toggle only widens it to "everything".
  const ppOn = isSafetyOn('pulsepoint') || pulse.length > 0;
  set(TRAFFIC_FLOW_LAYER, isSafetyOn('trafficFlow'));
  set(TRAFFIC_INC_CASING_LAYER, isSafetyOn('trafficInc'));
  set(TRAFFIC_INC_LINE_LAYER, isSafetyOn('trafficInc'));
  set(PULSEPOINT_DOT_LAYER, ppOn);
  if (!ppOn && popupKey && popupKey.startsWith('pp:')) closePopup();
  if (!isSafetyOn('trafficInc') && popupKey && popupKey.startsWith('ti:')) closePopup();
}

// --- sources ------------------------------------------------------------

function syncPulse() {
  const map = getMap();
  const src = map?.getSource(PULSEPOINT_SOURCE_ID);
  if (!src) return;
  const fc = {
    type: 'FeatureCollection',
    features: pulse.map((x, idx) => ({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [x.lng, x.lat] },
      properties: {
        idx,
        id: x.id,
        kind: x.kind,
        // Image name for the symbol layer: the PulsePoint respond-icon code, or
        // absent -> the layer's coalesce falls back to the generated pin.
        ...(x.iconType ? { icon: `pp-${x.iconType}` } : {}),
        type: x.type || 'Incident',
        address: x.address || '',
        units: (x.units || [])
          .map((u) => `${u.id}${u.status ? ` (${u.status})` : ''}`)
          .join(', '),
        ageMin: x.ageMin == null ? '' : String(x.ageMin),
      },
    })),
  };
  lastPulseFC = fc;
  src.setData(fc);
  for (const code of new Set(pulse.map((x) => x.iconType).filter(Boolean))) ensurePpIcon(code);
  if (popupKey && popupKey.startsWith('pp:')) refreshPopup();
}

function syncInc() {
  const src = getMap()?.getSource(TRAFFIC_INC_SOURCE_ID);
  if (!src) return;
  src.setData({ type: 'FeatureCollection', features: inc });
}

// --- interactions -----------------------------------------------------

function wire() {
  const map = getMap();
  if (!map || wired) return;
  wired = true;

  // Backstop: if anything still asks for a `pp-<code>` image we haven't loaded
  // (eager loading in syncPulse is the primary path), fetch it. Map-level
  // listener — survives theme swaps.
  map.on('styleimagemissing', (e) => {
    const id = e && e.id;
    if (id && id.startsWith('pp-')) ensurePpIcon(id.slice(3));
  });

  registerMarkerLayer({
    layer: PULSEPOINT_DOT_LAYER,
    resolve: (f) => {
      const p = f.properties;
      return {
        key: `pp:${p.id}`,
        label: p.type || 'Incident',
        sublabel: 'PulsePoint',
        color:
          p.kind === 'fire' ? '#dc2626'
          : p.kind === 'medical' ? '#2563eb'
          : p.kind === 'traffic' ? '#ea580c'
          : '#6b7280',
        open: () => openPulsePopup(p.id, f.geometry.coordinates),
      };
    },
  });

  map.on('mouseenter', PULSEPOINT_DOT_LAYER, () => {
    map.getCanvas().style.cursor = 'pointer';
  });
  map.on('mouseleave', PULSEPOINT_DOT_LAYER, () => {
    map.getCanvas().style.cursor = '';
  });

  // Traffic-incident lines aren't marker-menu targets (they're linework), so
  // give them their own click.
  map.on('click', TRAFFIC_INC_LINE_LAYER, (e) => {
    if (!isSafetyOn('trafficInc')) return;
    const f = e.features && e.features[0];
    if (!f) return;
    openIncPopup(f.properties, e.lngLat);
  });
  map.on('mouseenter', TRAFFIC_INC_LINE_LAYER, () => {
    map.getCanvas().style.cursor = 'pointer';
  });
  map.on('mouseleave', TRAFFIC_INC_LINE_LAYER, () => {
    map.getCanvas().style.cursor = '';
  });
}

// --- popups ---------------------------------------------------------

function openPulsePopup(id, lngLat) {
  const x = pulse.find((p) => p.id === id);
  if (!x) return;
  closePopup();
  popupKey = `pp:${id}`;
  popup = new maplibregl.Popup({
    offset: 12,
    closeButton: true,
    className: 'livemap-stop-popup livemap-cat-popup',
    maxWidth: '280px',
  })
    .setLngLat(lngLat || [x.lng, x.lat])
    .setHTML(pulsePopupHTML(x))
    .addTo(getMap());
  popup.on('close', clearPopup);
}

function refreshPopup() {
  const id = popupKey.slice(3);
  const x = pulse.find((p) => p.id === id);
  if (!popup || !x) return;
  popup.setHTML(pulsePopupHTML(x));
}

function openIncPopup(props, lngLat) {
  closePopup();
  popupKey = `ti:${props.from}|${props.to}`;
  popup = new maplibregl.Popup({
    offset: 8,
    closeButton: true,
    className: 'livemap-stop-popup livemap-cat-popup',
    maxWidth: '280px',
  })
    .setLngLat(lngLat)
    .setHTML(incPopupHTML(props))
    .addTo(getMap());
  popup.on('close', clearPopup);
}

function closePopup() {
  if (popup) popup.remove();
  clearPopup();
}
function clearPopup() {
  popup = null;
  popupKey = null;
}

function pulsePopupHTML(x) {
  const rows = [];
  if (x.ageMin != null) rows.push(row('Reported', x.ageMin === 0 ? 'just now' : `${x.ageMin} min ago`));
  const active = (x.units || []).filter((u) => !u.cleared);
  if (active.length) rows.push(row('Units', active.map((u) => `${u.id}${u.status ? ` ${u.status}` : ''}`).join(', ')));
  const addr = x.address ? `<div class="ls-empty">${esc(x.address)}</div>` : '';
  return `
    <div class="ls-pop">
      <div class="ls-name">${esc(x.type || 'Incident')} <span class="ls-tag">PULSEPOINT</span></div>
      ${addr}
      ${rows.join('')}
    </div>`;
}

function incPopupHTML(p) {
  const rows = [];
  const seg = [p.from, p.to].filter(Boolean).join(' → ');
  if (seg) rows.push(`<div class="ls-empty">${esc(seg)}</div>`);
  if (p.desc) rows.push(row('Detail', p.desc));
  if (p.delay) rows.push(row('Delay', p.delay));
  return `
    <div class="ls-pop">
      <div class="ls-name">${esc(p.cat || 'Incident')} <span class="ls-tag">TRAFFIC</span></div>
      ${rows.join('')}
    </div>`;
}

function row(k, v) {
  return `<div class="ls-row"><span class="ls-route">${esc(k)}</span><span class="ls-eta">${esc(v)}</span></div>`;
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
