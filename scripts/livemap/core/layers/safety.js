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
import { isDispatcher, onDispatcher } from '../data/session.js';
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
  onDispatcher(() => applyVis());
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
  g.fillStyle = '#666666'; // UVA Text Gray
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
  // Traffic flow + incidents are dispatcher-only, regardless of a toggle state
  // left behind in localStorage from an earlier authed session.
  const disp = isDispatcher();
  set(TRAFFIC_FLOW_LAYER, disp && isSafetyOn('trafficFlow'));
  set(TRAFFIC_INC_CASING_LAYER, disp && isSafetyOn('trafficInc'));
  set(TRAFFIC_INC_LINE_LAYER, disp && isSafetyOn('trafficInc'));
  set(PULSEPOINT_DOT_LAYER, ppOn);
  if (!ppOn && popupKey && popupKey.startsWith('pp:')) closePopup();
  if ((!disp || !isSafetyOn('trafficInc')) && popupKey && popupKey.startsWith('ti:')) closePopup();
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
        // UVA palette: Emergency / Cyan / Orange / Text Gray.
        color:
          p.kind === 'fire' ? '#df1e43'
          : p.kind === 'medical' ? '#009fdf'
          : p.kind === 'traffic' ? '#e57200'
          : '#666666',
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
    className: 'livemap-stop-popup livemap-cat-popup livemap-incident-popup',
    maxWidth: '340px',
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

// Order units run through in the popup's grouped list.
const PP_UNIT_ORDER = ['OS', 'AE', 'SG', 'ER', 'TR', 'TA', 'DP', 'AK', 'AR'];

/** PulsePoint incident popup — structured like testmap's `.incident-popup`:
 *  respond-icon + title + meta, "Routes Nearby" pills, units grouped by status. */
function pulsePopupHTML(x) {
  const code = String(x.iconType || '').toLowerCase();
  const iconUrl = code ? `${PP_ICON_BASE}${code}_list.png` : '';
  const icon = iconUrl
    ? `<div class="incident-popup__icon"><img src="${esc(iconUrl)}" alt="" onerror="this.style.display='none'"></div>`
    : `<div class="incident-popup__icon"><span class="incident-popup__icon-fallback">${esc(
        (x.label || 'I').charAt(0),
      )}</span></div>`;

  const meta = [];
  if (x.ageMin != null) meta.push(metaLine(`Received ${fmtWhen(x.receivedAt, x.ageMin)}`));
  if (x.firstOnScene) {
    const mins = Math.max(0, Math.round((Date.now() - x.firstOnScene) / 60_000));
    meta.push(metaLine(`First unit on scene ${fmtWhen(x.firstOnScene, mins)}`));
  }
  if (x.status) meta.push(metaLine(`Status: ${esc(x.status)}`));
  if (x.address) meta.push(metaLine(`Location: ${esc(x.address)}`));

  const routePills = (x.routes || [])
    .map(
      (r) =>
        `<span class="incident-popup__route" style="background:${esc(r.color || '#334')};border-color:${esc(
          r.color || '#334',
        )};color:${contrastText(r.color)}">${esc(r.name)}</span>`,
    )
    .join('');
  const routesSection = routePills
    ? `<div class="incident-popup__section">
         <div class="incident-popup__section-title">Routes Nearby</div>
         <div class="incident-popup__routes-list">${routePills}</div>
       </div>`
    : '';

  const unitsSection = renderUnitGroups(x.units || []);

  return `
    <div class="incident-popup">
      <div class="incident-popup__header">
        ${icon}
        <div class="incident-popup__details">
          <div class="incident-popup__title">${esc(x.label || x.type || 'Incident')}</div>
          ${meta.length ? `<div class="incident-popup__meta">${meta.join('')}</div>` : ''}
        </div>
      </div>
      ${routesSection}
      ${unitsSection}
    </div>`;
}

function metaLine(html) {
  return `<div class="incident-popup__meta-line">${html}</div>`;
}

/** "8 min ago · 4:12 PM" (or "just now · …"). ms may be null. */
function fmtWhen(ms, mins) {
  const rel = mins <= 0 ? 'just now' : `${mins} min ago`;
  const abs = ms
    ? new Date(ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
    : '';
  return abs ? `${rel} · ${abs}` : rel;
}

function renderUnitGroups(units) {
  const live = units.filter((u) => u.id && !u.cleared);
  if (!live.length) return '';
  const byKey = new Map();
  for (const u of live) {
    const k = u.statusKey || u.statusLabel || '?';
    if (!byKey.has(k)) byKey.set(k, { label: u.statusLabel, color: u.statusColor, key: u.statusKey, units: [] });
    byKey.get(k).units.push(u);
  }
  const groups = [...byKey.values()].sort(
    (a, b) =>
      (PP_UNIT_ORDER.indexOf(a.key) + 1 || 99) - (PP_UNIT_ORDER.indexOf(b.key) + 1 || 99),
  );
  const body = groups
    .map(
      (g) => `
      <div class="incident-popup__unit-status-group">
        <div class="incident-popup__unit-status-title">${esc(g.label)}</div>
        <div class="incident-popup__unit-list">${g.units
          .map(
            (u) =>
              `<span class="incident-unit" style="color:${esc(g.color)};border-color:${esc(
                g.color,
              )}">${esc(u.id)}</span>`,
          )
          .join('')}</div>
      </div>`,
    )
    .join('');
  return `<div class="incident-popup__section incident-popup__units">
    <div class="incident-popup__section-title">Units</div>${body}</div>`;
}

/** Black or white text for a solid hex background (YIQ). */
function contrastText(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec(String(hex || ''));
  if (!m) return '#f8fafc';
  const n = parseInt(m[1], 16);
  const yiq = (((n >> 16) & 255) * 299 + ((n >> 8) & 255) * 587 + (n & 255) * 114) / 1000;
  return yiq >= 150 ? '#1b1f27' : '#f8fafc';
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
