// livemap/apps/vandispatch2/map-overlays.js
// -----------------------------------------------------------------------------
// The map overlays that /vandispatch2 renders itself (instead of livemap's
// shared micro-trips / safety layers) so it matches the Leaflet /vandispatch
// pixel-for-pixel:
//
//   * numbered pickup / drop-off markers  (van-coloured 30px circle + FGDC digit)
//   * their .ondemand-driver-popup stop cards
//   * the Spare service-area outline       (themed dashed under-layer)
//   * PulsePoint respond-icon pins + the pulsating halo, service-area-filtered
//   * the click-to-reveal van / ride route polyline (Spare duty polyline, ORS
//     leg-routing fallback, pickup/drop-off fit-to-bounds when there's no route)
//
// All of this is ported close to verbatim from html/vandispatch.html. Leaflet
// markers become maplibregl.Marker DOM markers (there are only a handful);
// Leaflet polylines become features in the baked `vd-route` GeoJSON source.
// -----------------------------------------------------------------------------

import { onStyleReady } from '../../core/map.js';
import { onThemeChange, getEffectiveTheme } from '../../core/theme.js';
import { API_BASE } from '../../core/config.js';
import { debounce } from '../../core/util.js';
import {
  VD_AREA_SOURCE_ID,
  VD_AREA_FILL_LAYER,
  VD_AREA_LINE_LAYER,
  VD_ROUTE_SOURCE_ID,
  VD_ROUTE_CASING_LAYER,
  VD_ROUTE_LINE_LAYER,
  VD_ROUTE_DASH_LAYER,
} from '../../core/basemap-style.js';
import {
  onChange,
  getVehicleData,
  getVanColor,
  getSpareTrips,
  getOdStops,
  getOdVehicles,
  computeVehicleStopGroups,
  OD_ACTIVE,
} from './data.js';
import {
  onSelectionChange,
  onMapBackground,
  getSelected,
  selectVan,
  clearSelection,
} from './selection.js';
import { onMicroVehicles } from '../../core/data/microtransit.js';
import { escHtml, svgIcon, contrastColor, fmtTime, haversineM } from './helpers.js';

const U = (p) => `${API_BASE}${p}`;
const FONT = "'FGDC', sans-serif";

let map = null;

// ===========================================================================
//  Numbered pickup / drop-off markers  (ported createStopOrderMarker + popups)
// ===========================================================================

/** vandispatch's exact numbered marker: a van-coloured disc, white 3px ring,
 *  FGDC digit in the contrasting ink colour. Returned as a DOM element for a
 *  maplibregl.Marker. */
function stopOrderMarkerEl(number, color) {
  const SIZE = 30;
  const STROKE = 3;
  const sw = Math.round((SIZE + STROKE * 2) * 100) / 100;
  const textColor = contrastColor(color);
  const el = document.createElement('div');
  el.className = 'vd-stop-marker';
  el.innerHTML = `<svg width="${sw}" height="${sw}" viewBox="0 0 ${sw} ${sw}" xmlns="http://www.w3.org/2000/svg">
    <circle cx="${sw / 2}" cy="${sw / 2}" r="${SIZE / 2}" fill="${color}" stroke="white" stroke-width="${STROKE}"/>
    <text x="${sw / 2}" y="${sw / 2}" text-anchor="middle" dominant-baseline="central" font-size="16" font-weight="bold" fill="${textColor}" font-family="${FONT}">${escHtml(String(number))}</text>
  </svg>`;
  return el;
}

// data.js's computeVehicleStopGroups() already merges same-spot Spare stops and
// numbers them the way the trip cards do — this module just plots the result.
let stopMarkers = []; // maplibregl.Marker[]

/** Select a van without triggering selectVan's toggle-off (used by the stop
 *  discs, which frequently overlap their own van). */
function ensureVanSelected(source, id) {
  const cur = getSelected();
  const nid = id != null ? String(id) : null;
  if (cur && cur.source === source && cur.id === nid) return;
  selectVan(source, id);
}
let stopPopup = null;

function clearStopMarkers() {
  for (const m of stopMarkers) m.remove();
  stopMarkers = [];
}

function closeStopPopup() {
  if (stopPopup) {
    stopPopup.remove();
    stopPopup = null;
  }
}

/** Spare stop popup — ported buildStopPopupHtml. */
function spareStopPopupHTML(order, stops, vanLabel, vanColor) {
  const rows = stops
    .map((s) => {
      const isPickup = s.kind === 'Pickup';
      const iconCls = isPickup
        ? 'ondemand-driver-popup__ride-location-icon--pickup'
        : 'ondemand-driver-popup__ride-location-icon--dropoff';
      return `<div class="ondemand-driver-popup__ride-location">
        <span class="ondemand-driver-popup__ride-location-icon ${iconCls}">${isPickup ? 'P' : 'D'}</span>
        <span class="ondemand-driver-popup__ride-location-text"><strong>${escHtml(
          s.name,
        )}</strong> — ${s.kind} at ${fmtTime(s.ts)}<br>${escHtml(s.address || '—')}</span>
      </div>`;
    })
    .join('');
  const shared =
    stops.length > 1
      ? `<div class="ondemand-driver-popup__label" style="margin-top:8px;color:#b45309">${svgIcon(
          'warn',
        )} Shared stop — ${stops.length} events happen here</div>`
      : '';
  return `<div class="ondemand-driver-popup__content">
    <div class="ondemand-driver-popup__section">
      <div class="ondemand-driver-popup__label">Stop #${order} — <span style="color:${vanColor}">${escHtml(
        vanLabel,
      )}</span></div>
    </div>
    <div class="ondemand-driver-popup__divider"></div>
    <div class="ondemand-driver-popup__ride-locations">${rows}</div>
    ${shared}
  </div>`;
}

/** OnDemand stop popup — ported buildOdStopPopupHtml. */
function odStopPopupHTML(s, color) {
  const isPickup = s.stopType === 'pickup';
  const riders = (s.riders || []).join(', ') || 'Rider';
  return `<div class="ondemand-driver-popup__content">
    <div class="ondemand-driver-popup__section">
      <div class="ondemand-driver-popup__label">Stop #${s.order} — <span style="color:${color}">${escHtml(
        s.vehicleName || 'Van',
      )}</span></div>
    </div>
    <div class="ondemand-driver-popup__divider"></div>
    <div class="ondemand-driver-popup__ride-locations">
      <div class="ondemand-driver-popup__ride-location">
        <span class="ondemand-driver-popup__ride-location-icon ondemand-driver-popup__ride-location-icon--${
          isPickup ? 'pickup' : 'dropoff'
        }">${isPickup ? 'P' : 'D'}</span>
        <span class="ondemand-driver-popup__ride-location-text"><strong>${escHtml(
          riders,
        )}</strong> — ${isPickup ? 'Pickup' : 'Drop-off'}${
          Number.isFinite(s.stopTs) ? ` at ${fmtTime(s.stopTs)}` : ''
        }<br>${escHtml(s.address || '—')}</span>
      </div>
    </div>
  </div>`;
}

function openStopPopup(lngLat, html) {
  closeStopPopup();
  stopPopup = new maplibregl.Popup({
    className: 'vd-driver-popup',
    closeButton: true,
    offset: 16,
    maxWidth: '320px',
  })
    .setLngLat(lngLat)
    .setHTML(html)
    .addTo(map);
}

function renderStopMarkers() {
  if (!map) return;
  clearStopMarkers();

  // Spare — infer each van's ordered stop sequence (data.js already merges
  // same-spot stops and numbers them the way the trip cards do).
  const byVehicle = computeVehicleStopGroups();
  for (const vehicleId of Object.keys(byVehicle)) {
    const v = getVehicleData()[vehicleId] || {};
    const label = v.identifier || 'Van';
    const color = getVanColor(label, v.markerColor || '#E57200');
    byVehicle[vehicleId].forEach((group, idx) => {
      const order = idx + 1;
      const el = stopOrderMarkerEl(order, color);
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        // Also select the van — a stop disc frequently sits right on top of its
        // van (the van drives to its stops), and the DOM disc would otherwise
        // swallow the click that would have selected the van + drawn its route.
        // Guard against selectVan's toggle so clicking a stop of the already-
        // selected van doesn't deselect it.
        ensureVanSelected('spare', vehicleId);
        openStopPopup(group.lngLat, spareStopPopupHTML(order, group.stops, label, color));
      });
      const mk = new maplibregl.Marker({ element: el, anchor: 'center' })
        .setLngLat(group.lngLat)
        .addTo(map);
      stopMarkers.push(mk);
    });
  }

  // OnDemand — the schedule feed already carries a per-van stop order.
  const byVeh = {};
  for (const s of getOdStops()) {
    if (s.lat == null || s.lng == null) continue;
    const rec = (s.rides && s.rides[0]) || {};
    const rs = s.rideStatus || rec.rideStatus || '';
    if (!OD_ACTIVE.has(rs) && rs !== '') continue;
    (byVeh[s.vehicleId] || (byVeh[s.vehicleId] = [])).push(s);
  }
  for (const vid of Object.keys(byVeh)) {
    const entries = byVeh[vid].slice().sort((a, b) => (a.order || 0) - (b.order || 0));
    const color = entries[0].vehicleColor || '#ec4899';
    entries.forEach((s) => {
      if (!Number.isFinite(s.order)) return;
      const el = stopOrderMarkerEl(s.order, color);
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        ensureVanSelected('od', vid); // see the spare branch — the disc covers its van
        openStopPopup([s.lng, s.lat], odStopPopupHTML(s, color));
      });
      const mk = new maplibregl.Marker({ element: el, anchor: 'center' })
        .setLngLat([s.lng, s.lat])
        .addTo(map);
      stopMarkers.push(mk);
    });
  }
}

// ===========================================================================
//  Service-area outline  (ported loadServiceArea / serviceAreaStyle)
// ===========================================================================

let serviceAreaPolys = null; // [[outerRing, hole...], ...] of [lng,lat] — for point-in-area tests
let areaFC = null; // last service-area GeoJSON Feature (survives a theme-swap style rebuild)

function areaColor() {
  return getEffectiveTheme() === 'light' ? '#232D4B' : '#C8CBD2';
}

function restyleArea() {
  if (!map) return;
  const c = areaColor();
  if (map.getLayer(VD_AREA_FILL_LAYER)) map.setPaintProperty(VD_AREA_FILL_LAYER, 'fill-color', c);
  if (map.getLayer(VD_AREA_LINE_LAYER)) map.setPaintProperty(VD_AREA_LINE_LAYER, 'line-color', c);
}

function syncArea(fc) {
  if (!map) return;
  if (fc !== undefined) areaFC = fc;
  const src = map.getSource(VD_AREA_SOURCE_ID);
  if (!src) return;
  const data = areaFC || { type: 'FeatureCollection', features: [] };
  src.setData(data.type === 'Feature' ? { type: 'FeatureCollection', features: [data] } : data);
  const show = !!areaFC;
  for (const id of [VD_AREA_FILL_LAYER, VD_AREA_LINE_LAYER]) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', show ? 'visible' : 'none');
  }
  restyleArea();
}

async function loadServiceArea() {
  try {
    const r = await fetch(U('/api/spare/service-area'), { credentials: 'include', cache: 'no-store' });
    if (r.status === 204 || !r.ok) return;
    const gj = await r.json();
    const geom = gj && gj.geometry ? gj.geometry : gj;
    if (geom && geom.type === 'Polygon') serviceAreaPolys = [geom.coordinates];
    else if (geom && geom.type === 'MultiPolygon') serviceAreaPolys = geom.coordinates;
    else serviceAreaPolys = null;
    syncArea(gj && gj.type === 'Feature' ? gj : { type: 'Feature', geometry: geom, properties: {} });
    // Re-filter PulsePoint now that we have an area.
    renderIncidents();
  } catch {
    /* leave the last outline up */
  }
}

function pointInRing(lng, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0];
    const yi = ring[i][1];
    const xj = ring[j][0];
    const yj = ring[j][1];
    if (yi > lat !== yj > lat && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}
function inServiceArea(lat, lng) {
  if (!serviceAreaPolys) return false; // only-inside rule: no area -> show nothing
  for (const poly of serviceAreaPolys) {
    if (pointInRing(lng, lat, poly[0]) && !poly.slice(1).some((h) => pointInRing(lng, lat, h)))
      return true;
  }
  return false;
}

// ===========================================================================
//  PulsePoint incidents  (ported normIncidents / incidentPopupHtml / halo)
// ===========================================================================

const INCIDENT_ICON_BASE = '/v1/pulsepoint/respond_icons/';
const HALO_DIAMETER = 110;

// PulsePoint incident type code -> label (from testmap / vandispatch).
const INCIDENT_TYPE_LABELS = {
  AED: 'AED Alarm', AC: 'Aircraft Crash', AE: 'Aircraft Emergency', AES: 'Aircraft Emergency Standby',
  OA: 'Alarm', AR: 'Animal Rescue', AF: 'Appliance Fire', AI: 'Arson Investigation',
  AA: 'Auto Aid', BT: 'Bomb Threat', BP: 'Burn Permit', CMA: 'Carbon Monoxide',
  CHIM: 'Chimney Fire', CR: 'Cliff Rescue', TCP: 'Collision Involving Pedestrian',
  TCS: 'Collision Involving Structure', TCT: 'Collision Involving Train', CF: 'Commercial Fire',
  CL: 'Commercial Lockout', CA: 'Community Activity', CP: 'Community Paramedicine',
  CSR: 'Confined Space Rescue', WF: 'Confirmed Fire', WSF: 'Confirmed Structure Fire',
  WVEG: 'Confirmed Vegetation Fire', CB: 'Controlled Burn/Prescribed Fire', EQ: 'Earthquake',
  EE: 'Electrical Emergency', ELF: 'Electrical Fire', ELR: 'Elevator Rescue',
  EER: 'Elevator/Escalator Rescue', EM: 'Emergency', ER: 'Emergency Response',
  TCE: 'Expanded Traffic Collision', EX: 'Explosion', EF: 'Extinguished Fire', FIRE: 'Fire',
  FA: 'Fire Alarm', FW: 'Fire Watch', FWI: 'Fireworks Investigation', FLW: 'Flood Warning',
  FL: 'Flooding', FULL: 'Full Assignment', GAS: 'Gas Leak', HC: 'Hazardous Condition',
  HMR: 'Hazardous Response', HMI: 'Hazmat Investigation', IR: 'Ice Rescue', IF: 'Illegal Fire',
  IA: 'Industrial Accident', IFT: 'Interfacility Transfer', INV: 'Investigation',
  LR: 'Ladder Request', LZ: 'Landing Zone', LA: 'Lift Assist', LO: 'Lockout', MA: 'Manual Alarm',
  MF: 'Marine Fire', ME: 'Medical Emergency', MC: 'Move-up/Cover', MCI: 'Multi Casualty',
  MU: 'Mutual Aid', NO: 'Notification', OI: 'Odor Investigation', OF: 'Outside Fire',
  PE: 'Pipeline Emergency', PF: 'Pole Fire', PA: 'Police Assist', PLE: 'Powerline Emergency',
  PS: 'Public Service', RTE: 'Railroad/Train Emergency', GF: 'Refuse/Garbage Fire', RES: 'Rescue',
  RF: 'Residential Fire', RL: 'Residential Lockout', RR: 'Rope Rescue', SH: 'Sheared Hydrant',
  SD: 'Smoke Detector', SI: 'Smoke Investigation', STBY: 'Standby', ST: 'Strike Team/Task Force',
  SC: 'Structural Collapse', SF: 'Structure Fire', TF: 'Tank Fire', TR: 'Technical Rescue',
  TEST: 'Test', TOW: 'Tornado Warning', TC: 'Traffic Collision', TRNG: 'Training',
  TE: 'Transformer Explosion', TD: 'Tree Down', TNR: 'Trench Rescue', TRBL: 'Trouble Alarm',
  TSW: 'Tsunami Warning', USAR: 'Urban Search and Rescue', VEG: 'Vegetation Fire',
  VF: 'Vehicle Fire', VL: 'Vehicle Lockout', VS: 'Vessel Sinking', WE: 'Water Emergency',
  WR: 'Water Rescue', WFA: 'Waterflow Alarm', WX: 'Weather Incident', WA: 'Wires Arching',
  WD: 'Wires Down', WDA: 'Wires Down/Arcing', WCF: 'Working Commercial Fire',
  WRF: 'Working Residential Fire',
};

const INCIDENT_UNIT_STATUS_INFO = {
  DP: { label: 'Dispatched', color: '#f57c00', background: 'rgba(245,124,0,0.16)', border: 'rgba(245,124,0,0.38)' },
  AK: { label: 'Acknowledged', color: '#f57c00', background: 'rgba(245,124,0,0.16)', border: 'rgba(245,124,0,0.38)' },
  ER: { label: 'En Route', color: '#00cc00', background: 'rgba(0,204,0,0.16)', border: 'rgba(0,204,0,0.38)' },
  SG: { label: 'Staged', color: '#cc0000', background: 'rgba(204,0,0,0.16)', border: 'rgba(204,0,0,0.38)' },
  OS: { label: 'On Scene', color: '#cc0000', background: 'rgba(204,0,0,0.16)', border: 'rgba(204,0,0,0.38)' },
  AE: { label: 'Available On Scene', color: '#cc0000', background: 'rgba(204,0,0,0.16)', border: 'rgba(204,0,0,0.38)' },
  TR: { label: 'Transport', color: '#ffc107', background: 'rgba(255,193,7,0.16)', border: 'rgba(255,193,7,0.38)' },
  TA: { label: 'Transport Arrived', color: '#1976d2', background: 'rgba(25,118,210,0.16)', border: 'rgba(25,118,210,0.38)' },
  AR: { label: 'Cleared From Incident', color: '#8a8a8a', background: 'rgba(138,138,138,0.16)', border: 'rgba(138,138,138,0.38)' },
};
const INCIDENT_UNIT_STATUS_ALIASES = {
  DP: 'DP', DISPATCHED: 'DP', DISPATCH: 'DP', AK: 'AK', ACK: 'AK', ACKNOWLEDGED: 'AK',
  ER: 'ER', 'EN ROUTE': 'ER', ENROUTE: 'ER', SG: 'SG', STAGED: 'SG',
  OS: 'OS', 'ON SCENE': 'OS', 'ON-SCENE': 'OS', ONSCENE: 'OS',
  AE: 'AE', 'AVAILABLE ON SCENE': 'AE', 'AVAIL ON SCENE': 'AE',
  TR: 'TR', TRANSPORT: 'TR', TRANSPORTING: 'TR', TA: 'TA', 'TRANSPORT ARRIVED': 'TA',
  AR: 'AR', CLEARED: 'AR', 'CLEARED FROM INCIDENT': 'AR',
};
const INCIDENT_UNIT_STATUS_ORDER = ['OS', 'AE', 'SG', 'ER', 'TR', 'TA', 'DP', 'AK', 'AR'];
const INCIDENT_UNIT_FALLBACK_LABEL = 'Status Unknown';

function _incidentType(rec) {
  const cands = [
    rec.PulsePointIncidentCallTypeCode, rec.PulsePointIncidentTypeCode,
    rec.CallTypeCode, rec.TypeCode, rec.PulsePointIncidentCallType, rec.CallType,
  ];
  for (const v of cands) {
    if (v == null) continue;
    const t = String(v).trim();
    if (/^[A-Za-z0-9]{1,6}$/.test(t)) return t.toUpperCase();
    const words = t.match(/[A-Za-z0-9]+/g);
    if (words && words.length >= 2) {
      const acr = words.map((w) => w[0]).join('');
      if (/^[A-Za-z0-9]{1,4}$/.test(acr)) return acr.toUpperCase();
    }
  }
  return '';
}
function normIncidents(root) {
  const arr =
    root && root.incidents && Array.isArray(root.incidents.active) ? root.incidents.active : [];
  const out = [];
  for (const rec of arr) {
    const lat = parseFloat(rec.Latitude ?? rec.latitude ?? rec.lat);
    const lng = parseFloat(rec.Longitude ?? rec.longitude ?? rec.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
    const id = String(
      rec.ID ?? rec.PulsePointIncidentID ?? rec.PulsePointIncidentCallNumber ?? `${lat.toFixed(6)},${lng.toFixed(6)}`,
    );
    const type = _incidentType(rec);
    out.push({
      id, lat, lng, type, raw: rec,
      iconUrl: type ? `${INCIDENT_ICON_BASE}${type.toLowerCase()}_map_active.png` : '',
    });
  }
  return out;
}

function _normUnitStatus(value) {
  const raw = value == null ? '' : String(value).trim();
  if (!raw) return { key: '', raw: '', label: '', info: null };
  const canonical = INCIDENT_UNIT_STATUS_ALIASES[raw.toUpperCase()] || '';
  const info = canonical ? INCIDENT_UNIT_STATUS_INFO[canonical] || null : null;
  return { key: canonical, raw, info, label: (info && info.label) || raw };
}
function _extractIncidentUnits(rec) {
  const units = [];
  let updNotified = false;
  const src = Array.isArray(rec && rec.Unit) ? rec.Unit : [];
  for (const entry of src) {
    if (!entry) continue;
    let name =
      [entry.UnitID, entry.Unit, entry.Name].find((v) => typeof v === 'string' && v.trim()) || '';
    const status =
      [entry.PulsePointDispatchStatus, entry.DispatchStatus, entry.Status].find(
        (v) => typeof v === 'string' && v.trim(),
      ) || '';
    name = name.trim();
    if (name.toUpperCase() === 'UVA2') {
      updNotified = true;
      continue;
    }
    const n = _normUnitStatus(status);
    const displayText = name || n.label;
    if (!displayText) continue;
    units.push({ displayText, statusKey: n.key, statusLabel: n.label, colorInfo: n.info, tooltip: n.label });
  }
  if (updNotified) units.updNotified = true;
  return units;
}
function _renderIncidentUnit(u) {
  if (!u || !u.displayText) return '';
  const s = [];
  if (u.colorInfo) {
    if (u.colorInfo.color) s.push(`color:${u.colorInfo.color}`);
    if (u.colorInfo.background) s.push(`background:${u.colorInfo.background}`);
    if (u.colorInfo.border) s.push(`border-color:${u.colorInfo.border}`);
  }
  const styleAttr = s.length ? ` style="${s.join(';')}"` : '';
  const titleAttr = u.tooltip ? ` title="${escHtml(u.tooltip)}"` : '';
  return `<span class="incident-unit${
    u.statusKey ? ` incident-unit--${u.statusKey.toLowerCase()}` : ''
  }"${styleAttr}${titleAttr}>${escHtml(u.displayText)}</span>`;
}
function _renderIncidentUnitsSection(units) {
  const valid = (units || []).filter((u) => u && u.displayText);
  const updBadge =
    units && units.updNotified
      ? `<div class="incident-popup__unit-list"><span class="incident-unit incident-unit--upd-notified">UPD Notified</span></div>`
      : '';
  if (!valid.length && !updBadge) return '';
  const groupsMap = new Map();
  valid.forEach((u, i) => {
    const label = u.statusLabel || u.statusKey || INCIDENT_UNIT_FALLBACK_LABEL;
    const mapKey = u.statusKey ? `k:${u.statusKey}` : `l:${label.toLowerCase()}`;
    let g = groupsMap.get(mapKey);
    if (!g) {
      const idx = u.statusKey ? INCIDENT_UNIT_STATUS_ORDER.indexOf(u.statusKey) : -1;
      g = { label, units: [], sortIndex: idx === -1 ? INCIDENT_UNIT_STATUS_ORDER.length : idx, first: i };
      groupsMap.set(mapKey, g);
    }
    g.units.push(u);
  });
  const groupsHtml = [...groupsMap.values()]
    .sort((a, b) => a.sortIndex - b.sortIndex || a.first - b.first || a.label.localeCompare(b.label))
    .map((g) => {
      const list = g.units.map(_renderIncidentUnit).filter(Boolean).join('');
      return list
        ? `<div class="incident-popup__unit-status-group"><div class="incident-popup__unit-status-title">${escHtml(
            g.label,
          )}</div><div class="incident-popup__unit-list">${list}</div></div>`
        : '';
    })
    .filter(Boolean)
    .join('');
  if (!groupsHtml && !updBadge) return '';
  return `<div class="incident-popup__section incident-popup__units"><div class="incident-popup__section-title">Units</div>${groupsHtml}${updBadge}</div>`;
}
function _incidentReceivedTime(rec) {
  const d = Date.parse(rec.CallReceivedDateTime || rec.CallReceivedDateTimeLocal || '');
  if (!Number.isFinite(d)) return null;
  const dt = new Date(d);
  const T24 = { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' };
  try {
    return {
      display: new Intl.DateTimeFormat('en-US', { ...T24, timeZone: 'America/New_York' }).format(dt),
      full: new Intl.DateTimeFormat('en-US', {
        dateStyle: 'medium', timeStyle: 'short', hourCycle: 'h23', timeZone: 'America/New_York',
      }).format(dt),
    };
  } catch {
    const f = dt.toLocaleString('en-US', { hourCycle: 'h23' });
    return { display: f, full: f };
  }
}
function incidentPopupHTML(inc) {
  const rec = inc.raw || {};
  const typeLabel =
    INCIDENT_TYPE_LABELS[inc.type] || rec.PulsePointIncidentCallType || rec.CallType || 'Incident';
  const listIcon = inc.type ? `${INCIDENT_ICON_BASE}${inc.type.toLowerCase()}_list.png` : '';
  const iconHtml = listIcon
    ? `<div class="incident-popup__icon"><img src="${escHtml(
        listIcon,
      )}" alt="${escHtml(typeLabel)} icon" onerror="this.style.display='none'"></div>`
    : `<div class="incident-popup__icon"><span class="incident-popup__icon-fallback">${escHtml(
        (typeLabel || 'I').charAt(0),
      )}</span></div>`;
  const t = _incidentReceivedTime(rec);
  const receivedLine = t
    ? `<div class="incident-popup__meta-line" title="${escHtml(t.full)}">Received ${escHtml(
        t.display,
      )}</div>`
    : '';
  const loc =
    rec.FullDisplayAddress || rec.DisplayAddress || rec.Address || rec.MedicalEmergencyDisplayAddress || rec.CrossStreet || '';
  const locLine = loc
    ? `<div class="incident-popup__meta-line">Location: ${escHtml(String(loc).trim())}</div>`
    : '';
  const metaLines = [receivedLine, locLine].filter(Boolean).join('');
  const metaHtml = metaLines ? `<div class="incident-popup__meta">${metaLines}</div>` : '';
  const unitsHtml = _renderIncidentUnitsSection(_extractIncidentUnits(rec));
  return `<div class="incident-popup">
    <div class="incident-popup__header">
      ${iconHtml}
      <div class="incident-popup__details">
        <div class="incident-popup__title">${escHtml(typeLabel)}</div>
        ${metaHtml}
      </div>
    </div>
    ${unitsHtml}
  </div>`;
}

let incidentEntries = new Map(); // id -> { iconMarker, haloMarker }
let incidentData = []; // last normalized+filtered list
let incidentPopup = null;

function closeIncidentPopup() {
  if (incidentPopup) {
    incidentPopup.remove();
    incidentPopup = null;
  }
}

function iconMarkerEl(inc) {
  const el = document.createElement('div');
  el.className = 'incident-marker-icon';
  if (inc.iconUrl) {
    const img = document.createElement('img');
    img.src = inc.iconUrl;
    img.alt = '';
    img.width = 40;
    img.height = 40;
    img.onerror = () => {
      el.innerHTML =
        '<div style="width:14px;height:14px;border-radius:50%;background:#ef4444;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.4)"></div>';
    };
    el.appendChild(img);
  } else {
    el.innerHTML =
      '<div style="width:14px;height:14px;border-radius:50%;background:#ef4444;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.4)"></div>';
  }
  return el;
}
function haloMarkerEl() {
  const el = document.createElement('div');
  el.className = 'incident-halo-icon';
  el.style.setProperty('--incident-halo-diameter', `${HALO_DIAMETER}px`);
  el.innerHTML = '<div class="incident-halo incident-halo--animated"></div>';
  return el;
}

function clearIncidents() {
  for (const e of incidentEntries.values()) {
    e.iconMarker.remove();
    e.haloMarker.remove();
  }
  incidentEntries = new Map();
}

/** Rebuild incident markers from `incidentData` (already normalized) against the
 *  current service-area filter. */
function renderIncidents() {
  if (!map) return;
  const seen = new Set();
  for (const inc of incidentData) {
    if (!inServiceArea(inc.lat, inc.lng)) continue;
    seen.add(inc.id);
    let entry = incidentEntries.get(inc.id);
    if (!entry) {
      const haloMarker = new maplibregl.Marker({ element: haloMarkerEl(), anchor: 'center' })
        .setLngLat([inc.lng, inc.lat])
        .addTo(map);
      const iconEl = iconMarkerEl(inc);
      // Anchor near the pin tip (Leaflet used iconAnchor [20,38] on a 40px icon).
      const iconMarker = new maplibregl.Marker({ element: iconEl, anchor: 'bottom', offset: [0, 2] })
        .setLngLat([inc.lng, inc.lat])
        .addTo(map);
      iconEl.addEventListener('click', (e) => {
        e.stopPropagation();
        closeIncidentPopup();
        incidentPopup = new maplibregl.Popup({
          className: 'vd-incident-popup',
          closeButton: true,
          offset: 20,
          maxWidth: '360px',
        })
          .setLngLat([inc.lng, inc.lat])
          .setHTML(incidentPopupHTML(inc))
          .addTo(map);
      });
      entry = { iconMarker, haloMarker };
      incidentEntries.set(inc.id, entry);
    } else {
      entry.iconMarker.setLngLat([inc.lng, inc.lat]);
      entry.haloMarker.setLngLat([inc.lng, inc.lat]);
    }
  }
  for (const [id, entry] of incidentEntries) {
    if (!seen.has(id)) {
      entry.iconMarker.remove();
      entry.haloMarker.remove();
      incidentEntries.delete(id);
    }
  }
}

async function loadIncidents() {
  if (!serviceAreaPolys) return; // need the area to filter against (only-inside rule)
  try {
    const r = await fetch(U('/v1/pulsepoint/incidents'), { cache: 'no-store' });
    if (!r.ok) return;
    const root = await r.json();
    incidentData = normIncidents(root);
    renderIncidents();
  } catch {
    /* keep the last set */
  }
}

// ===========================================================================
//  Route polyline  (ported showRequestRoute / showVanFullRoute / showOdVanRoute)
// ===========================================================================

let routeDrawSeq = 0;
let fallbackPins = []; // maplibregl.Marker[] — green/red pickup/dropoff dots when there's no line

function clearFallbackPins() {
  for (const m of fallbackPins) m.remove();
  fallbackPins = [];
}
function addFallbackPin(lngLat, isPickup) {
  const el = document.createElement('div');
  el.className = 'vd-route-pin';
  el.style.cssText = `width:14px;height:14px;border-radius:50%;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.4);background:${
    isPickup ? '#4ade80' : '#f87171'
  }`;
  const m = new maplibregl.Marker({ element: el, anchor: 'center' }).setLngLat(lngLat).addTo(map);
  fallbackPins.push(m);
}

function routeLayersVisible(v) {
  for (const id of [VD_ROUTE_CASING_LAYER, VD_ROUTE_LINE_LAYER, VD_ROUTE_DASH_LAYER]) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', v ? 'visible' : 'none');
  }
}

function setRouteFeatures(features) {
  const src = map && map.getSource(VD_ROUTE_SOURCE_ID);
  if (!src) return;
  src.setData({ type: 'FeatureCollection', features: features || [] });
  routeLayersVisible(!!(features && features.length));
}

export function clearRequestRoute() {
  routeDrawSeq++;
  clearFallbackPins();
  setRouteFeatures([]);
}

function lineFeature(coords, color, dash) {
  return {
    type: 'Feature',
    geometry: { type: 'LineString', coordinates: coords },
    properties: { color: color || '#E57200', dash: dash ? 1 : 0 },
  };
}

/** Standard Google Encoded Polyline decoder -> [[lng,lat],...] (MapLibre order). */
function decodeGooglePolyline(encoded) {
  const points = [];
  let index = 0;
  let lat = 0;
  let lng = 0;
  while (index < encoded.length) {
    let b;
    let shift = 0;
    let result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    lat += result & 1 ? ~(result >> 1) : result >> 1;
    shift = 0;
    result = 0;
    do {
      b = encoded.charCodeAt(index++) - 63;
      result |= (b & 0x1f) << shift;
      shift += 5;
    } while (b >= 0x20);
    lng += result & 1 ? ~(result >> 1) : result >> 1;
    points.push([lng / 1e5, lat / 1e5]);
  }
  return points;
}

function fitTo(coords, opts) {
  if (!coords.length || !map) return;
  let w = Infinity;
  let s = Infinity;
  let e = -Infinity;
  let n = -Infinity;
  for (const [lng, lat] of coords) {
    if (lng < w) w = lng;
    if (lng > e) e = lng;
    if (lat < s) s = lat;
    if (lat > n) n = lat;
  }
  if (w === e && s === n) {
    map.easeTo({ center: [w, s], zoom: Math.max(map.getZoom(), 15), duration: 500 });
  } else {
    map.fitBounds(
      [
        [w, s],
        [e, n],
      ],
      { padding: 60, maxZoom: 16, duration: 500, ...(opts || {}) },
    );
  }
}

function lngLatFromGeoJson(loc) {
  if (!loc || !Array.isArray(loc.coordinates)) return null;
  const [lng, lat] = loc.coordinates;
  return Number.isFinite(lng) && Number.isFinite(lat) ? [lng, lat] : null;
}

/** One ride's route (Spare currentRoute), or its pickup/drop-off framed if the
 *  route isn't available (>20 min out, or empty). Ported showRequestRoute. */
async function showRequestRoute(requestId) {
  clearRequestRoute();
  const seq = ++routeDrawSeq;
  const req = getSpareTrips().find((t) => t.id === requestId);
  const v = req && req.vehicleId ? getVehicleData()[req.vehicleId] || {} : {};
  const label = v.identifier || 'Van';
  const color = getVanColor(label, v.markerColor || '#E57200');

  let data = null;
  try {
    const res = await fetch(U(`/api/spare/requests/${encodeURIComponent(requestId)}/route`), {
      credentials: 'include',
    });
    if (res.ok) data = await res.json();
  } catch {
    /* fall through to the fallback */
  }
  if (seq !== routeDrawSeq) return;

  const feats = [];
  const bounds = [];
  const polylines = (data && data.polylines) || {};
  for (const [key, dashed] of [
    ['routeToPickup', true],
    ['routeToDropoff', false],
  ]) {
    const encoded = polylines[key];
    if (!encoded) continue;
    const pts = decodeGooglePolyline(encoded);
    if (!pts.length) continue;
    feats.push(lineFeature(pts, color, dashed));
    bounds.push(...pts);
  }
  for (const s of (data && data.stopovers) || []) {
    const p = lngLatFromGeoJson(s.location);
    if (p) bounds.push(p);
  }

  if (!feats.length && req) {
    const pu = lngLatFromGeoJson(req.scheduledPickupLocation || req.requestedPickupLocation);
    const doP = lngLatFromGeoJson(req.scheduledDropoffLocation || req.requestedDropoffLocation);
    if (pu) {
      addFallbackPin(pu, true);
      bounds.push(pu);
    }
    if (doP) {
      addFallbackPin(doP, false);
      bounds.push(doP);
    }
  } else {
    setRouteFeatures(feats);
  }
  if (bounds.length) fitTo(bounds);
}

/** OnDemand ride — no trip routing available, so mark + frame pickup/drop-off. */
function showOdRequestRoute(rideKey) {
  clearRequestRoute();
  const bounds = [];
  for (const s of getOdStops()) {
    const rec = (s.rides && s.rides[0]) || {};
    const key = s.rideId || rec.rideId || `${s.vehicleId}|${(s.riders || []).join(',')}`;
    if (key !== rideKey) continue;
    if (!Number.isFinite(s.lat) || !Number.isFinite(s.lng)) continue;
    addFallbackPin([s.lng, s.lat], s.stopType === 'pickup');
    bounds.push([s.lng, s.lat]);
  }
  if (bounds.length) fitTo(bounds);
}

async function fetchRouteLeg(startLngLat, endLngLat) {
  try {
    const res = await fetch(U('/api/routes/leg'), {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      // /api/routes/leg expects [lat, lng] pairs (Leaflet order) — match vandispatch.
      body: JSON.stringify({ start: [startLngLat[1], startLngLat[0]], end: [endLngLat[1], endLngLat[0]] }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    // Response coordinates are [lat, lng]; convert to [lng, lat] for MapLibre.
    return Array.isArray(data.coordinates) ? data.coordinates.map(([la, ln]) => [ln, la]) : null;
  } catch {
    return null;
  }
}

async function fetchDutyPolyline(dutyId) {
  try {
    const res = await fetch(U(`/api/spare/duties/${encodeURIComponent(dutyId)}/route`), {
      credentials: 'include',
    });
    if (!res.ok) return null;
    const data = await res.json();
    const coords = data && data.polyline && data.polyline.coordinates;
    if (!Array.isArray(coords) || !coords.length) return null;
    // Spare returns [lng, lat] — already MapLibre order.
    return coords.map(([lng, lat]) => [lng, lat]);
  } catch {
    return null;
  }
}

async function drawLeggedRoute(startLngLat, stopLngLats, color, fit, seq) {
  if (!stopLngLats.length) {
    clearRequestRoute();
    return;
  }
  const waypoints = [startLngLat, ...stopLngLats];
  const legs = [];
  const bounds = [waypoints[0]];
  for (let i = 0; i < waypoints.length - 1; i++) {
    const leg = await fetchRouteLeg(waypoints[i], waypoints[i + 1]);
    if (seq !== routeDrawSeq) return; // superseded
    if (!leg || !leg.length) continue;
    legs.push(leg);
    bounds.push(...leg);
  }
  clearFallbackPins();
  setRouteFeatures(legs.map((leg) => lineFeature(leg, color, false)));
  if (fit && bounds.length > 1) fitTo(bounds);
}

async function showVanFullRoute(vehicleId, opts) {
  const fit = !(opts && opts.fit === false);
  const seq = ++routeDrawSeq;
  const v = getVehicleData()[vehicleId] || {};
  const label = v.identifier || 'Van';
  const color = getVanColor(label, v.markerColor || '#E57200');
  const here = lngLatFromGeoJson(v.currentLocation && v.currentLocation.location);

  const dutyId = v.currentLocation && v.currentLocation.dutyId;
  const dutyPoints = dutyId ? await fetchDutyPolyline(dutyId) : null;
  if (seq !== routeDrawSeq) return;
  if (dutyPoints && dutyPoints.length) {
    clearFallbackPins();
    setRouteFeatures([lineFeature(dutyPoints, color, false)]);
    if (fit) fitTo(dutyPoints);
    return;
  }
  // Fallback: road-route from the van through its own ordered stop groups.
  const groups = computeVehicleStopGroups()[vehicleId] || [];
  if (!here || !groups.length) {
    clearRequestRoute();
    return;
  }
  await drawLeggedRoute(here, groups.map((g) => g.lngLat), color, fit, seq);
}

async function showOdVanRoute(vehicleId, opts) {
  const fit = !(opts && opts.fit === false);
  const seq = ++routeDrawSeq;
  const van = getOdVehicles().find((x) => String(x.vehicleId) === String(vehicleId));
  if (!van || !Number.isFinite(van.lng) || !Number.isFinite(van.lat)) {
    clearRequestRoute();
    return;
  }
  const stops = getOdStops()
    .filter((s) => String(s.vehicleId) === String(vehicleId) && Number.isFinite(s.lat) && Number.isFinite(s.lng))
    .slice()
    .sort((a, b) => (a.order || 0) - (b.order || 0));
  const color = (stops[0] && stops[0].vehicleColor) || '#ec4899';
  await drawLeggedRoute([van.lng, van.lat], stops.map((s) => [s.lng, s.lat]), color, fit, seq);
}

/** Called by trip-board.js when a trip card is clicked. Drops any van selection
 *  first — otherwise the next position tick would redraw the van's full route
 *  over this one ride's route. */
export function showTripRoute(kind, ref) {
  clearSelection();
  if (kind === 'spare') showRequestRoute(ref);
  else showOdRequestRoute(ref);
}

/** Re-draw the selected van's route without re-framing. */
function redrawSelectedRoute() {
  const sel = getSelected();
  if (!sel || !sel.id) return;
  if (sel.source === 'spare') showVanFullRoute(sel.id, { fit: false });
  else showOdVanRoute(sel.id, { fit: false });
}

// --- keep the drawn route tracking the van as it moves ------------------
// The van marker updates in ~real time off the Spare position SSE (livemap's
// core/data/microtransit.js re-emits onMicroVehicles on every pushed fix). The
// route should follow at the same cadence — mirrors /vandispatch, which
// re-draws showVanFullRoute on every vehicleLocation event. Redraw only on a
// meaningful move (>= MIN_MOVE_M) and no more than once per MIN_REDRAW_MS, with
// a trailing redraw so the last position isn't missed.
const MIN_MOVE_M = 8;
const MIN_REDRAW_MS = 2500;
let lastRouteAnchor = null; // { key, lng, lat } at the last redraw
let lastMicroList = [];
let trackCooldownUntil = 0;
let trackTrailingTimer = 0;

/** Reset the tracker (a fresh selection draws + frames via onSelectionChange). */
function resetRouteTracking() {
  lastRouteAnchor = null;
  trackCooldownUntil = 0;
  if (trackTrailingTimer) {
    clearTimeout(trackTrailingTimer);
    trackTrailingTimer = 0;
  }
}

function trackSelectedVan() {
  const sel = getSelected();
  if (!sel || !sel.id) {
    resetRouteTracking();
    return;
  }
  const wantId = (sel.source === 'spare' ? 'sp:' : 'od:') + sel.id;
  const v = lastMicroList.find((x) => x.id === wantId);
  if (!v || !Number.isFinite(v.lng) || !Number.isFinite(v.lat)) return;
  const key = `${sel.source}:${sel.id}`;
  const cur = [v.lng, v.lat];
  if (
    lastRouteAnchor &&
    lastRouteAnchor.key === key &&
    haversineM([lastRouteAnchor.lng, lastRouteAnchor.lat], cur) < MIN_MOVE_M
  ) {
    return; // parked / GPS jitter
  }
  const now = Date.now();
  if (now >= trackCooldownUntil) {
    lastRouteAnchor = { key, lng: v.lng, lat: v.lat };
    trackCooldownUntil = now + MIN_REDRAW_MS;
    redrawSelectedRoute();
  } else if (!trackTrailingTimer) {
    trackTrailingTimer = setTimeout(() => {
      trackTrailingTimer = 0;
      trackSelectedVan();
    }, trackCooldownUntil - now);
  }
}

// ===========================================================================
//  Lifecycle
// ===========================================================================

export function startMapOverlays(theMap) {
  map = theMap;

  onStyleReady(() => {
    // A theme swap rebuilds the style doc — re-feed the sources + re-add markers.
    syncArea();
    restyleArea();
    renderStopMarkers();
    renderIncidents();
    redrawSelectedRoute();
  });

  onThemeChange(() => restyleArea());

  // data.js emits `change` once per fetcher (~6× per poll cycle). Debounce the
  // marker rebuild + route redraw so a poll burst does the work once, not six
  // times (which churned the DOM stop markers and fired six duty-route fetches).
  const onPollSettled = debounce(() => {
    renderStopMarkers();
    redrawSelectedRoute();
  }, 400);
  onChange(onPollSettled);

  onSelectionChange((sel) => {
    resetRouteTracking();
    if (!sel || !sel.id) {
      clearRequestRoute();
      return;
    }
    if (sel.source === 'spare') showVanFullRoute(sel.id);
    else showOdVanRoute(sel.id);
  });

  // A click on empty map clears a route drawn by a trip-card click too (those
  // don't set a selection, so onSelectionChange never fires for them).
  onMapBackground(() => clearRequestRoute());

  // Follow the selected van's live position (Spare SSE via microtransit.js) and
  // keep its route in step — same cadence as the marker, not the 10s panel poll.
  onMicroVehicles((list) => {
    lastMicroList = list || [];
    trackSelectedVan();
  });

  loadServiceArea();
  loadIncidents();
  setInterval(loadServiceArea, 3_600_000);
  setInterval(loadIncidents, 20_000);
}
