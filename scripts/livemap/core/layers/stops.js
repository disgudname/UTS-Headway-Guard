// livemap/core/layers/stops.js
// -----------------------------------------------------------------------------
// UTS stops: a route-coloured pie-chart bead per physical stop (one wedge per
// route serving it — the legacy testmap treatment), clustered when zoomed out,
// with a click popup listing live arrival ETAs per route.
//
// The source + layers are baked into the basemap style doc (see
// core/layers/stop-style.js); this module keeps the source fed, rasterises one
// pie image per distinct colour-set, polls arrival times, and owns the popup.
// A stop is dropped from the source when every route serving it is hidden in
// the route picker.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { getMap, onStyleReady } from '../map.js';
import { lsGet, lsSet } from '../util.js';
import { registerMarkerLayer } from '../marker-menu.js';
import {
  startVehicleFeed,
  onStops,
  getStops,
  onMetadata,
  getRouteColor,
  getRouteName,
} from '../data/transloc.js';
import { isRouteShown, onRouteVisibility } from './routes.js';
import {
  STOP_SOURCE_ID as SRC,
  STOP_POINT_LAYER as POINT,
  STOP_CLUSTER_LAYER as CLUSTER,
  STOP_CLUSTER_COUNT_LAYER as CLUSTER_COUNT,
} from './stop-style.js';

const ARRIVALS_URL = `${API_BASE}/v1/transloc/stop_arrivals`;
const ARRIVALS_POLL_MS = 25_000;
const MAX_ETAS_PER_ROUTE = 3;
const VIS_KEY = 'livemap.stops.visible';

let stopsShown = lsGet(VIS_KEY, '1') !== '0';

let stops = []; // physical stop groups from transloc.js
let arrivals = new Map(); // routeStopId(str) -> { routeId, routeDescription, color, secs: number[] }
let popup = null;
let popupKey = null;

export function installStopLayer() {
  onStyleReady(onStyleRebuilt);
  onStops((list) => {
    stops = list;
    syncSource();
  });
  onMetadata(syncSource); // route colours landed/changed
  onRouteVisibility(() => syncSource()); // picker toggled
  startVehicleFeed(); // idempotent; drives the stop metadata too

  const seed = getStops();
  if (seed.length) {
    stops = seed;
    syncSource();
  }

  pollArrivals();
  setInterval(pollArrivals, ARRIVALS_POLL_MS);
}

// --- style lifecycle --------------------------------------------------------

function onStyleRebuilt() {
  regeneratePies(); // a style swap wipes the image atlas
  syncSource();
  applyStopVisibility(); // setStyle resets layer visibility
  wireInteractions();
}

// --- show / hide ----------------------------------------------------------------

export function areStopsVisible() {
  return stopsShown;
}

export function setStopsVisible(v) {
  stopsShown = !!v;
  lsSet(VIS_KEY, stopsShown ? '1' : '0');
  applyStopVisibility();
}

function applyStopVisibility() {
  const map = getMap();
  if (!map) return;
  const vis = stopsShown ? 'visible' : 'none';
  for (const id of [CLUSTER, CLUSTER_COUNT, POINT]) {
    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
  }
  if (!stopsShown) closePopup();
}

let wired = false;
function wireInteractions() {
  const map = getMap();
  if (!map || wired) return;
  wired = true;

  map.on('styleimagemissing', (e) => {
    if (!e.id) return;
    if (e.id.startsWith('livemap-pie-')) {
      putPie(e.id.slice('livemap-pie-'.length).split('_'));
    } else if (e.id.startsWith('livemap-count-')) {
      putCount(e.id.slice('livemap-count-'.length));
    }
  });

  // Stop-bead clicks route through the shared marker menu (so a click that also
  // lands on a bus / another stop offers a pick). A cluster is still its own
  // "zoom to expand" click.
  registerMarkerLayer({
    layer: POINT,
    resolve: (f) => {
      const s = stops.find((x) => x.key === f.properties.key);
      if (!s) return null;
      return {
        key: `stop:${s.key}`,
        label: s.name,
        colors: visibleColors(s),
        open: () => openPopup(s.key),
      };
    },
  });

  map.on('click', CLUSTER, (e) => {
    const f = e.features && e.features[0];
    const src = map.getSource(SRC);
    if (!f || !src || !src.getClusterExpansionZoom) return;
    src.getClusterExpansionZoom(f.properties.cluster_id, (err, zoom) => {
      if (err) return;
      map.easeTo({ center: f.geometry.coordinates, zoom: zoom + 0.4, duration: 500 });
    });
  });
  for (const layer of [POINT, CLUSTER]) {
    map.on('mouseenter', layer, () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', layer, () => {
      map.getCanvas().style.cursor = '';
    });
  }
}

// --- source ---------------------------------------------------------------

/** Distinct route colours for this stop's *shown* routes, in route order. A
 *  stop with no shown route is dropped from the source entirely (see
 *  syncSource) — stops track the route selector, not just the hidden set. */
function visibleColors(stop) {
  const out = [];
  for (const rid of stop.routeIds) {
    if (!isRouteShown(rid)) continue;
    const c = getRouteColor(rid);
    if (!out.includes(c)) out.push(c);
  }
  return out;
}

function syncSource() {
  const map = getMap();
  const src = map && map.getSource(SRC);
  if (!src) return;

  const features = [];
  for (const s of stops) {
    const colors = visibleColors(s);
    if (!colors.length) continue; // every route here is hidden
    ensurePie(colors);
    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [s.lng, s.lat] },
      properties: {
        key: s.key,
        name: s.name,
        pie: pieImageId(colors),
        routeIds: s.routeIds.join(','),
      },
    });
  }
  src.setData({ type: 'FeatureCollection', features });

  if (popupKey) refreshPopup();
}

// --- pie icons ------------------------------------------------------------------
// One rasterised pie per distinct colour-set: a white disc, a thin dark ring,
// then N equal wedges (1 colour -> a solid fill). Cached by colour-set.

const DPR = 2;
const PIE_PX = 26 * DPR; // logical 26px; icon-size scales from there
const pieSets = new Map(); // pieId -> colors[]

function pieImageId(colors) {
  return `livemap-pie-${colors.join('_')}`;
}

function ensurePie(colors) {
  const id = pieImageId(colors);
  if (pieSets.has(id) && getMap()?.hasImage(id)) return;
  pieSets.set(id, colors.slice());
  putPie(colors);
}

function regeneratePies() {
  for (const colors of pieSets.values()) putPie(colors);
}

function putPie(colors) {
  const map = getMap();
  if (!map || !colors || !colors.length) return;
  const id = pieImageId(colors);
  try {
    const img = drawPie(colors);
    if (map.hasImage(id)) map.updateImage(id, img);
    else map.addImage(id, img, { pixelRatio: DPR });
  } catch (err) {
    console.warn('[livemap] pie image failed', id, err && err.message);
  }
}

// --- cluster count glyphs -----------------------------------------------------
// Baked as canvas glyphs, not a MapLibre `text-field`: this basemap's SDF glyphs
// intermittently fail to parse in the worker, and the local-font fallback
// mis-centres the digits. Drawn on demand by the styleimagemissing handler,
// centred on the glyph's true ink box.

const COUNT_PX = 26 * DPR;
const COUNT_FONT = '"Libre Franklin", system-ui, "Segoe UI", Roboto, sans-serif';

function drawCount(text) {
  const cv = document.createElement('canvas');
  cv.width = COUNT_PX;
  cv.height = COUNT_PX;
  const g = cv.getContext('2d');
  const fpx = 13 * DPR;
  g.font = `700 ${fpx}px ${COUNT_FONT}`;
  g.textAlign = 'left';
  g.textBaseline = 'alphabetic';
  const s = String(text);
  const m = g.measureText(s);
  const l = m.actualBoundingBoxLeft ?? 0;
  const r = m.actualBoundingBoxRight ?? m.width;
  const asc = m.actualBoundingBoxAscent ?? fpx * 0.72;
  const desc = m.actualBoundingBoxDescent ?? 0;
  const x = (COUNT_PX - (l + r)) / 2 + l;
  const y = (COUNT_PX - (asc + desc)) / 2 + asc;
  g.lineJoin = 'round';
  g.lineWidth = 2.5 * DPR;
  g.strokeStyle = 'rgba(11, 15, 24, 0.55)'; // faint edge against the navy disc
  g.strokeText(s, x, y);
  g.fillStyle = '#ffffff';
  g.fillText(s, x, y);
  return g.getImageData(0, 0, COUNT_PX, COUNT_PX);
}

function putCount(text) {
  const map = getMap();
  if (!map || !text) return;
  const id = `livemap-count-${text}`;
  try {
    const img = drawCount(text);
    if (map.hasImage(id)) map.updateImage(id, img);
    else map.addImage(id, img, { pixelRatio: DPR });
  } catch (err) {
    console.warn('[livemap] cluster count image failed', id, err && err.message);
  }
}

function drawPie(colors) {
  const cv = document.createElement('canvas');
  cv.width = PIE_PX;
  cv.height = PIE_PX;
  const g = cv.getContext('2d');
  const c = PIE_PX / 2;
  const outer = c - 1 * DPR; // white disc
  const ring = outer - 1.5 * DPR; // dark keyline
  const fill = ring - 1 * DPR; // wedge radius

  g.beginPath();
  g.arc(c, c, outer, 0, Math.PI * 2);
  g.fillStyle = '#ffffff';
  g.fill();

  g.beginPath();
  g.arc(c, c, ring, 0, Math.PI * 2);
  g.fillStyle = '#1f2937';
  g.fill();

  if (colors.length === 1) {
    g.beginPath();
    g.arc(c, c, fill, 0, Math.PI * 2);
    g.fillStyle = colors[0];
    g.fill();
  } else {
    const step = (Math.PI * 2) / colors.length;
    let a = -Math.PI / 2;
    for (const color of colors) {
      g.beginPath();
      g.moveTo(c, c);
      g.arc(c, c, fill, a, a + step);
      g.closePath();
      g.fillStyle = color;
      g.fill();
      a += step;
    }
  }

  return g.getImageData(0, 0, PIE_PX, PIE_PX);
}

// --- arrivals -----------------------------------------------------------------

async function pollArrivals() {
  try {
    const r = await fetch(ARRIVALS_URL, { cache: 'no-store' });
    if (!r.ok) throw new Error(`arrivals HTTP ${r.status}`);
    const data = await r.json();
    const list = Array.isArray(data) ? data : Array.isArray(data?.Arrivals) ? data.Arrivals : [];
    const next = new Map();
    for (const entry of list) {
      const rsid = String(entry.RouteStopID ?? entry.RouteStopId ?? '');
      if (!rsid) continue;
      const secs = [];
      for (const t of entry.Times || []) {
        const n = Number(t.Seconds);
        if (Number.isFinite(n)) secs.push(Math.max(0, n));
      }
      secs.sort((a, b) => a - b);
      next.set(rsid, {
        routeId: String(entry.RouteID ?? entry.RouteId ?? ''),
        routeDescription: entry.RouteDescription || '',
        color: entry.Color || '',
        secs: secs.slice(0, MAX_ETAS_PER_ROUTE),
      });
    }
    arrivals = next;
    if (popupKey) refreshPopup();
  } catch (err) {
    console.warn('[livemap] stop arrivals poll failed', err);
  }
}

function etaLabel(sec) {
  const m = Math.round(sec / 60);
  if (m <= 0) return 'Due';
  return `${m} min`;
}

// --- popup ----------------------------------------------------------------------

function openPopup(key, lngLat) {
  const stop = stops.find((s) => s.key === key);
  if (!stop) return;
  closePopup();
  popupKey = key;
  popup = new maplibregl.Popup({
    offset: 12,
    closeButton: true,
    className: 'livemap-stop-popup',
    maxWidth: '290px',
  })
    .setLngLat(lngLat || [stop.lng, stop.lat])
    .setHTML(popupHTML(stop))
    .addTo(getMap());
  popup.on('close', () => {
    if (popupKey === key) {
      popupKey = null;
      popup = null;
    }
  });
}

function closePopup() {
  if (popup) popup.remove();
  popup = null;
  popupKey = null;
}

function refreshPopup() {
  const stop = stops.find((s) => s.key === popupKey);
  if (!popup || !stop) return;
  popup.setHTML(popupHTML(stop));
}

function popupHTML(stop) {
  // Gather ETAs per route from this physical stop's member route-stops.
  const rows = [];
  for (const m of stop.members) {
    if (!isRouteShown(m.routeId)) continue;
    const a = arrivals.get(String(m.routeStopId));
    const routeId = a?.routeId || m.routeId;
    const name = a?.routeDescription || getRouteName(routeId) || `Route ${routeId}`;
    const color = a?.color || getRouteColor(routeId);
    const etas = a && a.secs.length ? a.secs.map(etaLabel).join(', ') : '—';
    rows.push({ name, color, etas, has: !!(a && a.secs.length) });
  }
  // De-dupe by route name (a stop can list the same line twice).
  const seen = new Set();
  const uniq = rows.filter((r) => (seen.has(r.name) ? false : seen.add(r.name)));
  uniq.sort((a, b) => Number(b.has) - Number(a.has) || a.name.localeCompare(b.name));

  const body = uniq.length
    ? uniq
        .map(
          (r) => `
      <div class="ls-row">
        <span class="ls-sw" style="background:${escapeAttr(r.color)}"></span>
        <span class="ls-route">${escapeHTML(r.name)}</span>
        <span class="ls-eta${r.has ? '' : ' is-none'}">${escapeHTML(r.etas)}</span>
      </div>`,
        )
        .join('')
    : '<div class="ls-empty">No routes shown here</div>';

  return `
    <div class="ls-pop">
      <div class="ls-name">${escapeHTML(stop.name)}</div>
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
