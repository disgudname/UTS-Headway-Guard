// livemap/core/layers/vehicles.js
// -----------------------------------------------------------------------------
// Live buses drawn as a GPU layer (one GeoJSON source, an icon layer, a label
// layer). Positions ease between reports on a requestAnimationFrame loop so
// movement reads as smooth rather than teleporting every ~10s.
//
// Both agencies render here: UTS from data/transloc.js and — when the CAT
// overlay is on — CAT from data/cat.js, with identical markers (the two read
// apart by route colour, the dashed CAT route lines, and a "CAT" tag in the
// popup). State is keyed by a per-agency id prefix so one feed's update never
// prunes the other's buses.
//
// Markers:
//   moving bus  -> route-coloured rounded arrow, rotated to heading
//   stopped bus -> route-coloured rounded square (no rotation)
//   stale fix   -> same, drawn desaturated
// Click a bus for a popup (route, number, speed, occupancy, fix age) with a
// Follow toggle that locks the camera to it until you pan away.
// -----------------------------------------------------------------------------

import { VEHICLE_ANIM_MS, VEHICLE_STALE_S } from '../config.js';
import { getMap, onStyleReady } from '../map.js';
import { clamp, lsGet, lsSet } from '../util.js';
import {
  startVehicleFeed,
  onVehicles,
  onMetadata,
  getRouteColor,
  getRouteName,
} from '../data/transloc.js';
import { isDispatcher, onDispatcher, startSession } from '../data/session.js';
import {
  getBlock,
  getDrivers,
  getDriverShift,
  onDispatchData,
  startDispatchFeed,
} from '../data/dispatch.js';
import {
  onCatVehicles,
  onCatEnabled,
  isCatEnabled,
  onCatRouteVisibility,
  isCatRouteHidden,
} from '../data/cat.js';
import {
  onMicroVehicles,
  startMicrotransitFeed,
  vanManifest,
} from '../data/microtransit.js';
import { isRouteHidden, onRouteVisibility } from './routes.js';
import {
  VEHICLE_SOURCE_ID as SRC,
  VEHICLE_PIN_LAYER as HIT_LAYER,
} from './vehicle-style.js';
import { registerMarkerLayer } from '../marker-menu.js';

const UTS_PREFIX = 'uts:';
const CAT_PREFIX = 'cat:';
const MICRO_PREFIX = 'micro:'; // On-Demand + Spare (dispatcher-only)

// Last raw list + adapter per feed, so a route-picker change can re-run ingest
// and drop/restore that agency's now-hidden/shown buses without a fresh poll.
const feeds = new Map(); // prefix -> { list, adapt }

const LABELS_KEY = 'livemap.vehicles.labels';
let labelsShown = lsGet(LABELS_KEY, '1') !== '0';

const state = new Map(); // id -> anim record
let rafId = 0;
let followId = null;
let programmaticMove = false;
let popup = null;
let popupId = null;
let followChip = null;

// --- public -----------------------------------------------------------------

export function installVehicleLayer() {
  onStyleReady(onStyleRebuilt);
  onVehicles((list) => ingest(list, UTS_PREFIX, fromUts));
  onCatVehicles((list) => ingest(isCatEnabled() ? list : [], CAT_PREFIX, fromCat));
  onCatEnabled((on) => {
    if (!on) ingest([], CAT_PREFIX, fromCat); // overlay off -> drop CAT buses
  });
  onMicroVehicles((list) => ingest(list, MICRO_PREFIX, fromMicro)); // [] when off/unauthed
  // Route picker toggled: re-run the last ingest so hidden routes' buses go away
  // (and come back) without waiting for the next feed tick.
  onRouteVisibility(() => reingest(UTS_PREFIX));
  onCatRouteVisibility(() => reingest(CAT_PREFIX));
  onMetadata(() => {
    // Route colours arrived/changed: regenerate images and repaint.
    regenerateImages();
    scheduleFrame();
  });
  const rederiveAll = () => {
    // Dispatcher flip: nameLabel (block vs number) changes and the pill/bare
    // choice changes. Drop cached images, recompute props, repaint.
    regenerateImages();
    for (const prefix of feeds.keys()) reingest(prefix);
    scheduleFrame();
  };
  // Dispatcher status flipped (block vs number pill) or the block/driver
  // mapping refreshed — re-derive every marker and repaint.
  onDispatcher(rederiveAll);
  onDispatchData(rederiveAll);

  ensureFollowChip();
  loadPinSvg().catch(() => {}); // warm the SVG fetch
  startSession();
  startDispatchFeed();
  startVehicleFeed();
  startMicrotransitFeed(); // no-op output until a dispatcher enables the overlay
}

export function followVehicle(id) {
  const key = String(id);
  if (!state.has(key)) return false;
  followId = key;
  updateFollowChip();
  scheduleFrame();
  return true;
}

export function stopFollow() {
  if (!followId) return;
  followId = null;
  updateFollowChip();
}

export function getVehicleIds() {
  return [...state.keys()];
}

/**
 * A flat snapshot of every currently-tracked vehicle, for the search box.
 * `id` is the prefixed state key (pass it straight to followVehicle).
 */
export function listVehicles() {
  const out = [];
  for (const [id, s] of state) {
    const p = s.props || {};
    out.push({
      id,
      label: p.label || '',
      routeName: p.routeName || '',
      routeColor: p.routeColor || '',
      block: p.block || '',
      agency: p.agency || 'uts',
      lng: s.lng,
      lat: s.lat,
    });
  }
  return out;
}

/**
 * Follow a vehicle referenced loosely — by its number/name, its block, or a
 * raw/prefixed state id. Used by the dispatcher bridge (`?bus=`, focusBus
 * postMessage). Returns true once a match is found and followed.
 */
export function followByRef(ref) {
  const want = String(ref == null ? '' : ref).trim().toLowerCase();
  if (!want) return false;
  const norm = (s) => String(s == null ? '' : s).trim().toLowerCase();
  for (const [key, s] of state) {
    const p = s.props || {};
    if (
      norm(key) === want ||
      norm(key.replace(/^[a-z]+:/, '')) === want ||
      norm(p.label) === want ||
      norm(p.block) === want ||
      norm(p.block).replace(/[[\]]/g, '') === want.replace(/[[\]]/g, '')
    ) {
      followId = key;
      updateFollowChip();
      scheduleFrame();
      return true;
    }
  }
  return false;
}

// --- style lifecycle ------------------------------------------------------------

// The source + symbol layers are baked into the style document (see
// core/layers/vehicle-style.js). After the first load and after every theme
// swap we only need to re-generate the marker images (a style swap wipes them)
// and repaint.
function onStyleRebuilt(map) {
  regenerateImages(); // atlas wiped: re-raster pins, drop composite cache
  wireInteractions(map);
  render(); // rebuilds features + any composites the new frame needs
}

// --- show / hide the number + speed pills -------------------------------------
//
// Parity with the legacy testmap: the number/speed bubbles are a dispatcher-only
// overlay. A public viewer (and every kiosk / embed / ?adminMode=false view)
// gets bare route-coloured markers, no bubbles — regardless of the "Bus labels"
// toggle, which only does anything once you're signed in.

export function areLabelsVisible() {
  return labelsShown;
}

export function setLabelsVisible(v) {
  labelsShown = !!v;
  lsSet(LABELS_KEY, labelsShown ? '1' : '0');
  // Pills live inside the composite marker image now; toggling them just changes
  // which image each feature asks for on the next sync.
  scheduleFrame();
}

/** Dispatcher, signed in, with the "Vehicle labels" toggle on. */
function pillsWanted() {
  return labelsShown && isDispatcher();
}

let imageMissingWired = false;
function wireImageMissing(map) {
  if (imageMissingWired) return;
  imageMissingWired = true;
  map.on('styleimagemissing', (e) => {
    const pin = /^livemap-pin-(#[0-9a-f]{6})-(s|m)(d?)$/i.exec(e.id);
    if (pin) {
      ensureMarker(pin[1]);
      return;
    }
    const spec = compositeSpecs.get(e.id);
    if (spec) rebuildComposite(e.id, spec);
  });
}

let interactionsWired = false;
function wireInteractions(map) {
  wireImageMissing(map);
  if (interactionsWired) return;
  interactionsWired = true;

  // Bus clicks route through the shared marker menu (so an overlapping bus /
  // stop offers a pick rather than one popup winning arbitrarily).
  registerMarkerLayer({
    layer: HIT_LAYER,
    resolve: (f) => {
      const s = state.get(f.properties.id);
      if (!s) return null;
      return {
        key: `veh:${f.properties.id}`,
        ...chipText(s.props),
        color: s.props.routeColor,
        open: () => openPopup(f.properties.id),
      };
    },
  });
  map.on('mouseenter', HIT_LAYER, () => {
    map.getCanvas().style.cursor = 'pointer';
  });
  map.on('mouseleave', HIT_LAYER, () => {
    map.getCanvas().style.cursor = '';
  });
  // Any real user camera gesture releases the follow lock — pan, wheel / pinch
  // zoom, right-drag rotate, tilt. A user gesture carries `originalEvent` (the
  // DOM event); our own setCenter / flyTo don't, so following isn't self-
  // released. Zoom counts now too: previously you could scroll-zoom while still
  // locked and the camera kept yanking back to the bus.
  const releaseOnGesture = (e) => {
    if (followId && !programmaticMove && e && e.originalEvent) stopFollow();
  };
  for (const ev of ['dragstart', 'zoomstart', 'rotatestart', 'pitchstart']) {
    map.on(ev, releaseOnGesture);
  }
}

// --- data ingest + animation -----------------------------------------------

// Each feed's raw vehicle -> the common shape deriveProps/render work from.
function fromUts(v) {
  return {
    id: v.id,
    routeId: v.routeId,
    lng: v.lng,
    lat: v.lat,
    heading: v.heading,
    routeColor: getRouteColor(v.routeId),
    routeName: v.routeName || getRouteName(v.routeId) || 'Not in service',
    label: String(v.name),
    block: getBlock(v.id),
    drivers: getDrivers(v.id),
    speedMph: v.speedMph,
    dim: v.stale || v.ageS > VEHICLE_STALE_S,
    agency: 'uts',
    occupancy: v.occupancy,
    onboard: v.onboard,
    capacity: v.capacity,
    ageS: v.ageS,
  };
}

function fromCat(v) {
  const spd = Number(v.speed) || 0;
  return {
    id: v.id,
    routeId: v.routeId || '',
    lng: v.lng,
    lat: v.lat,
    heading: Number(v.heading) || 0,
    routeColor: v.color,
    routeName: v.routeName || 'CAT',
    label: String(v.label),
    speedMph: spd,
    // CAT sends no speed/heading — cat.js derives both from movement between
    // polls, so the number is an estimate (shown with a ~).
    speedEstimated: !!v.speedEstimated,
    dim: false, // the CAT feed already drops stale units
    agency: 'cat',
    occupancy: null,
    onboard: null,
    capacity: null,
    ageS: null,
  };
}

function fromMicro(v) {
  const spd = Number(v.speedMph) || 0;
  const shift = v.driver ? getDriverShift(v.driver) : null;
  return {
    id: v.id,
    routeId: '',
    lng: v.lng,
    lat: v.lat,
    heading: Number(v.heading) || 0,
    routeColor: v.color,
    routeName: v.source === 'spare' ? 'UVA FlexRide' : 'UVA Ride',
    label: String(v.label),
    speedMph: spd,
    speedEstimated: false,
    dim: !!v.stale,
    agency: v.source === 'spare' ? 'spare' : 'ondemand',
    spareId: v.source === 'spare' ? String(v.id || '').replace(/^sp:/, '') : '',
    // rich-card fields
    driver: v.driver || '',
    driverShift: shift ? `${shift.start}–${shift.end}` : '',
    descr: v.descr || '',
    plate: v.plate || '',
    seats: v.seats || 0,
    access: Array.isArray(v.access) ? v.access : [],
    occupancy: null,
    onboard: null,
    capacity: null,
    ageS: null,
  };
}

/**
 * A "real" block is the bracketed kind ("[04]", "[01]/[04]"). The dispatch feed
 * sometimes drops a plain-language W2W position ("Training", "Charter") into the
 * same field — those are not blocks and don't belong on the marker pill / chip
 * (the vehicle number is shown instead). They still appear in the popup.
 */
function isBlockValue(block) {
  return /\[\s*\d/.test(String(block || ''));
}

/** How a unit of this agency is named in popups / menu chips / search. */
export function unitNoun(agency) {
  return agency === 'cat'
    ? 'CAT bus'
    : agency === 'spare'
      ? 'Van'
      : agency === 'ondemand'
        ? 'Vehicle'
        : 'Bus';
}

/**
 * A human label for one unit: "Bus 1234", "CAT bus 403", "Van 4". Some feeds
 * already bake a worded name into the number ("Van 4", "BUS 241") — when the
 * raw label carries letters, trust it as-is rather than doubling the noun.
 */
export function unitDisplayName(agency, label) {
  const l = String(label == null ? '' : label).trim();
  if (!l) return unitNoun(agency);
  return /[a-z]/i.test(l) ? l : `${unitNoun(agency)} ${l}`;
}

/**
 * Primary / secondary text for a radial-menu chip, by audience:
 *   public           -> route name        + "<noun> <number>"
 *   dispatcher, block -> block             + "<noun> <number>"
 *   dispatcher, none  -> "<noun> <number>" + route name
 * The block value is shown verbatim — in UTS-speak the brackets ARE "block"
 * ("[04]" is read "block four"), so no "Block" prefix. Blocks are UTS-only.
 */
function chipText(p) {
  const noun = unitNoun(p.agency);
  const unit = `${noun} ${p.label}`;
  if (p.agency === 'uts' && isDispatcher()) {
    return isBlockValue(p.block)
      ? { label: p.block, sublabel: unit }
      : { label: unit, sublabel: p.routeName || 'Not in service' };
  }
  return { label: p.routeName || 'Not in service', sublabel: unit };
}

/** True if this feed's bus belongs to a route the picker has switched off.
 *  Only UTS + CAT have route pickers; a blank routeId is never hidden. */
function routeHiddenFor(prefix, routeId) {
  if (!routeId) return false;
  if (prefix === CAT_PREFIX) return isCatRouteHidden(routeId);
  if (prefix === UTS_PREFIX) return isRouteHidden(routeId);
  return false;
}

/** Re-run the last ingest for one feed (after a route-picker change). */
function reingest(prefix) {
  const f = feeds.get(prefix);
  if (f) ingest(f.list, prefix, f.adapt);
}

/** Merge one feed's vehicles into `state`. `prefix` namespaces the ids so this
 *  feed's absence never prunes the other agency's buses. */
function ingest(rawList, prefix, adapt) {
  const now = performance.now();
  const seen = new Set();
  feeds.set(prefix, { list: Array.isArray(rawList) ? rawList : [], adapt });

  const showOOS = isDispatcher(); // out-of-service buses are staff-only

  for (const raw of Array.isArray(rawList) ? rawList : []) {
    const v = adapt(raw);
    if (routeHiddenFor(prefix, v.routeId)) continue; // route switched off in the picker
    // Hide no-route / out-of-service buses from the public (both agencies).
    if (
      !showOOS &&
      (prefix === UTS_PREFIX || prefix === CAT_PREFIX) &&
      (!v.routeId || v.routeId === '0' || v.routeName === 'Not in service')
    ) {
      continue;
    }
    const id = prefix + v.id;
    seen.add(id);
    const props = deriveProps(v);
    ensureMarker(props.routeColor);

    let s = state.get(id);
    if (!s) {
      s = {
        lng: v.lng, lat: v.lat, heading: v.heading,
        fromLng: v.lng, fromLat: v.lat, fromHeading: v.heading,
        toLng: v.lng, toLat: v.lat, toHeading: v.heading,
        startT: now, endT: now, props,
      };
      state.set(id, s);
    } else {
      s.fromLng = s.lng; s.fromLat = s.lat; s.fromHeading = s.heading;
      s.toLng = v.lng; s.toLat = v.lat; s.toHeading = v.heading;
      s.startT = now;
      s.endT = now + VEHICLE_ANIM_MS;
      s.props = props;
    }
  }

  for (const id of [...state.keys()]) {
    if (id.startsWith(prefix) && !seen.has(id)) {
      state.delete(id);
      if (followId === id) stopFollow();
      if (popupId === id) closePopup();
    }
  }

  if (popupId && state.has(popupId)) refreshPopupContent();
  scheduleFrame();
}

function deriveProps(v) {
  const stopped = v.speedMph < 1.2;
  const mph = Math.max(0, Math.round(v.speedMph));
  const block = v.block || '';
  // Dispatchers think in blocks: when signed in, the name pill carries the
  // bracketed block ("[04]"), falling back to the vehicle number when the bus
  // has no block — or only a plain-language one like "Training"/"Charter",
  // which stays in the popup, not on the pill. The public always sees the number.
  const dispatch = isDispatcher();
  const nameLabel = dispatch && isBlockValue(block) ? block : v.label;
  return {
    routeColor: v.routeColor,
    routeName: v.routeName,
    label: v.label, // always the vehicle number
    block,
    drivers: Array.isArray(v.drivers) ? v.drivers : [],
    nameLabel, // what the pill actually draws
    // microtransit rich-card fields (unset for UTS/CAT)
    spareId: v.spareId || '',
    driver: v.driver || '',
    driverShift: v.driverShift || '',
    descr: v.descr || '',
    plate: v.plate || '',
    seats: v.seats || 0,
    access: Array.isArray(v.access) ? v.access : [],
    speedLabel: `${v.speedEstimated && !stopped ? '~' : ''}${mph} MPH`,
    stopped,
    dim: v.dim,
    agency: v.agency,
    speedMph: v.speedMph,
    speedEstimated: !!v.speedEstimated,
    occupancy: v.occupancy,
    onboard: v.onboard,
    capacity: v.capacity,
    ageS: v.ageS,
  };
}

function scheduleFrame() {
  if (!rafId) rafId = requestAnimationFrame(tick);
}

function tick() {
  rafId = 0;
  render();
}

// setData re-tiles the whole GeoJSON on the worker; calling it at 60fps starves
// the worker (and on a busy one can corrupt its output — "Unimplemented type: 3").
// The camera/popup still update every frame; the source syncs at ~20fps.
const SOURCE_SYNC_MS = 55;
let lastSyncAt = 0;
let syncQueued = false;

function render() {
  const map = getMap();
  if (!map) return;

  const now = performance.now();
  let animating = false;

  for (const [, s] of state) {
    const span = s.endT - s.startT;
    const t = span > 0 ? clamp((now - s.startT) / span, 0, 1) : 1;
    const e = easeOutCubic(t);
    s.lng = lerp(s.fromLng, s.toLng, e);
    s.lat = lerp(s.fromLat, s.toLat, e);
    s.heading = lerpAngle(s.fromHeading, s.toHeading, e);
    if (t < 1) animating = true;
  }

  syncSource(now, animating);

  if (followId && state.has(followId)) {
    const s = state.get(followId);
    programmaticMove = true;
    map.setCenter([s.lng, s.lat]);
    programmaticMove = false;
  }
  if (popupId && popup && state.has(popupId)) {
    const s = state.get(popupId);
    popup.setLngLat([s.lng, s.lat]);
  }

  if (animating || followId || syncQueued) rafId = requestAnimationFrame(tick);
}

/** Push the current interpolated positions to the GeoJSON source, throttled. */
function syncSource(now, animating) {
  const map = getMap();
  const src = map && map.getSource(SRC);
  if (!src) return;

  if (animating && now - lastSyncAt < SOURCE_SYNC_MS) {
    syncQueued = true; // a frame is already scheduled; it'll retry
    return;
  }
  syncQueued = false;
  lastSyncAt = now;

  const wantPills = pillsWanted();
  const features = [];
  for (const [id, s] of state) {
    const p = s.props;

    // Dispatcher with labels on -> one composite image (pin + both pills, pin
    // baked pre-rotated). Everyone else -> the bare teardrop, rotated by the
    // layer. `ensureComposite` returns null until this colour's pin has
    // rasterised, so we fall back to the bare pin for a frame or two.
    let icon = null;
    let iconRotate = 0;
    if (wantPills) {
      icon = ensureComposite(p.routeColor, p.stopped, p.dim, s.heading, p.nameLabel, p.speedLabel);
    }
    if (!icon) {
      icon = pinImageId(p.routeColor, p.stopped, p.dim);
      iconRotate = s.heading;
    }

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [s.lng, s.lat] },
      properties: {
        id,
        icon,
        iconRotate,
        // Whole-marker stacking: southernmost vehicle's image on top (Leaflet-style).
        sortKey: Math.round((40 - s.lat) * 1e6),
      },
    });
  }
  src.setData({ type: 'FeatureCollection', features });
}

// --- marker images (rasterised from media/busmarker.svg) --------------------

const DPR = 2; // rasterise at 2x for retina crispness
const PIN_W = 40 * DPR; // logical pin box; SVG viewBox is 52.99 x 86.99
const PIN_H = Math.round((86.99 / 52.99) * PIN_W);
const PIN_ROUTE_HEX = '#0b7a26'; // the placeholder fill in busmarker.svg's <style>

const pinSpecs = new Map(); // pinId -> { color, stopped, dim }
const pinInFlight = new Set(); // pinIds currently rasterising
const pinCanvasCache = new Map(); // pinId -> HTMLCanvasElement (PIN_W x PIN_H), for compositing
let pinSvgPromise = null;

function pinImageId(color, stopped, dim) {
  return `livemap-pin-${color}-${stopped ? 's' : 'm'}${dim ? 'd' : ''}`;
}

// Tight opaque-pixel extents of the teardrop from the pin canvas's centre
// (canvas px, 2x): how far the *visible* pin reaches up / down / sideways. Used
// to seat the pills right against the marker instead of against its (padded,
// rotation-swept) bounding box. pinId -> { up, down, half }.
const pinInkCache = new Map();

/** The rasterised teardrop canvas for a variant, or null if not ready yet. */
function pinCanvasFor(color, stopped, dim) {
  return pinCanvasCache.get(pinImageId(color, stopped, dim)) || null;
}

/** Scan a pin canvas's alpha for the tight ink box, as offsets from centre. */
function computeInk(cv) {
  const w = cv.width;
  const h = cv.height;
  const { data } = cv.getContext('2d').getImageData(0, 0, w, h);
  let minX = w;
  let minY = h;
  let maxX = 0;
  let maxY = 0;
  let found = false;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      if (data[(y * w + x) * 4 + 3] > 8) {
        found = true;
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (!found) return { up: h / 2, down: h / 2, half: w / 2 };
  const cx = w / 2;
  const cy = h / 2;
  return {
    up: cy - minY,
    down: maxY - cy,
    half: Math.max(cx - minX, maxX - cx),
  };
}

function loadPinSvg() {
  if (!pinSvgPromise) {
    pinSvgPromise = fetch('/busmarker.svg')
      .then((r) => {
        if (!r.ok) throw new Error(`busmarker.svg HTTP ${r.status}`);
        return r.text();
      })
      .catch((err) => {
        pinSvgPromise = null;
        throw err;
      });
  }
  return pinSvgPromise;
}

/**
 * Ensure the pin images for this route colour exist. Rasterising is async (the
 * SVG has to decode), so if we only built the exact variant asked for, the very
 * frame a bus crosses the stopped/stale threshold would reference an image
 * that isn't in the atlas yet and MapLibre would drop the whole symbol for a
 * beat — the "briefly blinks out" flicker. Building all four
 * moving/stopped × live/stale variants up front (once per colour) means a state
 * flip never hits a missing image.
 */
function ensureMarker(color) {
  for (const stopped of [false, true]) {
    for (const dim of [false, true]) {
      const id = pinImageId(color, stopped, dim);
      if (pinSpecs.has(id) && !pinInFlight.has(id)) continue;
      pinSpecs.set(id, { color, stopped, dim });
      rasterisePin(id, color, stopped, dim);
    }
  }
}

/** Re-rasterise every known pin (a style swap wipes the image atlas) and drop
 *  the composite bookkeeping so syncSource rebuilds those lazily. */
function regenerateImages() {
  pinCanvasCache.clear();
  pinInkCache.clear();
  for (const [id, spec] of pinSpecs) rasterisePin(id, spec.color, spec.stopped, spec.dim);
  compositeSpecs.clear();
  compositeLru.length = 0;
}

// --- composite marker (pin + pills baked into one image) -------------------
// The dispatcher marker is a single image: the teardrop (drawn already rotated
// to a 15-degree heading bucket) with the number pill above and the speed pill
// below. One image per (colour, stopped, stale, headingBucket, nameText,
// speedText). Because it's ONE image on ONE layer, `symbol-sort-key` stacks the
// whole marker as a unit — no more pills from one bus drawing over another's
// pin. Cached with an LRU; a style swap clears it (regenerateImages).

const COMPOSITE_HEADING_STEP = 15; // degrees per baked-rotation bucket
const COMPOSITE_MAX = 200; // atlas-friendly cap; LRU-evicted beyond this
const compositeSpecs = new Map(); // id -> { color, stopped, dim, bucket, nameText, speedText }
const compositeLru = []; // ids, oldest first

// Clearance (composite-canvas px, 2x) from the pin's centre to the nearest edge
// of each pill: rotate the teardrop's *tight ink box* by the heading and take
// how far it actually reaches straight up / straight down, plus a small margin.
// This seats the pills against the visible marker at every heading, instead of
// against a padded box that balloons at diagonal angles.
const PILL_MARGIN_PX = 9;
const DEFAULT_INK = { up: PIN_H * 0.42, down: PIN_H * 0.42, half: PIN_W * 0.34 };

function pillGapsPx(headingDeg, ink) {
  const k = ink || DEFAULT_INK;
  const r = ((Number(headingDeg) || 0) * Math.PI) / 180;
  const si = Math.sin(r);
  const co = Math.cos(r);
  let minY = Infinity;
  let maxY = -Infinity;
  for (const px of [-k.half, k.half]) {
    for (const py of [-k.up, k.down]) {
      const wy = px * si + py * co; // canvas ctx.rotate maps (px,py).y -> px·sin + py·cos
      if (wy < minY) minY = wy;
      if (wy > maxY) maxY = wy;
    }
  }
  return { up: -minY + PILL_MARGIN_PX, down: maxY + PILL_MARGIN_PX };
}

function compositeId(color, stopped, dim, bucket, nameText, speedText) {
  const t = (s) => String(s || '').replace(/[^A-Za-z0-9]+/g, '_');
  return `livemap-vhc-${color}-${stopped ? 's' : 'm'}${dim ? 'd' : ''}-${bucket}-${t(nameText)}-${t(speedText)}`;
}

function lruAdd(id) {
  const i = compositeLru.indexOf(id);
  if (i >= 0) compositeLru.splice(i, 1);
  compositeLru.push(id);
  while (compositeLru.length > COMPOSITE_MAX) {
    const old = compositeLru.shift();
    compositeSpecs.delete(old);
    try { getMap()?.removeImage(old); } catch { /* not present */ }
  }
}

function lruTouch(id) {
  const i = compositeLru.indexOf(id);
  if (i >= 0) { compositeLru.splice(i, 1); compositeLru.push(id); }
}

/** Get (building if needed) the composite image id for this vehicle's current
 *  look. Returns null until the underlying pin has finished rasterising — the
 *  caller falls back to the bare pin for a frame. */
function ensureComposite(color, stopped, dim, headingDeg, nameText, speedText) {
  const step = COMPOSITE_HEADING_STEP;
  const bucket = (((Math.round((Number(headingDeg) || 0) / step) * step) % 360) + 360) % 360;
  const id = compositeId(color, stopped, dim, bucket, nameText, speedText);
  const map = getMap();
  if (!map) return null;
  if (map.hasImage(id)) {
    lruTouch(id);
    return id;
  }
  const pinCv = pinCanvasFor(color, stopped, dim);
  if (!pinCv) return null;
  const spec = {
    color, stopped, dim, bucket,
    nameText: String(nameText || ''),
    speedText: String(speedText || ''),
  };
  compositeSpecs.set(id, spec);
  try {
    buildComposite(id, spec, pinCv);
    lruAdd(id);
  } catch (err) {
    console.warn('[livemap] composite build failed', id, err && err.message);
    compositeSpecs.delete(id);
    return null;
  }
  return id;
}

/** styleimagemissing fallback: redraw a composite whose image the atlas lost. */
function rebuildComposite(id, spec) {
  const pinCv = pinCanvasFor(spec.color, spec.stopped, spec.dim);
  if (!pinCv) return;
  try {
    buildComposite(id, spec, pinCv);
  } catch (err) {
    console.warn('[livemap] composite rebuild failed', id, err && err.message);
  }
}

function buildComposite(id, spec, pinCv) {
  const map = getMap();
  if (!map) return;

  const rad = (spec.bucket * Math.PI) / 180;
  const ink = pinInkCache.get(pinImageId(spec.color, spec.stopped, spec.dim));
  const nameCv = spec.nameText ? pillCanvas(false, spec.color, spec.nameText, spec.dim) : null;
  const speedCv = spec.speedText ? pillCanvas(true, spec.color, spec.speedText, spec.dim) : null;
  const gap = pillGapsPx(spec.bucket, ink);

  const nameW = nameCv ? nameCv.width : 0;
  const nameH = nameCv ? nameCv.height : 0;
  const speedW = speedCv ? speedCv.width : 0;
  const speedH = speedCv ? speedCv.height : 0;

  // Symmetric canvas so icon-anchor:'center' lands the geo point on the pin's
  // centre. The rotated pin sweeps a circle of ~half its ink diagonal.
  const k = ink || DEFAULT_INK;
  const pinReach = Math.ceil(Math.hypot(k.half, Math.max(k.up, k.down))) + 2;
  const topExt = Math.max(pinReach, gap.up + nameH);
  const botExt = Math.max(pinReach, gap.down + speedH);
  const halfW = Math.max(pinReach, nameW / 2, speedW / 2);
  const W = Math.ceil(2 * halfW);
  const H = Math.ceil(2 * Math.max(topExt, botExt));

  const cv = document.createElement('canvas');
  cv.width = W;
  cv.height = H;
  const c = cv.getContext('2d');
  const cx = W / 2;
  const cy = H / 2;

  c.save();
  c.translate(cx, cy);
  c.rotate(rad);
  c.drawImage(pinCv, -PIN_W / 2, -PIN_H / 2);
  c.restore();

  if (nameCv) c.drawImage(nameCv, Math.round(cx - nameW / 2), Math.round(cy - gap.up - nameH));
  if (speedCv) c.drawImage(speedCv, Math.round(cx - speedW / 2), Math.round(cy + gap.down));

  const data = c.getImageData(0, 0, W, H);
  if (map.hasImage(id)) map.updateImage(id, data);
  else map.addImage(id, data, { pixelRatio: DPR });
  scheduleFrame();
}

// --- pills (bubble shape + centred text, drawn onto a canvas) --------------

const LBL_FONT_STACK = '"Libre Franklin", system-ui, "Segoe UI", Roboto, sans-serif';
// Trimmed ~10% from the first composite-marker pass (which was deliberately
// generous) — the block/speed pills were reading heavier than the teardrop.
const LBL_CFG = {
  name: { fontPx: 20 * DPR, h: 35 * DPR, padX: 9 * DPR },
  speed: { fontPx: 16.5 * DPR, h: 29 * DPR, padX: 7.5 * DPR },
};

let _measureCtx = null;
function measureCtx() {
  if (!_measureCtx) _measureCtx = document.createElement('canvas').getContext('2d');
  return _measureCtx;
}

/** A stadium pill with centred text, on its own canvas (for compositing). */
function pillCanvas(small, color, text, dim) {
  const cfg = small ? LBL_CFG.speed : LBL_CFG.name;
  const font = `700 ${cfg.fontPx}px ${LBL_FONT_STACK}`;
  const bg = dim ? mutedColor(color) : color;

  const m = measureCtx();
  m.font = font;
  const textW = Math.ceil(m.measureText(String(text)).width);

  const h = cfg.h;
  const w = Math.max(h, textW + cfg.padX * 2);

  const cv = document.createElement('canvas');
  cv.width = w;
  cv.height = h;
  const g = cv.getContext('2d');

  const inset = 1.5 * DPR;
  const x = inset;
  const y = inset;
  const ww = w - inset * 2;
  const hh = h - inset * 2;
  const r = hh / 2; // full stadium ends

  g.beginPath();
  g.moveTo(x + r, y);
  g.arcTo(x + ww, y, x + ww, y + hh, r);
  g.arcTo(x + ww, y + hh, x, y + hh, r);
  g.arcTo(x, y + hh, x, y, r);
  g.arcTo(x, y, x + ww, y, r);
  g.closePath();

  g.fillStyle = bg;
  g.fill();
  g.lineWidth = 1.5 * DPR;
  g.strokeStyle = dim ? '#d8dde6' : '#ffffff';
  g.stroke();

  g.fillStyle = contrastText(bg);
  g.font = font;
  g.textAlign = 'center';
  g.textBaseline = 'middle';
  g.fillText(String(text), w / 2, h / 2 + 0.5 * DPR);

  return cv;
}

/** Desaturate + slightly darken a hex colour — the "stale GPS fix" cue. */
function mutedColor(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec(String(hex));
  if (!m) return '#8a94a6';
  const n = parseInt(m[1], 16);
  let r = (n >> 16) & 255;
  let g = (n >> 8) & 255;
  let b = n & 255;
  const gray = 0.299 * r + 0.587 * g + 0.114 * b;
  const t = 0.72; // pull most of the way toward grey
  r += (gray - r) * t;
  g += (gray - g) * t;
  b += (gray - b) * t;
  const d = 0.8; // and knock it down a touch
  return `#${[r, g, b].map((v) => Math.round(v * d).toString(16).padStart(2, '0')).join('')}`;
}

/** Black or white text, whichever reads better on `hex` (YIQ). */
function contrastText(hex) {
  const m = /^#?([0-9a-f]{6})$/i.exec(String(hex));
  if (!m) return '#ffffff';
  const n = parseInt(m[1], 16);
  const yiq = (((n >> 16) & 255) * 299 + ((n >> 8) & 255) * 587 + (n & 255) * 114) / 1000;
  return yiq >= 150 ? '#1b1f27' : '#ffffff';
}

async function rasterisePin(id, color, stopped, dim) {
  const map = getMap();
  if (!map || pinInFlight.has(id)) return;
  pinInFlight.add(id);
  try {
    const svgText = await loadPinSvg();
    const themed = themePinSvg(svgText, color, stopped, dim);
    const img = await svgToImageData(themed, PIN_W, PIN_H);
    const m = getMap();
    if (m) {
      if (m.hasImage(id)) m.updateImage(id, img);
      else m.addImage(id, img, { pixelRatio: DPR });
      // Keep the pixels around as a canvas so the composite builder can draw
      // the (rotated) pin into a bigger canvas.
      let cv = pinCanvasCache.get(id);
      if (!cv) {
        cv = document.createElement('canvas');
        cv.width = PIN_W;
        cv.height = PIN_H;
        pinCanvasCache.set(id, cv);
      }
      cv.getContext('2d').putImageData(img, 0, 0);
      pinInkCache.set(id, computeInk(cv));
      scheduleFrame(); // repaint now that the icon exists
    }
  } catch (err) {
    console.warn('[livemap] pin raster failed', id, err && err.message);
  } finally {
    pinInFlight.delete(id);
  }
}

/** Recolour busmarker.svg for a route; mark "stopped" and/or "stale". */
function themePinSvg(svgText, color, stopped, dim) {
  // Stopped vehicles keep their full route colour — only the centre-ring→square
  // swap below signals "stopped". (Darkening the body read as a different route
  // when the colour-coded pills aren't visible to compare against.)
  const bodyFill = dim ? mutedColor(color) : color;
  // The marks drawn ON the teardrop body (centre ring, heading arrow, stopped
  // square) flip white->dark when the route colour is light — the same contrast
  // rule the pills use. #halo stays white: it's the outline against the basemap.
  // An inline style beats the stylesheet's `.st0 { fill:#fff }`.
  const mark = `style="fill:${contrastText(bodyFill)}"`;

  let out = svgText.replaceAll(PIN_ROUTE_HEX, bodyFill);
  out = out.replace(/(<path id="center_ring" class="st0")/, `$1 ${mark}`);
  out = out.replace(/(<path id="heading" class="st0")/, `$1 ${mark}`);

  if (stopped) {
    // Stopped cue: *replace* the centre ring with a filled square (the legacy
    // cue). Appending a square on top of the ring left the ring's outer edge
    // peeking out, which read as "the circle never goes away".
    const SQUARE = `<rect id="center_ring" class="st0" ${mark} x="18" y="35" width="17" height="17" rx="2.5"/>`;
    const swapped = out.replace(/<path id="center_ring"[^>]*\/>/, SQUARE);
    out =
      swapped !== out
        ? swapped
        : // SVG changed shape — fall back to overlaying a square big enough to
          // cover the ring entirely.
          out.replace(/<\/svg>\s*$/, `${SQUARE}</svg>`);
  }
  return out;
}

function svgToImageData(svgText, w, h) {
  return new Promise((resolve, reject) => {
    const url = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgText);
    const im = new Image();
    im.onload = () => {
      const cv = document.createElement('canvas');
      cv.width = w;
      cv.height = h;
      const g = cv.getContext('2d');
      g.drawImage(im, 0, 0, w, h);
      resolve(g.getImageData(0, 0, w, h));
    };
    im.onerror = () => reject(new Error('svg image decode failed'));
    im.src = url;
  });
}

// --- popup ----------------------------------------------------------------------

function openPopup(id) {
  const s = state.get(id);
  if (!s) return;
  closePopup();
  popupId = id;
  popup = new maplibregl.Popup({
    offset: 18,
    closeButton: true,
    className: 'livemap-vehicle-popup',
    maxWidth: '260px',
  })
    .setLngLat([s.lng, s.lat])
    .setHTML(popupHTML(id))
    .addTo(getMap());
  popup.on('close', () => {
    if (popupId === id) {
      popupId = null;
      popup = null;
    }
  });
  wirePopupButtons();
}

function closePopup() {
  if (popup) popup.remove();
  popup = null;
  popupId = null;
}

function refreshPopupContent() {
  if (!popup || !popupId) return;
  popup.setHTML(popupHTML(popupId));
  wirePopupButtons();
}

function wirePopupButtons() {
  const el = popup && popup.getElement();
  if (!el) return;
  const btn = el.querySelector('[data-action="follow"]');
  if (btn) {
    btn.addEventListener('click', () => {
      if (followId === popupId) stopFollow();
      else followVehicle(popupId);
      refreshPopupContent();
    });
  }
}

function popupHTML(id) {
  const s = state.get(id);
  if (!s) return '';
  const p = s.props;
  const following = followId === id;
  const speed = `${p.speedEstimated ? '~' : ''}${Math.round(p.speedMph)} mph${
    p.speedEstimated ? ' (est.)' : ''
  }`;
  const age =
    p.ageS == null
      ? null
      : p.ageS < 60
        ? `${Math.round(p.ageS)}s ago`
        : `${Math.round(p.ageS / 60)} min ago`;

  let occ = '';
  if (typeof p.occupancy === 'number') {
    const pct = clamp(Math.round(p.occupancy * 100), 0, 100);
    const label =
      p.onboard != null && p.capacity
        ? `${p.onboard} / ${p.capacity}`
        : `${pct}%`;
    occ = `
      <div class="lv-occ">
        <div class="lv-occ-bar"><span style="width:${pct}%"></span></div>
        <div class="lv-occ-label">${label} aboard</div>
      </div>`;
  }

  const metaBits = [p.stopped ? 'Stopped' : speed];
  if (age) metaBits.push(`fix ${age}${p.dim ? ' · stale' : ''}`);

  const tag =
    p.agency === 'cat' ? 'CAT'
    : p.agency === 'spare' ? 'UVA FLEXRIDE'
    : p.agency === 'ondemand' ? 'UVA RIDE'
    : '';

  // Dispatchers lead with the block; everyone else leads with the route. The
  // block is shown verbatim — brackets already mean "block" in UTS-speak.
  const dispatch = isDispatcher();
  const blockLine = dispatch && p.block ? `<div class="lv-name">${escapeHTML(p.block)}</div>` : '';
  const unitLine = `<div class="${blockLine ? 'lv-meta' : 'lv-name'}">${unitNoun(p.agency)} ${escapeHTML(p.label)}</div>`;
  // Driver line(s). UTS buses: from getDrivers(); vans: from the merged
  // On-Demand driver name + a name-matched W2W shift.
  const driverBits = [];
  if (dispatch) {
    for (const d of p.drivers) {
      driverBits.push(
        escapeHTML(d.name) +
          (d.start || d.end
            ? ` <span class="lv-shift">${escapeHTML(`${d.start}–${d.end}`)}</span>`
            : ''),
      );
    }
    if (p.driver) {
      driverBits.push(
        escapeHTML(p.driver) +
          (p.driverShift ? ` <span class="lv-shift">${escapeHTML(p.driverShift)}</span>` : ''),
      );
    }
  }
  const driverLine = driverBits.length
    ? `<div class="lv-meta">${driverBits.join(', ')}</div>`
    : '';

  // Van blurb: "White Karsan eJest" (On-Demand) or "Gray Hyundai Sonata · 4 seats" (Spare).
  const vanBits = [];
  if (p.descr) vanBits.push(escapeHTML(p.descr));
  if (p.plate && p.plate !== p.label) vanBits.push(escapeHTML(p.plate));
  if (p.seats) vanBits.push(`${p.seats} seats`);
  const vanLine = vanBits.length ? `<div class="lv-meta">${vanBits.join(' · ')}</div>` : '';
  const accessLine =
    p.access && p.access.length
      ? `<div class="lv-meta lv-access">${escapeHTML(p.access.join(', '))}</div>`
      : '';

  // A van's ordered destination list (vandispatch-style) — no route lines.
  let manifestBlock = '';
  if (p.agency === 'spare' || p.agency === 'ondemand') {
    const rows = vanManifest(p.agency === 'spare' ? 'spare' : 'ride', p.agency === 'spare' ? p.spareId : p.label);
    if (rows.length) {
      manifestBlock = `
      <div class="lv-manifest">
        <div class="lv-manifest-h">Stops</div>
        ${rows
          .slice(0, 8)
          .map((s) => {
            const who = s.rider ? ` — ${escapeHTML(s.rider)}` : '';
            const when = s.time ? ` <span class="lv-shift">${escapeHTML(s.time)}</span>` : '';
            return `<div class="lv-manifest-row"><span class="lv-manifest-n">${s.n || '·'}</span><span class="lv-manifest-k lv-manifest-k--${s.kind === 'Pickup' ? 'p' : 'd'}">${s.kind}</span>${who}${when}${s.addr ? `<div class="lv-manifest-addr">${escapeHTML(s.addr)}</div>` : ''}</div>`;
          })
          .join('')}
      </div>`;
    }
  }

  return `
    <div class="lv-pop">
      <div class="lv-route">
        <span class="lv-swatch" style="background:${p.routeColor}"></span>
        ${escapeHTML(p.routeName)}${tag ? ` <span class="ls-tag">${tag}</span>` : ''}
      </div>
      ${blockLine}
      ${unitLine}
      ${driverLine}
      ${vanLine}
      ${accessLine}
      <div class="lv-meta">${metaBits.join(' · ')}</div>
      ${manifestBlock}
      ${occ}
      <button type="button" class="lv-follow${following ? ' is-on' : ''}" data-action="follow">
        ${following ? 'Following — tap to release' : `Follow this ${unitNoun(p.agency).toLowerCase()}`}
      </button>
    </div>`;
}

function escapeHTML(str) {
  return String(str).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[ch]);
}

// --- follow chip -------------------------------------------------------------

function ensureFollowChip() {
  if (followChip) return;
  followChip = document.createElement('div');
  followChip.className = 'livemap-follow-chip';
  followChip.hidden = true;
  followChip.innerHTML =
    '<span class="lfc-text"></span><button type="button" class="lfc-x" aria-label="Stop following">&times;</button>';
  // The whole chip is the release target, not just the tiny ×.
  followChip.addEventListener('click', stopFollow);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && followId) stopFollow();
  });
  document.body.appendChild(followChip);
}

function updateFollowChip() {
  ensureFollowChip();
  const s = followId && state.get(followId);
  if (!s) {
    followChip.hidden = true;
    return;
  }
  followChip.hidden = false;
  const p = s.props;
  const name = /[a-z]/i.test(String(p.label))
    ? String(p.label).trim()
    : `${unitNoun(p.agency).toLowerCase()} ${p.label}`;
  followChip.querySelector('.lfc-text').textContent = `Following ${name}`;
}

// --- math -----------------------------------------------------------------------

const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);
const lerp = (a, b, t) => a + (b - a) * t;
function lerpAngle(a, b, t) {
  const d = ((((b - a) % 360) + 540) % 360) - 180;
  return a + d * t;
}
