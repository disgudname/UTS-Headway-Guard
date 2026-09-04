// livemap/core/layers/routes.js
// -----------------------------------------------------------------------------
// UTS route lines. The casing + line layers and their GeoJSON source are baked
// into the basemap style document (see core/layers/route-style.js); this module
// keeps the source fed and owns per-route visibility.
//
// Visibility is per RouteID — one row per route in the picker, NOT merged by
// name. TransLoc gives several RouteIDs the same name (schedule variants of one
// line, e.g. two "Gold Line"s and three "Purple Line"s) whose routings differ;
// each is its own toggle so you can show exactly the variant(s) you want. The
// route's InfoText is what tells the variants apart and is shown under the name.
// hidden/pinned are RouteID sets — the source features are keyed on RouteID too.
//
// The picker lists EVERY route (matches testmap), sorted active-first. A
// route's LINE, though, only draws when it has a bus on it right now — the line
// is a live "are these buses tagged to their route?" diagnostic. Ticking an
// idle route "pins" it so its line appears the moment a bus shows up (and, for
// convenience, right away if geometry is known).
//
// Overlap striping: where two lines follow the same street, that stretch is
// drawn as an alternating cycle of every sharing line's colour (the
// TransLoc-patented look, US7920967B1). route-overlap.js does the detection and
// geometry slicing; syncSource() hands it one polyline per *shown RouteID*
// (visibleReps). The whole striping call is wrapped so a bug there degrades to
// plain solid lines, never a blank map.
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { lsGet, lsSet, emitter, debounce } from '../util.js';
import { startVehicleFeed, onRoutes, onVehicles, getRoutes } from '../data/transloc.js';
import { ROUTE_SOURCE_ID as SRC } from './route-style.js';
import { stripeRoutes, plainRouteFeatures } from './route-overlap.js';

// v2: the v1 keys were written by the old name-grouped picker, which pinned
// EVERY schedule variant of a line when you turned that line on. Read back into
// the per-RouteID picker that reads as "Gold Line on 2×, Purple Line on 3×"
// even when only one variant has buses. Start clean and drop the stale keys.
const HIDDEN_KEY = 'livemap.routes.hidden.v2';
const PINNED_KEY = 'livemap.routes.pinned.v2';
try {
  for (const k of ['livemap.routes.hidden', 'livemap.routes.pinned', 'livemap.routes.offshown']) {
    localStorage.removeItem(k);
  }
} catch {
  /* private mode / storage disabled */
}

const bus = emitter();
const hidden = loadSet(HIDDEN_KEY); // RouteIDs the user explicitly turned OFF
const pinned = loadSet(PINNED_KEY); // idle RouteIDs the user explicitly turned ON
let routes = []; // [{ id, name, color, coords, info }]
// RouteIDs with a bus on them in the LATEST vehicle report. A route line is a
// live diagnostic — "is this route's line drawn?" answers "are its buses tagged
// to it?" — so there is NO linger: lose the last bus, lose the line this tick.
let activeRouteIds = new Set();

/** fn(list) — one entry per RouteID (schedule variants are separate rows, not
 *  merged by name). Active routes first, then by name, then by InfoText. Each:
 *  { id, name, info, color, active, hidden, shown } */
export const onRouteVisibility = (fn) => {
  const off = bus.on('change', fn);
  fn(routeList());
  return off;
};

/** Is this RouteID currently hidden by the route picker? */
export function isRouteHidden(routeId) {
  return hidden.has(String(routeId));
}

/** Is this RouteID's line actually drawn right now — i.e. its row in the route
 *  selector is checked (it has a bus or is pinned, and isn't switched off)?
 *  Stops key their visibility off this so a stop only shows when a route the
 *  user is actually looking at serves it. */
export function isRouteShown(routeId) {
  return lineShows(routeId);
}

function sameSet(a, b) {
  if (a.size !== b.size) return false;
  for (const x of a) if (!b.has(x)) return false;
  return true;
}

const zoomSync = debounce(() => syncSource(), 120);
let zoomWired = false;

export function installRouteLayer() {
  onStyleReady(() => {
    // Pixel-space dash cadence changes with zoom — recompute the striping on
    // zoom settle (pan does not need it). Attach once; map-level listeners
    // survive a style/theme swap.
    if (!zoomWired) {
      const map = getMap();
      if (map) {
        map.on('zoomend', zoomSync);
        zoomWired = true;
      }
    }
    syncSource();
  });
  onRoutes((list) => {
    routes = list;
    pruneHidden();
    syncSource();
    bus.emit('change', routeList());
  });
  // A route with no bus in the current report drops off the map + the picker.
  onVehicles((list) => {
    const next = new Set();
    for (const v of list) {
      const rid = String(v.routeId ?? '');
      if (rid && rid !== '0') next.add(rid);
    }
    if (!sameSet(next, activeRouteIds)) {
      activeRouteIds = next;
      syncSource();
      bus.emit('change', routeList());
    }
  });
  startVehicleFeed(); // idempotent; the feed drives route metadata too

  // If metadata already arrived before this ran, seed from it.
  const seed = getRoutes();
  if (seed.length) {
    routes = seed;
    pruneHidden();
    syncSource();
    bus.emit('change', routeList());
  }
}

// --- visibility -------------------------------------------------------------

/** Toggle a single RouteID (one schedule variant) on/off. */
export function setRouteHidden(id, hide) {
  const s = String(id);
  if (hide) {
    hidden.add(s);
    pinned.delete(s);
  } else {
    hidden.delete(s);
    if (!routeActive(s)) pinned.add(s); // idle route -> pin so its line shows
  }
  persist();
  syncSource();
  bus.emit('change', routeList());
}

export function setAllHidden(hide) {
  if (hide) {
    for (const r of routes) hidden.add(r.id);
    pinned.clear();
  } else {
    hidden.clear(); // reveal every active route; leave idle routes as they were
  }
  persist();
  syncSource();
  bus.emit('change', routeList());
}

// --- internals ------------------------------------------------------------

/** Does this RouteID have a bus on it in the latest report? */
function routeActive(id) {
  return activeRouteIds.has(String(id));
}

/** Should this route's LINE be on the map? On when it has a bus (or is pinned)
 *  and hasn't been explicitly switched off. */
function lineShows(id) {
  const s = String(id);
  return !hidden.has(s) && (routeActive(s) || pinned.has(s));
}

function routeList() {
  const out = routes.map((r) => {
    const shown = lineShows(r.id);
    return {
      id: r.id,
      name: r.name,
      info: r.info || '',
      color: r.color,
      active: routeActive(r.id),
      hidden: !shown, // "off" in the picker = not currently on the map
      shown,
    };
  });
  // Active first; then by name so a line's variants sit together; then by
  // InfoText for a stable order within a line.
  out.sort((a, b) =>
    a.active === b.active
      ? a.name.localeCompare(b.name) || a.info.localeCompare(b.info)
      : a.active
        ? -1
        : 1,
  );
  return out;
}

/** One polyline per shown RouteID — schedule variants are drawn independently
 *  now (the picker toggles them one by one). Two variants of the same line that
 *  share a street just stripe same-colour-on-same-colour there and diverge
 *  where their routings differ, which is exactly what we want to show. */
function visibleReps() {
  const out = [];
  for (const r of routes) {
    if (!lineShows(r.id)) continue;
    if (!Array.isArray(r.coords) || r.coords.length < 2) continue;
    out.push({ key: r.id, color: r.color, coords: r.coords });
  }
  return out;
}

function syncSource() {
  const map = getMap();
  const src = map && map.getSource(SRC);
  if (!src) return;
  const reps = visibleReps();

  let features;
  try {
    const zoom = typeof map.getZoom === 'function' ? map.getZoom() : 14;
    const centerLat = map.getCenter ? map.getCenter().lat : 38.035;
    features = stripeRoutes(reps, zoom, centerLat).features;
    if (reps.length && (!Array.isArray(features) || features.length === 0)) {
      features = plainRouteFeatures(reps);
    }
  } catch (err) {
    console.error('[livemap] route striping failed; drawing plain lines', err);
    features = plainRouteFeatures(reps);
  }

  src.setData({ type: 'FeatureCollection', features: features || [] });
}

function pruneHidden() {
  const live = new Set(routes.map((r) => r.id));
  for (const id of [...hidden]) if (!live.has(id)) hidden.delete(id);
  for (const id of [...pinned]) if (!live.has(id)) pinned.delete(id);
}

function loadSet(key) {
  try {
    const arr = JSON.parse(lsGet(key, '[]'));
    return new Set(Array.isArray(arr) ? arr.map(String) : []);
  } catch {
    return new Set();
  }
}

function persist() {
  lsSet(HIDDEN_KEY, JSON.stringify([...hidden]));
  lsSet(PINNED_KEY, JSON.stringify([...pinned]));
}
