// livemap/core/layers/routes.js
// -----------------------------------------------------------------------------
// UTS route lines. The casing + line layers and their GeoJSON source are baked
// into the basemap style document (see core/layers/route-style.js); this module
// keeps the source fed and owns per-route visibility.
//
// Visibility is grouped by route *name*: TransLoc exposes several RouteIDs that
// share a name (schedule variants of the same line, e.g. two "Gold Line"s and
// three "Purple Line"s). Riders think in lines, not RouteIDs, so the picker
// toggles a whole name-group at once. The hidden/pinned sets store individual
// RouteIDs, which is what the source features are keyed on.
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
// geometry slicing; syncSource() just hands it the visible name-group
// representatives and drops the result into the source. The whole striping call
// is wrapped so a bug there degrades to plain solid lines, never a blank map.
// -----------------------------------------------------------------------------

import { getMap, onStyleReady } from '../map.js';
import { lsGet, lsSet, emitter, debounce } from '../util.js';
import { startVehicleFeed, onRoutes, onVehicles, getRoutes } from '../data/transloc.js';
import { ROUTE_SOURCE_ID as SRC } from './route-style.js';
import { stripeRoutes, plainRouteFeatures } from './route-overlap.js';

const HIDDEN_KEY = 'livemap.routes.hidden';
const PINNED_KEY = 'livemap.routes.pinned';

const bus = emitter();
const hidden = loadSet(HIDDEN_KEY); // RouteIDs the user explicitly turned OFF
const pinned = loadSet(PINNED_KEY); // idle RouteIDs the user explicitly turned ON
let routes = []; // [{ id, name, color, coords }]
// RouteIDs with a bus on them in the LATEST vehicle report. A route line is a
// live diagnostic — "is this route's line drawn?" answers "are its buses tagged
// to it?" — so there is NO linger: lose the last bus, lose the line this tick.
let activeRouteIds = new Set();

/** fn(groups) — [{ name, color, ids, active, hidden }], active groups first. */
export const onRouteVisibility = (fn) => {
  const off = bus.on('change', fn);
  fn(groups());
  return off;
};

/** Is this RouteID currently hidden by the route picker? */
export function isRouteHidden(routeId) {
  return hidden.has(String(routeId));
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
    bus.emit('change', groups());
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
      bus.emit('change', groups());
    }
  });
  startVehicleFeed(); // idempotent; the feed drives route metadata too

  // If metadata already arrived before this ran, seed from it.
  const seed = getRoutes();
  if (seed.length) {
    routes = seed;
    pruneHidden();
    syncSource();
    bus.emit('change', groups());
  }
}

// --- visibility -------------------------------------------------------------

export function setGroupHidden(name, hide) {
  const ids = routes.filter((r) => r.name === name).map((r) => r.id);
  if (!ids.length) return;
  for (const id of ids) {
    if (hide) {
      hidden.add(id);
      pinned.delete(id);
    } else {
      hidden.delete(id);
      if (!routeActive(id)) pinned.add(id); // idle route -> pin so its line shows
    }
  }
  persist();
  syncSource();
  bus.emit('change', groups());
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
  bus.emit('change', groups());
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

function groups() {
  const byName = new Map();
  for (const r of routes) {
    let g = byName.get(r.name);
    if (!g) {
      g = { name: r.name, color: r.color, ids: [] };
      byName.set(r.name, g);
    }
    g.ids.push(r.id);
  }
  const out = [...byName.values()];
  for (const g of out) {
    g.active = g.ids.some((id) => routeActive(id));
    g.hidden = !g.ids.some((id) => lineShows(id)); // "off" = nothing drawing
  }
  // Active lines first, then A–Z within each block.
  out.sort((a, b) => (a.active === b.active ? a.name.localeCompare(b.name) : a.active ? -1 : 1));
  return out;
}

/** One representative polyline per visible line (name-group): the RouteID
 *  variant with the most shape points. Striping works on lines, not schedule
 *  variants, so a line never "overlaps itself". */
function visibleReps() {
  const byName = new Map();
  for (const r of routes) {
    if (!lineShows(r.id)) continue;
    if (!Array.isArray(r.coords) || r.coords.length < 2) continue;
    const cur = byName.get(r.name);
    if (!cur || r.coords.length > cur.coords.length) {
      byName.set(r.name, { key: r.name, color: r.color, coords: r.coords });
    }
  }
  return [...byName.values()];
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
