// livemap/core/dispatcher-bridge.js
// -----------------------------------------------------------------------------
// Lets another page drive the map — the dispatcher console's "fly to bus"
// button, or an embed/kiosk that wants a specific starting view. Two channels:
//
//   URL params:  ?bus=1234  (or ?follow= / ?vehicle=)  -> follow that vehicle
//                ?centerLat=&centerLon=&centerZoom=     -> override start view
//                ?fit=west,south,east,north             -> 'whole route' view
//                  (as fitBounds below; ignored when ?bus= is set)
//   postMessage: { source:'dispatcher', type:'dispatcher:focusBus',
//                  vehicleId|identifier|bus|name }      -> follow
//                { source:'dispatcher', type:'dispatcher:centerMap',
//                  lat, lng, zoom? }                    -> flyTo
//                { source:'dispatcher', type:'dispatcher:showRoutes',
//                  routes:[RouteID,...] | null }       -> draw only those
//                  routes (and their buses/stops); null = every route again
//                { source:'dispatcher', type:'dispatcher:fitBounds',
//                  bounds:[[west,south],[east,north]] } -> frame that box and
//                  keep it framed (on resize) until the user moves the map
//   Back to the parent (only when embedded): { source:'livemap',
//   type:'livemap:view', mode:'follow'|'route'|null } whenever the view mode
//   changes, so /driver can light up its Follow / Whole route buttons.
//   (same shapes the legacy testmap accepts, so the existing dispatcher page
//    works against livemap unchanged.)
// -----------------------------------------------------------------------------

import { param, paramBool, paramNum } from './util.js';
import { getMap } from './map.js';
import { DEFAULT_VIEW } from './config.js';
import { followByRef, onFollowChange, stopFollow } from './layers/vehicles.js';
import { setOnlyRoutes } from './layers/routes.js';

const FOLLOW_RETRY_MS = 500;

let followWanted = null; // bus asked for but not in the feed yet
let fitBox = null; // [[w,s],[e,n]] while the 'whole route' view is on
let following = false;
let lastReported;

export function installDispatcherBridge() {
  const map = getMap();

  // Initial view override (embed / far-flung stop / kiosk).
  const lat = paramNum('centerLat');
  const lng = paramNum('centerLon');
  if (map && Number.isFinite(lat) && Number.isFinite(lng)) {
    const z = paramNum('centerZoom');
    map.jumpTo({ center: [lng, lat], zoom: Number.isFinite(z) ? z : 16 });
  }

  // Follow a vehicle named in the URL — retry a while so it works even before
  // the feed has delivered that bus.
  const ref = param('bus') || param('follow') || param('vehicle');
  if (ref) requestFollow(ref, 40);

  window.addEventListener('message', onMessage);
  if (!map) return;

  onFollowChange((id) => {
    following = !!id;
    if (id) { followWanted = null; fitBox = null; }
    report();
  });
  // A user gesture ends the 'whole route' view, and a follow still waiting for
  // its bus (the follow itself is released in layers/vehicles.js). With
  // ?followZoom a zoom keeps a follow, so only a pan cancels the wait.
  const onGesture = (e) => {
    if (!e || !e.originalEvent) return;
    let changed = false;
    if (fitBox) { fitBox = null; changed = true; }
    const touches = e.originalEvent.touches;
    const pan = e.type === 'dragstart' && !(touches && touches.length > 1);
    if (followWanted && (pan || !paramBool('followZoom'))) { followWanted = null; changed = true; }
    if (changed) report();
  };
  for (const ev of ['dragstart', 'zoomstart', 'rotatestart', 'pitchstart']) map.on(ev, onGesture);
  map.on('resize', () => { if (fitBox) fitTo(fitBox, 0); });
  if (!ref) {
    const f = String(param('fit') || '').split(',').map(Number);
    if (f.length === 4 && f.every(Number.isFinite)) {
      fitBox = [[f[0], f[1]], [f[2], f[3]]];
      fitTo(fitBox, 0);
    }
  }
  report();
}

function report() {
  if (window.parent === window) return;
  const mode = following || followWanted ? 'follow' : fitBox ? 'route' : null;
  if (mode === lastReported) return;
  lastReported = mode;
  window.parent.postMessage({ source: 'livemap', type: 'livemap:view', mode }, window.location.origin);
}

function requestFollow(ref, tries) {
  fitBox = null;
  followWanted = ref;
  report();
  tryFollow(ref, tries);
}

function fitTo(box, duration) {
  const map = getMap();
  if (!map) return;
  map.fitBounds(box, { padding: 40, maxZoom: 17, duration });
}

function tryFollow(ref, tries) {
  if (followWanted !== ref) return; // cancelled, or a newer request took over
  if (followByRef(ref)) { followWanted = null; return; }
  if (tries > 0) setTimeout(() => tryFollow(ref, tries - 1), FOLLOW_RETRY_MS);
  // The /driver map (?followZoom) is opened before the bus may be in the feed
  // (not signed onto a route yet): keep looking, slowly, until it shows up.
  else if (paramBool('followZoom')) setTimeout(() => tryFollow(ref, 0), 5000);
}

function onMessage(e) {
  // Only trust same-origin messages from our opener / parent.
  if (e.origin && e.origin !== window.location.origin) return;
  const d = e.data;
  if (!d || typeof d !== 'object' || d.source !== 'dispatcher') return;

  const type = String(d.type || '');
  if (/focusBus/i.test(type)) {
    const ref = d.vehicleId ?? d.identifier ?? d.bus ?? d.name ?? d.id;
    if (ref != null) requestFollow(String(ref), 20);
    return;
  }
  if (/showRoutes/i.test(type)) {
    const r = d.routes;
    if (r === null) setOnlyRoutes(null);
    else if (Array.isArray(r)) setOnlyRoutes(r.map(String));
    return;
  }
  if (/fitBounds/i.test(type)) {
    const b = d.bounds;
    const ok = Array.isArray(b) && b.length === 2 && b.flat().length === 4 && b.flat().every(Number.isFinite);
    if (!ok) return;
    followWanted = null;
    stopFollow();
    fitBox = b;
    fitTo(b, 600);
    report();
    return;
  }
  if (/centerMap/i.test(type)) {
    const map = getMap();
    if (!map) return;
    const lat = Number(d.lat ?? d.centerLat);
    const lng = Number(d.lng ?? d.lon ?? d.centerLon);
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      map.flyTo({
        center: [lng, lat],
        zoom: Number.isFinite(Number(d.zoom)) ? Number(d.zoom) : map.getZoom(),
        duration: 700,
      });
    } else {
      map.flyTo({ center: DEFAULT_VIEW.center, zoom: DEFAULT_VIEW.zoom, duration: 700 });
    }
  }
}
