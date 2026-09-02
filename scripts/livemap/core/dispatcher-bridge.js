// livemap/core/dispatcher-bridge.js
// -----------------------------------------------------------------------------
// Lets another page drive the map — the dispatcher console's "fly to bus"
// button, or an embed/kiosk that wants a specific starting view. Two channels:
//
//   URL params:  ?bus=1234  (or ?follow= / ?vehicle=)  -> follow that vehicle
//                ?centerLat=&centerLon=&centerZoom=     -> override start view
//   postMessage: { source:'dispatcher', type:'dispatcher:focusBus',
//                  vehicleId|identifier|bus|name }      -> follow
//                { source:'dispatcher', type:'dispatcher:centerMap',
//                  lat, lng, zoom? }                    -> flyTo
//   (same shapes the legacy testmap accepts, so the existing dispatcher page
//    works against livemap unchanged.)
// -----------------------------------------------------------------------------

import { param, paramNum } from './util.js';
import { getMap } from './map.js';
import { DEFAULT_VIEW } from './config.js';
import { followByRef } from './layers/vehicles.js';

const FOLLOW_RETRY_MS = 500;

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
  if (ref) tryFollow(ref, 40);

  window.addEventListener('message', onMessage);
}

function tryFollow(ref, tries) {
  if (followByRef(ref)) return;
  if (tries > 0) setTimeout(() => tryFollow(ref, tries - 1), FOLLOW_RETRY_MS);
}

function onMessage(e) {
  // Only trust same-origin messages from our opener / parent.
  if (e.origin && e.origin !== window.location.origin) return;
  const d = e.data;
  if (!d || typeof d !== 'object' || d.source !== 'dispatcher') return;

  const type = String(d.type || '');
  if (/focusBus/i.test(type)) {
    const ref = d.vehicleId ?? d.identifier ?? d.bus ?? d.name ?? d.id;
    if (ref != null) tryFollow(String(ref), 20);
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
