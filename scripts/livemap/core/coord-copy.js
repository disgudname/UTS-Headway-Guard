// livemap/core/coord-copy.js
// -----------------------------------------------------------------------------
// Right-click (long-press on touch) a spot on the map to copy its GPS
// coordinates to the clipboard -- e.g. for radioing a location in or dropping
// it into Spare. Mirrors the same feature on /vandispatch.
//
// Only installed for the full interactive shell (see boot.js) -- a kiosk/embed
// display has no dispatcher sitting at it to right-click with, and no clipboard
// to copy into.
// -----------------------------------------------------------------------------

import { getMap } from './map.js';

function escapeHTML(str) {
  return String(str).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[ch]);
}

export function installCoordCopy() {
  const map = getMap();
  if (!map) return;

  let popup = null;
  const closePopup = () => {
    if (popup) {
      popup.remove();
      popup = null;
    }
  };

  map.on('contextmenu', (e) => {
    const { lat, lng } = e.lngLat;
    const coordText = `${lat.toFixed(6)}, ${lng.toFixed(6)}`;
    navigator.clipboard
      .writeText(coordText)
      .then(() => {
        closePopup();
        popup = new maplibregl.Popup({
          offset: 12,
          closeButton: false,
          closeOnClick: false,
          className: 'livemap-stop-popup',
          maxWidth: '260px',
        })
          .setLngLat(e.lngLat)
          .setHTML(`<div class="lv-coord-pop">📋 Copied ${escapeHTML(coordText)}</div>`)
          .addTo(map);
        setTimeout(closePopup, 1500);
      })
      .catch((err) => {
        console.error('[livemap] failed to copy coordinates to clipboard', err);
      });
  });
}
