// livemap/core/user-gesture.js
// -----------------------------------------------------------------------------
// What the user's hands are doing on the map, read from the DOM (pointer + wheel
// events on the canvas container) rather than MapLibre's camera events: on this
// MapLibre build a wheel zoom fires `zoomstart` WITHOUT `originalEvent`, so the
// camera events can't tell a user's zoom from our own setCenter / fitBounds.
//
// Used by the /driver map (follow with ?followZoom, the 'whole route' view):
//   isUserGesturing()  fingers/mouse down, or a wheel turned in the last 400 ms
//   onUserTouch(fn)    any touch, click, wheel or key on the map
//   onUserPan(fn)      one finger / the mouse dragged 10 px or more
// -----------------------------------------------------------------------------

const PAN_PX = 10;
const WHEEL_MS = 400;

const pointers = new Map(); // pointerId -> { x, y } where it went down
let lastWheel = -Infinity;
let panned = false; // this one-pointer press already counted as a pan
const touchFns = new Set();
const panFns = new Set();
let installed = false;

export function installUserGestureWatch(map) {
  if (installed || !map) return;
  installed = true;
  const el = map.getCanvasContainer();
  const touch = (e) => touchFns.forEach((fn) => fn(e));
  el.addEventListener('pointerdown', (e) => {
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    panned = false;
    touch(e);
  }, true);
  el.addEventListener('pointermove', (e) => {
    const p = pointers.get(e.pointerId);
    if (!p || panned || pointers.size !== 1) return; // two fingers = pinch, not a pan
    if (Math.hypot(e.clientX - p.x, e.clientY - p.y) >= PAN_PX) {
      panned = true;
      panFns.forEach((fn) => fn(e));
    }
  }, true);
  const up = (e) => pointers.delete(e.pointerId);
  for (const t of ['pointerup', 'pointercancel', 'pointerleave']) el.addEventListener(t, up, true);
  el.addEventListener('wheel', (e) => {
    lastWheel = performance.now();
    touch(e);
  }, { capture: true, passive: true });
  el.addEventListener('keydown', touch, true);
}

export const isUserGesturing = () =>
  pointers.size > 0 || performance.now() - lastWheel < WHEEL_MS;

export function onUserTouch(fn) {
  touchFns.add(fn);
  return () => touchFns.delete(fn);
}

export function onUserPan(fn) {
  panFns.add(fn);
  return () => panFns.delete(fn);
}
