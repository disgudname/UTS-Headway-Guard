// livemap/core/marker-menu.js
// -----------------------------------------------------------------------------
// Overlapping-marker disambiguation. Feature layers (stops, vehicles, …)
// register themselves here instead of wiring their own map click handlers; this
// module owns the single map-wide click. On a click it queries every
// registered layer within a small pixel radius:
//
//   0 targets  -> nothing (other click handlers still run)
//   1 target   -> open it directly (old behaviour)
//   2–7        -> fan a small radial menu of chips to pick from
//   8+         -> a "zoom in" hint instead (too crowded to pick)
// -----------------------------------------------------------------------------

import { getMap } from './map.js';
import { isMenuSuppressed } from './modes.js';

const HIT_RADIUS_PX = 22;
const MENU_MAX = 7; // 8+ -> zoom hint
const CHIP_PX = 92; // round chip diameter

const sources = []; // { layer, resolve(feature) -> target | null }
let wired = false;
let menuEl = null;
let menuAnchor = null; // LngLat the open menu is pinned to
let onMove = null; // map 'move' handler that keeps menuEl over menuAnchor

/**
 * @param {{ layer: string, resolve: (feature) => ({
 *   key: string, label: string, sublabel?: string,
 *   color?: string, colors?: string[], open: () => void
 * } | null) }} src
 */
export function registerMarkerLayer(src) {
  sources.push(src);
}

export function installMarkerMenu() {
  const map = getMap();
  if (!map || wired) return;
  // A plain kiosk is look-don't-touch: no disambiguation menu, no popups.
  if (isMenuSuppressed()) return;
  wired = true;

  map.on('click', onMapClick);
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeMenu();
  });
}

function onMapClick(e) {
  const map = getMap();
  if (!map) return;

  // A click on the menu itself is handled by the chip listeners.
  if (menuEl && e.originalEvent && e.originalEvent.target.closest('.livemap-marker-menu')) return;
  closeMenu();

  const { x, y } = e.point;
  const box = [
    [x - HIT_RADIUS_PX, y - HIT_RADIUS_PX],
    [x + HIT_RADIUS_PX, y + HIT_RADIUS_PX],
  ];
  const layers = sources.map((s) => s.layer).filter((id) => map.getLayer(id));
  if (!layers.length) return;

  const hits = map.queryRenderedFeatures(box, { layers });
  const seen = new Set();
  const targets = [];
  for (const f of hits) {
    const src = sources.find((s) => s.layer === f.layer.id);
    if (!src) continue;
    let t;
    try {
      t = src.resolve(f);
    } catch (err) {
      console.warn('[livemap] marker resolve threw', err);
      continue;
    }
    if (!t || seen.has(t.key)) continue;
    seen.add(t.key);
    targets.push(t);
  }

  if (targets.length === 0) return;
  if (targets.length === 1) {
    targets[0].open();
    return;
  }
  if (targets.length > MENU_MAX) {
    showZoomHint(e.point);
    return;
  }
  showMenu(e.point, e.lngLat, targets);
}

/** Keep the open menu / hint pinned over its geographic anchor as the map moves,
 *  so a menu opened near the edge can be panned fully into view. */
function pinToAnchor() {
  const map = getMap();
  if (!menuEl || !menuAnchor || !map) return;
  const p = map.project(menuAnchor);
  menuEl.style.left = `${p.x}px`;
  menuEl.style.top = `${p.y}px`;
}

function trackAnchor(lngLat) {
  const map = getMap();
  menuAnchor = lngLat || null;
  if (!map || !menuAnchor) return;
  onMove = pinToAnchor;
  map.on('move', onMove);
}

function untrackAnchor() {
  const map = getMap();
  if (map && onMove) map.off('move', onMove);
  onMove = null;
  menuAnchor = null;
}

// --- menu -----------------------------------------------------------------------

function chipBackground(t) {
  const cols = (t.colors && t.colors.length ? t.colors : [t.color || '#000']).filter(Boolean);
  if (cols.length > 1) {
    const step = 360 / cols.length;
    const stops = cols.map((c, i) => `${c} ${i * step}deg ${(i + 1) * step}deg`).join(', ');
    return `conic-gradient(from -90deg, ${stops})`;
  }
  return cols[0] || '#000000';
}

/** Black/white text for a solid colour (YIQ); white for multi-colour pies. */
function chipTextColor(t) {
  const cols = (t.colors && t.colors.length ? t.colors : [t.color]).filter(Boolean);
  if (cols.length !== 1) return '#ffffff';
  const m = /^#?([0-9a-f]{6})$/i.exec(cols[0]);
  if (!m) return '#ffffff';
  const n = parseInt(m[1], 16);
  const yiq = (((n >> 16) & 255) * 299 + ((n >> 8) & 255) * 587 + (n & 255) * 114) / 1000;
  return yiq >= 150 ? '#1b1f27' : '#ffffff';
}

function showMenu(point, lngLat, targets) {
  const map = getMap();
  const host = map.getContainer();

  menuEl = document.createElement('div');
  menuEl.className = 'livemap-marker-menu';
  menuEl.style.left = `${point.x}px`;
  menuEl.style.top = `${point.y}px`;
  trackAnchor(lngLat);

  const dot = document.createElement('button');
  dot.type = 'button';
  dot.className = 'lmm-center';
  dot.setAttribute('aria-label', 'Close');
  dot.textContent = '×';
  dot.addEventListener('click', closeMenu);
  menuEl.appendChild(dot);

  // Even spacing on a full circle; grow the ring so the round chips never touch
  // and carry a bit of air between them.
  const n = targets.length;
  const ring = Math.max(72, CHIP_PX / 2 / Math.sin(Math.PI / n) + 14);

  targets.forEach((t, i) => {
    const a = -Math.PI / 2 + (i / n) * Math.PI * 2;
    const cx = Math.cos(a) * ring;
    const cy = Math.sin(a) * ring;

    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = 'lmm-chip';
    chip.style.transform = `translate(-50%, -50%) translate(${cx}px, ${cy}px)`;
    chip.style.background = chipBackground(t);
    chip.style.color = chipTextColor(t);
    chip.title = t.sublabel ? `${t.label} — ${t.sublabel}` : t.label;
    chip.innerHTML = `<span class="lmm-text">${escapeHTML(t.label)}${
      t.sublabel ? `<span class="lmm-sub">${escapeHTML(t.sublabel)}</span>` : ''
    }</span>`;
    chip.addEventListener('click', (ev) => {
      ev.stopPropagation();
      closeMenu();
      t.open();
    });
    menuEl.appendChild(chip);
  });

  host.appendChild(menuEl);
  requestAnimationFrame(() => menuEl && menuEl.classList.add('is-open'));
}

function showZoomHint(point) {
  const map = getMap();
  const host = map.getContainer();
  menuEl = document.createElement('div');
  menuEl.className = 'livemap-marker-menu lmm-hint';
  menuEl.style.left = `${point.x}px`;
  menuEl.style.top = `${point.y}px`;
  menuEl.textContent = 'Zoom in to pick';
  host.appendChild(menuEl);
  requestAnimationFrame(() => menuEl && menuEl.classList.add('is-open'));
  setTimeout(closeMenu, 1600);
}

function closeMenu() {
  if (!menuEl) return;
  const el = menuEl;
  menuEl = null;
  untrackAnchor();
  el.classList.remove('is-open');
  setTimeout(() => el.remove(), 160);
}

function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
