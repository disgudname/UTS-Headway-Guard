// livemap/core/theme.js
// -----------------------------------------------------------------------------
// The map's light/dark state. Three user-facing modes:
//
//   'auto'  -> follow the OS setting (prefers-color-scheme). In kiosk shells
//              this can later be swapped for solar (civil-twilight) switching.
//   'light' -> force day
//   'dark'  -> force night
//
// Applying a theme swaps the whole basemap style document (see basemap-style.js)
// and stamps <html data-theme> so the UI chrome in css/livemap.css follows.
// -----------------------------------------------------------------------------

import { STORAGE, SOLAR_ANCHOR } from './config.js';
import { lsGet, lsSet, emitter, param } from './util.js';
import { loadBasemapTreatments } from './basemap-style.js';
import { getMap, replayStyleBuilders, whenStyleReady } from './map.js';
import { useSolarTheme } from './modes.js';
import { isDarkNow } from './solar.js';

// How often the kiosk shells re-check the sun while in 'auto'.
const SOLAR_RECHECK_MS = 10 * 60_000;

const bus = emitter();
/** on(fn(effective, mode)) — fires on every theme change; returns unsubscribe. */
export const onThemeChange = (fn) => bus.on('change', fn);

const VALID_MODES = new Set(['auto', 'light', 'dark']);

// ?theme=light|dark|auto is an explicit override (kiosks, embeds, screenshots).
// It wins over stored preference for this page load but is not persisted.
const urlTheme = normalizeMode(param('theme', null), null);
let mode = urlTheme ?? normalizeMode(lsGet(STORAGE.themeMode, 'auto'));
let treatments = null; // { light, dark } style docs
let applying = false;
let currentEffective = null; // the 'light'|'dark' currently stamped/applied

function normalizeMode(m, fallback = 'auto') {
  return VALID_MODES.has(m) ? m : fallback;
}

const systemDarkQuery =
  typeof window !== 'undefined' && window.matchMedia
    ? window.matchMedia('(prefers-color-scheme: dark)')
    : null;

function systemPrefersDark() {
  return !!systemDarkQuery && systemDarkQuery.matches;
}

/** The concrete theme ('light' | 'dark') implied by the current mode. */
export function getEffectiveTheme() {
  if (mode === 'light' || mode === 'dark') return mode;
  // 'auto': kiosk shells follow the sun over Grounds; everyone else the OS.
  if (useSolarTheme()) {
    return isDarkNow(SOLAR_ANCHOR.lat, SOLAR_ANCHOR.lon) ? 'dark' : 'light';
  }
  return systemPrefersDark() ? 'dark' : 'light';
}

export function getThemeMode() {
  return mode;
}

/**
 * Load the basemap treatments and apply the current theme. Returns the initial
 * style document so the caller can hand it straight to createMap().
 */
export async function initTheme() {
  treatments = await loadBasemapTreatments();
  const effective = getEffectiveTheme();
  stampDocument(effective);

  // React to OS theme flips while in 'auto'.
  systemDarkQuery?.addEventListener?.('change', () => {
    if (mode === 'auto') applyTheme();
  });

  // Kiosk shells have no OS to listen to — poll the sun instead.
  if (useSolarTheme()) {
    setInterval(() => {
      if (mode === 'auto' && getEffectiveTheme() !== currentEffective) applyTheme();
    }, SOLAR_RECHECK_MS);
  }

  bus.emit('change', effective, mode);
  return treatments[effective];
}

/** Change mode, persist it, and re-apply. */
export function setThemeMode(nextMode) {
  const next = normalizeMode(nextMode);
  if (next === mode) return;
  mode = next;
  lsSet(STORAGE.themeMode, mode);
  applyTheme();
}

/** Convenience: flip between light and dark (drops 'auto'). */
export function toggleTheme() {
  setThemeMode(getEffectiveTheme() === 'dark' ? 'light' : 'dark');
}

function stampDocument(effective) {
  currentEffective = effective;
  if (typeof document !== 'undefined') {
    document.documentElement.dataset.theme = effective;
  }
}

function applyTheme() {
  const map = getMap();
  const effective = getEffectiveTheme();
  stampDocument(effective);
  bus.emit('change', effective, mode);

  if (!map || !treatments || applying) return;
  applying = true;

  // setStyle wipes custom layers; replay them once the new style settles.
  map.once('style.load', () => {
    whenStyleReady(() => {
      replayStyleBuilders();
      applying = false;
    });
  });
  map.setStyle(treatments[effective], { diff: false });
}
