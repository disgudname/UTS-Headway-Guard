// livemap/core/data/session.js
// -----------------------------------------------------------------------------
// "Am I a signed-in dispatcher?" — one small shared signal the rest of livemap
// reads to decide dispatcher-only presentation (block numbers on markers, the
// On-Demand panel section, etc.). It probes a dispatcher-gated endpoint that is
// cheap and already polled elsewhere; a 200 means dispatcher, a 401 means
// public. Re-checked periodically so a login/logout in another tab is picked up.
//
//   isDispatcher()      -> bool  dispatcher presentation should show
//   isAuthed()          -> bool  the dispatcher cookie is actually valid
//   onDispatcher(fn)     -> fn(bool)   replays current value, then on change
//   refresh()           -> re-probe now (after a sign-in / sign-out)
//
// `?dispatcher=1` forces it on for local dev (no auth cookie on localhost).
// `?adminMode=false` keeps a real dispatcher signed in but drops the
// dispatcher-only presentation (blocks on pills, OOS buses, …) — same "show me
// the public view" escape hatch the legacy testmap had.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter, paramBool } from '../util.js';
import { dispatcherOverlaysAllowed } from '../modes.js';

const PROBE_URL = `${API_BASE}/v1/uts/on_duty`;
const RECHECK_MS = 5 * 60_000;

const bus = emitter();
const forced = paramBool('dispatcher');

let dispatcher = forced;
let started = false;

/** The cookie is valid (regardless of the ?adminMode=false veto). */
export const isAuthed = () => dispatcher;
/** Dispatcher-only presentation should be shown. */
export const isDispatcher = () => dispatcher && dispatcherOverlaysAllowed();
export const onDispatcher = (fn) => {
  try { fn(isDispatcher()); } catch (e) { console.error('[livemap] dispatcher listener threw', e); }
  return bus.on('change', fn);
};

export function startSession() {
  if (started) return;
  started = true;
  if (forced) return; // pinned on for dev
  probe();
  setInterval(probe, RECHECK_MS);
}

/** Probe again right now — call after a dispatcher sign-in / sign-out. */
export function refresh() {
  if (forced) return Promise.resolve();
  return probe();
}

async function probe() {
  let next = dispatcher;
  try {
    const r = await fetch(PROBE_URL, { credentials: 'include', cache: 'no-store' });
    if (r.status === 401 || r.status === 403) next = false;
    else if (r.ok) next = true;
    // other statuses (5xx, network) -> leave the last known value alone
  } catch {
    /* network hiccup — keep the current value */
  }
  if (next !== dispatcher) {
    dispatcher = next;
    bus.emit('change', isDispatcher());
  }
}
