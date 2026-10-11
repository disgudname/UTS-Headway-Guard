// livemap/core/data/soc.js
// -----------------------------------------------------------------------------
// Battery state of charge for the electric buses, from `/api/viriciti/soc`
// (ViriCiti / ChargePoint, see viriciti_client.py). Keyed by bus number, which
// is the TransLoc vehicle name. Shown to dispatchers only, so it only polls
// while one is signed in.
//
//   getSoc(busNumber) -> { soc, charging, low, at } | null   (at: ms of the reading)
//   onSoc(fn)         -> fn()   when any reading changes
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter, paramBool } from '../util.js';
import { isDispatcher, onDispatcher } from './session.js';

const URL = `${API_BASE}/api/viriciti/soc`;
const POLL_MS = 60_000;

const bus = emitter();
export const onSoc = (fn) => bus.on('change', fn);

const byBus = new Map(); // bus number(str) -> { soc, charging, low, at }
let started = false;
let timer = 0;

export const getSoc = (busNumber) => byBus.get(String(busNumber)) || null;

export function startSocFeed() {
  if (started) return;
  started = true;

  if (paramBool('mock')) {
    // ?mock=1&dispatcher=1: one low bus and one on the charger.
    byBus.set('6100', { soc: 22, charging: false, low: true, at: Date.now() });
    byBus.set('6101', { soc: 64, charging: true, low: false, at: Date.now() });
    bus.emit('change');
    return;
  }

  onDispatcher((isDisp) => {
    if (isDisp) spin();
    else idle();
  });
  if (isDispatcher()) spin();
}

function spin() {
  if (timer) return;
  poll();
  timer = setInterval(poll, POLL_MS);
}

function idle() {
  clearInterval(timer);
  timer = 0;
  if (byBus.size) {
    byBus.clear();
    bus.emit('change');
  }
}

async function poll() {
  try {
    const r = await fetch(URL, { cache: 'no-store' });
    if (!r.ok) return;
    const data = await r.json();
    const vehicles = data && data.vehicles ? data.vehicles : {};

    const next = new Map();
    for (const [name, v] of Object.entries(vehicles)) {
      if (!v || typeof v.soc !== 'number') continue;
      const at = Date.parse(v.timestamp);
      next.set(String(name), {
        soc: v.soc,
        charging: !!v.charging,
        low: !!v.low_battery,
        at: Number.isFinite(at) ? at : 0,
      });
    }

    let changed = next.size !== byBus.size;
    if (!changed) {
      for (const [k, v] of next) {
        const w = byBus.get(k);
        if (!w || w.soc !== v.soc || w.charging !== v.charging || w.low !== v.low || w.at !== v.at) {
          changed = true;
          break;
        }
      }
    }
    if (changed) {
      byBus.clear();
      for (const [k, v] of next) byBus.set(k, v);
      bus.emit('change');
    }
  } catch {
    /* network hiccup: keep the last readings */
  }
}
