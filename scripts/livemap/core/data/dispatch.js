// livemap/core/data/dispatch.js
// -----------------------------------------------------------------------------
// Dispatcher-only per-vehicle data: the RESOLVED block a bus is running plus its
// on-duty driver(s) and shift times. Source is `/v1/dispatch/vehicle-drivers`,
// which the server has already reduced from the raw interlined TransLoc block
// (e.g. "[05]/[03]") down to the single block that matches the bus's current
// route (or the W2W position name — "Training", "Charter", ...). The public
// `/v1/vehicle_drivers` deliberately strips `block`, so this only ever has data
// for a signed-in dispatcher.
//
//   getBlock(vehicleId)   -> "[05]" | "Training" | "" (none)
//   getDrivers(vehicleId) -> [{ name, start, end }]
//   onDispatchData(fn)    -> fn()   when the mapping changes
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { emitter, paramBool } from '../util.js';
import { isDispatcher, onDispatcher } from './session.js';

const URL = `${API_BASE}/v1/dispatch/vehicle-drivers`;
const ROSTER_URL = `${API_BASE}/v1/dispatch/block-drivers`;
const POLL_MS = 30_000;

const bus = emitter();
export const onDispatchData = (fn) => bus.on('change', fn);

const blockByVehicle = new Map(); // vehicleId(str) -> resolved block label
const driversByVehicle = new Map(); // vehicleId(str) -> [{ name, start, end }]
const shiftByName = new Map(); // driver name -> { start, end, position } for a currently-active shift
let started = false;
let timer = 0;

export const getBlock = (vehicleId) => blockByVehicle.get(String(vehicleId)) || '';
export const getDrivers = (vehicleId) => driversByVehicle.get(String(vehicleId)) || [];
/** A currently-on-duty driver's shift, matched by name — used for van driver
 *  cards, where the vehicle isn't in /vehicle-drivers. */
export const getDriverShift = (name) => (name ? shiftByName.get(name.trim()) || null : null);

export function startDispatchFeed() {
  if (started) return;
  started = true;

  if (paramBool('mock')) {
    seedMock();
    return;
  }

  // Poll only while we're a dispatcher; (re)start when that flips true.
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
  if (blockByVehicle.size || driversByVehicle.size || shiftByName.size) {
    blockByVehicle.clear();
    driversByVehicle.clear();
    shiftByName.clear();
    bus.emit('change');
  }
}

async function poll() {
  loadRoster();
  try {
    const r = await fetch(URL, { credentials: 'include', cache: 'no-store' });
    if (!r.ok) {
      if (r.status === 401 || r.status === 403) idle();
      return;
    }
    const data = await r.json();
    const vd = data && data.vehicle_drivers ? data.vehicle_drivers : {};

    const nextBlocks = new Map();
    const nextDrivers = new Map();
    for (const [id, info] of Object.entries(vd)) {
      if (!info || typeof info !== 'object') continue;
      const block = typeof info.block === 'string' ? info.block.trim() : '';
      if (block) nextBlocks.set(String(id), block);
      const drivers = Array.isArray(info.drivers)
        ? info.drivers
            .map((d) => ({
              name: (d && (d.name || '')).toString().trim(),
              start: (d && (d.shift_start_label || '')).toString().trim(),
              end: (d && (d.shift_end_label || '')).toString().trim(),
            }))
            .filter((d) => d.name)
        : [];
      if (drivers.length) nextDrivers.set(String(id), drivers);
    }

    if (!sameMap(nextBlocks, blockByVehicle) || !sameDrivers(nextDrivers, driversByVehicle)) {
      blockByVehicle.clear();
      for (const [k, v] of nextBlocks) blockByVehicle.set(k, v);
      driversByVehicle.clear();
      for (const [k, v] of nextDrivers) driversByVehicle.set(k, v);
      bus.emit('change');
    }
  } catch {
    /* network hiccup — keep the last mapping */
  }
}

/** The W2W roster -> name -> currently-active shift. Feeds van driver cards. */
async function loadRoster() {
  try {
    const r = await fetch(ROSTER_URL, { credentials: 'include', cache: 'no-store' });
    if (!r.ok) return;
    const data = await r.json();
    const byBlock = data && data.assignments_by_block ? data.assignments_by_block : {};
    const now = Date.now();
    const next = new Map();
    for (const group of Object.values(byBlock)) {
      const rows = (group && group.any) || [];
      for (const a of rows) {
        const name = (a && a.name ? a.name : '').toString().trim();
        if (!name) continue;
        const start = Number(a.start_ts) || 0;
        const end = Number(a.end_ts) || 0;
        if (start && end && (now < start - 30 * 60_000 || now > end)) continue; // not on now
        // Prefer the one whose window we're actually inside.
        const active = start <= now && now <= end;
        const cur = next.get(name);
        if (!cur || (active && !cur._active)) {
          next.set(name, {
            start: (a.start_label || '').toString().trim(),
            end: (a.end_label || '').toString().trim(),
            position: (a.position_name || '').toString().trim(),
            _active: active,
          });
        }
      }
    }
    let changed = next.size !== shiftByName.size;
    if (!changed) {
      for (const [k, v] of next) {
        const w = shiftByName.get(k);
        if (!w || w.start !== v.start || w.end !== v.end || w.position !== v.position) {
          changed = true;
          break;
        }
      }
    }
    if (changed) {
      shiftByName.clear();
      for (const [k, v] of next) shiftByName.set(k, v);
      bus.emit('change');
    }
  } catch {
    /* keep last roster */
  }
}

function sameMap(a, b) {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) if (b.get(k) !== v) return false;
  return true;
}
function sameDrivers(a, b) {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) {
    const w = b.get(k);
    if (!w || w.length !== v.length) return false;
    for (let i = 0; i < v.length; i++) {
      if (v[i].name !== w[i].name || v[i].start !== w[i].start || v[i].end !== w[i].end) return false;
    }
  }
  return true;
}

// --- mock -----------------------------------------------------------------
// ?mock=1&dispatcher=1 gets a few blocks + drivers on the mock fleet (6100..).

function seedMock() {
  const seed = [
    ['6100', '[01]', [{ name: 'Zia Ihsan', start: '12:00', end: '22:30' }]],
    ['6101', '[02]', [{ name: 'Cara Bickers', start: '20:00', end: '22:30' }]],
    ['6102', '[09]', [{ name: 'Noel Peets', start: '14:00', end: '22:30' }]],
    ['6103', '[12]', [{ name: 'Reggie Hunter', start: '14:30', end: '22:30' }]],
    ['6104', 'Training', []],
    ['6105', '[07]', [{ name: 'Aaron Boullester', start: '14:30', end: '22:30' }]],
    ['6106', 'Charter', []],
    // 6107..6110 intentionally have no block (number-primary fallback)
  ];
  for (const [id, block, drivers] of seed) {
    blockByVehicle.set(id, block);
    if (drivers.length) driversByVehicle.set(id, drivers);
  }
  bus.emit('change');
}
