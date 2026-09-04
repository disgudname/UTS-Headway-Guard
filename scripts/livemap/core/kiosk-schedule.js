// livemap/core/kiosk-schedule.js
// -----------------------------------------------------------------------------
// adminKiosk time-of-day overlay schedule — parity with testmap's admin kiosk.
//
// A back-office / dispatcher wall display has no one to toggle overlays, so it
// runs them on a clock: the UVA Ride (On-Demand) overlay auto-enables overnight
// (19:30 -> 05:30 local), when the fixed-route buses have stopped, and turns
// itself off again in the morning. Mirrors testmap's
// ADMIN_KIOSK_ONDEMAND_START/END schedule.
//
// Only wired for operator mode 'adminKiosk' (see apps/boot.js), and only acts
// once the micro feed reports itself available (i.e. the dispatch cookie is
// present on this machine) — otherwise there's nothing to show.
//
// NOT ported from testmap:
//   * the 02:30-04:30 switch to the "UVA Health" agency feed — livemap is
//     single-feed (one TRANSLOC_BASE), it has no agencies / changeAgency.
//   * the 07:00-19:00 Spare/FlexRide auto-enable — in testmap that fires for
//     *every* authorised session, not just kiosks, so it's a broader behaviour
//     change, not an adminKiosk gap.
// -----------------------------------------------------------------------------

import { onMicroAvailable, isMicroAvailable, setMicroEnabled } from './data/microtransit.js';

// Local-time window, in minutes past midnight. START > END => wraps midnight.
const RIDE_START_MIN = 19 * 60 + 30; // 19:30
const RIDE_END_MIN = 5 * 60 + 30; //   05:30
const MAX_TICK_MS = 30 * 60 * 1000; // never sleep longer than 30 min
const MIN_TICK_MS = 1000;

let timer = 0;

function nowMinutes() {
  const d = new Date();
  return d.getHours() * 60 + d.getMinutes();
}

/** Are we inside the overnight UVA Ride window right now? */
function rideWindowActive() {
  const m = nowMinutes();
  return RIDE_START_MIN <= RIDE_END_MIN
    ? m >= RIDE_START_MIN && m < RIDE_END_MIN
    : m >= RIDE_START_MIN || m < RIDE_END_MIN; // wrapped window
}

/** ms until the next window boundary (so we re-check right when it flips). */
function msToNextBoundary() {
  const d = new Date();
  const nowMs =
    ((d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds()) * 1000 + d.getMilliseconds();
  const startMs = RIDE_START_MIN * 60 * 1000;
  const endMs = RIDE_END_MIN * 60 * 1000;
  const dayMs = 24 * 60 * 60 * 1000;
  const nextStart = nowMs < startMs ? startMs : startMs + dayMs;
  const nextEnd = nowMs < endMs ? endMs : endMs + dayMs;
  const target = rideWindowActive() ? nextEnd : nextStart;
  return Math.max(MIN_TICK_MS, Math.min(MAX_TICK_MS, target - nowMs));
}

function enforce() {
  if (isMicroAvailable()) {
    setMicroEnabled('ride', rideWindowActive());
  }
  clearTimeout(timer);
  timer = setTimeout(enforce, msToNextBoundary());
}

/** Call once, from boot.js, only in operator mode 'adminKiosk'. */
export function startKioskSchedule() {
  onMicroAvailable(() => enforce()); // replays current value, then on change
  enforce();
}
