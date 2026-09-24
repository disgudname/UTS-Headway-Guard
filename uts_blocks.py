"""Static UTS "Block Package" schedule, compiled offline by build_uts_blocks.py
into config/uts_blocks.json (see that script's module docstring for the source
spreadsheet format, and why this needs an offline build step rather than
parsing at runtime -- the source .xlsx workbooks aren't even present in the
deployed environment, only on whoever's machine UTS ops last handed them to).

Two uses:

  1. bus_eta.py's live ETAs: lets a bus that's intentionally holding at a
     scheduled "timestop" (a UTS-specific term -- never "timepoint") get
     correctly-delayed downstream ETAs instead of just neutralizing its
     dwelling speed and guessing a typical pace (see bus_eta.py's
     DWELL_DETECTION_RADIUS_M comment for the pre-existing problem this fixes).
     scheduled_hold_epoch() answers "does THIS SPECIFIC block have a scheduled
     visit here near this live estimate?" -- it needs to know which block the
     vehicle is running (app.py already tracks this live via
     vehicle_block_lookup(), refreshed every BLOCK_REFRESH_S from TransLoc's
     own dispatch data) so it can tell a bus running early it'll have to wait,
     even before it physically gets there.

  2. trip_planner.py's UTS scheduled-wait fallback, the same role cat_gtfs.py
     plays for CAT (see trip_planner.py's _ride_leg). next_scheduled_arrival_epoch()
     doesn't know or care which specific block a rider will end up catching --
     it just answers "when's the next bus due here at all," same as a rider
     reading a printed schedule would.

config/uts_timestops.json maps a timestop code to the TransLoc StopID it
resolves to, PER RouteID -- confirmed live (2026-09-16) that TransLoc mints a
separate StopID per route for the exact same physical location, not one
shared StopID (e.g. "Madison Ave @ Preston Ave" is StopID 675 under Orange
Line's RouteID but 914 under Green Line's), so there's no way to collapse this
to one shared id per code. Only (route, code) pairs with a real live vehicle
to confirm against were mapped; an unmapped pair just leaves the schedule-hold
feature inactive for that combination -- never wrong, just a no-op.

Every route but Silver changes its timestops through the day (Gold: BAR/HER before
18:00, CHP/LIB after; Green weekend: CHP only). uts_timestops.json only says where a
code physically is, so is_timestop_at() answers "is this stop a timestop right now"
from the schedule itself. NOTE: it is NOT wired into the live ETA -- the layover cap in
app.py stays day-independent on purpose (a time-aware cap made weekend Orange ~70% late
when it was deployed 2026-09-20, and a Sunday replay on 2026-09-23 scored worse again:
>2 min late 3.1% -> 5.0%; see HANDOFF.md for why). Scheduled holds already follow the
schedule per block.

The "HOW TO GO OUT-OF-SERVICE" notes (see build_uts_blocks.py) end a block's public
service: after its last scheduled departure the bus keeps carrying passengers only
as far as a named stop. out_of_service_plan() hands bus_eta.py that stop so it can
refuse to predict stops the bus is never going to serve. Cut-off stops that aren't
timestops (e.g. McCormick Rd Dorms) live in config/uts_landmarks.json.
"""

from __future__ import annotations

import json
from bisect import bisect_left
from datetime import datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")

_CONFIG_DIR = Path(__file__).resolve().parent / "config"
_BLOCKS_PATH = _CONFIG_DIR / "uts_blocks.json"
_TIMESTOPS_PATH = _CONFIG_DIR / "uts_timestops.json"
_LANDMARKS_PATH = _CONFIG_DIR / "uts_landmarks.json"

# How far (seconds) a live/historical ETA's own estimated arrival time may be
# from a scheduled entry for that entry to be considered "the same lap" of the
# block's day, not some earlier or later lap. Generous relative to the
# ~15-40 min headways these blocks actually run (see build_uts_blocks.py) --
# this exists to reject the wrong lap entirely, not to second-guess the live
# estimate's own precision.
MATCH_TOLERANCE_S = 25 * 60.0

# How far AHEAD of a scheduled visit a bus may plausibly be running and still be matched to
# that (upcoming) visit -- i.e. "early, so it waits for the schedule". Buses measured at
# timestops arrive ~2-6 min early (headway data, fall 2026); a bus that would have to be
# ~20 min early for the next visit is far more likely LATE for the previous one. With the
# plain "nearest visit" rule, a bus ~21 min behind a 40-minute block (Gold, concert night
# 2026-09-20 20:01) matched the NEXT visit and was held ~19 min for it -- ETAs 20 min late.
# Picking the previous visit instead gives no hold (the bus is already past it).
EARLY_MATCH_LIMIT_S = 10 * 60.0

_blocks: Dict[str, Dict] = {}
_timestops: Dict[str, Dict[str, str]] = {}  # code -> {route_id: stop_id}
_stop_id_to_code: Dict[Tuple[str, str], str] = {}  # (route_id, stop_id) -> code
_landmarks: Dict[str, Dict[str, str]] = {}  # code -> {route_id: stop_id}; NOT timestops, only out-of-service cut-offs


def _load() -> None:
    global _blocks, _timestops, _stop_id_to_code, _landmarks
    _landmarks = json.loads(_LANDMARKS_PATH.read_text(encoding="utf-8")) if _LANDMARKS_PATH.exists() else {}
    _blocks = json.loads(_BLOCKS_PATH.read_text(encoding="utf-8")).get("blocks", {}) if _BLOCKS_PATH.exists() else {}
    _timestops = json.loads(_TIMESTOPS_PATH.read_text(encoding="utf-8")) if _TIMESTOPS_PATH.exists() else {}
    _stop_id_to_code = {
        (str(route_id), str(stop_id)): code
        for code, by_route in _timestops.items()
        for route_id, stop_id in by_route.items()
    }


_load()


def timestop_code_for_stop(route_id: str, stop_id: str) -> Optional[str]:
    """Is (route_id, stop_id) a mapped timestop? Returns its code, or None."""
    return _stop_id_to_code.get((str(route_id), str(stop_id)))


def _weekday_groups_matching(block: Dict, weekday: int) -> List[Dict]:
    return [g for g in block.get("weekday_groups", []) if weekday in (g.get("weekdays") or [])]


def _block_serves_route(block: Dict, route_id: str) -> bool:
    """Is this block one of the ones build_uts_blocks.py tagged as belonging to
    route_id's own route family (see config/uts_route_ids.json)? Confirmed live
    as a real bug without this check: Gold Line block [11] and Silver Line
    blocks [13]/[14] all visit the "MCQ" timestop (Massie Rd @ JPJ South Lot),
    so a route-blind "nearest scheduled visit" search could pin a Gold Line
    trip to a Silver block's completely unrelated schedule just because its
    MCQ time happened to be numerically closer, surfacing a nonsense hold Gold
    was never actually going to make. A block with no route_ids recorded
    (older data predating this field) is treated as unrestricted rather than
    silently excluded."""
    route_ids = block.get("route_ids")
    return not route_ids or str(route_id) in route_ids


def scheduled_hold_epoch(
    route_id: str, stop_id: str, block_id: Optional[str], reference_ts: float,
) -> Optional[float]:
    """If block_id has a scheduled visit to the timestop at (route_id, stop_id)
    near reference_ts (a live/historical ETA's own estimate of when the vehicle
    will get there -- NOT necessarily real "now"), returns that visit's
    scheduled epoch. None if stop_id isn't a mapped timestop for this route,
    the block has no schedule data, or nothing scheduled is close enough in
    time to plausibly be the same lap (see MATCH_TOLERANCE_S)."""
    if not block_id:
        return None
    code = timestop_code_for_stop(route_id, stop_id)
    if code is None:
        return None
    block = _blocks.get(block_id)
    if not block:
        return None

    local_dt = datetime.fromtimestamp(reference_ts, tz=NY_TZ)
    prev_epoch: Optional[float] = None   # latest visit at/before reference_ts (bus is late for it)
    next_epoch: Optional[float] = None   # earliest visit after reference_ts (bus is early for it)
    # A schedule entry just after local midnight could belong to the previous
    # evening's weekday-group rolling past midnight (see build_uts_blocks.py's
    # rollover handling) just as easily as to today's own group -- check both,
    # same shape as cat_gtfs.scheduled_departures_s.
    for day_offset in (0, -1):
        d = (local_dt + timedelta(days=day_offset)).date()
        midnight_ts = datetime.combine(d, dtime.min, tzinfo=NY_TZ).timestamp()
        for group in _weekday_groups_matching(block, d.weekday()):
            for time_s, entry_code in group.get("stops", []):
                if entry_code != code:
                    continue
                epoch = midnight_ts + time_s
                if abs(epoch - reference_ts) > MATCH_TOLERANCE_S:
                    continue
                if epoch <= reference_ts:
                    if prev_epoch is None or epoch > prev_epoch:
                        prev_epoch = epoch
                elif next_epoch is None or epoch < next_epoch:
                    next_epoch = epoch
    if next_epoch is not None and next_epoch - reference_ts <= EARLY_MATCH_LIMIT_S:
        # Plausibly early for the upcoming visit -- unless the previous visit is much closer.
        if prev_epoch is None or (next_epoch - reference_ts) <= (reference_ts - prev_epoch):
            return next_epoch
        return prev_epoch
    if prev_epoch is not None:
        return prev_epoch   # too far ahead of the next visit to be early for it: late for this one
    return next_epoch       # no earlier visit to blame (first of the day): keep the old behavior


def next_scheduled_arrival_epoch(route_id: str, stop_id: str, after_ts: float) -> Optional[float]:
    """Earliest scheduled epoch any block is due at (route_id, stop_id) at or
    after after_ts, regardless of which block -- the UTS analogue of
    cat_gtfs.scheduled_departures_s, used as trip_planner.py's scheduled-wait
    fallback when no live data exists (see _ride_leg)."""
    code = timestop_code_for_stop(route_id, stop_id)
    if code is None:
        return None
    local_dt = datetime.fromtimestamp(after_ts, tz=NY_TZ)
    best: Optional[float] = None
    for day_offset in (0, 1):
        d = (local_dt + timedelta(days=day_offset)).date()
        midnight_ts = datetime.combine(d, dtime.min, tzinfo=NY_TZ).timestamp()
        for block in _blocks.values():
            if not _block_serves_route(block, route_id):
                continue
            for group in _weekday_groups_matching(block, d.weekday()):
                for time_s, entry_code in group.get("stops", []):
                    if entry_code != code:
                        continue
                    epoch = midnight_ts + time_s
                    if epoch >= after_ts and (best is None or epoch < best):
                        best = epoch
    return best


def best_matching_block(route_id: str, stop_id: str, reference_ts: float) -> Optional[str]:
    """Which block is most plausibly the one due at (route_id, stop_id) closest
    to reference_ts -- lets a caller with no live vehicle to ask (trip
    planning, not live tracking) pin down a SPECIFIC block's schedule from one
    matched timestop, then follow that same block's schedule for the rest of
    the ride (see hold_for_ride) instead of re-querying "nearest across every
    block" at each stop, which would spuriously match a completely different
    block's next lap ~20-40 minutes later every time and misread normal
    headway as an enormous "hold". None if stop_id isn't a mapped timestop, or
    nothing scheduled is within MATCH_TOLERANCE_S of reference_ts."""
    code = timestop_code_for_stop(route_id, stop_id)
    if code is None:
        return None
    local_dt = datetime.fromtimestamp(reference_ts, tz=NY_TZ)
    best_block: Optional[str] = None
    best_diff: Optional[float] = None
    for day_offset in (0, -1):
        d = (local_dt + timedelta(days=day_offset)).date()
        midnight_ts = datetime.combine(d, dtime.min, tzinfo=NY_TZ).timestamp()
        weekday = d.weekday()
        for block_id, block in _blocks.items():
            if not _block_serves_route(block, route_id):
                continue
            for group in _weekday_groups_matching(block, weekday):
                for time_s, entry_code in group.get("stops", []):
                    if entry_code != code:
                        continue
                    epoch = midnight_ts + time_s
                    diff = abs(epoch - reference_ts)
                    if diff <= MATCH_TOLERANCE_S and (best_diff is None or diff < best_diff):
                        best_diff = diff
                        best_block = block_id
    return best_block


def hold_for_ride(
    route_id: str, stop_id: str, block_id: Optional[str], reference_ts: float,
) -> Tuple[Optional[float], Optional[str]]:
    """Trip-planning counterpart to scheduled_hold_epoch, for a rider's
    planned ride rather than a live vehicle: if block_id is None (the first
    mapped timestop this ride's walk has hit), pins one via
    best_matching_block; either way, then checks THAT SAME block's schedule
    at this stop. Returns (hold_epoch_or_None, the block_id used or newly
    pinned) -- the caller (trip_planner._estimate_ride_seconds) threads the
    returned block_id into its next call so the whole ride stays pinned to
    one consistent block's schedule instead of drifting between blocks stop
    to stop."""
    if block_id is None:
        block_id = best_matching_block(route_id, stop_id, reference_ts)
        if block_id is None:
            return None, None
    return scheduled_hold_epoch(route_id, stop_id, block_id, reference_ts), block_id


# A stop counts as a timestop at time T if some block on the route has a scheduled visit
# to it within this many seconds of T. Timestop visits are 10-20 minutes apart, so half
# an hour either side covers the gaps without leaking across the 18:00 route change.
ACTIVE_TIMESTOP_WINDOW_S = 30 * 60

_visit_cache: Tuple[Optional[Dict], Dict] = (None, {})


def _visit_index() -> Dict[Tuple[str, str], Dict[int, List[int]]]:
    """{(route_id or "*", code): {weekday: sorted [time_s, ...]}} over every block's schedule.
    Rebuilt whenever _blocks is replaced (tests monkeypatch it)."""
    global _visit_cache
    if _visit_cache[0] is not _blocks:
        idx: Dict[Tuple[str, str], Dict[int, List[int]]] = {}
        for block in _blocks.values():
            routes = [str(r) for r in (block.get("route_ids") or [])] or ["*"]
            for group in block.get("weekday_groups", []):
                for weekday in group.get("weekdays") or []:
                    for time_s, code in group.get("stops", []):
                        for route in routes:
                            idx.setdefault((route, code), {}).setdefault(weekday, []).append(time_s)
        for by_weekday in idx.values():
            for times in by_weekday.values():
                times.sort()
        _visit_cache = (_blocks, idx)
    return _visit_cache[1]


def _any_within(times: Optional[List[int]], t: float, window: float) -> bool:
    if not times:
        return False
    i = bisect_left(times, t - window)
    return i < len(times) and times[i] <= t + window


def is_timestop_at(route_id: str, stop_id: str, when: float) -> bool:
    """Is (route_id, stop_id) a timestop AT THIS TIME OF DAY? It must be a mapped timestop
    (uts_timestops.json) AND some block serving the route must have a scheduled visit to
    that code within ACTIVE_TIMESTOP_WINDOW_S of `when`. Time is read as America/New_York
    local time and also checks the previous day's schedule, whose entries run past midnight
    (Night Pilot), the same way scheduled_hold_epoch does."""
    code = timestop_code_for_stop(route_id, stop_id)
    if code is None:
        return False
    idx = _visit_index()
    local = datetime.fromtimestamp(when, tz=NY_TZ)
    seconds = local.hour * 3600 + local.minute * 60 + local.second
    weekday = local.weekday()
    for route_key in (str(route_id), "*"):
        by_weekday = idx.get((route_key, code))
        if not by_weekday:
            continue
        if _any_within(by_weekday.get(weekday), seconds, ACTIVE_TIMESTOP_WINDOW_S):
            return True
        if _any_within(by_weekday.get((weekday - 1) % 7), seconds + 86400, ACTIVE_TIMESTOP_WINDOW_S):
            return True
    return False


def _stop_for_code(route_id: str, code: Optional[str]) -> Optional[str]:
    """The StopID a timestop code or landmark code resolves to on this route, if mapped."""
    if not code:
        return None
    return (_timestops.get(code) or {}).get(str(route_id)) or (_landmarks.get(code) or {}).get(str(route_id))


def out_of_service_plan(
    route_id: str, block_id: Optional[str], when: float,
) -> Optional[Tuple[str, float, str, float]]:
    """If block_id is about to make (or is making) its last public trip of the day on this
    route, returns (leave_stop_id, leave_epoch, cutoff_stop_id, active_from_epoch):

      leave_stop_id / leave_epoch -- the scheduled last departure the note names
      cutoff_stop_id -- the last stop the bus will serve (the note's "as far as ..." stop,
                        else the "stay in service until ..." stop)
      active_from_epoch -- how early a bus may be and still count as on that last trip

    None if the block has no note today, the cut-off can't be resolved to a stop on this
    route (an unmapped stop just leaves the feature off -- never wrong, only a no-op), or
    the note's departure is not within a few hours of `when`."""
    block = _blocks.get(block_id) if block_id else None
    if not block or not _block_serves_route(block, route_id):
        return None
    local_dt = datetime.fromtimestamp(when, tz=NY_TZ)
    for day_offset in (0, -1):
        d = (local_dt + timedelta(days=day_offset)).date()
        midnight_ts = datetime.combine(d, dtime.min, tzinfo=NY_TZ).timestamp()
        for group in _weekday_groups_matching(block, d.weekday()):
            note = group.get("out_of_service")
            if not note:
                continue
            leave_epoch = midnight_ts + note["leave_s"]
            if not (leave_epoch - 3600 <= when <= leave_epoch + 3 * 3600):
                continue
            leave_stop = _stop_for_code(route_id, note.get("leave_code"))
            cutoff_stop = _stop_for_code(route_id, note.get("last_code") or note.get("until_code"))
            if leave_stop is None or cutoff_stop is None:
                continue
            return leave_stop, leave_epoch, cutoff_stop, leave_epoch - EARLY_MATCH_LIMIT_S
    return None


def is_loaded() -> bool:
    return bool(_blocks)
