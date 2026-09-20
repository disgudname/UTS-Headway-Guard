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
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")

_CONFIG_DIR = Path(__file__).resolve().parent / "config"
_BLOCKS_PATH = _CONFIG_DIR / "uts_blocks.json"
_TIMESTOPS_PATH = _CONFIG_DIR / "uts_timestops.json"

# How far (seconds) a live/historical ETA's own estimated arrival time may be
# from a scheduled entry for that entry to be considered "the same lap" of the
# block's day, not some earlier or later lap. Generous relative to the
# ~15-40 min headways these blocks actually run (see build_uts_blocks.py) --
# this exists to reject the wrong lap entirely, not to second-guess the live
# estimate's own precision.
MATCH_TOLERANCE_S = 25 * 60.0

_blocks: Dict[str, Dict] = {}
_timestops: Dict[str, Dict[str, str]] = {}  # code -> {route_id: stop_id}
_stop_id_to_code: Dict[Tuple[str, str], str] = {}  # (route_id, stop_id) -> code


def _load() -> None:
    global _blocks, _timestops, _stop_id_to_code
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
    best_epoch: Optional[float] = None
    best_diff: Optional[float] = None
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
                diff = abs(epoch - reference_ts)
                if diff <= MATCH_TOLERANCE_S and (best_diff is None or diff < best_diff):
                    best_diff = diff
                    best_epoch = epoch
    return best_epoch


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


def is_timestop_active(route_id: str, stop_id: str, reference_ts: float) -> bool:
    """Is (route_id, stop_id) a timestop where drivers are ACTUALLY scheduled to
    layover around reference_ts? The config/uts_timestops.json mapping is per
    route only, but the Block Packages differ by day group and time of day (e.g.
    Green weekday: MP/HER 07:30-17:45, CHP/JPA 18:00-22:00; Green weekend: CHP
    only). So a mapped stop counts only if some block serving this route has a
    scheduled visit to its code within MATCH_TOLERANCE_S of reference_ts. Where
    the route has no schedule data at all for that day (e.g. Night Pilot), there is
    nothing to contradict the mapping, so it stays a timestop (the old behavior)."""
    code = timestop_code_for_stop(route_id, stop_id)
    if code is None:
        return False
    local_dt = datetime.fromtimestamp(reference_ts, tz=NY_TZ)
    have_schedule = False
    for day_offset in (0, -1):
        d = (local_dt + timedelta(days=day_offset)).date()
        midnight_ts = datetime.combine(d, dtime.min, tzinfo=NY_TZ).timestamp()
        for block in _blocks.values():
            if not _block_serves_route(block, route_id):
                continue
            for group in _weekday_groups_matching(block, d.weekday()):
                stops = group.get("stops", [])
                if day_offset == 0 and stops:
                    have_schedule = True
                for time_s, entry_code in stops:
                    if entry_code == code and abs(midnight_ts + time_s - reference_ts) <= MATCH_TOLERANCE_S:
                        return True
    return not have_schedule


def is_loaded() -> bool:
    return bool(_blocks)
