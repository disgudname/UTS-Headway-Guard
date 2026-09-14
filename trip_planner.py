"""Trip-planning core for /livemap: "walk -> ride (maybe one transfer) -> walk".

This module is intentionally decoupled from TransLoc/CAT's raw JSON shapes and from
FastAPI's `state` object -- callers in app.py adapt those into the plain dataclasses
below (`Line`, `Stop`, `RouteService`) so this module can be unit tested with small
synthetic fixtures and, if the callers ever want to, reused outside app.py entirely.

Two things here exist specifically because a naive planner would strand someone:

- `build_route_service()` reads TransLoc's own per-block schedule (already polled into
  `state.blocks_cache` every ~5s -- see app.py's updater loop) to know exactly when a
  route stops running today, rather than assuming a route that exists is a route that's
  in service right now.
- The same function also detects *interlining*: TransLoc represents a schedule/route
  change mid-shift (e.g. Gold Line's RouteID literally changes at 5:50pm) as two
  schedule phases sharing one physical block. A rider mid-ride across that boundary is
  not stranded and does not need to "transfer" -- see `chain_next` below.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

# --- walking --------------------------------------------------------------------

EARTH_RADIUS_M = 6_371_000.0
WALK_SPEED_MPS = 1.3  # ~2.9 mph, a relaxed campus walking pace
WALK_DETOUR_FACTOR = 1.3  # straight-line underestimates any real path; pad for it
WALK_SNAP_RADIUS_M = 600.0  # how far a rider is assumed willing to walk to/from a stop
MAX_WALK_ONLY_DISTANCE_M = 3000.0  # beyond this, a walk-only itinerary isn't worth offering
TRANSFER_WALK_RADIUS_M = 250.0  # how far apart two stops can be and still count as a transfer

# --- service windows / interlining -----------------------------------------------

NON_PASSENGER_RE = re.compile(r"charter|training|\btest\b", re.IGNORECASE)
INTERLINE_GAP_TOLERANCE_S = 120.0  # same BlockId, phases this close together = one run
LAST_RIDE_WARNING_S = 20 * 60.0  # flag "last bus" inside this window of a route's end


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


@dataclass
class WalkLeg:
    kind: str = field(default="walk", init=False)
    coordinates: List[Tuple[float, float]] = field(default_factory=list)  # [(lat, lon), ...]
    distance_m: float = 0.0
    duration_s: float = 0.0
    source: str = "straight_line"  # swap point for a real router later -- see ROUTING_ENGINE.md


def estimate_walk_leg(start: Tuple[float, float], end: Tuple[float, float]) -> WalkLeg:
    """The only place walk-leg geometry gets computed. v1 is a straight-line estimate;
    a real router (see ROUTING_ENGINE.md) drops in here without touching any caller --
    they all consume {coordinates, distance_m, duration_s, source} regardless of how it
    was produced."""
    straight_m = haversine_m(start[0], start[1], end[0], end[1])
    distance_m = straight_m * WALK_DETOUR_FACTOR
    duration_s = distance_m / WALK_SPEED_MPS
    return WalkLeg(
        coordinates=[start, end],
        distance_m=distance_m,
        duration_s=duration_s,
        source="straight_line",
    )


# --- route graph ------------------------------------------------------------------


@dataclass
class Stop:
    id: str
    name: str
    lat: float
    lon: float
    source: str  # "uts" | "cat"


@dataclass
class Line:
    """One ridable line: an ordered sequence of stops a vehicle visits, in the order
    it visits them. UTS routes are loops (`loop=True`, any stop can reach any other);
    CAT patterns are point-to-point (`loop=False`, only forward order is reachable)."""

    id: str
    name: str
    color: str
    source: str  # "uts" | "cat"
    stops: List[Stop]
    loop: bool


def is_non_passenger_group(block_group_id: str, route_name: str) -> bool:
    """Charter/Training/Test block groups exist in TransLoc's schedule but aren't real
    passenger service -- there's no existing blocklist for this anywhere else in the
    app, so this is new and scoped to the trip planner only."""
    return bool(NON_PASSENGER_RE.search(block_group_id or "")) or bool(
        NON_PASSENGER_RE.search(route_name or "")
    )


def _parse_clock(time_str: str, reference_date: datetime) -> Optional[datetime]:
    """Parse TransLoc's "HH:MM AM/PM" block time against reference_date's calendar day.
    Unlike app.py's `_parse_block_time_today` (which this deliberately does not reuse --
    that's shared dispatcher-critical code and touching it is out of scope here), this
    parser is midnight-safe: `build_route_service` calls it once for the phase start,
    then again for the phase end with a rollover check, since a phase can legitimately
    span midnight (e.g. a Night Pilot block from 11:30pm to 1:30am)."""
    if not time_str:
        return None
    try:
        parsed = datetime.strptime(time_str.strip(), "%I:%M %p")
    except ValueError:
        return None
    return reference_date.replace(
        hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0
    )


def _parse_phase_window(
    start_str: str, end_str: str, reference_date: datetime
) -> Optional[Tuple[float, float]]:
    start_dt = _parse_clock(start_str, reference_date)
    end_dt = _parse_clock(end_str, reference_date)
    if start_dt is None or end_dt is None:
        return None
    if end_dt < start_dt:
        end_dt += timedelta(days=1)  # end time rolled past midnight
    return start_dt.timestamp(), end_dt.timestamp()


@dataclass
class RoutePhase:
    route_id: str
    block_group_id: str
    block_id: str
    start_ts: float
    end_ts: float


@dataclass
class RouteService:
    """Today's real service windows per UTS RouteID, plus which route each route's
    physical vehicle continues into (interlining) when one schedule phase hands off to
    another with no meaningful gap."""

    windows: Dict[str, List[Tuple[float, float]]] = field(default_factory=dict)
    chain_next: Dict[str, str] = field(default_factory=dict)

    def effective_window(self, route_id: str) -> Optional[Tuple[float, float]]:
        """This route's own window, extended forward through any interline chain (e.g.
        Gold Line 67's effective end becomes Gold Line 57's end, since it's the same
        physical bus continuing under a relabeled RouteID)."""
        windows = self.windows.get(route_id)
        if not windows:
            return None
        start = min(w[0] for w in windows)
        end = max(w[1] for w in windows)
        seen = {route_id}
        current = route_id
        while current in self.chain_next:
            nxt = self.chain_next[current]
            if nxt in seen:
                break
            nxt_windows = self.windows.get(nxt)
            if not nxt_windows:
                break
            end = max(end, max(w[1] for w in nxt_windows))
            seen.add(nxt)
            current = nxt
        return start, end


def build_route_service(
    block_groups: List[dict], reference_date: datetime
) -> RouteService:
    """Build today's per-RouteID service windows + interline chain table from TransLoc's
    own `GetDispatchBlockGroupData` shape (already polled into `state.blocks_cache`, no
    new fetch needed -- see app.py's updater loop).

    `block_groups` is the raw list: [{BlockGroupId, Blocks: [{BlockId, BlockStartTime,
    BlockEndTime, Route: {RouteId, Description}, ...}]}].
    """
    windows: Dict[str, List[Tuple[float, float]]] = {}
    chain_next: Dict[str, str] = {}

    for group in block_groups or []:
        if not isinstance(group, dict):
            continue
        block_group_id = str(group.get("BlockGroupId") or "").strip()
        phases: List[RoutePhase] = []
        for block in group.get("Blocks") or []:
            if not isinstance(block, dict):
                continue
            route = block.get("Route") or {}
            route_id = route.get("RouteId") if route.get("RouteId") is not None else route.get("RouteID")
            route_name = route.get("Description") or route.get("RouteName") or ""
            if route_id is None:
                continue
            if is_non_passenger_group(block_group_id, route_name):
                continue
            window = _parse_phase_window(
                block.get("BlockStartTime") or "", block.get("BlockEndTime") or "", reference_date
            )
            if window is None:
                continue
            block_id = str(block.get("BlockId") or "")
            start_ts, end_ts = window
            phases.append(
                RoutePhase(
                    route_id=str(route_id),
                    block_group_id=block_group_id,
                    block_id=block_id,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            )
            windows.setdefault(str(route_id), []).append((start_ts, end_ts))

        # Interlining: within this block group, a phase that ends where the very next
        # (by start time) phase on the SAME BlockId begins is one physical vehicle
        # continuing under a (possibly) different RouteID -- e.g. Gold Line's RouteID
        # 67 ending at 5:50pm and RouteID 57 starting at 5:51pm, both "Gold_01".
        by_block_id: Dict[str, List[RoutePhase]] = {}
        for phase in phases:
            by_block_id.setdefault(phase.block_id, []).append(phase)
        for chain in by_block_id.values():
            chain.sort(key=lambda p: p.start_ts)
            for a, b in zip(chain, chain[1:]):
                if a.route_id == b.route_id:
                    continue
                if b.start_ts - a.end_ts <= INTERLINE_GAP_TOLERANCE_S:
                    chain_next[a.route_id] = b.route_id

    return RouteService(windows=windows, chain_next=chain_next)


# --- trip finding -------------------------------------------------------------------


@dataclass
class RideLeg:
    kind: str = field(default="ride", init=False)
    line_id: str = ""
    line_name: str = ""
    color: str = "#888888"
    board_stop: Optional[Stop] = None
    alight_stop: Optional[Stop] = None
    path: List[Stop] = field(default_factory=list)  # every stop from board to alight, in order
    wait_s: Optional[float] = None  # None = no live ETA available
    ride_s: Optional[float] = None
    ride_s_source: str = "heuristic"  # "historical" if every segment came from real data
    service_ends_ts: Optional[float] = None
    last_ride_warning: bool = False


@dataclass
class Itinerary:
    legs: List[object] = field(default_factory=list)
    total_duration_s: float = 0.0
    duration_is_estimate: bool = False  # true if any leg's wait/ride time is unknown


SECONDS_PER_HOP_ESTIMATE = 90.0  # rough per-stop dwell+travel time -- used per-segment
# whenever hop_time_fn (see find_trips) has no real historical sample for that segment.

# (route_id, from_stop_id, to_stop_id, when_epoch_s) -> seconds, or None if unknown.
# See trip_planner_history.HopTimeModel.lookup for the real implementation, built from
# headway_storage's arrival events. Kept as an injected callback (not an import) so
# this module stays testable without needing real event history.
HopTimeFn = Callable[[str, str, str, float], Optional[float]]


def _stop_index_within_radius(
    line: Line, point: Tuple[float, float], radius_m: float
) -> List[int]:
    return [
        i
        for i, s in enumerate(line.stops)
        if haversine_m(point[0], point[1], s.lat, s.lon) <= radius_m
    ]


def nearby_stop_ids(
    lines: List[Line], point: Tuple[float, float], radius_m: float = WALK_SNAP_RADIUS_M
) -> set:
    """Every stop id within radius_m of point, across all given lines. Public helper
    for callers that need to pre-fetch live data for a bounded set of stops before
    calling find_trips -- e.g. CAT has no bulk "every stop's ETAs" endpoint the way
    TransLoc does, so app.py uses this to fetch CAT ETAs only for stops a trip might
    actually board/alight at, rather than every CAT stop system-wide."""
    ids: set = set()
    for line in lines:
        for i in _stop_index_within_radius(line, point, radius_m):
            ids.add(line.stops[i].id)
    return ids


def _hop_distance(line: Line, board_idx: int, alight_idx: int) -> Optional[int]:
    """Number of stops between board and alight in the direction the line travels, or
    None if unreachable (destination is "behind" the boarding stop on a non-loop line)."""
    n = len(line.stops)
    if alight_idx >= board_idx:
        return alight_idx - board_idx
    if line.loop:
        return (n - board_idx) + alight_idx
    return None


def _segment_stop_pairs(line: Line, board_idx: int, alight_idx: int) -> List[Tuple[Stop, Stop]]:
    """Every consecutive (from, to) stop pair a rider passes through between board_idx
    and alight_idx, in travel order (wrapping around for a loop)."""
    n = len(line.stops)
    pairs: List[Tuple[Stop, Stop]] = []
    i = board_idx
    while i != alight_idx:
        j = (i + 1) % n
        pairs.append((line.stops[i], line.stops[j]))
        i = j
    return pairs


def _path_stops(line: Line, board_idx: int, alight_idx: int) -> List[Stop]:
    """Every stop from board to alight, inclusive, in travel order (wrapping around
    for a loop) -- the real geometry a rider actually passes through, for rendering
    the ride leg on a map instead of a straight line cutting through buildings."""
    n = len(line.stops)
    path = [line.stops[board_idx]]
    i = board_idx
    while i != alight_idx:
        i = (i + 1) % n
        path.append(line.stops[i])
    return path


def _estimate_ride_seconds(
    line: Line,
    board_idx: int,
    alight_idx: int,
    when: float,
    hop_time_fn: Optional[HopTimeFn],
) -> Tuple[float, str]:
    """Sum real historical segment times where available (see HopTimeFn), falling back
    to the flat per-segment heuristic elsewhere. Returns (total_seconds, source) where
    source is "historical" only if EVERY segment had a real sample -- a ride that's
    partly guessed shouldn't claim full historical confidence."""
    total = 0.0
    all_historical = True
    for from_stop, to_stop in _segment_stop_pairs(line, board_idx, alight_idx):
        seconds = hop_time_fn(line.id, from_stop.id, to_stop.id, when) if hop_time_fn else None
        if seconds is None:
            all_historical = False
            seconds = SECONDS_PER_HOP_ESTIMATE
        total += seconds
    return total, ("historical" if all_historical else "heuristic")


def _ride_leg(
    line: Line,
    board_idx: int,
    alight_idx: int,
    board_time: float,
    route_service: Optional[RouteService],
    live_wait_lookup: Dict[Tuple[str, str], Optional[float]],
    hop_time_fn: Optional[HopTimeFn] = None,
) -> Optional[Tuple[RideLeg, float]]:
    """Build one ride leg boarding at `board_time` (epoch seconds), or None if this line
    can't be ridden from board_idx to alight_idx at all, or isn't in service at that time.
    Returns (leg, time_rider_actually_alights)."""
    hops = _hop_distance(line, board_idx, alight_idx)
    if hops is None or hops == 0:
        return None

    board_stop = line.stops[board_idx]
    alight_stop = line.stops[alight_idx]
    wait_s = live_wait_lookup.get((line.id, board_stop.id))
    ride_s, ride_s_source = _estimate_ride_seconds(line, board_idx, alight_idx, board_time, hop_time_fn)

    actual_board_time = board_time + (wait_s or 0.0)
    service_ends_ts: Optional[float] = None
    last_ride_warning = False

    if line.source == "uts":
        if route_service is None:
            return None
        window = route_service.effective_window(line.id)
        if window is None:
            return None  # not scheduled today at all (e.g. a night-only variant at noon)
        start_ts, end_ts = window
        if actual_board_time < start_ts or actual_board_time > end_ts:
            return None  # route hasn't started yet today, or has already ended
        alight_time_est = actual_board_time + ride_s
        if alight_time_est > end_ts:
            return None  # would still be riding after the (possibly chained) route ends
        service_ends_ts = end_ts
        last_ride_warning = (end_ts - actual_board_time) <= LAST_RIDE_WARNING_S
    else:
        # CAT: no block-schedule data available (separate agency/API). Require a live
        # ETA as the availability signal -- no live arrival means don't recommend it,
        # rather than presenting a pattern that may not actually be running right now.
        if wait_s is None:
            return None

    leg = RideLeg(
        line_id=line.id,
        line_name=line.name,
        color=line.color,
        board_stop=board_stop,
        alight_stop=alight_stop,
        path=_path_stops(line, board_idx, alight_idx),
        wait_s=wait_s,
        ride_s=ride_s,
        ride_s_source=ride_s_source,
        service_ends_ts=service_ends_ts,
        last_ride_warning=last_ride_warning,
    )
    return leg, actual_board_time + ride_s


def find_trips(
    origin: Tuple[float, float],
    destination: Tuple[float, float],
    lines: List[Line],
    route_service: RouteService,
    live_wait_lookup: Dict[Tuple[str, str], Optional[float]],
    when: float,
    max_results: int = 4,
    hop_time_fn: Optional[HopTimeFn] = None,
) -> List[Itinerary]:
    """Rank up to `max_results` walk -> ride[-> walk -> ride] -> walk itineraries.

    `live_wait_lookup` maps (line_id, stop_id) -> seconds until next arrival, or should
    simply omit a key when no live ETA is known for that line/stop pair right now.

    `hop_time_fn`, if given, is consulted for real historical per-segment ride times
    (see trip_planner_history.HopTimeModel.lookup); segments it doesn't know fall back
    to the flat SECONDS_PER_HOP_ESTIMATE heuristic.
    """
    origin_candidates: Dict[str, List[int]] = {}
    dest_candidates: Dict[str, List[int]] = {}
    for line in lines:
        oi = _stop_index_within_radius(line, origin, WALK_SNAP_RADIUS_M)
        if oi:
            origin_candidates[line.id] = oi
        di = _stop_index_within_radius(line, destination, WALK_SNAP_RADIUS_M)
        if di:
            dest_candidates[line.id] = di

    lines_by_id = {ln.id: ln for ln in lines}
    itineraries: List[Itinerary] = []

    def nearest_index(line: Line, indices: List[int], point: Tuple[float, float]) -> int:
        return min(indices, key=lambda i: haversine_m(point[0], point[1], line.stops[i].lat, line.stops[i].lon))

    # Walk-only: always a candidate for a short enough trip, and NOT implied by any
    # ride-based candidate below (those only ever compare routes against each other).
    # Without this, a two-transfer 15-minute bus itinerary can "win" over a literal
    # 4-minute walk simply because no walk-only option was ever generated to compete
    # with it -- confirmed live: Rice Hall -> Scott Stadium is 259m straight-line, but
    # every candidate here is ride-based, so the best of them (however much worse than
    # walking) always surfaced as the top result until this was added.
    straight_m = haversine_m(origin[0], origin[1], destination[0], destination[1])
    if straight_m <= MAX_WALK_ONLY_DISTANCE_M:
        itineraries.append(_build_itinerary([estimate_walk_leg(origin, destination)]))

    # Direct rides. Tries every (origin-adjacent stop, destination-adjacent stop) pair
    # on the line, not just the single stop nearest each point independently -- on a
    # loop, the stop nearest the origin and the stop nearest the destination can sit on
    # "opposite sides" of the loop's one-way travel order even when they're
    # geographically close, while a different nearby stop pairing offers a much shorter
    # ride. Picking only the nearest-to-each-point pair (as an earlier version of this
    # did) missed those short rides entirely and could surface a needless transfer --
    # or a needless bus ride at all -- instead of a short direct hop. Confirmed live:
    # Rice Hall -> Scott Stadium (259m apart) was only offered as multi-transfer,
    # 600m-plus-walk itineraries until this was fixed.
    for line_id in set(origin_candidates) & set(dest_candidates):
        line = lines_by_id[line_id]
        pair = _best_direct_pair(line, origin_candidates[line_id], dest_candidates[line_id])
        if pair is None:
            continue
        board_idx, alight_idx = pair
        board_stop = line.stops[board_idx]
        alight_stop = line.stops[alight_idx]

        walk_to = estimate_walk_leg(origin, (board_stop.lat, board_stop.lon))
        board_time = when + walk_to.duration_s
        result = _ride_leg(line, board_idx, alight_idx, board_time, route_service, live_wait_lookup, hop_time_fn)
        if result is None:
            continue
        ride_leg, alight_time = result
        walk_from = estimate_walk_leg((alight_stop.lat, alight_stop.lon), destination)

        legs = [walk_to, ride_leg, walk_from]
        itineraries.append(_build_itinerary(legs))

    # One-transfer rides: a line through the origin, a line through the destination,
    # connected either at a shared physical stop or a short walk between two stops.
    for line_a_id, a_origin_idxs in origin_candidates.items():
        line_a = lines_by_id[line_a_id]
        for line_b_id, b_dest_idxs in dest_candidates.items():
            if line_a_id == line_b_id:
                continue
            line_b = lines_by_id[line_b_id]
            best = _best_transfer(line_a, a_origin_idxs, line_b, b_dest_idxs)
            if best is None:
                continue
            a_board_idx, a_alight_idx, b_board_idx, transfer_walk = best

            a_board_stop = line_a.stops[a_board_idx]
            walk_to = estimate_walk_leg(origin, (a_board_stop.lat, a_board_stop.lon))
            board_time = when + walk_to.duration_s
            result_a = _ride_leg(
                line_a, a_board_idx, a_alight_idx, board_time, route_service, live_wait_lookup, hop_time_fn
            )
            if result_a is None:
                continue
            ride_a, arrive_at_transfer = result_a

            a_alight_stop = line_a.stops[a_alight_idx]
            b_board_stop = line_b.stops[b_board_idx]
            transfer_leg = None
            board_b_time = arrive_at_transfer
            if transfer_walk:
                transfer_leg = estimate_walk_leg(
                    (a_alight_stop.lat, a_alight_stop.lon), (b_board_stop.lat, b_board_stop.lon)
                )
                board_b_time += transfer_leg.duration_s

            b_alight_idx = nearest_index(line_b, b_dest_idxs, destination)
            result_b = _ride_leg(
                line_b, b_board_idx, b_alight_idx, board_b_time, route_service, live_wait_lookup, hop_time_fn
            )
            if result_b is None:
                continue
            ride_b, alight_time = result_b

            b_alight_stop = line_b.stops[b_alight_idx]
            walk_from = estimate_walk_leg((b_alight_stop.lat, b_alight_stop.lon), destination)

            legs = [walk_to, ride_a]
            if transfer_leg is not None:
                legs.append(transfer_leg)
            legs += [ride_b, walk_from]
            itineraries.append(_build_itinerary(legs))

    # Rank purely by duration. duration_is_estimate is informational (surfaced to the
    # rider so they know a wait/ride time is a guess, not confirmed) -- it must NOT be
    # a sort key ahead of actual time, or a genuinely slower itinerary that happens to
    # have a live ETA could out-rank a much faster one that's merely unconfirmed.
    itineraries.sort(key=lambda it: it.total_duration_s)
    return itineraries[:max_results]


def _best_direct_pair(
    line: Line, origin_idxs: List[int], dest_idxs: List[int]
) -> Optional[Tuple[int, int]]:
    """The (board_idx, alight_idx) pair -- among every combination of an
    origin-adjacent stop and a destination-adjacent stop on this line -- with the
    fewest hops between them. See find_trips' direct-rides comment for why trying
    every pair, not just the stop nearest each point independently, matters on a loop."""
    best: Optional[Tuple[int, int, int]] = None
    for board_idx in origin_idxs:
        for alight_idx in dest_idxs:
            hops = _hop_distance(line, board_idx, alight_idx)
            if hops is None or hops == 0:
                continue
            if best is None or hops < best[2]:
                best = (board_idx, alight_idx, hops)
    return None if best is None else (best[0], best[1])


def _best_transfer(
    line_a: Line, a_origin_idxs: List[int], line_b: Line, b_dest_idxs: List[int]
) -> Optional[Tuple[int, int, int, bool]]:
    """Find the best (board_a, alight_a, board_b, needs_walk) transfer point between two
    lines: prefer a shared physical stop (same id), fall back to the closest pair of
    stops within TRANSFER_WALK_RADIUS_M. Returns None if no transfer is possible at all
    (line_a never gets anywhere line_b can pick up from)."""
    best: Optional[Tuple[int, int, int, bool, float]] = None
    for a_idx, a_stop in enumerate(line_a.stops):
        for b_idx, b_stop in enumerate(line_b.stops):
            if a_stop.id == b_stop.id:
                dist = 0.0
                needs_walk = False
            else:
                dist = haversine_m(a_stop.lat, a_stop.lon, b_stop.lat, b_stop.lon)
                if dist > TRANSFER_WALK_RADIUS_M:
                    continue
                needs_walk = True
            for board_a in a_origin_idxs:
                hops_a = _hop_distance(line_a, board_a, a_idx)
                if hops_a is None or hops_a == 0:
                    continue
                for board_b_candidate in b_dest_idxs:
                    hops_b = _hop_distance(line_b, b_idx, board_b_candidate)
                    if hops_b is None:
                        continue
                    score = hops_a + hops_b + (1 if needs_walk else 0)
                    if best is None or score < best[4]:
                        best = (board_a, a_idx, b_idx, needs_walk, score)
    if best is None:
        return None
    board_a, alight_a, board_b, needs_walk, _score = best
    return board_a, alight_a, board_b, needs_walk


def _build_itinerary(legs: List[object]) -> Itinerary:
    total = 0.0
    is_estimate = False
    for leg in legs:
        if isinstance(leg, WalkLeg):
            total += leg.duration_s
            is_estimate = is_estimate or leg.source == "straight_line"
        elif isinstance(leg, RideLeg):
            if leg.wait_s is None or leg.ride_s is None:
                is_estimate = True
            total += (leg.wait_s or 0.0) + (leg.ride_s or 0.0)
    return Itinerary(legs=legs, total_duration_s=total, duration_is_estimate=is_estimate)
