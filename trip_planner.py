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
import os
import re
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import httpx

# --- walking --------------------------------------------------------------------

EARTH_RADIUS_M = 6_371_000.0
WALK_SPEED_MPS = 1.3  # ~2.9 mph, a relaxed campus walking pace
WALK_DETOUR_FACTOR = 1.3  # straight-line underestimates any real path; pad for it
WALK_SNAP_RADIUS_M = 600.0  # how far a rider is assumed willing to walk to/from a stop
MAX_WALK_ONLY_DISTANCE_M = 3000.0  # beyond this, a walk-only itinerary isn't worth offering
TRANSFER_WALK_RADIUS_M = 250.0  # how far apart two stops can be and still count as a transfer

# Self-hosted Valhalla (see ROUTING_ENGINE.md). Unset in dev/most deployments, in which
# case estimate_walk_leg() falls back to the straight-line estimate below unconditionally.
WALK_ROUTER_URL = os.getenv("WALK_ROUTER_URL", "").strip() or None
# Fly Machines don't get a real tun device, so reaching a Tailscale peer from the Fly
# app goes through tailscaled's local outbound HTTP proxy rather than a direct socket --
# see the Dockerfile/start.sh tailscale setup. Unset when calling the router directly
# (e.g. testing from the home LAN itself).
WALK_ROUTER_PROXY_URL = os.getenv("WALK_ROUTER_PROXY_URL", "").strip() or None
# Home-LAN/Tailscale hop, not a public API -- fail fast to the straight-line fallback
# rather than let one slow walk leg stall a trip-planning request.
WALK_ROUTER_TIMEOUT_S = 2.0
# A single find_trips() call can ask for dozens of walk legs (one candidate stop pair
# per direct line, plus one per line-A x line-B transfer combination) -- if the router
# is actually down, that's dozens of independent WALK_ROUTER_TIMEOUT_S waits stacked
# back to back, confirmed live: a Rice Hall -> Pinn Hall request during a home-server
# outage spun for minutes before finally failing, and since httpx.post here is
# synchronous it blocks the single-CPU event loop the whole time, stalling every other
# request too. Once one call fails, assume the router stays down for this long and skip
# straight to the fallback for every other walk leg -- both in this request and any
# other concurrent one -- rather than re-discovering the same outage per candidate.
WALK_ROUTER_COOLDOWN_S = 30.0
_walk_router_down_until = 0.0  # monotonic time; 0.0 == not currently known to be down

# Ranking preference, not a real-world speed adjustment: a minute spent walking counts
# for more than a minute spent riding when picking which stops/itinerary to prefer.
# Goal is to minimize walking while still weighing that against a long ride -- an
# unweighted straight time comparison alone will happily trade a few minutes of extra
# walking for a marginally shorter ride, or vice versa, past the point that actually
# feels like a good trade to a rider. This only affects ranking/candidate-selection;
# the duration shown to the rider (Itinerary.total_duration_s) is always the real,
# unweighted total. 1.4 is a starting point (common range for this kind of walk
# penalty in transit routing is roughly 1.3-2x), not derived from rider data.
WALK_RANK_WEIGHT = 1.4

# A transfer is a real cost beyond the minutes it takes -- a missed connection,
# unfamiliar stop, or an extra wait in the weather all make a same-duration trip with
# a transfer worse than one without. Added to rank_cost per transfer (see
# _build_itinerary) so a one-transfer option must beat a direct/walk-only one by MORE
# than this to out-rank it -- i.e. a direct option up to TRANSFER_RANK_PENALTY_S
# *slower* still wins. Only affects ranking/ordering, never the duration shown to the
# rider (Itinerary.total_duration_s is always the real, unweighted total, same
# principle as WALK_RANK_WEIGHT above). User-specified: 3 minutes.
TRANSFER_RANK_PENALTY_S = 180.0

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


def _decode_valhalla_shape(encoded: str, precision: int = 6) -> List[Tuple[float, float]]:
    """Decode Valhalla's polyline (Google polyline algorithm, 1e-6 precision) into
    [(lat, lon), ...]."""
    inv = 10**-precision
    decoded: List[Tuple[float, float]] = []
    previous = [0, 0]
    index = 0
    while index < len(encoded):
        coords = [0, 0]
        for i in range(2):
            shift, result = 0, 0
            while True:
                byte = ord(encoded[index]) - 63
                index += 1
                result |= (byte & 0x1F) << shift
                shift += 5
                if byte < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else (result >> 1)
            coords[i] = previous[i] + delta
            previous[i] = coords[i]
        decoded.append((coords[0] * inv, coords[1] * inv))
    return decoded


def _routed_walk_leg(start: Tuple[float, float], end: Tuple[float, float]) -> Optional[WalkLeg]:
    """Call the self-hosted Valhalla router for a real pedestrian route. Returns None on
    any failure so the caller can fall back to the straight-line estimate -- a degraded
    walking line is better than no itinerary (see ROUTING_ENGINE.md)."""
    global _walk_router_down_until
    if time.monotonic() < _walk_router_down_until:
        return None  # already known down -- see WALK_ROUTER_COOLDOWN_S above
    try:
        response = httpx.post(
            WALK_ROUTER_URL,
            json={
                "locations": [
                    {"lat": start[0], "lon": start[1]},
                    {"lat": end[0], "lon": end[1]},
                ],
                "costing": "pedestrian",
                "units": "kilometers",
            },
            timeout=WALK_ROUTER_TIMEOUT_S,
            proxy=WALK_ROUTER_PROXY_URL,
        )
        response.raise_for_status()
        leg = response.json()["trip"]["legs"][0]
        _walk_router_down_until = 0.0  # a live response proves it's back up
        return WalkLeg(
            coordinates=_decode_valhalla_shape(leg["shape"]),
            distance_m=leg["summary"]["length"] * 1000.0,
            duration_s=leg["summary"]["time"],
            source="routed",
        )
    except Exception:
        _walk_router_down_until = time.monotonic() + WALK_ROUTER_COOLDOWN_S
        return None


def estimate_walk_leg(start: Tuple[float, float], end: Tuple[float, float]) -> WalkLeg:
    """The only place walk-leg geometry gets computed. Tries the self-hosted router
    first when WALK_ROUTER_URL is configured; a real router (see ROUTING_ENGINE.md)
    drops in here without touching any caller -- they all consume
    {coordinates, distance_m, duration_s, source} regardless of how it was produced."""
    if WALK_ROUTER_URL:
        routed = _routed_walk_leg(start, end)
        if routed is not None:
            return routed
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
    # Arc-length position (metres) along the parent Line's `shape`, if it has one --
    # i.e. how far along the route's real road-following polyline this stop projects
    # to. None when the line has no `shape` (e.g. CAT, whose patterns already carry
    # true stop order with no polyline projection needed -- see
    # app.py's _cat_lines_for_trip_planner). Used only to slice `shape` for a ride
    # leg's rendered geometry (see _ride_leg_shape); never affects routing/ranking.
    arc_pos: Optional[float] = None


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
    # The route's real road-following shape (decoded from TransLoc's EncodedPolyline),
    # and `shape_cum[i]` = cumulative arc length (metres) from shape[0] to shape[i] --
    # i.e. the same polyline+cumulative-distance pair Stop.arc_pos was projected onto.
    # None for lines with no such shape available (CAT) -- ride-leg rendering falls
    # back to straight lines through the stops themselves in that case.
    shape: Optional[List[Tuple[float, float]]] = None
    shape_cum: Optional[List[float]] = None


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
    # Rendered geometry: the line's real road-following shape sliced between board and
    # alight (see _ride_leg_shape) when available, else a straight-line fallback
    # through `path`'s stop points. This is what riders see drawn on the map -- never
    # used for hop-counting/routing, which stays based on `path`/stop indices.
    coordinates: List[Tuple[float, float]] = field(default_factory=list)
    wait_s: Optional[float] = None  # None = no live ETA available
    wait_s_source: Optional[str] = None  # "live" | "extrapolated" -- see _first_catchable_wait
    ride_s: Optional[float] = None
    ride_s_source: str = "heuristic"  # "historical" if every segment came from real data
    service_ends_ts: Optional[float] = None
    last_ride_warning: bool = False


@dataclass
class Itinerary:
    legs: List[object] = field(default_factory=list)
    total_duration_s: float = 0.0  # real, unweighted -- what's shown to the rider
    rank_cost: float = 0.0  # WALK_RANK_WEIGHT-adjusted -- ranking only, never displayed
    duration_is_estimate: bool = False  # true if any leg's wait/ride time is unknown


SECONDS_PER_HOP_ESTIMATE = 90.0  # rough per-stop dwell+travel time -- used per-segment
# whenever hop_time_fn (see find_trips) has no real historical sample for that segment.

# (route_id, from_stop_id, to_stop_id, when_epoch_s) -> seconds, or None if unknown.
# See trip_planner_history.HopTimeModel.lookup for the real implementation, built from
# headway_storage's arrival events. Kept as an injected callback (not an import) so
# this module stays testable without needing real event history.
HopTimeFn = Callable[[str, str, str, float], Optional[float]]

# (pattern_id, stop_id, after_epoch_s) -> the next scheduled departure at/after
# after_epoch_s, as an epoch timestamp, or None if the pattern has no more
# service at that stop that day. See cat_gtfs.py for the real implementation
# (CAT's own published GTFS timetable); kept as an injected callback, not an
# import, for the same reason HopTimeFn is -- this module stays testable
# without a real GTFS feed on hand.
CatScheduleFn = Callable[[str, str, float], Optional[float]]

# Same shape as CatScheduleFn, for UTS instead: (route_id, stop_id, after_epoch_s)
# -> the next scheduled visit at/after after_epoch_s, or None. route_service's
# own schedule data only ever covers a route's overall operating WINDOW (does
# it run at all right now), not a per-stop time -- this fills that gap for the
# handful of named "timestop" locations UTS's own Block Packages document a
# real schedule for (see uts_blocks.py). Most UTS stops aren't a mapped
# timestop at all, so this returning None for a given stop is the common case,
# not an error -- see _ride_leg's UTS branch, which never rejects a leg just
# because this comes back empty, unlike the CAT branch which has no other
# schedule signal to fall back on.
UtsScheduleFn = Callable[[str, str, float], Optional[float]]


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
    for a loop). Used for hop-counting and as the rendering fallback when the line has
    no real `shape` to slice (see _ride_leg_shape) -- connecting these points with
    straight lines can cut through buildings/blocks, so prefer the real shape when
    it's available."""
    n = len(line.stops)
    path = [line.stops[board_idx]]
    i = board_idx
    while i != alight_idx:
        i = (i + 1) % n
        path.append(line.stops[i])
    return path


def _point_on_shape(
    shape: List[Tuple[float, float]], shape_cum: List[float], arc_s: float
) -> Tuple[float, float]:
    """The point on `shape` at arc-length `arc_s` (metres from shape[0]), linearly
    interpolated between whichever two shape vertices bracket it. Clamped to the
    shape's actual extent, so an out-of-range arc_s (float rounding at either end)
    degrades to that end point rather than extrapolating nonsense."""
    arc_s = max(0.0, min(shape_cum[-1], arc_s))
    for i in range(len(shape) - 1):
        if shape_cum[i] <= arc_s <= shape_cum[i + 1]:
            seg = shape_cum[i + 1] - shape_cum[i]
            t = 0.0 if seg <= 0 else (arc_s - shape_cum[i]) / seg
            lat = shape[i][0] + t * (shape[i + 1][0] - shape[i][0])
            lon = shape[i][1] + t * (shape[i + 1][1] - shape[i][1])
            return (lat, lon)
    return shape[-1]


def _slice_shape(
    shape: List[Tuple[float, float]], shape_cum: List[float], start_s: float, end_s: float
) -> List[Tuple[float, float]]:
    """The portion of `shape` between two arc-length positions (metres), including
    interpolated points at both ends -- NOT just the nearest shape vertices, so the
    slice starts/ends exactly at the stop rather than snapping to wherever the
    original polyline happened to have a vertex. Assumes start_s <= end_s; loop
    wraparound is the caller's job (see _ride_leg_shape)."""
    points = [_point_on_shape(shape, shape_cum, start_s)]
    for i, s in enumerate(shape_cum):
        if start_s < s < end_s:
            points.append(shape[i])
    points.append(_point_on_shape(shape, shape_cum, end_s))
    return points


def _ride_leg_shape(line: Line, board_stop: Stop, alight_stop: Stop) -> Optional[List[Tuple[float, float]]]:
    """The line's real road-following shape, sliced to just the portion between
    board_stop and alight_stop -- what actually gets drawn on the map for a ride leg,
    rather than a straight line connecting each stop's point (which cuts through
    buildings/blocks whenever consecutive stops aren't on a straight street).

    Returns None when the line has no shape data (e.g. CAT) or either stop never got
    projected onto one (arc_pos is None) -- callers fall back to _path_stops's
    straight-line-through-stops rendering in that case, same as before this existed."""
    if not line.shape or not line.shape_cum or len(line.shape) < 2:
        return None
    if board_stop.arc_pos is None or alight_stop.arc_pos is None:
        return None
    start_s, end_s = board_stop.arc_pos, alight_stop.arc_pos
    if end_s >= start_s:
        return _slice_shape(line.shape, line.shape_cum, start_s, end_s)
    if not line.loop:
        # A non-loop line can't wrap -- alight "behind" board on the shape means
        # something upstream is inconsistent; let the caller's straight-line fallback
        # handle it rather than fabricating a backwards or nonsensical slice.
        return None
    # Loop wraparound: ride continues past the shape's end, back through its start.
    # Simple concatenation, no de-duplication at the seam -- a loop's polyline isn't
    # guaranteed to close exactly (shape[-1] == shape[0]), so trimming a point there
    # on the assumption it's a duplicate can silently cut a real stretch of road. A
    # loop that does close exactly just gets one harmless zero-length segment instead.
    total = line.shape_cum[-1]
    first = _slice_shape(line.shape, line.shape_cum, start_s, total)
    second = _slice_shape(line.shape, line.shape_cum, 0.0, end_s)
    return first + second


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


MAX_HEADWAY_EXTRAPOLATIONS = 20  # guard against spinning forever on a bogus/zero headway


def _first_catchable_wait(times: Optional[List[float]], min_wait_s: float) -> Optional[Tuple[float, str]]:
    """`times` is every vehicle currently en route to this stop, as seconds-from-now
    (TransLoc/CAT report one entry per active vehicle, not just the single soonest,
    and bus_eta.py's own estimates are merged in alongside them -- see
    _uts_live_wait_lookup/_bus_eta_wait_lookup/_cat_live_wait_lookup in app.py). Only
    an entry at or after `min_wait_s` (how long from now the rider will actually be
    standing at the stop) is a bus they can catch; anything sooner will have already
    left. `times` is sorted ascending, so the first catchable entry is also the
    soonest one. Returns (seconds, "live") for a real known upcoming arrival.

    If NONE of the known arrivals are catchable, extrapolate one forward using the
    real observed gap between the known arrivals (a live proxy for this route's
    actual current headway) instead of giving up -- confirmed live: on a
    sparsely-vehicled loop route (e.g. Silver, 2 vehicles), every source only ever
    reports each vehicle's single NEXT pass at a stop; once a rider's first leg takes
    longer than both of those, "wait unknown" showed up even with bus_eta's own
    farther-reaching estimates merged in, because neither source has any THIRD,
    later arrival to offer -- the route obviously keeps running, there's just no
    direct observation of when it'll next be there. Returns (seconds, "extrapolated")
    in that case. Needs at least 2 known times to have a real gap to measure; a
    single already-too-soon data point gives no basis to guess one, so that case
    (and an empty/all-non-positive-gap list) still returns None -- extrapolating off
    nothing would be a guess dressed up as data, not an estimate."""
    if not times:
        return None
    for t in times:
        if t >= min_wait_s:
            return (t, "live")
    if len(times) < 2:
        return None
    gaps = [b - a for a, b in zip(times, times[1:]) if b > a]
    if not gaps:
        return None
    headway = statistics.median(gaps)
    if headway <= 0:
        return None
    projected = times[-1]
    for _ in range(MAX_HEADWAY_EXTRAPOLATIONS):
        if projected >= min_wait_s:
            return (projected, "extrapolated")
        projected += headway
    return None


def _live_wait_with_chain(
    line: Line,
    stop_id: str,
    route_service: Optional[RouteService],
    live_wait_lookup: Dict[Tuple[str, str], List[float]],
    min_wait_s: float = 0.0,
) -> Optional[Tuple[float, str]]:
    """Soonest (seconds, source) wait for this line/stop that the rider could actually
    catch given `min_wait_s` -- source is "live" or "extrapolated", see
    _first_catchable_wait -- following the same interline chain that extends a
    route's service window forward (RouteService.effective_window) if the line's own
    id has no catchable entry.

    Why this matters: TransLoc's live vehicle feed reports under whichever RouteID is
    CURRENTLY active -- once Gold Line's vehicle relabels from RouteID 67 to 57 at
    5:51pm, GetStopArrivalTimes stops returning anything under "67" entirely, even
    though 67 is still a valid boardable line for the next few hours (its effective
    window, extended through the chain, doesn't end until 57's window does). Without
    this, every itinerary using the now-relabeled RouteID shows a live wait as
    "unknown" despite the exact same physical vehicle having a perfectly good live ETA
    one hop away in the chain table -- confirmed live: 100% live coverage on every
    currently-active RouteID, yet "wait unknown" showing up constantly regardless."""
    wait = _first_catchable_wait(live_wait_lookup.get((line.id, stop_id)), min_wait_s)
    if wait is not None or route_service is None or line.source != "uts":
        return wait
    seen = {line.id}
    current = line.id
    while current in route_service.chain_next:
        nxt = route_service.chain_next[current]
        if nxt in seen:
            break
        wait = _first_catchable_wait(live_wait_lookup.get((nxt, stop_id)), min_wait_s)
        if wait is not None:
            return wait
        seen.add(nxt)
        current = nxt
    return None


def _ride_leg(
    line: Line,
    board_idx: int,
    alight_idx: int,
    board_time: float,
    when: float,
    route_service: Optional[RouteService],
    live_wait_lookup: Dict[Tuple[str, str], List[float]],
    hop_time_fn: Optional[HopTimeFn] = None,
    cat_schedule_fn: Optional[CatScheduleFn] = None,
    uts_schedule_fn: Optional[UtsScheduleFn] = None,
) -> Optional[Tuple[RideLeg, float]]:
    """Build one ride leg. `board_time` (epoch seconds) is the earliest the rider can
    physically be standing at this stop -- e.g. `when` plus however long the walk here
    takes. `when` is the trip search's own reference time, the same epoch the live
    `Seconds`-from-now values in `live_wait_lookup` are anchored to. Returns None if
    this line can't be ridden from board_idx to alight_idx at all, or isn't in service
    at that time. Returns (leg, time_rider_actually_alights).

    live_wait_lookup's wait is picked as the soonest vehicle the rider could actually
    catch (see _first_catchable_wait) -- not just the single soonest bus system-wide,
    which may already be gone by the time a rider who has to walk there arrives."""
    hops = _hop_distance(line, board_idx, alight_idx)
    if hops is None or hops == 0:
        return None

    board_stop = line.stops[board_idx]
    alight_stop = line.stops[alight_idx]
    min_wait_s = max(0.0, board_time - when)  # how long it takes to walk/transfer here
    wait_result = _live_wait_with_chain(line, board_stop.id, route_service, live_wait_lookup, min_wait_s)
    ride_s, ride_s_source = _estimate_ride_seconds(line, board_idx, alight_idx, board_time, hop_time_fn)

    # absolute_wait_s is seconds-from-`when` (already picked to be >= min_wait_s, i.e. a
    # bus the rider can actually catch) -- so boarding happens at when + absolute_wait_s.
    # But `wait_s` on the leg below is the number actually shown to the rider and summed
    # into the itinerary's total time, and "seconds since I started planning this trip"
    # is not what a rider means by "wait" -- they mean how long they stand at the stop
    # *after* walking there. Subtracting the walk/transfer time they've already spent
    # gives that real, experienced wait instead of double-counting it.
    if wait_result is not None:
        absolute_wait_s, wait_s_source = wait_result
        actual_board_time = when + absolute_wait_s
        wait_s: Optional[float] = max(0.0, absolute_wait_s - min_wait_s)
    else:
        actual_board_time = board_time
        wait_s = None
        wait_s_source = None
    service_ends_ts: Optional[float] = None
    last_ride_warning = False

    if line.source == "uts":
        # Board_stop is one of the handful of named "timestop" locations UTS's
        # own Block Packages document a real per-stop schedule for (see
        # uts_blocks.py) -- most stops aren't, so this staying None is the
        # common case, not a problem: route_service's window check below is
        # still the real existence check for a UTS leg either way. Only
        # consulted once no live/extrapolated wait exists, same gate as CAT's
        # branch below, so a real live ETA is never second-guessed by a
        # schedule that can go stale (a detour, a delay) in a way live data
        # can't.
        if wait_s is None or wait_s_source == "extrapolated":
            scheduled_ts = uts_schedule_fn(line.id, board_stop.id, board_time) if uts_schedule_fn else None
            if scheduled_ts is not None:
                actual_board_time = scheduled_ts
                wait_s = max(0.0, (scheduled_ts - when) - min_wait_s)
                wait_s_source = "scheduled"
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
        # CAT: prefer a genuinely LIVE ETA as the availability signal -- an
        # extrapolated one is a real, well-founded projection (see
        # _first_catchable_wait) but is still just a guess about whether the
        # route is STILL running by then, and unlike UTS's block-schedule
        # window above, there used to be no independent way to check that
        # guess at all. Now there is: cat_gtfs.py's real published timetable
        # (see CatScheduleFn) -- fall back to it instead of refusing the leg
        # outright, which is what made CAT unusable for a "Later" search
        # (no live vehicle exists yet for a future time) and flaky even for
        # "now" whenever nothing happened to be live-tracked at query time.
        if wait_s is None or wait_s_source == "extrapolated":
            scheduled_ts = cat_schedule_fn(line.id, board_stop.id, board_time) if cat_schedule_fn else None
            if scheduled_ts is None:
                return None
            actual_board_time = scheduled_ts
            wait_s = max(0.0, (scheduled_ts - when) - min_wait_s)
            wait_s_source = "scheduled"

    path = _path_stops(line, board_idx, alight_idx)
    coordinates = _ride_leg_shape(line, board_stop, alight_stop) or [(s.lat, s.lon) for s in path]
    leg = RideLeg(
        line_id=line.id,
        line_name=line.name,
        color=line.color,
        board_stop=board_stop,
        alight_stop=alight_stop,
        path=path,
        coordinates=coordinates,
        wait_s=wait_s,
        wait_s_source=wait_s_source,
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
    live_wait_lookup: Dict[Tuple[str, str], List[float]],
    when: float,
    max_results: int = 4,
    hop_time_fn: Optional[HopTimeFn] = None,
    cat_schedule_fn: Optional[CatScheduleFn] = None,
    uts_schedule_fn: Optional[UtsScheduleFn] = None,
) -> List[Itinerary]:
    """Rank up to `max_results` walk -> ride[-> walk -> ride] -> walk itineraries.

    `live_wait_lookup` maps (line_id, stop_id) -> a sorted list of seconds-until-arrival
    for every vehicle currently en route to that stop (TransLoc/CAT report one entry per
    active vehicle, not just the single soonest), or should simply omit a key when no
    live ETA is known for that line/stop pair right now. See _ride_leg/
    _first_catchable_wait for why the full list matters: a rider who has to walk to the
    stop may not be able to catch the very soonest bus.

    `hop_time_fn`, if given, is consulted for real historical per-segment ride times
    (see trip_planner_history.HopTimeModel.lookup); segments it doesn't know fall back
    to the flat SECONDS_PER_HOP_ESTIMATE heuristic.

    `cat_schedule_fn`, if given, is consulted for a CAT leg once no live/extrapolated
    wait is available -- see cat_gtfs.py and _ride_leg's CAT branch. `uts_schedule_fn`
    plays the same role for a UTS leg (see uts_blocks.py and _ride_leg's UTS branch) --
    `route_service` alone only knows whether a route is running at all, not a per-stop
    time.
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
        pair = _best_direct_pair(line, origin_candidates[line_id], dest_candidates[line_id], origin, destination)
        if pair is None:
            continue
        board_idx, alight_idx = pair
        board_stop = line.stops[board_idx]
        alight_stop = line.stops[alight_idx]

        walk_to = estimate_walk_leg(origin, (board_stop.lat, board_stop.lon))
        board_time = when + walk_to.duration_s
        result = _ride_leg(
            line, board_idx, alight_idx, board_time, when, route_service, live_wait_lookup, hop_time_fn,
            cat_schedule_fn, uts_schedule_fn,
        )
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
            best = _best_transfer(line_a, a_origin_idxs, line_b, b_dest_idxs, origin, destination)
            if best is None:
                continue
            a_board_idx, a_alight_idx, b_board_idx, b_alight_idx, transfer_walk = best

            a_board_stop = line_a.stops[a_board_idx]
            walk_to = estimate_walk_leg(origin, (a_board_stop.lat, a_board_stop.lon))
            board_time = when + walk_to.duration_s
            result_a = _ride_leg(
                line_a,
                a_board_idx,
                a_alight_idx,
                board_time,
                when,
                route_service,
                live_wait_lookup,
                hop_time_fn,
                cat_schedule_fn,
                uts_schedule_fn,
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

            result_b = _ride_leg(
                line_b,
                b_board_idx,
                b_alight_idx,
                board_b_time,
                when,
                route_service,
                live_wait_lookup,
                hop_time_fn,
                cat_schedule_fn,
                uts_schedule_fn,
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
    # a sort key ahead of actual cost, or a genuinely slower itinerary that happens to
    # have a live ETA could out-rank a much faster one that's merely unconfirmed.
    # Ranked by rank_cost (WALK_RANK_WEIGHT-adjusted), not the raw total_duration_s
    # shown to the rider -- see WALK_RANK_WEIGHT's docstring for why walking and
    # riding aren't weighed 1:1 here.
    itineraries.sort(key=lambda it: it.rank_cost)
    return itineraries[:max_results]


def _walk_seconds(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Same cost formula as estimate_walk_leg, without building a WalkLeg -- used to
    score candidate stop choices during search (see _best_direct_pair/_best_transfer),
    where constructing real leg objects for every candidate would be wasted work."""
    return haversine_m(a[0], a[1], b[0], b[1]) * WALK_DETOUR_FACTOR / WALK_SPEED_MPS


RANKING_HOP_SPEED_MPS = 6.0  # rough average bus speed incl. stops/traffic -- ranking only
RANKING_HOP_FLOOR_S = 20.0  # even adjacent stops involve some slow/stop/dwell/go overhead


def _ranking_ride_seconds(line: Line, board_idx: int, alight_idx: int) -> float:
    """Ranking-only ride-time estimate between two stop indices, used when comparing
    candidate board/alight pairs (see _best_direct_pair/_best_transfer) -- NOT the
    estimate shown to the rider (see _estimate_ride_seconds, which prefers real
    historical data). SECONDS_PER_HOP_ESTIMATE is a flat system-wide average that's
    fine for a typical hop, but badly overstates a hop between two stops that happen
    to sit right next to each other -- confirmed live: two stops ~65m apart scored as
    if skipping that one hop cost a full 90s, which made walking an extra ~65m to
    reach the farther one look like a wash against saving that "90s" hop, so the
    search picked the farther stop over a much closer one for a savings that wasn't
    real. Estimating each hop from the actual distance between its two stops instead
    fixes that without needing real per-segment history to exist yet."""
    return sum(
        max(RANKING_HOP_FLOOR_S, haversine_m(a.lat, a.lon, b.lat, b.lon) / RANKING_HOP_SPEED_MPS)
        for a, b in _segment_stop_pairs(line, board_idx, alight_idx)
    )


def _best_direct_pair(
    line: Line,
    origin_idxs: List[int],
    dest_idxs: List[int],
    origin: Tuple[float, float],
    destination: Tuple[float, float],
) -> Optional[Tuple[int, int]]:
    """The (board_idx, alight_idx) pair -- among every combination of an
    origin-adjacent stop and a destination-adjacent stop on this line -- with the
    lowest estimated TOTAL time: walk-to-board + ride + walk-from-alight.

    An earlier version of this picked the pair with the fewest hops alone, which
    could walk right past the closest stop to shave off one hop, or hop off several
    stops early and walk the rest of the way, whenever that trimmed the hop count --
    confirmed live. Ride time is estimated geometrically per candidate pair (see
    _ranking_ride_seconds), and the walk legs it costs to reach that pairing count
    against it too, in the same units, so the comparison is apples-to-apples."""
    best: Optional[Tuple[int, int, float]] = None
    for board_idx in origin_idxs:
        board_stop = line.stops[board_idx]
        walk_to = _walk_seconds(origin, (board_stop.lat, board_stop.lon))
        for alight_idx in dest_idxs:
            hops = _hop_distance(line, board_idx, alight_idx)
            if hops is None or hops == 0:
                continue
            alight_stop = line.stops[alight_idx]
            walk_from = _walk_seconds((alight_stop.lat, alight_stop.lon), destination)
            score = WALK_RANK_WEIGHT * (walk_to + walk_from) + _ranking_ride_seconds(line, board_idx, alight_idx)
            if best is None or score < best[2]:
                best = (board_idx, alight_idx, score)
    return None if best is None else (best[0], best[1])


def _best_transfer(
    line_a: Line,
    a_origin_idxs: List[int],
    line_b: Line,
    b_dest_idxs: List[int],
    origin: Tuple[float, float],
    destination: Tuple[float, float],
) -> Optional[Tuple[int, int, int, int, bool]]:
    """Find the best (board_a, alight_a, board_b, alight_b, needs_walk) transfer point
    between two lines, scored the same way as _best_direct_pair -- lowest estimated
    total time (walk-to-board + ride_a + transfer walk + ride_b + walk-from-alight),
    not just fewest combined hops, for the same reason: a lower hop count isn't better
    if reaching it costs more walking than it saves. Returns None if no transfer is
    possible at all (line_a never gets anywhere line_b can pick up from).

    Returning alight_b matters: it's the specific line_b stop this search actually
    optimized ride_b's length around. A caller that re-derives "the stop nearest the
    destination" independently instead can land on a DIFFERENT stop than what was
    scored here -- confirmed live: a transfer whose second leg re-boarded practically
    back where the first leg started, because the alight stop the search chose was
    silently discarded and replaced with an unrelated nearest-to-destination pick."""
    best: Optional[Tuple[int, int, int, int, bool, float]] = None
    for a_idx, a_stop in enumerate(line_a.stops):
        for b_idx, b_stop in enumerate(line_b.stops):
            if a_stop.id == b_stop.id:
                transfer_walk_s = 0.0
                needs_walk = False
            else:
                dist = haversine_m(a_stop.lat, a_stop.lon, b_stop.lat, b_stop.lon)
                if dist > TRANSFER_WALK_RADIUS_M:
                    continue
                transfer_walk_s = dist * WALK_DETOUR_FACTOR / WALK_SPEED_MPS
                needs_walk = True
            for board_a in a_origin_idxs:
                hops_a = _hop_distance(line_a, board_a, a_idx)
                if hops_a is None or hops_a == 0:
                    continue
                board_a_stop = line_a.stops[board_a]
                walk_to = _walk_seconds(origin, (board_a_stop.lat, board_a_stop.lon))
                for alight_b in b_dest_idxs:
                    hops_b = _hop_distance(line_b, b_idx, alight_b)
                    if hops_b is None or hops_b == 0:
                        continue
                    alight_b_stop = line_b.stops[alight_b]
                    walk_from = _walk_seconds((alight_b_stop.lat, alight_b_stop.lon), destination)
                    score = (
                        WALK_RANK_WEIGHT * (walk_to + transfer_walk_s + walk_from)
                        + _ranking_ride_seconds(line_a, board_a, a_idx)
                        + _ranking_ride_seconds(line_b, b_idx, alight_b)
                    )
                    if best is None or score < best[5]:
                        best = (board_a, a_idx, b_idx, alight_b, needs_walk, score)
    if best is None:
        return None
    board_a, alight_a, board_b, alight_b, needs_walk, _score = best
    return board_a, alight_a, board_b, alight_b, needs_walk


def _build_itinerary(legs: List[object]) -> Itinerary:
    total = 0.0
    rank_cost = 0.0
    is_estimate = False
    ride_leg_count = 0
    for leg in legs:
        if isinstance(leg, WalkLeg):
            total += leg.duration_s
            rank_cost += WALK_RANK_WEIGHT * leg.duration_s
            is_estimate = is_estimate or leg.source == "straight_line"
        elif isinstance(leg, RideLeg):
            ride_leg_count += 1
            if leg.wait_s is None or leg.ride_s is None:
                is_estimate = True
            ride_time = (leg.wait_s or 0.0) + (leg.ride_s or 0.0)
            total += ride_time
            rank_cost += ride_time
    # Number of ride legs minus one -- a walk-only (0 ride legs) or direct (1 ride leg)
    # itinerary has 0 transfers; connecting two lines (2 ride legs) has 1, etc.
    transfers = max(0, ride_leg_count - 1)
    rank_cost += transfers * TRANSFER_RANK_PENALTY_S
    return Itinerary(legs=legs, total_duration_s=total, rank_cost=rank_cost, duration_is_estimate=is_estimate)
