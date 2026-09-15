"""Live stop-ETA engine: predicts when a specific vehicle will reach a specific stop,
combining its real-time position along the route with the historical hop-time model --
our own replacement for TransLoc's GetStopArrivalTimes, starting life as a second
opinion shown alongside TransLoc's in livemap's vehicle/stop popups.

Approach (researched before building, not invented from scratch -- see the survey at
arxiv.org/pdf/1904.05037 and TCRP Synthesis 48, and TheTransitClock/Transitime, a
mature open-source reference implementation of exactly this class of system):
real-time transit ETA systems combine a schedule/historical baseline with a
real-time correction whose influence decays with distance from the live
observation -- classically a per-segment Kalman filter (Shalaby & Farhan 2004;
TheTransitClock's own predictor). This module is a deliberately simplified,
tractable version of that same idea, NOT a recursive Kalman filter with tuned
process/measurement noise covariances -- but the same qualitative shape:

  1. The vehicle's CURRENT segment (between the stop it last passed and the one
     immediately ahead) is estimated directly from its live position + smoothed
     speed (Vehicle.s_pos / Vehicle.ema_mps, already computed continuously by
     app.py's own vehicle-tracking loop) -- the most reliable signal available,
     since it describes what's happening right now.
  2. Every stop-to-stop segment further downstream is estimated from the
     historical hop-time model (trip_planner_history.HopTimeModel), scaled by a
     "pace factor": how the vehicle's current speed compares to what's
     historically typical for the segment it's in right now. That factor decays
     back toward 1.0 (i.e., trust the historical baseline more) the further
     downstream the segment is -- a vehicle stuck at one red light right now is a
     weak predictor of traffic eight stops away, but a bus already running several
     minutes behind schedule right now is a much better-than-nothing predictor of
     the very next stop.

Requires a Line with real shape data (`shape`/`shape_cum`) and stops with `arc_pos`
set -- see trip_planner.py's Stop/Line and app.py's _ordered_route_stops_with_coords/
_trip_planner_line_from_graph, which already compute exactly this for the trip
planner. UTS only: CAT has no block-schedule/hop-time infrastructure to draw a
historical baseline from (see trip_planner.py's CAT branch for the same reason).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from trip_planner import Line, Stop, HopTimeFn, SECONDS_PER_HOP_ESTIMATE, haversine_m

# How much the current segment's live pace factor still influences a downstream
# segment's estimate, per hop of distance from the vehicle's current position.
# 0.6 halves-ish the correction's weight every ~1.5 hops -- close to fully faded
# out (< 5% of the original correction left) by 6-7 stops out, which matches the
# literature's qualitative point that real-time deviation is a weak predictor many
# stops downstream while still meaningfully informing the next couple of stops.
PACE_DECAY = 0.6

# Clamp on the live/historical speed ratio itself -- guards against a momentarily
# stopped bus (near-zero speed) or a GPS glitch producing an absurd multiplier that
# would otherwise blow up every downstream segment's estimate.
PACE_RATIO_MIN = 0.35
PACE_RATIO_MAX = 3.0

# Floor under the vehicle's live speed when projecting its CURRENT (partial)
# segment -- a bus stopped at a light isn't "never arriving," it's just slow;
# without a floor, along_mps == 0 would make that leg's ETA infinite.
MIN_PROJECTION_MPS = 1.0

# A stop within this many real-world (straight-line) metres of the vehicle's
# CURRENT position is treated as "arriving now" regardless of what arc-length
# ordering says -- see the ARRIVING_RADIUS_M guard in estimate_stop_eta_s for
# why this exists: it's not a speed/pace tuning constant, it's a correction for
# a structural limitation of arc-length ("nearest point on the polyline")
# position matching on a route whose shape passes near itself (e.g. an
# out-and-back stretch, or a loop that returns close to where it started).
# Confirmed live on Gold Line: a vehicle genuinely ~129m from a stop by real
# coordinates projected to an arc-length position ~178m PAST that same stop,
# because the nearest point on the polyline happened to be on a different pass
# of a self-near road than the stop's own projection landed on -- arc-length
# math alone then concluded the stop was nearly a full loop away. 150m is
# generous enough to catch that kind of projection ambiguity (which showed up
# at ~130-180m in the wild) while nowhere near typical stop-to-stop spacing
# (confirmed live: UTS stops are essentially never under ~300m apart on the
# routes this was tested against), so it shouldn't ever misfire on a genuinely
# distant stop.
ARRIVING_RADIUS_M = 150.0

# Plausibility bounds on the speed any single historical hop-time bucket is
# allowed to imply (hop_distance_m / hop_seconds) -- see the inline comment
# where this is applied for why: a 3-sample bucket (MIN_SAMPLES in
# trip_planner_history.py) can easily carry one anomalous trip's duration as
# its median. ~1.3 m/s (≈3mph, brisk-walking pace) to 18 m/s (≈40mph, well
# above any UTS road's real limit) -- generous enough to never touch a
# legitimately slow or fast real segment, tight enough to catch a bucket
# implying the bus crawled or teleported.
MIN_HOP_SPEED_MPS = 1.3
MAX_HOP_SPEED_MPS = 18.0

# What the distance-fallback path (see estimate_stop_eta_s's "hop_dist /
# projection_mps" comment) decays TOWARD as hops get further from the vehicle,
# instead of projecting its exact current speed indefinitely far downstream.
# Confirmed live: a vehicle that was simply stopped right now (ema_mps at
# MIN_PROJECTION_MPS's floor -- a red light, a stop, a brief dwell) had that
# crawl speed applied undamped to every fallback hop for the rest of a
# near-full-loop prediction, inflating a real ~24-minute trip to ~100+ minutes.
# The historical-hop path already decays its live correction back toward a
# baseline (pace_ratio -> 1.0, i.e. "trust the historical bucket") the further
# out you project -- the fallback path needs the same shape, but has no
# historical baseline to decay toward, so it decays toward this flat "typical
# UTS operating speed including stops" constant instead. ~5.5 m/s (~12mph) sits
# in the middle of the real moving-vehicle speeds observed live (roughly 5-10
# m/s for buses actually underway).
TYPICAL_BUS_SPEED_MPS = 5.5


@dataclass
class BusEtaEstimate:
    seconds: float
    # "live" -- target is the vehicle's very next stop, pure position+speed projection.
    # "historical" -- every downstream segment had a real hop-time sample.
    # "projected" -- at least one downstream segment had no historical sample and
    #   fell back to that segment's own real distance at the vehicle's current pace
    #   (NOT a flat guess -- see estimate_stop_eta_s's inline comment on why a flat
    #   per-hop constant was tried and rejected: confirmed live, stop spacing on
    #   the same route ranges from ~2m to 1000m+, so a flat number is badly wrong
    #   at either end).
    source: str


def _forward_distance(from_s: float, to_s: float, route_length_m: float) -> float:
    """Distance traveling forward (increasing arc-length, wrapping past the route's
    end back to its start) from from_s to to_s. Always in [0, route_length_m)."""
    if route_length_m <= 0:
        return 0.0
    return (to_s - from_s) % route_length_m


def _next_stop_index(stops: List[Stop], vehicle_s_pos: float, route_length_m: float) -> Optional[int]:
    """Index of the first stop ahead of the vehicle in travel direction -- the one
    that defines the vehicle's current (partial) segment. None if no stop has a
    known arc_pos."""
    best_idx = None
    best_dist = None
    for i, s in enumerate(stops):
        if s.arc_pos is None:
            continue
        d = _forward_distance(vehicle_s_pos, s.arc_pos, route_length_m)
        if best_dist is None or d < best_dist:
            best_dist = d
            best_idx = i
    return best_idx


def estimate_stop_eta_s(
    line: Line,
    vehicle_s_pos: float,
    vehicle_ema_mps: float,
    target_stop: Stop,
    hop_time_fn: Optional[HopTimeFn],
    when: float,
    vehicle_lat: Optional[float] = None,
    vehicle_lon: Optional[float] = None,
) -> Optional[BusEtaEstimate]:
    """Seconds until this vehicle reaches target_stop, or None if the line/target
    don't carry the shape+arc_pos data this needs (e.g. CAT, or a UTS route whose
    shape hasn't been captured live yet -- see trip_planner.py's shapeSource).

    vehicle_lat/vehicle_lon are optional but strongly recommended: they enable the
    ARRIVING_RADIUS_M real-world-proximity guard (see that constant's comment) that
    catches a specific, confirmed-live failure mode on routes whose shape passes
    near itself, where arc-length position matching alone can conclude a stop the
    vehicle is genuinely right next to is actually almost a full loop away. Without
    them, ETAs still work, just without that safety net."""
    if not line.shape_cum or len(line.shape_cum) < 2:
        return None
    if target_stop.arc_pos is None:
        return None
    stops = line.stops
    if len(stops) < 2:
        return None

    route_length_m = line.shape_cum[-1]
    if route_length_m <= 0:
        return None

    if vehicle_lat is not None and vehicle_lon is not None:
        if haversine_m(vehicle_lat, vehicle_lon, target_stop.lat, target_stop.lon) <= ARRIVING_RADIUS_M:
            return BusEtaEstimate(seconds=0.0, source="live")

    next_idx = _next_stop_index(stops, vehicle_s_pos, route_length_m)
    if next_idx is None:
        return None
    next_stop = stops[next_idx]

    # The vehicle's current (partial) segment: live-projected, not historical --
    # this is the one piece of the estimate grounded entirely in "what's happening
    # right now" rather than a typical-day baseline.
    dist_to_next = _forward_distance(vehicle_s_pos, next_stop.arc_pos, route_length_m)
    projection_mps = max(MIN_PROJECTION_MPS, vehicle_ema_mps)
    current_leg_s = dist_to_next / projection_mps

    if next_stop.id == target_stop.id:
        return BusEtaEstimate(seconds=current_leg_s, source="live")

    # Pace factor: how the vehicle's actual current speed compares to what's
    # historically typical for the segment it's in right now (the one it's
    # currently between the previous stop and next_stop for -- see module
    # docstring). prev_stop is next_idx - 1 because the ordered stop list wraps
    # around a loop the same way the route itself does.
    prev_stop = stops[(next_idx - 1) % len(stops)]
    current_seg_dist = _forward_distance(prev_stop.arc_pos, next_stop.arc_pos, route_length_m) if prev_stop.arc_pos is not None else None
    historical_current_seg_s = (
        hop_time_fn(line.id, prev_stop.id, next_stop.id, when) if hop_time_fn else None
    )
    # pace_ratio compares two speeds -- the vehicle's own live speed, and what a
    # (possibly 3-sample, possibly noisy) historical bucket implies for the
    # segment it's currently in -- and that ratio, via effective_ratio's decay
    # below, multiplies EVERY downstream hop for the next several stops. A single
    # implausible speed on EITHER side of the ratio (not just the hop_s values
    # summed later, which already get their own clamp) therefore has an outsized,
    # compounding effect. Confirmed live: an implausibly fast one-bucket estimate
    # for the vehicle's current segment alone was enough to inflate a whole
    # multi-stop prediction by thousands of seconds. Clamp both speeds into the
    # same plausible range before dividing.
    pace_ratio = 1.0
    if current_seg_dist and current_seg_dist > 0 and historical_current_seg_s and historical_current_seg_s > 0:
        historical_expected_mps = current_seg_dist / historical_current_seg_s
        if historical_expected_mps > 0:
            clamped_expected_mps = max(MIN_HOP_SPEED_MPS, min(MAX_HOP_SPEED_MPS, historical_expected_mps))
            clamped_vehicle_mps = max(MIN_HOP_SPEED_MPS, min(MAX_HOP_SPEED_MPS, vehicle_ema_mps))
            pace_ratio = clamped_vehicle_mps / clamped_expected_mps
    pace_ratio = max(PACE_RATIO_MIN, min(PACE_RATIO_MAX, pace_ratio))

    total_s = current_leg_s
    all_historical = historical_current_seg_s is not None
    idx = next_idx
    hop_number = 0
    guard = 0
    while stops[idx].id != target_stop.id:
        guard += 1
        if guard > len(stops):
            return None  # target unreachable in one lap -- shouldn't happen on a loop, but never spin forever
        nxt_idx = (idx + 1) % len(stops)
        a, b = stops[idx], stops[nxt_idx]
        hop_number += 1
        hop_dist = (
            _forward_distance(a.arc_pos, b.arc_pos, route_length_m)
            if a.arc_pos is not None and b.arc_pos is not None
            else None
        )
        hop_s = hop_time_fn(line.id, a.id, b.id, when) if hop_time_fn else None
        if hop_s is None:
            # NOT trip_planner.SECONDS_PER_HOP_ESTIMATE -- that flat per-hop
            # constant is fine for trip-planning's rough duration estimate, but
            # real UTS stop spacing on the SAME route ranges from ~2m to 1000m+
            # (confirmed live), so a flat count-of-hops number is wildly wrong at
            # either end: it starves a real 1km hop and badly overstates a
            # same-corner hop. Falling back to this segment's own real distance
            # projected at the vehicle's current pace keeps the estimate
            # distance-aware even with zero historical coverage for this segment
            # -- confirmed against a live TransLoc side-by-side comparison this
            # was necessary; the flat constant produced errors of several
            # hundred seconds on longer routes before this fix.
            #
            # That pace decays toward TYPICAL_BUS_SPEED_MPS as hop_number grows,
            # same shape as pace_ratio's decay below -- confirmed live, a vehicle
            # that was simply stopped RIGHT NOW (red light, brief dwell) had its
            # near-zero speed projected undamped across an entire ~20-hop,
            # near-full-loop prediction, inflating a real ~24-minute trip to
            # ~100 minutes. A vehicle that's temporarily slow this second is a
            # weak predictor of its pace ten stops from now; by then, "typical"
            # is the better assumption.
            fallback_decay = PACE_DECAY ** hop_number
            fallback_mps = TYPICAL_BUS_SPEED_MPS + (projection_mps - TYPICAL_BUS_SPEED_MPS) * fallback_decay
            hop_s = hop_dist / fallback_mps if hop_dist else SECONDS_PER_HOP_ESTIMATE
            all_historical = False
        elif hop_dist:
            # A historical bucket needs MIN_SAMPLES=3 real samples to exist at
            # all (see trip_planner_history.py) -- a low enough bar that one
            # anomalous trip (a driver break, a detour, a stalled light) can
            # dominate a bucket's median and imply a wildly implausible speed
            # for that one segment. Confirmed live: a single such bucket, summed
            # into a ~20-hop far-side-of-the-loop prediction, inflated the total
            # to 5x what TransLoc (and the vehicle's own measured speed) implied.
            # Clamping each hop's IMPLIED speed to a plausible urban-bus range
            # bounds any one noisy bucket's damage without discarding real data
            # that's simply on the slow or fast end for a legitimate reason
            # (a long light, a straight empty stretch).
            implied_mps = hop_dist / hop_s if hop_s > 0 else 0.0
            if implied_mps < MIN_HOP_SPEED_MPS:
                hop_s = hop_dist / MIN_HOP_SPEED_MPS
            elif implied_mps > MAX_HOP_SPEED_MPS:
                hop_s = hop_dist / MAX_HOP_SPEED_MPS
        decay = PACE_DECAY ** hop_number
        effective_ratio = 1.0 + (pace_ratio - 1.0) * decay
        effective_ratio = max(0.1, effective_ratio)  # guard divide-by-near-zero below
        total_s += hop_s / effective_ratio
        idx = nxt_idx

    return BusEtaEstimate(seconds=total_s, source="historical" if all_historical else "projected")
