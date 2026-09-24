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

import re
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from trip_planner import Line, Stop, HopTimeFn, SECONDS_PER_HOP_ESTIMATE, haversine_m

# (route_id, stop_id, block_id, reference_ts) -> scheduled epoch seconds that
# block is due at that stop, or None if that stop isn't a scheduled "timestop"
# for this route, or nothing in the block's schedule is close enough to
# reference_ts to plausibly be the same lap. reference_ts is an ESTIMATE of
# when the vehicle will reach that stop (built up as estimate_stop_eta_s walks
# forward), not necessarily real "now" -- see uts_blocks.scheduled_hold_epoch,
# the only implementation of this today. Optional/UTS-only (no such schedule
# exists for CAT) -- every caller must tolerate None throughout.
ScheduledTimestopFn = Callable[[str, str, str, float], Optional[float]]
# (route_id, stop_id, when) -> is this stop a timestop AT THIS TIME OF DAY (see uts_blocks.is_timestop_at)
IsTimestopFn = Callable[[str, str, float], bool]
# (route_id, block_id, when) -> (leave_stop_id, leave_epoch, cutoff_stop_id, active_from_epoch) for a block on its
# last public trip of the day, else None (see uts_blocks.out_of_service_plan)
OutOfServiceFn = Callable[[str, str, float], Optional[Tuple[str, float, str, float]]]

# How much the current segment's live pace factor still influences a downstream
# segment's estimate, per hop of distance from the vehicle's current position.
# 0.6 halves-ish the correction's weight every ~1.5 hops -- close to fully faded
# out (< 5% of the original correction left) by 6-7 stops out, which matches the
# literature's qualitative point that real-time deviation is a weak predictor many
# stops downstream while still meaningfully informing the next couple of stops.
PACE_DECAY = 0.6

# Clamp on the live/historical speed ratio itself -- guards against a momentarily
# stopped bus (near-zero speed) or a GPS glitch producing an absurd multiplier that
# would otherwise blow up every downstream segment's estimate. The lower bound was
# 0.35 until a live 30-minute ETA-vs-reality log (2026-09-19) showed why that was
# too eager: whenever a prediction jumped >90s LATER, the bus was stopped at that
# poll 73% of the time (vs 35% of polls in general) -- a red light or a brief stop
# was being read as "this bus is running slow" and inflating every nearby stop's ETA
# until it moved again.
PACE_RATIO_MIN = 0.5
PACE_RATIO_MAX = 3.0

# Floor under the vehicle's live speed when projecting its CURRENT (partial)
# segment, and when comparing its pace to history -- a bus stopped at a light isn't
# "never arriving," it's just slow; without a floor, along_mps == 0 would make that
# leg's ETA infinite. ~3 m/s (~7 mph), not the near-nothing it used to be (1.0):
# a bus stopped RIGHT NOW is nearly always at a light, a crosswalk or a stop and will
# be moving at a normal pace again within a minute, so its instantaneous speed is
# a poor predictor of the next few hundred metres. See PACE_RATIO_MIN for the
# live data that showed a 1.0 floor over-reacting to momentary stops.
MIN_PROJECTION_MPS = 3.0

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
# math alone then concluded the stop was nearly a full loop away.
#
# IMPORTANT: this radius alone is NOT sufficient to decide "arriving now" --
# confirmed live on Green Line/Loop, where Stadium Rd @ Runk Dining Hall and
# Hereford Dr @ Runk Dining Hall are only ~144m apart (and a third stop,
# Hereford Dr @ Johnson House, sits ~117-135m away, on a DIFFERENT part of the
# loop reached only after wrapping past the route's recorded end) -- a real
# UTS stop cluster well inside this radius, disproving the original "stops
# are essentially never under 300m apart" assumption this constant shipped
# with. Naively trusting proximity alone made a vehicle sitting at Hereford
# Dr @ Runk show "Due" for Stadium Rd (the stop it had JUST LEFT).
#
# Two mechanisms are required in addition to this radius before the override
# fires -- see _forward_sweep_passes_near:
#   1. Walking the route's ACTUAL polyline forward (not a straight line, not
#      arc-length arithmetic, and NOT the vehicle's instantaneous heading --
#      an earlier version compared heading to a straight-line bearing, but
#      that breaks on exactly the curving/self-overlapping roads this whole
#      mechanism exists for: a bus can be pointed away from a stop's
#      straight-line direction right now while a curve just ahead is about to
#      take it straight there -- flagged as a real risk before that version
#      shipped).
#   2. FORWARD_SWEEP_MATCH_RADIUS_M, a MUCH tighter per-vertex match distance
#      than this outer gate -- confirmed live, that distinction is load-
#      bearing, not belt-and-suspenders: in the exact stop cluster above, the
#      forward-walked road never gets closer than ~119m to Stadium (it
#      genuinely doesn't go anywhere near it) while it passes within ~3m of
#      Johnson House -- so "was any swept vertex within this OUTER 150m
#      radius" cannot tell them apart (both qualify), but "did the swept path
#      actually get close" cleanly can.
ARRIVING_RADIUS_M = 150.0

# The "arriving now" overrides (both the ARRIVING_RADIUS_M one and the
# ARRIVING_RADIUS_EXEMPT_M one) additionally require the stop to be arc-length
# close to the vehicle: within this many metres ahead, or -- since arc-length can
# land on the wrong pass of a self-near road and read a stop just behind as "almost
# a full lap ahead" (the original confirmed-live Gold Line bug, ~178m off) -- within
# this many metres short of a full lap. Anything in between is a DIFFERENT part of
# the route that just happens to be physically close, which the real-world radius
# and forward sweep can't tell apart on their own. Confirmed live 2026-09-19 (Gold
# Line, Massie Rd): a bus driving OUT along Massie Rd passes within metres of
# "Massie Rd @ JPJ South Lot", a stop on the RETURN pass ~3.5km of route ahead, and
# was reported as "Due" for it -- the road is the same physical road, both ways.
ARRIVING_ARC_WINDOW_M = 600.0

# Below this real-world distance, skip the forward-sweep sanity check entirely
# -- the vehicle is essentially standing on the stop's own coordinates.
ARRIVING_RADIUS_EXEMPT_M = 40.0

# How close (real metres) a forward-swept polyline vertex must come to the
# target to count as "the road actually passes by here soon" -- see
# ARRIVING_RADIUS_M's comment for why this needs to be much tighter than that
# outer gate, not the same value. Confirmed live: a stop the road genuinely
# passes should come within single-digit metres of *some* vertex (2.8m for
# Johnson House); a stop that just happens to sit inside the outer radius
# without the road ever actually approaching it stayed 119m+ away at every
# vertex checked (Stadium Rd). 60m splits those with real margin on both
# sides.
FORWARD_SWEEP_MATCH_RADIUS_M = 60.0

# How far forward (real road metres, walked along the route's actual polyline
# -- not a straight line, not arc-length arithmetic) to look for the target
# stop before giving up. Comfortably more than the ~130-180m the confirmed
# Gold Line misprojection bug showed.
FORWARD_SWEEP_M = 400.0

# How much dead-reckoning the current (partial) segment's live-speed
# projection trusts a vehicle's speed once it's essentially AT a stop, in
# arc-length metres. See the dwell-time comment in estimate_stop_eta_s: a
# vehicle sitting at a stop (a scheduled recovery/hold, a long boarding, a
# red light right at the stop) reads as "very slow" the same way a vehicle
# genuinely stuck in mid-block traffic does, but the two mean very different
# things for how fast it'll travel once it resumes. Confirmed live: a Green
# Line bus dwelling at a timepoint stop showed ~7 minutes to a stop a few hops
# away, which dropped to ~2 minutes the instant it actually pulled away --
# the live-speed pace correction was reading "dwelling" as "running behind."
DWELL_DETECTION_RADIUS_M = 40.0

# Buses leave a scheduled timestop a little AFTER its scheduled time, not exactly on it:
# measured from real GPS tracks (2026-09-19, Gold/Green/Orange at Chapel and Shannon
# Library), the 11 clean visits left 0 to 1.9 minutes after the scheduled minute (median
# ~1.1). The hold clamp treats the schedule as the moment the bus is released, so
# without this every ETA past a timestop came in ~a minute early. 45s was deliberately
# under that median. Re-measured 2026-09-20 from ~20,000 headway-event departures at
# timestops the schedule really governs (fall semester): median 12-48 s after the
# scheduled time per route/stop, ~27 s overall, so 30 s. Early is the cheaper miss.
SCHEDULED_DEPARTURE_LAG_S = 30.0

# (route_id, stop_id) -> is this a mapped timestop? Independent of whether a hold is
# scheduled right now -- see POST_HOLD_HOP_ALLOWANCE_S.
#
# History hops that START at a timestop include the layover (arrival-to-arrival, and
# buses routinely sit there minutes): Green's Chapel hop is 267s and Orange's Shannon
# Library hop 362s in history, for ~60-100 seconds of real driving. When the hold
# clamp already accounts for the wait, that layover must not be counted a second time;
# cap such a hop at driving it at typical speed plus a little boarding/pull-out time.
# The cap applies at EVERY mapped timestop, not just when a hold is scheduled right now:
# history pools other days and hours (weekday-evening hops back weekend estimates), and a
# stop that holds on weekdays but not on a Saturday would otherwise smuggle its weekday
# layover into a Saturday ETA -- confirmed live 2026-09-19: Orange's Pinn Hall hop is 550s
# in weekday-evening history (a layover) and was being added to every Orange estimate past
# it on a Saturday, where Pinn Hall has no hold: 36% of Orange predictions >2 min late.
POST_HOLD_HOP_ALLOWANCE_S = 30.0

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

# Normal (non-layover) time a bus sits at a stop, ~ the median dwell across every UTS stop
# that isn't a layover (18 s over 9 months of headway events, 2026-09-20). Used where a
# scheduled hold governs departure instead of the stop's own measured dwell (dwell_fn mode).
TYPICAL_DWELL_S = 20.0


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


# A bus sitting AT its out-of-service cut-off stop projects a few metres past the end of its final segment (GPS and
# polyline-projection jitter). Confirmed live 2026-09-23 21:55 (Orange [05] parked at the Library, its cut-off): with a
# strict "position <= cut-off" test it fell out of its last run and every stop reappeared.
OOS_CUTOFF_TOL_M = 40.0
# A bus this long after its scheduled last departure that is not on its last-run stretch has finished (a lap is ~30-40
# min), even if this process never saw it there (e.g. the server restarted mid-run).
OOS_DONE_AFTER_S = 45 * 60.0

OosPlan = Tuple[str, float, str, float]


def out_of_service_phase(line: Line, vehicle_s_pos: float, plan: Optional[OosPlan], when: float) -> str:
    """Where a bus is relative to its block's out-of-service plan (uts_blocks.out_of_service_plan):
      "na"      no usable plan (none, unmapped stop, or a full-lap cut-off, which has no separate "past it" region)
      "before"  the active window (10 min before the last scheduled departure) has not opened yet
      "run"     between the last departure stop and the cut-off stop (or sitting at the cut-off): on its last trip
      "outside" anywhere else -- either still approaching its last departure, or already past the cut-off and
                heading to the lot. Position alone cannot tell those two apart; see out_of_service_finished."""
    if plan is None or not line.shape_cum or len(line.shape_cum) < 2:
        return "na"
    leave_id, _leave_epoch, cut_id, active_from = str(plan[0]), plan[1], str(plan[2]), plan[3]
    if leave_id == cut_id:
        return "na"
    by_id = {str(st.id): st for st in line.stops}
    leave_stop, cut_stop = by_id.get(leave_id), by_id.get(cut_id)
    if leave_stop is None or cut_stop is None or leave_stop.arc_pos is None or cut_stop.arc_pos is None:
        return "na"
    if when < active_from:
        return "before"
    length = line.shape_cum[-1]
    span = _forward_distance(leave_stop.arc_pos, cut_stop.arc_pos, length)
    past = _forward_distance(leave_stop.arc_pos, vehicle_s_pos, length)
    return "run" if 0.0 < past <= span + OOS_CUTOFF_TOL_M else "outside"


def out_of_service_finished(phase: str, run_seen: bool, when: float, leave_epoch: float) -> bool:
    """A bus that was seen on its last-run stretch and is now outside it has passed its cut-off; a bus never seen there
    is treated the same once OOS_DONE_AFTER_S has passed since its scheduled last departure. Such a bus serves no more
    stops, so the caller should publish no ETAs for it."""
    return phase == "outside" and (run_seen or when >= leave_epoch + OOS_DONE_AFTER_S)


# Evening route change (Gold 67->57, Green 68->54, Orange 53->55): a block's bus leaves a named stop at a scheduled time
# and from then on follows the post-6PM route, which skips some stops the pre-6PM route serves (Emmet St, University/
# Newcomb, most of the JPA stretch...). TransLoc only moves the bus to the new route id ~1-2 min after that time, so until
# then every ETA for a stop the new route skips is a bus that will never come.
ROUTE_CHANGE_JUST_LEFT_M = 3000.0  # a bus this far past the change stop has just made the change (it holds ~1-2 min, ~1 km)

RouteChangePlan = Tuple[str, float, List[str], Optional[float]]


def _norm_stop_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def route_change_hidden_stops(
    line: Line, vehicle_s_pos: float, plan: Optional[RouteChangePlan], when: float,
    change_stop_eta_s: Optional[float],
) -> set:
    """IDs of stops on the pre-6PM `line` this bus will never reach because of its evening route change
    (uts_blocks.route_change_plan). Only stops the post-6PM route doesn't serve (matched by stop NAME, since TransLoc
    renumbers IDs per variant) are ever hidden. Two situations:
      * still heading for the change stop: if it will reach it for the trip the note names (its ETA there is closer to
        the note's time than to the block's previous scheduled visit there; with no previous visit, any ETA), every
        skipped stop beyond it is hidden. A bus on the earlier trip keeps them, since it still serves them this lap.
      * just left it (or sitting at it at/after the note's time): every skipped stop is hidden.
    Nothing is hidden without a usable plan, shape, stop match or change-stop ETA (a no-op, never a wrong hide)."""
    if plan is None or not line.shape_cum or len(line.shape_cum) < 2:
        return set()
    change_id, change_epoch, served_names, prev_visit_epoch = str(plan[0]), plan[1], plan[2], plan[3]
    change_stop = next((st for st in line.stops if str(st.id) == change_id), None)
    if change_stop is None or change_stop.arc_pos is None:
        return set()
    served = {_norm_stop_name(n) for n in served_names}
    skipped = [st for st in line.stops if st.arc_pos is not None and _norm_stop_name(st.name) not in served]
    if not skipped:
        return set()
    length = line.shape_cum[-1]
    past_change = _forward_distance(change_stop.arc_pos, vehicle_s_pos, length)  # change stop -> bus
    if past_change <= OOS_CUTOFF_TOL_M and when >= change_epoch:
        return {str(st.id) for st in skipped}  # at the change stop as the change time arrives: about to leave
    if OOS_CUTOFF_TOL_M < past_change <= ROUTE_CHANGE_JUST_LEFT_M and change_epoch - 60.0 <= when:
        return {str(st.id) for st in skipped}  # has just left it
    if change_stop_eta_s is None:
        return set()
    if prev_visit_epoch is not None and when + change_stop_eta_s < (prev_visit_epoch + change_epoch) / 2.0:
        return set()
    def ahead(to_s: float) -> float:
        # A bus sitting AT a stop projects a few metres either side of it (GPS / polyline jitter); a stop that hair
        # behind it is "here", not a full lap away (a "Due" reading must never be hidden as if it were beyond).
        d = _forward_distance(vehicle_s_pos, to_s, length)
        return 0.0 if d >= length - OOS_CUTOFF_TOL_M else d

    ahead_of_change = 0.0 if past_change <= OOS_CUTOFF_TOL_M else ahead(change_stop.arc_pos)
    return {str(st.id) for st in skipped if ahead(st.arc_pos) > ahead_of_change + 1.0}


def _nearest_polyline_point(lat: float, lon: float, shape: List[Tuple[float, float]]) -> Tuple[int, float]:
    """(segment_index, fraction_along_segment) of the point on `shape` nearest
    to (lat, lon) -- a fresh, direct nearest-point search using only real
    coordinates and the route's real shape. Same brute-force approach as
    app.py's own _project_onto_polyline (duplicated here in pure form to
    avoid a circular import -- app.py imports this module)."""
    best_seg, best_frac, best_d2 = 0, 0.0, float("inf")
    for i in range(len(shape) - 1):
        alat, alon = shape[i]
        blat, blon = shape[i + 1]
        dlat, dlon = blat - alat, blon - alon
        seg2 = dlat * dlat + dlon * dlon
        t = 0.0 if seg2 == 0 else max(0.0, min(1.0, ((lat - alat) * dlat + (lon - alon) * dlon) / seg2))
        rlat, rlon = alat + t * dlat, alon + t * dlon
        d2 = (lat - rlat) ** 2 + (lon - rlon) ** 2
        if d2 < best_d2:
            best_d2 = d2
            best_seg, best_frac = i, t
    return best_seg, best_frac


def _held_at_other_stop(
    line,
    target_stop,
    vehicle_lat: float,
    vehicle_lon: float,
    vehicle_block_id: Optional[str],
    scheduled_timestop_fn,
    when: float,
) -> bool:
    """True if the vehicle is sitting on a stop other than target_stop that its
    block is still scheduled to hold at (departure epoch later than `when`)."""
    if scheduled_timestop_fn is None or not vehicle_block_id:
        return False
    for s in line.stops:
        if s.id == target_stop.id or s.lat is None or s.lon is None:
            continue
        if haversine_m(vehicle_lat, vehicle_lon, s.lat, s.lon) > DWELL_DETECTION_RADIUS_M:
            continue
        hold_epoch = scheduled_timestop_fn(line.id, s.id, vehicle_block_id, when)
        if hold_epoch is not None and hold_epoch > when:
            return True
    return False


def _forward_sweep_passes_near(
    line: Line, vehicle_lat: float, vehicle_lon: float, target_lat: float, target_lon: float,
) -> bool:
    """Does the route's ACTUAL polyline, walked forward for up to
    FORWARD_SWEEP_M real metres starting from wherever the vehicle's REAL
    coordinates land on it, come within FORWARD_SWEEP_MATCH_RADIUS_M (much
    tighter than ARRIVING_RADIUS_M -- see that constant's comment for why) of
    (target_lat, target_lon)?

    Deliberately does NOT use vehicle_s_pos as the starting point -- on
    exactly the self-overlapping routes this exists for, vehicle_s_pos can
    itself be the corrupted value (confirmed live: the original Gold Line bug
    was a vehicle's own arc-length position landing on the wrong physical pass
    of the road). Re-deriving a fresh starting point from the vehicle's real
    lat/lon sidesteps that -- it's always ground truth, never a stale/biased
    arc-length projection. Also does NOT re-check that starting point against
    the target before sweeping (an earlier version of this function did, and
    a regression test caught it: that first check ignores direction entirely,
    so on a straight road it could just as easily "confirm" a stop directly
    BEHIND the vehicle as one ahead of it -- only points strictly ahead, walked
    forward one real polyline vertex at a time, are checked). Follows the real
    road shape (so it correctly handles a curving road), not straight-line
    arithmetic or the vehicle's instantaneous heading -- see ARRIVING_RADIUS_M's
    comment for why both of those break down here. Loops the polyline (wraps
    past its last vertex back to its first) so a vehicle near the end of the
    shape can still sweep into a stop near the start."""
    shape = line.shape
    cum = line.shape_cum
    n = len(shape) if shape else 0
    if n < 2 or not cum or len(cum) != n:
        return False
    n_segs = n - 1

    seg, frac = _nearest_polyline_point(vehicle_lat, vehicle_lon, shape)
    seg_len = cum[seg + 1] - cum[seg]
    # Distance still ahead to reach shape[i + 1] -- computed and added to
    # `swept` BEFORE each vertex is checked (not after), so a single long
    # segment can't let a vertex miles down the road slip through the
    # FORWARD_SWEEP_M budget check on a stale, too-small swept total.
    swept = (1.0 - frac) * seg_len
    i = seg
    guard = 0
    while swept <= FORWARD_SWEEP_M and guard < n_segs:
        vlat, vlon = shape[i + 1]
        if haversine_m(vlat, vlon, target_lat, target_lon) <= FORWARD_SWEEP_MATCH_RADIUS_M:
            return True
        guard += 1
        i = (i + 1) % n_segs
        swept += cum[i + 1] - cum[i]
    return False


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
    vehicle_dir_sign: int = 0,
    vehicle_block_id: Optional[str] = None,
    scheduled_timestop_fn: Optional[ScheduledTimestopFn] = None,
    is_timestop_fn: Optional[IsTimestopFn] = None,
    dwell_fn: Optional[Callable[[str, str, float], float]] = None,
    out_of_service_fn: Optional[OutOfServiceFn] = None,
) -> Optional[BusEtaEstimate]:
    """Seconds until this vehicle reaches target_stop, or None if the line/target
    don't carry the shape+arc_pos data this needs (e.g. CAT, or a UTS route whose
    shape hasn't been captured live yet -- see trip_planner.py's shapeSource).

    vehicle_lat/vehicle_lon are optional but strongly recommended: they enable the
    ARRIVING_RADIUS_M real-world-proximity guard (see that constant's comment) that
    catches a specific, confirmed-live failure mode on routes whose shape passes
    near itself, where arc-length position matching alone can conclude a stop the
    vehicle is genuinely right next to is actually almost a full loop away. Without
    them, ETAs still work, just without that safety net.

    vehicle_dir_sign is app.py's own signed flag (Vehicle.dir_sign) for whether the
    vehicle's arc-length position is actually advancing in the route's canonical
    forward direction (+1), retreating (-1), or unknown/stationary (0) -- computed
    independently, from a wrap-aware delta between consecutive polls' real arc-length
    positions (see the vehicle-tracking loop in app.py). Every hop-walk below
    (_next_stop_index onward) assumes forward travel; confirmed live, a vehicle
    running a detour pattern with dir_sign=-1 (Gold Line, RouteID 67) had its ETA to
    a stop ~6935m of real forward travel away (~21 min, matching TransLoc almost
    exactly) computed as 196.5s -- off by 6x, because the forward-only math was
    applied to a vehicle actually moving the other way relative to the captured
    shape. The ARRIVING_RADIUS_M proximity check above is unaffected by this (it's
    driven by real GPS distance, not arc-length direction) and still applies first.

    dwell_fn (optional): (route_id, stop_id, epoch) -> typical seconds a bus sits at
    that stop. When given, hop_time_fn is expected to return DRIVING-ONLY hop times
    (leave A -> arrive B, see trip_planner_history.build_drive_time_samples) and the
    walk adds a dwell at every stop it passes through, instead of relying on layover
    time being baked into hop history and capped afterwards (is_timestop_fn's cap is
    skipped in this mode). A scheduled hold still applies on top: the bus leaves at
    the LATER of "arrival + normal dwell" and "scheduled departure + lag", so an early
    bus waits for the schedule and a late one just leaves after a normal dwell.

    vehicle_block_id/scheduled_timestop_fn (both optional, UTS-only) let a bus
    intentionally holding at a scheduled "timestop" push out every downstream
    ETA to account for it, rather than just neutralizing its dwelling speed and
    falling back to a typical-pace guess the way dwelling_at_next/dwelling_at_prev
    already do below on their own. Every time the walk below passes THROUGH a
    stop (not just arriving at target_stop itself -- see the in-loop comment),
    it checks whether that stop is this specific block's own scheduled timestop
    and, if the schedule says it's not due to leave yet, clamps the running
    total forward to that scheduled departure. Because this uses the block's
    OWN schedule (not a proximity guess), it fires even for a stop the vehicle
    hasn't reached yet -- a bus running early on the far side of its loop
    already shows a held ETA for a timestop several stops ahead, not just once
    it's physically sitting there. The clamp only ever pushes an ETA LATER
    (`max()`), never earlier, so a bus already running behind schedule is
    completely unaffected -- there's no failure mode where this makes an
    already-accurate live estimate worse."""
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
        dist_m = haversine_m(vehicle_lat, vehicle_lon, target_stop.lat, target_stop.lon)
        arc_ahead_m = _forward_distance(vehicle_s_pos, target_stop.arc_pos, route_length_m)
        arc_plausible = arc_ahead_m <= ARRIVING_ARC_WINDOW_M or arc_ahead_m >= route_length_m - ARRIVING_ARC_WINDOW_M
        if dist_m <= ARRIVING_RADIUS_EXEMPT_M and arc_plausible:
            # Essentially standing on the stop's own coordinates -- skip the
            # sanity check below entirely, it'd just be measuring GPS noise.
            return BusEtaEstimate(seconds=0.0, source="live")
        # Not applied while the vehicle is sitting on a DIFFERENT stop that it's
        # scheduled to hold at: a bus parked at a timestop with the next stop
        # within ARRIVING_RADIUS_M (confirmed live 2026-09-19: Orange Loop at
        # Shannon Library, Monroe Hall 150m ahead) is "near" the target but
        # nowhere near arriving -- it hasn't been released yet. Fall through so
        # the hold clamps below apply.
        if (
            dist_m <= ARRIVING_RADIUS_M
            and arc_plausible
            and not _held_at_other_stop(
                line, target_stop, vehicle_lat, vehicle_lon, vehicle_block_id, scheduled_timestop_fn, when
            )
            and _forward_sweep_passes_near(line, vehicle_lat, vehicle_lon, target_stop.lat, target_stop.lon)
        ):
            return BusEtaEstimate(seconds=0.0, source="live")

    if vehicle_dir_sign < 0:
        # Confidently moving backward relative to the shape's forward direction --
        # every hop-walk below would silently produce nonsense (see docstring). No
        # valid arc-length-based estimate exists for this vehicle/stop pair right
        # now; better to report nothing than a confidently wrong number.
        return None

    next_idx = _next_stop_index(stops, vehicle_s_pos, route_length_m)
    if next_idx is None:
        return None
    next_stop = stops[next_idx]
    # prev_stop is next_idx - 1 because the ordered stop list wraps around a loop
    # the same way the route itself does. Needed up here (not just for pace_ratio
    # further down) -- see dwelling_at_prev below.
    prev_stop = stops[(next_idx - 1) % len(stops)]

    # Dwelling vs. genuinely crawling: a vehicle sitting still because it's ON HOLD
    # at a stop (a scheduled recovery, boarding, a timepoint wait) tells us nothing
    # about how fast it'll travel once it goes. Two distinct symptoms if this isn't
    # accounted for, both confirmed live (user reports):
    #   1. dwelling_at_next (already at/near next_stop, arc-length hasn't quite
    #      caught up): the depressed live speed feeds into pace_ratio further down
    #      and reads as "running behind schedule", INFLATING every downstream hop
    #      for as long as it sits there -- a Green Line vehicle held for ~5 minutes
    #      showed a STATIC ~5-minute ETA for the stop after next the entire time,
    #      dropping to ~2 minutes the instant it pulled away.
    #   2. dwelling_at_prev (arc-length has already ticked past prev_stop even
    #      though the vehicle is still sitting right there): current_leg_s below
    #      would otherwise project the vehicle's live speed across the FULL
    #      upcoming hop as if the trip were already underway -- the opposite,
    #      falsely LOW failure mode explicitly flagged before this was fixed: a
    #      10-minute layover reading as a static ~1-minute ETA to the stop after
    #      it for the entire layover, because nothing about that math knows the
    #      bus hasn't moved an inch yet.
    # DWELL_DETECTION_RADIUS_M is much tighter than ARRIVING_RADIUS_M -- being
    # generously "near" a stop isn't enough to conclude dwelling, only sitting
    # right on top of one is.
    dwelling_at_next = (
        vehicle_lat is not None
        and vehicle_lon is not None
        and haversine_m(vehicle_lat, vehicle_lon, next_stop.lat, next_stop.lon) <= DWELL_DETECTION_RADIUS_M
    )
    dwelling_at_prev = (
        not dwelling_at_next
        and vehicle_lat is not None
        and vehicle_lon is not None
        and prev_stop.lat is not None
        and prev_stop.lon is not None
        and haversine_m(vehicle_lat, vehicle_lon, prev_stop.lat, prev_stop.lon) <= DWELL_DETECTION_RADIUS_M
    )

    # The vehicle's current (partial) segment: live-projected, not historical --
    # this is the one piece of the estimate grounded entirely in "what's happening
    # right now" rather than a typical-day baseline. EXCEPT when dwelling_at_prev:
    # the vehicle hasn't started this hop yet, so live-speed-projecting the FULL
    # hop distance is exactly backwards. Use the segment's own historical time (or
    # a distance/typical-speed fallback with no historical coverage) instead --
    # same "trust the baseline, the live signal means nothing right now" reasoning
    # pace_ratio's neutralization below applies to every hop after this one.
    dist_to_next = _forward_distance(vehicle_s_pos, next_stop.arc_pos, route_length_m)
    projection_mps = max(MIN_PROJECTION_MPS, vehicle_ema_mps)
    if dwelling_at_prev:
        prev_to_next_s = hop_time_fn(line.id, prev_stop.id, next_stop.id, when) if hop_time_fn else None
        current_leg_s = prev_to_next_s if prev_to_next_s and prev_to_next_s > 0 else dist_to_next / TYPICAL_BUS_SPEED_MPS
        hold_epoch = (
            scheduled_timestop_fn(line.id, prev_stop.id, vehicle_block_id, when)
            if scheduled_timestop_fn is not None and vehicle_block_id else None
        )
        if dwell_fn is not None:
            # Driving-only history: the bus is still sitting at prev_stop, so it still
            # owes (the rest of) its dwell before the hop can start. The elapsed part
            # isn't known here, so charge the typical (or, at a scheduled hold, the
            # normal) dwell in full.
            current_leg_s += TYPICAL_DWELL_S if hold_epoch is not None else dwell_fn(line.id, prev_stop.id, when)
        elif hold_epoch is not None or (is_timestop_fn and is_timestop_fn(line.id, prev_stop.id, when)):
            # The hop history for a hop leaving a timestop already contains the layover
            # being added below -- drive it at typical speed instead (see
            # POST_HOLD_HOP_ALLOWANCE_S).
            current_leg_s = min(current_leg_s, dist_to_next / TYPICAL_BUS_SPEED_MPS + POST_HOLD_HOP_ALLOWANCE_S)
        # Scheduled timestop hold, dwelling_at_prev counterpart: the main
        # hop-walk below starts at next_stop and checks each stop it departs
        # from in turn (see the in-loop comment) -- but when dwelling_at_prev,
        # the vehicle is still physically sitting at PREV_stop, which the walk
        # below never looks at (it starts one stop ahead). Without this, a
        # bus dwelling at a mapped timestop whose arc-length has already
        # ticked past it gets NO hold applied at all -- confirmed live
        # (2026-09-16): a bus holding at a timestop with ~2 minutes left
        # showed a 62s ETA for the very next stop, tagged "historical" (the
        # unmistakable signature of this exact branch). The vehicle hasn't
        # actually departed prev_stop yet, so the ride can't have started
        # either -- add whatever's left of the hold in front of the ride time
        # rather than max()-ing against it (there's no prior "arrival time at
        # prev_stop" to reconcile against here, unlike the loop below -- it's
        # effectively 0, the vehicle is already there).
        if hold_epoch is not None:
            current_leg_s = max(0.0, hold_epoch + SCHEDULED_DEPARTURE_LAG_S - when) + current_leg_s
    else:
        current_leg_s = dist_to_next / projection_mps

    # Out-of-service cut-off (see uts_blocks.out_of_service_plan): a block on its last public trip of the
    # day only carries passengers as far as a named stop, then goes to the lot / becomes a Night Pilot block.
    # A prediction for a stop the walk can only reach by going past that cut-off is a stop this bus will
    # never serve, so return None for it instead of a confident ETA. `in_final` turns on once the bus is on
    # that last trip: either it is already between the last departure stop and the cut-off, or the walk
    # below reaches the last departure's own scheduled visit. Buses already past the cut-off and heading to
    # the lot are deliberately left alone -- by position alone they can't be told apart from a bus still
    # approaching its last departure.
    in_final = False
    cutoff_done = False
    leave_id = leave_epoch = cut_id = None
    leave_guard = -1
    full_lap = False
    at_cutoff_overshoot = False
    if out_of_service_fn is not None and vehicle_block_id:
        plan = out_of_service_fn(line.id, vehicle_block_id, when)
        if plan is not None:
            leave_id, leave_epoch, cut_id, active_from = str(plan[0]), plan[1], str(plan[2]), plan[3]
            by_id = {str(st.id): st for st in stops}
            leave_stop, cut_stop = by_id.get(leave_id), by_id.get(cut_id)
            if (
                leave_stop is None or cut_stop is None
                or leave_stop.arc_pos is None or cut_stop.arc_pos is None
            ):
                leave_id = cut_id = None
            else:
                # A cut-off equal to the leave stop is the NEXT time the bus is back there: one full lap (Orange
                # weekend [05] "final loop" then Night Pilot from the Library; Silver [14] "as far as MCQ").
                full_lap = cut_id == leave_id
            if leave_id is not None and when >= active_from:
                span = route_length_m if full_lap else _forward_distance(leave_stop.arc_pos, cut_stop.arc_pos, route_length_m)
                past = _forward_distance(leave_stop.arc_pos, vehicle_s_pos, route_length_m)
                if full_lap:
                    # By position alone the start of the lap (just left the stop) is unambiguous, and so is the
                    # end of it (about to return) once enough time has passed since the scheduled departure; the
                    # stretch in between is on the lap. A bus still approaching its departure gets flagged when
                    # the walk reaches that scheduled visit instead.
                    on_lap = (
                        (when >= leave_epoch - 60.0 and 0.0 < past <= 0.85 * span)
                        or (when >= leave_epoch + 600.0 and past > 0.85 * span)
                    )
                else:
                    on_lap = 0.0 < past <= span + OOS_CUTOFF_TOL_M
                if on_lap or (dwelling_at_prev and str(prev_stop.id) == leave_id):
                    in_final = True
                    at_cutoff_overshoot = (not full_lap) and past > span

    # A bus sitting at (or a hair past) its cut-off stop has the first stop BEYOND the cut-off as its next stop, which
    # the shortcut below would happily return: nothing past the cut-off is served, except the cut-off stop itself.
    if at_cutoff_overshoot and str(target_stop.id) != cut_id:
        return None

    if next_stop.id == target_stop.id:
        return BusEtaEstimate(seconds=current_leg_s, source="live" if not dwelling_at_prev else "historical")

    # Pace factor: how the vehicle's actual current speed compares to what's
    # historically typical for the segment it's in right now (the one it's
    # currently between prev_stop and next_stop for -- see module docstring).
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
    # same plausible range before dividing. Neutralized (see dwelling comment
    # above) for either flavor of dwelling -- a paused vehicle is a weak predictor
    # of its own pace, let alone the pace of hops further out.
    dwelling = dwelling_at_next or dwelling_at_prev

    pace_ratio = 1.0
    if (
        not dwelling
        and current_seg_dist
        and current_seg_dist > 0
        and historical_current_seg_s
        and historical_current_seg_s > 0
    ):
        historical_expected_mps = current_seg_dist / historical_current_seg_s
        if historical_expected_mps > 0:
            clamped_expected_mps = max(MIN_HOP_SPEED_MPS, min(MAX_HOP_SPEED_MPS, historical_expected_mps))
            clamped_vehicle_mps = max(MIN_PROJECTION_MPS, min(MAX_HOP_SPEED_MPS, vehicle_ema_mps))
            pace_ratio = clamped_vehicle_mps / clamped_expected_mps
    pace_ratio = max(PACE_RATIO_MIN, min(PACE_RATIO_MAX, pace_ratio))

    total_s = current_leg_s
    all_historical = historical_current_seg_s is not None
    idx = next_idx
    hop_number = 0
    guard = 0
    # Same dwelling neutralization as pace_ratio above, applied to the OTHER place
    # a depressed live speed can leak in: the fallback_mps baseline a hop with no
    # historical bucket decays from (a few lines down). Without this, a vehicle
    # dwelling somewhere with no historical coverage on the hops right after it
    # would still get every one of those hops inflated from its parked-speed
    # projection_mps before any decay even starts -- the exact same failure this
    # module already fixed for pace_ratio, just reachable through a second path.
    fallback_baseline_mps = TYPICAL_BUS_SPEED_MPS if dwelling else projection_mps
    while stops[idx].id != target_stop.id:
        guard += 1
        if guard > len(stops):
            return None  # target unreachable in one lap -- shouldn't happen on a loop, but never spin forever
        if cutoff_done:
            return None  # the bus stops serving at its cut-off stop; the target lies beyond it
        nxt_idx = (idx + 1) % len(stops)
        a, b = stops[idx], stops[nxt_idx]
        held_here = bool(is_timestop_fn and is_timestop_fn(line.id, a.id, when + total_s)) and dwell_fn is None

        # Scheduled timestop hold: total_s right now represents the estimated
        # time to REACH `a` (every hop added so far, including current_leg_s
        # from before this loop even started -- so this also covers the very
        # first stop, next_stop, on the loop's first iteration). If `a` is this
        # block's own scheduled timestop and it isn't due to leave until later,
        # clamp forward to that departure before adding the next hop -- see the
        # docstring above. Deliberately NOT applied to target_stop's own arrival
        # (only stops the walk passes THROUGH, i.e. departs from) -- a rider
        # asking "when does it reach this stop" wants its honest physical
        # arrival, not a hold that hasn't started yet.
        scheduled_hold = False
        if scheduled_timestop_fn is not None and vehicle_block_id:
            hold_epoch = scheduled_timestop_fn(line.id, a.id, vehicle_block_id, when + total_s)
            if hold_epoch is not None:
                if leave_epoch is not None and str(a.id) == leave_id and abs(hold_epoch - leave_epoch) < 1.0:
                    in_final = True  # this visit IS the block's last departure: everything after is the final trip
                    leave_guard = guard
                scheduled_hold = True
                if dwell_fn is not None:
                    # Leaves at the later of (arrival + normal dwell) and (scheduled
                    # departure + lag): early waits for the schedule, late just goes.
                    total_s = max(total_s + TYPICAL_DWELL_S, hold_epoch + SCHEDULED_DEPARTURE_LAG_S - when)
                else:
                    total_s = max(total_s, hold_epoch + SCHEDULED_DEPARTURE_LAG_S - when)
                held_here = True
        if dwell_fn is not None and not scheduled_hold:
            total_s += dwell_fn(line.id, a.id, when + total_s)

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
            fallback_mps = TYPICAL_BUS_SPEED_MPS + (fallback_baseline_mps - TYPICAL_BUS_SPEED_MPS) * fallback_decay
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
            if held_here:
                hop_s = min(hop_s, hop_dist / TYPICAL_BUS_SPEED_MPS + POST_HOLD_HOP_ALLOWANCE_S)
        decay = PACE_DECAY ** hop_number
        effective_ratio = 1.0 + (pace_ratio - 1.0) * decay
        effective_ratio = max(0.1, effective_ratio)  # guard divide-by-near-zero below
        total_s += hop_s / effective_ratio
        if in_final and str(a.id) == cut_id and not (full_lap and guard == leave_guard):
            cutoff_done = True  # (a full-lap cut-off is the NEXT visit to the leave stop, not the departure itself)
        idx = nxt_idx

    if cutoff_done:
        return None  # the walk had to pass the out-of-service cut-off to arrive here (target is the very next stop)
    return BusEtaEstimate(seconds=total_s, source="historical" if all_historical else "projected")
