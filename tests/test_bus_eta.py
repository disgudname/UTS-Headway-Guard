import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import bus_eta as eta
from trip_planner import Line, Stop, SECONDS_PER_HOP_ESTIMATE, haversine_m


def _shape(n=13, step_m=100.0):
    """A straight synthetic route shape, n vertices, step_m metres apart (approx --
    good enough for these unit tests, doesn't need to be geographically real)."""
    deg_per_m = 1.0 / 111_320.0
    shape = [(0.0, i * step_m * deg_per_m) for i in range(n)]
    cum = [i * step_m for i in range(n)]
    return shape, cum


def _line(stop_positions_m, route_id="67", n=13, step_m=100.0):
    """stop_positions_m: list of arc-length positions (metres) for each stop, in
    travel order -- e.g. [0, 300, 900, 1200] for 4 stops along the shape."""
    shape, cum = _shape(n=n, step_m=step_m)
    stops = [
        Stop(id=f"s{i}", name=f"s{i}", lat=0.0, lon=pos / 111_320.0, source="uts", arc_pos=pos)
        for i, pos in enumerate(stop_positions_m)
    ]
    return Line(id=route_id, name="Loop", color="#fff", source="uts", stops=stops, loop=True, shape=shape, shape_cum=cum)


def _flat_hop_time_fn(seconds):
    return lambda route_id, a, b, when: seconds


def test_returns_none_without_shape_data():
    line = Line(id="cat-7", name="X", color="#fff", source="cat", stops=[], loop=False)
    target = Stop(id="a", name="a", lat=0.0, lon=0.0, source="cat", arc_pos=100.0)
    assert eta.estimate_stop_eta_s(line, 0.0, 5.0, target, None, 0.0) is None


def test_returns_none_when_target_has_no_arc_pos():
    line = _line([0, 300, 900])
    target = Stop(id="nowhere", name="nowhere", lat=0.0, lon=0.0, source="uts")
    assert eta.estimate_stop_eta_s(line, 0.0, 5.0, target, None, 0.0) is None


def test_current_partial_segment_uses_pure_live_projection():
    # Vehicle sits 100m before stop s1 (at 300m); target IS s1 -- this should be a
    # pure live-speed projection, no historical data involved at all.
    line = _line([0, 300, 900])
    result = eta.estimate_stop_eta_s(line, 200.0, 10.0, line.stops[1], hop_time_fn=None, when=0.0)
    assert result is not None
    assert result.source == "live"
    assert abs(result.seconds - 10.0) < 0.01  # 100m remaining / 10 m/s = 10s


def test_current_partial_segment_floors_a_near_stopped_vehicle():
    line = _line([0, 300, 900])
    # Vehicle essentially stopped (0.01 m/s) 100m from the next stop -- must not
    # report a near-infinite ETA; MIN_PROJECTION_MPS floors the projection speed.
    result = eta.estimate_stop_eta_s(line, 200.0, 0.01, line.stops[1], hop_time_fn=None, when=0.0)
    assert result is not None
    assert result.seconds == 100.0 / eta.MIN_PROJECTION_MPS


def test_downstream_segments_use_historical_hop_time_when_pace_is_typical():
    line = _line([0, 300, 900, 1200])
    # Vehicle exactly at s0 (arc_pos 0), moving at exactly the pace that matches
    # the historical hop time for the s0->s1 segment (300m in 60s = 5 m/s) -- pace
    # ratio should land at ~1.0, so downstream segments use the historical time
    # essentially unscaled.
    hop_time_fn = _flat_hop_time_fn(60.0)  # every hop "historically" takes 60s
    result = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0)
    assert result is not None
    assert result.source == "historical"
    # current leg (0->300m at 5m/s) = 60s live-projected, then two more 60s hops,
    # each only lightly adjusted since pace_ratio ~= 1.
    assert 175.0 < result.seconds < 185.0


def test_running_fast_shrinks_downstream_estimates_with_decay():
    line = _line([0, 300, 900, 1200, 1500, 1800])
    hop_time_fn = _flat_hop_time_fn(60.0)  # historical pace: 300m/60s = 5 m/s
    # Vehicle moving twice as fast as historically typical right now.
    result = eta.estimate_stop_eta_s(line, 0.0, 10.0, line.stops[5], hop_time_fn, when=0.0)
    baseline = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[5], hop_time_fn, when=0.0)
    assert result is not None and baseline is not None
    assert result.seconds < baseline.seconds
    # The correction should fade with distance -- the LAST hop (far downstream)
    # should be adjusted less aggressively than the FIRST hop right after the
    # vehicle's current position. Check this indirectly: a route with only 2 hops
    # after the current segment should show a bigger relative speedup than one
    # with 5, for the same starting pace ratio.
    short_line = _line([0, 300, 900])
    short_result = eta.estimate_stop_eta_s(short_line, 0.0, 10.0, short_line.stops[2], hop_time_fn, when=0.0)
    short_baseline = eta.estimate_stop_eta_s(short_line, 0.0, 5.0, short_line.stops[2], hop_time_fn, when=0.0)
    long_speedup = 1 - (result.seconds / baseline.seconds)
    short_speedup = 1 - (short_result.seconds / short_baseline.seconds)
    assert short_speedup > long_speedup * 0.9  # earlier hops get more of the correction


def test_missing_hop_time_falls_back_to_distance_over_live_pace_not_a_flat_guess():
    line = _line([0, 300, 900])
    # Vehicle is exactly AT stop 0 -- reaching stop 2 needs two full hops
    # (0->1: 300m, 1->2: 600m), with no partial "current leg" distance to
    # live-project and no historical data for either hop. Vehicle speed is set
    # to exactly TYPICAL_BUS_SPEED_MPS so the fallback's decay-toward-typical
    # (see test_missing_hop_time_fallback_decays_toward_typical_speed for that
    # behavior specifically) is a no-op here, keeping this test's arithmetic
    # about distance-proportionality alone.
    speed = eta.TYPICAL_BUS_SPEED_MPS
    result = eta.estimate_stop_eta_s(line, 0.0, speed, line.stops[2], hop_time_fn=lambda *a: None, when=0.0)
    assert result is not None
    assert result.source == "projected"
    # Total real distance (900m) / speed -- NOT trip_planner's flat per-hop
    # constant, which would badly misjudge either of these two very
    # differently-sized hops (300m vs 600m) the same way.
    assert abs(result.seconds - 900.0 / speed) < 0.01


def test_missing_hop_time_fallback_scales_with_each_segments_own_distance():
    # Regression for a real bug found live: a flat per-hop constant was wildly
    # wrong on real UTS routes, where consecutive stop gaps on the SAME route
    # range from ~2m to 1000m+. A short hop and a long hop with equally-missing
    # historical data must NOT get the same estimate.
    speed = eta.TYPICAL_BUS_SPEED_MPS  # see note above -- keeps decay a no-op
    short_hop_line = _line([0, 10])  # 10m gap -- basically at the same corner
    long_hop_line = _line([0, 900])  # 900m gap
    short_result = eta.estimate_stop_eta_s(
        short_hop_line, 0.0, speed, short_hop_line.stops[1], hop_time_fn=lambda *a: None, when=0.0
    )
    long_result = eta.estimate_stop_eta_s(
        long_hop_line, 0.0, speed, long_hop_line.stops[1], hop_time_fn=lambda *a: None, when=0.0
    )
    assert short_result is not None and long_result is not None
    assert short_result.seconds < 5.0  # ~1.8s -- not the old flat 90s
    assert abs(long_result.seconds - 900.0 / speed) < 0.01


def test_missing_hop_time_fallback_decays_toward_typical_speed():
    # Regression for a real bug found live: a vehicle that's simply stopped
    # RIGHT NOW (a red light, a brief dwell -- ema_mps at its floor) had that
    # crawl speed projected undamped across an entire ~20-hop, near-full-loop
    # prediction, inflating a real ~24-minute trip to ~100 minutes. A stopped
    # vehicle's pace right now is a weak predictor of its pace ten stops out;
    # the fallback must decay toward TYPICAL_BUS_SPEED_MPS the same way the
    # historical-hop pace correction already does.
    line = _line([0, 300, 600, 900, 1200, 1500, 1800, 2100])
    crawling_mps = eta.MIN_PROJECTION_MPS  # effectively stopped
    result = eta.estimate_stop_eta_s(
        line, 0.0, crawling_mps, line.stops[7], hop_time_fn=lambda *a: None, when=0.0
    )
    assert result is not None
    # Without the decay this would be 2100m / MIN_PROJECTION_MPS = 2100s;
    # decayed toward ~5.5 m/s over 7 hops, it should land far below that.
    assert result.seconds < 2100.0 / crawling_mps / 2


def test_one_noisy_historical_bucket_cannot_dominate_a_long_multi_hop_sum():
    # Regression for a real bug found live: a historical bucket only needs
    # MIN_SAMPLES=3 real samples to exist (trip_planner_history.py) -- one
    # anomalous trip (a driver break, a stalled light) can dominate that
    # bucket's median. Confirmed live: a single such bucket, summed into a
    # ~20-hop prediction, inflated the total to 5x what the vehicle's own
    # measured speed implied. One absurdly slow bucket amid many normal ones
    # must not blow up the total.
    line = _line([0, 300, 600, 900, 1200])
    normal = 300.0 / 5.0  # 60s per 300m hop at a normal 5 m/s pace
    bad_bucket_s = 300.0 / 0.05  # implies 0.05 m/s -- a single wildly anomalous sample

    def hop_time_fn(route_id, a, b, when):
        return bad_bucket_s if (a, b) == ("s1", "s2") else normal

    result = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[4], hop_time_fn, when=0.0)
    assert result is not None
    # Without the clamp this would be ~60*3 + 6000 = ~6180s; clamped, the bad
    # bucket is capped at the plausibility floor speed instead of trusted verbatim.
    assert result.seconds < 500.0


def test_noisy_bucket_on_the_CURRENT_segment_cannot_inflate_every_downstream_hop():
    # Regression for a second, worse real bug found live: a noisy bucket for the
    # vehicle's CURRENT segment specifically doesn't just mis-estimate that one
    # hop -- it feeds pace_ratio, which (via effective_ratio's decay) scales
    # EVERY downstream hop for the next several stops. An implausibly fast
    # implied speed there (tiny historical duration for the current segment)
    # drove pace_ratio toward its floor, which INFLATES rather than shrinks
    # every subsequent hop -- confirmed live, this compounded into a
    # multi-thousand-second error across ~10 consecutive stops on one vehicle.
    line = _line([0, 300, 600, 900, 1200, 1500])
    normal = 300.0 / 5.0  # 60s per 300m hop at a normal 5 m/s pace

    def hop_time_fn(route_id, a, b, when):
        if (a, b) == ("s0", "s1"):  # the vehicle's CURRENT segment
            return 0.1  # implies an absurd 3000 m/s -- one noisy sample
        return normal

    # Vehicle moving at a normal, unremarkable pace -- nothing about ITS speed
    # should cause a blowup; only the noisy bucket should be capable of that,
    # and it must not be able to either.
    result = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[5], hop_time_fn, when=0.0)
    baseline = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[5], lambda *a: normal, when=0.0)
    assert result is not None and baseline is not None
    # A noisy current-segment bucket may still shift the total somewhat (that's
    # the real-time-correction mechanism doing its job), but must stay within a
    # sane multiple of what every-hop-normal produces -- not many times larger.
    assert result.seconds < baseline.seconds * 2.0


def test_arriving_radius_overrides_arc_length_on_a_self_overlapping_route():
    # Regression for a real bug found live on Gold Line: a route whose shape
    # passes near itself (an out-and-back stretch, or a loop returning close to
    # where it started) can have a vehicle's TRUE nearest point on the polyline
    # land on a different pass of the road than a nearby stop's own arc_pos --
    # so arc-length math alone concludes a stop the vehicle is genuinely right
    # next to is almost a full loop away. Confirmed live: a vehicle ~129m from a
    # stop by real coordinates had an arc_pos placing it ~178m PAST that same
    # stop, giving an 8000+ second estimate for something already arriving.
    line = _line([0, 6000], n=101, step_m=100.0)  # 10km loop
    target = line.stops[1]
    # Vehicle is geographically RIGHT NEXT TO the target stop (50m away, just
    # short of it -- i.e. still approaching), but its arc-length position landed
    # ~178m PAST the stop's arc_pos (the real confirmed-live case), so arc-length
    # math alone reads the stop as almost a full lap (9822m) ahead.
    vehicle_lat, vehicle_lon = target.lat, target.lon - (50.0 / 111_320.0)
    assert haversine_m(vehicle_lat, vehicle_lon, target.lat, target.lon) < eta.ARRIVING_RADIUS_M
    result = eta.estimate_stop_eta_s(
        line, 6178.0, 5.0, target, hop_time_fn=lambda *a: None, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
    )
    assert result is not None
    assert result.seconds == 0.0
    assert result.source == "live"


def test_arriving_radius_does_not_fire_for_a_stop_on_the_other_pass_of_the_same_road():
    # Regression for a real bug found live on Gold Line (Massie Rd): the route
    # drives OUT along Massie Rd and comes BACK the same road, so a bus on the
    # outbound pass is physically within metres of stops that belong to the
    # return pass, ~3.5km of route ahead -- and was reported "Due" for them.
    # Arc-length says that stop is thousands of metres away, and that has to win
    # over "it's right there on the map".
    line = _line([0, 6000], n=101, step_m=100.0)
    target = line.stops[1]
    for offset_m in (20.0, 100.0):  # inside the exempt radius, and inside the outer radius
        vehicle_lat, vehicle_lon = target.lat, target.lon - (offset_m / 111_320.0)
        result = eta.estimate_stop_eta_s(
            line, 2500.0, 5.0, target, hop_time_fn=lambda *a: 60.0, when=0.0,
            vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
        )
        assert result is not None
        assert result.seconds > 100.0, offset_m
        assert result.source != "live" or result.seconds > 100.0


def test_arriving_radius_does_not_misfire_on_an_already_passed_nearby_stop():
    # Regression for a real bug found live on Green Line: Stadium Rd @ Runk
    # Dining Hall and Hereford Dr @ Runk Dining Hall are only ~144m apart in
    # real coordinates despite being consecutive (not overlapping) stops -- so
    # a vehicle sitting at Hereford, having JUST LEFT Stadium, showed "Due" for
    # Stadium too.
    #
    # Real stops like this sit on a DIFFERENT street, not simply N metres
    # behind on the same road -- Stadium Rd vs Hereford Dr, at some real angle
    # to each other -- so the target's real coordinates here get a
    # perpendicular offset too, not just a same-axis "behind" gap. (A purely
    # colinear "behind" test is a stricter, degenerate case where ANY vertex
    # only slightly ahead of the vehicle can still land within
    # ARRIVING_RADIUS_M of a same-axis target close behind -- that's just
    # geometry, not a bug in the forward-only walk itself; see
    # test_forward_sweep_does_not_fire_on_a_stop_directly_behind_the_vehicle
    # for that narrower claim, with a target far enough behind to sidestep it.)
    deg_per_m_lon = 1.0 / 111_320.0
    deg_per_m_lat = 1.0 / 110_540.0
    line = _line([0, 300, 6000], n=101, step_m=100.0)
    target = line.stops[1]  # arc_pos=300 unaffected -- only its real coordinates change below
    target.lat = 100.0 * deg_per_m_lat
    target.lon = 250.0 * deg_per_m_lon
    vehicle_lat, vehicle_lon = 0.0, 350.0 * deg_per_m_lon
    assert haversine_m(vehicle_lat, vehicle_lon, target.lat, target.lon) <= eta.ARRIVING_RADIUS_M
    result = eta.estimate_stop_eta_s(
        line, 400.0, 5.0, target, hop_time_fn=lambda *a: 60.0, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
    )
    assert result is not None
    assert result.seconds > 0.0
    assert result.source != "live"


def test_arriving_radius_does_not_misfire_on_a_moderate_forward_stop():
    # Regression for a real bug found live on Green Line: Hereford Dr @
    # Johnson House sits ~117m from Hereford Dr @ Runk Dining Hall in real
    # coordinates, but ~920m of genuine forward travel away (the first stop of
    # the next lap) -- well beyond FORWARD_SWEEP_M, so the override must not
    # fire even though the stop is well within ARRIVING_RADIUS_M. Perpendicular
    # offset for the same reason as the already-passed test above.
    deg_per_m_lon = 1.0 / 111_320.0
    deg_per_m_lat = 1.0 / 110_540.0
    line = _line([0, 900, 6000], n=101, step_m=100.0)
    target = line.stops[0]  # arc_pos=0
    target.lat = 100.0 * deg_per_m_lat
    vehicle_lat, vehicle_lon = 0.0, 100.0 * deg_per_m_lon
    assert haversine_m(vehicle_lat, vehicle_lon, target.lat, target.lon) < eta.ARRIVING_RADIUS_M
    result = eta.estimate_stop_eta_s(
        line, 9000.0, 5.0, target, hop_time_fn=lambda *a: 60.0, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
    )
    assert result is not None
    # 1000m of genuine forward travel at 5 m/s -- not the arriving-radius
    # override's instant "0.0s" (a real, non-trivial live projection to the
    # actual next stop is the correct answer here, not "Due").
    assert result.seconds > 100.0


def _curving_line():
    """A->B heads due east; B->C turns sharply north; C->D heads back west,
    ending up physically close to (but arc-length far past) where the route
    started -- models a hairpin/self-overlap without reusing the straight-line
    _line() helper. Used to prove the arriving-radius check follows the REAL
    road shape instead of a straight-line bearing (see the module comment on
    why an earlier, heading-based version of this check was rejected: a bus
    can be pointed due east *right now* while the road it's on curves back
    within a couple hundred metres to a stop that, by straight-line bearing
    alone, looks like it's behind or off to the side)."""
    deg_per_m = 1.0 / 111_320.0
    shape = [
        (0.0, 0.0),                     # A
        (0.0, 100 * deg_per_m),         # B -- A->B heads ~due east
        (0.0015, 100 * deg_per_m),      # C -- B->C turns sharply north
        (0.0015, -20 * deg_per_m),      # D -- C->D heads back west
    ]
    cum = [0.0]
    for i in range(1, len(shape)):
        cum.append(cum[-1] + haversine_m(*shape[i - 1], *shape[i]))
    return Line(id="curve", name="Curve", color="#fff", source="uts", stops=[], loop=True, shape=shape, shape_cum=cum), shape, cum


def test_forward_sweep_follows_a_curving_road_not_a_straight_line_bearing():
    line, shape, cum = _curving_line()
    # Vehicle is 30m into segment A->B, heading due east (bearing ~90). D sits
    # physically close to the road just past the curve, but a straight-line
    # bearing from the vehicle to D points roughly north-west/behind --
    # heading_diff against a ~90 degree heading would be well over 90 degrees,
    # which an earlier, heading-based version of this check would have
    # rejected. The forward sweep, which walks the actual A->B->C->D shape
    # starting from the vehicle's real coordinates, must still find it.
    vehicle_lat, vehicle_lon = 0.0, 30.0 / 111_320.0
    target_lat, target_lon = shape[3]  # D
    assert cum[3] - 30.0 < eta.FORWARD_SWEEP_M  # sanity: reachable within the sweep horizon
    assert eta._forward_sweep_passes_near(line, vehicle_lat, vehicle_lon, target_lat, target_lon)


def test_forward_sweep_has_a_bounded_horizon():
    # Same curving shape, but padded further out past FORWARD_SWEEP_M of real
    # road travel -- the sweep must not "find" a stop that's genuinely too far
    # ahead, even along a real, followable path.
    deg_per_m = 1.0 / 111_320.0
    line, shape, cum = _curving_line()
    far_lat, far_lon = 0.0015, -3000 * deg_per_m  # far past D, well beyond the sweep horizon
    shape = shape + [(far_lat, far_lon)]
    cum = cum + [cum[-1] + haversine_m(shape[-2][0], shape[-2][1], far_lat, far_lon)]
    line = Line(id="curve", name="Curve", color="#fff", source="uts", stops=[], loop=True, shape=shape, shape_cum=cum)
    vehicle_lat, vehicle_lon = 0.0, 30.0 / 111_320.0
    assert cum[-1] - 30.0 > eta.FORWARD_SWEEP_M  # sanity: genuinely out of reach
    assert not eta._forward_sweep_passes_near(line, vehicle_lat, vehicle_lon, far_lat, far_lon)


def test_forward_sweep_does_not_fire_on_a_stop_directly_behind_the_vehicle():
    # Regression for a real bug caught by this very test suite: an earlier
    # version of the sweep checked the vehicle's OWN starting point against
    # the target before walking forward at all -- but that check doesn't
    # know about direction, so on a plain straight road it "confirmed" a stop
    # sitting directly BEHIND the vehicle just as readily as one ahead. Only
    # points strictly ahead (walked forward one real vertex at a time) may
    # ever match.
    #
    # The gap here (300m) deliberately exceeds ARRIVING_RADIUS_M: on a plain
    # colinear road, a target CLOSER behind than ARRIVING_RADIUS_M is a
    # genuinely ambiguous case for ANY radius-based check, not a bug in this
    # one specifically -- the first vertex checked can sit arbitrarily close
    # to the vehicle's own true position (a vehicle mid-segment can be a
    # hair's width from the next vertex), so a same-axis target within that
    # radius of the vehicle's OWN position will always be within radius of
    # that first vertex too, "ahead" or not. That's just geometry on a single
    # straight line; real close-behind stops (see the already-passed test
    # below) sit on a genuinely different street at a real angle, which this
    # unambiguously-far-behind case doesn't need to model.
    deg_per_m = 1.0 / 111_320.0
    shape = [(0.0, i * 100 * deg_per_m) for i in range(51)]
    cum = [i * 100.0 for i in range(51)]
    line = Line(id="straight", name="Straight", color="#fff", source="uts", stops=[], loop=True, shape=shape, shape_cum=cum)
    vehicle_lat, vehicle_lon = 0.0, 350.0 * deg_per_m
    target_lat, target_lon = 0.0, 50.0 * deg_per_m
    assert not eta._forward_sweep_passes_near(line, vehicle_lat, vehicle_lon, target_lat, target_lon)


def test_arriving_radius_does_not_misfire_on_a_genuinely_distant_stop():
    line = _line([0, 5000])
    target = line.stops[1]
    # Vehicle is nowhere near the target stop, geographically or by arc-length.
    result = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, target, hop_time_fn=lambda *a: None, when=0.0,
        vehicle_lat=0.0, vehicle_lon=0.0,
    )
    assert result is not None
    assert result.seconds > 0.0


def test_estimate_still_works_without_vehicle_lat_lon_supplied():
    # vehicle_lat/vehicle_lon are optional -- omitting them just skips the
    # real-world-proximity guard, not a hard requirement.
    line = _line([0, 300])
    result = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[1], hop_time_fn=lambda *a: None, when=0.0)
    assert result is not None


def test_wraps_forward_around_a_loop_when_target_is_behind_the_vehicle():
    line = _line([0, 300, 900])
    # Vehicle just past s2 (900m), target is s0 (position 0) -- reaching it means
    # completing the rest of the loop, not going "backward".
    result = eta.estimate_stop_eta_s(line, 950.0, 5.0, line.stops[0], hop_time_fn=lambda *a: 60.0, when=0.0)
    assert result is not None
    assert result.seconds > 0


# --- Scheduled timestop hold clamp (vehicle_block_id/scheduled_timestop_fn) ---

def _hold_fn(held_stop_id, held_epoch, held_block_id="B1"):
    """A ScheduledTimestopFn stub: returns held_epoch only for the exact
    (stop_id, block_id) it's configured for, None otherwise -- so a test can
    assert the clamp fires exactly where expected and nowhere else."""
    def _fn(route_id, stop_id, block_id, reference_ts):
        if stop_id == held_stop_id and block_id == held_block_id:
            return held_epoch
        return None
    return _fn


def test_schedule_hold_pushes_out_downstream_stops():
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    baseline = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0)
    assert baseline is not None and 175.0 < baseline.seconds < 185.0

    # Block "B1" isn't scheduled to leave s1 until t=500 -- far later than the
    # live/historical model alone would ever produce for reaching s1 (~60s).
    result = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[1].id, 500.0),
    )
    assert result is not None
    # Held until 500s at s1, then two more 60s hops to reach s3.
    assert 615.0 < result.seconds < 625.0
    assert result.seconds > baseline.seconds


def test_schedule_hold_beats_arriving_radius_shortcut_for_next_stop():
    # Confirmed live 2026-09-19 (Orange Loop): bus parked on a timestop with the
    # next stop ~150m ahead. The "within ARRIVING_RADIUS_M" shortcut used to
    # report 0s/"live" for that next stop, ignoring the scheduled hold entirely.
    line = _line([0, 100, 900])
    s0, s1 = line.stops[0], line.stops[1]
    hold = _hold_fn(s0.id, 300.0)
    kwargs = dict(
        vehicle_lat=s0.lat, vehicle_lon=s0.lon,
        vehicle_block_id="B1", scheduled_timestop_fn=hold,
    )
    held = eta.estimate_stop_eta_s(line, 0.0, 0.0, s1, _flat_hop_time_fn(60.0), when=0.0, **kwargs)
    assert held is not None and held.seconds >= 300.0
    # Hold already expired: shortcut behaves as before.
    kwargs["scheduled_timestop_fn"] = _hold_fn(s0.id, -10.0)
    released = eta.estimate_stop_eta_s(line, 0.0, 0.0, s1, _flat_hop_time_fn(60.0), when=0.0, **kwargs)
    assert released is not None and released.seconds < 300.0


def test_schedule_hold_does_not_affect_arrival_at_the_held_stop_itself():
    # Asking for the ETA to s1 itself -- the stop about to hold -- should stay
    # an honest physical arrival estimate, not the hold's departure time. The
    # hold only affects stops AFTER the one it applies to (see docstring).
    line = _line([0, 300, 900])
    result = eta.estimate_stop_eta_s(
        line, 200.0, 10.0, line.stops[1], hop_time_fn=None, when=0.0,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[1].id, 5000.0),
    )
    assert result is not None
    assert abs(result.seconds - 10.0) < 0.01  # unchanged from test_current_partial_segment_uses_pure_live_projection


def test_schedule_hold_never_makes_a_late_running_bus_look_earlier():
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    baseline = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0)
    assert baseline is not None

    # The "schedule" says s1 was due at t=1 -- long past by the time any live
    # estimate would reach it -- simulating a bus already running behind. The
    # clamp uses max(), so this must never pull the estimate earlier or later.
    result = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[1].id, 1.0),
    )
    assert result is not None
    assert abs(result.seconds - baseline.seconds) < 0.01


def test_schedule_hold_inactive_without_a_block_id():
    # scheduled_timestop_fn is provided but vehicle_block_id is None (e.g. the
    # vehicle's block assignment isn't known right now) -- must be a no-op,
    # never call the fn with a meaningless block id.
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    baseline = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0)
    result = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, line.stops[3], hop_time_fn, when=0.0,
        vehicle_block_id=None, scheduled_timestop_fn=_hold_fn(line.stops[1].id, 5000.0),
    )
    assert result is not None and baseline is not None
    assert abs(result.seconds - baseline.seconds) < 0.01


def test_schedule_hold_applies_when_arc_length_has_ticked_past_the_held_stop():
    # Regression, confirmed live (2026-09-16): a bus dwelling at a mapped
    # timestop whose arc-length position has already ticked past that stop
    # (dwelling_at_prev -- see that block's own comment) got NO hold applied
    # at all. The main hop-walk starts at next_stop and never looks back at
    # prev_stop, which is where the vehicle is actually still sitting -- a
    # real bus holding at Madison Ave @ Preston Ave with ~2 minutes left on
    # its scheduled departure showed a 62s ETA for the very next stop, tagged
    # "historical" (this exact branch's source label), instead of reflecting
    # the hold at all.
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    # Vehicle sitting exactly on stop 0's real coordinates (lat=0, lon=0 --
    # see _line's coordinate convention), but arc-length has already ticked to
    # 50m -- past stop 0 (arc_pos=0), nowhere near stop 1 (arc_pos=300) --
    # triggering dwelling_at_prev, not dwelling_at_next.
    vehicle_s_pos = 50.0
    vehicle_lat, vehicle_lon = 0.0, 0.0

    baseline = eta.estimate_stop_eta_s(
        line, vehicle_s_pos, 5.0, line.stops[1], hop_time_fn, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
    )
    assert baseline is not None
    assert baseline.source == "historical"
    assert abs(baseline.seconds - 60.0) < 0.01  # plain historical hop time, no hold

    # Block "B1" isn't due to leave stop 0 until t=500 -- far later than the
    # 60s historical hop time alone would suggest.
    result = eta.estimate_stop_eta_s(
        line, vehicle_s_pos, 5.0, line.stops[1], hop_time_fn, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[0].id, 500.0),
    )
    assert result is not None
    assert abs(result.seconds - 560.0) < 0.01  # 500s hold + 60s ride, not just 60s

    # The hold must also cascade into a target FURTHER downstream, since
    # current_leg_s seeds the main loop's total_s.
    result_further = eta.estimate_stop_eta_s(
        line, vehicle_s_pos, 5.0, line.stops[2], hop_time_fn, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[0].id, 500.0),
    )
    assert result_further is not None
    assert result_further.seconds > result.seconds


def test_schedule_hold_dwelling_at_prev_never_makes_a_late_bus_look_earlier():
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    vehicle_s_pos = 50.0
    vehicle_lat, vehicle_lon = 0.0, 0.0
    baseline = eta.estimate_stop_eta_s(
        line, vehicle_s_pos, 5.0, line.stops[1], hop_time_fn, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
    )
    # "Schedule" says stop 0 was due at t=-500 -- long past relative to
    # when=0.0 -- simulating a bus already running behind. max(0, ...) must
    # keep this a no-op, never pull the estimate into negative territory.
    result = eta.estimate_stop_eta_s(
        line, vehicle_s_pos, 5.0, line.stops[1], hop_time_fn, when=0.0,
        vehicle_lat=vehicle_lat, vehicle_lon=vehicle_lon,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[0].id, -500.0),
    )
    assert result is not None and baseline is not None
    assert abs(result.seconds - baseline.seconds) < 0.01
