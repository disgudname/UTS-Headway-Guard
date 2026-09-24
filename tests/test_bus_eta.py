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
    # Without the decay this would be 2100m / MIN_PROJECTION_MPS (=700s);
    # decayed toward ~5.5 m/s over 7 hops, it should land clearly below that.
    assert result.seconds < 2100.0 / crawling_mps * 0.75


def test_a_momentarily_stopped_bus_is_not_read_as_running_slow():
    # Found in a live ETA-vs-reality log (2026-09-19): predictions jumped >90s LATER
    # mostly when the bus was stopped (73% of the time vs 35% baseline) and jumped
    # back once it moved -- a red light being read as a slow trip. Any live speed at or
    # below the floor must give the same answer, and that answer must sit close to a
    # normally-moving bus's, not several times higher.
    line = _line([0, 300, 600, 900, 1200])
    hop = lambda *a: 40.0  # 300m in 40s = 7.5 m/s typical
    stopped = eta.estimate_stop_eta_s(line, 100.0, 0.05, line.stops[4], hop_time_fn=hop, when=0.0)
    at_floor = eta.estimate_stop_eta_s(line, 100.0, eta.MIN_PROJECTION_MPS, line.stops[4], hop_time_fn=hop, when=0.0)
    cruising = eta.estimate_stop_eta_s(line, 100.0, 7.5, line.stops[4], hop_time_fn=hop, when=0.0)
    assert stopped.seconds == at_floor.seconds
    assert stopped.seconds < cruising.seconds * 1.5


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
    # Held until 500s (+ the bus's usual departure lag) at s1, then two more 60s hops
    # to reach s3.
    expected = 500.0 + eta.SCHEDULED_DEPARTURE_LAG_S + 120.0
    assert expected - 5.0 < result.seconds < expected + 5.0
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
    # 500s hold + departure lag + 60s ride, not just 60s
    assert abs(result.seconds - (500.0 + eta.SCHEDULED_DEPARTURE_LAG_S + 60.0)) < 0.01

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


def test_buses_are_assumed_to_leave_a_timestop_a_little_after_its_scheduled_time():
    line = _line([0, 300, 900, 1200])
    hop_time_fn = _flat_hop_time_fn(60.0)
    hold = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, line.stops[2], hop_time_fn, when=0.0,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[1].id, 500.0),
    )
    assert abs(hold.seconds - (500.0 + eta.SCHEDULED_DEPARTURE_LAG_S + 60.0)) < 0.5


def test_hop_history_leaving_a_held_timestop_is_not_double_counted():
    # History hops that START at a timestop include the layover (Green's Chapel hop is
    # 267s in history for ~60s of driving). The hold clamp already waits for the
    # scheduled departure, so that hop must be driven at typical speed instead.
    line = _line([0, 300, 900, 1200])
    inflated = _flat_hop_time_fn(400.0)  # 600m hop "taking" 400s, layover included
    result = eta.estimate_stop_eta_s(
        line, 0.0, 5.0, line.stops[2], inflated, when=0.0,
        vehicle_block_id="B1", scheduled_timestop_fn=_hold_fn(line.stops[1].id, 500.0),
    )
    drive = 600.0 / eta.TYPICAL_BUS_SPEED_MPS + eta.POST_HOLD_HOP_ALLOWANCE_S
    assert abs(result.seconds - (500.0 + eta.SCHEDULED_DEPARTURE_LAG_S + drive)) < 0.5
    # ...but a hop that doesn't start at a held timestop keeps its history time.
    plain = eta.estimate_stop_eta_s(line, 0.0, 5.0, line.stops[2], inflated, when=0.0)
    assert plain.seconds > 400.0


def test_hop_leaving_a_mapped_timestop_never_carries_layover_even_with_no_hold_scheduled():
    # Regression found live 2026-09-19 (Orange, Saturday): history is pooled across day
    # types, so a stop that holds on weekdays (Pinn Hall: a 550s hop in weekday-evening
    # history) but NOT on a Saturday smuggled its weekday layover into every ETA past it.
    # Being a mapped timestop is enough to strip the layover from the outgoing hop; no
    # hold has to be scheduled at that moment.
    line = _line([0, 300, 900, 1200])
    inflated = _flat_hop_time_fn(400.0)  # 600m hop "taking" 400s
    is_timestop = lambda route_id, stop_id, when: stop_id == line.stops[1].id
    # Vehicle at 100m: its next stop is the timestop (200m away = 40s live), then the
    # timestop -> s2 hop is the one that must be capped.
    with_map = eta.estimate_stop_eta_s(
        line, 100.0, 5.0, line.stops[2], inflated, when=0.0, is_timestop_fn=is_timestop,
    )
    without = eta.estimate_stop_eta_s(line, 100.0, 5.0, line.stops[2], inflated, when=0.0)
    drive = 600.0 / eta.TYPICAL_BUS_SPEED_MPS + eta.POST_HOLD_HOP_ALLOWANCE_S
    # (the capped hop is still scaled by the vehicle's pace factor like any other hop, so
    # it can only get shorter than the cap, never longer)
    assert 40.0 < with_map.seconds <= 40.0 + drive + 0.5
    assert without.seconds > with_map.seconds + 100.0


# --- Out-of-service cut-off (out_of_service_fn) ---
# Six stops 300m apart; block "B1" makes its last public departure from s1 and serves nothing
# past s3 (the cut-off), like Gold block [10]: leave BAR 19:55, carry passengers as far as the dorms.

def _oos_setup():
    line = _line([0, 300, 600, 900, 1200, 1500], n=17)
    leave_epoch, active_from = 1000.0, 400.0
    plan = (line.stops[1].id, leave_epoch, line.stops[3].id, active_from)
    return line, plan, leave_epoch


def _oos_fn(plan):
    return lambda route_id, block_id, when: plan if block_id == "B1" else None


def test_out_of_service_target_beyond_cutoff_is_none_and_up_to_cutoff_is_served():
    line, plan, leave_epoch = _oos_setup()
    hop = _flat_hop_time_fn(60.0)
    kw = dict(
        hop_time_fn=hop, when=900.0, vehicle_block_id="B1",
        scheduled_timestop_fn=_hold_fn(line.stops[1].id, leave_epoch), out_of_service_fn=_oos_fn(plan),
    )
    # Bus still upstream of the last departure (100m): reaching s1 IS the last departure.
    assert eta.estimate_stop_eta_s(line, 100.0, 5.0, line.stops[2], **kw) is not None
    assert eta.estimate_stop_eta_s(line, 100.0, 5.0, line.stops[3], **kw) is not None  # the cut-off itself
    assert eta.estimate_stop_eta_s(line, 100.0, 5.0, line.stops[4], **kw) is None
    assert eta.estimate_stop_eta_s(line, 100.0, 5.0, line.stops[0], **kw) is None  # next lap: never served


def test_out_of_service_bus_already_between_leave_and_cutoff_is_on_its_last_trip():
    line, plan, leave_epoch = _oos_setup()
    kw = dict(hop_time_fn=_flat_hop_time_fn(60.0), when=1010.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan))
    assert eta.estimate_stop_eta_s(line, 400.0, 5.0, line.stops[3], **kw) is not None
    assert eta.estimate_stop_eta_s(line, 400.0, 5.0, line.stops[4], **kw) is None


def test_out_of_service_not_applied_before_the_active_window_or_without_a_plan():
    line, plan, leave_epoch = _oos_setup()
    hop = _flat_hop_time_fn(60.0)
    # Same position as above but far too early in the day to be the last trip (the bus is just
    # passing this stretch on an earlier lap): predictions beyond the cut-off stand.
    early = eta.estimate_stop_eta_s(
        line, 400.0, 5.0, line.stops[4], hop, when=100.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan),
    )
    assert early is not None
    # A block with no plan is untouched.
    other = eta.estimate_stop_eta_s(
        line, 400.0, 5.0, line.stops[4], hop, when=1010.0, vehicle_block_id="B2", out_of_service_fn=_oos_fn(plan),
    )
    assert other is not None
    # A visit to the leave stop that is not the note's scheduled departure (another lap's hold)
    # does not start the last trip.
    not_final = eta.estimate_stop_eta_s(
        line, 100.0, 5.0, line.stops[4], hop_time_fn=hop, when=100.0, vehicle_block_id="B1",
        scheduled_timestop_fn=_hold_fn(line.stops[1].id, 555.0), out_of_service_fn=_oos_fn(plan),
    )
    assert not_final is not None


def test_timestop_cap_receives_the_time_the_bus_reaches_the_stop():
    line = _line([0, 300, 900, 1200])
    seen = []

    def is_timestop(route_id, stop_id, when):
        seen.append((stop_id, when))
        return False

    eta.estimate_stop_eta_s(
        line, 100.0, 5.0, line.stops[3], _flat_hop_time_fn(100.0), when=5000.0, is_timestop_fn=is_timestop,
    )
    assert seen and all(w >= 5000.0 for _, w in seen)
    assert seen[0][1] < seen[-1][1]  # later stops are asked about later times


# --- full-lap cut-off: the cut-off is the NEXT arrival at the stop the bus leaves from ---
# Same six stops; leave = cut-off = s3 (900 m). Orange weekend [05]: leave the Library, make the loop, become Night Pilot.

def _full_lap_setup():
    line = _line([0, 300, 600, 900, 1200, 1500], n=17)
    plan = (line.stops[3].id, 1000.0, line.stops[3].id, 400.0)
    return line, plan


def test_full_lap_bus_just_past_the_leave_stop_is_served_until_it_returns_and_no_further():
    line, plan = _full_lap_setup()
    kw = dict(hop_time_fn=_flat_hop_time_fn(60.0), when=1010.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan))
    for i in (5, 0, 1, 2, 3):  # everything on the lap, ending with the stop it returns to
        assert eta.estimate_stop_eta_s(line, 1300.0, 5.0, line.stops[i], **kw) is not None, i
    assert eta.estimate_stop_eta_s(line, 1300.0, 5.0, line.stops[4], **kw) is None  # would need a second pass past s3


def test_full_lap_bus_nearing_the_end_of_its_lap_gets_no_eta_past_the_return_stop():
    line, plan = _full_lap_setup()
    kw = dict(hop_time_fn=_flat_hop_time_fn(60.0), when=2000.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan))
    assert eta.estimate_stop_eta_s(line, 800.0, 5.0, line.stops[3], **kw) is not None  # the return itself
    assert eta.estimate_stop_eta_s(line, 800.0, 5.0, line.stops[4], **kw) is None       # Night Pilot territory


def test_full_lap_bus_still_approaching_its_departure_is_not_cut_off_at_the_departure():
    line, plan = _full_lap_setup()
    kw = dict(
        hop_time_fn=_flat_hop_time_fn(60.0), when=500.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan),
        scheduled_timestop_fn=_hold_fn(line.stops[3].id, 1000.0),
    )
    # Reaching s3 is the scheduled departure; the stops after it are the final lap and must still be predicted.
    for i in (4, 5, 0, 1):
        assert eta.estimate_stop_eta_s(line, 800.0, 5.0, line.stops[i], **kw) is not None, i


# --- a bus parked AT its cut-off stop, and a bus that has finished its last run ---

def test_bus_parked_at_the_cutoff_stop_still_counts_as_on_its_last_run():
    # Orange [05] parked at the Library 2026-09-23: it projected a few metres past the end of its final segment and
    # every stop reappeared. The cut-off stop is s3 (900 m); park the bus 20 m past it.
    line, plan, leave_epoch = _oos_setup()
    kw = dict(hop_time_fn=_flat_hop_time_fn(60.0), when=1010.0, vehicle_block_id="B1", out_of_service_fn=_oos_fn(plan))
    assert eta.estimate_stop_eta_s(line, 920.0, 5.0, line.stops[4], **kw) is None       # beyond the cut-off: nothing
    assert eta.estimate_stop_eta_s(line, 920.0, 5.0, line.stops[3], **kw) is not None   # the cut-off stop itself


def test_out_of_service_phase_run_outside_before_and_na():
    line, plan, leave_epoch = _oos_setup()
    assert eta.out_of_service_phase(line, 400.0, plan, 1010.0) == "run"        # between leave (300) and cut-off (900)
    assert eta.out_of_service_phase(line, 920.0, plan, 1010.0) == "run"        # parked at the cut-off (tolerance)
    assert eta.out_of_service_phase(line, 1300.0, plan, 1010.0) == "outside"   # past the cut-off, heading away
    assert eta.out_of_service_phase(line, 100.0, plan, 1010.0) == "outside"    # not yet at the last departure
    assert eta.out_of_service_phase(line, 400.0, plan, 100.0) == "before"      # window not open yet
    assert eta.out_of_service_phase(line, 400.0, None, 1010.0) == "na"
    full_lap = (line.stops[3].id, 1000.0, line.stops[3].id, 400.0)
    assert eta.out_of_service_phase(line, 400.0, full_lap, 1010.0) == "na"


def test_a_bus_seen_on_its_last_run_is_finished_once_it_is_outside_it():
    assert eta.out_of_service_finished("outside", True, 1010.0, 1000.0)
    assert not eta.out_of_service_finished("run", True, 1010.0, 1000.0)
    # never seen on the last run (e.g. server restart): only finished long after the scheduled departure
    assert not eta.out_of_service_finished("outside", False, 1000.0 + 600.0, 1000.0)
    assert eta.out_of_service_finished("outside", False, 1000.0 + eta.OOS_DONE_AFTER_S, 1000.0)
    assert not eta.out_of_service_finished("before", True, 1010.0, 1000.0)


# ---- evening route change: hide the stops the post-6PM route skips -------------------------------------------------
# Loop of 5 stops, 1000 m apart on a 5000 m loop: s0 (0), s1 (1000), s2 CHANGE STOP (2000), s3 (3000), s4 (4000).
# The post-6PM route serves s0, s2 and s4 only, so s1 and s3 are the "skipped" stops.
def _rc_line():
    shape, cum = _shape(n=51, step_m=100.0)
    names = ["Zero", "One", "Two", "Three", "Four"]
    stops = [Stop(id=f"s{i}", name=names[i], lat=0.0, lon=i * 1000 / 111_320.0, source="uts", arc_pos=i * 1000.0)
             for i in range(5)]
    return Line(id="67", name="Loop", color="#fff", source="uts", stops=stops, loop=True, shape=shape, shape_cum=cum)


_RC_T = 1_000_000.0  # the note's change time
_RC_SERVED = ["Zero", "Two", "Four"]


def _rc(s_pos, when, eta_at_change, prev=_RC_T - 2700.0):
    return eta.route_change_hidden_stops(_rc_line(), s_pos, ("s2", _RC_T, _RC_SERVED, prev), when, eta_at_change)


def test_route_change_hides_skipped_stops_beyond_the_change_stop_only():
    # Bus at 1500 m heading for the change stop (500 m away), due there for the noted trip.
    # s3 comes right after the change stop; s1 is 500 m BEHIND the bus, i.e. a full lap away, reached after the change
    assert _rc(1500.0, _RC_T - 120.0, 120.0) == {"s1", "s3"}


def test_route_change_hides_skipped_stops_after_the_change_stop_when_bus_is_upstream_of_them_all():
    # Bus at 500 m: s1 (skipped) is between it and the change stop, so it is still served on the old route.
    assert _rc(500.0, _RC_T - 300.0, 300.0) == {"s3"}


def test_route_change_keeps_everything_for_a_bus_on_the_earlier_trip():
    # Bus reaches the change stop ~30 min before the note's time (the previous scheduled visit was 45 min before): it
    # is still on the old route for that lap, so nothing is hidden.
    assert _rc(1500.0, _RC_T - 2100.0, 120.0) == set()


def test_route_change_without_a_previous_visit_treats_any_approach_as_the_change_trip():
    # Orange [07]-style: the block's first visit to the stop IS the change visit.
    assert _rc(1500.0, _RC_T - 2100.0, 120.0, prev=None) == {"s1", "s3"}


def test_route_change_bus_parked_at_the_change_stop_hides_beyond_it_until_the_change_time_then_everything():
    assert _rc(2000.0, _RC_T - 300.0, 0.0) == {"s1", "s3"}   # nothing it will still serve on the old route is hidden
    assert _rc(2010.0, _RC_T + 5.0, 0.0) == {"s1", "s3"}   # about to leave / just leaving: the whole skipped set


def test_route_change_bus_just_past_the_change_stop_hides_all_skipped_stops():
    assert _rc(2400.0, _RC_T + 30.0, 4000.0) == {"s1", "s3"}
    # Well before the note's time, a bus 400 m past the stop still has a whole lap to run on the old route: s1 and s3
    # both come up before it is back at the change stop, so neither is hidden.
    assert _rc(2400.0, _RC_T - 600.0, 4000.0) == set()


def test_route_change_never_hides_a_due_reading_at_a_stop_the_bus_is_sitting_on():
    # Bus a hair past s3 (3 m) while waiting to reach the change stop next lap: "Due" at s3 must stay visible.
    hidden = _rc(3003.0, _RC_T - 300.0, 4000.0)
    assert "s3" not in hidden


def test_route_change_is_a_noop_without_a_plan_eta_shape_or_skipped_stops():
    line = _rc_line()
    assert eta.route_change_hidden_stops(line, 1500.0, None, _RC_T, 100.0) == set()
    assert _rc(1500.0, _RC_T - 120.0, None) == set()   # no ETA at the change stop: do nothing rather than guess
    assert eta.route_change_hidden_stops(line, 1500.0, ("nope", _RC_T, _RC_SERVED, None), _RC_T, 100.0) == set()
    all_served = ["Zero", "One", "Two", "Three", "Four"]
    assert eta.route_change_hidden_stops(line, 1500.0, ("s2", _RC_T, all_served, None), _RC_T, 100.0) == set()
    bare = Line(id="67", name="x", color="#fff", source="uts", stops=line.stops, loop=True)
    assert eta.route_change_hidden_stops(bare, 1500.0, ("s2", _RC_T, _RC_SERVED, None), _RC_T, 100.0) == set()


def test_route_change_matches_stop_names_ignoring_case_and_punctuation():
    line = _rc_line()
    served = ["zero", "TWO", "Four"]   # same names, sloppy spelling / mojibake-safe
    hidden = eta.route_change_hidden_stops(line, 1500.0, ("s2", _RC_T, served, None), _RC_T - 120.0, 120.0)
    assert hidden == {"s1", "s3"}
