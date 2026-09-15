import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import trip_planner as tp

REFERENCE_DATE = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)


def _ts(hour: int, minute: int = 0, day_offset: int = 0) -> float:
    return REFERENCE_DATE.replace(hour=hour, minute=minute).timestamp() + day_offset * 86400


# --- estimate_walk_leg -------------------------------------------------------------


def test_estimate_walk_leg_applies_detour_factor_and_speed():
    # Roughly 1111m north (0.01 degrees latitude near the equator).
    leg = tp.estimate_walk_leg((0.0, 0.0), (0.01, 0.0))
    straight_m = tp.haversine_m(0.0, 0.0, 0.01, 0.0)
    assert leg.source == "straight_line"
    assert abs(leg.distance_m - straight_m * tp.WALK_DETOUR_FACTOR) < 1.0
    assert abs(leg.duration_s - leg.distance_m / tp.WALK_SPEED_MPS) < 1.0


# --- build_route_service: non-passenger exclusion + basic windows ------------------


def _gold_block_group():
    """Matches the real shape pulled live from uva.transloc.com on 2026-09-14: Gold
    Line's RouteID changes from 67 (day) to 57 (evening) mid-shift, same physical block
    ("Gold_01"), one minute apart."""
    return {
        "BlockGroupId": "[09]",
        "Blocks": [
            {
                "BlockId": "Gold_01",
                "BlockStartTime": "05:00 AM",
                "BlockEndTime": "05:50 PM",
                "Route": {"RouteId": 67, "Description": "Gold Line"},
            },
            {
                "BlockId": "Gold_01",
                "BlockStartTime": "05:51 PM",
                "BlockEndTime": "10:00 PM",
                "Route": {"RouteId": 57, "Description": "Gold Line"},
            },
        ],
    }


def _charter_block_group():
    return {
        "BlockGroupId": "Charter",
        "Blocks": [
            {
                "BlockId": "Charter_01",
                "BlockStartTime": "12:00 AM",
                "BlockEndTime": "11:59 PM",
                "Route": {"RouteId": 2, "Description": "Charter"},
            }
        ],
    }


def test_build_route_service_excludes_non_passenger_groups():
    service = tp.build_route_service([_charter_block_group()], REFERENCE_DATE)
    assert "2" not in service.windows


def test_build_route_service_windows_match_schedule():
    service = tp.build_route_service([_gold_block_group()], REFERENCE_DATE)
    assert service.windows["67"] == [(_ts(5), _ts(17, 50))]
    assert service.windows["57"] == [(_ts(17, 51), _ts(22))]


def test_build_route_service_detects_interline_chain():
    service = tp.build_route_service([_gold_block_group()], REFERENCE_DATE)
    assert service.chain_next.get("67") == "57"
    # effective_window extends 67's end through 57's end, since it's the same vehicle.
    start, end = service.effective_window("67")
    assert start == _ts(5)
    assert end == _ts(22)


def test_build_route_service_no_chain_across_different_blocks():
    # Two unrelated blocks in the same group should NOT be treated as one vehicle.
    group = {
        "BlockGroupId": "[01]",
        "Blocks": [
            {
                "BlockId": "Green_01",
                "BlockStartTime": "07:28 AM",
                "BlockEndTime": "05:45 PM",
                "Route": {"RouteId": 68, "Description": "Green Line"},
            },
            {
                "BlockId": "Green_02",
                "BlockStartTime": "05:46 PM",
                "BlockEndTime": "10:00 PM",
                "Route": {"RouteId": 54, "Description": "Green Loop"},
            },
        ],
    }
    service = tp.build_route_service([group], REFERENCE_DATE)
    assert "68" not in service.chain_next


def test_build_route_service_midnight_rollover():
    group = {
        "BlockGroupId": "[03]",
        "Blocks": [
            {
                "BlockId": "NP_1",
                "BlockStartTime": "11:30 PM",
                "BlockEndTime": "12:30 AM",
                "Route": {"RouteId": 59, "Description": "Night Pilot"},
            }
        ],
    }
    service = tp.build_route_service([group], REFERENCE_DATE)
    start, end = service.windows["59"][0]
    assert end > start
    assert (end - start) == 3600.0


# --- _ride_leg: service-window gating + interline rescue ---------------------------


def _line(line_id: str, loop: bool = True, source: str = "uts") -> tp.Line:
    stops = [
        tp.Stop(id=f"{line_id}-A", name="A", lat=0.0, lon=0.0, source=source),
        tp.Stop(id=f"{line_id}-B", name="B", lat=0.001, lon=0.0, source=source),
        tp.Stop(id=f"{line_id}-C", name="C", lat=0.002, lon=0.0, source=source),
    ]
    return tp.Line(id=line_id, name=line_id, color="#ffdd00", source=source, stops=stops, loop=loop)


def test_ride_leg_excluded_after_route_ends_with_no_chain():
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(17, 50))]})
    line = _line("67")
    # Boarding at 5:55pm -- five minutes after this route's own window closed, and
    # nothing chains forward from it.
    result = tp._ride_leg(line, 0, 2, _ts(17, 55), _ts(17, 55), service, {})
    assert result is None


def test_ride_leg_rescued_by_interline_chain_near_boundary():
    service = tp.RouteService(
        windows={"67": [(_ts(5), _ts(17, 50))], "57": [(_ts(17, 51), _ts(22))]},
        chain_next={"67": "57"},
    )
    line = _line("67")
    # Boarding at 5:49pm, one minute before 67's nominal end -- the same physical bus
    # keeps going as 57 afterward, so this must NOT be excluded.
    result = tp._ride_leg(line, 0, 2, _ts(17, 49), _ts(17, 49), service, {})
    assert result is not None
    leg, _alight_time = result
    assert leg.service_ends_ts == _ts(22)


def test_ride_leg_not_scheduled_today_returns_none():
    service = tp.RouteService(windows={})
    line = _line("999")
    result = tp._ride_leg(line, 0, 2, _ts(12), _ts(12), service, {})
    assert result is None


def test_ride_leg_cat_requires_live_eta():
    line = _line("cat-7", loop=False, source="cat")
    # No live wait known for this stop -> don't recommend it.
    assert tp._ride_leg(line, 0, 2, _ts(12), _ts(12), None, {}) is None
    # A live wait makes it available.
    result = tp._ride_leg(line, 0, 2, _ts(12), _ts(12), None, {("cat-7", "cat-7-A"): [300.0]})
    assert result is not None


def test_ride_leg_picks_the_first_bus_the_rider_can_actually_catch():
    # Two vehicles serving the same stop: one arriving in 1 minute (long gone by the
    # time a rider who has to walk there shows up), one in 6 minutes. Regression for a
    # real bug: the old code took the single soonest live ETA and added it on top of
    # the walk time, effectively claiming a rider could catch a bus that would have
    # already left before they arrived at the stop.
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    line = _line("67")
    when = _ts(12)
    board_time = when + 180.0  # takes 3 minutes to walk to the stop
    live_wait_lookup = {("67", "67-A"): [60.0, 360.0]}  # 1 min (uncatchable), 6 min (catchable)
    result = tp._ride_leg(line, 0, 2, board_time, when, service, live_wait_lookup)
    assert result is not None
    leg, _alight_time = result
    # The bus itself is 6 minutes out, but 3 of those minutes are spent walking there --
    # the rider only actually stands and waits for the remaining 3.
    assert leg.wait_s == 180.0


def test_ride_leg_cat_not_recommended_when_every_live_bus_is_uncatchable():
    line = _line("cat-7", loop=False, source="cat")
    when = _ts(12)
    board_time = when + 300.0  # takes 5 minutes to walk to the stop
    live_wait_lookup = {("cat-7", "cat-7-A"): [60.0, 120.0]}  # both gone before rider arrives
    assert tp._ride_leg(line, 0, 2, board_time, when, None, live_wait_lookup) is None


def test_ride_leg_path_includes_every_stop_in_travel_order():
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    line = _line("67")
    result = tp._ride_leg(line, 0, 2, _ts(12), _ts(12), service, {})
    assert result is not None
    leg, _alight_time = result
    assert [s.id for s in leg.path] == ["67-A", "67-B", "67-C"]


def test_ride_leg_path_wraps_for_a_loop():
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    line = _line("67")
    # Board at C (index 2), alight at A (index 0): on a loop this wraps forward
    # through the full stop list rather than going "backward".
    result = tp._ride_leg(line, 2, 0, _ts(12), _ts(12), service, {})
    assert result is not None
    leg, _alight_time = result
    assert [s.id for s in leg.path] == ["67-C", "67-A"]


def test_hop_distance_non_loop_cannot_go_backward():
    line = _line("cat-7", loop=False, source="cat")
    assert tp._hop_distance(line, 2, 0) is None
    assert tp._hop_distance(line, 0, 2) == 2


def test_hop_distance_loop_wraps_around():
    line = _line("67", loop=True)
    assert tp._hop_distance(line, 2, 0) == 1  # wraps past the end back to index 0


# --- find_trips: end-to-end direct itinerary ---------------------------------------


def test_find_trips_direct_ride_within_service_window():
    line = _line("67")
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    origin = (-0.0005, 0.0)  # near stop A
    destination = (0.0025, 0.0)  # near stop C
    itineraries = tp.find_trips(origin, destination, [line], service, {}, when=_ts(12))
    assert itineraries
    kinds = [leg.kind for leg in itineraries[0].legs]
    assert kinds == ["walk", "ride", "walk"]


def test_find_trips_total_duration_does_not_double_count_the_walk_to_the_stop():
    # Regression: the displayed/summed wait must be the time actually spent standing
    # at the stop, not "seconds from when the search started" (which already has the
    # walk time baked in) -- otherwise the walk gets counted twice in the total.
    line = _line("67")
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    origin = (-0.0005, 0.0)  # near stop A
    destination = (0.0025, 0.0)  # near stop C
    when = _ts(12)
    walk_to = tp.estimate_walk_leg(origin, (line.stops[0].lat, line.stops[0].lon))
    # One bus that's already gone by the time the rider gets there, one that's 4
    # minutes further out and genuinely catchable.
    live_wait_lookup = {("67", "67-A"): [walk_to.duration_s - 5.0, walk_to.duration_s + 240.0]}
    itineraries = tp.find_trips(origin, destination, [line], service, live_wait_lookup, when=when)
    ride = next(leg for it in itineraries for leg in it.legs if leg.kind == "ride")
    itinerary = next(it for it in itineraries if ride in it.legs)
    assert abs(ride.wait_s - 240.0) < 0.01  # the real, experienced wait -- not 240 + the walk time
    walk_legs = [leg for leg in itinerary.legs if leg.kind == "walk"]
    expected_total = sum(leg.duration_s for leg in walk_legs) + ride.wait_s + ride.ride_s
    assert abs(itinerary.total_duration_s - expected_total) < 0.01


def test_find_trips_offers_only_walk_when_route_already_ended():
    line = _line("67")
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(17, 50))]})
    origin = (-0.0005, 0.0)
    destination = (0.0025, 0.0)
    itineraries = tp.find_trips(origin, destination, [line], service, {}, when=_ts(20))
    # No ride is possible (the route ended hours ago), but a walk-only fallback must
    # still be offered rather than leaving the rider with nothing at all.
    assert len(itineraries) == 1
    assert [leg.kind for leg in itineraries[0].legs] == ["walk"]


def test_find_trips_prefers_a_short_walk_over_a_roundabout_ride():
    # Regression for a real bug (confirmed live: Rice Hall -> Scott Stadium, 259m
    # apart, was only ever offered as multi-transfer itineraries because no walk-only
    # candidate existed to compete with them). Loop shaped like an out-and-back: stop 1
    # (outbound leg) sits geographically right next to stop 6 (the parallel return
    # leg), but they're 5 hops apart in the loop's travel order -- so the only ride
    # option is a long way around, while the two points are a trivial walk apart.
    out_lats = [0.0] * 4
    out_lons = [0.0, 0.001, 0.002, 0.003]
    back_lats = [0.0005] * 4
    back_lons = [0.003, 0.002, 0.001, 0.0]
    stops = [
        tp.Stop(id=f"out{i}", name=f"out{i}", lat=out_lats[i], lon=out_lons[i], source="uts")
        for i in range(4)
    ] + [
        tp.Stop(id=f"back{i}", name=f"back{i}", lat=back_lats[i], lon=back_lons[i], source="uts")
        for i in range(4)
    ]
    line = tp.Line(id="67", name="Loop", color="#fff", source="uts", stops=stops, loop=True)
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    origin = (stops[1].lat, stops[1].lon)  # "out1"
    destination = (stops[6].lat, stops[6].lon)  # "back2" -- ~55m from origin
    itineraries = tp.find_trips(origin, destination, [line], service, {}, when=_ts(12))
    assert itineraries
    assert [leg.kind for leg in itineraries[0].legs] == ["walk"]


def test_best_direct_pair_beats_nearest_to_each_point_independently():
    # Out-and-back loop (same shape as test_find_trips_prefers_a_short_walk_over_a_
    # roundabout_ride): "out1" and "back2" sit right next to each other physically
    # (~56m apart) but are 5 hops apart the long way around the loop, and only 3 hops
    # apart the OTHER way around. Picking the stop nearest EACH point independently
    # (board=out1 exact, alight=back2 exact -- both zero walk) takes the long 5-hop
    # path. The exhaustive search must find that riding the shorter 3-hop path,
    # despite it costing a small walk at both ends, is actually cheaper.
    out_lons = [0.0, 0.008, 0.016, 0.024]
    back_lons = [0.024, 0.016, 0.008, 0.0]
    stops = [
        tp.Stop(id=f"out{i}", name=f"out{i}", lat=0.0, lon=out_lons[i], source="uts")
        for i in range(4)
    ] + [
        tp.Stop(id=f"back{i}", name=f"back{i}", lat=0.0005, lon=back_lons[i], source="uts")
    for i in range(4)
    ]
    line = tp.Line(id="loop1", name="Loop", color="#fff", source="uts", stops=stops, loop=True)
    origin = (stops[1].lat, stops[1].lon)  # "out1"
    destination = (stops[6].lat, stops[6].lon)  # "back2" -- ~56m from origin
    origin_idxs = [1, 6]  # both within walking radius of the origin
    dest_idxs = [6, 1]  # both within walking radius of the destination
    assert tp._best_direct_pair(line, origin_idxs, dest_idxs, origin, destination) == (6, 1)


def test_best_direct_pair_does_not_walk_past_the_closest_stop_to_save_one_hop():
    # Regression for a real bug reported live: the planner would send a rider to a
    # FARTHER stop on the same line than the one right next to them, purely because
    # boarding there happened to shave a hop off the ride -- then also get off early
    # and walk the rest of the way. Fewer hops must not automatically win if reaching
    # them costs meaningfully more walking than the hop savings are worth.
    #
    # Loop order: 0=OriginHere (at the origin), 1=OriginFar (~555m away, but 1 hop
    # closer to the destination stop), 2=Mid, 3=DestinationHere (at the destination).
    stops = [
        tp.Stop(id="origin-here", name="origin-here", lat=0.0, lon=0.0, source="uts"),
        tp.Stop(id="origin-far", name="origin-far", lat=0.0, lon=0.005, source="uts"),
        tp.Stop(id="mid", name="mid", lat=0.0025, lon=0.0025, source="uts"),
        tp.Stop(id="destination-here", name="destination-here", lat=0.005, lon=0.0, source="uts"),
    ]
    line = tp.Line(id="67", name="Loop", color="#fff", source="uts", stops=stops, loop=True)
    origin = (0.0, 0.0)
    destination = (0.005, 0.0)
    # Boarding at stop 1 instead of stop 0 saves one hop (3 hops -> 2 hops) but costs a
    # long walk to reach it in the first place -- not a trade worth making.
    origin_idxs = [0, 1]
    dest_idxs = [3]
    assert tp._best_direct_pair(line, origin_idxs, dest_idxs, origin, destination) == (0, 3)


def test_best_direct_pair_does_not_treat_a_short_hop_as_a_full_flat_estimate():
    # Regression for a real bug reported live (Johnson House -> Pinn Hall on Night
    # Pilot): two stops sat only ~64m apart on the same line -- "close" (45m from the
    # rider) then, one hop later, "far" (109m from the rider). The flat
    # SECONDS_PER_HOP_ESTIMATE (90s) values that one short hop the same as any other,
    # making "walk 64m further to save a hop" look like a wash (in production it
    # narrowly picked the farther stop). Ranking a hop by the real distance between
    # its two stops instead correctly recognizes this particular hop is a quick ~65m
    # hop, not worth an extra 64m walk to skip.
    stops = [
        tp.Stop(id="close", name="close", lat=0.0, lon=0.000404, source="uts"),  # ~45m out
        tp.Stop(id="far", name="far", lat=0.0, lon=0.000979, source="uts"),  # ~109m out
        tp.Stop(id="alight", name="alight", lat=0.0, lon=0.003, source="uts"),  # ~334m past "far"
    ]
    line = tp.Line(id="59", name="Night Pilot", color="#fff", source="uts", stops=stops, loop=True)
    origin = (0.0, 0.0)
    destination = (stops[2].lat, stops[2].lon)
    origin_idxs = [0, 1]  # both "close" and "far" are within walking radius
    dest_idxs = [2]
    assert tp._best_direct_pair(line, origin_idxs, dest_idxs, origin, destination) == (0, 2)


def test_best_transfer_returns_its_own_optimized_alight_stop():
    # Regression for a real bug reported live: find_trips used to re-derive the
    # transfer's second-leg alight stop by picking whichever destination-candidate
    # stop was geometrically nearest, completely ignoring which one _best_transfer's
    # own search had actually validated as reachable/optimal. On a non-loop line that
    # geometrically-nearest stop can sit BEHIND the boarding point (unreachable),
    # which silently dropped the whole itinerary -- explaining CAT trips going missing,
    # not just looking wrong.
    #
    # Geometry: "shared" (the transfer point) sits far (~800m+) from both origin and
    # destination, so neither line has a spurious direct ride of its own -- the only
    # way to complete this trip is the a->b transfer, isolating the bug.
    line_a = tp.Line(
        id="a", name="A", color="#fff", source="uts", loop=False,
        stops=[
            tp.Stop(id="origin-stop", name="origin-stop", lat=0.0, lon=0.0, source="uts"),
            tp.Stop(id="shared", name="shared", lat=0.005, lon=0.005, source="uts"),
        ],
    )
    # decoy (index 0) sits BEFORE shared (index 1) in travel order -- unreachable from
    # it on a non-loop line -- but is geometrically the closest destination candidate
    # (it's placed exactly at the destination). real-dest (index 3) is reachable (2
    # hops) but ~55m farther away.
    line_b = tp.Line(
        id="b", name="B", color="#fff", source="uts", loop=False,
        stops=[
            tp.Stop(id="decoy", name="decoy", lat=0.0104, lon=0.0104, source="uts"),
            tp.Stop(id="shared", name="shared", lat=0.005, lon=0.005, source="uts"),
            tp.Stop(id="mid", name="mid", lat=0.0075, lon=0.0075, source="uts"),
            tp.Stop(id="real-dest", name="real-dest", lat=0.0104, lon=0.01085, source="uts"),
        ],
    )
    service = tp.RouteService(windows={"a": [(_ts(5), _ts(22))], "b": [(_ts(5), _ts(22))]})
    origin = (0.0, 0.0)
    destination = (0.0104, 0.0104)  # exactly at "decoy"; ~55m from "real-dest"

    itineraries = tp.find_trips(origin, destination, [line_a, line_b], service, {}, when=_ts(12))
    ride_legs = [leg for it in itineraries for leg in it.legs if leg.kind == "ride" and leg.line_id == "b"]
    assert ride_legs, "the b-leg transfer must not be silently dropped"
    assert ride_legs[0].alight_stop.id == "real-dest"


def test_live_wait_follows_the_interline_chain_when_the_boarding_line_has_none():
    # Regression for a real bug reported live ("wait unknown" showing up constantly):
    # TransLoc's live vehicle feed reports under whichever RouteID is CURRENTLY
    # active -- once Gold Line's vehicle relabels from 67 to 57, GetStopArrivalTimes
    # stops returning anything under "67" at all, even though 67 is still a valid
    # boardable line (via the interline-extended service window). The live wait must
    # follow that same chain instead of giving up at the first empty lookup.
    service = tp.RouteService(
        windows={"67": [(_ts(5), _ts(17, 50))], "57": [(_ts(17, 51), _ts(22))]},
        chain_next={"67": "57"},
    )
    line = _line("67")
    live_wait_lookup = {("57", "67-A"): [300.0]}  # only the relabeled RouteID has a live entry
    result = tp._ride_leg(line, 0, 2, _ts(17, 49), _ts(17, 49), service, live_wait_lookup)
    assert result is not None
    leg, _alight_time = result
    assert leg.wait_s == 300.0
