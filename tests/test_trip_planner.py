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
    result = tp._ride_leg(line, 0, 2, _ts(17, 55), service, {})
    assert result is None


def test_ride_leg_rescued_by_interline_chain_near_boundary():
    service = tp.RouteService(
        windows={"67": [(_ts(5), _ts(17, 50))], "57": [(_ts(17, 51), _ts(22))]},
        chain_next={"67": "57"},
    )
    line = _line("67")
    # Boarding at 5:49pm, one minute before 67's nominal end -- the same physical bus
    # keeps going as 57 afterward, so this must NOT be excluded.
    result = tp._ride_leg(line, 0, 2, _ts(17, 49), service, {})
    assert result is not None
    leg, _alight_time = result
    assert leg.service_ends_ts == _ts(22)


def test_ride_leg_not_scheduled_today_returns_none():
    service = tp.RouteService(windows={})
    line = _line("999")
    result = tp._ride_leg(line, 0, 2, _ts(12), service, {})
    assert result is None


def test_ride_leg_cat_requires_live_eta():
    line = _line("cat-7", loop=False, source="cat")
    # No live wait known for this stop -> don't recommend it.
    assert tp._ride_leg(line, 0, 2, _ts(12), None, {}) is None
    # A live wait makes it available.
    result = tp._ride_leg(line, 0, 2, _ts(12), None, {("cat-7", "cat-7-A"): 300.0})
    assert result is not None


def test_ride_leg_path_includes_every_stop_in_travel_order():
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    line = _line("67")
    result = tp._ride_leg(line, 0, 2, _ts(12), service, {})
    assert result is not None
    leg, _alight_time = result
    assert [s.id for s in leg.path] == ["67-A", "67-B", "67-C"]


def test_ride_leg_path_wraps_for_a_loop():
    service = tp.RouteService(windows={"67": [(_ts(5), _ts(22))]})
    line = _line("67")
    # Board at C (index 2), alight at A (index 0): on a loop this wraps forward
    # through the full stop list rather than going "backward".
    result = tp._ride_leg(line, 2, 0, _ts(12), service, {})
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
    # Loop order: 0=OriginClose, 1=DestFar(but in-radius), 2=DestClose, 3=OriginFar.
    # Picking the stop nearest EACH point independently (the old behaviour) gives
    # board=0, alight=2 -> 2 hops. But board=0, alight=1 is only 1 hop and both stops
    # are still valid candidates (within radius of their respective point) -- the
    # exhaustive search must find that shorter pairing instead.
    stops = [
        tp.Stop(id="origin-close", name="origin-close", lat=0.0, lon=0.0, source="uts"),
        tp.Stop(id="dest-far", name="dest-far", lat=0.0015, lon=0.0, source="uts"),
        tp.Stop(id="dest-close", name="dest-close", lat=0.001, lon=0.0, source="uts"),
        tp.Stop(id="origin-far", name="origin-far", lat=0.0005, lon=0.0, source="uts"),
    ]
    line = tp.Line(id="loop1", name="Loop", color="#fff", source="uts", stops=stops, loop=True)
    origin_idxs = [0, 3]  # both within radius of the origin
    dest_idxs = [1, 2]  # both within radius of the destination
    assert tp._best_direct_pair(line, origin_idxs, dest_idxs) == (0, 1)
