import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

pytest.importorskip("google.transit.gtfs_realtime_pb2")

import gtfs_feed as g  # noqa: E402
import gtfs_rt as rt  # noqa: E402
from tests.test_gtfs_feed import H, _block, _inputs  # noqa: E402

DAY = datetime(2026, 10, 19, tzinfo=ZoneInfo("America/New_York")).timestamp()  # Monday 00:00
SERVICE_DATE = "20261019"
# Route 10's stops are RouteStopIDs 100 (A), 101 (B), 102 (C), 103 (D); physical stop ids 1-4.


def _index(blocks, purple=None):
    _, _, index = g.build_feed_with_index(_inputs(blocks, purple=purple))
    return index


def _bus(route="10", block="[09]", vid="7"):
    return rt.Vehicle(vehicle_id=vid, label="18432", transloc_route_id=route, lat=38.0, lon=-78.49, block=block)


def _two_trips():
    return _index({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB"), (7 * H + 1200, "AA")])})


def test_a_bus_is_matched_to_its_blocks_trip_by_its_next_stop():
    index = _two_trips()
    now = DAY + 7 * H + 60
    etas = [("101", 90.0), ("102", 250.0)]  # heading to B, then C: the first trip (A -> C)
    m = rt.match_trip(index, SERVICE_DATE, DAY, _bus(), etas, now)
    assert m is not None and m.trip.trip_id.endswith("-001")
    assert [m.trip.stops[k].stop_id for k, _ in m.updates] == ["2", "3"]
    assert m.updates[0][1] == int(now + 90)


def test_the_trip_closest_in_time_wins_at_a_shared_stop():
    index = _two_trips()
    # C ends trip 1 (arrives ~07:04) and starts trip 2 (leaves 07:10). A bus reaching C at 07:09 is on trip 2.
    m = rt.match_trip(index, SERVICE_DATE, DAY, _bus(), [("102", 60.0)], DAY + 7 * H + 480)
    assert m.trip.trip_id.endswith("-002") and m.next_index == 0


def test_another_blocks_bus_or_a_far_off_time_is_not_matched():
    index = _two_trips()
    assert rt.match_trip(index, SERVICE_DATE, DAY, _bus(block="[11]"), [("101", 60.0)], DAY + 7 * H) is None
    assert rt.match_trip(index, SERVICE_DATE, DAY, _bus(), [("101", 60.0)], DAY + 12 * H) is None
    assert rt.match_trip(index, "20261025", DAY, _bus(), [("101", 60.0)], DAY + 7 * H) is None  # Sunday: no service


def test_purple_gets_a_start_time_inside_its_band():
    purple = {"weekdays": [0, 1, 2, 3, 4], "bands": [{"start_s": 6 * H, "end_s": 7 * H, "headway_s": 300}]}
    index = _index({}, purple=purple)
    # route 20 stops 300..303; heading to stop 302 (offset 250 s into the lap: 2 hops of 120 s + one 10 s dwell), due in 40 s at 06:30:00
    now = DAY + 6 * H + 30 * 60 - 40
    m = rt.match_trip(index, SERVICE_DATE, DAY, _bus(route="20", block=None), [("302", 40.0)], now)
    assert m is not None and m.trip.frequency
    assert m.start_time == "06:25:50"


def test_feeds_carry_trip_ids_from_the_static_feed():
    index = _two_trips()
    now = DAY + 7 * H + 60
    etas = {("7", "10"): [("101", 90.0), ("102", 250.0)]}
    positions, updates, stats = rt.build_realtime(
        index, SERVICE_DATE, DAY, [_bus(), _bus(route="99", vid="8")], etas, now)
    assert stats["vehicles"] == 1 and stats["matched"] == 1  # route 99 is not in the feed
    (vp,) = positions.entity
    assert vp.vehicle.trip.trip_id in index.trips and vp.vehicle.stop_id == "2"
    # the block's next trip (C -> A) also gets the bus's later ETAs when they are on the same route
    assert [e.trip_update.trip.trip_id[-3:] for e in updates.entity] == ["001"]
    tu = updates.entity[0].trip_update
    assert [u.stop_id for u in tu.stop_time_update] == ["2", "3"]
    assert positions.SerializeToString()


def test_alerts_target_what_the_feed_has():
    index = _two_trips()
    notices = [
        {"id": "a", "message": "Long text", "short_message": "Short", "target_scope": "all", "severity": "red"},
        {"id": "b", "message": "Stop closed", "target_scope": "stops", "target_stop_ids": ["2", "99999"]},
        {"id": "c", "message": "Detour", "target_scope": "route", "target_route_ids": ["10", "11"]},
        {"id": "d", "message": "Lot shuttle", "target_scope": "route", "target_route_ids": ["60"]},
        {"id": "e", "message": "Staff only", "auth_only": True},
    ]
    msg = rt.build_alerts(index, notices, ["1", "2", "3"], DAY)
    by_id = {e.id: e.alert for e in msg.entity}
    assert set(by_id) == {"notice-a", "notice-b", "notice-c"}
    assert by_id["notice-a"].informed_entity[0].agency_id == "UTS"
    assert by_id["notice-a"].header_text.translation[0].text == "Short"
    assert by_id["notice-a"].description_text.translation[0].text == "Long text"
    assert [e.stop_id for e in by_id["notice-b"].informed_entity] == ["2"]
    assert [e.route_id for e in by_id["notice-c"].informed_entity] == ["gold"]  # 10 and 11 are both Gold


def test_stop_times_strictly_increase_even_when_two_etas_tie():
    # Validator E022 on prod (Green [01], 10-19): two neighbouring stops given the same second
    index = _two_trips()
    now = DAY + 7 * H + 60
    etas = {("7", "10"): [("101", 90.2), ("102", 90.4), ("103", 300.0)]}
    _, updates, _ = rt.build_realtime(index, SERVICE_DATE, DAY, [_bus()], etas, now)
    times = []
    for e in updates.entity:
        for u in e.trip_update.stop_time_update:
            assert u.departure.time == u.arrival.time
            times.append(u.arrival.time)
    assert times == sorted(set(times)) and len(times) == len(set(times))
    assert int(now + 90) in times and int(now + 300) in times  # the tied stop C is the one left out
