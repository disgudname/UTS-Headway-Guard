import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app  # noqa: E402
import uts_blocks  # noqa: E402
from app import Route, Vehicle, _recent_track_heading, _schedule_pass_hint, cumulative_distance, project_vehicle_to_route  # noqa: E402

NY = ZoneInfo("America/New_York")

# An out-and-back road, the shape of Gold on Massie Rd: north for ~1.1 km, then back south on the same line.
POLY = [(0.0, 0.0), (0.005, 0.0), (0.01, 0.0), (0.005, 0.0), (0.0, 0.0)]


def _route():
    cum, length = cumulative_distance(POLY)
    return Route(id=67, name="test", encoded="", poly=POLY, cum=cum, length_m=length)


def test_recent_track_heading_from_a_moving_bus():
    track = [(0.0, 0.0010, 0.0), (15.0, 0.0012, 0.0), (30.0, 0.0015, 0.0)]
    assert abs(_recent_track_heading(track, 0.0015, 0.0)) < 1.0 or _recent_track_heading(track, 0.0015, 0.0) > 359.0


def test_recent_track_heading_none_for_a_bus_that_has_not_moved():
    track = [(0.0, 0.0010, 0.0), (15.0, 0.0010, 0.00001)]
    assert _recent_track_heading(track, 0.0010, 0.0) is None


def test_recent_track_heading_keeps_the_direction_a_parked_bus_came_in_on():
    # Drove south, then sat: the heading is still south.
    track = [(0.0, 0.0030, 0.0), (15.0, 0.0020, 0.0), (30.0, 0.0020, 0.0), (45.0, 0.0020, 0.0)]
    h = _recent_track_heading(track, 0.0020, 0.0)
    assert 170.0 < h < 190.0


def test_first_poll_heading_picks_the_pass_the_bus_is_driving():
    # Regression, 2026-10-12 05:00: Gold [11] bus 58 joined route 67 at JPJ South Lot heading south and was placed
    # on the northbound pass (a cached overnight heading), so the next stop read 554 s instead of 45 s.
    route = _route()
    veh = Vehicle(id=58, name="58", lat=0.0025, lon=0.0, ts_ms=0, ground_mps=10.0, age_s=0)
    south = _recent_track_heading([(0.0, 0.0030, 0.0)], 0.0025, 0.0)
    north = _recent_track_heading([(0.0, 0.0020, 0.0)], 0.0025, 0.0)
    s_south, _ = project_vehicle_to_route(veh, route, None, south, None)
    s_north, _ = project_vehicle_to_route(veh, route, None, north, None)
    half = route.length_m / 2
    assert s_south > half
    assert s_north < half


def test_schedule_hint_picks_the_pass_that_makes_the_next_timestop(monkeypatch):
    # Regression, 2026-10-13 05:00: Gold [09] bus 30 parked overnight, no heading. Its next timestop is up the
    # northbound pass, due soon, so the northbound pass wins; due a long time out, the far pass would do as well.
    route = _route()
    now = 1_000_000.0
    stop_s = route.length_m * 0.45  # near the top of the northbound pass
    monkeypatch.setattr(uts_blocks, "is_loaded", lambda: True)
    monkeypatch.setattr(uts_blocks, "next_block_timestop", lambda rid, block, ts: (now + 120.0, "889"))
    monkeypatch.setitem(app._bus_eta_stop_arcs, "67", {"889": stop_s})
    s = _schedule_pass_hint(route, "[09]", 0.0025, 0.0, now)
    assert s is not None and s < route.length_m / 2


def test_schedule_hint_needs_a_block():
    assert _schedule_pass_hint(_route(), None, 0.0025, 0.0, 1_000_000.0) is None


def test_schedule_hint_none_off_a_shared_road(monkeypatch):
    poly = [(0.0, 0.0), (0.0, 0.01)]
    cum, length = cumulative_distance(poly)
    route = Route(id=67, name="test", encoded="", poly=poly, cum=cum, length_m=length)
    monkeypatch.setattr(uts_blocks, "is_loaded", lambda: True)
    assert _schedule_pass_hint(route, "[09]", 0.00005, 0.005, 1_000_000.0) is None


def test_next_block_timestop_on_the_real_sheet():
    # [09] weekday: CSW 05:00 (not a mapped timestop on 67), then BAR 05:10 (Barracks, 889 on route 67).
    uts_blocks._load()
    ts = datetime(2026, 10, 13, 5, 1, 31, tzinfo=NY).timestamp()
    epoch, stop_id = uts_blocks.next_block_timestop("67", "[09]", ts)
    assert stop_id == "889"
    assert datetime.fromtimestamp(epoch, NY).strftime("%H:%M") == "05:10"


def test_gps_hop_of_a_stopped_bus_does_not_flip_the_pass():
    # Regression, 2026-10-11 07:56 and 10-15 18:09: Gold buses sitting at McCormick Rd @ UVA Chapel. The fix hopped
    # ~15 m back down the road at 2-3 mph; this poll's step pointed at the other pass, which won, and everything
    # ahead read 8-15 min late. The heading now comes from the recent track, which still says the bus came north.
    route = _route()
    at_stop = 0.0025
    hop = at_stop - 15.0 / 111_000.0
    prev_s, prev_idx = project_vehicle_to_route(
        Vehicle(id=2, name="2", lat=at_stop, lon=0.0, ts_ms=0, ground_mps=0.0, age_s=0), route, None, 0.0, None)
    assert prev_s < route.length_m / 2
    veh = Vehicle(id=2, name="2", lat=hop, lon=0.0, ts_ms=0, ground_mps=1.3, age_s=0)
    step = app.bearing_between((at_stop, 0.0), (hop, 0.0))
    s_old, _ = project_vehicle_to_route(veh, route, prev_idx, step, prev_s)
    assert s_old > route.length_m / 2  # the old rule: one hop flips it
    track = [(0.0, 0.0015, 0.0), (15.0, 0.0020, 0.0), (30.0, at_stop, 0.0), (180.0, hop, 0.0)]
    s_new, _ = project_vehicle_to_route(veh, route, prev_idx, _recent_track_heading(track, hop, 0.0), prev_s)
    assert s_new < route.length_m / 2
