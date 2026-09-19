import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app


class _Route:
    pass


def _out_and_back_route(offset_m=0.0):
    """Runs 1km east along y=0, then back west along y=offset_m (north of it) --
    two passes of "the same road". offset_m=0 retraces it exactly; ~15 is a divided
    road's two carriageways."""
    deg_lon = 1.0 / 111_320.0
    deg_lat = 1.0 / 110_540.0
    east = [(0.0, i * 100.0 * deg_lon) for i in range(11)]
    west = [(offset_m * deg_lat, (1000.0 - i * 100.0) * deg_lon) for i in range(11)]
    poly = east + west
    r = _Route()
    r.poly = poly
    r.cum = app.cumulative_distance(poly)[0]
    return r


def _vehicle(lat, lon):
    return app.Vehicle(id=1, name="1", lat=lat, lon=lon, ts_ms=0, ground_mps=5.0, age_s=1.0, heading=0.0)


def _project(route, lat, lon, heading):
    return app.project_vehicle_to_route(_vehicle(lat, lon), route, None, heading)


def test_exactly_retraced_road_picks_the_pass_matching_heading():
    r = _out_and_back_route(0.0)
    lat, lon = 0.0, 500.0 / 111_320.0
    s_out, _ = _project(r, lat, lon, 90.0)     # heading east -> outbound pass
    s_back, _ = _project(r, lat, lon, 270.0)   # heading west -> return pass
    assert abs(s_out - 500.0) < 5.0
    assert s_back > 1000.0


def test_divided_road_gps_jitter_cannot_flip_the_pass():
    # Vehicle is driving east on the outbound carriageway but its GPS reads ~8m
    # over toward the return carriageway (which is 15m away) -- plain
    # nearest-point would pick the return pass; heading must not let it.
    r = _out_and_back_route(15.0)
    lat, lon = 8.0 / 110_540.0, 500.0 / 111_320.0
    s, _ = _project(r, lat, lon, 90.0)
    assert abs(s - 500.0) < 5.0


def test_far_closer_segment_still_wins_over_heading():
    # A vehicle nowhere near the return pass keeps its outbound match even if its
    # heading reads oddly (e.g. stale after a stop).
    r = _out_and_back_route(200.0)
    s, _ = _project(r, 0.0, 500.0 / 111_320.0, 270.0)
    assert abs(s - 500.0) < 5.0


def test_continuity_keeps_a_vehicle_on_its_pass_when_heading_is_ambiguous():
    # Bus rounding a bend: heading (from a poll or two of GPS movement) reads ~90 deg
    # off BOTH passes. Without continuity the projection is a coin flip on noise;
    # with last poll's position it must stay put rather than teleport to the other
    # pass ~1km of route away.
    r = _out_and_back_route(0.0)
    lat, lon = 0.0, 500.0 / 111_320.0
    s_stay_out, _ = app.project_vehicle_to_route(_vehicle(lat, lon), r, None, 0.0, 480.0)
    s_stay_back, _ = app.project_vehicle_to_route(_vehicle(lat, lon), r, None, 0.0, 1520.0)
    assert abs(s_stay_out - 500.0) < 5.0
    assert s_stay_back > 1000.0


def test_clear_opposite_heading_can_still_override_continuity():
    # Continuity is a soft nudge, not a lock: a vehicle stuck on the wrong pass must
    # still be able to correct itself when its heading is unmistakably the other way.
    r = _out_and_back_route(0.0)
    lat, lon = 0.0, 500.0 / 111_320.0
    s, _ = app.project_vehicle_to_route(_vehicle(lat, lon), r, None, 90.0, 1500.0)  # thinks it's on return, heading east
    assert abs(s - 500.0) < 5.0
