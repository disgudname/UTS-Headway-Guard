import csv
import io
import sys
import zipfile
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import gtfs_feed as g  # noqa: E402


def _encode(points):
    """Google polyline encoder (the inverse of gtfs_feed.decode_polyline), for building test routes."""
    out, plat, plon = [], 0, 0
    for lat, lon in points:
        for v, prev in ((round(lat * 1e5), plat), (round(lon * 1e5), plon)):
            d = v - prev
            d = ~(d << 1) if d < 0 else d << 1
            while d >= 0x20:
                out.append(chr((0x20 | (d & 0x1F)) + 63))
                d >>= 5
            out.append(chr(d + 63))
        plat, plon = round(lat * 1e5), round(lon * 1e5)
    return "".join(out)


# A square loop, ~1.1 km a side: stops A (addr 1), B (2), C (3), D (4) at the corners, 120 s apart, 10 s dwell.
CORNERS = [(38.0, -78.5), (38.0, -78.4875), (38.01, -78.4875), (38.01, -78.5)]


def _route(rid, addrs, desc="Gold Line", first_rsid=100):
    by_addr = dict(zip([1, 2, 3, 4], CORNERS))
    stops = [
        {"RouteStopID": first_rsid + k, "AddressID": a, "Name": f"Street @ Stop {a}", "Latitude": by_addr[a][0],
         "Longitude": by_addr[a][1], "Order": k + 1, "SecondsToNextStop": 120, "SecondsAtStop": 10}
        for k, a in enumerate(addrs)
    ]
    line = [by_addr[a] for a in addrs] + [by_addr[addrs[0]]]
    return {"RouteID": rid, "Description": desc, "MapLineColor": "#ffdd00", "Stops": stops,
            "EncodedPolyline": _encode(line)}


CONFIG = {
    "agency": {"agency_id": "UTS", "agency_name": "UTS", "agency_url": "https://example.org/",
               "agency_timezone": "America/New_York"},
    "routes": [{"route_id": "gold", "name": "Gold Line", "transloc_ids": ["10", "11"]},
               {"route_id": "purple", "name": "Purple Line", "transloc_ids": ["20"]}],
    "purple": {"route_id": "purple", "patterns": [{"transloc_id": "20", "from_s": 0, "to_s": 86400}]},
    "headsigns": {"BB": "Bee"},
}
# Route 10 (day) has timestops AA (stop A) and BB (stop C); route 11 (evening) has BB (C) and EV (B).
TIMESTOPS = {"AA": {"10": "100"}, "BB": {"10": "102", "11": "202"}, "EV": {"11": "201"}}
LANDMARKS = {"DD": {"10": "103"}}
H = 3600


def _inputs(blocks, level=lambda d: "full", purple=None, days=7, known_until=None):
    return g.Inputs(
        routes_raw=[_route(10, [1, 2, 3, 4]), _route(11, [1, 2, 3, 4], first_rsid=200),
                    _route(20, [1, 2, 3, 4], "Purple Line", 300)],
        blocks=blocks, timestops=TIMESTOPS, landmarks=LANDMARKS,
        evening_pairs={"10": {"to": "11"}}, purple_headways=purple or {}, config=CONFIG, service_level=level,
        start=date(2026, 10, 19), days=days, known_until=known_until,  # a Monday
    )


def _block(stops, weekdays=(0, 1, 2, 3, 4), **extra):
    group = {"weekdays": list(weekdays), "stops": [[t, c] for t, c in stops], **extra}
    return {"route_ids": ["10", "11"], "weekday_groups": [group]}


def _times(tables, trip_id):
    return [(r["stop_id"], r["arrival_time"], r["departure_time"], r["timepoint"])
            for r in tables["stop_times.txt"] if r["trip_id"] == trip_id]


def test_each_pair_of_timestop_visits_is_a_trip_along_the_route():
    tables, warnings = g.build_feed(_inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB"), (7 * H + 1200, "AA")])}))
    assert not warnings
    trips = tables["trips.txt"]
    assert [t["trip_headsign"] for t in trips] == ["Bee", "Stop 1"]  # config name, else the name after "@"
    assert {t["block_id"] for t in trips} == {"09-daily_MTWRF"}
    # A -> B -> C: 120 s hops, 10 s dwell at B; the 600 s the sheet allows leaves a hold at C
    assert _times(tables, trips[0]["trip_id"]) == [
        ("1", "07:00:00", "07:00:00", 1), ("2", "07:02:00", "07:02:10", 0), ("3", "07:04:10", "07:04:10", 1),
    ]
    # C -> D -> A wraps round the loop
    assert [s for s, *_ in _times(tables, trips[1]["trip_id"])] == ["3", "4", "1"]


def test_a_leg_shorter_than_planned_is_squeezed_to_the_sheet():
    tables, _ = g.build_feed(_inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 125, "BB")])}))
    rows = _times(tables, tables["trips.txt"][0]["trip_id"])
    assert rows[-1][1] == "07:02:05"  # 250 s of planned running fitted into 125 s


def test_the_out_of_service_note_ends_the_day_at_its_cutoff():
    oos = {"leave_code": "BB", "leave_s": 7 * H + 600, "until_code": "AA", "last_code": "DD", "then": "lot"}
    tables, _ = g.build_feed(_inputs({"[09]": _block(
        [(7 * H, "AA"), (7 * H + 600, "BB"), (7 * H + 1200, "AA")], out_of_service=oos)}))
    trips = tables["trips.txt"]
    assert len(trips) == 2
    last = _times(tables, trips[1]["trip_id"])
    assert [s for s, *_ in last] == ["3", "4"]  # C on to the "as far as" stop D, nothing after
    assert last[-1][3] == 0  # not a timed stop


def test_the_evening_route_change_picks_the_post_6pm_route():
    rc = {"leave_code": "BB", "leave_s": 18 * H}
    tables, _ = g.build_feed(_inputs({"[09]": _block(
        [(17 * H + 1800, "AA"), (18 * H, "BB"), (18 * H + 900, "EV")], route_change=rc)}))
    shapes = [t["shape_id"] for t in tables["trips.txt"]]
    assert shapes[0].startswith("gold-10-") and shapes[1].startswith("gold-11-")


def test_a_change_no_single_route_serves_is_stitched_where_the_routes_meet():
    # AA is only on route 10, EV only on route 11: drive 10 from A to B (the first stop 11 also has), then 11
    rc = {"leave_code": "AA", "leave_s": 18 * H}
    tables, _ = g.build_feed(_inputs({"[09]": _block([(18 * H, "AA"), (18 * H + 900, "EV")], route_change=rc)}))
    trips = tables["trips.txt"]
    assert len(trips) == 1 and trips[0]["shape_id"].startswith("gold-10-")
    assert [s for s, *_ in _times(tables, trips[0]["trip_id"])] == ["1", "2"]


def test_calendar_follows_the_service_level_of_each_day():
    levels = {date(2026, 10, 20): "recess", date(2026, 10, 21): "none", date(2026, 10, 22): "health"}
    blocks = {
        "[09]": {"route_ids": ["10"], "has_recess": True, "weekday_groups": [
            {"weekdays": [0, 1, 2, 3, 4], "stops": [[7 * H, "AA"], [7 * H + 600, "BB"]]},
            {"weekdays": [0, 1], "service": "recess", "stops": [[8 * H, "AA"], [8 * H + 600, "BB"]]},
        ]},
        "[13]": _block([(7 * H, "AA"), (7 * H + 600, "BB")]),  # a route with no recess schedule
    }
    purple = {"weekdays": [0, 1, 2, 3, 4], "bands": [{"start_s": 6 * H, "end_s": 7 * H, "headway_s": 300}]}
    tables, _ = g.build_feed(_inputs(blocks, level=lambda d: levels.get(d, "full"), purple=purple, days=4))
    by_date = {}
    for r in tables["calendar_dates.txt"]:
        by_date.setdefault(r["date"], set()).add(r["service_id"])
    assert by_date["20261019"] == {"full_MTWRF", "daily_MTWRF", "purple_MTWRF"}
    assert by_date["20261020"] == {"recess_MT", "daily_MTWRF", "purple_MTWRF"}  # recess sheet, Silver-style regular
    assert "20261021" not in by_date  # no service at all
    assert by_date["20261022"] == {"purple_MTWRF"}  # Health Commuter: Purple only


def test_purple_runs_on_headways_not_times():
    purple = {"weekdays": [0, 1, 2, 3, 4], "bands": [{"start_s": 6 * H, "end_s": 7 * H, "headway_s": 180}]}
    tables, _ = g.build_feed(_inputs({}, purple=purple))
    (freq,) = tables["frequencies.txt"]
    assert freq["start_time"] == "06:00:00" and freq["headway_secs"] == 180 and freq["exact_times"] == 0
    rows = _times(tables, freq["trip_id"])
    assert [s for s, *_ in rows] == ["1", "2", "3", "4", "1"]  # one whole lap
    assert all(tp == 0 for *_, tp in rows)


def test_the_feed_stops_where_the_calendar_does():
    tables, _ = g.build_feed(_inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB")])}, days=60,
                                     known_until=date(2026, 10, 21)))
    assert tables["feed_info.txt"][0]["feed_end_date"] == "20261021"
    assert max(r["date"] for r in tables["calendar_dates.txt"]) == "20261021"


def test_shape_distances_grow_along_a_trip_that_wraps_the_loop():
    tables, _ = g.build_feed(_inputs({"[09]": _block([(7 * H, "BB"), (7 * H + 900, "BB")])}))
    rows = [r for r in tables["stop_times.txt"]]
    dists = [float(r["shape_dist_traveled"]) for r in rows]
    assert [r["stop_id"] for r in rows] == ["3", "4", "1", "2", "3"]
    assert dists == sorted(dists) and dists[0] == 0.0 and 4.0 < dists[-1] < 5.0  # a ~4.5 km lap
    shape = [r for r in tables["shapes.txt"] if r["shape_id"] == tables["trips.txt"][0]["shape_id"]]
    assert abs(float(shape[-1]["shape_dist_traveled"]) - dists[-1]) < 0.01


def test_zip_has_the_files_and_drops_empty_columns():
    tables, _ = g.build_feed(_inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB")])}))
    z = zipfile.ZipFile(io.BytesIO(g.write_zip(tables)))
    assert {"agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar_dates.txt",
            "shapes.txt", "feed_info.txt"} <= set(z.namelist())
    assert "frequencies.txt" not in z.namelist()  # no Purple here
    routes = list(csv.DictReader(io.StringIO(z.read("routes.txt").decode())))
    assert [r["route_id"] for r in routes] == ["gold"]  # Purple has no trips, so no route
    assert "route_short_name" in routes[0]
    assert "agency_phone" not in z.read("agency.txt").decode()


def test_gtfs_time_runs_past_midnight():
    assert g.gtfs_time(26 * H + 61) == "26:01:01"


def test_a_timestop_override_moves_where_the_feed_starts_a_block():
    # Gold's "CSW" starts really begin at Chemistry: the sheet's CSW 07:30 -> BAR 07:40 becomes a timed trip from there.
    # Here: route 10 has no "YD" timestop, the override puts it at stop B (RouteStopID 101).
    import copy
    inp = _inputs({"[09]": _block([(7 * H + 1800, "YD"), (7 * H + 2400, "BB")])})
    tables, _ = g.build_feed(inp)
    assert tables["trips.txt"] == []  # without the override it is a deadhead
    inp.config = copy.deepcopy(CONFIG)
    inp.config["timestop_overrides"] = {"10": {"YD": "101"}}
    tables, _ = g.build_feed(inp)
    (trip,) = tables["trips.txt"]
    assert _times(tables, trip["trip_id"]) == [("2", "07:30:00", "07:30:00", 1), ("3", "07:32:00", "07:32:00", 1)]


def test_translocs_name_counters_are_dropped():
    inp = _inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB")])})
    inp.routes_raw[0]["Stops"][0]["Name"] = "McCormick Rd @ Chemistry Building (2)"
    tables, _ = g.build_feed(inp)
    assert {s["stop_id"]: s["stop_name"] for s in tables["stops.txt"]}["1"] == "McCormick Rd @ Chemistry Building"


def test_stop_names_come_from_translocs_current_description():
    # TransLoc's Name went stale when Whitehead Rd became Carl Smith Way; Description has the current name
    inp = _inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 600, "BB")])})
    stop = inp.routes_raw[0]["Stops"][0]
    stop["Name"], stop["Description"] = "Whitehead Rd @ Scott Stadium", "Carl Smith Way @ Scott Stadium"
    tables, _ = g.build_feed(inp)
    assert {s["stop_id"]: s["stop_name"] for s in tables["stops.txt"]}["1"] == "Carl Smith Way @ Scott Stadium"


def test_headsign_zones_give_per_stop_headsigns():
    # "next area the bus is heading to": a zone starts at its stop and runs round the loop to the next zone
    import copy
    inp = _inputs({"[09]": _block([(7 * H, "AA"), (7 * H + 900, "AA")])})  # a full lap A -> B -> C -> D -> A
    inp.config = copy.deepcopy(CONFIG)
    inp.config["headsign_zones"] = {"10": [["1", "North"], ["3", "South"]]}
    tables, warnings = g.build_feed(inp)
    assert not warnings
    (trip,) = tables["trips.txt"]
    assert trip["trip_headsign"] == "North"
    rows = [(r["stop_id"], r["stop_headsign"]) for r in tables["stop_times.txt"]]
    # A, B in North (same as the trip: left blank); C, D in South; the final A keeps D's (the bus is arriving)
    assert rows == [("1", ""), ("2", ""), ("3", "South"), ("4", "South"), ("1", "South")]
