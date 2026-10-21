import json
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import onboard_log  # noqa: E402
import onboard_score  # noqa: E402

NY = ZoneInfo("America/New_York")


def ts(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=NY).timestamp()


VEH = SimpleNamespace(id=4021, lat=38.03, lon=-78.51, ground_mps=4.24, age_s=3.0)
OUT = {
    "bus": "18232", "status": "in_service", "time": 0,
    "route": {"id": "55", "name": "Gold Line"}, "block": "[07]",
    "position": {"lat": 38.03, "lon": -78.51, "source": "transloc", "device_offset_m": 640},
    "at_stop": {"id": "101", "name": "Newcomb", "seconds": 0.0, "stopped": True},
    "stops": [{"id": "102", "seconds": 61.23}, {"id": "103", "seconds": None}],
}


def test_service_day_keeps_a_night_together():
    assert onboard_log.service_day(ts(2026, 10, 21, 23, 30)) == "2026-10-21"
    assert onboard_log.service_day(ts(2026, 10, 22, 2, 30)) == "2026-10-21"
    assert onboard_log.service_day(ts(2026, 10, 22, 4, 30)) == "2026-10-22"


def test_build_record():
    rec = onboard_log.build_record(OUT, 1000.04, VEH, {"lat": 38.1, "lon": -78.5, "heading": 90.4, "speed": None})
    assert rec["veh"] == "4021" and rec["tl"] == [38.03, -78.51, 4.2, 3.0]
    assert rec["dev"] == [38.1, -78.5, 90.0, None]
    assert rec["src"] == "transloc" and rec["off_m"] == 640
    assert rec["route"] == "55" and rec["block"] == "[07]"
    assert rec["at"] == ["101", True]
    assert rec["eta"] == [["102", 61.23], ["103", None]]


def test_build_record_not_in_service_and_no_device():
    rec = onboard_log.build_record({"bus": "99999", "status": "unknown_bus", "position": None}, 5.0, None,
                                   {"lat": None, "lon": None, "heading": None, "speed": None})
    assert rec == {"t": 5.0, "bus": "99999", "status": "unknown_bus"}


def test_record_throttles_per_bus_and_writes_by_service_day(tmp_path):
    log = onboard_log.OnboardLog(tmp_path)
    t = ts(2026, 10, 22, 1, 0)
    assert log.record({"t": t, "bus": "18232"})
    assert not log.record({"t": t + 2, "bus": "18232"})  # a second tab
    assert log.record({"t": t + 2, "bus": "19232"})
    assert log.record({"t": t + 5, "bus": "18232"})
    rows = log.read("2026-10-21")
    assert [(r["bus"], r["t"]) for r in rows] == [("18232", t), ("19232", t + 2), ("18232", t + 5)]


def test_prune_drops_old_days(tmp_path):
    (tmp_path / "2026-01-01.jsonl").write_text("{}\n")
    log = onboard_log.OnboardLog(tmp_path)
    log.record({"t": ts(2026, 10, 21, 12), "bus": "1"})
    assert not (tmp_path / "2026-01-01.jsonl").exists()


# --- scorer, on a straight 2 km line with stops every 500 m --------------------------------------
def straight_graph():
    lat0, lon0 = 38.0, -78.5
    pts = [(lat0 + i * 0.0009, lon0) for i in range(21)]  # ~100 m apart, ~2 km
    cum = [i * 99.5 for i in range(21)]
    stops = {("55", str(100 + k)): (lat0 + k * 5 * 0.0009, lon0, f"Stop {k}", cum[k * 5]) for k in range(1, 4)}
    return stops, {"55": (pts, cum)}


def drive(stops, eta_bias_s=0.0, now_at_stop="102"):
    """A bus driving north at 10 m/s, polled every 5 s, given ETAs off by eta_bias_s."""
    recs = []
    for i in range(0, 200, 5):
        s = 10.0 * i
        lat = 38.0 + s / 99.5 * 0.0009
        etas = [[sid, max(0.0, (arc - s) / 10.0) + eta_bias_s] for (_r, sid), (_a, _b, _n, arc) in sorted(stops.items()) if arc > s]
        rec = {"t": 1000.0 + i, "bus": "18232", "status": "in_service", "veh": "4021", "route": "55",
               "tl": [lat, -78.5, 10.0, 1.0], "src": "transloc", "eta": etas}
        sid = next((sid for (_r, sid), v in stops.items() if abs(v[3] - s) < 20), None)
        if sid == now_at_stop:
            rec["at"] = [sid, True]
        recs.append(rec)
    return recs


def test_scorer_perfect_etas_score_zero():
    stops, lines = straight_graph()
    recs = drive(stops)
    polls = onboard_score.to_polls(recs)["18232"]
    rows = [r for r in onboard_score.eta_compare.score_rows(polls, stops, lines) if r["ours"] is not None]
    assert rows and max(abs(r["ours"]) for r in rows) < 15


def test_scorer_late_etas_show_positive():
    stops, lines = straight_graph()
    polls = onboard_score.to_polls(drive(stops, eta_bias_s=120.0))["18232"]
    rows = [r for r in onboard_score.eta_compare.score_rows(polls, stops, lines) if r["ours"] is not None]
    assert rows and min(r["ours"] for r in rows) > 90


def test_now_at_matches_the_pass():
    stops, lines = straight_graph()
    checks = onboard_score.now_at_check(drive(stops), stops, lines)
    assert checks and all(c[4] for c in checks)


def test_now_at_flags_a_stop_the_bus_never_passed():
    stops, lines = straight_graph()
    recs = drive(stops)
    recs[3]["at"] = ["999", True]
    checks = onboard_score.now_at_check(recs, stops, lines)
    assert [c[2] for c in checks if not c[4]] == ["999"]


def test_report_runs():
    stops, lines = straight_graph()
    text = onboard_score.report("2026-10-21", drive(stops), stops, lines, "18232")
    assert "Bus 18232" in text and "ETAs:" in text
    json.dumps(text)


def test_endpoint_logs_live_answers_only(tmp_path, monkeypatch):
    import asyncio
    import app

    async def fake_state(bus, at, lat, lon, heading, speed):
        return dict(OUT, bus=bus, time=int(ts(2026, 10, 21, 20) * 1000)), VEH

    monkeypatch.setattr(app, "_onboard_state", fake_state)
    monkeypatch.setattr(app, "ONBOARD_LOG", onboard_log.OnboardLog(tmp_path))
    out = asyncio.run(app.onboard_state(bus=" 18232 ", at=None, lat=38.1, lon=-78.5, heading=None, speed=None))
    assert out["bus"] == "18232"
    asyncio.run(app.onboard_state(bus="18232", at=123, lat=None, lon=None, heading=None, speed=None))
    rows = app.ONBOARD_LOG.read("2026-10-21")
    assert len(rows) == 1 and rows[0]["dev"][:2] == [38.1, -78.5] and rows[0]["eta"][0] == ["102", 61.23]


def test_record_extra_tags_bus_drops_junk_and_caps_size(tmp_path, monkeypatch):
    log = onboard_log.OnboardLog(tmp_path)
    t = ts(2026, 10, 21, 20)
    n = log.record_extra("events", "18232", [{"t": t, "kind": "this-is"}, "junk", {"x": "y" * 2000}], t)
    assert n == 1 and log.read("2026-10-21", "events") == [{"t": t, "kind": "this-is", "bus": "18232"}]
    monkeypatch.setattr(onboard_log, "MAX_EXTRA_FILE_BYTES", 10)
    assert log.record_extra("events", "18232", [{"t": t}], t) == 0


def test_prune_drops_sensor_files_sooner(tmp_path):
    for name in ("2026-09-01.sensors.jsonl", "2026-09-01.events.jsonl", "2026-09-01.jsonl"):
        (tmp_path / name).write_text("{}\n")
    onboard_log.OnboardLog(tmp_path).prune(ts(2026, 10, 21, 12))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["2026-09-01.events.jsonl", "2026-09-01.jsonl"]


def test_telemetry_endpoint(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import app

    monkeypatch.setattr(app, "ONBOARD_LOG", onboard_log.OnboardLog(tmp_path))
    client = TestClient(app.app)
    r = client.post("/v1/onboard/telemetry", json={"bus": "18232", "events": [{"t": 1, "kind": "stopped"}],
                                                   "sensors": [{"t": 1, "sd": 0.02}]})
    assert r.status_code == 200 and r.json()["written"] == {"events": 1, "sensors": 1}
    assert client.post("/v1/onboard/telemetry", json={"events": []}).status_code == 400
    assert client.post("/v1/onboard/telemetry", content=b"x" * (600 * 1024)).status_code == 413


def sensors_for(recs, stand_at=None, stand_s=0):
    """1 Hz tablet GPS along the drive() path; optionally standing still at arc metres `stand_at` for stand_s."""
    out, t, s = [], 1000.0, 0.0
    while s < 1950:
        speed = 10.0
        if stand_at is not None and stand_at <= s < stand_at + 1 and stand_s > 0:
            speed, stand_s = 0.0, stand_s - 1
        out.append({"t": t, "bus": "18232", "lat": 38.0 + s / 99.5 * 0.0009, "lon": -78.5, "acc": 5, "spd": speed,
                    "n": 60, "sd": 0.02 if speed == 0 else 0.4})
        s += speed
        t += 1
    return out


def test_announcement_timing():
    stops, lines = straight_graph()
    recs = drive(stops)
    # Stop 102 sits at arc 995 m: reached (30 m short) at t=1096.5 with no standing. Stand there 10 s instead.
    sens = sensors_for(recs, stand_at=990, stand_s=10)
    recs = [dict(r, t=r["t"]) for r in recs] + [dict(recs[-1], t=1300.0)]
    tracks = onboard_score.device_tracks(sens, recs)
    arc = stops[("55", "102")][3]
    reach = 1000.0 + (arc - 30) / 10.0
    events = [{"t": reach - 8, "kind": "this-is", "route": "55", "stop": "102", "d": 110, "src": "local"},
              {"t": 1101.0, "kind": "stopped", "route": "55", "stop": "102"},
              {"t": 1102.0, "kind": "next-stop", "route": "55", "stop": "103", "from": "102", "stopped": True}]
    events = [dict(e, bus="18232") for e in events]
    chk = onboard_score.announcement_check(events, tracks, stops, lines)
    assert abs(chk["this_is"][0][3] - (-8)) < 2
    assert chk["stopped"][0][3] is not None and 0 <= chk["stopped"][0][3] <= 2
    assert chk["next_stop"][0][4] >= 0
    still, moving = onboard_score.motion_thresholds(sens)
    assert still and moving and max(still) < min(moving)
    text = onboard_score.report("2026-10-21", recs, stops, lines, "18232", events, sens)
    assert "'This is X'" in text and "motion sd" in text
