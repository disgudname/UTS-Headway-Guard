import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import anti_bunching_log as abl

NY = ZoneInfo("America/New_York")
NOW = datetime(2026, 10, 19, 9, 0, tzinfo=NY).timestamp()


def payload(adj=34, sev="Green", assigned=3):
    vehicles = [] if not assigned else [
        {"Adjustment": adj, "Severity": sev, "TimeStampUTC": "/Date(1792380738977-0600)/", "VehicleID": 26, "VehicleName": "14532"},
        {"Adjustment": 0, "Severity": "Green", "TimeStampUTC": "/Date(1792380738977-0600)/", "VehicleID": 12, "VehicleName": "18532"},
    ]
    return [
        {"AssignedVehicles": 2, "LoopSeconds": 1819, "RouteDescription": "Gold Line", "RouteID": 57, "TargetGap": 909,
         "TimeStampUTC": "/Date(1792380738960-0600)/", "VehicleAntiBunching": []},
        {"AssignedVehicles": assigned, "LoopSeconds": 1500, "RouteDescription": "Purple Line", "RouteID": 74,
         "TargetGap": 500 if assigned else 0, "TimeStampUTC": "/Date(1792380738960-0600)/", "VehicleAntiBunching": vehicles},
    ]


def test_transloc_epoch():
    assert abl.transloc_epoch("/Date(1792380738960-0600)/") == 1792380738.96
    assert abl.transloc_epoch(None) is None


def test_logs_only_matching_routes_and_only_changes(tmp_path: Path):
    log = abl.AntiBunchingLog(tmp_path)
    assert log.record(payload(), NOW) == 1
    assert log.record(payload(), NOW + 15) == 0          # nothing changed
    assert log.record(payload(adj=90, sev="Red"), NOW + 30) == 1
    rows = log.read("2026-10-19")
    assert [r["route_id"] for r in rows] == ["74", "74"]
    assert rows[0]["target_gap_s"] == 500 and rows[0]["vehicles"][0] == ["12", "18532", 0, "Green"]
    assert rows[1]["vehicles"][1] == ["26", "14532", 90, "Red"]
    assert log.days() == ["2026-10-19"]


def test_route_going_out_of_service_logs_once(tmp_path: Path):
    log = abl.AntiBunchingLog(tmp_path)
    log.record(payload(), NOW)
    assert log.record(payload(assigned=0), NOW + 15) == 1
    assert log.record(payload(assigned=0), NOW + 30) == 0
    fresh = abl.AntiBunchingLog(tmp_path / "other")
    assert fresh.record(payload(assigned=0), NOW) == 0     # never running today: nothing


def test_prunes_old_days(tmp_path: Path):
    (tmp_path / "2026-01-01.jsonl").write_text(json.dumps({"t": 0}) + "\n", encoding="utf-8")
    abl.AntiBunchingLog(tmp_path).record(payload(), NOW)
    assert not (tmp_path / "2026-01-01.jsonl").exists()
