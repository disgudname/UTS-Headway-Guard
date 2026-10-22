import asyncio
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app


def _transloc_entry(route_id, route_stop_id, name, seconds):
    return {
        "RouteId": route_id, "RouteStopId": route_stop_id, "RouteDescription": name, "StopId": 113,
        "Color": "#ff7300", "Times": [{"Seconds": s, "Text": "", "IsArriving": False} for s in seconds],
    }


def _run(data, stop_ids, ours, routes_raw, monkeypatch):
    async def fake_eta():
        return {"arrivals": ours}

    monkeypatch.setattr(app, "eta_uts_stop_arrivals", fake_eta)
    monkeypatch.setattr(app.state, "routes_raw", routes_raw, raising=False)
    return asyncio.run(app._swap_in_our_etas(data, stop_ids))


def test_ours_replace_transloc_per_route_with_fallback(monkeypatch):
    data = [
        _transloc_entry(53, 675, "Orange Line", [315, 734]),
        _transloc_entry(68, 914, "Green Line", [335]),
        _transloc_entry(90, 999, "Purple Lots Shuttle", [60]),
        {"RouteDescription": "7", "Color": "#000", "Times": [{"Seconds": 120}]},  # CAT
    ]
    ours = [
        {"RouteId": "53", "RouteStopId": "675", "Times": [{"VehicleId": "24", "Seconds": 400.4}]},
        {"RouteId": "90", "RouteStopId": "999", "Times": [{"VehicleId": "9", "Seconds": 900.0}]},
    ]
    out = _run(data, ["113"], ours, [], monkeypatch)
    assert out[0]["Times"][0]["Seconds"] == 400 and out[0]["EtaSource"] == "ours"
    assert [t["Seconds"] for t in out[1]["Times"]] == [335] and out[1]["EtaSource"] == "transloc"
    assert out[2]["Times"][0]["Seconds"] == 60 and out[2]["EtaSource"] == "transloc"  # shuttle stays
    assert "EtaSource" not in out[3]


def test_route_transloc_left_out_is_added(monkeypatch):
    routes_raw = [{
        "RouteID": 67, "Description": "Gold Line", "MapLineColor": "#e5b800",
        "Stops": [{"StopID": 113, "RouteStopID": 875, "Description": "Madison Ave @ Preston Ave"},
                  {"StopID": 5, "RouteStopID": 876, "Description": "Elsewhere"}],
    }]
    ours = [
        {"RouteId": "67", "RouteStopId": "875", "Times": [{"VehicleId": "63", "Seconds": 105.8}]},
        {"RouteId": "67", "RouteStopId": "876", "Times": [{"VehicleId": "63", "Seconds": 200.0}]},
    ]
    out = _run([], ["113"], ours, routes_raw, monkeypatch)
    assert len(out) == 1
    assert out[0]["RouteDescription"] == "Gold Line"
    assert out[0]["Times"][0]["Seconds"] == 106
    assert out[0]["EtaSource"] == "ours"
