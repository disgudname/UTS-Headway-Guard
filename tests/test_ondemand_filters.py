import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import build_ondemand_vehicle_stop_plans, build_ondemand_virtual_stops


def _build_schedule_with_pending(status: str):
    now = datetime.now(timezone.utc)
    return [
        {
            "vehicle_id": "veh-1",
            "stops": [
                {
                    "timestamp": now.isoformat(),
                    "position": {"latitude": 38.0, "longitude": -78.0},
                    "address": "123 Main St",
                    "rides": [
                        {
                            "ride_id": "ride-1",
                            "status": status,
                            "stop_type": "pickup",
                            "rider": {"first_name": "Taylor", "last_name": "Swift"},
                        }
                    ],
                }
            ],
        }
    ]


def test_virtual_stops_include_pending_status():
    schedules = _build_schedule_with_pending("pending")
    stops = build_ondemand_virtual_stops(schedules, datetime.now(timezone.utc))
    assert len(stops) == 1
    stop = stops[0]
    assert stop.get("rideStatus") == "pending"
    assert stop.get("rideId") == "ride-1"


def test_stop_plans_include_pending_status():
    schedules = _build_schedule_with_pending("Pending driver accept")
    plans = build_ondemand_vehicle_stop_plans(schedules)
    assert "veh-1" in plans
    entries = plans["veh-1"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry.get("rideStatus") == "Pending driver accept"
    assert entry.get("rideId") == "ride-1"
    ride_details = entry.get("rides")
    assert isinstance(ride_details, list) and len(ride_details) == 1
    assert ride_details[0].get("rideStatus") == "Pending driver accept"
    assert ride_details[0].get("rideId") == "ride-1"


def test_virtual_stops_include_ride_status_and_id():
    schedules = _build_schedule_with_pending("accepted")
    stops = build_ondemand_virtual_stops(schedules, datetime.now(timezone.utc))
    assert len(stops) == 1
    stop = stops[0]
    assert stop.get("rideStatus") == "accepted"
    assert stop.get("rideId") == "ride-1"


def test_stop_plans_include_ride_status_and_id():
    schedules = _build_schedule_with_pending("accepted")
    plans = build_ondemand_vehicle_stop_plans(schedules)
    assert "veh-1" in plans
    entries = plans["veh-1"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry.get("rideStatus") == "accepted"
    assert entry.get("rideId") == "ride-1"
    ride_details = entry.get("rides")
    assert isinstance(ride_details, list) and len(ride_details) == 1
    assert ride_details[0].get("rideStatus") == "accepted"
    assert ride_details[0].get("rideId") == "ride-1"


def test_status_falls_back_to_status_map_when_missing_from_schedule():
    schedules = _build_schedule_with_pending("")
    status_map = {"ride-1": "in_progress"}
    stops = build_ondemand_virtual_stops(
        schedules, datetime.now(timezone.utc), ride_status_map=status_map
    )
    assert len(stops) == 1
    stop = stops[0]
    assert stop.get("rideStatus") == "in_progress"


def test_status_map_overrides_schedule_value():
    schedules = _build_schedule_with_pending("pending")
    status_map = {"ride-1": "complete"}

    stops = build_ondemand_virtual_stops(
        schedules, datetime.now(timezone.utc), ride_status_map=status_map
    )
    assert len(stops) == 1
    stop = stops[0]
    assert stop.get("rideStatus") == "complete"

    plans = build_ondemand_vehicle_stop_plans(
        schedules, ride_status_map=status_map
    )
    assert "veh-1" in plans
    entries = plans["veh-1"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry.get("rideStatus") == "complete"
    assert entry.get("rideId") == "ride-1"
    ride_details = entry.get("rides")
    assert isinstance(ride_details, list) and len(ride_details) == 1
    assert ride_details[0].get("rideStatus") == "complete"
