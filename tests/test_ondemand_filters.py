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


def test_virtual_stops_ignore_pending_status_without_pending_ids():
    schedules = _build_schedule_with_pending("pending")
    stops = build_ondemand_virtual_stops(schedules, datetime.now(timezone.utc))
    assert stops == []


def test_stop_plans_ignore_pending_status_without_pending_ids():
    schedules = _build_schedule_with_pending("Pending driver accept")
    plans = build_ondemand_vehicle_stop_plans(schedules)
    assert plans == {}
