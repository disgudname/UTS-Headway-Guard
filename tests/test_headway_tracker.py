import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from headway_tracker import HeadwayTracker, VehicleSnapshot


class MemoryHeadwayStorage:
    def __init__(self):
        self.events = []

    def write_events(self, events):
        self.events.extend(events)

    def query_events(self, *args, **kwargs):
        return []


def _basic_stop():
    return {
        "StopID": "STOP",
        "Latitude": 0.0,
        "Longitude": 0.0,
        "RouteID": "R1",
        "ApproachSets": [
            {
                "name": "main",
                "bubbles": [
                    {"lat": 0.0, "lng": -0.0006, "radius_m": 70.0, "order": 1},
                    {"lat": 0.0, "lng": 0.0, "radius_m": 30.0, "order": 2},
                ],
            }
        ],
    }


def test_arrival_logged_when_bus_passes_through_bubbles_without_stopping():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_basic_stop()])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=-0.0010, route_id="R1", timestamp=base)]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=-0.0006,
                route_id="R1",
                timestamp=base + timedelta(seconds=10),
            )
        ]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=20),
            )
        ]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0004,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
            )
        ]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.001,
                route_id="R1",
                timestamp=base + timedelta(seconds=40),
            )
        ]
    )

    # For pass-through arrivals (Method 2), arrival and departure are logged
    # at the same time (when bus exits final bubble), so dwell is 0
    assert [e.event_type for e in storage.events] == ["arrival", "departure"]
    assert storage.events[0].timestamp == base + timedelta(seconds=30)  # arrival at exit
    assert storage.events[1].timestamp == base + timedelta(seconds=30)  # departure at exit
    assert storage.events[1].dwell_seconds == 0


def test_arrival_logged_when_bus_stops_in_final_bubble():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_basic_stop()])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=-0.0006, route_id="R1", timestamp=base)]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=20),
            )
        ]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=40),
            )
        ]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0005,
                route_id="R1",
                timestamp=base + timedelta(seconds=70),
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival", "departure"]
    assert storage.events[0].timestamp == base + timedelta(seconds=40)
    assert storage.events[1].timestamp == base + timedelta(seconds=70)
    assert storage.events[1].dwell_seconds == 30


def test_no_arrival_when_skipping_outer_bubble():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_basic_stop()])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=0.00025, route_id="R1", timestamp=base)]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0006,
                route_id="R1",
                timestamp=base + timedelta(seconds=20),
            )
        ]
    )

    assert storage.events == []


def test_route_mismatch_prevents_headway_logging():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    stop = _basic_stop()
    stop["RouteID"] = "R2"
    tracker.update_stops([stop])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=-0.0010, route_id="R1", timestamp=base)]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=-0.0006,
                route_id="R1",
                timestamp=base + timedelta(seconds=20),
            )
        ]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=40),
            )
        ]
    )

    assert storage.events == []
