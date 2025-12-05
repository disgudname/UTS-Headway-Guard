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


def test_headway_tracker_ignores_jitter_near_departure_boundary():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage, arrival_distance_threshold_m=30.0, departure_distance_threshold_m=60.0
    )
    tracker.update_stops([
        {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0},
    ])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="1", lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )
    assert [e.event_type for e in storage.events] == ["arrival"]

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1", lat=0.0, lon=0.00036, route_id="R1", timestamp=base + timedelta(seconds=30)
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1", lat=0.0, lon=0.0001, route_id="R1", timestamp=base + timedelta(seconds=60)
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1", lat=0.0, lon=0.001, route_id="R1", timestamp=base + timedelta(seconds=120)
            )
        ]
    )
    assert [e.event_type for e in storage.events] == ["arrival", "departure"]


def test_headway_tracker_waits_for_departure_threshold_before_switching_stops():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage, arrival_distance_threshold_m=70.0, departure_distance_threshold_m=90.0
    )
    tracker.update_stops(
        [
            {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0},
            {"StopID": "B", "Latitude": 0.0, "Longitude": 0.0006},
        ]
    )

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="9", lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )
    assert [e.event_type for e in storage.events] == ["arrival"]

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="9", lat=0.0, lon=0.0004, route_id="R1", timestamp=base + timedelta(seconds=45)
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="9", lat=0.0, lon=0.0009, route_id="R1", timestamp=base + timedelta(seconds=120)
            )
        ]
    )
    assert [e.event_type for e in storage.events] == ["arrival", "departure", "arrival"]
    assert [e.stop_id for e in storage.events] == ["A", "A", "B"]


def test_headway_tracker_departure_time_tracks_movement_start():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage, arrival_distance_threshold_m=30.0, departure_distance_threshold_m=60.0
    )
    tracker.update_stops(
        [
            {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0},
        ]
    )

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="3", lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )

    movement_start = base + timedelta(seconds=30)
    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="3", lat=0.0, lon=0.00032, route_id="R1", timestamp=movement_start)]
    )
    assert len(storage.events) == 1

    exit_time = base + timedelta(seconds=90)
    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="3", lat=0.0, lon=0.0007, route_id="R1", timestamp=exit_time)]
    )

    assert [e.event_type for e in storage.events] == ["arrival", "departure"]
    assert storage.events[-1].timestamp == movement_start
    assert storage.events[-1].dwell_seconds == 30
