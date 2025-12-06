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
        [VehicleSnapshot(vehicle_id="1", vehicle_name=None, lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )
    assert [e.event_type for e in storage.events] == ["arrival"]

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1",
                vehicle_name=None,
                lat=0.0,
                lon=0.00036,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1",
                vehicle_name=None,
                lat=0.0,
                lon=0.0001,
                route_id="R1",
                timestamp=base + timedelta(seconds=60),
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="1",
                vehicle_name=None,
                lat=0.0,
                lon=0.001,
                route_id="R1",
                timestamp=base + timedelta(seconds=120),
            )
        ]
    )
    assert [e.event_type for e in storage.events] == ["arrival", "departure"]


def test_headway_tracker_discards_duplicate_arrivals_at_same_stop():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage, arrival_distance_threshold_m=30.0, departure_distance_threshold_m=60.0)
    tracker.update_stops([
        {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0},
    ])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="dup", vehicle_name=None, lat=0.0, lon=0.0, route_id="R1", timestamp=base
            )
        ]
    )
    assert [e.event_type for e in storage.events] == ["arrival"]

    tracker.vehicle_states.clear()

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="dup",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival"]
    assert storage.events[0].timestamp == base


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
        [VehicleSnapshot(vehicle_id="9", vehicle_name=None, lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )
    assert [e.event_type for e in storage.events] == ["arrival"]

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="9",
                vehicle_name=None,
                lat=0.0,
                lon=0.0004,
                route_id="R1",
                timestamp=base + timedelta(seconds=45),
            )
        ]
    )
    assert len(storage.events) == 1

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="9",
                vehicle_name=None,
                lat=0.0,
                lon=0.0009,
                route_id="R1",
                timestamp=base + timedelta(seconds=120),
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
        [VehicleSnapshot(vehicle_id="3", vehicle_name=None, lat=0.0, lon=0.0, route_id="R1", timestamp=base)]
    )

    movement_start = base + timedelta(seconds=30)
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="3",
                vehicle_name=None,
                lat=0.0,
                lon=0.00032,
                route_id="R1",
                timestamp=movement_start,
            )
        ]
    )
    assert len(storage.events) == 1

    exit_time = base + timedelta(seconds=90)
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="3",
                vehicle_name=None,
                lat=0.0,
                lon=0.0007,
                route_id="R1",
                timestamp=exit_time,
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival", "departure"]
    assert storage.events[-1].timestamp == movement_start
    assert storage.events[-1].dwell_seconds == 30


def test_headway_tracker_merges_route_ids_for_shared_locations():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage, arrival_distance_threshold_m=30.0, departure_distance_threshold_m=60.0
    )
    tracker.update_stops(
        [
            {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0, "Routes": [{"RouteID": "R1"}]},
            {"StopID": "B", "Latitude": 0.0, "Longitude": 0.0, "Routes": [{"RouteID": "R2"}]},
        ]
    )

    assert tracker.stop_lookup["A"].route_ids == {"R1", "R2"}
    assert tracker.stop_lookup["B"].route_ids == {"R1", "R2"}

    nearest = tracker._nearest_stop(0.0, 0.0, "R2", threshold=30.0)
    assert nearest == ("A", "R2")


def test_headway_tracker_uses_stop_history_when_route_missing():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage, arrival_distance_threshold_m=30.0, departure_distance_threshold_m=60.0
    )
    tracker.update_stops([
        {"StopID": "A", "Latitude": 0.0, "Longitude": 0.0},
    ])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="5", vehicle_name=None, lat=0.0, lon=0.0, route_id=None, timestamp=base)]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="5",
                vehicle_name=None,
                lat=0.0,
                lon=0.0007,
                route_id=None,
                timestamp=base + timedelta(seconds=60),
            )
        ]
    )

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="5",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=120),
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival", "departure", "arrival"]
    assert storage.events[-1].headway_departure_arrival == 60


def test_headway_tracker_respects_stop_approach_cone():
    storage = MemoryHeadwayStorage()
    approach_config = {
        "EAST": (90.0, 30.0, 70.0),
        "WEST": (90.0, 10.0, 70.0),
    }
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=70.0,
        departure_distance_threshold_m=70.0,
        stop_approach=approach_config,
    )
    tracker.update_stops(
        [
            {"StopID": "EAST", "Latitude": 0.0, "Longitude": 0.0003},
            {"StopID": "WEST", "Latitude": 0.0, "Longitude": -0.0003},
        ]
    )

    nearest = tracker._nearest_stop(0.0, 0.0, None, threshold=70.0, heading_deg=270.0)
    assert nearest == ("WEST", None)


def test_headway_tracker_uses_approach_radius_instead_of_circle():
    storage = MemoryHeadwayStorage()
    approach_config = {
        "EAST": (90.0, 20.0, 80.0),
    }
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=30.0,
        departure_distance_threshold_m=70.0,
        stop_approach=approach_config,
    )
    tracker.update_stops(
        [
            {"StopID": "EAST", "Latitude": 0.0, "Longitude": 0.0007},
        ]
    )

    nearest = tracker._nearest_stop(0.0, 0.0012, None, threshold=30.0, heading_deg=270.0)
    assert nearest == ("EAST", None)


def test_headway_tracker_rejects_missing_heading_in_cone():
    storage = MemoryHeadwayStorage()
    approach_config = {
        "NORTH": (0.0, 20.0, 80.0),
    }
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=80.0,
        departure_distance_threshold_m=80.0,
        stop_approach=approach_config,
    )
    tracker.update_stops([
        {"StopID": "NORTH", "Latitude": 0.0, "Longitude": 0.0},
    ])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Vehicle is north of the stop and within the cone radius but has no heading
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="northbound",
                vehicle_name=None,
                lat=0.0005,
                lon=0.0,
                route_id="R1",
                timestamp=base,
            )
        ]
    )

    assert storage.events == []

    # Same position with a valid heading toward the stop should be accepted
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="northbound",
                vehicle_name=None,
                lat=0.0005,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
                heading_deg=180.0,
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival"]


def test_headway_tracker_requires_vehicle_heading_for_cone():
    storage = MemoryHeadwayStorage()
    approach_config = {
        "EAST": (90.0, 20.0, 80.0),
    }
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=80.0,
        departure_distance_threshold_m=80.0,
        stop_approach=approach_config,
    )
    tracker.update_stops([
        {"StopID": "EAST", "Latitude": 0.0, "Longitude": 0.0004},
    ])

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="east",
                vehicle_name=None,
                lat=0.0,
                lon=0.0006,
                route_id="R1",
                timestamp=base,
                heading_deg=90.0,
            )
        ]
    )

    assert storage.events == []

    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="east",
                vehicle_name=None,
                lat=0.0,
                lon=0.0006,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
                heading_deg=260.0,
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival"]


def test_headway_tracker_requires_entering_saved_cone_for_arrival():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=70.0,
        departure_distance_threshold_m=70.0,
        stop_approach={},
    )

    tracker.update_stops([
        {"StopID": "NORTH", "Latitude": 0.0, "Longitude": 0.0},
    ])

    # Saved cone pointing north with a reasonable tolerance and radius, but
    # the tracker never receives refreshed stops that include it.
    tracker.stop_approach = {"NORTH": (0.0, 15.0, 70.0)}

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Vehicle is south of the stop (outside the north-facing cone) but within distance
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="cone",
                vehicle_name=None,
                lat=-0.0005,
                lon=0.0,
                route_id="R1",
                timestamp=base,
            )
        ]
    )

    assert storage.events == []

    # Vehicle enters the cone from the south, ending up north of the stop
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="cone",
                vehicle_name=None,
                lat=0.0005,
                lon=0.0,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
                heading_deg=180.0,
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival"]


def test_headway_tracker_cone_uses_stop_to_vehicle_bearing_and_opposing_heading():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(
        storage=storage,
        arrival_distance_threshold_m=80.0,
        departure_distance_threshold_m=80.0,
        stop_approach={},
    )

    tracker.update_stops([
        {"StopID": "EASTBOUND", "Latitude": 0.0, "Longitude": 0.0},
    ])

    # Cone opens to the west of the stop (bearing 270), expecting vehicles to travel eastbound
    tracker.stop_approach = {"EASTBOUND": (270.0, 20.0, 80.0)}

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Vehicle is west of the stop and heading east; should be accepted
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="eastbound",
                vehicle_name=None,
                lat=0.0,
                lon=-0.0006,
                route_id="R1",
                timestamp=base,
                heading_deg=90.0,
            )
        ]
    )

    assert [e.event_type for e in storage.events] == ["arrival"]

    # Vehicle on same side of stop but cone pointed the wrong way should be rejected
    storage.events.clear()
    tracker.stop_approach = {"EASTBOUND": (90.0, 20.0, 80.0)}
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="eastbound",
                vehicle_name=None,
                lat=0.0,
                lon=-0.0006,
                route_id="R1",
                timestamp=base + timedelta(seconds=30),
                heading_deg=90.0,
            )
        ]
    )

    assert storage.events == []


def test_build_transloc_stops_merges_approach_config():
    from app import _build_transloc_stops

    approach_config = {"123": (45.0, 15.0)}
    stops = _build_transloc_stops(
        [
            {
                "RouteID": "1",
                "Stops": [
                    {"StopID": "123", "Latitude": 0.0, "Longitude": 0.0},
                ],
            }
        ],
        approach_config=approach_config,
    )

    assert stops[0]["ApproachBearingDeg"] == 45.0
    assert stops[0]["ApproachToleranceDeg"] == 15.0
