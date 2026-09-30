import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from headway_tracker import HeadwayTracker, VehicleSnapshot
from app import _build_transloc_stops


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


def test_lat_lon_merge_returns_all_address_ids():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)

    stop_a = {
        "StopID": "A",
        "Latitude": 1.0,
        "Longitude": 2.0,
        "AddressID": 100,
        "RouteID": "R1",
    }
    stop_b = {
        "StopID": "B",
        "Latitude": 1.0,
        "Longitude": 2.0,
        "AddressID": 200,
        "RouteID": "R2",
    }

    tracker.update_stops([stop_a, stop_b])

    assert len(tracker.stops) == 1
    merged = tracker.stops[0]
    assert merged.address_ids == {"100", "200"}
    assert merged.address_id == "100,200"
    assert merged.route_ids == {"R1", "R2"}
    assert tracker.address_lookup["100"] is merged
    assert tracker.address_lookup["200"] is merged


def test_route_specific_stop_id_recorded_at_shared_physical_stop():
    """A physical stop served by two routes gets two different RouteStopIDs from
    TransLoc. The merged StopPoint keeps only the first-seen one for internal
    tracking, but an arrival logged for the *other* route must record that
    route's own RouteStopID, not the merged group's -- otherwise a stop shared
    by multiple routes silently corrupts trip_planner_history.py's per-route
    hop-time buckets."""
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)

    approach = {
        "name": "main",
        "bubbles": [
            {"lat": 0.0, "lng": -0.0006, "radius_m": 70.0, "order": 1},
            {"lat": 0.0, "lng": 0.0, "radius_m": 30.0, "order": 2},
        ],
    }
    stop_r1 = {
        "StopID": "STOP_R1",
        "Latitude": 0.0,
        "Longitude": 0.0,
        "RouteID": "R1",
        "ApproachSets": [approach],
    }
    stop_r2 = {
        "StopID": "STOP_R2",
        "Latitude": 0.0,
        "Longitude": 0.0,
        "RouteID": "R2",
    }
    tracker.update_stops([stop_r1, stop_r2])

    merged = tracker.stops[0]
    assert merged.stop_id == "STOP_R1"  # first-seen, used only for internal tracking
    assert merged.route_stop_ids == {"R1": "STOP_R1", "R2": "STOP_R2"}

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    tracker.process_snapshots(
        [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=-0.0006, route_id="R2", timestamp=base)]
    )
    tracker.process_snapshots(
        [
            VehicleSnapshot(
                vehicle_id="bus",
                vehicle_name=None,
                lat=0.0,
                lon=0.0,
                route_id="R2",
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
                route_id="R2",
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
                route_id="R2",
                timestamp=base + timedelta(seconds=70),
            )
        ]
    )

    arrival = next(e for e in storage.events if e.event_type == "arrival")
    assert arrival.route_id == "R2"
    assert arrival.stop_id == "STOP_R2"


def test_address_id_survives_build_and_tracker():
    routes = [
        {
            "RouteID": 1,
            "Stops": [
                {
                    "StopID": 10,
                    "StopName": "Test Stop",
                    "Latitude": 1.0,
                    "Longitude": 2.0,
                    "AddressID": 555,
                }
            ],
        }
    ]

    stops = _build_transloc_stops(routes)
    tracker = HeadwayTracker(storage=MemoryHeadwayStorage())
    tracker.update_stops(stops)

    assert len(tracker.stops) == 1
    stop = tracker.stops[0]
    assert stop.address_id == "555"
    assert stop.address_ids == {"555"}


def _stop_at(stop_id, lon_final, lon_first, route="R1"):
    return {
        "StopID": stop_id,
        "Latitude": 0.0,
        "Longitude": lon_final,
        "RouteID": route,
        "AddressID": stop_id,
        "ApproachSets": [
            {
                "name": f"{stop_id} approach",
                "bubbles": [
                    {"lat": 0.0, "lng": lon_first, "radius_m": 50.0, "order": 1},
                    {"lat": 0.0, "lng": lon_final, "radius_m": 40.0, "order": 2},
                ],
            }
        ],
    }


def _snap(lon, seconds, route="R1", base=datetime(2024, 1, 1, tzinfo=timezone.utc)):
    return [VehicleSnapshot(vehicle_id="bus", vehicle_name=None, lat=0.0, lon=lon, route_id=route,
                            timestamp=base + timedelta(seconds=seconds))]


def test_stop_across_the_street_does_not_get_a_route_activation_arrival():
    # NEAR is approached from the west; FACING (the stop across the street, 10 m away)
    # from the east. A bus that properly arrives at NEAR is also parked inside FACING's
    # final bubble -- that must not log a second, "route_activation" arrival at FACING.
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_stop_at("NEAR", 0.0, -0.0006), _stop_at("FACING", 0.00009, 0.0007)])
    tracker.process_snapshots(_snap(-0.0006, 0))
    for t in (20, 40, 60, 80):
        tracker.process_snapshots(_snap(0.0, t))
    arrivals = [(e.stop_id, e.arrival_type) for e in storage.events if e.event_type == "arrival"]
    assert arrivals == [("NEAR", "stopped")]


def test_second_approach_set_of_same_stop_does_not_repeat_the_arrival():
    stop = _stop_at("STOP", 0.0, -0.0006)
    stop["ApproachSets"].append({
        "name": "other direction",
        "bubbles": [
            {"lat": 0.0, "lng": 0.0007, "radius_m": 50.0, "order": 1},
            {"lat": 0.0, "lng": 0.00005, "radius_m": 40.0, "order": 2},
        ],
    })
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([stop])
    tracker.process_snapshots(_snap(-0.0006, 0))
    for t in (20, 40, 60, 80, 100):
        tracker.process_snapshots(_snap(0.0, t))
    assert [e.event_type for e in storage.events] == ["arrival"]


def test_bus_coming_on_route_while_parked_still_gets_a_route_activation_arrival():
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage, tracked_route_ids={"R1"})  # like prod: off-route buses skipped
    tracker.update_stops([_stop_at("NEAR", 0.0, -0.0006), _stop_at("FACING", 0.00009, 0.0007)])
    tracker.process_snapshots(_snap(0.0, 0, route=None))   # parked, out of service
    tracker.process_snapshots(_snap(0.0, 20, route=None))
    tracker.process_snapshots(_snap(0.0, 40))              # comes on-route, still parked
    tracker.process_snapshots(_snap(0.0, 60))              # (speed needs a second on-route poll)
    arrivals = [(e.stop_id, e.arrival_type) for e in storage.events if e.event_type == "arrival"]
    assert arrivals == [("NEAR", "route_activation")]  # only the nearer of the two final bubbles


def test_isolated_stopped_bus_in_final_bubble_is_still_logged():
    # A running bus found stopped in a final bubble it never came through bubble #1 for,
    # with no other arrival open or recent (e.g. bubble #1 skipped between polls), is
    # still worth an arrival.
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_stop_at("STOP", 0.0, -0.0006)])
    tracker.process_snapshots(_snap(-0.003, 0))
    tracker.process_snapshots(_snap(0.0, 20))
    tracker.process_snapshots(_snap(0.0, 40))
    assert [(e.stop_id, e.arrival_type) for e in storage.events if e.event_type == "arrival"] == [("STOP", "route_activation")]


def test_next_stop_200m_on_still_logged_when_its_first_bubble_was_missed():
    # Stops 200 m apart, both reached within a minute. A real arrival at the second one
    # that only shows up as route_activation (its bubble #1 missed between polls) is kept;
    # only stops within ROUTE_ACTIVATION_NEAR_M of a recent arrival are treated as phantoms.
    storage = MemoryHeadwayStorage()
    tracker = HeadwayTracker(storage=storage)
    tracker.update_stops([_stop_at("A", 0.0, -0.0006), _stop_at("B", 0.0018, 0.0026)])
    tracker.process_snapshots(_snap(-0.0006, 0))
    tracker.process_snapshots(_snap(0.0, 10))
    tracker.process_snapshots(_snap(0.0, 20))
    tracker.process_snapshots(_snap(0.0009, 30))    # left A, between the stops
    tracker.process_snapshots(_snap(0.0018, 40))    # straight into B's final bubble
    tracker.process_snapshots(_snap(0.0018, 50))
    arrivals = [(e.stop_id, e.arrival_type) for e in storage.events if e.event_type == "arrival"]
    assert arrivals == [("A", "stopped"), ("B", "route_activation")]
