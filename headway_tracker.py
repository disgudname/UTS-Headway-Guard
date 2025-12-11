from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from collections import deque
import json
import math
import os

from headway_storage import HeadwayEvent, HeadwayStorage


HEADWAY_DISTANCE_THRESHOLD_M = float(60.0)
DEFAULT_HEADWAY_CONFIG_PATH = Path("config/headway_config.json")
DEFAULT_STOP_APPROACH_CONFIG_PATH = Path("config/stop_approach.json")
DEFAULT_DATA_DIRS = [Path(p) for p in os.getenv("DATA_DIRS", "/data").split(":")]
STOP_SPEED_THRESHOLD_MPS = 0.5
MOVEMENT_CONFIRMATION_DISPLACEMENT_M = 2.0
MOVEMENT_CONFIRMATION_MIN_DURATION_S = 20.0
QUICK_DEPARTURE_MIN_DURATION_S = 5.0
STOP_ASSOCIATION_HYSTERESIS_M = 15.0  # Extra distance needed to disassociate from a stop
SPEED_HISTORY_SIZE = 3  # Number of speed samples to track for noise filtering
BUBBLE_PROGRESS_STALE_SECONDS = 120.0  # Drop bubble state if we have not heard from the vehicle


@dataclass
class VehicleSnapshot:
    vehicle_id: Optional[str]
    vehicle_name: Optional[str]
    lat: float
    lon: float
    route_id: Optional[str]
    timestamp: datetime
    heading_deg: Optional[float] = None
    block: Optional[str] = None


@dataclass
class VehiclePresence:
    current_stop_id: Optional[str] = None
    arrival_time: Optional[datetime] = None
    route_id: Optional[str] = None
    departure_started_at: Optional[datetime] = None
    speed_history: List[float] = None  # Recent speed samples for noise filtering

    def __post_init__(self):
        if self.speed_history is None:
            self.speed_history = []


@dataclass
class ApproachBubble:
    lat: float
    lon: float
    radius_m: float
    order: int


@dataclass
class ApproachSet:
    name: str
    bubbles: List[ApproachBubble]


@dataclass
class StopPoint:
    stop_id: str
    lat: float
    lon: float
    route_ids: Set[str]
    approach_sets: Optional[List[ApproachSet]] = None


@dataclass
class BubbleProgressState:
    """Tracks a vehicle's progress through an approach bubble set."""
    stop_id: str
    set_index: int  # Which approach set is being tracked
    set_name: str
    highest_bubble_reached: int  # Highest bubble order number reached (1 = first/outermost)
    max_bubble_order: int  # The highest bubble order in this set (the stop bubble)
    route_id: Optional[str]
    entered_at: datetime
    last_seen: datetime
    next_expected_order: int = 1  # Next bubble order required for valid progression
    in_final_bubble: bool = False  # Currently in the stop bubble
    entered_final_at: Optional[datetime] = None  # When entered final bubble
    left_final_at: Optional[datetime] = None  # When exited the final bubble
    stopped_in_final: bool = False  # Has the bus stopped in the final bubble
    stopped_at: Optional[datetime] = None  # When the bus stopped
    arrival_logged: bool = False  # Whether arrival has been logged for this approach


class HeadwayTracker:
    def __init__(
        self,
        storage: HeadwayStorage,
        *,
        arrival_distance_threshold_m: float = HEADWAY_DISTANCE_THRESHOLD_M,
        departure_distance_threshold_m: float = HEADWAY_DISTANCE_THRESHOLD_M,
        tracked_route_ids: Optional[Set[str]] = None,
        tracked_stop_ids: Optional[Set[str]] = None,
    ):
        self.storage = storage
        self.arrival_distance_threshold_m = min(arrival_distance_threshold_m, departure_distance_threshold_m)
        self.departure_distance_threshold_m = max(arrival_distance_threshold_m, departure_distance_threshold_m)
        self.tracked_route_ids = tracked_route_ids or set()
        self.tracked_stop_ids = tracked_stop_ids or set()
        self.vehicle_states: Dict[str, VehiclePresence] = {}
        self.last_arrival: Dict[Tuple[str, str], datetime] = {}
        self.last_departure: Dict[Tuple[str, str], datetime] = {}
        self.last_vehicle_arrival: Dict[Tuple[str, str, Optional[str]], datetime] = {}
        self.last_vehicle_departure: Dict[Tuple[str, str, Optional[str]], datetime] = {}
        self.stops: List[StopPoint] = []
        self.stop_lookup: Dict[str, StopPoint] = {}
        self.recent_stop_association_failures: deque = deque(maxlen=25)
        self.recent_snapshot_diagnostics: deque = deque(maxlen=50)
        self.last_snapshots: Dict[str, VehicleSnapshot] = {}
        self.pending_departure_movements: Dict[str, Dict[str, object]] = {}
        self.final_bubble_exits: Dict[Tuple[str, str], datetime] = {}
        # Bubble-based approach tracking: {vehicle_id: {stop_id: {set_idx: BubbleProgressState}}}
        self.vehicle_bubble_progress: Dict[str, Dict[str, Dict[int, BubbleProgressState]]] = {}
        # Recent bubble activations for visualization (exposed via API)
        self.recent_bubble_activations: deque = deque(maxlen=100)
        print(
            f"[headway] tracker initialized routes={sorted(self.tracked_route_ids) if self.tracked_route_ids else 'all'} "
            f"stops={sorted(self.tracked_stop_ids) if self.tracked_stop_ids else 'all'}"
        )

    def update_stops(self, stops: Iterable[dict]) -> None:
        updated: List[StopPoint] = []
        for stop in stops:
            stop_id = stop.get("StopID") or stop.get("StopId")
            if stop_id is None:
                continue
            lat = stop.get("Latitude")
            if lat is None:
                lat = stop.get("Lat")
            lon = stop.get("Longitude")
            if lon is None:
                lon = stop.get("Lon")
            if lon is None:
                lon = stop.get("Lng")
            if lat is None or lon is None:
                continue
            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (TypeError, ValueError):
                continue
            route_ids = self._extract_route_ids(stop)
            # Parse approach sets (bubble configurations)
            approach_sets: Optional[List[ApproachSet]] = None
            raw_sets = stop.get("ApproachSets")
            if raw_sets and isinstance(raw_sets, list):
                approach_sets = []
                for raw_set in raw_sets:
                    if not isinstance(raw_set, dict):
                        continue
                    set_name = raw_set.get("name", "")
                    raw_bubbles = raw_set.get("bubbles", [])
                    if not raw_bubbles:
                        continue
                    bubbles: List[ApproachBubble] = []
                    for raw_bubble in raw_bubbles:
                        if not isinstance(raw_bubble, dict):
                            continue
                        b_lat = _parse_float(raw_bubble.get("lat"))
                        b_lng = _parse_float(raw_bubble.get("lng"))
                        b_radius = _parse_float(raw_bubble.get("radius_m")) or 25.0
                        b_order = raw_bubble.get("order", len(bubbles) + 1)
                        if b_lat is not None and b_lng is not None:
                            bubbles.append(ApproachBubble(lat=b_lat, lon=b_lng, radius_m=b_radius, order=int(b_order)))
                    if bubbles:
                        # Sort by order
                        bubbles.sort(key=lambda b: b.order)
                        approach_sets.append(ApproachSet(name=set_name, bubbles=bubbles))
            updated.append(
                StopPoint(
                    stop_id=str(stop_id),
                    lat=lat_f,
                    lon=lon_f,
                    route_ids=route_ids,
                    approach_sets=approach_sets,
                )
            )

        # Merge route IDs across stops that share the same physical location. Some
        # providers, like TransLoc, emit unique stop IDs per route even when the
        # latitude/longitude are identical. Without merging, vehicles can be inside
        # the arrival radius but still hit a route_mismatch diagnostic.
        location_routes: Dict[Tuple[float, float], Set[str]] = {}
        for stop in updated:
            key = (round(stop.lat, 6), round(stop.lon, 6))
            if key not in location_routes:
                location_routes[key] = set()
            location_routes[key].update(stop.route_ids)

        for stop in updated:
            key = (round(stop.lat, 6), round(stop.lon, 6))
            merged_routes = set(stop.route_ids)
            merged_routes.update(location_routes.get(key, set()))
            stop.route_ids = merged_routes

        self.stops = updated
        self.stop_lookup = {stop.stop_id: stop for stop in updated}
        if not updated:
            print("[headway] stop update received no stops; tracker inputs unavailable")

    def process_snapshots(self, snapshots: Sequence[VehicleSnapshot]) -> None:
        if not self.stops:
            return
        events: List[HeadwayEvent] = []
        for snap in snapshots:
            if snap.lat is None or snap.lon is None:
                continue
            route_id_norm = self._normalize_id(snap.route_id)
            if self.tracked_route_ids and (route_id_norm is None or route_id_norm not in self.tracked_route_ids):
                continue
            current_stop = self._nearest_stop(
                snap.lat,
                snap.lon,
                route_id_norm,
                threshold=self.arrival_distance_threshold_m,
                heading_deg=snap.heading_deg,
            )
            vid = self._normalize_id(snap.vehicle_id)
            if vid is None:
                continue
            if current_stop is None:
                self._log_stop_association_failure(snap, route_id_norm)
            prev_state = self.vehicle_states.get(vid, VehiclePresence())
            current_state = prev_state
            prev_stop = prev_state.current_stop_id
            distance_from_prev_stop = self._distance_to_stop(prev_stop, snap.lat, snap.lon)
            timestamp = snap.timestamp if snap.timestamp.tzinfo else snap.timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc)
            snap.timestamp = timestamp
            departure_recorded = False
            departure_trigger = None

            prev_snap = self.last_snapshots.get(vid)

            delta_seconds = None
            speed_mps = None
            raw_speed_mps = None
            if prev_snap:
                delta_seconds = (timestamp - prev_snap.timestamp).total_seconds()
                if delta_seconds > 0:
                    movement_distance = self._haversine(prev_snap.lat, prev_snap.lon, snap.lat, snap.lon)
                    raw_speed_mps = movement_distance / delta_seconds
                    # Apply GPS noise filtering using median of recent speeds
                    speed_mps = self._calculate_filtered_speed(prev_state.speed_history, raw_speed_mps)

            block_label = (snap.block or "unknown").strip() if snap.block else "unknown"

            # Track bubble-based arrivals
            bubble_arrival = self._track_bubble_progress(vid, snap, route_id_norm, speed_mps, raw_speed_mps)
            if bubble_arrival:
                bubble_stop_id, bubble_route_id, bubble_arrival_time, bubble_dwell = bubble_arrival
                # Log the bubble-based arrival
                arrival_key = (vid, bubble_stop_id, bubble_route_id)
                prev_vehicle_arrival = self.last_vehicle_arrival.get(arrival_key)
                prev_vehicle_departure = self.last_vehicle_departure.get(arrival_key)
                duplicate = prev_vehicle_arrival is not None and (
                    prev_vehicle_departure is None or prev_vehicle_departure <= prev_vehicle_arrival
                )
                if not duplicate:
                    headway_aa, headway_da = self._record_arrival_headways(bubble_route_id, bubble_stop_id, bubble_arrival_time)
                    self.final_bubble_exits.pop((vid, bubble_stop_id), None)
                    current_state = VehiclePresence(
                        current_stop_id=bubble_stop_id,
                        arrival_time=bubble_arrival_time,
                        route_id=bubble_route_id or route_id_norm,
                        departure_started_at=None,
                        speed_history=prev_state.speed_history,
                    )
                    events.append(
                        HeadwayEvent(
                            timestamp=bubble_arrival_time,
                            route_id=bubble_route_id,
                            stop_id=bubble_stop_id,
                            vehicle_id=vid,
                            block=block_label,
                            vehicle_name=snap.vehicle_name,
                            event_type="arrival",
                            headway_arrival_arrival=headway_aa,
                            headway_departure_arrival=headway_da,
                            dwell_seconds=bubble_dwell if bubble_dwell > 0 else None,
                        )
                    )
                    self.last_vehicle_arrival[arrival_key] = bubble_arrival_time
                    print(f"[headway] bubble arrival: vehicle={vid} stop={bubble_stop_id} dwell={bubble_dwell}s")

            # Departure detection with hysteresis
            # Use larger threshold for disassociation to prevent flapping at boundary
            has_left_prev_stop = True
            if prev_stop is not None and distance_from_prev_stop is not None:
                departure_threshold = self.departure_distance_threshold_m + STOP_ASSOCIATION_HYSTERESIS_M
                has_left_prev_stop = distance_from_prev_stop >= departure_threshold

            bubble_departure_time = None
            bubble_departed = False
            if prev_stop is not None and prev_state.arrival_time is not None:
                bubble_departure_time = self.final_bubble_exits.get((vid, prev_stop))
                if bubble_departure_time and bubble_departure_time >= prev_state.arrival_time:
                    bubble_departed = True
                    has_left_prev_stop = True
                    departure_trigger = "bubble_exit"

            movement_start_time = prev_state.departure_started_at
            movement_confirmed = False
            pending_movement = self.pending_departure_movements.get(vid)
            if prev_stop is not None and distance_from_prev_stop is not None:
                if speed_mps is not None and speed_mps > STOP_SPEED_THRESHOLD_MPS:
                    if distance_from_prev_stop < self.departure_distance_threshold_m and movement_start_time is None:
                        movement_start_time = timestamp
                    if movement_start_time is not None:
                        if pending_movement is None or pending_movement.get("start_time") != movement_start_time:
                            pending_movement = {
                                "start_time": movement_start_time,
                                "start_lat": snap.lat,
                                "start_lon": snap.lon,
                                "start_distance": distance_from_prev_stop,
                                "movement_count": 1,
                            }
                            self.pending_departure_movements[vid] = pending_movement
                        else:
                            pending_movement["movement_count"] = pending_movement.get("movement_count", 1) + 1
                elif distance_from_prev_stop < self.arrival_distance_threshold_m:
                    movement_start_time = None
                    self.pending_departure_movements.pop(vid, None)
                elif distance_from_prev_stop >= self.arrival_distance_threshold_m and movement_start_time is None:
                    movement_start_time = timestamp
                    self.pending_departure_movements.pop(vid, None)
            elif prev_stop is None:
                self.pending_departure_movements.pop(vid, None)

            pending_movement = self.pending_departure_movements.get(vid)
            if pending_movement:
                movement_duration = None
                start_time = pending_movement.get("start_time")
                if start_time:
                    movement_duration = (timestamp - start_time).total_seconds()
                movement_displacement = self._haversine(
                    pending_movement.get("start_lat", snap.lat),
                    pending_movement.get("start_lon", snap.lon),
                    snap.lat,
                    snap.lon,
                )
                movement_count = pending_movement.get("movement_count", 1)
                start_distance = pending_movement.get("start_distance")
                near_stop = (
                    distance_from_prev_stop is not None
                    and distance_from_prev_stop < self.departure_distance_threshold_m
                )
                moving_away = (
                    distance_from_prev_stop is None
                    or start_distance is None
                    or distance_from_prev_stop >= start_distance
                )
                fast_departure = (
                    moving_away
                    and speed_mps is not None
                    and speed_mps > STOP_SPEED_THRESHOLD_MPS
                    and distance_from_prev_stop is not None
                    and distance_from_prev_stop < self.departure_distance_threshold_m
                )
                quick_departure = (
                    moving_away
                    and movement_duration is not None
                    and movement_duration >= QUICK_DEPARTURE_MIN_DURATION_S
                    and distance_from_prev_stop is not None
                    and distance_from_prev_stop >= self.arrival_distance_threshold_m
                )
                # Strengthen movement confirmation - require combination of conditions
                # to prevent GPS noise from triggering false departures
                has_displacement = movement_displacement >= MOVEMENT_CONFIRMATION_DISPLACEMENT_M
                has_duration = (
                    movement_duration is not None
                    and movement_duration >= MOVEMENT_CONFIRMATION_MIN_DURATION_S
                )
                has_multiple_observations = movement_count >= 2

                # Require either (displacement AND multiple observations) OR sustained duration OR quick departure
                sustained_movement = (
                    (has_displacement and has_multiple_observations)
                    or has_duration
                    or quick_departure
                    or not near_stop
                )

                if sustained_movement and moving_away and (fast_departure or has_displacement or quick_departure):
                    movement_confirmed = True
                    movement_start_time = pending_movement.get("start_time") or movement_start_time
                    if not bubble_departed:
                        departure_trigger = "speed" if fast_departure else departure_trigger
                    self.pending_departure_movements.pop(vid, None)

            if movement_confirmed:
                has_left_prev_stop = True
                departure_trigger = departure_trigger or "movement"

            if prev_stop and (
                bubble_departed
                or movement_confirmed
                or (has_left_prev_stop and (current_stop is None or current_stop[0] != prev_stop))
            ):
                dwell_seconds = None
                departure_timestamp = bubble_departure_time or movement_start_time or timestamp
                if (
                    movement_confirmed
                    and prev_snap
                    and delta_seconds
                    and delta_seconds > 0
                    and speed_mps
                    and speed_mps > 0
                    and prev_stop is not None
                    and distance_from_prev_stop is not None
                    and not bubble_departed
                ):
                    prev_snap_distance = self._distance_to_stop(prev_stop, prev_snap.lat, prev_snap.lon)
                    if prev_snap_distance is not None and prev_snap_distance <= self.arrival_distance_threshold_m:
                        threshold_distance = max(prev_snap_distance, min(5.0, self.departure_distance_threshold_m))
                        distance_delta = distance_from_prev_stop - prev_snap_distance
                        if distance_delta > 0 and distance_from_prev_stop >= threshold_distance:
                            fraction = (threshold_distance - prev_snap_distance) / distance_delta
                            fraction = min(max(fraction, 0.0), 1.0)
                            interpolated_time = prev_snap.timestamp + timedelta(seconds=fraction * delta_seconds)
                            # Ensure interpolated timestamp is within bounds
                            departure_timestamp = max(prev_snap.timestamp, min(timestamp, interpolated_time))
                if prev_state.arrival_time:
                    dwell_seconds = (departure_timestamp - prev_state.arrival_time).total_seconds()
                    dwell_seconds = max(dwell_seconds, 0.0)
                if prev_stop:
                    route_for_departure = prev_state.route_id or route_id_norm
                    existing_departure = self.last_vehicle_departure.get((vid, prev_stop, route_for_departure))

                    # Prevent duplicate departures - similar to arrival duplicate prevention
                    duplicate_departure = False
                    if existing_departure is not None:
                        # Check if we already recorded a departure after the most recent arrival
                        if prev_state.arrival_time is None or existing_departure > prev_state.arrival_time:
                            duplicate_departure = True

                    if not duplicate_departure:
                        keys = []
                        if route_for_departure:
                            keys.append((route_for_departure, prev_stop))
                        keys.append((None, prev_stop))
                        for key in keys:
                            self.last_departure[key] = departure_timestamp
                        self.last_vehicle_departure[(vid, prev_stop, route_for_departure)] = departure_timestamp
                        for arrival_key in list(self.last_vehicle_arrival.keys()):
                            if arrival_key[0] == vid and arrival_key[1] == prev_stop:
                                self.last_vehicle_departure[arrival_key] = departure_timestamp
                        if departure_trigger is None:
                            departure_trigger = "bubble_exit" if bubble_departed else "distance"
                        self.pending_departure_movements.pop(vid, None)
                        events.append(
                            HeadwayEvent(
                                timestamp=departure_timestamp,
                                route_id=prev_state.route_id,
                                stop_id=prev_stop,
                                vehicle_id=vid,
                                block=block_label,
                                vehicle_name=snap.vehicle_name,
                                event_type="departure",
                                headway_arrival_arrival=None,
                                headway_departure_arrival=None,
                                dwell_seconds=dwell_seconds,
                            )
                        )
                        departure_recorded = True
                        if bubble_departed:
                            self.final_bubble_exits.pop((vid, prev_stop), None)

            # Vehicle state tracking (for departure detection, speed history)
            # Arrivals are now handled exclusively by bubble-based tracking above
            self.vehicle_states[vid] = VehiclePresence(
                current_stop_id=current_state.current_stop_id,
                arrival_time=current_state.arrival_time,
                route_id=current_state.route_id,
                departure_started_at=movement_start_time if current_state.current_stop_id else None,
                speed_history=current_state.speed_history,
            )

            target_stop_id = current_state.current_stop_id or (current_stop[0] if current_stop else None)
            self.recent_snapshot_diagnostics.append(
                {
                    "timestamp": self._isoformat(timestamp),
                    "vehicle_id": vid,
                    "vehicle_name": snap.vehicle_name,
                    "block": block_label,
                    "route_id": route_id_norm,
                    "heading_deg": snap.heading_deg,
                    "previous_stop_id": prev_stop,
                    "distance_from_previous_stop": distance_from_prev_stop,
                    "has_left_previous_stop": has_left_prev_stop,
                    "target_stop_id": target_stop_id,
                    "target_stop_distance": self._distance_to_stop(target_stop_id, snap.lat, snap.lon),
                    "departure_recorded": departure_recorded,
                    "departure_started_at": self._isoformat(movement_start_time),
                    "departure_trigger": departure_trigger,
                    "speed_mps": speed_mps,
                }
            )

            # Save snapshot for future speed calculations
            self.last_snapshots[vid] = snap

        if events:
            try:
                self.storage.write_events(events)
                print(f"[headway] recorded {len(events)} events")
            except Exception as exc:
                print(f"[headway] failed to write events: {exc}")

    def _track_bubble_progress(
        self,
        vid: str,
        snap: VehicleSnapshot,
        route_id_norm: Optional[str],
        speed_mps: Optional[float],
        raw_speed_mps: Optional[float],
    ) -> Optional[Tuple[str, Optional[str], datetime, float]]:
        """
        Track vehicle progress through approach bubble sets.
        Returns (stop_id, route_id, arrival_time, dwell_seconds) if an arrival should be logged.
        """
        timestamp = snap.timestamp
        vehicle_progress = self.vehicle_bubble_progress.get(vid, {})

        # Drop stale bubble tracking when a vehicle has not reported in a while
        stale_stop_ids: List[str] = []
        for stop_id, set_progress in vehicle_progress.items():
            stale_set_indices: List[int] = []
            for set_idx, progress in set_progress.items():
                last_seen = progress.last_seen or progress.entered_at
                if last_seen and (timestamp - last_seen).total_seconds() > BUBBLE_PROGRESS_STALE_SECONDS:
                    stale_set_indices.append(set_idx)

            for set_idx in stale_set_indices:
                set_progress.pop(set_idx, None)

            if not set_progress:
                stale_stop_ids.append(stop_id)

        for stop_id in stale_stop_ids:
            vehicle_progress.pop(stop_id, None)

        arrivals_to_log: List[Tuple[str, Optional[str], datetime, float]] = []

        # Check all stops with approach sets
        for stop in self.stops:
            if not stop.approach_sets:
                continue
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            if stop.route_ids and (route_id_norm is None or route_id_norm not in stop.route_ids):
                continue

            stop_progress = vehicle_progress.get(stop.stop_id, {})

            # Check which bubbles the vehicle is currently in for each approach set
            for set_idx, approach_set in enumerate(stop.approach_sets):
                if not approach_set.bubbles:
                    continue

                max_order = max(b.order for b in approach_set.bubbles)
                current_bubbles_in: List[int] = []

                for bubble in approach_set.bubbles:
                    dist = self._haversine(snap.lat, snap.lon, bubble.lat, bubble.lon)
                    if dist <= bubble.radius_m:
                        current_bubbles_in.append(bubble.order)

                if current_bubbles_in:
                    progress = stop_progress.get(set_idx)

                    if progress is None:
                        # Require traversal to start in the outermost bubble (order 1)
                        if 1 not in current_bubbles_in:
                            continue

                        progress = BubbleProgressState(
                            stop_id=stop.stop_id,
                            set_index=set_idx,
                            set_name=approach_set.name,
                            highest_bubble_reached=1,
                            max_bubble_order=max_order,
                            route_id=route_id_norm,
                            entered_at=timestamp,
                            last_seen=timestamp,
                            next_expected_order=2,
                        )
                        stop_progress[set_idx] = progress
                        self._log_bubble_activation(vid, snap, stop, set_idx, approach_set.name, 1, "entered")
                    else:
                        # Update progress
                        progress.last_seen = timestamp

                    # Track progression through bubbles strictly in order
                    while progress.next_expected_order in current_bubbles_in and progress.next_expected_order <= max_order:
                        progress.highest_bubble_reached = progress.next_expected_order
                        progress.next_expected_order += 1
                        self._log_bubble_activation(
                            vid,
                            snap,
                            stop,
                            set_idx,
                            approach_set.name,
                            progress.highest_bubble_reached,
                            "progressed",
                        )

                    currently_in_final = (
                        max_order in current_bubbles_in and progress.highest_bubble_reached == max_order
                    )

                    if currently_in_final and not progress.in_final_bubble:
                        progress.in_final_bubble = True
                        progress.entered_final_at = progress.entered_final_at or timestamp
                        self._log_bubble_activation(
                            vid, snap, stop, set_idx, approach_set.name, progress.max_bubble_order, "entered_final"
                        )

                    if currently_in_final:
                        # Check if bus has stopped
                        observed_speed = raw_speed_mps if raw_speed_mps is not None else speed_mps
                        if observed_speed is not None and observed_speed <= STOP_SPEED_THRESHOLD_MPS:
                            if not progress.stopped_in_final:
                                progress.stopped_in_final = True
                                progress.stopped_at = timestamp
                                # Log arrival when bus stops
                                if not progress.arrival_logged:
                                    progress.arrival_logged = True
                                    dwell = 0.0  # Will be calculated on departure
                                    arrivals_to_log.append((stop.stop_id, route_id_norm, timestamp, dwell))
                                    self._log_bubble_activation(
                                        vid,
                                        snap,
                                        stop,
                                        set_idx,
                                        approach_set.name,
                                        progress.max_bubble_order,
                                        "arrival_stopped",
                                    )
                    else:
                        # Left the final bubble without stopping
                        if progress.in_final_bubble and not progress.arrival_logged:
                            progress.arrival_logged = True
                            arrival_time = progress.entered_final_at or timestamp
                            arrivals_to_log.append((stop.stop_id, route_id_norm, arrival_time, 0.0))
                            self._log_bubble_activation(
                                vid,
                                snap,
                                stop,
                                set_idx,
                                approach_set.name,
                                progress.max_bubble_order,
                                "arrival_passthrough",
                            )

                        if progress.in_final_bubble and progress.arrival_logged:
                            progress.left_final_at = progress.left_final_at or timestamp
                            self.final_bubble_exits[(vid, stop.stop_id)] = progress.left_final_at

                        progress.in_final_bubble = False

                    if stop_progress:
                        vehicle_progress[stop.stop_id] = stop_progress

                else:
                    progress = stop_progress.get(set_idx)
                    # Vehicle is not in any bubble for this set
                    if progress:
                        # Was tracking this set - check if we completed it or abandoned it
                        if progress.in_final_bubble and not progress.arrival_logged:
                            # Was in final bubble but left without stopping - log pass-through arrival with 0s dwell
                            progress.arrival_logged = True
                            arrival_time = progress.entered_final_at or timestamp
                            arrivals_to_log.append((stop.stop_id, route_id_norm, arrival_time, 0.0))
                            self._log_bubble_activation(
                                vid, snap, stop, set_idx, approach_set.name, progress.max_bubble_order, "arrival_passthrough"
                            )

                        if progress.in_final_bubble and progress.arrival_logged:
                            progress.left_final_at = progress.left_final_at or timestamp
                            self.final_bubble_exits[(vid, stop.stop_id)] = progress.left_final_at

                        # Clear progress for this set
                        self._log_bubble_activation(vid, snap, stop, set_idx, approach_set.name, 0, "exited")
                        stop_progress.pop(set_idx, None)

                    if stop_progress:
                        vehicle_progress[stop.stop_id] = stop_progress
                    elif stop.stop_id in vehicle_progress:
                        vehicle_progress.pop(stop.stop_id, None)

        if vehicle_progress:
            self.vehicle_bubble_progress[vid] = vehicle_progress
        elif vid in self.vehicle_bubble_progress:
            del self.vehicle_bubble_progress[vid]

        # Return the first arrival to log (typically should only be one)
        if arrivals_to_log:
            return arrivals_to_log[0]
        return None

    def _log_bubble_activation(
        self,
        vid: str,
        snap: VehicleSnapshot,
        stop: StopPoint,
        set_index: int,
        set_name: str,
        bubble_order: int,
        event_type: str,
    ) -> None:
        """Log bubble activation events for visualization."""
        self.recent_bubble_activations.append({
            "timestamp": self._isoformat(snap.timestamp),
            "vehicle_id": vid,
            "vehicle_name": snap.vehicle_name,
            "stop_id": stop.stop_id,
            "set_index": set_index,
            "set_name": set_name,
            "bubble_order": bubble_order,
            "event_type": event_type,
            "lat": snap.lat,
            "lon": snap.lon,
        })

    def get_active_bubble_states(self) -> List[Dict[str, Any]]:
        """Get current bubble progress states for all vehicles (for API)."""
        states = []
        for vid, stop_progress in self.vehicle_bubble_progress.items():
            for stop_id, set_progress in stop_progress.items():
                stop = self.stop_lookup.get(stop_id)
                for progress in set_progress.values():
                    states.append({
                        "vehicle_id": vid,
                        "stop_id": stop_id,
                        "set_index": progress.set_index,
                        "set_name": progress.set_name,
                        "highest_bubble_reached": progress.highest_bubble_reached,
                        "max_bubble_order": progress.max_bubble_order,
                        "in_final_bubble": progress.in_final_bubble,
                        "stopped_in_final": progress.stopped_in_final,
                        "arrival_logged": progress.arrival_logged,
                        "last_seen": self._isoformat(progress.last_seen),
                        "entered_at": self._isoformat(progress.entered_at),
                        "bubbles": [
                            {"lat": b.lat, "lon": b.lon, "radius_m": b.radius_m, "order": b.order}
                            for b in (stop.approach_sets[progress.set_index].bubbles if stop and stop.approach_sets else [])
                        ],
                    })
        return states

    def _record_arrival_headways(
        self, route_id: Optional[str], stop_id: Optional[str], timestamp: datetime
    ) -> Tuple[Optional[float], Optional[float]]:
        if stop_id is None:
            return None, None
        keys: List[Tuple[Optional[str], str]] = []
        if route_id is not None:
            keys.append((route_id, stop_id))
        keys.append((None, stop_id))

        prev_arrival = None
        prev_departure = None
        for key in keys:
            if prev_arrival is None:
                prev_arrival = self.last_arrival.get(key)
            if prev_departure is None:
                prev_departure = self.last_departure.get(key)

        # If the tracker was restarted, the in-memory caches will be empty even
        # though earlier events exist on disk. To avoid emitting `null` headways
        # in that case, backfill the most recent arrival/departure from storage
        # when no cached values are present. Keep the lookup narrow (same day)
        # to avoid expensive scans.
        if prev_arrival is None or prev_departure is None:
            day_start = datetime.combine(timestamp.date(), datetime.min.time(), tzinfo=timezone.utc)
            history = self.storage.query_events(
                start=day_start,
                end=timestamp,
                route_ids={route_id} if route_id else None,
                stop_ids={stop_id},
            )
            for event in reversed(history):
                if prev_arrival is None and event.event_type == "arrival":
                    prev_arrival = event.timestamp
                if prev_departure is None and event.event_type == "departure":
                    prev_departure = event.timestamp
                if prev_arrival is not None and prev_departure is not None:
                    break

        for key in keys:
            self.last_arrival[key] = timestamp
        arrival_arrival = None
        departure_arrival = None
        if prev_arrival is not None:
            arrival_arrival = max((timestamp - prev_arrival).total_seconds(), 0.0)
        if prev_departure is not None:
            departure_arrival = max((timestamp - prev_departure).total_seconds(), 0.0)
        return arrival_arrival, departure_arrival

    def _log_stop_association_failure(
        self, snap: VehicleSnapshot, route_id_norm: Optional[str]
    ) -> None:
        diagnosis = self._diagnose_stop_association(snap.lat, snap.lon, route_id_norm, snap.heading_deg)
        detail = {
            "timestamp": self._isoformat(snap.timestamp),
            "vehicle_id": self._normalize_id(snap.vehicle_id),
            "vehicle_name": snap.vehicle_name,
            "route_id": route_id_norm,
            "lat": snap.lat,
            "lon": snap.lon,
            "heading_deg": snap.heading_deg,
        }
        detail.update(diagnosis)
        self.recent_stop_association_failures.append(detail)

    def _diagnose_stop_association(
        self,
        lat: float,
        lon: float,
        route_id: Optional[str],
        heading_deg: Optional[float],
    ) -> dict:
        """Simple distance-based stop association diagnosis."""
        if not self.stops:
            return {"reason": "no_stops"}

        nearest_any: Optional[Tuple[StopPoint, float]] = None
        nearest_route_mismatch: Optional[Tuple[StopPoint, float]] = None

        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            dist = self._haversine(lat, lon, stop.lat, stop.lon)

            if nearest_any is None or dist < nearest_any[1]:
                nearest_any = (stop, dist)

            if route_id and stop.route_ids and route_id not in stop.route_ids:
                if dist <= self.arrival_distance_threshold_m and (
                    nearest_route_mismatch is None or dist < nearest_route_mismatch[1]
                ):
                    nearest_route_mismatch = (stop, dist)

        if nearest_route_mismatch is not None:
            stop, dist = nearest_route_mismatch
            return {
                "reason": "route_mismatch",
                "nearest_stop_id": stop.stop_id,
                "nearest_stop_route_ids": sorted(stop.route_ids),
                "distance_m": dist,
                "threshold_m": self.arrival_distance_threshold_m,
            }
        if nearest_any is not None:
            stop, dist = nearest_any
            return {
                "reason": "beyond_distance",
                "nearest_stop_id": stop.stop_id,
                "nearest_stop_route_ids": sorted(stop.route_ids),
                "distance_m": dist,
                "threshold_m": self.arrival_distance_threshold_m,
            }
        return {"reason": "unknown"}

    def _nearest_stop(
        self,
        lat: float,
        lon: float,
        route_id: Optional[str],
        *,
        threshold: float,
        heading_deg: Optional[float] = None,
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Find nearest stop within threshold distance. Used for departure tracking only."""
        best: Optional[Tuple[str, Optional[str], float]] = None
        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            if route_id and self.tracked_route_ids and route_id not in stop.route_ids and stop.route_ids:
                continue
            if route_id and stop.route_ids and route_id not in stop.route_ids:
                continue
            dist = self._haversine(lat, lon, stop.lat, stop.lon)
            if dist <= threshold:
                if best is None or dist < best[2]:
                    associated_route = route_id
                    if not associated_route and stop.route_ids:
                        associated_route = next(iter(stop.route_ids))
                    best = (stop.stop_id, associated_route, dist)
        if best is None:
            return None
        return best[0], best[1]

    def _extract_route_ids(self, stop: dict) -> Set[str]:
        route_ids: Set[str] = set()
        for key in ("RouteIds", "RouteIDs"):
            vals = stop.get(key)
            if isinstance(vals, list):
                for val in vals:
                    rid = self._normalize_id(val)
                    if rid is not None:
                        route_ids.add(rid)
        routes_raw = stop.get("Routes")
        if isinstance(routes_raw, list):
            for entry in routes_raw:
                if isinstance(entry, dict):
                    rid = self._normalize_id(entry.get("RouteID") or entry.get("RouteId"))
                    if rid is not None:
                        route_ids.add(rid)
        rid_single = self._normalize_id(stop.get("RouteID") or stop.get("RouteId"))
        if rid_single is not None:
            route_ids.add(rid_single)
        return route_ids

    def _normalize_id(self, value: Optional[object]) -> Optional[str]:
        if value is None:
            return None
        try:
            if isinstance(value, str):
                text = value.strip()
                return text if text else None
            return str(int(value))
        except Exception:
            try:
                return str(value)
            except Exception:
                return None

    def _isoformat(self, dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _destination_point(self, lat_deg: float, lon_deg: float, bearing_deg: float, distance_m: float) -> Tuple[float, float]:
        radius_earth = 6371000.0
        bearing_rad = math.radians(bearing_deg)
        lat1 = math.radians(lat_deg)
        lon1 = math.radians(lon_deg)
        ratio = distance_m / radius_earth

        lat2 = math.asin(
            math.sin(lat1) * math.cos(ratio)
            + math.cos(lat1) * math.sin(ratio) * math.cos(bearing_rad)
        )
        lon2 = lon1 + math.atan2(
            math.sin(bearing_rad) * math.sin(ratio) * math.cos(lat1),
            math.cos(ratio) - math.sin(lat1) * math.sin(lat2),
        )

        return math.degrees(lat2), math.degrees(lon2)

    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        return 2 * r_earth * math.asin(math.sqrt(a))

    def _bearing_degrees(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlon = math.radians(lon2 - lon1)
        x = math.sin(dlon) * math.cos(lat2_rad)
        y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)
        bearing = math.degrees(math.atan2(x, y))
        return (bearing + 360.0) % 360.0

    def _distance_to_stop(self, stop_id: Optional[str], lat: float, lon: float) -> Optional[float]:
        if stop_id is None:
            return None
        stop = self.stop_lookup.get(stop_id)
        if stop is None:
            return None
        return self._haversine(lat, lon, stop.lat, stop.lon)

    def _calculate_filtered_speed(
        self, speed_history: List[float], current_speed: Optional[float]
    ) -> Optional[float]:
        """Calculate GPS noise-filtered speed using median of recent samples."""
        if current_speed is None:
            return None

        # Add current speed to history
        speed_history.append(current_speed)

        # Keep only recent samples
        while len(speed_history) > SPEED_HISTORY_SIZE:
            speed_history.pop(0)

        # Use median to filter out GPS noise spikes
        if len(speed_history) >= 2:
            sorted_speeds = sorted(speed_history)
            mid = len(sorted_speeds) // 2
            if len(sorted_speeds) % 2 == 0:
                return (sorted_speeds[mid - 1] + sorted_speeds[mid]) / 2.0
            else:
                return sorted_speeds[mid]
        else:
            return current_speed


def load_headway_config(path: Path = DEFAULT_HEADWAY_CONFIG_PATH) -> Tuple[Set[str], Set[str]]:
    route_ids: Set[str] = set()
    stop_ids: Set[str] = set()
    if path.exists():
        try:
            raw = json.loads(path.read_text())
            route_ids_raw = raw.get("route_ids") if isinstance(raw, dict) else None
            stop_ids_raw = raw.get("stop_ids") if isinstance(raw, dict) else None
            if isinstance(route_ids_raw, list):
                for item in route_ids_raw:
                    if item is None:
                        continue
                    route_ids.add(str(item).strip())
            if isinstance(stop_ids_raw, list):
                for item in stop_ids_raw:
                    if item is None:
                        continue
                    stop_ids.add(str(item).strip())
        except Exception as exc:
            print(f"[headway] failed to load config {path}: {exc}")
    return route_ids, stop_ids


def _read_data_file(
    path: Path,
    *,
    data_dirs: Optional[Sequence[Path]] = None,
) -> Tuple[Optional[Path], Optional[str]]:
    if data_dirs is None:
        data_dirs = DEFAULT_DATA_DIRS
    path_obj = Path(path)
    candidates: List[Path]
    if path_obj.is_absolute():
        candidates = [path_obj]
    else:
        candidates = [base / path_obj for base in data_dirs]
        candidates.append(path_obj)
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            return candidate, candidate.read_text()
        except Exception as exc:
            print(f"[headway] failed to read data file {candidate}: {exc}")
            return candidate, None
    return None, None


def load_approach_sets_config(
    path: Path = DEFAULT_STOP_APPROACH_CONFIG_PATH,
    *,
    data_dirs: Optional[Sequence[Path]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Load approach bubble sets from the stop approach config file."""
    config: Dict[str, List[Dict[str, Any]]] = {}
    resolved_path, raw_text = _read_data_file(path, data_dirs=data_dirs)
    if not raw_text:
        return config
    try:
        raw = json.loads(raw_text)
        if isinstance(raw, dict):
            for stop_id, entry in raw.items():
                if not isinstance(entry, dict):
                    continue
                approach_sets = entry.get("approach_sets")
                if approach_sets and isinstance(approach_sets, list):
                    config[str(stop_id)] = approach_sets
    except Exception as exc:
        print(f"[headway] failed to load approach sets config {resolved_path or path}: {exc}")
    return config


def _parse_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
