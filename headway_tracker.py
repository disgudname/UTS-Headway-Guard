from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from collections import deque
import json
import math
import os

from headway_storage import HeadwayEvent, HeadwayStorage


# Configuration constants
DEFAULT_HEADWAY_CONFIG_PATH = Path("config/headway_config.json")
DEFAULT_STOP_APPROACH_CONFIG_PATH = Path("config/stop_approach.json")
DEFAULT_DATA_DIRS = [Path(p) for p in os.getenv("DATA_DIRS", "/data").split(":")]

# Distance threshold for headway tracking (meters)
HEADWAY_DISTANCE_THRESHOLD_M = float(60.0)

# Speed threshold for considering a bus "stopped" (meters per second)
STOP_SPEED_THRESHOLD_MPS = 0.5

# How long before we drop stale bubble tracking state
BUBBLE_PROGRESS_STALE_SECONDS = 120.0

# Distance from final bubble at which we abandon tracking (meters)
# Allows buses to temporarily exit bubbles (GPS drift) and re-enter
APPROACH_ABANDONMENT_DISTANCE_M = 400.0


@dataclass
class VehicleSnapshot:
    """A single position report for a vehicle."""
    vehicle_id: Optional[str]
    vehicle_name: Optional[str]
    lat: float
    lon: float
    route_id: Optional[str]
    timestamp: datetime  # This should be the fetch start time, NOT TransLoc's timestamp
    heading_deg: Optional[float] = None
    block: Optional[str] = None


@dataclass
class ApproachBubble:
    """A single bubble in an approach set."""
    lat: float
    lon: float
    radius_m: float
    order: int


@dataclass
class ApproachSet:
    """An ordered set of approach bubbles for a stop."""
    name: str
    bubbles: List[ApproachBubble]


@dataclass
class StopPoint:
    """A transit stop with optional approach bubble sets."""
    stop_id: str
    lat: float
    lon: float
    route_ids: Set[str]
    approach_sets: Optional[List[ApproachSet]] = None
    address_id: Optional[str] = None  # Physical location ID - same address_id = same physical stop
    stop_name: Optional[str] = None  # Human-readable stop name


@dataclass
class BubbleProgressState:
    """
    Tracks a vehicle's progress through an approach bubble set.

    State machine:
    1. Vehicle enters bubble #1 -> tracking begins
    2. Vehicle progresses through bubbles in order (1 -> 2 -> ... -> N)
    3. Vehicle can temporarily exit bubbles (GPS drift) and re-enter
    4. Tracking is only abandoned if vehicle goes 400m+ from final bubble
    5. Vehicle enters final bubble (N):
       - If bus stops -> log arrival immediately
       - If bus exits without stopping -> log arrival at exit time
    6. Vehicle exits final bubble -> log departure
    """
    stop_id: str
    set_index: int
    set_name: str
    max_bubble_order: int  # The highest bubble order in this set (the final/stop bubble)
    route_id: Optional[str]
    entered_at: datetime  # When first bubble was entered
    last_seen: datetime  # Last time we saw this vehicle in any bubble of this set

    # Progress tracking
    highest_bubble_reached: int = 1  # Highest bubble order number reached
    next_expected_order: int = 2  # Next bubble order required for valid progression

    # Final bubble location (for abandonment distance check)
    final_bubble_lat: Optional[float] = None
    final_bubble_lon: Optional[float] = None

    # Final bubble state
    in_final_bubble: bool = False
    entered_final_at: Optional[datetime] = None

    # Arrival tracking
    stopped_in_final: bool = False  # Has the bus stopped in the final bubble?
    arrival_logged: bool = False
    arrival_time: Optional[datetime] = None  # When arrival was logged

    # Departure tracking
    departure_logged: bool = False


class HeadwayTracker:
    """
    Tracks vehicle arrivals and departures at stops using approach bubbles.

    Arrival detection:
    - Method 1: Bus traverses bubbles in order, stops in final bubble -> arrival logged when stopped
    - Method 2: Bus traverses bubbles in order, passes through final bubble -> arrival logged when exiting

    Departure detection:
    - Logged when bus exits the final bubble (after arrival has been logged)

    All timestamps use the fetch start time from TransLoc, not TransLoc's internal timestamps.
    """

    def __init__(
        self,
        storage: HeadwayStorage,
        *,
        arrival_distance_threshold_m: float = 60.0,
        departure_distance_threshold_m: float = 60.0,
        tracked_route_ids: Optional[Set[str]] = None,
        tracked_stop_ids: Optional[Set[str]] = None,
        route_name_lookup: Optional[Callable[[Optional[str]], Optional[str]]] = None,
        vehicle_block_lookup: Optional[Callable[[Optional[str]], Optional[str]]] = None,
    ):
        self.storage = storage
        self.arrival_distance_threshold_m = arrival_distance_threshold_m
        self.departure_distance_threshold_m = departure_distance_threshold_m
        self.tracked_route_ids = tracked_route_ids or set()
        self.tracked_stop_ids = tracked_stop_ids or set()

        # Lookup callbacks for enriching events with display names
        # route_name_lookup: (route_id) -> route_name (e.g., "Orange Line")
        # vehicle_block_lookup: (vehicle_id) -> block (e.g., "[06]")
        self.route_name_lookup = route_name_lookup
        self.vehicle_block_lookup = vehicle_block_lookup

        # Stop data
        self.stops: List[StopPoint] = []
        self.stop_lookup: Dict[str, StopPoint] = {}

        # Bubble progress tracking: {vehicle_id: {stop_id: {set_idx: BubbleProgressState}}}
        self.vehicle_bubble_progress: Dict[str, Dict[str, Dict[int, BubbleProgressState]]] = {}

        # Last snapshots for speed calculation
        self.last_snapshots: Dict[str, VehicleSnapshot] = {}

        # Headway calculation: track last arrival/departure times
        self.last_arrival: Dict[Tuple[Optional[str], str], datetime] = {}  # (route_id, stop_id) -> time
        self.last_departure: Dict[Tuple[Optional[str], str], datetime] = {}
        self.last_vehicle_arrival: Dict[Tuple[str, str, Optional[str]], datetime] = {}  # (vid, stop_id, route_id) -> time
        self.last_vehicle_departure: Dict[Tuple[str, str, Optional[str]], datetime] = {}

        # Diagnostics
        self.recent_stop_association_failures: deque = deque(maxlen=25)
        self.recent_snapshot_diagnostics: deque = deque(maxlen=50)
        self.recent_bubble_activations: deque = deque(maxlen=100)

        # Legacy compatibility - these aren't used in new logic but kept for API compatibility
        self.vehicle_states: Dict[str, Any] = {}
        self.final_bubble_exits: Dict[Tuple[str, str], datetime] = {}

        print(
            f"[headway] tracker initialized routes={sorted(self.tracked_route_ids) if self.tracked_route_ids else 'all'} "
            f"stops={sorted(self.tracked_stop_ids) if self.tracked_stop_ids else 'all'}"
        )

    def update_stops(self, stops: Iterable[dict]) -> None:
        """Update the list of stops from TransLoc data."""
        # First pass: collect all stop data and group by AddressID (physical location)
        # AddressID is the key unifier - same AddressID = same physical stop
        address_groups: Dict[str, Dict[str, Any]] = {}  # address_id -> merged stop data

        for stop in stops:
            stop_id = stop.get("StopID") or stop.get("StopId") or stop.get("RouteStopID")
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

            # Get AddressID - the physical location identifier
            address_id = stop.get("AddressID") or stop.get("AddressId")
            if address_id is not None:
                address_id = str(address_id)

            # Use AddressID when available; otherwise keep it as None without fallbacks.
            address_key = address_id if address_id is not None else str(stop_id)

            route_ids = self._extract_route_ids(stop)
            approach_sets = self._parse_approach_sets(stop.get("ApproachSets"))

            # Get stop name
            stop_name = stop.get("Name") or stop.get("StopName") or stop.get("Description")
            if stop_name:
                stop_name = str(stop_name).strip()

            if address_key not in address_groups:
                address_groups[address_key] = {
                    "stop_id": str(stop_id),  # Use the first stop_id we see
                    "lat": lat_f,
                    "lon": lon_f,
                    "route_ids": set(),
                    "approach_sets": [],
                    "address_id": address_id,
                    "stop_name": stop_name,  # Use the first non-empty name we see
                }
            elif stop_name and not address_groups[address_key].get("stop_name"):
                # Use the first non-empty stop name we encounter
                address_groups[address_key]["stop_name"] = stop_name

            # Merge route IDs from all stops at this physical location
            address_groups[address_key]["route_ids"].update(route_ids)

            # Merge approach sets (avoid duplicates)
            if approach_sets:
                existing_sets = address_groups[address_key]["approach_sets"]
                for new_set in approach_sets:
                    # Check if this approach set is already added (by name)
                    if not any(s.name == new_set.name for s in existing_sets):
                        existing_sets.append(new_set)

        # Create StopPoint objects from merged address groups
        updated: List[StopPoint] = []
        for addr_id, data in address_groups.items():
            updated.append(
                StopPoint(
                    stop_id=data["stop_id"],
                    lat=data["lat"],
                    lon=data["lon"],
                    route_ids=data["route_ids"],
                    approach_sets=data["approach_sets"] if data["approach_sets"] else None,
                    address_id=data["address_id"],
                    stop_name=data.get("stop_name"),
                )
            )

        self.stops = updated
        self.stop_lookup = {stop.stop_id: stop for stop in updated}
        # Also create lookup by address_id for quick access
        self.address_lookup: Dict[str, StopPoint] = {stop.address_id: stop for stop in updated if stop.address_id}
        if not updated:
            print("[headway] stop update received no stops; tracker inputs unavailable")
        else:
            print(f"[headway] loaded {len(updated)} physical stops from {len(list(stops))} stop entries")

    def _parse_approach_sets(self, raw_sets: Any) -> Optional[List[ApproachSet]]:
        """Parse approach sets from stop data."""
        if not raw_sets or not isinstance(raw_sets, list):
            return None

        approach_sets: List[ApproachSet] = []
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
                bubbles.sort(key=lambda b: b.order)
                approach_sets.append(ApproachSet(name=set_name, bubbles=bubbles))

        return approach_sets if approach_sets else None

    def process_snapshots(self, snapshots: Sequence[VehicleSnapshot]) -> None:
        """Process a batch of vehicle snapshots and detect arrivals/departures."""
        if not self.stops:
            return

        events: List[HeadwayEvent] = []

        # Deduplicate snapshots by vehicle_id - same vehicle can appear multiple times
        # if it's assigned to multiple routes. We only want to process each vehicle once.
        seen_vehicle_ids: Set[str] = set()

        for snap in snapshots:
            if snap.lat is None or snap.lon is None:
                continue

            vid = self._normalize_id(snap.vehicle_id)
            if vid is None:
                continue

            # Skip if we've already processed this vehicle in this batch
            if vid in seen_vehicle_ids:
                continue
            seen_vehicle_ids.add(vid)

            route_id = self._normalize_id(snap.route_id)
            if self.tracked_route_ids and (route_id is None or route_id not in self.tracked_route_ids):
                continue

            # Normalize timestamp to UTC
            timestamp = snap.timestamp
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = timestamp.astimezone(timezone.utc)
            snap.timestamp = timestamp

            # Calculate speed from previous snapshot
            speed_mps = self._calculate_speed(vid, snap)

            # Process bubble-based arrival/departure detection
            new_events = self._process_bubble_tracking(vid, snap, route_id, speed_mps)
            events.extend(new_events)

            # Log diagnostics
            self._log_diagnostics(vid, snap, route_id, speed_mps)

            # Save snapshot for next speed calculation
            self.last_snapshots[vid] = snap

        # Write events to storage
        if events:
            try:
                self.storage.write_events(events)
                print(f"[headway] recorded {len(events)} events")
            except Exception as exc:
                print(f"[headway] failed to write events: {exc}")

    def _calculate_speed(self, vid: str, snap: VehicleSnapshot) -> Optional[float]:
        """Calculate speed in m/s based on distance from previous snapshot."""
        prev_snap = self.last_snapshots.get(vid)
        if not prev_snap:
            return None

        delta_seconds = (snap.timestamp - prev_snap.timestamp).total_seconds()
        if delta_seconds <= 0:
            return None

        distance = self._haversine(prev_snap.lat, prev_snap.lon, snap.lat, snap.lon)
        return distance / delta_seconds

    def _process_bubble_tracking(
        self,
        vid: str,
        snap: VehicleSnapshot,
        route_id: Optional[str],
        speed_mps: Optional[float],
    ) -> List[HeadwayEvent]:
        """
        Process bubble tracking for a vehicle and return any arrival/departure events.

        Logic:
        1. For each stop with approach sets, check which bubbles the vehicle is in
        2. Track progression through bubbles (must go through in order starting from #1)
        3. When in final bubble:
           - If stopped (speed <= threshold): log arrival immediately
           - If not stopped: wait until exit to log arrival
        4. When exiting final bubble: log departure (if arrival was logged)

        Deduplication:
        - Only ONE arrival per vehicle per stop until departure
        - Only ONE departure per vehicle per stop after arrival
        - Multiple approach sets at the same stop share arrival/departure state
        """
        events: List[HeadwayEvent] = []
        timestamp = snap.timestamp

        # Get or create vehicle's bubble progress tracking
        vehicle_progress = self.vehicle_bubble_progress.get(vid, {})

        # Clean up stale tracking states
        self._cleanup_stale_progress(vehicle_progress, timestamp)

        # Track which stops we're currently tracking
        active_stop_ids: Set[str] = set()

        # Track which stops have already logged arrival/departure THIS processing cycle
        # to prevent multiple approach sets from logging duplicates
        stops_with_arrival_this_cycle: Set[str] = set()
        stops_with_departure_this_cycle: Set[str] = set()

        for stop in self.stops:
            if not stop.approach_sets:
                continue
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue

            # IMPORTANT: Only process stops that this bus's route actually serves
            # This prevents logging arrivals when a bus passes a stop it doesn't serve
            if route_id and stop.route_ids and route_id not in stop.route_ids:
                continue

            stop_progress = vehicle_progress.get(stop.stop_id, {})

            for set_idx, approach_set in enumerate(stop.approach_sets):
                if not approach_set.bubbles:
                    continue

                max_order = max(b.order for b in approach_set.bubbles)

                # Find which bubbles the vehicle is currently in
                current_bubbles = self._get_bubbles_vehicle_is_in(snap, approach_set)

                progress = stop_progress.get(set_idx)

                # Find the final bubble for this approach set (for abandonment distance check)
                final_bubble = next((b for b in approach_set.bubbles if b.order == max_order), None)

                if current_bubbles:
                    active_stop_ids.add(stop.stop_id)

                    if progress is None:
                        # Start new tracking only if vehicle is in bubble #1
                        if 1 in current_bubbles:
                            progress = BubbleProgressState(
                                stop_id=stop.stop_id,
                                set_index=set_idx,
                                set_name=approach_set.name,
                                max_bubble_order=max_order,
                                route_id=route_id,
                                entered_at=timestamp,
                                last_seen=timestamp,
                                highest_bubble_reached=1,
                                next_expected_order=2,
                                final_bubble_lat=final_bubble.lat if final_bubble else None,
                                final_bubble_lon=final_bubble.lon if final_bubble else None,
                            )
                            stop_progress[set_idx] = progress
                            self._log_bubble_activation(vid, snap, stop, set_idx, approach_set.name, 1, "entered")
                    else:
                        # Update existing tracking
                        progress.last_seen = timestamp

                        # Progress through bubbles in order
                        while (progress.next_expected_order in current_bubbles and
                               progress.next_expected_order <= max_order):
                            progress.highest_bubble_reached = progress.next_expected_order
                            self._log_bubble_activation(
                                vid, snap, stop, set_idx, approach_set.name,
                                progress.highest_bubble_reached, "progressed"
                            )
                            progress.next_expected_order += 1

                        # Check if we're in the final bubble
                        in_final = (max_order in current_bubbles and
                                   progress.highest_bubble_reached == max_order)

                        if in_final:
                            if not progress.in_final_bubble:
                                # Just entered final bubble
                                progress.in_final_bubble = True
                                progress.entered_final_at = timestamp
                                self._log_bubble_activation(
                                    vid, snap, stop, set_idx, approach_set.name,
                                    max_order, "entered_final"
                                )

                            # Check if bus has stopped
                            if speed_mps is not None and speed_mps <= STOP_SPEED_THRESHOLD_MPS:
                                if not progress.stopped_in_final:
                                    progress.stopped_in_final = True

                                    # Log arrival when bus stops (Method 1)
                                    # Only log if we haven't already logged arrival for this stop this cycle
                                    if not progress.arrival_logged and stop.stop_id not in stops_with_arrival_this_cycle:
                                        progress.arrival_logged = True
                                        progress.arrival_time = timestamp
                                        stops_with_arrival_this_cycle.add(stop.stop_id)
                                        event = self._create_arrival_event(
                                            vid, snap, stop.stop_id, route_id, timestamp,
                                            arrival_type="stopped"
                                        )
                                        events.append(event)
                                        self._log_bubble_activation(
                                            vid, snap, stop, set_idx, approach_set.name,
                                            max_order, "arrival_stopped"
                                        )
                        else:
                            # Not in final bubble anymore (was in final, now not)
                            if progress.in_final_bubble:
                                # Exiting final bubble
                                if not progress.arrival_logged and stop.stop_id not in stops_with_arrival_this_cycle:
                                    # Log arrival at exit time (Method 2 - pass-through)
                                    progress.arrival_logged = True
                                    progress.arrival_time = timestamp
                                    stops_with_arrival_this_cycle.add(stop.stop_id)
                                    event = self._create_arrival_event(
                                        vid, snap, stop.stop_id, route_id, timestamp,
                                        arrival_type="passthrough"
                                    )
                                    events.append(event)
                                    self._log_bubble_activation(
                                        vid, snap, stop, set_idx, approach_set.name,
                                        max_order, "arrival_passthrough"
                                    )

                                # Log departure - only if not already logged for this stop this cycle
                                if progress.arrival_logged and not progress.departure_logged and stop.stop_id not in stops_with_departure_this_cycle:
                                    progress.departure_logged = True
                                    stops_with_departure_this_cycle.add(stop.stop_id)
                                    dwell = self._calculate_dwell(progress.arrival_time, timestamp)
                                    event = self._create_departure_event(
                                        vid, snap, stop.stop_id, progress.route_id, timestamp, dwell
                                    )
                                    events.append(event)
                                    self._log_bubble_activation(
                                        vid, snap, stop, set_idx, approach_set.name,
                                        max_order, "departure"
                                    )

                                progress.in_final_bubble = False

                else:
                    # Vehicle is not in any bubble for this set
                    if progress:
                        # Check distance to final bubble - only abandon if > 400m away
                        distance_to_final = None
                        if progress.final_bubble_lat is not None and progress.final_bubble_lon is not None:
                            distance_to_final = self._haversine(
                                snap.lat, snap.lon,
                                progress.final_bubble_lat, progress.final_bubble_lon
                            )

                        should_abandon = (
                            distance_to_final is not None and
                            distance_to_final > APPROACH_ABANDONMENT_DISTANCE_M
                        )

                        if progress.in_final_bubble:
                            # Was in final bubble, now out - log arrival/departure
                            if not progress.arrival_logged and stop.stop_id not in stops_with_arrival_this_cycle:
                                # Log pass-through arrival (Method 2)
                                progress.arrival_logged = True
                                progress.arrival_time = timestamp
                                stops_with_arrival_this_cycle.add(stop.stop_id)
                                event = self._create_arrival_event(
                                    vid, snap, stop.stop_id, route_id, timestamp,
                                    arrival_type="passthrough"
                                )
                                events.append(event)
                                self._log_bubble_activation(
                                    vid, snap, stop, set_idx, approach_set.name,
                                    progress.max_bubble_order, "arrival_passthrough"
                                )

                            # Log departure - only if not already logged for this stop this cycle
                            if progress.arrival_logged and not progress.departure_logged and stop.stop_id not in stops_with_departure_this_cycle:
                                progress.departure_logged = True
                                stops_with_departure_this_cycle.add(stop.stop_id)
                                dwell = self._calculate_dwell(progress.arrival_time, timestamp)
                                event = self._create_departure_event(
                                    vid, snap, stop.stop_id, progress.route_id, timestamp, dwell
                                )
                                events.append(event)
                                self._log_bubble_activation(
                                    vid, snap, stop, set_idx, approach_set.name,
                                    progress.max_bubble_order, "departure"
                                )

                            # Clear the in_final_bubble flag but keep tracking
                            progress.in_final_bubble = False

                            # Only fully abandon if we've completed arrival/departure or are too far away
                            if (progress.arrival_logged and progress.departure_logged) or should_abandon:
                                self._log_bubble_activation(vid, snap, stop, set_idx, approach_set.name, 0, "exited")
                                stop_progress.pop(set_idx, None)

                        elif should_abandon:
                            # Not in final bubble and too far away - abandon tracking
                            self._log_bubble_activation(vid, snap, stop, set_idx, approach_set.name, 0, "abandoned")
                            stop_progress.pop(set_idx, None)
                        # else: Vehicle temporarily outside bubbles but within 400m - keep tracking

                # Update stop progress
                if stop_progress:
                    vehicle_progress[stop.stop_id] = stop_progress
                elif stop.stop_id in vehicle_progress:
                    vehicle_progress.pop(stop.stop_id, None)

        # Update vehicle progress
        if vehicle_progress:
            self.vehicle_bubble_progress[vid] = vehicle_progress
        elif vid in self.vehicle_bubble_progress:
            del self.vehicle_bubble_progress[vid]

        return events

    def _get_bubbles_vehicle_is_in(self, snap: VehicleSnapshot, approach_set: ApproachSet) -> Set[int]:
        """Return the set of bubble order numbers the vehicle is currently inside."""
        bubbles_in: Set[int] = set()
        for bubble in approach_set.bubbles:
            dist = self._haversine(snap.lat, snap.lon, bubble.lat, bubble.lon)
            if dist <= bubble.radius_m:
                bubbles_in.add(bubble.order)
        return bubbles_in

    def _cleanup_stale_progress(
        self,
        vehicle_progress: Dict[str, Dict[int, BubbleProgressState]],
        current_time: datetime
    ) -> None:
        """Remove stale bubble tracking states."""
        stale_stop_ids: List[str] = []
        for stop_id, set_progress in vehicle_progress.items():
            stale_set_indices: List[int] = []
            for set_idx, progress in set_progress.items():
                age = (current_time - progress.last_seen).total_seconds()
                if age > BUBBLE_PROGRESS_STALE_SECONDS:
                    stale_set_indices.append(set_idx)

            for set_idx in stale_set_indices:
                set_progress.pop(set_idx, None)

            if not set_progress:
                stale_stop_ids.append(stop_id)

        for stop_id in stale_stop_ids:
            vehicle_progress.pop(stop_id, None)

    def _create_arrival_event(
        self,
        vid: str,
        snap: VehicleSnapshot,
        stop_id: str,
        route_id: Optional[str],
        timestamp: datetime,
        arrival_type: str = "stopped",
    ) -> HeadwayEvent:
        """Create an arrival event and update headway tracking.

        Args:
            arrival_type: "stopped" if bus stopped in final bubble, "passthrough" if it passed through
        """
        # Check for duplicate
        arrival_key = (vid, stop_id, route_id)
        prev_arrival = self.last_vehicle_arrival.get(arrival_key)
        prev_departure = self.last_vehicle_departure.get(arrival_key)

        # Calculate headways
        headway_aa, headway_da = self._calculate_headways(route_id, stop_id, timestamp)

        # Update tracking
        self._update_arrival_tracking(vid, stop_id, route_id, timestamp)

        # Look up enrichment data
        route_name = self.route_name_lookup(route_id) if self.route_name_lookup else None
        block = snap.block
        if block is None and self.vehicle_block_lookup:
            block = self.vehicle_block_lookup(vid)

        # Get stop info from lookup
        stop_point = self.stop_lookup.get(stop_id)
        address_id = stop_point.address_id if stop_point else None
        stop_name = stop_point.stop_name if stop_point else None

        print(f"[headway] arrival: vehicle={vid} stop={stop_id} route={route_id} block={block} type={arrival_type}")

        return HeadwayEvent(
            timestamp=timestamp,
            route_id=route_id,
            stop_id=stop_id,
            vehicle_id=vid,
            vehicle_name=snap.vehicle_name,
            event_type="arrival",
            headway_arrival_arrival=headway_aa,
            headway_departure_arrival=headway_da,
            dwell_seconds=None,
            route_name=route_name,
            address_id=address_id,
            stop_name=stop_name,
            block=block,
            arrival_type=arrival_type,
        )

    def _create_departure_event(
        self,
        vid: str,
        snap: VehicleSnapshot,
        stop_id: str,
        route_id: Optional[str],
        timestamp: datetime,
        dwell_seconds: Optional[float],
    ) -> HeadwayEvent:
        """Create a departure event and update tracking."""
        # Update tracking
        self._update_departure_tracking(vid, stop_id, route_id, timestamp)

        # Look up enrichment data
        route_name = self.route_name_lookup(route_id) if self.route_name_lookup else None
        block = snap.block
        if block is None and self.vehicle_block_lookup:
            block = self.vehicle_block_lookup(vid)

        # Get stop info from lookup
        stop_point = self.stop_lookup.get(stop_id)
        address_id = stop_point.address_id if stop_point else None
        stop_name = stop_point.stop_name if stop_point else None

        print(f"[headway] departure: vehicle={vid} stop={stop_id} dwell={dwell_seconds:.1f}s block={block}" if dwell_seconds else f"[headway] departure: vehicle={vid} stop={stop_id} block={block}")

        return HeadwayEvent(
            timestamp=timestamp,
            route_id=route_id,
            stop_id=stop_id,
            vehicle_id=vid,
            vehicle_name=snap.vehicle_name,
            event_type="departure",
            headway_arrival_arrival=None,
            headway_departure_arrival=None,
            dwell_seconds=dwell_seconds,
            route_name=route_name,
            address_id=address_id,
            stop_name=stop_name,
            block=block,
        )

    def _calculate_dwell(self, arrival_time: Optional[datetime], departure_time: datetime) -> Optional[float]:
        """Calculate dwell time in seconds."""
        if arrival_time is None:
            return None
        dwell = (departure_time - arrival_time).total_seconds()
        return max(dwell, 0.0)

    def _calculate_headways(
        self,
        route_id: Optional[str],
        stop_id: str,
        timestamp: datetime
    ) -> Tuple[Optional[float], Optional[float]]:
        """Calculate arrival-to-arrival and departure-to-arrival headways."""
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

        # Backfill from storage if needed
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

        headway_aa = None
        headway_da = None
        if prev_arrival is not None:
            headway_aa = max((timestamp - prev_arrival).total_seconds(), 0.0)
        if prev_departure is not None:
            headway_da = max((timestamp - prev_departure).total_seconds(), 0.0)

        return headway_aa, headway_da

    def _update_arrival_tracking(
        self,
        vid: str,
        stop_id: str,
        route_id: Optional[str],
        timestamp: datetime
    ) -> None:
        """Update arrival tracking dictionaries."""
        keys: List[Tuple[Optional[str], str]] = []
        if route_id is not None:
            keys.append((route_id, stop_id))
        keys.append((None, stop_id))

        for key in keys:
            self.last_arrival[key] = timestamp

        self.last_vehicle_arrival[(vid, stop_id, route_id)] = timestamp

    def _update_departure_tracking(
        self,
        vid: str,
        stop_id: str,
        route_id: Optional[str],
        timestamp: datetime
    ) -> None:
        """Update departure tracking dictionaries."""
        keys: List[Tuple[Optional[str], str]] = []
        if route_id is not None:
            keys.append((route_id, stop_id))
        keys.append((None, stop_id))

        for key in keys:
            self.last_departure[key] = timestamp

        self.last_vehicle_departure[(vid, stop_id, route_id)] = timestamp

        # Also update for any route_id variant
        for arrival_key in list(self.last_vehicle_arrival.keys()):
            if arrival_key[0] == vid and arrival_key[1] == stop_id:
                self.last_vehicle_departure[arrival_key] = timestamp

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
            "stop_name": stop.stop_name,
            "set_index": set_index,
            "set_name": set_name,
            "bubble_order": bubble_order,
            "event_type": event_type,
            "lat": snap.lat,
            "lon": snap.lon,
        })

    def _log_diagnostics(
        self,
        vid: str,
        snap: VehicleSnapshot,
        route_id: Optional[str],
        speed_mps: Optional[float],
    ) -> None:
        """Log diagnostic information for debugging."""
        # Find nearest stop for diagnostics
        nearest_stop = self._find_nearest_stop(snap.lat, snap.lon, route_id)

        self.recent_snapshot_diagnostics.append({
            "timestamp": self._isoformat(snap.timestamp),
            "vehicle_id": vid,
            "vehicle_name": snap.vehicle_name,
            "route_id": route_id,
            "heading_deg": snap.heading_deg,
            "speed_mps": speed_mps,
            "nearest_stop_id": nearest_stop[0] if nearest_stop else None,
            "nearest_stop_distance": nearest_stop[1] if nearest_stop else None,
            "lat": snap.lat,
            "lon": snap.lon,
        })

        # Log stop association failures if no stop is nearby
        if nearest_stop is None or nearest_stop[1] > self.arrival_distance_threshold_m:
            self._log_stop_association_failure(snap, route_id)

    def _find_nearest_stop(
        self,
        lat: float,
        lon: float,
        route_id: Optional[str]
    ) -> Optional[Tuple[str, float]]:
        """Find the nearest stop and its distance."""
        nearest: Optional[Tuple[str, float]] = None
        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            if route_id and stop.route_ids and route_id not in stop.route_ids:
                continue
            dist = self._haversine(lat, lon, stop.lat, stop.lon)
            if nearest is None or dist < nearest[1]:
                nearest = (stop.stop_id, dist)
        return nearest

    def _log_stop_association_failure(self, snap: VehicleSnapshot, route_id: Optional[str]) -> None:
        """Log when a vehicle can't be associated with any stop."""
        diagnosis = self._diagnose_stop_association(snap.lat, snap.lon, route_id)
        detail = {
            "timestamp": self._isoformat(snap.timestamp),
            "vehicle_id": self._normalize_id(snap.vehicle_id),
            "vehicle_name": snap.vehicle_name,
            "route_id": route_id,
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
    ) -> dict:
        """Diagnose why a vehicle couldn't be associated with a stop."""
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

    def get_active_bubble_states(self) -> List[Dict[str, Any]]:
        """Get current bubble progress states for all vehicles (for API)."""
        states = []
        for vid, stop_progress in self.vehicle_bubble_progress.items():
            for stop_id, set_progress in stop_progress.items():
                stop = self.stop_lookup.get(stop_id)
                for progress in set_progress.values():
                    bubbles = []
                    if stop and stop.approach_sets and progress.set_index < len(stop.approach_sets):
                        bubbles = [
                            {"lat": b.lat, "lon": b.lon, "radius_m": b.radius_m, "order": b.order}
                            for b in stop.approach_sets[progress.set_index].bubbles
                        ]
                    # Determine arrival_type for this state
                    # If arrival has been logged, show whether it was stopped or passthrough
                    # If still in progress, show current status
                    arrival_type = None
                    if progress.arrival_logged:
                        arrival_type = "stopped" if progress.stopped_in_final else "passthrough"
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
                        "arrival_type": arrival_type,
                        "last_seen": self._isoformat(progress.last_seen),
                        "entered_at": self._isoformat(progress.entered_at),
                        "bubbles": bubbles,
                    })
        return states

    def _extract_route_ids(self, stop: dict) -> Set[str]:
        """Extract route IDs from a stop dictionary."""
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
        """Normalize an ID value to a string."""
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
        """Format a datetime as ISO8601 UTC string."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate the great-circle distance between two points in meters."""
        r_earth = 6371000.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        return 2 * r_earth * math.asin(math.sqrt(a))


# Configuration loading functions

def load_headway_config(path: Path = DEFAULT_HEADWAY_CONFIG_PATH) -> Tuple[Set[str], Set[str]]:
    """Load headway tracking configuration from JSON file."""
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
    """Read a data file from one of the configured data directories."""
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
    """Parse a value as a float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
