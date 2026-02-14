from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
import math

from fullbus_storage import FullBusEvent, FullBusStorage


# Close an episode if the vehicle disappears from the feed for this long
STALE_EPISODE_TIMEOUT_S = 180.0


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two lat/lon points."""
    r_earth = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * r_earth * math.asin(math.sqrt(a))


@dataclass
class ActiveEpisode:
    start_time: datetime
    vehicle_id: str
    vehicle_name: str
    block: str
    route_id: str
    route_name: str
    nearest_stop_id: str
    nearest_stop_name: str
    lat: float
    lon: float
    capacity: int
    peak_occupation: int
    last_seen: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class FullBusTracker:
    def __init__(
        self,
        storage: FullBusStorage,
        route_name_lookup: Callable[[Optional[str]], Optional[str]],
        vehicle_block_lookup: Callable[[Optional[str]], Optional[str]],
    ):
        self.storage = storage
        self.route_name_lookup = route_name_lookup
        self.vehicle_block_lookup = vehicle_block_lookup
        self.active_episodes: Dict[str, ActiveEpisode] = {}

    def process_cycle(
        self,
        vehicle_capacities: Dict[int, Dict[str, Any]],
        vehicles_raw: List[Dict[str, Any]],
        stops_raw: List[Dict[str, Any]],
        now: datetime,
    ) -> None:
        # Build a quick lookup: vehicle_id -> vehicle record
        vehicle_by_id: Dict[int, Dict[str, Any]] = {}
        for v in vehicles_raw:
            vid = v.get("VehicleID")
            if vid is not None:
                vehicle_by_id[vid] = v

        seen_vehicle_ids: set = set()

        for vid, cap_data in vehicle_capacities.items():
            capacity = cap_data.get("capacity")
            current_occ = cap_data.get("current_occupation")
            if capacity is None or current_occ is None:
                continue
            if capacity <= 0:
                continue

            vid_str = str(vid)
            seen_vehicle_ids.add(vid_str)
            is_full = current_occ >= capacity

            if is_full:
                if vid_str in self.active_episodes:
                    # Update existing episode
                    ep = self.active_episodes[vid_str]
                    if current_occ > ep.peak_occupation:
                        ep.peak_occupation = current_occ
                    ep.last_seen = now
                else:
                    # Start new episode
                    veh = vehicle_by_id.get(vid)
                    if veh is None:
                        # Try string key in case of type mismatch
                        veh = vehicle_by_id.get(str(vid))
                    if veh is None:
                        print(
                            f"[fullbus] WARNING: vehicle {vid} has capacity data "
                            f"but not found in vehicles_raw "
                            f"(vehicles_raw has {len(vehicle_by_id)} entries, "
                            f"keys sample: {list(vehicle_by_id.keys())[:5]})"
                        )
                        veh = {}

                    lat = veh.get("Latitude")
                    lon = veh.get("Longitude")
                    route_id = veh.get("RouteID")
                    vehicle_name = veh.get("Name") or ""

                    # RouteID 0 means unassigned — treat as no route
                    if route_id == 0:
                        route_id = None

                    route_name = ""
                    if route_id is not None:
                        route_name = self.route_name_lookup(str(route_id)) or ""

                    block = self.vehicle_block_lookup(vid_str) or ""

                    nearest_stop_id = ""
                    nearest_stop_name = ""
                    if lat is not None and lon is not None:
                        stop_id, stop_name = self._find_nearest_stop(
                            lat, lon, stops_raw
                        )
                        nearest_stop_id = stop_id or ""
                        nearest_stop_name = stop_name or ""

                    self.active_episodes[vid_str] = ActiveEpisode(
                        start_time=now,
                        vehicle_id=vid_str,
                        vehicle_name=str(vehicle_name),
                        block=block,
                        route_id=str(route_id) if route_id is not None else "",
                        route_name=route_name,
                        nearest_stop_id=nearest_stop_id,
                        nearest_stop_name=nearest_stop_name,
                        lat=lat if lat is not None else 0.0,
                        lon=lon if lon is not None else 0.0,
                        capacity=capacity,
                        peak_occupation=current_occ,
                        last_seen=now,
                    )
                    print(
                        f"[fullbus] new episode: vehicle={vehicle_name or vid_str} "
                        f"route={route_name or route_id} "
                        f"stop={nearest_stop_name or nearest_stop_id} "
                        f"occ={current_occ}/{capacity} "
                        f"lat={lat} lon={lon} "
                        f"veh_found={'yes' if veh else 'NO'}"
                    )
            else:
                # Not full - close episode if one exists
                if vid_str in self.active_episodes:
                    self._close_episode(vid_str, now)

        # Close stale episodes (vehicle disappeared from feed)
        stale_ids = []
        for vid_str, ep in self.active_episodes.items():
            if vid_str not in seen_vehicle_ids:
                elapsed = (now - ep.last_seen).total_seconds()
                if elapsed > STALE_EPISODE_TIMEOUT_S:
                    stale_ids.append(vid_str)

        for vid_str in stale_ids:
            print(f"[fullbus] closing stale episode for vehicle {vid_str}")
            self._close_episode(vid_str, self.active_episodes[vid_str].last_seen)

    def _close_episode(self, vehicle_id: str, end_time: datetime) -> None:
        ep = self.active_episodes.pop(vehicle_id, None)
        if ep is None:
            return
        event = FullBusEvent(
            start_time=ep.start_time,
            end_time=end_time,
            vehicle_id=ep.vehicle_id,
            vehicle_name=ep.vehicle_name,
            block=ep.block,
            route_id=ep.route_id,
            route_name=ep.route_name,
            nearest_stop_id=ep.nearest_stop_id,
            nearest_stop_name=ep.nearest_stop_name,
            lat=ep.lat,
            lon=ep.lon,
            capacity=ep.capacity,
            peak_occupation=ep.peak_occupation,
        )
        try:
            self.storage.write_event(event)
            duration = (end_time - ep.start_time).total_seconds()
            print(
                f"[fullbus] closed episode: vehicle={ep.vehicle_name or ep.vehicle_id} "
                f"duration={duration:.0f}s peak={ep.peak_occupation}/{ep.capacity}"
            )
        except Exception as exc:
            print(f"[fullbus] failed to write event: {exc}")

    def _find_nearest_stop(
        self,
        lat: float,
        lon: float,
        stops_raw: List[Dict[str, Any]],
    ) -> tuple:
        """Return (stop_id, stop_name) of the nearest stop, or (None, None)."""
        best_dist = float("inf")
        best_id = None
        best_name = None
        for stop in stops_raw:
            slat = stop.get("Latitude")
            slon = stop.get("Longitude")
            if slat is None or slon is None:
                continue
            try:
                d = _haversine(lat, lon, float(slat), float(slon))
            except (ValueError, TypeError):
                continue
            if d < best_dist:
                best_dist = d
                best_id = str(
                    stop.get("StopID")
                    or stop.get("StopId")
                    or stop.get("AddressID")
                    or stop.get("AddressId")
                    or ""
                )
                best_name = stop.get("Name") or stop.get("Description") or ""
        return best_id, best_name

    def get_active_episodes(self) -> List[dict]:
        result = []
        for ep in self.active_episodes.values():
            result.append(
                {
                    "start_time": ep.start_time.isoformat().replace("+00:00", "Z"),
                    "vehicle_id": ep.vehicle_id,
                    "vehicle_name": ep.vehicle_name,
                    "block": ep.block,
                    "route_id": ep.route_id,
                    "route_name": ep.route_name,
                    "nearest_stop_id": ep.nearest_stop_id,
                    "nearest_stop_name": ep.nearest_stop_name,
                    "lat": ep.lat,
                    "lon": ep.lon,
                    "capacity": ep.capacity,
                    "peak_occupation": ep.peak_occupation,
                    "active": True,
                }
            )
        return result
