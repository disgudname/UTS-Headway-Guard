from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
import json
import math

from headway_storage import HeadwayEvent, HeadwayStorage


HEADWAY_DISTANCE_THRESHOLD_M = float(60.0)
DEFAULT_HEADWAY_CONFIG_PATH = Path("config/headway_config.json")


@dataclass
class VehicleSnapshot:
    vehicle_id: Optional[str]
    lat: float
    lon: float
    route_id: Optional[str]
    timestamp: datetime


@dataclass
class VehiclePresence:
    current_stop_id: Optional[str] = None
    arrival_time: Optional[datetime] = None
    route_id: Optional[str] = None


@dataclass
class StopPoint:
    stop_id: str
    lat: float
    lon: float
    route_ids: Set[str]


class HeadwayTracker:
    def __init__(
        self,
        storage: HeadwayStorage,
        *,
        distance_threshold_m: float = HEADWAY_DISTANCE_THRESHOLD_M,
        tracked_route_ids: Optional[Set[str]] = None,
        tracked_stop_ids: Optional[Set[str]] = None,
    ):
        self.storage = storage
        self.distance_threshold_m = distance_threshold_m
        self.tracked_route_ids = tracked_route_ids or set()
        self.tracked_stop_ids = tracked_stop_ids or set()
        self.vehicle_states: Dict[str, VehiclePresence] = {}
        self.last_arrival: Dict[Tuple[str, str], datetime] = {}
        self.stops: List[StopPoint] = []
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
            lat = stop.get("Latitude") or stop.get("Lat")
            lon = stop.get("Longitude") or stop.get("Lon") or stop.get("Lng")
            if lat is None or lon is None:
                continue
            try:
                lat_f = float(lat)
                lon_f = float(lon)
            except (TypeError, ValueError):
                continue
            route_ids = self._extract_route_ids(stop)
            updated.append(StopPoint(stop_id=str(stop_id), lat=lat_f, lon=lon_f, route_ids=route_ids))
        self.stops = updated

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
            current_stop = self._nearest_stop(snap.lat, snap.lon, route_id_norm)
            vid = self._normalize_id(snap.vehicle_id)
            if vid is None:
                continue
            prev_state = self.vehicle_states.get(vid, VehiclePresence())
            prev_stop = prev_state.current_stop_id

            timestamp = snap.timestamp if snap.timestamp.tzinfo else snap.timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc)

            # Departure detection
            if prev_stop and (current_stop is None or current_stop[0] != prev_stop):
                dwell_seconds = None
                if prev_state.arrival_time:
                    dwell_seconds = (timestamp - prev_state.arrival_time).total_seconds()
                    dwell_seconds = max(dwell_seconds, 0.0)
                events.append(
                    HeadwayEvent(
                        timestamp=timestamp,
                        route_id=prev_state.route_id,
                        stop_id=prev_stop,
                        vehicle_id=vid,
                        event_type="departure",
                        headway_seconds=None,
                        dwell_seconds=dwell_seconds,
                    )
                )

            # Arrival detection
            arrival_stop_id = None
            arrival_route_id = route_id_norm
            arrival_time = None
            if current_stop is not None:
                arrival_stop_id, arrival_route_id = current_stop
                if arrival_route_id is None:
                    arrival_route_id = route_id_norm
                if arrival_route_id is None:
                    arrival_route_id = current_stop[1]
                if prev_stop != arrival_stop_id:
                    headway_seconds = self._record_arrival(arrival_route_id, arrival_stop_id, timestamp)
                    events.append(
                        HeadwayEvent(
                            timestamp=timestamp,
                            route_id=arrival_route_id,
                            stop_id=arrival_stop_id,
                            vehicle_id=vid,
                            event_type="arrival",
                            headway_seconds=headway_seconds,
                            dwell_seconds=None,
                        )
                    )
                    arrival_time = timestamp
                else:
                    arrival_time = prev_state.arrival_time or timestamp

            self.vehicle_states[vid] = VehiclePresence(
                current_stop_id=arrival_stop_id if current_stop else None,
                arrival_time=arrival_time if current_stop else None,
                route_id=arrival_route_id if current_stop else None,
            )

        if events:
            try:
                self.storage.write_events(events)
                print(f"[headway] recorded {len(events)} events")
            except Exception as exc:
                print(f"[headway] failed to write events: {exc}")

    def _record_arrival(self, route_id: Optional[str], stop_id: Optional[str], timestamp: datetime) -> Optional[float]:
        if route_id is None or stop_id is None:
            return None
        key = (route_id, stop_id)
        prev = self.last_arrival.get(key)
        self.last_arrival[key] = timestamp
        if prev is None:
            return None
        delta = (timestamp - prev).total_seconds()
        return max(delta, 0.0)

    def _nearest_stop(self, lat: float, lon: float, route_id: Optional[str]) -> Optional[Tuple[str, Optional[str]]]:
        best: Optional[Tuple[str, Optional[str], float]] = None
        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            if route_id and self.tracked_route_ids and route_id not in stop.route_ids and stop.route_ids:
                continue
            if route_id and stop.route_ids and route_id not in stop.route_ids:
                continue
            dist = self._haversine(lat, lon, stop.lat, stop.lon)
            if dist <= self.distance_threshold_m:
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

    def _haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        r_earth = 6371000.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        return 2 * r_earth * math.asin(math.sqrt(a))


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
