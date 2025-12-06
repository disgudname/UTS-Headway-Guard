from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from collections import deque
import json
import math
import os

from headway_storage import HeadwayEvent, HeadwayStorage


HEADWAY_DISTANCE_THRESHOLD_M = float(60.0)
STOP_APPROACH_DEFAULT_RADIUS_M = float(60.0)
DEFAULT_HEADWAY_CONFIG_PATH = Path("config/headway_config.json")
DEFAULT_STOP_APPROACH_CONFIG_PATH = Path("config/stop_approach.json")
DEFAULT_DATA_DIRS = [Path(p) for p in os.getenv("DATA_DIRS", "/data").split(":")]


@dataclass
class VehicleSnapshot:
    vehicle_id: Optional[str]
    vehicle_name: Optional[str]
    lat: float
    lon: float
    route_id: Optional[str]
    timestamp: datetime
    heading_deg: Optional[float] = None


@dataclass
class VehiclePresence:
    current_stop_id: Optional[str] = None
    arrival_time: Optional[datetime] = None
    route_id: Optional[str] = None
    departure_started_at: Optional[datetime] = None


@dataclass
class StopPoint:
    stop_id: str
    lat: float
    lon: float
    route_ids: Set[str]
    approach_bearing_deg: Optional[float] = None
    approach_tolerance_deg: Optional[float] = None
    approach_radius_m: Optional[float] = None


class HeadwayTracker:
    def __init__(
        self,
        storage: HeadwayStorage,
        *,
        arrival_distance_threshold_m: float = HEADWAY_DISTANCE_THRESHOLD_M,
        departure_distance_threshold_m: float = HEADWAY_DISTANCE_THRESHOLD_M,
        tracked_route_ids: Optional[Set[str]] = None,
        tracked_stop_ids: Optional[Set[str]] = None,
        stop_approach: Optional[Dict[str, Tuple[float, float, float]]] = None,
        stop_approach_config_path: Path = DEFAULT_STOP_APPROACH_CONFIG_PATH,
    ):
        self.storage = storage
        self.arrival_distance_threshold_m = min(arrival_distance_threshold_m, departure_distance_threshold_m)
        self.departure_distance_threshold_m = max(arrival_distance_threshold_m, departure_distance_threshold_m)
        self.tracked_route_ids = tracked_route_ids or set()
        self.tracked_stop_ids = tracked_stop_ids or set()
        self.stop_approach = stop_approach or load_stop_approach_config(stop_approach_config_path)
        self.vehicle_states: Dict[str, VehiclePresence] = {}
        self.last_arrival: Dict[Tuple[str, str], datetime] = {}
        self.last_departure: Dict[Tuple[str, str], datetime] = {}
        self.last_vehicle_arrival: Dict[Tuple[str, str, Optional[str]], datetime] = {}
        self.last_vehicle_departure: Dict[Tuple[str, str, Optional[str]], datetime] = {}
        self.stops: List[StopPoint] = []
        self.stop_lookup: Dict[str, StopPoint] = {}
        self.recent_stop_association_failures: deque = deque(maxlen=25)
        self.recent_arrival_suppressions: deque = deque(maxlen=25)
        self.recent_snapshot_diagnostics: deque = deque(maxlen=50)
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
            approach_bearing = _parse_float(stop.get("ApproachBearingDeg"))
            approach_tolerance = _parse_float(stop.get("ApproachToleranceDeg"))
            approach_radius = _parse_float(stop.get("ApproachRadiusM"))
            config_approach = self.stop_approach.get(str(stop_id))
            if approach_bearing is None and config_approach:
                approach_bearing = config_approach[0]
            if approach_tolerance is None and config_approach:
                approach_tolerance = config_approach[1]
            if approach_radius is None and config_approach and len(config_approach) > 2:
                approach_radius = config_approach[2]
            if (approach_bearing is not None and approach_tolerance is not None) and approach_radius is None:
                approach_radius = STOP_APPROACH_DEFAULT_RADIUS_M
            updated.append(
                StopPoint(
                    stop_id=str(stop_id),
                    lat=lat_f,
                    lon=lon_f,
                    route_ids=route_ids,
                    approach_bearing_deg=approach_bearing,
                    approach_tolerance_deg=approach_tolerance,
                    approach_radius_m=approach_radius,
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
            prev_stop = prev_state.current_stop_id
            distance_from_prev_stop = self._distance_to_stop(prev_stop, snap.lat, snap.lon)

            timestamp = snap.timestamp if snap.timestamp.tzinfo else snap.timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc)
            departure_recorded = False

            # Departure detection
            has_left_prev_stop = True
            if prev_stop is not None and distance_from_prev_stop is not None:
                has_left_prev_stop = distance_from_prev_stop >= self.departure_distance_threshold_m

            movement_start_time = prev_state.departure_started_at
            if prev_stop is not None and distance_from_prev_stop is not None:
                if distance_from_prev_stop >= self.arrival_distance_threshold_m:
                    if movement_start_time is None:
                        movement_start_time = timestamp
                else:
                    movement_start_time = None

            if prev_stop and has_left_prev_stop and (current_stop is None or current_stop[0] != prev_stop):
                dwell_seconds = None
                departure_timestamp = movement_start_time or timestamp
                if prev_state.arrival_time:
                    dwell_seconds = (departure_timestamp - prev_state.arrival_time).total_seconds()
                    dwell_seconds = max(dwell_seconds, 0.0)
                if prev_stop:
                    route_for_departure = prev_state.route_id or route_id_norm
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
                events.append(
                    HeadwayEvent(
                        timestamp=departure_timestamp,
                        route_id=prev_state.route_id,
                        stop_id=prev_stop,
                        vehicle_id=vid,
                        vehicle_name=snap.vehicle_name,
                        event_type="departure",
                        headway_arrival_arrival=None,
                        headway_departure_arrival=None,
                        dwell_seconds=dwell_seconds,
                    )
                )
                departure_recorded = True

            # Arrival detection
            arrival_stop_id = None
            arrival_route_id = route_id_norm
            arrival_time = None
            arrival_suppression_reason = None
            arrival_key = None
            arrival_recorded = False
            if current_stop is not None:
                arrival_stop_id, arrival_route_id = current_stop
                if arrival_route_id is None:
                    arrival_route_id = route_id_norm
                if arrival_route_id is None:
                    arrival_route_id = current_stop[1]
                duplicate_arrival = False
                arrival_key = (vid, arrival_stop_id, arrival_route_id)
                prev_vehicle_arrival = self.last_vehicle_arrival.get(arrival_key)
                prev_vehicle_departure = self.last_vehicle_departure.get(arrival_key)
                if prev_vehicle_arrival is not None and (
                    prev_vehicle_departure is None or prev_vehicle_departure <= prev_vehicle_arrival
                ):
                    duplicate_arrival = True

                if prev_stop == arrival_stop_id and prev_state.route_id != arrival_route_id and not duplicate_arrival:
                    headway_arrival_arrival, headway_departure_arrival = self._record_arrival_headways(
                        arrival_route_id, arrival_stop_id, timestamp
                    )
                    events.append(
                        HeadwayEvent(
                        timestamp=timestamp,
                        route_id=arrival_route_id,
                        stop_id=arrival_stop_id,
                        vehicle_id=vid,
                        vehicle_name=snap.vehicle_name,
                        event_type="arrival",
                        headway_arrival_arrival=headway_arrival_arrival,
                        headway_departure_arrival=headway_departure_arrival,
                        dwell_seconds=None,
                    )
                    )
                    self.last_vehicle_arrival[arrival_key] = timestamp
                    arrival_time = prev_state.arrival_time or timestamp
                    arrival_recorded = True
                elif prev_stop == arrival_stop_id:
                    arrival_time = prev_state.arrival_time or prev_vehicle_arrival or timestamp
                elif not duplicate_arrival and (prev_stop is None or has_left_prev_stop):
                    headway_arrival_arrival, headway_departure_arrival = self._record_arrival_headways(
                        arrival_route_id, arrival_stop_id, timestamp
                    )
                    events.append(
                        HeadwayEvent(
                        timestamp=timestamp,
                        route_id=arrival_route_id,
                        stop_id=arrival_stop_id,
                        vehicle_id=vid,
                        vehicle_name=snap.vehicle_name,
                        event_type="arrival",
                        headway_arrival_arrival=headway_arrival_arrival,
                        headway_departure_arrival=headway_departure_arrival,
                        dwell_seconds=None,
                    )
                    )
                    self.last_vehicle_arrival[arrival_key] = timestamp
                    arrival_time = timestamp
                    arrival_recorded = True
                elif duplicate_arrival:
                    arrival_time = prev_vehicle_arrival or prev_state.arrival_time or timestamp
                    arrival_suppression_reason = "duplicate_arrival_same_vehicle"
                else:
                    arrival_stop_id = prev_stop
                    arrival_route_id = prev_state.route_id or arrival_route_id
                    arrival_time = prev_state.arrival_time
                    arrival_suppression_reason = "has_not_left_previous_stop"
            elif prev_stop and not has_left_prev_stop:
                arrival_stop_id = prev_stop
                arrival_route_id = prev_state.route_id or route_id_norm
                arrival_time = prev_state.arrival_time
                arrival_suppression_reason = "still_at_previous_stop"

            if arrival_stop_id and arrival_suppression_reason:
                arrival_key = arrival_key or (vid, arrival_stop_id, arrival_route_id)
                self._log_arrival_suppression(
                    snap,
                    arrival_stop_id,
                    arrival_route_id,
                    arrival_suppression_reason,
                    distance_from_prev_stop,
                    has_left_prev_stop,
                    previous_stop_id=prev_stop,
                    previous_arrival_time=prev_state.arrival_time,
                    previous_departure_started_at=movement_start_time,
                    last_vehicle_arrival=self.last_vehicle_arrival.get(arrival_key) if arrival_key else None,
                    last_vehicle_departure=self.last_vehicle_departure.get(arrival_key) if arrival_key else None,
                )

            self.vehicle_states[vid] = VehiclePresence(
                current_stop_id=arrival_stop_id,
                arrival_time=arrival_time if arrival_stop_id else None,
                route_id=arrival_route_id if arrival_stop_id else None,
                departure_started_at=movement_start_time if arrival_stop_id else None,
            )

            target_stop_id = arrival_stop_id or (current_stop[0] if current_stop else None)
            stop_meta = self.stop_lookup.get(target_stop_id)
            self.recent_snapshot_diagnostics.append(
                {
                    "timestamp": self._isoformat(timestamp),
                    "vehicle_id": vid,
                    "vehicle_name": snap.vehicle_name,
                    "route_id": route_id_norm,
                    "heading_deg": snap.heading_deg,
                    "previous_stop_id": prev_stop,
                    "distance_from_previous_stop": distance_from_prev_stop,
                    "has_left_previous_stop": has_left_prev_stop,
                    "arrival_stop_id": target_stop_id,
                    "arrival_route_id": arrival_route_id,
                    "arrival_stop_distance": self._distance_to_stop(target_stop_id, snap.lat, snap.lon),
                    "arrival_recorded": arrival_recorded,
                    "arrival_suppressed_reason": arrival_suppression_reason,
                    "duplicate_arrival": arrival_key in self.last_vehicle_arrival if arrival_key else False,
                    "departure_recorded": departure_recorded,
                    "departure_started_at": self._isoformat(movement_start_time),
                    "approach_bearing_deg": getattr(stop_meta, "approach_bearing_deg", None),
                    "approach_tolerance_deg": getattr(stop_meta, "approach_tolerance_deg", None),
                    "approach_radius_m": getattr(stop_meta, "approach_radius_m", None),
                }
            )

        if events:
            try:
                self.storage.write_events(events)
                print(f"[headway] recorded {len(events)} events")
            except Exception as exc:
                print(f"[headway] failed to write events: {exc}")

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

    def _log_arrival_suppression(
        self,
        snap: VehicleSnapshot,
        stop_id: str,
        route_id: Optional[str],
        reason: str,
        distance_from_prev_stop: Optional[float],
        has_left_prev_stop: bool,
        *,
        previous_stop_id: Optional[str] = None,
        previous_arrival_time: Optional[datetime] = None,
        previous_departure_started_at: Optional[datetime] = None,
        last_vehicle_arrival: Optional[datetime] = None,
        last_vehicle_departure: Optional[datetime] = None,
    ) -> None:
        stop_distance = None
        stop_heading = None
        approach_bearing = None
        tolerance = None
        radius = None
        stop = self.stop_lookup.get(stop_id)
        if stop:
            stop_distance = self._haversine(snap.lat, snap.lon, stop.lat, stop.lon)
            if stop.approach_bearing_deg is not None:
                approach_bearing = stop.approach_bearing_deg
                tolerance = stop.approach_tolerance_deg
                radius = stop.approach_radius_m
                stop_heading = self._bearing_degrees(stop.lat, stop.lon, snap.lat, snap.lon)

        self.recent_arrival_suppressions.append(
            {
                "timestamp": self._isoformat(snap.timestamp),
                "vehicle_id": self._normalize_id(snap.vehicle_id),
                "vehicle_name": snap.vehicle_name,
                "route_id": route_id,
                "stop_id": stop_id,
                "reason": reason,
                "distance_from_prev_stop": distance_from_prev_stop,
                "has_left_prev_stop": has_left_prev_stop,
                "distance_from_stop": stop_distance,
                "stop_bearing": stop_heading,
                "approach_bearing_deg": approach_bearing,
                "approach_tolerance_deg": tolerance,
                "approach_radius_m": radius,
                "heading_deg": snap.heading_deg,
                "previous_stop_id": previous_stop_id,
                "previous_arrival_time": self._isoformat(previous_arrival_time),
                "previous_departure_started_at": self._isoformat(previous_departure_started_at),
                "last_vehicle_arrival": self._isoformat(last_vehicle_arrival),
                "last_vehicle_departure": self._isoformat(last_vehicle_departure),
            }
        )

    def _diagnose_stop_association(
        self,
        lat: float,
        lon: float,
        route_id: Optional[str],
        heading_deg: Optional[float],
    ) -> dict:
        if not self.stops:
            return {"reason": "no_stops"}

        nearest_any: Optional[Tuple[StopPoint, float, float, dict]] = None
        nearest_route_mismatch: Optional[Tuple[StopPoint, float, float, dict]] = None
        nearest_cone_block: Optional[Tuple[StopPoint, float, float, dict]] = None

        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            approach_config = self.stop_approach.get(stop.stop_id)
            approach_bearing = stop.approach_bearing_deg
            approach_tolerance = stop.approach_tolerance_deg
            approach_radius = stop.approach_radius_m

            if approach_config:
                if approach_bearing is None:
                    approach_bearing = approach_config[0]
                if approach_tolerance is None:
                    approach_tolerance = approach_config[1]
                if approach_radius is None and len(approach_config) > 2:
                    approach_radius = approach_config[2]

            requires_cone = approach_bearing is not None and approach_tolerance is not None
            cone_radius = approach_radius if requires_cone else None
            effective_threshold = cone_radius if cone_radius is not None else self.arrival_distance_threshold_m
            dist = self._haversine(lat, lon, stop.lat, stop.lon)

            position_bearing = None
            target_heading = None
            heading_ok: Optional[bool] = None
            distance_from_cone_start = None
            cone_start_lat = None
            cone_start_lon = None
            heading_missing = heading_deg is None or not math.isfinite(heading_deg)
            cone_position_ok = None
            if requires_cone:
                cone_start_lat, cone_start_lon = self._destination_point(
                    stop.lat, stop.lon, approach_bearing, effective_threshold
                )
                target_heading = (approach_bearing + 180.0) % 360.0
                position_bearing = self._bearing_degrees(cone_start_lat, cone_start_lon, lat, lon)
                distance_from_cone_start = self._haversine(cone_start_lat, cone_start_lon, lat, lon)
                cone_position_ok = distance_from_cone_start <= effective_threshold and _is_within_bearing(
                    position_bearing, target_heading, approach_tolerance
                )
                if not heading_missing:
                    heading_ok = _is_within_bearing(heading_deg, target_heading, approach_tolerance)

            stop_meta = {
                "approach_bearing_deg": approach_bearing,
                "approach_tolerance_deg": approach_tolerance,
                "approach_radius_m": approach_radius,
                "position_bearing_deg": position_bearing,
                "target_heading_deg": target_heading,
                "heading_within_cone": heading_ok,
                "heading_missing": heading_missing if requires_cone else None,
                "cone_start_lat": cone_start_lat,
                "cone_start_lon": cone_start_lon,
                "distance_from_cone_start": distance_from_cone_start,
                "position_within_cone": cone_position_ok,
            }

            if requires_cone and heading_ok is None:
                heading_ok = False
                stop_meta["heading_within_cone"] = heading_ok

            if nearest_any is None or dist < nearest_any[1]:
                nearest_any = (stop, dist, effective_threshold, stop_meta)

            if route_id and stop.route_ids and route_id not in stop.route_ids:
                if dist <= effective_threshold and (
                    nearest_route_mismatch is None or dist < nearest_route_mismatch[1]
                ):
                    nearest_route_mismatch = (stop, dist, effective_threshold, stop_meta)
                continue

            if dist <= effective_threshold:
                if requires_cone:
                    if heading_ok is False or not cone_position_ok:
                        if nearest_cone_block is None or dist < nearest_cone_block[1]:
                            nearest_cone_block = (stop, dist, effective_threshold, stop_meta)
                        continue

        if nearest_route_mismatch is not None:
            stop, dist, threshold, meta = nearest_route_mismatch
            return {
                "reason": "route_mismatch",
                "nearest_stop_id": stop.stop_id,
                "nearest_stop_route_ids": sorted(stop.route_ids),
                "distance_m": dist,
                "threshold_m": threshold,
                **meta,
            }
        if nearest_cone_block is not None:
            stop, dist, threshold, meta = nearest_cone_block
            return {
                "reason": "outside_cone",
                "nearest_stop_id": stop.stop_id,
                "nearest_stop_route_ids": sorted(stop.route_ids),
                "distance_m": dist,
                "threshold_m": threshold,
                **meta,
            }
        if nearest_any is not None:
            stop, dist, threshold, meta = nearest_any
            return {
                "reason": "beyond_distance",
                "nearest_stop_id": stop.stop_id,
                "nearest_stop_route_ids": sorted(stop.route_ids),
                "distance_m": dist,
                "threshold_m": threshold,
                **meta,
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
        best: Optional[Tuple[str, Optional[str], float]] = None
        for stop in self.stops:
            if self.tracked_stop_ids and stop.stop_id not in self.tracked_stop_ids:
                continue
            if route_id and self.tracked_route_ids and route_id not in stop.route_ids and stop.route_ids:
                continue
            if route_id and stop.route_ids and route_id not in stop.route_ids:
                continue
            approach_config = self.stop_approach.get(stop.stop_id)
            approach_bearing = stop.approach_bearing_deg
            approach_tolerance = stop.approach_tolerance_deg
            approach_radius = stop.approach_radius_m

            if approach_config:
                if approach_bearing is None:
                    approach_bearing = approach_config[0]
                if approach_tolerance is None:
                    approach_tolerance = approach_config[1]
                if approach_radius is None and len(approach_config) > 2:
                    approach_radius = approach_config[2]

            requires_cone = approach_bearing is not None and approach_tolerance is not None
            cone_radius = approach_radius if requires_cone else None
            effective_threshold = cone_radius if cone_radius is not None else threshold
            dist = self._haversine(lat, lon, stop.lat, stop.lon)
            if dist <= effective_threshold:
                if requires_cone:
                    cone_start_lat, cone_start_lon = self._destination_point(
                        stop.lat, stop.lon, approach_bearing, effective_threshold
                    )
                    position_bearing = self._bearing_degrees(cone_start_lat, cone_start_lon, lat, lon)
                    distance_from_cone_start = self._haversine(cone_start_lat, cone_start_lon, lat, lon)
                    target_heading = (approach_bearing + 180.0) % 360.0
                    position_ok = distance_from_cone_start <= effective_threshold and _is_within_bearing(
                        position_bearing, target_heading, approach_tolerance
                    )
                    heading_ok = False
                    if heading_deg is not None and math.isfinite(heading_deg):
                        heading_ok = _is_within_bearing(heading_deg, target_heading, approach_tolerance)
                    if not heading_ok or not position_ok:
                        continue
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


def load_stop_approach_config(
    path: Path = DEFAULT_STOP_APPROACH_CONFIG_PATH,
    *,
    data_dirs: Optional[Sequence[Path]] = None,
) -> Dict[str, Tuple[float, float, float]]:
    config: Dict[str, Tuple[float, float, float]] = {}
    resolved_path, raw_text = _read_data_file(path, data_dirs=data_dirs)
    if not raw_text:
        return config
    try:
        raw = json.loads(raw_text)
        if isinstance(raw, dict):
            for stop_id, entry in raw.items():
                if not isinstance(entry, dict):
                    continue
                bearing = _parse_float(entry.get("bearing_deg") or entry.get("bearing"))
                tolerance = _parse_float(entry.get("tolerance_deg") or entry.get("tolerance"))
                radius = _parse_float(entry.get("radius_m") or entry.get("radius"))
                if radius is None:
                    radius = STOP_APPROACH_DEFAULT_RADIUS_M
                if bearing is None or tolerance is None:
                    continue
                config[str(stop_id)] = (
                    bearing,
                    max(0.0, tolerance),
                    max(0.0, radius),
                )
    except Exception as exc:
        print(f"[headway] failed to load stop approach config {resolved_path or path}: {exc}")
    return config


def _parse_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_within_bearing(actual: float, target: float, tolerance: float) -> bool:
    normalized = abs((actual - target + 180.0) % 360.0 - 180.0)
    return normalized <= tolerance
