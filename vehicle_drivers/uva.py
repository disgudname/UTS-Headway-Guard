"""
UVA Vehicle Drivers Provider

This module implements the VehicleDriversProvider for University of Virginia
Transit Service (UTS). It integrates with:

1. TransLoc - For vehicle block assignments
2. WhenToWork (W2W) - For driver schedules
3. OnDemand - For paratransit vehicle/driver matching

Other agencies can use this as a reference implementation for creating their
own providers that integrate with their scheduling systems.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import httpx

from . import (
    DriverInfo,
    VehicleDriverEntry,
    VehicleDriversProvider,
    VehicleDriversResult,
)


# UVA-specific route-to-block mappings
# Based on UTS block assignments:
# [01], [02] = Green
# [03], [04] = Night Pilot
# [05], [06], [07], [08] = Orange
# [09], [10], [11], [12] = Gold
# [13], [14] = Silver
# [15], [16], [17], [18] = Blue (dedicated)
# [20]-[26] = Red/Blue (Blue only 0700-0800, Red rest of day)
ROUTE_TO_BLOCKS: Dict[str, Set[str]] = {
    "green": {"01", "02"},
    "night pilot": {"03", "04"},
    "orange": {"05", "06", "07", "08"},
    "gold": {"09", "10", "11", "12"},
    "yellow": {"09", "10", "11", "12"},  # Yellow is same as Gold
    "silver": {"13", "14"},
    "blue": {"15", "16", "17", "18", "20", "21", "22", "23", "24", "25", "26"},
    "red": {"20", "21", "22", "23", "24", "25", "26"},
}

# Preferred (dedicated) blocks for each route - these take priority over shared blocks
ROUTE_PREFERRED_BLOCKS: Dict[str, Set[str]] = {
    "blue": {"15", "16", "17", "18"},
}

# Blocks that have separate AM/PM driver assignments
AM_PM_BLOCKS: Set[str] = {f"{number:02d}" for number in range(20, 27)}

# OnDemand block names in W2W
ONDEMAND_BLOCK_NAMES = ["OnDemand Driver", "OnDemand EB"]


@dataclass
class UVAProviderConfig:
    """Configuration for UVA Vehicle Drivers Provider."""
    w2w_key: Optional[str] = None
    w2w_url: str = "https://www3.whentowork.com/cgi-bin/w2w.dll/api/AssignedShiftList"
    timezone: str = "America/New_York"


class UVAVehicleDriversProvider(VehicleDriversProvider):
    """
    Vehicle Drivers Provider for University of Virginia Transit Service.

    This provider integrates with:
    - TransLoc for vehicle block assignments
    - WhenToWork for driver schedules
    - OnDemand for paratransit services

    Dependencies are injected to allow for testing and flexibility:
    - fetch_block_groups_fn: Async function to fetch TransLoc block data
    - get_vehicle_state_fn: Function to get current vehicle state
    - get_ondemand_data_fn: Optional async function to get OnDemand vehicle data
    - record_api_call_fn: Optional function to record API calls for monitoring
    """

    def __init__(
        self,
        config: UVAProviderConfig,
        fetch_block_groups_fn: Callable[..., Coroutine[Any, Any, List[Dict[str, Any]]]],
        get_vehicle_state_fn: Callable[[], Dict[str, Any]],
        get_vehicle_block_cache_fn: Callable[[], Dict[str, Dict[str, Any]]],
        set_vehicle_block_cache_fn: Callable[[str, Dict[str, Any]], None],
        get_ondemand_data_fn: Optional[Callable[..., Coroutine[Any, Any, Dict[str, Any]]]] = None,
        record_api_call_fn: Optional[Callable[[str, str, int], None]] = None,
        w2w_assignments_cache: Optional[Any] = None,
    ):
        self.config = config
        self.tz = ZoneInfo(config.timezone)
        self._fetch_block_groups = fetch_block_groups_fn
        self._get_vehicle_state = get_vehicle_state_fn
        self._get_vehicle_block_cache = get_vehicle_block_cache_fn
        self._set_vehicle_block_cache = set_vehicle_block_cache_fn
        self._get_ondemand_data = get_ondemand_data_fn
        self._record_api_call = record_api_call_fn
        self._w2w_cache = w2w_assignments_cache

    def get_agency_name(self) -> str:
        return "University of Virginia"

    async def fetch_driver_assignments(self) -> Dict[str, Any]:
        """Fetch raw driver assignments from WhenToWork."""
        if self._w2w_cache is not None:
            return await self._w2w_cache.get(self._fetch_w2w_assignments_internal)
        return await self._fetch_w2w_assignments_internal()

    async def _fetch_w2w_assignments_internal(self) -> Dict[str, Any]:
        """Internal method to fetch W2W assignments."""
        now = datetime.now(self.tz)
        service_day = now
        if now.time() < dtime(hour=2, minute=30):
            service_day = now - timedelta(days=1)

        if not self.config.w2w_key:
            return {
                "disabled": True,
                "fetched_at": int(now.timestamp() * 1000),
                "assignments_by_block": {},
            }

        params = {
            "start_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
            "end_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
            "key": self.config.w2w_key,
        }

        url = httpx.URL(self.config.w2w_url)
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=20)

        if self._record_api_call:
            log_url = str(httpx.URL(str(url), params={**params, "key": "***"}))
            self._record_api_call("GET", log_url, response.status_code)

        response.raise_for_status()
        payload = response.json()

        shifts: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            raw_shifts = payload.get("AssignedShiftList")
            if isinstance(raw_shifts, list):
                shifts = raw_shifts

        assignments = self._build_driver_assignments(shifts, now)
        return {
            "fetched_at": int(now.timestamp() * 1000),
            "assignments_by_block": assignments,
        }

    async def fetch_vehicle_drivers(self) -> VehicleDriversResult:
        """
        Fetch the current vehicle-to-driver mappings.

        This joins:
        1. TransLoc blocks (vehicle_id -> block assignment)
        2. W2W assignments (block -> driver with time windows)
        3. Vehicle names from TransLoc vehicle data
        4. OnDemand vehicles with driver names
        """
        now = datetime.now(self.tz)
        now_ts = int(now.timestamp() * 1000)

        # Fetch block data from TransLoc
        try:
            async with httpx.AsyncClient() as client:
                block_groups = await self._fetch_block_groups(client, include_metadata=False)
            blocks_with_times = self._build_block_mapping_with_times(block_groups)
            print(f"[vehicle_drivers] Found {len(blocks_with_times)} vehicles with block data")
        except Exception as exc:
            print(f"[vehicle_drivers] blocks fetch failed: {exc}")
            blocks_with_times = {}

        # Fetch W2W assignments
        try:
            w2w_data = await self.fetch_driver_assignments()
            assignments_by_block = w2w_data.get("assignments_by_block", {})
        except Exception as exc:
            print(f"[vehicle_drivers] w2w fetch failed: {exc}")
            assignments_by_block = {}

        vehicle_drivers: Dict[str, VehicleDriverEntry] = {}

        # Add OnDemand W2W positions
        for block_name in ONDEMAND_BLOCK_NAMES:
            current_drivers = self._find_current_drivers(block_name, assignments_by_block, now_ts)
            if current_drivers:
                drivers = [
                    DriverInfo(
                        name=d["name"],
                        shift_start=d["start_ts"],
                        shift_start_label=d.get("start_label", ""),
                        shift_end=d["end_ts"],
                        shift_end_label=d.get("end_label", ""),
                    )
                    for d in current_drivers
                ]
                vehicle_drivers[block_name] = VehicleDriverEntry(
                    vehicle_id=block_name,
                    block=block_name,
                    drivers=drivers,
                    vehicle_name=None,
                )

        # Select current block for each vehicle
        blocks_mapping = self._select_blocks_for_vehicles(
            blocks_with_times, assignments_by_block, now_ts
        )
        print(f"[vehicle_drivers] After filtering: {len(blocks_mapping)} vehicles remain")

        # Get vehicle state
        vehicle_state = self._get_vehicle_state()
        vehicle_names = vehicle_state.get("vehicle_names", {})
        vehicle_routes = vehicle_state.get("vehicle_routes", {})
        vehicle_block_cache = self._get_vehicle_block_cache()

        # Process each vehicle with block assignment
        for vehicle_id, block_name in blocks_mapping.items():
            entry = self._build_vehicle_entry(
                vehicle_id=vehicle_id,
                block_name=block_name,
                assignments_by_block=assignments_by_block,
                vehicle_names=vehicle_names,
                vehicle_routes=vehicle_routes,
                vehicle_block_cache=vehicle_block_cache,
                now_ts=now_ts,
            )
            if entry:
                vehicle_drivers[vehicle_id] = entry

        # Process OnDemand vehicles if available
        if self._get_ondemand_data:
            try:
                ondemand_data = await self._get_ondemand_data(now=now)
                vehicles_list = ondemand_data.get("vehicles", []) if isinstance(ondemand_data, dict) else []

                for vehicle_entry in vehicles_list:
                    if not isinstance(vehicle_entry, dict):
                        continue

                    vehicle_id = (
                        vehicle_entry.get("vehicle_id")
                        or vehicle_entry.get("VehicleID")
                        or vehicle_entry.get("vehicleId")
                    )
                    if vehicle_id is None:
                        continue
                    vehicle_id_str = str(vehicle_id).strip()
                    if not vehicle_id_str:
                        continue

                    driver_name = vehicle_entry.get("driverName", "")
                    if not driver_name:
                        continue

                    vehicle_name = (
                        vehicle_entry.get("callName")
                        or vehicle_entry.get("call_name")
                    )

                    matched_driver = self._find_ondemand_driver_by_name(
                        driver_name, assignments_by_block, now_ts
                    )

                    if matched_driver:
                        driver_info = DriverInfo(
                            name=matched_driver["name"],
                            shift_start=matched_driver["start_ts"],
                            shift_start_label=matched_driver.get("start_label", ""),
                            shift_end=matched_driver["end_ts"],
                            shift_end_label=matched_driver.get("end_label", ""),
                        )
                        vehicle_drivers[vehicle_id_str] = VehicleDriverEntry(
                            vehicle_id=vehicle_id_str,
                            block=matched_driver["block"],
                            drivers=[driver_info],
                            vehicle_name=vehicle_name,
                        )
            except Exception as exc:
                print(f"[vehicle_drivers] ondemand fetch failed: {exc}")

        return VehicleDriversResult(
            fetched_at=now_ts,
            vehicle_drivers=vehicle_drivers,
        )

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    def _build_driver_assignments(
        self, shifts: List[Dict[str, Any]], now: datetime
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Build assignments_by_block structure from W2W shifts."""
        assignments: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        fallback_ts = int(now.timestamp() * 1000)

        for shift in shifts:
            if not isinstance(shift, dict):
                continue

            position_name = shift.get("POSITION_NAME")
            block_number, explicit_period = self._extract_block_from_position_name(position_name)
            if not block_number:
                continue

            first = str(shift.get("FIRST_NAME") or "").strip()
            last = str(shift.get("LAST_NAME") or "").strip()
            name = (first + " " + last).strip() or "OPEN"

            start_dt = self._parse_w2w_datetime(shift.get("START_DATE"), shift.get("START_TIME"))
            if start_dt is None:
                continue

            end_dt = self._parse_w2w_datetime(shift.get("END_DATE"), shift.get("END_TIME"))
            if end_dt is None:
                duration_hours = self._parse_duration_hours(shift.get("DURATION"))
                if duration_hours:
                    end_dt = start_dt + timedelta(hours=duration_hours)

            if end_dt is None:
                continue
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)
            if end_dt <= now:
                continue

            period = explicit_period or ("am" if start_dt.hour < 12 else "pm")
            if period == "any":
                pass
            elif block_number not in AM_PM_BLOCKS:
                period = "any"
            elif period not in {"am", "pm"}:
                period = "any"

            entry = assignments.setdefault(block_number, {})
            bucket = entry.setdefault(period, [])

            start_ts = int(start_dt.timestamp() * 1000)
            end_ts = int(end_dt.timestamp() * 1000)

            color_id_raw = shift.get("COLOR_ID")
            color_id = str(color_id_raw).strip() if color_id_raw is not None else None
            if color_id == "":
                color_id = None
            if color_id == "9":  # Driver didn't come in
                continue

            bucket.append({
                "name": name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_label": self._format_driver_time(start_dt),
                "end_label": self._format_driver_time(end_dt),
                "color_id": color_id,
                "position_name": position_name,
            })

        for entry in assignments.values():
            for drivers in entry.values():
                drivers.sort(key=lambda item: item.get("start_ts") or fallback_ts)

        return assignments

    def _extract_block_from_position_name(self, value: Any) -> Tuple[Optional[str], str]:
        """Extract block number from W2W position name."""
        if value is None:
            return None, ""
        text = str(value).strip()
        if not text:
            return None, ""

        # Handle OnDemand positions
        if "OnDemand" in text or "On Demand" in text:
            if "EB" in text.upper():
                return "OnDemand EB", "any"
            return "OnDemand Driver", "any"

        # Handle "Block 01" format
        match = re.match(r"Block\s*(\d{1,2})\s*(AM|PM)?", text, re.IGNORECASE)
        if match:
            block_num = match.group(1).zfill(2)
            period = match.group(2).lower() if match.group(2) else ""
            return block_num, period

        # Handle "[01]" format
        match = re.search(r"\[(\d{1,2})\]", text)
        if match:
            block_num = match.group(1).zfill(2)
            period = ""
            if " AM" in text.upper():
                period = "am"
            elif " PM" in text.upper():
                period = "pm"
            return block_num, period

        return None, ""

    def _parse_w2w_datetime(
        self, date_str: Any, time_str: Any
    ) -> Optional[datetime]:
        """Parse W2W date/time strings into datetime."""
        if date_str is None or time_str is None:
            return None

        date_s = str(date_str).strip()
        time_s = str(time_str).strip()
        if not date_s or not time_s:
            return None

        # Try M/D/YYYY format
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                date_part = datetime.strptime(date_s, fmt).date()
                break
            except ValueError:
                continue
        else:
            return None

        # Parse time
        for fmt in ("%I:%M %p", "%H:%M:%S", "%H:%M"):
            try:
                time_part = datetime.strptime(time_s, fmt).time()
                break
            except ValueError:
                continue
        else:
            return None

        return datetime.combine(date_part, time_part, tzinfo=self.tz)

    def _parse_duration_hours(self, value: Any) -> Optional[float]:
        """Parse duration string to hours."""
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _format_driver_time(self, dt: datetime) -> str:
        """Format datetime to compact time label (e.g., '6a', '2:30p')."""
        hour = dt.hour
        minute = dt.minute
        suffix = "a" if hour < 12 else "p"
        display_hour = hour % 12
        if display_hour == 0:
            display_hour = 12
        if minute == 0:
            return f"{display_hour}{suffix}"
        return f"{display_hour}:{minute:02d}{suffix}"

    def _find_current_drivers(
        self,
        block_number: str,
        assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
        now_ts: int,
    ) -> List[Dict[str, Any]]:
        """Find all drivers currently assigned to a block."""
        if block_number not in assignments_by_block:
            return []

        periods_dict = assignments_by_block[block_number]
        matching_drivers = []

        for period, drivers in periods_dict.items():
            for driver in drivers:
                start_ts = driver.get("start_ts", 0)
                end_ts = driver.get("end_ts", 0)
                if start_ts <= now_ts < end_ts:
                    matching_drivers.append(driver)

        matching_drivers.sort(key=lambda d: d.get("start_ts", 0))
        return matching_drivers

    def _split_interlined_blocks(self, block_name: str) -> List[str]:
        """Split interlined block names into individual block numbers."""
        if not block_name:
            return []

        parts = block_name.split("/")
        result = []

        for part in parts:
            match = re.search(r'\[(\d{1,2})\]', part)
            if match:
                number = match.group(1)
                normalized = str(int(number)).zfill(2)
                result.append(normalized)

        return result

    def _get_blocks_for_route(self, route_name: Optional[str]) -> Optional[Set[str]]:
        """Get valid block numbers for a route name."""
        if not route_name:
            return None
        route_lower = route_name.lower()
        for key, blocks in ROUTE_TO_BLOCKS.items():
            if key in route_lower:
                return blocks
        return None

    def _get_preferred_blocks_for_route(self, route_name: Optional[str]) -> Optional[Set[str]]:
        """Get preferred block numbers for a route name."""
        if not route_name:
            return None
        route_lower = route_name.lower()
        for key, blocks in ROUTE_PREFERRED_BLOCKS.items():
            if key in route_lower:
                return blocks
        return None

    def _find_ondemand_driver_by_name(
        self,
        driver_name: str,
        assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
        now_ts: int,
    ) -> Optional[Dict[str, Any]]:
        """Find an OnDemand driver assignment by matching driver name."""
        if not driver_name:
            return None

        normalized_search_name = self._normalize_driver_name(driver_name)
        if not normalized_search_name:
            return None

        for block_name in ONDEMAND_BLOCK_NAMES:
            if block_name not in assignments_by_block:
                continue

            periods_dict = assignments_by_block[block_name]
            for period, drivers in periods_dict.items():
                for driver in drivers:
                    start_ts = driver.get("start_ts", 0)
                    end_ts = driver.get("end_ts", 0)

                    if start_ts <= now_ts < end_ts:
                        w2w_driver_name = driver.get("name", "")
                        normalized_w2w_name = self._normalize_driver_name(w2w_driver_name)

                        if normalized_w2w_name == normalized_search_name:
                            position_name = driver.get("position_name") or block_name
                            return {
                                "name": driver["name"],
                                "start_ts": start_ts,
                                "end_ts": end_ts,
                                "start_label": driver.get("start_label"),
                                "end_label": driver.get("end_label"),
                                "block": position_name,
                            }

        return None

    def _normalize_driver_name(self, name: str) -> str:
        """Normalize driver names for matching."""
        if not name:
            return ""
        return re.sub(r"\s+", " ", name.strip()).lower()

    def _build_block_mapping_with_times(
        self, block_groups: List[Dict[str, Any]]
    ) -> Dict[str, List[Tuple[str, int, int]]]:
        """Build vehicle_id -> [(block_name, start_ts, end_ts), ...] mapping."""
        # This is a simplified version - the full implementation is in app.py
        # Other agencies would implement their own version based on their AVL system
        mapping: Dict[str, List[Tuple[str, int, int]]] = {}

        for group in block_groups or []:
            block_group_id = str(group.get("BlockGroupId") or "").strip()
            if not block_group_id:
                continue

            vehicle_id = group.get("VehicleId") or group.get("VehicleID")
            if vehicle_id is None:
                continue

            vehicle_id_str = str(vehicle_id)

            # Extract time info from trips
            blocks_list = group.get("Blocks") or []
            for block in blocks_list:
                trips = block.get("Trips") or []
                for trip in trips:
                    start_ts = trip.get("StartTimeUtc", 0)
                    end_ts = trip.get("EndTimeUtc", 0)

                    if vehicle_id_str not in mapping:
                        mapping[vehicle_id_str] = []

                    mapping[vehicle_id_str].append((block_group_id, start_ts, end_ts))

        return mapping

    def _select_blocks_for_vehicles(
        self,
        blocks_with_times: Dict[str, List[Tuple[str, int, int]]],
        assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
        now_ts: int,
    ) -> Dict[str, str]:
        """Select current block for each vehicle."""
        blocks_mapping = {}

        for vehicle_id, block_list in blocks_with_times.items():
            # Try to find currently active block
            for block_name, start_ts, end_ts in block_list:
                if start_ts <= now_ts < end_ts:
                    blocks_mapping[vehicle_id] = block_name
                    break
            else:
                # Check if any W2W driver shift is active for any of the blocks
                for block_name, start_ts, end_ts in block_list:
                    block_numbers = self._split_interlined_blocks(block_name)
                    for block_number in block_numbers:
                        drivers = self._find_current_drivers(block_number, assignments_by_block, now_ts)
                        if drivers:
                            blocks_mapping[vehicle_id] = block_name
                            break
                    if vehicle_id in blocks_mapping:
                        break

        return blocks_mapping

    def _build_vehicle_entry(
        self,
        vehicle_id: str,
        block_name: str,
        assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
        vehicle_names: Dict[str, str],
        vehicle_routes: Dict[str, str],
        vehicle_block_cache: Dict[str, Dict[str, Any]],
        now_ts: int,
    ) -> Optional[VehicleDriverEntry]:
        """Build a VehicleDriverEntry for a vehicle with block assignment."""
        block_numbers = self._split_interlined_blocks(block_name)
        vehicle_name = vehicle_names.get(vehicle_id)
        current_route = vehicle_routes.get(vehicle_id)

        valid_blocks_for_route = self._get_blocks_for_route(current_route)
        preferred_blocks_for_route = self._get_preferred_blocks_for_route(current_route)

        # Collect drivers by block
        drivers_by_block: Dict[str, List[Dict[str, Any]]] = {}
        for block_number in block_numbers:
            block_drivers = self._find_current_drivers(block_number, assignments_by_block, now_ts)
            if block_drivers:
                drivers_by_block[block_number] = block_drivers

        # Select which block to use
        selected_block_number = None
        w2w_position_name = None

        if drivers_by_block:
            # Try preferred blocks first
            if preferred_blocks_for_route:
                for blk_num in drivers_by_block:
                    if blk_num in preferred_blocks_for_route:
                        selected_block_number = blk_num
                        best_driver = max(drivers_by_block[blk_num], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")
                        break

            # Then try valid blocks for route
            if selected_block_number is None and valid_blocks_for_route:
                for blk_num in drivers_by_block:
                    if blk_num in valid_blocks_for_route:
                        selected_block_number = blk_num
                        best_driver = max(drivers_by_block[blk_num], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")
                        break

            # Check cache for out-of-service vehicles
            if selected_block_number is None:
                cached = vehicle_block_cache.get(vehicle_id)
                if cached:
                    cached_block = cached.get("block_number")
                    cached_shift_end = cached.get("shift_end_ts", 0)
                    if cached_shift_end > now_ts and cached_block in drivers_by_block:
                        selected_block_number = cached_block
                        best_driver = max(drivers_by_block[cached_block], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")

            # Fallback to most recent shift
            if selected_block_number is None:
                best_start_ts = -1
                for blk_num, blk_drivers in drivers_by_block.items():
                    for drv in blk_drivers:
                        start_ts = drv.get("start_ts", 0)
                        if start_ts > best_start_ts:
                            best_start_ts = start_ts
                            selected_block_number = blk_num
                            w2w_position_name = drv.get("position_name")

        # Collect drivers from selected block
        all_drivers: List[DriverInfo] = []
        max_shift_end_ts = 0

        if selected_block_number and selected_block_number in drivers_by_block:
            seen_drivers: Set[Tuple[str, int, int]] = set()
            for driver in drivers_by_block[selected_block_number]:
                driver_key = (driver["name"], driver["start_ts"], driver["end_ts"])
                if driver_key not in seen_drivers:
                    seen_drivers.add(driver_key)
                    all_drivers.append(DriverInfo(
                        name=driver["name"],
                        shift_start=driver["start_ts"],
                        shift_start_label=driver.get("start_label", ""),
                        shift_end=driver["end_ts"],
                        shift_end_label=driver.get("end_label", ""),
                    ))
                    if driver["end_ts"] > max_shift_end_ts:
                        max_shift_end_ts = driver["end_ts"]

        all_drivers.sort(key=lambda d: d.shift_start)
        final_block = w2w_position_name if w2w_position_name else block_name

        # Update cache
        if selected_block_number and max_shift_end_ts > now_ts:
            self._set_vehicle_block_cache(vehicle_id, {
                "block_number": selected_block_number,
                "position_name": w2w_position_name,
                "shift_end_ts": max_shift_end_ts,
            })

        return VehicleDriverEntry(
            vehicle_id=vehicle_id,
            block=final_block,
            drivers=all_drivers,
            vehicle_name=vehicle_name,
        )
