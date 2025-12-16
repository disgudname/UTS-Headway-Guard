"""
Vehicle Drivers Module

This module provides a modular interface for fetching vehicle-to-driver mappings.
Different transit agencies can implement their own providers by subclassing
VehicleDriversProvider and implementing the required methods.

Example usage:
    from vehicle_drivers import VehicleDriversProvider
    from vehicle_drivers.uva import UVAVehicleDriversProvider

    # Create provider with agency-specific dependencies
    provider = UVAVehicleDriversProvider(
        w2w_key="...",
        w2w_url="...",
        fetch_block_groups_fn=fetch_block_groups,
        get_vehicle_state_fn=get_vehicle_state,
        ondemand_client=ondemand_client,
    )

    # Fetch vehicle drivers
    result = await provider.fetch_vehicle_drivers()
    # Returns: {"fetched_at": <timestamp_ms>, "vehicle_drivers": {...}}
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DriverInfo:
    """Information about a driver's shift."""
    name: str
    shift_start: int  # timestamp in milliseconds
    shift_start_label: str  # e.g., "6a"
    shift_end: int  # timestamp in milliseconds
    shift_end_label: str  # e.g., "10a"


@dataclass
class VehicleDriverEntry:
    """Vehicle-to-driver mapping entry."""
    vehicle_id: str
    block: str  # Block identifier (e.g., "[01]", "OnDemand Driver")
    drivers: List[DriverInfo] = field(default_factory=list)
    vehicle_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "vehicle_id": self.vehicle_id,
            "block": self.block,
            "drivers": [
                {
                    "name": d.name,
                    "shift_start": d.shift_start,
                    "shift_start_label": d.shift_start_label,
                    "shift_end": d.shift_end,
                    "shift_end_label": d.shift_end_label,
                }
                for d in self.drivers
            ],
            "vehicle_name": self.vehicle_name,
        }


@dataclass
class VehicleDriversResult:
    """Result from fetching vehicle drivers."""
    fetched_at: int  # timestamp in milliseconds
    vehicle_drivers: Dict[str, VehicleDriverEntry] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "fetched_at": self.fetched_at,
            "vehicle_drivers": {
                vid: entry.to_dict() for vid, entry in self.vehicle_drivers.items()
            },
        }


class VehicleDriversProvider(ABC):
    """
    Abstract base class for vehicle-to-driver mapping providers.

    Transit agencies can implement this interface to provide their own
    vehicle-driver mappings based on their scheduling systems.

    The provider is responsible for:
    1. Fetching vehicle block assignments (from AVL system like TransLoc)
    2. Fetching driver schedules (from scheduling system like WhenToWork)
    3. Joining the data to produce vehicle -> driver mappings

    Implementations should handle:
    - Caching (to avoid excessive API calls)
    - Error handling (graceful degradation when data sources fail)
    - Time zone handling (for shift time calculations)
    """

    @abstractmethod
    async def fetch_vehicle_drivers(self) -> VehicleDriversResult:
        """
        Fetch the current vehicle-to-driver mappings.

        Returns:
            VehicleDriversResult containing the fetched_at timestamp and
            a dictionary mapping vehicle_id to VehicleDriverEntry.

        The implementation should:
        1. Fetch current block assignments for vehicles
        2. Fetch current driver schedules
        3. Join the data to map each vehicle to its driver(s)
        4. Handle overlapping shifts (during driver swaps)
        5. Handle interlined blocks if applicable
        """
        pass

    @abstractmethod
    async def fetch_driver_assignments(self) -> Dict[str, Any]:
        """
        Fetch raw driver assignments from the scheduling system.

        Returns:
            Dictionary containing:
            - "fetched_at": timestamp in milliseconds
            - "assignments_by_block": Dict mapping block identifiers to driver info
            - "disabled": True if the scheduling system integration is disabled

        This is useful for debugging and for the block-drivers endpoint.
        """
        pass

    def get_agency_name(self) -> str:
        """
        Get the name of the agency this provider serves.

        Returns:
            Human-readable agency name (e.g., "University of Virginia")
        """
        return "Unknown Agency"


# Export public API
__all__ = [
    "DriverInfo",
    "VehicleDriverEntry",
    "VehicleDriversResult",
    "VehicleDriversProvider",
]
