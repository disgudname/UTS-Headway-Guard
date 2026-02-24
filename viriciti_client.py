"""ViriCiti SDK WebSocket client for live vehicle SOC data."""

import asyncio
import json
import os
from typing import Dict, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class VehicleSOC:
    """State of charge data for a vehicle."""
    bus_number: str   # The user-facing bus number (ViriCiti "name" field)
    vid: str          # ViriCiti internal ID (e.g., "gillig_1430")
    soc: float        # State of charge percentage 0-100
    odo: Optional[float]  # Odometer in km
    power: Optional[float]  # kW; negative = charging, positive = discharging
    timestamp: datetime

    @property
    def is_charging(self) -> bool:
        """True if power indicates the bus is plugged in and charging."""
        return self.power is not None and self.power < -1.0


class ViriCitiClient:
    """WebSocket client for ViriCiti SDK live data.

    Connects to ViriCiti's WebSocket API to receive real-time SOC updates
    for electric vehicles. Data is keyed by bus number (the "name" field
    in ViriCiti) to match TransLoc vehicle naming.

    Usage:
        client = ViriCitiClient.from_env()
        client._on_soc_update = my_callback
        await client.connect_and_subscribe()
    """

    SDK_WS_URL = "wss://sdk.viriciti.com/api/v2/live"
    SDK_REST_URL = "https://sdk.viriciti.com"

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._vehicles: list[dict] = []           # Raw vehicle list from API
        self._vid_to_bus: Dict[str, str] = {}     # vid -> bus number mapping
        self._soc_cache: Dict[str, VehicleSOC] = {}  # bus_number -> SOC data
        self._connected = False
        self._on_soc_update: Optional[Callable[[VehicleSOC], None]] = None

    @classmethod
    def from_env(cls) -> "ViriCitiClient":
        """Create client from environment variables.

        Requires:
            VIRICITI_API_KEY: API key from portal.viriciti.com/sdk
        """
        api_key = os.getenv("VIRICITI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("Missing VIRICITI_API_KEY environment variable")
        return cls(api_key=api_key)

    async def fetch_vehicle_list(self) -> list[dict]:
        """Fetch available vehicles from Static API and build vid->bus mapping."""
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.SDK_REST_URL}/api/v1/my/assets",
                headers={"x-api-key": self._api_key},
                timeout=15.0
            )
            resp.raise_for_status()
            self._vehicles = resp.json()
            # Build vid -> bus number mapping
            self._vid_to_bus = {
                v["vid"]: v["name"]
                for v in self._vehicles
                if v.get("vid") and v.get("name")
            }
            return self._vehicles

    async def fetch_current_soc(self) -> Dict[str, VehicleSOC]:
        """Fetch current SOC for all vehicles via REST API (one-shot, no WebSocket)."""
        import httpx

        if not self._vid_to_bus:
            await self.fetch_vehicle_list()

        if not self._vid_to_bus:
            return {}

        request_body = {vid: ["soc", "odo", "power"] for vid in self._vid_to_bus.keys()}

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.SDK_REST_URL}/api/v2/state",
                headers={
                    "x-api-key": self._api_key,
                    "Content-Type": "application/json"
                },
                json=request_body,
                timeout=15.0
            )
            resp.raise_for_status()
            data = resp.json()

        # Parse response into VehicleSOC objects
        for vid, params in data.items():
            bus_number = self._vid_to_bus.get(vid)
            if not bus_number:
                continue

            soc_val = params.get("soc", {}).get("value")
            soc_time = params.get("soc", {}).get("time", 0)
            odo_val = params.get("odo", {}).get("value")
            power_val = params.get("power", {}).get("value")

            if soc_val is not None:
                self._soc_cache[bus_number] = VehicleSOC(
                    bus_number=bus_number,
                    vid=vid,
                    soc=float(soc_val),
                    odo=float(odo_val) if odo_val is not None else None,
                    power=float(power_val) if power_val is not None else None,
                    timestamp=datetime.fromtimestamp(soc_time / 1000, tz=timezone.utc)
                )

        return self._soc_cache

    async def connect_and_subscribe(self):
        """Connect to WebSocket and subscribe to SOC for all vehicles.

        This method runs indefinitely, automatically reconnecting on disconnect.
        SOC updates are delivered via the _on_soc_update callback.
        """
        import websockets

        url = f"{self.SDK_WS_URL}?apiKey={self._api_key}"

        while True:
            try:
                async with websockets.connect(url) as ws:
                    self._connected = True
                    print("[viriciti] WebSocket connected")

                    # Fetch vehicle list to get vid->bus mapping
                    await self.fetch_vehicle_list()
                    vids = list(self._vid_to_bus.keys())

                    if not vids:
                        print("[viriciti] No vehicles found, retrying in 60s")
                        self._connected = False
                        await asyncio.sleep(60)
                        continue

                    # Seed initial state from REST API so offline buses still appear
                    try:
                        await self.fetch_current_soc()
                        if self._on_soc_update:
                            for soc_data in self._soc_cache.values():
                                self._on_soc_update(soc_data)
                        print(f"[viriciti] Seeded initial state for {len(self._soc_cache)} vehicles")
                    except Exception as e:
                        print(f"[viriciti] Could not seed initial state: {e}")

                    # Subscribe to SOC, odometer, and power for all vehicles
                    subscription = {"vehicles": {vid: ["soc", "odo", "power"] for vid in vids}}
                    await ws.send(json.dumps(subscription))
                    print(f"[viriciti] Subscribed to {len(vids)} vehicles: {list(self._vid_to_bus.values())}")

                    # Process incoming messages
                    async for message in ws:
                        self._handle_message(message)

            except Exception as e:
                self._connected = False
                print(f"[viriciti] WebSocket error: {e}, reconnecting in 5s")
                await asyncio.sleep(5)

    def _handle_message(self, raw: str):
        """Process incoming WebSocket message."""
        try:
            msg = json.loads(raw)
            msg_type = msg.get("type")
            payload = msg.get("payload", {})

            if msg_type == "vehicles":
                vid = payload.get("vid")
                label = payload.get("label")
                value = payload.get("value")
                ts_ms = payload.get("time", 0)

                if not vid or value is None:
                    return

                bus_number = self._vid_to_bus.get(vid)
                if not bus_number:
                    return

                # Get or create SOC entry
                existing = self._soc_cache.get(bus_number)
                timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

                if label == "soc":
                    soc_data = VehicleSOC(
                        bus_number=bus_number,
                        vid=vid,
                        soc=float(value),
                        odo=existing.odo if existing else None,
                        power=existing.power if existing else None,
                        timestamp=timestamp
                    )
                    self._soc_cache[bus_number] = soc_data
                    if self._on_soc_update:
                        self._on_soc_update(soc_data)

                elif label == "odo" and existing:
                    # Update odometer on existing entry
                    existing.odo = float(value)

                elif label == "power" and existing:
                    # Update power and broadcast so charging state stays current
                    existing.power = float(value)
                    if self._on_soc_update:
                        self._on_soc_update(existing)

            elif msg_type == "error":
                print(f"[viriciti] Error from server: {payload}")

        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            pass  # Skip malformed messages

    def get_all_soc(self) -> Dict[str, VehicleSOC]:
        """Return current SOC cache (keyed by bus number)."""
        return dict(self._soc_cache)

    def get_soc_by_bus(self, bus_number: str) -> Optional[VehicleSOC]:
        """Get SOC for specific bus number."""
        return self._soc_cache.get(bus_number)

    @property
    def is_connected(self) -> bool:
        """Whether WebSocket is currently connected."""
        return self._connected

    @property
    def vehicle_count(self) -> int:
        """Number of vehicles being tracked."""
        return len(self._vid_to_bus)
