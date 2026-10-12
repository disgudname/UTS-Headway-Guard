"""ViriCiti SDK WebSocket client for live vehicle SOC data."""

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urljoin


class _FormParser(HTMLParser):
    """Collects the first <form> on a page: its action and its named inputs."""

    def __init__(self) -> None:
        super().__init__()
        self.action: Optional[str] = None
        self.fields: List[Dict[str, str]] = []
        self._in_form = False
        self._done = False

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        a = {k: (v or "") for k, v in attrs}
        if tag == "form" and not self._done and not self._in_form:
            self._in_form = True
            self.action = a.get("action", "")
        elif self._in_form and tag in ("input", "button") and a.get("name"):
            self.fields.append({"name": a["name"], "type": a.get("type", "text").lower(),
                                "value": a.get("value", "")})

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self._in_form:
            self._in_form = False
            self._done = True


class ViriCitiLoginError(RuntimeError):
    """The dashboard login did not end in a session (bad password, changed page, ...)."""


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
    # Fallback when there is no SDK key: log in to the dashboard like a browser
    # (plain HTML forms at accounts.viriciti.com) and poll its own endpoints,
    # which return the same shapes as the SDK.
    DASHBOARD_URL = "https://dashboard.viriciti.com"
    LOGIN_RETRY_S = 1800.0  # after a failed login, so a bad password cannot lock the account

    def __init__(self, api_key: str = "", username: str = "", password: str = "", poll_s: float = 30.0):
        self._api_key = api_key
        self._username = username
        self._password = password
        self._poll_s = max(10.0, poll_s)
        self._http: Any = None  # httpx.AsyncClient holding the dashboard session cookies
        self._vehicles: list[dict] = []           # Raw vehicle list from API
        self._vid_to_bus: Dict[str, str] = {}     # vid -> bus number mapping
        self._soc_cache: Dict[str, VehicleSOC] = {}  # bus_number -> SOC data
        self._connected = False
        self._on_soc_update: Optional[Callable[[VehicleSOC], None]] = None
        # Dashboard login bookkeeping, served by status() (epoch seconds)
        self._login_count = 0
        self._last_login = 0.0
        self._last_poll_ok = 0.0
        self._last_error = ""       # short, never a URL or a page body
        self._last_error_at = 0.0

    @classmethod
    def from_env(cls) -> "ViriCitiClient":
        """Create client from environment variables.

        Requires one of:
            VIRICITI_API_KEY: API key from portal.viriciti.com/sdk
            VIRICITI_USERNAME + VIRICITI_PASSWORD: a dashboard.viriciti.com
                login (polled every VIRICITI_POLL_S)
        """
        api_key = os.getenv("VIRICITI_API_KEY", "").strip()
        username = os.getenv("VIRICITI_USERNAME", "").strip()
        password = os.getenv("VIRICITI_PASSWORD", "")
        if not api_key and not (username and password):
            raise RuntimeError("Missing VIRICITI_API_KEY or VIRICITI_USERNAME/VIRICITI_PASSWORD")
        try:
            poll_s = float(os.getenv("VIRICITI_POLL_S", "30"))
        except ValueError:
            poll_s = 30.0
        return cls(api_key=api_key, username=username, password=password, poll_s=poll_s)

    @property
    def _use_dashboard(self) -> bool:
        return bool(self._username and self._password) and not self._api_key

    def _dashboard_headers(self) -> Dict[str, str]:
        return {
            "Origin": self.DASHBOARD_URL,
            "Referer": f"{self.DASHBOARD_URL}/dashboard",
        }

    async def _session_ok(self) -> bool:
        resp = await self._http.get(f"{self.DASHBOARD_URL}/api/v1/sessions/my", timeout=15.0)
        return resp.status_code == 200

    async def login(self) -> None:
        """Log in to the dashboard and keep the session cookies in self._http.

        Walks whatever forms the login shows (email, then password), sending
        hidden fields back unchanged. Never logs the credentials or a page body.
        """
        import httpx

        if self._http is not None:
            await self._http.aclose()
        self._http = httpx.AsyncClient(follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0 Safari/537.36",
        })
        resp = await self._http.get(f"{self.DASHBOARD_URL}/dashboard", timeout=20.0)
        password_sent = False
        for _ in range(5):
            form = _FormParser()
            form.feed(resp.text)
            types = {f["type"] for f in form.fields}
            names = {f["name"].lower() for f in form.fields}
            has_password = "password" in types
            has_user = "email" in types or bool(names & {"username", "email"})
            if form.action is None or not (has_password or has_user):
                break
            if has_password and password_sent:
                raise ViriCitiLoginError("login rejected (check VIRICITI_USERNAME / VIRICITI_PASSWORD)")
            data: Dict[str, str] = {}
            for f in form.fields:
                if f["type"] == "password":
                    data[f["name"]] = self._password
                elif f["type"] == "email" or f["name"].lower() in ("username", "email"):
                    data[f["name"]] = self._username
                elif f["type"] not in ("checkbox", "radio"):
                    data[f["name"]] = f["value"]
            password_sent = password_sent or has_password
            resp = await self._http.post(urljoin(str(resp.url), form.action), data=data, timeout=20.0)
        if not await self._session_ok():
            raise ViriCitiLoginError(
                f"login did not reach a session (stopped at {resp.url.host}{resp.url.path}, HTTP {resp.status_code})")
        self._login_count += 1
        self._last_login = time.time()

    async def fetch_vehicle_list(self) -> list[dict]:
        """Fetch available vehicles from Static API and build vid->bus mapping."""
        import httpx
        if self._use_dashboard:
            if self._http is None:
                await self.login()
            resp = await self._http.get(
                f"{self.DASHBOARD_URL}/api/v2/portal/assets",
                params={"filtered": "true", "fields": "name vid"},
                headers=self._dashboard_headers(),
                timeout=15.0
            )
        else:
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

        if self._use_dashboard:
            resp = await self._http.post(
                f"{self.DASHBOARD_URL}/api/v1/viostate",
                headers=self._dashboard_headers(),
                json=request_body,
                timeout=15.0
            )
        else:
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
        if self._use_dashboard:
            await self._poll_dashboard()
            return

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

    async def _poll_dashboard(self):
        """Poll the dashboard's state endpoint forever, logging in again when the session ends."""
        import httpx

        sent: Dict[str, tuple] = {}  # bus_number -> last (soc, power, timestamp) delivered
        while True:
            delay = self._poll_s
            try:
                if self._http is None:
                    await self.login()
                    self._vid_to_bus = {}  # re-read the bus list with the new session
                await self.fetch_current_soc()
                if not self._connected:
                    print(f"[viriciti] dashboard polling OK, {len(self._soc_cache)} vehicles")
                self._connected = True
                self._last_poll_ok = time.time()
                for bus, soc_data in self._soc_cache.items():
                    key = (soc_data.soc, soc_data.power, soc_data.timestamp)
                    if sent.get(bus) != key:
                        sent[bus] = key
                        if self._on_soc_update:
                            self._on_soc_update(soc_data)
            except ViriCitiLoginError as e:
                self._connected = False
                self._http = None
                self._note_error("login failed")
                print(f"[viriciti] {e}; next try in {int(self.LOGIN_RETRY_S / 60)} min")
                delay = self.LOGIN_RETRY_S
            except httpx.HTTPStatusError as e:
                self._connected = False
                self._note_error(f"HTTP {e.response.status_code}")
                if e.response.status_code in (401, 403):
                    print(f"[viriciti] dashboard session ended ({e.response.status_code}), logging in again")
                    self._http = None
                else:
                    print(f"[viriciti] dashboard poll failed: HTTP {e.response.status_code}")
            except Exception as e:
                self._connected = False
                self._note_error(type(e).__name__)
                print(f"[viriciti] dashboard poll failed: {e}")
            await asyncio.sleep(delay)

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

    def _note_error(self, text: str) -> None:
        self._last_error = text
        self._last_error_at = time.time()

    def status(self) -> Dict[str, Any]:
        """How the connection is doing right now (no secrets), for /api/viriciti/soc."""
        def iso(ts: float) -> Optional[str]:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds") if ts else None

        return {
            "connected": self._connected,
            "vehicle_count": self.vehicle_count,
            "mode": "dashboard" if self._use_dashboard else "sdk",
            "logins": self._login_count,  # since the app started; more than 1 means a session ended
            "last_login": iso(self._last_login),
            "last_poll_ok": iso(self._last_poll_ok),
            "last_error": self._last_error or None,
            "last_error_at": iso(self._last_error_at),
        }


if __name__ == "__main__":
    # Check a login from the command line: prints each bus and its charge.
    async def _main() -> None:
        client = ViriCitiClient.from_env()
        for bus, soc in sorted((await client.fetch_current_soc()).items()):
            print(f"{bus}: {soc.soc:.0f}% as of {soc.timestamp.isoformat()}")

    asyncio.run(_main())
