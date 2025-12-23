"""
UTS Operations Dashboard Service — Dispatcher API (FastAPI skeleton)

Purpose
=======
Proxy TransLoc data sources, enrich the feed with safety context, and surface
operations dashboards for UVA's University Transit Service.

Key features in this skeleton
-----------------------------
- Poll TransLoc for routes/vehicles. (HTTP calls sketched; plug in API key & base.)
- Fetch & cache Overpass speed limits per route (fetch-once; invalidate on polyline change).
- Persist telemetry for replay, expose block assignment, and surface low-clearance
  guidance for over-height coaches.
- REST endpoints + Server-Sent Events (SSE) stream.

Run
---
$ uvicorn app:app --reload --port 8080

Environment
-----------
- PYTHON >= 3.10
- pip install fastapi uvicorn httpx pydantic
"""

from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any, Iterable, Union, Sequence, Set, Mapping, Awaitable
from dataclasses import dataclass, field
import asyncio, time, math, os, json, re, base64, hashlib, secrets, csv, io, uuid
from datetime import date, datetime, timedelta, time as dtime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
import httpx
from collections import deque, defaultdict
import xml.etree.ElementTree as ET
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from headway_storage import HeadwayStorage, parse_iso8601_utc
from headway_tracker import (
    HeadwayTracker,
    VehicleSnapshot,
    load_headway_config,
    load_approach_sets_config,
    DEFAULT_STOP_APPROACH_CONFIG_PATH,
    HEADWAY_DISTANCE_THRESHOLD_M,
)

from fastapi import Body, FastAPI, HTTPException, Request, Response, Query
from fastapi.responses import (
    JSONResponse,
    StreamingResponse,
    HTMLResponse,
    FileResponse,
    RedirectResponse,
)
from pathlib import Path
from urllib.parse import quote, unquote, urlparse, urlunparse, parse_qsl, urlencode

from tickets_store import TicketStore
from push_subscriptions import PushSubscriptionStore
from ondemand_client import OnDemandClient
from vehicle_drivers import VehicleDriversProvider
from vehicle_drivers.uva import UVAVehicleDriversProvider, UVAProviderConfig
from uva_athletics import (
    NY_TZ as UVA_TZ,
    ensure_uva_athletics_cache,
    is_home_location,
    load_cached_events,
)
from service_level import (
    SERVICE_SCHEDULE_URL,
    ServiceLevelResult,
    ServiceLevelCache,
    get_service_date,
    parse_service_schedule,
)

# Ensure local time aligns with Charlottesville, VA
os.environ.setdefault("TZ", "America/New_York")
if hasattr(time, "tzset"):
    time.tzset()

# ---------------------------
# Config
# ---------------------------
TRANSLOC_BASE = os.getenv("TRANSLOC_BASE", "https://uva.transloc.com/Services/JSONPRelay.svc")
TRANSLOC_KEY  = os.getenv("TRANSLOC_KEY", "8882812681")
OVERPASS_EP   = os.getenv("OVERPASS_EP", "https://overpass-api.de/api/interpreter")
CAT_API_BASE  = os.getenv("CAT_API_BASE", "https://catpublic.etaspot.net/service.php")
CAT_API_TOKEN = os.getenv("CAT_API_TOKEN", "TESTING")
TRANSLOC_HTTP_TIMEOUT = httpx.Timeout(20.0, connect=5.0, read=20.0, write=20.0, pool=20.0)
TRANSLOC_HTTP_LIMITS = httpx.Limits(max_keepalive_connections=20, max_connections=200)
PULSEPOINT_ENDPOINT = os.getenv(
    "PULSEPOINT_ENDPOINT",
    "https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid=54000,00300",
)
PULSEPOINT_PASSPHRASE = os.getenv("PULSEPOINT_PASSPHRASE", "tombrady5rings")
PULSEPOINT_ICON_BASE = os.getenv(
    "PULSEPOINT_ICON_BASE", "https://web.pulsepoint.org/images/respond_icons/"
)
AMTRAKER_URL = os.getenv("AMTRAKER_URL", "https://api-v3.amtraker.com/v3/trains")
RIDESYSTEMS_CLIENTS_URL = os.getenv(
    "RIDESYSTEMS_CLIENTS_URL",
    "https://admin.ridesystems.net/api/Clients/GetClients",
)
W2W_ASSIGNED_SHIFT_URL = os.getenv(
    "W2W_ASSIGNED_SHIFT_URL",
    "https://www7.whentowork.com/cgi-bin/w2wG.dll/api/AssignedShiftList",
)
W2W_KEY = os.getenv("W2W_KEY")
if W2W_KEY:
    W2W_KEY = W2W_KEY.strip()
W2W_ASSIGNMENT_TTL_S = int(os.getenv("W2W_ASSIGNMENT_TTL_S", "45"))
W2W_POSITION_RE = re.compile(r"\[(\d{1,2})(?:\s*(AM|PM))?\]", re.IGNORECASE)
AM_PM_BLOCKS: set[str] = {f"{number:02d}" for number in range(20, 27)}
W2W_TIME_RE = re.compile(
    r"^\s*(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*([AP])?M?\s*$",
    re.IGNORECASE,
)
_W2W_KEY_QUERY_RE = re.compile(r"(?i)(key=)([^&'\"\s]+)")
_W2W_KEY_ENCODED_RE = re.compile(r"(?i)(key%3D)([^&'\"\s]+)")
TRAIN_TARGET_STATION_CODE = os.getenv("TRAIN_TARGET_STATION_CODE", "").strip().upper()
ORS_KEY = (os.getenv("ORS_KEY") or "").strip()
ORS_DIRECTIONS_URL = os.getenv(
    "ORS_DIRECTIONS_URL", "https://api.openrouteservice.org/v2/directions/driving-car"
).strip()
ORS_HTTP_TIMEOUT_S = float(os.getenv("ORS_HTTP_TIMEOUT_S", "10"))

VEH_REFRESH_S   = int(os.getenv("VEH_REFRESH_S", "5"))
ROUTE_REFRESH_S = int(os.getenv("ROUTE_REFRESH_S", "60"))
BLOCK_REFRESH_S = int(os.getenv("BLOCK_REFRESH_S", "30"))
STALE_FIX_S     = int(os.getenv("STALE_FIX_S", "90"))
VEHICLE_STALE_THRESHOLD_S = int(
    os.getenv("VEHICLE_STALE_THRESHOLD_S", str(60 * 60))
)

# Grace window to keep routes "active" despite brief data hiccups (prevents dispatcher flicker)
ROUTE_GRACE_S   = int(os.getenv("ROUTE_GRACE_S", "60"))

EMA_ALPHA       = float(os.getenv("EMA_ALPHA", "0.40"))
MIN_SPEED_FLOOR = float(os.getenv("MIN_SPEED_FLOOR", "1.2"))
MAX_SPEED_CEIL  = float(os.getenv("MAX_SPEED_CEIL", "22.0"))
MPH_TO_MPS      = 0.44704
DEFAULT_CAP_MPS = 25 * MPH_TO_MPS
HEADING_JITTER_M = float(os.getenv("HEADING_JITTER_M", "3.0"))

# Low clearance lookup around 14th Street bridge
BRIDGE_LAT = 38.03404931117353
BRIDGE_LON = -78.4995922309842
LOW_CLEARANCE_SEARCH_M = 25 * 1609.34  # 25 miles in meters
LOW_CLEARANCE_LIMIT_FT = 11 + 11/12    # 11'11"
BRIDGE_IGNORE_RADIUS_M = 100.0

# Driver/Dispatcher configuration
OVERHEIGHT_BUSES = [
    "25131","25231","25331","25431","17132","14132","12432","18532"
]
LOW_CLEARANCE_RADIUS = 122
BRIDGE_RADIUS = 117
ALL_BUSES = [
    "12132","12232","12332","12432","12532","12632",
    "14132","14232","14332","14432","14532",
    "17132","17232","17332","17432","17532","17632","17732",
    "18132","18232","18332","18432","18532","18632","18732","18832",
    "19132","19232","19332","19432","19532",
    "20131","20231","20331","20431",
    "24012","24112","24212","24312","24412",
    "25131","25231","25331","25431",
]

# Data directories (support multiple mirrored volumes)
DATA_DIRS = [Path(p) for p in os.getenv("DATA_DIRS", "/data").split(":")]
PRIMARY_DATA_DIR = DATA_DIRS[0]

VEH_LOG_DIRS = [
    Path(p)
    for p in os.getenv(
        "VEH_LOG_DIRS",
        os.getenv("VEH_LOG_DIR", str(PRIMARY_DATA_DIR / "vehicle_logs")),
    ).split(":")
]
VEH_LOG_DIR = VEH_LOG_DIRS[0]
HEADWAY_DIR = PRIMARY_DATA_DIR / "headway"

TICKETS_PATH = Path(os.getenv("TICKETS_PATH", str(PRIMARY_DATA_DIR / "tickets.json")))
if TICKETS_PATH.is_absolute():
    try:
        _tickets_commit_name = str(TICKETS_PATH.relative_to(PRIMARY_DATA_DIR))
    except ValueError:
        _tickets_commit_name = str(TICKETS_PATH)
else:
    _tickets_commit_name = str(TICKETS_PATH)
tickets_store = TicketStore(TICKETS_PATH)

# Push notifications (Web Push)
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:ops@virginia.edu")
PUSH_SUBSCRIPTIONS_PATH = PRIMARY_DATA_DIR / "push_subscriptions.json"
push_subscription_store = PushSubscriptionStore(PUSH_SUBSCRIPTIONS_PATH)

# Track sent alert IDs to prevent duplicate notifications
_sent_alert_ids: set = set()
_SENT_ALERT_IDS_PATH = PRIMARY_DATA_DIR / "sent_alert_ids.json"

def _load_sent_alert_ids() -> set:
    try:
        if _SENT_ALERT_IDS_PATH.exists():
            data = json.loads(_SENT_ALERT_IDS_PATH.read_text())
            return set(data.get("ids", []))
    except Exception:
        pass
    return set()

def _save_sent_alert_ids(ids: set) -> None:
    # Keep only last 1000 IDs to prevent unbounded growth
    recent_ids = list(ids)[-1000:]
    _SENT_ALERT_IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SENT_ALERT_IDS_PATH.write_text(
        json.dumps({"ids": recent_ids, "updated_at": datetime.now(timezone.utc).isoformat()})
    )

# ---------------------------
# System Notices (admin-managed alerts for index.html)
# ---------------------------
SYSTEM_NOTICES_PATH = PRIMARY_DATA_DIR / "system_notices.json"

def _load_system_notices() -> List[Dict[str, Any]]:
    """Load system notices from disk."""
    try:
        if SYSTEM_NOTICES_PATH.exists():
            data = json.loads(SYSTEM_NOTICES_PATH.read_text())
            return data.get("notices", [])
    except Exception:
        pass
    return []

def _save_system_notices(notices: List[Dict[str, Any]]) -> None:
    """Save system notices to disk."""
    SYSTEM_NOTICES_PATH.parent.mkdir(parents=True, exist_ok=True)
    SYSTEM_NOTICES_PATH.write_text(
        json.dumps({
            "notices": notices,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }, indent=2)
    )

def _get_active_system_notices(include_auth_only: bool = False) -> List[Dict[str, Any]]:
    """Get currently active system notices, optionally filtering auth-only notices."""
    notices = _load_system_notices()
    now = datetime.now(timezone.utc)
    active = []
    for notice in notices:
        # Check time window
        start_str = notice.get("start_time")
        end_str = notice.get("end_time")
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                if now < start_dt:
                    continue
            except Exception:
                pass
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                if now > end_dt:
                    continue
            except Exception:
                pass
        # Check visibility
        if notice.get("auth_only") and not include_auth_only:
            continue
        active.append(notice)
    return active

# Shared secret required for /sync endpoint
SYNC_SECRET = os.getenv("SYNC_SECRET")

# Dispatcher authentication helpers
DISPATCH_PASSWORDS: Dict[str, str] = {}
DISPATCH_PASSWORD_LABELS: Dict[str, str] = {}
DISPATCH_PASSWORD_TYPES: Dict[str, str] = {}
_DISPATCH_PASSWORD_CACHE: Optional[Tuple[Tuple[str, str, str, str], ...]] = None

_NON_SECRET_ENV_KEYS = {
    "PATH",
    "PWD",
    "HOME",
    "HOSTNAME",
    "TERM",
    "SHLVL",
    "PORT",
    "LOGNAME",
    "USER",
    "TZ",
    "SHELL",
}
_NON_SECRET_ENV_PREFIXES = (
    "PYTHON",
    "LC_",
    "CAAS_",
    "GPG_",
)


def _refresh_dispatch_passwords(force: bool = False) -> None:
    """Load dispatcher passwords from environment secrets."""

    global DISPATCH_PASSWORDS
    global DISPATCH_PASSWORD_LABELS
    global DISPATCH_PASSWORD_TYPES
    global _DISPATCH_PASSWORD_CACHE

    secrets_list: list[Tuple[str, str, str, str]] = []
    for key, value in os.environ.items():
        if key.upper() != key:
            continue
        if not value:
            continue
        access_type = "uts"
        raw_label: Optional[str] = None
        if key.endswith("_CAT_PASS"):
            access_type = "cat"
            base_label = key[: -len("_CAT_PASS")]
            if base_label.endswith("_CAT"):
                raw_label = base_label[: -len("_CAT")]
            else:
                raw_label = base_label
        elif key.endswith("_PASS"):
            raw_label = key[:-5]
        else:
            continue
        normalized = raw_label.strip().lower()
        if not normalized:
            continue
        display_label = raw_label.strip()
        if display_label.isupper():
            display_label = display_label.lower()
        normalized_key = f"{normalized}::{access_type}"
        secrets_list.append((normalized_key, value, display_label, access_type))

    secrets_list.sort()
    cache_state = tuple(secrets_list)
    if not force and cache_state == _DISPATCH_PASSWORD_CACHE:
        return

    DISPATCH_PASSWORDS = {
        normalized: secret for normalized, secret, _label, _type in secrets_list
    }
    DISPATCH_PASSWORD_LABELS = {
        normalized: label for normalized, _secret, label, _type in secrets_list
    }
    DISPATCH_PASSWORD_TYPES = {
        normalized: access_type
        for normalized, _secret, _label, access_type in secrets_list
    }
    _DISPATCH_PASSWORD_CACHE = cache_state


def _iter_loaded_secrets() -> Iterable[Tuple[str, str]]:
    """Yield Fly.io-provided environment secrets in sorted order."""

    items: list[Tuple[str, str]] = []
    for key, value in os.environ.items():
        if not key or not value:
            continue
        if key in _NON_SECRET_ENV_KEYS:
            continue
        if any(key.startswith(prefix) for prefix in _NON_SECRET_ENV_PREFIXES):
            continue
        if key != key.upper():
            continue
        items.append((key, value))

    items.sort(key=lambda item: item[0])
    return items


EXPECTED_ENV_KEYS = sorted(
    {
        "ADSB_CACHE_TTL_S",
        "AMTRAKER_TTL_S",
        "AMTRAKER_URL",
        "BLOCK_REFRESH_S",
        "CAT_API_BASE",
        "CAT_API_TOKEN",
        "CAT_METADATA_TTL_S",
        "CAT_SERVICE_ALERT_TTL_S",
        "CAT_STOP_ETA_TTL_S",
        "CAT_VEHICLE_TTL_S",
        "DATA_DIRS",
        "DISPATCHER_DOWNED_REFRESH_S",
        "DISPATCH_COOKIE_MAX_AGE",
        "DISPATCH_COOKIE_SECURE",
        "EMA_ALPHA",
        "FLY_ALLOC_ID",
        "FLY_MACHINE_ID",
        "FLY_REGION",
        "HEADING_JITTER_M",
        "MAX_SPEED_CEIL",
        "MIN_SPEED_FLOOR",
        "ONDEMAND_DEFAULT_MARKER_COLOR",
        "ONDEMAND_POSITIONS_TTL_S",
        "ONDEMAND_RIDES_LOOKAHEAD_MIN",
        "ONDEMAND_RIDES_LOOKBACK_MIN",
        "ONDEMAND_RIDES_TTL_S",
        "ONDEMAND_RIDES_URL",
        "ONDEMAND_RIDES_WINDOW_PADDING_MIN",
        "ONDEMAND_SCHEDULES_URL",
        "ONDEMAND_VIRTUAL_STOP_MAX_AGE_S",
        "ORS_HTTP_TIMEOUT_S",
        "ORS_KEY",
        "OVERPASS_EP",
        "PULSEPOINT_ICON_TTL_S",
        "PULSEPOINT_PASSPHRASE",
        "PULSEPOINT_TTL_S",
        "RIDESYSTEMS_CLIENT_TTL_S",
        "ROUTE_GRACE_S",
        "ROUTE_REFRESH_S",
        "STALE_FIX_S",
        "SYNC_SECRET",
        "TICKETS_PATH",
        "TRAIN_TARGET_STATION_CODE",
        "TRANSLOC_ARRIVALS_TTL_S",
        "TRANSLOC_BASE",
        "TRANSLOC_BLOCKS_TTL_S",
        "TRANSLOC_KEY",
        "VEHICLE_STALE_THRESHOLD_S",
        "VEH_LOG_DIR",
        "VEH_LOG_INTERVAL_S",
        "VEH_LOG_MIN_MOVE_M",
        "VEH_LOG_RETENTION_MS",
        "VEH_REFRESH_S",
        "W2W_ASSIGNMENT_TTL_S",
        "W2W_KEY",
    }
)


def _missing_env_vars() -> list[str]:
    missing: list[str] = []
    for key in EXPECTED_ENV_KEYS:
        if not os.getenv(key):
            missing.append(key)
    return missing
DISPATCH_COOKIE_NAME = "dispatcher_auth"
DISPATCH_COOKIE_MAX_AGE = int(os.getenv("DISPATCH_COOKIE_MAX_AGE", str(7 * 24 * 3600)))
DISPATCH_COOKIE_SECURE = os.getenv("DISPATCH_COOKIE_SECURE", "").lower() in {
    "1",
    "true",
    "yes",
}

# Vehicle position logging
VEH_LOG_URL = f"{TRANSLOC_BASE}/GetMapVehiclePoints?APIKey={TRANSLOC_KEY}&returnVehiclesNotAssignedToRoute=true"
VEH_LOG_INTERVAL_S = int(os.getenv("VEH_LOG_INTERVAL_S", "4"))
VEH_LOG_RETENTION_MS = int(os.getenv("VEH_LOG_RETENTION_MS", str(7 * 24 * 3600 * 1000)))
VEH_LOG_MIN_MOVE_M = float(os.getenv("VEH_LOG_MIN_MOVE_M", "3"))
LAST_LOG_POS: Dict[int, Tuple[float, float]] = {}
LAST_ROUTE_SNAPSHOT_HASH: Dict[Tuple[str, str], str] = {}

TRANSLOC_ARRIVALS_TTL_S = int(os.getenv("TRANSLOC_ARRIVALS_TTL_S", "15"))
TRANSLOC_BLOCKS_TTL_S = int(os.getenv("TRANSLOC_BLOCKS_TTL_S", "60"))
TRANSLOC_VEHICLE_ESTIMATES_TTL_S = int(os.getenv("TRANSLOC_VEHICLE_ESTIMATES_TTL_S", "5"))
TRANSLOC_CAPACITIES_TTL_S = int(os.getenv("TRANSLOC_CAPACITIES_TTL_S", "10"))
VEHICLE_DRIVERS_TTL_S = int(os.getenv("VEHICLE_DRIVERS_TTL_S", "30"))  # Cache driver mapping for 30s
CAT_METADATA_TTL_S = int(os.getenv("CAT_METADATA_TTL_S", str(5 * 60)))
CAT_VEHICLE_TTL_S = int(os.getenv("CAT_VEHICLE_TTL_S", "5"))
CAT_SERVICE_ALERT_TTL_S = int(os.getenv("CAT_SERVICE_ALERT_TTL_S", "60"))
CAT_STOP_ETA_TTL_S = int(os.getenv("CAT_STOP_ETA_TTL_S", "30"))
PULSEPOINT_TTL_S = int(os.getenv("PULSEPOINT_TTL_S", "20"))
PULSEPOINT_ICON_TTL_S = int(os.getenv("PULSEPOINT_ICON_TTL_S", str(24 * 3600)))
AMTRAKER_TTL_S = int(os.getenv("AMTRAKER_TTL_S", "30"))
RIDESYSTEMS_CLIENT_TTL_S = int(os.getenv("RIDESYSTEMS_CLIENT_TTL_S", str(12 * 3600)))
ONDEMAND_POSITIONS_TTL_S = int(os.getenv("ONDEMAND_POSITIONS_TTL_S", "5"))
ONDEMAND_SCHEDULES_URL = (os.getenv("ONDEMAND_SCHEDULES_URL") or "").strip()
ONDEMAND_RIDES_URL = (os.getenv("ONDEMAND_RIDES_URL") or "").strip()
ONDEMAND_RIDES_TTL_S = int(os.getenv("ONDEMAND_RIDES_TTL_S", "30"))
ONDEMAND_RIDES_LOOKBACK_MIN = int(
    os.getenv("ONDEMAND_RIDES_LOOKBACK_MIN", str(8 * 60))
)
ONDEMAND_RIDES_LOOKAHEAD_MIN = int(
    os.getenv("ONDEMAND_RIDES_LOOKAHEAD_MIN", str(2 * 60))
)
ONDEMAND_RIDES_WINDOW_PADDING_MIN = int(
    os.getenv("ONDEMAND_RIDES_WINDOW_PADDING_MIN", "30")
)
ONDEMAND_VIRTUAL_STOP_MAX_AGE_S = int(os.getenv("ONDEMAND_VIRTUAL_STOP_MAX_AGE_S", str(10 * 60)))
ONDEMAND_DEFAULT_MARKER_COLOR = (os.getenv("ONDEMAND_DEFAULT_MARKER_COLOR") or "#ec4899").strip() or "#ec4899"
if not ONDEMAND_DEFAULT_MARKER_COLOR.startswith("#"):
    ONDEMAND_DEFAULT_MARKER_COLOR = f"#{ONDEMAND_DEFAULT_MARKER_COLOR.lstrip('#')}"
ADSB_CACHE_TTL_S = float(os.getenv("ADSB_CACHE_TTL_S", "15"))
DISPATCHER_DOWNED_SHEET_URL = os.getenv(
    "DISPATCHER_DOWNED_SHEET_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vRZz9HtiUnA6MONcaHw_Kz1Cd8dHhm7Gt9OBuOy7bPfNiHaGYvkVlONxttrUgNCjXdLDnDcgCh4IeQH/pub?gid=0&single=true&output=csv",
)
DISPATCHER_DOWNED_REFRESH_S = int(os.getenv("DISPATCHER_DOWNED_REFRESH_S", "60"))


VEHICLE_STALE_FIELDS = (
    "IsStale",
    "Stale",
    "StaleGPS",
    "HasValidGps",
    "IsRealtime",
    "SecondsSinceReport",
    "SecondsSinceLastReport",
    "SecondsSinceLastUpdate",
    "SecondsSinceUpdate",
    "SecondsSinceLastGps",
    "LastGpsAgeSeconds",
    "LocationAge",
    "GPSSignalAge",
    "Age",
    "AgeInSeconds",
)

def prune_old_entries() -> None:
    cutoff = int(time.time() * 1000) - VEH_LOG_RETENTION_MS
    for log_dir in VEH_LOG_DIRS:
        if not log_dir.exists():
            continue
        for path in log_dir.glob("*.jsonl"):
            try:
                dt = datetime.strptime(path.stem, "%Y%m%d_%H")
            except ValueError:
                continue
            end_ms = int(dt.timestamp() * 1000) + 3600 * 1000
            if end_ms < cutoff:
                try:
                    path.unlink()
                except OSError:
                    pass

def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{int(time.time()*1000)}.tmp")
    tmp.write_text(data)
    tmp.replace(path)


def _write_data_file(name: str, data: str) -> None:
    path_obj = Path(name)
    if path_obj.is_absolute():
        _atomic_write(path_obj, data)
        return
    for base in DATA_DIRS:
        _atomic_write(base / path_obj, data)


tickets_store.set_commit_handler(lambda payload: _write_data_file(_tickets_commit_name, payload))

# ---------------------------
# Geometry helpers
# ---------------------------
R_EARTH = 6371000.0

def to_rad(d: float) -> float: return d * math.pi / 180.0

def haversine(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = a; lat2, lon2 = b
    dlat = to_rad(lat2-lat1); dlon = to_rad(lon2-lon1)
    s = math.sin(dlat/2)**2 + math.cos(to_rad(lat1))*math.cos(to_rad(lat2))*math.sin(dlon/2)**2
    return 2 * R_EARTH * math.asin(math.sqrt(s))

def decode_polyline(enc: str) -> List[Tuple[float, float]]:
    # Minimal Google Encoded Polyline Decoder
    points = []; index = lat = lng = 0
    while index < len(enc):
        result = shift = 0
        while True:
            b = ord(enc[index]) - 63; index += 1
            result |= (b & 0x1f) << shift; shift += 5
            if b < 0x20: break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat
        result = shift = 0
        while True:
            b = ord(enc[index]) - 63; index += 1
            result |= (b & 0x1f) << shift; shift += 5
            if b < 0x20: break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng
        points.append((lat / 1e5, lng / 1e5))
    return points


def _serialize_vehicle_for_log(vehicle: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(vehicle, Mapping):
        return None
    vid = vehicle.get("VehicleID")
    lat_raw = vehicle.get("Latitude")
    lon_raw = vehicle.get("Longitude")
    if vid is None or lat_raw is None or lon_raw is None:
        return None
    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except (TypeError, ValueError):
        return None

    entry: Dict[str, Any] = {
        "VehicleID": vid,
        "Latitude": lat,
        "Longitude": lon,
    }

    route_id = vehicle.get("RouteID") or vehicle.get("RouteId")
    if route_id is not None:
        entry["RouteID"] = route_id

    heading = vehicle.get("Heading")
    if heading is not None:
        entry["Heading"] = heading

    speed = vehicle.get("GroundSpeed")
    if speed is not None:
        entry["GroundSpeed"] = speed

    name = vehicle.get("Name") or vehicle.get("Label") or vehicle.get("CallName")
    if name not in (None, ""):
        entry["Name"] = name
        entry.setdefault("Label", name)

    call_name = vehicle.get("CallName")
    if call_name not in (None, "", name):
        entry["CallName"] = call_name

    timestamp = (
        vehicle.get("Timestamp")
        or vehicle.get("TimeStamp")
        or vehicle.get("ReportTimestamp")
        or vehicle.get("UpdatedOn")
        or vehicle.get("UpdatedAt")
    )
    if timestamp is not None:
        entry["Timestamp"] = timestamp

    for flag in VEHICLE_STALE_FIELDS:
        if flag in vehicle:
            entry[flag] = vehicle.get(flag)

    return entry

def ll_to_xy(lat: float, lon: float, ref_lat: float, ref_lon: float) -> Tuple[float, float]:
    """Approximate meters in a local tangent plane using equirectangular scaling.
    Use ref point for origin; good enough for segment-level projection.
    """
    kx = 111320.0 * math.cos(to_rad((lat + ref_lat) * 0.5))
    ky = 110540.0
    return ((lon - ref_lon) * kx, (lat - ref_lat) * ky)

def bearing_between(a: Tuple[float,float], b: Tuple[float,float]) -> float:
    """Approximate heading in degrees from point ``a`` to ``b`` using a local
    tangent plane where 0° is north and 90° is east."""
    dx, dy = ll_to_xy(b[0], b[1], a[0], a[1])
    return (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0

def heading_diff(a: float, b: float) -> float:
    """Smallest absolute difference between two headings in degrees."""
    return abs((a - b + 180.0) % 360.0 - 180.0)

def cumulative_distance(poly: List[Tuple[float,float]]) -> Tuple[List[float], float]:
    cum = [0.0]
    for i in range(1, len(poly)):
        cum.append(cum[-1] + haversine(poly[i-1], poly[i]))
    return cum, cum[-1] if cum else 0.0

def parse_msajax(s: Optional[str]) -> Optional[int]:
    """Extract UTC milliseconds from TransLoc's ``/Date(ms±offset)/`` format."""
    if not s:
        return None
    try:
        m = re.search(r"/Date\((\d+)([-+]\d{4})?\)/", s)
        if not m:
            return None
        base_ms = int(m.group(1))
        offset_raw = m.group(2)
        if not offset_raw:
            return base_ms
        try:
            sign = 1 if offset_raw.startswith("+") else -1
            hours = int(offset_raw[1:3])
            minutes = int(offset_raw[3:])
            offset_ms = sign * (hours * 60 + minutes) * 60 * 1000
            return base_ms + offset_ms
        except ValueError:
            return base_ms
    except ValueError as e:
        print(f"[parse_msajax] invalid timestamp {s!r}: {e}")
        return None


def normalize_bus_name(name: Optional[str]) -> str:
    """Normalize bus identifiers to bare numeric strings.

    TransLoc sometimes returns names with suffixes or whitespace; mileage
    tracking and service crew views expect plain numeric bus numbers. This
    helper strips any non-digit characters so lookups remain consistent.
    """
    if not name:
        return ""
    return re.sub(r"\D", "", str(name))


def parse_maxheight(val: Optional[str]) -> Optional[float]:
    """Parse an OSM maxheight tag into feet."""
    if not val:
        return None
    s = str(val).strip().lower()
    try:
        if "'" in s:
            m = re.match(r"\s*(\d+)\s*'\s*(\d+)?", s)
            if m:
                ft = float(m.group(1))
                inch = float(m.group(2) or 0)
                return ft + inch/12.0
        if "m" in s:
            num = float(re.findall(r"[0-9.]+", s)[0])
            return num * 3.28084
        num = float(re.findall(r"[0-9.]+", s)[0])
        return num
    except Exception:
        return None


def parse_maxspeed(val: Optional[str]) -> Optional[float]:
    """Parse an OSM maxspeed tag into miles per hour."""
    if not val:
        return None
    s = str(val).strip().lower()
    try:
        num = float(re.findall(r"[0-9.]+", s)[0])
    except Exception:
        return None
    if "km/h" in s or "kph" in s:
        return num * 0.621371
    return num

# ---------------------------
# Data models
# ---------------------------
@dataclass
class Vehicle:
    id: Optional[int]
    name: str
    lat: float
    lon: float
    ts_ms: int
    ground_mps: float
    age_s: float
    heading: float = 0.0
    s_pos: float = 0.0
    ema_mps: float = 0.0
    dir_sign: int = 0  # +1 forward, -1 reverse, 0 unknown
    seg_idx: int = 0
    along_mps: float = 0.0

@dataclass
class Route:
    id: int
    name: str
    encoded: str
    poly: List[Tuple[float, float]]
    cum: List[float]
    length_m: float
    color: Optional[str] = None
    seg_caps_mps: List[float] = field(default_factory=list)
    seg_names: List[str] = field(default_factory=list)


@dataclass
class BusDay:
    total_miles: float = 0.0
    reset_miles: float = 0.0
    day_miles: float = 0.0
    blocks: set[str] = field(default_factory=set)
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None


LOW_CLEARANCES_CACHE: Optional[List[Dict[str, float]]] = None

# ---------------------------
# HTTP clients
# ---------------------------
def sanitize_transloc_base(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None
    cleaned = url.strip()
    if not cleaned:
        return None
    # Decode URL-encoded values (handles double-encoding like %253A -> %3A -> :)
    # Keep decoding while the string contains encoded characters
    prev = None
    while prev != cleaned and '%' in cleaned:
        prev = cleaned
        cleaned = unquote(cleaned)
    return cleaned.rstrip("/")


DEFAULT_TRANSLOC_BASE = sanitize_transloc_base(TRANSLOC_BASE) or TRANSLOC_BASE.rstrip("/")


def build_transloc_url(base_url: Optional[str], path: str) -> str:
    base = sanitize_transloc_base(base_url) or DEFAULT_TRANSLOC_BASE
    if base != DEFAULT_TRANSLOC_BASE:
        parsed = urlparse(base)
        if parsed.scheme and parsed.netloc and (parsed.path in ("", "/")):
            default_path = urlparse(DEFAULT_TRANSLOC_BASE).path
            if default_path:
                parsed = parsed._replace(path=default_path)
                base = urlunparse(parsed)
                base = base.rstrip("/") or base
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def transloc_host_base(base_url: Optional[str]) -> str:
    sanitized = sanitize_transloc_base(base_url) or DEFAULT_TRANSLOC_BASE
    parsed = urlparse(sanitized)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    parsed_default = urlparse(DEFAULT_TRANSLOC_BASE)
    if parsed_default.scheme and parsed_default.netloc:
        return f"{parsed_default.scheme}://{parsed_default.netloc}"
    return "https://uva.transloc.com"


def is_default_transloc_base(base_url: Optional[str]) -> bool:
    """Check if base_url resolves to the default TransLoc base.

    Handles cases where client passes just the host (e.g. https://uva.transloc.com)
    which build_transloc_url would expand to the full default path.
    """
    sanitized = sanitize_transloc_base(base_url)
    if sanitized is None:
        return True
    if sanitized == DEFAULT_TRANSLOC_BASE:
        return True
    # Check if it's just the host of the default (no path or root path)
    # which build_transloc_url would expand to the default
    parsed = urlparse(sanitized)
    default_parsed = urlparse(DEFAULT_TRANSLOC_BASE)
    if (parsed.scheme == default_parsed.scheme and
        parsed.netloc == default_parsed.netloc and
        parsed.path in ("", "/")):
        return True
    return False


async def fetch_routes_with_shapes(client: httpx.AsyncClient, base_url: Optional[str] = None):
    url = build_transloc_url(base_url, f"GetRoutesForMapWithScheduleWithEncodedLine?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])


async def fetch_routes_catalog(client: httpx.AsyncClient, base_url: Optional[str] = None):
    url = build_transloc_url(base_url, f"GetRoutes?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("d", [])
    return []


async def fetch_stops(client: httpx.AsyncClient, base_url: Optional[str] = None) -> List[Dict[str, Any]]:
    url = build_transloc_url(base_url, f"GetStops?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("d", [])
    return []

async def fetch_vehicles(
    client: httpx.AsyncClient,
    include_unassigned: bool = True,
    base_url: Optional[str] = None,
):
    # returnVehiclesNotAssignedToRoute=true returns vehicles even if not assigned to a route
    flag = "true" if include_unassigned else "false"
    url = build_transloc_url(
        base_url,
        f"GetMapVehiclePoints?APIKey={TRANSLOC_KEY}&returnVehiclesNotAssignedToRoute={flag}",
    )
    r = await client.get(url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_vehicle_capacities(
    client: httpx.AsyncClient,
    base_url: Optional[str] = None,
) -> Dict[int, Dict[str, Any]]:
    """
    Fetch vehicle capacities from TransLoc.
    Returns a dict mapping vehicle_id -> {capacity, current_occupation, percentage}
    """
    url = build_transloc_url(base_url, f"GetVehicleCapacities?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    capacities_list = data if isinstance(data, list) else data.get("d", [])

    # Convert list to dict keyed by VehicleID for fast lookup
    capacities_dict: Dict[int, Dict[str, Any]] = {}
    for item in capacities_list:
        if isinstance(item, dict) and "VehicleID" in item:
            vid = item.get("VehicleID")
            if vid is not None:
                capacities_dict[vid] = {
                    "capacity": item.get("Capacity"),
                    "current_occupation": item.get("CurrentOccupation"),
                    "percentage": item.get("Percentage"),
                }
    return capacities_dict

async def fetch_block_groups(
    client: httpx.AsyncClient,
    base_url: Optional[str] = None,
    *,
    include_metadata: bool = False,
) -> Union[List[Dict], Tuple[List[Dict], Dict[str, Any]]]:
    d = datetime.now(ZoneInfo("America/New_York"))
    ds = f"{d.month}/{d.day}/{d.year}"
    r1_url = build_transloc_url(
        base_url, f"GetScheduleVehicleCalendarByDateAndRoute?dateString={quote(ds)}"
    )
    r1 = await client.get(r1_url, timeout=TRANSLOC_HTTP_TIMEOUT)
    record_api_call("GET", r1_url, r1.status_code)
    r1.raise_for_status()
    sched = r1.json()
    sched = sched if isinstance(sched, list) else sched.get("d", [])
    ids = ",".join(str(s.get("ScheduleVehicleCalendarID")) for s in sched if s.get("ScheduleVehicleCalendarID"))
    payload: Dict[str, Any] = {}
    block_groups: List[Dict[str, Any]] = []
    if ids:
        r2_url = build_transloc_url(
            base_url, f"GetDispatchBlockGroupData?scheduleVehicleCalendarIdsString={ids}"
        )
        r2 = await client.get(r2_url, timeout=TRANSLOC_HTTP_TIMEOUT)
        record_api_call("GET", r2_url, r2.status_code)
        r2.raise_for_status()
        payload = r2.json()
        block_groups = payload.get("BlockGroups", [])

    if include_metadata:
        return block_groups, payload
    return block_groups


def _extract_plain_language_blocks(
    block_groups: Iterable[Dict[str, Any]],
    vehicle_roster: Optional[Iterable[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    def _safe_int(value: Any) -> Optional[int]:
        try:
            iv = int(value)
        except (TypeError, ValueError):
            return None
        return iv if iv else None

    roster_names: Dict[int, str] = {}
    if vehicle_roster:
        for entry in vehicle_roster:
            if not isinstance(entry, dict):
                continue
            vehicle_id = _safe_int(entry.get("VehicleID") or entry.get("VehicleId"))
            if not vehicle_id:
                continue
            name_val = entry.get("Name") or entry.get("VehicleName")
            name = str(name_val).strip() if name_val is not None else ""
            if name:
                roster_names[vehicle_id] = name

    plain_blocks: List[Dict[str, Any]] = []
    for group in block_groups:
        group_id_raw = group.get("BlockGroupId")
        group_id = str(group_id_raw).strip() if group_id_raw is not None else ""
        if not group_id:
            continue
        if re.search(r"\[(\d+)\]", group_id):
            continue
        blocks = group.get("Blocks") or []
        for block in blocks:
            block_id_raw = block.get("BlockId")
            block_id = str(block_id_raw).strip() if block_id_raw is not None else ""
            if not block_id:
                continue

            route_info = block.get("Route") or {}
            route_id = route_info.get("RouteId") or route_info.get("RouteID")
            try:
                route_id = int(route_id)
            except (TypeError, ValueError):
                route_id = None
            route_name = (
                route_info.get("Description")
                or route_info.get("RouteName")
                or ""
            )
            route_color = _normalize_hex_color(
                route_info.get("Color")
                or route_info.get("RouteColor")
            )

            vehicle_id = _safe_int(block.get("VehicleId"))
            vehicle_name = ""
            trips = block.get("Trips") or []
            for trip in trips:
                if not vehicle_id:
                    vehicle_id = _safe_int(trip.get("VehicleID"))
                if not vehicle_name:
                    name_val = trip.get("VehicleName")
                    if name_val:
                        vehicle_name = str(name_val).strip()
                if vehicle_id and vehicle_name:
                    break

            if not vehicle_name:
                name_val = block.get("VehicleName")
                if name_val:
                    vehicle_name = str(name_val).strip()

            if not vehicle_name and vehicle_id:
                roster_name = roster_names.get(vehicle_id)
                if roster_name:
                    vehicle_name = roster_name

            plain_blocks.append(
                {
                    "block_id": block_id,
                    "block_group_id": group_id,
                    "vehicle_id": vehicle_id,
                    "vehicle_name": vehicle_name or None,
                    "route_id": route_id,
                    "route_name": route_name or None,
                    "route_color": route_color,
                }
            )

    return plain_blocks


def _block_entry_vehicle_id(entry: Dict[str, Any]) -> Optional[str]:
    """Normalize the vehicle identifier for a block entry."""

    return _normalize_vehicle_id_str(
        entry.get("vehicle_id")
        or entry.get("vehicleId")
        or entry.get("VehicleId")
        or entry.get("VehicleID")
    )

async def fetch_overpass_speed_profile(route: Route, client: httpx.AsyncClient) -> Tuple[List[float], List[str]]:
    """Build per-segment speed caps (m/s) and road names using OSM maxspeed data."""
    pts = route.poly
    if len(pts) < 2:
        return [], []
    lats = [p[0] for p in pts]
    lons = [p[1] for p in pts]
    pad = 0.001  # ~100m padding to cover route bbox
    min_lat, max_lat = min(lats)-pad, max(lats)+pad
    min_lon, max_lon = min(lons)-pad, max(lons)+pad
    query = f"""
[out:json][timeout:25];
way({min_lat},{min_lon},{max_lat},{max_lon})["maxspeed"]; 
out geom;
"""
    try:
        r = await client.post(OVERPASS_EP, data=query, timeout=60)
        record_api_call("POST", OVERPASS_EP, r.status_code)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return [DEFAULT_CAP_MPS for _ in range(len(pts)-1)], [""] * (len(pts)-1)

    ways: List[Dict[str, Any]] = []
    for el in data.get("elements", []):
        mph = parse_maxspeed(el.get("tags", {}).get("maxspeed"))
        if mph is None:
            continue
        ways.append({
            "speed_mps": mph * MPH_TO_MPS,
            "geometry": el.get("geometry", []),
            "name": el.get("tags", {}).get("name", ""),
        })

    caps: List[float] = []
    names: List[str] = []
    for i in range(len(pts)-1):
        a = pts[i]
        b = pts[i+1]
        mid_lat = (a[0] + b[0]) / 2
        mid_lon = (a[1] + b[1]) / 2
        best_speed = DEFAULT_CAP_MPS
        best_name = ""
        best_d = 50.0  # meters
        for way in ways:
            speed = way["speed_mps"]
            name = way.get("name", "")
            for node in way["geometry"]:
                d = haversine((mid_lat, mid_lon), (node.get("lat"), node.get("lon")))
                if d < best_d:
                    best_d = d
                    best_speed = speed
                    best_name = name
        caps.append(best_speed)
        names.append(best_name)

    return caps, names


async def fetch_low_clearances(client: httpx.AsyncClient) -> List[Dict[str, float]]:
    query = f"""
[out:json][timeout:25];
(
  node(around:{int(LOW_CLEARANCE_SEARCH_M)},{BRIDGE_LAT},{BRIDGE_LON})["maxheight"];
  way(around:{int(LOW_CLEARANCE_SEARCH_M)},{BRIDGE_LAT},{BRIDGE_LON})["maxheight"];
);
out center;
"""
    r = await client.post(OVERPASS_EP, data=query, timeout=60)
    record_api_call("POST", OVERPASS_EP, r.status_code)
    r.raise_for_status()
    data = r.json()
    items: List[Dict[str, float]] = []
    for el in data.get("elements", []):
        mh = el.get("tags", {}).get("maxheight")
        ft = parse_maxheight(mh)
        if ft is None or ft > LOW_CLEARANCE_LIMIT_FT:
            continue
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        if lat is None or lon is None:
            continue
        if haversine((lat, lon), (BRIDGE_LAT, BRIDGE_LON)) < BRIDGE_IGNORE_RADIUS_M:
            continue
        items.append({"lat": lat, "lon": lon, "maxheight": mh})
    return items

# ---------------------------
# Core maths (abridged but functional)
# ---------------------------
def project_vehicle_to_route(v: Vehicle, route: Route, prev_idx: Optional[int] = None,
                             heading: Optional[float] = None) -> Tuple[float, int]:
    """Project vehicle to the nearest point on the polyline (by segment),
    returning cumulative arc-length ``s`` (meters) and the segment index.

    When multiple segments are nearly equidistant, prefer the one aligned with
    ``heading`` or closest to ``prev_idx`` to stabilise projections on
    overlapping bidirectional segments.
    """
    pts = route.poly; cum = route.cum
    best_d2 = 1e30
    best_s = 0.0
    best_i = 0
    best_heading: Optional[float] = None
    for i in range(len(pts) - 1):
        a_lat, a_lon = pts[i]
        b_lat, b_lon = pts[i+1]
        # Local XY in meters with A as origin
        ax, ay = 0.0, 0.0
        bx, by = ll_to_xy(b_lat, b_lon, a_lat, a_lon)
        px, py = ll_to_xy(v.lat, v.lon, a_lat, a_lon)
        vx, vy = bx - ax, by - ay
        wx, wy = px - ax, py - ay
        vv = vx*vx + vy*vy
        t = 0.0 if vv <= 0 else max(0.0, min(1.0, (wx*vx + wy*vy) / vv))
        projx = ax + t*vx; projy = ay + t*vy
        dx = px - projx; dy = py - projy
        d2 = dx*dx + dy*dy
        seg_len = haversine((a_lat, a_lon), (b_lat, b_lon))
        s = cum[i] + t * seg_len
        seg_heading = bearing_between((a_lat, a_lon), (b_lat, b_lon))
        if d2 < best_d2 - 1e-6:
            best_d2 = d2; best_s = s; best_i = i; best_heading = seg_heading
        elif abs(d2 - best_d2) <= 4.0:  # within ~2 m
            prefer = False
            if heading is not None and best_heading is not None:
                if heading_diff(heading, seg_heading) + 1e-3 < heading_diff(heading, best_heading):
                    prefer = True
            elif prev_idx is not None:
                nseg = len(pts) - 1
                # Treat the polyline as circular when comparing segment indices
                diff_new = abs(i - prev_idx)
                diff_best = abs(best_i - prev_idx)
                if min(diff_new, nseg - diff_new) < min(diff_best, nseg - diff_best):
                    prefer = True
            if prefer:
                best_d2 = d2; best_s = s; best_i = i; best_heading = seg_heading
    return best_s, best_i

# ---------------------------
# App & state
# ---------------------------
app = FastAPI(title="UTS Operations Dashboard")


@app.on_event("startup")
async def init_ondemand_client() -> None:
    try:
        app.state.ondemand_client = OnDemandClient.from_env()
    except RuntimeError as exc:
        print(f"[ondemand] client not configured: {exc}")
        app.state.ondemand_client = None


@app.on_event("startup")
async def init_transloc_client() -> None:
    app.state.transloc_client = httpx.AsyncClient(
        timeout=TRANSLOC_HTTP_TIMEOUT,
        limits=TRANSLOC_HTTP_LIMITS,
    )
    app.state.transloc_timing_history: Dict[str, deque[float]] = {}


@app.on_event("shutdown")
async def shutdown_ondemand_client() -> None:
    client = getattr(app.state, "ondemand_client", None)
    if client is not None:
        await client.aclose()
    transloc_client = getattr(app.state, "transloc_client", None)
    if transloc_client is not None:
        await transloc_client.aclose()


@app.on_event("startup")
async def warm_caches() -> None:
    """Pre-warm caches after initial data is loaded to avoid cold-cache latency."""
    async def _warm():
        # Wait for updater to populate initial data
        await asyncio.sleep(8)
        try:
            client = _get_transloc_client(None)
            async with state.lock:
                vehicle_ids = [
                    v.get("VehicleID") for v in getattr(state, "vehicles_raw", [])
                    if v.get("VehicleID") and v.get("RouteID")
                ]
            if vehicle_ids:
                # Pre-fetch stop estimates to warm the cache
                await _fetch_vehicle_stop_estimates_raw(
                    vehicle_ids=vehicle_ids,
                    base_url=None,
                    quantity=3,
                    client=client,
                )
                print(f"[startup] warmed stop estimates cache for {len(vehicle_ids)} vehicles")
        except Exception as e:
            print(f"[startup] cache warming failed: {e}")

    # Run in background so it doesn't block startup
    asyncio.create_task(_warm())


BASE_DIR = Path(__file__).resolve().parent
HTML_DIR = BASE_DIR / "html"
CSS_DIR = BASE_DIR / "css"
SCRIPT_DIR = BASE_DIR / "scripts"
FONT_DIR = BASE_DIR / "fonts"
MEDIA_DIR = BASE_DIR / "media"
ARRIVAL_SOUNDS_DIR = MEDIA_DIR / "arrivalsounds"


def _load_html(name: str) -> str:
    return (HTML_DIR / name).read_text(encoding="utf-8")


DRIVER_HTML = _load_html("driver.html")
DISPATCHER_HTML = _load_html("dispatcher.html")
MAP_HTML = _load_html("map.html")
TESTMAP_HTML = _load_html("testmap.html")
KIOSKMAP_HTML = _load_html("kioskmap.html")
CATTESTMAP_HTML = _load_html("cattestmap.html")
MADMAP_HTML = _load_html("madmap.html")
METROMAP_HTML = _load_html("metromap.html")
ADMIN_HTML = _load_html("admin.html")
SERVICECREW_HTML = _load_html("servicecrew.html")
LANDING_HTML = _load_html("index.html")
APICALLS_HTML = _load_html("apicalls.html")
DEBUG_HTML = _load_html("debug.html")
REPLAY_HTML = _load_html("replay.html")
RIDERSHIP_HTML = _load_html("ridership.html")
TRANSLOC_TICKER_HTML = _load_html("transloc_ticker.html")
SITEMAP_HTML = _load_html("sitemap.html")
ARRIVALSDISPLAY_HTML = _load_html("arrivalsdisplay.html")
CLOCKDISPLAY_HTML = _load_html("clockdisplay.html")
STATUSSIGNAGE_HTML = _load_html("statussignage.html")
BUS_TABLE_HTML = _load_html("buses.html")
NOT_FOUND_HTML = _load_html("404.html")
RADAR_HTML = _load_html("radar.html")
EINK_BLOCK_HTML = _load_html("eink-block.html")
STOP_APPROACH_HTML = _load_html("stop-approach.html")
DOWNED_HTML = _load_html("downed.html")
IPS_HTML = _load_html("ips.html")
LOGIN_HTML = _load_html("login.html")
REPAIRS_HTML = _load_html("repairs.html")
REPAIRS_SCREEN_HTML = _load_html("repairsscreen.html")
REPAIRS_EXPORT_HTML = _load_html("repairsexport.html")
HEADWAY_HTML = _load_html("headway.html")
HEADWAY_DIAGNOSTICS_HTML = _load_html("headway_diagnostics.html")
OFFLINE_HTML = _load_html("offline.html")

ADSB_URL_TEMPLATE = "https://opendata.adsb.fi/api/v2/lat/{lat}/lon/{lon}/dist/{dist}"
ADSB_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "*",
}

_downed_sheet_lock = asyncio.Lock()
_downed_sheet_csv: Optional[str] = None
_downed_sheet_fetched_at: float = 0.0
_downed_sheet_last_attempt: float = 0.0
_downed_sheet_error: Optional[str] = None

@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    if "text/html" in request.headers.get("accept", ""):
        return HTMLResponse(NOT_FOUND_HTML, status_code=404)
    detail = getattr(exc, "detail", "Not Found")
    return JSONResponse({"detail": detail}, status_code=404)

API_CALL_LOG = deque(maxlen=100)
API_CALL_SUBS: set[asyncio.Queue] = set()
SERVICECREW_SUBS: set[asyncio.Queue] = set()
TESTMAP_VEHICLES_SUBS: set[asyncio.Queue] = set()

def record_api_call(method: str, url: str, status: int) -> None:
    item = {"ts": int(time.time()*1000), "method": method, "url": url, "status": status}
    API_CALL_LOG.append(item)
    encoded = f"data: {json.dumps(item)}\n\n"
    for q in list(API_CALL_SUBS):
        try:
            q.put_nowait(encoded)
        except asyncio.QueueFull:
            pass  # Drop update for slow clients

def broadcast_testmap_vehicles(payload: List[Dict[str, Any]]) -> None:
    """Broadcast vehicle updates to all SSE subscribers."""
    if not TESTMAP_VEHICLES_SUBS:
        return
    # Filter out stale vehicles once, encode JSON once, send to all
    filtered = [v for v in payload if not v.get("IsVeryStale")]
    data = {"ts": int(time.time() * 1000), "vehicles": filtered}
    encoded = f"data: {json.dumps(data)}\n\n"
    for q in list(TESTMAP_VEHICLES_SUBS):
        try:
            q.put_nowait(encoded)
        except asyncio.QueueFull:
            pass  # Drop update for slow clients

CONFIG_KEYS = [
    "TRANSLOC_BASE","TRANSLOC_KEY","OVERPASS_EP",
    "VEH_REFRESH_S","ROUTE_REFRESH_S","STALE_FIX_S","ROUTE_GRACE_S",
    "EMA_ALPHA","MIN_SPEED_FLOOR","MAX_SPEED_CEIL",
    "DEFAULT_CAP_MPS","BRIDGE_LAT","BRIDGE_LON","LOW_CLEARANCE_SEARCH_M",
    "LOW_CLEARANCE_LIMIT_FT","BRIDGE_IGNORE_RADIUS_M","OVERHEIGHT_BUSES",
    "LOW_CLEARANCE_RADIUS","BRIDGE_RADIUS","ALL_BUSES"
]
CONFIG_NAME = "config.json"
CONFIG_FILE = PRIMARY_DATA_DIR / CONFIG_NAME

CONFIG_METADATA: Dict[str, Any] = {}

def _current_machine_info() -> Dict[str, str]:
    machine_id = os.getenv("FLY_MACHINE_ID") or os.getenv("FLY_ALLOC_ID") or "unknown"
    region = os.getenv("FLY_REGION") or "unknown"
    return {"machine_id": machine_id, "region": region}


def _provenance_headers(info: Dict[str, str]) -> Dict[str, str]:
    return {"X-Machine-Id": info["machine_id"], "X-Region": info["region"]}


def _base_response_fields(ok: bool, saved: bool, info: Dict[str, str]) -> Dict[str, Any]:
    return {
        "ok": ok,
        "saved": saved,
        "saved_by": info,
        "_served_by": info,
    }

EINK_BLOCK_LAYOUT_NAME = "eink_block_layout.json"

class TTLCache:
    def __init__(self, ttl: float):
        self.ttl = ttl
        self.value: Any = None
        self.ts: float = 0.0
        self.lock = asyncio.Lock()
        self._inflight: Optional[asyncio.Task] = None

    async def get(self, fetcher):
        async with self.lock:
            now = time.time()
            if self.value is not None and now - self.ts < self.ttl:
                return self.value
            # Singleflight: reuse in-flight fetch task
            if self._inflight is not None:
                inflight_task = self._inflight
            else:
                inflight_task = asyncio.create_task(fetcher())
                self._inflight = inflight_task

        try:
            data = await inflight_task
        except Exception:
            async with self.lock:
                if self._inflight is inflight_task:
                    self._inflight = None
            raise

        async with self.lock:
            if self._inflight is inflight_task:
                self.value = data
                self.ts = time.time()
                self._inflight = None
        return data


class StaleWhileRevalidateCache:
    def __init__(self, ttl: float):
        self.ttl = ttl
        self.value: Any = None
        self.ts: float = 0.0
        self.refresh_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()

    async def get(self, fetcher):
        async with self.lock:
            if self.value is None:
                if self.refresh_task is None:
                    self.refresh_task = asyncio.create_task(fetcher())
                # Reuse the existing refresh_task (completed or running) to seed a cold cache.
                seed_task = self.refresh_task
                seed = True
            else:
                seed = False
                seed_task = None
                value = self.value
                ts = self.ts
                refresh_task = self.refresh_task

        if seed:
            try:
                data = await seed_task
            except Exception as exc:
                async with self.lock:
                    if self.refresh_task is seed_task:
                        self.refresh_task = None
                print(
                    f"[transloc_vehicle_estimates_cache] seed failed: {exc}"
                )
                return {}, "seed_failed"

            if data is None:
                data = {}

            async with self.lock:
                if self.value is None:
                    self.value = data
                    self.ts = time.time()
                value = self.value
                if self.refresh_task is seed_task:
                    self.refresh_task = None

            return value, "seed"

        now = time.time()
        is_fresh = now - ts < self.ttl
        if (not is_fresh) and (refresh_task is None or refresh_task.done()):
            async with self.lock:
                if self.refresh_task is None or self.refresh_task.done():
                    self.refresh_task = asyncio.create_task(self._refresh(fetcher))

        assert value is not None
        return value, "fresh" if is_fresh else "stale"

    async def _refresh(self, fetcher):
        start = time.perf_counter()
        try:
            data = await fetcher()
        except Exception as exc:
            duration = time.perf_counter() - start
            print(
                f"[transloc_vehicle_estimates_cache] refresh failed after {duration:.2f}s: {exc}"
            )
            return

        duration = time.perf_counter() - start
        async with self.lock:
            self.value = data
            self.ts = time.time()
        print(
            f"[transloc_vehicle_estimates_cache] refresh completed in {duration:.2f}s"
        )


class PerKeyStaleWhileRevalidateCache:
    def __init__(self, ttl: float, max_keys: int = 100):
        self.ttl = ttl
        self.max_keys = max_keys
        self._caches: Dict[Any, StaleWhileRevalidateCache] = {}
        self._access_order: List[Any] = []
        self._lock = asyncio.Lock()

    async def get(self, key: Any, fetcher):
        async with self._lock:
            cache = self._caches.get(key)
            if cache is None:
                # Evict oldest entries if at capacity
                while len(self._caches) >= self.max_keys and self._access_order:
                    oldest = self._access_order.pop(0)
                    self._caches.pop(oldest, None)
                cache = StaleWhileRevalidateCache(self.ttl)
                self._caches[key] = cache
                self._access_order.append(key)
            else:
                # Move to end (most recently used)
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)
        return await cache.get(fetcher)


class PerKeyTTLCache:
    def __init__(self, ttl: float, max_keys: int = 100):
        self.ttl = ttl
        self.max_keys = max_keys
        self._caches: Dict[Any, TTLCache] = {}
        self._access_order: List[Any] = []
        self._lock = asyncio.Lock()

    async def get(self, key: Any, fetcher):
        async with self._lock:
            cache = self._caches.get(key)
            if cache is None:
                # Evict oldest entries if at capacity
                while len(self._caches) >= self.max_keys and self._access_order:
                    oldest = self._access_order.pop(0)
                    self._caches.pop(oldest, None)
                cache = TTLCache(self.ttl)
                self._caches[key] = cache
                self._access_order.append(key)
            else:
                # Move to end (most recently used)
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)
        return await cache.get(fetcher)


def load_config() -> None:
    global CONFIG_METADATA
    for base in DATA_DIRS:
        path = base / CONFIG_NAME
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            print(f"[load_config] error: {e}")
            continue
        metadata: Dict[str, Any] = {}
        if isinstance(data, dict):
            candidate = data.get("_metadata") or data.get("metadata")
            if isinstance(candidate, dict):
                metadata = candidate
            config_source = data.get("config") if isinstance(data.get("config"), dict) else data
        else:
            config_source = {}
        CONFIG_METADATA = metadata
        for k, v in config_source.items():
            if k in CONFIG_KEYS:
                cur = globals().get(k)
                if isinstance(cur, list):
                    globals()[k] = v if isinstance(v, list) else [str(v)]
                else:
                    try:
                        globals()[k] = type(cur)(v)
                    except Exception:
                        globals()[k] = v
        return


def save_config(
    provenance: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, Any], bool, Optional[Dict[str, Any]]]:
    global CONFIG_METADATA
    info = provenance or _current_machine_info()
    metadata = {
        "saved_at": int(time.time()),
        "machine_id": info.get("machine_id", "unknown"),
        "region": info.get("region", "unknown"),
    }
    CONFIG_METADATA = metadata
    payload_map = {k: globals().get(k) for k in CONFIG_KEYS}
    payload_map["_metadata"] = metadata
    try:
        payload = json.dumps(payload_map)
    except Exception as e:
        print(f"[save_config] encode error: {e}")
        return metadata, False, None
    try:
        _write_data_file(CONFIG_NAME, payload)
    except Exception as e:
        print(f"[save_config] error persisting config: {e}")
        return metadata, False, None
    return metadata, True, None


load_config()


def _normalize_layout_cell(cell: Any) -> Any:
    if cell is None:
        return None
    if isinstance(cell, (int, float)):
        cell = str(cell)
    if isinstance(cell, str):
        text = cell.strip()
        return text
    if isinstance(cell, dict):
        result: Dict[str, str] = {}
        if "value" in cell:
            value = cell.get("value")
        elif "label" in cell:
            value = cell.get("label")
        elif "block" in cell:
            value = cell.get("block")
        else:
            value = ""
        display = cell.get("display") if "display" in cell else cell.get("text")
        value_str = str(value).strip() if value is not None else ""
        display_str = str(display).strip() if display is not None else ""
        if value_str:
            result["value"] = value_str
        if display_str:
            result["display"] = display_str
        return result if result else None
    return None


def _is_layout_cell_empty(cell: Any) -> bool:
    if cell is None:
        return True
    if isinstance(cell, str):
        return not cell.strip()
    if isinstance(cell, dict):
        return not any(str(value).strip() for value in cell.values())
    return True


def _column_has_content(column: Sequence[Any]) -> bool:
    return any(not _is_layout_cell_empty(cell) for cell in column)


def _row_has_content(columns: Sequence[Sequence[Any]], row_index: int) -> bool:
    for column in columns:
        if row_index < len(column) and not _is_layout_cell_empty(column[row_index]):
            return True
    return False


def _trim_layout_edges(layout: List[List[Any]]) -> List[List[Any]]:
    if not layout:
        return []

    start = 0
    end = len(layout)

    while start < end and not _column_has_content(layout[start]):
        start += 1
    while end > start and not _column_has_content(layout[end - 1]):
        end -= 1

    trimmed_columns = [list(column) for column in layout[start:end]]
    if not trimmed_columns:
        return []

    max_rows = max(len(column) for column in trimmed_columns)
    if max_rows == 0:
        return trimmed_columns

    top = 0
    bottom = max_rows - 1

    while top <= bottom and not _row_has_content(trimmed_columns, top):
        top += 1
    while bottom >= top and not _row_has_content(trimmed_columns, bottom):
        bottom -= 1

    if top == 0 and bottom == max_rows - 1:
        return trimmed_columns

    result: List[List[Any]] = []
    for column in trimmed_columns:
        if bottom < top:
            result.append([])
        else:
            result.append(column[top : bottom + 1])
    return result


def _normalize_layout_column(column: Any) -> List[Any]:
    if not isinstance(column, list):
        return []
    normalized: List[Any] = []
    for cell in column:
        normalized.append(_normalize_layout_cell(cell))
    return normalized


def _normalize_layout(layout: Any) -> List[List[Any]]:
    if not isinstance(layout, list):
        return []
    normalized: List[List[Any]] = []
    for column in layout:
        normalized.append(_normalize_layout_column(column))
    return _trim_layout_edges(normalized)


def _decode_layout_payload(payload: Any) -> Tuple[List[List[Any]], Optional[int], Optional[Dict[str, Any]]]:
    layout_data: Any
    updated_at: Optional[int] = None
    saved_by: Optional[Dict[str, Any]] = None
    if isinstance(payload, dict):
        layout_data = payload.get("layout")
        ts = payload.get("updated_at")
        if isinstance(ts, (int, float)):
            updated_at = int(ts)
        sb = payload.get("saved_by")
        if isinstance(sb, dict):
            saved_by = {
                "machine_id": sb.get("machine_id"),
                "region": sb.get("region"),
            }
    else:
        layout_data = payload
    layout = _normalize_layout(layout_data)
    return layout, updated_at, saved_by


def _normalize_layout_identifier(layout_id: Any) -> str:
    if layout_id is None:
        return "default"
    if isinstance(layout_id, (int, float)) and not isinstance(layout_id, bool):
        layout_id = str(int(layout_id)) if float(layout_id).is_integer() else str(layout_id)
    text = str(layout_id).strip()
    if not text:
        return "default"
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    sanitized = sanitized.strip("_")
    if not sanitized:
        return "default"
    return sanitized[:64]


def _decode_layout_collection(raw: Any) -> Dict[str, Dict[str, Any]]:
    layouts: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict) and isinstance(raw.get("layouts"), dict):
        for key, value in raw["layouts"].items():
            layout, updated_at, saved_by = _decode_layout_payload(value)
            if not layout:
                continue
            identifier = _normalize_layout_identifier(key)
            entry: Dict[str, Any] = {"layout": layout, "updated_at": updated_at}
            if saved_by:
                entry["saved_by"] = saved_by
            layouts[identifier] = entry
    else:
        layout, updated_at, saved_by = _decode_layout_payload(raw)
        if layout:
            entry = {"layout": layout, "updated_at": updated_at}
            if saved_by:
                entry["saved_by"] = saved_by
            layouts["default"] = entry
    return layouts


def _read_eink_layout_store() -> Dict[str, Dict[str, Any]]:
    store: Dict[str, Dict[str, Any]] = {}
    for base in DATA_DIRS:
        path = base / EINK_BLOCK_LAYOUT_NAME
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text())
        except Exception as exc:
            print(f"[eink_layout] error reading {path}: {exc}")
            continue
        layouts = _decode_layout_collection(raw)
        for key, value in layouts.items():
            existing = store.get(key)
            if not existing or (value.get("updated_at") or 0) >= (existing.get("updated_at") or 0):
                store[key] = value
    return store


def _determine_layout_identifier(payload: Any, *candidates: Any) -> str:
    values: List[Any] = [candidate for candidate in candidates if candidate is not None]
    if isinstance(payload, dict):
        for key in ("layout_id", "layoutId", "layout_key", "layoutKey", "name"):
            if key in payload:
                values.append(payload[key])
    for value in values:
        identifier = _normalize_layout_identifier(value)
        if identifier == "default" and isinstance(value, str) and not value.strip():
            continue
        return identifier
    return "default"


def load_eink_block_layout(layout_id: Optional[str] = None) -> Tuple[List[List[Any]], Optional[int], str, Dict[str, Dict[str, Any]]]:
    store = _read_eink_layout_store()
    requested_id = _normalize_layout_identifier(layout_id)
    if requested_id not in store:
        requested_id = "default" if "default" in store else (next(iter(store.keys()), requested_id))
    entry = store.get(requested_id, {"layout": [], "updated_at": None})
    layout = entry.get("layout", [])
    updated_at = entry.get("updated_at")
    return layout, updated_at, requested_id, store


def save_eink_block_layout(
    layout_payload: Any,
    layout_id: Optional[str] = None,
    provenance: Optional[Dict[str, str]] = None,
) -> Tuple[List[List[Any]], int, str, Dict[str, Dict[str, Any]], Dict[str, Any]]:
    layout = _normalize_layout(layout_payload)
    if not layout:
        raise ValueError("layout must contain at least one column")
    store = _read_eink_layout_store()
    identifier = _normalize_layout_identifier(layout_id)
    timestamp = int(time.time())
    entry: Dict[str, Any] = {"layout": layout, "updated_at": timestamp}
    info = provenance or _current_machine_info()
    if info:
        entry["saved_by"] = {
            "machine_id": info.get("machine_id", "unknown"),
            "region": info.get("region", "unknown"),
        }
    store[identifier] = entry
    serialized_layouts: Dict[str, Dict[str, Any]] = {}
    for key, value in store.items():
        item: Dict[str, Any] = {
            "layout": value["layout"],
            "updated_at": value.get("updated_at"),
        }
        if value.get("saved_by"):
            item["saved_by"] = value.get("saved_by")
        serialized_layouts[key] = item
    payload = {
        "layouts": serialized_layouts,
        "updated_at": timestamp,
    }
    encoded = json.dumps(payload)
    _write_data_file(EINK_BLOCK_LAYOUT_NAME, encoded)
    return layout, timestamp, identifier, store, None


def remove_eink_block_layout(
    layout_id: Optional[str] = None,
) -> Tuple[bool, Dict[str, Dict[str, Any]], Optional[Dict[str, Any]]]:
    store = _read_eink_layout_store()
    identifier = _normalize_layout_identifier(layout_id)
    if identifier == "default":
        raise ValueError("default layout cannot be deleted")
    existing = store.pop(identifier, None)
    if existing is None:
        return False, store, None
    timestamp = int(time.time())
    serialized_layouts = {
        key: {"layout": value["layout"], "updated_at": value.get("updated_at")}
        for key, value in store.items()
    }
    payload = {"layouts": serialized_layouts, "updated_at": timestamp}
    encoded = json.dumps(payload)
    _write_data_file(EINK_BLOCK_LAYOUT_NAME, encoded)
    return True, store, None


class State:
    def __init__(self):
        self.routes: Dict[int, Route] = {}
        self.vehicles_by_route: Dict[int, Dict[int, Vehicle]] = {}
        # Remember each vehicle's last known direction (+1/-1) even if it drops
        # out of the feed briefly. This helps preserve ring assignment when a
        # bus is stationary and TransLoc stops reporting movement, preventing
        # leader picking from resetting the bus to direction 0.
        self.last_dir_sign: Dict[int, int] = {}
        # Cache the most recent stabilised heading per vehicle so front-ends
        # can restore orientation immediately on reload.
        self.last_headings: Dict[int, Dict[str, Any]] = {}
        self.last_headings_dirty: bool = False
        self.lock = asyncio.Lock()
        self.last_overpass_note: str = ""
        # Added: error surfacing & active route tracking
        self.last_error: str = ""
        self.last_error_ts: float = 0.0
        self.active_route_ids: set[int] = set()
        self.route_last_seen: dict[int, float] = {}
        # Caches for proxied TransLoc data
        self.blocks_cache: Optional[Dict] = None
        self.blocks_cache_ts: float = 0.0
        self.anti_cache: Optional[Dict] = None
        self.anti_cache_ts: float = 0.0
        # Per-day mileage and block history
        self.bus_days: Dict[str, Dict[str, BusDay]] = {}
        self.vehicles_raw: List[Dict[str, Any]] = []
        self.stops: List[Dict[str, Any]] = []
        # Vehicle capacities from GetVehicleCapacities API
        self.vehicle_capacities: Dict[int, Dict[str, Any]] = {}
        # Pre-computed route ID to name mapping for performance
        self.route_id_to_name: Dict[Any, str] = {}
        # Cache of last known block assignment per vehicle
        # Used to persist block/driver info when vehicle goes out of service
        # until driver shift ends. Format:
        # {vehicle_id: {"block_number": "06", "position_name": "[06]",
        #               "drivers": [...], "shift_end_ts": 1234567890}}
        self.vehicle_block_cache: Dict[str, Dict[str, Any]] = {}
        # Pre-fetched stop estimates for all active vehicles (updated in background)
        self.stop_estimates: Dict[Any, List[Dict[str, Any]]] = {}
        # Pre-materialized testmap vehicles payload (updated in background)
        self.testmap_vehicles_payload: Optional[List[Dict[str, Any]]] = None
        self.testmap_vehicles_ts: float = 0.0
        # Pre-computed vehicle dropdown lists (sorted, deduplicated)
        self.vehicles_dropdown_active: List[Dict[str, Any]] = []
        self.vehicles_dropdown_all: List[Dict[str, Any]] = []
        # Cached vehicle driver mapping
        self.vehicle_drivers_cache: Optional[Dict[str, Any]] = None
        self.vehicle_drivers_ts: float = 0.0
        # Anti-bunching status tracking (anti-flap protection)
        # Require 2 consecutive failures before switching to OFFLINE
        # Require 1 successful evaluation to return to ONLINE
        self.anti_bunching_consecutive_failures: int = 0
        self.anti_bunching_last_status: str = "N/A"
        self.anti_bunching_status_ts: float = 0.0
        # Service level cache (scraped from parking.virginia.edu)
        self.service_level_cache: ServiceLevelCache = ServiceLevelCache()
        self.service_level_lock: asyncio.Lock = asyncio.Lock()

state = State()
MILEAGE_NAME = "mileage.json"
MILEAGE_FILE = PRIMARY_DATA_DIR / MILEAGE_NAME

transloc_arrivals_cache = TTLCache(TRANSLOC_ARRIVALS_TTL_S)
transloc_blocks_cache = TTLCache(TRANSLOC_BLOCKS_TTL_S)
transloc_vehicle_estimates_cache = PerKeyStaleWhileRevalidateCache(
    TRANSLOC_VEHICLE_ESTIMATES_TTL_S
)
transloc_capacities_cache = PerKeyTTLCache(TRANSLOC_CAPACITIES_TTL_S)
cat_routes_cache = TTLCache(CAT_METADATA_TTL_S)
cat_stops_cache = TTLCache(CAT_METADATA_TTL_S)
cat_patterns_cache = TTLCache(CAT_METADATA_TTL_S)
cat_vehicles_cache = TTLCache(CAT_VEHICLE_TTL_S)
cat_service_alerts_cache = TTLCache(CAT_SERVICE_ALERT_TTL_S)
cat_stop_etas_cache = PerKeyTTLCache(CAT_STOP_ETA_TTL_S)
pulsepoint_cache = TTLCache(PULSEPOINT_TTL_S)
pulsepoint_icon_cache = PerKeyTTLCache(PULSEPOINT_ICON_TTL_S)
amtraker_cache = TTLCache(AMTRAKER_TTL_S)
ridesystems_clients_cache = TTLCache(RIDESYSTEMS_CLIENT_TTL_S)
ondemand_positions_cache = TTLCache(ONDEMAND_POSITIONS_TTL_S)
ondemand_rides_cache = TTLCache(ONDEMAND_RIDES_TTL_S)
ondemand_schedules_cache_state: Dict[str, Any] = {"etag": None, "data": None}
ondemand_schedules_cache_lock = asyncio.Lock()
w2w_assignments_cache = TTLCache(W2W_ASSIGNMENT_TTL_S)
vehicle_drivers_cache = TTLCache(VEHICLE_DRIVERS_TTL_S)
adsb_cache: Dict[Tuple[str, str, str], Tuple[float, Any]] = {}
adsb_cache_lock = asyncio.Lock()


def _is_within_ondemand_operating_window(now: Optional[datetime] = None) -> bool:
    tz = ZoneInfo("America/New_York")
    current = now.astimezone(tz) if now else datetime.now(tz)
    minutes = current.hour * 60 + current.minute + current.second / 60.0
    start_minutes = 19 * 60 + 30
    end_minutes = 5 * 60 + 30
    return minutes >= start_minutes or minutes < end_minutes

PULSEPOINT_FIRST_ON_SCENE: Dict[str, Dict[str, Any]] = {}
PULSEPOINT_FIRST_ON_SCENE_LOCK = asyncio.Lock()

VEHICLE_HEADINGS_NAME = Path(os.environ.get("VEHICLE_HEADINGS_FILE", "vehicle_headings.json")).name


def normalize_heading_deg(value: float) -> float:
    return ((value % 360) + 360) % 360


def decode_vehicle_headings_payload(raw: Any) -> Dict[int, Dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    result: Dict[int, Dict[str, Any]] = {}
    for key, entry in raw.items():
        try:
            vid = int(key)
        except (TypeError, ValueError):
            continue
        heading_val: Optional[float]
        updated_at: Optional[int] = None
        if isinstance(entry, dict):
            heading_val = entry.get("heading")
            if heading_val is None:
                heading_val = entry.get("Heading")
            ts_val = (
                entry.get("updated_at")
                or entry.get("updatedAt")
                or entry.get("timestamp")
                or entry.get("ts_ms")
                or entry.get("ts")
            )
            if ts_val is not None:
                try:
                    updated_at = int(ts_val)
                except (TypeError, ValueError):
                    updated_at = None
        else:
            heading_val = entry
        try:
            heading_float = float(heading_val) if heading_val is not None else None
        except (TypeError, ValueError):
            heading_float = None
        if heading_float is None or not math.isfinite(heading_float):
            continue
        normalized = normalize_heading_deg(heading_float)
        result[vid] = {"heading": normalized, "updated_at": updated_at}
    return result


def load_vehicle_headings() -> None:
    loaded: Dict[int, Dict[str, Any]] = {}
    for base in DATA_DIRS:
        path = base / VEHICLE_HEADINGS_NAME
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text())
        except Exception as e:
            print(f"[vehicle_headings] error reading {path}: {e}")
            continue
        loaded = decode_vehicle_headings_payload(raw)
        break
    state.last_headings = loaded
    state.last_headings_dirty = False


def save_vehicle_headings() -> None:
    try:
        payload: Dict[str, Dict[str, Any]] = {}
        for vid, entry in state.last_headings.items():
            heading_val = entry.get("heading") if isinstance(entry, dict) else None
            if heading_val is None or not math.isfinite(heading_val):
                continue
            record: Dict[str, Any] = {"heading": float(heading_val)}
            ts_val = entry.get("updated_at") if isinstance(entry, dict) else None
            if ts_val is not None:
                try:
                    record["updated_at"] = int(ts_val)
                except (TypeError, ValueError):
                    pass
            payload[str(vid)] = record
        payload_json = json.dumps(payload)
    except Exception as e:
        print(f"[vehicle_headings] encode error: {e}")
        return
    try:
        _write_data_file(VEHICLE_HEADINGS_NAME, payload_json)
    except Exception as e:
        print(f"[vehicle_headings] commit error: {e}")


load_vehicle_headings()

def load_bus_days() -> None:
    for base in DATA_DIRS:
        path = base / MILEAGE_NAME
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
            for date, buses in data.items():
                day: Dict[str, BusDay] = {}
                for bus, bd in buses.items():
                    day[bus] = BusDay(
                        total_miles=bd.get("total_miles", 0.0),
                        reset_miles=bd.get("reset_miles", 0.0),
                        day_miles=bd.get("day_miles", 0.0),
                        blocks=set(bd.get("blocks", [])),
                        last_lat=bd.get("last_lat"),
                        last_lon=bd.get("last_lon"),
                    )
                state.bus_days[date] = day
            return
        except Exception as e:
            print(f"[load_bus_days] error: {e}")
    

def save_bus_days() -> None:
    try:
        payload: Dict[str, Dict[str, Any]] = {}
        for date, buses in state.bus_days.items():
            payload[date] = {}
            for bus, bd in buses.items():
                payload[date][bus] = {
                    "total_miles": bd.total_miles,
                    "reset_miles": bd.reset_miles,
                    "day_miles": bd.day_miles,
                    "blocks": sorted(list(bd.blocks)),
                    "last_lat": bd.last_lat,
                    "last_lon": bd.last_lon,
                }
        payload_json = json.dumps(payload)
    except Exception as e:
        print(f"[save_bus_days] encode error: {e}")
        return
    try:
        _write_data_file(MILEAGE_NAME, payload_json)
    except Exception as e:
        print(f"[save_bus_days] commit error: {e}")

# ---------------------------
# ADS-B proxy
# ---------------------------


@app.api_route("/adsb", methods=["GET", "OPTIONS"])
async def adsb_proxy(request: Request, lat: Optional[str] = None, lon: Optional[str] = None, dist: Optional[str] = None):
    cors_headers = ADSB_CORS_HEADERS.copy()
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=cors_headers)
    if lat is None or lon is None or dist is None:
        raise HTTPException(status_code=400, detail="lat, lon, and dist are required", headers=cors_headers)
    upstream_url = ADSB_URL_TEMPLATE.format(lat=lat, lon=lon, dist=dist)
    key = (lat, lon, dist)
    now = time.time()
    async with adsb_cache_lock:
        cached = adsb_cache.get(key)
        if cached and now - cached[0] < ADSB_CACHE_TTL_S:
            return JSONResponse(content=cached[1], status_code=200, headers=cors_headers)
    try:
        async with httpx.AsyncClient() as client:
            upstream_resp = await client.get(upstream_url, timeout=10)
            record_api_call("GET", upstream_url, upstream_resp.status_code)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="upstream request failed", headers=cors_headers) from exc
    try:
        payload = upstream_resp.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="invalid upstream response", headers=cors_headers) from exc
    async with adsb_cache_lock:
        adsb_cache[key] = (time.time(), payload)
    return JSONResponse(content=payload, status_code=upstream_resp.status_code, headers=cors_headers)

# ---------------------------
# Sync endpoint
# ---------------------------

@app.post("/sync")
async def receive_sync(payload: dict):
    secret = payload.get("secret")
    if SYNC_SECRET is None or secret != SYNC_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    name = payload.get("name")
    data = payload.get("data")
    if (
        not isinstance(name, str)
        or name not in {CONFIG_NAME, MILEAGE_NAME, VEHICLE_HEADINGS_NAME, EINK_BLOCK_LAYOUT_NAME}
        or not isinstance(data, str)
    ):
        raise HTTPException(status_code=400, detail="invalid payload")
    parsed_headings: Optional[Dict[int, Dict[str, Any]]] = None
    if name == VEHICLE_HEADINGS_NAME:
        try:
            parsed_headings = decode_vehicle_headings_payload(json.loads(data))
        except Exception as e:
            print(f"[sync] error decoding vehicle headings payload: {e}")
            parsed_headings = None
    elif name == EINK_BLOCK_LAYOUT_NAME:
        try:
            decoded = json.loads(data)
            layouts = _decode_layout_collection(decoded)
            if not layouts:
                raise ValueError("empty layout collection")
        except Exception as exc:
            print(f"[sync] invalid layout payload: {exc}")
            raise HTTPException(status_code=400, detail="invalid payload") from exc
    for base in DATA_DIRS:
        path = base / name
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(data)
        except Exception as e:
            print(f"[sync] error writing {path}: {e}")
    if parsed_headings is not None:
        async with state.lock:
            state.last_headings = parsed_headings
            state.last_headings_dirty = False
    return {"ok": True}


def _require_sync_secret(payload: Dict[str, Any]) -> None:
    if SYNC_SECRET is None:
        return
    if payload.get("secret") != SYNC_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")


# ---------------------------
# Health
# ---------------------------
@app.get("/v1/health")
async def health():
    async with state.lock:
        ok = not bool(state.last_error)
        return {"ok": ok, "last_error": (state.last_error or None), "last_error_ts": (state.last_error_ts or None)}


def _coerce_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if math.isnan(seconds) or math.isinf(seconds):
            return None
        if seconds > 10_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.isdigit():
                return datetime.fromtimestamp(float(text), tz=timezone.utc)
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                return parsedate_to_datetime(text)
            except (TypeError, ValueError):
                return None
    return None


def _normalize_hex_color(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("#"):
        text = text[1:]
    text = text.lower()
    if not re.fullmatch(r"[0-9a-f]{3,8}", text):
        return None
    if len(text) == 3:
        text = "".join(ch * 2 for ch in text)
    if len(text) not in {6, 8}:
        return None
    return f"#{text}"


async def fetch_ondemand_schedules(client: OnDemandClient) -> List[Dict[str, Any]]:
    if not ONDEMAND_SCHEDULES_URL:
        return []

    async with ondemand_schedules_cache_lock:
        cached_data = ondemand_schedules_cache_state.get("data")
        cached_etag = ondemand_schedules_cache_state.get("etag")

    headers: Dict[str, str] = {}
    if cached_etag:
        headers["If-None-Match"] = cached_etag

    try:
        response = await client.get_resource(
            ONDEMAND_SCHEDULES_URL, extra_headers=headers or None
        )
    except Exception as exc:
        print(f"[ondemand] schedules fetch failed: {exc}")
        return cached_data or []

    if response.status_code == 304:
        return cached_data or []
    if response.status_code >= 400:
        print(f"[ondemand] schedules fetch error: {response.status_code}")
        return cached_data or []

    try:
        payload = response.json()
    except ValueError as exc:
        print(f"[ondemand] schedules decode failed: {exc}")
        return cached_data or []

    data: List[Dict[str, Any]]
    if isinstance(payload, list):
        data = payload
    elif isinstance(payload, dict):
        candidate = payload.get("data")
        data = candidate if isinstance(candidate, list) else []
    else:
        data = []

    async with ondemand_schedules_cache_lock:
        ondemand_schedules_cache_state["data"] = data
        ondemand_schedules_cache_state["etag"] = response.headers.get("ETag")

    return data


def _format_ondemand_timestamp(value: datetime) -> str:
    iso = value.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    if iso.endswith("+00:00"):
        return f"{iso[:-6]}Z"
    return iso


def _extract_schedule_time_bounds(
    schedules: Optional[Sequence[Dict[str, Any]]],
) -> tuple[Optional[datetime], Optional[datetime]]:
    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None

    if not schedules:
        return (None, None)

    for vehicle in schedules:
        if not isinstance(vehicle, dict):
            continue
        stops = vehicle.get("stops")
        if not isinstance(stops, list):
            continue
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            ts = _parse_timestamp(stop.get("timestamp") or stop.get("time"))
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
            if earliest is None or ts < earliest:
                earliest = ts
            if latest is None or ts > latest:
                latest = ts

    return (earliest, latest)


def _normalize_ride_id(value: Any) -> str:
    ride_id = str(value).strip() if value not in {None, ""} else ""
    return ride_id.lower()


def _resolve_ride_status(
    ride: Mapping[str, Any], status_lookup: Mapping[str, Any]
) -> Optional[str]:
    """Resolve ride status from lookup or ride object.

    status_lookup can be either:
    - Dict[str, str]: old format mapping ride_id -> status
    - Dict[str, Dict[str, Any]]: new format mapping ride_id -> ride_info with 'status' key
    """
    ride_id_val = ride.get("ride_id") or ride.get("rideId") or ride.get("id")
    ride_id_normalized = _normalize_ride_id(ride_id_val)
    if ride_id_normalized:
        mapped = status_lookup.get(ride_id_normalized)
        if isinstance(mapped, dict):
            # New format: ride_info dict with 'status' key
            status_val = mapped.get("status")
            if status_val not in {None, ""}:
                return status_val
        elif mapped not in {None, ""}:
            # Old format: direct status string
            return mapped
    status_raw = ride.get("status")
    return status_raw if status_raw not in {None, ""} else None


def _get_ride_info(
    ride_id: str, rides_map: Mapping[str, Any]
) -> Optional[Dict[str, Any]]:
    """Get full ride info from the rides map."""
    ride_id_normalized = _normalize_ride_id(ride_id)
    if not ride_id_normalized:
        return None
    info = rides_map.get(ride_id_normalized)
    return info if isinstance(info, dict) else None


async def fetch_ondemand_rides(
    client: OnDemandClient, schedules: Optional[Sequence[Dict[str, Any]]] = None
) -> Dict[str, Dict[str, Any]]:
    """Fetch ride details including status and pickup/dropoff addresses.

    Returns a dict mapping ride_id to ride info including:
    - status: the ride status (pending, in_progress, etc.)
    - pickup_address: pickup location address (if available)
    - dropoff_address: dropoff location address (if available)
    - rider_name: rider name (if available)
    - vehicle_id: assigned vehicle ID (if available)
    """
    if not ONDEMAND_RIDES_URL:
        return {}

    async def fetch() -> Dict[str, Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        schedule_start, schedule_end = _extract_schedule_time_bounds(schedules)
        padding = timedelta(minutes=max(0, ONDEMAND_RIDES_WINDOW_PADDING_MIN))
        start_dt = schedule_start - padding if schedule_start else now - timedelta(
            minutes=max(1, ONDEMAND_RIDES_LOOKBACK_MIN)
        )
        end_dt = schedule_end + padding if schedule_end else now + timedelta(
            minutes=max(1, ONDEMAND_RIDES_LOOKAHEAD_MIN)
        )
        params = {
            "start_time": _format_ondemand_timestamp(start_dt),
            "end_time": _format_ondemand_timestamp(end_dt),
            "include_histories": "true",
        }

        parsed = urlparse(ONDEMAND_RIDES_URL)
        merged_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        merged_params.update(params)
        url = urlunparse(parsed._replace(query=urlencode(merged_params)))

        try:
            response = await client.get_resource(url)
        except Exception as exc:
            print(f"[ondemand] rides fetch failed: {exc}")
            return {}

        if response.status_code >= 400:
            print(f"[ondemand] rides fetch error: {response.status_code}")
            return {}

        try:
            payload = response.json()
        except ValueError as exc:
            print(f"[ondemand] rides decode failed: {exc}")
            return {}

        data: Sequence[Any] = []
        if isinstance(payload, list):
            data = payload
        elif isinstance(payload, dict):
            candidate = payload.get("data")
            if isinstance(candidate, list):
                data = candidate

        rides_map: Dict[str, Dict[str, Any]] = {}
        for ride in data:
            if not isinstance(ride, dict):
                continue
            ride_id_val = ride.get("ride_id") or ride.get("rideId") or ride.get("id")
            ride_id_normalized = _normalize_ride_id(ride_id_val)
            if not ride_id_normalized:
                continue

            ride_info: Dict[str, Any] = {}

            # Extract status
            status_val = ride.get("status")
            if status_val not in {None, ""}:
                ride_info["status"] = status_val

            # Extract pickup address
            pickup = ride.get("pickup") or ride.get("origin") or ride.get("pickup_location")
            if isinstance(pickup, dict):
                pickup_addr = pickup.get("address") or pickup.get("label") or pickup.get("name")
                if pickup_addr:
                    ride_info["pickup_address"] = str(pickup_addr).strip()
            elif isinstance(pickup, str) and pickup.strip():
                ride_info["pickup_address"] = pickup.strip()
            # Also check top-level fields
            if "pickup_address" not in ride_info:
                for key in ("pickup_address", "pickupAddress", "origin_address", "originAddress"):
                    val = ride.get(key)
                    if isinstance(val, str) and val.strip():
                        ride_info["pickup_address"] = val.strip()
                        break

            # Extract dropoff address
            dropoff = ride.get("dropoff") or ride.get("destination") or ride.get("dropoff_location")
            if isinstance(dropoff, dict):
                dropoff_addr = dropoff.get("address") or dropoff.get("label") or dropoff.get("name")
                if dropoff_addr:
                    ride_info["dropoff_address"] = str(dropoff_addr).strip()
            elif isinstance(dropoff, str) and dropoff.strip():
                ride_info["dropoff_address"] = dropoff.strip()
            # Also check top-level fields
            if "dropoff_address" not in ride_info:
                for key in ("dropoff_address", "dropoffAddress", "destination_address", "destinationAddress"):
                    val = ride.get(key)
                    if isinstance(val, str) and val.strip():
                        ride_info["dropoff_address"] = val.strip()
                        break

            # Extract rider name
            rider_name = _format_rider_name(ride.get("rider") or ride.get("passenger"))
            if rider_name:
                ride_info["rider_name"] = rider_name

            # Extract vehicle ID
            vehicle_id = ride.get("vehicle_id") or ride.get("vehicleId")
            if vehicle_id not in {None, ""}:
                ride_info["vehicle_id"] = str(vehicle_id).strip()

            if ride_info:
                rides_map[ride_id_normalized] = ride_info

        return rides_map

    return await ondemand_rides_cache.get(fetch)


def build_ondemand_virtual_stops(
    schedules: Sequence[Dict[str, Any]],
    now: datetime,
    pending_ride_ids: Optional[Set[str]] = None,
    rides_map: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    if not schedules:
        return []

    cutoff = now - timedelta(seconds=max(0, ONDEMAND_VIRTUAL_STOP_MAX_AGE_S))
    records: List[Dict[str, Any]] = []
    rides_lookup: Dict[str, Dict[str, Any]] = {}
    if rides_map:
        rides_lookup = {
            _normalize_ride_id(key): value
            for key, value in rides_map.items()
            if key not in {None, ""} and isinstance(value, dict)
        }

    for vehicle in schedules:
        if not isinstance(vehicle, dict):
            continue
        vehicle_id_raw = vehicle.get("vehicle_id") or vehicle.get("vehicleId")
        vehicle_id = str(vehicle_id_raw).strip() if vehicle_id_raw is not None else ""
        if not vehicle_id:
            continue
        call_name_raw = vehicle.get("call_name") or vehicle.get("callName")
        call_name = (
            str(call_name_raw).strip() if call_name_raw not in {None, ""} else None
        )

        stops = vehicle.get("stops")
        if not isinstance(stops, list):
            continue
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            position = stop.get("position") or {}
            lat = _coerce_float(
                position.get("latitude")
                or position.get("lat")
                or stop.get("latitude")
                or stop.get("lat")
            )
            lng = _coerce_float(
                position.get("longitude")
                or position.get("lon")
                or position.get("lng")
                or stop.get("longitude")
                or stop.get("lon")
            )
            if lat is None or lng is None:
                continue
            stop_timestamp = _parse_timestamp(stop.get("timestamp") or stop.get("time"))
            if stop_timestamp is None:
                continue
            if stop_timestamp.tzinfo is None:
                stop_timestamp = stop_timestamp.replace(tzinfo=timezone.utc)
            else:
                stop_timestamp = stop_timestamp.astimezone(timezone.utc)
            if stop_timestamp < cutoff:
                continue
            rides = stop.get("rides")
            if not isinstance(rides, list):
                continue
            address_value = stop.get("address") or stop.get("label") or ""
            address = str(address_value).strip()
            for ride in rides:
                if not isinstance(ride, dict):
                    continue
                status_raw = _resolve_ride_status(ride, rides_lookup)
                stop_type_raw = ride.get("stop_type") or ride.get("stopType")
                stop_type = str(stop_type_raw or "").strip().lower()
                if stop_type not in {"pickup", "dropoff"}:
                    continue
                riders: List[str] = []
                rider_name = _format_rider_name(ride.get("rider") or ride.get("passenger"))
                if rider_name:
                    riders.append(rider_name)
                extra_riders = ride.get("riders") or ride.get("passengers")
                if isinstance(extra_riders, list):
                    for extra in extra_riders:
                        formatted = _format_rider_name(extra)
                        if formatted:
                            riders.append(formatted)
                capacity_value = ride.get("capacity")
                capacity = 1
                try:
                    if capacity_value is not None:
                        capacity_candidate = int(float(capacity_value))
                        if capacity_candidate > 0:
                            capacity = capacity_candidate
                except (TypeError, ValueError):
                    capacity = 1
                service_id_raw = ride.get("service_id") or ride.get("serviceId")
                service_id = (
                    str(service_id_raw).strip() if service_id_raw not in {None, ""} else None
                )
                record = {
                    "lat": lat,
                    "lng": lng,
                    "address": address,
                    "stopType": stop_type,
                    "capacity": capacity,
                    "serviceId": service_id,
                    "vehicleId": vehicle_id,
                    "stopTimestamp": stop_timestamp.isoformat(),
                    "riders": riders,
                }
                ride_id_val = ride.get("ride_id") or ride.get("rideId") or ride.get("id")
                ride_id = str(ride_id_val).strip() if ride_id_val not in {None, ""} else ""
                if ride_id:
                    record["rideId"] = ride_id
                if status_raw is not None:
                    record["rideStatus"] = status_raw
                if call_name:
                    record["callName"] = call_name
                records.append(record)
    return records


def _format_rider_name(rider: Any) -> Optional[str]:
    if isinstance(rider, dict):
        first = rider.get("first_name") or rider.get("firstName") or rider.get("first")
        last = rider.get("last_name") or rider.get("lastName") or rider.get("last")
        name_parts = [str(part).strip() for part in (first, last) if part not in {None, ""}]
        joined = " ".join([part for part in name_parts if part])
        if joined:
            return joined
        for key in ("name", "username", "email"):
            candidate = rider.get(key)
            if isinstance(candidate, str):
                candidate_stripped = candidate.strip()
                if candidate_stripped:
                    return candidate_stripped
    elif isinstance(rider, str):
        rider_stripped = rider.strip()
        if rider_stripped:
            return rider_stripped
    return None


def build_ondemand_next_stop_targets(
    schedules: Sequence[Dict[str, Any]],
    now: datetime,
    pending_ride_ids: Optional[Set[str]] = None,
    rides_map: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    targets: Dict[str, Dict[str, Any]] = {}
    if not schedules:
        return targets

    rides_lookup: Dict[str, Dict[str, Any]] = {}
    if rides_map:
        rides_lookup = {
            _normalize_ride_id(key): value
            for key, value in rides_map.items()
            if key not in {None, ""} and isinstance(value, dict)
        }

    for vehicle in schedules:
        if not isinstance(vehicle, dict):
            continue
        vehicle_id_raw = vehicle.get("vehicle_id") or vehicle.get("vehicleId")
        vehicle_id = str(vehicle_id_raw).strip() if vehicle_id_raw is not None else ""
        if not vehicle_id:
            continue
        stops = vehicle.get("stops")
        if not isinstance(stops, list):
            continue

        best_future: tuple[datetime, Dict[str, Any]] | None = None
        best_past: tuple[datetime, Dict[str, Any]] | None = None

        for stop in stops:
            if not isinstance(stop, dict):
                continue
            position = stop.get("position") or {}
            lat = _coerce_float(
                position.get("latitude")
                or position.get("lat")
                or stop.get("latitude")
                or stop.get("lat")
            )
            lng = _coerce_float(
                position.get("longitude")
                or position.get("lon")
                or position.get("lng")
                or stop.get("longitude")
                or stop.get("lon")
            )
            if lat is None or lng is None:
                continue

            stop_timestamp = _parse_timestamp(stop.get("timestamp") or stop.get("time"))
            if stop_timestamp is None:
                continue
            if stop_timestamp.tzinfo is None:
                stop_timestamp = stop_timestamp.replace(tzinfo=timezone.utc)
            else:
                stop_timestamp = stop_timestamp.astimezone(timezone.utc)

            rides = stop.get("rides")
            if not isinstance(rides, list):
                continue

            address_value = stop.get("address") or stop.get("label") or ""
            address = str(address_value).strip() or "Stop"

            riders: List[str] = []
            chosen_stop_type = ""
            ride_status_for_target: Optional[str] = None

            for ride in rides:
                if not isinstance(ride, dict):
                    continue
                ride_status_raw = _resolve_ride_status(ride, rides_lookup)
                stop_type_raw = ride.get("stop_type") or ride.get("stopType")
                stop_type = str(stop_type_raw or "").strip().lower()
                if stop_type not in {"pickup", "dropoff"}:
                    continue

                chosen_stop_type = chosen_stop_type or stop_type
                rider_name = _format_rider_name(ride.get("rider") or ride.get("passenger"))
                if rider_name:
                    riders.append(rider_name)
                extra_riders = ride.get("riders") or ride.get("passengers")
                if isinstance(extra_riders, list):
                    for extra in extra_riders:
                        formatted = _format_rider_name(extra)
                        if formatted:
                            riders.append(formatted)
                if ride_status_raw not in {None, ""}:
                    ride_status_for_target = ride_status_for_target or ride_status_raw

            if not chosen_stop_type:
                continue

            target: Dict[str, Any] = {
                "lat": lat,
                "lng": lng,
                "address": address,
                "stopType": chosen_stop_type,
                "stopTimestamp": stop_timestamp.isoformat(),
            }
            if ride_status_for_target not in {None, ""}:
                target["rideStatus"] = ride_status_for_target
            if riders:
                target["riders"] = riders

            if stop_timestamp >= now:
                if best_future is None or stop_timestamp < best_future[0]:
                    best_future = (stop_timestamp, target)
            elif best_past is None or stop_timestamp > best_past[0]:
                best_past = (stop_timestamp, target)

        selected = best_future or best_past
        if selected:
            targets[vehicle_id] = selected[1]

    return targets


def build_ondemand_vehicle_stop_plans(
    schedules: Sequence[Dict[str, Any]],
    pending_ride_ids: Optional[Set[str]] = None,
    rides_map: Optional[Mapping[str, Any]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Build stop plans for each vehicle from schedule data.

    Also supplements with pickup addresses from rides_map for in_progress rides
    that only have dropoff stops in the schedule (pickup already completed).
    """
    plans: Dict[str, List[Dict[str, Any]]] = {}
    rides_lookup: Dict[str, Dict[str, Any]] = {}
    if rides_map:
        rides_lookup = {
            _normalize_ride_id(key): value
            for key, value in rides_map.items()
            if key not in {None, ""} and isinstance(value, dict)
        }
    for vehicle in schedules:
        if not isinstance(vehicle, dict):
            continue
        vehicle_id_raw = vehicle.get("vehicle_id") or vehicle.get("vehicleId")
        vehicle_id = str(vehicle_id_raw).strip() if vehicle_id_raw is not None else ""
        if not vehicle_id:
            continue
        stops = vehicle.get("stops")
        if not isinstance(stops, list):
            continue
        entries: List[Dict[str, Any]] = []
        order = 1
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            rides = stop.get("rides")
            if not isinstance(rides, list):
                continue
            address_value = stop.get("address") or stop.get("label") or ""
            address = str(address_value).strip() or "Unknown stop"
            grouped: Dict[str, List[str]] = {}
            rides_by_type: Dict[str, List[Dict[str, Any]]] = {}
            for ride in rides:
                if not isinstance(ride, dict):
                    continue
                status_raw = _resolve_ride_status(ride, rides_lookup)
                stop_type_raw = ride.get("stop_type") or ride.get("stopType")
                stop_type = str(stop_type_raw or "").strip().lower()
                if stop_type not in {"pickup", "dropoff"}:
                    continue
                rider_name = _format_rider_name(ride.get("rider") or ride.get("passenger"))
                ride_riders: List[str] = []
                if rider_name:
                    ride_riders.append(rider_name)
                    grouped.setdefault(stop_type, []).append(rider_name)
                extra_riders = ride.get("riders") or ride.get("passengers")
                if isinstance(extra_riders, list):
                    for extra in extra_riders:
                        formatted = _format_rider_name(extra)
                        if formatted:
                            ride_riders.append(formatted)
                            grouped.setdefault(stop_type, []).append(formatted)

                ride_record: Dict[str, Any] = {"stopType": stop_type}
                ride_id_val = ride.get("ride_id") or ride.get("rideId") or ride.get("id")
                ride_id = str(ride_id_val).strip() if ride_id_val not in {None, ""} else ""
                if ride_id:
                    ride_record["rideId"] = ride_id
                    # For in_progress rides with dropoff, include pickup address from rides_map
                    if stop_type == "dropoff":
                        ride_info = rides_lookup.get(_normalize_ride_id(ride_id))
                        if isinstance(ride_info, dict):
                            pickup_addr = ride_info.get("pickup_address")
                            if pickup_addr:
                                ride_record["pickupAddress"] = pickup_addr
                if status_raw is not None:
                    ride_record["rideStatus"] = status_raw
                if ride_riders:
                    ride_record["riders"] = ride_riders

                rides_by_type.setdefault(stop_type, []).append(ride_record)
            # Sort so dropoffs are ordered before pickups at the same location
            sorted_stop_types = sorted(grouped.keys(), key=lambda t: (0 if t == "dropoff" else 1))
            for stop_type in sorted_stop_types:
                riders = grouped[stop_type]
                if not riders:
                    continue
                entry = {
                    "order": order,
                    "address": address,
                    "stopType": stop_type,
                    "riders": riders,
                }
                ride_details = rides_by_type.get(stop_type)
                if ride_details:
                    entry["rides"] = ride_details
                    if len(ride_details) == 1:
                        ride_detail = ride_details[0]
                        ride_id = ride_detail.get("rideId")
                        ride_status = ride_detail.get("rideStatus")
                        if ride_id:
                            entry["rideId"] = ride_id
                        if ride_status not in {None, ""}:
                            entry["rideStatus"] = ride_status
                if entries:
                    last = entries[-1]
                    if (
                        last.get("address", "").lower() == address.lower()
                        and last.get("stopType") == stop_type
                    ):
                        existing_names = set(last.get("riders", []))
                        for name in riders:
                            if name not in existing_names:
                                last.setdefault("riders", []).append(name)
                                existing_names.add(name)
                        if ride_details:
                            existing_rides = last.setdefault("rides", [])
                            existing_ids = {
                                ride.get("rideId")
                                for ride in existing_rides
                                if isinstance(ride, dict)
                            }
                            for ride in ride_details:
                                if not isinstance(ride, dict):
                                    continue
                                ride_id_value = ride.get("rideId")
                                if ride_id_value and ride_id_value in existing_ids:
                                    continue
                                existing_rides.append(ride)
                        continue
                entries.append(entry)
                order += 1
        if entries:
            plans[vehicle_id] = entries
    return plans


async def _collect_ondemand_data(
    client: OnDemandClient, *, now: Optional[datetime] = None
) -> Dict[str, Any]:
    current_dt = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    client_module = getattr(client.__class__, "__module__", "") if client else ""
    bypass_window = client_module.startswith("tests.") or not isinstance(
        client, OnDemandClient
    )
    if not bypass_window and not _is_within_ondemand_operating_window(current_dt):
        return {"vehicles": [], "ondemandStops": []}

    async def fetch() -> Any:
        def extract_driver_name(entry: Dict[str, Any]) -> str:
            driver_info = entry.get("driver") if isinstance(entry, dict) else None
            if isinstance(driver_info, dict):
                first_name = driver_info.get("first_name") or driver_info.get("firstName")
                last_name = driver_info.get("last_name") or driver_info.get("lastName")
                name_parts = [
                    str(part).strip() for part in (first_name, last_name) if part not in {None, ""}
                ]
                joined_name = " ".join([part for part in name_parts if part])
                if joined_name:
                    return joined_name
            for key in ("driverName", "driver_name", "driver"):
                value = entry.get(key) if isinstance(entry, dict) else None
                if isinstance(value, str):
                    value_stripped = value.strip()
                    if value_stripped:
                        return value_stripped
            return ""

        # Fetch roster and positions in parallel
        roster: List[Dict[str, Any]] = []
        positions_data: Any = []
        try:
            roster_result, positions_result = await asyncio.gather(
                client.get_vehicle_details(),
                client.get_vehicle_positions(),
                return_exceptions=True,
            )
            if isinstance(roster_result, Exception):
                print(f"[ondemand] vehicle roster fetch failed: {roster_result}")
                roster = []
            else:
                roster = roster_result
            if isinstance(positions_result, Exception):
                print(f"[ondemand] vehicle positions fetch failed: {positions_result}")
                positions_data = []
            else:
                positions_data = positions_result
        except Exception as exc:
            print(f"[ondemand] parallel fetch failed: {exc}")

        color_map: Dict[str, str] = {}
        driver_name_map: Dict[str, str] = {}
        last_active_map: Dict[str, str] = {}
        for entry in roster:
            if not isinstance(entry, dict):
                continue
            vehicle_id = entry.get("vehicle_id")
            if vehicle_id is None:
                continue
            vehicle_key = str(vehicle_id).strip()
            if not vehicle_key:
                continue
            # Normalise the colour so the frontend can prepend a '#'.
            color = entry.get("color")
            color_value = str(color).strip() if color is not None else ""
            if color_value:
                normalized_color = color_value.lstrip("#").lower()
                if normalized_color:
                    color_map[vehicle_key] = normalized_color
            driver_name = (
                entry.get("driver_name")
                or entry.get("driverName")
                or entry.get("driver")
            )
            driver_name_value = (
                str(driver_name).strip()
                if driver_name not in {None, ""}
                else ""
            )
            if driver_name_value:
                driver_name_map[vehicle_key] = driver_name_value

            last_active_value = entry.get("last_active_at") or entry.get("lastActiveAt")
            if isinstance(last_active_value, str):
                last_active_value = last_active_value.strip()
            if last_active_value:
                last_active_map[vehicle_key] = str(last_active_value)

        if not isinstance(positions_data, list):
            return positions_data

        for entry in positions_data:
            if not isinstance(entry, dict):
                continue
            vehicle_id = entry.get("vehicle_id") or entry.get("VehicleID")
            if vehicle_id is None:
                continue
            vehicle_key = str(vehicle_id).strip()
            if not vehicle_key:
                continue
            color = color_map.get(vehicle_key)
            if color:
                entry["color"] = color
                entry["color_hex"] = f"#{color}"
            driver_name = driver_name_map.get(vehicle_key) or extract_driver_name(entry)
            if driver_name:
                entry["driverName"] = driver_name
            last_active = last_active_map.get(vehicle_key)
            if last_active:
                entry["last_active_at"] = last_active
                entry.setdefault("lastActiveAt", last_active)
        return positions_data

    raw_positions = await ondemand_positions_cache.get(fetch)

    # Fetch schedules and rides in parallel for lower latency
    schedules: List[Dict[str, Any]] = []
    pending_ride_ids: Set[str] = set()
    rides_map: Dict[str, Dict[str, Any]] = {}
    try:
        schedules_result, rides_result = await asyncio.gather(
            fetch_ondemand_schedules(client),
            fetch_ondemand_rides(client, None),  # Uses default time window
            return_exceptions=True,
        )
        if isinstance(schedules_result, Exception):
            print(f"[ondemand] schedules processing failed: {schedules_result}")
            schedules = []
        else:
            schedules = schedules_result
        if isinstance(rides_result, Exception):
            print(f"[ondemand] rides processing failed: {rides_result}")
            rides_map = {}
        else:
            rides_map = rides_result
    except Exception as exc:
        print(f"[ondemand] parallel schedules/rides fetch failed: {exc}")

    pending_ride_ids = {
        ride_id
        for ride_id, ride_info in rides_map.items()
        if isinstance(ride_info, dict)
        and str(ride_info.get("status", "")).strip().lower().startswith("pending")
    }

    # Enrich the schedule rides with statuses from the rides endpoint so every
    # ride in the final payload carries a status.
    for vehicle in schedules or []:
        if not isinstance(vehicle, dict):
            continue
        stops = vehicle.get("stops")
        if not isinstance(stops, list):
            continue
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            rides = stop.get("rides")
            if not isinstance(rides, list):
                continue
            for ride in rides:
                if not isinstance(ride, dict):
                    continue
                ride_id_val = ride.get("ride_id") or ride.get("rideId") or ride.get("id")
                ride_id_key = (
                    str(ride_id_val).strip().lower() if ride_id_val not in {None, ""} else ""
                )
                if not ride_id_key:
                    continue
                ride_info = rides_map.get(ride_id_key)
                if isinstance(ride_info, dict):
                    status_value = ride_info.get("status")
                    if status_value not in {None, ""}:
                        ride["status"] = status_value

    stop_plans = build_ondemand_vehicle_stop_plans(
        schedules, pending_ride_ids, rides_map
    )
    next_stop_targets = build_ondemand_next_stop_targets(
        schedules, current_dt, pending_ride_ids, rides_map
    )

    vehicles: List[Dict[str, Any]] = []
    positions_list = raw_positions if isinstance(raw_positions, list) else []
    for entry in positions_list:
        if not isinstance(entry, dict):
            continue
        vehicle_id_raw = (
            entry.get("vehicle_id")
            or entry.get("VehicleID")
            or entry.get("device_uuid")
            or entry.get("device_id")
        )
        vehicle_id = str(vehicle_id_raw).strip() if vehicle_id_raw is not None else ""
        if not vehicle_id:
            continue
        position = entry.get("position") or {}
        lat = _coerce_float(position.get("latitude") or position.get("lat") or entry.get("lat"))
        lng = _coerce_float(
            position.get("longitude")
            or position.get("lon")
            or position.get("lng")
            or entry.get("lon")
        )
        if lat is None or lng is None:
            continue
        heading = _coerce_float(entry.get("heading") or entry.get("Heading"))
        last_update_dt = _parse_timestamp(
            entry.get("last_update")
            or entry.get("lastUpdate")
            or entry.get("last_seen")
            or entry.get("timestamp")
        )
        if last_update_dt is not None:
            if last_update_dt.tzinfo is None:
                last_update_dt = last_update_dt.replace(tzinfo=timezone.utc)
            else:
                last_update_dt = last_update_dt.astimezone(timezone.utc)
        last_update = last_update_dt.isoformat() if last_update_dt is not None else None
        service_id_raw = entry.get("service_id") or entry.get("serviceId")
        service_id = (
            str(service_id_raw).strip() if service_id_raw not in {None, ""} else None
        )
        color = (
            _normalize_hex_color(entry.get("markerColor"))
            or _normalize_hex_color(entry.get("color_hex"))
            or _normalize_hex_color(entry.get("color"))
            or ONDEMAND_DEFAULT_MARKER_COLOR
        )
        vehicle_payload: Dict[str, Any] = {
            "vehicleId": vehicle_id,
            "lat": lat,
            "lng": lng,
            "heading": heading,
            "lastUpdate": last_update,
            "serviceId": service_id,
            "markerColor": color,
            "status": entry.get("status") or entry.get("Status"),
        }
        last_active_at = entry.get("last_active_at") or entry.get("lastActiveAt")
        if last_active_at:
            vehicle_payload["last_active_at"] = last_active_at
            vehicle_payload["lastActiveAt"] = last_active_at
        if "speed" in entry:
            vehicle_payload["speed"] = entry.get("speed")
        if "stale" in entry:
            vehicle_payload["stale"] = bool(entry.get("stale"))
        if "enabled" in entry:
            vehicle_payload["enabled"] = entry.get("enabled")
        if "eligible" in entry:
            vehicle_payload["eligible"] = entry.get("eligible")
        call_name = entry.get("call_name") or entry.get("callName")
        if call_name:
            vehicle_payload["callName"] = call_name
        driver_name = entry.get("driverName")
        if driver_name:
            driver_name_value = str(driver_name).strip()
            if driver_name_value:
                vehicle_payload["driverName"] = driver_name_value
        device_uuid = entry.get("device_uuid") or entry.get("deviceUuid")
        if device_uuid:
            vehicle_payload["deviceUuid"] = device_uuid
        device_id = entry.get("device_id") or entry.get("deviceId")
        if device_id:
            vehicle_payload["deviceId"] = device_id
        stop_plan = stop_plans.get(vehicle_id)
        if stop_plan:
            vehicle_payload["stops"] = stop_plan
        next_stop = next_stop_targets.get(vehicle_id)
        if next_stop:
            vehicle_payload["nextStop"] = next_stop
        vehicles.append(vehicle_payload)

    ondemand_stops = build_ondemand_virtual_stops(
        schedules, current_dt, pending_ride_ids, rides_map
    )

    return {"vehicles": vehicles, "ondemandStops": ondemand_stops}


def _extract_ors_coordinates(payload: Any) -> List[List[float]]:
    coordinates: List[Any] = []
    if not isinstance(payload, dict):
        return []

    routes = payload.get("routes")
    if isinstance(routes, list) and routes:
        first_route = routes[0] if isinstance(routes[0], dict) else None
        geometry = first_route.get("geometry") if isinstance(first_route, dict) else None
        if isinstance(geometry, dict):
            coordinates = geometry.get("coordinates") or []
        elif isinstance(geometry, list):
            coordinates = geometry

    if not coordinates:
        features = payload.get("features")
        if isinstance(features, list) and features:
            first_feature = features[0] if isinstance(features[0], dict) else None
            geometry = first_feature.get("geometry") if isinstance(first_feature, dict) else None
            if isinstance(geometry, dict):
                coordinates = geometry.get("coordinates") or []

    if not isinstance(coordinates, list):
        return []

    flattened: List[Any] = []
    if coordinates and isinstance(coordinates[0], (list, tuple)) and coordinates[0]:
        if isinstance(coordinates[0][0], (list, tuple)):
            for segment in coordinates:
                if not isinstance(segment, (list, tuple)):
                    continue
                for point in segment:
                    flattened.append(point)
        else:
            flattened = coordinates

    latlngs: List[List[float]] = []
    for point in flattened:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        lng = _coerce_float(point[0])
        lat = _coerce_float(point[1])
        if lat is None or lng is None:
            continue
        latlngs.append([lat, lng])

    return latlngs


async def _fetch_openrouteservice_route(
    start_lat: float, start_lng: float, end_lat: float, end_lng: float
) -> List[List[float]]:
    if not ORS_KEY:
        raise RuntimeError("ORS_KEY is not configured")

    params = {
        "start": f"{start_lng},{start_lat}",
        "end": f"{end_lng},{end_lat}",
        "geometry_format": "geojson",
    }
    headers = {"Authorization": ORS_KEY}

    async with httpx.AsyncClient(timeout=ORS_HTTP_TIMEOUT_S) as client:
        response = await client.get(ORS_DIRECTIONS_URL, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()
    return _extract_ors_coordinates(data)


async def _build_ondemand_payload(request: Request) -> Dict[str, Any]:
    _require_dispatcher_access(request)
    dispatcher_info = _get_dispatcher_secret_info(request)
    if dispatcher_info and dispatcher_info[1] == "cat":
        raise HTTPException(status_code=403, detail="ondemand data unavailable for CAT access")
    client: Optional[OnDemandClient] = getattr(app.state, "ondemand_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="ondemand client not configured")
    return await _collect_ondemand_data(client)


@app.get("/api/ondemand")
async def api_ondemand_positions(request: Request):
    return await _build_ondemand_payload(request)


@app.get("/api/ondemand/vehicles/positions")
async def api_ondemand_positions_legacy(request: Request):
    data = await _build_ondemand_payload(request)
    vehicles = data.get("vehicles") if isinstance(data, dict) else []
    return {"vehicles": vehicles}


@app.post("/api/ondemand/routes")
async def api_ondemand_routes(
    request: Request, payload: Optional[Dict[str, Any]] = Body(None)
):
    _require_dispatcher_access(request)
    if not ORS_KEY:
        raise HTTPException(status_code=503, detail="openrouteservice not configured")

    client: Optional[OnDemandClient] = getattr(app.state, "ondemand_client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="ondemand client not configured")

    vehicle_filter: Set[str] = set()
    if isinstance(payload, dict):
        requested_ids = payload.get("vehicleIds")
        if isinstance(requested_ids, list):
            for value in requested_ids:
                value_text = str(value).strip()
                if value_text:
                    vehicle_filter.add(value_text)

    data = await _collect_ondemand_data(client)
    vehicles = data.get("vehicles") if isinstance(data, dict) else []

    routes: List[Dict[str, Any]] = []
    for entry in vehicles:
        if not isinstance(entry, dict):
            continue
        vehicle_id_raw = (
            entry.get("vehicleId")
            or entry.get("vehicle_id")
            or entry.get("VehicleID")
            or entry.get("deviceUuid")
            or entry.get("device_id")
            or entry.get("deviceId")
        )
        vehicle_id = str(vehicle_id_raw).strip() if vehicle_id_raw not in {None, ""} else ""
        if not vehicle_id:
            continue
        if vehicle_filter and vehicle_id not in vehicle_filter:
            continue

        start_lat = _coerce_float(entry.get("lat"))
        start_lng = _coerce_float(entry.get("lng"))
        if start_lat is None or start_lng is None:
            continue

        next_stop = entry.get("nextStop") if isinstance(entry.get("nextStop"), dict) else None
        if not next_stop:
            continue
        end_lat = _coerce_float(next_stop.get("lat"))
        end_lng = _coerce_float(next_stop.get("lng"))
        if end_lat is None or end_lng is None:
            continue

        try:
            coordinates = await _fetch_openrouteservice_route(
                start_lat, start_lng, end_lat, end_lng
            )
        except httpx.HTTPError as exc:
            print(f"[ondemand] ORS request failed for {vehicle_id}: {exc}")
            continue
        except Exception as exc:
            print(f"[ondemand] unexpected ORS error for {vehicle_id}: {exc}")
            continue

        if not coordinates:
            continue

        color = (
            _normalize_hex_color(entry.get("markerColor"))
            or _normalize_hex_color(entry.get("color_hex"))
            or _normalize_hex_color(entry.get("color"))
            or ONDEMAND_DEFAULT_MARKER_COLOR
        )

        routes.append({
            "vehicleId": vehicle_id,
            "coordinates": coordinates,
            "color": color,
        })

    return {"routes": routes}


# ---------------------------
# REST: Headway tracking
# ---------------------------


def _get_headway_storage() -> HeadwayStorage:
    storage = getattr(app.state, "headway_storage", None)
    if storage is None:
        raise HTTPException(status_code=503, detail="headway storage unavailable")
    return storage


def _attach_vehicle_names(events: Sequence[HeadwayEvent]) -> Dict[str, str]:
    mapping = dict(getattr(app.state, "headway_vehicle_names", {}) or {})
    for ev in events:
        vehicle_id = ev.vehicle_id
        if vehicle_id is None:
            continue
        key = str(vehicle_id)
        if ev.vehicle_name:
            mapping[key] = ev.vehicle_name
        else:
            name = mapping.get(key)
            if name:
                ev.vehicle_name = name
    return mapping


def _iso_or_none(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


@app.get("/api/dispatch/headway/diagnostics")
async def headway_diagnostics(
    request: Request,
    recent_hours: float = Query(
        2.0,
        description="How many recent hours of events to include (capped at 12)",
    ),
):
    _require_dispatcher_access(request)
    tracker = getattr(app.state, "headway_tracker", None)
    if tracker is None:
        raise HTTPException(status_code=503, detail="headway tracker unavailable")
    storage = _get_headway_storage()

    try:
        recent_window_hours = max(0.1, min(float(recent_hours), 12.0))
    except (TypeError, ValueError):
        recent_window_hours = 2.0

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=recent_window_hours)
    recent_events = storage.query_events(start, end)
    if len(recent_events) > 40:
        recent_events = recent_events[-40:]

    attached_vehicle_names = _attach_vehicle_names(recent_events)
    if attached_vehicle_names:
        app.state.headway_vehicle_names = attached_vehicle_names
    vehicle_name_lookup = {str(k): v for k, v in attached_vehicle_names.items() if v}
    unmatched_vehicle_events = [
        {
            "vehicle_id": ev.vehicle_id,
            "route_id": ev.route_id,
            "stop_id": ev.stop_id,
            "event_type": ev.event_type,
            "timestamp": _iso_or_none(ev.timestamp),
        }
        for ev in recent_events
        if ev.vehicle_id and not ev.vehicle_name
    ]

    stops_payload = [
        {
            "stop_id": stop.stop_id,
            "lat": stop.lat,
            "lon": stop.lon,
            "route_ids": sorted(stop.route_ids),
            "approach_sets": [
                {"name": s.name, "bubbles": [{"lat": b.lat, "lon": b.lon, "radius_m": b.radius_m, "order": b.order} for b in s.bubbles]}
                for s in (stop.approach_sets or [])
            ],
        }
        for stop in tracker.stops
    ]

    vehicle_states = [
        {
            "vehicle_id": vid,
            "route_id": state.route_id,
            "current_stop_id": state.current_stop_id,
            "arrival_time": _iso_or_none(state.arrival_time),
            "departure_started_at": _iso_or_none(state.departure_started_at),
        }
        for vid, state in tracker.vehicle_states.items()
    ]

    def _dict_from_route_stop_time_map(data: Mapping[Tuple[Optional[str], str], datetime]) -> List[dict]:
        return [
            {"route_id": route_id, "stop_id": stop_id, "timestamp": _iso_or_none(ts)}
            for (route_id, stop_id), ts in data.items()
        ]

    def _dict_from_vehicle_stop_time_map(data: Mapping[Tuple[str, str], datetime]) -> List[dict]:
        mapped: List[dict] = []
        for key, ts in data.items():
            if not isinstance(key, tuple):
                continue
            if len(key) == 2:
                vehicle_id, stop_id = key
                route_id = None
            elif len(key) == 3:
                vehicle_id, stop_id, route_id = key
            else:
                continue
            mapped.append(
                {
                    "vehicle_id": vehicle_id,
                    "stop_id": stop_id,
                    "route_id": route_id,
                    "timestamp": _iso_or_none(ts),
                }
            )
        return mapped

    response = {
        "fetched_at": _iso_or_none(end),
        "thresholds_m": {
            "arrival_distance_threshold_m": tracker.arrival_distance_threshold_m,
            "departure_distance_threshold_m": tracker.departure_distance_threshold_m,
        },
        "tracked_route_ids": sorted(tracker.tracked_route_ids) if tracker.tracked_route_ids else [],
        "tracked_stop_ids": sorted(tracker.tracked_stop_ids) if tracker.tracked_stop_ids else [],
        "stop_count": len(tracker.stops),
        "stops": stops_payload,
        "vehicle_states": vehicle_states,
        "last_arrivals": _dict_from_route_stop_time_map(tracker.last_arrival),
        "last_departures": _dict_from_route_stop_time_map(tracker.last_departure),
        "last_vehicle_arrivals": _dict_from_vehicle_stop_time_map(tracker.last_vehicle_arrival),
        "last_vehicle_departures": _dict_from_vehicle_stop_time_map(tracker.last_vehicle_departure),
        "recent_stop_association_failures": list(tracker.recent_stop_association_failures),
        "recent_snapshot_diagnostics": list(tracker.recent_snapshot_diagnostics),
        "recent_bubble_activations": list(tracker.recent_bubble_activations),
        "active_bubble_states": tracker.get_active_bubble_states(),
        "vehicle_name_lookup": vehicle_name_lookup,
        "attached_vehicle_names": attached_vehicle_names,
        "unmatched_vehicle_events": unmatched_vehicle_events,
        "recent_events": [ev.to_dict() for ev in recent_events],
    }

    return response


@app.post("/v1/headway/clear")
async def clear_headway_logs(request: Request):
    _require_dispatcher_access(request)
    storage = _get_headway_storage()
    try:
        deleted_files = storage.clear()
    except Exception as exc:
        print(f"[headway] failed to clear logs: {exc}")
        raise HTTPException(status_code=500, detail="failed to clear headway logs") from exc
    return {"deleted_files": deleted_files}


@app.get("/api/headway")
async def api_headway(
    start: str = Query(..., description="Start timestamp (ISO-8601 UTC)"),
    end: str = Query(..., description="End timestamp (ISO-8601 UTC)"),
    route_ids: Optional[str] = Query(None, description="Comma-separated route IDs"),
    stop_ids: Optional[str] = Query(None, description="Comma-separated stop IDs"),
):
    start_dt = _parse_headway_timestamp(start)
    end_dt = _parse_headway_timestamp(end)
    routes = _parse_headway_ids(route_ids)
    stops = _parse_headway_ids(stop_ids)
    storage = _get_headway_storage()
    events = storage.query_events(
        start_dt,
        end_dt,
        route_ids=routes if routes else None,
        stop_ids=stops if stops else None,
    )
    vehicle_names = _attach_vehicle_names(events)

    # Enrich events with route_name, block, stop_name from current state if missing
    route_id_to_name = getattr(state, "route_id_to_name", None) or {}
    blocks_cache = getattr(state, "blocks_cache", None)
    plain_language_blocks = blocks_cache.get("plain_language_blocks", []) if blocks_cache else []

    # Get stop lookup from headway tracker
    headway_tracker = getattr(app.state, "headway_tracker", None)
    stop_lookup = headway_tracker.stop_lookup if headway_tracker else {}
    address_lookup = getattr(headway_tracker, "address_lookup", {}) if headway_tracker else {}

    # Build vehicle_id -> block lookup
    vehicle_to_block: Dict[str, str] = {}
    for block_entry in plain_language_blocks:
        vid = _block_entry_vehicle_id(block_entry)
        if vid is not None:
            block_id = block_entry.get("block_id") or block_entry.get("block")
            if block_id:
                vehicle_to_block[vid] = block_id

    enriched_events = []
    for ev in events:
        ev_dict = ev.to_dict()

        # Enrich route_name if missing
        if not ev_dict.get("route_name") and ev_dict.get("route_id"):
            rid = ev_dict["route_id"]
            route_name = (
                route_id_to_name.get(rid) or
                route_id_to_name.get(str(rid))
            )
            if not route_name:
                try:
                    route_name = route_id_to_name.get(int(rid))
                except (ValueError, TypeError):
                    pass
            if route_name:
                ev_dict["route_name"] = route_name

        # Enrich block if missing
        if not ev_dict.get("block") and ev_dict.get("vehicle_id"):
            vid = ev_dict["vehicle_id"]
            block = vehicle_to_block.get(str(vid))
            if block:
                ev_dict["block"] = block

        # Enrich stop_name and address_id if missing or address_id is lat/lon fallback
        stop_id = ev_dict.get("stop_id")
        address_id = ev_dict.get("address_id")
        stop_point = None

        # Try to find the stop point
        if stop_id and stop_id in stop_lookup:
            stop_point = stop_lookup[stop_id]
        elif address_id and address_id in address_lookup:
            stop_point = address_lookup[address_id]

        if stop_point:
            # Enrich stop_name if missing
            if not ev_dict.get("stop_name") and stop_point.stop_name:
                ev_dict["stop_name"] = stop_point.stop_name
            # Fix address_id if it's a lat/lon fallback (starts with "loc_")
            if address_id and address_id.startswith("loc_") and stop_point.address_id and not stop_point.address_id.startswith("loc_"):
                ev_dict["address_id"] = stop_point.address_id

        enriched_events.append(ev_dict)

    return {"events": enriched_events, "vehicle_names": vehicle_names}


@app.get("/api/headway/bubbles")
async def api_headway_bubbles():
    """Get current bubble activation states for all vehicles."""
    tracker = getattr(app.state, "headway_tracker", None)
    if not tracker:
        return {"active_states": [], "recent_activations": []}

    now = datetime.now(timezone.utc)
    activation_cutoff = now - timedelta(seconds=120)
    recent_activations = []
    for activation in tracker.recent_bubble_activations:
        ts = activation.get("timestamp")
        try:
            ts_dt = parse_iso8601_utc(ts) if ts else None
        except Exception:
            ts_dt = None
        if ts_dt and ts_dt < activation_cutoff:
            continue
        recent_activations.append(activation)

    return {
        "active_states": tracker.get_active_bubble_states(),
        "recent_activations": recent_activations,
    }


@app.get("/api/headway/export")
async def api_headway_export(
    start: str = Query(..., description="Start timestamp (ISO-8601 UTC)"),
    end: str = Query(..., description="End timestamp (ISO-8601 UTC)"),
    route_ids: Optional[str] = Query(None, description="Comma-separated route IDs"),
    stop_ids: Optional[str] = Query(None, description="Comma-separated stop IDs"),
    threshold_minutes: Optional[float] = Query(
        None, description="Headway threshold in minutes for flagging rows"
    ),
    headway_type: str = Query(
        "arrival_arrival",
        description="Headway type to evaluate threshold (arrival_arrival or departure_arrival)",
    ),
    timezone_name: Optional[str] = Query(
        None,
        description="IANA timezone name to localize timestamps (defaults to America/New_York)",
    ),
):
    start_dt = _parse_headway_timestamp(start)
    end_dt = _parse_headway_timestamp(end)
    routes = _parse_headway_ids(route_ids)
    stops = _parse_headway_ids(stop_ids)
    storage = _get_headway_storage()
    events = storage.query_events(
        start_dt,
        end_dt,
        route_ids=routes if routes else None,
        stop_ids=stops if stops else None,
    )

    headway_field = (
        "headway_departure_arrival"
        if headway_type == "departure_arrival"
        else "headway_arrival_arrival"
    )

    tz = UVA_TZ
    if timezone_name:
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid timezone_name")

    def _format_date(dt: datetime) -> str:
        return dt.astimezone(tz).strftime("%m-%d-%Y")

    def _format_time(dt: datetime) -> str:
        return dt.astimezone(tz).strftime("%I:%M:%S %p").lstrip("0")

    def _format_hms(seconds: Optional[float]) -> str:
        if seconds is None:
            return ""
        total_seconds = int(round(seconds))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    events_sorted = sorted(events, key=lambda ev: ev.timestamp)
    pending: Dict[Tuple[str, str, str, str], List[HeadwayEvent]] = {}
    paired: List[Tuple[Optional[HeadwayEvent], Optional[HeadwayEvent]]] = []

    def _event_key(ev: HeadwayEvent) -> Tuple[str, str, str, str]:
        stop_identifier = ev.stop_id or ev.address_id or ""
        return (
            ev.route_id or "",
            stop_identifier,
            ev.vehicle_id or "",
            ev.block or "",
        )

    for ev in events_sorted:
        key = _event_key(ev)
        if ev.event_type == "arrival":
            pending.setdefault(key, []).append(ev)
        elif ev.event_type == "departure":
            queue = pending.get(key)
            if queue:
                arrival_ev = queue.pop(0)
                paired.append((arrival_ev, ev))
            else:
                paired.append((None, ev))

    for key, arrivals in pending.items():
        for arrival_ev in arrivals:
            paired.append((arrival_ev, None))

    paired.sort(key=lambda pair: (pair[0].timestamp if pair[0] else pair[1].timestamp))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "Route",
            "Arrival Date",
            "Stop",
            "Vehicle",
            "Arrival Time",
            "Departure Time",
            "Dwell",
            "Headway",
        ]
    )

    for arrival_ev, departure_ev in paired:
        arrival_ts = arrival_ev.timestamp if arrival_ev else None
        departure_ts = departure_ev.timestamp if departure_ev else None

        dwell_seconds = departure_ev.dwell_seconds if departure_ev else None
        headway_value = None
        for candidate in (arrival_ev, departure_ev):
            if candidate:
                candidate_value = getattr(candidate, headway_field, None)
                if candidate_value is not None:
                    headway_value = candidate_value
                    break

        route_label = (
            (arrival_ev.route_name if arrival_ev else None)
            or (arrival_ev.route_id if arrival_ev else None)
            or (departure_ev.route_name if departure_ev else None)
            or (departure_ev.route_id if departure_ev else None)
            or ""
        )
        stop_label = (
            (arrival_ev.stop_name if arrival_ev else None)
            or (arrival_ev.stop_id if arrival_ev else None)
            or (arrival_ev.address_id if arrival_ev else None)
            or (departure_ev.stop_name if departure_ev else None)
            or (departure_ev.stop_id if departure_ev else None)
            or (departure_ev.address_id if departure_ev else None)
            or ""
        )
        vehicle_label = (
            (arrival_ev.vehicle_name if arrival_ev else None)
            or (arrival_ev.vehicle_id if arrival_ev else None)
            or (departure_ev.vehicle_name if departure_ev else None)
            or (departure_ev.vehicle_id if departure_ev else None)
            or ""
        )

        writer.writerow(
            [
                route_label,
                _format_date(arrival_ts) if arrival_ts else "",
                stop_label,
                vehicle_label,
                _format_time(arrival_ts) if arrival_ts else "",
                _format_time(departure_ts) if departure_ts else "",
                _format_hms(dwell_seconds),
                _format_hms(headway_value),
            ]
        )
    payload = buf.getvalue()
    headers = {
        "Content-Disposition": "attachment; filename=\"headway_export.csv\"",
        "Content-Type": "text/csv",
    }
    return Response(content=payload, media_type="text/csv", headers=headers)


# ---------------------------
# REST: UVA Athletics (home games)
# ---------------------------


def _parse_local_date_param(value: Optional[str], label: str) -> Optional[date]:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid {label} format; expected YYYY-MM-DD")


def _parse_local_time_param(value: Optional[str], label: str) -> Optional[dtime]:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"invalid {label} format; expected HH:MM")


def _load_cached_uva_events(now: datetime) -> List[Dict[str, Any]]:
    cache = ensure_uva_athletics_cache(now=now)
    events = cache.get("events") if isinstance(cache, dict) else None
    if isinstance(events, list):
        return events
    return load_cached_events()


@app.get("/api/uva_athletics/home")
async def api_uva_home_events(
    start_date: Optional[str] = Query(None, description="Local start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Local end date (YYYY-MM-DD)"),
    start_time: Optional[str] = Query(None, description="Local start time filter (HH:MM, 24-hour)"),
    end_time: Optional[str] = Query(None, description="Local end time filter (HH:MM, 24-hour)"),
):
    """
    Return cached UVA athletics home games sourced from the WMT ICS feed.

    The cache refreshes once per day shortly after 03:00 America/New_York, or on
    the first request after that threshold if the service was offline. Query
    parameters filter by local date and time; when no date window is provided the
    endpoint defaults to upcoming events (start time after "now").
    """

    now = datetime.now(UVA_TZ)
    start_date_val = _parse_local_date_param(start_date, "start_date")
    end_date_val = _parse_local_date_param(end_date, "end_date")
    if start_date_val and end_date_val and end_date_val < start_date_val:
        raise HTTPException(status_code=400, detail="end_date must be on or after start_date")

    start_time_val = _parse_local_time_param(start_time, "start_time")
    end_time_val = _parse_local_time_param(end_time, "end_time")
    if start_time_val and end_time_val and end_time_val < start_time_val:
        raise HTTPException(status_code=400, detail="end_time must be on or after start_time")

    cached_events = _load_cached_uva_events(now)
    filtered: List[Dict[str, Any]] = []
    for ev in cached_events:
        if not ev.get("is_home") and not is_home_location(ev.get("raw_location", "")):
            continue

        start_str = ev.get("start_time")
        end_str = ev.get("end_time")
        if not start_str or not end_str:
            continue
        try:
            start_dt = datetime.fromisoformat(start_str)
            end_dt = datetime.fromisoformat(end_str)
        except ValueError:
            continue

        start_local = start_dt.astimezone(UVA_TZ)
        end_local = end_dt.astimezone(UVA_TZ)

        if not start_date_val and not end_date_val and start_local < now:
            continue
        if start_date_val and start_local.date() < start_date_val:
            continue
        if end_date_val and start_local.date() > end_date_val:
            continue

        start_local_time = start_local.time()
        if start_time_val and start_local_time < start_time_val:
            continue
        if end_time_val and start_local_time > end_time_val:
            continue

        filtered.append(
            {
                "start_time": start_local.isoformat(),
                "end_time": end_local.isoformat(),
                "sport": ev.get("sport", ""),
                "opponent": ev.get("opponent", ""),
                "city": ev.get("city", ""),
                "state": ev.get("state", ""),
                "extra_location_detail": ev.get("extra_location_detail"),
                "raw_summary": ev.get("raw_summary", ""),
                "raw_location": ev.get("raw_location", ""),
                "uid": ev.get("uid", ""),
            }
        )

    filtered.sort(key=lambda e: e["start_time"])
    return {"events": filtered}


# ---------------------------
# Startup background updater
# ---------------------------
@app.on_event("startup")
async def startup():
    async with state.lock:
        load_bus_days()
        load_vehicle_headings()

    try:
        headway_route_ids, headway_stop_ids = load_headway_config()
        approach_sets_config = load_approach_sets_config(data_dirs=DATA_DIRS)
        headway_storage = HeadwayStorage(HEADWAY_DIR)

        # Create lookup callbacks that read from state at event time
        def route_name_lookup(route_id: Optional[str]) -> Optional[str]:
            if route_id is None:
                return None
            route_id_to_name = getattr(state, "route_id_to_name", None)
            if not route_id_to_name:
                print(f"[headway] route_name_lookup: no route_id_to_name map available")
                return None
            # Try string key first, then int key (TransLoc uses int RouteID)
            result = route_id_to_name.get(route_id) or route_id_to_name.get(str(route_id))
            if result:
                return result
            try:
                result = route_id_to_name.get(int(route_id))
                if not result:
                    print(f"[headway] route_name_lookup: route_id={route_id} not found in map with {len(route_id_to_name)} entries")
                return result
            except (ValueError, TypeError):
                print(f"[headway] route_name_lookup: route_id={route_id} not found, int conversion failed")
                return None

        def vehicle_block_lookup(vehicle_id: Optional[str]) -> Optional[str]:
            if vehicle_id is None:
                return None
            # Get block from the cached blocks data
            blocks_cache = getattr(state, "blocks_cache", None)
            if not blocks_cache:
                print(f"[headway] vehicle_block_lookup: no blocks_cache available")
                return None
            vehicle_to_block = blocks_cache.get("vehicle_to_block", {})
            norm_vid = _normalize_vehicle_id_str(vehicle_id)
            if norm_vid and vehicle_to_block:
                block_id = vehicle_to_block.get(norm_vid)
                if block_id:
                    return block_id
            plain_language_blocks = blocks_cache.get("plain_language_blocks", [])
            for block_entry in plain_language_blocks:
                vid = _block_entry_vehicle_id(block_entry)
                if vid is not None and vid == norm_vid:
                    # Return the block name (e.g., "[06]" or position_name)
                    return block_entry.get("block_id") or block_entry.get("block")
            print(f"[headway] vehicle_block_lookup: vehicle_id={vehicle_id} not found in {len(plain_language_blocks)} blocks")
            return None

        headway_tracker = HeadwayTracker(
            storage=headway_storage,
            arrival_distance_threshold_m=HEADWAY_DISTANCE_THRESHOLD_M,
            departure_distance_threshold_m=HEADWAY_DISTANCE_THRESHOLD_M,
            tracked_route_ids=headway_route_ids,
            tracked_stop_ids=headway_stop_ids,
            route_name_lookup=route_name_lookup,
            vehicle_block_lookup=vehicle_block_lookup,
        )
        app.state.headway_storage = headway_storage
        app.state.headway_tracker = headway_tracker
        app.state.approach_sets_config = approach_sets_config
    except Exception as exc:
        print(f"[headway] initialization failed: {exc}")
        app.state.headway_storage = None
        app.state.headway_tracker = None
        app.state.approach_sets_config = {}

    async def updater():
        await asyncio.sleep(0.1)
        client = _get_transloc_client(None)
        while True:
            start = time.time()
            headway_snapshots: List[VehicleSnapshot] = []
            try:
                routes_catalog: List[Dict[str, Any]] = []
                routes_raw = await fetch_routes_with_shapes(client)
                stops_raw: List[Dict[str, Any]] = []
                try:
                    stops_raw = await fetch_stops(client)
                except Exception as e:
                    print(f"[updater] stops fetch error: {e}")
                try:
                    routes_catalog = await fetch_routes_catalog(client)
                except Exception as e:
                    routes_catalog = []
                    print(f"[updater] routes catalog fetch error: {e}")
                vehicles_raw = await fetch_vehicles(client, include_unassigned=True)
                vehicle_name_lookup = _build_vehicle_name_lookup(vehicles_raw)
                # Fetch vehicle capacities
                try:
                    vehicle_capacities = await fetch_vehicle_capacities(client)
                except Exception as e:
                    vehicle_capacities = {}
                    print(f"[updater] capacity fetch error: {e}")
                # Fetch stop estimates for all active vehicles (pre-populate for fast serving)
                stop_estimates: Dict[Any, List[Dict[str, Any]]] = {}
                try:
                    active_vehicle_ids = [
                        v.get("VehicleID") for v in vehicles_raw
                        if v.get("VehicleID") and v.get("RouteID")
                    ]
                    if active_vehicle_ids:
                        stop_estimates = await _fetch_vehicle_stop_estimates_raw(
                            vehicle_ids=active_vehicle_ids,
                            base_url=None,
                            quantity=3,
                            client=client,
                        )
                        print(f"[updater] pre-fetched stop estimates for {len(stop_estimates)} vehicles")
                except Exception as e:
                    print(f"[updater] stop estimates fetch error: {e}")
                try:
                    block_groups, block_meta = await fetch_block_groups(
                        client, include_metadata=True
                    )
                except Exception as e:
                    block_groups = []
                    block_meta = {}
                    print(f"[updater] block fetch error: {e}")
                fetch_completed_at = datetime.now(timezone.utc)

                # Enrich vehicle name lookup with roster information from block metadata
                if isinstance(block_meta, dict):
                    roster_entries: List[Dict[str, Any]] = []
                    vehicles_meta = block_meta.get("Vehicles")
                    if isinstance(vehicles_meta, list):
                        roster_entries.extend(vehicles_meta)
                    sched_trip = block_meta.get("ScheduleTripVehicles")
                    if isinstance(sched_trip, list):
                        roster_entries.extend(sched_trip)
                    for rec in roster_entries:
                        vid = _normalize_vehicle_id_str(
                            rec.get("VehicleID")
                            or rec.get("VehicleId")
                            or rec.get("vehicle_id")
                            or rec.get("vehicleId")
                        )
                        name = _pick_vehicle_name_record(rec)
                        if vid and name and vid not in vehicle_name_lookup:
                            vehicle_name_lookup[vid] = name
                    async with state.lock:
                        state.routes_raw = routes_raw
                        state.routes_catalog_raw = routes_catalog
                        state.vehicles_raw = vehicles_raw
                        state.stops_raw = stops_raw
                        state.vehicle_capacities = vehicle_capacities
                        state.stop_estimates = stop_estimates
                        try:
                            trimmed_routes = [_trim_transloc_route(r) for r in routes_raw]
                            if routes_catalog:
                                trimmed_routes = _merge_transloc_route_metadata(trimmed_routes, routes_catalog)
                            stops = _build_transloc_stops(
                                trimmed_routes,
                                stops_raw,
                                approach_sets_config=getattr(app.state, "approach_sets_config", None),
                            )
                            state.stops = stops
                            tracker_ref = getattr(app.state, "headway_tracker", None)
                            if tracker_ref is not None:
                                if stops:
                                    tracker_ref.update_stops(stops)
                                else:
                                    state.headway_stop_warning_ts = time.time()
                                    print("[headway] no stops available to update tracker")
                        except Exception as e:
                            print(f"[headway] failed to refresh stops: {e}")
                        # Update complete routes & roster (all buses/all routes)
                        try:
                            state.routes_all = {}
                            combined_routes: List[Dict[str, Any]] = []
                            if routes_catalog:
                                combined_routes.extend(routes_catalog)
                            if routes_raw:
                                combined_routes.extend(routes_raw)
                            for r in combined_routes:
                                rid = r.get("RouteID") or r.get("RouteId")
                                if not rid:
                                    continue
                                desc = (
                                    r.get("Description")
                                    or r.get("RouteName")
                                    or f"Route {rid}"
                                )
                                info = (
                                    r.get("InfoText")
                                    or r.get("Info")
                                    or ""
                                ).strip()
                                disp = f"{desc} — {info}" if info else desc
                                state.routes_all[rid] = disp
                            # Pre-compute route_id_to_name for testmap endpoint performance
                            route_id_to_name_map: Dict[Any, str] = {}
                            for r in routes_raw or []:
                                rid = r.get("RouteID") or r.get("RouteId")
                                route_name = r.get("Description") or r.get("RouteName") or r.get("LongName") or r.get("ShortName")
                                if rid is not None and route_name:
                                    info_text = r.get("InfoText")
                                    if info_text and isinstance(info_text, str) and info_text.strip():
                                        route_id_to_name_map[rid] = f"{route_name} ({info_text.strip()})"
                                    else:
                                        route_id_to_name_map[rid] = route_name
                            state.route_id_to_name = route_id_to_name_map
                            # Pre-materialize testmap vehicles payload for instant serving
                            try:
                                assigned_map: Dict[Any, Tuple[int, Vehicle]] = {}
                                for rid, vehs in state.vehicles_by_route.items():
                                    for veh in vehs.values():
                                        if veh.id is not None:
                                            assigned_map[veh.id] = (rid, veh)
                                testmap_payload = _assemble_transloc_vehicles(
                                    raw_vehicle_records=vehicles_raw,
                                    assigned=assigned_map,
                                    include_stale=True,  # Include all, filter at serve time
                                    capacities=vehicle_capacities,
                                    stop_estimates=stop_estimates,
                                    route_id_to_name=route_id_to_name_map,
                                )
                                state.testmap_vehicles_payload = testmap_payload
                                state.testmap_vehicles_ts = time.time()
                                # Broadcast to SSE subscribers
                                broadcast_testmap_vehicles(testmap_payload)
                            except Exception as e:
                                print(f"[updater] testmap payload build error: {e}")
                            if not hasattr(state, "roster_names"):
                                state.roster_names = set()
                            for _v in vehicles_raw or []:
                                nm = str(_v.get("Name") or "-")
                                if nm:
                                    state.roster_names.add(nm)
                            # Pre-compute sorted vehicle dropdown lists
                            try:
                                dropdown_all = []
                                dropdown_active = []
                                seen_names: set = set()
                                sorted_vehicles = sorted(vehicles_raw or [], key=lambda x: str(x.get("Name") or "-"))
                                for v in sorted_vehicles:
                                    name = str(v.get("Name") or "-")
                                    if name in seen_names:
                                        continue
                                    seen_names.add(name)
                                    vid = v.get("VehicleID")
                                    age = v.get("Seconds")
                                    item = {
                                        "id": vid,
                                        "name": name,
                                        "route_id": v.get("RouteID"),
                                        "age_seconds": age,
                                    }
                                    if vid is not None and vid in vehicle_capacities:
                                        cap_data = vehicle_capacities[vid]
                                        item["capacity"] = cap_data.get("capacity")
                                        item["current_occupation"] = cap_data.get("current_occupation")
                                        item["percentage"] = cap_data.get("percentage")
                                    dropdown_all.append(item)
                                    if age is not None and age <= STALE_FIX_S:
                                        dropdown_active.append(item)
                                state.vehicles_dropdown_all = dropdown_all
                                state.vehicles_dropdown_active = dropdown_active
                            except Exception as e:
                                print(f"[updater] dropdown build error: {e}")
                        except Exception as e:
                            print(f"[updater] roster/routes_all error: {e}")
                        # Build active routes set (with grace window to prevent flicker)
                        fresh = []
                        fresh_all = []  # includes unassigned vehicles for mileage tracking
                        for v in vehicles_raw:
                            tms = parse_msajax(v.get("TimeStampUTC") or v.get("TimeStamp"))
                            age = v.get("Seconds") if v.get("Seconds") is not None else (max(0, (time.time()*1000 - tms)/1000) if tms else 9999)
                            if age <= STALE_FIX_S:
                                fresh_all.append(v)
                                if v.get("RouteID") and v.get("RouteID") != 0:
                                    fresh.append(v)
                        active_ids = {v["RouteID"] for v in fresh}
                        now_ts = time.time()
                        for rid in active_ids:
                            state.route_last_seen[rid] = now_ts
                        keep_ids = set()
                        for rid in set(list(getattr(state, 'routes', {}).keys()) + list(active_ids)):
                            last = state.route_last_seen.get(rid, 0)
                            if (rid in active_ids) or (now_ts - last <= ROUTE_GRACE_S):
                                keep_ids.add(rid)
                        state.active_route_ids = set(active_ids)

                        # Update / add routes for all keep_ids (not just currently active)
                        for r in routes_raw:
                            rid = r.get("RouteID")
                            if not rid or rid not in keep_ids:
                                continue
                            enc = r.get("EncodedPolyline") or ""
                            desc = r.get("Description") or f"Route {rid}"
                            info = (r.get("InfoText") or "").strip()
                            name = f"{desc} — {info}" if info else desc
                            prev = state.routes.get(rid)
                            if not prev or prev.encoded != enc:
                                poly = decode_polyline(enc)
                                if len(poly) < 2:
                                    continue
                                cum, length = cumulative_distance(poly)
                                col = r.get("MapLineColor") or r.get("Color") or r.get("RouteColor")
                                if col and not str(col).startswith("#"):
                                    col = f"#{col}"
                                route = Route(id=rid, name=name, encoded=enc, poly=poly, cum=cum, length_m=length, color=col)
                                # fetch speed profile (fetch-once policy)
                                caps, names = await fetch_overpass_speed_profile(route, client)
                                route.seg_caps_mps = caps
                                route.seg_names = names
                                state.routes[rid] = route

                        # Vehicles per route: rebuild from fresh data only to avoid lingering assignments
                        prev_map = getattr(state, 'vehicles_by_route', {})
                        new_map: Dict[int, Dict[int, Vehicle]] = {rid: {} for rid in keep_ids}
                        for v in fresh:
                            rid = v["RouteID"]
                            if rid not in keep_ids or rid not in state.routes:
                                continue
                            name = str(v.get("Name") or "-")
                            raw_vid = v.get("VehicleID")
                            try:
                                vid = int(raw_vid) if raw_vid is not None else None
                            except (TypeError, ValueError):
                                vid = raw_vid
                            tsms = parse_msajax(v.get("TimeStampUTC") or v.get("TimeStamp")) or int(time.time()*1000)
                            mps = (v.get("GroundSpeed") or 0.0) * MPH_TO_MPS
                            lat = v.get("Latitude")
                            lon = v.get("Longitude")
                            prev = prev_map.get(rid, {}).get(vid)
                            heading = None
                            if prev and prev.lat is not None and prev.lon is not None and lat is not None and lon is not None:
                                move = haversine((prev.lat, prev.lon), (lat, lon))
                                if move >= HEADING_JITTER_M:
                                    heading = bearing_between((prev.lat, prev.lon), (lat, lon))
                                else:
                                    heading = prev.heading
                            if heading is None and vid is not None:
                                cached = state.last_headings.get(vid)
                                cached_heading = cached.get("heading") if isinstance(cached, dict) else None
                                if cached_heading is not None and math.isfinite(cached_heading):
                                    heading = float(cached_heading)
                            if heading is None or not math.isfinite(heading):
                                heading = 0.0
                            heading = normalize_heading_deg(float(heading))
                            age_s = v.get("Seconds")
                            if age_s is None:
                                age_s = max(0, (time.time()*1000 - tsms) / 1000)
                            veh = Vehicle(id=vid, name=name, lat=lat, lon=lon, ts_ms=tsms,
                                          ground_mps=mps, age_s=age_s, heading=heading)
                            prev_idx = prev.seg_idx if prev else None
                            s_pos, seg_idx = project_vehicle_to_route(veh, state.routes[rid], prev_idx, heading)
                            prev_sign = prev.dir_sign if prev else state.last_dir_sign.get(vid, 0)
                            ema = prev.ema_mps if prev else (mps if mps > 0 else 6.0)
                            L = state.routes[rid].length_m
                            dt = ((veh.ts_ms - prev.ts_ms) / 1000.0) if prev else 0.0
                            if prev and dt > 0:
                                raw = s_pos - prev.s_pos
                                delta = ((raw + L / 2) % L) - L / 2
                                along_mps = delta / dt
                            else:
                                along_mps = 0.0
                            DIR_EPS = 0.3
                            if along_mps > DIR_EPS:
                                dir_sign = +1
                            elif along_mps < -DIR_EPS:
                                dir_sign = -1
                            else:
                                dir_sign = prev_sign
                                if abs(along_mps) <= DIR_EPS and prev_sign == 0 and seg_idx is not None:
                                    seg_heading = bearing_between(
                                        state.routes[rid].poly[seg_idx],
                                        state.routes[rid].poly[seg_idx + 1],
                                    )
                                    dir_sign = +1 if heading_diff(heading, seg_heading) <= 90 else -1
                            disp = abs(along_mps)
                            measured = 0.5 * mps + 0.5 * disp if mps > 0 else (disp if prev else mps)
                            ema = EMA_ALPHA * measured + (1 - EMA_ALPHA) * ema
                            ema = max(MIN_SPEED_FLOOR, min(MAX_SPEED_CEIL, ema))
                            veh.s_pos = s_pos; veh.ema_mps = ema; veh.dir_sign = dir_sign
                            veh.seg_idx = seg_idx; veh.along_mps = along_mps
                            new_map[rid][vid] = veh
                            state.last_dir_sign[vid] = dir_sign
                        state.vehicles_by_route = new_map

                        # Use the most recent block assignments we have on hand while building snapshots
                        vehicle_to_block: Dict[str, str] = {}
                        blocks_cache = getattr(state, "blocks_cache", None)
                        if isinstance(blocks_cache, dict):
                            cached_map = blocks_cache.get("vehicle_to_block")
                            if isinstance(cached_map, dict):
                                vehicle_to_block = dict(cached_map)

                        for rid, vehs in new_map.items():
                            for veh in vehs.values():
                                if veh.lat is None or veh.lon is None:
                                    continue
                                veh_id_text = str(veh.id) if veh.id is not None else None
                                snapshot_name = vehicle_name_lookup.get(veh_id_text) if veh_id_text else None
                                if snapshot_name is None and veh.name and veh.name != "-":
                                    snapshot_name = str(veh.name)

                                snapshot_block = vehicle_to_block.get(veh_id_text) if veh_id_text else None

                                headway_snapshots.append(
                                    VehicleSnapshot(
                                        vehicle_id=veh_id_text,
                                        vehicle_name=snapshot_name,
                                        lat=veh.lat,
                                        lon=veh.lon,
                                        route_id=str(rid) if rid is not None else None,
                                        timestamp=fetch_completed_at,
                                        heading_deg=veh.heading,
                                        block=snapshot_block,
                                    )
                                )
                        current_vehicle_ids: set[int] = set()
                        for vehs in new_map.values():
                            for veh in vehs.values():
                                vid = veh.id
                                if vid is None:
                                    continue
                                current_vehicle_ids.add(vid)
                                heading_val = veh.heading
                                if heading_val is None or not math.isfinite(heading_val):
                                    continue
                                normalized_heading = normalize_heading_deg(float(heading_val))
                                entry = state.last_headings.get(vid)
                                if not isinstance(entry, dict):
                                    state.last_headings[vid] = {
                                        "heading": normalized_heading,
                                        "updated_at": int(veh.ts_ms),
                                    }
                                    state.last_headings_dirty = True
                                else:
                                    prev_heading = entry.get("heading")
                                    if prev_heading is None or not math.isfinite(prev_heading) or abs(prev_heading - normalized_heading) > 1e-6:
                                        entry["heading"] = normalized_heading
                                        entry["updated_at"] = int(veh.ts_ms)
                                        state.last_headings_dirty = True
                                    else:
                                        entry["updated_at"] = int(veh.ts_ms)
                        stale_ids = [vid for vid in list(state.last_headings.keys()) if vid not in current_vehicle_ids]
                        if stale_ids:
                            for vid in stale_ids:
                                state.last_headings.pop(vid, None)
                            state.last_headings_dirty = True
                        if state.last_headings_dirty:
                            save_vehicle_headings()
                            state.last_headings_dirty = False
                        # Track per-day mileage for all fresh vehicles (including RouteID 0)
                        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                        bus_days = state.bus_days.setdefault(today, {})
                        for v in fresh_all:
                            name = normalize_bus_name(v.get("Name"))
                            if not name:
                                continue
                            lat = v.get("Latitude")
                            lon = v.get("Longitude")
                            if lat is None or lon is None:
                                continue
                            bd = bus_days.get(name)
                            if bd is None:
                                bd = BusDay()
                                prev_bd = None
                                for d in sorted(state.bus_days.keys(), reverse=True):
                                    if d < today:
                                        prev_bd = state.bus_days[d].get(name)
                                        if prev_bd:
                                            break
                                if prev_bd:
                                    bd.total_miles = prev_bd.total_miles
                                    bd.reset_miles = prev_bd.reset_miles
                                    bd.last_lat = prev_bd.last_lat
                                    bd.last_lon = prev_bd.last_lon
                                bus_days[name] = bd
                            if bd.last_lat is not None and bd.last_lon is not None:
                                delta_miles = haversine((bd.last_lat, bd.last_lon), (lat, lon)) / 1609.34
                                bd.total_miles += delta_miles
                                bd.day_miles += delta_miles
                            bd.last_lat = lat
                            bd.last_lon = lon
                        # Update block assignments cache and history
                        color_by_route = {rid: r.color for rid, r in state.routes.items() if r.color}
                        route_by_bus = {}
                        for rid, vehs in state.vehicles_by_route.items():
                            for v in vehs.values():
                                route_by_bus[str(v.name)] = rid
                        vehicle_roster: List[Dict[str, Any]] = []
                        if isinstance(block_meta, dict):
                            vehicles_raw = block_meta.get("Vehicles")
                            if isinstance(vehicles_raw, list):
                                vehicle_roster.extend(vehicles_raw)
                            sched_trip = block_meta.get("ScheduleTripVehicles")
                            if isinstance(sched_trip, list):
                                vehicle_roster.extend(sched_trip)

                        plain_language_blocks = _extract_plain_language_blocks(
                            block_groups, vehicle_roster=vehicle_roster
                        )

                        vehicle_to_block = {}
                        for block in plain_language_blocks:
                            vid = _block_entry_vehicle_id(block)
                            block_id = block.get("block_id") or block.get("block")
                            if vid and block_id:
                                vehicle_to_block[vid] = block_id

                        for block in plain_language_blocks:
                            vid = _normalize_vehicle_id_str(
                                block.get("vehicle_id")
                                or block.get("VehicleID")
                                or block.get("vehicleId")
                            )
                            name = _pick_vehicle_name_record(block)
                            if vid and name and vid not in vehicle_name_lookup:
                                vehicle_name_lookup[vid] = name

                        state.headway_vehicle_names = vehicle_name_lookup

                        state.blocks_cache = {
                            "block_groups": block_groups,
                            "color_by_route": color_by_route,
                            "route_by_bus": route_by_bus,
                            "plain_language_blocks": plain_language_blocks,
                            "vehicle_to_block": vehicle_to_block,
                        }
                        state.blocks_cache_ts = time.time()
                        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                        bus_days = state.bus_days.setdefault(today, {})
                        for grp in block_groups:
                            for blk in grp.get("Blocks", []):
                                bid = blk.get("BlockId")
                                for trip in blk.get("Trips", []):
                                    bus = normalize_bus_name(trip.get("VehicleName"))
                                    if bus and bid:
                                        bd = bus_days.setdefault(bus, BusDay())
                                        bd.blocks.add(bid)
                        save_bus_days()
                    tracker = getattr(app.state, "headway_tracker", None)
                    if tracker and headway_snapshots:
                        tracker.process_snapshots(headway_snapshots)
            except Exception as e:
                # record last error for UI surfacing
                try:
                    async with state.lock:
                        state.last_error = str(e)
                        state.last_error_ts = time.time()
                except Exception as inner:
                    print(f"[updater] failed recording last_error: {inner}")
                print("[updater] error:", e)
            # sleep until next
            dt = max(0.5, VEH_REFRESH_S - (time.time()-start))
            await asyncio.sleep(dt)
    async def vehicle_logger():
        await asyncio.sleep(0.1)
        client = _get_transloc_client(None)
        while True:
            ts = int(time.time()*1000)
            try:
                r = await client.get(VEH_LOG_URL)
                r.raise_for_status()
                data = r.json()
                vehicles = data if isinstance(data, list) else data.get("d", [])

                # Capture a full snapshot of vehicles but avoid
                # writing a new log entry unless at least one moves.
                valid: list[dict] = []
                moved = False
                for v in vehicles:
                    serialized = _serialize_vehicle_for_log(v)
                    if not serialized:
                        continue
                    vid = serialized["VehicleID"]
                    pos = (serialized["Latitude"], serialized["Longitude"])
                    last = LAST_LOG_POS.get(vid)
                    if not moved and (last is None or haversine(pos, last) >= VEH_LOG_MIN_MOVE_M):
                        moved = True
                    LAST_LOG_POS[vid] = pos
                    valid.append(serialized)
                vehicles = valid
                vehicle_ids = {v.get("VehicleID") for v in vehicles}

                blocks: Dict[int, str] = {}
                if vehicle_ids:
                    try:
                        ds = datetime.now().strftime("%m/%d/%Y")
                        sched_url = (
                            "https://uva.transloc.com/Services/JSONPRelay.svc/"
                            f"GetScheduleVehicleCalendarByDateAndRoute?dateString={quote(ds)}"
                        )
                        sr = await client.get(sched_url)
                        sr.raise_for_status()
                        sched = sr.json() or []
                        ids = ",".join(str(s["ScheduleVehicleCalendarID"]) for s in sched)
                        if ids:
                            block_url = (
                                "https://uva.transloc.com/Services/JSONPRelay.svc/"
                                f"GetDispatchBlockGroupData?scheduleVehicleCalendarIdsString={ids}"
                            )
                            br = await client.get(block_url)
                            br.raise_for_status()
                            data2 = br.json() or {}
                            groups = data2.get("BlockGroups", [])
                            alias = {
                                "[01]": "[01]/[04]",
                                "[03]": "[05]/[03]",
                                "[04]": "[01]/[04]",
                                "[05]": "[05]/[03]",
                                "[06]": "[22]/[06]",
                                "[10]": "[20]/[10]",
                                "[15]": "[26]/[15]",
                                "[16] AM": "[21]/[16] AM",
                                "[17]": "[23]/[17]",
                                "[18] AM": "[24]/[18] AM",
                                "[20] AM": "[20]/[10]",
                                "[21] AM": "[21]/[16] AM",
                                "[22] AM": "[22]/[06]",
                                "[23]": "[23]/[17]",
                                "[24] AM": "[24]/[18] AM",
                                "[26] AM": "[26]/[15]",
                            }
                            mapping: Dict[int, str] = {}
                            for g in groups:
                                block = (g.get("BlockGroupId") or "").strip()
                                vehicle_id = (
                                    g.get("Blocks", [{}])[0]
                                    .get("Trips", [{}])[0]
                                    .get("VehicleID")
                                    or g.get("VehicleId")
                                )
                                if block and "[" in block and vehicle_id is not None:
                                    mapping[vehicle_id] = alias.get(block, block)
                            blocks = mapping
                    except Exception as e:
                        print(f"[vehicle_logger] block error: {e}")

                blocks = {vid: name for vid, name in blocks.items() if vid in vehicle_ids}
                day_key = datetime.fromtimestamp(ts / 1000).strftime("%Y%m%d")
                minimal_routes: Optional[List[Dict[str, Any]]] = None
                routes_serialized: Optional[str] = None
                routes_hash: Optional[str] = None
                try:
                    routes_raw: List[Dict[str, Any]] = []
                    routes_catalog_raw: List[Dict[str, Any]] = []
                    async with state.lock:
                        routes_raw = list(getattr(state, "routes_raw", []))
                        routes_catalog_raw = list(getattr(state, "routes_catalog_raw", []))
                    trimmed_routes = [_trim_transloc_route(r) for r in routes_raw]
                    if routes_catalog_raw:
                        trimmed_routes = _merge_transloc_route_metadata(trimmed_routes, routes_catalog_raw)
                    minimal: List[Dict[str, Any]] = []
                    for route in trimmed_routes:
                        if not isinstance(route, dict):
                            continue
                        rid = route.get("RouteID")
                        if rid is None:
                            continue
                        entry_min: Dict[str, Any] = {"RouteID": rid}
                        for key in ("Description", "RouteName", "InfoText", "MapLineColor", "EncodedPolyline"):
                            val = route.get(key)
                            if val not in (None, ""):
                                entry_min[key] = val
                        minimal.append(entry_min)
                    minimal.sort(key=lambda item: str(item.get("RouteID")))
                    minimal_routes = minimal
                    routes_serialized = json.dumps(
                        minimal_routes,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    routes_hash = hashlib.sha256(routes_serialized.encode("utf-8")).hexdigest()
                except Exception as route_err:
                    minimal_routes = None
                    routes_serialized = None
                    routes_hash = None
                    print(f"[vehicle_logger] route snapshot error: {route_err}")

                ondemand_payload: Dict[str, Any] = {}
                try:
                    ondemand_client = getattr(app.state, "ondemand_client", None)
                    if ondemand_client is not None:
                        ondemand_payload = await _collect_ondemand_data(
                            ondemand_client,
                            now=datetime.fromtimestamp(ts / 1000, timezone.utc),
                        )
                except Exception as exc:
                    ondemand_payload = {}
                    print(f"[vehicle_logger] ondemand fetch failed: {exc}")

                should_log_entry = moved and bool(vehicles)
                if ondemand_payload.get("vehicles") or ondemand_payload.get("ondemandStops"):
                    should_log_entry = True
                if not should_log_entry:
                    await asyncio.sleep(VEH_LOG_INTERVAL_S)
                    continue

                entry = {"ts": ts, "vehicles": vehicles, "blocks": blocks}
                if ondemand_payload.get("vehicles") or ondemand_payload.get("ondemandStops"):
                    entry["ondemand"] = ondemand_payload
                fname = datetime.fromtimestamp(ts/1000).strftime("%Y%m%d_%H.jsonl")
                for log_dir in VEH_LOG_DIRS:
                    path = log_dir / fname
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if routes_serialized is not None and routes_hash is not None:
                        routes_path = log_dir / f"{day_key}_routes.json"
                        route_hash_key = (os.fspath(log_dir), day_key)
                        try:
                            if (
                                LAST_ROUTE_SNAPSHOT_HASH.get(route_hash_key) != routes_hash
                                or not routes_path.exists()
                            ):
                                routes_path.parent.mkdir(parents=True, exist_ok=True)
                                with routes_path.open("w") as rf:
                                    rf.write(routes_serialized)
                                    rf.flush()
                                    os.fsync(rf.fileno())
                                LAST_ROUTE_SNAPSHOT_HASH[route_hash_key] = routes_hash
                        except Exception as route_write_err:
                            print(f"[vehicle_logger] failed to persist routes: {route_write_err}")
                    with path.open("a") as f:
                        f.write(json.dumps(entry) + "\n")
                        f.flush()
                        os.fsync(f.fileno())
                prune_old_entries()
            except Exception as e:
                print(f"[vehicle_logger] error: {e}")
            await asyncio.sleep(VEH_LOG_INTERVAL_S)

    asyncio.create_task(updater())
    asyncio.create_task(vehicle_logger())

    # Background poller for ondemand data during operating hours (19:30-05:30 ET)
    async def ondemand_poller():
        await asyncio.sleep(1)  # Let other startup tasks complete
        poll_interval_s = max(ONDEMAND_POSITIONS_TTL_S, 5)
        idle_check_interval_s = 60  # Check every minute when outside operating hours
        while True:
            try:
                if not _is_within_ondemand_operating_window():
                    await asyncio.sleep(idle_check_interval_s)
                    continue
                client: Optional[OnDemandClient] = getattr(app.state, "ondemand_client", None)
                if client is None:
                    await asyncio.sleep(idle_check_interval_s)
                    continue
                # Warm the caches by fetching ondemand data
                await _collect_ondemand_data(client)
            except Exception as exc:
                print(f"[ondemand_poller] error: {exc}")
            await asyncio.sleep(poll_interval_s)

    asyncio.create_task(ondemand_poller())

    # Background poller for push notifications on new TransLoc service alerts
    async def push_notification_poller():
        global _sent_alert_ids
        _sent_alert_ids = _load_sent_alert_ids()
        print(f"[push_notification_poller] started, loaded {len(_sent_alert_ids)} previously sent alert IDs")
        await asyncio.sleep(5)  # Let other startup tasks complete
        poll_interval_s = 60  # Check every minute
        poll_count = 0
        while True:
            try:
                poll_count += 1
                if not VAPID_PUBLIC_KEY or not VAPID_PRIVATE_KEY:
                    if poll_count == 1:
                        print("[push_notification_poller] VAPID keys not configured, sleeping")
                    await asyncio.sleep(poll_interval_s * 5)  # Less frequent check if not configured
                    continue
                subscriptions = await push_subscription_store.get_all_subscriptions()
                if not subscriptions:
                    if poll_count == 1:
                        print("[push_notification_poller] no subscribers, sleeping")
                    await asyncio.sleep(poll_interval_s)
                    continue
                if poll_count <= 3 or poll_count % 10 == 0:
                    print(f"[push_notification_poller] poll #{poll_count}, {len(subscriptions)} subscribers")
                # Fetch recent alerts from TransLoc
                async with httpx.AsyncClient(timeout=10.0) as client:
                    url = f"{transloc_host_base(None)}/Secure/Services/RoutesService.svc/GetMessagesPaged"
                    resp = await client.get(url, params={
                        "showInactive": "false",
                        "rows": "20",
                        "page": "1",
                        "sortIndex": "StartDateUtc",
                        "sortOrder": "desc",
                    })
                    if resp.status_code != 200:
                        print(f"[push_notification_poller] TransLoc API returned {resp.status_code}")
                        await asyncio.sleep(poll_interval_s)
                        continue
                    data = resp.json()
                    alerts = data.get("Rows", []) or data.get("Data", []) if isinstance(data, dict) else []
                    print(f"[push_notification_poller] fetched {len(alerts)} alerts from TransLoc, {len(_sent_alert_ids)} already sent")
                    new_alerts = []
                    for alert in alerts:
                        alert_id = str(alert.get("MessageId") or alert.get("Id", ""))
                        if not alert_id:
                            print(f"[push_notification_poller] skipping alert with no ID: {alert}")
                            continue
                        if alert_id in _sent_alert_ids:
                            continue
                        message = (alert.get("MessageText") or alert.get("Message") or "").strip()
                        if not message:
                            print(f"[push_notification_poller] skipping alert {alert_id} with no message")
                            continue
                        new_alerts.append({
                            "id": alert_id,
                            "title": "UTS Service Alert",
                            "body": message[:200],
                            "icon": "/media/icon-192.png",
                            "tag": f"alert-{alert_id}",
                            "url": "/map",
                        })
                        print(f"[push_notification_poller] new alert {alert_id}: {message[:50]}...")
                    if new_alerts:
                        from pywebpush import webpush, WebPushException
                        print(f"[push_notification_poller] sending {len(new_alerts)} alerts to {len(subscriptions)} subscribers")
                        for alert_payload in new_alerts:
                            sent_count = 0
                            for sub in subscriptions:
                                try:
                                    webpush(
                                        subscription_info=sub.to_subscription_info(),
                                        data=json.dumps(alert_payload),
                                        vapid_private_key=VAPID_PRIVATE_KEY,
                                        vapid_claims={"sub": VAPID_SUBJECT},
                                    )
                                    sent_count += 1
                                except WebPushException as e:
                                    if e.response and e.response.status_code == 410:
                                        # Subscription expired
                                        print(f"[push_notification_poller] removing expired subscription")
                                        await push_subscription_store.remove_subscription(sub.endpoint)
                                    else:
                                        print(f"[push_notification_poller] WebPushException: {e}")
                                except Exception as push_err:
                                    print(f"[push_notification_poller] push error: {push_err}")
                            _sent_alert_ids.add(alert_payload["id"])
                            print(f"[push_notification_poller] sent alert {alert_payload['id']} to {sent_count}/{len(subscriptions)} subscribers")
                        _save_sent_alert_ids(_sent_alert_ids)
            except Exception as exc:
                print(f"[push_notification_poller] error: {exc}")
            await asyncio.sleep(poll_interval_s)

    asyncio.create_task(push_notification_poller())

# ---------------------------
# REST: Routes
# ---------------------------
@app.get("/v1/routes")
async def list_routes():
    async with state.lock:
        items = []
        routes_all = getattr(state, "routes_all", {}) or {}
        actives = set(state.vehicles_by_route.keys())
        for rid, disp_name in routes_all.items():
            veh_count = len(state.vehicles_by_route.get(rid, {}))
            name = disp_name if veh_count > 0 else f"{disp_name} (inactive)"
            length_m = state.routes.get(rid).length_m if rid in state.routes else None
            items.append({
                "id": rid,
                "name": name,
                "length_m": length_m,
                "active_vehicles": veh_count
            })
        items.sort(key=lambda x: str(x["name"]))
        return {"routes": items}

@app.get("/v1/routes_all")
async def routes_all():
    async with state.lock:
        items = []
        if hasattr(state, "routes_all") and isinstance(state.routes_all, dict):
            actives = set(state.vehicles_by_route.keys())
            for rid, name in state.routes_all.items():
                items.append({"id": rid, "name": name, "active": (rid in actives)})
        items.sort(key=lambda x: str(x["name"]))
        return {"routes": items}

@app.get("/v1/routes/{route_id}")
async def route_info(route_id: int):
    async with state.lock:
        route = state.routes.get(route_id)
        if not route:
            raise HTTPException(404, "route not found or inactive")
        return {"id": route.id, "name": route.name, "color": route.color, "length_m": route.length_m}

@app.get("/v1/routes/{route_id}/shape")
async def route_shape(route_id: int):
    async with state.lock:
        route = state.routes.get(route_id)
        if not route:
            raise HTTPException(404, "route not found or inactive")
        return {"polyline": route.encoded, "color": route.color}

# ---------------------------
# REST: Vehicles roster (for driver dropdown)
# ---------------------------
@app.get("/v1/roster/vehicles")
async def roster_vehicles():
    async with state.lock:
        names = sorted(list(getattr(state, "roster_names", set())))
        return {"vehicles": [{"name": n} for n in names]}

@app.get("/v1/vehicles")
async def all_vehicles(include_stale: int = 1, include_unassigned: int = 1):
    """
    Returns a flat list of vehicles for dropdowns.
    include_unassigned=1 -> includes lot/unassigned units.
    include_stale=1     -> includes stale units.

    Uses cached vehicle data from background polling loop for fast response.
    """
    # Fast path: use pre-computed dropdown lists for common cases
    if include_unassigned:
        async with state.lock:
            if include_stale and state.vehicles_dropdown_all:
                return {"vehicles": list(state.vehicles_dropdown_all)}
            elif not include_stale and state.vehicles_dropdown_active:
                return {"vehicles": list(state.vehicles_dropdown_active)}

    # Fallback: compute on request for filtered cases
    async with state.lock:
        data = state.vehicles_raw.copy() if state.vehicles_raw else []
        capacities = state.vehicle_capacities.copy()

    # Filter by unassigned if needed
    if not include_unassigned:
        data = [v for v in data if v.get("RouteID")]

    items = []
    seen = set()
    for v in data:
        name = str(v.get("Name") or "-")
        if name in seen:
            continue
        seen.add(name)
        age = v.get("Seconds")
        if not include_stale and (age is None or age > STALE_FIX_S):
            continue
        vid = v.get("VehicleID")
        item = {
            "id": vid,
            "name": name,
            "route_id": v.get("RouteID"),
            "age_seconds": age,
        }
        # Add capacity data if available for this vehicle
        if vid is not None and vid in capacities:
            cap_data = capacities[vid]
            item["capacity"] = cap_data.get("capacity")
            item["current_occupation"] = cap_data.get("current_occupation")
            item["percentage"] = cap_data.get("percentage")
        items.append(item)
    items.sort(key=lambda x: str(x["name"]))
    return {"vehicles": items}


@app.get("/v1/vehicle_headings")
async def vehicle_headings():
    async with state.lock:
        payload: Dict[str, Dict[str, Any]] = {}
        for vid, entry in state.last_headings.items():
            if not isinstance(entry, dict):
                continue
            heading_val = entry.get("heading")
            if heading_val is None or not math.isfinite(heading_val):
                continue
            record: Dict[str, Any] = {"heading": normalize_heading_deg(float(heading_val))}
            ts_val = entry.get("updated_at")
            if ts_val is not None:
                try:
                    record["updated_at"] = int(ts_val)
                except (TypeError, ValueError):
                    pass
            payload[str(vid)] = record
    return {"headings": payload}


@app.get("/v1/vehicle_drivers")
async def public_vehicle_drivers():
    """
    Public endpoint for vehicle ID to vehicle name mappings.
    Returns ONLY vehicle names, stripping out sensitive information (driver names,
    block assignments, shift times) that should not be publicly accessible.

    Returns:
        {
            "fetched_at": <timestamp_ms>,
            "vehicle_drivers": {
                "123": {
                    "vehicle_name": "Bus 123"
                },
                "456": {
                    "vehicle_name": "Bus 456"
                }
            }
        }
    """
    try:
        # Use TTL cache to avoid repeated expensive joins
        full_data = await vehicle_drivers_cache.get(_fetch_vehicle_drivers)

        # Strip out sensitive fields, only keep vehicle_name
        filtered_vehicle_drivers = {}
        for vehicle_id, info in full_data.get("vehicle_drivers", {}).items():
            if isinstance(info, dict) and "vehicle_name" in info:
                filtered_vehicle_drivers[vehicle_id] = {
                    "vehicle_name": info["vehicle_name"]
                }

        return {
            "fetched_at": full_data.get("fetched_at"),
            "vehicle_drivers": filtered_vehicle_drivers
        }
    except Exception as exc:
        print(f"[vehicle_drivers] fetch failed: {exc}")
        detail = {
            "message": "vehicle-driver mapping unavailable",
            "reason": str(exc),
        }
        raise HTTPException(status_code=502, detail=detail) from exc


@app.get("/v1/routes/{route_id}/vehicles_raw")
async def route_vehicles_raw(route_id: int):
    async with state.lock:
        vehs = state.vehicles_by_route.get(route_id, {})
        route = state.routes.get(route_id)
        items = []
        for v in vehs.values():
            seg_idx = getattr(v, "seg_idx", 0)
            road = ""
            cap = DEFAULT_CAP_MPS
            if route:
                if route.seg_names and seg_idx < len(route.seg_names):
                    road = route.seg_names[seg_idx]
                if route.seg_caps_mps and seg_idx < len(route.seg_caps_mps):
                    cap = route.seg_caps_mps[seg_idx]
            items.append({
                "id": v.id,
                "name": v.name,
                "lat": v.lat,
                "lon": v.lon,
                "heading": getattr(v, "heading", 0.0),
                "ground_mps": v.ground_mps,
                "s_pos": getattr(v, "s_pos", 0.0),
                "ema_mps": getattr(v, "ema_mps", 0.0),
                "dir_sign": getattr(v, "dir_sign", 0),
                "age_s": getattr(v, "age_s", 0.0),
                "road": road,
                "speed_limit_mps": cap,
            })
        return {"vehicles": items}

# ---------------------------
# REST: Dispatch helpers
# ---------------------------

async def _fetch_downed_sheet_csv() -> str:
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        resp = await client.get(
            DISPATCHER_DOWNED_SHEET_URL,
            headers={"Cache-Control": "no-cache"},
        )
    record_api_call("GET", DISPATCHER_DOWNED_SHEET_URL, resp.status_code)
    resp.raise_for_status()
    return resp.text


def _clean_sheet_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text.strip()


def _normalize_header_label(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip()).lower()


def _find_status_indices(headers: Sequence[Any]) -> List[int]:
    return [idx for idx, text in enumerate(headers) if _normalize_header_label(text) == "status"]


def _find_delivery_date_index(headers: Sequence[Any]) -> int:
    for idx, text in enumerate(headers):
        normalized = _normalize_header_label(text)
        if not normalized:
            continue
        if (
            normalized == "actual delivery date"
            or normalized == "delivery date"
            or "delivery date" in normalized
        ):
            return idx
    return -1


def _find_down_date_index(headers: Sequence[Any]) -> int:
    for idx, text in enumerate(headers):
        normalized = _normalize_header_label(text)
        if not normalized:
            continue
        if normalized == "date" or normalized == "down date" or "down date" in normalized:
            return idx
    return -1


def _parse_sheet_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("T", " ").replace("Z", "").strip()
    try:
        # datetime.fromisoformat handles many common cases
        parsed = datetime.fromisoformat(normalized)
        return parsed
    except (TypeError, ValueError):
        pass

    date_patterns = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%b %d %Y",
        "%b %d, %Y",
    ]
    datetime_patterns = [
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%m-%d-%Y %H:%M",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%y %H:%M",
        "%m-%d-%y %H:%M:%S",
    ]

    for fmt in datetime_patterns:
        try:
            return datetime.strptime(normalized, fmt)
        except (TypeError, ValueError):
            continue

    for fmt in date_patterns:
        try:
            return datetime.strptime(normalized, fmt)
        except (TypeError, ValueError):
            continue

    first_token = normalized.split()[0]
    if first_token != normalized:
        for fmt in date_patterns:
            try:
                return datetime.strptime(first_token, fmt)
            except (TypeError, ValueError):
                continue

    return None


def _get_preferred_status(values: Sequence[Any], status_indices: Sequence[int]) -> str:
    if not status_indices:
        return ""
    for idx in reversed(status_indices):
        if idx < 0 or idx >= len(values):
            continue
        value = values[idx]
        if value and str(value).strip():
            return str(value)
    for idx in status_indices:
        if idx < 0 or idx >= len(values):
            continue
        value = values[idx]
        if value and str(value).strip():
            return str(value)
    return ""


def _get_display_date(values: Sequence[Any], delivery_idx: int, down_idx: int) -> str:
    if 0 <= delivery_idx < len(values):
        delivery = values[delivery_idx]
        if delivery and str(delivery).strip():
            return str(delivery)
    if 0 <= down_idx < len(values):
        down = values[down_idx]
        if down and str(down).strip():
            return str(down)
    return ""




KIOSK_COLUMN_WHITELIST: Set[str] = {
    "bus",
    "p&t support vehicle",
    "vehicle",
    "status",
    "date",
    "down date",
    "actual delivery date",
    "delivery date",
    "supervisor",
    "notes/description",
    "notes",
    "description",
    "diagnostic date",
    "diag date",
    "mechanic",
    "diagnostic description",
    "current eta",
    "eta",
}


def _parse_downed_sheet_csv(csv_text: Optional[str]) -> Dict[str, Any]:
    if not csv_text:
        return {"headerLine": [], "sections": []}

    reader = csv.reader(io.StringIO(csv_text))
    rows: List[List[str]] = [[_clean_sheet_cell(cell) for cell in row] for row in reader]
    if not rows:
        return {"headerLine": [], "sections": []}

    header_line = rows[0]
    sections: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    SECTION_TITLES = {"Bus", "P&T Support Vehicle"}

    for raw_row in rows[1:]:
        if not any(raw_row):
            continue
        first = raw_row[0]
        if first in SECTION_TITLES:
            current = {
                "title": first,
                "headers": list(raw_row),
                "rows": [],
            }
            sections.append(current)
            continue
        if current is None:
            continue
        current["rows"].append(list(raw_row))

    for section in sections:
        headers = section.get("headers") or []
        status_indices = _find_status_indices(headers)
        delivery_idx = _find_delivery_date_index(headers)
        down_idx = _find_down_date_index(headers)
        processed_rows: List[Dict[str, Any]] = []
        for row in section.get("rows", []):
            values = list(row)
            if len(values) < len(headers):
                values.extend([""] * (len(headers) - len(values)))
            status_text = _get_preferred_status(values, status_indices)
            processed_rows.append(
                {
                    "values": values,
                    "statusText": status_text,
                    "displayDate": _get_display_date(values, delivery_idx, down_idx),
                }
            )
        section["rows"] = processed_rows

    return {"headerLine": header_line, "sections": sections}


def _filter_kiosk_sections(sections: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for section in sections or []:
        headers = list(section.get("headers") or [])
        normalized_headers = [_normalize_header_label(text) for text in headers]
        keep_indices = [
            idx
            for idx, normalized in enumerate(normalized_headers)
            if normalized in KIOSK_COLUMN_WHITELIST
        ]
        if not keep_indices:
            continue

        filtered_rows: List[Dict[str, Any]] = []
        for row in section.get("rows", []):
            if not isinstance(row, dict):
                continue
            values = list(row.get("values") or [])
            filtered_values = [
                values[idx] if idx < len(values) else ""
                for idx in keep_indices
            ]
            filtered_rows.append({**row, "values": filtered_values})

        if not filtered_rows:
            continue

        filtered_section = dict(section)
        filtered_section["headers"] = [headers[idx] for idx in keep_indices]
        filtered_section["rows"] = filtered_rows
        filtered.append(filtered_section)
    return filtered


async def _get_cached_downed_sheet() -> Tuple[str, float, Optional[str]]:
    global _downed_sheet_csv, _downed_sheet_fetched_at, _downed_sheet_last_attempt, _downed_sheet_error

    now = time.time()
    async with _downed_sheet_lock:
        if _downed_sheet_csv is not None and now - _downed_sheet_fetched_at < DISPATCHER_DOWNED_REFRESH_S:
            return _downed_sheet_csv, _downed_sheet_fetched_at, _downed_sheet_error
        if now - _downed_sheet_last_attempt < DISPATCHER_DOWNED_REFRESH_S:
            if _downed_sheet_csv is not None:
                return _downed_sheet_csv, _downed_sheet_fetched_at, _downed_sheet_error
            raise HTTPException(status_code=503, detail="downed bus sheet unavailable")
        _downed_sheet_last_attempt = now

    try:
        csv_text = await _fetch_downed_sheet_csv()
    except Exception as exc:
        err_msg = str(exc)
        print(f"[downed_sheet] fetch failed: {exc}")
        async with _downed_sheet_lock:
            _downed_sheet_error = err_msg
            cached_csv = _downed_sheet_csv
            cached_ts = _downed_sheet_fetched_at
        if cached_csv is None:
            raise HTTPException(status_code=503, detail="downed bus sheet unavailable") from exc
        return cached_csv, cached_ts, err_msg

    fetch_ts = time.time()
    async with _downed_sheet_lock:
        _downed_sheet_csv = csv_text
        _downed_sheet_fetched_at = fetch_ts
        _downed_sheet_error = None
        return csv_text, fetch_ts, None


def _normalize_plain_block_name(text: str) -> Optional[str]:
    normalized = re.sub(r"\s+", " ", text.strip())
    if not normalized:
        return None
    lowered = normalized.lower()
    if lowered.startswith("block "):
        normalized = normalized[6:]
        normalized = re.sub(r"\s+", " ", normalized.strip())
        if not normalized:
            return None
    # If this is purely numeric, zero-pad to match block formatting.
    if normalized.isdigit():
        return str(int(normalized)).zfill(2)
    return normalized


def _extract_block_from_position_name(value: Any) -> Tuple[Optional[str], str]:
    if value is None:
        return None, ""
    text = str(value)

    # Check for OnDemand positions
    text_lower = text.lower().strip()
    if "ondemand" in text_lower:
        # Normalize the OnDemand position name
        if "eb" in text_lower:
            return "OnDemand EB", "any"
        elif "driver" in text_lower:
            return "OnDemand Driver", "any"

    match = W2W_POSITION_RE.search(text)
    if match:
        number = match.group(1)
        period = (match.group(2) or "").strip().lower()
        try:
            block_number = str(int(number)).zfill(2)
        except (TypeError, ValueError):
            return None, ""
        if period not in {"am", "pm"}:
            period = ""
        return block_number, period

    normalized = _normalize_plain_block_name(text)
    if not normalized:
        return None, ""
    return normalized, "any"


def _parse_w2w_time_components(value: Any) -> Optional[Tuple[int, int, int]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered == "noon":
        return 12, 0, 0
    if lowered in {"midnight", "12am"}:
        return 0, 0, 0
    match = W2W_TIME_RE.match(text)
    if not match:
        return None
    try:
        hour = int(match.group(1))
        minute = int(match.group(2) or 0)
        second = int(match.group(3) or 0)
    except (TypeError, ValueError):
        return None
    if minute >= 60 or second >= 60:
        return None
    suffix = match.group(4)
    if suffix:
        suffix = suffix.lower()
        hour = hour % 12
        if suffix == "p":
            hour += 12
    if hour >= 24:
        hour = hour % 24
    return hour, minute, second


def _parse_w2w_datetime(date_str: Any, time_str: Any, tz: ZoneInfo) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        base_date = datetime.strptime(str(date_str).strip(), "%m/%d/%Y")
    except Exception:
        return None
    time_parts = _parse_w2w_time_components(time_str)
    if time_parts is None:
        return None
    hour, minute, second = time_parts
    try:
        return datetime(
            base_date.year,
            base_date.month,
            base_date.day,
            hour,
            minute,
            second,
            tzinfo=tz,
        )
    except ValueError:
        return None


def _format_driver_time(dt: datetime) -> str:
    hour = dt.hour
    minute = dt.minute
    if minute:
        return f"{hour:02d}:{minute:02d}"
    return f"{hour:02d}:00"


def _parse_duration_hours(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        duration = float(text)
    except (TypeError, ValueError):
        return None
    if math.isnan(duration) or math.isinf(duration):
        return None
    if duration <= 0:
        return None
    return duration


def _build_driver_assignments(
    shifts: Iterable[Dict[str, Any]], now: datetime, tz: ZoneInfo
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    assignments: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    fallback_ts = int(now.timestamp() * 1000)
    for shift in shifts:
        if not isinstance(shift, dict):
            continue
        position_name = shift.get("POSITION_NAME")
        block_number, explicit_period = _extract_block_from_position_name(position_name)
        if not block_number:
            continue
        first = str(shift.get("FIRST_NAME") or "").strip()
        last = str(shift.get("LAST_NAME") or "").strip()
        name = (first + " " + last).strip() or "OPEN"
        start_dt = _parse_w2w_datetime(shift.get("START_DATE"), shift.get("START_TIME"), tz)
        if start_dt is None:
            continue
        end_dt = _parse_w2w_datetime(shift.get("END_DATE"), shift.get("END_TIME"), tz)
        if end_dt is None:
            duration_hours = _parse_duration_hours(shift.get("DURATION"))
            if duration_hours:
                end_dt = start_dt + timedelta(hours=duration_hours)
        if end_dt is None:
            continue
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        # Hide shifts that have already concluded so dispatcher only sees
        # current or upcoming assignments even when we query the previous
        # service day overnight.
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
        # Skip shifts with COLOR_ID 9 (driver didn't come in / not going to come in)
        if color_id == "9":
            continue

        # COLOR_ID 7 = Junior/Senior Driving training pair
        is_training = color_id == "7"

        assignment_entry = {
            "name": name,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_label": _format_driver_time(start_dt),
            "end_label": _format_driver_time(end_dt),
            "color_id": color_id,
            "position_name": position_name,  # Store original W2W POSITION_NAME
        }

        # Mark as training shift if COLOR_ID 7
        if is_training:
            assignment_entry["is_training"] = True

        bucket.append(assignment_entry)
    for entry in assignments.values():
        for drivers in entry.values():
            drivers.sort(key=lambda item: item.get("start_ts") or fallback_ts)
    return assignments


def _redact_w2w_error(message: str) -> str:
    if not message:
        return message
    redacted = _W2W_KEY_QUERY_RE.sub(r"\1***", message)
    return _W2W_KEY_ENCODED_RE.sub(r"\1***", redacted)


async def _fetch_w2w_assignments():
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    service_day = now
    if now.time() < dtime(hour=2, minute=30):
        service_day = now - timedelta(days=1)
    if not W2W_KEY:
        return {
            "disabled": True,
            "fetched_at": int(now.timestamp() * 1000),
            "assignments_by_block": {},
        }
    params = {
        "start_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
        "end_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
        "key": W2W_KEY,
    }
    url = httpx.URL(W2W_ASSIGNED_SHIFT_URL)
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, timeout=20)
    log_url = str(httpx.URL(str(url), params={**params, "key": "***"}))
    record_api_call("GET", log_url, response.status_code)
    response.raise_for_status()
    payload = response.json()
    shifts: Iterable[Dict[str, Any]] = []
    if isinstance(payload, dict):
        raw_shifts = payload.get("AssignedShiftList")
        if isinstance(raw_shifts, list):
            shifts = raw_shifts
    assignments = _build_driver_assignments(shifts, now, tz)
    return {
        "fetched_at": int(now.timestamp() * 1000),
        "assignments_by_block": assignments,
    }


@app.get("/v1/dispatch/block-drivers")
async def dispatch_block_drivers(request: Request):
    _require_dispatcher_access(request)
    try:
        return await w2w_assignments_cache.get(_fetch_w2w_assignments)
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[block_drivers] fetch failed: {exc}")
        detail = {
            "message": "driver assignments unavailable",
            "reason": _redact_w2w_error(str(exc)),
        }
        raise HTTPException(status_code=502, detail=detail) from exc


@app.get("/v1/dispatch/blocks")
async def dispatch_blocks(request: Request):
    _require_dispatcher_access(request)
    async with state.lock:
        if state.blocks_cache:
            return state.blocks_cache
    async with httpx.AsyncClient() as client:
        block_groups, block_meta = await fetch_block_groups(client, include_metadata=True)
    async with state.lock:
        color_by_route = {rid: r.color for rid, r in state.routes.items() if r.color}
        route_by_bus: Dict[str, int] = {}
        for rid, vehs in state.vehicles_by_route.items():
            route = state.routes.get(rid)
            if rid in {0, None}:
                continue
            if route and route.name and "out of service" in route.name.lower():
                continue
            for v in vehs.values():
                name = str(v.name).strip()
                if not name:
                    continue
                route_by_bus[name] = rid
        vehicle_roster: List[Dict[str, Any]] = []
        if isinstance(block_meta, dict):
            vehicles_raw = block_meta.get("Vehicles")
            if isinstance(vehicles_raw, list):
                vehicle_roster.extend(vehicles_raw)
            sched_trip = block_meta.get("ScheduleTripVehicles")
            if isinstance(sched_trip, list):
                vehicle_roster.extend(sched_trip)

        plain_language_blocks = _extract_plain_language_blocks(
            block_groups, vehicle_roster=vehicle_roster
        )
        res = {
            "block_groups": block_groups,
            "color_by_route": color_by_route,
            "route_by_bus": route_by_bus,
            "plain_language_blocks": plain_language_blocks,
        }
        state.blocks_cache = res
        state.blocks_cache_ts = time.time()
        return res


# Route name to valid block numbers mapping
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
# For Blue, prefer [15]-[18] over [20]-[26] which are shared with Red
ROUTE_PREFERRED_BLOCKS: Dict[str, Set[str]] = {
    "blue": {"15", "16", "17", "18"},
}


def _get_blocks_for_route(route_name: Optional[str]) -> Optional[Set[str]]:
    """
    Get the set of valid block numbers for a route name.

    Args:
        route_name: Route name like "Orange Line", "Gold Line", etc.

    Returns:
        Set of valid block numbers (zero-padded) for that route, or None if not found.
    """
    if not route_name:
        return None
    route_lower = route_name.lower()
    for key, blocks in ROUTE_TO_BLOCKS.items():
        if key in route_lower:
            return blocks
    return None


def _get_preferred_blocks_for_route(route_name: Optional[str]) -> Optional[Set[str]]:
    """
    Get the set of PREFERRED block numbers for a route name.

    For routes with both dedicated and shared blocks (like Blue which has
    [15]-[18] dedicated and [20]-[26] shared with Red), this returns only
    the dedicated blocks. Used to prioritize dedicated blocks when matching.

    Args:
        route_name: Route name like "Blue Line", etc.

    Returns:
        Set of preferred block numbers, or None if no preference.
    """
    if not route_name:
        return None
    route_lower = route_name.lower()
    for key, blocks in ROUTE_PREFERRED_BLOCKS.items():
        if key in route_lower:
            return blocks
    return None


def _split_interlined_blocks(block_name: str) -> List[str]:
    """
    Split interlined block names from TransLoc into individual block numbers.

    TransLoc uses interlined blocks like "[01]/[04]" while WhenToWork tracks
    individual blocks "01" and "04" separately.

    Examples:
        "[01]/[04]" -> ["01", "04"]
        "[20]/[10]" -> ["20", "10"]
        "[01]" -> ["01"]
        "[16] AM" -> ["16"]
    """
    if not block_name:
        return []

    # Split on "/" to handle interlined blocks
    parts = block_name.split("/")
    result = []

    for part in parts:
        # Extract just the number, removing brackets and AM/PM
        # Match pattern like "[01]" or "[16] AM"
        match = re.search(r'\[(\d{1,2})\]', part)
        if match:
            number = match.group(1)
            # Zero-pad to 2 digits to match W2W format
            normalized = str(int(number)).zfill(2)
            result.append(normalized)

    return result


def _find_current_drivers(
    block_number: str,
    assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
    now_ts: int
) -> List[Dict[str, Any]]:
    """
    Find all drivers currently assigned to a block based on time windows.

    Handles overlapping shifts (e.g., during driver swaps where shifts may overlap
    by 15-30 minutes). Returns all active drivers sorted by shift start time
    (outgoing driver first, incoming driver second).

    Args:
        block_number: Zero-padded block number like "01" or "20"
        assignments_by_block: W2W assignments structure from _build_driver_assignments
        now_ts: Current timestamp in milliseconds

    Returns:
        List of driver assignment dicts, sorted by start_ts (earliest first).
        Returns empty list if no active drivers.
    """
    if block_number not in assignments_by_block:
        return []

    periods_dict = assignments_by_block[block_number]
    matching_drivers = []

    # Check all periods (am, pm, any)
    for period, drivers in periods_dict.items():
        for driver in drivers:
            start_ts = driver.get("start_ts", 0)
            end_ts = driver.get("end_ts", 0)

            # Check if current time is within this driver's shift
            if start_ts <= now_ts < end_ts:
                matching_drivers.append(driver)

    # Sort by start time (outgoing driver first)
    matching_drivers.sort(key=lambda d: d.get("start_ts", 0))

    return matching_drivers


def _find_current_driver(
    block_number: str,
    assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
    now_ts: int
) -> Optional[Dict[str, Any]]:
    """
    Find the driver currently assigned to a block based on time windows.

    DEPRECATED: Use _find_current_drivers() for overlap support.
    This function is maintained for backward compatibility with tests.

    Args:
        block_number: Zero-padded block number like "01" or "20"
        assignments_by_block: W2W assignments structure from _build_driver_assignments
        now_ts: Current timestamp in milliseconds

    Returns:
        Driver assignment dict with name, shift_end, etc., or None if no active driver
    """
    drivers = _find_current_drivers(block_number, assignments_by_block, now_ts)
    return drivers[0] if drivers else None


def _normalize_driver_name(name: str) -> str:
    """
    Normalize driver names for matching between TransLoc and WhenToWork.

    Removes extra whitespace, converts to lowercase for case-insensitive matching.
    """
    if not name:
        return ""
    # Remove extra whitespace and convert to lowercase
    normalized = re.sub(r"\s+", " ", name.strip()).lower()
    return normalized


def _find_ondemand_driver_by_name(
    driver_name: str,
    assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]],
    now_ts: int
) -> Optional[Dict[str, Any]]:
    """
    Find an ondemand driver assignment by matching driver name.

    Args:
        driver_name: Driver name from TransLoc ondemand data
        assignments_by_block: W2W assignments structure
        now_ts: Current timestamp in milliseconds

    Returns:
        Driver assignment dict with name, shift_end, block, etc., or None if no match
    """
    if not driver_name:
        return None

    normalized_search_name = _normalize_driver_name(driver_name)
    if not normalized_search_name:
        return None

    # Check both OnDemand Driver and OnDemand EB blocks
    for block_name in ["OnDemand Driver", "OnDemand EB"]:
        if block_name not in assignments_by_block:
            continue

        periods_dict = assignments_by_block[block_name]
        for period, drivers in periods_dict.items():
            for driver in drivers:
                start_ts = driver.get("start_ts", 0)
                end_ts = driver.get("end_ts", 0)

                # Check if current time is within this driver's shift
                if start_ts <= now_ts < end_ts:
                    w2w_driver_name = driver.get("name", "")
                    normalized_w2w_name = _normalize_driver_name(w2w_driver_name)

                    # Match driver names
                    if normalized_w2w_name == normalized_search_name:
                        # Return driver with block information
                        # Use W2W position_name if available, otherwise use normalized block_name
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


def _build_ondemand_vehicle_entries(
    assignments_by_block: Dict[str, Dict[str, List[Dict[str, Any]]]], now_ts: int
) -> Dict[str, Dict[str, Any]]:
    """Build vehicle entries directly from OnDemand W2W positions."""

    vehicle_entries: Dict[str, Dict[str, Any]] = {}
    for block_name in ["OnDemand Driver", "OnDemand EB"]:
        current_drivers = _find_current_drivers(block_name, assignments_by_block, now_ts)
        if not current_drivers:
            continue

        vehicle_entries[block_name] = {
            "block": block_name,
            "drivers": [
                {
                    "name": driver.get("name"),
                    "shift_start": driver.get("start_ts"),
                    "shift_start_label": driver.get("start_label"),
                    "shift_end": driver.get("end_ts"),
                    "shift_end_label": driver.get("end_label"),
                }
                for driver in current_drivers
            ],
            # Use block name as identifier when no vehicle ID is provided
            "vehicle_id": block_name,
            "vehicle_name": None,
        }

    return vehicle_entries


async def _fetch_vehicle_drivers():
    """
    Build a mapping of vehicle_id -> {block, drivers, vehicle_name}.

    This joins:
    1. TransLoc blocks (vehicle_id -> raw block like "[01]" or "[04]")
    2. W2W assignments (block number -> driver with time windows)
    3. Vehicle names from TransLoc vehicle data
    4. OnDemand vehicles with driver names (matched by name to W2W assignments)

    Each vehicle is mapped to its current block assignment(s) and corresponding driver(s).

    For interlined blocks (e.g., "[05]/[03]" from TransLoc), all blocks are combined
    under a single vehicle_id entry:
    - All blocks are included in the "block" field (e.g., "[05]/[03]")
    - All drivers from all blocks are merged into a single "drivers" array
    - Duplicate drivers (same name and shift times) are deduplicated

    Handles overlapping shifts: When shifts overlap (e.g., during driver swaps), the
    "drivers" array will contain multiple entries sorted by shift start time
    (outgoing driver first, incoming driver second).

    For ondemand vehicles:
    - Driver names from ondemand positions are matched with W2W "OnDemand Driver" or "OnDemand EB" positions
    - Only vehicles where the driver appears in BOTH ondemand and W2W are included
    - Vehicle names are pulled from the "call_name" field in ondemand positions
    """
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    now_ts = int(now.timestamp() * 1000)

    # Fetch both data sources
    # Get block groups with time information
    try:
        async with httpx.AsyncClient() as client:
            block_groups = await fetch_block_groups(client, include_metadata=False)
        blocks_with_times = _build_block_mapping_with_times(block_groups)

        # Diagnostic logging
        print(f"[vehicle_drivers] Found {len(blocks_with_times)} vehicles with block data")
        print(f"[vehicle_drivers] Current time: {now} ({now_ts})")
    except Exception as exc:
        print(f"[vehicle_drivers] blocks fetch failed: {exc}")
        blocks_with_times = {}

    try:
        w2w_data = await w2w_assignments_cache.get(_fetch_w2w_assignments)
        assignments_by_block = w2w_data.get("assignments_by_block", {})
    except Exception as exc:
        print(f"[vehicle_drivers] w2w fetch failed: {exc}")
        assignments_by_block = {}

    vehicle_drivers: Dict[str, Any] = {}

    # Add OnDemand W2W positions directly so they appear even without ondemand vehicle data
    vehicle_drivers.update(_build_ondemand_vehicle_entries(assignments_by_block, now_ts))

    # Select current block for each vehicle, considering both TransLoc block times
    # and W2W driver shift times. A vehicle is included if:
    # 1. TransLoc block time is currently active, OR
    # 2. A W2W driver shift is currently active for that block
    # This allows showing driver/block info before and after revenue service.
    blocks_mapping = {}
    for vehicle_id, block_list in blocks_with_times.items():
        # First try: select block based on TransLoc block times
        selected_block = _select_current_or_next_block(block_list, now_ts)
        if selected_block:
            blocks_mapping[vehicle_id] = selected_block
        else:
            # Second try: check if any W2W driver shift is active for any of the blocks
            # This handles pre-service and post-service periods
            for block_name, start_ts, end_ts in block_list:
                block_numbers = _split_interlined_blocks(block_name)
                for block_number in block_numbers:
                    drivers = _find_current_drivers(block_number, assignments_by_block, now_ts)
                    if drivers:
                        # Found active driver shift - use this block
                        blocks_mapping[vehicle_id] = block_name
                        print(f"[vehicle_drivers] Vehicle {vehicle_id} included via W2W shift (block {block_name})")
                        break
                if vehicle_id in blocks_mapping:
                    break

    print(f"[vehicle_drivers] After filtering: {len(blocks_mapping)} vehicles remain")

    # Build vehicle_id -> vehicle_name and vehicle_id -> route_name mappings from raw vehicle data
    vehicle_names = {}
    vehicle_routes = {}  # vehicle_id -> route_name (e.g., "Orange Line")
    async with state.lock:
        route_id_to_name = dict(state.route_id_to_name) if state.route_id_to_name else {}
        for rec in state.vehicles_raw:
            vid = rec.get("VehicleID") or rec.get("VehicleId")
            if vid is not None:
                vid_str = str(vid)
                vname = rec.get("Name") or rec.get("VehicleName")
                if vname:
                    vehicle_names[vid_str] = vname
                # Capture current route for this vehicle
                route_id = rec.get("RouteID") or rec.get("RouteId")
                if route_id is not None and route_id_to_name:
                    route_name = route_id_to_name.get(route_id) or route_id_to_name.get(str(route_id))
                    if route_name:
                        vehicle_routes[vid_str] = route_name

    # Build the vehicle -> driver mapping (continue adding to existing entries)

    # Process regular buses with block assignments
    for vehicle_id, block_name in blocks_mapping.items():
        # Extract block numbers from raw block name
        # For single blocks: "[01]" -> ["01"]
        # For interlined blocks that couldn't be parsed: "[05]/[03]" -> ["05", "03"]
        # Note: _build_block_mapping_with_times now returns specific block numbers
        # based on trip timing (e.g., "[20]" at 6 AM, "[10]" at 3 PM) rather than
        # the full interlined string "[20]/[10]", so block_name will usually be a
        # single block at this point.
        block_numbers = _split_interlined_blocks(block_name)

        # Get the vehicle name (may be None if not found)
        vehicle_name = vehicle_names.get(vehicle_id)

        # Determine which block to display based on the vehicle's CURRENT route
        # For interlined blocks (e.g., "[22]/[06]"), we need to pick the block that
        # matches the route the vehicle is currently operating on, and ONLY show
        # drivers assigned to that specific block.
        current_route = vehicle_routes.get(vehicle_id)
        valid_blocks_for_route = _get_blocks_for_route(current_route)
        preferred_blocks_for_route = _get_preferred_blocks_for_route(current_route)

        # Collect drivers by block, but we'll filter to only use route-matching blocks
        drivers_by_block = {}  # block_number -> list of active drivers with metadata

        for block_number in block_numbers:
            # Collect drivers currently active for this block
            block_drivers = _find_current_drivers(block_number, assignments_by_block, now_ts)
            if block_drivers:
                drivers_by_block[block_number] = block_drivers

        # Determine which specific block to use for this vehicle
        # Priority:
        # 1. Preferred block that matches current route (e.g., [15]-[18] for Blue)
        # 2. Any block that matches current route (e.g., [20]-[26] for Blue/Red)
        # 3. Cached block assignment (vehicle out of service, driver shift still active)
        # 4. Fallback to most recent driver shift
        selected_block_number = None
        w2w_position_name = None
        used_cache = False

        if drivers_by_block:
            # First: try to find a PREFERRED block that matches the current route
            # This prioritizes dedicated blocks (e.g., [17] for Blue) over shared ones ([23])
            if preferred_blocks_for_route:
                for blk_num in drivers_by_block:
                    if blk_num in preferred_blocks_for_route:
                        selected_block_number = blk_num
                        best_driver = max(drivers_by_block[blk_num], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")
                        break

            # Second: try any block that matches the current route
            if selected_block_number is None and valid_blocks_for_route:
                for blk_num in drivers_by_block:
                    if blk_num in valid_blocks_for_route:
                        selected_block_number = blk_num
                        best_driver = max(drivers_by_block[blk_num], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")
                        break

            # Third: if no route match (out of service), check the cache
            # Use cached block if the driver's shift hasn't ended yet
            if selected_block_number is None:
                cached = state.vehicle_block_cache.get(vehicle_id)
                if cached:
                    cached_block = cached.get("block_number")
                    cached_shift_end = cached.get("shift_end_ts", 0)
                    # Use cache if shift hasn't ended and block has active drivers
                    if cached_shift_end > now_ts and cached_block in drivers_by_block:
                        selected_block_number = cached_block
                        best_driver = max(drivers_by_block[cached_block], key=lambda d: d.get("start_ts", 0))
                        w2w_position_name = best_driver.get("position_name")
                        used_cache = True

            # Third fallback: use the block with the most recent driver shift
            if selected_block_number is None:
                best_start_ts = -1
                for blk_num, blk_drivers in drivers_by_block.items():
                    for drv in blk_drivers:
                        start_ts = drv.get("start_ts", 0)
                        if start_ts > best_start_ts:
                            best_start_ts = start_ts
                            selected_block_number = blk_num
                            w2w_position_name = drv.get("position_name")

        # Collect drivers ONLY from the selected block (not all interlined blocks)
        all_drivers = []
        seen_drivers = set()
        max_shift_end_ts = 0
        if selected_block_number and selected_block_number in drivers_by_block:
            for driver in drivers_by_block[selected_block_number]:
                driver_key = (driver["name"], driver["start_ts"], driver["end_ts"])
                if driver_key not in seen_drivers:
                    seen_drivers.add(driver_key)
                    all_drivers.append({
                        "name": driver["name"],
                        "shift_start": driver["start_ts"],
                        "shift_start_label": driver["start_label"],
                        "shift_end": driver["end_ts"],
                        "shift_end_label": driver["end_label"],
                    })
                    # Track latest shift end for cache expiry
                    if driver["end_ts"] > max_shift_end_ts:
                        max_shift_end_ts = driver["end_ts"]

        # Sort drivers by shift start time (for consistency with overlapping shifts)
        all_drivers.sort(key=lambda d: d["shift_start"])

        # Use W2W position name if found, otherwise fall back to TransLoc block_name
        final_block = w2w_position_name if w2w_position_name else block_name

        # Update cache: store current block assignment for this vehicle
        # This persists the assignment when vehicle goes out of service
        if selected_block_number and max_shift_end_ts > now_ts and not used_cache:
            state.vehicle_block_cache[vehicle_id] = {
                "block_number": selected_block_number,
                "position_name": w2w_position_name,
                "shift_end_ts": max_shift_end_ts,
            }

        # Create entry for this vehicle
        vehicle_drivers[vehicle_id] = {
            "block": final_block,
            "drivers": all_drivers,
            "vehicle_name": vehicle_name,
            "vehicle_id": vehicle_id,
        }

    # Process ondemand vehicles
    # Get ondemand client and fetch vehicle data
    ondemand_client = getattr(app.state, "ondemand_client", None)
    if ondemand_client is not None:
        try:
            ondemand_data = await _collect_ondemand_data(ondemand_client, now=now)
            vehicles_list = ondemand_data.get("vehicles") if isinstance(ondemand_data, dict) else []

            for vehicle_entry in vehicles_list:
                if not isinstance(vehicle_entry, dict):
                    continue

                # Extract vehicle ID
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

                # Extract driver name from ondemand data
                driver_name = vehicle_entry.get("driverName", "")
                if not driver_name:
                    continue

                # Extract vehicle name from call_name field in ondemand positions
                vehicle_name = (
                    vehicle_entry.get("callName")
                    or vehicle_entry.get("call_name")
                )

                # Match driver name with W2W assignments
                matched_driver = _find_ondemand_driver_by_name(
                    driver_name, assignments_by_block, now_ts
                )

                # Only include vehicles where the driver appears in both ondemand and W2W
                if matched_driver:
                    # OnDemand drivers typically don't have overlapping shifts,
                    # but we use the same structure for consistency
                    drivers_list = [{
                        "name": matched_driver["name"],
                        "shift_start": matched_driver["start_ts"],
                        "shift_start_label": matched_driver["start_label"],
                        "shift_end": matched_driver["end_ts"],
                        "shift_end_label": matched_driver["end_label"],
                    }]

                    vehicle_drivers[vehicle_id_str] = {
                        "block": matched_driver["block"],
                        "drivers": drivers_list,
                        "vehicle_name": vehicle_name,
                        "vehicle_id": vehicle_id_str,  # Include vehicle_id for consistency
                    }

        except Exception as exc:
            print(f"[vehicle_drivers] ondemand fetch failed: {exc}")

    return {
        "fetched_at": now_ts,
        "vehicle_drivers": vehicle_drivers,
    }


@app.get("/v1/dispatch/vehicle-drivers")
async def dispatch_vehicle_drivers(request: Request):
    """
    Get the mapping of vehicle IDs to their current block assignments and drivers.

    This endpoint joins TransLoc block assignments with WhenToWork driver assignments.

    For vehicles with multiple blocks (interlined blocks from TransLoc, e.g., "[05]/[03]"),
    all blocks are returned under a single vehicle_id entry:
    - All blocks are included in the "block" field (e.g., "[05]/[03]")
    - All drivers from all blocks are merged into a single "drivers" array
    - Duplicate drivers (same name and shift times) are deduplicated

    Handles overlapping shifts: When driver shifts overlap (e.g., during handoffs),
    the "drivers" array will contain multiple entries sorted by shift start time
    (outgoing driver first, incoming driver second).

    Also includes ondemand vehicles, but ONLY if their driver appears in both the
    ondemand positions AND W2W assignments. OnDemand vehicles use block names
    "OnDemand Driver" or "OnDemand EB" based on their W2W position assignments.
    Vehicle names for ondemand vehicles come from the "call_name" field.

    Returns:
        {
            "fetched_at": <timestamp_ms>,
            "vehicle_drivers": {
                "123": {
                    "block": "[01]",
                    "drivers": [
                        {
                            "name": "John Doe",
                            "shift_start": <timestamp_ms>,
                            "shift_start_label": "6a",
                            "shift_end": <timestamp_ms>,
                            "shift_end_label": "10a"
                        }
                    ],
                    "vehicle_name": "Bus 123",
                    "vehicle_id": "123"
                },
                "456": {
                    "block": "[05]/[03]",
                    "drivers": [
                        {
                            "name": "Driver A",
                            "shift_start": <timestamp_ms>,
                            "shift_start_label": "6a",
                            "shift_end": <timestamp_ms>,
                            "shift_end_label": "10a"
                        }
                    ],
                    "vehicle_name": "Bus 456",
                    "vehicle_id": "456"
                },
                "789": {
                    "block": "[02]",
                    "drivers": [
                        {
                            "name": "Outgoing Driver",
                            "shift_start": <timestamp_ms>,
                            "shift_start_label": "6a",
                            "shift_end": <timestamp_ms>,
                            "shift_end_label": "10a"
                        },
                        {
                            "name": "Incoming Driver",
                            "shift_start": <timestamp_ms>,
                            "shift_start_label": "9:45a",
                            "shift_end": <timestamp_ms>,
                            "shift_end_label": "6p"
                        }
                    ],  // Overlapping shift during driver swap
                    "vehicle_name": "Bus 789",
                    "vehicle_id": "789"
                },
                "999": {
                    "block": "OnDemand Driver",
                    "drivers": [
                        {
                            "name": "Jane Smith",
                            "shift_start": <timestamp_ms>,
                            "shift_start_label": "8a",
                            "shift_end": <timestamp_ms>,
                            "shift_end_label": "5p"
                        }
                    ],
                    "vehicle_name": "OnDemand 1",
                    "vehicle_id": "999"
                }
            }
        }
    """
    _require_dispatcher_access(request)
    try:
        return await _fetch_vehicle_drivers()
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[vehicle_drivers] fetch failed: {exc}")
        detail = {
            "message": "vehicle-driver mapping unavailable",
            "reason": str(exc),
        }
        raise HTTPException(status_code=502, detail=detail) from exc


@app.get("/v1/dispatcher/downed_buses")
async def dispatcher_downed_buses(request: Request):
    _require_dispatcher_access(request)
    csv_text, fetched_at, error = await _get_cached_downed_sheet()
    payload = {
        "csv": csv_text,
        "fetched_at": int(fetched_at * 1000) if fetched_at else None,
    }
    if error:
        payload["error"] = error
    return payload


@app.get("/v1/kiosk/downed_buses")
async def kiosk_downed_buses():
    csv_text, fetched_at, error = await _get_cached_downed_sheet()
    parsed = _parse_downed_sheet_csv(csv_text)
    filtered_sections = _filter_kiosk_sections(parsed.get("sections", []))
    payload: Dict[str, Any] = {
        "headerLine": parsed.get("headerLine", []),
        "sections": filtered_sections,
        "fetched_at": int(fetched_at * 1000) if fetched_at else None,
    }
    if error:
        payload["error"] = error
    return payload


# ---------------------------
# REST: E-ink block layout
# ---------------------------


@app.get("/api/eink-block/layout")
async def get_eink_block_layout(
    layout_id: Optional[str] = Query(None),
    layout_name: Optional[str] = Query(None, alias="name"),
    layout_key: Optional[str] = Query(None, alias="layoutKey"),
    layout_query: Optional[str] = Query(None, alias="layout"),
    include_all: bool = Query(False, alias="all"),
):
    user_specified = False
    for value in (layout_id, layout_name, layout_key, layout_query):
        if value is None:
            continue
        if isinstance(value, str):
            if value.strip():
                user_specified = True
                break
        else:
            user_specified = True
            break
    include_all = bool(include_all or not user_specified)
    requested_id = _determine_layout_identifier(
        None, layout_id, layout_name, layout_key, layout_query
    )
    layout, updated_at, resolved_id, store = load_eink_block_layout(requested_id)
    response: Dict[str, Any] = {
        "layout": layout,
        "updated_at": updated_at,
        "layout_id": resolved_id,
        "available_layouts": sorted(store.keys()),
    }
    if include_all:
        response["layouts"] = {
            key: value["layout"] for key, value in store.items()
        }
        response["layouts_updated_at"] = {
            key: value.get("updated_at") for key, value in store.items()
        }
    return response


@app.post("/api/eink-block/layout")
async def update_eink_block_layout(
    payload: Any = Body(...),
    layout_id: Optional[str] = Query(None),
    layout_name: Optional[str] = Query(None, alias="name"),
    layout_key: Optional[str] = Query(None, alias="layoutKey"),
    layout_query: Optional[str] = Query(None, alias="layout"),
):
    machine_info = _current_machine_info()
    headers = _provenance_headers(machine_info)
    layout_payload = payload.get("layout") if isinstance(payload, dict) else payload
    if layout_payload is None:
        body = _base_response_fields(False, False, machine_info)
        body["error"] = "layout is required"
        return JSONResponse(body, status_code=400, headers=headers)
    try:
        identifier = _determine_layout_identifier(
            payload, layout_id, layout_name, layout_key, layout_query
        )
        layout, updated_at, resolved_id, store, _commit_info_unused = save_eink_block_layout(
            layout_payload, identifier, machine_info
        )
    except ValueError as exc:
        body = _base_response_fields(False, False, machine_info)
        body["error"] = str(exc)
        return JSONResponse(body, status_code=400, headers=headers)
    except Exception as exc:
        print(f"[eink_layout] error saving layout: {exc}")
        body = _base_response_fields(False, False, machine_info)
        body["error"] = "failed to save layout"
        return JSONResponse(body, status_code=500, headers=headers)
    response_body = _base_response_fields(True, True, machine_info)
    response_body.update(
        {
            "layout": layout,
            "updated_at": updated_at,
            "layout_id": resolved_id,
            "available_layouts": sorted(store.keys()),
        }
    )
    return JSONResponse(response_body, headers=headers)


@app.delete("/api/eink-block/layout")
async def delete_eink_block_layout_endpoint(
    layout_id: Optional[str] = Query(None),
    layout_name: Optional[str] = Query(None, alias="name"),
    layout_key: Optional[str] = Query(None, alias="layoutKey"),
    layout_query: Optional[str] = Query(None, alias="layout"),
):
    machine_info = _current_machine_info()
    headers = _provenance_headers(machine_info)
    try:
        identifier = _determine_layout_identifier(
            None, layout_id, layout_name, layout_key, layout_query
        )
        deleted, store, _commit_info_unused = remove_eink_block_layout(identifier)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        print(f"[eink_layout] error deleting layout: {exc}")
        raise HTTPException(status_code=500, detail="failed to delete layout") from exc
    response = {
        "ok": True,
        "deleted": deleted,
        "available_layouts": sorted(store.keys()),
    }
    return JSONResponse(response, headers=headers)


def _normalize_hex_color(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if not text.startswith("#") and len(text) in {3, 6}:
        return f"#{text}"
    return text if text.startswith("#") else f"#{text}"


def _trim_transloc_route(raw: Dict[str, Any]) -> Dict[str, Any]:
    rid = raw.get("RouteID") or raw.get("RouteId")
    color = (
        raw.get("MapLineColor")
        or raw.get("RouteColor")
        or raw.get("Color")
    )
    trimmed: Dict[str, Any] = {
        "RouteID": rid,
        "Description": raw.get("Description"),
        "InfoText": raw.get("InfoText"),
        "MapLineColor": _normalize_hex_color(color),
        "EncodedPolyline": raw.get("EncodedPolyline") or raw.get("Polyline"),
        "IsVisibleOnMap": raw.get("IsVisibleOnMap"),
    }
    stops: List[Dict[str, Any]] = []
    for stop in raw.get("Stops") or []:
        route_stop_id = stop.get("RouteStopID") or stop.get("RouteStopId")
        stop_id = stop.get("StopID") or stop.get("StopId")
        name = (
            stop.get("StopName")
            or stop.get("Name")
            or stop.get("Description")
            or "Stop"
        )
        stops.append(
            {
                "RouteStopID": route_stop_id,
                "StopID": stop_id,
                "Name": name,
                "Description": stop.get("Description") or name,
                "Latitude": stop.get("Latitude") or stop.get("Lat"),
                "Longitude": stop.get("Longitude") or stop.get("Lon") or stop.get("Lng"),
                "AddressID": stop.get("AddressID") or stop.get("AddressId"),
                "RouteIds": [rid] if rid is not None else [],
            }
        )
    trimmed["Stops"] = stops
    return trimmed


def _coerce_route_id(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return text
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return value


def _merge_transloc_route_metadata(
    primary: List[Dict[str, Any]],
    supplemental_raw: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    route_by_id: Dict[Any, Dict[str, Any]] = {}
    for entry in primary:
        if not isinstance(entry, dict):
            continue
        rid = _coerce_route_id(entry.get("RouteID") or entry.get("RouteId"))
        if rid is None:
            continue
        entry["RouteID"] = rid
        route_by_id[rid] = entry

    for raw in supplemental_raw or []:
        if not isinstance(raw, dict):
            continue
        rid = _coerce_route_id(raw.get("RouteID") or raw.get("RouteId"))
        if rid is None:
            continue
        trimmed = _trim_transloc_route(raw)
        trimmed["RouteID"] = rid
        existing = route_by_id.get(rid)
        if existing:
            for key in ["Description", "InfoText", "RouteName", "LongName", "ShortName"]:
                if not existing.get(key) and trimmed.get(key):
                    existing[key] = trimmed[key]
            if trimmed.get("MapLineColor") and not existing.get("MapLineColor"):
                existing["MapLineColor"] = trimmed["MapLineColor"]
            if "IsVisibleOnMap" in trimmed and existing.get("IsVisibleOnMap") is None:
                existing["IsVisibleOnMap"] = trimmed.get("IsVisibleOnMap")
            if trimmed.get("Stops") and not existing.get("Stops"):
                existing["Stops"] = trimmed["Stops"]
        else:
            route_by_id[rid] = trimmed

    return list(route_by_id.values())


def _route_membership_from_routes(routes: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    membership: Dict[str, Set[str]] = defaultdict(set)
    for route in routes:
        rid = route.get("RouteID") or route.get("RouteId")
        if rid is None:
            continue
        try:
            rid_norm = str(int(rid))
        except Exception:
            rid_norm = str(rid)
        for stop in route.get("Stops", []) or []:
            sid = stop.get("StopID") or stop.get("StopId") or stop.get("RouteStopID") or stop.get("RouteStopId")
            if sid is None:
                continue
            membership[str(sid)].add(rid_norm)
    return membership


def _normalize_transloc_stop(stop: Dict[str, Any], *, route_membership: Mapping[str, Set[str]], fallback_route_id: Any = None) -> Optional[Dict[str, Any]]:
    stop_id = stop.get("StopID") or stop.get("StopId") or stop.get("RouteStopID") or stop.get("RouteStopId")
    if stop_id is None:
        return None
    name = (
        stop.get("StopName")
        or stop.get("Name")
        or stop.get("Description")
        or "Stop"
    )
    route_ids: Set[str] = set()
    for key in ("RouteIds", "RouteIDs"):
        vals = stop.get(key)
        if isinstance(vals, list):
            for val in vals:
                if val is None:
                    continue
                try:
                    route_ids.add(str(int(val)))
                except Exception:
                    route_ids.add(str(val))
    if isinstance(stop.get("Routes"), list):
        for entry in stop["Routes"]:
            if not isinstance(entry, dict):
                continue
            rid_val = entry.get("RouteID") or entry.get("RouteId")
            if rid_val is None:
                continue
            try:
                route_ids.add(str(int(rid_val)))
            except Exception:
                route_ids.add(str(rid_val))
    direct_route = stop.get("RouteID") or stop.get("RouteId") or fallback_route_id
    if direct_route is not None:
        try:
            route_ids.add(str(int(direct_route)))
        except Exception:
            route_ids.add(str(direct_route))
    for rid in route_membership.get(str(stop_id), set()):
        route_ids.add(rid)

    normalized = {
        "RouteStopID": stop.get("RouteStopID") or stop.get("RouteStopId"),
        "StopID": stop_id,
        "Name": name,
        "Description": stop.get("Description") or name,
        "Latitude": stop.get("Latitude") or stop.get("Lat"),
        "Longitude": stop.get("Longitude") or stop.get("Lon") or stop.get("Lng"),
        "AddressID": stop.get("AddressID") or stop.get("AddressId"),
    }

    if route_ids:
        normalized["RouteIds"] = sorted(route_ids)
    return normalized


def _merge_stop_entry(target: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in incoming.items():
        if value is None:
            continue
        if key == "RouteIds":
            current = target.setdefault(key, [])
            if isinstance(current, list):
                for item in value:
                    if item not in current:
                        current.append(item)
            continue
        if target.get(key) is None:
            target[key] = value
    return target


def _build_transloc_stops(
    routes: List[Dict[str, Any]],
    extra_stops: Optional[Iterable[Dict[str, Any]]] = None,
    *,
    approach_sets_config: Optional[Mapping[str, List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    route_membership = _route_membership_from_routes(routes)
    merged: Dict[str, Dict[str, Any]] = {}

    def _ingest(stop: Optional[Dict[str, Any]], fallback_route_id: Any = None) -> None:
        if not stop:
            return
        normalized = _normalize_transloc_stop(
            stop, route_membership=route_membership, fallback_route_id=fallback_route_id
        )
        if not normalized:
            return
        stop_id = normalized.get("StopID") or normalized.get("StopId")
        if stop_id is None:
            return
        # Apply approach sets (bubbles)
        if approach_sets_config:
            sets = approach_sets_config.get(str(stop_id))
            if sets:
                normalized["ApproachSets"] = sets
        existing = merged.get(str(stop_id))
        if existing:
            merged[str(stop_id)] = _merge_stop_entry(existing, normalized)
        else:
            merged[str(stop_id)] = dict(normalized)

    for stop in extra_stops or []:
        _ingest(stop)

    for route in routes:
        rid = route.get("RouteID")
        for stop in route.get("Stops", []) or []:
            _ingest(stop, fallback_route_id=rid)

    return list(merged.values())


def _apply_stop_approach_to_stops(
    stops: Iterable[Dict[str, Any]],
    approach_sets_config: Optional[Mapping[str, List[Dict[str, Any]]]] = None,
) -> None:
    """Apply approach bubble sets to stop data."""
    if not approach_sets_config:
        return
    for stop in stops:
        stop_id = stop.get("StopID") or stop.get("StopId")
        if stop_id is None:
            continue
        sets = approach_sets_config.get(str(stop_id))
        if sets:
            stop["ApproachSets"] = sets


def _serialize_stop_approach_config(
    approach_sets_config: Optional[Mapping[str, List[Dict[str, Any]]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Serialize approach sets config to JSON format for saving."""
    serialized: Dict[str, Dict[str, Any]] = {}
    if approach_sets_config:
        for stop_id, sets in approach_sets_config.items():
            serialized[str(stop_id)] = {
                "approach_sets": sets,
            }
    return serialized


def _trim_arrivals_payload(data: Any) -> List[Dict[str, Any]]:
    items = data if isinstance(data, list) else data.get("d") or data.get("Arrivals") or []
    trimmed: List[Dict[str, Any]] = []
    for entry in items:
        route_stop_id = entry.get("RouteStopId") or entry.get("RouteStopID")
        route_id = entry.get("RouteId") or entry.get("RouteID")
        times = []
        for t in entry.get("Times") or []:
            seconds = t.get("Seconds")
            if seconds is None:
                continue
            times.append({"Seconds": seconds})
        trimmed.append(
            {
                "RouteStopId": route_stop_id,
                "RouteId": route_id,
                "RouteDescription": entry.get("RouteDescription") or entry.get("RouteName"),
                "Times": times,
            }
        )
    return trimmed


async def _fetch_transloc_arrivals_for_base(
    base_url: Optional[str], *, client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    resolved_client = _get_transloc_client(client)
    url = build_transloc_url(base_url, f"GetStopArrivalTimes?APIKey={TRANSLOC_KEY}")
    resp = await resolved_client.get(url)
    record_api_call("GET", url, resp.status_code)
    resp.raise_for_status()
    data = resp.json()
    return _trim_arrivals_payload(data)


async def _get_transloc_stops(
    base_url: Optional[str] = None, *, client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    if is_default_transloc_base(base_url):
        async with state.lock:
            cached = getattr(state, "stops_raw", None)
            if cached:
                return list(cached)
    resolved_client = _get_transloc_client(client)
    try:
        return await fetch_stops(resolved_client, base_url=base_url)
    except Exception as exc:
        print(f"[_get_transloc_stops] fetch error: {exc}")
        return []


async def _get_transloc_arrivals(base_url: Optional[str] = None) -> List[Dict[str, Any]]:
    if is_default_transloc_base(base_url):
        async def fetch():
            return await _fetch_transloc_arrivals_for_base(DEFAULT_TRANSLOC_BASE)

        return await transloc_arrivals_cache.get(fetch)

    return await _fetch_transloc_arrivals_for_base(base_url)


def _build_block_mapping(block_groups: List[Dict[str, Any]]) -> Dict[str, str]:
    alias = {
        "[01]": "[01]/[04]",
        "[03]": "[05]/[03]",
        "[04]": "[01]/[04]",
        "[05]": "[05]/[03]",
        "[06]": "[22]/[06]",
        "[10]": "[20]/[10]",
        "[15]": "[26]/[15]",
        "[16] AM": "[21]/[16] AM",
        "[17]": "[23]/[17]",
        "[18] AM": "[24]/[18] AM",
        "[20] AM": "[20]/[10]",
        "[21] AM": "[21]/[16] AM",
        "[22] AM": "[22]/[06]",
        "[23]": "[23]/[17]",
        "[24] AM": "[24]/[18] AM",
        "[26] AM": "[26]/[15]",
    }
    mapping: Dict[str, str] = {}
    for group in block_groups or []:
        raw_block = str(group.get("BlockGroupId") or "").strip()
        if not raw_block:
            continue
        block_name = alias.get(raw_block, raw_block)
        vehicle_ids: List[Any] = []
        vehicle_ids.append(group.get("VehicleId") or group.get("VehicleID"))
        for block in group.get("Blocks") or []:
            for trip in block.get("Trips") or []:
                vehicle_ids.append(trip.get("VehicleID") or trip.get("VehicleId"))
        for vid in vehicle_ids:
            if vid is None:
                continue
            mapping[str(vid)] = block_name
    return mapping


def _build_block_mapping_raw(block_groups: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build a vehicle-to-block mapping using raw block IDs without interlining.

    This version does not apply the alias dictionary that combines blocks like
    "[01]" and "[04]" into "[01]/[04]". Instead, it returns the raw BlockGroupId
    as provided by TransLoc, matching how WhenToWork tracks individual blocks.
    """
    mapping: Dict[str, str] = {}
    for group in block_groups or []:
        raw_block = str(group.get("BlockGroupId") or "").strip()
        if not raw_block:
            continue
        # Use raw_block directly without aliasing
        vehicle_ids: List[Any] = []
        vehicle_ids.append(group.get("VehicleId") or group.get("VehicleID"))
        for block in group.get("Blocks") or []:
            for trip in block.get("Trips") or []:
                vehicle_ids.append(trip.get("VehicleID") or trip.get("VehicleId"))
        for vid in vehicle_ids:
            if vid is None:
                continue
            mapping[str(vid)] = raw_block
    return mapping


def _parse_dotnet_date(date_str: str) -> Optional[int]:
    """
    Parse .NET JSON date format like '/Date(1757019600000)/' to milliseconds timestamp.

    Args:
        date_str: .NET JSON date string

    Returns:
        Timestamp in milliseconds, or None if parsing fails
    """
    if not date_str:
        return None
    match = re.search(r'/Date\((\d+)\)/', date_str)
    if match:
        try:
            return int(match.group(1))
        except (ValueError, IndexError):
            return None
    return None


def _parse_block_time_today(time_str: str, date: datetime) -> Optional[int]:
    """
    Parse a time-of-day string like "03:00 PM" and combine with a date to create a timestamp.

    Args:
        time_str: Time string in format "HH:MM AM/PM" (e.g., "03:00 PM", "06:05 PM")
        date: Date to combine with the time (should be timezone-aware)

    Returns:
        Timestamp in milliseconds, or None if parsing fails
    """
    if not time_str or not date:
        return None

    try:
        # Parse the time string (e.g., "03:00 PM")
        time_obj = datetime.strptime(time_str.strip(), "%I:%M %p").time()

        # Combine with the date (preserving timezone)
        combined = datetime.combine(date.date(), time_obj, tzinfo=date.tzinfo)

        # Convert to milliseconds
        return int(combined.timestamp() * 1000)
    except (ValueError, AttributeError) as e:
        return None


def _extract_block_numbers_from_group_id(block_group_id: str) -> List[str]:
    """
    Extract all block numbers from a BlockGroupId.

    Examples:
        "[20]/[10]" -> ["20", "10"]
        "[21]/[16] AM" -> ["21", "16"]
        "[16] PM" -> ["16"]

    Returns:
        List of block numbers as strings, zero-padded to 2 digits, in order of appearance
    """
    if not block_group_id:
        return []

    # Find all block numbers in brackets
    matches = re.findall(r'\[(\d{1,2})\]', block_group_id)

    # Zero-pad to 2 digits
    return [str(int(num)).zfill(2) for num in matches]


def _infer_block_number_from_route(route_info: Dict[str, Any], block_group_id: str) -> Optional[str]:
    """
    Infer the specific block number from route information within an interlined block.

    DEPRECATED: This function uses hardcoded route-to-block mappings which are unreliable.
    The new approach in _build_block_mapping_with_times uses chronological ordering instead.

    For interlined blocks like "[20]/[10]", different trips correspond to different
    block numbers based on the route they operate on.

    Args:
        route_info: Route information from Block or Trip (contains Description, RouteName, etc.)
        block_group_id: The BlockGroupId this route belongs to (e.g., "[20]/[10]")

    Returns:
        Specific block number (e.g., "20" or "10"), or None if cannot be determined
    """
    # Extract route name/description
    route_name = route_info.get("RouteName") or route_info.get("Description") or ""
    route_name_lower = route_name.lower()

    # Map route name patterns to block numbers
    # NOTE: These mappings are fragile and route names can appear in multiple block groups
    # with different block numbers. Use chronological ordering for interlined blocks instead.
    if "red line" in route_name_lower:
        return "20"
    elif "gold line" in route_name_lower or "yellow line" in route_name_lower:
        return "10"
    elif "blue line" in route_name_lower:
        return "16"
    elif "orange line" in route_name_lower:
        return "22"
    elif "green line" in route_name_lower:
        return "18"
    elif "purple line" in route_name_lower:
        return "06"
    elif "silver line" in route_name_lower:
        return "26"
    elif "pink line" in route_name_lower:
        return "21"
    elif "brown line" in route_name_lower:
        return "24"
    elif "teal line" in route_name_lower or "turquoise line" in route_name_lower:
        return "23"

    # If we can't infer from route name, try to extract from block_group_id
    # For single blocks like "[20] PM", return "20"
    # For interlined blocks, we can't determine without route info
    if "/" not in block_group_id:
        # Single block like "[20] PM" or "[16] AM"
        match = re.search(r'\[(\d+)\]', block_group_id)
        if match:
            return match.group(1).zfill(2)

    return None


def _build_block_mapping_with_times(
    block_groups: List[Dict[str, Any]],
    reference_date: Optional[datetime] = None
) -> Dict[str, List[Tuple[str, Optional[int], Optional[int]]]]:
    """
    Build a vehicle-to-block mapping with time windows at the trip level.

    For interlined blocks (e.g., "[20]/[10]"), this function parses individual trips
    to determine which specific block number is active during each time window.
    This prevents incorrectly associating drivers from one block with vehicles
    operating on a different block within the same interlined group.

    Returns a mapping of vehicle_id -> List[(block_name, start_ts_ms, end_ts_ms)]
    where each vehicle can have multiple blocks at different times of day, and
    for interlined blocks, each time window is associated with the specific
    active block number.

    Time windows are extracted from trip-level BlockStartTime and BlockEndTime,
    combined with today's date in America/New_York timezone.

    Args:
        block_groups: List of block group data from GetDispatchBlockGroupData
        reference_date: Date to use for time-of-day parsing (defaults to today in America/New_York)
    """
    # Default to today in America/New_York timezone
    if reference_date is None:
        tz = ZoneInfo("America/New_York")
        reference_date = datetime.now(tz)

    # mapping: vehicle_id -> List[(specific_block_number, start_ts_ms, end_ts_ms)]
    mapping: Dict[str, List[Tuple[str, Optional[int], Optional[int]]]] = {}

    for group in block_groups or []:
        block_group_id = str(group.get("BlockGroupId") or "").strip()
        if not block_group_id:
            continue

        # Determine if this is an interlined block (contains "/")
        is_interlined = "/" in block_group_id

        # Collect all vehicle IDs for this block group
        group_vehicle_ids: Set[str] = set()
        vid = group.get("VehicleId") or group.get("VehicleID")
        if vid is not None and vid != 0:
            group_vehicle_ids.add(str(vid))

        blocks = group.get("Blocks") or []

        # For interlined blocks, parse each Block separately to determine specific block numbers
        # For non-interlined blocks, use the block_group_id directly
        if is_interlined:
            # Extract block numbers from BlockGroupId in order
            available_block_numbers = _extract_block_numbers_from_group_id(block_group_id)

            # Collect all vehicle IDs for this block group
            vehicle_ids_for_group: Set[str] = set(group_vehicle_ids)
            for block in blocks:
                trips = block.get("Trips") or []
                for trip in trips:
                    trip_vid = trip.get("VehicleID") or trip.get("VehicleId")
                    if trip_vid is not None and trip_vid != 0:
                        vehicle_ids_for_group.add(str(trip_vid))

            # Skip if no vehicles assigned
            if not vehicle_ids_for_group:
                continue

            # Collect all blocks with their timing info and route names
            blocks_with_timing = []
            for block in blocks:
                block_start_str = block.get("BlockStartTime")
                block_end_str = block.get("BlockEndTime")
                start_ts: Optional[int] = None
                end_ts: Optional[int] = None

                if block_start_str:
                    start_ts = _parse_block_time_today(block_start_str, reference_date)
                if block_end_str:
                    end_ts = _parse_block_time_today(block_end_str, reference_date)

                # Get route info for diagnostic logging
                route_info = block.get("Route") or {}
                route_name = route_info.get("RouteName") or route_info.get("Description") or "Unknown"

                blocks_with_timing.append({
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "start_str": block_start_str,
                    "end_str": block_end_str,
                    "route_name": route_name
                })

            # Sort blocks by start time (chronological order)
            # Blocks without start times go to the end
            blocks_with_timing.sort(key=lambda b: b["start_ts"] if b["start_ts"] is not None else float('inf'))

            # Assign block numbers based on ROUTE, not chronological order.
            # For example, [05]/[03] has:
            #   - Orange Line/Orange Loop blocks → should get [05]
            #   - Night Pilot blocks → should get [03]
            # Using chronological order fails when there are more Block entries than
            # block numbers (e.g., 3 blocks but only 2 numbers in BlockGroupId).
            use_full_group_id = (len(blocks_with_timing) == 1 and len(available_block_numbers) > 1)

            for idx, block_data in enumerate(blocks_with_timing):
                specific_block_num = None
                block_to_use = block_group_id  # Default fallback

                if use_full_group_id:
                    # Can't determine specific block when there's only one Block but multiple numbers
                    pass
                elif block_data["start_ts"] is not None:
                    # Try to match block number based on route name
                    # Priority: preferred blocks first (e.g., [15]-[18] for Blue), then any valid block
                    route_name = block_data.get("route_name", "")
                    preferred_blocks = _get_preferred_blocks_for_route(route_name)
                    valid_blocks_for_route = _get_blocks_for_route(route_name)

                    # First try preferred blocks (dedicated blocks for the route)
                    if preferred_blocks:
                        for blk_num in available_block_numbers:
                            if blk_num in preferred_blocks:
                                specific_block_num = blk_num
                                block_to_use = f"[{specific_block_num}]"
                                break

                    # Then try any valid block for the route
                    if specific_block_num is None and valid_blocks_for_route:
                        for blk_num in available_block_numbers:
                            if blk_num in valid_blocks_for_route:
                                specific_block_num = blk_num
                                block_to_use = f"[{specific_block_num}]"
                                break

                    # Fallback to chronological if route matching fails
                    if specific_block_num is None and idx < len(available_block_numbers):
                        specific_block_num = available_block_numbers[idx]
                        block_to_use = f"[{specific_block_num}]"

                start_ts = block_data["start_ts"]
                end_ts = block_data["end_ts"]
                route_name = block_data["route_name"]

                # Diagnostic logging for first few interlined blocks
                if specific_block_num and len(mapping) < 3 and vehicle_ids_for_group:
                    tz = ZoneInfo("America/New_York")
                    if start_ts and end_ts:
                        start_dt = datetime.fromtimestamp(start_ts / 1000, tz)
                        end_dt = datetime.fromtimestamp(end_ts / 1000, tz)
                        print(f"[vehicle_drivers] Interlined {block_group_id} -> Block [{specific_block_num}] ({route_name}) for vehicle {list(vehicle_ids_for_group)[0]}: {start_dt} - {end_dt} (chronological order)")

                # Add this specific block to all associated vehicles
                for vid_str in vehicle_ids_for_group:
                    if vid_str not in mapping:
                        mapping[vid_str] = []
                    mapping[vid_str].append((block_to_use, start_ts, end_ts))

        else:
            # Non-interlined block: use block_group_id directly (existing logic)
            # Collect all vehicle IDs
            vehicle_ids: Set[str] = set(group_vehicle_ids)

            for block in blocks:
                for trip in block.get("Trips") or []:
                    trip_vid = trip.get("VehicleID") or trip.get("VehicleId")
                    if trip_vid is not None and trip_vid != 0:
                        vehicle_ids.add(str(trip_vid))

            # Skip if no vehicles assigned
            if not vehicle_ids:
                continue

            # Extract time window from Block-level fields
            # Process ALL blocks to find earliest start and latest end
            start_ts: Optional[int] = None
            end_ts: Optional[int] = None
            start_time_str: Optional[str] = None
            end_time_str: Optional[str] = None

            for block in blocks:
                block_start_str = block.get("BlockStartTime")
                if block_start_str:
                    block_start_ts = _parse_block_time_today(block_start_str, reference_date)
                    if block_start_ts is not None:
                        if start_ts is None or block_start_ts < start_ts:
                            start_ts = block_start_ts
                            start_time_str = block_start_str

                block_end_str = block.get("BlockEndTime")
                if block_end_str:
                    block_end_ts = _parse_block_time_today(block_end_str, reference_date)
                    if block_end_ts is not None:
                        if end_ts is None or block_end_ts > end_ts:
                            end_ts = block_end_ts
                            end_time_str = block_end_str

            # Diagnostic logging
            if len(mapping) < 3 and vehicle_ids:
                tz = ZoneInfo("America/New_York")
                if start_ts and end_ts:
                    start_dt = datetime.fromtimestamp(start_ts / 1000, tz)
                    end_dt = datetime.fromtimestamp(end_ts / 1000, tz)
                    print(f"[vehicle_drivers] Block {block_group_id} for vehicle {list(vehicle_ids)[0]}: {start_time_str} -> {start_dt} | {end_time_str} -> {end_dt}")
                else:
                    print(f"[vehicle_drivers] Block {block_group_id} for vehicle {list(vehicle_ids)[0]}: No time info (start={start_time_str}, end={end_time_str})")

            # Add this block to all associated vehicles
            for vid_str in vehicle_ids:
                if vid_str not in mapping:
                    mapping[vid_str] = []
                mapping[vid_str].append((block_group_id, start_ts, end_ts))

    return mapping


def _select_current_or_next_block(
    blocks_with_times: List[Tuple[str, Optional[int], Optional[int]]],
    now_ts: int
) -> Optional[str]:
    """
    Select the currently active block based on time windows.
    Only returns blocks where start_ts <= now_ts < end_ts.
    Does not return future or past blocks.

    This ensures that vehicles only show blocks and drivers when they are
    actually active, preventing duplicate block assignments when a vehicle
    will run the same block later in the day.

    For blocks without time information (start_ts or end_ts is None),
    falls back to returning the first block to maintain backwards compatibility.

    Args:
        blocks_with_times: List of (block_name, start_ts_ms, end_ts_ms) tuples
        now_ts: Current timestamp in milliseconds

    Returns:
        Block name if currently active, or first block without time info, or None
    """
    if not blocks_with_times:
        return None

    current_blocks = []
    blocks_without_time_info = []

    for block_name, start_ts, end_ts in blocks_with_times:
        # Track blocks without time information separately
        if start_ts is None or end_ts is None:
            blocks_without_time_info.append(block_name)
            continue

        # Check if block is currently active
        if start_ts <= now_ts < end_ts:
            current_blocks.append((block_name, start_ts, end_ts))

    # Return current block if found
    if current_blocks:
        # If multiple current blocks (shouldn't happen), return the one that started first
        current_blocks.sort(key=lambda x: x[1])  # Sort by start_ts
        return current_blocks[0][0]

    # Fall back to first block without time info (maintains backwards compatibility)
    if blocks_without_time_info:
        return blocks_without_time_info[0]

    # No currently active block
    return None


async def _fetch_transloc_blocks_for_base(base_url: Optional[str]) -> Dict[str, str]:
    client = _get_transloc_client(None)
    block_groups = await fetch_block_groups(client, base_url=base_url)
    return _build_block_mapping(block_groups)


async def _fetch_transloc_blocks_raw_for_base(base_url: Optional[str]) -> Dict[str, str]:
    """Fetch TransLoc blocks without interlining, matching WhenToWork's block structure."""
    client = _get_transloc_client(None)
    block_groups = await fetch_block_groups(client, base_url=base_url)
    return _build_block_mapping_raw(block_groups)


async def _get_transloc_blocks(base_url: Optional[str] = None) -> Dict[str, str]:
    if is_default_transloc_base(base_url):
        async def fetch():
            return await _fetch_transloc_blocks_for_base(DEFAULT_TRANSLOC_BASE)

        return await transloc_blocks_cache.get(fetch)

    return await _fetch_transloc_blocks_for_base(base_url)


async def _get_transloc_blocks_raw(base_url: Optional[str] = None) -> Dict[str, str]:
    """Get TransLoc blocks without interlining, matching WhenToWork's block structure."""
    if is_default_transloc_base(base_url):
        async def fetch():
            return await _fetch_transloc_blocks_raw_for_base(DEFAULT_TRANSLOC_BASE)

        return await transloc_blocks_cache.get(fetch)

    return await _fetch_transloc_blocks_raw_for_base(base_url)


def _describe_transloc_source(base_url: Optional[str]) -> str:
    if is_default_transloc_base(base_url):
        return "default TransLoc feed"
    sanitized = sanitize_transloc_base(base_url)
    if sanitized:
        return sanitized
    if base_url:
        return base_url
    return "default TransLoc feed"


def _transloc_error_detail(exc: httpx.HTTPError, base_url: Optional[str]) -> str:
    source = _describe_transloc_source(base_url)
    prefix = f"TransLoc request failed for {source}"
    if isinstance(exc, httpx.TimeoutException):
        return f"{prefix}: request timed out"
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        reason = exc.response.reason_phrase or ""
        if reason:
            return f"{prefix}: HTTP {status} {reason}"
        return f"{prefix}: HTTP {status}"
    if isinstance(exc, httpx.RequestError):
        return f"{prefix}: {exc.__class__.__name__}: {exc}"
    return f"{prefix}: {exc}"


def _get_transloc_client(client: Optional[httpx.AsyncClient] = None) -> httpx.AsyncClient:
    if client is not None:
        return client
    stored = getattr(app.state, "transloc_client", None)
    if stored is None:
        raise RuntimeError("TransLoc client not initialized")
    return stored


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * pct
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return ordered[int(idx)]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (idx - lower)


def _compute_percentiles(values: Sequence[float]) -> Tuple[Optional[float], Optional[float]]:
    return _percentile(values, 0.5), _percentile(values, 0.95)


def _format_percentile(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 1000:.1f} ms"


def _record_transloc_timing(metric: str, duration_s: float) -> None:
    history: Dict[str, deque[float]] = getattr(app.state, "transloc_timing_history", {})
    if not history:
        history = app.state.transloc_timing_history = {}
    series = history.setdefault(metric, deque(maxlen=200))
    before_p50, before_p95 = _compute_percentiles(series)
    series.append(duration_s)
    after_p50, after_p95 = _compute_percentiles(series)
    before_text = f"{_format_percentile(before_p50)}/{_format_percentile(before_p95)}"
    after_text = f"{_format_percentile(after_p50)}/{_format_percentile(after_p95)}"
    print(
        f"[timing] {metric}: {duration_s * 1000:.1f} ms (p50/p95 {before_text} -> {after_text})"
    )


async def _measure_transloc_call(name: str, coro: Awaitable[Any]) -> Any:
    start = time.perf_counter()
    result = await coro
    _record_transloc_timing(name, time.perf_counter() - start)
    return result


async def _proxy_transloc_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    base_url: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
):
    try:
        resolved_client = _get_transloc_client(client)
        r = await resolved_client.get(url, params=params)
        record_api_call("GET", str(r.url), r.status_code)
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url or url)
        raise HTTPException(status_code=502, detail=detail) from exc


def _coerce_epoch_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        candidate = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            candidate = float(text)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
            except ValueError:
                return None
            candidate = parsed.timestamp()
    else:
        return None
    for _ in range(3):
        if candidate > 1e11:
            candidate /= 1000.0
        else:
            break
    if candidate <= 0:
        return None
    return candidate


def _vehicle_age_seconds(seconds_since_report: Any = None, timestamp: Any = None) -> Optional[float]:
    seconds_val = _coerce_float(seconds_since_report)
    if seconds_val is not None and seconds_val >= 0:
        return seconds_val
    epoch = _coerce_epoch_seconds(timestamp)
    if epoch is None:
        return None
    age = time.time() - epoch
    if age < 0:
        return 0.0
    return age


def _parse_headway_ids(param: Optional[str]) -> Set[str]:
    ids: Set[str] = set()
    if not param:
        return ids
    for part in param.split(","):
        text = part.strip()
        if text:
            ids.add(text)
    return ids


def _parse_headway_timestamp(value: str) -> datetime:
    try:
        return parse_iso8601_utc(value)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid timestamp format; use ISO-8601")


def _normalize_vehicle_id_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        text = str(value).strip()
        return text or None


def _pick_vehicle_name_record(record: Dict[str, Any]) -> str:
    for key in (
        "vehicle_name",
        "VehicleName",
        "Name",
        "Label",
        "vehicle_label",
        "VehicleLabel",
    ):
        val = record.get(key)
        if val:
            text = str(val).strip()
            if text:
                return text
    return ""


def _build_vehicle_name_lookup(records: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for rec in records or []:
        vid = (
            rec.get("VehicleID")
            or rec.get("VehicleId")
            or rec.get("vehicle_id")
            or rec.get("vehicleId")
        )
        vid_norm = _normalize_vehicle_id_str(vid)
        name = _pick_vehicle_name_record(rec)
        if vid_norm and name:
            mapping[vid_norm] = name
    return mapping


async def build_transloc_snapshot(
    base_url: Optional[str] = None, include_stale: bool = False
) -> Dict[str, Any]:
    # Run all data fetches in parallel for performance
    if is_default_transloc_base(base_url):
        (routes_raw, extra_routes_raw), (assigned, raw_vehicle_records), stops_raw = await asyncio.gather(
            _load_transloc_route_sources(base_url),
            _load_transloc_vehicle_sources(base_url),
            _load_transloc_stop_sources(base_url),
        )
    else:
        client = _get_transloc_client(None)
        (routes_raw, extra_routes_raw), (assigned, raw_vehicle_records), stops_raw = await asyncio.gather(
            _load_transloc_route_sources(base_url, client=client),
            _load_transloc_vehicle_sources(base_url, client=client),
            _load_transloc_stop_sources(base_url, client=client),
        )

    metadata = await _assemble_transloc_metadata(
        base_url=base_url,
        routes_raw=routes_raw,
        extra_routes_raw=extra_routes_raw,
        stops_raw=stops_raw,
    )

    # Get cached capacities from state
    async with state.lock:
        capacities = state.vehicle_capacities.copy()

    vehicles = _assemble_transloc_vehicles(
        raw_vehicle_records=raw_vehicle_records,
        assigned=assigned,
        include_stale=include_stale,
        capacities=capacities,
    )
    return {
        "fetched_at": int(time.time()),
        **metadata,
        "vehicles": vehicles,
    }


async def _load_transloc_route_sources(
    base_url: Optional[str], client: Optional[httpx.AsyncClient] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if is_default_transloc_base(base_url):
        async with state.lock:
            routes_raw = list(getattr(state, "routes_raw", []))
            extra_routes_raw = list(getattr(state, "routes_catalog_raw", []))
            return routes_raw, extra_routes_raw
    resolved_client = _get_transloc_client(client)
    routes_raw = await fetch_routes_with_shapes(resolved_client, base_url=base_url)
    try:
        extra_routes_raw = await fetch_routes_catalog(resolved_client, base_url=base_url)
    except Exception as e:
        extra_routes_raw = []
        print(f"[snapshot] routes catalog fetch error: {e}")
    return routes_raw, extra_routes_raw


async def _load_transloc_vehicle_sources(
    base_url: Optional[str], client: Optional[httpx.AsyncClient] = None
) -> Tuple[Dict[Any, Tuple[int, Vehicle]], List[Dict[str, Any]]]:
    if is_default_transloc_base(base_url):
        async with state.lock:
            assigned: Dict[Any, Tuple[int, Vehicle]] = {}
            for rid, vehs in state.vehicles_by_route.items():
                for veh in vehs.values():
                    if veh.id is None:
                        continue
                    assigned[veh.id] = (rid, veh)
            raw_vehicle_records = list(getattr(state, "vehicles_raw", []))
            return assigned, raw_vehicle_records
    resolved_client = _get_transloc_client(client)
    vehicles_raw = await fetch_vehicles(
        resolved_client, include_unassigned=True, base_url=base_url
    )
    return {}, list(vehicles_raw)


async def _load_transloc_stop_sources(
    base_url: Optional[str], client: Optional[httpx.AsyncClient] = None
) -> List[Dict[str, Any]]:
    if is_default_transloc_base(base_url):
        async with state.lock:
            return list(getattr(state, "stops_raw", []))
    resolved_client = _get_transloc_client(client)
    try:
        return await fetch_stops(resolved_client, base_url=base_url)
    except Exception as exc:
        print(f"[snapshot] stop fetch error: {exc}")
        return []


async def _assemble_transloc_metadata(
    *,
    base_url: Optional[str],
    routes_raw: List[Dict[str, Any]],
    extra_routes_raw: List[Dict[str, Any]],
    stops_raw: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    raw_routes = [_trim_transloc_route(r) for r in routes_raw]
    if extra_routes_raw:
        raw_routes = _merge_transloc_route_metadata(raw_routes, extra_routes_raw)
    stops = _build_transloc_stops(
        raw_routes, stops_raw,
        approach_sets_config=getattr(app.state, "approach_sets_config", None),
    )
    blocks = await _get_transloc_blocks(base_url)
    return {"routes": raw_routes, "stops": stops, "blocks": blocks}


async def _fetch_vehicle_stop_estimates_raw(
    vehicle_ids: List[Any],
    base_url: Optional[str] = None,
    quantity: int = 3,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    Fetch stop estimates directly from TransLoc API (no caching).
    Used by background updater to pre-populate state.stop_estimates.
    """
    if not vehicle_ids:
        return {}

    try:
        vehicle_id_strings = ",".join(str(vid) for vid in vehicle_ids)
        url = build_transloc_url(base_url, "GetVehicleRouteStopEstimates")
        params = {
            "APIKey": TRANSLOC_KEY,
            "quantity": quantity,
            "vehicleIdStrings": vehicle_id_strings
        }
        resolved_client = _get_transloc_client(client)
        data = await _proxy_transloc_get(
            url, params=params, base_url=base_url, client=resolved_client
        )

        estimates_by_vehicle = {}
        if data and isinstance(data, dict):
            vehicles = data.get("Vehicles", [])
            if isinstance(vehicles, list):
                for vehicle in vehicles:
                    if isinstance(vehicle, dict):
                        vid = vehicle.get("VehicleID")
                        estimates = vehicle.get("Estimates", [])
                        if vid is not None and isinstance(estimates, list):
                            estimates_by_vehicle[vid] = estimates
        return estimates_by_vehicle
    except Exception as e:
        print(f"[vehicle_estimates_raw] Failed to fetch stop estimates: {e}")
        return {}


async def _fetch_vehicle_stop_estimates(
    vehicle_ids: List[Any],
    base_url: Optional[str] = None,
    quantity: int = 3,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    Fetch stop estimates for multiple vehicles from TransLoc API.

    Args:
        vehicle_ids: List of vehicle IDs to fetch estimates for
        base_url: Optional TransLoc base URL override
        quantity: Number of stops to return per vehicle (default 3)

    Returns:
        Dict mapping vehicle ID to list of stop estimates
    """
    if not vehicle_ids:
        return {}

    # Use cache keyed by (base_url, normalized vehicle set, quantity) to avoid repeated API calls
    normalized_vehicle_ids = tuple(sorted({str(vid) for vid in vehicle_ids}))
    cache_key = (base_url or "default", normalized_vehicle_ids, quantity)

    async def fetch():
        try:
            # Convert vehicle IDs to comma-separated string
            vehicle_id_strings = ",".join(str(vid) for vid in vehicle_ids)

            url = build_transloc_url(base_url, "GetVehicleRouteStopEstimates")
            params = {
                "APIKey": TRANSLOC_KEY,
                "quantity": quantity,
                "vehicleIdStrings": vehicle_id_strings
            }

            data = await _proxy_transloc_get(
                url, params=params, base_url=base_url, client=client
            )

            # Build a dict mapping vehicle ID to estimates
            estimates_by_vehicle = {}
            if data and isinstance(data, dict):
                vehicles = data.get("Vehicles", [])
                if isinstance(vehicles, list):
                    for vehicle in vehicles:
                        if isinstance(vehicle, dict):
                            vid = vehicle.get("VehicleID")
                            estimates = vehicle.get("Estimates", [])
                            if vid is not None and isinstance(estimates, list):
                                estimates_by_vehicle[vid] = estimates

            return estimates_by_vehicle
        except Exception as e:
            # If the estimates fetch fails, log it but don't fail the whole request
            print(f"[vehicle_estimates] Failed to fetch stop estimates: {e}")
            return {}

    estimates, cache_state = await transloc_vehicle_estimates_cache.get(cache_key, fetch)
    state_label = {
        "seed": "blocking seed fetch",
        "fresh": "served fresh",
        "stale": "served stale",
    }.get(cache_state, cache_state)
    print(
        f"[testmap_vehicles] stop estimate cache {state_label} "
        f"(base={cache_key[0]}, vehicles={len(normalized_vehicle_ids)}, qty={quantity})"
    )
    return estimates


async def _fetch_vehicle_stop_estimates_guarded(
    *,
    vehicle_ids: List[Any],
    base_url: Optional[str] = None,
    quantity: int = 3,
    timeout_seconds: float = 3.0,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[Any, List[Dict[str, Any]]]:
    # For default base_url, try to serve from pre-fetched state first (instant)
    if is_default_transloc_base(base_url):
        async with state.lock:
            cached_estimates = state.stop_estimates
        if cached_estimates:
            print(f"[vehicle_estimates] served from pre-fetched state ({len(cached_estimates)} vehicles)")
            return cached_estimates

    # Fall back to cached/fetched estimates
    try:
        return await asyncio.wait_for(
            _fetch_vehicle_stop_estimates(
                vehicle_ids=vehicle_ids,
                base_url=base_url,
                quantity=quantity,
                client=client,
            ),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        print("[vehicle_estimates] Stop estimates request timed out")
        return {}


def _assemble_transloc_vehicles(
    *,
    raw_vehicle_records: List[Dict[str, Any]],
    assigned: Dict[Any, Tuple[int, Vehicle]],
    include_stale: bool,
    capacities: Optional[Dict[int, Dict[str, Any]]] = None,
    stop_estimates: Optional[Dict[Any, List[Dict[str, Any]]]] = None,
    route_id_to_name: Optional[Dict[Any, str]] = None,
) -> List[Dict[str, Any]]:
    vehicles: List[Dict[str, Any]] = []
    for rec in raw_vehicle_records:
        vid = rec.get("VehicleID") or rec.get("VehicleId")
        if vid is None:
            continue
        rid = rec.get("RouteID") or rec.get("RouteId")
        lat = rec.get("Latitude") or rec.get("Lat")
        lon = rec.get("Longitude") or rec.get("Lon") or rec.get("Lng")
        if lat is None or lon is None:
            continue
        heading = rec.get("Heading")
        ground_speed = rec.get("GroundSpeed") or rec.get("Speed") or 0.0
        seconds_raw = rec.get("Seconds") or rec.get("SecondsSinceReport")
        seconds = _coerce_float(seconds_raw)
        assigned_rec = assigned.get(vid)
        if assigned_rec:
            rid = assigned_rec[0]
            veh = assigned_rec[1]
            heading = getattr(veh, "heading", heading)
            ground_speed = veh.ground_mps / MPH_TO_MPS
            seconds = _coerce_float(getattr(veh, "age_s", seconds))
        is_stale = bool(seconds is not None and seconds > STALE_FIX_S)
        is_very_stale = False  # Tracks if vehicle is hour+ old
        age_for_filter = _vehicle_age_seconds(
            seconds_since_report=seconds if seconds is not None else seconds_raw,
            timestamp=(
                rec.get("LastUpdated")
                or rec.get("LastUpdate")
                or rec.get("Timestamp")
                or rec.get("TimeStamp")
                or rec.get("DateTime")
                or rec.get("DateTimeUTC")
            ),
        )
        # Track if vehicle is hour+ old (very stale)
        if age_for_filter is not None and age_for_filter >= VEHICLE_STALE_THRESHOLD_S:
            is_very_stale = True
        if (
            not include_stale
            and age_for_filter is not None
            and age_for_filter >= VEHICLE_STALE_THRESHOLD_S
        ):
            continue
        seconds_output = seconds if seconds is not None else seconds_raw
        vehicle_data = {
            "VehicleID": vid,
            "RouteID": rid if rid is not None else 0,
            "routeID": rid if rid is not None else 0,
            "Latitude": lat,
            "Longitude": lon,
            "Heading": heading,
            "GroundSpeed": ground_speed,
            "Name": rec.get("Name") or rec.get("VehicleName"),
            "SecondsSinceReport": seconds_output,
            "IsStale": is_stale,
            "IsVeryStale": is_very_stale,
        }
        # Add route name if available
        if route_id_to_name and rid is not None:
            route_name = route_id_to_name.get(rid)
            if route_name:
                vehicle_data["RouteName"] = route_name
        # Add capacity data if available for this vehicle
        if capacities and vid in capacities:
            cap_data = capacities[vid]
            # Only include capacity data if capacity is not None and not 0
            if cap_data.get("capacity") is not None and cap_data.get("capacity") != 0:
                vehicle_data["capacity"] = cap_data["capacity"]
                if cap_data.get("current_occupation") is not None:
                    vehicle_data["current_occupation"] = cap_data["current_occupation"]
                if cap_data.get("percentage") is not None:
                    vehicle_data["percentage"] = cap_data["percentage"]
        # Add stop estimates if available for this vehicle
        if stop_estimates and vid in stop_estimates:
            vehicle_data["Estimates"] = stop_estimates[vid]
        vehicles.append(vehicle_data)
    return vehicles


@app.get("/v1/testmap/transloc")
async def testmap_transloc_snapshot(
    base_url: Optional[str] = Query(None), stale: bool = Query(False)
):
    try:
        return await build_transloc_snapshot(base_url=base_url, include_stale=stale)
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url)
        raise HTTPException(status_code=502, detail=detail) from exc


@app.get("/v1/testmap/transloc/vehicles")
async def testmap_transloc_vehicles(
    base_url: Optional[str] = Query(None), stale: bool = Query(False)
):
    handler_start = time.perf_counter()
    client = _get_transloc_client(None)
    try:
        if is_default_transloc_base(base_url):
            # Fast path: serve from pre-materialized payload if available
            async with state.lock:
                payload = state.testmap_vehicles_payload
                payload_ts = state.testmap_vehicles_ts
            if payload is not None:
                # Filter by stale param
                if stale:
                    vehicles = payload
                else:
                    # Filter out hour+ old vehicles (IsVeryStale)
                    # Note: IsStale (90s+) are kept but shown with stale styling
                    vehicles = [v for v in payload if not v.get("IsVeryStale")]
                _record_transloc_timing("handler_total", time.perf_counter() - handler_start)
                print(f"[testmap_vehicles] served pre-materialized payload ({len(vehicles)} vehicles)")
                return {
                    "fetched_at": int(payload_ts),
                    "vehicles": vehicles,
                }

            # Fallback: Optimized path for default TransLoc base when payload not yet built
            # - Use pre-cached capacities and route names from state
            # - Get vehicle IDs from state first to parallelize stop estimates fetch
            cap_start = time.perf_counter()
            async with state.lock:
                capacities = dict(state.vehicle_capacities)
                route_id_to_name = dict(state.route_id_to_name)
                # Get vehicle IDs from state for parallel stop estimates fetch
                cached_vehicle_ids = [
                    rec.get("VehicleID") or rec.get("VehicleId")
                    for rec in getattr(state, "vehicles_raw", [])
                    if (rec.get("VehicleID") or rec.get("VehicleId")) is not None
                ]
            _record_transloc_timing("capacities", time.perf_counter() - cap_start)

            # Run vehicle/route sources and stop estimates in parallel
            vehicle_task = asyncio.create_task(
                _measure_transloc_call(
                    "vehicles", _load_transloc_vehicle_sources(base_url, client=client)
                )
            )
            route_task = asyncio.create_task(
                _measure_transloc_call(
                    "routes", _load_transloc_route_sources(base_url, client=client)
                )
            )
            stop_estimate_task = asyncio.create_task(
                _measure_transloc_call(
                    "stop_estimates",
                    _fetch_vehicle_stop_estimates_guarded(
                        vehicle_ids=cached_vehicle_ids,
                        base_url=base_url,
                        quantity=3,
                        client=client,
                    ),
                )
            )

            (assigned, raw_vehicle_records), (routes_raw, _), stop_estimates = await asyncio.gather(
                vehicle_task,
                route_task,
                stop_estimate_task,
            )

            # If route_id_to_name is empty (first request before background update),
            # build it from routes_raw as fallback
            if not route_id_to_name:
                for route in routes_raw:
                    rid = route.get("RouteID") or route.get("RouteId")
                    route_name = route.get("Description") or route.get("RouteName") or route.get("LongName") or route.get("ShortName")
                    if rid is not None and route_name:
                        info_text = route.get("InfoText")
                        if info_text and isinstance(info_text, str) and info_text.strip():
                            route_id_to_name[rid] = f"{route_name} ({info_text.strip()})"
                        else:
                            route_id_to_name[rid] = route_name
        else:
            # Non-default base URL: use caching and shared client
            cache_key = base_url or "default"

            async def fetch_capacities_cached():
                async def fetch():
                    try:
                        return await fetch_vehicle_capacities(client, base_url=base_url)
                    except Exception as e:
                        print(f"[testmap] capacity fetch error: {e}")
                        return {}
                return await transloc_capacities_cache.get(cache_key, fetch)

            # Fetch vehicle sources, route sources, and capacities in parallel
            vehicle_task = asyncio.create_task(
                _measure_transloc_call(
                    "vehicles", _load_transloc_vehicle_sources(base_url, client=client)
                )
            )
            route_task = asyncio.create_task(
                _measure_transloc_call(
                    "routes", _load_transloc_route_sources(base_url, client=client)
                )
            )
            capacities_task = asyncio.create_task(
                _measure_transloc_call(
                    "capacities", fetch_capacities_cached()
                )
            )
            (assigned, raw_vehicle_records), (routes_raw, _), capacities = await asyncio.gather(
                vehicle_task,
                route_task,
                capacities_task,
            )

            # Build route ID to name mapping
            route_id_to_name = {}
            for route in routes_raw:
                rid = route.get("RouteID") or route.get("RouteId")
                route_name = route.get("Description") or route.get("RouteName") or route.get("LongName") or route.get("ShortName")
                if rid is not None and route_name:
                    info_text = route.get("InfoText")
                    if info_text and isinstance(info_text, str) and info_text.strip():
                        route_id_to_name[rid] = f"{route_name} ({info_text.strip()})"
                    else:
                        route_id_to_name[rid] = route_name

            # Extract vehicle IDs and fetch stop estimates
            vehicle_ids = [
                rec.get("VehicleID") or rec.get("VehicleId")
                for rec in raw_vehicle_records
                if (rec.get("VehicleID") or rec.get("VehicleId")) is not None
            ]
            stop_estimates = await _measure_transloc_call(
                "stop_estimates",
                _fetch_vehicle_stop_estimates_guarded(
                    vehicle_ids=vehicle_ids,
                    base_url=base_url,
                    quantity=3,
                    client=client,
                ),
            )

        vehicles = _assemble_transloc_vehicles(
            raw_vehicle_records=raw_vehicle_records,
            assigned=assigned,
            include_stale=stale,
            capacities=capacities,
            stop_estimates=stop_estimates,
            route_id_to_name=route_id_to_name,
        )
        return {
            "fetched_at": int(time.time()),
            "vehicles": vehicles,
        }
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url)
        raise HTTPException(status_code=502, detail=detail) from exc
    finally:
        _record_transloc_timing("handler_total", time.perf_counter() - handler_start)


@app.get("/v1/testmap/transloc/metadata")
async def testmap_transloc_metadata(request: Request, base_url: Optional[str] = Query(None)):
    try:
        routes_raw, extra_routes_raw = await _load_transloc_route_sources(base_url)
        metadata = await _assemble_transloc_metadata(
            base_url=base_url,
            routes_raw=routes_raw,
            extra_routes_raw=extra_routes_raw,
        )

        # Filter routes based on authentication for unauthenticated users
        if not _has_dispatcher_access(request):
            # Only show routes where IsVisibleOnMap is not explicitly False
            filtered_routes = [
                route for route in metadata.get("routes", [])
                if route.get("IsVisibleOnMap") is not False
            ]
            metadata["routes"] = filtered_routes

        metadata["fetched_at"] = int(time.time())
        return metadata
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url)
        raise HTTPException(status_code=502, detail=detail) from exc


def _extract_cat_array(root: Any, keys: List[str]) -> List[Any]:
    if isinstance(root, list):
        return root
    if not isinstance(root, dict):
        return []
    for key in keys:
        val = root.get(key)
        if isinstance(val, list):
            return val
    for key in ["data", "Data", "result", "Result", "items", "Items"]:
        val = root.get(key)
        if isinstance(val, list):
            return val
    return []


async def _cat_api_request(service: str, extra: Optional[Dict[str, Any]] = None) -> Any:
    params = {"service": service, "token": CAT_API_TOKEN}
    if extra:
        for k, v in extra.items():
            if v is not None:
                params[k] = v
    async with httpx.AsyncClient() as client:
        resp = await client.get(CAT_API_BASE, params=params, timeout=20)
        record_api_call("GET", str(resp.request.url), resp.status_code)
        resp.raise_for_status()
        return resp.json()


def _trim_cat_routes(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["routes", "Routes", "get_routes", "GetRoutes"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        rid = (
            entry.get("RouteID")
            or entry.get("routeID")
            or entry.get("RouteId")
            or entry.get("routeId")
            or entry.get("ID")
            or entry.get("Id")
            or entry.get("id")
        )
        color = (
            entry.get("Color")
            or entry.get("RouteColor")
            or entry.get("RouteHexColor")
            or entry.get("HexColor")
            or entry.get("color")
        )
        result.append(
            {
                "RouteID": rid,
                "RouteName": entry.get("RouteName")
                or entry.get("Description")
                or entry.get("description")
                or entry.get("Name")
                or entry.get("name"),
                "RouteAbbreviation": entry.get("RouteAbbreviation")
                or entry.get("ShortName")
                or entry.get("shortName")
                or entry.get("Abbreviation")
                or entry.get("abbreviation")
                or entry.get("abbr"),
                "Description": entry.get("Description") or entry.get("description"),
                "Color": color,
            }
        )
    return result


def _trim_cat_stops(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["stops", "Stops", "get_stops", "GetStops"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        stop_id = (
            entry.get("StopID")
            or entry.get("stopID")
            or entry.get("StopId")
            or entry.get("stopId")
            or entry.get("Id")
            or entry.get("id")
        )
        route_stop_id = (
            entry.get("RouteStopID")
            or entry.get("rsid")
            or entry.get("RouteStopId")
            or entry.get("RSID")
        )
        result.append(
            {
                "StopID": stop_id,
                "StopName": entry.get("StopName")
                or entry.get("Name")
                or entry.get("name"),
                "Latitude": entry.get("Latitude")
                or entry.get("Lat")
                or entry.get("lat"),
                "Longitude": entry.get("Longitude")
                or entry.get("Lon")
                or entry.get("lon")
                or entry.get("Lng")
                or entry.get("lng"),
                "RouteID": entry.get("RouteID")
                or entry.get("rid")
                or entry.get("RouteId")
                or entry.get("routeID")
                or entry.get("routeId")
                or entry.get("Route")
                or entry.get("route"),
                "RouteStopID": route_stop_id,
                "RouteStopId": route_stop_id,
            }
        )
    return result


def _trim_cat_patterns(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["patterns", "Patterns", "get_patterns", "GetPatterns"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        # Preserve the raw entry while normalising key casing so the front-end
        # has enough context (pattern id + route ids) to associate the polyline
        # with a route.
        normalized: Dict[str, Any] = {}

        def copy_if_present(keys: Iterable[str]) -> None:
            for key in keys:
                if key in entry:
                    normalized[key] = entry[key]

        copy_if_present(
            [
                "id",
                "Id",
                "patternId",
                "PatternId",
                "PatternID",
                "patternID",
                "extID",
                "ExtID",
                "name",
                "Name",
                "color",
                "Color",
                "routeColor",
                "RouteColor",
                "lineColor",
                "LineColor",
                "displayColor",
                "DisplayColor",
                "routes",
                "Routes",
                "routeIDs",
                "RouteIDs",
                "routeIds",
                "RouteIds",
                "encLine",
                "EncLine",
                "encodedPolyline",
                "EncodedPolyline",
                "polyline",
                "Polyline",
                "decLine",
                "DecLine",
                "decodedLine",
                "DecodedLine",
                "RouteID",
                "routeID",
                "RouteId",
                "routeId",
                "Route",
                "route",
            ]
        )

        pattern_id = (
            entry.get("PatternID")
            or entry.get("patternID")
            or entry.get("PatternId")
            or entry.get("patternId")
            or entry.get("id")
            or entry.get("Id")
        )
        if pattern_id is not None:
            normalized.setdefault("PatternID", pattern_id)
            normalized.setdefault("PatternId", pattern_id)
            normalized.setdefault("patternID", pattern_id)
            normalized.setdefault("patternId", pattern_id)
            normalized.setdefault("id", pattern_id)
            normalized.setdefault("Id", pattern_id)

        route_candidates: List[Any] = []
        for key in ["routes", "Routes", "routeIDs", "RouteIDs", "routeIds", "RouteIds"]:
            values = entry.get(key)
            if isinstance(values, list):
                route_candidates.extend(values)
        for key in ["RouteID", "routeID", "RouteId", "routeId", "Route", "route"]:
            value = entry.get(key)
            if value is not None:
                route_candidates.append(value)

        rid: Optional[Any] = None
        for candidate in route_candidates:
            if candidate is None:
                continue
            if isinstance(candidate, str) and candidate.strip() == "":
                continue
            rid = candidate
            break

        if rid is not None:
            normalized.setdefault("RouteID", rid)
            normalized.setdefault("routeID", rid)
            normalized.setdefault("RouteId", rid)
            normalized.setdefault("routeId", rid)
            normalized.setdefault("Route", rid)
            normalized.setdefault("route", rid)

        if route_candidates:
            normalized.setdefault("routes", route_candidates)
            normalized.setdefault("Routes", route_candidates)

        encoded = (
            entry.get("encLine")
            or entry.get("EncLine")
            or entry.get("encodedPolyline")
            or entry.get("EncodedPolyline")
            or entry.get("polyline")
            or entry.get("Polyline")
        )
        if encoded is not None:
            normalized.setdefault("encLine", encoded)
            normalized.setdefault("EncLine", encoded)
            normalized.setdefault("EncodedPolyline", encoded)

        decoded = entry.get("DecLine") or entry.get("decLine") or entry.get("decodedLine") or entry.get("DecodedLine")
        if decoded is not None:
            normalized.setdefault("DecLine", decoded)
            normalized.setdefault("decLine", decoded)
            normalized.setdefault("decodedLine", decoded)
            normalized.setdefault("DecodedLine", decoded)
            normalized.setdefault("DecodedPath", decoded)

        result.append(normalized)
    return result


def _trim_cat_vehicles(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["vehicles", "Vehicles", "get_vehicles", "GetVehicles"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        receive_time_raw = entry.get("receiveTime") or entry.get("ReceiveTime")
        age_for_filter = _vehicle_age_seconds(
            seconds_since_report=entry.get("SecondsSinceReport")
            or entry.get("secondsSinceReport"),
            timestamp=receive_time_raw,
        )
        if age_for_filter is not None and age_for_filter >= VEHICLE_STALE_THRESHOLD_S:
            continue
        route_id = (
            entry.get("RouteID")
            or entry.get("routeID")
            or entry.get("RouteId")
            or entry.get("routeId")
            or entry.get("route")
        )

        # Extract next stop info from the raw API response
        next_stop_id = entry.get("nextStopID")
        if next_stop_id is None:
            next_stop_id = entry.get("NextStopID")

        # Extract ETAs from minutesToNextStops array (primary source)
        # Each entry has: stopID, minutes, time (actual ETA like "01:41PM")
        etas = entry.get("ETAs") or entry.get("etas") or entry.get("MinutesToStops")
        minutes_to_next_stops = entry.get("minutesToNextStops") or entry.get("MinutesToNextStops")

        if etas is None and isinstance(minutes_to_next_stops, list) and len(minutes_to_next_stops) > 0:
            etas = []
            for stop_eta in minutes_to_next_stops:
                if not isinstance(stop_eta, dict):
                    continue
                stop_id = stop_eta.get("stopID") or stop_eta.get("StopID")
                minutes = stop_eta.get("minutes")
                if minutes is None:
                    minutes = stop_eta.get("Minutes")
                time_str = stop_eta.get("time") or stop_eta.get("Time")
                pattern_stop_id = stop_eta.get("patternStopID") or stop_eta.get("PatternStopID")
                direction = stop_eta.get("direction") or stop_eta.get("Direction")
                direction_abbr = stop_eta.get("directionAbbr") or stop_eta.get("DirectionAbbr")

                if stop_id is not None:
                    etas.append({
                        "StopID": stop_id,
                        "stopID": stop_id,
                        "Minutes": minutes,
                        "minutes": minutes,
                        "Time": time_str,
                        "time": time_str,
                        "PatternStopID": pattern_stop_id,
                        "patternStopID": pattern_stop_id,
                        "Direction": direction,
                        "direction": direction,
                        "DirectionAbbr": direction_abbr,
                        "directionAbbr": direction_abbr,
                    })

        result.append(
            {
                "VehicleID": entry.get("VehicleID")
                or entry.get("vehicleID")
                or entry.get("VehicleId")
                or entry.get("vehicleId")
                or entry.get("ID")
                or entry.get("Id")
                or entry.get("id"),
                "Name": entry.get("VehicleName")
                or entry.get("vehicleName")
                or entry.get("Name")
                or entry.get("name"),
                "EquipmentID": entry.get("EquipmentID") or entry.get("equipmentID"),
                "Latitude": entry.get("Latitude")
                or entry.get("Lat")
                or entry.get("lat"),
                "Longitude": entry.get("Longitude")
                or entry.get("Lon")
                or entry.get("lon")
                or entry.get("Lng")
                or entry.get("lng"),
                "Heading": entry.get("Heading") or entry.get("h"),
                "Speed": entry.get("Speed")
                or entry.get("speed")
                or entry.get("GpsSpeed")
                or entry.get("gpsSpeed"),
                "RouteID": route_id,
                "routeID": route_id,
                "RouteAbbreviation": entry.get("RouteAbbreviation")
                or entry.get("ShortName")
                or entry.get("shortName"),
                "RouteName": entry.get("RouteName")
                or entry.get("Description")
                or entry.get("routeName")
                or entry.get("name"),
                "ETAs": etas,
                "NextStopID": next_stop_id,
                "nextStopID": next_stop_id,
                "ReceiveTime": receive_time_raw,
                "receiveTime": receive_time_raw,
            }
        )
    return result


def _trim_cat_service_alerts(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["announcements", "Announcements", "alerts", "Alerts"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        result.append(
            {
                "ID": entry.get("ID") or entry.get("Id") or entry.get("AlertID") or entry.get("guid"),
                "Title": entry.get("Title") or entry.get("Name"),
                "Message": entry.get("Message") or entry.get("Description"),
                "Routes": entry.get("Routes") or entry.get("RouteNames") or entry.get("Route"),
                "StartDate": entry.get("StartDate") or entry.get("Effective"),
                "EndDate": entry.get("EndDate") or entry.get("Expiration"),
                "IsActive": entry.get("IsActive") or entry.get("Active") or entry.get("Status"),
            }
        )
    return result


def _trim_cat_stop_etas(payload: Any) -> List[Dict[str, Any]]:
    entries = _extract_cat_array(payload, ["etas", "ETAs", "get_stop_etas", "GetStopEtas"])
    result: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        stop_id = (
            entry.get("id")
            or entry.get("stopId")
            or entry.get("StopID")
            or entry.get("StopId")
        )
        stop_name = entry.get("stopName") or entry.get("StopName") or entry.get("Name")
        en_route_entries = entry.get("enRoute") or entry.get("EnRoute") or entry.get("enroute")
        normalized_en_route: List[Dict[str, Any]] = []
        if isinstance(en_route_entries, list):
            for eta in en_route_entries:
                if not isinstance(eta, dict):
                    continue
                # Extract stop ID - use stopID first (lowercase ID is the API format)
                eta_stop_id = eta.get("stopID")
                if eta_stop_id is None:
                    eta_stop_id = eta.get("stopId")
                if eta_stop_id is None:
                    eta_stop_id = eta.get("StopID")
                if eta_stop_id is None:
                    eta_stop_id = eta.get("StopId")
                if eta_stop_id is None:
                    eta_stop_id = stop_id
                route_id = eta.get("routeID") or eta.get("RouteID") or eta.get("routeId")
                route_key = eta.get("route") or eta.get("Route") or route_id
                # Use explicit None checks to handle 0 values correctly
                minutes = eta.get("minutes")
                if minutes is None:
                    minutes = eta.get("Minutes")
                seconds = eta.get("seconds")
                if seconds is None:
                    seconds = eta.get("Seconds")
                # Use "time" field as the display text (e.g., "01:57PM")
                time_str = eta.get("time") or eta.get("Time")
                text = eta.get("text") or eta.get("Text") or time_str
                normalized_en_route.append(
                    {
                        "StopID": eta_stop_id,
                        "stopID": eta_stop_id,
                        "StopId": eta_stop_id,
                        "StopName": eta.get("stopName")
                        or eta.get("StopName")
                        or eta.get("name")
                        or eta.get("Name")
                        or stop_name,
                        "RouteID": route_id,
                        "routeID": route_id,
                        "RouteId": route_id,
                        "route": route_key,
                        "Route": route_key,
                        "RouteName": eta.get("routeName")
                        or eta.get("RouteName")
                        or entry.get("routeName")
                        or entry.get("RouteName"),
                        "Minutes": minutes,
                        "minutes": minutes,
                        "Seconds": seconds,
                        "seconds": seconds,
                        "Text": text,
                        "text": text,
                        "Time": eta.get("time") or eta.get("Time"),
                        "Schedule": eta.get("schedule") or eta.get("Schedule"),
                        "Status": eta.get("status") or eta.get("Status"),
                        "StatusColor": eta.get("statuscolor")
                        or eta.get("StatusColor")
                        or eta.get("statusColor"),
                        "Direction": eta.get("direction") or eta.get("Direction"),
                        "DirectionAbbr": eta.get("directionAbbr") or eta.get("DirectionAbbr"),
                        "BlockID": eta.get("blockID") or eta.get("BlockID") or eta.get("blockId"),
                        "EquipmentID": eta.get("equipmentID")
                        or eta.get("EquipmentID")
                        or eta.get("equipmentId"),
                    }
                )
        result.append(
            {
                "id": stop_id,
                "StopID": stop_id,
                "stopID": stop_id,
                "StopId": stop_id,
                "stopName": stop_name,
                "StopName": stop_name,
                "enRoute": normalized_en_route,
                "EnRoute": normalized_en_route,
            }
        )
    return result


async def _get_cat_routes() -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request("get_routes")
        return _trim_cat_routes(payload)

    return await cat_routes_cache.get(fetch)


async def _get_cat_stops() -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request("get_stops")
        return _trim_cat_stops(payload)

    return await cat_stops_cache.get(fetch)


async def _get_cat_patterns() -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request("get_patterns")
        return _trim_cat_patterns(payload)

    return await cat_patterns_cache.get(fetch)


async def _get_cat_vehicles() -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request(
            "get_vehicles",
            {
                "includeETAData": "1",
                "inService": "0",
                "orderedETAArray": "1",
            },
        )
        return _trim_cat_vehicles(payload)

    return await cat_vehicles_cache.get(fetch)


async def _get_cat_service_alerts() -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request("get_service_announcements")
        return _trim_cat_service_alerts(payload)

    return await cat_service_alerts_cache.get(fetch)


async def _get_cat_stop_etas(stop_id: str) -> List[Dict[str, Any]]:
    async def fetch():
        payload = await _cat_api_request(
            "get_stop_etas",
            {"stopID": stop_id, "statusData": "1"},
        )
        return _trim_cat_stop_etas(payload)

    return await cat_stop_etas_cache.get(stop_id, fetch)


@app.get("/v1/testmap/cat/routes")
async def cat_routes_endpoint():
    return {"routes": await _get_cat_routes()}


@app.get("/v1/testmap/cat/stops")
async def cat_stops_endpoint():
    return {"stops": await _get_cat_stops()}


@app.get("/v1/testmap/cat/patterns")
async def cat_patterns_endpoint():
    return {"patterns": await _get_cat_patterns()}


@app.get("/v1/testmap/cat/vehicles")
async def cat_vehicles_endpoint():
    return {"vehicles": await _get_cat_vehicles()}


@app.get("/v1/testmap/cat/service-alerts")
async def cat_service_alerts_endpoint():
    return {"alerts": await _get_cat_service_alerts()}


@app.get("/v1/testmap/cat/stop-etas")
async def cat_stop_etas_endpoint(stop_id: str):
    data = await _get_cat_stop_etas(stop_id)
    return {"etas": data}



_PULSEPOINT_INCIDENT_ID_KEYS = (
    "ID",
    "IncidentID",
    "IncidentNumber",
    "PulsePointIncidentID",
    "PulsePointIncidentCallNumber",
    "CadIncidentNumber",
    "CADIncidentNumber",
)
_PULSEPOINT_INCIDENT_RECEIVED_FIELDS = (
    "CallReceivedDateTime",
    "ReceivedDateTime",
    "Received",
    "CallReceived",
    "FirstReceived",
    "CreateDate",
    "CreatedDateTime",
    "DispatchDateTime",
)
_PULSEPOINT_FIRST_ON_SCENE_FIELDS = (
    "FirstUnitOnSceneDateTime",
    "FirstOnSceneDateTime",
    "FirstUnitOnScene",
    "FirstOnScene",
    "FirstUnitArrivedDateTime",
    "FirstArrivedDateTime",
    "FirstArrivalDateTime",
    "CallFirstUnitOnSceneDateTime",
    "CallFirstOnSceneDateTime",
    "CallFirstUnitArrivedDateTime",
)
_PULSEPOINT_UNIT_TIME_FIELDS = (
    "UnitOnSceneDateTime",
    "UnitArrivedDateTime",
    "UnitAtSceneDateTime",
    "OnSceneDateTime",
    "ArrivedDateTime",
    "ArrivalDateTime",
    "OnSceneTime",
    "OnSceneTimestamp",
    "Arrived",
)
_PULSEPOINT_UNIT_STATUS_ALIASES = {
    "DP": "DP",
    "DISPATCHED": "DP",
    "DISPATCH": "DP",
    "AK": "AK",
    "ACK": "AK",
    "ACKNOWLEDGED": "AK",
    "ER": "ER",
    "EN ROUTE": "ER",
    "ENROUTE": "ER",
    "SG": "SG",
    "STAGED": "SG",
    "OS": "OS",
    "ON SCENE": "OS",
    "ON-SCENE": "OS",
    "ONSCENE": "OS",
    "AE": "AE",
    "AVAILABLE ON SCENE": "AE",
    "AVAILABLE ONSCENE": "AE",
    "AVAILABLE ON-SCENE": "AE",
    "AVAIL ON SCENE": "AE",
    "TR": "TR",
    "TRANSPORT": "TR",
    "TRANSPORTING": "TR",
    "TA": "TA",
    "TRANSPORT ARRIVED": "TA",
    "TRANSPORT-ARRIVED": "TA",
    "TRANSPORT ARRVD": "TA",
    "AR": "AR",
    "CLEARED": "AR",
    "CLEARED FROM INCIDENT": "AR",
}
_PULSEPOINT_UNIT_ON_SCENE_STATUS = {"OS", "AE"}
_PULSEPOINT_ON_SCENE_KEYWORDS = ("on scene", "onscene", "on-scene")
_PULSEPOINT_UNIT_STRING_RE = re.compile(r"^(.*?)\s*\(([^)]*)\)\s*$")


def _looks_like_pulsepoint_incident(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = {"ID", "FullDisplayAddress", "PulsePointIncidentCallType", "CallReceivedDateTime", "Latitude", "Longitude"}
    return sum(1 for key in keys if key in obj) >= 2


def _parse_incident_coordinate(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    if isinstance(value, str):
        try:
            numeric = float(value.strip())
        except (TypeError, ValueError):
            return None
        return numeric if math.isfinite(numeric) else None
    return None


def _normalize_incident_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _get_incident_identifier(incident: Dict[str, Any]) -> Optional[str]:
    for key in _PULSEPOINT_INCIDENT_ID_KEYS:
        value = incident.get(key)
        if value is None:
            continue
        normalized = _normalize_incident_id(value)
        if normalized:
            return normalized
    return None


def _get_incident_received_value(incident: Dict[str, Any]) -> str:
    for field in _PULSEPOINT_INCIDENT_RECEIVED_FIELDS:
        value = incident.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _derive_incident_lookup_id(incident: Dict[str, Any]) -> str:
    direct = _get_incident_identifier(incident)
    if direct:
        return direct
    lat = _parse_incident_coordinate(
        incident.get("Latitude") or incident.get("latitude") or incident.get("lat")
    )
    lon = _parse_incident_coordinate(
        incident.get("Longitude") or incident.get("longitude") or incident.get("lon")
    )
    if lat is None or lon is None:
        return ""
    received = _get_incident_received_value(incident)
    if received:
        return _normalize_incident_id(f"{lat:.6f}_{lon:.6f}_{received}")
    return _normalize_incident_id(f"{lat:.6f}_{lon:.6f}")


def _normalize_unit_status(value: Any) -> Tuple[str, str]:
    if value is None:
        return "", ""
    raw = str(value).strip()
    if not raw:
        return "", ""
    canonical = _PULSEPOINT_UNIT_STATUS_ALIASES.get(raw.upper(), "")
    return canonical, raw


def _parse_unit_string(text: str) -> Tuple[str, str, str]:
    trimmed = text.strip()
    if not trimmed:
        return "", "", ""
    match = _PULSEPOINT_UNIT_STRING_RE.match(trimmed)
    if match:
        name = match.group(1).strip()
        status = match.group(2).strip()
        return name, status, trimmed
    return trimmed, "", trimmed


def _extract_incident_units(incident: Dict[str, Any]) -> List[Dict[str, Any]]:
    units: List[Dict[str, Any]] = []
    raw_units = incident.get("Unit")
    if isinstance(raw_units, list):
        for entry in raw_units:
            name = ""
            status = ""
            raw_text = ""
            if isinstance(entry, dict):
                for key in ("UnitID", "Unit", "Name", "ApparatusID", "VehicleID"):
                    candidate = entry.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        name = candidate.strip()
                        break
                for key in (
                    "PulsePointDispatchStatus",
                    "DispatchStatus",
                    "Status",
                    "UnitStatus",
                ):
                    candidate = entry.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        status = candidate.strip()
                        break
                if isinstance(entry, str):
                    name, status, raw_text = _parse_unit_string(entry)
            elif isinstance(entry, str):
                name, status, raw_text = _parse_unit_string(entry)
            if not raw_text and isinstance(entry, dict):
                raw_text = entry.get("_raw") if isinstance(entry.get("_raw"), str) else ""
            canonical, raw_status = _normalize_unit_status(status)
            display = name or raw_status or raw_text
            if not display:
                continue
            units.append(
                {
                    "status_key": canonical,
                    "status_label": raw_status,
                    "raw_status": raw_status,
                    "display_text": display,
                }
            )
    if not units:
        string_candidates = [
            incident.get("_units"),
            incident.get("Units"),
            incident.get("Apparatus"),
            incident.get("UnitString"),
        ]
        source = next(
            (value for value in string_candidates if isinstance(value, str) and value.strip()),
            "",
        )
        if source:
            parts = [part.strip() for part in source.split(",") if part.strip()]
            for part in parts:
                name, status, raw_text = _parse_unit_string(part)
                canonical, raw_status = _normalize_unit_status(status)
                display = name or raw_status or raw_text
                if not display:
                    continue
                units.append(
                    {
                        "status_key": canonical,
                        "status_label": raw_status,
                        "raw_status": raw_status,
                        "display_text": display,
                    }
                )
    return units


def _unit_has_on_scene_status(unit: Dict[str, Any]) -> bool:
    status_key = str(unit.get("status_key") or "").strip().upper()
    if status_key in _PULSEPOINT_UNIT_ON_SCENE_STATUS:
        return True
    for field in ("status_label", "raw_status", "display_text"):
        value = unit.get(field)
        if isinstance(value, str) and value:
            lowered = value.lower()
            if any(keyword in lowered for keyword in _PULSEPOINT_ON_SCENE_KEYWORDS):
                return True
    return False


def _incident_has_on_scene_units(incident: Dict[str, Any]) -> bool:
    units = _extract_incident_units(incident)
    if not units:
        return False
    return any(_unit_has_on_scene_status(unit) for unit in units)


def _parse_incident_datetime(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and math.isfinite(value):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            try:
                numeric = float(text)
            except ValueError:
                numeric = None
            if numeric is not None and math.isfinite(numeric):
                seconds = numeric
                if seconds > 1e12:
                    seconds /= 1000.0
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
        candidate = text
        if candidate.endswith("Z") or candidate.endswith("z"):
            candidate = candidate[:-1] + "+00:00"
        match = re.search(r"([+-]\d{2})(\d{2})$", candidate)
        if match and ":" not in match.group(0):
            candidate = f"{candidate[:-5]}{match.group(1)}:{match.group(2)}"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            try:
                parsed = parsedate_to_datetime(text)
            except (TypeError, ValueError):
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed
    return None


def _extract_incident_first_on_scene_date(incident: Dict[str, Any]) -> Optional[datetime]:
    for field in _PULSEPOINT_FIRST_ON_SCENE_FIELDS:
        if field in incident:
            date = _parse_incident_datetime(incident.get(field))
            if date is not None:
                return date
    timeline = incident.get("Timeline") or incident.get("timeline")
    if isinstance(timeline, dict):
        for key, value in timeline.items():
            if not isinstance(key, str):
                continue
            if "scene" not in key.lower():
                continue
            date = _parse_incident_datetime(value)
            if date is not None:
                return date
    units = incident.get("Unit")
    earliest: Optional[datetime] = None
    if isinstance(units, list):
        for unit in units:
            if not isinstance(unit, dict):
                continue
            for field in _PULSEPOINT_UNIT_TIME_FIELDS:
                if field not in unit:
                    continue
                date = _parse_incident_datetime(unit.get(field))
                if date is None:
                    continue
                if earliest is None or date < earliest:
                    earliest = date
    return earliest


def _coerce_timestamp_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = _parse_incident_datetime(value)
        return int(parsed.timestamp() * 1000) if parsed else None
    if isinstance(value, (int, float)) and math.isfinite(value):
        numeric = float(value)
        if numeric > 1e12:
            return int(round(numeric))
        if numeric > 1e9:
            return int(round(numeric * 1000))
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"-?\d+(?:\.\d+)?", text):
            try:
                numeric = float(text)
            except ValueError:
                numeric = None
            if numeric is not None and math.isfinite(numeric):
                if numeric > 1e12:
                    return int(round(numeric))
                if numeric > 1e9:
                    return int(round(numeric * 1000))
        parsed = _parse_incident_datetime(text)
        if parsed is not None:
            return int(parsed.timestamp() * 1000)
    return None


def _extract_existing_first_on_scene(incident: Dict[str, Any]) -> Tuple[Optional[int], str]:
    source_candidates = (
        incident.get("_firstOnSceneTimestampSource"),
        incident.get("firstOnSceneTimestampSource"),
        incident.get("FirstOnSceneTimestampSource"),
    )
    source = next(
        (str(value).strip() for value in source_candidates if isinstance(value, str) and value.strip()),
        "",
    )
    ts_candidates = (
        incident.get("_firstOnSceneTimestamp"),
        incident.get("firstOnSceneTimestamp"),
        incident.get("FirstOnSceneTimestamp"),
    )
    for candidate in ts_candidates:
        ts = _coerce_timestamp_ms(candidate)
        if ts is not None:
            return ts, source
    return None, source


def _iter_pulsepoint_incidents(root: Any) -> Iterable[Dict[str, Any]]:
    seen: Set[int] = set()

    def _dig(node: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(node, dict):
            if id(node) in seen:
                return
            if _looks_like_pulsepoint_incident(node):
                seen.add(id(node))
                yield node
            for value in node.values():
                yield from _dig(value)
        elif isinstance(node, list):
            for item in node:
                yield from _dig(item)

    return _dig(root)


async def _update_pulsepoint_first_on_scene(payload: Any) -> None:
    incidents = list(_iter_pulsepoint_incidents(payload))
    seen_ids: Set[str] = set()
    now_ms = int(time.time() * 1000)
    async with PULSEPOINT_FIRST_ON_SCENE_LOCK:
        for incident in incidents:
            if not isinstance(incident, dict):
                continue
            incident_id = _derive_incident_lookup_id(incident)
            if not incident_id:
                continue
            seen_ids.add(incident_id)
            has_on_scene = _incident_has_on_scene_units(incident)
            data_date = _extract_incident_first_on_scene_date(incident)
            existing_entry = PULSEPOINT_FIRST_ON_SCENE.get(incident_id)
            timestamp = existing_entry.get("timestamp") if isinstance(existing_entry, dict) else None
            source = existing_entry.get("source") if isinstance(existing_entry, dict) else ""
            server_ts, server_source = _extract_existing_first_on_scene(incident)
            if server_ts is not None:
                prefer_server = (
                    timestamp is None
                    or source != "data"
                    or (server_source == "data" and (timestamp is None or server_ts <= timestamp))
                )
                if prefer_server:
                    timestamp = server_ts
                    source = server_source or source or ""
            if data_date is not None:
                data_ms = int(data_date.timestamp() * 1000)
                if timestamp is None or data_ms < timestamp or source != "data":
                    timestamp = data_ms
                    source = "data"
            if has_on_scene:
                if timestamp is None:
                    timestamp = now_ms
                    source = "observed"
                PULSEPOINT_FIRST_ON_SCENE[incident_id] = {"timestamp": int(timestamp), "source": source}
                incident["_firstOnSceneTimestamp"] = int(timestamp)
                if source:
                    incident["_firstOnSceneTimestampSource"] = source
                else:
                    incident.pop("_firstOnSceneTimestampSource", None)
            else:
                PULSEPOINT_FIRST_ON_SCENE.pop(incident_id, None)
                incident.pop("_firstOnSceneTimestamp", None)
                incident.pop("_firstOnSceneTimestampSource", None)
        stale_ids = [key for key in list(PULSEPOINT_FIRST_ON_SCENE.keys()) if key not in seen_ids]
        for key in stale_ids:
            PULSEPOINT_FIRST_ON_SCENE.pop(key, None)


def _derive_aes_key_iv(password: bytes, salt: bytes, key_len: int = 32, iv_len: int = 16) -> Tuple[bytes, bytes]:
    d = b""
    prev = b""
    while len(d) < key_len + iv_len:
        prev = hashlib.md5(prev + password + salt).digest()
        d += prev
    key = d[:key_len]
    iv = d[key_len:key_len + iv_len]
    return key, iv


def _decrypt_pulsepoint_payload(payload: Dict[str, Any]) -> Any:
    if not isinstance(payload, dict):
        return payload
    cipher_text = payload.get("ct")
    if not cipher_text:
        return payload
    try:
        ciphertext_bytes = base64.b64decode(cipher_text)
    except Exception:
        return payload
    salt_hex = payload.get("s")
    salt = bytes.fromhex(salt_hex) if isinstance(salt_hex, str) else b""
    iv_hex = payload.get("iv")
    pass_bytes = PULSEPOINT_PASSPHRASE.encode("utf-8")
    key, derived_iv = _derive_aes_key_iv(pass_bytes, salt)
    iv = bytes.fromhex(iv_hex) if isinstance(iv_hex, str) else derived_iv
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    try:
        decrypted = cipher.decrypt(ciphertext_bytes)
        plaintext = unpad(decrypted, AES.block_size)
    except Exception:
        return payload
    text = plaintext.decode("utf-8", errors="ignore")
    parsed: Any = text
    for _ in range(3):
        if isinstance(parsed, str):
            try:
                parsed = json.loads(parsed)
                continue
            except Exception:
                break
        break
    return parsed


async def _get_pulsepoint_incidents() -> Any:
    async def fetch():
        async with httpx.AsyncClient() as client:
            resp = await client.get(PULSEPOINT_ENDPOINT, timeout=20)
            record_api_call("GET", str(resp.request.url), resp.status_code)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                return data
        payload = _decrypt_pulsepoint_payload(data)
        await _update_pulsepoint_first_on_scene(payload)
        return payload

    return await pulsepoint_cache.get(fetch)


async def _proxy_pulsepoint_icon(icon_path: str) -> Response:
    normalised = icon_path.lstrip("/\\").strip()
    if not normalised:
        raise HTTPException(status_code=404, detail="Icon path not found")

    async def fetch():
        url = f"{PULSEPOINT_ICON_BASE}{normalised}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=20)
            record_api_call("GET", str(resp.request.url), resp.status_code)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type") or "application/octet-stream"
            return {"content": resp.content, "content_type": content_type}

    try:
        icon = await pulsepoint_icon_cache.get(normalised, fetch)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code, detail="PulsePoint icon fetch failed"
        ) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail="PulsePoint icon fetch failed") from exc
    return Response(content=icon["content"], media_type=icon["content_type"])


def _train_includes_station(train: Dict[str, Any], station: str) -> bool:
    if not station:
        return True
    stations = train.get("stations")
    if not isinstance(stations, list):
        return False
    target = station.upper()
    for stop in stations:
        if isinstance(stop, dict):
            code = stop.get("code") or stop.get("Code")
            if code and str(code).strip().upper() == target:
                return True
    return False


def _filter_trains_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    if not TRAIN_TARGET_STATION_CODE:
        return payload
    filtered: Dict[str, Any] = {}
    for key, value in payload.items():
        if not isinstance(value, list):
            continue
        subset = [train for train in value if isinstance(train, dict) and _train_includes_station(train, TRAIN_TARGET_STATION_CODE)]
        if subset:
            filtered[key] = subset
    return filtered


async def _get_amtraker_trains() -> Any:
    async def fetch():
        async with httpx.AsyncClient() as client:
            resp = await client.get(AMTRAKER_URL, timeout=20)
            record_api_call("GET", str(resp.request.url), resp.status_code)
            resp.raise_for_status()
            data = resp.json()
        return _filter_trains_payload(data)

    return await amtraker_cache.get(fetch)


async def _fetch_ridesystems_clients() -> List[Dict[str, str]]:
    async def fetch():
        async with httpx.AsyncClient() as client:
            resp = await client.get(RIDESYSTEMS_CLIENTS_URL, timeout=20)
            record_api_call("GET", str(resp.request.url), resp.status_code)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "").lower()
            if "application/json" in content_type:
                payload = resp.json()
                clients = payload if isinstance(payload, list) else payload.get("clients") or []
                items = []
                for entry in clients:
                    if not isinstance(entry, dict):
                        continue
                    name = (entry.get("Name") or entry.get("name") or "").strip()
                    web = (entry.get("WebAddress") or entry.get("webAddress") or "").strip()
                    if not name or not web:
                        continue
                    items.append({"name": name, "url": web})
                return items
            text = resp.text
            items: List[Dict[str, str]] = []
            try:
                root = ET.fromstring(text)
                for client_el in root.findall(".//Client"):
                    name_el = client_el.find("Name")
                    url_el = client_el.find("WebAddress")
                    name = name_el.text.strip() if name_el is not None and name_el.text else ""
                    web = url_el.text.strip() if url_el is not None and url_el.text else ""
                    if name and web:
                        items.append({"name": name, "url": web})
            except ET.ParseError:
                return []
            return items

    data = await ridesystems_clients_cache.get(fetch)
    # Normalise URLs to https
    normalised: List[Dict[str, str]] = []
    for entry in data:
        name = entry.get("name", "").strip()
        url = entry.get("url", "").strip()
        if not name or not url:
            continue
        if not url.startswith("http"):
            url = f"https://{url}"
        url = re.sub(r"^http://", "https://", url, flags=re.IGNORECASE)
        normalised.append({"name": name, "url": url})
    normalised.sort(key=lambda x: x["name"].lower())
    return normalised


async def _proxy_pulsepoint_incidents():
    return await _get_pulsepoint_incidents()


@app.get("/v1/pulsepoint/respond_icons/{icon_path:path}")
async def pulsepoint_icon_proxy(icon_path: str):
    return await _proxy_pulsepoint_icon(icon_path)


@app.get("/v1/pulsepoint/incidents")
async def pulsepoint_proxy():
    return await _proxy_pulsepoint_incidents()


@app.get("/v1/testmap/pulsepoint")
async def pulsepoint_endpoint():
    return await _proxy_pulsepoint_incidents()


@app.get("/v1/testmap/trains")
async def trains_endpoint():
    return await _get_amtraker_trains()


@app.get("/v1/testmap/ridesystems/clients")
async def ridesystems_clients_endpoint():
    clients = await _fetch_ridesystems_clients()
    return {"clients": clients}


@app.get("/v1/transloc/routes_with_shapes")
async def transloc_routes_with_shapes(base_url: Optional[str] = Query(None)):
    try:
        async with httpx.AsyncClient() as client:
            return await fetch_routes_with_shapes(client, base_url=base_url)
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url)
        raise HTTPException(status_code=502, detail=detail) from exc


@app.get("/v1/transloc/ridership")
async def transloc_ridership(
    startDate: str = Query(..., description="MM/DD/YYYY start date"),
    endDate: str = Query(..., description="MM/DD/YYYY end date"),
    base_url: Optional[str] = Query(None),
):
    url = build_transloc_url(base_url, "GetRidershipData")
    params = {"startDate": startDate, "endDate": endDate}
    return await _proxy_transloc_get(url, params=params, base_url=base_url)


@app.get("/v1/transloc/alerts")
async def transloc_alerts(
    showInactive: bool = Query(False),
    includeDeleted: bool = Query(False),
    messageTypeId: int = Query(1),
    search: bool = Query(False),
    rows: int = Query(10),
    page: int = Query(1),
    sortIndex: str = Query("StartDateUtc"),
    sortOrder: str = Query("asc"),
    base_url: Optional[str] = Query(None),
):
    root = transloc_host_base(base_url)
    url = f"{root}/Secure/Services/RoutesService.svc/GetMessagesPaged"
    params = {
        "showInactive": showInactive,
        "includeDeleted": includeDeleted,
        "messageTypeId": messageTypeId,
        "search": search,
        "rows": rows,
        "page": page,
        "sortIndex": sortIndex,
        "sortOrder": sortOrder,
    }
    return await _proxy_transloc_get(url, params=params, base_url=base_url)


@app.get("/v1/transloc/client_logo")
async def transloc_client_logo(base_url: Optional[str] = Query(None)):
    root = transloc_host_base(base_url)
    url = f"{root}/Images/clientLogo.jpg"
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=20)
            record_api_call("GET", str(r.url), r.status_code)
            r.raise_for_status()
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url or url)
        raise HTTPException(status_code=502, detail=detail) from exc
    content_type = r.headers.get("content-type") or "image/jpeg"
    return Response(content=r.content, media_type=content_type)


@app.get("/v1/transloc/stop_arrivals")
async def transloc_stop_arrivals(
    stopIDs: Optional[str] = Query(None, description="Comma-separated stop IDs"),
    stops: Optional[str] = Query(None, description="Alias for stopIDs"),
    base_url: Optional[str] = Query(None),
):
    url = build_transloc_url(base_url, "GetStopArrivalTimes")
    params: Dict[str, Any] = {"APIKey": TRANSLOC_KEY}
    if stopIDs:
        params["stopIDs"] = stopIDs
    if stops:
        params["stops"] = stops
    return await _proxy_transloc_get(url, params=params, base_url=base_url)


@app.get("/v1/transloc/vehicle_capacities")
async def transloc_vehicle_capacities(base_url: Optional[str] = Query(None)):
    url = build_transloc_url(base_url, "GetVehicleCapacities")
    params = {"APIKey": TRANSLOC_KEY}
    return await _proxy_transloc_get(url, params=params, base_url=base_url)

@app.get("/v1/transloc/vehicle_route_stop_estimates")
async def transloc_vehicle_route_stop_estimates(
    quantity: int = Query(3, ge=1, le=10),
    vehicle_ids: str = Query(..., alias="vehicleIdStrings"),
    base_url: Optional[str] = Query(None)
):
    """
    Proxy for TransLoc GetVehicleRouteStopEstimates endpoint.
    Returns ETA estimates for specified vehicles.

    Args:
        quantity: Number of stops to return per vehicle (1-10, default 3)
        vehicle_ids: Comma-separated list of vehicle IDs
        base_url: Optional TransLoc base URL override
    """
    url = build_transloc_url(base_url, "GetVehicleRouteStopEstimates")
    params = {
        "APIKey": TRANSLOC_KEY,
        "quantity": quantity,
        "vehicleIdStrings": vehicle_ids
    }
    return await _proxy_transloc_get(url, params=params, base_url=base_url)

@app.get("/v1/transloc/routes")
async def transloc_routes():
    async with state.lock:
        return getattr(state, "routes_raw", [])

@app.get("/v1/transloc/anti_bunching")
async def anti_bunching_raw():
    async with state.lock:
        now = time.time()
        if state.anti_cache and now - state.anti_cache_ts < VEH_REFRESH_S:
            return state.anti_cache
    try:
        async with httpx.AsyncClient() as client:
            url = f"{TRANSLOC_BASE}/GetAntiBunching"
            r = await client.get(url, timeout=20)
            record_api_call("GET", url, r.status_code)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, TRANSLOC_BASE)
        raise HTTPException(status_code=502, detail=detail) from exc
    async with state.lock:
        state.anti_cache = data
        state.anti_cache_ts = time.time()
    return data


def _compute_anti_bunching_status(routes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute anti-bunching system status from route data.

    Status rules:
    - N/A: No eligible routes (routes with 2+ assigned vehicles)
    - ONLINE: At least one eligible route AND at least one unit has guidance
    - OFFLINE: At least one eligible route AND zero units have guidance

    Returns a dict with status, eligible_routes, eligible_units, units_with_guidance.
    """
    eligible_routes = 0
    eligible_units = 0
    units_with_guidance = 0

    for route in routes:
        assigned = route.get("AssignedVehicles", 0)
        # A route is eligible if it has 2+ assigned vehicles
        if assigned >= 2:
            eligible_routes += 1
            eligible_units += assigned
            # Count vehicles that have guidance entries
            guidance_entries = route.get("VehicleAntiBunching", [])
            if isinstance(guidance_entries, list):
                units_with_guidance += len(guidance_entries)

    # Determine raw status (before anti-flap protection)
    if eligible_routes == 0:
        raw_status = "N/A"
    elif units_with_guidance > 0:
        raw_status = "ONLINE"
    else:
        raw_status = "OFFLINE"

    return {
        "raw_status": raw_status,
        "eligible_routes": eligible_routes,
        "eligible_units": eligible_units,
        "units_with_guidance": units_with_guidance,
    }


@app.get("/v1/transloc/anti_bunching/status")
async def anti_bunching_status(request: Request):
    """Get anti-bunching system status indicator.

    Requires dispatcher authentication.

    Returns a derived status value:
    - ONLINE: Anti-bunching is producing guidance for eligible routes
    - OFFLINE: Eligible routes exist but no guidance is being produced
    - N/A: No eligible routes (routes with 2+ vehicles in service)

    Anti-flap protection:
    - Requires 2 consecutive failed evaluations before switching to OFFLINE
    - Requires 1 successful evaluation to return to ONLINE
    """
    _require_dispatcher_access(request)
    # Fetch fresh anti-bunching data (uses internal cache)
    try:
        routes = await anti_bunching_raw()
        if not isinstance(routes, list):
            routes = []
    except HTTPException:
        # API error - treat as potential OFFLINE condition
        routes = []

    result = _compute_anti_bunching_status(routes)
    raw_status = result["raw_status"]
    now = datetime.now(timezone.utc)
    now_ts = time.time()

    # Apply anti-flap protection
    async with state.lock:
        prev_status = state.anti_bunching_last_status

        if raw_status == "N/A":
            # N/A is always immediate - no flap protection needed
            final_status = "N/A"
            state.anti_bunching_consecutive_failures = 0
        elif raw_status == "ONLINE":
            # Success: immediately return to ONLINE (1 success required)
            final_status = "ONLINE"
            state.anti_bunching_consecutive_failures = 0
        else:
            # raw_status == "OFFLINE"
            # Increment consecutive failures
            state.anti_bunching_consecutive_failures += 1
            if state.anti_bunching_consecutive_failures >= 2:
                # 2 consecutive failures: switch to OFFLINE
                final_status = "OFFLINE"
            else:
                # First failure: maintain previous status (unless N/A)
                if prev_status == "N/A":
                    final_status = "OFFLINE"
                else:
                    final_status = prev_status

        state.anti_bunching_last_status = final_status
        state.anti_bunching_status_ts = now_ts

    return {
        "status": final_status,
        "eligible_routes": result["eligible_routes"],
        "eligible_units": result["eligible_units"],
        "units_with_guidance": result["units_with_guidance"],
        "evaluated_at": now.isoformat(),
    }


# ---------------------------
# REST: UTS Service Level
# ---------------------------

# Use curl_cffi with browser impersonation to bypass TLS fingerprinting
# parking.virginia.edu uses protection that blocks non-browser TLS fingerprints
try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False


async def _fetch_service_level(bypass_cache: bool = False) -> ServiceLevelResult:
    """
    Fetch and parse the service level from parking.virginia.edu.

    Uses curl_cffi with Chrome impersonation to bypass TLS fingerprinting.

    Args:
        bypass_cache: If True, skip HTTP cache headers and force a fresh fetch

    Returns:
        ServiceLevelResult with current service level data
    """
    service_date = get_service_date()
    service_date_str = service_date.isoformat()
    now_ts = time.time()

    async with state.service_level_lock:
        cache = state.service_level_cache

        # Check if we have a valid cached result for current service date
        if not bypass_cache and cache.result is not None:
            if cache.result.service_date == service_date_str:
                # Cache hit for current service day
                return cache.result

        # Build request headers for conditional fetch
        headers = {}
        if not bypass_cache:
            if cache.etag:
                headers["If-None-Match"] = cache.etag
            if cache.last_modified:
                headers["If-Modified-Since"] = cache.last_modified

        try:
            if not CURL_CFFI_AVAILABLE:
                raise ImportError("curl_cffi not available")

            # Use curl_cffi with Chrome impersonation for TLS fingerprint bypass
            async with CurlAsyncSession() as session:
                resp = await session.get(
                    SERVICE_SCHEDULE_URL,
                    headers=headers if headers else None,
                    impersonate="chrome",
                    timeout=20,
                )
                record_api_call("GET", SERVICE_SCHEDULE_URL, resp.status_code)

                if resp.status_code == 304:
                    # Not modified - return cached result if valid for current date
                    if cache.result and cache.result.service_date == service_date_str:
                        return cache.result
                    # Cache is for a different date, need to re-fetch without cache headers
                    resp = await session.get(
                        SERVICE_SCHEDULE_URL,
                        impersonate="chrome",
                        timeout=20,
                    )
                    record_api_call("GET", SERVICE_SCHEDULE_URL, resp.status_code)

                if resp.status_code != 200:
                    error_msg = f"HTTP {resp.status_code} from {SERVICE_SCHEDULE_URL}"
                    # Return cached value if available for current date
                    if cache.result and cache.result.service_date == service_date_str:
                        return cache.result
                    return ServiceLevelResult(
                        service_date=service_date_str,
                        service_level="UNKNOWN",
                        notes=None,
                        scraped_at=datetime.now(UVA_TZ).isoformat(),
                        error=error_msg,
                    )

                html = resp.text
                result = parse_service_schedule(html, service_date)

                # Update cache
                cache.result = result
                cache.etag = resp.headers.get("ETag")
                cache.last_modified = resp.headers.get("Last-Modified")
                cache.fetched_at = now_ts
                state.service_level_cache = cache

                return result

        except ImportError as exc:
            # curl_cffi not available, fall back to error
            error_msg = f"curl_cffi library not available: {exc}"
            if cache.result and cache.result.service_date == service_date_str:
                return cache.result
            return ServiceLevelResult(
                service_date=service_date_str,
                service_level="UNKNOWN",
                notes=None,
                scraped_at=datetime.now(UVA_TZ).isoformat(),
                error=error_msg,
            )
        except Exception as exc:
            error_msg = f"Error fetching service schedule: {exc}"
            if cache.result and cache.result.service_date == service_date_str:
                return cache.result
            return ServiceLevelResult(
                service_date=service_date_str,
                service_level="UNKNOWN",
                notes=None,
                scraped_at=datetime.now(UVA_TZ).isoformat(),
                error=error_msg,
            )


@app.get("/v1/uts/service_level")
async def uts_service_level(request: Request):
    """Get the current UTS service level.

    Public endpoint.

    Returns:
        JSON with service_date, service_level, notes, source_url, scraped_at, source_hash.
        If unable to determine, service_level will be "UNKNOWN" with an error field.
    """
    result = await _fetch_service_level(bypass_cache=False)
    return result.to_dict()


@app.post("/v1/uts/service_level/refresh")
async def uts_service_level_refresh(request: Request):
    """Force refresh the UTS service level from the source page.

    Requires dispatcher authentication.
    Bypasses cache and re-scrapes the service schedule.
    Useful when the webpage is updated unexpectedly during the day.

    Returns:
        Same response shape as GET /v1/uts/service_level
    """
    _require_dispatcher_access(request)
    result = await _fetch_service_level(bypass_cache=True)
    return result.to_dict()


# ---------------------------
# REST: On-Duty Personnel
# ---------------------------

# Position codes for on-duty personnel lookup
ON_DUTY_POSITION_CODES = {
    "Sup": "Supervisor",
    "OnDemand Dispatch": "OnDemand Dispatcher",
}


def _get_current_and_next_shifts(
    all_shifts: List[Dict[str, Any]],
    now_ts: int,
    near_end_threshold_ms: int = 30 * 60 * 1000,
    transition_gap_ms: int = 2 * 60 * 1000,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Determine current and next shifts from a list of all shifts.

    Args:
        all_shifts: List of shift dicts with start_ts and end_ts
        now_ts: Current timestamp in milliseconds
        near_end_threshold_ms: Show next if current ends within this time (default 30 min)
        transition_gap_ms: Next must start within this time of current ending (default 2 min)

    Returns:
        Tuple of (current_shifts, next_shifts_to_show)
    """
    current = []
    future = []

    for shift in all_shifts:
        start_ts = shift.get("start_ts", 0)
        end_ts = shift.get("end_ts", 0)
        if start_ts <= now_ts < end_ts:
            current.append(shift)
        elif start_ts > now_ts:
            future.append(shift)

    # Sort future by start time to get the next one
    future.sort(key=lambda x: x.get("start_ts", 0))
    next_up = future[:1] if future else []

    # Determine if we should show next
    show_next = False
    if not current and next_up:
        # No one on duty, show next
        show_next = True
    elif current and next_up:
        # Check if ANY current shift ends within threshold AND next starts near that ending
        soonest_end = min(s.get("end_ts", 0) for s in current)
        next_start = next_up[0].get("start_ts", 0)
        if (soonest_end - now_ts) <= near_end_threshold_ms:
            if abs(next_start - soonest_end) <= transition_gap_ms:
                show_next = True

    return current, next_up if show_next else []


async def _fetch_on_duty_personnel() -> Dict[str, Any]:
    """
    Fetch on-duty personnel from W2W assignments.

    Returns supervisors and OnDemand dispatchers currently on shift,
    plus upcoming shifts when applicable:
    - If no one is currently on duty, shows the next scheduled person
    - If within 30 min of shift end AND next shift starts at that time, shows transition
    """
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    now_ts = int(now.timestamp() * 1000)

    try:
        w2w_data = await w2w_assignments_cache.get(_fetch_w2w_assignments)
        if w2w_data.get("disabled"):
            return {
                "supervisors": [],
                "supervisors_next": [],
                "ondemand_dispatchers": [],
                "ondemand_dispatchers_next": [],
                "fetched_at": now.isoformat(),
                "disabled": True,
            }
    except Exception as exc:
        print(f"[on_duty] w2w fetch failed: {exc}")
        return {
            "supervisors": [],
            "supervisors_next": [],
            "ondemand_dispatchers": [],
            "ondemand_dispatchers_next": [],
            "fetched_at": now.isoformat(),
            "error": str(exc),
        }

    # Parse the raw W2W shift data to find Sup and OnDemand Dispatch positions
    # Collect ALL shifts for the day (not just currently active)
    all_supervisors = []
    all_dispatchers = []

    # Fetch fresh W2W data with raw shifts
    try:
        if not W2W_KEY:
            return {
                "supervisors": [],
                "supervisors_next": [],
                "ondemand_dispatchers": [],
                "ondemand_dispatchers_next": [],
                "fetched_at": now.isoformat(),
                "disabled": True,
            }

        service_day = now
        if now.time() < dtime(hour=2, minute=30):
            service_day = now - timedelta(days=1)

        params = {
            "start_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
            "end_date": f"{service_day.month}/{service_day.day}/{service_day.year}",
            "key": W2W_KEY,
        }
        url = httpx.URL(W2W_ASSIGNED_SHIFT_URL)
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=20)
        record_api_call("GET", str(httpx.URL(str(url), params={**params, "key": "***"})), response.status_code)
        response.raise_for_status()
        payload = response.json()

        shifts = []
        if isinstance(payload, dict):
            raw_shifts = payload.get("AssignedShiftList")
            if isinstance(raw_shifts, list):
                shifts = raw_shifts

        for shift in shifts:
            if not isinstance(shift, dict):
                continue
            position_name = shift.get("POSITION_NAME", "")
            if not position_name:
                continue

            # Check if this is a Sup or OnDemand Dispatch position
            position_key = None
            for code in ON_DUTY_POSITION_CODES:
                if position_name.strip() == code:
                    position_key = code
                    break

            if not position_key:
                continue

            # Parse shift times
            start_dt = _parse_w2w_datetime(shift.get("START_DATE"), shift.get("START_TIME"), tz)
            if start_dt is None:
                continue
            end_dt = _parse_w2w_datetime(shift.get("END_DATE"), shift.get("END_TIME"), tz)
            if end_dt is None:
                duration_hours = _parse_duration_hours(shift.get("DURATION"))
                if duration_hours:
                    end_dt = start_dt + timedelta(hours=duration_hours)
            if end_dt is None:
                continue
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)

            # Skip shifts that have already ended
            start_ts = int(start_dt.timestamp() * 1000)
            end_ts = int(end_dt.timestamp() * 1000)
            if end_ts <= now_ts:
                continue

            # Skip shifts with COLOR_ID 9 (no-show)
            color_id = str(shift.get("COLOR_ID", "")).strip()
            if color_id == "9":
                continue

            # Build person entry with timestamps for sorting
            first = str(shift.get("FIRST_NAME") or "").strip()
            last = str(shift.get("LAST_NAME") or "").strip()
            name = (first + " " + last).strip() or "OPEN"

            person = {
                "name": name,
                "start": start_dt.strftime("%H:%M"),
                "end": end_dt.strftime("%H:%M"),
                "start_ts": start_ts,
                "end_ts": end_ts,
            }

            if position_key == "Sup":
                all_supervisors.append(person)
            elif position_key == "OnDemand Dispatch":
                all_dispatchers.append(person)

        # Determine current and next for each position type
        current_sups, next_sups = _get_current_and_next_shifts(all_supervisors, now_ts)
        current_disps, next_disps = _get_current_and_next_shifts(all_dispatchers, now_ts)

        # Remove internal timestamps from output
        def clean_person(p):
            return {"name": p["name"], "start": p["start"], "end": p["end"]}

        return {
            "supervisors": [clean_person(p) for p in current_sups],
            "supervisors_next": [clean_person(p) for p in next_sups],
            "ondemand_dispatchers": [clean_person(p) for p in current_disps],
            "ondemand_dispatchers_next": [clean_person(p) for p in next_disps],
            "fetched_at": now.isoformat(),
        }

    except Exception as exc:
        print(f"[on_duty] raw shift fetch failed: {exc}")
        return {
            "supervisors": [],
            "supervisors_next": [],
            "ondemand_dispatchers": [],
            "ondemand_dispatchers_next": [],
            "fetched_at": now.isoformat(),
            "error": str(exc),
        }


@app.get("/v1/uts/on_duty")
async def uts_on_duty(request: Request):
    """Get on-duty supervisors and dispatchers.

    Requires dispatcher authentication.

    Returns:
        JSON with supervisors and ondemand_dispatchers arrays.
        Each entry has name, start time, and end time.
    """
    _require_dispatcher_access(request)
    return await _fetch_on_duty_personnel()


# ---------------------------
# REST: Low clearances
# ---------------------------

@app.get("/v1/low_clearances")
async def low_clearances():
    global LOW_CLEARANCES_CACHE
    if LOW_CLEARANCES_CACHE is None:
        try:
            async with httpx.AsyncClient() as client:
                LOW_CLEARANCES_CACHE = await fetch_low_clearances(client)
        except Exception as e:
            raise HTTPException(502, f"overpass error: {e}")
    return {"clearances": LOW_CLEARANCES_CACHE}

# ---------------------------
# SSE: External API calls
# ---------------------------
@app.get("/v1/stream/api_calls")
async def stream_api_calls():
    async def gen():
        q: asyncio.Queue = asyncio.Queue(maxsize=10)  # Limit queue to prevent memory bloat
        API_CALL_SUBS.add(q)
        try:
            for item in list(API_CALL_LOG):
                yield f"data: {json.dumps(item)}\n\n"
            # Stream updates as they come (pre-encoded by record_api_call)
            while True:
                encoded = await q.get()
                yield encoded
        finally:
            API_CALL_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------------------------
# SSE: Testmap vehicle updates
# ---------------------------
@app.get("/v1/stream/testmap/vehicles")
async def stream_testmap_vehicles():
    """SSE stream for testmap vehicle position updates.

    Broadcasts vehicle updates whenever the background updater refreshes data.
    This replaces polling for clients that support SSE.
    """
    async def gen():
        q: asyncio.Queue = asyncio.Queue(maxsize=10)  # Limit queue to prevent memory bloat
        TESTMAP_VEHICLES_SUBS.add(q)
        try:
            # Send current state immediately on connect
            current_payload = state.testmap_vehicles_payload
            if current_payload:
                # Filter out hour+ old vehicles for SSE stream (default behavior)
                filtered = [v for v in current_payload if not v.get("IsVeryStale")]
                initial = {"ts": int(time.time() * 1000), "vehicles": filtered}
                yield f"data: {json.dumps(initial)}\n\n"
            # Then stream updates as they come (pre-encoded by broadcast function)
            while True:
                encoded = await q.get()
                yield encoded
        finally:
            TESTMAP_VEHICLES_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------------------------
# Static assets
# ---------------------------

def _serve_js_asset(name: str) -> FileResponse:
    return FileResponse(SCRIPT_DIR / name, media_type="application/javascript")


def _serve_css_asset(name: str) -> FileResponse:
    return FileResponse(CSS_DIR / name, media_type="text/css")


@app.get("/FGDC.ttf", include_in_schema=False)
async def fgdc_font():
    return FileResponse(FONT_DIR / "FGDC.ttf", media_type="font/ttf")


@app.get("/fonts/FGDC.ttf", include_in_schema=False)
async def fgdc_font_nested():
    return FileResponse(FONT_DIR / "FGDC.ttf", media_type="font/ttf")


@app.get("/ANTONIO.ttf", include_in_schema=False)
async def antonio_font():
    return FileResponse(FONT_DIR / "ANTONIO.ttf", media_type="font/ttf")


@app.get("/centurygothic.ttf", include_in_schema=False)
async def centurygothic_font():
    return FileResponse(FONT_DIR / "centurygothic.ttf", media_type="font/ttf")


@app.get("/busmarker.svg", include_in_schema=False)
async def busmarker_svg():
    return FileResponse(MEDIA_DIR / "busmarker.svg", media_type="image/svg+xml")


@app.get("/radar.wav", include_in_schema=False)
async def radar_wav():
    return FileResponse(MEDIA_DIR / "radar.wav", media_type="audio/wav")


@app.get("/UTSShield.png", include_in_schema=False)
async def headwayguard_icon():
    return FileResponse(MEDIA_DIR / "UTSShield.png", media_type="image/png")


_MEDIA_ASSETS: dict[str, str] = {
    "home.svg": "image/svg+xml",
    "driver.svg": "image/svg+xml",
    "dispatcher.svg": "image/svg+xml",
    "servicecrew.svg": "image/svg+xml",
    "map.svg": "image/svg+xml",
    "ridership.svg": "image/svg+xml",
    "headway.svg": "image/svg+xml",
    "replay.svg": "image/svg+xml",
    "downed.svg": "image/svg+xml",
    "testmap.svg": "image/svg+xml",
    "transloc.svg": "image/svg+xml",
    "CATlogo.png": "image/png",
    "apple-touch-icon-120.png": "image/png",
    "apple-touch-icon-152.png": "image/png",
    "apple-touch-icon-180.png": "image/png",
    "KidsCheering.mp3": "audio/mpeg",
    "favicon.ico": "image/x-icon",
    "favicon.svg": "image/svg+xml",
    "favicon-96x96.png": "image/png",
    "icon-192.png": "image/png",
    "icon-512.png": "image/png",
    "icon-maskable-192.png": "image/png",
    "icon-maskable-512.png": "image/png",
    "notification-badge.png": "image/png",
    "web.svg": "image/svg+xml",
}


@app.get("/media/{asset_name}", include_in_schema=False)
async def media_asset(asset_name: str):
    media_type = _MEDIA_ASSETS.get(asset_name)
    if media_type is None:
        raise HTTPException(status_code=404, detail="Media asset not found")
    path = MEDIA_DIR / asset_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Media asset not found")
    return FileResponse(path, media_type=media_type)


def _arrival_sound_files(subdir: Optional[str] = None) -> list[str]:
    """List MP3 files in the arrivalsounds directory or a subdirectory."""
    target_dir = ARRIVAL_SOUNDS_DIR
    if subdir:
        target_dir = ARRIVAL_SOUNDS_DIR / subdir

    if not target_dir.exists():
        return []

    files: list[str] = []
    for entry in sorted(target_dir.iterdir()):
        if not entry.is_file():
            continue

        name = entry.name
        lower = name.lower()
        if lower == "afile":
            continue
        if not lower.endswith(".mp3"):
            continue

        files.append(name)

    return files


@app.get("/media/arrivalsounds/", include_in_schema=False)
async def arrival_sounds_listing(request: Request):
    files = _arrival_sound_files()
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"files": files})

    links = "\n".join(
        f'<a href="{quote(name)}">{name}</a><br>'
        for name in files
    ) or "<p>No sounds available</p>"
    return HTMLResponse(content=f"<html><body>{links}</body></html>")


@app.get("/media/arrivalsounds/{filename}", include_in_schema=False)
async def arrival_sound_file(filename: str):
    safe_name = Path(filename).name
    lower = safe_name.lower()
    if lower == "afile" or not lower.endswith(".mp3"):
        raise HTTPException(status_code=404, detail="Media asset not found")

    path = ARRIVAL_SOUNDS_DIR / safe_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Media asset not found")

    return FileResponse(path, media_type="audio/mpeg")


@app.get("/media/arrivalsounds/passthrough/", include_in_schema=False)
async def arrival_sounds_passthrough_listing(request: Request):
    """List MP3 files in the passthrough subfolder."""
    files = _arrival_sound_files("passthrough")
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"files": files})

    links = "\n".join(
        f'<a href="{quote(name)}">{name}</a><br>'
        for name in files
    ) or "<p>No sounds available</p>"
    return HTMLResponse(content=f"<html><body>{links}</body></html>")


@app.get("/media/arrivalsounds/passthrough/{filename}", include_in_schema=False)
async def arrival_sound_passthrough_file(filename: str):
    """Serve an MP3 file from the passthrough subfolder."""
    safe_name = Path(filename).name
    lower = safe_name.lower()
    if lower == "afile" or not lower.endswith(".mp3"):
        raise HTTPException(status_code=404, detail="Media asset not found")

    path = ARRIVAL_SOUNDS_DIR / "passthrough" / safe_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Media asset not found")

    return FileResponse(path, media_type="audio/mpeg")


@app.get("/media/arrivalsounds/stopped/", include_in_schema=False)
async def arrival_sounds_stopped_listing(request: Request):
    """List MP3 files in the stopped subfolder."""
    files = _arrival_sound_files("stopped")
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"files": files})

    links = "\n".join(
        f'<a href="{quote(name)}">{name}</a><br>'
        for name in files
    ) or "<p>No sounds available</p>"
    return HTMLResponse(content=f"<html><body>{links}</body></html>")


@app.get("/media/arrivalsounds/stopped/{filename}", include_in_schema=False)
async def arrival_sound_stopped_file(filename: str):
    """Serve an MP3 file from the stopped subfolder."""
    safe_name = Path(filename).name
    lower = safe_name.lower()
    if lower == "afile" or not lower.endswith(".mp3"):
        raise HTTPException(status_code=404, detail="Media asset not found")

    path = ARRIVAL_SOUNDS_DIR / "stopped" / safe_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Media asset not found")

    return FileResponse(path, media_type="audio/mpeg")


@app.get("/map_defaults.js", include_in_schema=False)
async def map_defaults_js():
    return _serve_js_asset("map_defaults.js")


@app.get("/plane_globals.js", include_in_schema=False)
async def plane_globals_js():
    return _serve_js_asset("plane_globals.js")


@app.get("/markers.js", include_in_schema=False)
async def markers_js():
    return _serve_js_asset("markers.js")


@app.get("/planeObject.js", include_in_schema=False)
async def plane_object_js():
    return _serve_js_asset("planeObject.js")


@app.get("/planes_integration.js", include_in_schema=False)
async def planes_integration_js():
    return _serve_js_asset("planes_integration.js")


@app.get("/testmap-planes.js", include_in_schema=False)
async def testmap_planes_js():
    return _serve_js_asset("testmap-planes.js")


@app.get("/testmap-trains.js", include_in_schema=False)
async def testmap_trains_js():
    return _serve_js_asset("testmap-trains.js")


@app.get("/testmap.js", include_in_schema=False)
async def testmap_js():
    return _serve_js_asset("testmap.js")


@app.get("/testmap-core.js", include_in_schema=False)
async def testmap_core_js():
    return _serve_js_asset("testmap-core.js")


@app.get("/testmap-vehicles.js", include_in_schema=False)
async def testmap_vehicles_js():
    return _serve_js_asset("testmap-vehicles.js")


@app.get("/testmap-stops.js", include_in_schema=False)
async def testmap_stops_js():
    return _serve_js_asset("testmap-stops.js")


@app.get("/testmap-overlays.js", include_in_schema=False)
async def testmap_overlays_js():
    return _serve_js_asset("testmap-overlays.js")


@app.get("/testmap.css", include_in_schema=False)
async def testmap_css():
    return _serve_css_asset("testmap.css")


@app.get("/stop-approach.js", include_in_schema=False)
async def stop_approach_js():
    return _serve_js_asset("stop-approach.js")


@app.get("/stop-approach.css", include_in_schema=False)
async def stop_approach_css():
    return _serve_css_asset("stop-approach.css")


@app.get("/kioskmap.css", include_in_schema=False)
async def kioskmap_css():
    return _serve_css_asset("kioskmap.css")


@app.get("/kioskmap.js", include_in_schema=False)
async def kioskmap_js():
    return _serve_js_asset("kioskmap.js")


@app.get("/nav-bar.js", include_in_schema=False)
async def nav_bar_js():
    return _serve_js_asset("nav-bar.js")


@app.get("/css/marker-selection-menu.css", include_in_schema=False)
async def marker_selection_menu_css():
    return _serve_css_asset("marker-selection-menu.css")


@app.get("/scripts/marker-selection-menu.js", include_in_schema=False)
async def marker_selection_menu_js():
    return _serve_js_asset("marker-selection-menu.js")


@app.get("/scripts/push-notifications.js", include_in_schema=False)
async def push_notifications_js():
    return _serve_js_asset("push-notifications.js")


@app.get("/vehicle_log/{log_name}", include_in_schema=False)
async def vehicle_log_file(log_name: str):
    if not re.fullmatch(r"\d{8}_(?:\d{2}\.jsonl|routes\.json)", log_name):
        raise HTTPException(status_code=404, detail="Invalid log file")
    path = None
    for log_dir in VEH_LOG_DIRS:
        p = log_dir / log_name
        if p.exists():
            path = p
            break
    if path is None:
        raise HTTPException(status_code=404, detail="Log file not found")
    media_type = "application/x-ndjson" if log_name.endswith(".jsonl") else "application/json"
    return FileResponse(path, media_type=media_type)

# ---------------------------
# LANDING PAGE
# ---------------------------
@app.get("/")
async def landing_page():
    return HTMLResponse(LANDING_HTML)


# ---------------------------
# PWA SUPPORT
# ---------------------------
@app.get("/manifest.json")
async def manifest():
    return FileResponse(BASE_DIR / "manifest.json", media_type="application/manifest+json")


@app.get("/service-worker.js")
async def service_worker():
    return FileResponse(BASE_DIR / "service-worker.js", media_type="application/javascript")


@app.get("/offline")
async def offline_page():
    return HTMLResponse(OFFLINE_HTML)


@app.get("/sitemap")
async def sitemap_page():
    return HTMLResponse(SITEMAP_HTML)

@app.get("/login")
async def login_page():
    _refresh_dispatch_passwords()
    response = HTMLResponse(LOGIN_HTML)
    response.headers["Cache-Control"] = "no-store"
    return response

# ---------------------------
# MAP PAGE
# ---------------------------
@app.get("/map")
async def map_page():
    return HTMLResponse(TESTMAP_HTML)


@app.get("/radar")
async def radar_page():
    return HTMLResponse(RADAR_HTML)

@app.get("/eink-block")
async def eink_block_page():
    return HTMLResponse(EINK_BLOCK_HTML)

# ---------------------------
# TEST MAP PAGE
# ---------------------------
@app.get("/testmap")
async def testmap_page():
    return HTMLResponse(TESTMAP_HTML)

@app.get("/kioskmap")
async def kioskmap_page():
    return HTMLResponse(KIOSKMAP_HTML)

# ---------------------------
# CAT TEST MAP PAGE
# ---------------------------
@app.get("/cattestmap")
async def cattestmap_page():
    return HTMLResponse(CATTESTMAP_HTML)

# ---------------------------
# MAD MAP PAGE
# ---------------------------
@app.get("/madmap")
async def madmap_page():
    return HTMLResponse(MADMAP_HTML)

# ---------------------------
# METRO MAP PAGE
# ---------------------------
@app.get("/metromap")
async def metromap_page():
    return HTMLResponse(METROMAP_HTML)

# ---------------------------
# DEBUG PAGE
# ---------------------------
@app.get("/debug")
async def debug_page():
    return HTMLResponse(DEBUG_HTML)


# ---------------------------
# STOP APPROACH PAGE
# ---------------------------
@app.get("/stop-approach")
async def stop_approach_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(STOP_APPROACH_HTML)
    return _login_redirect(request)


# ---------------------------
# ADMIN PAGE
# ---------------------------
@app.get("/admin")
async def admin_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(ADMIN_HTML)
    return _login_redirect(request)

# ---------------------------
# BUSES PAGE
# ---------------------------
@app.get("/buses")
async def buses_page():
    return HTMLResponse(BUS_TABLE_HTML)

# ---------------------------
# CONFIG
# ---------------------------
@app.get("/v1/config")
async def get_config():
    return {k: globals().get(k) for k in CONFIG_KEYS}

@app.post("/v1/config")
async def set_config(payload: Dict[str, Any]):
    machine_info = _current_machine_info()
    headers = _provenance_headers(machine_info)
    try:
        for k, v in payload.items():
            if k in CONFIG_KEYS:
                cur = globals().get(k)
                if isinstance(cur, list):
                    if not isinstance(v, list):
                        v = [x.strip() for x in str(v).split(',') if x.strip()]
                    globals()[k] = v
                else:
                    try:
                        globals()[k] = type(cur)(v)
                    except Exception:
                        globals()[k] = v
        metadata, success, _commit_info_unused = save_config(machine_info)
    except Exception as exc:
        print(f"[config] error updating config: {exc}")
        body = _base_response_fields(False, False, machine_info)
        body["error"] = "failed to update config"
        return JSONResponse(body, status_code=500, headers=headers)
    if not success:
        body = _base_response_fields(False, False, machine_info)
        body["error"] = "failed to persist config"
        return JSONResponse(body, status_code=500, headers=headers)
    config_snapshot = {k: globals().get(k) for k in CONFIG_KEYS}
    response_body = _base_response_fields(True, True, machine_info)
    response_body.update(config_snapshot)
    response_body["saved_at"] = metadata.get("saved_at") if metadata else None
    return JSONResponse(response_body, headers=headers)


# ---------------------------
# SYSTEM NOTICES
# ---------------------------
@app.get("/v1/system-notices")
async def get_system_notices(request: Request, all: bool = Query(False)):
    """Get system notices. Public endpoint returns only active, public notices.
    With all=true (requires auth), returns all notices including inactive and auth-only."""
    if all:
        _require_dispatcher_access(request)
        return {"notices": _load_system_notices()}
    # Check if user is authenticated for auth-only notices
    is_authed = False
    try:
        _require_dispatcher_access(request)
        is_authed = True
    except Exception:
        pass
    return {"notices": _get_active_system_notices(include_auth_only=is_authed)}


@app.post("/v1/system-notices")
async def create_system_notice(request: Request):
    """Create a new system notice. Requires auth."""
    _require_dispatcher_access(request)
    body = await request.json()
    notice = {
        "id": str(uuid.uuid4()),
        "message": body.get("message", "").strip(),
        "severity": body.get("severity", "yellow"),  # red, yellow, green
        "start_time": body.get("start_time"),  # ISO8601 string or null
        "end_time": body.get("end_time"),  # ISO8601 string or null
        "auth_only": bool(body.get("auth_only", False)),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if not notice["message"]:
        raise HTTPException(status_code=400, detail="Message is required")
    if notice["severity"] not in ("red", "yellow", "green"):
        notice["severity"] = "yellow"
    notices = _load_system_notices()
    notices.append(notice)
    _save_system_notices(notices)
    return {"notice": notice}


@app.put("/v1/system-notices/{notice_id}")
async def update_system_notice(request: Request, notice_id: str):
    """Update an existing system notice. Requires auth."""
    _require_dispatcher_access(request)
    body = await request.json()
    notices = _load_system_notices()
    for i, notice in enumerate(notices):
        if notice.get("id") == notice_id:
            if "message" in body:
                notices[i]["message"] = body["message"].strip()
            if "severity" in body:
                sev = body["severity"]
                notices[i]["severity"] = sev if sev in ("red", "yellow", "green") else "yellow"
            if "start_time" in body:
                notices[i]["start_time"] = body["start_time"]
            if "end_time" in body:
                notices[i]["end_time"] = body["end_time"]
            if "auth_only" in body:
                notices[i]["auth_only"] = bool(body["auth_only"])
            notices[i]["updated_at"] = datetime.now(timezone.utc).isoformat()
            _save_system_notices(notices)
            return {"notice": notices[i]}
    raise HTTPException(status_code=404, detail="Notice not found")


@app.delete("/v1/system-notices/{notice_id}")
async def delete_system_notice(request: Request, notice_id: str):
    """Delete a system notice. Requires auth."""
    _require_dispatcher_access(request)
    notices = _load_system_notices()
    original_len = len(notices)
    notices = [n for n in notices if n.get("id") != notice_id]
    if len(notices) == original_len:
        raise HTTPException(status_code=404, detail="Notice not found")
    _save_system_notices(notices)
    return {"deleted": True}


@app.get("/v1/stop-approach")
@app.get("/api/stop-approach")
async def get_stop_approach(request: Request, base_url: Optional[str] = Query(None)):
    _require_dispatcher_access(request)
    try:
        routes_raw, extra_routes_raw = await _load_transloc_route_sources(base_url)
        stops_raw = await _get_transloc_stops(base_url)
        stops = _build_transloc_stops(
            routes_raw,
            stops_raw,
            approach_sets_config=getattr(app.state, "approach_sets_config", None),
        )
    except httpx.HTTPError as exc:
        detail = _transloc_error_detail(exc, base_url)
        raise HTTPException(status_code=502, detail=detail) from exc
    config_snapshot = _serialize_stop_approach_config(
        getattr(app.state, "approach_sets_config", {}),
    )
    return {
        "stops": stops,
        "config": config_snapshot,
        "fetched_at": int(time.time()),
    }


@app.post("/v1/stop-approach")
@app.post("/api/stop-approach")
async def set_stop_approach(request: Request, payload: Dict[str, Any]):
    _require_dispatcher_access(request)
    stop_id = payload.get("stop_id") or payload.get("StopId") or payload.get("StopID")
    if stop_id is None:
        raise HTTPException(status_code=400, detail="stop_id is required")

    # Handle approach_sets (ordered bubbles)
    approach_sets_raw = payload.get("approach_sets")
    approach_sets_validated: Optional[List[Dict[str, Any]]] = None
    if approach_sets_raw is not None and isinstance(approach_sets_raw, list):
        approach_sets_validated = []
        for idx, aset in enumerate(approach_sets_raw):
            if not isinstance(aset, dict):
                continue
            validated_set: Dict[str, Any] = {
                "name": str(aset.get("name", f"Approach {idx + 1}")),
                "bubbles": [],
            }
            bubbles = aset.get("bubbles")
            if bubbles and isinstance(bubbles, list):
                for bidx, bubble in enumerate(bubbles):
                    if not isinstance(bubble, dict):
                        continue
                    lat = _coerce_float(bubble.get("lat"))
                    lng = _coerce_float(bubble.get("lng"))
                    if lat is None or lng is None:
                        continue
                    bubble_radius = _coerce_float(bubble.get("radius_m"))
                    if bubble_radius is None:
                        bubble_radius = 25.0
                    order = bubble.get("order")
                    if order is None:
                        order = bidx + 1
                    validated_set["bubbles"].append({
                        "lat": lat,
                        "lng": lng,
                        "radius_m": max(5.0, min(200.0, bubble_radius)),
                        "order": int(order),
                    })
            approach_sets_validated.append(validated_set)

    # Update approach sets config
    updated_sets_config = dict(getattr(app.state, "approach_sets_config", {}) or {})
    if approach_sets_validated is not None:
        if approach_sets_validated:
            updated_sets_config[str(stop_id)] = approach_sets_validated
        elif str(stop_id) in updated_sets_config:
            del updated_sets_config[str(stop_id)]

    serialized = _serialize_stop_approach_config(updated_sets_config)
    try:
        encoded = json.dumps(serialized)
    except Exception as exc:
        print(f"[stop-approach] failed to encode config: {exc}")
        raise HTTPException(status_code=500, detail="Failed to encode config") from exc

    try:
        _write_data_file(str(DEFAULT_STOP_APPROACH_CONFIG_PATH), encoded)
    except Exception as exc:
        print(f"[stop-approach] failed to persist config: {exc}")
        raise HTTPException(status_code=500, detail="Failed to save config") from exc

    async with state.lock:
        app.state.approach_sets_config = updated_sets_config
        cached_stops = getattr(state, "stops", [])
        _apply_stop_approach_to_stops(cached_stops, updated_sets_config)
        tracker = getattr(app.state, "headway_tracker", None)
        if tracker and cached_stops:
            tracker.update_stops(cached_stops)

    return {
        "ok": True,
        "stop_id": str(stop_id),
        "saved_at": int(time.time()),
        "approach_sets": approach_sets_validated or [],
    }


@app.get("/v1/secrets")
async def get_secrets():
    secrets_payload = [
        {"name": key, "value": value} for key, value in _iter_loaded_secrets()
    ]
    return {"secrets": secrets_payload}


@app.get("/v1/missing-env")
async def get_missing_env():
    return {"missing": _missing_env_vars(), "checked": EXPECTED_ENV_KEYS}

# ---------------------------
# SERVICE CREW API
# ---------------------------
@app.get("/v1/servicecrew")
async def servicecrew_data(date: Optional[str] = None):
    if not date:
        date = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    async with state.lock:
        day = state.bus_days.get(date, {})
        buses = {}
        for bus in ALL_BUSES:
            bd = day.get(bus)
            if bd:
                buses[bus] = {
                    "blocks": sorted(list(bd.blocks)),
                    "actual_miles": bd.day_miles,
                    "reset_miles": bd.reset_miles,
                    "display_miles": bd.total_miles - bd.reset_miles,
                }
            else:
                buses[bus] = {"blocks": [], "actual_miles": 0.0, "reset_miles": 0.0, "display_miles": 0.0}
        return {"date": date, "buses": buses}

@app.post("/v1/servicecrew/reset/{bus_name}")
async def servicecrew_reset(bus_name: str):
    bus_name = normalize_bus_name(bus_name)
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    async with state.lock:
        day = state.bus_days.setdefault(today, {})
        bd = day.setdefault(bus_name, BusDay())
        bd.reset_miles = bd.total_miles
        save_bus_days()
    return {"status": "ok"}

@app.get("/v1/stream/servicecrew_refresh")
async def stream_servicecrew_refresh():
    async def gen():
        q: asyncio.Queue = asyncio.Queue(maxsize=10)  # Limit queue to prevent memory bloat
        SERVICECREW_SUBS.add(q)
        try:
            while True:
                encoded = await q.get()
                yield encoded
        finally:
            SERVICECREW_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.post("/v1/servicecrew/refresh")
async def servicecrew_refresh():
    item = {"ts": int(time.time()*1000)}
    encoded = f"data: {json.dumps(item)}\n\n"
    for q in list(SERVICECREW_SUBS):
        try:
            q.put_nowait(encoded)
        except asyncio.QueueFull:
            pass  # Drop update for slow clients
    return {"status": "ok"}

# ---------------------------
# SERVICE CREW PAGE
# ---------------------------
@app.get("/servicecrew")
async def servicecrew_page():
    return HTMLResponse(SERVICECREW_HTML)

# ---------------------------
# DRIVER PAGE
# ---------------------------
@app.get("/driver")
async def driver_page():
    return HTMLResponse(DRIVER_HTML)

# ---------------------------
# DISPATCHER PAGE
# ---------------------------
def _normalize_dispatch_password(password: Optional[str]) -> Optional[Tuple[str, str]]:
    if not isinstance(password, str):
        return None
    _refresh_dispatch_passwords()
    matches: list[Tuple[str, str]] = []
    for normalized_label, secret in DISPATCH_PASSWORDS.items():
        if secrets.compare_digest(password, secret):
            display_label = DISPATCH_PASSWORD_LABELS.get(normalized_label, normalized_label)
            access_type = DISPATCH_PASSWORD_TYPES.get(
                normalized_label,
                normalized_label.split("::")[-1],
            )
            matches.append((display_label, access_type))

    if not matches:
        return None

    # Prefer non-CAT access when a password is shared between multiple roles so
    # that CAT-specific UI changes only occur when the CAT credential is used
    # explicitly.
    for display_label, access_type in matches:
        if access_type != "cat":
            return display_label, access_type

    return matches[0]
    return None


def _normalized_dispatch_key(label: Optional[str], access_type: Optional[str]) -> Optional[str]:
    if not isinstance(label, str) or not isinstance(access_type, str):
        return None
    normalized_label = label.strip().lower()
    normalized_type = access_type.strip().lower()
    if not normalized_label or not normalized_type:
        return None
    return f"{normalized_label}::{normalized_type}"


def _dispatcher_cookie_value_for_label(label: str, access_type: str) -> Optional[str]:
    _refresh_dispatch_passwords()
    normalized_key = _normalized_dispatch_key(label, access_type)
    if not normalized_key:
        return None
    secret = DISPATCH_PASSWORDS.get(normalized_key)
    if not secret:
        return None
    display_label = DISPATCH_PASSWORD_LABELS.get(normalized_key, label.strip())
    normalized_type = DISPATCH_PASSWORD_TYPES.get(normalized_key, access_type.strip().lower())
    digest = hashlib.sha256(
        f"dispatcher::{display_label}:{normalized_type}:{secret}".encode("utf-8")
    ).hexdigest()
    return f"{display_label}:{normalized_type}:{digest}"


def _legacy_dispatcher_cookie_value(label: str) -> Optional[str]:
    normalized_key = _normalized_dispatch_key(label, "uts")
    if not normalized_key:
        return None
    secret = DISPATCH_PASSWORDS.get(normalized_key)
    if not secret:
        return None
    display_label = DISPATCH_PASSWORD_LABELS.get(normalized_key, label.strip())
    digest = hashlib.sha256(
        f"dispatcher::{display_label}:{secret}".encode("utf-8")
    ).hexdigest()
    return f"{display_label}:{digest}"


def _get_dispatcher_secret_info(request: Request) -> Optional[Tuple[str, str]]:
    _refresh_dispatch_passwords()
    provided = request.cookies.get(DISPATCH_COOKIE_NAME)
    if not provided:
        return None
    parts = provided.split(":", 2)
    if len(parts) == 3:
        label, access_type, _hash = parts
        expected = _dispatcher_cookie_value_for_label(label, access_type)
        if expected and secrets.compare_digest(provided, expected):
            normalized_key = _normalized_dispatch_key(label, access_type)
            if not normalized_key:
                return None
            stored_label = DISPATCH_PASSWORD_LABELS.get(normalized_key, label)
            stored_type = DISPATCH_PASSWORD_TYPES.get(
                normalized_key, access_type.strip().lower()
            )
            return stored_label, stored_type
    elif len(parts) == 2:
        label, _hash = parts
        expected = _legacy_dispatcher_cookie_value(label)
        if expected and secrets.compare_digest(provided, expected):
            return label, "uts"
    return None


def _get_dispatcher_secret_label(request: Request) -> Optional[str]:
    info = _get_dispatcher_secret_info(request)
    if info is None:
        return None
    label, _access_type = info
    return label


def _has_dispatcher_access(request: Request) -> bool:
    return _get_dispatcher_secret_info(request) is not None


def _require_dispatcher_access(request: Request) -> None:
    if not _has_dispatcher_access(request):
        raise HTTPException(status_code=401, detail="dispatcher auth required")


def _login_redirect(request: Request) -> RedirectResponse:
    target = request.url.path
    if request.url.query:
        target = f"{target}?{request.url.query}"
    encoded_target = quote(target, safe="/")
    return RedirectResponse(f"/login?return={encoded_target}", status_code=302)


@app.get("/api/dispatcher/auth")
async def dispatcher_auth_status(request: Request):
    info = _get_dispatcher_secret_info(request)
    secret_label = info[0] if info else None
    access_type = info[1] if info else None
    return {
        "required": True,
        "authorized": bool(secret_label),
        "secret": secret_label,
        "access_type": access_type,
    }


@app.get("/v1/index/init")
async def index_init(request: Request):
    """Combined endpoint for index.html initial page load.

    Returns auth status, service alerts, system notices, and service level
    in a single request to reduce page load latency.
    """
    # Get auth status (sync)
    info = _get_dispatcher_secret_info(request)
    is_authed = bool(info)
    auth_data = {
        "required": True,
        "authorized": is_authed,
        "secret": info[0] if info else None,
        "access_type": info[1] if info else None,
    }

    # Fetch async data in parallel
    async def fetch_alerts():
        try:
            root = transloc_host_base(None)
            url = f"{root}/Secure/Services/RoutesService.svc/GetMessagesPaged"
            params = {
                "showInactive": False,
                "includeDeleted": False,
                "messageTypeId": 1,
                "search": False,
                "rows": 5,
                "page": 1,
                "sortIndex": "StartDateUtc",
                "sortOrder": "asc",
            }
            return await _proxy_transloc_get(url, params=params, base_url=None)
        except Exception:
            return {"Rows": []}

    async def fetch_service_level():
        try:
            result = await _fetch_service_level(bypass_cache=False)
            return result.to_dict()
        except Exception:
            return {"service_level": "UNKNOWN"}

    # Run async fetches in parallel
    alerts_result, service_level_result = await asyncio.gather(
        fetch_alerts(),
        fetch_service_level(),
    )

    # Get system notices (sync, but depends on auth status)
    notices = _get_active_system_notices(include_auth_only=is_authed)

    return {
        "auth": auth_data,
        "alerts": alerts_result,
        "notices": notices,
        "service_level": service_level_result,
    }


@app.post("/api/dispatcher/auth")
async def dispatcher_auth(
    response: Response, payload: dict[str, Any] = Body(...)
):
    secret_info = _normalize_dispatch_password(payload.get("password"))
    if secret_info is not None:
        label, access_type = secret_info
        cookie_value = _dispatcher_cookie_value_for_label(label, access_type)
        if not cookie_value:
            raise HTTPException(status_code=401, detail="Incorrect password.")
        response.set_cookie(
            DISPATCH_COOKIE_NAME,
            cookie_value,
            max_age=DISPATCH_COOKIE_MAX_AGE,
            httponly=True,
            secure=DISPATCH_COOKIE_SECURE,
            samesite="lax",
        )
        return {"ok": True, "secret": label, "access_type": access_type}
    raise HTTPException(status_code=401, detail="Incorrect password.")


@app.post("/api/dispatcher/logout")
async def dispatcher_logout(response: Response):
    response.delete_cookie(DISPATCH_COOKIE_NAME)
    return {"ok": True}


# ---------------------------
# Push Notifications API
# ---------------------------
@app.get("/api/push/vapid-public-key")
async def get_vapid_public_key():
    """Return the VAPID public key for push subscription."""
    if not VAPID_PUBLIC_KEY:
        raise HTTPException(status_code=503, detail="Push notifications not configured")
    return {"publicKey": VAPID_PUBLIC_KEY}


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    """Subscribe to push notifications."""
    if not VAPID_PUBLIC_KEY or not VAPID_PRIVATE_KEY:
        raise HTTPException(status_code=503, detail="Push notifications not configured")
    data = await request.json()
    endpoint = data.get("endpoint")
    keys = data.get("keys", {})
    if not endpoint or not keys.get("p256dh") or not keys.get("auth"):
        raise HTTPException(status_code=400, detail="Invalid subscription data")
    user_agent = request.headers.get("user-agent")
    is_new = await push_subscription_store.add_subscription(endpoint, keys, user_agent)
    return {"status": "subscribed", "new": is_new}


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    """Unsubscribe from push notifications."""
    data = await request.json()
    endpoint = data.get("endpoint")
    if not endpoint:
        raise HTTPException(status_code=400, detail="Missing endpoint")
    removed = await push_subscription_store.remove_subscription(endpoint)
    return {"status": "unsubscribed", "found": removed}


@app.get("/api/push/status")
async def push_status():
    """Return push notification status (for diagnostics)."""
    count = await push_subscription_store.count()
    return {
        "configured": bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY),
        "subscription_count": count,
        "sent_alert_count": len(_sent_alert_ids),
    }


@app.post("/api/push/test")
async def push_test(request: Request):
    """Send a test push notification to all subscribers (admin only)."""
    _require_dispatcher_access(request)
    if not VAPID_PUBLIC_KEY or not VAPID_PRIVATE_KEY:
        raise HTTPException(status_code=503, detail="Push notifications not configured")
    data = await request.json()
    message = (data.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Missing message")
    subscriptions = await push_subscription_store.get_all_subscriptions()
    if not subscriptions:
        return {"sent": 0, "total": 0, "message": "No subscribers"}
    from pywebpush import webpush, WebPushException
    payload = {
        "title": "UTS Test Notification",
        "body": message[:200],
        "icon": "/media/icon-192.png",
        "tag": f"test-{int(datetime.now(timezone.utc).timestamp())}",
        "url": "/admin",
    }
    sent_count = 0
    for sub in subscriptions:
        try:
            webpush(
                subscription_info=sub.to_subscription_info(),
                data=json.dumps(payload),
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_SUBJECT},
            )
            sent_count += 1
        except WebPushException as e:
            if e.response and e.response.status_code == 410:
                await push_subscription_store.remove_subscription(sub.endpoint)
        except Exception as err:
            print(f"[push_test] error sending to subscriber: {err}")
    return {"sent": sent_count, "total": len(subscriptions)}


@app.get("/api/tickets")
async def list_tickets(includeClosed: bool = Query(False, alias="includeClosed")):
    tickets = await tickets_store.list_tickets(include_closed=includeClosed)
    info = _current_machine_info()
    return {"machine_id": info.get("machine_id", "unknown"), "tickets": tickets}


@app.get("/api/tickets/export.csv")
async def export_tickets_csv(
    start: str = Query(...),
    end: str = Query(...),
    date_field: str = Query("reported_at", alias="dateField"),
    include_closed: bool = Query(True, alias="includeClosed"),
    include_history: bool = Query(False, alias="includeHistory"),
):
    try:
        tickets = await tickets_store.export_tickets(
            start=start,
            end=end,
            date_field=date_field,
            include_closed=include_closed,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    header = [
        "vehicle",
        "ticket_id",
        "reported_at",
        "reported_by",
        "ops_status",
        "ops_description",
        "shop_status",
        "mechanic",
        "diag_date",
        "diagnosis_text",
        "started_at",
        "completed_at",
        "closed_at",
        "legacy_row_index",
        "legacy_source",
        "created_at",
        "updated_at",
    ]
    if include_history:
        header.append("history")
    writer.writerow(header)
    for ticket in tickets:
        row = [
            ticket.get("vehicle_label") or "",
            ticket.get("id") or "",
            ticket.get("reported_at") or "",
            ticket.get("reported_by") or "",
            ticket.get("ops_status") or "",
            ticket.get("ops_description") or "",
            ticket.get("shop_status") or "",
            ticket.get("mechanic") or "",
            ticket.get("diag_date") or "",
            ticket.get("diagnosis_text") or "",
            ticket.get("started_at") or "",
            ticket.get("completed_at") or "",
            ticket.get("closed_at") or "",
            "" if ticket.get("legacy_row_index") is None else ticket.get("legacy_row_index"),
            ticket.get("legacy_source") or "",
            ticket.get("created_at") or "",
            ticket.get("updated_at") or "",
        ]
        if include_history:
            history_value = json.dumps(ticket.get("history") or [], ensure_ascii=False)
            row.append(history_value)
        writer.writerow(row)
    content = buffer.getvalue()
    headers = {"Content-Disposition": 'attachment; filename="export.csv"'}
    return Response(content, media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/api/tickets/{ticket_id}")
async def get_ticket(ticket_id: str):
    ticket = await tickets_store.get_ticket(ticket_id)
    if ticket is None:
        raise HTTPException(status_code=404, detail="ticket not found")
    info = _current_machine_info()
    return {"machine_id": info.get("machine_id", "unknown"), "ticket": ticket}


@app.post("/api/tickets")
async def create_ticket(request: Request, payload: Dict[str, Any] = Body(...)):
    try:
        actor = _get_dispatcher_secret_label(request)
        ticket, _commit_info_unused = await tickets_store.create_ticket(
            payload, actor=actor
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    info = _current_machine_info()
    body = {"machine_id": info.get("machine_id", "unknown"), "ticket": ticket}
    headers = _provenance_headers(info)
    return JSONResponse(body, headers=headers)


@app.put("/api/tickets/{ticket_id}")
async def update_ticket(request: Request, ticket_id: str, payload: Dict[str, Any] = Body(...)):
    try:
        actor = _get_dispatcher_secret_label(request)
        ticket, _commit_info_unused = await tickets_store.update_ticket(
            ticket_id, payload, actor=actor
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="ticket not found") from exc
    info = _current_machine_info()
    body = {"machine_id": info.get("machine_id", "unknown"), "ticket": ticket}
    headers = _provenance_headers(info)
    return JSONResponse(body, headers=headers)


@app.post("/api/tickets/purge")
async def purge_tickets(payload: Dict[str, Any] = Body(...)):
    date_field = payload.get("dateField") or payload.get("date_field") or "reported_at"
    vehicles = payload.get("vehicles")
    if not isinstance(vehicles, list):
        vehicles = []
    hard = bool(payload.get("hard"))
    try:
        result, _commit_info_unused = await tickets_store.purge_tickets(
            start=payload.get("start"),
            end=payload.get("end"),
            date_field=date_field,
            vehicles=vehicles,
            hard=hard,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    info = _current_machine_info()
    body = {"machine_id": info.get("machine_id", "unknown"), **result}
    headers = _provenance_headers(info)
    return JSONResponse(body, headers=headers)


@app.get("/dispatcher")
async def dispatcher_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(DISPATCHER_HTML)
    return _login_redirect(request)


@app.get("/downed")
async def downed_page():
    return HTMLResponse(DOWNED_HTML)


@app.get("/repairsscreen")
async def repairs_screen_page():
    return HTMLResponse(REPAIRS_SCREEN_HTML)

# ---------------------------
# API CALLS PAGE
# ---------------------------
@app.get("/apicalls")
async def apicalls_page():
    return HTMLResponse(APICALLS_HTML)


@app.get("/repairs")
async def repairs_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(REPAIRS_HTML)
    return _login_redirect(request)


@app.get("/repairsexport")
async def repairs_export_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(REPAIRS_EXPORT_HTML)
    return _login_redirect(request)

# ---------------------------
# RIDERSHIP PAGE
# ---------------------------
@app.get("/ridership")
async def ridership_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(RIDERSHIP_HTML)
    return _login_redirect(request)

# ---------------------------
# HEADWAY PAGE
# ---------------------------
@app.get("/headway")
async def headway_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(HEADWAY_HTML)
    return _login_redirect(request)


@app.get("/headway-diagnostics")
async def headway_diagnostics_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(HEADWAY_DIAGNOSTICS_HTML)
    return _login_redirect(request)

# ---------------------------
# TRANSLOC TICKER PAGE
# ---------------------------
@app.get("/transloc_ticker")
async def transloc_ticker_page():
    return HTMLResponse(TRANSLOC_TICKER_HTML)

# ---------------------------
# ARRIVALS DISPLAY PAGE
# ---------------------------
@app.get("/arrivalsdisplay")
async def arrivalsdisplay_page():
    return HTMLResponse(ARRIVALSDISPLAY_HTML)

# ---------------------------
# CLOCK DISPLAY PAGE
# ---------------------------
@app.get("/clockdisplay")
async def clockdisplay_page():
    return HTMLResponse(CLOCKDISPLAY_HTML)

# ---------------------------
# STATUS SIGNAGE PAGE
# ---------------------------
@app.get("/statussignage")
async def statussignage_page():
    return HTMLResponse(STATUSSIGNAGE_HTML)

# ---------------------------
# REPLAY PAGE
# ---------------------------
@app.get("/replay")
async def replay_page(request: Request):
    _refresh_dispatch_passwords()
    if _has_dispatcher_access(request):
        return HTMLResponse(REPLAY_HTML)
    return _login_redirect(request)


@app.get("/ips")
async def ips_page():
    return HTMLResponse(IPS_HTML)