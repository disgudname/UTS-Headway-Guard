"""
Headway Guard Service — Dispatcher API (FastAPI skeleton)

Purpose
=======
Centralise anti-bunching calculations and expose them to driver/dispatcher clients.

Key features in this skeleton
-----------------------------
- Poll TransLoc for routes/vehicles. (HTTP calls sketched; plug in API key & base.)
- Fetch & cache Overpass speed limits per route (fetch-once; invalidate on polyline change).
- Compute headway = time along-route from follower → leader (arc-based).
- Target headway anchored to speed-limit profile, lightly blended with live EMA.
 - Dispatcher-friendly fields: status (OK/Ease off/HOLD), headway_sec, gap_label (Ahead/Behind/On target), countdown_sec, leader_name.
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
from typing import List, Dict, Optional, Tuple, Any, Iterable
from dataclasses import dataclass, field
import asyncio, time, math, os, json, re, base64, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from collections import deque, defaultdict
import xml.etree.ElementTree as ET
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from fastapi import FastAPI, HTTPException, Request, Response, Query
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, FileResponse
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

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
PULSEPOINT_ENDPOINT = os.getenv(
    "PULSEPOINT_ENDPOINT",
    "https://api.pulsepoint.org/v1/webapp?resource=incidents&agencyid=54000,00300",
)
PULSEPOINT_PASSPHRASE = os.getenv("PULSEPOINT_PASSPHRASE", "tombrady5rings")
AMTRAKER_URL = os.getenv("AMTRAKER_URL", "https://api-v3.amtraker.com/v3/trains")
RIDESYSTEMS_CLIENTS_URL = os.getenv(
    "RIDESYSTEMS_CLIENTS_URL",
    "https://admin.ridesystems.net/api/Clients/GetClients",
)
TRAIN_TARGET_STATION_CODE = os.getenv("TRAIN_TARGET_STATION_CODE", "").strip().upper()

VEH_REFRESH_S   = int(os.getenv("VEH_REFRESH_S", "10"))
ROUTE_REFRESH_S = int(os.getenv("ROUTE_REFRESH_S", "60"))
BLOCK_REFRESH_S = int(os.getenv("BLOCK_REFRESH_S", "30"))
STALE_FIX_S     = int(os.getenv("STALE_FIX_S", "90"))
VEHICLE_STALE_THRESHOLD_S = int(
    os.getenv("VEHICLE_STALE_THRESHOLD_S", str(60 * 60))
)

# Grace window to keep routes "active" despite brief data hiccups (prevents dispatcher flicker)
ROUTE_GRACE_S   = int(os.getenv("ROUTE_GRACE_S", "60"))

URBAN_FACTOR      = float(os.getenv("URBAN_FACTOR", "1.12"))
GREEN_FRAC        = float(os.getenv("GREEN_FRAC", "0.75"))
RED_FRAC          = float(os.getenv("RED_FRAC", "0.50"))
ONTARGET_TOL_SEC  = int(os.getenv("ONTARGET_TOL_SEC", "30"))
W_LIMIT           = float(os.getenv("W_LIMIT", "0.85"))  # 85% limits, 15% live

EMA_ALPHA       = float(os.getenv("EMA_ALPHA", "0.40"))
MIN_SPEED_FLOOR = float(os.getenv("MIN_SPEED_FLOOR", "1.2"))
MAX_SPEED_CEIL  = float(os.getenv("MAX_SPEED_CEIL", "22.0"))
LEADER_EPS_M   = float(os.getenv("LEADER_EPS_M", "8.0"))
STOPPED_MPS    = float(os.getenv("STOPPED_MPS", "0.5"))
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

# Comma-separated list of peer hosts (e.g. "peer1:8080,peer2:8080")
SYNC_PEERS = [p for p in os.getenv("SYNC_PEERS", "").split(",") if p]
# Shared secret required for /sync endpoint
SYNC_SECRET = os.getenv("SYNC_SECRET")

# Vehicle position logging
VEH_LOG_URL = f"{TRANSLOC_BASE}/GetMapVehiclePoints?APIKey={TRANSLOC_KEY}&returnVehiclesNotAssignedToRoute=true"
VEH_LOG_INTERVAL_S = int(os.getenv("VEH_LOG_INTERVAL_S", "4"))
VEH_LOG_RETENTION_MS = int(os.getenv("VEH_LOG_RETENTION_MS", str(7 * 24 * 3600 * 1000)))
VEH_LOG_MIN_MOVE_M = float(os.getenv("VEH_LOG_MIN_MOVE_M", "3"))
LAST_LOG_POS: Dict[int, Tuple[float, float]] = {}

TRANSLOC_ARRIVALS_TTL_S = int(os.getenv("TRANSLOC_ARRIVALS_TTL_S", "15"))
TRANSLOC_BLOCKS_TTL_S = int(os.getenv("TRANSLOC_BLOCKS_TTL_S", "60"))
CAT_METADATA_TTL_S = int(os.getenv("CAT_METADATA_TTL_S", str(5 * 60)))
CAT_VEHICLE_TTL_S = int(os.getenv("CAT_VEHICLE_TTL_S", "5"))
CAT_SERVICE_ALERT_TTL_S = int(os.getenv("CAT_SERVICE_ALERT_TTL_S", "60"))
CAT_STOP_ETA_TTL_S = int(os.getenv("CAT_STOP_ETA_TTL_S", "30"))
PULSEPOINT_TTL_S = int(os.getenv("PULSEPOINT_TTL_S", "20"))
AMTRAKER_TTL_S = int(os.getenv("AMTRAKER_TTL_S", "30"))
RIDESYSTEMS_CLIENT_TTL_S = int(os.getenv("RIDESYSTEMS_CLIENT_TTL_S", str(12 * 3600)))
ADSB_CACHE_TTL_S = float(os.getenv("ADSB_CACHE_TTL_S", "15"))

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

def propagate_file(name: str, data: str) -> None:
    if not SYNC_PEERS:
        return
    payload = {"name": name, "data": data}
    if SYNC_SECRET:
        payload["secret"] = SYNC_SECRET
    for peer in SYNC_PEERS:
        url = f"http://{peer.rstrip('/')}/sync"
        try:
            httpx.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"[sync] error sending {name} to {peer}: {e}")

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
    """Extract milliseconds from TransLoc's ``/Date(ms-offset)/`` format."""
    if not s:
        return None
    try:
        m = re.search(r"/Date\((\d+)(?:[-+]\d{4})?\)/", s)
        return int(m.group(1)) if m else None
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


def fmt_mmss(sec: float) -> str:
    """Format seconds as MM:SS."""
    sec = int(round(abs(sec)))
    return f"{sec//60:02d}:{sec%60:02d}"

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


def is_default_transloc_base(base_url: Optional[str]) -> bool:
    sanitized = sanitize_transloc_base(base_url)
    return sanitized is None or sanitized == DEFAULT_TRANSLOC_BASE


async def fetch_routes_with_shapes(client: httpx.AsyncClient, base_url: Optional[str] = None):
    url = build_transloc_url(base_url, f"GetRoutesForMapWithScheduleWithEncodedLine?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=20)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])


async def fetch_routes_catalog(client: httpx.AsyncClient, base_url: Optional[str] = None):
    url = build_transloc_url(base_url, f"GetRoutes?APIKey={TRANSLOC_KEY}")
    r = await client.get(url, timeout=20)
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
    r = await client.get(url, timeout=20)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_block_groups(client: httpx.AsyncClient, base_url: Optional[str] = None) -> List[Dict]:
    d = datetime.now(ZoneInfo("America/New_York"))
    ds = f"{d.month}/{d.day}/{d.year}"
    r1_url = build_transloc_url(
        base_url, f"GetScheduleVehicleCalendarByDateAndRoute?dateString={quote(ds)}"
    )
    r1 = await client.get(r1_url, timeout=20)
    record_api_call("GET", r1_url, r1.status_code)
    r1.raise_for_status()
    sched = r1.json()
    sched = sched if isinstance(sched, list) else sched.get("d", [])
    ids = ",".join(str(s.get("ScheduleVehicleCalendarID")) for s in sched if s.get("ScheduleVehicleCalendarID"))
    if ids:
        r2_url = build_transloc_url(
            base_url, f"GetDispatchBlockGroupData?scheduleVehicleCalendarIdsString={ids}"
        )
        r2 = await client.get(r2_url, timeout=20)
        record_api_call("GET", r2_url, r2.status_code)
        r2.raise_for_status()
        return r2.json().get("BlockGroups", [])
    return []

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
def find_seg_index_at_s(cum: List[float], s: float) -> int:
    lo, hi = 0, len(cum)-1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if cum[mid] <= s: lo = mid
        else: hi = mid
    return lo

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

def target_headway_sec(route: Route, veh_count: int) -> float:
    # speed-limit-based lap time / veh_count
    if route.length_m <= 0 or veh_count <= 0: return 0.0
    avg_cap = sum(route.seg_caps_mps)/max(1,len(route.seg_caps_mps)) if route.seg_caps_mps else DEFAULT_CAP_MPS
    lap = (route.length_m / max(avg_cap, 0.1)) * URBAN_FACTOR
    return lap / veh_count

def compute_status_for_route(route: Route, vehs_by_id: Dict[int, Vehicle]) -> List["VehicleView"]:
    """Compute headway guidance for each vehicle on a route.

    All active vehicles are ordered around the route's loop distance and each bus
    takes the next bus in that order as its leader.  Headways and countdowns are
    computed using modular gaps so the ring remains stable even as vehicles pass
    the zero‑distance boundary or stop temporarily.
    """

    vehicles = list(vehs_by_id.values())
    if not vehicles:
        return []

    loop_len = max(1.0, route.length_m)
    ordered = sorted(vehicles, key=lambda v: v.s_pos)
    n = len(ordered)
    target = target_headway_sec(route, n)
    results: List[VehicleView] = []

    def ref_speed(v: Vehicle) -> float:
        seg_idx = find_seg_index_at_s(route.cum, v.s_pos)
        cap = route.seg_caps_mps[seg_idx] if route.seg_caps_mps else DEFAULT_CAP_MPS
        return max(
            MIN_SPEED_FLOOR,
            min(MAX_SPEED_CEIL, W_LIMIT * cap + (1 - W_LIMIT) * v.ema_mps),
        )

    for idx, me in enumerate(ordered):
        if n == 1:
            results.append(
                VehicleView(
                    id=me.id,
                    name=me.name,
                    status="green",
                    headway_sec=None,
                    target_headway_sec=int(target) if target > 0 else None,
                    gap_label="—",
                    leader_name=None,
                    countdown_sec=None,
                    updated_at=min(me.ts_ms, int(time.time() * 1000)) // 1000,
                )
            )
            continue

        leader = ordered[(idx + 1) % n]
        gap_m = (leader.s_pos - me.s_pos) % loop_len
        gap_m = max(gap_m, LEADER_EPS_M)

        speed = ref_speed(me)
        headway = gap_m / max(speed, 0.1)
        diff = headway - target

        if target > 0 and headway < RED_FRAC * target:
            status = "red"
            gap_label = f"Ahead {fmt_mmss(diff)}"
            countdown = int(max(0, target - headway))
        elif target > 0 and headway < GREEN_FRAC * target:
            status = "yellow"
            gap_label = f"Ahead {fmt_mmss(diff)}"
            countdown = int(max(0, target - headway))
        else:
            status = "green"
            if target > 0 and abs(diff) <= ONTARGET_TOL_SEC:
                gap_label = "On target"
            elif diff < 0:
                gap_label = f"Ahead {fmt_mmss(diff)}"
            else:
                gap_label = f"Behind {fmt_mmss(diff)}"
            countdown = None

        results.append(
            VehicleView(
                id=me.id,
                name=me.name,
                status=status,
                headway_sec=int(headway),
                target_headway_sec=int(target) if target > 0 else None,
                gap_label=gap_label,
                leader_name=leader.name,
                countdown_sec=countdown,
                updated_at=min(me.ts_ms, int(time.time() * 1000)) // 1000,
            )
        )

    # Accumulate hold times: if a leader is holding, followers must also wait.
    if n > 1:
        bases = [v.countdown_sec for v in results]
        roots = [i for i, b in enumerate(bases) if not b] or [0]
        for start in roots:
            i = (start - 1) % n
            while i != start:
                if bases[i]:
                    leader_idx = (i + 1) % n
                    leader_hold = results[leader_idx].countdown_sec or 0
                    results[i].countdown_sec = bases[i] + leader_hold
                    bases[i] = results[i].countdown_sec
                    i = (i - 1) % n
                else:
                    break

    id_map = {v.id: v for v in ordered if v.id is not None}

    def sort_key(vv: VehicleView):
        v = id_map.get(vv.id)
        s = v.s_pos if v else 0.0
        return (round(s, 3), vv.updated_at)

    return sorted(results, key=sort_key)
# ---------------------------
# Presentation models
# ---------------------------
@dataclass
class VehicleView:
    id: Optional[int]
    name: str
    status: str  # "green" | "yellow" | "red"
    headway_sec: Optional[int]
    target_headway_sec: Optional[int]
    gap_label: str
    leader_name: Optional[str]
    countdown_sec: Optional[int]
    updated_at: int

# ---------------------------
# App & state
# ---------------------------
app = FastAPI(title="Headway Guard")

BASE_DIR = Path(__file__).resolve().parent
DRIVER_HTML = (BASE_DIR / "driver.html").read_text(encoding="utf-8")
DISPATCHER_HTML = (BASE_DIR / "dispatcher.html").read_text(encoding="utf-8")
MAP_HTML = (BASE_DIR / "map.html").read_text(encoding="utf-8")
TESTMAP_HTML = (BASE_DIR / "testmap.html").read_text(encoding="utf-8")
KIOSKMAP_HTML = (BASE_DIR / "kioskmap.html").read_text(encoding="utf-8")
CATTESTMAP_HTML = (BASE_DIR / "cattestmap.html").read_text(encoding="utf-8")
MADMAP_HTML = (BASE_DIR / "madmap.html").read_text(encoding="utf-8")
METROMAP_HTML = (BASE_DIR / "metromap.html").read_text(encoding="utf-8")
ADMIN_HTML = (BASE_DIR / "admin.html").read_text(encoding="utf-8")
SERVICECREW_HTML = (BASE_DIR / "servicecrew.html").read_text(encoding="utf-8")
LANDING_HTML = (BASE_DIR / "index.html").read_text(encoding="utf-8")
APICALLS_HTML = (BASE_DIR / "apicalls.html").read_text(encoding="utf-8")
DEBUG_HTML = (BASE_DIR / "debug.html").read_text(encoding="utf-8")
REPLAY_HTML = (BASE_DIR / "replay.html").read_text(encoding="utf-8")
RIDERSHIP_HTML = (BASE_DIR / "ridership.html").read_text(encoding="utf-8")
TRANSLOC_TICKER_HTML = (BASE_DIR / "transloc_ticker.html").read_text(encoding="utf-8")
ARRIVALSDISPLAY_HTML = (BASE_DIR / "arrivalsdisplay.html").read_text(encoding="utf-8")
BUS_TABLE_HTML = (BASE_DIR / "buses.html").read_text(encoding="utf-8")
NOT_FOUND_HTML = (BASE_DIR / "404.html").read_text(encoding="utf-8")
RADAR_HTML = (BASE_DIR / "radar.html").read_text(encoding="utf-8")

ADSB_URL_TEMPLATE = "https://opendata.adsb.fi/api/v2/lat/{lat}/lon/{lon}/dist/{dist}"
ADSB_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "*",
}

@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    if "text/html" in request.headers.get("accept", ""):
        return HTMLResponse(NOT_FOUND_HTML, status_code=404)
    detail = getattr(exc, "detail", "Not Found")
    return JSONResponse({"detail": detail}, status_code=404)

API_CALL_LOG = deque(maxlen=100)
API_CALL_SUBS: set[asyncio.Queue] = set()
SERVICECREW_SUBS: set[asyncio.Queue] = set()

def record_api_call(method: str, url: str, status: int) -> None:
    item = {"ts": int(time.time()*1000), "method": method, "url": url, "status": status}
    API_CALL_LOG.append(item)
    for q in list(API_CALL_SUBS):
        q.put_nowait(item)

CONFIG_KEYS = [
    "TRANSLOC_BASE","TRANSLOC_KEY","OVERPASS_EP",
    "VEH_REFRESH_S","ROUTE_REFRESH_S","STALE_FIX_S","ROUTE_GRACE_S",
    "URBAN_FACTOR","GREEN_FRAC","RED_FRAC","ONTARGET_TOL_SEC","W_LIMIT",
    "EMA_ALPHA","MIN_SPEED_FLOOR","MAX_SPEED_CEIL","LEADER_EPS_M",
    "DEFAULT_CAP_MPS","BRIDGE_LAT","BRIDGE_LON","LOW_CLEARANCE_SEARCH_M",
    "LOW_CLEARANCE_LIMIT_FT","BRIDGE_IGNORE_RADIUS_M","OVERHEIGHT_BUSES",
    "LOW_CLEARANCE_RADIUS","BRIDGE_RADIUS","ALL_BUSES"
]
CONFIG_NAME = "config.json"
CONFIG_FILE = PRIMARY_DATA_DIR / CONFIG_NAME

class TTLCache:
    def __init__(self, ttl: float):
        self.ttl = ttl
        self.value: Any = None
        self.ts: float = 0.0
        self.lock = asyncio.Lock()

    async def get(self, fetcher):
        async with self.lock:
            now = time.time()
            if self.value is not None and now - self.ts < self.ttl:
                return self.value
        data = await fetcher()
        async with self.lock:
            self.value = data
            self.ts = time.time()
        return data


class PerKeyTTLCache:
    def __init__(self, ttl: float):
        self.ttl = ttl
        self._caches: Dict[Any, TTLCache] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: Any, fetcher):
        async with self._lock:
            cache = self._caches.get(key)
            if cache is None:
                cache = TTLCache(self.ttl)
                self._caches[key] = cache
        return await cache.get(fetcher)


def load_config() -> None:
    for base in DATA_DIRS:
        path = base / CONFIG_NAME
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except Exception as e:
            print(f"[load_config] error: {e}")
            continue
        for k, v in data.items():
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

def save_config() -> None:
    try:
        payload = json.dumps({k: globals().get(k) for k in CONFIG_KEYS})
    except Exception as e:
        print(f"[save_config] encode error: {e}")
        return
    for base in DATA_DIRS:
        path = base / CONFIG_NAME
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload)
        except Exception as e:
            print(f"[save_config] error writing {path}: {e}")
    propagate_file(CONFIG_NAME, payload)


load_config()

class State:
    def __init__(self):
        self.routes: Dict[int, Route] = {}
        self.vehicles_by_route: Dict[int, Dict[int, Vehicle]] = {}
        self.headway_ema: Dict[int, float] = {}
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

state = State()
MILEAGE_NAME = "mileage.json"
MILEAGE_FILE = PRIMARY_DATA_DIR / MILEAGE_NAME

transloc_arrivals_cache = TTLCache(TRANSLOC_ARRIVALS_TTL_S)
transloc_blocks_cache = TTLCache(TRANSLOC_BLOCKS_TTL_S)
cat_routes_cache = TTLCache(CAT_METADATA_TTL_S)
cat_stops_cache = TTLCache(CAT_METADATA_TTL_S)
cat_patterns_cache = TTLCache(CAT_METADATA_TTL_S)
cat_vehicles_cache = TTLCache(CAT_VEHICLE_TTL_S)
cat_service_alerts_cache = TTLCache(CAT_SERVICE_ALERT_TTL_S)
cat_stop_etas_cache = PerKeyTTLCache(CAT_STOP_ETA_TTL_S)
pulsepoint_cache = TTLCache(PULSEPOINT_TTL_S)
amtraker_cache = TTLCache(AMTRAKER_TTL_S)
ridesystems_clients_cache = TTLCache(RIDESYSTEMS_CLIENT_TTL_S)
adsb_cache: Dict[Tuple[str, str, str], Tuple[float, Any]] = {}
adsb_cache_lock = asyncio.Lock()

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
    for base in DATA_DIRS:
        path = base / VEHICLE_HEADINGS_NAME
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload_json)
        except Exception as e:
            print(f"[vehicle_headings] error writing {path}: {e}")
    propagate_file(VEHICLE_HEADINGS_NAME, payload_json)


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
    for base in DATA_DIRS:
        path = base / MILEAGE_NAME
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload_json)
        except Exception as e:
            print(f"[save_bus_days] error writing {path}: {e}")
    propagate_file(MILEAGE_NAME, payload_json)

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
        or name not in {CONFIG_NAME, MILEAGE_NAME, VEHICLE_HEADINGS_NAME}
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

# ---------------------------
# Health
# ---------------------------
@app.get("/v1/health")
async def health():
    async with state.lock:
        ok = not bool(state.last_error)
        return {"ok": ok, "last_error": (state.last_error or None), "last_error_ts": (state.last_error_ts or None)}

# ---------------------------
# Startup background updater
# ---------------------------
@app.on_event("startup")
async def startup():
    async with state.lock:
        load_bus_days()
        load_vehicle_headings()

    async def updater():
        await asyncio.sleep(0.1)
        async with httpx.AsyncClient() as client:
            while True:
                start = time.time()
                try:
                    routes_catalog: List[Dict[str, Any]] = []
                    routes_raw = await fetch_routes_with_shapes(client)
                    try:
                        routes_catalog = await fetch_routes_catalog(client)
                    except Exception as e:
                        routes_catalog = []
                        print(f"[updater] routes catalog fetch error: {e}")
                    vehicles_raw = await fetch_vehicles(client, include_unassigned=True)
                    try:
                        block_groups = await fetch_block_groups(client)
                    except Exception as e:
                        block_groups = []
                        print(f"[updater] block fetch error: {e}")
                    async with state.lock:
                        state.routes_raw = routes_raw
                        state.routes_catalog_raw = routes_catalog
                        state.vehicles_raw = vehicles_raw
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
                            if not hasattr(state, "roster_names"):
                                state.roster_names = set()
                            for _v in vehicles_raw or []:
                                nm = str(_v.get("Name") or "-")
                                if nm:
                                    state.roster_names.add(nm)
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
                            raw_heading = v.get("Heading")
                            try:
                                heading = float(raw_heading) if raw_heading is not None else 0.0
                            except (TypeError, ValueError):
                                heading = 0.0
                            if (prev is None or getattr(prev, "heading", None) is None) and vid is not None:
                                cached = state.last_headings.get(vid)
                                cached_heading = cached.get("heading") if isinstance(cached, dict) else None
                                if cached_heading is not None and math.isfinite(cached_heading):
                                    heading = float(cached_heading)
                            if prev and prev.lat is not None and prev.lon is not None and lat is not None and lon is not None:
                                move = haversine((prev.lat, prev.lon), (lat, lon))
                                if move >= HEADING_JITTER_M:
                                    heading = bearing_between((prev.lat, prev.lon), (lat, lon))
                                else:
                                    heading = prev.heading
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
                        state.blocks_cache = {
                            "block_groups": block_groups,
                            "color_by_route": color_by_route,
                            "route_by_bus": route_by_bus,
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
        async with httpx.AsyncClient(timeout=20) as client:
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
                        vid = v.get("VehicleID")
                        lat = v.get("Latitude")
                        lon = v.get("Longitude")
                        if vid is None or lat is None or lon is None:
                            continue
                        pos = (lat, lon)
                        last = LAST_LOG_POS.get(vid)
                        if not moved and (last is None or haversine(pos, last) >= VEH_LOG_MIN_MOVE_M):
                            moved = True
                        LAST_LOG_POS[vid] = pos
                        valid.append(v)
                    vehicles = valid
                    if not moved or not vehicles:
                        await asyncio.sleep(VEH_LOG_INTERVAL_S)
                        continue
                    vehicle_ids = {v.get("VehicleID") for v in vehicles}

                    blocks: Dict[int, str] = {}
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
                    entry = {"ts": ts, "vehicles": vehicles, "blocks": blocks}
                    fname = datetime.fromtimestamp(ts/1000).strftime("%Y%m%d_%H.jsonl")
                    for log_dir in VEH_LOG_DIRS:
                        path = log_dir / fname
                        path.parent.mkdir(parents=True, exist_ok=True)
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
    """
    try:
        async with httpx.AsyncClient() as client:
            data = await fetch_vehicles(client, include_unassigned=bool(include_unassigned))
    except Exception as e:
        raise HTTPException(502, f"transit feed error: {e}")

    items = []
    seen = set()
    for v in data or []:
        name = str(v.get("Name") or "-")
        if name in seen:
            continue
        seen.add(name)
        age = v.get("Seconds")
        if not include_stale and (age is None or age > STALE_FIX_S):
            continue
        items.append({
            "id": v.get("VehicleID"),
            "name": name,
            "route_id": v.get("RouteID"),
            "age_seconds": age,
        })
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

@app.get("/v1/dispatch/blocks")
async def dispatch_blocks():
    async with state.lock:
        if state.blocks_cache:
            return state.blocks_cache
    async with httpx.AsyncClient() as client:
        block_groups = await fetch_block_groups(client)
    async with state.lock:
        color_by_route = {rid: r.color for rid, r in state.routes.items() if r.color}
        route_by_bus: Dict[str, int] = {}
        for rid, vehs in state.vehicles_by_route.items():
            for v in vehs.values():
                route_by_bus[str(v.name)] = rid
        res = {
            "block_groups": block_groups,
            "color_by_route": color_by_route,
            "route_by_bus": route_by_bus,
        }
        state.blocks_cache = res
        state.blocks_cache_ts = time.time()
        return res


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
        "RouteName": raw.get("RouteName"),
        "LongName": raw.get("LongName"),
        "ShortName": raw.get("ShortName"),
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
                "RouteStopId": route_stop_id,
                "StopID": stop_id,
                "StopId": stop_id,
                "StopName": name,
                "Name": name,
                "Description": stop.get("Description") or name,
                "Latitude": stop.get("Latitude") or stop.get("Lat"),
                "Longitude": stop.get("Longitude") or stop.get("Lon") or stop.get("Lng"),
                "AddressID": stop.get("AddressID") or stop.get("AddressId"),
                "RouteID": rid,
                "RouteIds": [rid] if rid is not None else [],
                "RouteIDs": [rid] if rid is not None else [],
                "Routes": [{"RouteID": rid}] if rid is not None else [],
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


def _build_transloc_stops(routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    stops: List[Dict[str, Any]] = []
    for route in routes:
        rid = route.get("RouteID")
        for stop in route.get("Stops", []):
            entry = dict(stop)
            entry.setdefault("RouteID", rid)
            entry.setdefault("RouteIds", [rid] if rid is not None else [])
            entry.setdefault("RouteIDs", [rid] if rid is not None else [])
            entry.setdefault("Routes", [{"RouteID": rid}] if rid is not None else [])
            stops.append(entry)
    return stops


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


async def _fetch_transloc_arrivals_for_base(base_url: Optional[str]) -> List[Dict[str, Any]]:
    async with httpx.AsyncClient() as client:
        url = build_transloc_url(base_url, f"GetStopArrivalTimes?APIKey={TRANSLOC_KEY}")
        resp = await client.get(url, timeout=20)
        record_api_call("GET", url, resp.status_code)
        resp.raise_for_status()
        data = resp.json()
    return _trim_arrivals_payload(data)


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


async def _fetch_transloc_blocks_for_base(base_url: Optional[str]) -> Dict[str, str]:
    async with httpx.AsyncClient() as client:
        block_groups = await fetch_block_groups(client, base_url=base_url)
    return _build_block_mapping(block_groups)


async def _get_transloc_blocks(base_url: Optional[str] = None) -> Dict[str, str]:
    if is_default_transloc_base(base_url):
        async def fetch():
            return await _fetch_transloc_blocks_for_base(DEFAULT_TRANSLOC_BASE)

        return await transloc_blocks_cache.get(fetch)

    return await _fetch_transloc_blocks_for_base(base_url)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


async def build_transloc_snapshot(base_url: Optional[str] = None) -> Dict[str, Any]:
    use_default = is_default_transloc_base(base_url)
    routes_raw: List[Dict[str, Any]] = []
    extra_routes_raw: List[Dict[str, Any]] = []
    assigned: Dict[Any, Tuple[int, Vehicle]] = {}
    if use_default:
        async with state.lock:
            routes_raw = list(getattr(state, "routes_raw", []))
            extra_routes_raw = list(getattr(state, "routes_catalog_raw", []))
            for rid, vehs in state.vehicles_by_route.items():
                for veh in vehs.values():
                    if veh.id is None:
                        continue
                    assigned[veh.id] = (rid, veh)
            raw_vehicle_records = list(getattr(state, "vehicles_raw", []))
    else:
        async with httpx.AsyncClient() as client:
            routes_raw = await fetch_routes_with_shapes(client, base_url=base_url)
            try:
                extra_routes_raw = await fetch_routes_catalog(client, base_url=base_url)
            except Exception as e:
                extra_routes_raw = []
                print(f"[snapshot] routes catalog fetch error: {e}")
            vehicles_raw = await fetch_vehicles(
                client, include_unassigned=True, base_url=base_url
            )
        assigned = {}
        raw_vehicle_records = list(vehicles_raw)
    raw_routes = [_trim_transloc_route(r) for r in routes_raw]
    if extra_routes_raw:
        raw_routes = _merge_transloc_route_metadata(raw_routes, extra_routes_raw)

    stops = _build_transloc_stops(raw_routes)
    arrivals = await _get_transloc_arrivals(base_url)
    blocks = await _get_transloc_blocks(base_url)

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
        is_stale = False
        assigned_rec = assigned.get(vid)
        if assigned_rec:
            rid = assigned_rec[0]
            veh = assigned_rec[1]
            heading = getattr(veh, "heading", heading)
            ground_speed = veh.ground_mps / MPH_TO_MPS
            seconds = _coerce_float(getattr(veh, "age_s", seconds))
        is_stale = bool(seconds is not None and seconds > STALE_FIX_S)
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
        if age_for_filter is not None and age_for_filter >= VEHICLE_STALE_THRESHOLD_S:
            continue
        seconds_output = seconds if seconds is not None else seconds_raw
        vehicles.append(
            {
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
            }
        )

    return {
        "fetched_at": int(time.time()),
        "routes": raw_routes,
        "stops": stops,
        "vehicles": vehicles,
        "arrivals": arrivals,
        "blocks": blocks,
    }


@app.get("/v1/testmap/transloc")
async def testmap_transloc_snapshot(base_url: Optional[str] = Query(None)):
    return await build_transloc_snapshot(base_url=base_url)


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
                "ETAs": entry.get("ETAs") or entry.get("etas") or entry.get("MinutesToStops"),
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
                eta_stop_id = (
                    eta.get("stopId")
                    or eta.get("StopID")
                    or eta.get("StopId")
                    or stop_id
                )
                route_id = eta.get("routeID") or eta.get("RouteID") or eta.get("routeId")
                route_key = eta.get("route") or eta.get("Route") or route_id
                minutes = eta.get("minutes") or eta.get("Minutes")
                seconds = eta.get("seconds") or eta.get("Seconds")
                text = eta.get("text") or eta.get("Text")
                if not text:
                    status = eta.get("status") or eta.get("Status")
                    if status and minutes is not None:
                        text = f"{status}"
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
        return _decrypt_pulsepoint_payload(data)

    return await pulsepoint_cache.get(fetch)


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


@app.get("/v1/testmap/pulsepoint")
async def pulsepoint_endpoint():
    return await _get_pulsepoint_incidents()


@app.get("/v1/testmap/trains")
async def trains_endpoint():
    return await _get_amtraker_trains()


@app.get("/v1/testmap/ridesystems/clients")
async def ridesystems_clients_endpoint():
    clients = await _fetch_ridesystems_clients()
    return {"clients": clients}

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
    async with httpx.AsyncClient() as client:
        url = f"{TRANSLOC_BASE}/GetAntiBunching"
        r = await client.get(url, timeout=20)
        record_api_call("GET", url, r.status_code)
        r.raise_for_status()
        data = r.json()
    async with state.lock:
        state.anti_cache = data
        state.anti_cache_ts = time.time()
    return data

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
# REST: Per-route status & instruction
# ---------------------------
@app.get("/v1/routes/{route_id}/status", response_model=List[VehicleView])
async def route_status(route_id: int):
    async with state.lock:
        route = state.routes.get(route_id)
        vehs = state.vehicles_by_route.get(route_id, {})
        if not route:
            # If the route isn't currently active, return empty list instead of 404
            return []
        return compute_status_for_route(route, vehs)

@app.get("/v1/routes/{route_id}/debug")
async def route_debug(route_id: int):
    async with state.lock:
        route = state.routes.get(route_id)
        vehs = state.vehicles_by_route.get(route_id, {})
        if not route:
            return []
        views = compute_status_for_route(route, vehs)
        by_id = {v.id: v for v in vehs.values()}
        out = []
        for vv in views:
            raw = by_id.get(vv.id)
            d = {
                "id": vv.id,
                "name": vv.name,
                "status": vv.status,
                "headway_sec": vv.headway_sec,
                "target_headway_sec": vv.target_headway_sec,
                "gap_label": vv.gap_label,
                "leader_name": vv.leader_name,
                "countdown_sec": vv.countdown_sec,
                "updated_at": vv.updated_at,
            }
            if raw:
                seg_idx = find_seg_index_at_s(route.cum, raw.s_pos)
                cap = route.seg_caps_mps[seg_idx] if route.seg_caps_mps else DEFAULT_CAP_MPS
                d.update({
                    "lat": raw.lat,
                    "lon": raw.lon,
                    "s_pos": raw.s_pos,
                    "ground_mps": raw.ground_mps,
                    "ema_mps": raw.ema_mps,
                    "dir_sign": raw.dir_sign,
                    "heading": raw.heading,
                    "age_s": raw.age_s,
                    "speed_limit_mph": cap / MPH_TO_MPS,
                })
            out.append(d)
        return out

@app.get("/v1/routes/{route_id}/vehicles/{vehicle_name}/instruction")
async def vehicle_instruction(route_id: int, vehicle_name: str):
    async with state.lock:
        route = state.routes.get(route_id)
        if not route:
            raise HTTPException(404, "route not found or inactive")
        vehs = state.vehicles_by_route.get(route_id, {})
        # find by Name (display), not ID
        me = next((v for v in vehs.values() if str(v.name) == vehicle_name), None)
        if not me:
            return JSONResponse({"order":"Waiting","headway":"—","target":"—","gap":"—","countdown":"—","leader":"—","updated_at": int(time.time())})
        views = compute_status_for_route(route, vehs)
        vv = next((x for x in views if x.name == vehicle_name), None)
        if not vv:
            return JSONResponse({"order":"Waiting","headway":"—","target":"—","gap":"—","countdown":"—","leader":"—","updated_at": int(time.time())})
        # human form
        order_map = {"green":"OK","yellow":"Ease off","red":"HOLD"}
        return {
            "order": order_map.get(vv.status, "OK"),
            "headway": f"{int(vv.headway_sec//60):02d}:{int(vv.headway_sec%60):02d}" if vv.headway_sec is not None else "—",
            "target": f"{int(vv.target_headway_sec//60):02d}:{int(vv.target_headway_sec%60):02d}" if vv.target_headway_sec else "—",
            "gap": vv.gap_label or "—",
            "countdown": f"{int(vv.countdown_sec//60):02d}:{int(vv.countdown_sec%60):02d}" if vv.countdown_sec is not None else "—",
            "leader": vv.leader_name or "—",
            "updated_at": vv.updated_at
        }

# ---------------------------
# SSE: Per-route stream
# ---------------------------
@app.get("/v1/stream/routes/{route_id}")
async def stream_route(route_id: int):
    async def gen():
        # poll every refresh and emit current list as JSON
        while True:
            async with state.lock:
                route = state.routes.get(route_id)
                vehs = state.vehicles_by_route.get(route_id, {})
                if route:
                    rows = compute_status_for_route(route, vehs)
                else:
                    rows = []
            yield f"data: {json.dumps([r.__dict__ for r in rows])}\n\n"
            await asyncio.sleep(VEH_REFRESH_S)
    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------------------------
# SSE: External API calls
# ---------------------------
@app.get("/v1/stream/api_calls")
async def stream_api_calls():
    async def gen():
        q: asyncio.Queue = asyncio.Queue()
        API_CALL_SUBS.add(q)
        try:
            for item in list(API_CALL_LOG):
                yield f"data: {json.dumps(item)}\n\n"
            while True:
                item = await q.get()
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            API_CALL_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------------------------
# Static assets
# ---------------------------

def _serve_js_asset(name: str) -> FileResponse:
    return FileResponse(BASE_DIR / name, media_type="application/javascript")


def _serve_css_asset(name: str) -> FileResponse:
    return FileResponse(BASE_DIR / name, media_type="text/css")


@app.get("/FGDC.ttf", include_in_schema=False)
async def fgdc_font():
    return FileResponse(BASE_DIR / "FGDC.ttf", media_type="font/ttf")


@app.get("/busmarker.svg", include_in_schema=False)
async def busmarker_svg():
    return FileResponse(BASE_DIR / "busmarker.svg", media_type="image/svg+xml")


@app.get("/radar.wav", include_in_schema=False)
async def radar_wav():
    return FileResponse(BASE_DIR / "radar.wav", media_type="audio/wav")


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


@app.get("/testmap.css", include_in_schema=False)
async def testmap_css():
    return _serve_css_asset("testmap.css")


@app.get("/kioskmap.css", include_in_schema=False)
async def kioskmap_css():
    return _serve_css_asset("kioskmap.css")


@app.get("/kioskmap.js", include_in_schema=False)
async def kioskmap_js():
    return _serve_js_asset("kioskmap.js")


@app.get("/vehicle_log/{log_name}", include_in_schema=False)
async def vehicle_log_file(log_name: str):
    if not re.fullmatch(r"\d{8}_\d{2}\.jsonl", log_name):
        raise HTTPException(status_code=404, detail="Invalid log file")
    path = None
    for log_dir in VEH_LOG_DIRS:
        p = log_dir / log_name
        if p.exists():
            path = p
            break
    if path is None:
        raise HTTPException(status_code=404, detail="Log file not found")
    return FileResponse(path, media_type="application/json")

# ---------------------------
# LANDING PAGE
# ---------------------------
@app.get("/")
async def landing_page():
    return HTMLResponse(LANDING_HTML)

# ---------------------------
# MAP PAGE
# ---------------------------
@app.get("/map")
async def map_page():
    return HTMLResponse(MAP_HTML)


@app.get("/radar")
async def radar_page():
    return HTMLResponse(RADAR_HTML)

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
# ADMIN PAGE
# ---------------------------
@app.get("/admin")
async def admin_page():
    return HTMLResponse(ADMIN_HTML)

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
    save_config()
    return {k: globals().get(k) for k in CONFIG_KEYS}

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
        q: asyncio.Queue = asyncio.Queue()
        SERVICECREW_SUBS.add(q)
        try:
            while True:
                item = await q.get()
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            SERVICECREW_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.post("/v1/servicecrew/refresh")
async def servicecrew_refresh():
    item = {"ts": int(time.time()*1000)}
    for q in list(SERVICECREW_SUBS):
        q.put_nowait(item)
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
@app.get("/dispatcher")
async def dispatcher_page():
    return HTMLResponse(DISPATCHER_HTML)

# ---------------------------
# API CALLS PAGE
# ---------------------------
@app.get("/apicalls")
async def apicalls_page():
    return HTMLResponse(APICALLS_HTML)

# ---------------------------
# RIDERSHIP PAGE
# ---------------------------
@app.get("/ridership")
async def ridership_page():
    return HTMLResponse(RIDERSHIP_HTML)

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
# REPLAY PAGE
# ---------------------------
@app.get("/replay")
async def replay_page():
    return HTMLResponse(REPLAY_HTML)
