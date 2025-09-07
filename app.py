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
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
import asyncio, time, math, os, json, re
from datetime import datetime
from zoneinfo import ZoneInfo
import httpx
from collections import deque

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, FileResponse
from pathlib import Path
from urllib.parse import quote

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

VEH_REFRESH_S   = int(os.getenv("VEH_REFRESH_S", "10"))
ROUTE_REFRESH_S = int(os.getenv("ROUTE_REFRESH_S", "60"))
BLOCK_REFRESH_S = int(os.getenv("BLOCK_REFRESH_S", "30"))
STALE_FIX_S     = int(os.getenv("STALE_FIX_S", "90"))

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

def cumulative_distance(poly: List[Tuple[float,float]]) -> Tuple[List[float], float]:
    cum = [0.0]
    for i in range(1, len(poly)):
        cum.append(cum[-1] + haversine(poly[i-1], poly[i]))
    return cum, cum[-1] if cum else 0.0

def parse_msajax(s: Optional[str]) -> Optional[int]:
    # e.g. "/Date(1693622400000-0400)/" -> 1693622400000
    if not s: return None
    try:
        i = s.find("("); j = s.find(")")
        return int(s[i+1:j].split("-")[0])
    except (ValueError, IndexError) as e:
        print(f"[parse_msajax] invalid timestamp {s!r}: {e}")
        return None


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
async def fetch_routes_with_shapes(client: httpx.AsyncClient):
    url = f"{TRANSLOC_BASE}/GetRoutesForMapWithScheduleWithEncodedLine?APIKey={TRANSLOC_KEY}"
    r = await client.get(url, timeout=20)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_vehicles(client: httpx.AsyncClient, include_unassigned: bool = True):
    # returnVehiclesNotAssignedToRoute=true returns vehicles even if not assigned to a route
    flag = "true" if include_unassigned else "false"
    url = f"{TRANSLOC_BASE}/GetMapVehiclePoints?APIKey={TRANSLOC_KEY}&returnVehiclesNotAssignedToRoute={flag}"
    r = await client.get(url, timeout=20)
    record_api_call("GET", url, r.status_code)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_block_groups(client: httpx.AsyncClient) -> List[Dict]:
    d = datetime.now(ZoneInfo("America/New_York"))
    ds = f"{d.month}/{d.day}/{d.year}"
    r1_url = f"{TRANSLOC_BASE}/GetScheduleVehicleCalendarByDateAndRoute?dateString={quote(ds)}"
    r1 = await client.get(r1_url, timeout=20)
    record_api_call("GET", r1_url, r1.status_code)
    r1.raise_for_status()
    sched = r1.json()
    sched = sched if isinstance(sched, list) else sched.get("d", [])
    ids = ",".join(str(s.get("ScheduleVehicleCalendarID")) for s in sched if s.get("ScheduleVehicleCalendarID"))
    if ids:
        r2_url = f"{TRANSLOC_BASE}/GetDispatchBlockGroupData?scheduleVehicleCalendarIdsString={ids}"
        r2 = await client.get(r2_url, timeout=20)
        record_api_call("GET", r2_url, r2.status_code)
        r2.raise_for_status()
        return r2.json().get("BlockGroups", [])
    return []

async def fetch_overpass_speed_profile(route: Route, client: httpx.AsyncClient) -> List[float]:
    """Build per-segment speed caps (m/s) using OSM maxspeed data."""
    pts = route.poly
    if len(pts) < 2:
        return []
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
        return [DEFAULT_CAP_MPS for _ in range(len(pts)-1)]

    ways: List[Dict[str, Any]] = []
    for el in data.get("elements", []):
        mph = parse_maxspeed(el.get("tags", {}).get("maxspeed"))
        if mph is None:
            continue
        ways.append({
            "speed_mps": mph * MPH_TO_MPS,
            "geometry": el.get("geometry", []),
        })

    caps: List[float] = []
    for i in range(len(pts)-1):
        a = pts[i]
        b = pts[i+1]
        mid_lat = (a[0] + b[0]) / 2
        mid_lon = (a[1] + b[1]) / 2
        best_speed = DEFAULT_CAP_MPS
        best_d = 50.0  # meters
        for way in ways:
            speed = way["speed_mps"]
            for node in way["geometry"]:
                d = haversine((mid_lat, mid_lon), (node.get("lat"), node.get("lon")))
                if d < best_d:
                    best_d = d
                    best_speed = speed
        caps.append(best_speed)

    return caps


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

def project_vehicle_to_route(v: Vehicle, route: Route) -> Tuple[float, int]:
    """Project vehicle to the nearest point on the polyline (by segment),
    returning cumulative arc-length s (meters) and the segment index.
    This fixes off-by-one leader selection caused by nearest-vertex snapping.
    """
    pts = route.poly; cum = route.cum
    best_d2 = 1e30
    best_s = 0.0
    best_i = 0
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
        if d2 < best_d2:
            best_d2 = d2
            seg_len = haversine((a_lat, a_lon), (b_lat, b_lon))
            best_s = cum[i] + t * seg_len
            best_i = i
    return best_s, best_i

def target_headway_sec(route: Route, veh_count: int) -> float:
    # speed-limit-based lap time / veh_count
    if route.length_m <= 0 or veh_count <= 0: return 0.0
    avg_cap = sum(route.seg_caps_mps)/max(1,len(route.seg_caps_mps)) if route.seg_caps_mps else DEFAULT_CAP_MPS
    lap = (route.length_m / max(avg_cap, 0.1)) * URBAN_FACTOR
    return lap / veh_count

def compute_status_for_route(route: Route, vehs_by_id: Dict[int, Vehicle]) -> List["VehicleView"]:
    """Compute per-vehicle status with a fresh leader selection algorithm.

    Buses may face one another on bidirectional sections of a route and can stop
    for extended periods.  Rather than relying on a global ordering that breaks
    near wrap points, we build direction "rings" and, within each ring, sort
    vehicles by their along-route position ``s_pos``.  The leader of a bus is the
    next vehicle in travel direction order (wrapping around the loop).  Stopped
    vehicles remain in the ring so they continue to act as leaders for following
    buses.  The routine handles any number of vehicles.
    """
    vs = list(vehs_by_id.values())
    if not vs:
        return []

    L = max(1.0, route.length_m)

    # Partition by travel direction.  Vehicles with unknown direction latch onto
    # the larger group so that they still participate in headway calculations.
    plus = [v for v in vs if v.dir_sign > 0]
    minus = [v for v in vs if v.dir_sign < 0]
    zeros = [v for v in vs if v.dir_sign == 0]
    if len(plus) >= len(minus):
        plus.extend(zeros)
    else:
        minus.extend(zeros)

    # If one ring is entirely stationary, merge rings so stopped buses still act
    # as leaders for moving vehicles.
    def all_stopped(group: List[Vehicle]) -> bool:
        return all(abs(v.ground_mps) < STOPPED_MPS for v in group)

    if plus and minus and (all_stopped(plus) or all_stopped(minus)):
        plus = vs
        minus = []

    # If one ring is empty while the other has vehicles, merge them so every bus
    # receives guidance.  When both rings have at least one bus we keep them
    # separate to avoid mixing opposing directions.
    if (len(plus) == 0 or len(minus) == 0) and len(vs) > 1:
        plus = vs
        minus = []

    def build_ring(group: List[Vehicle], forward: bool) -> List[VehicleView]:
        if not group:
            return []

        th = target_headway_sec(route, len(group))
        out: List[VehicleView] = []

        def ref_speed_for(v: Vehicle) -> float:
            seg_idx = find_seg_index_at_s(route.cum, v.s_pos)
            cap = route.seg_caps_mps[seg_idx] if route.seg_caps_mps else DEFAULT_CAP_MPS
            return max(
                MIN_SPEED_FLOOR,
                min(MAX_SPEED_CEIL, W_LIMIT * cap + (1 - W_LIMIT) * v.ema_mps),
            )

        ordered = sorted(group, key=lambda v: v.s_pos)
        n = len(ordered)

        for idx, me in enumerate(ordered):
            if n == 1:
                out.append(
                    VehicleView(
                        id=me.id,
                        name=me.name,
                        status="green",
                        headway_sec=None,
                        target_headway_sec=int(th) if th > 0 else None,
                        gap_label="—",
                        leader_name=None,
                        countdown_sec=None,
                        updated_at=min(me.ts_ms, int(time.time() * 1000)) // 1000,
                    )
                )
                continue

            leader = ordered[(idx + 1) % n] if forward else ordered[(idx - 1) % n]
            ds = (leader.s_pos - me.s_pos) % L if forward else (me.s_pos - leader.s_pos) % L
            ds = max(ds, 0.5)  # ensure non-zero gap so stationary leaders still count

            ref_speed = ref_speed_for(me)
            headway = ds / max(ref_speed, 0.1)
            diff = headway - th

            if th > 0 and headway < RED_FRAC * th:
                status = "red"
                gap = f"Ahead {fmt_mmss(diff)}"
                countdown = int(max(0, th - headway))
            elif th > 0 and headway < GREEN_FRAC * th:
                status = "yellow"
                gap = f"Ahead {fmt_mmss(diff)}"
                countdown = int(max(0, th - headway))
            else:
                status = "green"
                if abs(diff) <= ONTARGET_TOL_SEC:
                    gap = "On target"
                elif diff < 0:
                    gap = f"Ahead {fmt_mmss(diff)}"
                else:
                    gap = f"Behind {fmt_mmss(diff)}"
                countdown = None

            out.append(
                VehicleView(
                    id=me.id,
                    name=me.name,
                    status=status,
                    headway_sec=int(headway),
                    target_headway_sec=int(th) if th > 0 else None,
                    gap_label=gap,
                    leader_name=leader.name,
                    countdown_sec=(countdown if countdown is not None else None),
                    updated_at=min(me.ts_ms, int(time.time() * 1000)) // 1000,
                )
            )

        return out

    views: List[VehicleView] = []
    views.extend(build_ring(plus, forward=True))
    views.extend(build_ring(minus, forward=False))

    # Present forward ring first (sorted by along-route position), then reverse
    id_to_v = {v.id: v for v in vs if v.id is not None}

    def sort_key(vv: VehicleView):
        v = id_to_v.get(vv.id)
        dir_bucket = 0 if (v and v.dir_sign > 0) else 1
        s = v.s_pos if v else 0.0
        return (dir_bucket, round(s, 3), vv.updated_at)

    return sorted(views, key=sort_key)
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
ADMIN_HTML = (BASE_DIR / "admin.html").read_text(encoding="utf-8")
SERVICECREW_HTML = (BASE_DIR / "servicecrew.html").read_text(encoding="utf-8")
LANDING_HTML = (BASE_DIR / "index.html").read_text(encoding="utf-8")
APICALLS_HTML = (BASE_DIR / "apicalls.html").read_text(encoding="utf-8")
DEBUG_HTML = (BASE_DIR / "debug.html").read_text(encoding="utf-8")

API_CALL_LOG = deque(maxlen=100)
API_CALL_SUBS: set[asyncio.Queue] = set()

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

state = State()

MILEAGE_FILE = Path("/data/mileage.json")

def load_bus_days() -> None:
    if not MILEAGE_FILE.exists():
        return
    try:
        data = json.loads(MILEAGE_FILE.read_text())
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
        MILEAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
        MILEAGE_FILE.write_text(json.dumps(payload))
    except Exception as e:
        print(f"[save_bus_days] error: {e}")

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

    async def updater():
        await asyncio.sleep(0.1)
        async with httpx.AsyncClient() as client:
            while True:
                start = time.time()
                try:
                    routes_raw = await fetch_routes_with_shapes(client)
                    vehicles_raw = await fetch_vehicles(client, include_unassigned=True)
                    try:
                        block_groups = await fetch_block_groups(client)
                    except Exception as e:
                        block_groups = []
                        print(f"[updater] block fetch error: {e}")
                    async with state.lock:
                        # Update complete routes & roster (all buses/all routes)
                        try:
                            state.routes_all = {}
                            for r in routes_raw:
                                rid = r.get("RouteID")
                                if not rid:
                                    continue
                                desc = r.get("Description") or f"Route {rid}"
                                info = (r.get("InfoText") or "").strip()
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
                                route.seg_caps_mps = await fetch_overpass_speed_profile(route, client)
                                state.routes[rid] = route

                        # Vehicles per route: rebuild from fresh data only to avoid lingering assignments
                        prev_map = getattr(state, 'vehicles_by_route', {})
                        new_map: Dict[int, Dict[int, Vehicle]] = {rid: {} for rid in keep_ids}
                        for v in fresh:
                            rid = v["RouteID"]
                            if rid not in keep_ids or rid not in state.routes:
                                continue
                            name = str(v.get("Name") or "-")
                            vid = v.get("VehicleID")
                            tsms = parse_msajax(v.get("TimeStampUTC") or v.get("TimeStamp")) or int(time.time()*1000)
                            mps = (v.get("GroundSpeed") or 0.0) * MPH_TO_MPS
                            heading = v.get("Heading") or 0.0
                            age_s = v.get("Seconds")
                            if age_s is None:
                                age_s = max(0, (time.time()*1000 - tsms) / 1000)
                            veh = Vehicle(id=vid, name=name, lat=v.get("Latitude"), lon=v.get("Longitude"), ts_ms=tsms,
                                          ground_mps=mps, age_s=age_s, heading=heading)
                            s_pos, _ = project_vehicle_to_route(veh, state.routes[rid])
                            prev = prev_map.get(rid, {}).get(vid)
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
                            if along_mps > DIR_EPS: dir_sign = +1
                            elif along_mps < -DIR_EPS: dir_sign = -1
                            else: dir_sign = prev_sign
                            disp = abs(along_mps)
                            measured = 0.5 * mps + 0.5 * disp if mps > 0 else (disp if prev else mps)
                            ema = EMA_ALPHA * measured + (1 - EMA_ALPHA) * ema
                            ema = max(MIN_SPEED_FLOOR, min(MAX_SPEED_CEIL, ema))
                            veh.s_pos = s_pos; veh.ema_mps = ema; veh.dir_sign = dir_sign
                            new_map[rid][vid] = veh
                            state.last_dir_sign[vid] = dir_sign
                        state.vehicles_by_route = new_map
                        # Track per-day mileage for all fresh vehicles (including RouteID 0)
                        today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
                        bus_days = state.bus_days.setdefault(today, {})
                        for v in fresh_all:
                            name = str(v.get("Name") or "-")
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
                                    bus = str(trip.get("VehicleName") or "")
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
    asyncio.create_task(updater())

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

@app.get("/v1/routes/{route_id}/vehicles_raw")
async def route_vehicles_raw(route_id: int):
    async with state.lock:
        vehs = state.vehicles_by_route.get(route_id, {})
        items = []
        for v in vehs.values():
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

@app.get("/FGDC.ttf", include_in_schema=False)
async def fgdc_font():
    return FileResponse(BASE_DIR / "FGDC.ttf", media_type="font/ttf")

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
    today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    async with state.lock:
        day = state.bus_days.setdefault(today, {})
        bd = day.setdefault(bus_name, BusDay())
        bd.reset_miles = bd.total_miles
        save_bus_days()
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
