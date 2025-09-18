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

from fastapi import FastAPI, HTTPException, Request
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

SCHEMATIC_FILE_NAME = "uts_schematic.json"
SCHEMATIC_TMP_NAME = "uts_schematic.json.tmp"
SCHEMATIC_LOCK = asyncio.Lock()

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
MADMAP_HTML = (BASE_DIR / "madmap.html").read_text(encoding="utf-8")
METROMAP_HTML = (BASE_DIR / "metromap.html").read_text(encoding="utf-8")
UTS_SCHEMATIC_HTML = (BASE_DIR / "uts-schematic.html").read_text(encoding="utf-8")
ADMIN_HTML = (BASE_DIR / "admin.html").read_text(encoding="utf-8")
SERVICECREW_HTML = (BASE_DIR / "servicecrew.html").read_text(encoding="utf-8")
LANDING_HTML = (BASE_DIR / "index.html").read_text(encoding="utf-8")
APICALLS_HTML = (BASE_DIR / "apicalls.html").read_text(encoding="utf-8")
DEBUG_HTML = (BASE_DIR / "debug.html").read_text(encoding="utf-8")
REPLAY_HTML = (BASE_DIR / "replay.html").read_text(encoding="utf-8")
RIDERSHIP_HTML = (BASE_DIR / "ridership.html").read_text(encoding="utf-8")
TRANSLOC_TICKER_HTML = (BASE_DIR / "transloc_ticker.html").read_text(encoding="utf-8")
ARRIVALSDISPLAY_HTML = (BASE_DIR / "arrivalsdisplay.html").read_text(encoding="utf-8")
REGISTERDISPLAY_HTML = (BASE_DIR / "registerdisplay.html").read_text(encoding="utf-8")
BUS_TABLE_HTML = (BASE_DIR / "buses.html").read_text(encoding="utf-8")
NOT_FOUND_HTML = (BASE_DIR / "404.html").read_text(encoding="utf-8")

@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    if "text/html" in request.headers.get("accept", ""):
        return HTMLResponse(NOT_FOUND_HTML, status_code=404)
    detail = getattr(exc, "detail", "Not Found")
    return JSONResponse({"detail": detail}, status_code=404)

DEVICE_STOP_NAME = Path(os.environ.get("DEVICE_STOP_FILE", "device_stops.json")).name
DEVICE_STOP_FILE = PRIMARY_DATA_DIR / DEVICE_STOP_NAME

def load_device_stops() -> None:
    global DEVICE_STOPS
    for base in DATA_DIRS:
        path = base / DEVICE_STOP_NAME
        try:
            raw = json.loads(path.read_text())
            DEVICE_STOPS = {}
            for k, v in raw.items():
                if isinstance(v, dict):
                    DEVICE_STOPS[k] = {
                        "stopID": v.get("stopID", ""),
                        "friendlyName": v.get("friendlyName", ""),
                    }
                else:
                    DEVICE_STOPS[k] = {"stopID": v, "friendlyName": ""}
            return
        except Exception:
            continue
    DEVICE_STOPS = {}

def save_device_stops() -> None:
    try:
        payload = json.dumps(DEVICE_STOPS)
    except Exception as e:
        print(f"[device_stops] encode error: {e}")
        return
    for base in DATA_DIRS:
        path = base / DEVICE_STOP_NAME
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(payload)
        except Exception as e:
            print(f"[device_stops] save error for {path}: {e}")
    propagate_file(DEVICE_STOP_NAME, payload)

load_device_stops()

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


def _iso_now() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def default_schema() -> Dict[str, Any]:
    now = _iso_now()
    return {"meta": {"version": 1, "updatedAt": now}, "nodes": {}, "links": [], "routes": []}


def _write_schema_sync(schema: Dict[str, Any]) -> None:
    payload = json.dumps(schema, indent=2)
    errors: list[tuple[str, Exception]] = []
    wrote = False
    for base in DATA_DIRS:
        path = base / SCHEMATIC_FILE_NAME
        tmp_path = base / SCHEMATIC_TMP_NAME
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(payload, encoding="utf-8")
            os.replace(tmp_path, path)
            wrote = True
        except Exception as e:
            errors.append((str(path), e))
    if not wrote:
        joined = "; ".join(f"{p}: {err}" for p, err in errors) or "unknown error"
        raise RuntimeError(f"failed to write schematic: {joined}")
    for p, err in errors:
        print(f"[schema] warning writing {p}: {err}")


def _read_schema_sync() -> Dict[str, Any]:
    schema: Optional[Dict[str, Any]] = None
    for base in DATA_DIRS:
        path = base / SCHEMATIC_FILE_NAME
        if not path.exists():
            continue
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
            break
        except Exception as e:
            print(f"[schema] error reading {path}: {e}")
    if not isinstance(schema, dict):
        schema = default_schema()
        _write_schema_sync(schema)
        return schema
    meta = schema.get("meta")
    if not isinstance(meta, dict):
        schema["meta"] = {"version": 1, "updatedAt": _iso_now()}
    else:
        meta.setdefault("version", 1)
        meta.setdefault("updatedAt", _iso_now())
    if not isinstance(schema.get("nodes"), dict):
        schema["nodes"] = {}
    if not isinstance(schema.get("links"), list):
        schema["links"] = []
    if not isinstance(schema.get("routes"), list):
        schema["routes"] = []
    return schema


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_schema_payload(payload: Any) -> tuple[Optional[Dict[str, Any]], list[str]]:
    if not isinstance(payload, dict):
        return None, ["Schema must be an object"]
    errors: list[str] = []
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    nodes_input = payload.get("nodes")
    if not isinstance(nodes_input, dict):
        errors.append('"nodes" must be an object')
        nodes_input = {}
    nodes: Dict[str, Dict[str, Any]] = {}
    for node_id_raw, node_raw in nodes_input.items():
        node_id = str(node_id_raw)
        if not isinstance(node_raw, dict):
            errors.append(f"Node {node_id} must be an object")
            continue
        node = dict(node_raw)
        if not _is_number(node.get("x")) or not _is_number(node.get("y")):
            errors.append(f"Node {node_id} must have numeric x/y")
        node_type = node.get("type")
        if node_type not in {"stop", "bend"}:
            errors.append('Node {node_id} must have type "stop" or "bend"'.format(node_id=node_id))
        name = node.get("name")
        if not isinstance(name, str):
            errors.append(f"Node {node_id} must have a name")
        nodes[node_id] = node
    links_input = payload.get("links")
    if not isinstance(links_input, list):
        errors.append('"links" must be an array')
        links_input = []
    links: List[Dict[str, Any]] = []
    seen_link_ids: set[str] = set()
    link_pairs: set[str] = set()
    for link_raw in links_input:
        if not isinstance(link_raw, dict):
            errors.append("Link entries must be objects")
            continue
        link = dict(link_raw)
        lid = link.get("id")
        if not isinstance(lid, str) or not lid:
            errors.append("Link id must be a string")
        elif lid in seen_link_ids:
            errors.append(f"Duplicate link id {lid}")
        else:
            seen_link_ids.add(lid)
        a_raw = link.get("a")
        b_raw = link.get("b")
        a = str(a_raw) if a_raw is not None else None
        b = str(b_raw) if b_raw is not None else None
        link["a"] = a
        link["b"] = b
        if a not in nodes:
            errors.append(f"Link {lid or '?'} references unknown node {a_raw}")
        if b not in nodes:
            errors.append(f"Link {lid or '?'} references unknown node {b_raw}")
        if a is not None and b is not None:
            if a == b:
                errors.append(f"Link {lid or '?'} must connect two different nodes")
            else:
                key = "::".join(sorted([a, b]))
                if key in link_pairs:
                    errors.append(f"Duplicate link between {a} and {b}")
                else:
                    link_pairs.add(key)
        links.append(link)
    routes_input = payload.get("routes")
    if not isinstance(routes_input, list):
        errors.append('"routes" must be an array')
        routes_input = []
    routes: List[Dict[str, Any]] = []
    for route_raw in routes_input:
        if not isinstance(route_raw, dict):
            errors.append("Route entries must be objects")
            continue
        route = dict(route_raw)
        rid = route.get("id")
        if not isinstance(rid, str) or not rid:
            errors.append("Route id must be a string")
        path_input = route.get("path")
        if not isinstance(path_input, list):
            errors.append(f"Route {rid or '?'} must have a path array")
            path = []
        else:
            path = []
            for node_id in path_input:
                node_id_str = str(node_id)
                if node_id_str not in nodes:
                    errors.append(f"Route {rid or '?'} references missing node {node_id}")
                path.append(node_id_str)
        route["path"] = path
        stops_input = route.get("stops")
        if stops_input is None:
            route["stops"] = []
        elif not isinstance(stops_input, list):
            errors.append(f"Route {rid or '?'} stops must be an array")
            route["stops"] = []
        else:
            cleaned_stops: List[Dict[str, Any]] = []
            for stop in stops_input:
                if not isinstance(stop, dict):
                    errors.append(f"Route {rid or '?'} stop entries must be objects")
                    continue
                stop_clean = dict(stop)
                node_ref = stop_clean.get("node")
                if node_ref is not None:
                    node_ref_str = str(node_ref)
                    if node_ref_str not in nodes:
                        errors.append(f"Route {rid or '?'} stop references missing node {node_ref}")
                    stop_clean["node"] = node_ref_str
                cleaned_stops.append(stop_clean)
            route["stops"] = cleaned_stops
        routes.append(route)
    schema: Dict[str, Any] = {
        "meta": dict(meta),
        "nodes": nodes,
        "links": links,
        "routes": routes,
    }
    for k, v in payload.items():
        if k not in schema:
            schema[k] = v
    return schema, errors


def _stop_display_name(stop: Dict[str, Any]) -> str:
    for key in ("SignVerbiage", "Description", "Line1", "Line2"):
        val = stop.get(key)
        if isinstance(val, str):
            name = val.strip()
            if name:
                return name
    rstop = stop.get("RouteStopID")
    if rstop:
        return f"Stop {rstop}"
    return "Stop"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_stop_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s or None
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return str(value)
    try:
        s = str(value).strip()
    except Exception:
        return None
    return s or None


def _stop_key(lat: float, lon: float) -> tuple[float, float]:
    return (round(lat, 6), round(lon, 6))


def build_schematic_from_routes(routes_raw: List[Dict[str, Any]]) -> Dict[str, Any]:
    stop_records: List[Dict[str, Any]] = []
    coord_lookup: Dict[tuple[float, float], List[Dict[str, Any]]] = {}
    address_lookup: Dict[str, Dict[str, Any]] = {}
    for route in routes_raw or []:
        rid = route.get("RouteID")
        stops = route.get("Stops")
        if rid is None or not isinstance(stops, list):
            continue
        rid_str = str(rid)
        for stop in stops:
            address_id = _normalize_stop_id(stop.get("AddressID"))
            lat = _safe_float(stop.get("Latitude"))
            lon = _safe_float(stop.get("Longitude"))
            if lat is None or lon is None:
                if not address_id:
                    continue
            coord_key = _stop_key(lat, lon) if lat is not None and lon is not None else None
            entry: Optional[Dict[str, Any]] = address_lookup.get(address_id) if address_id else None
            if entry is None and coord_key is not None:
                for candidate in coord_lookup.get(coord_key, []):
                    existing_addr = candidate.get("address_id")
                    if existing_addr and address_id and existing_addr != address_id:
                        continue
                    entry = candidate
                    break
            if entry is None:
                if lat is None or lon is None:
                    continue
                entry = {
                    "lat": lat,
                    "lon": lon,
                    "names": [],
                    "name_set": set(),
                    "routes": set(),
                    "route_stop_pairs": set(),
                    "route_stop_ids": [],
                    "coord_keys": set(),
                    "address_id": address_id,
                }
                stop_records.append(entry)
            if coord_key is not None:
                entry.setdefault("coord_keys", set()).add(coord_key)
                bucket = coord_lookup.setdefault(coord_key, [])
                if entry not in bucket:
                    bucket.append(entry)
            if address_id:
                entry["address_id"] = address_id
                address_lookup[address_id] = entry
            name = _stop_display_name(stop)
            if name:
                norm = re.sub(r"\s+", " ", name).strip()
                if norm and norm.lower() not in entry.setdefault("name_set", set()):
                    entry.setdefault("names", []).append(norm)
                    entry["name_set"].add(norm.lower())
            entry.setdefault("routes", set()).add(rid_str)
            rstop = stop.get("RouteStopID")
            if rstop is not None:
                pair = (rid_str, rstop)
                route_pairs = entry.setdefault("route_stop_pairs", set())
                if pair not in route_pairs:
                    route_pairs.add(pair)
                    payload: Dict[str, Any] = {"route": rid_str, "routeStopId": rstop}
                    if address_id:
                        payload["addressId"] = address_id
                    entry.setdefault("route_stop_ids", []).append(payload)

    if not stop_records:
        raise ValueError("no stops with coordinates in TransLoc payload")

    for entry in stop_records:
        names = entry.get("names") or ["Stop"]
        names.sort(key=lambda s: s.lower())
        entry["names"] = names
        entry["routes"] = sorted(entry.get("routes", []), key=lambda r: (len(r), r))
        entry["route_stop_ids"] = sorted(
            entry.get("route_stop_ids", []),
            key=lambda rs: (rs.get("route"), rs.get("routeStopId"))
        )

    stop_records.sort(key=lambda entry: (entry["names"][0].lower(), entry.get("lat", 0.0), entry.get("lon", 0.0)))

    nodes: Dict[str, Dict[str, Any]] = {}
    node_lookup: Dict[Any, str] = {}
    fallback_counter = 1
    for entry in stop_records:
        lat_val = entry.get("lat")
        lon_val = entry.get("lon")
        if lat_val is None or lon_val is None:
            continue
        address_id = entry.get("address_id")
        if address_id:
            node_id = address_id
        else:
            node_id = f"STOP:{fallback_counter:03d}"
            fallback_counter += 1
        names = entry["names"]
        primary = names[0]
        aliases = names[1:]
        node_data: Dict[str, Any] = {
            "name": primary,
            "type": "stop",
            "x": 0.0,
            "y": 0.0,
            "lat": lat_val,
            "lon": lon_val,
        }
        if aliases:
            node_data["aliases"] = aliases
        if entry.get("routes"):
            node_data["routes"] = entry["routes"]
        if entry.get("route_stop_ids"):
            node_data["routeStops"] = entry["route_stop_ids"]
        if address_id:
            node_data["addressId"] = address_id
        nodes[node_id] = node_data
        for coord_key in entry.get("coord_keys", set()):
            node_lookup[coord_key] = node_id
        if address_id:
            node_lookup[f"addr:{address_id}"] = node_id

    def project(lat: float, lon: float) -> tuple[float, float]:
        if lon_span < 1e-9:
            x_ratio = 0.5
        else:
            x_ratio = (lon - min_lon) / lon_span
        if lat_span < 1e-9:
            y_ratio = 0.5
        else:
            y_ratio = (max_lat - lat) / lat_span
        x = pad_x + x_ratio * usable_w
        y = pad_y + y_ratio * usable_h
        return x, y

    min_lat = min(entry["lat"] for entry in stop_records if entry.get("lat") is not None)
    max_lat = max(entry["lat"] for entry in stop_records if entry.get("lat") is not None)
    min_lon = min(entry["lon"] for entry in stop_records if entry.get("lon") is not None)
    max_lon = max(entry["lon"] for entry in stop_records if entry.get("lon") is not None)
    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon

    view_width = 1600.0
    view_height = 1200.0
    pad_x = 80.0
    pad_y = 80.0
    usable_w = max(view_width - pad_x * 2, 100.0)
    usable_h = max(view_height - pad_y * 2, 100.0)

    for node_id, node in nodes.items():
        lat = node.get("lat")
        lon = node.get("lon")
        if lat is None or lon is None:
            continue
        x, y = project(lat, lon)
        node["x"] = x
        node["y"] = y

    def route_sort_key(route: Dict[str, Any]):
        rid = route.get("id")
        try:
            return (0, int(rid))
        except (TypeError, ValueError):
            return (1, str(rid))

    routes_out: List[Dict[str, Any]] = []
    for route in routes_raw or []:
        rid = route.get("RouteID")
        stops = route.get("Stops")
        if rid is None or not isinstance(stops, list) or not stops:
            continue
        rid_str = str(rid)
        desc = route.get("Description")
        info = (route.get("InfoText") or "").strip()
        base_name = desc.strip() if isinstance(desc, str) else f"Route {rid_str}"
        display_name = f"{base_name} — {info}" if info else base_name
        color_raw = route.get("MapLineColor") or route.get("Color") or route.get("RouteColor")
        color = color_raw.strip() if isinstance(color_raw, str) else None
        if color and not color.startswith("#"):
            color = f"#{color}"
        ordered_stops = sorted(
            stops,
            key=lambda s: (
                s.get("Order") if isinstance(s.get("Order"), (int, float)) else 0,
                s.get("RouteStopID") if s.get("RouteStopID") is not None else 0,
            ),
        )
        path_nodes: List[str] = []
        stop_refs: List[Dict[str, Any]] = []
        for stop in ordered_stops:
            address_id = _normalize_stop_id(stop.get("AddressID"))
            lat = _safe_float(stop.get("Latitude"))
            lon = _safe_float(stop.get("Longitude"))
            node_id: Optional[str] = None
            if address_id:
                node_id = node_lookup.get(f"addr:{address_id}")
            if node_id is None:
                if lat is None or lon is None:
                    continue
                node_id = node_lookup.get(_stop_key(lat, lon))
            if not node_id:
                continue
            if not path_nodes or path_nodes[-1] != node_id:
                path_nodes.append(node_id)
            ref: Dict[str, Any] = {"node": node_id}
            if address_id:
                ref["addressId"] = address_id
            rstop = stop.get("RouteStopID")
            if rstop is not None:
                ref["routeStopId"] = rstop
            order_val = stop.get("Order")
            if isinstance(order_val, (int, float)):
                ref["order"] = order_val
            stop_name = _stop_display_name(stop)
            if stop_name:
                ref["name"] = stop_name
            stop_refs.append(ref)
        if not path_nodes:
            continue
        route_entry: Dict[str, Any] = {
            "id": rid_str,
            "name": display_name,
            "color": color,
            "path": path_nodes,
            "stops": stop_refs,
        }
        for attr in ("IsVisibleOnMap", "IsCheckLineOnlyOnMap", "HideRouteLine", "IsRunning"):
            if attr in route and route[attr] is not None:
                key = attr[0].lower() + attr[1:]
                route_entry[key] = bool(route[attr])
        gtfs = route.get("GtfsId")
        if gtfs:
            route_entry["gtfsId"] = gtfs
        routes_out.append(route_entry)

    routes_out.sort(key=route_sort_key)

    links: List[Dict[str, Any]] = []
    link_map: Dict[tuple[str, str], str] = {}
    for route in routes_out:
        for a, b in zip(route["path"], route["path"][1:]):
            if a == b:
                continue
            key = tuple(sorted((a, b)))
            if key in link_map:
                continue
            link_id = f"L{len(link_map) + 1:03d}"
            link_map[key] = link_id
            links.append({"id": link_id, "a": a, "b": b})

    links.sort(key=lambda link: link["id"])

    schema: Dict[str, Any] = {
        "meta": {
            "version": 1,
            "updatedAt": _iso_now(),
            "source": "transloc",
            "sourceBase": TRANSLOC_BASE,
            "stopCount": len(nodes),
            "routeCount": len(routes_out),
        },
        "nodes": nodes,
        "links": links,
        "routes": routes_out,
    }
    return schema


async def read_schematic() -> Dict[str, Any]:
    async with SCHEMATIC_LOCK:
        return await asyncio.to_thread(_read_schema_sync)


async def write_schematic(schema: Dict[str, Any]) -> None:
    async with SCHEMATIC_LOCK:
        await asyncio.to_thread(_write_schema_sync, schema)

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
MILEAGE_NAME = "mileage.json"
MILEAGE_FILE = PRIMARY_DATA_DIR / MILEAGE_NAME

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
# Sync endpoint
# ---------------------------

@app.post("/sync")
def receive_sync(payload: dict):
    secret = payload.get("secret")
    if SYNC_SECRET is None or secret != SYNC_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    name = payload.get("name")
    data = payload.get("data")
    if (
        not isinstance(name, str)
        or name not in {DEVICE_STOP_NAME, CONFIG_NAME, MILEAGE_NAME}
        or not isinstance(data, str)
    ):
        raise HTTPException(status_code=400, detail="invalid payload")
    for base in DATA_DIRS:
        path = base / name
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(data)
        except Exception as e:
            print(f"[sync] error writing {path}: {e}")
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
                        state.routes_raw = routes_raw
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
                            vid = v.get("VehicleID")
                            tsms = parse_msajax(v.get("TimeStampUTC") or v.get("TimeStamp")) or int(time.time()*1000)
                            mps = (v.get("GroundSpeed") or 0.0) * MPH_TO_MPS
                            lat = v.get("Latitude")
                            lon = v.get("Longitude")
                            prev = prev_map.get(rid, {}).get(vid)
                            heading = v.get("Heading") or 0.0
                            if prev and prev.lat is not None and prev.lon is not None and lat is not None and lon is not None:
                                move = haversine((prev.lat, prev.lon), (lat, lon))
                                if move >= HEADING_JITTER_M:
                                    heading = bearing_between((prev.lat, prev.lon), (lat, lon))
                                else:
                                    heading = prev.heading
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
# UTS SCHEMATIC STORAGE API
# ---------------------------


@app.get("/api/schema")
async def get_schematic():
    try:
        return await read_schematic()
    except Exception as e:
        print(f"[schema] read error: {e}")
        raise HTTPException(status_code=500, detail="failed to load schematic")


@app.post("/api/schema")
async def save_schematic(payload: Dict[str, Any]):
    schema, errors = validate_schema_payload(payload)
    if schema is None or errors:
        return JSONResponse({"ok": False, "errors": errors or ["Schema must be an object"]}, status_code=400)
    meta = schema.setdefault("meta", {})
    meta["version"] = 1
    meta["updatedAt"] = _iso_now()
    try:
        await write_schematic(schema)
    except Exception as e:
        print(f"[schema] write error: {e}")
        raise HTTPException(status_code=500, detail="failed to save schematic")
    return {"ok": True, "updatedAt": meta["updatedAt"]}


@app.post("/api/import-stops")
async def import_stops_route():
    try:
        async with httpx.AsyncClient() as client:
            routes_raw = await fetch_routes_with_shapes(client)
    except httpx.HTTPError as e:
        print(f"[import-stops] http error: {e}")
        raise HTTPException(status_code=502, detail="failed to fetch stops from TransLoc")
    except Exception as e:
        print(f"[import-stops] unexpected error: {e}")
        raise HTTPException(status_code=500, detail="unexpected error fetching stops")
    if not isinstance(routes_raw, list) or not routes_raw:
        raise HTTPException(status_code=502, detail="no routes returned from TransLoc")
    try:
        schema = build_schematic_from_routes(routes_raw)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        print(f"[import-stops] build error: {e}")
        raise HTTPException(status_code=500, detail="failed to build schematic from stops")
    async with state.lock:
        state.routes_raw = routes_raw
    return schema

# ---------------------------
# Static assets
# ---------------------------

@app.get("/FGDC.ttf", include_in_schema=False)
async def fgdc_font():
    return FileResponse(BASE_DIR / "FGDC.ttf", media_type="font/ttf")

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

# ---------------------------
# TEST MAP PAGE
# ---------------------------
@app.get("/testmap")
async def testmap_page():
    return HTMLResponse(TESTMAP_HTML)

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
# UTS SCHEMATIC PAGE
# ---------------------------
@app.get("/uts-schematic")
async def uts_schematic_page():
    return HTMLResponse(UTS_SCHEMATIC_HTML)

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
# ARRIVALS DISPLAY REGISTRATION PAGE
# ---------------------------
@app.get("/registerdisplay")
async def registerdisplay_page():
    return HTMLResponse(REGISTERDISPLAY_HTML)


# ---------------------------
# DEVICE ↔ STOP MAPPING ENDPOINTS
# ---------------------------
@app.get("/device-stop")
async def get_device_stop(id: str):
    reg = DEVICE_STOPS.get(id)
    if not reg:
        raise HTTPException(status_code=404, detail="device not registered")
    return reg

@app.get("/device-stop/list")
async def list_device_stops():
    return {"devices": [{"id": k, **v} for k, v in DEVICE_STOPS.items()]}

@app.post("/device-stop")
async def set_device_stop(payload: Dict[str, str]):
    dev = payload.get("id")
    stop = payload.get("stopID")
    fname = payload.get("friendlyName", "")
    if not dev or not stop:
        raise HTTPException(status_code=400, detail="id and stopID required")
    DEVICE_STOPS[dev] = {"stopID": stop, "friendlyName": fname}
    save_device_stops()
    return {"ok": True}

@app.delete("/device-stop/{dev_id}")
async def delete_device_stop(dev_id: str):
    if dev_id not in DEVICE_STOPS:
        raise HTTPException(status_code=404, detail="device not registered")
    del DEVICE_STOPS[dev_id]
    save_device_stops()
    return {"ok": True}

# ---------------------------
# REPLAY PAGE
# ---------------------------
@app.get("/replay")
async def replay_page():
    return HTMLResponse(REPLAY_HTML)
