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
- Dispatcher-friendly fields: status (OK/Ease off/HOLD), headway_sec, gap_label (Hot/Cold/On target), countdown_sec, leader_name.
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
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import asyncio, time, math, os, json
import httpx

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse

# ---------------------------
# Config
# ---------------------------
TRANSLOC_BASE = os.getenv("TRANSLOC_BASE", "https://uva.transloc.com/Services/JSONPRelay.svc")
TRANSLOC_KEY  = os.getenv("TRANSLOC_KEY", "8882812681")
OVERPASS_EP   = os.getenv("OVERPASS_EP", "https://overpass-api.de/api/interpreter")

VEH_REFRESH_S   = int(os.getenv("VEH_REFRESH_S", "10"))
ROUTE_REFRESH_S = int(os.getenv("ROUTE_REFRESH_S", "60"))
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
MPH_TO_MPS      = 0.44704
DEFAULT_CAP_MPS = 25 * MPH_TO_MPS

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
    seg_caps_mps: List[float] = field(default_factory=list)

# ---------------------------
# HTTP clients
# ---------------------------
async def fetch_routes_with_shapes(client: httpx.AsyncClient):
    url = f"{TRANSLOC_BASE}/GetRoutesForMapWithScheduleWithEncodedLine?APIKey={TRANSLOC_KEY}"
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_vehicles(client: httpx.AsyncClient, include_unassigned: bool = True):
    # returnVehiclesNotAssignedToRoute=true returns vehicles even if not assigned to a route
    flag = "true" if include_unassigned else "false"
    url = f"{TRANSLOC_BASE}/GetMapVehiclePoints?APIKey={TRANSLOC_KEY}&returnVehiclesNotAssignedToRoute={flag}"
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("d", [])

async def fetch_overpass_speed_profile(route: Route, client: httpx.AsyncClient) -> List[float]:
    """Build per-segment speed caps (m/s). Minimal version: default cap only."""
    return [DEFAULT_CAP_MPS for _ in range(len(route.poly)-1)]

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
    """Direction-aware leader-finding with per-vehicle nearest-ahead selection.
    For each direction ring (+1 / -1), the leader of bus i is the bus j with the
    smallest strictly-positive along-track distance ds in that ring (no reliance
    on a global ordering that can wobble near wrap points). """
    vs = list(vehs_by_id.values())
    if not vs:
        return []

    L = max(1.0, route.length_m)

    # Partition by direction; unknowns latch onto the larger ring
    plus  = [v for v in vs if v.dir_sign > 0]
    minus = [v for v in vs if v.dir_sign < 0]
    zeros = [v for v in vs if v.dir_sign == 0]
    if len(plus) >= len(minus):
        plus.extend(zeros)
    else:
        minus.extend(zeros)

    # If a ring ends up with one or zero vehicles while another has more,
    # fall back to a single combined ring so that every bus gets a headway.
    # This avoids dispatcher entries with missing headway/gap when direction
    # detection leaves one vehicle isolated.
    if (len(plus) <= 1 or len(minus) <= 1) and len(vs) > 1:
        plus = vs
        minus = []

    def build_ring(group: List[Vehicle], forward: bool) -> List[VehicleView]:
        if not group:
            return []
        # Target headway for THIS ring only
        th = target_headway_sec(route, len(group))
        out: List[VehicleView] = []
        EPS = 1.0  # meters: ignore near-coincident positions when choosing a leader

        # Precompute segment caps lookup
        def ref_speed_for(v: Vehicle) -> float:
            seg_idx = find_seg_index_at_s(route.cum, v.s_pos)
            cap = route.seg_caps_mps[seg_idx] if route.seg_caps_mps else DEFAULT_CAP_MPS
            return max(MIN_SPEED_FLOOR, min(MAX_SPEED_CEIL, W_LIMIT * cap + (1 - W_LIMIT) * v.ema_mps))

        for me in group:
            if len(group) == 1:
                out.append(VehicleView(
                    id=me.id,
                    name=me.name,
                    status="green",
                    headway_sec=None,
                    target_headway_sec=int(th) if th > 0 else None,
                    gap_label="—",
                    leader_name=None,
                    countdown_sec=None,
                    updated_at=int(me.ts_ms / 1000),
                ))
                continue

            # Find nearest strictly-ahead bus in this ring
            best_ds = None
            best_leader = None
            for other in group:
                if other is me:
                    continue
                ds = ((other.s_pos - me.s_pos) % L) if forward else ((me.s_pos - other.s_pos) % L)
                if ds <= EPS:
                    continue
                if best_ds is None or ds < best_ds:
                    best_ds = ds
                    best_leader = other

            # Fallback: if everyone filtered by EPS, pick the smallest ds even if 0.
            # This ensures a stationary leader at the same s_pos is still recognised.
            if best_leader is None:
                for other in group:
                    if other is me:
                        continue
                    ds = ((other.s_pos - me.s_pos) % L) if forward else ((me.s_pos - other.s_pos) % L)
                    if best_ds is None or ds < best_ds:
                        best_ds = ds
                        best_leader = other

            # With leader chosen, compute time headway
            ds = max(best_ds or 0.5, 0.5)
            ref_speed = ref_speed_for(me)
            headway = ds / max(ref_speed, 0.1)
            diff = headway - th

            if th > 0 and headway < RED_FRAC * th:
                status = "red"; gap = f"Hot {abs(int(diff))}s"; countdown = int(max(0, th - headway))
            elif th > 0 and headway < GREEN_FRAC * th:
                status = "yellow"; gap = f"Hot {abs(int(diff))}s"; countdown = int(max(0, th - headway))
            else:
                status = "green"; gap = "On target" if abs(diff) <= ONTARGET_TOL_SEC else f"Cold {int(diff)}s"; countdown = None

            out.append(VehicleView(
                id=me.id,
                name=me.name,
                status=status,
                headway_sec=int(headway),
                target_headway_sec=int(th) if th > 0 else None,
                gap_label=gap,
                leader_name=(best_leader.name if best_leader else None),
                countdown_sec=(countdown if countdown is not None else None),
                updated_at=int(me.ts_ms / 1000),
            ))
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

class State:
    def __init__(self):
        self.routes: Dict[int, Route] = {}
        self.vehicles_by_route: Dict[int, Dict[int, Vehicle]] = {}
        self.headway_ema: Dict[int, float] = {}
        self.lock = asyncio.Lock()
        self.last_overpass_note: str = ""
        # Added: error surfacing & active route tracking
        self.last_error: str = ""
        self.last_error_ts: float = 0.0
        self.active_route_ids: set[int] = set()
        self.route_last_seen: dict[int, float] = {}

state = State()

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
    async def updater():
        await asyncio.sleep(0.1)
        async with httpx.AsyncClient() as client:
            while True:
                start = time.time()
                try:
                    routes_raw = await fetch_routes_with_shapes(client)
                    vehicles_raw = await fetch_vehicles(client, include_unassigned=True)
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
                        for v in vehicles_raw:
                            tms = parse_msajax(v.get("TimeStamp"))
                            age = v.get("Seconds") if v.get("Seconds") is not None else (max(0, (time.time()*1000 - tms)/1000) if tms else 9999)
                            if age <= STALE_FIX_S and v.get("RouteID") and v.get("RouteID") != 0:
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
                                route = Route(id=rid, name=name, encoded=enc, poly=poly, cum=cum, length_m=length)
                                # fetch speed profile (fetch-once policy)
                                route.seg_caps_mps = await fetch_overpass_speed_profile(route, client)
                                state.routes[rid] = route

                        # Vehicles per route: preserve previous snapshot, then apply fresh updates
                        prev_map = getattr(state, 'vehicles_by_route', {})
                        new_map: Dict[int, Dict[int, Vehicle]] = {rid: dict(prev_map.get(rid, {})) for rid in keep_ids}
                        for v in fresh:
                            rid = v["RouteID"]
                            if rid not in keep_ids or rid not in state.routes:
                                continue
                            name = str(v.get("Name") or "-")
                            vid = v.get("VehicleID")
                            tsms = parse_msajax(v.get("TimeStamp")) or int(time.time()*1000)
                            mps = (v.get("GroundSpeed") or 0.0) * MPH_TO_MPS
                            veh = Vehicle(id=vid, name=name, lat=v.get("Latitude"), lon=v.get("Longitude"), ts_ms=tsms,
                                          ground_mps=mps, age_s=v.get("Seconds") or 0.0)
                            s_pos, _ = project_vehicle_to_route(veh, state.routes[rid])
                            prev = new_map[rid].get(vid)
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
                            else: dir_sign = (prev.dir_sign if prev else 0)
                            disp = abs(along_mps)
                            measured = 0.5 * mps + 0.5 * disp if mps > 0 else (disp if prev else mps)
                            ema = EMA_ALPHA * measured + (1 - EMA_ALPHA) * ema
                            ema = max(MIN_SPEED_FLOOR, min(MAX_SPEED_CEIL, ema))
                            veh.s_pos = s_pos; veh.ema_mps = ema; veh.dir_sign = dir_sign
                            new_map[rid][vid] = veh
                        state.vehicles_by_route = new_map
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
# DRIVER PAGE (built-in)
# ---------------------------
DRIVER_SNIPPET = """
<!doctype html>
<meta charset="utf-8" />
<title>Driver Anti-Bunching</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root{ --bg:#0b0e11; --panel:#0f141a; --ink:#e8eef5; --muted:#9fb0c9; --line:#1f2630; --chip:#2a3442; --ok:#24c28a; --warn:#ffbf47; --bad:#ff6b6b; }
  html,body{height:100%}
  body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.45 system-ui,Segoe UI,Roboto,Arial;display:flex;align-items:center;justify-content:center}
  .card{width:min(560px,92vw);background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px 16px;box-shadow:0 10px 30px rgba(0,0,0,.35)}
  h1{margin:0 0 12px 0;font-size:18px}
  label{display:block;margin:8px 0 6px 2px;color:var(--muted)}
  select{width:100%;background:#10151c;color:var(--ink);border:1px solid var(--chip);border-radius:10px;padding:10px}
  .row{display:flex;gap:10px}
  .row>div{flex:1}
  .btn{width:100%;margin-top:12px;background:#15202b;border:1px solid var(--chip);color:var(--ink);border-radius:10px;padding:10px;cursor:pointer;font-weight:600}
  .btn:hover{filter:brightness(1.1)}
  .btn.danger{background:#2b1515;border-color:#523737}
  .btn.danger:hover{filter:brightness(1.05)}
  .out{margin-top:14px;border-top:1px solid var(--line);padding-top:12px}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
  .pill{display:inline-flex;align-items:center;gap:8px;border-radius:999px;padding:6px 10px;border:1px solid var(--chip);background:#10151c}
  .dot{width:10px;height:10px;border-radius:999px;background:#718096;box-shadow:0 0 0 1px rgba(0,0,0,.35) inset}
  .green .dot{background:var(--ok)} .yellow .dot{background:var(--warn)} .red .dot{background:var(--bad)}
  .green{color:#b9f4db} .yellow{color:#ffe29a} .red{color:#ffb0b0}
  .big{font-size:22px}
  .muted{color:var(--muted)}
</style>
<div class="card">
  <h1>Driver Anti-Bunching</h1>
  <div id="warn" class="muted" style="display:none;margin:6px 2px 2px"></div>

  <!-- Setup (pickers) -->
  <div id="setup">
    <div class="row">
      <div>
        <label>Unit Number</label>
        <select id="bus"></select>
      </div>
      <div>
        <label>Route</label>
        <select id="route"></select>
      </div>
    </div>
    <button id="go" class="btn">Start Anti-Bunching</button>
    <div class="muted" style="margin-top:8px">Pick your bus and route, then press Start.</div>
  </div>

  <!-- Active session (instructions only) -->
  <div id="session" style="display:none">
    <div id="selection" class="muted" style="margin:0 0 8px 2px"></div>
    <div id="out" class="out">
      <div class="muted">Connecting…</div>
    </div>
    <button id="end" class="btn danger">End Anti-Bunching</button>
  </div>
</div>

<script>
const $ = s=>document.querySelector(s);
const fmt = s=>{ if(s==null||!isFinite(s)) return "—"; s=Math.round(s); const m=Math.floor(s/60), r=s%60; return `${String(m).padStart(2,'0')}:${String(r).padStart(2,'0')}`; };
const pill = st=>{ const m={green:["OK","green"],yellow:["Ease off","yellow"],red:["HOLD","red"]}; const [t,c]=m[st]||["OK","green"]; return `<span class="pill ${c} big"><span class="dot"></span><span>${t}</span></span>`; };
async function j(u){ const r=await fetch(u,{cache:"no-store"}); if(!r.ok) throw new Error(r.status+" "+r.statusText); return r.json(); }

let timer=null, currentBus="", currentRoute=0;

// Load pickers once
async function loadBuses(){
  let list=[];
  try{ const d=await j('/v1/vehicles?include_stale=1&include_unassigned=1'); list=(d.vehicles||[]).map(x=>x.name); }
  catch(e){ try{ const d=await j('/v1/roster/vehicles'); list=(d.vehicles||[]).map(x=>x.name); } catch(_) { list=[]; } }
  const uniq=[...new Set(list.filter(Boolean))].sort((a,b)=>String(a).localeCompare(String(b)));
  $('#bus').innerHTML=uniq.map(n=>`<option>${n}</option>`).join('');
}
async function loadRoutes(){
  let list=[]; const d=await j('/v1/routes_all'); list=(d.routes||[]);
  list.sort((a,b)=>String(a.name||"").localeCompare(String(b.name||"")));
  $('#route').innerHTML=list.map(r=>`<option value="${r.id}">${r.name}${r.active?'':' (inactive)'}</option>`).join('');
}

function showSession(busName, routeId, routeLabel){
  $('#setup').style.display='none';
  $('#session').style.display='block';
  $('#selection').textContent = `Unit ${busName} • Route: ${routeLabel}`;
}
function showSetup(){
  $('#session').style.display='none';
  $('#setup').style.display='block';
}

async function tick(){
  try{
    const data=await j(`/v1/routes/${currentRoute}/vehicles/${encodeURIComponent(currentBus)}/instruction`);
    const cls=(data.order==='HOLD'?'red':(data.order==='Ease off'?'yellow':'green'));
    $('#out').innerHTML = `${pill(cls)}<div class=\"mono\" style=\"margin-top:10px\">Headway: ${data.headway||'—'} • Target: ${data.target||'—'}<br>Gap: ${data.gap||'—'} • Countdown: ${data.countdown||'—'}<br><span class=\"muted\">Leader: ${data.leader||'—'} • Updated: ${new Date((data.updated_at||Date.now()/1000)*1000).toLocaleTimeString()}</span></div>`;
  } catch(e){
    $('#out').innerHTML = `<div class=\"red mono\">Waiting for route to become active...</div><div class=\"muted\">Page will update when route becomes active. Ensure selected route and unit are correct.</div>`;
  }
}

// Start button
$('#go').onclick = ()=>{
  currentBus = $('#bus').value;
  currentRoute = Number($('#route').value);
  const routeLabel = $('#route').selectedOptions[0]?.text || `Route ${currentRoute}`;
  showSession(currentBus, currentRoute, routeLabel);
  if(timer) clearInterval(timer);
  tick();
  timer = setInterval(tick, 5000);
};

// End button
$('#end').onclick = ()=>{
  if(timer) { clearInterval(timer); timer=null; }
  currentBus = ""; currentRoute = 0;
  $('#out').innerHTML = '<div class=\"muted\">Pick your bus and route, then press Start.</div>';
  showSetup();
};

// Initial loads
document.addEventListener('DOMContentLoaded', async ()=>{ try{ await loadBuses(); await loadRoutes(); }catch(e){ $('#out').textContent='Error loading lists'; } });

// Health banner
async function pollHealth(){
  try{ const h=await j('/v1/health'); const w=$('#warn'); if(!h.ok && h.last_error){ w.style.display='block'; w.textContent='Feed error: '+h.last_error; } else { w.style.display='none'; w.textContent=''; } }
  catch(e){ const w=$('#warn'); w.style.display='block'; w.textContent='Health check failed'; }
  setTimeout(pollHealth, 15000);
}
pollHealth();
</script>
"""

@app.get("/driver")
async def driver_page():
    return HTMLResponse(DRIVER_SNIPPET)

# ---------------------------
# DISPATCHER PAGE (built-in)
# ---------------------------
DISPATCHER_SNIPPET = """
<!doctype html><meta charset=\"utf-8\"><title>UTS Anti-Bunching — Dispatcher</title>
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<style>
 body{font:15px system-ui;margin:0;background:#0b0e11;color:#e8eef5}
 header{display:flex;gap:10px;align-items:center;padding:12px 14px;border-bottom:1px solid #1f2630}
 select{background:#10151c;color:#e8eef5;border:1px solid #2a3442;border-radius:10px;padding:8px 10px}
 .chip{display:inline-block;border:1px solid #2a3442;border-radius:999px;padding:2px 8px;margin-left:8px;color:#cfe1ff}
 table{width:100%;border-collapse:collapse;margin-top:8px}
 th,td{border-bottom:1px solid #1f2630;padding:10px;text-align:left}
 .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
 .pill{display:inline-flex;gap:8px;align-items:center;border:1px solid #2a3442;border-radius:999px;padding:4px 8px;background:#10151c}
 .dot{width:10px;height:10px;border-radius:50%}
 .ok{color:#b6f0cb}.ok .dot{background:#24c28a}
 .warn{color:#ffe29a}.warn .dot{background:#ffbf47}
 .bad{color:#ffb0b0}.bad .dot{background:#ff6b6b}
 .hint{color:#9fb0c9}
 .banner{padding:8px 12px;display:none}
 .banner.error{background:#3b1f1f;color:#ffbfbf;border-bottom:1px solid #5a2b2b}
 .banner.info{background:#121922;color:#cfe1ff;border-bottom:1px solid #2a3442}
</style>
<header>
  <h1 style=\"font-size:16px;margin:0\">UTS Anti-Bunching — Dispatcher</h1>
  <label>Route: <select id=\"route\"></select></label>
  <span id=\"target\" class=\"chip\">Target —</span>
  <span id=\"upd\" class=\"chip\">Updated —</span>
</header>
<div id=\"banner\" class=\"banner info\"></div>
<main style=\"padding:0 12px 16px\">
  <table>
    <thead><tr>
      <th>Vehicle</th><th>Order</th><th>Headway</th><th>Gap vs Target</th><th>Leader</th><th>Countdown</th>
    </tr></thead>
    <tbody id=\"rows\"><tr><td class=\"hint\" colspan=\"6\">Loading…</td></tr></tbody>
  </table>
</main>
<script>
const $=s=>document.querySelector(s);
const fmt=s=>{if(s==null||!isFinite(s))return \"—\";s=Math.round(s);return String(Math.floor(s/60)).padStart(2,'0')+\":\"+String(s%60).padStart(2,'0')};
const pill=st=>{const m={green:[\"OK\",\"ok\"],yellow:[\"Slow Down\",\"warn\"],red:[\"HOLD\",\"bad\"]};const [t,c]=m[st]||[\"OK\",\"ok\"];return '<span class=\"pill '+c+'\"><span class=\"dot\"></span><span>'+t+'</span></span>'};
async function j(u){const r=await fetch(u,{cache:\"no-store\"});if(!r.ok)throw new Error(r.status);return r.json()}
function setBanner(txt,kind){ const b=$('#banner'); if(!txt){ b.style.display='none'; b.textContent=''; return; } b.className='banner '+(kind||'info'); b.textContent=txt; b.style.display='block'; }

let activeES=null, activeIV=null, currentRid=null, sessionId=0, userLocked=false;

async function loadRoutes(){
  const d=await j(\"/v1/routes\"); const list=(d.routes||[]);
  const sel=$('#route');
  const prev=currentRid;
  sel.innerHTML=list.map(r=>'<option value=\"'+r.id+'\">'+(r.name||(\"Route \"+r.id))+'</option>').join(\"\");
  if(prev){ sel.value=String(prev); }
  if(!sel.value && list.length){ sel.value=String(list[0].id); }
  if(!currentRid && sel.value) start(sel.value);
}

function render(rows){
  const t=rows.find(x=>x.target_headway_sec!=null)?.target_headway_sec; $('#target').textContent=\"Target \"+(t!=null?fmt(t):\"—\");
  const ts=rows[0]?.updated_at ? new Date(rows[0].updated_at * 1000) : new Date();
  $('#upd').textContent = "Updated " + ts.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', second:'2-digit'});
  const onlyBus = rows.length===1 && rows.every(x=>x.headway_sec==null || x.headway_sec===undefined);

  if(!rows.length){ $('#rows').innerHTML='<tr><td class=\"hint\" colspan=\"6\">No vehicles.</td></tr>'; return; }
  
  $('#rows').innerHTML=rows.map(v=>'<tr>'
    +'<td class=\"mono\">'+(v.name||\"—\")+'</td>'
    +'<td>'+ (onlyBus ? '<span class=\"pill ok\"><span class=\"dot\"></span><span>Only Bus</span></span>' : pill(v.status)) +'</td>'
    +'<td class=\"mono\">'+(v.headway_sec!=null?fmt(v.headway_sec):\"—\")+'</td>'
    +'<td class=\"mono\">'+(v.gap_label||\"—\")+'</td>'
    +'<td class=\"mono\">'+(v.leader_name||\"—\")+'</td>'
    +'<td class=\"mono\">'+(v.status!==\"green\"&&v.countdown_sec!=null?fmt(v.countdown_sec):\"—\")+'</td>'
    +'</tr>').join(\"\");
  setBanner(onlyBus ? 'Only 1 vehicle on route — headway control inactive.' : '', 'info');
}

function start(rid){
  rid=String(rid);
  // stop any previous channels
  if(activeES){ try{ activeES.close(); }catch(_){}; activeES=null; }
  if(activeIV){ clearInterval(activeIV); activeIV=null; }
  currentRid=rid;
  const sid=++sessionId;

  // Try SSE first
  try{
    const es=new EventSource(\"/v1/stream/routes/\"+rid);
    activeES=es;
    es.onmessage=ev=>{ if(rid!==currentRid || sid!==sessionId) return; try{ render(JSON.parse(ev.data||\"[]\")); }catch(_){} };
    es.onerror=_=>{ if(activeES===es){ es.close(); if(sid!==sessionId) return; activeES=null; startPoll(); } };
  }catch(_){ startPoll(); }

  function startPoll(){
    async function tick(){ if(rid!==currentRid || sid!==sessionId) return; try{ render(await j('/v1/routes/'+rid+'/status')); }catch(_){} }
    tick();
    activeIV=setInterval(tick, 10000);
  }
}

async function pollHealth(){
  try{ const h=await j('/v1/health'); if(!h.ok && h.last_error){ setBanner('Feed error: '+h.last_error, 'error'); }
       else{ /* keep info banner state */ } }
  catch(e){ setBanner('Health check failed', 'error'); }
  setTimeout(pollHealth, 15000);
}

document.addEventListener('DOMContentLoaded', ()=>{ loadRoutes(); pollHealth(); });
document.getElementById('route').addEventListener('change', e=> { userLocked=true; start(e.target.value); });
</script>
"""
@app.get("/dispatcher")
async def dispatcher_page():
    return HTMLResponse(DISPATCHER_SNIPPET)
