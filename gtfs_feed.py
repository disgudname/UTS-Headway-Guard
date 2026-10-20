"""Our own GTFS static feed for UTS (/gtfs.zip), built from the schedule we actually run.

TransLoc's own GTFS export (Admin > Reporting > GTFS File) is generated from its dispatch setup and is not usable
(checked 2026-10-19): no timetable, only one "every N seconds" entry per BLOCK, which add up to a bus every ~2 min on
Gold and Orange (really ~6); no holidays or breaks; game-day shuttles every weekend; Charter/Training/test routes in.

Where each part comes from:
  * routes, stops, stop order, shapes: TransLoc's live route list (GetRoutesForMapWithScheduleWithEncodedLine,
    app.py's state.routes_raw). A GTFS stop is a physical stop (TransLoc's AddressID), shared by every route.
  * times: the block packages (config/uts_blocks.json, see build_uts_blocks.py). Each block's day is a list of
    timestop visits; every pair of consecutive visits becomes one GTFS trip, along the route's stops between them.
    Timestops get the sheet's time (timepoint=1). Stops in between are spaced by TransLoc's planned stop-to-stop
    seconds (SecondsToNextStop / SecondsAtStop), squeezed to fit when the sheet allows less time than TransLoc
    plans; time left over is the hold at the next timestop. The "HOW TO GO OUT-OF-SERVICE" note adds the last trip
    (to the "as far as" stop) and nothing after it.
  * which route a visit pair runs on: the block's route family (config/uts_route_ids.json) narrowed to the routes
    that have both stops; where both the pre-6PM and post-6PM route do, the block's "EVENING ROUTE CHANGE" time
    decides (config/evening_route_stops.json says which route is which).
  * Purple: no timetable (anti-bunching), so frequencies.txt with exact_times=0 in the bands of
    config/purple_headways.json (scripts/build_purple_headways.py).
  * days: calendar_dates.txt only, one row per service and date, from the service level of each day
    (service_levels.py names; "none" runs nothing, "recess" runs the recess sheets for routes that have them, every
    other day the regular sheets -- the same rule as uts_blocks._groups_for_day; "health" runs Purple only). The
    feed ends on the last day the calendar knows (Inputs.known_until), so it never guesses at a break.

build_feed() is pure (no I/O, no clock): app.py hands it the inputs and caches the zip.
"""

from __future__ import annotations

import csv
import io
import math
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

NONE_LEVEL = "none"
RECESS_LEVEL = "recess"
# "Health Commuter" on the UTS calendar (e.g. Wed 2026-11-25, the day before Thanksgiving): only Purple, the health
# system commuter line, runs.
HEALTH_LEVEL = "health"

# Seconds in a day, and the earliest a route-change note can act as the change point when a block has no "EVENING
# ROUTE CHANGE" note (Orange [06] leaves MP at 18:00 on the post-1800 route; see uts_blocks.route_change_plan).
DAY_S = 86400
OOS_AS_CHANGE_MIN_S = int(17.5 * 3600)
OOS_AS_CHANGE_MAX_S = int(18.5 * 3600)

# A stop further than this from its route's line is reported; it still gets the nearest point.
STOP_SHAPE_WARN_M = 75.0
# When the line passes a stop more than once (both sides of Massie Rd), the first pass after the previous stop that
# is within this much of the closest pass is the stop's.
SHAPE_PASS_SLACK_M = 20.0

_DAY_LETTERS = "MTWRFSU"  # Monday..Sunday, as in uts_blocks weekday numbers 0..6


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def gtfs_time(seconds: float) -> str:
    s = int(round(seconds))
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def _haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371000.0 * math.asin(min(1.0, math.sqrt(h)))


def decode_polyline(encoded: str) -> List[Tuple[float, float]]:
    """Google encoded polyline (precision 5), as TransLoc's EncodedPolyline is."""
    points: List[Tuple[float, float]] = []
    index = lat = lon = 0
    while index < len(encoded):
        for which in (0, 1):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if which == 0:
                lat += delta
            else:
                lon += delta
        points.append((lat / 1e5, lon / 1e5))
    return points


def _text_color(hex_color: str) -> str:
    """Black or white, whichever reads better on the route color."""
    try:
        r, g, b = (int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
    except (ValueError, IndexError):
        return "000000"
    return "000000" if (0.299 * r + 0.587 * g + 0.114 * b) > 150 else "FFFFFF"


def _days_code(weekdays: Iterable[int]) -> str:
    return "".join(_DAY_LETTERS[d] for d in sorted(set(weekdays)))


# ---------------------------------------------------------------------------
# Route geometry
# ---------------------------------------------------------------------------

@dataclass
class RouteStop:
    route_stop_id: str
    stop_id: str  # physical stop (AddressID)
    name: str
    lat: float
    lon: float
    to_next_s: float
    dwell_s: float


@dataclass
class Route:
    transloc_id: str
    description: str
    color: str
    stops: List[RouteStop]
    line: List[Tuple[float, float]]
    _line_dist: List[float] = field(default_factory=list)
    _stop_pos: Optional[List[float]] = None  # each stop's distance along the line, unrolled from stop 0

    @property
    def length_m(self) -> float:
        return self._line_dist[-1] if self._line_dist else 0.0

    def index_of(self, route_stop_id: str) -> Optional[int]:
        for i, s in enumerate(self.stops):
            if s.route_stop_id == str(route_stop_id):
                return i
        return None


def parse_routes(routes_raw: Sequence[Dict[str, Any]]) -> Dict[str, Route]:
    out: Dict[str, Route] = {}
    for r in routes_raw or []:
        if not isinstance(r, dict) or r.get("RouteID") is None:
            continue
        stops = []
        for s in sorted(r.get("Stops") or [], key=lambda s: s.get("Order") or 0):
            rsid = s.get("RouteStopID") if s.get("RouteStopID") is not None else s.get("RouteStopId")
            addr = s.get("AddressID") if s.get("AddressID") is not None else s.get("AddressId")
            if rsid is None or addr is None or s.get("Latitude") is None or s.get("Longitude") is None:
                continue
            stops.append(RouteStop(
                route_stop_id=str(rsid), stop_id=str(addr),
                name=str(s.get("Name") or s.get("Description") or s.get("SignVerbiage") or "").strip(),
                lat=float(s["Latitude"]), lon=float(s["Longitude"]),
                to_next_s=float(s.get("SecondsToNextStop") or 0), dwell_s=float(s.get("SecondsAtStop") or 0),
            ))
        if not stops:
            continue
        line = decode_polyline(r.get("EncodedPolyline") or "")
        route = Route(
            transloc_id=str(r["RouteID"]), description=str(r.get("Description") or "").strip(),
            color=str(r.get("MapLineColor") or "").lstrip("#").upper()[:6], stops=stops, line=line,
        )
        dist = [0.0]
        for a, b in zip(line, line[1:]):
            dist.append(dist[-1] + _haversine_m(a, b))
        route._line_dist = dist
        out[route.transloc_id] = route
    return out


def _project(p: Tuple[float, float], a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
    """(fraction along a->b, distance in m) of p's closest point on segment a->b (flat-earth, fine at this scale)."""
    k = math.cos(math.radians(p[0]))
    ax, ay, bx, by, px, py = a[1] * k, a[0], b[1] * k, b[0], p[1] * k, p[0]
    dx, dy = bx - ax, by - ay
    seg2 = dx * dx + dy * dy
    t = 0.0 if seg2 == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / seg2))
    q = (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
    return t, _haversine_m(p, q)


def stop_positions(route: Route, warnings: List[str]) -> Optional[List[float]]:
    """Each stop's distance along the route line, increasing in stop order (unrolled past the line's end, since the
    line is a loop that need not start at stop 0). None if the route has no usable line."""
    if route._stop_pos is not None:
        return route._stop_pos
    line, dist = route.line, route._line_dist
    if len(line) < 2 or route.length_m <= 0:
        return None
    total = route.length_m
    n_seg = len(line) - 1

    def candidates(lo: float, hi: float, stop: RouteStop) -> List[Tuple[float, float]]:
        """(along, distance) of the closest point on every segment whose unrolled span overlaps [lo, hi]."""
        out = []
        lap = math.floor(lo / total)
        while lap * total <= hi:
            for j in range(n_seg):
                start, end = lap * total + dist[j], lap * total + dist[j + 1]
                if end < lo or start > hi:
                    continue
                t, d = _project((stop.lat, stop.lon), line[j], line[j + 1])
                along = start + t * (end - start)
                if lo <= along <= hi:
                    out.append((along, d))
            lap += 1
        return out

    positions: List[float] = []
    for i, stop in enumerate(route.stops):
        if i == 0:
            cands = candidates(0.0, total, stop)
        else:
            cands = candidates(positions[-1], positions[0] + total, stop)
        if not cands:
            positions.append(positions[-1] if positions else 0.0)
            continue
        best = min(d for _, d in cands)
        if best > STOP_SHAPE_WARN_M:
            warnings.append(f"route {route.transloc_id}: {stop.name} is {best:.0f} m from its line")
        if i == 0:
            along = min(cands, key=lambda c: c[1])[0]
        else:
            along = min(a for a, d in cands if d <= best + SHAPE_PASS_SLACK_M)
        positions.append(along)
    route._stop_pos = positions
    return positions


def _line_point(route: Route, along: float) -> Tuple[float, float]:
    total = route.length_m
    x = along % total if total else 0.0
    dist = route._line_dist
    lo, hi = 0, len(dist) - 1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if dist[mid] <= x:
            lo = mid
        else:
            hi = mid
    seg = dist[hi] - dist[lo]
    t = 0.0 if seg <= 0 else (x - dist[lo]) / seg
    a, b = route.line[lo], route.line[hi]
    return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)


def _line_slice(route: Route, start: float, end: float) -> List[Tuple[float, float, float]]:
    """Points of the line from unrolled distance start to end: [(lat, lon, distance from start)]."""
    total = route.length_m
    pts = [(*_line_point(route, start), 0.0)]
    lap = math.floor(start / total)
    while lap * total <= end:
        for j, d in enumerate(route._line_dist):
            along = lap * total + d
            if start < along < end:
                pts.append((route.line[j][0], route.line[j][1], along - start))
        lap += 1
    pts.append((*_line_point(route, end), end - start))
    # TransLoc's lines repeat points; GTFS validators flag two shape points at one spot
    out = [pts[0]]
    for p in pts[1:]:
        if (round(p[0], 6), round(p[1], 6)) != (round(out[-1][0], 6), round(out[-1][1], 6)):
            out.append(p)
    if len(out) == 1:
        out.append(pts[-1])
    return out


# ---------------------------------------------------------------------------
# Feed assembly
# ---------------------------------------------------------------------------

@dataclass
class Inputs:
    routes_raw: Sequence[Dict[str, Any]]
    blocks: Dict[str, Dict[str, Any]]                 # config/uts_blocks.json "blocks"
    timestops: Dict[str, Dict[str, str]]             # code -> {transloc route id: route stop id}
    landmarks: Dict[str, Dict[str, str]]             # same shape; out-of-service cut-offs only
    evening_pairs: Dict[str, Dict[str, Any]]          # pre-6PM route id -> {"to": post-6PM route id, ...}
    purple_headways: Dict[str, Any]                   # config/purple_headways.json
    config: Dict[str, Any]                            # config/gtfs.json
    service_level: Callable[[date], Optional[str]]   # "full" / "exam" / "recess" / "summer" / "none" / None
    start: date
    days: int
    version: str = ""
    known_until: Optional[date] = None               # last date the service calendar covers; the feed stops there

    @property
    def end(self) -> date:
        last = self.start + timedelta(days=max(1, self.days) - 1)
        return min(last, self.known_until) if self.known_until and self.known_until >= self.start else last


class _Feed:
    def __init__(self, inp: Inputs):
        self.inp = inp
        self.warnings: List[str] = []
        self.routes = parse_routes(inp.routes_raw)
        self.route_of: Dict[str, str] = {}  # transloc id -> gtfs route id
        for r in inp.config.get("routes", []):
            for tid in r.get("transloc_ids", []):
                self.route_of[str(tid)] = r["route_id"]
        self.evening = {str(v.get("to")) for v in (inp.evening_pairs or {}).values() if v.get("to")}
        self.trips: List[Dict[str, Any]] = []
        self.trip_index: Dict[str, "IndexedTrip"] = {}
        self.stop_times: List[Dict[str, Any]] = []
        self.frequencies: List[Dict[str, Any]] = []
        self.shapes: Dict[Tuple[str, int, int], str] = {}
        self.shape_rows: List[Dict[str, Any]] = []
        self.used_stops: Dict[str, RouteStop] = {}
        self.used_routes: set = set()
        self.services: Dict[str, Tuple[str, Tuple[int, ...]]] = {}  # service_id -> (kind, weekdays)

    # -- route choice -------------------------------------------------------

    def _stop_for(self, route_id: str, code: str) -> Optional[str]:
        return (self.inp.timestops.get(code) or {}).get(route_id) or (self.inp.landmarks.get(code) or {}).get(route_id)

    def _route_for(self, block: Dict[str, Any], a: str, b: str, evening: Optional[bool]) -> Optional[str]:
        family = [str(r) for r in block.get("route_ids") or []]
        cands = [
            r for r in family
            if r in self.routes and r in self.route_of and self._stop_for(r, a) and self._stop_for(r, b)
        ]
        if len(cands) > 1 and evening is not None:
            narrowed = [r for r in cands if (r in self.evening) == evening]
            cands = narrowed or cands
        if len(cands) > 1:
            # Still ambiguous: the route TransLoc lists first wins, so the choice is at least stable.
            order = [str(r.get("RouteID")) for r in self.inp.routes_raw]
            cands.sort(key=lambda r: order.index(r) if r in order else len(order))
        return cands[0] if cands else None

    def _run_s(self, route: Route, i: int, j: int) -> float:
        idx = _path(i, j, len(route.stops))
        return sum(route.stops[k].to_next_s for k in idx[:-1]) + sum(route.stops[k].dwell_s for k in idx[1:-1])

    def _stitch(self, block: Dict[str, Any], a: str, b: str) -> Optional[List[Tuple[Route, int, int]]]:
        """For a route-change leg no single route serves: Green [01] leaves Hereford at 17:45 for the Chapel, but the
        post-6PM Green Loop never passes Hereford. The bus drives the route that has `a` until the first stop it shares
        with a route that has `b`, then that route on to `b`. Returns the two pieces [(route, i, j)], the shortest such
        drive, or None."""
        family = [str(r) for r in block.get("route_ids") or [] if str(r) in self.routes and str(r) in self.route_of]
        best = None
        for r1 in family:
            sa = self._stop_for(r1, a)
            for r2 in family:
                sb = self._stop_for(r2, b)
                if r1 == r2 or not sa or not sb:
                    continue
                route1, route2 = self.routes[r1], self.routes[r2]
                i, j = route1.index_of(sa), route2.index_of(sb)
                if i is None or j is None:
                    continue
                on_r2 = {s.stop_id: k for k, s in enumerate(route2.stops)}
                for x in _path(i, i, len(route1.stops))[1:-1]:
                    k = on_r2.get(route1.stops[x].stop_id)
                    if k is None:
                        continue
                    run = self._run_s(route1, i, x) + route1.stops[x].dwell_s + (self._run_s(route2, k, j) if k != j else 0)
                    if best is None or run < best[0]:
                        best = (run, [(route1, i, x)] + ([(route2, k, j)] if k != j else []))
                    break
        return best[1] if best else None

    # -- shapes -------------------------------------------------------------

    def _shape(self, route: Route, i: int, j: int) -> Tuple[Optional[str], Optional[List[float]]]:
        """shape_id for the stretch of route from stop index i to j (j <= i wraps round the loop), and each stop's
        shape_dist_traveled along it (km)."""
        pos = stop_positions(route, self.warnings)
        if pos is None:
            return None, None
        n = len(route.stops)
        idx = _path(i, j, n)
        total = route.length_m
        along = []
        for step, k in enumerate(idx):
            p = pos[k] + (total if k < i or (step > 0 and k == i) else 0.0)
            along.append(p)
        for step in range(1, len(along)):
            if along[step] < along[step - 1]:
                along[step] = along[step - 1]
        key = (route.transloc_id, i, j)
        if key not in self.shapes:
            sid = f"{self.route_of[route.transloc_id]}-{route.transloc_id}-{route.stops[i].stop_id}-{route.stops[j].stop_id}"
            if i == j:
                sid += "-loop"
            self.shapes[key] = sid
            for seq, (lat, lon, d) in enumerate(_line_slice(route, along[0], along[-1])):
                self.shape_rows.append({
                    "shape_id": sid, "shape_pt_lat": f"{lat:.6f}", "shape_pt_lon": f"{lon:.6f}",
                    "shape_pt_sequence": seq, "shape_dist_traveled": f"{d / 1000:.4f}",
                })
        return self.shapes[key], [(a - along[0]) / 1000 for a in along]

    # -- trips --------------------------------------------------------------

    def _service(self, kind: str, weekdays: Sequence[int]) -> str:
        sid = f"{kind}_{_days_code(weekdays)}"
        self.services[sid] = (kind, tuple(sorted(set(weekdays))))
        return sid

    def _add_trip(
        self, *, route: Route, i: int, j: int, depart_s: float, sched_s: Optional[float], service_id: str,
        trip_id: str, block_id: Optional[str], headsign: Optional[str], timepoint_end: bool,
        frequency: Optional[Tuple[int, int, int]] = None,
    ) -> None:
        n = len(route.stops)
        idx = _path(i, j, n)
        hops = [route.stops[idx[k]].to_next_s for k in range(len(idx) - 1)]
        dwells = [route.stops[k].dwell_s for k in idx[1:-1]]
        run = sum(hops) + sum(dwells)
        scale = 1.0
        if sched_s is not None and run > sched_s > 0:
            scale = sched_s / run
        shape_id, dists = self._shape(route, i, j)
        gtfs_route = self.route_of[route.transloc_id]
        trip = {
            "route_id": gtfs_route, "service_id": service_id, "trip_id": trip_id,
            "trip_headsign": headsign or "", "block_id": block_id or "", "shape_id": shape_id or "",
        }
        self.trips.append(trip)
        self.used_routes.add(gtfs_route)
        indexed = IndexedTrip(
            trip_id=trip_id, route_id=gtfs_route, transloc_route_id=route.transloc_id, service_id=service_id,
            block=(block_id or "").split("-", 1)[0] or None, frequency=frequency, stops=[],
        )
        self.trip_index[trip_id] = indexed
        t = depart_s
        for k, stop_idx in enumerate(idx):
            stop = route.stops[stop_idx]
            arrive = t
            if 0 < k < len(idx) - 1:
                depart = arrive + dwells[k - 1] * scale
            else:
                depart = arrive
            first, last = k == 0, k == len(idx) - 1
            row = {
                "trip_id": trip_id, "arrival_time": gtfs_time(arrive), "departure_time": gtfs_time(depart),
                "stop_id": stop.stop_id, "stop_sequence": k + 1,
                "timepoint": 1 if (first or (last and timepoint_end)) and frequency is None else 0,
                "shape_dist_traveled": f"{dists[k]:.4f}" if dists else "",
            }
            self.stop_times.append(row)
            self.used_stops.setdefault(stop.stop_id, stop)
            indexed.stops.append(IndexedStop(
                k + 1, stop.route_stop_id, stop.stop_id, int(round(arrive)), int(round(depart)),
            ))
            if k < len(hops):
                t = depart + hops[k] * scale
        if frequency:
            start, end, headway = frequency
            self.frequencies.append({
                "trip_id": trip_id, "start_time": gtfs_time(start), "end_time": gtfs_time(end),
                "headway_secs": headway, "exact_times": 0,
            })

    def _headsign(self, code: Optional[str], route: Route, idx: Optional[int]) -> Optional[str]:
        names = self.inp.config.get("headsigns") or {}
        if code and names.get(code):
            return names[code]
        if idx is not None:
            name = self._stop_name(route.stops[idx])
            return name.split("@", 1)[1].strip() if "@" in name else name
        return None

    def _stop_name(self, stop: RouteStop) -> str:
        return (self.inp.config.get("stop_names") or {}).get(stop.stop_id) or stop.name

    def add_block_group(self, block_id: str, block: Dict[str, Any], gi: int, group: Dict[str, Any]) -> None:
        kind = "recess" if group.get("service") == "recess" else ("full" if block.get("has_recess") else "daily")
        service_id = self._service(kind, group.get("weekdays") or [])
        label = block_id.strip("[]")
        entries: List[Tuple[int, str]] = []
        for t, code in group.get("stops") or []:
            if not entries or entries[-1] != (t, code):
                entries.append((int(t), str(code)))
        oos = group.get("out_of_service")
        rc = group.get("route_change")
        change_s = rc.get("leave_s") if rc else None
        if change_s is None and oos and OOS_AS_CHANGE_MIN_S <= oos.get("leave_s", 0) <= OOS_AS_CHANGE_MAX_S:
            change_s = oos["leave_s"]
        final: Optional[Tuple[int, str, str]] = None  # (leave_s, leave_code, cutoff_code)
        if oos:
            cutoff = oos.get("last_code") or oos.get("until_code")
            leave = (int(oos["leave_s"]), oos.get("leave_code"))
            if leave in entries:
                entries = entries[:entries.index(leave) + 1]
                if cutoff:
                    final = (leave[0], leave[1], cutoff)
            else:
                self.warnings.append(f"{block_id} {_days_code(group.get('weekdays') or [])}: out-of-service note "
                                     f"leaves {leave[1]} at {gtfs_time(leave[0])}, not in its timetable")
        legs: List[Tuple[int, str, str, Optional[int]]] = [
            (ta, a, b, tb - ta) for (ta, a), (tb, b) in zip(entries, entries[1:]) if tb > ta
        ]
        if final:
            legs.append((final[0], final[1], final[2], None))
        n = 0
        for depart_s, a, b, sched in legs:
            evening = None if change_s is None else depart_s >= change_s
            rid = self._route_for(block, a, b, evening)
            if rid is not None:
                route = self.routes[rid]
                i, j = route.index_of(self._stop_for(rid, a)), route.index_of(self._stop_for(rid, b))
                if i is None or j is None:
                    self.warnings.append(f"route {rid}: timestop {a if i is None else b} is not on its stop list")
                    continue
                pieces = [(route, i, j)]
            else:
                pieces = self._stitch(block, a, b) if evening is not None else None
                if not pieces:
                    continue  # a deadhead (pull-out from the yard) or a pair no route serves
            runs = [self._run_s(r, i, j) for r, i, j in pieces]
            joins = [r.stops[j].dwell_s for r, _, j in pieces[:-1]]
            total = sum(runs) + sum(joins)
            scale = sched / total if sched and total > sched else 1.0
            t = float(depart_s)
            final_route, _, final_j = pieces[-1]
            headsign = self._headsign(b, final_route, final_j)
            for k, (route, i, j) in enumerate(pieces):
                last = k == len(pieces) - 1
                piece_sched = (sched - (t - depart_s)) if last and sched else runs[k] * scale
                n += 1
                self._add_trip(
                    route=route, i=i, j=j, depart_s=t, sched_s=piece_sched, service_id=service_id,
                    trip_id=f"{label}-{service_id}-{gi}-{n:03d}", block_id=f"{label}-{service_id}",
                    headsign=headsign, timepoint_end=last and sched is not None,
                )
                if not last:
                    t += (runs[k] + joins[k]) * scale

    def add_purple(self) -> None:
        cfg = self.inp.config.get("purple") or {}
        bands = (self.inp.purple_headways or {}).get("bands") or []
        weekdays = (self.inp.purple_headways or {}).get("weekdays") or [0, 1, 2, 3, 4]
        if not cfg or not bands:
            return
        service_id = self._service("purple", weekdays)
        for n, band in enumerate(bands, 1):
            start, end, headway = int(band["start_s"]), int(band["end_s"]), int(band["headway_s"])
            pattern = next(
                (p for p in cfg.get("patterns", []) if int(p["from_s"]) <= start < int(p["to_s"])), None,
            )
            route = self.routes.get(str(pattern["transloc_id"])) if pattern else None
            if route is None or str(route.transloc_id) not in self.route_of:
                self.warnings.append(f"Purple band at {gtfs_time(start)} has no route to run on")
                continue
            self._add_trip(
                route=route, i=0, j=0, depart_s=start, sched_s=None, service_id=service_id,
                trip_id=f"purple-{n:02d}", block_id=None, headsign=None, timepoint_end=False,
                frequency=(start, end, headway),
            )

    # -- tables ---------------------------------------------------------------

    def calendar_dates(self) -> List[Dict[str, Any]]:
        rows = []
        for k in range((self.inp.end - self.inp.start).days + 1):
            d = self.inp.start + timedelta(days=k)
            try:
                level = self.inp.service_level(d)
            except Exception:
                level = None
            if level == NONE_LEVEL:
                continue
            recess = level == RECESS_LEVEL
            for sid, (kind, weekdays) in sorted(self.services.items()):
                if d.weekday() not in weekdays:
                    continue
                if kind == "recess" and not recess or kind == "full" and recess:
                    continue
                if level == HEALTH_LEVEL and kind != "purple":
                    continue
                rows.append({"service_id": sid, "date": d.strftime("%Y%m%d"), "exception_type": 1})
        return rows

    def tables(self) -> Dict[str, List[Dict[str, Any]]]:
        cfg = self.inp.config
        agency = dict(cfg.get("agency") or {})
        routes = []
        for i, r in enumerate(cfg.get("routes", [])):
            if r["route_id"] not in self.used_routes:
                continue
            color = next((self.routes[t].color for t in r.get("transloc_ids", []) if t in self.routes), "")
            routes.append({
                "route_id": r["route_id"], "agency_id": agency.get("agency_id", ""),
                "route_short_name": "", "route_long_name": r.get("name") or "", "route_type": 3,
                "route_color": color, "route_text_color": _text_color(color) if color else "",
                "route_sort_order": i,
            })
        stops = [
            {"stop_id": s.stop_id, "stop_name": self._stop_name(s), "stop_lat": f"{s.lat:.6f}",
             "stop_lon": f"{s.lon:.6f}", "location_type": 0}
            for s in sorted(self.used_stops.values(), key=lambda s: int(s.stop_id) if s.stop_id.isdigit() else 0)
        ]
        calendar_dates = self.calendar_dates()
        active = {r["service_id"] for r in calendar_dates}
        trips = [t for t in self.trips if t["service_id"] in active]
        kept = {t["trip_id"] for t in trips}
        end = self.inp.end
        info = dict(cfg.get("feed_info") or {})
        info.update({
            "feed_start_date": self.inp.start.strftime("%Y%m%d"), "feed_end_date": end.strftime("%Y%m%d"),
            "feed_version": self.inp.version or self.inp.start.isoformat(),
        })
        shapes_used = {t["shape_id"] for t in trips}
        return {
            "agency.txt": [agency],
            "routes.txt": routes,
            "stops.txt": stops,
            "trips.txt": trips,
            "stop_times.txt": [r for r in self.stop_times if r["trip_id"] in kept],
            "frequencies.txt": [r for r in self.frequencies if r["trip_id"] in kept],
            "calendar_dates.txt": calendar_dates,
            "shapes.txt": [r for r in self.shape_rows if r["shape_id"] in shapes_used],
            "fare_attributes.txt": [{
                "fare_id": "free", "price": "0.00", "currency_type": "USD", "payment_method": 0, "transfers": "",
                "agency_id": agency.get("agency_id", ""),
            }],
            "feed_info.txt": [info],
        }


@dataclass
class IndexedStop:
    sequence: int
    route_stop_id: str   # TransLoc RouteStopID, what the live ETA engine keys stops by
    stop_id: str         # GTFS stop_id (physical stop, AddressID)
    arrival_s: int       # seconds after the service day's start, as in stop_times.txt
    departure_s: int


@dataclass
class IndexedTrip:
    """One trip as the live feeds (gtfs_rt.py) need it: enough to match a bus to it and to name its stops."""
    trip_id: str
    route_id: str                        # GTFS route_id
    transloc_route_id: str
    service_id: str
    block: Optional[str]                 # block number as in uts_blocks.json without brackets ("09"); None for Purple
    frequency: Optional[Tuple[int, int, int]]  # (start_s, end_s, headway_s) for a frequency trip
    stops: List[IndexedStop]


@dataclass
class FeedIndex:
    trips: Dict[str, IndexedTrip]
    services_by_date: Dict[str, set]     # "YYYYMMDD" -> service_ids running that service day
    route_of: Dict[str, str]             # TransLoc route id -> GTFS route_id
    agency_id: str


def _path(i: int, j: int, n: int) -> List[int]:
    """Stop indices from i forward to j round a loop of n stops; i == j is one whole lap."""
    out = [i]
    k = i
    while True:
        k = (k + 1) % n
        out.append(k)
        if k == j:
            return out


def build_feed(inp: Inputs) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    """(tables by file name, warnings). Every table is a list of row dicts."""
    tables, warnings, _ = build_feed_with_index(inp)
    return tables, warnings


def build_feed_with_index(inp: Inputs) -> Tuple[Dict[str, List[Dict[str, Any]]], List[str], FeedIndex]:
    """build_feed plus the FeedIndex the live GTFS-RT feeds match buses against (same trip ids)."""
    feed = _Feed(inp)
    for block_id in sorted(inp.blocks):
        block = inp.blocks[block_id]
        for gi, group in enumerate(block.get("weekday_groups") or []):
            feed.add_block_group(block_id, block, gi, group)
    feed.add_purple()
    tables = feed.tables()
    for name in ("stops.txt", "trips.txt", "calendar_dates.txt"):
        if not tables[name]:
            feed.warnings.append(f"{name} is empty")
    services: Dict[str, set] = {}
    for r in tables["calendar_dates.txt"]:
        services.setdefault(r["date"], set()).add(r["service_id"])
    kept = {t["trip_id"] for t in tables["trips.txt"]}
    index = FeedIndex(
        trips={tid: t for tid, t in feed.trip_index.items() if tid in kept}, services_by_date=services,
        route_of=dict(feed.route_of), agency_id=str((inp.config.get("agency") or {}).get("agency_id") or ""),
    )
    return tables, sorted(set(feed.warnings)), index


# Column order per file (GTFS reference order); a column every row leaves blank is dropped.
_COLUMNS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone",
                   "agency_fare_url", "agency_email"],
    "routes.txt": ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "route_color",
                   "route_text_color", "route_sort_order"],
    "stops.txt": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type"],
    "trips.txt": ["route_id", "service_id", "trip_id", "trip_headsign", "block_id", "shape_id"],
    "stop_times.txt": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "timepoint",
                       "shape_dist_traveled"],
    "frequencies.txt": ["trip_id", "start_time", "end_time", "headway_secs", "exact_times"],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    "shapes.txt": ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"],
    "fare_attributes.txt": ["fare_id", "price", "currency_type", "payment_method", "transfers", "agency_id"],
    "feed_info.txt": ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date",
                      "feed_version", "feed_contact_email", "feed_contact_url"],
}
# Columns GTFS requires to be present even when empty ("transfers" empty = unlimited).
_KEEP_EMPTY = {("fare_attributes.txt", "transfers"), ("routes.txt", "route_short_name")}


def write_zip(tables: Dict[str, List[Dict[str, Any]]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, rows in tables.items():
            if not rows:
                continue  # an optional file with nothing in it is left out
            cols = [c for c in _COLUMNS.get(name, list(rows[0])) if any(str(r.get(c, "")) != "" for r in rows)
                    or (name, c) in _KEEP_EMPTY]
            text = io.StringIO()
            w = csv.writer(text, lineterminator="\n")
            w.writerow(cols)
            for r in rows:
                w.writerow([r.get(c, "") for c in cols])
            z.writestr(name, text.getvalue())
    return buf.getvalue()
