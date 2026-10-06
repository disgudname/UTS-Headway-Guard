"""The box score: one service day of UTS written up like a baseball box score (/boxscore).

build() turns a day's door-counter rows (TransLoc GetRidershipData), the day's bus -> block map (state.bus_days, the
same data /v1/servicecrew serves) and TransLoc's block schedule into a small dict: a line score per route (riders by
"inning", nine stretches of the day), a batting table per block, the stars of the day and the busiest stops.
BoxScoreStore keeps one JSON file per day under <data dir>/boxscores/ and works out the notes that need history
("busiest Tuesday since August").

A service day runs 04:00 to 04:00, so Night Pilot's after-midnight riders stay with the evening before; that is why
build() wants the rows and bus maps of two calendar days.

Placing a boarding on a block follows scripts/build_block_cards.py (same rules, kept in step by hand): the bus map
lists every block a bus TOUCHED, without times, so a bus listed on two blocks of one route has each boarding placed
on the blocks scheduled out at that time, minus any another bus is covering, then on the one its timestop times fit,
and is split evenly only if still tied.
"""
from __future__ import annotations

import bisect
import collections
import json
import re
import statistics
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

BLOCK_FAMILY = {
    **{b: "Green" for b in ("01", "02")},
    **{b: "Night Pilot" for b in ("03", "04")},
    **{b: "Orange" for b in ("05", "06", "07", "08")},
    **{b: "Gold" for b in ("09", "10", "11", "12")},
    **{b: "Silver" for b in ("13", "14")},
    **{b: "Purple" for b in ("17", "18", "19", "20", "21", "22", "23", "24", "25")},
}
FAMILY_COLOR = {"Green": "#0c8103", "Night Pilot": "#232d48", "Orange": "#ff7300", "Gold": "#ffdd00",
                "Silver": "#5f6367", "Purple": "#662c90"}
FAMILY_NAME = {"Green": "Green Line", "Night Pilot": "Night Pilot", "Orange": "Orange Line", "Gold": "Gold Line",
               "Silver": "Silver Line", "Purple": "Purple Line"}
OTHER = "Other"  # charters, orientation runs, anything that is neither one of the six routes nor a lot shuttle
# Bump when build()'s output changes shape, so stored days are rebuilt (app.py's _boxscore_loop looks at "v").
VERSION = 2
# Game days. The lot and fan shuttles ("Purple Lots Shuttle", "Post-Game Fan Shuttle"...) only run for a home football
# game or another big event (a stadium concert), so a day with this many riders on them is an event day whether or
# not anyone told us what it was. Event days are only ranked against each other.
EVENT_MIN_SHUTTLE_RIDERS = 500
SHUTTLE_COLOR = {"purple": "#8420d2", "blue": "#0072bc", "red": "#f60303", "post-game": "#6134aa"}
IGNORED_ROUTES = ("training", "test route")  # driver training and TransLoc's test route are not service
# Home football games already played when the game log started (the athletics feed only lists upcoming ones), and
# events that are not football ("name" instead of "opponent"). Dates checked against shuttle ridership. 08-29 is the
# NC State opener, moved to Scott Stadium from Brazil, and 04-04 a concert (user, 2026-10-06); the other opponents
# are from virginiasports.com's 2026 schedule.
KNOWN_HOME_GAMES = {
    "2026-04-04": {"name": "Luke Combs at Scott Stadium", "kickoff": None},
    "2026-08-29": {"opponent": "NC State", "kickoff": None},
    "2026-09-11": {"opponent": "Norfolk State", "kickoff": None},
    "2026-09-26": {"opponent": "Delaware", "kickoff": None},
}
SERVICE_DAY_START_H = 4
# Nine "innings": the hour each one starts at, counted from the service day's midnight (so 25 is 01:00 next morning).
INNING_STARTS = (4, 7, 9, 11, 13, 15, 17, 19, 21)
INNING_LABELS = ("Early", "7a", "9a", "11a", "1p", "3p", "5p", "7p", "Late")
COVER_S = 45 * 60
FIT_HOURS = range(7, 17)
FIT_BEST_MAX_S = 300
FIT_MARGIN_S = 240
FIT_SMOOTH_HOURS = 2
FIRST_DAY = date(2026, 3, 9)  # block numbers mean what they mean today from this day on


def family_of(route_name: Any) -> Optional[str]:
    text = str(route_name or "").lower()
    if "shuttle" in text:  # "Purple Lots Shuttle" is a game-day lot shuttle, not the Purple Line
        return None
    for fam in FAMILY_COLOR:
        if fam.lower() in text:
            return fam
    return None


def group_numbers(group_id: Any) -> List[str]:
    return [n.zfill(2) for n in re.findall(r"\[(\d+)\]", str(group_id or ""))]


def parse_time(text: str) -> datetime:
    return datetime.strptime(text, "%m/%d/%Y %I:%M:%S %p")


def _iso_s(text: Any) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(text or ""))
    return (int(m[1] or 0) * 3600 + int(m[2] or 0) * 60 + int(m[3] or 0)) if m else 0


def schedule_pieces(day: date, block_groups: Dict[date, List[Dict[str, Any]]]) -> Dict[str, List[Tuple[int, int]]]:
    """{block: [(start_s, end_s)]} for one service day, seconds from its midnight, from TransLoc block groups of that
    day and the next (whose 00:00-02:00 Night Pilot tail belongs to this one)."""
    out: Dict[str, List[Tuple[int, int]]] = collections.defaultdict(list)
    for offset in (0, 1):
        for g in block_groups.get(day + timedelta(days=offset)) or []:
            gid = g.get("BlockGroupId") or ""
            if "detour" in gid.lower():  # a second copy of the block's hours, for the days a detour is on
                continue
            numbers = group_numbers(gid)
            for blk in g.get("Blocks") or []:
                for trip in blk.get("Trips") or []:
                    fam = family_of(trip.get("RouteName"))
                    match = [n for n in numbers if BLOCK_FAMILY.get(n) == fam]
                    if len(match) != 1:
                        continue
                    start, end = _iso_s(trip.get("StartTime")), _iso_s(trip.get("EndTime"))
                    tail = end <= SERVICE_DAY_START_H * 3600
                    if tail != bool(offset):
                        continue
                    out[match[0]].append((start + 86400 * offset, end + 86400 * offset))
    # TransLoc lists a Night Pilot tail on mornings after a night the block did not run: a tail alone is not service.
    return {b: sorted(p) for b, p in out.items() if not all(s >= 86400 for s, _ in p)}


def timetables(day: date, packages: Dict[str, Any], recess: bool) -> Dict[str, Dict[str, List[int]]]:
    """{block: {timestop code: sorted scheduled seconds}} from the block packages (config/uts_blocks.json)."""
    out = {}
    for name, package in (packages or {}).items():
        for g in package.get("weekday_groups") or []:
            if (g.get("service") == "recess") == recess and day.weekday() in g.get("weekdays", []):
                table: Dict[str, List[int]] = collections.defaultdict(list)
                for sec, code in g.get("stops") or []:
                    table[code].append(sec)
                out[name.strip("[]")] = {c: sorted(v) for c, v in table.items()}
                break
    return out


def timestop_codes(timestops: Dict[str, Dict[str, str]]) -> Dict[Tuple[int, int], str]:
    return {(int(route), int(rsid)): code for code, by_route in (timestops or {}).items() for route, rsid in by_route.items()}


def block_by_hour(stop_events, candidates, tables, codes) -> Dict[int, str]:
    """{hour: block} for a bus listed on several blocks of one route, from when it is at timestops (weekday daytime
    route only; see scripts/build_block_cards.py for how well this scores)."""
    visits: Dict[int, List[Tuple[int, str]]] = collections.defaultdict(list)
    last: Dict[str, int] = {}
    for when, route_id, rsid in sorted(stop_events):
        code = codes.get((route_id, rsid))
        if not code:
            continue
        sec = when.hour * 3600 + when.minute * 60 + when.second
        if code not in last or sec - last[code] > 240:
            visits[when.hour].append((sec, code))
        last[code] = sec
    clear = {}
    for hour in FIT_HOURS:
        scores = {}
        for n in candidates:
            devs = []
            for sec, code in visits.get(hour, []):
                times = tables[n].get(code)
                if times:
                    k = bisect.bisect_left(times, sec)
                    devs.append(min(abs(times[x] - sec) for x in (k - 1, k) if 0 <= x < len(times)))
            if len(devs) >= 2:
                scores[n] = statistics.median(devs)
        ranked = sorted(scores.items(), key=lambda kv: kv[1])
        if len(ranked) >= 2 and ranked[0][1] <= FIT_BEST_MAX_S and ranked[1][1] - ranked[0][1] >= FIT_MARGIN_S:
            clear[hour] = ranked[0][0]
    if not clear:
        return {}
    out = {}
    for hour in range(24):
        near = collections.Counter(b for h, b in clear.items() if abs(h - hour) <= FIT_SMOOTH_HOURS)
        out[hour] = near.most_common(1)[0][0] if near else clear[min(clear, key=lambda h: abs(h - hour))]
    return out


def _inning(hour_of_service_day: int) -> int:
    return max(0, bisect.bisect_right(INNING_STARTS, hour_of_service_day) - 1)


def _clock(seconds: float) -> str:
    seconds = int(seconds) % 86400
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}"


def build(
    day: date,
    rows: Iterable[Dict[str, Any]],
    bus_days: Dict[date, Dict[str, Dict[str, Any]]],
    block_groups: Dict[date, List[Dict[str, Any]]],
    packages: Optional[Dict[str, Any]] = None,
    timestops: Optional[Dict[str, Dict[str, str]]] = None,
    level: Optional[str] = None,
    game: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """The box score of one service day. bus_days is {calendar day: {bus: {"blocks": [...], "miles": float}}} for the
    day and the next; rows are door-counter rows covering both. game is that day's home football game, if one is
    known: {"opponent": ..., "kickoff": "HH:MM" or None}, or another known event: {"name": ...}."""
    start = datetime(day.year, day.month, day.day, SERVICE_DAY_START_H)
    end = start + timedelta(days=1)
    pieces = schedule_pieces(day, block_groups)
    tables = timetables(day, packages or {}, recess=level in ("recess", "summer")) if day.weekday() < 5 else {}
    codes = timestop_codes(timestops or {})

    def listed(bus: str, on: date) -> Dict[str, List[str]]:
        by_family: Dict[str, set] = collections.defaultdict(set)
        for g in (bus_days.get(on, {}).get(bus) or {}).get("blocks") or []:
            for n in group_numbers(g):
                if n in BLOCK_FAMILY:
                    by_family[BLOCK_FAMILY[n]].add(n)
        return {fam: sorted(ns) for fam, ns in by_family.items()}

    events = []
    cover: Dict[str, List[datetime]] = collections.defaultdict(list)
    stop_events: Dict[Tuple[str, str], list] = collections.defaultdict(list)
    for row in rows:
        entries = row.get("Entries") or 0
        if not entries:
            continue
        try:
            when = parse_time(row["ClientTime"])
        except (KeyError, ValueError):
            continue
        if not start <= when < end:
            continue
        bus = str(row.get("Vehicle") or "")
        route_name = str(row.get("Route") or "").strip()
        if any(word in route_name.lower() for word in IGNORED_ROUTES):
            continue
        fam = family_of(route_name)
        numbers = listed(bus, when.date()).get(fam, []) if fam else []
        # A lot or fan shuttle gets a line of its own, under its TransLoc name.
        events.append((when, bus, fam or (route_name if "shuttle" in route_name.lower() else OTHER), numbers, entries,
                       row.get("RouteStop") or "?"))
        if len(numbers) == 1:
            cover[numbers[0]].append(when)
        elif len(numbers) > 1 and when.date() == day and all(n in tables for n in numbers):
            stop_events[(bus, fam)].append((when, row.get("RouteID"), row.get("RouteStopID")))
    for times in cover.values():
        times.sort()
    fits = {key: block_by_hour(ev, listed(key[0], day)[key[1]], tables, codes) for key, ev in stop_events.items()}

    def covered(block: str, when: datetime) -> bool:
        times = cover.get(block) or []
        k = bisect.bisect_left(times, when)
        return any(abs((times[x] - when).total_seconds()) <= COVER_S for x in (k - 1, k) if 0 <= x < len(times))

    line: Dict[str, List[float]] = collections.defaultdict(lambda: [0.0] * len(INNING_STARTS))
    kickoff = None
    if game and game.get("kickoff"):
        try:
            hh, mm = (int(x) for x in str(game["kickoff"]).split(":")[:2])
            kickoff = datetime(day.year, day.month, day.day, hh, mm)
        except ValueError:
            kickoff = None
    before_kickoff: Dict[str, float] = collections.defaultdict(float)
    by_hour = [0.0] * 24
    stops: Dict[str, float] = collections.defaultdict(float)
    block_riders: Dict[str, float] = collections.defaultdict(float)
    block_hours: Dict[str, List[float]] = collections.defaultdict(lambda: [0.0] * 24)
    block_buses: Dict[str, Dict[str, float]] = collections.defaultdict(lambda: collections.defaultdict(float))
    block_span: Dict[str, List[datetime]] = {}
    bus_riders: Dict[str, float] = collections.defaultdict(float)
    family_buses: Dict[str, set] = collections.defaultdict(set)
    first = last = None
    for when, bus, fam, numbers, entries, stop in sorted(events, key=lambda e: e[0]):
        service_hour = int((when - datetime(day.year, day.month, day.day)).total_seconds() // 3600)
        line[fam][_inning(service_hour)] += entries
        by_hour[when.hour] += entries
        stops[stop] += entries
        bus_riders[bus] += entries
        family_buses[fam].add(bus)
        if kickoff and when < kickoff:
            before_kickoff[fam] += entries
        first, last = first or when, when
        if len(numbers) > 1:
            sec = service_hour * 3600 + when.minute * 60
            out_now = [n for n in numbers if any(s - 900 <= sec <= e + 900 for s, e in pieces.get(n, []))]
            numbers = out_now or numbers
            numbers = [n for n in numbers if not covered(n, when)] or numbers
            fit = fits.get((bus, fam), {}).get(when.hour) if when.date() == day else None
            if fit in numbers:
                numbers = [fit]
        for n in numbers:
            share = entries / len(numbers)
            block_riders[n] += share
            block_hours[n][when.hour] += share
            block_buses[n][bus] += share
            span = block_span.setdefault(n, [when, when])
            span[1] = when

    # Miles: each bus's day, split between the blocks it carried riders on (or, with no counter data, was listed on).
    block_miles: Dict[str, float] = collections.defaultdict(float)
    total_miles = 0.0
    buses_out = 0
    for bus, info in (bus_days.get(day) or {}).items():
        miles = float(info.get("miles") or 0)
        if miles <= 5:
            continue
        total_miles += miles
        buses_out += 1
        carried = {n: block_buses[n][bus] for n in block_buses if block_buses[n].get(bus)}
        if not carried:
            numbers = sorted({n for ns in listed(bus, day).values() for n in ns})
            carried = {n: 1.0 for n in numbers}
        weight = sum(carried.values())
        for n, share in carried.items():
            block_miles[n] += miles * share / weight

    shuttles = sorted((f for f in line if f not in FAMILY_COLOR and f != OTHER), key=lambda f: -sum(line[f]))
    routes = []
    for fam in list(FAMILY_COLOR) + shuttles + [OTHER]:
        blocks = []
        for n in sorted(b for b, f in BLOCK_FAMILY.items() if f == fam and (block_riders.get(b) or b in pieces)):
            hours = sum(e - s for s, e in pieces.get(n, [])) / 3600
            riders = block_riders.get(n, 0.0)
            peak = max(range(24), key=lambda h: block_hours[n][h]) if riders else None
            buses = [b for b, v in sorted(block_buses[n].items(), key=lambda kv: -kv[1]) if v >= max(5, 0.05 * riders)]
            blocks.append({
                "block": n,
                "buses": buses,
                "riders": round(riders),
                "hours": round(hours, 1),
                "per_hour": round(riders / hours, 1) if hours and riders else None,
                "miles": round(block_miles.get(n, 0)) or None,
                "peak_hour": peak,
                "peak_riders": round(block_hours[n][peak]) if peak is not None else None,
                "first": _clock(pieces[n][0][0]) if n in pieces else None,
                "last": _clock(pieces[n][-1][1]) if n in pieces else None,
                "scheduled": n in pieces,
            })
        total = sum(line[fam])
        if not total:  # TransLoc's template lists blocks on No Service days too: no riders, no line
            continue
        kind = "route" if fam in FAMILY_COLOR else "other" if fam == OTHER else "shuttle"
        shuttle_color = next((c for word, c in SHUTTLE_COLOR.items() if word in fam.lower()), "#8a8478")
        routes.append({
            "family": fam,
            "kind": kind,
            "name": FAMILY_NAME.get(fam, "Charters & other" if fam == OTHER else fam),
            "color": FAMILY_COLOR.get(fam, shuttle_color if kind == "shuttle" else "#8a8478"),
            "before_kickoff": round(before_kickoff.get(fam, 0)) if kickoff else None,
            "innings": [round(v) for v in line[fam]],
            "riders": round(total),
            "buses": len(family_buses.get(fam, ())),
            "miles": round(sum(block_miles.get(b["block"], 0) for b in blocks)) or None,
            "blocks": blocks,
        })

    played = [b for r in routes for b in r["blocks"] if b["riders"]]
    stars = []
    if played:
        mvp = max(played, key=lambda b: b["riders"])
        stars.append({"block": mvp["block"], "why": f"{mvp['riders']:,} riders, the most of any block", "title": "Most riders"})
        # Three different blocks: each star goes to the best block that does not already have one.
        rest = [b for b in played if b is not mvp]
        rate = max((b for b in rest if b["hours"] >= 3 and b["per_hour"]), key=lambda b: b["per_hour"], default=None)
        if rate:
            stars.append({"block": rate["block"], "why": f"{rate['per_hour']} riders an hour over {rate['hours']} hours", "title": "Best rate"})
        hot = max((b for b in rest if b is not rate), key=lambda b: b["peak_riders"] or 0, default=None)
        if hot:
            stars.append({"block": hot["block"], "why": f"{hot['peak_riders']} riders in the {hot['peak_hour']:02d}:00 hour", "title": "Biggest hour"})
        for s in stars:
            s["family"] = BLOCK_FAMILY[s["block"]]

    top_bus = max(bus_riders.items(), key=lambda kv: kv[1], default=None)
    riders_total = sum(r["riders"] for r in routes)
    shuttle_riders = sum(r["riders"] for r in routes if r["kind"] == "shuttle")
    event = None
    if game or shuttle_riders >= EVENT_MIN_SHUTTLE_RIDERS:
        event = {
            "kind": "football" if game and not game.get("name") else "event",
            "name": (game or {}).get("name"),
            "opponent": (game or {}).get("opponent"),
            "kickoff": (game or {}).get("kickoff"),
            "shuttle_riders": shuttle_riders,
            "regular_riders": sum(r["riders"] for r in routes if r["kind"] == "route"),
        }
    peak_hour = max(range(24), key=lambda h: by_hour[h]) if riders_total else None
    return {
        "v": VERSION,
        "date": day.isoformat(),
        "level": level,
        "event": event,
        "innings": list(INNING_LABELS),
        "routes": routes,
        "totals": {
            "riders": riders_total,
            "innings": [sum(r["innings"][i] for r in routes) for i in range(len(INNING_STARTS))],
            "miles": round(total_miles),
            "buses": buses_out,
            "blocks": len(played),
            "first_boarding": first.strftime("%H:%M") if first else None,
            "last_boarding": last.strftime("%H:%M") if last else None,
            "peak_hour": peak_hour,
            "peak_riders": round(by_hour[peak_hour]) if peak_hour is not None else None,
        },
        "by_hour": [round(v) for v in by_hour],
        "stops": [{"name": name, "riders": round(n)} for name, n in sorted(stops.items(), key=lambda kv: -kv[1])[:8]],
        "stars": stars,
        "top_bus": {"bus": top_bus[0], "riders": round(top_bus[1])} if top_bus and top_bus[0] else None,
    }


def _ordinal(n: int) -> str:
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def notes(box: Dict[str, Any], history: List[Dict[str, Any]]) -> List[str]:
    """Short lines that need other days to say: how this day ranks against the same kind of day at the same service
    level. history is every stored day's summary (see BoxScoreStore.summaries), this one included or not."""
    day = date.fromisoformat(box["date"])
    weekday = day.strftime("%A")
    kind = lambda d: "weekday" if d.weekday() < 5 else d.strftime("%A")
    out: List[str] = []
    if box.get("event"):
        # A game day is ranked against the other game days, whatever day of the week they fell on.
        games = [h for h in history if h["date"] != box["date"] and h.get("event") and h.get("shuttle_riders")]
        mine = box["event"]["shuttle_riders"]
        if games and mine:
            rank = 1 + sum(1 for h in games if h["shuttle_riders"] > mine)
            out.append(f"{mine:,} riders on the lot and fan shuttles, {_ordinal(rank)} of {len(games) + 1} event days on record.")
        return out
    peers = [h for h in history if h["date"] != box["date"] and h.get("level") == box.get("level")
             and not h.get("event") and kind(date.fromisoformat(h["date"])) == kind(day) and h.get("riders")]
    if len(peers) < 3:
        return out
    oldest = date.fromisoformat(min(h["date"] for h in peers))
    since = f"{oldest.strftime('%b')} {oldest.day}"
    label = "weekday" if day.weekday() < 5 else weekday
    riders = box["totals"]["riders"]
    rank = 1 + sum(1 for h in peers if h["riders"] > riders)
    typical = statistics.median(h["riders"] for h in peers)
    if typical:
        diff = round(100 * (riders - typical) / typical)
        how = "right on" if abs(diff) < 3 else f"{abs(diff)}% {'above' if diff > 0 else 'below'}"
        out.append(f"{riders:,} riders: {how} a typical {label} ({round(typical):,}), {_ordinal(rank)} of {len(peers) + 1} since {since}.")
    same = [h for h in peers if date.fromisoformat(h["date"]).weekday() == day.weekday()]
    if day.weekday() < 5 and len(same) >= 3:
        r2 = 1 + sum(1 for h in same if h["riders"] > riders)
        if r2 == 1:
            out.append(f"The busiest {weekday} of the {len(same) + 1} on record.")
        elif r2 == len(same) + 1:
            out.append(f"The quietest {weekday} of the {len(same) + 1} on record.")
    for route in box["routes"]:
        fam = route["family"]
        past = [h["routes"].get(fam, 0) for h in peers if h.get("routes", {}).get(fam)]
        if len(past) < 3 or not route["riders"]:
            continue
        if route["riders"] > max(past):
            out.append(f"{route['name']} had its best {label} on record: {route['riders']:,} riders (old mark {max(past):,}).")
        elif route["riders"] < min(past):
            out.append(f"{route['name']} had its quietest {label} on record: {route['riders']:,} riders.")
    return out


class BoxScoreStore:
    """One JSON file per service day under <data dir>/boxscores/."""

    def __init__(self, data_dir: Path):
        self.dir = Path(data_dir) / "boxscores"
        self._lock = threading.Lock()
        self._summaries: Optional[Dict[str, Dict[str, Any]]] = None

    def path(self, day: date) -> Path:
        return self.dir / f"{day.isoformat()}.json"

    def load(self, day: date) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(self.path(day).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    def save(self, box: Dict[str, Any]) -> None:
        with self._lock:
            self.dir.mkdir(parents=True, exist_ok=True)
            path = self.path(date.fromisoformat(box["date"]))
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(box, separators=(",", ":")), encoding="utf-8")
            tmp.replace(path)
            if self._summaries is not None:
                self._summaries[box["date"]] = self._summary(box)

    @staticmethod
    def _summary(box: Dict[str, Any]) -> Dict[str, Any]:
        return {"date": box["date"], "level": box.get("level"), "final": bool(box.get("final")), "v": box.get("v", 1),
                "event": bool(box.get("event")), "shuttle_riders": (box.get("event") or {}).get("shuttle_riders", 0),
                "riders": box.get("totals", {}).get("riders", 0),
                "routes": {r["family"]: r["riders"] for r in box.get("routes", [])}}

    def summaries(self) -> List[Dict[str, Any]]:
        with self._lock:
            if self._summaries is None:
                self._summaries = {}
                for path in sorted(self.dir.glob("20*.json")) if self.dir.exists() else []:
                    try:
                        box = json.loads(path.read_text(encoding="utf-8"))
                        self._summaries[box["date"]] = self._summary(box)
                    except (OSError, ValueError, KeyError):
                        continue
            return [self._summaries[k] for k in sorted(self._summaries)]

    def days(self) -> List[str]:
        return [s["date"] for s in self.summaries()]


class GameLog:
    """Home football games by date, in <data dir>/boxscores/games.json. The athletics feed only lists games that have
    not been played yet, so each one is written down while it is still on the feed."""

    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "boxscores" / "games.json"
        self._lock = threading.Lock()
        try:
            self.games: Dict[str, Dict[str, Any]] = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            self.games = {}
        for day, game in KNOWN_HOME_GAMES.items():
            self.games.setdefault(day, dict(game))

    def game(self, day: date) -> Optional[Dict[str, Any]]:
        return self.games.get(day.isoformat())

    def record(self, events: Iterable[Dict[str, Any]]) -> int:
        """Takes uva_athletics.load_cached_events() and keeps the home football games. Returns how many were new or
        changed."""
        changed = 0
        with self._lock:
            for event in events or []:
                if not str(event.get("sport") or "").lower().startswith("football"):
                    continue
                if not event.get("is_home", str(event.get("city") or "").lower() == "charlottesville"):
                    continue
                try:
                    start = datetime.fromisoformat(str(event.get("start_time")))
                except ValueError:
                    continue
                # The feed gives midnight for a game whose kick time is not set yet.
                kickoff = start.strftime("%H:%M") if (start.hour, start.minute) != (0, 0) else None
                game = {"opponent": event.get("opponent") or None, "kickoff": kickoff}
                if self.games.get(start.date().isoformat()) != game:
                    self.games[start.date().isoformat()] = game
                    changed += 1
            if changed:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self.path.with_suffix(".tmp")
                tmp.write_text(json.dumps(self.games, indent=1, sort_keys=True), encoding="utf-8")
                tmp.replace(self.path)
        return changed
