"""Which block a boarding belongs to: the one copy of the rules /boxscore (boxscore.py) and the trading cards
(scripts/build_block_cards.py) both use.

The bus map (state.bus_days, what /v1/servicecrew serves) lists every block a bus TOUCHED that day, without times,
so a bus dispatch had on [09] for a minute before moving it to [11] is listed on both. Counting it on both gave [09]
2,201 riders on 2026-08-26, two buses' worth. So a bus listed on several blocks of one route has each boarding placed
by, in order (place()):
  1. the blocks scheduled to be out at that time;
  2. of those, the ones no other bus (one listed on that block alone) is covering right then (Cover);
  3. of those, the one its timestop times fit (block_by_hour: weekday daytime route, blocks with a block package);
  4. an even split between whatever is left.
Cover goes before the timetable fit because the fit is weak on Orange (four buses a few minutes apart) and on
spring's detour days: tried the other way round, it put two all-day buses on [05] on 2026-08-26.
"""
from __future__ import annotations

import bisect
import collections
import re
import statistics
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
# Game days. The lot and fan shuttles ("Purple Lots Shuttle", "Post-Game Fan Shuttle"...) only run for a home football
# game or another big event (a stadium concert), so a day with this many riders on them is an event day whether or
# not anyone told us what it was (2026-08-29, 09-11 and 09-26 each had 6,000+).
EVENT_MIN_SHUTTLE_RIDERS = 500
SCHEDULE_SLACK_S = 15 * 60  # a block counts as out from this long before its piece starts to this long after it ends
COVER_S = 45 * 60  # a block counts as covered by another bus if that bus boarded someone within this long
# Telling a bus's block from when it is at timestops. Only the daytime route on weekdays: the evening and weekend
# routes pass some timestops twice a loop, which makes "nearest scheduled time" meaningless. Scored 2026-10-06 on
# buses listed on one block only (pick between the true block and each other block of the route, hour by hour):
# right 98% of the time on Gold, 100% Green, 94% Silver, 87% Orange, before the smoothing below.
FIT_HOURS = range(7, 17)
FIT_BEST_MAX_S = 300   # the winning block's timestops must be within 5 min (median)
FIT_MARGIN_S = 240     # and the runner-up at least 4 min worse
FIT_SMOOTH_HOURS = 2   # an hour's answer is the majority of the clear hours within +/- this many


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


def block_for(numbers: Iterable[str], family: Optional[str]) -> Optional[str]:
    """The block number, out of a block group's numbers, that runs this route family ("[17]/[10]" + Gold -> 10)."""
    match = [n for n in numbers if BLOCK_FAMILY.get(n) == family]
    return match[0] if len(match) == 1 else None


def blocks_by_family(groups: Iterable[Any]) -> Dict[str, List[str]]:
    """{route family: the blocks of that route} out of the block groups a bus is listed on for a day."""
    by_family: Dict[str, set] = collections.defaultdict(set)
    for g in groups or []:
        for n in group_numbers(g):
            if n in BLOCK_FAMILY:
                by_family[BLOCK_FAMILY[n]].add(n)
    return {fam: sorted(ns) for fam, ns in by_family.items()}


def parse_time(text: str) -> datetime:
    return datetime.strptime(text, "%m/%d/%Y %I:%M:%S %p")


def iso_s(text: Any) -> int:
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(text or ""))
    return (int(m[1] or 0) * 3600 + int(m[2] or 0) * 60 + int(m[3] or 0)) if m else 0


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
    """{(route id, route stop id): timestop code} from config/uts_timestops.json."""
    return {(int(route), int(rsid)): code for code, by_route in (timestops or {}).items() for route, rsid in by_route.items()}


def block_by_hour(stop_events, candidates, tables, codes) -> Dict[int, str]:
    """{hour: block} for a bus listed on several blocks of one route: which of them its timestop times fit, hour by
    hour, smoothed (a bus does not change block every hour). Empty if the timetable never gives a clear answer."""
    visits: Dict[int, List[Tuple[int, str]]] = collections.defaultdict(list)
    last: Dict[str, int] = {}
    for when, route_id, rsid in sorted(stop_events):
        code = codes.get((route_id, rsid))
        if not code:
            continue
        sec = when.hour * 3600 + when.minute * 60 + when.second
        if code not in last or sec - last[code] > 240:  # door events under 4 min apart are one stay at the stop
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
        if near:
            out[hour] = near.most_common(1)[0][0]
        else:  # early morning, evening, or a quiet stretch: the nearest clear hour's answer carries
            out[hour] = clear[min(clear, key=lambda h: abs(h - hour))]
    return out


class Cover:
    """When each block had a bus of its own boarding riders (a bus listed on that block alone)."""

    def __init__(self) -> None:
        self._times: Dict[str, List[datetime]] = collections.defaultdict(list)

    def add(self, block: str, when: datetime) -> None:
        self._times[block].append(when)

    def sort(self) -> None:
        for times in self._times.values():
            times.sort()

    def covered(self, block: str, when: datetime) -> bool:
        times = self._times.get(block) or []
        k = bisect.bisect_left(times, when)
        return any(abs((times[x] - when).total_seconds()) <= COVER_S for x in (k - 1, k) if 0 <= x < len(times))


def place(
    numbers: List[str],
    when: datetime,
    sec: int,
    pieces: Dict[str, Sequence[Sequence[Any]]],
    cover: Cover,
    fit: Optional[str],
) -> Tuple[List[str], bool]:
    """The blocks one boarding goes to (shared evenly if more than one), out of the blocks of its route its bus is
    listed on, and whether the timestop fit settled it. sec is the boarding's time in the clock pieces uses
    ({block: [(start_s, end_s, ...)]}); fit is block_by_hour's answer for that hour, if there is one."""
    if len(numbers) <= 1:
        return numbers, False
    out_now = [n for n in numbers
               if any(p[0] - SCHEDULE_SLACK_S <= sec <= p[1] + SCHEDULE_SLACK_S for p in pieces.get(n, []))]
    numbers = out_now or numbers
    numbers = [n for n in numbers if not cover.covered(n, when)] or numbers
    if fit in numbers and len(numbers) > 1:
        return [fit], True
    return numbers, False
