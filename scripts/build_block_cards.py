"""Bakes the data for the trading cards (/blockcards, /buscards, /packs): one "baseball card" per UTS block and
one per bus, written to scripts/cards-data.js. scripts/cards.js draws them; the pages fetch nothing else.

Bus cards come from data-local/bus_days/ (scripts/bus_days_pull.py: every day's blocks and miles per bus, back to
2025-09) plus whatever ridership days are on disk.

Block cards:

Per block it works out, from public TransLoc data and the ridership pulls in data-local/ridership/
(scripts/ridership_pull.py):
  - the schedule (pieces, hours) per day type, from one week of TransLoc block groups;
  - timestops and loops per weekday, from config/uts_blocks.json (blocks 01-14 only, Purple has no block package);
  - median riders and miles per day, the best day, the busiest stop, boardings by hour and the bus it usually gets,
    over Full Service days only. Which days those were is read off the data: block [06] only runs on Full Service
    (user, 2026-10-06), so a weekday counts when the [06] bus carried Orange riders, and a weekend counts when the
    Friday before and the Monday after both did. Exam, recess and summer days drop out on their own.


  python scripts/build_block_cards.py [--week-of 2026-09-28] [--since 2026-03-09]

--week-of is the Monday of the week whose schedule goes on the cards (pick a normal full-service week).
--since is the first day of ridership to use. Not before 2026-03-09: that is when the blocks became what they are now
(before it [15], [16] and [26] existed and the interlines were different, e.g. "[22]/[06]").
"""
import argparse
import collections
import datetime
import gzip
import json
import statistics
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# The rules for placing a boarding on a block are shared with /boxscore.
from block_attribution import (  # noqa: E402
    BLOCK_FAMILY,
    EVENT_MIN_SHUTTLE_RIDERS,
    FAMILY_COLOR,
    Cover,
    block_by_hour,
    block_for,
    blocks_by_family,
    family_of,
    group_numbers,
    iso_s,
    parse_time,
    place,
    timestop_codes,
    timetables,
)

RIDERSHIP = ROOT / "data-local" / "ridership"
BUS_DAYS = ROOT / "data-local" / "bus_days"
DATA_JS = ROOT / "scripts" / "cards-data.js"
BLOCKS_SINCE = datetime.date(2026, 3, 9)  # block numbers mean what they mean today from this day on
TRANSLOC = "https://uva.transloc.com/Services/JSONPRelay.svc/"

# What a route id is called on a card when its family has more than one variant.
VARIANT_LABEL = {68: "Day", 54: "Loop", 53: "Day", 55: "Loop", 67: "Day", 57: "Evening", 72: "Early", 74: "Midday", 73: "PM"}
TIMESTOP_NAMES = {"BAR": "Barracks", "CHP": "Chapel", "CSW": "Carl Smith Way", "HER": "Hereford", "JPA": "JPA",
                  "LIB": "Library", "MCQ": "McCormick", "MP": "Madison/Preston", "PIN": "Pinn Hall"}
DAY_TYPES = ("wkd", "sat", "sun")
CANARY_BLOCK = "06"
CANARY_MIN_RIDERS = 100  # a normal day is ~700; well clear of a stray boarding on a bus that was only labelled [06]
# A home football game or other big event turns a day upside down (lot shuttles carry most of the riders, blocks end
# early or run late), so those days are left out of the typical-day numbers. The lot and fan shuttles only run on such
# days: block_attribution.EVENT_MIN_SHUTTLE_RIDERS riders on them marks one.
SERVICE_DAY_START_S = 4 * 3600  # a service day runs to 04:00, so Night Pilot's after-midnight trips stay with the evening


def get_json(path, **params):
    url = TRANSLOC + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def day_type(day):
    return "wkd" if day.weekday() < 5 else ("sat" if day.weekday() == 5 else "sun")


def week_schedule(monday):
    """{service day: {block: [(start_s, end_s, family, route_id)]}} with seconds counted from the service day's midnight, and
    {day type: {block group id: {block: seconds}}} for splitting one bus's miles between the blocks it ran."""
    out = collections.defaultdict(lambda: collections.defaultdict(list))
    by_group = collections.defaultdict(lambda: collections.defaultdict(dict))
    for i in range(8):  # one extra day for Sunday night's after-midnight Night Pilot
        day = monday + datetime.timedelta(days=i)
        cals = get_json("GetScheduleVehicleCalendarByDateAndRoute", dateString=f"{day.month}/{day.day}/{day.year}")
        ids = ",".join(str(c["ScheduleVehicleCalendarID"]) for c in cals if c.get("ScheduleVehicleCalendarID"))
        groups = get_json("GetDispatchBlockGroupData", scheduleVehicleCalendarIdsString=ids).get("BlockGroups", [])
        for g in groups:
            gid = g.get("BlockGroupId") or ""
            if "detour" in gid.lower():
                continue
            numbers = group_numbers(gid)
            for blk in g.get("Blocks") or []:
                for trip in blk.get("Trips") or []:
                    fam = family_of(trip.get("RouteName"))
                    block = block_for(numbers, fam)
                    if not block:
                        continue
                    start, end = iso_s(trip["StartTime"]), iso_s(trip["EndTime"])
                    if monday <= day < monday + datetime.timedelta(days=7):
                        seconds = by_group[(day, gid)]
                        seconds[block] = seconds.get(block, 0) + end - start
                    service_day = day
                    if end <= SERVICE_DAY_START_S:
                        service_day, start, end = day - datetime.timedelta(days=1), start + 86400, end + 86400
                    if monday <= service_day < monday + datetime.timedelta(days=7):
                        out[service_day][block].append((start, end, fam, trip.get("RouteID")))
    # TransLoc lists Night Pilot [04]'s 00:00-02:00 tail on Thursday too, though Wednesday night has no [04]: a
    # service day with only an after-midnight tail and no evening piece is not a day the block runs.
    for blocks in out.values():
        for block in [b for b, segs in blocks.items() if all(seg[0] >= 86400 for seg in segs)]:
            del blocks[block]
    group_seconds = collections.defaultdict(dict)
    for (day, gid), seconds in by_group.items():
        group_seconds[day_type(day)][gid] = seconds
    return out, group_seconds


def merge_pieces(segments, by_route=False):
    """Joins back-to-back segments on the same route family (TransLoc splits a block at every route-id flip), or,
    with by_route, on the same route id: the block's lineup of route variants."""
    pieces = []
    for start, end, fam, route_id in sorted(segments):
        key = route_id if by_route else fam
        if pieces and pieces[-1][2] == key and start - pieces[-1][1] <= 5 * 60:
            pieces[-1][1] = max(pieces[-1][1], end)
        else:
            pieces.append([start, end, key])
    for p in pieces:  # 23:59 -> 00:00 seams and 07:28 / 21:58 style pull-out offsets
        p[0], p[1] = round(p[0] / 300) * 300, round(p[1] / 300) * 300
    for a, b in zip(pieces, pieces[1:]):  # a 06:56 / 06:58 route flip rounds to 06:55 / 07:00: close the gap
        if 0 < b[0] - a[1] <= 5 * 60:
            a[1] = b[0]
    return pieces


def typical(schedule, block, kind):
    """The pieces this block runs on a typical day of this type (the most common pattern across the week), the same
    day split by route variant instead, and how many days of this type it runs."""
    patterns, lineups = collections.Counter(), {}
    for day, blocks in sorted(schedule.items()):
        if day_type(day) == kind and block in blocks:
            key = json.dumps(merge_pieces(blocks[block]))
            patterns[key] += 1
            lineups.setdefault(key, merge_pieces(blocks[block], by_route=True))
    if not patterns:
        return [], [], 0
    key = patterns.most_common(1)[0][0]
    return json.loads(key), lineups[key], sum(patterns.values())


def full_service_days(since):
    """The ordinary Full Service days in data-local/ridership/: Full Service going by block [06] (see the module
    docstring), minus game and event days."""
    canary, shuttle = {}, collections.Counter()
    for path in sorted(RIDERSHIP.glob("*.json.gz")):
        day = datetime.date.fromisoformat(path.name[:10])
        blocks_path = RIDERSHIP / f"{day}.blocks.json"
        if day < since or not blocks_path.exists():
            continue
        canary[day] = 0
        buses = {b for b, v in json.loads(blocks_path.read_text())["buses"].items()
                 if any(CANARY_BLOCK in group_numbers(g) for g in v.get("blocks") or [])}
        for row in json.loads(gzip.decompress(path.read_bytes())):
            if "shuttle" in (row.get("Route") or "").lower():
                shuttle[day] += row.get("Entries") or 0
            if day.weekday() < 5 and row.get("Vehicle") in buses and family_of(row.get("Route")) == BLOCK_FAMILY[CANARY_BLOCK]:
                when = parse_time(row["ClientTime"])
                if when.date() == day and 9 <= when.hour < 17:  # the bus is Purple [19] before 08:25
                    canary[day] += row.get("Entries") or 0
    full = {d for d, n in canary.items() if d.weekday() < 5 and n >= CANARY_MIN_RIDERS}
    for day in canary:
        if day.weekday() >= 5:
            friday = day - datetime.timedelta(days=day.weekday() - 4)
            monday = day + datetime.timedelta(days=7 - day.weekday())
            if friday in full and (monday in full or monday not in canary):
                full.add(day)
    events = sorted(d for d in full if shuttle[d] >= EVENT_MIN_SHUTTLE_RIDERS)
    if events:
        print("left out as game/event days:", ", ".join(f"{d} ({shuttle[d]:,} shuttle riders)" for d in events))
    return full - set(events)


def date_ranges(days):
    """Sorted days as [first, last] runs, a run ending at a gap of more than four days (a long weekend is not a gap)."""
    runs = []
    for day in sorted(days):
        if runs and (day - runs[-1][1]).days <= 4:
            runs[-1][1] = day
        else:
            runs.append([day, day])
    return [[a.isoformat(), b.isoformat()] for a, b in runs]


def ridership(schedule_by_type, group_seconds, since, full):
    """Boardings, miles and buses per block per Full Service day.

    A bus listed on several blocks of one route has each boarding placed by block_attribution.place() (scheduled out,
    not covered by another bus, timestop fit, then an even split)."""
    riders = collections.defaultdict(lambda: collections.defaultdict(float))   # block -> service day -> boardings
    by_hour = collections.defaultdict(lambda: collections.defaultdict(float))  # block -> hour -> boardings (weekdays)
    by_stop = collections.defaultdict(lambda: collections.defaultdict(float))
    miles = collections.defaultdict(lambda: collections.defaultdict(float))
    buses = collections.defaultdict(lambda: collections.defaultdict(set))      # block -> bus -> days
    days = set()
    split = fitted = total_boardings = 0.0
    codes = timestop_codes(json.loads((ROOT / "config" / "uts_timestops.json").read_text(encoding="utf-8")))
    packages = json.loads((ROOT / "config" / "uts_blocks.json").read_text(encoding="utf-8"))["blocks"]
    for path in sorted(RIDERSHIP.glob("*.json.gz")):
        day = datetime.date.fromisoformat(path.name[:10])
        blocks_path = RIDERSHIP / f"{day}.blocks.json"
        if day < since or not blocks_path.exists():
            continue
        if day in full:
            days.add(day)
        bus_groups = {b: v for b, v in json.loads(blocks_path.read_text())["buses"].items() if v.get("blocks")}
        kind = day_type(day)
        # bus -> family -> the blocks of that route it is listed on
        candidates = {bus: blocks_by_family(info["blocks"]) for bus, info in bus_groups.items()}

        tables = timetables(day, packages, recess=False) if day.weekday() < 5 else {}
        stop_events = collections.defaultdict(list)  # (bus, family) -> door events, for buses listed on 2+ blocks
        events = []
        cover = Cover()  # when a bus listed on one block alone boarded someone
        for row in json.loads(gzip.decompress(path.read_bytes())):
            entries = row.get("Entries") or 0
            fam = family_of(row.get("Route"))
            numbers = candidates.get(row.get("Vehicle"), {}).get(fam)
            if not entries or not numbers:
                continue
            when = parse_time(row["ClientTime"])
            events.append((when, row["Vehicle"], fam, numbers, entries, row.get("RouteStop") or "?"))
            if len(numbers) == 1:
                cover.add(numbers[0], when)
            elif when.date() == day and all(n in tables for n in numbers):
                stop_events[(row["Vehicle"], fam)].append((when, row.get("RouteID"), row.get("RouteStopID")))
        fits = {key: block_by_hour(ev, candidates[key[0]][key[1]], tables, codes) for key, ev in stop_events.items()}
        cover.sort()

        attributed = collections.defaultdict(lambda: collections.defaultdict(float))  # bus -> block -> boardings
        for when, bus, fam, numbers, entries, stop in events:
            fit = fits.get((bus, fam), {}).get(when.hour) if when.date() == day else None
            numbers, by_fit = place(numbers, when, when.hour * 3600 + when.minute * 60, schedule_by_type[kind], cover, fit)
            service_day = (when - datetime.timedelta(seconds=SERVICE_DAY_START_S)).date()
            for block in numbers:
                share = entries / len(numbers)
                attributed[bus][block] += share
                if service_day not in full:
                    continue
                riders[block][service_day] += share
                by_stop[block][stop] += share
                if service_day.weekday() < 5:
                    by_hour[block][when.hour] += share
            if service_day in full:
                total_boardings += entries
                split += entries if len(numbers) > 1 else 0
                fitted += entries if by_fit else 0

        if day not in full:
            continue
        for bus, info in bus_groups.items():
            # Miles are per bus per day; a bus that ran several blocks has them split by the scheduled hours of the
            # block groups it was on ("[19]/[06]" is the morning Purple piece + Orange, not [19]'s afternoon bus)...
            hours = collections.defaultdict(float)
            for g in info["blocks"]:
                known = group_seconds[kind].get(g)
                for n in group_numbers(g):
                    if n in BLOCK_FAMILY:
                        hours[n] += known.get(n, 0) if known else sum(e - s for s, e, _ in schedule_by_type[kind].get(n, []))
            # ...and between blocks of one route, by where its riders were placed above rather than by the schedule.
            for numbers in candidates[bus].values():
                carried = sum(attributed[bus][n] for n in numbers)
                if len(numbers) > 1 and carried:
                    pool = sum(hours[n] for n in numbers)
                    for n in numbers:
                        hours[n] = pool * attributed[bus][n] / carried
            total = sum(hours.values())
            counted = sum(attributed[bus].values())
            for n in hours:
                if counted and attributed[bus][n] < 0.1 * counted:
                    continue  # it barely carried anyone on this block: not this block's bus
                buses[n][bus].add(day)
            for n in hours if total and info.get("actual_miles") else []:
                miles[n][day] += info["actual_miles"] * hours[n] / total
    print(f"bus listed on two blocks of one route: {fitted / (total_boardings or 1):.1%} of boardings placed by timestop "
          f"times, {split / (total_boardings or 1):.1%} split evenly")
    return riders, by_hour, by_stop, miles, buses, days


def season_of(day):
    return ("Spring " if day.month <= 6 else "Fall ") + str(day.year)


def bus_season(day):
    """Fall runs from the start of Full Service (Aug 20) to New Year, summer from the end of exams (May 11)."""
    md = (day.month, day.day)
    name = "Spring" if md < (5, 11) else "Summer" if md < (8, 20) else "Fall"
    return f"{name} {day.year}"


def bus_cards():
    """One card per bus: miles and days in service per season, the blocks and routes it gets, riders where counted."""
    seasons = collections.defaultdict(lambda: collections.defaultdict(lambda: {"days": 0, "miles": 0.0}))
    best_miles, block_days, family_days, other_days, weekend_days = {}, {}, {}, {}, collections.Counter()
    first_seen, last_seen, order = {}, {}, []
    for path in sorted(BUS_DAYS.glob("*.blocks.json")):
        day = datetime.date.fromisoformat(path.name[:10])
        for bus, info in json.loads(path.read_text())["buses"].items():
            if bus not in order:
                order.append(bus)
            miles = info.get("actual_miles") or 0
            if miles > 5:
                first_seen.setdefault(bus, day)
                last_seen[bus] = day
                season = seasons[bus][bus_season(day)]
                season["days"] += 1
                season["miles"] += miles
                weekend_days[bus] += day.weekday() >= 5
                if miles > best_miles.get(bus, (0, None))[0]:
                    best_miles[bus] = (miles, day)
            if day < BLOCKS_SINCE:
                continue
            numbers = {n for g in info.get("blocks") or [] for n in group_numbers(g) if n in BLOCK_FAMILY}
            for n in numbers:
                block_days.setdefault(bus, collections.Counter())[n] += 1
            for fam in {BLOCK_FAMILY[n] for n in numbers}:
                family_days.setdefault(bus, collections.Counter())[fam] += 1
            for g in info.get("blocks") or []:
                if not group_numbers(g):
                    kind = "Training" if "Training" in g else "Charter" if "Charter" in g else "Event"
                    other_days.setdefault(bus, collections.Counter())[kind] += 1

    riders = collections.defaultdict(lambda: collections.defaultdict(int))  # bus -> day -> boardings
    by_stop = collections.defaultdict(collections.Counter)
    by_family = collections.defaultdict(collections.Counter)
    counted_days = 0
    for path in sorted(RIDERSHIP.glob("*.json.gz")):
        day = datetime.date.fromisoformat(path.name[:10])
        counted_days += 1
        for row in json.loads(gzip.decompress(path.read_bytes())):
            entries, bus = row.get("Entries") or 0, row.get("Vehicle")
            if entries and bus:
                riders[bus][day] += entries
                by_stop[bus][row.get("RouteStop") or "?"] += entries
                by_family[bus][family_of(row.get("Route")) or "Other"] += entries

    cards = []
    for bus in order:
        if bus not in first_seen:
            continue
        mix = family_days.get(bus, collections.Counter()) + other_days.get(bus, collections.Counter())
        total_mix = sum(mix.values()) or 1
        top_family = family_days.get(bus, collections.Counter()).most_common(1)
        carried = riders.get(bus, {})
        best = max(carried.items(), key=lambda kv: kv[1], default=None)
        stop = by_stop[bus].most_common(1)
        blocks = block_days.get(bus, collections.Counter())
        cards.append({
            "bus": bus,
            "family": top_family[0][0] if top_family else None,
            "color": FAMILY_COLOR[top_family[0][0]] if top_family else "#4a4f5a",
            "first_seen": first_seen[bus].isoformat(),
            "last_seen": last_seen[bus].isoformat(),
            "seasons": [{"name": name, "days": v["days"], "miles": round(v["miles"])}
                        for name, v in sorted(seasons[bus].items(), key=lambda kv: (kv[0][-4:], "SpSuFa".index(kv[0][:2])))],
            "days": sum(v["days"] for v in seasons[bus].values()),
            "miles": round(sum(v["miles"] for v in seasons[bus].values())),
            "weekend_days": weekend_days[bus],
            "best_miles": {"date": best_miles[bus][1].isoformat(), "miles": round(best_miles[bus][0])},
            "blocks": [{"block": n, "days": d} for n, d in blocks.most_common(5)],
            "block_count": len(blocks),
            "block_days": sum(1 for _ in blocks.elements()),
            "mix": [{"name": name, "share": round(n / total_mix, 3)} for name, n in mix.most_common()],
            "night_days": blocks["03"] + blocks["04"],
            "riders": {
                "total": sum(carried.values()),
                "days": sum(1 for v in carried.values() if v >= 20),
                "median": round(median_of(v for v in carried.values() if v >= 20) or 0),
                "best": {"date": best[0].isoformat(), "riders": best[1]} if best else None,
                "top_stop": {"name": stop[0][0], "share": round(stop[0][1] / sum(by_stop[bus].values()), 3)} if stop else None,
            } if sum(carried.values()) >= 500 else None,
        })
    span = sorted(datetime.date.fromisoformat(p.name[:10]) for p in BUS_DAYS.glob("*.blocks.json"))
    info = {"from": min(first_seen.values()).isoformat(), "to": span[-1].isoformat(), "rider_days": counted_days,
            "blocks_since": BLOCKS_SINCE.isoformat()}
    return cards, info


def median_of(values):
    values = [v for v in values if v > 0]
    return statistics.median(values) if values else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week-of", default="2026-09-28")
    ap.add_argument("--since", default="2026-03-09")
    args = ap.parse_args()
    monday = datetime.date.fromisoformat(args.week_of)
    since = datetime.date.fromisoformat(args.since)

    schedule, group_seconds = week_schedule(monday)
    full = full_service_days(since)
    packages = json.loads((ROOT / "config" / "uts_blocks.json").read_text(encoding="utf-8"))["blocks"]
    pieces, lineups, run_days = {}, {}, {}
    for block in BLOCK_FAMILY:
        pieces[block], lineups[block], run_days[block] = {}, {}, {}
        for kind in DAY_TYPES:
            pieces[block][kind], lineups[block][kind], run_days[block][kind] = typical(schedule, block, kind)
    schedule_by_type = {k: {b: pieces[b][k] for b in BLOCK_FAMILY} for k in DAY_TYPES}
    riders, by_hour, by_stop, miles, buses, days = ridership(schedule_by_type, group_seconds, since, full)

    cards = []
    for block, fam in sorted(BLOCK_FAMILY.items()):
        rows = {}
        for kind in DAY_TYPES:
            if not pieces[block][kind]:
                continue
            hours = sum(e - s for s, e, _ in pieces[block][kind]) / 3600
            r = median_of(v for d, v in riders[block].items() if day_type(d) == kind)
            m = median_of(v for d, v in miles[block].items() if day_type(d) == kind and v > 5)
            rows[kind] = {
                "days": run_days[block][kind],
                "pieces": pieces[block][kind],
                "lineup": lineups[block][kind],
                "hours": round(hours, 1),
                "riders": round(r) if r else None,
                "per_hour": round(r / hours, 1) if r and hours else None,
                "miles": round(m) if m else None,
            }
        best = max(riders[block].items(), key=lambda kv: kv[1], default=None)
        stop = max(by_stop[block].items(), key=lambda kv: kv[1], default=None)
        bus = max(buses[block].items(), key=lambda kv: len(kv[1]), default=None)
        weekdays = len({d for d in riders[block] if d.weekday() < 5}) or 1
        card = {
            "block": block,
            "family": fam,
            "color": FAMILY_COLOR[fam],
            "rows": rows,
            "week_hours": round(sum(r["hours"] * r["days"] for r in rows.values()), 1),
            "week_riders": round(sum((r["riders"] or 0) * r["days"] for r in rows.values())),
            "best_day": {"date": best[0].isoformat(), "riders": round(best[1])} if best else None,
            "top_stop": {"name": stop[0], "share": round(stop[1] / sum(by_stop[block].values()), 3)} if stop else None,
            "usual_bus": {"bus": bus[0], "days": len(bus[1]), "of": len({d for b in buses[block].values() for d in b})} if bus else None,
            "by_hour": [round(by_hour[block].get(h, 0) / weekdays) for h in range(24)],
        }
        # One line per semester, like a baseball card's season rows: the typical weekday (or, for a block with no
        # weekday service, whatever days it runs).
        card["seasons"] = []
        for season in sorted({season_of(d) for d in days}, key=lambda name: (name[-4:], name[0] != "S")):
            pick = lambda d: season_of(d) == season and (d.weekday() < 5 or "wkd" not in rows)
            r = median_of(v for d, v in riders[block].items() if pick(d))
            m = median_of(v for d, v in miles[block].items() if pick(d) and v > 5)
            if r or m:
                card["seasons"].append({"name": season, "days": len({d for d in riders[block] if pick(d)}),
                                        "riders": round(r) if r else None, "miles": round(m) if m else None})
        package = packages.get(f"[{block}]")
        if package:
            group = next((g for g in package["weekday_groups"] if g.get("service") != "recess" and 2 in g["weekdays"]),
                         package["weekday_groups"][0])
            # The block package's evening route change, not TransLoc's fixed clock-time flip, is what the bus follows.
            change = (group.get("route_change") or {}).get("leave_s")
            lineup = (rows.get("wkd") or {}).get("lineup") or []
            for a, b in zip(lineup, lineup[1:]):
                if change and a[1] == b[0] and abs(a[1] - change) <= 30 * 60:
                    a[1] = b[0] = change
            codes = [c for _, c in group["stops"]]
            order = list(dict.fromkeys(codes))
            card["timestops"] = {
                "count": len(codes),
                "codes": order,
                "names": [TIMESTOP_NAMES.get(c, c) for c in order],
                "loops": max(codes.count(c) for c in order),
                "then": (group.get("out_of_service") or {}).get("then"),
            }
        # The variant the block spends most of its week on is the bold line on the front; the rest are ghosts.
        seconds = collections.Counter()
        for r in rows.values():
            for start, end, route_id in r["lineup"]:
                seconds[route_id] += (end - start) * r["days"]
        card["routes"] = [route_id for route_id, _ in seconds.most_common()]
        cards.append(card)

    routes = get_json("GetRoutesForMapWithScheduleWithEncodedLine")
    shapes = {}
    for route_id in sorted({r for c in cards for r in c["routes"]}):
        route = next((r for r in routes if r.get("RouteID") == route_id), None)
        if route:
            shapes[str(route_id)] = route.get("EncodedPolyline") or ""

    data = {
        "generated": datetime.date.today().isoformat(),
        "schedule_week": monday.isoformat(),
        "ridership_from": min(days).isoformat() if days else None,
        "ridership_to": max(days).isoformat() if days else None,
        "ridership_days": len(days),
        "ridership_ranges": date_ranges(days),
        "cards": cards,
        "shapes": shapes,
        "variants": {str(k): v for k, v in VARIANT_LABEL.items()},
    }
    data["buses"], data["bus_info"] = bus_cards()
    data["family_colors"] = FAMILY_COLOR
    DATA_JS.write_text("// Generated by scripts/build_block_cards.py. Do not edit.\nwindow.CARD_DATA = "
                       + json.dumps(data, separators=(",", ":")) + ";\n", encoding="utf-8")
    print(f"{len(cards)} cards, {len(days)} Full Service days: {data['ridership_ranges']}")
    for c in cards:
        w = c["rows"].get("wkd") or {}
        print(c["block"], c["family"], "wkd", w.get("hours"), "h", w.get("riders"), "riders", w.get("miles"), "mi",
              "| sat", (c["rows"].get("sat") or {}).get("riders"), "| sun", (c["rows"].get("sun") or {}).get("riders"),
              "| best", c["best_day"], "| bus", c["usual_bus"], "| stop", c["top_stop"])
    print(len(data["buses"]), "bus cards,", data["bus_info"])
    for c in data["buses"]:
        print(c["bus"], c["family"], c["first_seen"], c["days"], "days", c["miles"], "mi", "blocks", c["block_count"],
              [(x["block"], x["days"]) for x in c["blocks"][:3]], [(m["name"], m["share"]) for m in c["mix"][:3]],
              "riders", (c["riders"] or {}).get("median"))


if __name__ == "__main__":
    main()
