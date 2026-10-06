"""Bakes the data for /blockcards (html/blockcards.html): one "baseball card" per UTS block.

Per block it works out, from public TransLoc data and the ridership pulls in data-local/ridership/
(scripts/ridership_pull.py):
  - the schedule (pieces, hours) per day type, from one week of TransLoc block groups;
  - timestops and loops per weekday, from config/uts_blocks.json (blocks 01-14 only, Purple has no block package);
  - median riders and miles per day, the best day, the busiest stop, boardings by hour and the bus it usually gets.

The result replaces the JSON between the BLOCKCARDS-DATA markers in html/blockcards.html.

  python scripts/build_block_cards.py [--week-of 2026-09-28] [--since 2026-08-25]

--week-of is the Monday of the week whose schedule goes on the cards (pick a normal full-service week).
"""
import argparse
import collections
import datetime
import gzip
import json
import re
import statistics
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RIDERSHIP = ROOT / "data-local" / "ridership"
PAGE = ROOT / "html" / "blockcards.html"
TRANSLOC = "https://uva.transloc.com/Services/JSONPRelay.svc/"
MARK_START = "/*BLOCKCARDS-DATA-START*/"
MARK_END = "/*BLOCKCARDS-DATA-END*/"

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
# The route whose line is drawn on the card front.
FAMILY_SHAPE_ROUTE = {"Green": 68, "Night Pilot": 59, "Orange": 53, "Gold": 67, "Silver": 58, "Purple": 74}
TIMESTOP_NAMES = {"BAR": "Barracks", "CHP": "Chapel", "CSW": "Carl Smith Way", "HER": "Hereford", "JPA": "JPA",
                  "LIB": "Library", "MCQ": "McCormick", "MP": "Madison/Preston", "PIN": "Pinn Hall"}
DAY_TYPES = ("wkd", "sat", "sun")
SERVICE_DAY_START_S = 4 * 3600  # a service day runs to 04:00, so Night Pilot's after-midnight trips stay with the evening


def get_json(path, **params):
    url = TRANSLOC + path + ("?" + urllib.parse.urlencode(params) if params else "")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def family_of(route_name):
    for fam in FAMILY_COLOR:
        if fam.lower() in (route_name or "").lower():
            return fam
    return None


def group_numbers(group_id):
    return [n.zfill(2) for n in re.findall(r"\[(\d+)\]", group_id or "")]


def iso_s(text):
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", text or "")
    return int(m[1] or 0) * 3600 + int(m[2] or 0) * 60 + int(m[3] or 0)


def day_type(day):
    return "wkd" if day.weekday() < 5 else ("sat" if day.weekday() == 5 else "sun")


def block_for(numbers, family):
    """The block number, out of a block group's numbers, that runs this route family ("[17]/[10]" + Gold -> 10)."""
    match = [n for n in numbers if BLOCK_FAMILY.get(n) == family]
    return match[0] if len(match) == 1 else None


def week_schedule(monday):
    """{service day: {block: [(start_s, end_s, family)]}} with seconds counted from the service day's midnight, and
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
                        out[service_day][block].append((start, end, fam))
    # TransLoc lists Night Pilot [04]'s 00:00-02:00 tail on Thursday too, though Wednesday night has no [04]: a
    # service day with only an after-midnight tail and no evening piece is not a day the block runs.
    for blocks in out.values():
        for block in [b for b, segs in blocks.items() if all(s >= 86400 for s, _, _ in segs)]:
            del blocks[block]
    group_seconds = collections.defaultdict(dict)
    for (day, gid), seconds in by_group.items():
        group_seconds[day_type(day)][gid] = seconds
    return out, group_seconds


def merge_pieces(segments):
    """Joins back-to-back segments on the same route family (TransLoc splits a block at every route-id flip)."""
    pieces = []
    for start, end, fam in sorted(segments):
        if pieces and pieces[-1][2] == fam and start - pieces[-1][1] <= 5 * 60:
            pieces[-1][1] = max(pieces[-1][1], end)
        else:
            pieces.append([start, end, fam])
    for p in pieces:  # 23:59 -> 00:00 seams and 07:28 / 21:58 style pull-out offsets
        p[0], p[1] = round(p[0] / 300) * 300, round(p[1] / 300) * 300
    return pieces


def typical(schedule, block, kind):
    """The pieces this block runs on a typical day of this type (the most common pattern across the week)."""
    patterns = collections.Counter()
    for day, blocks in schedule.items():
        if day_type(day) == kind and block in blocks:
            patterns[json.dumps(merge_pieces(blocks[block]))] += 1
    days = sum(patterns.values())
    return (json.loads(patterns.most_common(1)[0][0]), days) if patterns else ([], 0)


def parse_time(text):
    return datetime.datetime.strptime(text, "%m/%d/%Y %I:%M:%S %p")


def ridership(schedule_by_type, group_seconds, since):
    riders = collections.defaultdict(lambda: collections.defaultdict(int))   # block -> service day -> boardings
    by_hour = collections.defaultdict(lambda: collections.defaultdict(int))  # block -> hour -> boardings (weekdays)
    by_stop = collections.defaultdict(lambda: collections.defaultdict(int))
    miles = collections.defaultdict(lambda: collections.defaultdict(float))
    buses = collections.defaultdict(lambda: collections.defaultdict(set))    # block -> bus -> days
    days = set()
    for path in sorted(RIDERSHIP.glob("*.json.gz")):
        day = datetime.date.fromisoformat(path.name[:10])
        blocks_path = RIDERSHIP / f"{day}.blocks.json"
        if day < since or not blocks_path.exists():
            continue
        days.add(day)
        bus_groups = {b: v for b, v in json.loads(blocks_path.read_text())["buses"].items() if v.get("blocks")}
        kind = day_type(day)
        for bus, info in bus_groups.items():
            numbers = sorted({n for g in info["blocks"] for n in group_numbers(g)} & set(BLOCK_FAMILY))
            # Miles are per bus per day; a bus that ran several blocks has them split by the scheduled hours of the
            # block groups it was on ("[19]/[06]" is the morning Purple piece + Orange, not [19]'s afternoon bus).
            hours = collections.defaultdict(float)
            for g in info["blocks"]:
                known = group_seconds[kind].get(g)
                for n in group_numbers(g):
                    if n in BLOCK_FAMILY:
                        hours[n] += known.get(n, 0) if known else sum(e - s for s, e, _ in schedule_by_type[kind].get(n, []))
            total = sum(hours.values())
            for n in numbers:
                buses[n][bus].add(day)
                if total and info.get("actual_miles"):
                    miles[n][day] += info["actual_miles"] * hours[n] / total
        for row in json.loads(gzip.decompress(path.read_bytes())):
            entries = row.get("Entries") or 0
            info = bus_groups.get(row.get("Vehicle"))
            if not entries or not info:
                continue
            when = parse_time(row["ClientTime"])
            fam = family_of(row.get("Route"))
            numbers = sorted({n for g in info["blocks"] for n in group_numbers(g) if BLOCK_FAMILY.get(n) == fam})
            if not numbers:
                continue
            block = numbers[0]
            if len(numbers) > 1:  # same bus on two blocks of one route that day: go by the clock
                sec = when.hour * 3600 + when.minute * 60
                for n in numbers:
                    if any(s - 900 <= sec <= e + 900 for s, e, _ in schedule_by_type[kind].get(n, [])):
                        block = n
                        break
            service_day = (when - datetime.timedelta(seconds=SERVICE_DAY_START_S)).date()
            riders[block][service_day] += entries
            by_stop[block][row.get("RouteStop") or "?"] += entries
            if service_day.weekday() < 5:
                by_hour[block][when.hour] += entries
    return riders, by_hour, by_stop, miles, buses, days


def median_of(values):
    values = [v for v in values if v > 0]
    return statistics.median(values) if values else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week-of", default="2026-09-28")
    ap.add_argument("--since", default="2026-08-25")
    args = ap.parse_args()
    monday = datetime.date.fromisoformat(args.week_of)
    since = datetime.date.fromisoformat(args.since)

    schedule, group_seconds = week_schedule(monday)
    packages = json.loads((ROOT / "config" / "uts_blocks.json").read_text(encoding="utf-8"))["blocks"]
    pieces, run_days = {}, {}
    for block in BLOCK_FAMILY:
        pieces[block], run_days[block] = {}, {}
        for kind in DAY_TYPES:
            pieces[block][kind], run_days[block][kind] = typical(schedule, block, kind)
    schedule_by_type = {k: {b: pieces[b][k] for b in BLOCK_FAMILY} for k in DAY_TYPES}
    riders, by_hour, by_stop, miles, buses, days = ridership(schedule_by_type, group_seconds, since)

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
            "best_day": {"date": best[0].isoformat(), "riders": best[1]} if best else None,
            "top_stop": {"name": stop[0], "share": round(stop[1] / sum(by_stop[block].values()), 3)} if stop else None,
            "usual_bus": {"bus": bus[0], "days": len(bus[1]), "of": len({d for b in buses[block].values() for d in b})} if bus else None,
            "by_hour": [round(by_hour[block].get(h, 0) / weekdays) for h in range(24)],
        }
        package = packages.get(f"[{block}]")
        if package:
            group = next((g for g in package["weekday_groups"] if g.get("service") != "recess" and 2 in g["weekdays"]),
                         package["weekday_groups"][0])
            codes = [c for _, c in group["stops"]]
            order = list(dict.fromkeys(codes))
            card["timestops"] = {
                "count": len(codes),
                "codes": order,
                "names": [TIMESTOP_NAMES.get(c, c) for c in order],
                "loops": max(codes.count(c) for c in order),
                "then": (group.get("out_of_service") or {}).get("then"),
            }
        cards.append(card)

    routes = get_json("GetRoutesForMapWithScheduleWithEncodedLine")
    shapes = {}
    for fam, route_id in FAMILY_SHAPE_ROUTE.items():
        route = next((r for r in routes if r.get("RouteID") == route_id), None)
        if route:
            shapes[fam] = route.get("EncodedPolyline") or ""

    data = {
        "generated": datetime.date.today().isoformat(),
        "schedule_week": monday.isoformat(),
        "ridership_from": min(days).isoformat() if days else None,
        "ridership_to": max(days).isoformat() if days else None,
        "ridership_days": len(days),
        "cards": cards,
        "shapes": shapes,
    }
    html = PAGE.read_text(encoding="utf-8")
    head, rest = html.split(MARK_START, 1)
    _, tail = rest.split(MARK_END, 1)
    PAGE.write_text(head + MARK_START + json.dumps(data, separators=(",", ":")) + MARK_END + tail, encoding="utf-8")
    print(f"{len(cards)} cards, ridership {data['ridership_from']}..{data['ridership_to']} ({len(days)} days)")
    for c in cards:
        w = c["rows"].get("wkd") or {}
        print(c["block"], c["family"], "wkd", w.get("hours"), "h", w.get("riders"), "riders", w.get("miles"), "mi",
              "| sat", (c["rows"].get("sat") or {}).get("riders"), "| sun", (c["rows"].get("sun") or {}).get("riders"),
              "| best", c["best_day"], "| bus", c["usual_bus"], "| stop", c["top_stop"])


if __name__ == "__main__":
    main()
