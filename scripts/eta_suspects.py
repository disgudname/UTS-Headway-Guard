"""Which bus a health-check breach comes from, and what that bus was doing.

Used by eta_health_check.py (see HANDOFF.md section 7). A breach used to be reported as "ENGINE" whenever it wasn't
a slow-bus pattern, and most of the time a look at the watch log showed it was ONE bus doing something nobody could
predict: a new driver's odd pull-out (2026-10-14 05:00, [09]), a bus leaving a timestop before sheet time (10-13
08:30), a bus skipping part of the route (10-09 12:30). find_suspects() does that look for every run:

  * groups the breach's rows by bus (late predictions, full-lap flips, visits TransLoc predicted that we didn't),
  * re-scores the run without the top bus (would it have passed?),
  * compares with TransLoc on the same predictions (equally wrong = the bus did something unpredictable),
  * and reads the bus's own track around the bad stretch for the known kinds of odd behaviour (tags below).

Read-only, offline: works from a run's rows, polls and saved graph only (plus uts_blocks for sheet times).
"""
import datetime
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import eta_compare  # noqa: E402

try:
    import uts_blocks  # noqa: E402
    uts_blocks._load()
except Exception:  # sheet times are a nice-to-have here
    uts_blocks = None

LATE_MISS_S = 120
MIN_GROUP_ROWS = 15          # a bus with fewer bad rows than this is not a suspect
MIN_GROUP_SHARE = 0.25       # ...nor one with less than this share of the breach's rows
LOOK_BACK_S = 300            # how far before the bad stretch to read the bus's track
LOOK_AHEAD_S = 120           # ...and after it (a GPS freeze only shows once the bus jumps back)
JOINED_RECENTLY_S = 300      # bad rows starting this soon after a bus joined the route = pull-out / route change
PARKED_RADIUS_M = 20.0
PARKED_MIN_S = 90.0
OFF_ROUTE_M = 80.0
BACKWARDS_M = 200.0
FROZEN_MIN_S = 60.0
FROZEN_JUMP_M = 250.0
EARLY_LEAVE_S = 20.0         # left a timestop at least this long before its sheet time
TL_OFF_S = 90.0              # TransLoc's median error on the same rows beyond this = TransLoc was wrong too
TL_OK_S = 60.0
STOP_ZONE_M = 40.0


def _hm(t):
    return datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S")


def _nearest_stop(stops, route, lat, lon):
    best = min(((eta_compare.haversine(lat, lon, s[0], s[1]), s[2], sid) for (r, sid), s in stops.items() if r == route),
               default=None)
    return best  # (metres, name, stop id) or None


def _where(stops, route, lat, lon):
    """'<stop> (N m)': the route's own nearest stop, or any route's when the bus is not near one of its own."""
    own = _nearest_stop(stops, route, lat, lon)
    if own and own[0] <= STOP_ZONE_M:
        return f"{own[1]} ({own[0]:.0f} m)"
    anyr = min(((eta_compare.haversine(lat, lon, v[0], v[1]), v[2]) for v in stops.values()), default=None)
    if anyr and (own is None or anyr[0] < own[0]):
        return f"{anyr[1]} ({anyr[0]:.0f} m)"
    return f"{own[1]} ({own[0]:.0f} m)" if own else "?"


def _dist_to_route(lat, lon, poly):
    best = 1e9
    for a, b in zip(poly, poly[1:]):
        bx, by = eta_compare.to_xy(b[0], b[1], a[0], a[1])
        px, py = eta_compare.to_xy(lat, lon, a[0], a[1])
        vv = bx * bx + by * by
        t = 0.0 if vv == 0 else max(0.0, min(1.0, (px * bx + py * by) / vv))
        best = min(best, math.hypot(px - t * bx, py - t * by))
    return best


def bus_tags(veh, route, t0, t1, polls, stops, lines, block=None, unwrapped=None):
    """Plain-words notes on what the bus did from LOOK_BACK_S before t0 to t1."""
    track = [(p["t"], x[1], x[2], x[3], x[4]) for p in polls for x in p["veh"] if x[0] == veh]
    if not track:
        return []
    tags = []
    on_route = [x for x in track if x[1] == route]
    log_start = polls[0]["t"]

    # Pull-out / route change: the bad stretch starts soon after the bus showed up on this route.
    if on_route:
        joined = on_route[0][0]
        before = [x for x in track if x[0] < joined]
        if joined > log_start + 30 and t0 - joined <= JOINED_RECENTLY_S:
            prev = before[-1][1] if before else None
            what = "pulling out (not on a route before)" if prev in (None, "0") else f"just moved over from route {prev}"
            tags.append(f"bad stretch starts {t0 - joined:.0f} s after it joined the route at {_hm(joined)}, {what}")

    win = [x for x in track if t0 - LOOK_BACK_S <= x[0] <= t1 + LOOK_AHEAD_S]
    poly = lines[route][0] if route in lines else None

    # GPS froze: the same fix for a while, then a jump. Seen most at the Emmet St / McCormick Rd corner on weekday
    # daytime (a cell dead spot, HANDOFF.md 2026-10-13): the bus kept driving, so this is not a stop.
    frozen = []
    i = 0
    while i < len(win) - 1:
        j = i
        while j + 1 < len(win) and (win[j + 1][2], win[j + 1][3]) == (win[i][2], win[i][3]):
            j += 1
        dur = win[j][0] - win[i][0]
        if dur >= FROZEN_MIN_S and j + 1 < len(win):
            jump = eta_compare.haversine(win[i][2], win[i][3], win[j + 1][2], win[j + 1][3])
            if jump >= FROZEN_JUMP_M:
                frozen.append((win[i][0], win[j][0]))
                tags.append(f"GPS froze {dur:.0f} s at {_where(stops, route, win[i][2], win[i][3])} "
                            f"({_hm(win[i][0])}-{_hm(win[j][0])}), then jumped {jump:.0f} m: it was moving, the "
                            "position wasn't updating")
        i = j + 1

    # Sat still somewhere.
    early_noted = set()  # (stop id, sheet epoch) the sat-still notes already called early
    i = 0
    while i < len(win):
        j = i
        while j + 1 < len(win) and eta_compare.haversine(win[i][2], win[i][3], win[j + 1][2], win[j + 1][3]) <= PARKED_RADIUS_M:
            j += 1
        dur = win[j][0] - win[i][0]
        in_freeze = any(a <= win[i][0] + 30 and win[j][0] - 30 <= b for a, b in frozen)
        if dur >= PARKED_MIN_S and not in_freeze:
            ns = _nearest_stop(stops, route, win[i][2], win[i][3])
            note = f"sat still {dur:.0f} s at {_where(stops, route, win[i][2], win[i][3])}, {_hm(win[i][0])}-{_hm(win[j][0])}"
            left = win[j + 1][0] if j + 1 < len(win) else None
            if ns and ns[0] <= STOP_ZONE_M and block and uts_blocks is not None and left is not None:
                sheet = uts_blocks.scheduled_hold_epoch(route, ns[2], block, win[i][0])
                if sheet is not None and sheet > win[i][0] - 60:
                    if left < sheet - EARLY_LEAVE_S:
                        note += f"; left {sheet - left:.0f} s BEFORE its sheet time {_hm(sheet)[:5]}"
                        early_noted.add((ns[2], sheet))
                    elif left < sheet:
                        note += f"; left {sheet - left:.0f} s before its sheet time {_hm(sheet)[:5]}"
                    else:
                        note += f"; left {left - sheet:.0f} s after its sheet time {_hm(sheet)[:5]}"
            elif ns and ns[0] > STOP_ZONE_M:
                note += " (not at one of this route's stops)"
            tags.append(note)
        i = j + 1

    # Left a timestop before its sheet time without sitting still long enough for the note above: a short dwell
    # or a drive-through (10-21 19:40, [10] at the Chapel after 85 s and Barracks after 40 s). The route position
    # passing the stop finds the visit; it left at the first fix after that more than STOP_ZONE_M from the stop
    # (the pass itself can come at arrival: a bus stops just beyond the stop's point on the line).
    if block and uts_blocks is not None and route in lines:
        timestops = [sid for (r, sid) in stops if r == route and uts_blocks.timestop_code_for_stop(route, sid)]
        passes = eta_compare.find_crossings({}, stops, lines, {(route, sid, veh) for sid in timestops},
                                            unwrapped or {})
        for (_r, sid, _v), times in sorted(passes.items(), key=lambda kv: kv[1][0]):
            lat, lon = stops[(route, sid)][:2]
            for passed in times:
                left = next((x[0] for x in on_route if x[0] >= passed
                             and eta_compare.haversine(x[2], x[3], lat, lon) > STOP_ZONE_M), None)
                if left is None or not (t0 - LOOK_BACK_S <= left <= t1 + LOOK_AHEAD_S):
                    continue
                sheet = uts_blocks.scheduled_hold_epoch(route, sid, block, left)
                if sheet is None or left >= sheet - EARLY_LEAVE_S or (sid, sheet) in early_noted:
                    continue
                tags.append(f"left timestop {stops[(route, sid)][2]} at {_hm(left)}, {sheet - left:.0f} s BEFORE "
                            f"its sheet time {_hm(sheet)[:5]}")

    # Off the route (detour, skipped part of the route, a lot).
    if poly:
        off = [(x[0], _dist_to_route(x[2], x[3], poly), x) for x in win if x[1] == route]
        far = [o for o in off if o[1] >= OFF_ROUTE_M]
        if len(far) >= 2:
            worst = max(far, key=lambda o: o[1])
            tags.append(f"off the route {len(far)} polls, up to {worst[1]:.0f} m away near "
                        f"{_where(stops, route, worst[2][2], worst[2][3])}, "
                        f"{_hm(far[0][0])}-{_hm(far[-1][0])}")

    # Drove against the route's direction.
    u = (unwrapped or {}).get((veh, route))
    if u:
        seg = [(t, x) for t, x in u if t0 - LOOK_BACK_S <= t <= t1]
        peak = None
        for k, (t, x) in enumerate(seg):
            if peak is None or x > peak[1]:
                peak = (t, x)
            if peak[1] - x >= BACKWARDS_M:
                pos = next((p for p in win if p[0] >= t), win[-1])
                ns = _nearest_stop(stops, route, pos[2], pos[3])
                tags.append(f"drove {peak[1] - x:.0f} m against the route's direction near {ns[1] if ns else '?'} "
                            f"by {_hm(t)}")
                break

    return tags


# TransLoc-only rows this share of which are visits after the bus's block leaves the route by its own timetable (its
# last public trip, or its evening route change) are the block doing what the sheet says: our blank there is the
# point of out_of_service_plan / route_change_plan, and TransLoc is predicting a lap the bus won't run in service.
# 2026-10-22 21:45, 12532 [01]: Green Loop ends leaving the Chapel at 22:00, out of service after Madison Ave @
# Preston Ave, then Night Pilot; TransLoc kept predicting the stops past it and the run called it ONE BUS.
PLANNED_MIN_SHARE = 0.8


def _planned_end(route, block, rows, stops):
    """A plain note when most of these TransLoc-only rows come after the block leaves this route by its timetable,
    else None."""
    if not block or uts_blocks is None or not rows:
        return None
    t0 = min(r["t"] for r in rows)
    oos = uts_blocks.out_of_service_plan(route, block, t0)
    change = None if oos else uts_blocks.route_change_plan(route, block, t0)
    if oos:
        leave_stop, at = oos[0], oos[1]
    elif change:
        leave_stop, at = change[0], change[1]
    else:
        return None
    after = sum(1 for r in rows if r["t"] + r.get("remaining", 0.0) >= at - 60)
    if after < PLANNED_MIN_SHARE * len(rows):
        return None
    def name(sid):
        return (stops.get((route, sid)) or (None, None, f"stop {sid}"))[2]
    what = f"out of service after {name(oos[2])}" if oos else "then its evening route"
    return (f"its timetable takes it off this route: leaves {name(leave_stop)} at {_hm(at)[:5]}, {what}; "
            f"{after} of these are visits after that, which we leave blank on purpose")


def _tl_note(rows):
    tl = [r["tl"] for r in rows if r.get("tl") is not None]
    if len(tl) < 5:
        return None, "TransLoc had no predictions for these"
    med = statistics.median(tl)
    if abs(med) >= TL_OFF_S:
        return True, f"TransLoc was off too (median {med:+.0f} s on the same predictions)"
    if abs(med) <= TL_OK_S:
        return False, f"TransLoc had these right (median {med:+.0f} s)"
    return None, f"TransLoc was in between (median {med:+.0f} s)"


def _group(rows, key=lambda r: (r["key"][0], r["key"][2])):
    g = defaultdict(list)
    for r in rows:
        g[key(r)].append(r)
    return sorted(g.items(), key=lambda kv: -len(kv[1]))


def find_suspects(rows, polls, stops, lines, names, bus_names=None, late_limit_pct=3.0,
                  skip_route_prefixes=("Purple Line",)):
    """[{kind, bus, vehicle_id, block, route, window, rows, share, without_pct, transloc, tags, verdict, text}]."""
    bus_names = bus_names or {}
    blocks = {}
    for p in polls:
        for x in p["ours"]:
            if len(x) > 5 and x[5]:
                blocks.setdefault((x[2], x[0]), str(x[5]).split()[0])
    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _mps in p["veh"]:
            tracks.setdefault((veh, route), []).append((p["t"], lat, lon))
    unwrapped = eta_compare.build_unwrapped(tracks, lines)

    def shuttle(rid):
        # Game-day lot shuttles ("Blue Lots Shuttle", "Post-Game Fan Shuttle"): never scored (HANDOFF.md section 7).
        return "shuttle" in names.get(rid, rid).lower()

    def skip(rid):
        return names.get(rid, rid).startswith(skip_route_prefixes) or shuttle(rid)

    scored = [r for r in rows if r["ours"] is not None and not skip(r["key"][0])]
    late = [r for r in scored if r["ours"] > LATE_MISS_S]
    flips = [r for r in rows if r["ours"] is not None and r["tl"] is not None
             and abs(r["ours"]) > 1200 and abs(r["tl"]) < 120 and not skip(r["key"][0])]
    first_t = polls[min(1, len(polls) - 1)]["t"] if polls else 0
    transloc_only = [r for r in rows if r["ours"] is None and r["tl"] is not None and r["t"] > first_t
                     and not shuttle(r["key"][0])]
    out = []

    def add(kind, group_rows, total, extra=None):
        (route, veh) = (group_rows[0]["key"][0], group_rows[0]["key"][2])
        if len(group_rows) < MIN_GROUP_ROWS and kind == "late":
            return
        share = len(group_rows) / max(1, total)
        if kind == "late" and share < MIN_GROUP_SHARE:
            return
        t0, t1 = min(r["t"] for r in group_rows), max(r["t"] for r in group_rows)
        block = blocks.get((veh, route))
        tags = bus_tags(veh, route, t0, t1, polls, stops, lines, block, unwrapped)
        tl_off, tl_text = _tl_note(group_rows)
        d = {
            "kind": kind, "bus": bus_names.get(veh, f"vehicle {veh}"), "vehicle_id": veh, "block": block,
            "route": names.get(route, route), "route_id": route, "tl_off": tl_off, "window": f"{_hm(t0)}-{_hm(t1)}", "rows": len(group_rows),
            "share_pct": round(100 * share), "transloc": tl_text, "tags": tags,
        }
        if extra:
            d.update(extra)
        explained = bool(tags) or tl_off is True
        passes = d.get("without_pct") is None or d["without_pct"] <= late_limit_pct
        if explained and passes:
            d["verdict"] = "ONE BUS"
        elif passes and tl_off is False:
            d["verdict"] = "ENGINE, ONE BUS"  # one bus, TransLoc right, nothing odd seen: worth a real look
        elif passes:
            d["verdict"] = "ONE BUS?"
        else:
            d["verdict"] = "SEVERAL BUSES"
        what = "; ".join(tags) if tags else "nothing unusual in its track"
        d["text"] = (f"{d['verdict']}: {d['bus']} {block or ''} on {d['route']}, {d['window']}, "
                     f"{len(group_rows)} {kind} rows ({d['share_pct']}%)"
                     + (f", run would be {d['without_pct']}% without it" if d.get("without_pct") is not None else "")
                     + f". {what}. {tl_text}.")
        out.append(d)

    if scored and late and 100.0 * len(late) / len(scored) > late_limit_pct:
        for (route, veh), g in _group(late)[:3]:
            rest = [r for r in scored if (r["key"][0], r["key"][2]) != (route, veh)]
            add("late", g, len(late), {"without_pct": _late_pct(rest)})
        explained = {(d["vehicle_id"], k) for d in out for k in [d["route_id"]] if d["tags"] or d["tl_off"]}
        if len(explained) > 1:
            rest = [r for r in scored if (r["key"][2], r["key"][0]) not in explained]
            pct = _late_pct(rest)
            if pct <= late_limit_pct:
                for d in out:
                    if (d["vehicle_id"], d["route_id"]) in explained and d["verdict"] == "SEVERAL BUSES":
                        d["verdict"] = "ONE OF A FEW BUSES"
                        d["text"] = d["verdict"] + d["text"][d["text"].index(":"):]
                out.append({"kind": "late", "verdict": "A FEW BUSES", "without_pct": pct,
                            "text": f"A FEW BUSES: without the {len(explained)} buses above the run would be {pct}%."})
    with_shuttles = [r for r in rows if r["ours"] is not None and not names.get(r["key"][0], r["key"][0]).startswith(skip_route_prefixes)]
    if any(shuttle(r["key"][0]) for r in with_shuttles) and _late_pct(with_shuttles) > late_limit_pct >= _late_pct(scored):
        out.append({"kind": "late", "verdict": "LOT SHUTTLES", "text":
                    f"LOT SHUTTLES: without the game-day lot shuttles the run is {_late_pct(scored)}% late. They don't count."})
    purple_flips = [r for r in rows if r["ours"] is not None and r["tl"] is not None
                    and abs(r["ours"]) > 1200 and abs(r["tl"]) < 120 and skip(r["key"][0])]
    if purple_flips and not flips:
        at_stop = sum(1 for r in purple_flips if (r.get("ours_s") or 99) <= 30 and abs(r.get("past_m") or 999) <= 50)
        out.append({"kind": "full-lap flip", "verdict": "PURPLE", "text":
                    f"PURPLE: all {len(purple_flips)} full-lap flip rows were on Purple or a lot shuttle"
                    + (f"; {at_stop} were us saying Due with the bus at the stop (right), the scorer counting its next lap"
                       if at_stop else "") + ". Not the engine."})
    for (route, veh), g in _group(flips)[:3]:
        same = next((d for d in out if d.get("vehicle_id") == veh and d.get("route_id") == route), None)
        if same:
            same["text"] += f" Also {len(g)} full-lap flip rows at {_hm(g[0]['t'])}."
            same.setdefault("also", []).append("full-lap flip")
        else:
            add("full-lap flip", g, len(flips))
    tl_groups = _group(transloc_only)
    if not transloc_only and any(r["ours"] is None and r["tl"] is not None and shuttle(r["key"][0]) for r in rows):
        out.append({"kind": "TransLoc-only", "verdict": "LOT SHUTTLES", "text":
                    "LOT SHUTTLES: every visit TransLoc predicted and we didn't was a game-day lot shuttle. They don't count."})
    if transloc_only and len(tl_groups[0][1]) < MIN_GROUP_ROWS:
        out.append({"kind": "TransLoc-only", "verdict": "SMALL", "text":
                    f"SMALL: the visits TransLoc predicted and we didn't are spread thin ({len(transloc_only)} rows "
                    f"over {len(tl_groups)} buses, at most {len(tl_groups[0][1])} on one)."})
    for (route, veh), g in tl_groups[:3]:
        if len(g) < MIN_GROUP_ROWS:
            break
        same = next((d for d in out if d.get("vehicle_id") == veh and d.get("route_id") == route), None)
        if same:
            same["text"] += f" Also {len(g)} visits TransLoc predicted and we didn't."
            same.setdefault("also", []).append("TransLoc-only")
        else:
            add("TransLoc-only", g, len(transloc_only))
            d = out[-1]
            planned = _planned_end(route, d["block"], g, stops)
            if planned:
                d["verdict"] = "PLANNED"
                d["tags"].insert(0, planned)
                d["text"] = (f"PLANNED: {d['bus']} {d['block']} on {d['route']}, {d['window']}, {len(g)} TransLoc-only "
                             f"rows ({d['share_pct']}%). {'; '.join(d['tags'])}. {d['transloc']}.")
    return out


def _late_pct(rows):
    return round(100.0 * sum(1 for r in rows if r["ours"] > LATE_MISS_S) / max(1, len(rows)), 1)
