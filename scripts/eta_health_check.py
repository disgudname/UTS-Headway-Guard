"""One unattended ETA health check: watch prod, score it, append a summary line, flag problems.

    python scripts/eta_health_check.py [minutes=30] [poll_seconds=15]

Runs the same logging + scoring as eta_watch.py / eta_compare.py (see HANDOFF.md section 7), then
appends ONE JSON line to data-local/eta_watch/health_results.jsonl with the headline numbers and any
threshold breaches. Exit code is 0 for a clean run, 2 if a threshold was breached (so a scheduler or
wrapper can react), 1 if the check itself couldn't run. Read-only: public GET requests only.
"""
import datetime
import json
import re
import statistics
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eta_compare  # noqa: E402
import eta_suspects  # noqa: E402
import eta_watch  # noqa: E402

OUT_DIR = Path("data-local/eta_watch")
RESULTS = OUT_DIR / "health_results.jsonl"

# Starting thresholds from HANDOFF.md section 7 -- adjust once weekday data exists.
LATE_MISS_S = 120
MAX_LATE_MISS_PCT = 3.0
MAX_ROUTE_MEDIAN_ABS_S = 90
MAX_ROUTE_LATE_BIAS_S = 60
FULL_LAP_OFF_S = 1200
FULL_LAP_TL_OK_S = 120
# A "Due" (<= DUE_S) shown while the bus is 0..JUST_PASSED_M metres PAST the stop is the display lagging a few
# seconds behind a bus that just went by, not a full-lap failure -- but the scorer sees the bus's next crossing a lap
# away and calls it one. Seen 2026-09-20 01:30 (Night Pilot, 6 "flips": all "Due" with the bus 24-216 m past the stop,
# 3-40 s after it passed). These are counted separately (flips_just_passed) and never raise a breach.
DUE_S = 30
JUST_PASSED_M = 300
# Share of estimates that used real history for EVERY hop. Two tiers, because they mean different things:
#   below BREACH -> the hop-time table has probably emptied again (the original bug measured ~1%): a real breach.
#   below NOTE   -> thin history, informational only. Weekend routes 54/55/57 are new this semester, so weekend
#                   daytime runs sit at ~10-15% (07:30 Sun 11.8%, 09:49 Sun 14.2%) while weekday-evening/night runs
#                   are 50-90%. It should rise on its own as weekend days accumulate -- revisit when it does.
MIN_HISTORICAL_PCT_BREACH = 5.0
MIN_HISTORICAL_PCT_NOTE = 30.0
MIN_ROUTE_ROWS = 20         # don't judge a route on a handful of predictions
MIN_ROWS = 50               # too little data to judge anything
# "Visits TransLoc predicted that we didn't" had two noise sources (2026-09-23, weekday runs): the first polls of a run,
# before our own feed has anything for the buses (45 of one run's 66), and a bus whose whole prediction set drops out for
# exactly one 15 s poll as it pulls off a stop/layover and then comes back unchanged. Neither is a rider-visible gap
# worth a breach; both are still counted (only_transloc_ignored) so they stay visible. Gaps of 2+ polls still count.
WARMUP_POLLS = 2
# Purple (Mon-Fri commuter route) stages buses at different stops by time of day, for hold lengths that change every
# day and can't be predicted (user, 2026-09-26; drivers are told to ignore TransLoc's anti-bunching guidance too). Its
# typical error across all horizons sat at 110-150 s on every weekday run, so the all-horizon median |error| limit only
# produced noise. It is judged instead on what riders can use: predictions made under NEAR_TERM_S out (same 90 s limit),
# and its own too-late share, kept out of the global one it was dragging over 3% on its own (routes 73/74 ran ~6-10%
# >2 min late in the 2026-09-24/25 replay; 12% flags a real regression).
NEAR_TERM_ONLY_ROUTE_PREFIXES = ("Purple Line",)
NEAR_TERM_S = 300
NEAR_TERM_MAX_LATE_MISS_PCT = 12.0
# Telling "the buses ran slow" from "the engine got it wrong" (diagnose(), 2026-10-09). A bus that is simply slower than
# its history shows as an error that is small close in and grows the further out the prediction was made (10-08 17:00
# Gold: -14 s under 3 min out, -174 s at 10-20 min). An ETA that is already off under CLOSE_IN_S is not pace, it is the
# engine (position, hold, timetable). Whether slow is TODAY or every day comes from the same slot's earlier runs: the
# engine's history is summarised below the median on purpose (10-02), so some lean toward "bus after our ETA" is normal.
CLOSE_IN_S = 180
# "On target" windows (the MBTA's bus prediction accuracy windows, the nearest thing to an industry standard, 2026-10-17):
# (shown up to, may come this early, may come this late), seconds. A prediction counts if the bus came within its
# window; predictions shown 15+ min out aren't judged. Error here is the scorer's: positive = the bus left before the
# time shown, so "early" is the positive side.
ON_TARGET_WINDOWS = ((180, 60, 60), (360, 90, 120), (600, 150, 210), (900, 240, 360))
CLOSE_IN_OK_S = 45
SLOWER_THAN_USUAL_S = 45
USUAL_RUNS = 10
MIN_USUAL_RUNS = 3


def watch(minutes, every, log):
    log.parent.mkdir(parents=True, exist_ok=True)
    end = time.time() + minutes * 60
    seen_routes = set()
    with log.open("a", encoding="utf-8") as f:
        while time.time() < end:
            started = time.time()
            rec = eta_watch.poll()
            seen_routes.update(v[1] for v in rec["veh"])
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            f.flush()
            time.sleep(max(0.0, every - (time.time() - started)))
    return seen_routes


def service_level():
    """Today's service level from prod's log ("full", "exam", "recess", "summer", "none"), or None if it can't be read."""
    try:
        return (eta_watch.fetch("/v1/service-levels?days=1").get("today") or {}).get("level")
    except Exception:
        pass
    try:  # a prod build from before /v1/service-levels: read the calendar's own wording
        label = ((eta_watch.fetch("/v1/service-schedule?days=1").get("today") or {}).get("services") or {}).get("UVA Transit")
        label = str(label or "").lower()
        return next((lvl for word, lvl in (("summer", "summer"), ("exam", "exam"), ("recess", "recess"),
                                           ("no service", "none"), ("full", "full")) if word in label), None)
    except Exception:
        return None


def service_calendar(now):
    """The public calendar's row for the SERVICE day this run falls in: {"service_date", "services", "notes"}, or None.
    A service day runs to 04:00, so the 00:00 and 01:30 runs read the day before (a Saturday with Night Pilot
    "No Service" is why Sunday's 00:00 run is empty). `notes` is where a game day says so
    ("Home Football Game Route Service Ends at 4PM")."""
    try:
        day = (now - datetime.timedelta(hours=4)).date().isoformat()
        for d in eta_watch.fetch(f"/v1/service-schedule?start={day}&days=1").get("days") or []:
            if d.get("date") == day:
                return {"service_date": day, "services": d.get("services") or {}, "notes": d.get("notes") or ""}
    except Exception:
        pass
    return None


def is_game_day(summary):
    return "football" in str((summary.get("calendar") or {}).get("notes") or "").lower()


def service_notices(names):
    """Public system notices active right now (detours, closed stops, delays), trimmed to what the verdict needs, or
    None if they can't be read. Staff-only notices are not visible here (public GET). A route-scoped notice gets its
    route names from `names` (TransLoc route id -> name); a stop-scoped one keeps its stop ids."""
    try:
        notices = eta_watch.fetch("/v1/system-notices").get("notices") or []
    except Exception:
        return None
    out = []
    for n in notices:
        scope = n.get("target_scope") or "all"
        d = {"text": n.get("message") or n.get("short_message") or "", "severity": n.get("severity"), "scope": scope}
        if scope == "route":
            d["routes"] = [names.get(str(r), str(r)) for r in n.get("target_route_ids") or []]
        elif scope == "stops":
            d["stop_ids"] = [str(s) for s in n.get("target_stop_ids") or []]
        for k in ("start_time", "end_time"):
            if n.get(k):
                d[k] = n[k]
        out.append(d)
    return out


TRANSLOC_ALERTS_URL = "https://uva.transloc.com/Services/JSONPRelay.svc/GetTwitterJSON"  # the rider site's Alerts bell
ROUTE_WORDS = ("Gold", "Green", "Orange", "Silver", "Purple", "Night Pilot")


def transloc_alerts(names, now):
    """TransLoc's rider-facing service alerts active right now, in the same shape as service_notices(), or None if
    they can't be read. They carry no targeting, so the routes are the ones the text names ("Gold and Green Line");
    an alert naming none counts as system-wide."""
    try:
        req = urllib.request.Request(TRANSLOC_ALERTS_URL, headers={"User-Agent": "eta-watch"})
        with urllib.request.urlopen(req, timeout=20) as r:
            alerts = json.load(r) or []
    except Exception:
        return None
    out = []
    for a in alerts:
        try:
            start = datetime.datetime.strptime(a["created_at"], "%a %b %d %H:%M:%S %z %Y")
            end = datetime.datetime.strptime(a["end_date"], "%a %b %d %H:%M:%S %z %Y") if a.get("end_date") else None
        except (KeyError, ValueError, TypeError):
            start = end = None
        if (start and now < start) or (end and now > end):
            continue
        text = " ".join(str(a.get("text") or "").split())
        words = [w for w in ROUTE_WORDS if re.search(rf"\b{w}\b", text, re.I)]
        routes = sorted({n.split(" — ")[0].strip() for n in names.values() for w in words if n.lower().startswith(w.lower())}
                        | {f"{w} Line" if w != "Night Pilot" else w for w in words})
        d = {"source": "transloc", "text": text, "scope": "route" if routes else "all"}
        if routes:
            d["routes"] = routes
        if start:
            d["start_time"] = start.isoformat()
        if end:
            d["end_time"] = end.isoformat()
        out.append(d)
    return out


def notices_for_route(summary, name):
    """Every active notice that could touch this route: system-wide, aimed at it, or aimed at some stops (a stop
    notice can't be tied to a route from here, so it counts for every route)."""
    base = name.split(" — ")[0].strip()
    return [n for n in summary.get("notices") or []
            if n["scope"] != "route" or any(base == r.split(" — ")[0].strip() for r in n.get("routes", []))]


def route_names():
    try:
        return {str(l["id"]): l["name"] for l in eta_watch.fetch("/v1/trip-planner/uts-graph")["lines"]}
    except Exception:
        return {}


def graph_names(log):
    """Route names from the graph saved next to a log (eta_compare.load_graph), for routes the live list lacks:
    a route that stopped running before the run ended, or a re-score of an old log. Without a name, a
    NEAR_TERM_ONLY route would silently be judged by the all-horizon rules."""
    try:
        g = json.loads(Path(str(log) + ".graph.json").read_text(encoding="utf-8"))
        return {str(l["id"]): l["name"] for l in g["lines"]}
    except Exception:
        return {}


def transloc_only_gaps(rows, warmup_until=None):
    """(persistent, warmup, single_poll, gaps): rows where TransLoc predicted a visit and we didn't, split into
    the ones that are worth a breach and the two known noise kinds (see WARMUP_POLLS). A row is `warmup` if its
    poll is at or before `warmup_until`; `single_poll` if it is a one-poll hole: we DID predict that same (route, stop,
    vehicle) at the next poll, and at the previous poll either we did too or TransLoc had nothing for that vehicle on
    that route yet (a bus's first poll on a new route: TransLoc's arrivals feed moves it a poll before its vehicle feed,
    which ours follows -- seen at the 2026-09-25 evening route change, Gold buses 18 and 39). `gaps` is how many
    separate (route, vehicle) stretches of consecutive polls the persistent rows make up: one bus missing for 45 s
    is one gap however many stops it covers."""
    times = sorted({r["t"] for r in rows})
    idx = {t: i for i, t in enumerate(times)}
    present = {}
    tl_on_route = {}
    for r in rows:
        if r["ours"] is not None:
            present.setdefault(r["key"], set()).add(idx[r["t"]])
        if r["tl"] is not None:
            tl_on_route.setdefault((r["key"][0], r["key"][2]), set()).add(idx[r["t"]])
    persistent = warmup = single = 0
    missing = {}
    for r in rows:
        if r["ours"] is not None or r["tl"] is None:
            continue
        i = idx[r["t"]]
        have = present.get(r["key"], ())
        route_new = (i - 1) not in tl_on_route.get((r["key"][0], r["key"][2]), ())
        if warmup_until is not None and r["t"] <= warmup_until:
            warmup += 1
        elif (i + 1) in have and ((i - 1) in have or route_new):
            single += 1
        else:
            persistent += 1
            missing.setdefault((r["key"][0], r["key"][2]), set()).add(i)
    gaps = sum(1 for polls in missing.values() for i in polls if (i - 1) not in polls)
    return persistent, warmup, single, gaps


def on_target(pairs):
    """% of (shown seconds, error) pairs inside ON_TARGET_WINDOWS, or None if none were judgeable."""
    hit = n = 0
    for shown, err in pairs:
        if shown is None or err is None:
            continue
        w = next(((early, late) for upto, early, late in ON_TARGET_WINDOWS if shown < upto), None)
        if w is None:
            continue
        n += 1
        hit += -w[1] <= err <= w[0]
    return round(100.0 * hit / n, 1) if n else None


def analyze(rows, names, warmup_until=None):
    both = [r for r in rows if r["ours"] is not None and r["tl"] is not None]
    ours = [r["ours"] for r in rows if r["ours"] is not None]
    out = {"scored": len(rows), "both": len(both)}
    if not ours:
        out["inconclusive"] = "no scoreable predictions (buses not running, or log too short)"
        return out, []
    late = 100.0 * sum(1 for e in ours if e > LATE_MISS_S) / len(ours)
    out.update(
        median_err_s=round(statistics.median(ours)),
        median_abs_err_s=round(statistics.median(abs(e) for e in ours)),
        late_over_2min_pct=round(late, 1),
    )
    judged = [r for r in both if "shuttle" not in names.get(r["key"][0], "").lower()]
    if judged:
        out["on_target_pct"] = on_target((r["ours_s"], r["ours"]) for r in judged)
        out["tl_on_target_pct"] = on_target((r["tl_s"], r["tl"]) for r in judged)
    tl = [r["tl"] for r in both]
    if tl:
        out["tl_median_err_s"] = round(statistics.median(tl))
        out["tl_late_over_2min_pct"] = round(100.0 * sum(1 for e in tl if e > LATE_MISS_S) / len(tl), 1)
    persistent, warmup, single, gaps = transloc_only_gaps(rows, warmup_until)
    out["only_transloc"] = persistent
    if persistent:
        out["only_transloc_gaps"] = gaps
    if warmup or single:
        out["only_transloc_ignored"] = {"warmup": warmup, "single_poll": single}

    srcs = [r["source"] for r in rows if r["ours"] is not None and r["source"]]
    if srcs:
        out["historical_pct"] = round(100.0 * srcs.count("historical") / len(srcs), 1)

    by_route = defaultdict(list)
    for r in rows:
        if r["ours"] is not None:
            by_route[r["key"][0]].append(r["ours"])
    out["routes"] = {
        names.get(rid, rid): {
            "n": len(e),
            "median_abs_err_s": round(statistics.median(abs(x) for x in e)),
            "median_err_s": round(statistics.median(e)),
        }
        for rid, e in by_route.items()
    }
    for rid in by_route:
        pct = on_target((r["ours_s"], r["ours"]) for r in rows if r["key"][0] == rid)
        if pct is not None:
            out["routes"][names.get(rid, rid)]["on_target_pct"] = pct
        close = [r["ours"] for r in rows if r["ours"] is not None and r["key"][0] == rid and r["remaining"] < CLOSE_IN_S]
        if len(close) >= MIN_ROUTE_ROWS:
            out["routes"][names.get(rid, rid)]["close_in_median_err_s"] = round(statistics.median(close))

    candidates = [r for r in both if abs(r["ours"]) > FULL_LAP_OFF_S and abs(r["tl"]) < FULL_LAP_TL_OK_S]

    def just_passed(r):
        past = r.get("past_m")
        return r.get("ours_s") is not None and r["ours_s"] <= DUE_S and past is not None and 0 < past <= JUST_PASSED_M

    flips = [r for r in candidates if not just_passed(r)]
    out["full_lap_flips"] = len(flips)
    out["flips_just_passed"] = len(candidates) - len(flips)

    problems = []
    if len(rows) < MIN_ROWS:
        out["inconclusive"] = f"only {len(rows)} scoreable predictions -- too little data to trust"
        return out, []

    def near_term_only(rid):
        return names.get(rid, rid).startswith(NEAR_TERM_ONLY_ROUTE_PREFIXES)

    rest = [r["ours"] for r in rows if r["ours"] is not None and not near_term_only(r["key"][0])]
    late_rest = 100.0 * sum(1 for e in rest if e > LATE_MISS_S) / len(rest) if rest else 0.0
    if rest and len(rest) != len(ours):
        out["late_over_2min_pct_excl_near_term_only"] = round(late_rest, 1)
    if late_rest > MAX_LATE_MISS_PCT:
        problems.append(f"{late_rest:.1f}% of predictions >2 min late (limit {MAX_LATE_MISS_PCT}%)")
    for rid, e in by_route.items():
        if len(e) < MIN_ROUTE_ROWS:
            continue
        name = names.get(rid, rid)
        if near_term_only(rid):
            near = [r["ours"] for r in rows if r["ours"] is not None and r["key"][0] == rid and r["remaining"] < NEAR_TERM_S]
            route_late = 100.0 * sum(1 for x in e if x > LATE_MISS_S) / len(e)
            out["routes"][name]["late_over_2min_pct"] = round(route_late, 1)
            if near:
                out["routes"][name]["near_term_median_abs_err_s"] = round(statistics.median(abs(x) for x in near))
            if len(near) >= MIN_ROUTE_ROWS and statistics.median(abs(x) for x in near) > MAX_ROUTE_MEDIAN_ABS_S:
                problems.append(f"{name}: median |error| under {NEAR_TERM_S // 60} min out "
                                f"{statistics.median(abs(x) for x in near):.0f}s (limit {MAX_ROUTE_MEDIAN_ABS_S}s)")
            if route_late > NEAR_TERM_MAX_LATE_MISS_PCT:
                problems.append(f"{name}: {route_late:.1f}% of predictions >2 min late (limit {NEAR_TERM_MAX_LATE_MISS_PCT}%)")
        elif statistics.median(abs(x) for x in e) > MAX_ROUTE_MEDIAN_ABS_S:
            problems.append(f"{name}: median |error| {statistics.median(abs(x) for x in e):.0f}s (limit {MAX_ROUTE_MEDIAN_ABS_S}s)")
        if statistics.median(e) > MAX_ROUTE_LATE_BIAS_S:
            problems.append(f"{name}: late bias {statistics.median(e):+.0f}s (limit +{MAX_ROUTE_LATE_BIAS_S}s)")
    if flips:
        problems.append(f"{len(flips)} full-lap flip(s): our ETA >20 min off while TransLoc was within 2 min")
    if out["only_transloc"]:
        problems.append(f"{out['only_transloc']} visit(s) TransLoc predicted that we didn't "
                        f"({out['only_transloc_gaps']} separate bus gap(s))")
    if "historical_pct" in out:
        pct = out["historical_pct"]
        if pct < MIN_HISTORICAL_PCT_BREACH:
            problems.append(f"only {pct}% of estimates use real history (hop-time table has probably emptied)")
        elif pct < MIN_HISTORICAL_PCT_NOTE:
            out["notes"] = [f"thin history: only {pct}% of estimates use real history for every hop (expected on weekend "
                            "daytime while the weekend routes' history builds up; not a problem by itself)"]
    return out, problems


def usual_route_errors(summary, earlier):
    """{route name: median of its median_err_s} over the last USUAL_RUNS earlier runs of the same slot: same start
    time and length, same kind of day (Mon-Fri together, Sat and Sun each alone), same service level (runs from
    before the level was logged, 2026-10-05, were all Full Service). A home football day is only compared with other
    football days (calendar notes, logged from 2026-10-09). Runs with a service alert active (logged from 2026-10-22)
    are left out: a detour day is not a usual day. Only our own notices count for that: a TransLoc alert is live from
    the day it is posted (10-19 for a 10-23 game), and when it applies is only in its text."""
    def slot(d):
        day = d.get("day")
        return (str(d.get("when", ""))[11:16], d.get("minutes"), day if day in ("Sat", "Sun") else "wk", d.get("service_level") or "full",
                is_game_day(d))

    seen = defaultdict(list)
    for d in earlier:
        if (slot(d) == slot(summary) and d.get("when") != summary.get("when")
                and not any(n.get("source") != "transloc" for n in d.get("notices") or [])):
            for name, r in (d.get("routes") or {}).items():
                if r.get("n", 0) >= MIN_ROUTE_ROWS:
                    seen[name].append(r["median_err_s"])
    return {name: round(statistics.median(v[-USUAL_RUNS:])) for name, v in seen.items() if len(v) >= MIN_USUAL_RUNS}


def diagnose(summary, earlier=()):
    """One plain line per thing that is off, each starting with whose fault it is:
    BUSES SLOW (slower than usual today: road or dwell), ENGINE LEAN (the same gap most days in this slot: our history
    is too fast there) or ENGINE (wrong for a reason that is not the bus's pace). See CLOSE_IN_S above."""
    usual = usual_route_errors(summary, earlier)
    out = []
    for name, r in (summary.get("routes") or {}).items():
        med = r["median_err_s"]
        if r["n"] < MIN_ROUTE_ROWS or abs(med) <= MAX_ROUTE_LATE_BIAS_S or name.startswith(NEAR_TERM_ONLY_ROUTE_PREFIXES):
            continue
        close = r.get("close_in_median_err_s")
        if med > 0:
            out.append(f"{name}: ENGINE -- buses came {med} s BEFORE our ETA (a hold or slow stretch we expected did not happen)")
        elif close is not None and abs(close) > CLOSE_IN_OK_S:
            out.append(f"{name}: ENGINE -- off by {abs(close)} s even under {CLOSE_IN_S // 60} min out, so not just a slow bus")
        elif name not in usual:
            out.append(f"{name}: BUSES SLOWER THAN HISTORY -- arrived {-med} s after our ETA, close-in ETAs fine; "
                       "too few earlier runs of this slot to say if that is today or every day")
        elif med < usual[name] - SLOWER_THAN_USUAL_S:
            was = f"{-usual[name]} s" if usual[name] < 0 else "on time or a little early"
            out.append(f"{name}: BUSES SLOW today -- arrived {-med} s after our ETA, usual for this slot is "
                       f"{was}; close-in ETAs fine. Road or dwell, not the engine")
        else:
            out.append(f"{name}: ENGINE LEAN -- buses arrive {-med} s after our ETA, about the same as most days in this "
                       f"slot ({-usual[name]} s): our history is too fast here, it is not today's traffic")
        for n in notices_for_route(summary, name):
            out[-1] += f" [SERVICE ALERT ({n.get('source', 'ours')}): \"{n['text'][:160]}\" -- may explain this]"
    verdicts = dict()
    for kind, verdict in summary.get("suspect_verdicts") or []:
        verdicts.setdefault(kind, set()).add(verdict)

    def whose(kind):
        """Who a breach of this kind is on, from eta_suspects' look at the buses behind it."""
        v = verdicts.get(kind, set())
        if "RECURRING" in v:
            return ("ENGINE (RECURRING: the same block at the same time on earlier days, so a timetable or engine rule, "
                    "not the bus; see suspects)")
        if "A FEW BUSES" in v or (v and v <= {"ONE BUS", "ONE OF A FEW BUSES"}):
            return "ONE BUS (not the engine; see suspects)"
        if "LOT SHUTTLES" in v:
            return "LOT SHUTTLES (game day, don't count; see suspects)"
        if v == {"SMALL"}:
            return "SMALL (a few scattered rows, nothing to chase; see suspects)"
        if v == {"PURPLE"}:
            return "PURPLE (scorer edge case at a stop, not the engine; see suspects)"
        if "ENGINE, ONE BUS" in v:
            return "ENGINE, ONE BUS (TransLoc had it right and the bus did nothing odd: worth a look; see suspects)"
        if "ONE BUS?" in v:
            return "PROBABLY ONE BUS (see suspects)"
        return "ENGINE"

    for b in summary.get("breaches") or []:
        if "late" in b and "bias" not in b:
            who = "Purple staging (buses leaving a hold early), known" if b.startswith(NEAR_TERM_ONLY_ROUTE_PREFIXES) else whose("late")
            out.append(f"{who} -- buses came more than 2 min before our ETA: {b}")
        elif "full-lap" in b:
            out.append(f"{whose('full-lap flip')} -- {b}")
        elif "TransLoc predicted" in b:
            out.append(f"{whose('TransLoc-only')} -- {b}")
        elif "real history" in b:
            out.append(f"ENGINE -- {b}")
    if summary.get("breaches"):
        for n in summary.get("notices") or []:
            where = ", ".join(n.get("routes") or []) or (f"stops {', '.join(n['stop_ids'])}" if n.get("stop_ids") else "system-wide")
            out.append(f"SERVICE ALERT active ({where}): \"{n['text'][:200]}\" -- check whether it explains the breaches above")
    return out


def bus_names():
    """TransLoc vehicle id -> bus number ("21" -> "12432"), from prod's vehicle list. {} if it can't be read."""
    out = {}

    def walk(o):
        if isinstance(o, dict):
            if "id" in o and "name" in o:
                out[str(o["id"])] = str(o["name"])
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    try:
        walk(eta_watch.fetch("/v1/vehicles"))
    except Exception:
        pass
    return out


def find_suspects(rows, polls, stops, lines, names):
    """eta_suspects.find_suspects, never raising: a bug in the digging must not lose the run."""
    try:
        return eta_suspects.find_suspects(rows, polls, stops, lines, names, bus_names(),
                                          late_limit_pct=MAX_LATE_MISS_PCT,
                                          skip_route_prefixes=NEAR_TERM_ONLY_ROUTE_PREFIXES)
    except Exception as exc:
        return [{"kind": "error", "verdict": "ERROR", "text": f"suspect search failed: {type(exc).__name__}: {exc}"}]


def earlier_results():
    try:
        return [json.loads(l) for l in RESULTS.read_text(encoding="utf-8").splitlines() if l.strip()]
    except Exception:
        return []


# A suspect "ONE BUS" verdict only looks at this run. The same block on the same route, same kind of breach, within
# RECUR_WINDOW_MIN of the same time on an earlier day is not a bus having an odd day: it is a timetable / engine rule
# firing every day. Orange [07] lost its Emmet / JPA ETAs 17:00-17:20 every weekday 10-13..10-22 and was called
# "ONE BUS" on 10-20 and 10-21 because it happened to sit at a layover both times.
RECUR_WINDOW_MIN = 20
# Repeat by place or by feed, not by a rule of ours: the Emmet / McCormick cell dead spot freezes GPS most weekdays, and
# a bus TransLoc moves to its evening route before it reaches the change stop drives "backward" on the new shape
# (blank is right there; TransLoc's numbers are wrong). Those keep the per-run verdict.
RECUR_EXEMPT = ("GPS froze", "just moved over from route", "pulling out (not on a route before)")
# A rule firing is a solid block of rows; a bus blanking for a poll or two here and there over hours is not.
RECUR_MIN_ROWS = 200
_SUSPECT_RE = re.compile(r"^[^:]+: \S+ (\[\d+\]) on (.+?), (\d\d):(\d\d):\d\d-\S+, (\d+) (.+?) rows")


def _suspect_slot(text):
    """(block, route name, kind, minute of day, rows) from a suspect line, or None (no block, or not a bus line)."""
    m = _SUSPECT_RE.match(text or "")
    return (m.group(1), m.group(2), m.group(6), int(m.group(3)) * 60 + int(m.group(4)), int(m.group(5))) if m else None


def mark_recurring(suspects, earlier, today):
    """Re-label every bus suspect whose block / route / kind / time also showed up on an earlier day as RECURRING."""
    for d in suspects:
        slot = _suspect_slot(d.get("text"))
        if (not slot or slot[4] < RECUR_MIN_ROWS or d.get("verdict") in ("LOT SHUTTLES", "PURPLE", "SMALL", "ERROR")
                or any(x in d.get("text", "") for x in RECUR_EXEMPT)):
            continue
        days = set()
        for r in earlier:
            day = str(r.get("when", ""))[:10]
            if not day or day == today:
                continue
            for t in r.get("suspects") or []:
                o = _suspect_slot(t)
                if o and o[:3] == slot[:3] and abs(o[3] - slot[3]) <= RECUR_WINDOW_MIN:
                    days.add(day)
        if days:
            was = d["verdict"]
            d["verdict"] = "RECURRING"
            d["text"] = (f"RECURRING (also on {', '.join(sorted(days)[-5:])}; this run alone said {was})"
                         + d["text"][d["text"].index(":"):])


def main():
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 30
    every = float(sys.argv[2]) if len(sys.argv) > 2 else 15
    now = datetime.datetime.now().astimezone()
    log = OUT_DIR / f"{now:%Y%m%d-%H%M}.jsonl"
    summary = {
        "when": now.isoformat(timespec="seconds"),
        "day": f"{now:%a}",
        "minutes": minutes,
        "log": str(log),
    }
    try:
        summary["service_level"] = service_level()
        calendar = service_calendar(now)
        if calendar:
            summary["calendar"] = calendar
        names = route_names()
        ours, theirs = service_notices(names), transloc_alerts(names, now)
        if ours is not None or theirs is not None:
            summary["notices"] = (theirs or []) + (ours or [])
        seen = watch(minutes, every, log)
        summary["purple_in_service"] = any(names.get(r, "").lower().startswith("purple") for r in seen)
        polls = [json.loads(l) for l in log.open(encoding="utf-8") if l.strip()]
        stops, lines = eta_compare.load_graph(log)
        names = {**graph_names(log), **names}
        rows = eta_compare.score_rows(polls, stops, lines)
        warmup_until = polls[min(WARMUP_POLLS, len(polls)) - 1]["t"] if polls else None
        stats, problems = analyze(rows, names, warmup_until)
        suspects = find_suspects(rows, polls, stops, lines, names) if problems else []
        mark_recurring(suspects, earlier_results(), f"{now:%Y-%m-%d}")
    except Exception as exc:  # network down, prod down, scorer bug...
        summary.update(error=f"{type(exc).__name__}: {exc}", breaches=[])
        with RESULTS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")
        print("health check failed:", summary["error"])
        return 1

    summary.update(stats)
    summary["breaches"] = problems
    if suspects:
        summary["suspects"] = [d["text"] for d in suspects]
        # (kind, verdict) for every breach kind a suspect covers, including ones folded into its line ("also").
        summary["suspect_verdicts"] = sorted({(k, d["verdict"]) for d in suspects for k in [d["kind"], *d.get("also", [])]})
    summary["causes"] = diagnose(summary, earlier_results())
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary) + "\n")
    print(json.dumps(summary, indent=2))
    if problems:
        print("\nBREACHES:\n  " + "\n  ".join(problems))
    else:
        print("\nInconclusive." if "inconclusive" in stats else "\nAll clear.")
    return 2 if problems else 0


def notify():
    """Hand the fresh result to the Claude-review + ntfy step; never let it affect the exit code."""
    try:
        import subprocess
        subprocess.run([sys.executable, str(Path(__file__).resolve().parent / "eta_health_notify.py")],
                       timeout=420)
    except Exception as exc:
        print("notify step failed:", exc)


if __name__ == "__main__":
    code = main()
    notify()
    sys.exit(code)
