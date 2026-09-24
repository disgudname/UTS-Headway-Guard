"""One unattended ETA health check: watch prod, score it, append a summary line, flag problems.

    python scripts/eta_health_check.py [minutes=30] [poll_seconds=15]

Runs the same logging + scoring as eta_watch.py / eta_compare.py (see HANDOFF.md section 7), then
appends ONE JSON line to data-local/eta_watch/health_results.jsonl with the headline numbers and any
threshold breaches. Exit code is 0 for a clean run, 2 if a threshold was breached (so a scheduler or
wrapper can react), 1 if the check itself couldn't run. Read-only: public GET requests only.
"""
import datetime
import json
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eta_compare  # noqa: E402
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


def route_names():
    try:
        return {str(l["id"]): l["name"] for l in eta_watch.fetch("/v1/trip-planner/uts-graph")["lines"]}
    except Exception:
        return {}


def transloc_only_gaps(rows, warmup_until=None):
    """(persistent, warmup, single_poll): rows where TransLoc predicted a visit and we didn't, split into
    the ones that are worth a breach and the two known noise kinds (see WARMUP_POLLS). A row is `warmup` if its
    poll is at or before `warmup_until`; `single_poll` if we DID predict that same (route, stop, vehicle) at both
    the previous and the next poll (one 15 s hole, then back)."""
    times = sorted({r["t"] for r in rows})
    idx = {t: i for i, t in enumerate(times)}
    present = {}
    for r in rows:
        if r["ours"] is not None:
            present.setdefault(r["key"], set()).add(idx[r["t"]])
    persistent = warmup = single = 0
    for r in rows:
        if r["ours"] is not None or r["tl"] is None:
            continue
        i = idx[r["t"]]
        have = present.get(r["key"], ())
        if warmup_until is not None and r["t"] <= warmup_until:
            warmup += 1
        elif (i - 1) in have and (i + 1) in have:
            single += 1
        else:
            persistent += 1
    return persistent, warmup, single


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
    tl = [r["tl"] for r in both]
    if tl:
        out["tl_median_err_s"] = round(statistics.median(tl))
        out["tl_late_over_2min_pct"] = round(100.0 * sum(1 for e in tl if e > LATE_MISS_S) / len(tl), 1)
    persistent, warmup, single = transloc_only_gaps(rows, warmup_until)
    out["only_transloc"] = persistent
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
    if late > MAX_LATE_MISS_PCT:
        problems.append(f"{late:.1f}% of predictions >2 min late (limit {MAX_LATE_MISS_PCT}%)")
    for rid, e in by_route.items():
        if len(e) < MIN_ROUTE_ROWS:
            continue
        name = names.get(rid, rid)
        if statistics.median(abs(x) for x in e) > MAX_ROUTE_MEDIAN_ABS_S:
            problems.append(f"{name}: median |error| {statistics.median(abs(x) for x in e):.0f}s (limit {MAX_ROUTE_MEDIAN_ABS_S}s)")
        if statistics.median(e) > MAX_ROUTE_LATE_BIAS_S:
            problems.append(f"{name}: late bias {statistics.median(e):+.0f}s (limit +{MAX_ROUTE_LATE_BIAS_S}s)")
    if flips:
        problems.append(f"{len(flips)} full-lap flip(s): our ETA >20 min off while TransLoc was within 2 min")
    if out["only_transloc"]:
        problems.append(f"{out['only_transloc']} visit(s) TransLoc predicted that we didn't")
    if "historical_pct" in out:
        pct = out["historical_pct"]
        if pct < MIN_HISTORICAL_PCT_BREACH:
            problems.append(f"only {pct}% of estimates use real history (hop-time table has probably emptied)")
        elif pct < MIN_HISTORICAL_PCT_NOTE:
            out["notes"] = [f"thin history: only {pct}% of estimates use real history for every hop (expected on weekend "
                            "daytime while the weekend routes' history builds up; not a problem by itself)"]
    return out, problems


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
        names = route_names()
        seen = watch(minutes, every, log)
        summary["purple_in_service"] = any(names.get(r, "").lower().startswith("purple") for r in seen)
        polls = [json.loads(l) for l in log.open(encoding="utf-8") if l.strip()]
        stops, lines = eta_compare.load_graph()
        rows = eta_compare.score_rows(polls, stops, lines)
        warmup_until = polls[min(WARMUP_POLLS, len(polls)) - 1]["t"] if polls else None
        stats, problems = analyze(rows, names, warmup_until)
    except Exception as exc:  # network down, prod down, scorer bug...
        summary.update(error=f"{type(exc).__name__}: {exc}", breaches=[])
        with RESULTS.open("a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")
        print("health check failed:", summary["error"])
        return 1

    summary.update(stats)
    summary["breaches"] = problems
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
