"""Score the ETAs logged by scripts/eta_watch.py against what the buses actually did.

    python scripts/eta_compare.py data-local/eta_watch/run1.jsonl

"Actual arrival" = the moment a bus's logged GPS track came closest to a stop (must pass
within PASS_RADIUS_M), interpolated between polls. Each logged prediction is matched to
that vehicle's next actual pass of that stop; predictions whose pass never happened inside
the logging window are skipped (nothing to score them against).
Error = predicted arrival - actual arrival, so positive means the estimate was too late.
"""
import json
import math
import statistics
import sys
import urllib.request

BASE = "https://uts-headway-guard.fly.dev"
PASS_RADIUS_M = 40.0
MERGE_S = 90.0  # detections of the same vehicle/stop this close together are one visit
BUCKETS = [(0, 120, "<2 min"), (120, 300, "2-5 min"), (300, 600, "5-10 min"), (600, 1200, "10-20 min"), (1200, 1e9, "20+ min")]


def to_xy(lat, lon, lat0, lon0):
    return (lon - lon0) * 111320.0 * math.cos(math.radians(lat0)), (lat - lat0) * 110540.0


def seg_closest(p0, p1, stop_lat, stop_lon):
    """(min distance m, fraction 0..1 along p0->p1) of the stop to the straight segment."""
    (t0, la0, lo0), (t1, la1, lo1) = p0, p1
    ax, ay = 0.0, 0.0
    bx, by = to_xy(la1, lo1, la0, lo0)
    sx, sy = to_xy(stop_lat, stop_lon, la0, lo0)
    vv = bx * bx + by * by
    f = 0.0 if vv == 0 else max(0.0, min(1.0, (sx * bx + sy * by) / vv))
    return math.hypot(sx - f * bx, sy - f * by), f


def load_stops():
    with urllib.request.urlopen(BASE + "/v1/trip-planner/uts-graph", timeout=30) as r:
        g = json.load(r)
    stops = {}
    for line in g["lines"]:
        for s in line["stops"]:
            if s.get("lat") is not None:
                stops[(str(line["id"]), str(s["id"]))] = (s["lat"], s["lon"], s["name"])
    return stops


def find_passes(tracks, stops, wanted):
    """{(route, stop, veh): [pass epoch, ...]} for the (route, stop, veh) keys in `wanted`."""
    passes = {}
    for (route, stop, veh) in wanted:
        track = tracks.get((veh, route))
        if not track or (route, stop) not in stops:
            continue
        slat, slon, _ = stops[(route, stop)]
        hits = []
        for a, b in zip(track, track[1:]):
            if b[0] - a[0] > 120:  # a gap in the log, don't interpolate across it
                continue
            d, f = seg_closest(a, b, slat, slon)
            if d <= PASS_RADIUS_M:
                hits.append(a[0] + f * (b[0] - a[0]))
        merged = []
        for h in hits:
            if not merged or h - merged[-1] > MERGE_S:
                merged.append(h)
        if merged:
            passes[(route, stop, veh)] = merged
    return passes


def summarize(name, errs):
    if not errs:
        return f"  {name:<6} n=0"
    abs_e = [abs(e) for e in errs]
    within = lambda s: 100.0 * sum(1 for e in abs_e if e <= s) / len(abs_e)
    return (f"  {name:<6} n={len(errs):<5} median err {statistics.median(errs):+6.0f}s   median |err| {statistics.median(abs_e):5.0f}s   "
            f"mean |err| {statistics.mean(abs_e):5.0f}s   within 1min {within(60):3.0f}%  2min {within(120):3.0f}%  5min {within(300):3.0f}%")


def main():
    path = sys.argv[1]
    polls = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    print(f"{len(polls)} polls, {(polls[-1]['t'] - polls[0]['t']) / 60:.1f} min\n")
    stops = load_stops()

    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _mps in p["veh"]:
            tracks.setdefault((veh, route), []).append((p["t"], lat, lon))

    wanted = {(r, s, v) for p in polls for r, s, v, _ in p["ours"] + p["tl"]}
    passes = find_passes(tracks, stops, wanted)
    print(f"{sum(len(v) for v in passes.values())} real stop visits observed\n")

    rows = []  # one per (poll, route, stop, veh) with a scoreable actual pass
    for p in polls:
        t = p["t"]
        ours = {(r, s, v): sec for r, s, v, sec in p["ours"]}
        tl = {(r, s, v): sec for r, s, v, sec in p["tl"]}
        for key in set(ours) | set(tl):
            # Only passes still ahead of this poll: a prediction made just after the bus
            # went by is about its NEXT lap and must not be scored against the pass
            # that just happened.
            actual = next((a for a in passes.get(key, []) if a >= t), None)
            if actual is None or actual - t > 3600:
                continue
            rows.append({
                "t": t, "key": key, "remaining": actual - t,
                "ours": (t + ours[key] - actual) if key in ours else None,
                "tl": (t + tl[key] - actual) if key in tl else None,
            })
    if not rows:
        print("Nothing to score -- no predicted stop visits were observed. Log for longer?")
        return

    both = [r for r in rows if r["ours"] is not None and r["tl"] is not None]
    print("=== Overall (only predictions both engines made, so it's apples to apples) ===")
    print(summarize("ours", [r["ours"] for r in both]))
    print(summarize("TL", [r["tl"] for r in both]))
    if both:
        wins = sum(1 for r in both if abs(r["ours"]) < abs(r["tl"]))
        ties = sum(1 for r in both if abs(r["ours"]) == abs(r["tl"]))
        print(f"  ours closer than TransLoc in {100.0 * wins / len(both):.0f}% of {len(both)} pairs ({ties} ties)")

    print("\n=== By how far away the bus really was when the prediction was made ===")
    for lo, hi, label in BUCKETS:
        sub = [r for r in both if lo <= r["remaining"] < hi]
        if sub:
            print(f"{label}")
            print(summarize("ours", [r["ours"] for r in sub]))
            print(summarize("TL", [r["tl"] for r in sub]))

    only_tl = [r for r in rows if r["ours"] is None and r["tl"] is not None]
    only_ours = [r for r in rows if r["tl"] is None and r["ours"] is not None]
    print("\n=== Coverage (predictions for a real, later-observed visit) ===")
    print(f"  both gave one: {len(both)}   only TransLoc: {len(only_tl)}   only ours: {len(only_ours)}")
    if only_tl:
        missing = {}
        for r in only_tl:
            missing[r["key"]] = missing.get(r["key"], 0) + 1
        print("  most-missed by us (route, stop, vehicle -> polls):")
        for (route, stop, veh), n in sorted(missing.items(), key=lambda kv: -kv[1])[:5]:
            print(f"    route {route}  {stops[(route, stop)][2]}  bus {veh}: {n}")

    print("\n=== Our 10 worst misses ===")
    for r in sorted((r for r in rows if r["ours"] is not None), key=lambda r: -abs(r["ours"]))[:10]:
        route, stop, veh = r["key"]
        tl_txt = f"{r['tl']:+.0f}s" if r["tl"] is not None else "n/a"
        print(f"  {stops[(route, stop)][2]} (route {route}, bus {veh}) real {r['remaining']:.0f}s away: ours {r['ours']:+.0f}s, TL {tl_txt}")


if __name__ == "__main__":
    main()
