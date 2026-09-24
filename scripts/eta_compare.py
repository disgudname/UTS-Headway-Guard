"""Score the ETAs logged by scripts/eta_watch.py against what the buses actually did.

    python scripts/eta_compare.py data-local/eta_watch/run1.jsonl [more.jsonl ...]

"Actual arrival" = the moment a bus's position ALONG ITS ROUTE (not just its GPS point)
crosses the stop's position along the route. Each bus's logged GPS track is matched to the
route polyline with a Viterbi pass that prefers steady forward motion, so a bus driving out
along a road the route also returns on can never be mistaken for one passing the return-side
stop. Each logged prediction is then matched to that vehicle's next real crossing of that
stop; predictions whose crossing never happened inside the logging window are skipped.
Error = predicted arrival - actual arrival, so positive means the estimate was too late.
"""
import json
import math
import statistics
import sys
import urllib.request
from pathlib import Path

BASE = "https://uts-headway-guard.fly.dev"
OFF_ROUTE_M = 60.0      # GPS samples farther than this from the route shape are ignored (detours)
CAND_SLACK_M = 40.0     # keep route-position candidates within this of the closest one
MAX_MPS = 25.0          # fastest plausible bus, for the transition model
MERGE_S = 90.0          # crossings of the same stop this close together are one visit
BUCKETS = [(0, 120, "<2 min"), (120, 300, "2-5 min"), (300, 600, "5-10 min"), (600, 1200, "10-20 min"), (1200, 1e9, "20+ min")]


def to_xy(lat, lon, lat0, lon0):
    return (lon - lon0) * 111320.0 * math.cos(math.radians(lat0)), (lat - lat0) * 110540.0


def haversine(lat1, lon1, lat2, lon2):
    x, y = to_xy(lat2, lon2, lat1, lon1)
    return math.hypot(x, y)


def load_graph(log=None):
    """Stop/route geometry. TransLoc renumbers RouteStopIDs whenever the service variant changes (day vs.
    evening, detours), so a log can only be scored against the graph from when it was recorded. With `log`,
    the graph is saved next to it (<log>.graph.json) on first use and re-read from there afterwards; a log
    with no saved graph falls back to today's live graph (fine for a fresh log, wrong for an old one)."""
    saved = Path(str(log) + ".graph.json") if log else None
    if saved is not None and saved.exists():
        g = json.loads(saved.read_text(encoding="utf-8"))
    else:
        with urllib.request.urlopen(BASE + "/v1/trip-planner/uts-graph", timeout=30) as r:
            raw = r.read()
        g = json.loads(raw)
        if saved is not None:
            saved.write_bytes(raw)
    stops, lines = {}, {}
    for line in g["lines"]:
        lines[str(line["id"])] = (line["poly"], line["cum"])
        for s in line["stops"]:
            if s.get("lat") is not None and s.get("arc_pos") is not None:
                stops[(str(line["id"]), str(s["id"]))] = (s["lat"], s["lon"], s["name"], s["arc_pos"])
    return stops, lines


def candidates(lat, lon, poly, cum):
    """[(arc position, distance m)] -- the closest point of each distinct stretch of route."""
    best = []
    for i in range(len(poly) - 1):
        a, b = poly[i], poly[i + 1]
        bx, by = to_xy(b[0], b[1], a[0], a[1])
        px, py = to_xy(lat, lon, a[0], a[1])
        vv = bx * bx + by * by
        t = 0.0 if vv == 0 else max(0.0, min(1.0, (px * bx + py * by) / vv))
        d = math.hypot(px - t * bx, py - t * by)
        best.append((d, cum[i] + t * math.sqrt(vv)))
    dmin = min(d for d, _ in best)
    if dmin > OFF_ROUTE_M:
        return []
    picked = []
    for d, s in sorted(best):
        if d > dmin + CAND_SLACK_M:
            break
        if all(abs(s - ps) > 50.0 for ps, _ in picked):
            picked.append((s, d))
        if len(picked) >= 8:
            break
    return picked


def signed_move(s_from, s_to, length):
    return ((s_to - s_from + length / 2.0) % length) - length / 2.0


def unwrapped_track(track, poly, cum):
    """[(t, unwrapped arc position m)] for one vehicle's (t, lat, lon) samples: the
    Viterbi-best path over route-position candidates, preferring steady forward motion."""
    length = cum[-1]
    layers = []
    for t, lat, lon in track:
        c = candidates(lat, lon, poly, cum)
        if c:
            layers.append((t, c))
    if not layers:
        return []
    cost = [[d / 10.0 for _, d in layers[0][1]]]
    back = [[-1] * len(layers[0][1])]
    for k in range(1, len(layers)):
        dt = layers[k][0] - layers[k - 1][0]
        row, brow = [], []
        for s2, d2 in layers[k][1]:
            best, bi = 1e18, 0
            for j, (s1, _) in enumerate(layers[k - 1][1]):
                mv = signed_move(s1, s2, length)
                hi = MAX_MPS * min(dt, 120.0) + 60.0
                trans = 0.0 if -60.0 <= mv <= hi else 5.0 + (max(-60.0 - mv, mv - hi)) / 100.0
                v = cost[k - 1][j] + trans
                if v < best:
                    best, bi = v, j
            row.append(best + d2 / 10.0)
            brow.append(bi)
        cost.append(row)
        back.append(brow)
    idx = min(range(len(cost[-1])), key=lambda j: cost[-1][j])
    path = []
    for k in range(len(layers) - 1, -1, -1):
        path.append((layers[k][0], layers[k][1][idx][0]))
        idx = back[k][idx]
    path.reverse()
    out, u = [], path[0][1]
    out.append((path[0][0], u))
    for (t0, s0), (t1, s1) in zip(path, path[1:]):
        u += signed_move(s0, s1, length)
        out.append((t1, u))
    return out


def build_unwrapped(tracks, lines):
    """{(veh, route): [(t, unwrapped arc position m), ...]} for every tracked vehicle."""
    return {
        (veh, route): unwrapped_track(track, *lines[route])
        for (veh, route), track in tracks.items() if route in lines
    }


def position_at(u, t):
    """Unwrapped arc position at time t (linear between samples, clamped to the ends)."""
    if not u:
        return None
    if t <= u[0][0]:
        return u[0][1]
    for (t0, x0), (t1, x1) in zip(u, u[1:]):
        if t0 <= t <= t1:
            return x0 if t1 == t0 else x0 + (x1 - x0) * (t - t0) / (t1 - t0)
    return u[-1][1]


def find_crossings(tracks, stops, lines, wanted, unwrapped=None):
    """{(route, stop, veh): [epoch the bus's route position crossed the stop's, ...]}."""
    if unwrapped is None:
        unwrapped = build_unwrapped(tracks, lines)
    out = {}
    for (route, stop, veh) in wanted:
        u = unwrapped.get((veh, route))
        if not u or (route, stop) not in stops:
            continue
        arc, length = stops[(route, stop)][3], lines[route][1][-1]
        hits = []
        for (t0, u0), (t1, u1) in zip(u, u[1:]):
            if u1 <= u0 or t1 - t0 > 120:
                continue
            k = math.ceil((u0 - arc) / length)
            target = arc + k * length
            if u0 <= target <= u1:
                hits.append(t0 + (target - u0) / (u1 - u0) * (t1 - t0))
        merged = []
        for h in hits:
            if not merged or h - merged[-1] > MERGE_S:
                merged.append(h)
        if merged:
            out[(route, stop, veh)] = merged
    return out


def summarize(name, errs):
    if not errs:
        return f"  {name:<6} n=0"
    abs_e = [abs(e) for e in errs]
    within = lambda s: 100.0 * sum(1 for e in abs_e if e <= s) / len(abs_e)
    return (f"  {name:<6} n={len(errs):<5} median err {statistics.median(errs):+6.0f}s   median |err| {statistics.median(abs_e):5.0f}s   "
            f"mean |err| {statistics.mean(abs_e):5.0f}s   within 1min {within(60):3.0f}%  2min {within(120):3.0f}%  5min {within(300):3.0f}%")


def score_rows(polls, stops, lines):
    """One row per (poll, route, stop, veh) whose real crossing was observed later in the log."""
    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _mps in p["veh"]:
            tracks.setdefault((veh, route), []).append((p["t"], lat, lon))
    wanted = {(x[0], x[1], x[2]) for p in polls for x in p["ours"] + p["tl"]}
    unwrapped = build_unwrapped(tracks, lines)
    crossings = find_crossings(tracks, stops, lines, wanted, unwrapped)
    rows = []
    for p in polls:
        t = p["t"]
        ours = {(x[0], x[1], x[2]): x[3] for x in p["ours"]}
        tl = {(x[0], x[1], x[2]): x[3] for x in p["tl"]}
        src = {(x[0], x[1], x[2]): (x[4] if len(x) > 4 else None) for x in p["ours"]}
        for key in set(ours) | set(tl):
            actual = next((a for a in crossings.get(key, []) if a >= t - 1.0), None)
            if actual is None or actual - t > 3600:
                continue
            # How far the bus's route position is BEYOND the stop right now (metres; positive = it has
            # already passed the stop, within half a lap). A "Due" reading a few seconds after the bus
            # went by looks like a full-lap error to the scorer, because the next crossing is a lap away.
            route, stop, veh = key
            pos = position_at(unwrapped.get((veh, route)), t)
            past_m = None
            if pos is not None and (route, stop) in stops:
                length = lines[route][1][-1]
                past_m = ((pos - stops[(route, stop)][3] + length / 2.0) % length) - length / 2.0
            rows.append({
                "t": t, "key": key, "remaining": max(0.0, actual - t), "past_m": past_m,
                "ours": (t + ours[key] - actual) if key in ours else None,
                "tl": (t + tl[key] - actual) if key in tl else None,
                "ours_s": ours.get(key), "tl_s": tl.get(key), "source": src.get(key),
            })
    return rows


def main():
    paths = sys.argv[1:]
    rows, minutes = [], 0.0
    for path in paths:
        stops, lines = load_graph(path)
        polls = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
        minutes += (polls[-1]["t"] - polls[0]["t"]) / 60
        rows += score_rows(polls, stops, lines)
    print(f"{len(paths)} log(s), {minutes:.1f} min total, {len(rows)} scoreable predictions\n")
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

    print("\n=== Our 10 worst misses ===")
    for r in sorted((r for r in rows if r["ours"] is not None), key=lambda r: -abs(r["ours"]))[:10]:
        route, stop, veh = r["key"]
        tl_txt = f"{r['tl']:+.0f}s" if r["tl"] is not None else "n/a"
        print(f"  {stops[(route, stop)][2]} (route {route}, bus {veh}) real {r['remaining']:.0f}s away: ours {r['ours']:+.0f}s, TL {tl_txt}")


if __name__ == "__main__":
    main()
