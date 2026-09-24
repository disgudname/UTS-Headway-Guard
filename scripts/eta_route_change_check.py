"""Replays the evening-route-change rule (bus_eta.route_change_hidden_stops) over one eta_watch log and reports what it
would have hidden. Post-hoc filter on the logged predictions, so it uses the logged bus positions and the logged ETA at
the change stop instead of re-running the ETA engine; it shows what the rule would drop, not a full engine replay.

  wrong shown   a prediction, for a bus whose route id later flipped, that the bus did not make good on BEFORE its flip
                (it never got within 60 m of the stop, or only after the flip) -- what riders were wrongly shown
  hidden ok     of those, how many the rule hides            (want: nearly all)
  hidden bad    a prediction the bus DID make good on before its flip that the rule would hide anyway (want: zero)
Usage: python scripts/eta_route_change_check.py data-local/eta_watch/<log>.jsonl
"""
import collections
import datetime
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import bus_eta  # noqa: E402
import eta_compare  # noqa: E402
import uts_blocks  # noqa: E402
from trip_planner import Line, Stop  # noqa: E402

OLD = {"67": "57", "68": "54", "53": "55"}
REACH_M = 60.0


def _dist(lat1, lon1, lat2, lon2):
    return math.hypot((lat1 - lat2) * 111000.0, (lon1 - lon2) * 111000.0 * math.cos(math.radians(lat1)))


def main():
    log = sys.argv[1]
    polls = [json.loads(l) for l in open(log, encoding="utf-8") if l.strip()]
    graph = json.loads(Path(log + ".graph.json").read_text(encoding="utf-8"))["lines"]
    stops_arc, lines_poly = eta_compare.load_graph(log)
    lines = {}
    for g in graph:
        lines[str(g["id"])] = Line(
            id=str(g["id"]), name=g["name"], color=g["color"], source="uts", loop=True, shape=g["poly"],
            shape_cum=g["cum"],
            stops=[Stop(id=str(s["id"]), name=s["name"], lat=s["lat"], lon=s["lon"], source="uts", arc_pos=s["arc_pos"])
                   for s in g["stops"] if s.get("arc_pos") is not None],
        )
    coord = {(str(g["id"]), str(s["id"])): (s["lat"], s["lon"], s["name"]) for g in graph for s in g["stops"]}
    tracks = collections.defaultdict(list)
    tracks_any = collections.defaultdict(list)
    for p in polls:
        for veh, route, lat, lon, _mps in p["veh"]:
            tracks[(veh, route)].append((p["t"], lat, lon))
            tracks_any[veh].append((p["t"], lat, lon))
    unwrapped = eta_compare.build_unwrapped(tracks, {k: v for k, v in lines_poly.items()})
    end = polls[-1]["t"]
    served_norm = {old: {bus_eta._norm_stop_name(n) for n in uts_blocks._evening_pairs[old]["served_names"]} for old in OLD}
    _norm = bus_eta._norm_stop_name

    wanted = {(row[0], row[1], row[2]) for p in polls for row in p["ours"] if row[0] in OLD}
    crossings = eta_compare.find_crossings(tracks, stops_arc, lines_poly, wanted, unwrapped)

    def reach_time(route, stop, veh, t0):
        """First time at/after t0 the bus's position ALONG THE ROUTE crossed the stop (the other side of the road,
        or the opposite direction of the same street, doesn't count), else None."""
        return next((a for a in crossings.get((route, stop, veh), []) if a >= t0 - 1.0), None)

    flip_t = {}  # bus -> first poll where it is predicted on a post-6PM route
    for p in polls:
        for r, _s, v, *_ in p["ours"]:
            if r in OLD.values() and v not in flip_t:
                flip_t[v] = p["t"]
    SLACK_S = 120.0  # TransLoc flips ~1-2 min after the bus physically changes over
    stats = collections.Counter()
    missed = collections.Counter()
    block_seen = {}
    dbg13 = collections.Counter()
    bad = []
    per_route = collections.defaultdict(collections.Counter)
    for p in polls:
        t = p["t"]
        by_bus = collections.defaultdict(list)
        for row in p["ours"]:
            if row[0] in OLD:
                by_bus[(row[0], row[2])].append(row)
        for (route, veh), rows in by_bus.items():
            block = rows[0][5]
            if not block and t - block_seen.get(veh, ("", -1e9))[1] <= 300.0:  # app.py's block memory
                block = block_seen[veh][0]
            plan = uts_blocks.route_change_plan(route, block, t) if block else None
            if plan is not None:
                block_seen[veh] = (block, t)
            line = lines.get(route)
            u = unwrapped.get((veh, route))
            s_pos = eta_compare.position_at(u, t) if u else None
            if plan is None or line is None or s_pos is None:
                hidden = set()
            else:
                s_pos = s_pos % line.shape_cum[-1]
                cs_eta = next((r[3] for r in rows if r[1] == str(plan[0])), None)
                hidden = bus_eta.route_change_hidden_stops(line, s_pos, plan, t, cs_eta)
            for _r, stop, _v, secs, *_ in rows:
                if secs > 1800 or t + secs + 240 > end:
                    continue
                lat, lon, name = coord[(route, stop)]
                rt = reach_time(route, stop, veh, t)
                cutoff = flip_t.get(veh)
                served_before_flip = rt is not None and (cutoff is None or rt < cutoff - SLACK_S)
                made_good = served_before_flip
                skipped = _norm(name) not in served_norm[route]
                wrong = skipped and cutoff is not None and t + secs >= cutoff - SLACK_S and not served_before_flip
                if wrong and stop not in hidden:
                    missed[(route, veh, block, "reached later" if rt is not None else "never")] += 1
                    if veh == "13" and rt is None: dbg13[(name[:32], round(s_pos or -1), plan[0] if plan else None)] += 1
                if wrong:
                    stats["wrong shown"] += 1
                    per_route[route]["wrong shown"] += 1
                    if stop in hidden:
                        stats["hidden ok"] += 1
                        per_route[route]["hidden ok"] += 1
                if stop in hidden:
                    stats["hidden total"] += 1
                    if made_good:
                        stats["hidden bad"] += 1
                        per_route[route]["hidden bad"] += 1
                        bad.append((datetime.datetime.fromtimestamp(t).strftime("%H:%M:%S"), route, veh, name, round(secs)))
                stats["judged"] += 1
    print(dict(stats))
    for r, c in sorted(per_route.items()):
        print(" route", r, dict(c))
    print("wrong-shown NOT hidden, by (route, bus, block):", dict(missed))
    print("bus13 unhidden never:", dict(list(dbg13.items())[:12]))
    print("hidden but the bus DID reach the stop (first 15):")
    for b in bad[:15]:
        print("  ", b)


if __name__ == "__main__":
    main()
