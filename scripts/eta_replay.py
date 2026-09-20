"""Offline replay: score a candidate ETA history against real bus tracks.

Feeds every bus position in eta_watch logs back through bus_eta.estimate_stop_eta_s under
(a) the current history (arrival->arrival hops, layover baked in, capped at mapped timestops)
and (b) driving-only hops + separate dwell (trip_planner_history.build_drive_and_dwell_samples),
both built ONLY from headway events before the logs start, then scores each against the
real arrival times with eta_compare's direction-aware scorer.

  python scripts/eta_replay.py <headway_dir> "2026-09-20T03:00" <log.jsonl> [...]

Not modelled: the block schedule (scheduled holds need the vehicle's block id, which the logs
don't record) and the app's median-of-3 smoothing. Both variants are treated the same, so the
comparison between them is fair; the absolute numbers differ a little from the live app's.
"""
import glob
import json
import statistics as st
import sys
import types
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.modules.setdefault("httpx", types.ModuleType("httpx"))  # bus_eta -> trip_planner imports it; unused offline
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import bus_eta  # noqa: E402
import eta_compare as ec  # noqa: E402
import trip_planner_history as tph  # noqa: E402
import uts_blocks  # noqa: E402
from headway_storage import HeadwayStorage  # noqa: E402
from trip_planner import Line, Stop  # noqa: E402

NY = ZoneInfo("America/New_York")
MPH_TO_MPS = 0.44704


def route_stop_names():
    """(route_id, stop name) -> RouteStopID from TransLoc's live route stop lists; names that
    occur twice on one route are ambiguous and left out."""
    import urllib.request
    with urllib.request.urlopen(ec.BASE + "/v1/transloc/routes", timeout=30) as r:
        routes = json.load(r)
    table, dup = {}, set()
    for route in routes:
        for stop in route.get("Stops", []):
            key = (str(route["RouteID"]), (stop.get("Description") or stop.get("Name") or "").strip())
            if key in table and table[key] != str(stop["RouteStopID"]):
                dup.add(key)
            table[key] = str(stop["RouteStopID"])
    return {k: v for k, v in table.items() if k not in dup}


def build_models(headway_dir, cutoff):
    storage = HeadwayStorage(Path(headway_dir))
    resolve = tph.stop_id_resolver(route_stop_names())

    def buckets(samples):
        return {k: {"seconds": st.median(v), "samples": len(v)} for k, v in samples.items() if len(v) >= tph.MIN_SAMPLES}

    old_raw = tph.HopTimeModel(buckets(tph.build_hop_time_samples(storage, now=cutoff)))
    old_res = tph.HopTimeModel(buckets(tph.build_hop_time_samples(storage, now=cutoff, resolve_stop_id=resolve)))
    drive, dwell = tph.build_drive_and_dwell_samples(storage, now=cutoff, resolve_stop_id=resolve)
    return {"old_raw": old_raw, "old_res": old_res, "drive": tph.HopTimeModel(buckets(drive)),
            "dwell": tph.DwellModel.from_samples(dwell), "dwell_q25": tph.DwellModel.from_samples(dwell, 0.25)}


def load_lines():
    import urllib.request
    with urllib.request.urlopen(ec.BASE + "/v1/trip-planner/uts-graph", timeout=30) as r:
        graph = json.load(r)
    lines = {}
    for e in graph["lines"]:
        stops = [Stop(id=s["id"], name=s["name"], lat=s["lat"], lon=s["lon"], source="uts", arc_pos=s.get("arc_pos"))
                 for s in e["stops"]]
        lines[str(e["id"])] = Line(id=str(e["id"]), name=e["name"], color="888888", source="uts", stops=stops,
                                   loop=True, shape=[tuple(p) for p in e["poly"]], shape_cum=list(e["cum"]))
    return lines


def replay(polls, lines, ec_stops, ec_lines, variants):
    """{variant: polls-with-'ours'-replaced} for one log."""
    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _ in p["veh"]:
            tracks.setdefault((veh, route), []).append((p["t"], lat, lon))
    unwrapped = ec.build_unwrapped(tracks, ec_lines)
    ema = {}
    out = {name: [] for name in variants}
    for p in polls:
        rows = {name: [] for name in variants}
        for veh, route, lat, lon, mph in p["veh"]:
            line = lines.get(route)
            u = unwrapped.get((veh, route))
            if line is None or not u:
                continue
            v = max(0.0, mph * MPH_TO_MPS)
            e = v if (veh, route) not in ema else 0.4 * v + 0.6 * ema[(veh, route)]
            ema[(veh, route)] = e
            e = max(1.2, min(22.0, e))
            if e >= 22.0:
                e = bus_eta.TYPICAL_BUS_SPEED_MPS
            length = line.shape_cum[-1]
            s_pos = ec.position_at(u, p["t"]) % length
            for stop in line.stops:
                if stop.arc_pos is None:
                    continue
                for name, kw in variants.items():
                    r = bus_eta.estimate_stop_eta_s(line, s_pos, e, stop, when=p["t"], vehicle_lat=lat, vehicle_lon=lon,
                                                    vehicle_dir_sign=1, **kw)
                    if r is not None:
                        rows[name].append([route, stop.id, veh, round(r.seconds, 1), r.source])
        for name in variants:
            out[name].append({"t": p["t"], "veh": p["veh"], "ours": rows[name], "tl": p["tl"]})
    return out


def stats(errs):
    if not errs:
        return "n=0"
    a = [abs(x) for x in errs]
    n = len(errs)
    return (f"n={n:<5} median {st.median(errs):+5.0f}s  |err| {st.median(a):4.0f}s  within 1m {100 * sum(x <= 60 for x in a) / n:3.0f}%  "
            f"2m {100 * sum(x <= 120 for x in a) / n:3.0f}%  late>2m {100 * sum(x > 120 for x in errs) / n:4.1f}%")


def main():
    headway_dir, cutoff_s, *logs = sys.argv[1:]
    cutoff = datetime.fromisoformat(cutoff_s).replace(tzinfo=NY)
    print(f"building history from events before {cutoff} ...", flush=True)
    m = build_models(headway_dir, cutoff)
    lines = load_lines()
    ec_stops, ec_lines = ec.load_graph()
    capped = lambda r, s: uts_blocks.timestop_code_for_stop(r, s) is not None  # noqa: E731
    def scheduled(route, stop, _block, ref):
        # Offline stand-in for the app's live block tracking: the block whose schedule has the
        # nearest visit to this timestop (fine on weekends, where each route runs ~one bus).
        block = uts_blocks.best_matching_block(route, stop, ref)
        return uts_blocks.scheduled_hold_epoch(route, stop, block, ref) if block else None

    sched = dict(vehicle_block_id="auto", scheduled_timestop_fn=scheduled)
    variants = {
        "old_res": dict(hop_time_fn=m["old_res"].lookup, is_timestop_fn=capped),
        "old_sch": dict(hop_time_fn=m["old_res"].lookup, is_timestop_fn=capped, **sched),
        "new_nosch": dict(hop_time_fn=m["drive"].lookup, dwell_fn=m["dwell"].lookup),
        "new_sch": dict(hop_time_fn=m["drive"].lookup, dwell_fn=m["dwell"].lookup, **sched),
        "new_sch25": dict(hop_time_fn=m["drive"].lookup, dwell_fn=m["dwell_q25"].lookup, **sched),
        "new_no25": dict(hop_time_fn=m["drive"].lookup, dwell_fn=m["dwell_q25"].lookup),
    }
    names = ("live",) + tuple(variants)
    scored = {n: [] for n in names}
    for path in logs:
        polls = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
        replays = replay(polls, lines, ec_stops, ec_lines, variants)
        per = {"live": {(r["t"], r["key"]): r for r in ec.score_rows(polls, ec_stops, ec_lines)}}
        for name, pp in replays.items():
            per[name] = {(r["t"], r["key"]): r for r in ec.score_rows(pp, ec_stops, ec_lines)}
        for k, row in per["live"].items():
            if all(k in per[n] and per[n][k]["ours"] is not None for n in variants) and row["ours"] is not None and row["tl"] is not None:
                for n in names:
                    scored[n].append(dict(per[n][k], tl=row["tl"], remaining=row["remaining"], route=k[1][0], veh=k[1][2]))
        print(f"  scored {path}", flush=True)

    def block(title, rows_by):
        print(f"\n=== {title} ===")
        for n, rows in rows_by.items():
            print(f"  {n:<8}", stats([r["ours"] for r in rows]))
        print(f"  {'TL':<8}", stats([r["tl"] for r in rows_by['live']]))

    import os
    import pickle
    if os.environ.get("ETA_REPLAY_DUMP"):
        pickle.dump(scored, open(os.environ["ETA_REPLAY_DUMP"], "wb"))
    block("all predictions", scored)
    for lo, hi, label in ec.BUCKETS:
        sub = {n: [r for r in rows if lo <= r["remaining"] < hi] for n, rows in scored.items()}
        if sub["live"]:
            block(label, sub)
    for route in sorted({r["route"] for r in scored["live"]}):
        block(f"route {route}", {n: [r for r in rows if r["route"] == route] for n, rows in scored.items()})


if __name__ == "__main__":
    main()
