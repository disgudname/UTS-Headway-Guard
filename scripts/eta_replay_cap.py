"""Replay eta_watch logs through bus_eta with the REAL per-bus block ids the logs carry, comparing:
  live      = what production predicted at the time
  old       = the day-independent layover cap (what app.py uses), holds from the block schedule, no cut-off
  tod       = + a time-aware layover cap (uts_blocks.is_timestop_at; NOT wired into app.py, see HANDOFF.md)
  tod_oos   = + the out-of-service cut-off (uts_blocks.out_of_service_plan)

  python scripts/eta_replay_cap.py data-local/headway_archive/headway "2026-09-27T08:00" data-local/eta_watch/20260927-*.jsonl

The cutoff is when the hop-time history is frozen (use the morning of the logs' day so nothing leaks in).
Prints overall and per-route accuracy for each variant; compare the Orange (55) lines on a Sunday.
Logs older than 2026-09-21 carry no BlockId, so Sunday Orange/Green are assumed to be blocks [05]/[01].
Same caveats as eta_replay.py: no median-of-3 smoothing, so absolute numbers differ a little from live.
"""
import json
import statistics as st
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import eta_replay as er  # noqa: E402  (also puts ROOT on sys.path)
import bus_eta  # noqa: E402
import eta_compare as ec  # noqa: E402
import uts_blocks  # noqa: E402

MPH = er.MPH_TO_MPS


def replay(polls, lines, variants):
    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _ in p["veh"]:
            tracks.setdefault((veh, route), []).append((p["t"], lat, lon))
    unwrapped = ec.build_unwrapped(tracks, ec_lines)
    ema = {}
    out = {n: [] for n in variants}
    for p in polls:
        block_of = {x[2]: x[5] for x in p["ours"] if len(x) > 5 and x[5]}
        rows = {n: [] for n in variants}
        for veh, route, lat, lon, mph in p["veh"]:
            line = lines.get(route)
            u = unwrapped.get((veh, route))
            if line is None or not u:
                continue
            v = max(0.0, (mph or 0.0) * MPH)
            e = v if (veh, route) not in ema else 0.4 * v + 0.6 * ema[(veh, route)]
            ema[(veh, route)] = e
            e = max(1.2, min(22.0, e))
            if e >= 22.0:
                e = bus_eta.TYPICAL_BUS_SPEED_MPS
            s_pos = ec.position_at(u, p["t"]) % line.shape_cum[-1]
            # 2026-09-20 logs predate the BlockId field; on a Sunday there is exactly one Orange (block [05]) and one
            # Green (block [01]) on the road, so those can be filled in. Gold (two buses, 09/11) stays unknown.
            blk = block_of.get(veh) or {"55": "[05]", "54": "[01]"}.get(route)
            for stop in line.stops:
                if stop.arc_pos is None:
                    continue
                for n, kw in variants.items():
                    r = bus_eta.estimate_stop_eta_s(
                        line, s_pos, e, stop, when=p["t"], vehicle_lat=lat, vehicle_lon=lon,
                        vehicle_dir_sign=1, vehicle_block_id=blk, **kw)
                    if r is not None:
                        rows[n].append([route, stop.id, veh, round(r.seconds, 1), r.source])
        for n in variants:
            out[n].append({"t": p["t"], "veh": p["veh"], "ours": rows[n], "tl": p["tl"]})
    return out


def stats(errs):
    if not errs:
        return "n=0"
    a = [abs(x) for x in errs]
    n = len(errs)
    return (f"n={n:<6} median {st.median(errs):+5.0f}s  |err| {st.median(a):4.0f}s  within 1m {100*sum(x<=60 for x in a)/n:3.0f}%  "
            f"late>2m {100*sum(x>120 for x in errs)/n:4.1f}%  early>2m {100*sum(x<-120 for x in errs)/n:4.1f}%")


if __name__ == "__main__":
    headway_dir, cutoff_s, *logs = sys.argv[1:]
    import glob
    logs = sorted({f for a in logs for f in (glob.glob(a) or [a])})  # cmd.exe does not expand *.jsonl
    cutoff = datetime.fromisoformat(cutoff_s).replace(tzinfo=er.NY)
    print("building history...", flush=True)
    m = er.build_models(headway_dir, cutoff)
    lines = er.load_lines()
    ec_stops, ec_lines = ec.load_graph()
    old_cap = lambda r, s, w: uts_blocks.timestop_code_for_stop(r, s) is not None  # noqa: E731
    hold = uts_blocks.scheduled_hold_epoch
    hop = m["old_res"].lookup
    variants = {
        "old": dict(hop_time_fn=hop, is_timestop_fn=old_cap, scheduled_timestop_fn=hold),
        "tod": dict(hop_time_fn=hop, is_timestop_fn=uts_blocks.is_timestop_at, scheduled_timestop_fn=hold),
        "tod_oos": dict(hop_time_fn=hop, is_timestop_fn=uts_blocks.is_timestop_at, scheduled_timestop_fn=hold,
                        out_of_service_fn=uts_blocks.out_of_service_plan),
    }
    names = ("live",) + tuple(variants)
    allrows = {n: [] for n in names}
    for path in logs:
        polls = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
        rep = replay(polls, lines, variants)
        per = {"live": {(r["t"], r["key"]): r for r in ec.score_rows(polls, ec_stops, ec_lines)}}
        for n, pp in rep.items():
            per[n] = {(r["t"], r["key"]): r for r in ec.score_rows(pp, ec_stops, ec_lines)}
        rows_run = {n: [] for n in names}
        # only compare predictions that exist under every variant (the cut-off may drop some on purpose)
        dropped = 0
        for k, row in per["live"].items():
            if row["ours"] is None or row["tl"] is None:
                continue
            if not all(k in per[n] and per[n][k]["ours"] is not None for n in variants if n != "tod_oos"):
                continue
            if k not in per["tod_oos"] or per["tod_oos"][k]["ours"] is None:
                dropped += 1
                for n in names:
                    if n in per and k in per[n]:
                        pass
                continue
            for n in names:
                rows_run[n].append(dict(per[n][k], tl=row["tl"], veh=k[1][2], route=k[1][0], t=k[0]))
        print(f"\n### {Path(path).name}   predictions dropped by cut-off (no longer shown): {dropped}")
        for n in names:
            print(f"  {n:<8}", stats([r['ours'] for r in rows_run[n]]))
            allrows[n] += rows_run[n]
        for rt in sorted({r["route"] for r in rows_run["live"]}):
            print(f"  route {rt}:")
            for n in ("live", "old", "tod"):
                print(f"     {n:<5}", stats([r["ours"] for r in rows_run[n] if r["route"] == rt]))
        # per-vehicle for worst live vehicle
        worst = {}
        for r in rows_run["live"]:
            if r["ours"] > 120:
                worst[r["veh"]] = worst.get(r["veh"], 0) + 1
        for veh, c in sorted(worst.items(), key=lambda x: -x[1])[:2]:
            print(f"  -- vehicle {veh} (live late>2m rows {c}):", end="")
            for n in names:
                e = [r["ours"] for r in rows_run[n] if r["veh"] == veh]
                print(f"  {n}: {100*sum(x>120 for x in e)/len(e):.0f}% late", end="")
            print()
    print("\n=== ALL LOGS ===")
    for n in names:
        print(f"  {n:<8}", stats([r['ours'] for r in allrows[n]]))
