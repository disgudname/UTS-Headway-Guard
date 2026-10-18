"""Offline replay of Purple Line ETAs: the live dwell-mode engine with and without purple_release.

Feeds every Purple bus position in eta_watch logs back through bus_eta.estimate_stop_eta_s the way
app.py's _compute_bus_eta_arrivals does for dwell-mode routes (driving-only hops, 40th-percentile
dwell minus the time already sat at the stop), built only from headway events before `cutoff`,
then once more with staged buses released by the bus ahead (purple_release.release_epochs), and
scores both against the real arrivals with eta_compare's scorer. Each log is read against the
route graph saved next to it.

  python scripts/eta_replay_purple.py <headway_dir> "2026-10-01T00:00" <log.jsonl> [...]
"""
import json
import os
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
import purple_release  # noqa: E402
import trip_planner_history as tph  # noqa: E402
from headway_storage import HeadwayStorage  # noqa: E402
from trip_planner import Line, Stop  # noqa: E402

NY = ZoneInfo("America/New_York")
MPH_TO_MPS = 0.44704
ZONE_MAX_GAP_S = 90.0  # app.py _BUS_ETA_STOP_ZONE_MAX_GAP_S


def build_models(headway_dir, cutoff):
    drive, dwell = tph.build_drive_and_dwell_samples(HeadwayStorage(Path(headway_dir)), now=cutoff)
    buckets = {k: {"seconds": st.median(v), "samples": len(v)} for k, v in drive.items() if len(v) >= tph.MIN_SAMPLES}
    return tph.HopTimeModel(buckets), tph.DwellModel.from_samples(dwell, tph.DWELL_QUANTILE)


def lines_from(log):
    g = json.loads(Path(str(log) + ".graph.json").read_text(encoding="utf-8"))
    out = {}
    for e in g["lines"]:
        if not e["name"].startswith("Purple Line"):
            continue
        stops = [Stop(id=s["id"], name=s["name"], lat=s["lat"], lon=s["lon"], source="uts", arc_pos=s.get("arc_pos"))
                 for s in e["stops"]]
        out[str(e["id"])] = Line(id=str(e["id"]), name=e["name"], color="888888", source="uts", stops=stops,
                                 loop=True, shape=[tuple(p) for p in e["poly"]], shape_cum=list(e["cum"]))
    return out


def replay(polls, lines, ec_lines, drive, dwell, use_release):
    tracks = {}
    for p in polls:
        for veh, route, lat, lon, _ in p["veh"]:
            if route in lines:
                tracks.setdefault((veh, route), []).append((p["t"], lat, lon))
    unwrapped = ec.build_unwrapped(tracks, ec_lines)
    ema, zones, out = {}, {}, []
    released = set()
    for p in polls:
        now = p["t"]
        buses = []
        for veh, route, lat, lon, mph in p["veh"]:
            line = lines.get(route)
            u = unwrapped.get((veh, route))
            if line is None or not u:
                continue
            v = max(0.0, mph * MPH_TO_MPS)
            e = v if (veh, route) not in ema else 0.4 * v + 0.6 * ema[(veh, route)]
            ema[(veh, route)] = e
            e = max(1.2, min(22.0, e))
            s_pos = ec.position_at(u, now) % line.shape_cum[-1]
            at = bus_eta.stop_zone_at(line, lat, lon)
            prev = zones.get((veh, route))
            if at is None:
                zones.pop((veh, route), None)
                since = now
            else:
                since = prev[1] if prev and prev[0] == at and now - prev[2] <= ZONE_MAX_GAP_S else now
                zones[(veh, route)] = (at, since, now)
            base_dwell = bus_eta.elapsed_dwell_fn(dwell.lookup, at, now - since, now)

            def eta(stop, release, _l=line, _s=s_pos, _e=e, _lat=lat, _lon=lon, _d=base_dwell):
                d = _d if release is None else bus_eta.release_dwell_fn(_d, release[0], release[1], now)
                r = bus_eta.estimate_stop_eta_s(_l, _s, _e, stop, drive.lookup, now, dwell_fn=d,
                                                vehicle_lat=_lat, vehicle_lon=_lon, vehicle_dir_sign=1)
                return None if r is None else r.seconds

            buses.append((route, purple_release.Bus(vid=veh, stops=line.stops, at_stop_id=at, since=since, eta=eta)))
        releases = purple_release.release_epochs([b for _, b in buses], now) if use_release else {}
        released |= {(now, vid) for vid in releases}
        rows = []
        for route, bus in buses:
            for stop in lines[route].stops:
                if stop.arc_pos is None:
                    continue
                sec = bus.eta(stop, releases.get(bus.vid))
                if sec is not None:
                    rows.append([route, stop.id, bus.vid, round(sec, 1), "replay"])
        out.append({"t": now, "veh": p["veh"], "ours": rows, "tl": p["tl"]})
    return out, released


def stats(errs):
    if not errs:
        return "n=0"
    a = [abs(x) for x in errs]
    n = len(errs)
    return (f"n={n:<6} median {st.median(errs):+5.0f}s  |err| {st.median(a):4.0f}s  within 1m {100 * sum(x <= 60 for x in a) / n:3.0f}%  "
            f"2m {100 * sum(x <= 120 for x in a) / n:3.0f}%  late>2m {100 * sum(x > 120 for x in errs) / n:4.1f}%")


def main():
    headway_dir, cutoff_s, *logs = sys.argv[1:]
    shift = float(os.environ.get("PURPLE_RELEASE_SHIFT_S", "0"))  # try the rules' lags moved earlier/later
    if shift:
        purple_release._rules_cache = [purple_release.Rule(r.stage, r.trigger, r.first_hour, r.last_hour, r.offset_s + shift)
                                       for r in purple_release.load_rules()]
        print(f"rule lags shifted {shift:+.0f} s", flush=True)
    cutoff = datetime.fromisoformat(cutoff_s).replace(tzinfo=NY)
    print(f"building history from events before {cutoff} ...", flush=True)
    drive, dwell = build_models(headway_dir, cutoff)
    scored = {"live": [], "base": [], "release": []}
    touched = {"live": [], "base": [], "release": []}
    for path in logs:
        polls = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
        lines = lines_from(path)
        if not lines:
            continue
        _ec_stops, ec_lines = ec.load_graph(path)
        ec_stops = {k: v for k, v in _ec_stops.items() if k[0] in lines}
        base, _ = replay(polls, lines, ec_lines, drive, dwell, False)
        rel, n_rel = replay(polls, lines, ec_lines, drive, dwell, True)
        per = {
            "live": {(r["t"], r["key"]): r for r in ec.score_rows(polls, ec_stops, ec_lines)},
            "base": {(r["t"], r["key"]): r for r in ec.score_rows(base, ec_stops, ec_lines)},
            "release": {(r["t"], r["key"]): r for r in ec.score_rows(rel, ec_stops, ec_lines)},
        }
        kept = 0
        for k, row in per["base"].items():
            if k in per["release"] and k in per["live"] and row["ours"] is not None and per["release"][k]["ours"] is not None \
                    and per["live"][k]["ours"] is not None:
                kept += 1
                for n in scored:
                    scored[n].append(dict(per[n][k], log=Path(path).name, remaining=row["remaining"]))
                if (k[0], k[1][2]) in n_rel:
                    for n in touched:
                        touched[n].append(dict(per[n][k], log=Path(path).name, remaining=row["remaining"]))
        print(f"  {Path(path).name}: {kept} rows, {len(n_rel)} bus-polls released", flush=True)

    def block(title, by):
        print(f"\n=== {title} ===")
        for n, rows in by.items():
            print(f"  {n:<8}", stats([r["ours"] for r in rows]))

    block("all Purple predictions", scored)
    block("predictions from a bus the rule released (staged at Lower JPA / Lane)", touched)
    for lo, hi, label in ec.BUCKETS:
        block(f"released, {label}", {n: [r for r in rows if lo <= r["remaining"] < hi] for n, rows in touched.items()})
    for lo, hi, label in ec.BUCKETS:
        block(label, {n: [r for r in rows if lo <= r["remaining"] < hi] for n, rows in scored.items()})
    for slot in sorted({r["log"][9:13] for r in scored["base"]}):
        block(f"runs at {slot}", {n: [r for r in rows if r["log"][9:13] == slot] for n, rows in scored.items()})


if __name__ == "__main__":
    main()
