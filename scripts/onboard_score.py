"""How did the live /onboard signs do? Scores one service day of the /onboard log against what the buses did.

Pulls /data/onboard_log/<date>.jsonl (written by onboard_log.py on every live GET /v1/onboard) off the
Fly volume, then for each bus:

- ETAs: every ETA the sign was given, scored with eta_compare's scorer against the bus's own track (its
  TransLoc fixes in the same log). Error > 0 = the bus came earlier than shown (the miss-the-bus side).
- "Now at": each time the sign said the bus had stopped at a stop, did the bus really pass that stop
  within a couple of minutes? A miss there is a wrong-pass / twin-stop announcement.
- Tablet GPS: how often the sign used the tablet's fix, and how far it sat from TransLoc's.
- Announcements (from the tablet's own events, <date>.events.jsonl): when "This is X" played against
  when the bus really reached X (its 1 Hz GPS track from <date>.sensors.jsonl), and how soon after the bus
  stopped "The next stop is Y" played.
- Motion: how much the accelerometer buzzed while GPS said stopped vs moving, to set the page's
  still_sd / move_sd thresholds from real rides.

  python scripts/onboard_score.py [YYYY-MM-DD] [--bus 18232] [--no-pull]

The date is the service day (it runs to 04:00), default today's. Files land in data-local/onboard_log/
(git-ignored). The stop graph is saved next to the log on first scoring, so score a day soon after it
ran: TransLoc renumbers stops when the service variant changes.
"""
import argparse
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eta_compare  # noqa: E402
from anti_bunching_compare import pull  # noqa: E402

NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data-local" / "onboard_log"
NOW_AT_EARLY_S = 120.0   # "now at" said this long before the bus's pass of the stop still counts
NOW_AT_LATE_S = 60.0     # ... or this long after it
BUCKETS = [(0, 60, "<1 min"), (60, 180, "1-3 min"), (180, 600, "3-10 min"), (600, 1200, "10-20 min"), (1200, 1e9, "20+ min")]


def to_polls(recs):
    """{bus: eta_compare-style polls} from log lines: our ETAs + the bus's TransLoc fix per line."""
    out = defaultdict(list)
    for r in recs:
        if r.get("status") != "in_service" or not r.get("veh") or not r.get("tl") or not r.get("route"):
            continue
        veh, route = r["veh"], str(r["route"])
        lat, lon, mps = r["tl"][0], r["tl"][1], r["tl"][2]
        ours = [[route, str(sid), veh, sec, None, r.get("block")] for sid, sec in r.get("eta") or [] if sec is not None]
        out[r["bus"]].append({"t": r["t"], "ours": ours, "tl": [], "veh": [[veh, route, lat, lon, mps]]})
    return out


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))] if xs else None


def clock(t):
    return datetime.fromtimestamp(t, NY).strftime("%H:%M:%S")


def now_at_check(recs, stops, lines):
    """[(t, route, stop, veh, ok, nearest pass offset s or None)] -- one per 'now at' episode."""
    episodes, prev = [], None
    for r in recs:
        at = r.get("at")
        key = (r.get("route"), at[0]) if at and at[1] else None
        if key and key != prev and r.get("veh"):
            episodes.append((r["t"], str(r["route"]), str(at[0]), r["veh"]))
        prev = key
    tracks = defaultdict(list)
    for r in recs:
        if r.get("veh") and r.get("tl") and r.get("route"):
            tracks[(r["veh"], str(r["route"]))].append((r["t"], r["tl"][0], r["tl"][1]))
    wanted = {(route, stop, veh) for _t, route, stop, veh in episodes}
    crossings = eta_compare.find_crossings(tracks, stops, lines, wanted)
    last_seen = {k: max(x[0] for x in v) for k, v in tracks.items()}
    out = []
    for t, route, stop, veh in episodes:
        passes = crossings.get((route, stop, veh), [])
        if not passes and last_seen.get((veh, route), 0.0) < t + NOW_AT_EARLY_S:
            continue  # the log ends before the bus could have left: can't judge it
        nearest = min((p - t for p in passes), key=abs, default=None)
        ok = nearest is not None and -NOW_AT_LATE_S <= nearest <= NOW_AT_EARLY_S
        out.append((t, route, stop, veh, ok, nearest))
    return out


ARRIVE_M = 30.0          # "reached the stop" = this close short of it along the route (eta_compare.ARRIVE_M)
STOP_MPS = 0.5           # 1 Hz GPS at or under this near a stop: the bus is standing there
STOP_NEAR_M = 45.0


def device_tracks(sensors, recs):
    """{route: [(t, lat, lon, spd)]}: the tablet's 1 Hz GPS, each fix put on the route the sign had at the time."""
    routed = sorted((r["t"], str(r["route"])) for r in recs if r.get("route"))
    out = defaultdict(list)
    j = -1
    for s in sorted((s for s in sensors if s.get("lat") is not None and (s.get("acc") or 99) <= 30), key=lambda s: s["t"]):
        while j + 1 < len(routed) and routed[j + 1][0] <= s["t"]:
            j += 1
        if j >= 0 and s["t"] - routed[j][0] <= 60:
            out[routed[j][1]].append((s["t"], s["lat"], s["lon"], s.get("spd")))
    return out


def stop_time(track, stop, t_from, t_to):
    """First fix in [t_from, t_to] with the bus standing within STOP_NEAR_M of the stop, or None."""
    lat, lon = stop[0], stop[1]
    for t, a, b, spd in track:
        if t_from <= t <= t_to and spd is not None and spd <= STOP_MPS and eta_compare.haversine(a, b, lat, lon) <= STOP_NEAR_M:
            return t
    return None


def announcement_check(events, tracks, stops, lines):
    """{'this_is': [(t, route, stop, offset vs arrival s, offset vs standing s, d)], 'next_stop': [(t, route, from, stop,
    s after the bus stood at `from`)], 'stopped': [(t, route, stop, s after the GPS track shows it standing)]}."""
    trk = {("dev", r): [(t, a, b) for t, a, b, _s in v] for r, v in tracks.items()}
    wanted = {(str(e.get("route")), str(e.get("stop")), "dev") for e in events if e.get("stop") and e.get("route")}
    short = {k: (*v[:3], (v[3] - ARRIVE_M) % lines[k[0]][1][-1]) for k, v in stops.items() if k[0] in lines}
    arrivals = eta_compare.find_crossings(trk, short, lines, wanted)
    out = {"this_is": [], "next_stop": [], "stopped": []}
    for e in events:
        route, sid, t = str(e.get("route")), str(e.get("stop")), e["t"]
        st = stops.get((route, sid))
        track = tracks.get(route, [])
        if st is None or not track or not (track[0][0] <= t <= track[-1][0]):
            continue
        if e["kind"] == "this-is":
            arr = min(arrivals.get((route, sid, "dev"), []), key=lambda a: abs(a - t), default=None)
            stand = stop_time(track, st, t - 60, t + 120)
            out["this_is"].append((t, route, sid, None if arr is None or abs(arr - t) > 300 else t - arr,
                                   None if stand is None else t - stand, e.get("d"), e.get("src")))
        elif e["kind"] == "next-stop" and e.get("stopped") and e.get("from"):
            frm = stops.get((route, str(e["from"])))
            stand = stop_time(track, frm, t - 300, t + 5) if frm else None
            if stand is not None:
                out["next_stop"].append((t, route, str(e["from"]), sid, t - stand))
        elif e["kind"] == "stopped":
            stand = stop_time(track, st, t - 60, t + 60)
            out["stopped"].append((t, route, sid, None if stand is None else t - stand))
    return out


def motion_thresholds(sensors):
    """(still sd list, moving sd list): the page's vibration measure while 1 Hz GPS said stopped / driving."""
    still = [s["sd"] for s in sensors if s.get("n") and s.get("sd") is not None and s.get("spd") is not None and s["spd"] <= 0.2]
    moving = [s["sd"] for s in sensors if s.get("n") and s.get("sd") is not None and s.get("spd") is not None and s["spd"] >= 3]
    return still, moving


def secs(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "n=0"
    return f"n={len(xs)}, median {statistics.median(xs):+.1f}s, middle 80% {pct(xs, 10):+.1f}..{pct(xs, 90):+.1f}s"


def report(day, recs, stops, lines, only_bus=None, events=(), sensors=()):
    lines_out = []
    say = lines_out.append
    by_bus = defaultdict(list)
    for r in recs:
        by_bus[str(r.get("bus"))].append(r)
    if only_bus:
        by_bus = {b: v for b, v in by_bus.items() if b == only_bus}
    say(f"/onboard log, service day {day}: {sum(len(v) for v in by_bus.values())} lines, buses {', '.join(sorted(by_bus)) or 'none'}")
    for bus, rs in sorted(by_bus.items()):
        rs.sort(key=lambda r: r["t"])
        say("")
        say(f"=== Bus {bus}: {clock(rs[0]['t'])} - {clock(rs[-1]['t'])}, {len(rs)} polls ===")
        status = defaultdict(int)
        for r in rs:
            status[r.get("status")] += 1
        say("  status: " + ", ".join(f"{k} {v}" for k, v in sorted(status.items(), key=lambda kv: -kv[1])))
        routes = sorted({str(r["route"]) for r in rs if r.get("route")})
        missing = [rid for rid in routes if rid not in lines]
        if missing:
            say(f"  NOTE: route(s) {', '.join(missing)} not in the saved stop graph (renumbered since?): not scored")

        # Tablet GPS
        src = defaultdict(int)
        for r in rs:
            if r.get("src"):
                src[r["src"]] += 1
        with_dev = [r for r in rs if r.get("dev")]
        say(f"  position used: " + (", ".join(f"{k} {v}" for k, v in src.items()) or "none")
            + f"; tablet sent GPS on {len(with_dev)} of {len(rs)} polls")
        offs = []
        for r in with_dev:
            if r.get("tl"):
                offs.append(eta_compare.haversine(r["dev"][0], r["dev"][1], r["tl"][0], r["tl"][1]))
        if offs:
            say(f"  tablet vs TransLoc: median {statistics.median(offs):.0f} m, 90th pct {pct(offs, 90):.0f} m, "
                f"max {max(offs):.0f} m, over 100 m on {sum(1 for o in offs if o > 100)} polls")

        # ETAs
        polls = to_polls(rs).get(bus, [])
        scored = eta_compare.score_rows(polls, stops, lines) if polls else []
        scored = [x for x in scored if x["ours"] is not None]
        say(f"  ETAs: {sum(len(p['ours']) for p in polls)} given, {len(scored)} scoreable (bus later seen at the stop)")
        if scored:
            say(eta_compare.summarize("all", [x["ours"] for x in scored]))
            for lo, hi, label in BUCKETS:
                sub = [x["ours"] for x in scored if lo <= x["remaining"] < hi]
                if sub:
                    say(eta_compare.summarize(label, sub))
            next_stop = []
            for p in polls:
                if p["ours"]:
                    k = tuple(p["ours"][0][:3])
                    next_stop += [x["ours"] for x in scored if x["t"] == p["t"] and x["key"] == k]
            if next_stop:
                say(eta_compare.summarize("next", next_stop) + "   <- the stop on the big line")
            say("  worst 8 (+ = bus came earlier than shown):")
            for x in sorted(scored, key=lambda x: -abs(x["ours"]))[:8]:
                route, stop, _veh = x["key"]
                name = stops.get((route, stop), (None, None, stop))[2]
                say(f"    {clock(x['t'])} {name}: shown {x['ours_s']:.0f}s, really {x['remaining']:.0f}s -> {x['ours']:+.0f}s")

        # Now at
        checks = now_at_check(rs, stops, lines)
        if checks:
            bad = [c for c in checks if not c[4]]
            say(f"  'Now at' (stopped) shown {len(checks)} times, {len(bad)} not matched to the bus passing that stop:")
            for t, route, stop, _veh, _ok, nearest in bad[:10]:
                name = stops.get((route, stop), (None, None, stop))[2]
                when = f"nearest pass {nearest:+.0f}s" if nearest is not None else "bus never seen passing it"
                say(f"    {clock(t)} {name} ({when})")

        # The tablet's own announcements, timed against its 1 Hz GPS track
        ev = sorted((e for e in events if str(e.get("bus")) == bus and "t" in e), key=lambda e: e["t"])
        sn = [s for s in sensors if str(s.get("bus")) == bus]
        if not ev and not sn:
            continue
        kinds = defaultdict(int)
        for e in ev:
            kinds[e.get("kind")] += 1
        say(f"  tablet events: " + (", ".join(f"{k} {v}" for k, v in sorted(kinds.items())) or "none")
            + f"; {len(sn)} sensor seconds ({sum(1 for s in sn if s.get('n'))} with motion)")
        tracks = device_tracks(sn, rs)
        chk = announcement_check(ev, tracks, stops, lines)
        if chk["this_is"]:
            say("  'This is X' (- = before the bus reached X, 30 m short of it):")
            say("    vs reaching the stop:  " + secs([x[3] for x in chk["this_is"]]))
            say("    vs standing at it:     " + secs([x[4] for x in chk["this_is"]]))
            src = defaultdict(int)
            for x in chk["this_is"]:
                src[x[6]] += 1
            say("    decided by: " + ", ".join(f"{k} {v}" for k, v in src.items()))
            for x in sorted(chk["this_is"], key=lambda x: -abs(x[3] or 0))[:6]:
                if x[3] is not None and abs(x[3]) > 15:
                    name = stops.get((x[1], x[2]), (None, None, x[2]))[2]
                    say(f"    off: {clock(x[0])} {name} {x[3]:+.0f}s from reaching it, {x[5]} m away, by {x[6]}")
        if chk["next_stop"]:
            say("  'The next stop is Y' after the bus stood at the stop before: " + secs([x[4] for x in chk["next_stop"]]))
        if chk["stopped"]:
            say("  page's 'stopped' vs the GPS track standing: " + secs([x[3] for x in chk["stopped"]]))
        still, moving = motion_thresholds(sn)
        if still and moving:
            say(f"  motion sd (m/s^2), GPS stopped: median {statistics.median(still):.3f}, 90th pct {pct(still, 90):.3f} (n={len(still)});"
                f" driving: 10th pct {pct(moving, 10):.3f}, median {statistics.median(moving):.3f} (n={len(moving)})")
            say("    the page uses still_sd 0.08 / move_sd 0.25 unless ?still_sd= / ?move_sd= say otherwise")
    return "\n".join(lines_out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("date", nargs="?")
    ap.add_argument("--bus")
    ap.add_argument("--no-pull", action="store_true")
    args = ap.parse_args()
    day = args.date or (datetime.now(NY) - timedelta(hours=4)).strftime("%Y-%m-%d")
    local = OUT / f"{day}.jsonl"
    if not args.no_pull:
        print(f"pulling /data/onboard_log/{day}.jsonl ...")
        local.with_suffix(".tmp").unlink(missing_ok=True)
        if not pull(f"/data/onboard_log/{day}.jsonl", local.with_suffix(".tmp")):
            if not local.exists():
                sys.exit(1)
        else:
            local.with_suffix(".tmp").replace(local)
    extra = {}
    for kind in ("events", "sensors"):
        path = OUT / f"{day}.{kind}.jsonl"
        if not args.no_pull:
            path.with_suffix(".tmp").unlink(missing_ok=True)
            if pull(f"/data/onboard_log/{day}.{kind}.jsonl", path.with_suffix(".tmp")):
                path.with_suffix(".tmp").replace(path)
        extra[kind] = [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()] if path.exists() else []
    recs = [json.loads(x) for x in local.read_text(encoding="utf-8").splitlines() if x.strip()]
    stops, lines = eta_compare.load_graph(local)
    text = report(day, recs, stops, lines, args.bus, extra["events"], extra["sensors"])
    print(text)
    (OUT / f"{day}{'-' + args.bus if args.bus else ''}.report.txt").write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
