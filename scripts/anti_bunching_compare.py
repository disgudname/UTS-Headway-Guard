"""Does Purple do what TransLoc's anti-bunching guidance says? One service day's report.

Pulls the day's guidance log (/data/anti_bunching/<date>.jsonl, written by anti_bunching_log.py) and
headway events (/data/headway/<UTC date>.csv, the day's and the next UTC day's, since Purple runs
past 00:00Z) off the Fly volume, then for every Purple stop visit of 60 s or more (a hold) asks:

- what TransLoc said when the bus pulled in: Adjustment (s) and Severity;
- what it said when the bus left (did it leave on Green, or against Red/Yellow);
- how well two readings of the guidance predict the departure: "hold for Adjustment seconds" and
  "leave when the bus's Severity turns Green", next to the typical hold at that stop;
- how the real gaps between buses compare with TransLoc's TargetGap, at Lower JPA and Lane.

  python scripts/anti_bunching_compare.py [YYYY-MM-DD] [--no-pull]

Files land in data-local/anti_bunching/ (git-ignored); the report is printed and saved next to them.
"""
import bisect
import collections
import csv
import json
import os
import statistics as st
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data-local" / "anti_bunching"
MIN_HOLD_S = 60.0
STAGING = ("West Complex", "Lane Rd")  # the sheet's staging stops (Lower JPA, Lane)


def pull(remote: str, local: Path) -> bool:
    local.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, MSYS_NO_PATHCONV="1")
    r = subprocess.run(["fly", "ssh", "sftp", "get", remote, str(local)], cwd=ROOT, env=env,
                       capture_output=True, text=True, timeout=300)
    if r.returncode != 0 or not local.exists():
        print(f"  could not pull {remote}: {(r.stderr or r.stdout).strip()[:200]}")
        return False
    return True


def load_guidance(path: Path):
    """vehicle name -> sorted [(start, end, adjustment, severity, target_gap, route_id)]."""
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_route = collections.defaultdict(list)
    for r in rows:
        by_route[r["route_id"]].append(r)
    out = collections.defaultdict(list)
    for rid, rs in by_route.items():
        rs.sort(key=lambda r: r["t"])
        for i, r in enumerate(rs):
            end = rs[i + 1]["t"] if i + 1 < len(rs) else r["t"] + 600
            for vid, name, adj, sev in r["vehicles"]:
                out[name].append((r["t"], end, adj, sev, r["target_gap_s"], rid))
    for v in out.values():
        v.sort()
    return out


def guidance_at(intervals, t):
    i = bisect.bisect_right([iv[0] for iv in intervals], t) - 1
    if i >= 0 and intervals[i][0] <= t < intervals[i][1]:
        return intervals[i]
    return None


def green_from(intervals, t):
    """First time at or after t the bus's Severity is Green (None if never while logged)."""
    for start, end, _adj, sev, _gap, _rid in intervals:
        if end <= t:
            continue
        if sev == "Green":
            return max(start, t)
    return None


def load_visits(paths, day):
    """Purple stop visits on the local service day: (stop, bus name, route, arrive, depart)."""
    ev = collections.defaultdict(list)
    for p in paths:
        if not p.exists():
            continue
        with p.open(encoding="utf-8") as fh:
            for row in csv.reader(fh):
                if len(row) < 10 or "Purple Line" not in row[2]:
                    continue
                t = datetime.fromisoformat(row[0].replace("Z", "+00:00")).timestamp()
                if datetime.fromtimestamp(t, NY).strftime("%Y-%m-%d") != day:
                    continue
                ev[row[7] or row[6]].append((t, row[5], row[9], row[1]))
    visits = []
    for bus, es in ev.items():
        es.sort()
        i = 0
        while i < len(es):
            j = i
            while j + 1 < len(es) and es[j + 1][1] == es[i][1]:
                j += 1
            arr = [e[0] for e in es[i:j + 1] if e[2] == "arrival"]
            dep = [e[0] for e in es[i:j + 1] if e[2] == "departure"]
            if arr and dep and dep[-1] > arr[0]:
                visits.append((es[i][1], bus, es[i][3], arr[0], dep[-1]))
            i = j + 1
    return visits


def q(xs):
    if not xs:
        return "n=0"
    qs = st.quantiles(xs) if len(xs) > 1 else [xs[0]] * 3
    return f"median {st.median(xs):+.0f}s (middle half {qs[0]:+.0f}..{qs[2]:+.0f}, n={len(xs)})"


def report(day, guidance, visits):
    lines = [f"Purple vs TransLoc anti-bunching guidance, {day}"]
    holds = [v for v in visits if v[4] - v[3] >= MIN_HOLD_S]
    typical = {s: st.median([v[4] - v[3] for v in holds if v[0] == s]) for s in {v[0] for v in holds}}
    covered = [(v, guidance.get(v[1], [])) for v in holds]
    covered = [(v, g) for v, g in covered if guidance_at(g, v[3]) is not None]
    lines.append(f"holds of {MIN_HOLD_S:.0f}+ s: {len(holds)}; with guidance logged at arrival: {len(covered)}")
    if not covered:
        lines.append("nothing to compare (no guidance logged, or no Purple service)")
        return "\n".join(lines)

    by_stop = collections.defaultdict(list)
    for v, g in covered:
        by_stop[v[0]].append((v, g))
    for stop, items in sorted(by_stop.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"\n== {stop} ({len(items)} holds, typical {typical[stop]:.0f} s) ==")
        sev_arr = collections.Counter(guidance_at(g, v[3])[3] for v, g in items)
        sev_dep = collections.Counter((guidance_at(g, v[4] - 1) or (None, None, None, "none"))[3] for v, g in items)
        lines.append(f"  severity when it pulled in: {dict(sev_arr)}")
        lines.append(f"  severity when it left:      {dict(sev_dep)}")
        adj = [guidance_at(g, v[3])[2] for v, g in items]
        hold = [v[4] - v[3] for v, _ in items]
        lines.append(f"  TransLoc Adjustment at arrival: {q([a for a in adj if a is not None])}; actual hold {q(hold)}")
        err_adj = [(v[4] - v[3]) - a for (v, _), a in zip(items, adj) if a is not None]
        lines.append(f"  actual hold minus Adjustment: {q(err_adj)}")
        gr = [(green_from(g, v[3]), v) for v, g in items]
        err_green = [v[4] - t for t, v in gr if t is not None]
        lines.append(f"  left after the bus's guidance turned Green: {q(err_green)}")
        e_adj = [abs(x) for x in err_adj]
        e_green = [abs(x) for x in err_green]
        e_typ = [abs((v[4] - v[3]) - typical[stop]) for v, _ in items]
        lines.append(f"  predicting the departure, median |error|: hold Adjustment {st.median(e_adj) if e_adj else float('nan'):.0f} s, "
                     f"leave on Green {st.median(e_green) if e_green else float('nan'):.0f} s, typical hold {st.median(e_typ):.0f} s")
        if len(adj) > 3:
            try:
                lines.append(f"  correlation Adjustment vs actual hold: {st.correlation([float(a) for a in adj if a is not None], [float(v[4] - v[3]) for (v, _), a in zip(items, adj) if a is not None]):+.2f}")
            except st.StatisticsError:
                pass

    lines.append("\n== spacing: gaps between consecutive Purple departures vs TransLoc's TargetGap ==")
    for key in STAGING:
        deps = sorted([v for v in visits if key in v[0]], key=lambda v: v[4])
        rows = []
        for a, b in zip(deps, deps[1:]):
            if a[1] == b[1] or b[4] - a[4] > 3600:
                continue
            g = guidance_at(guidance.get(b[1], []), b[4])
            if g and g[4]:
                rows.append(((b[4] - a[4]) - g[4], b[4] - a[4], g[4]))
        if rows:
            lines.append(f"  {key}: actual gap {q([r[1] for r in rows])}; TargetGap {q([r[2] for r in rows])}; "
                         f"actual minus target {q([r[0] for r in rows])}")
    return "\n".join(lines)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    day = args[0] if args else (datetime.now(NY) - timedelta(hours=4)).strftime("%Y-%m-%d")
    d = datetime.strptime(day, "%Y-%m-%d").date()
    gpath = OUT / f"{day}.jsonl"
    hpaths = [OUT / "headway" / f"{x.isoformat()}.csv" for x in (d, d + timedelta(days=1))]
    if "--no-pull" not in sys.argv:
        pull(f"/data/anti_bunching/{day}.jsonl", gpath)
        for p in hpaths:
            pull(f"/data/headway/{p.name}", p)
    if not gpath.exists():
        print(f"no guidance log for {day}")
        return 1
    text = report(day, load_guidance(gpath), load_visits(hpaths, day))
    print(text)
    (OUT / f"report_{day}.txt").write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
