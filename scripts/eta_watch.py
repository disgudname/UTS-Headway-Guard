"""Log our ETAs, TransLoc's ETAs and real bus positions from prod for a while, so
scripts/eta_compare.py can score both against what the buses actually did.

    python scripts/eta_watch.py [minutes=30] [poll_seconds=15] [out=data-local/eta_watch/<timestamp>.jsonl]

One JSON line per poll: {"t": epoch, "ours": [[route, stop, veh, seconds, source], ...],
"tl": [[route, stop, veh, seconds], ...], "veh": [[veh, route, lat, lon, mps], ...]}.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

BASE = "https://uts-headway-guard.fly.dev"


def fetch(path):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "eta-watch"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def poll():
    t = time.time()
    ours, tl, veh = [], [], []
    try:
        for e in fetch("/v1/eta/uts_stop_arrivals")["arrivals"]:
            for x in e["Times"]:
                ours.append([str(e["RouteId"]), str(e["RouteStopId"]), str(x["VehicleId"]), x["Seconds"], x.get("Source")])
    except Exception as exc:
        print("ours failed:", exc)
    try:
        for e in fetch("/v1/transloc/stop_arrivals"):
            for x in e.get("Times", []):
                tl.append([str(e["RouteId"]), str(e["RouteStopId"]), str(x["VehicleId"]), x["Seconds"]])
    except Exception as exc:
        print("transloc failed:", exc)
    try:
        for v in fetch("/v1/testmap/transloc/vehicles")["vehicles"]:
            veh.append([str(v["VehicleID"]), str(v["RouteID"]), v["Latitude"], v["Longitude"], v.get("GroundSpeed")])
    except Exception as exc:
        print("vehicles failed:", exc)
    return {"t": t, "ours": ours, "tl": tl, "veh": veh}


def main():
    minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 30
    every = float(sys.argv[2]) if len(sys.argv) > 2 else 15
    out = Path(sys.argv[3]) if len(sys.argv) > 3 else Path("data-local/eta_watch") / f"{time.strftime('%Y%m%d-%H%M%S')}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    end = time.time() + minutes * 60
    n = 0
    print(f"logging to {out} for {minutes} min", flush=True)
    with out.open("a", encoding="utf-8") as f:
        while time.time() < end:
            started = time.time()
            rec = poll()
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            f.flush()
            n += 1
            if n % 10 == 0:
                print(f"{n} polls, {len(rec['veh'])} vehicles, {len(rec['ours'])} ours / {len(rec['tl'])} tl", flush=True)
            time.sleep(max(0.0, every - (time.time() - started)))
    print(f"done: {n} polls -> {out}", flush=True)


if __name__ == "__main__":
    main()
