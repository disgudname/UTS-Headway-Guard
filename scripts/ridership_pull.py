"""Pulls TransLoc's GetRidershipData (through prod's /v1/transloc/ridership) one day at a time and saves each day
gzipped as data-local/ridership/<yyyy-mm-dd>.json.gz, plus that day's bus -> block map from /v1/servicecrew as
<yyyy-mm-dd>.blocks.json. Resumable (skips days already saved). TransLoc times out on multi-day ranges, hence one day each.

  python scripts/ridership_pull.py 2026-08-20 2026-09-24
"""
import datetime
import gzip
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://uts-headway-guard.fly.dev"
OUT = Path(__file__).resolve().parents[1] / "data-local" / "ridership"


def get(url, timeout=170):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def main():
    d0 = datetime.date.fromisoformat(sys.argv[1])
    d1 = datetime.date.fromisoformat(sys.argv[2])
    OUT.mkdir(parents=True, exist_ok=True)
    d = d0
    while d <= d1:
        stamp = d.isoformat()
        blocks_path = OUT / f"{stamp}.blocks.json"
        if not blocks_path.exists():
            try:
                blocks_path.write_bytes(get(f"{BASE}/v1/servicecrew?date={stamp}", 60))
            except Exception as exc:
                print(stamp, "blocks failed", exc, flush=True)
        path = OUT / f"{stamp}.json.gz"
        if not path.exists():
            nxt = d + datetime.timedelta(days=1)
            q = urllib.parse.urlencode({"startDate": f"{d.month}/{d.day}/{d.year}", "endDate": f"{nxt.month}/{nxt.day}/{nxt.year}"})
            for attempt in range(1, 6):
                try:
                    raw = get(f"{BASE}/v1/transloc/ridership?{q}")
                    rows = json.loads(raw)
                    if not isinstance(rows, list):
                        raise ValueError(str(rows)[:100])
                    path.write_bytes(gzip.compress(raw))
                    print(stamp, "ok", len(rows), "rows", flush=True)
                    break
                except Exception as exc:
                    print(stamp, "attempt", attempt, "failed:", str(exc)[:100], flush=True)
                    time.sleep(5)
            else:
                print(stamp, "GAVE UP", flush=True)
        d += datetime.timedelta(days=1)
    print("done", flush=True)


if __name__ == "__main__":
    main()
