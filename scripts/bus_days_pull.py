"""Saves each day's /v1/servicecrew answer (which blocks every bus touched, and its miles) from prod as
data-local/bus_days/<yyyy-mm-dd>.blocks.json. Resumable (skips days already saved); prod has these back to 2025-09.
scripts/build_block_cards.py reads them for the bus cards.

  python scripts/bus_days_pull.py 2025-08-28 2026-10-05
"""
import concurrent.futures
import datetime
import sys
import urllib.request
from pathlib import Path

BASE = "https://uts-headway-guard.fly.dev"
OUT = Path(__file__).resolve().parents[1] / "data-local" / "bus_days"


def pull(day):
    path = OUT / f"{day}.blocks.json"
    if path.exists():
        return None
    for _ in range(4):
        try:
            with urllib.request.urlopen(f"{BASE}/v1/servicecrew?date={day}", timeout=60) as r:
                path.write_bytes(r.read())
            return None
        except Exception as exc:
            error = exc
    return f"{day} failed: {error}"


def main():
    d0, d1 = (datetime.date.fromisoformat(a) for a in sys.argv[1:3])
    OUT.mkdir(parents=True, exist_ok=True)
    days = [d0 + datetime.timedelta(days=i) for i in range((d1 - d0).days + 1)]
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        for problem in pool.map(pull, days):
            if problem:
                print(problem, flush=True)
    print("done,", len(list(OUT.glob("*.blocks.json"))), "days on disk")


if __name__ == "__main__":
    main()
