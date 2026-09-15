"""Occasional batch job: rebuilds a DEEP historical hop-time model from the FULL
headway archive (not just the 60-day window trip_planner_history.py's own daily
refresh uses), correcting each event's stop_id via an accumulating
(route_id, address_id) -> RouteStopID table fetched from TransLoc's live route feed.

Why this is needed at all: headway_tracker.py's update_stops() used to collapse a
physical stop shared by multiple routes down to whichever route's RouteStopID it
happened to see first ("the first stop_id we see"), then stamped that borrowed ID
onto every route's arrival events regardless of which route was actually passing
through -- see StopPoint.route_stop_ids' docstring for the fix. That bug contaminated
essentially every recorded event for a shared stop before the fix shipped. This
script un-mixes that history rather than discarding it: `address_id` (the physical
location) was never affected by the bug, so relabeling each old event by
(route_id, address_id) through a table of *currently correct* RouteStopIDs recovers
the right stop_id for it. Verified against the real archive: before this correction,
only ~2% of checkable historical hop-time buckets matched a route's real live stop
sequence; after, ~98% did.

TransLoc's live route feed only ever lists routes in the *current* schedule phase
(a summer-only or single-semester route won't appear outside its season), so one
run's correction table only covers whatever's running right now. The table is
therefore accumulated across runs -- each run only ever ADDS/refreshes coverage,
never shrinks it -- so running this occasionally (weekly is plenty; route topology
doesn't change often) over a semester gradually covers every route that runs at all.

Run manually and occasionally -- this is NOT wired into the live app's own daily
refresh (trip_planner_history.py's LOOKBACK_DAYS=60 cache still refreshes itself
automatically, unaffected by this script):

    python build_eta_model.py                      # against /data (production)
    python build_eta_model.py --data-dir ./scratch  # against a local copy, for testing

Writes two files under <data-dir>:
  route_stop_corrections.json      -- accumulating (route,address)->RouteStopID
                                       table (see above).
  trip_planner_hop_times_deep.json -- corrected hop-time buckets built from the
                                       FULL local headway archive, same shape as
                                       trip_planner_history.py's own cache.
                                       HopTimeModel.load_model() reads this as a
                                       fallback for any bucket the live 60-day
                                       model hasn't accumulated MIN_SAMPLES for yet
                                       -- it never overrides fresher live data.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

from headway_storage import HeadwayStorage
import trip_planner_history as tph

TRANSLOC_ROUTES_URL = "https://uva.transloc.com/Services/JSONPRelay.svc/GetRoutesForMapWithScheduleWithEncodedLine"
TRANSLOC_API_KEY = "8882812681"

# Effectively "the whole archive" -- HeadwayStorage.query_events just skips
# day-files that don't exist, so an oversized lookback costs nothing but a few
# missed globs before the real data starts.
DEEP_LOOKBACK_DAYS = 3650


def fetch_live_corrections() -> Dict[str, str]:
    """"{route_id}|{address_id}" -> RouteStopID, for every stop of every route
    TransLoc currently considers in-schedule -- the same feed headway_tracker.py's
    own update_stops() consumes."""
    resp = httpx.get(TRANSLOC_ROUTES_URL, params={"APIKey": TRANSLOC_API_KEY}, timeout=30)
    resp.raise_for_status()
    routes = resp.json()
    out: Dict[str, str] = {}
    for route in routes or []:
        for stop in route.get("Stops") or []:
            rid = stop.get("RouteID")
            addr = stop.get("AddressID")
            rsid = stop.get("RouteStopID")
            if rid is None or addr is None or rsid is None:
                continue
            out[f"{rid}|{addr}"] = str(rsid)
    return out


def load_corrections(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("corrections", {})
    except (OSError, json.JSONDecodeError):
        return {}


def save_corrections(path: Path, corrections: Dict[str, str], now: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": now.isoformat(), "corrections": corrections}
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def build_deep_buckets(storage: HeadwayStorage, corrections: Dict[str, str], now: datetime) -> Dict[str, Any]:
    def resolve(ev) -> str:
        return corrections.get(f"{ev.route_id}|{ev.address_id}", ev.stop_id)

    samples = tph.build_hop_time_samples(
        storage, now=now, lookback_days=DEEP_LOOKBACK_DAYS, resolve_stop_id=resolve
    )
    return {
        key: {"seconds": statistics.median(values), "samples": len(values)}
        for key, values in samples.items()
        if len(values) >= tph.MIN_SAMPLES
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default="/data", help="Base data directory (default: /data)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    headway_dir = data_dir / "headway"
    corrections_path = data_dir / "route_stop_corrections.json"
    deep_cache_path = data_dir / "trip_planner_hop_times_deep.json"

    now = datetime.now(tph.NY_TZ)
    storage = HeadwayStorage(headway_dir)

    print("[build_eta_model] fetching live TransLoc route/stop data...")
    fresh = fetch_live_corrections()
    live_routes = {k.split("|", 1)[0] for k in fresh}
    print(f"[build_eta_model] {len(fresh)} (route,address) pairs from {len(live_routes)} currently-live routes: {sorted(live_routes, key=lambda x: int(x) if x.isdigit() else 0)}")

    corrections = load_corrections(corrections_path)
    before = len(corrections)
    corrections.update(fresh)
    save_corrections(corrections_path, corrections, now)
    print(f"[build_eta_model] correction table: {before} -> {len(corrections)} total (route,address) pairs known -> {corrections_path}")

    print(f"[build_eta_model] rebuilding deep hop-time model from {headway_dir} ...")
    buckets = build_deep_buckets(storage, corrections, now)
    tmp = deep_cache_path.with_suffix(".tmp")
    deep_cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps({"refreshed_at": now.isoformat(), "buckets": buckets}), encoding="utf-8")
    tmp.replace(deep_cache_path)
    print(f"[build_eta_model] wrote {len(buckets)} buckets -> {deep_cache_path}")


if __name__ == "__main__":
    main()
