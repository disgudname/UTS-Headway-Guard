"""Historical stop-to-stop travel time model for the trip planner, built from real
headway events instead of the flat SECONDS_PER_HOP_ESTIMATE heuristic in trip_planner.py.

Mirrors uva_athletics.py's caching pattern deliberately (same shape: `_load_cache` /
`_write_cache` / `is_cache_stale` / `ensure_*_cache`) -- refreshes once per day shortly
after 03:00 America/New_York, lazily on the first request after that threshold rather
than via a dedicated background task loop. No new polling infrastructure needed: this
reads local HeadwayStorage CSVs (one file per day, see headway_storage.py), which are
already being written by the app's existing headway tracking.

Why day-of-week and hour matter: travel time on a Sunday afternoon is nothing like a
Wednesday at 5pm (traffic, pedestrian crossings, event closures) -- a single average
across all days would be misleading in exactly the cases where accuracy matters most
(rush hour). Buckets are therefore (route_id, from_stop_id, to_stop_id, weekday, hour).
"""

from __future__ import annotations

import json
import os
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
REFRESH_HOUR_LOCAL = 3
LOOKBACK_DAYS = 60  # enough to smooth out noise without dragging in stale, pre-reroute data
MIN_SAMPLES = 3  # a bucket with fewer real samples than this isn't trusted
MAX_PLAUSIBLE_HOP_S = 3600.0  # guards against layovers/gaps being mistaken for one hop
CACHE_PATH = Path(os.getenv("TRIP_PLANNER_HOP_TIME_CACHE", "/data/trip_planner_hop_times.json"))


def _load_cache() -> Dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _write_cache(data: Dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = CACHE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    tmp_path.replace(CACHE_PATH)


def _cache_last_refreshed(cache: Dict[str, Any]) -> Optional[datetime]:
    ts = cache.get("refreshed_at") if isinstance(cache, dict) else None
    if not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=NY_TZ)
        return dt
    except ValueError:
        return None


def is_cache_stale(cache: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(NY_TZ)
    last_refresh = _cache_last_refreshed(cache)
    if last_refresh is None:
        return True
    refresh_today = now.replace(hour=REFRESH_HOUR_LOCAL, minute=0, second=0, microsecond=0)
    target_refresh = refresh_today if now >= refresh_today else refresh_today - timedelta(days=1)
    if last_refresh < target_refresh:
        return True
    if (now - last_refresh) > timedelta(days=1, hours=1):
        return True
    return False


def _bucket_key(route_id: str, from_stop_id: str, to_stop_id: str, weekday: int, hour: int) -> str:
    return f"{route_id}|{from_stop_id}|{to_stop_id}|{weekday}|{hour}"


def build_hop_time_samples(storage, now: Optional[datetime] = None) -> Dict[str, List[float]]:
    """Read the last LOOKBACK_DAYS of headway events and bucket real stop-to-stop
    travel-time samples by (route_id, from_stop_id, to_stop_id, weekday, hour).

    Two consecutive "arrival" events sharing the same `block` (one physical vehicle's
    run) at two different stops are one real historical sample of how long that hop
    took -- dwell time included, since a rider re-boarding cares about total elapsed
    time between stops, not just moving time.
    """
    now = now or datetime.now(NY_TZ)
    start = now - timedelta(days=LOOKBACK_DAYS)
    events = storage.query_events(start, now)

    runs: Dict[Tuple[str, str], List] = defaultdict(list)  # (block, local_date) -> [events]
    for ev in events:
        if ev.event_type != "arrival" or not ev.block or not ev.stop_id or not ev.route_id:
            continue
        local_date = ev.timestamp.astimezone(NY_TZ).date().isoformat()
        runs[(ev.block, local_date)].append(ev)

    samples: Dict[str, List[float]] = defaultdict(list)
    for run_events in runs.values():
        run_events.sort(key=lambda e: e.timestamp)
        for a, b in zip(run_events, run_events[1:]):
            if a.stop_id == b.stop_id or a.route_id != b.route_id:
                continue
            duration = (b.timestamp - a.timestamp).total_seconds()
            if duration <= 0 or duration > MAX_PLAUSIBLE_HOP_S:
                continue
            local_dt = a.timestamp.astimezone(NY_TZ)
            key = _bucket_key(a.route_id, a.stop_id, b.stop_id, local_dt.weekday(), local_dt.hour)
            samples[key].append(duration)
    return samples


def refresh_hop_time_cache(storage, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(NY_TZ)
    samples = build_hop_time_samples(storage, now=now)
    buckets = {
        key: {"seconds": statistics.median(values), "samples": len(values)}
        for key, values in samples.items()
        if len(values) >= MIN_SAMPLES
    }
    payload = {"refreshed_at": now.isoformat(), "buckets": buckets}
    _write_cache(payload)
    return payload


def ensure_hop_time_cache(storage, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Load the cached model, rebuilding it first if it's stale. Call this once per
    request that needs a HopTimeModel (cheap when fresh -- just a JSON read)."""
    cache = _load_cache()
    if is_cache_stale(cache, now=now):
        return refresh_hop_time_cache(storage, now=now)
    return cache


class HopTimeModel:
    """Read-only lookup over a cached bucket table -- this is what gets passed as
    trip_planner.find_trips's `hop_time_fn`. Bucketed by (route_id, from_stop_id,
    to_stop_id, weekday, hour); no same-hour/cross-weekday fallback in v1 -- a missing
    bucket just means trip_planner falls back to its own flat heuristic for that
    segment, which is a safe default (never blocks an itinerary, only makes its
    duration estimate less precise)."""

    def __init__(self, buckets: Dict[str, Dict[str, Any]]):
        self._buckets = buckets

    def lookup(self, route_id: str, from_stop_id: str, to_stop_id: str, when: float) -> Optional[float]:
        local_dt = datetime.fromtimestamp(when, tz=NY_TZ)
        key = _bucket_key(route_id, from_stop_id, to_stop_id, local_dt.weekday(), local_dt.hour)
        bucket = self._buckets.get(key)
        if not bucket:
            return None
        try:
            return float(bucket["seconds"])
        except (KeyError, TypeError, ValueError):
            return None

    @classmethod
    def from_cache(cls, cache: Dict[str, Any]) -> "HopTimeModel":
        buckets = cache.get("buckets") if isinstance(cache, dict) else None
        return cls(buckets if isinstance(buckets, dict) else {})


def load_model(storage, now: Optional[datetime] = None) -> HopTimeModel:
    """Convenience: ensure the cache is fresh and return a ready-to-use HopTimeModel."""
    cache = ensure_hop_time_cache(storage, now=now)
    return HopTimeModel.from_cache(cache)
