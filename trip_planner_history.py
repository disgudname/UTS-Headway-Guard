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
from typing import Any, Callable, Dict, List, Optional, Tuple

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


def build_hop_time_samples(
    storage,
    now: Optional[datetime] = None,
    lookback_days: Optional[int] = None,
    resolve_stop_id: Optional[Callable[[Any], str]] = None,
) -> Dict[str, List[float]]:
    """Read the last `lookback_days` (default LOOKBACK_DAYS) of headway events and
    bucket real stop-to-stop travel-time samples by (route_id, from_stop_id,
    to_stop_id, weekday, hour).

    Two consecutive "arrival" events sharing the same `block` (one physical vehicle's
    run) at two different stops are one real historical sample of how long that hop
    took -- dwell time included, since a rider re-boarding cares about total elapsed
    time between stops, not just moving time.

    `resolve_stop_id`, if given, replaces `ev.stop_id` for bucketing purposes --
    build_eta_model.py (an offline, occasionally-run script, NOT part of this
    module's own daily refresh) passes one that re-derives the correct RouteStopID
    from `ev.address_id` via an accumulated (route,address)->RouteStopID table, to
    recover history recorded before headway_tracker.py's stop-id fix (see that
    file's StopPoint.route_stop_ids docstring). The live daily refresh below never
    passes this -- events it reads were already recorded correctly by the fixed
    tracker, so there's nothing to resolve.
    """
    now = now or datetime.now(NY_TZ)
    lookback_days = LOOKBACK_DAYS if lookback_days is None else lookback_days
    resolve_stop_id = resolve_stop_id or (lambda ev: ev.stop_id)
    start = now - timedelta(days=lookback_days)

    samples: Dict[str, List[float]] = defaultdict(list)

    # One NY-local calendar day at a time, rather than one big query_events(start,
    # now) covering the whole lookback -- a "run" is grouped by (block, local_date)
    # below (local_date is NY-based) and so never spans an NY day boundary anyway,
    # meaning day-by-day changes nothing about the result as long as each window
    # aligns to NY days too (query_events itself is UTC-file-based internally and
    # handles a window spanning two on-disk files transparently). This bounds peak
    # memory to a single day's parsed events instead of the entire window:
    # confirmed live, a full-archive rebuild (~2.6M events across 9+ months)
    # OOM-killed the app's small production machine when loaded in one shot via a
    # single query_events() call.
    day_cursor = start.astimezone(NY_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = now.astimezone(NY_TZ)
    while day_cursor <= end_local:
        day_end = day_cursor + timedelta(days=1, microseconds=-1)
        day_events = storage.query_events(max(day_cursor, start), min(day_end, now))
        day_cursor += timedelta(days=1)

        runs: Dict[Tuple[str, str], List] = defaultdict(list)  # (block, local_date) -> [events]
        for ev in day_events:
            if ev.event_type != "arrival" or not ev.block or not ev.stop_id or not ev.route_id:
                continue
            local_date = ev.timestamp.astimezone(NY_TZ).date().isoformat()
            runs[(ev.block, local_date)].append(ev)

        for run_events in runs.values():
            run_events.sort(key=lambda e: e.timestamp)
            for a, b in zip(run_events, run_events[1:]):
                if a.route_id != b.route_id:
                    continue
                a_stop, b_stop = resolve_stop_id(a), resolve_stop_id(b)
                if a_stop == b_stop:
                    continue
                duration = (b.timestamp - a.timestamp).total_seconds()
                if duration <= 0 or duration > MAX_PLAUSIBLE_HOP_S:
                    continue
                local_dt = a.timestamp.astimezone(NY_TZ)
                key = _bucket_key(a.route_id, a_stop, b_stop, local_dt.weekday(), local_dt.hour)
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
    duration estimate less precise).

    `fallback`, if given, is tried when this model's own buckets miss -- load_model()
    wires the occasionally-run build_eta_model.py's DEEP_CACHE_PATH in as the live
    60-day model's fallback, so a route/stop/time combo the last 60 days haven't
    accumulated MIN_SAMPLES for yet can still use real history further back, without
    that deeper (and occasionally stale) data ever overriding fresher live buckets."""

    def __init__(self, buckets: Dict[str, Dict[str, Any]], fallback: Optional["HopTimeModel"] = None):
        self._buckets = buckets
        self._fallback = fallback

    def lookup(self, route_id: str, from_stop_id: str, to_stop_id: str, when: float) -> Optional[float]:
        local_dt = datetime.fromtimestamp(when, tz=NY_TZ)
        key = _bucket_key(route_id, from_stop_id, to_stop_id, local_dt.weekday(), local_dt.hour)
        bucket = self._buckets.get(key)
        if bucket:
            try:
                return float(bucket["seconds"])
            except (KeyError, TypeError, ValueError):
                pass
        if self._fallback is not None:
            return self._fallback.lookup(route_id, from_stop_id, to_stop_id, when)
        return None

    @classmethod
    def from_cache(cls, cache: Dict[str, Any], fallback: Optional["HopTimeModel"] = None) -> "HopTimeModel":
        buckets = cache.get("buckets") if isinstance(cache, dict) else None
        return cls(buckets if isinstance(buckets, dict) else {}, fallback=fallback)


# Written only by the occasional, manually-run build_eta_model.py -- never by this
# module's own daily refresh. See that script's docstring.
DEEP_CACHE_PATH = Path(os.getenv("TRIP_PLANNER_DEEP_HOP_TIME_CACHE", "/data/trip_planner_hop_times_deep.json"))


def _load_deep_cache() -> Dict[str, Any]:
    if not DEEP_CACHE_PATH.exists():
        return {}
    try:
        with DEEP_CACHE_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def load_model(storage, now: Optional[datetime] = None) -> HopTimeModel:
    """Convenience: ensure the live (60-day) cache is fresh, layer the deep/corrected
    historical model (if build_eta_model.py has ever been run) in as its fallback,
    and return a ready-to-use HopTimeModel."""
    cache = ensure_hop_time_cache(storage, now=now)
    deep_cache = _load_deep_cache()
    deep_model = HopTimeModel.from_cache(deep_cache) if deep_cache.get("buckets") else None
    return HopTimeModel.from_cache(cache, fallback=deep_model)
