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
import math
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

# Until 2026-09-30 headway_tracker logged a "route_activation" arrival for a bus parked
# inside the final bubble of a stop it wasn't at: mostly the stop across the street
# (every evening Gold arrival at UVA Chapel doubled as one at Shannon Library). ~27% of
# all arrivals, every week back through at least August. A phantom sits between two real
# visits, so it replaced the real hop A -> C with A -> phantom -> C (e.g. 273 "Library ->
# Garrett Hall" samples) and starved the real hops. Both builders drop them: a
# route_activation arrival is a phantom when the same run has a real arrival within
# PHANTOM_WINDOW_S at a stop within PHANTOM_NEAR_M, or is mid-visit at another stop.
# Arrivals at the next stop (150-400 m on) are real and kept: a time-only rule that also
# dropped those made replayed ETAs later. Needs stop positions (set_stop_coords, fed by
# app.py from the live route stop lists); a stop it has no position for is kept.
# TransLoc only lists the routes running right now, and the rebuild runs at 03:00 when
# almost nothing is, so positions are remembered (merged, saved to STOP_COORDS_PATH) rather
# than replaced: daytime/evening/weekend route stops all stay known.
PHANTOM_WINDOW_S = 60.0
PHANTOM_NEAR_M = 60.0
STOP_COORDS_PATH = Path(os.getenv("TRIP_PLANNER_STOP_COORDS", "/data/trip_planner_stop_coords.json"))
_stop_coords: Optional[Dict[str, Tuple[float, float]]] = None  # None = not loaded from disk yet


def _known_stop_coords() -> Dict[str, Tuple[float, float]]:
    global _stop_coords
    if _stop_coords is None:
        try:
            with STOP_COORDS_PATH.open("r", encoding="utf-8") as f:
                _stop_coords = {k: (float(v[0]), float(v[1])) for k, v in json.load(f).items()}
        except (OSError, ValueError, TypeError, IndexError):
            _stop_coords = {}
    return _stop_coords


def set_stop_coords(coords: Dict[str, Tuple[float, float]]) -> None:
    """Merge RouteStopID -> (lat, lon) into the known positions (for drop_phantom_route_activations);
    saved to STOP_COORDS_PATH whenever something new or moved shows up."""
    known = _known_stop_coords()
    changed = {k: (float(v[0]), float(v[1])) for k, v in coords.items() if known.get(k) != (float(v[0]), float(v[1]))}
    if not changed:
        return
    known.update(changed)
    try:
        STOP_COORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = STOP_COORDS_PATH.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(known, f)
        tmp_path.replace(STOP_COORDS_PATH)
    except OSError as exc:
        print(f"[trip-planner-history] could not save stop positions: {exc}")


def _distance_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat = math.radians((a[0] + b[0]) / 2)
    return math.hypot((a[0] - b[0]) * 111_320.0, (a[1] - b[1]) * 111_320.0 * math.cos(lat))


def drop_phantom_route_activations(
    run_events: List[Any], coords: Optional[Dict[str, Tuple[float, float]]] = None,
) -> List[Any]:
    """One run's events (sorted by time) without phantom route_activation arrivals, or the
    departure that closes each one. See PHANTOM_NEAR_M. Keyed by the recorded stop_id."""
    coords = _known_stop_coords() if coords is None else coords
    if not coords:
        return run_events

    def is_ra(ev) -> bool:
        return ev.event_type == "arrival" and getattr(ev, "arrival_type", None) == "route_activation"

    real = [(ev.timestamp, str(ev.stop_id)) for ev in run_events if ev.event_type == "arrival" and not is_ra(ev)]
    visits = []  # (arrival, departure or None, stop) of real visits
    pending: Dict[str, datetime] = {}
    for ev in run_events:
        stop = str(ev.stop_id)
        if ev.event_type == "arrival" and not is_ra(ev):
            pending.setdefault(stop, ev.timestamp)
        elif ev.event_type == "departure" and stop in pending:
            visits.append((pending.pop(stop), ev.timestamp, stop))
    visits.extend((t, None, stop) for stop, t in pending.items())

    out: List[Any] = []
    phantom_stops: set = set()
    for ev in run_events:
        stop = str(ev.stop_id)
        if is_ra(ev) and stop in coords:
            t = ev.timestamp
            near = any(
                other != stop and other in coords
                and abs((rt - t).total_seconds()) <= PHANTOM_WINDOW_S
                and _distance_m(coords[stop], coords[other]) <= PHANTOM_NEAR_M
                for rt, other in real
            )
            inside = any(other != stop and a <= t and (d is None or t <= d) for a, d, other in visits)
            if near or inside:
                phantom_stops.add(stop)
                continue
        elif ev.event_type == "departure" and stop in phantom_stops:
            phantom_stops.discard(stop)
            continue
        out.append(ev)
    return out
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


_WEEKDAYS = (0, 1, 2, 3, 4)
_WEEKEND = (5, 6)


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

        runs: Dict[Tuple[str, str], List] = defaultdict(list)  # (block or vehicle, local_date) -> [events]
        for ev in day_events:
            if ev.event_type not in ("arrival", "departure") or not ev.stop_id or not ev.route_id:
                continue
            # A run is one vehicle's consecutive arrivals. Prefer the schedule block, but
            # fall back to the vehicle itself: `block` comes from a schedule-assignment
            # feed and is empty on almost every recent day (checked 2026-09-19: 0 of
            # ~10,800 arrivals on most days), which had left the live 60-day table with
            # ~320 buckets, none for the routes currently in service -- so ETAs for
            # nearly every stop fell back to a distance/speed guess. Same vehicle + same
            # route + same local day is the same physical run for our purposes.
            run_id = ev.block or (f"vehicle:{ev.vehicle_id}" if ev.vehicle_id else None)
            if not run_id:
                continue
            local_date = ev.timestamp.astimezone(NY_TZ).date().isoformat()
            runs[(run_id, local_date)].append(ev)

        for run_events in runs.values():
            run_events.sort(key=lambda e: e.timestamp)
            # Departures are read only to spot phantom arrivals (see PHANTOM_NEAR_M).
            run_events = [e for e in drop_phantom_route_activations(run_events) if e.event_type == "arrival"]
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


# ---------------------------------------------------------------------------
# Driving-only hops + separate dwell (see bus_eta.estimate_stop_eta_s's dwell_fn).
#
# build_hop_time_samples() above measures arrival(A) -> arrival(B), so any layover at A is
# baked into that hop; bus_eta then has to cap it after the fact for every mapped
# timestop, which can't know that a stop is a layover on weekdays but not on weekends
# (history is pooled across day groups) and reopened a weekday-layover leak when made
# day-aware. Here a hop is last departure(A) -> first arrival(B) (driving only) and the time
# the bus sits at each stop (first arrival -> last departure of the visit) is recorded
# separately, so layover length simply shows up in the dwell data of the day group it
# actually happens in.
#
# Live only for routes with no block schedule (Purple, see app.py's
# BUS_ETA_DWELL_MODE_ROUTE_PREFIXES): their buses stage at different stops by time of day
# (Fontaine in the morning, the hospital later), which only dwell history can express.
# Routes with a block package keep arrival->arrival hops + the schedule hold clamp.

DWELL_KEY = "DWELL"
DEFAULT_DWELL_S = 20.0  # median dwell of non-layover stops, 9 months of headway events (2026-09-20)
MAX_PLAUSIBLE_DWELL_S = 3600.0


def stop_id_resolver(route_stop_names: Dict[Tuple[str, str], str]) -> Callable[[Any], str]:
    """Re-key an event to its route's OWN RouteStopID by stop name. Before 2026-09-14
    headway_tracker stamped one route's stop ID onto every route sharing that physical
    stop (commit 9c2ec69), so most older events sit under IDs that don't match the route's
    live stop sequence and never line up with the ETA engine's hop lookups. The events
    still carry the right stop_name, so `route_stop_names` ((route_id, stop_name) ->
    RouteStopID, built from the live route stop lists, unique names only) recovers them.
    Unknown route/name keeps the recorded ID."""
    def resolve(ev) -> str:
        return route_stop_names.get((str(ev.route_id), (ev.stop_name or "").strip()), ev.stop_id)
    return resolve


def build_drive_and_dwell_samples(
    storage,
    now: Optional[datetime] = None,
    lookback_days: Optional[int] = None,
    resolve_stop_id: Optional[Callable[[Any], str]] = None,
) -> Tuple[Dict[str, List[float]], Dict[str, List[float]]]:
    """(drive_samples, dwell_samples), same bucket keys as build_hop_time_samples.
    Dwell buckets use to_stop_id == DWELL_KEY and are keyed by the departure's weekday/hour."""
    now = now or datetime.now(NY_TZ)
    lookback_days = LOOKBACK_DAYS if lookback_days is None else lookback_days
    resolve_stop_id = resolve_stop_id or (lambda ev: ev.stop_id)
    start = now - timedelta(days=lookback_days)
    drive: Dict[str, List[float]] = defaultdict(list)
    dwell: Dict[str, List[float]] = defaultdict(list)

    day_cursor = start.astimezone(NY_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = now.astimezone(NY_TZ)
    while day_cursor <= end_local:
        day_end = day_cursor + timedelta(days=1, microseconds=-1)
        day_events = storage.query_events(max(day_cursor, start), min(day_end, now))
        day_cursor += timedelta(days=1)

        runs: Dict[Tuple[str, str], List] = defaultdict(list)
        for ev in day_events:
            if ev.event_type not in ("arrival", "departure") or not ev.stop_id or not ev.route_id:
                continue
            run_id = ev.block or (f"vehicle:{ev.vehicle_id}" if ev.vehicle_id else None)
            if not run_id:
                continue
            runs[(run_id, ev.timestamp.astimezone(NY_TZ).date().isoformat())].append(ev)

        for run_events in runs.values():
            run_events.sort(key=lambda e: e.timestamp)
            run_events = drop_phantom_route_activations(run_events)
            # One visit = every consecutive event at the same (route, stop). A bus staged at a
            # stop for minutes is logged as arrive/depart/arrive/depart... (GPS jitter in and out
            # of the final bubble, "route_activation" re-arrivals): ~13% of all arrivals on
            # 2026-09-25, 2-3 per lap at JPA @ West Complex. Taking each departure's own
            # dwell_seconds split the hold into slivers and dropped the rest, so Purple's
            # staging holds (2-6 min, measured) never reached the dwell data.
            visits: List[Dict[str, Any]] = []
            for ev in run_events:
                stop = resolve_stop_id(ev)
                if not visits or (visits[-1]["route"], visits[-1]["stop"]) != (ev.route_id, stop):
                    visits.append({"route": ev.route_id, "stop": stop, "arr": None, "dep": None})
                v = visits[-1]
                if ev.event_type == "arrival" and v["arr"] is None:
                    v["arr"] = ev.timestamp
                elif ev.event_type == "departure":
                    v["dep"] = ev.timestamp
            for v, nxt in zip(visits, visits[1:] + [None]):
                if v["dep"] is None:
                    continue
                local_dt = v["dep"].astimezone(NY_TZ)
                if v["arr"] is not None:
                    held = (v["dep"] - v["arr"]).total_seconds()
                    if 0 <= held <= MAX_PLAUSIBLE_DWELL_S:
                        dwell[_bucket_key(v["route"], v["stop"], DWELL_KEY, local_dt.weekday(), local_dt.hour)].append(held)
                if nxt is None or nxt["arr"] is None or nxt["route"] != v["route"]:
                    continue
                duration = (nxt["arr"] - v["dep"]).total_seconds()
                if duration <= 0 or duration > MAX_PLAUSIBLE_HOP_S:
                    continue
                drive[_bucket_key(v["route"], v["stop"], nxt["stop"], local_dt.weekday(), local_dt.hour)].append(duration)
    return drive, dwell


class DwellModel:
    """Typical seconds a bus sits at (route, stop) around `when`. Unlike HopTimeModel this
    NEVER pools across day groups: a stop that is a 7-minute layover on weekdays but a
    normal 20 s stop on weekends must not lend its weekday dwell to a weekend ETA. Widens
    only within the same day group (other days, then hour +/-1, +/-2), then falls back to
    DEFAULT_DWELL_S -- "no evidence of a layover here at this time" means a normal stop."""

    def __init__(self, buckets: Dict[str, Dict[str, Any]], default_s: float = DEFAULT_DWELL_S):
        self._buckets = buckets
        self._default = default_s

    def _vals(self, route_id: str, stop_id: str, weekdays, hours) -> List[float]:
        out = []
        for wd in weekdays:
            for hr in hours:
                if 0 <= hr <= 23:
                    bucket = self._buckets.get(_bucket_key(route_id, stop_id, DWELL_KEY, wd, hr))
                    if bucket and isinstance(bucket.get("seconds"), (int, float)):
                        out.append(float(bucket["seconds"]))
        return out

    def lookup(self, route_id: str, stop_id: str, when: float) -> float:
        local_dt = datetime.fromtimestamp(when, tz=NY_TZ)
        group = _WEEKEND if local_dt.weekday() in _WEEKEND else _WEEKDAYS
        hour = local_dt.hour
        others = tuple(wd for wd in group if wd != local_dt.weekday())
        for weekdays, hours in (
            ((local_dt.weekday(),), (hour,)), (others, (hour,)),
            (group, (hour - 1, hour + 1)), (group, (hour - 2, hour + 2)),
        ):
            vals = self._vals(route_id, stop_id, weekdays, hours)
            if vals:
                return statistics.median(vals)
        return self._default

    @classmethod
    def from_samples(cls, samples: Dict[str, List[float]], quantile: float = 0.5) -> "DwellModel":
        """quantile < 0.5 leans toward SHORT dwells: with no schedule to say whether a bus is
        early (waits) or late (leaves promptly), a lower value errs early, the cheaper miss."""
        def pick(values: List[float]) -> float:
            ordered = sorted(values)
            return ordered[min(len(ordered) - 1, int(quantile * len(ordered)))]

        return cls({
            key: {"seconds": pick(v), "samples": len(v)}
            for key, v in samples.items() if len(v) >= MIN_SAMPLES
        })


# Staging holds vary a lot (1-10 min at the same stop and hour), so the median dwell makes
# every lap where the bus does NOT stage long read too late. Offline replay of Purple
# 2026-09-24/25 (drive+dwell, minus the time the bus has already sat at its stop): median
# dwell took route 73's ">2 min late" share 9.8% -> 14.5%; the 40th percentile kept it at
# 10.0% while still cutting median |error| 123 -> 104 s (74) and 128 -> 121 s (73).
DWELL_QUANTILE = 0.40
DRIVE_DWELL_CACHE_PATH = Path(os.getenv("TRIP_PLANNER_DRIVE_DWELL_CACHE", "/data/trip_planner_drive_dwell.json"))


def refresh_drive_dwell_cache(storage, now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(NY_TZ)
    drive, dwell = build_drive_and_dwell_samples(storage, now=now)
    payload = {
        "refreshed_at": now.isoformat(),
        "drive": {k: {"seconds": statistics.median(v), "samples": len(v)} for k, v in drive.items() if len(v) >= MIN_SAMPLES},
        "dwell": DwellModel.from_samples(dwell, DWELL_QUANTILE)._buckets,
    }
    DRIVE_DWELL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = DRIVE_DWELL_CACHE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp_path.replace(DRIVE_DWELL_CACHE_PATH)
    return payload


_drive_dwell_memo: Dict[str, Any] = {}  # last cache payload, so a fresh cache isn't re-read every ETA computation


def load_drive_dwell_models(storage, now: Optional[datetime] = None) -> Tuple[HopTimeModel, DwellModel]:
    """(driving-only hop model, dwell model), rebuilt at most once a day like the hop cache.
    The rebuild scans LOOKBACK_DAYS of events (~11 s locally): call it off the event loop."""
    global _drive_dwell_memo
    cache = _drive_dwell_memo
    if is_cache_stale(cache, now=now) and DRIVE_DWELL_CACHE_PATH.exists():
        try:
            with DRIVE_DWELL_CACHE_PATH.open("r", encoding="utf-8") as f:
                cache = json.load(f)
        except (OSError, json.JSONDecodeError):
            cache = {}
    if is_cache_stale(cache, now=now):
        cache = refresh_drive_dwell_cache(storage, now=now)
    _drive_dwell_memo = cache
    return HopTimeModel(cache.get("drive") or {}), DwellModel(cache.get("dwell") or {})


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
        # No bucket for this exact weekday and hour. These routes run about once an hour
        # in the evening/weekend, so 3 samples for one specific weekday+hour is often out
        # of reach even with 60 days of data (checked 2026-09-19: only ~25% of hops on the
        # weekend routes had a bucket for a Saturday 6pm). Widen in steps, staying inside
        # the same day group (Mon-Fri, or Sat-Sun) and pooling by median:
        #   1. the other day(s) of the group, same hour
        #   2. the whole group, hour +/- 1
        #   3. the whole group, hour +/- 2
        # then (see below) the other day group in the same three steps.
        group = _WEEKEND if local_dt.weekday() in _WEEKEND else _WEEKDAYS
        hour = local_dt.hour
        others = tuple(wd for wd in group if wd != local_dt.weekday())
        other_group = _WEEKDAYS if group is _WEEKEND else _WEEKEND
        #   4-6. the OTHER day group, same hour then +/-1, +/-2 -- last resort before the
        #        distance/speed guess. Safe for the routes that need it: the evening/weekend
        #        route ids (54/55/57...) run the same pattern on weekday evenings as on
        #        weekends, and where both exist their hop times match (Saturday 6pm vs
        #        weekday 6pm, same route+hop: median ratio 1.00 over 89 hops, 2026-09-19);
        #        daytime weekday service uses different route ids, so it never leaks in.
        #        This took Green/Orange from 5-6 of 20 hops covered on a Saturday evening to 20.
        for weekdays, hours in (
            (others, (hour,)), (group, (hour - 1, hour + 1)), (group, (hour - 2, hour + 2)),
            (other_group, (hour,)), (other_group, (hour - 1, hour + 1)), (other_group, (hour - 2, hour + 2)),
        ):
            pooled = self._median_over(route_id, from_stop_id, to_stop_id, weekdays, hours)
            if pooled is not None:
                return pooled
        if self._fallback is not None:
            return self._fallback.lookup(route_id, from_stop_id, to_stop_id, when)
        return None

    def _median_over(self, route_id: str, from_stop_id: str, to_stop_id: str, weekdays, hours) -> Optional[float]:
        values = [
            float(bucket["seconds"])
            for wd in weekdays
            for hr in hours
            if 0 <= hr <= 23
            for bucket in [self._buckets.get(_bucket_key(route_id, from_stop_id, to_stop_id, wd, hr))]
            if bucket and isinstance(bucket.get("seconds"), (int, float))
        ]
        return statistics.median(values) if values else None

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
