"""CAT's published GTFS static schedule (see charlottesville.gov/1843/GTFS-Data-
for-Developers), used ONLY to let the trip planner offer a CAT ride when no live
vehicle is being tracked for it yet -- the live API (catpublic.etaspot.net) only
ever reports vehicles currently en route, so a "Later" search (or simply a gap
between live reports right now) has nothing live to go on at all. See
trip_planner.py's CAT branch in _ride_leg for how this gets used as a fallback,
never a replacement, for a genuinely live wait.

Confirmed live (2026-09-16) by downloading the feed and cross-checking against
the live API: trips.txt's trip_short_name carries a "-NN" suffix (e.g.
"06:30:00-50"), and NN is the EXACT same PatternID the live API's own
scheduleNumber field encodes and this app already uses as a CAT Line's id (see
app.py's _cat_lines_for_trip_planner / _pattern_id_from_schedule_number) --
pattern 50's GTFS stop sequence (12800 -> 14926 -> 19883 -> ...) and shape_id
("1 DOWNTOWN/RIVERSIDE") match the live pattern of the same id exactly. So
joining schedule data onto a trip-planner Line is a direct dict lookup by
pattern id, not fuzzy stop-sequence matching.

This is a periodically-republished static export (feed_info.txt carries a
version number and a ~1 year validity window), not a live/streaming feed --
refresh() re-downloads at most once a day.
"""

from __future__ import annotations

import csv
import io
import os
import re
import time
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import httpx
from zoneinfo import ZoneInfo

GTFS_URL = os.getenv("CAT_GTFS_URL", "https://apps.charlottesville.gov/publicfiles/GTFS.zip")
CACHE_PATH = Path(os.getenv("CAT_GTFS_CACHE", "/data/cat_gtfs.zip"))
NY_TZ = ZoneInfo("America/New_York")
# A full republished export, not a live feed -- daily is plenty, and matches the
# refresh cadence uva_athletics.py already uses for a similarly-static resource.
REFRESH_INTERVAL_S = 24 * 3600.0
_SECONDS_PER_DAY = 24 * 3600

_PATTERN_SUFFIX_RE = re.compile(r"-(\d+)$")


@dataclass
class _Service:
    """One calendar.txt service_id: which weekdays it runs, its valid date range,
    and any calendar_dates.txt exceptions layered on top."""

    weekdays: Tuple[bool, bool, bool, bool, bool, bool, bool]  # Mon .. Sun, datetime.weekday() order
    start_date: date
    end_date: date
    added: frozenset = field(default_factory=frozenset)
    removed: frozenset = field(default_factory=frozenset)

    def active_on(self, d: date) -> bool:
        if d in self.removed:
            return False
        if d in self.added:
            return True
        return self.start_date <= d <= self.end_date and self.weekdays[d.weekday()]


@dataclass
class _Schedule:
    services: Dict[str, _Service]
    # pattern_id -> stop_id -> [(service_id, seconds_since_that_service_day's_midnight), ...]
    # -- GTFS times can exceed 24:00:00 for a trip that continues past midnight on
    # the SAME service day, e.g. 25:30:00 for 1:30am the next calendar date.
    departures: Dict[str, Dict[str, List[Tuple[str, int]]]]
    refreshed_at: datetime


_schedule: Optional[_Schedule] = None
_last_fetch_attempt = 0.0


def _parse_gtfs_time(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        h, m, s = (int(p) for p in value.strip().split(":"))
        return h * 3600 + m * 60 + s
    except ValueError:
        return None


def _parse_gtfs_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%Y%m%d").date()
    except ValueError:
        return None


def _parse_services(calendar_csv: str, calendar_dates_csv: str) -> Dict[str, _Service]:
    services: Dict[str, _Service] = {}
    for row in csv.DictReader(io.StringIO(calendar_csv)):
        sid = row.get("service_id")
        start = _parse_gtfs_date(row.get("start_date"))
        end = _parse_gtfs_date(row.get("end_date"))
        if not sid or start is None or end is None:
            continue
        weekdays = tuple(
            row.get(day) == "1"
            for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        )
        services[sid] = _Service(weekdays=weekdays, start_date=start, end_date=end)

    added: Dict[str, Set[date]] = {}
    removed: Dict[str, Set[date]] = {}
    for row in csv.DictReader(io.StringIO(calendar_dates_csv)):
        sid = row.get("service_id")
        d = _parse_gtfs_date(row.get("date"))
        if not sid or d is None:
            continue
        if row.get("exception_type") == "1":
            added.setdefault(sid, set()).add(d)
        elif row.get("exception_type") == "2":
            removed.setdefault(sid, set()).add(d)
    for sid, dates in added.items():
        if sid in services:
            services[sid].added = frozenset(dates)
        else:
            # Exists only via calendar_dates additions (no calendar.txt row) --
            # a wide-open range with an empty weekday mask means `active_on` only
            # ever trusts `added`/`removed`, never the range/weekday fields.
            services[sid] = _Service(
                weekdays=(False,) * 7, start_date=date.min, end_date=date.max, added=frozenset(dates)
            )
    for sid, dates in removed.items():
        if sid in services:
            services[sid].removed = frozenset(dates)
    return services


def _parse_departures(trips_csv: str, stop_times_csv: str) -> Dict[str, Dict[str, List[Tuple[str, int]]]]:
    service_by_trip: Dict[str, str] = {}
    pattern_by_trip: Dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(trips_csv)):
        trip_id = row.get("trip_id")
        if not trip_id:
            continue
        service_by_trip[trip_id] = row.get("service_id") or ""
        m = _PATTERN_SUFFIX_RE.search(row.get("trip_short_name") or "")
        if m:
            pattern_by_trip[trip_id] = m.group(1)

    departures: Dict[str, Dict[str, List[Tuple[str, int]]]] = {}
    for row in csv.DictReader(io.StringIO(stop_times_csv)):
        trip_id = row.get("trip_id") or ""
        pattern_id = pattern_by_trip.get(trip_id)
        stop_id = row.get("stop_id")
        dep_s = _parse_gtfs_time(row.get("departure_time") or row.get("arrival_time"))
        if pattern_id is None or not stop_id or dep_s is None:
            continue
        service_id = service_by_trip.get(trip_id, "")
        departures.setdefault(pattern_id, {}).setdefault(stop_id, []).append((service_id, dep_s))
    return departures


def _load_from_zip(raw: bytes) -> _Schedule:
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = set(zf.namelist())

        def read(name: str) -> str:
            return zf.read(name).decode("utf-8-sig") if name in names else ""

        services = _parse_services(read("calendar.txt"), read("calendar_dates.txt"))
        departures = _parse_departures(read("trips.txt"), read("stop_times.txt"))
    return _Schedule(services=services, departures=departures, refreshed_at=datetime.now(NY_TZ))


def refresh(force: bool = False) -> bool:
    """(Re)download + parse the feed if the in-memory copy is missing or stale.
    Synchronous/blocking (plain httpx, like uva_athletics.py's own daily refresh)
    -- callers on the request path should run this via asyncio.to_thread, same as
    trip_planner.find_trips itself, so a slow/unreachable charlottesville.gov
    never stalls the event loop. Returns True if a (re)load actually happened.
    Failures are swallowed: a stale or absent schedule just means CAT falls back
    to live-only availability, same as before this module existed."""
    global _schedule, _last_fetch_attempt
    now = time.monotonic()
    if not force and _schedule is not None and (now - _last_fetch_attempt) < REFRESH_INTERVAL_S:
        return False
    _last_fetch_attempt = now

    raw: Optional[bytes] = None
    try:
        resp = httpx.get(GTFS_URL, timeout=30.0)
        resp.raise_for_status()
        raw = resp.content
        try:
            CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = CACHE_PATH.with_suffix(".tmp")
            tmp_path.write_bytes(raw)
            tmp_path.replace(CACHE_PATH)
        except OSError as exc:
            print(f"[cat_gtfs] could not persist cache to {CACHE_PATH}: {exc}")
    except Exception as exc:
        print(f"[cat_gtfs] fetch failed: {exc}")
        if _schedule is not None:
            return False  # keep serving the existing in-memory copy
        try:
            raw = CACHE_PATH.read_bytes()
        except OSError:
            return False

    try:
        _schedule = _load_from_zip(raw)
        return True
    except Exception as exc:
        print(f"[cat_gtfs] parse failed: {exc}")
        return False


def _active_service_ids(d: date) -> Set[str]:
    if _schedule is None:
        return set()
    return {sid for sid, svc in _schedule.services.items() if svc.active_on(d)}


def scheduled_departures_s(pattern_id: str, stop_id: str, target_date: date) -> List[int]:
    """Every scheduled departure -- as seconds since LOCAL (America/New_York)
    midnight of `target_date` -- at `stop_id` for `pattern_id`'s trips actually
    running that day. Checks target_date's own active services plus the PREVIOUS
    day's (a service day's late trips can carry GTFS times past 24:00:00, e.g.
    25:30:00 for 1:30am -- relevant to a query made right after local midnight).
    Empty if the pattern doesn't serve that stop, or isn't scheduled that day at
    all (weekend-only route on a weekday, a holiday calendar_dates removal,
    etc.)."""
    if _schedule is None:
        return []
    entries = _schedule.departures.get(pattern_id, {}).get(stop_id)
    if not entries:
        return []
    today_ids = _active_service_ids(target_date)
    yesterday_ids = _active_service_ids(target_date - timedelta(days=1))
    out: List[int] = []
    for service_id, dep_s in entries:
        if service_id in today_ids and dep_s < _SECONDS_PER_DAY:
            out.append(dep_s)  # an ordinary same-day trip
        elif service_id in yesterday_ids and dep_s >= _SECONDS_PER_DAY:
            out.append(dep_s - _SECONDS_PER_DAY)  # yesterday's service rolling past midnight
        # A dep_s >= _SECONDS_PER_DAY tagged with TODAY's service belongs to
        # TOMORROW (today's own service day continuing past midnight), not
        # today -- deliberately excluded here; it surfaces when the caller
        # queries target_date + 1, where it becomes that day's "yesterday" case.
    return sorted(out)


def is_loaded() -> bool:
    return _schedule is not None


__all__ = [
    "CACHE_PATH",
    "GTFS_URL",
    "NY_TZ",
    "is_loaded",
    "refresh",
    "scheduled_departures_s",
]
