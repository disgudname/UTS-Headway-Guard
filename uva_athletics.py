from __future__ import annotations

"""
UVA athletics feed ingestion and caching utilities.

Data is pulled from the public WMT ICS feed for Virginia sports and cached
locally to avoid excessive upstream traffic. The cache refreshes once per day
shortly after 03:00 America/New_York (or the first request after that if the
process was offline). Home events are defined as those with locations that point
to Charlottesville, VA.
"""

from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
import os
import re

import httpx
from zoneinfo import ZoneInfo

ICS_URL = (
    "https://api.calendar.wmt.digital/api/calendar/calendar.ics?username=virginiasports"
    "&category%5B%5D=124&category%5B%5D=135&category%5B%5D=125&category%5B%5D=126&category%5B%5D=127"
    "&category%5B%5D=128&category%5B%5D=129&category%5B%5D=130&category%5B%5D=139&category%5B%5D=138"
    "&category%5B%5D=137&category%5B%5D=136&category%5B%5D=134&category%5B%5D=133&category%5B%5D=132"
    "&category%5B%5D=131&category%5B%5D=140&category%5B%5D=141&category%5B%5D=142&category%5B%5D=143"
    "&category%5B%5D=144&category%5B%5D=145"
)

NY_TZ = ZoneInfo("America/New_York")
REFRESH_HOUR_LOCAL = 3
CACHE_PATH = Path(os.getenv("UVA_ATHLETICS_CACHE", "/data/uva_athletics_cache.json"))


@dataclass
class ParsedEvent:
    dtstart: datetime
    dtend: datetime
    summary: str
    description: str
    location: str
    uid: str


_CITY_STATE_RE = re.compile(r"\s*,\s*")
_CHARLOTTESVILLE_RE = re.compile(r"charlottesville\s*,?\s*va?\. ?", re.IGNORECASE)


def _unfold_ics_lines(ics_text: str) -> List[str]:
    lines = ics_text.splitlines()
    unfolded: List[str] = []
    for line in lines:
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line.strip("\r"))
    return unfolded


def _parse_datetime(value: str, tzid: Optional[str]) -> Optional[datetime]:
    tz = ZoneInfo(tzid) if tzid else NY_TZ
    try:
        if value.endswith("Z"):
            dt = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            return dt.astimezone(tz)
        if "T" in value:
            dt = datetime.strptime(value, "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=tz)
        dt_date = datetime.strptime(value, "%Y%m%d").date()
        return datetime.combine(dt_date, dtime.min, tzinfo=tz)
    except ValueError:
        return None


def parse_ics_events(ics_text: str) -> List[ParsedEvent]:
    events: List[ParsedEvent] = []
    lines = _unfold_ics_lines(ics_text)
    current: Dict[str, Any] = {}
    for line in lines:
        if line == "BEGIN:VEVENT":
            current = {}
            continue
        if line == "END:VEVENT":
            try:
                events.append(
                    ParsedEvent(
                        dtstart=current["dtstart"],
                        dtend=current.get("dtend", current["dtstart"]),
                        summary=current.get("summary", ""),
                        description=current.get("description", ""),
                        location=current.get("location", ""),
                        uid=current.get("uid", ""),
                    )
                )
            except KeyError:
                pass
            current = {}
            continue
        if ":" not in line:
            continue
        key_part, value = line.split(":", 1)
        main_part, *param_parts = key_part.split(";")
        key_upper = main_part.upper()
        params: Dict[str, str] = {}
        for param in param_parts:
            if "=" in param:
                pkey, pval = param.split("=", 1)
                params[pkey.upper()] = pval
        tzid = params.get("TZID")
        if key_upper == "DTSTART":
            dtstart = _parse_datetime(value.strip(), tzid)
            if dtstart:
                current["dtstart"] = dtstart
        elif key_upper == "DTEND":
            dtend = _parse_datetime(value.strip(), tzid)
            if dtend:
                current["dtend"] = dtend
        elif key_upper == "SUMMARY":
            current["summary"] = value.strip()
        elif key_upper == "DESCRIPTION":
            current["description"] = value.strip()
        elif key_upper == "LOCATION":
            current["location"] = value.strip()
        elif key_upper == "UID":
            current["uid"] = value.strip()
    return events


def parse_sport_and_opponent(summary: str) -> Tuple[str, str]:
    raw = summary or ""
    cleaned = raw
    if cleaned.lower().startswith("virginia "):
        cleaned = cleaned[len("Virginia ") :]
    split_match = re.split(r"\s+(?:vs\.?|at)\s+", cleaned, maxsplit=1, flags=re.IGNORECASE)
    if len(split_match) == 2:
        sport, opponent = split_match
    else:
        sport, opponent = cleaned, ""
    return sport.strip(), opponent.strip()


def _normalize_state(state: str) -> str:
    cleaned = state.strip().upper().replace(".", "")
    if cleaned == "VA" or cleaned == "VIRGINIA":
        return "VA"
    return cleaned


def parse_location(location: str) -> Tuple[str, str, Optional[str]]:
    raw = location or ""
    extra_detail = None

    city_state_part = raw
    if " | " in raw:
        city_state_part, extra_detail = raw.split(" | ", 1)

    city_state_part = city_state_part.replace("\\,", ",").strip()

    city = ""
    state = ""
    if "," in city_state_part:
        city_part, state_part = [p.strip() for p in city_state_part.rsplit(",", 1)]
        city = city_part
        cleaned_state = re.sub(r"[^\w\s]", "", state_part)
        state = _normalize_state(cleaned_state)
    else:
        city = city_state_part

    return city or "", state or "", extra_detail


def is_home_location(location: str) -> bool:
    """
    Return True if the location indicates a home game.
    Home games always take place in Charlottesville,
    and the ICS location field often contains variations like 'Charlottesville, Va.'.
    A simple case-insensitive substring check is sufficient.
    """
    if not location:
        return False

    loc = location.lower()
    if "charlottesville" in loc:
        return True

    # Fallback to parsed fields if needed
    city, state, _ = parse_location(location)
    if city and city.lower() == "charlottesville":
        return True

    return False


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
        json.dump(data, f, ensure_ascii=False, indent=2)
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


def refresh_uva_athletics_cache() -> Dict[str, Any]:
    with httpx.Client(timeout=30) as client:
        resp = client.get(ICS_URL)
        resp.raise_for_status()
        ics_text = resp.text
    parsed_events = parse_ics_events(ics_text)
    events_payload = []
    for ev in parsed_events:
        sport, opponent = parse_sport_and_opponent(ev.summary)
        city, state, extra_detail = parse_location(ev.location)
        events_payload.append(
            {
                "start_time": ev.dtstart.astimezone(NY_TZ).isoformat(),
                "end_time": ev.dtend.astimezone(NY_TZ).isoformat(),
                "sport": sport,
                "opponent": opponent,
                "city": city,
                "state": state,
                "extra_location_detail": extra_detail,
                "raw_summary": ev.summary,
                "raw_location": ev.location,
                "uid": ev.uid,
                "description": ev.description,
                "is_home": is_home_location(ev.location),
            }
        )
    payload = {"refreshed_at": datetime.now(NY_TZ).isoformat(), "events": events_payload}
    _write_cache(payload)
    return payload


def ensure_uva_athletics_cache(now: Optional[datetime] = None) -> Dict[str, Any]:
    cache = _load_cache()
    if is_cache_stale(cache, now=now):
        return refresh_uva_athletics_cache()
    return cache


def load_cached_events() -> List[Dict[str, Any]]:
    cache = _load_cache()
    events = cache.get("events") if isinstance(cache, dict) else None
    return events if isinstance(events, list) else []


__all__ = [
    "CACHE_PATH",
    "ICS_URL",
    "NY_TZ",
    "ensure_uva_athletics_cache",
    "is_cache_stale",
    "is_home_location",
    "load_cached_events",
    "parse_location",
    "parse_sport_and_opponent",
    "refresh_uva_athletics_cache",
]
