"""Which kind of service day each date was: a permanent log kept next to the history it explains.

The scraped calendar (service_schedule.py) only knows the days it has been shown and forgets them after
KEEP_HISTORY_DAYS; this log is never pruned. One entry per date in <data dir>/service_levels.json:

    {"days": {"2026-10-05": {"level": "recess", "label": "Recess Service", "notes": "Fall Break", "source": "calendar"}}}

Levels: "full", "exam" (exam service), "recess" (fall break, Thanksgiving...), "summer" (summer service, a form
of recess) and "none". The name is kept for the record; the ETA history only cares whether a day was recess-like
(history_class), and exam days count as regular there.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from service_schedule import _write_atomic

FULL, EXAM, RECESS, SUMMER, NONE = "full", "exam", "recess", "summer", "none"
RECESS_LIKE = (RECESS, SUMMER)

# For dates with no logged entry (user, 2026-10-05): exam service ran 2026-04-29 through 2026-05-08, summer service
# 2026-05-11 through 2026-08-19, and every day since 2026-08-20 has been Full Service apart from the breaks the
# calendar logged. Other earlier dates are unknown.
KNOWN_RANGES = (
    (date(2026, 4, 29), date(2026, 5, 8), EXAM),
    (date(2026, 5, 11), date(2026, 8, 19), SUMMER),
)
FULL_SINCE = date(2026, 8, 20)


def level_from_calendar(text: Any) -> Optional[str]:
    """The calendar's "UVA Transit" cell ("Full Service", "Recess Service", "No Service"...) as a level, or None."""
    t = str(text or "").lower()
    if "summer" in t:
        return SUMMER
    if "exam" in t:
        return EXAM
    if "recess" in t:
        return RECESS
    if "no service" in t:
        return NONE
    if "full" in t:
        return FULL
    return None


def default_level(d: date) -> Optional[str]:
    for start, end, level in KNOWN_RANGES:
        if start <= d <= end:
            return level
    return FULL if d >= FULL_SINCE else None


def history_class(level: Optional[str]) -> Optional[str]:
    """"recess" for a recess-like day (its hop/dwell history is kept apart from Full Service days'), else None."""
    return RECESS if level in RECESS_LIKE else None


class ServiceLevelLog:
    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "service_levels.json"
        self.days: Dict[str, Dict[str, Any]] = {}
        try:
            self.days = json.loads(self.path.read_text(encoding="utf-8")).get("days") or {}
        except (OSError, ValueError):
            pass

    def record(self, d: date, level: str, label: str = "", notes: str = "", source: str = "calendar") -> bool:
        """Log (or correct) a day's level; True if the entry changed."""
        entry = {"level": level, "label": label, "notes": notes, "source": source}
        if self.days.get(d.isoformat()) == entry:
            return False
        self.days[d.isoformat()] = entry
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(self.path, json.dumps({"days": dict(sorted(self.days.items()))}, indent=1).encode("utf-8"))
        return True

    def record_calendar_day(self, d: date, calendar_entry: Optional[Dict[str, Any]]) -> bool:
        """Log a day from its service_schedule entry, if that names a level."""
        entry = calendar_entry or {}
        label = str((entry.get("services") or {}).get("UVA Transit") or "")
        level = level_from_calendar(label)
        return bool(level) and self.record(d, level, label, str(entry.get("notes") or ""))

    def level(self, d: date) -> Optional[str]:
        entry = self.days.get(d.isoformat())
        return entry["level"] if entry and entry.get("level") else default_level(d)

    def entry(self, d: date) -> Dict[str, Any]:
        logged = self.days.get(d.isoformat())
        if logged:
            return {"date": d.isoformat(), **logged}
        return {"date": d.isoformat(), "level": default_level(d), "label": "", "notes": "", "source": "default"}
