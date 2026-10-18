"""Log of TransLoc's anti-bunching guidance (GetAntiBunching), for the routes we want to study.

TransLoc gives each route a LoopSeconds and a TargetGap (seconds between buses) and each bus an
Adjustment (seconds) and a Severity (Green / Yellow / Red). Drivers are told to ignore it, so
nothing has ever recorded it: this keeps it next to the headway events so it can be compared with
what the buses actually did (scripts/anti_bunching_compare.py).

One file per local service date in `<data dir>/anti_bunching/YYYY-MM-DD.jsonl`, one line per route
per poll, written only when that route's guidance changed since the last line (a reader carries the
last line forward). Kept for KEEP_DAYS.
"""
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
KEEP_DAYS = 120
_DATE_RE = re.compile(r"/Date\((-?\d+)")


def transloc_epoch(value: Any) -> Optional[float]:
    """'/Date(1792380738960-0600)/' -> epoch seconds (the number is UTC ms; the offset is display only)."""
    m = _DATE_RE.search(str(value or ""))
    return int(m.group(1)) / 1000.0 if m else None


def route_rows(routes: Iterable[Dict[str, Any]], name_keywords: Tuple[str, ...]) -> List[Dict[str, Any]]:
    """Compact rows for the routes whose RouteDescription contains one of name_keywords."""
    rows = []
    for r in routes or []:
        name = str(r.get("RouteDescription") or "")
        if not any(k.lower() in name.lower() for k in name_keywords):
            continue
        vehicles = sorted(
            [[str(v.get("VehicleID")), str(v.get("VehicleName") or ""), v.get("Adjustment"), v.get("Severity")]
             for v in (r.get("VehicleAntiBunching") or [])],
            key=lambda v: v[0],
        )
        rows.append({
            "route_id": str(r.get("RouteID")), "route": name,
            "assigned": r.get("AssignedVehicles"), "loop_s": r.get("LoopSeconds"), "target_gap_s": r.get("TargetGap"),
            "tl_ts": transloc_epoch(r.get("TimeStampUTC")), "vehicles": vehicles,
        })
    return rows


class AntiBunchingLog:
    def __init__(self, directory: Path, name_keywords: Tuple[str, ...] = ("Purple Line",)):
        self.directory = Path(directory)
        self.name_keywords = name_keywords
        self._last: Dict[str, Tuple] = {}  # route id -> what was last written
        self._pruned_for: Optional[str] = None

    def _path(self, day: str) -> Path:
        return self.directory / f"{day}.jsonl"

    def record(self, routes: Iterable[Dict[str, Any]], now: float) -> int:
        """Append the changed routes' guidance; returns the number of lines written."""
        day = datetime.fromtimestamp(now, NY_TZ).strftime("%Y-%m-%d")
        lines = []
        for row in route_rows(routes, self.name_keywords):
            key = (row["assigned"], row["loop_s"], row["target_gap_s"], tuple(map(tuple, row["vehicles"])))
            if not row["vehicles"] and not row["assigned"]:
                key = ()  # route not running: one line when it stops, then nothing
            prev = self._last.get(row["route_id"])
            if prev is not None and prev[0] == day and prev[1] == key:
                continue
            self._last[row["route_id"]] = (day, key)
            if not key and prev is None:
                continue  # not running and nothing logged for it today
            lines.append(json.dumps({"t": round(now, 1), **row}, separators=(",", ":")))
        if lines:
            self.directory.mkdir(parents=True, exist_ok=True)
            with self._path(day).open("a", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        if self._pruned_for != day:
            self._pruned_for = day
            self.prune(now)
        return len(lines)

    def prune(self, now: float) -> None:
        cutoff = (datetime.fromtimestamp(now, NY_TZ) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
        for p in self.directory.glob("*.jsonl"):
            if p.stem < cutoff:
                try:
                    p.unlink()
                except OSError:
                    pass

    def read(self, day: str) -> List[Dict[str, Any]]:
        path = self._path(day)
        if not path.exists():
            return []
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    def days(self) -> List[str]:
        return sorted(p.stem for p in self.directory.glob("*.jsonl")) if self.directory.exists() else []
