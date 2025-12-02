from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple
import csv


ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return _to_utc(dt).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso8601_utc(value: str) -> datetime:
    text = value.strip()
    if text.lower().endswith("z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


@dataclass
class HeadwayEvent:
    timestamp: datetime
    route_id: Optional[str]
    stop_id: Optional[str]
    vehicle_id: Optional[str]
    event_type: str
    headway_seconds: Optional[float]
    dwell_seconds: Optional[float]

    def to_row(self) -> List[str]:
        return [
            _isoformat(self.timestamp),
            self.route_id or "",
            self.stop_id or "",
            self.vehicle_id or "",
            self.event_type,
            "" if self.headway_seconds is None else f"{self.headway_seconds:.3f}",
            "" if self.dwell_seconds is None else f"{self.dwell_seconds:.3f}",
        ]

    def to_dict(self) -> dict:
        return {
            "timestamp": _isoformat(self.timestamp),
            "route_id": self.route_id,
            "stop_id": self.stop_id,
            "vehicle_id": self.vehicle_id,
            "event_type": self.event_type,
            "headway_seconds": self.headway_seconds,
            "dwell_seconds": self.dwell_seconds,
        }


class HeadwayStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def _file_for_date(self, dt: datetime) -> Path:
        return self.base_dir / f"{_to_utc(dt).date().isoformat()}.csv"

    def write_events(self, events: Sequence[HeadwayEvent]) -> None:
        if not events:
            return
        self.base_dir.mkdir(parents=True, exist_ok=True)
        rows = [e.to_row() for e in events]
        path = self._file_for_date(events[0].timestamp)
        with path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    def _iter_files(self, start: datetime, end: datetime) -> Iterable[Tuple[datetime, Path]]:
        current = _to_utc(start).date()
        end_date = _to_utc(end).date()
        while current <= end_date:
            yield datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc), self.base_dir / f"{current.isoformat()}.csv"
            current += timedelta(days=1)

    def query_events(
        self,
        start: datetime,
        end: datetime,
        route_ids: Optional[Set[str]] = None,
        stop_ids: Optional[Set[str]] = None,
    ) -> List[HeadwayEvent]:
        start_utc = _to_utc(start)
        end_utc = _to_utc(end)
        if end_utc < start_utc:
            return []

        route_filter = {r for r in route_ids} if route_ids else None
        stop_filter = {s for s in stop_ids} if stop_ids else None

        events: List[HeadwayEvent] = []
        for _, path in self._iter_files(start_utc, end_utc):
            if not path.exists():
                continue
            with path.open("r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 5:
                        continue
                    try:
                        ts = parse_iso8601_utc(row[0])
                    except Exception:
                        continue
                    if ts < start_utc or ts > end_utc:
                        continue
                    route_id = row[1] or None
                    stop_id = row[2] or None
                    if route_filter and (route_id is None or route_id not in route_filter):
                        continue
                    if stop_filter and (stop_id is None or stop_id not in stop_filter):
                        continue
                    vehicle_id = row[3] or None
                    event_type = row[4]
                    headway_seconds = None
                    dwell_seconds = None
                    if len(row) > 5 and row[5]:
                        try:
                            headway_seconds = float(row[5])
                        except ValueError:
                            headway_seconds = None
                    if len(row) > 6 and row[6]:
                        try:
                            dwell_seconds = float(row[6])
                        except ValueError:
                            dwell_seconds = None
                    events.append(
                        HeadwayEvent(
                            timestamp=ts,
                            route_id=route_id,
                            stop_id=stop_id,
                            vehicle_id=vehicle_id,
                            event_type=event_type,
                            headway_seconds=headway_seconds,
                            dwell_seconds=dwell_seconds,
                        )
                    )
        events.sort(key=lambda e: e.timestamp)
        return events
