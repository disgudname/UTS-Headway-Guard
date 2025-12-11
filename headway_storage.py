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
    block: Optional[str] = None
    vehicle_name: Optional[str] = None
    event_type: str = ""
    headway_arrival_arrival: Optional[float] = None
    headway_departure_arrival: Optional[float] = None
    dwell_seconds: Optional[float] = None

    def to_row(self) -> List[str]:
        return [
            _isoformat(self.timestamp),
            self.route_id or "",
            self.stop_id or "",
            self.vehicle_id or "",
            self.block or "",
            self.vehicle_name or "",
            self.event_type,
            ""
            if self.headway_arrival_arrival is None
            else f"{self.headway_arrival_arrival:.3f}",
            ""
            if self.headway_departure_arrival is None
            else f"{self.headway_departure_arrival:.3f}",
            "" if self.dwell_seconds is None else f"{self.dwell_seconds:.3f}",
        ]

    def to_dict(self) -> dict:
        return {
            "timestamp": _isoformat(self.timestamp),
            "route_id": self.route_id,
            "stop_id": self.stop_id,
            "vehicle_id": self.vehicle_id,
            "block": self.block,
            "vehicle_name": self.vehicle_name,
            "event_type": self.event_type,
            "headway_arrival_arrival": self.headway_arrival_arrival,
            "headway_departure_arrival": self.headway_departure_arrival,
            # Backwards compatibility for consumers expecting the legacy key.
            "headway_seconds": self.headway_arrival_arrival,
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
        grouped_rows: dict[Path, List[List[str]]] = {}
        for event in events:
            path = self._file_for_date(event.timestamp)
            grouped_rows.setdefault(path, []).append(event.to_row())

        for path, rows in grouped_rows.items():
            with path.open("a", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

    def clear(self) -> int:
        if not self.base_dir.exists():
            return 0
        deleted = 0
        for path in self.base_dir.glob("*.csv"):
            try:
                path.unlink()
                deleted += 1
            except FileNotFoundError:
                continue
        return deleted

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
                    block = None
                    vehicle_name = None
                    event_type_idx = 4
                    headway_arrival_idx = 5
                    headway_departure_idx = 6
                    dwell_idx = 7

                    if len(row) >= 10:
                        block = row[4] or None
                        vehicle_name = row[5] or None
                        event_type_idx = 6
                        headway_arrival_idx = 7
                        headway_departure_idx = 8
                        dwell_idx = 9
                    elif len(row) >= 9:
                        vehicle_name = row[4] or None
                        event_type_idx = 5
                        headway_arrival_idx = 6
                        headway_departure_idx = 7
                        dwell_idx = 8

                    event_type = row[event_type_idx] if len(row) > event_type_idx else ""
                    headway_arrival_arrival = None
                    headway_departure_arrival = None
                    dwell_seconds = None

                    if len(row) > headway_arrival_idx and row[headway_arrival_idx]:
                        try:
                            headway_arrival_arrival = float(row[headway_arrival_idx])
                        except ValueError:
                            headway_arrival_arrival = None
                    if len(row) > headway_departure_idx:
                        val = row[headway_departure_idx]
                        if val:
                            try:
                                headway_departure_arrival = float(val)
                            except ValueError:
                                headway_departure_arrival = None
                        if (
                            headway_departure_arrival is None
                            and len(row) == headway_departure_idx + 1
                            and val
                        ):
                            try:
                                dwell_seconds = float(val)
                            except ValueError:
                                dwell_seconds = None
                    if len(row) > dwell_idx and row[dwell_idx]:
                        try:
                            dwell_seconds = float(row[dwell_idx])
                        except ValueError:
                            dwell_seconds = None
                    if len(row) > dwell_idx + 1 and row[dwell_idx + 1] and vehicle_name is None:
                        vehicle_name = row[dwell_idx + 1]
                    events.append(
                        HeadwayEvent(
                            timestamp=ts,
                            route_id=route_id,
                            stop_id=stop_id,
                            vehicle_id=vehicle_id,
                            block=block,
                            vehicle_name=vehicle_name,
                            event_type=event_type,
                            headway_arrival_arrival=headway_arrival_arrival,
                            headway_departure_arrival=headway_departure_arrival,
                            dwell_seconds=dwell_seconds,
                        )
                    )
        events.sort(key=lambda e: e.timestamp)
        return events
