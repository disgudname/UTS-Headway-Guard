from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Set
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


CSV_HEADER = [
    "start_time",
    "end_time",
    "vehicle_id",
    "vehicle_name",
    "block",
    "route_id",
    "route_name",
    "nearest_stop_id",
    "nearest_stop_name",
    "lat",
    "lon",
    "capacity",
    "peak_occupation",
]


@dataclass
class FullBusEvent:
    start_time: datetime
    end_time: datetime
    vehicle_id: str
    vehicle_name: str
    block: str
    route_id: str
    route_name: str
    nearest_stop_id: str
    nearest_stop_name: str
    lat: float
    lon: float
    capacity: int
    peak_occupation: int

    def to_row(self) -> List[str]:
        return [
            _isoformat(self.start_time),
            _isoformat(self.end_time),
            self.vehicle_id or "",
            self.vehicle_name or "",
            self.block or "",
            self.route_id or "",
            self.route_name or "",
            self.nearest_stop_id or "",
            self.nearest_stop_name or "",
            f"{self.lat:.6f}" if self.lat is not None else "",
            f"{self.lon:.6f}" if self.lon is not None else "",
            str(self.capacity) if self.capacity is not None else "",
            str(self.peak_occupation) if self.peak_occupation is not None else "",
        ]

    def to_dict(self) -> dict:
        return {
            "start_time": _isoformat(self.start_time),
            "end_time": _isoformat(self.end_time),
            "vehicle_id": self.vehicle_id,
            "vehicle_name": self.vehicle_name,
            "block": self.block,
            "route_id": self.route_id,
            "route_name": self.route_name,
            "nearest_stop_id": self.nearest_stop_id,
            "nearest_stop_name": self.nearest_stop_name,
            "lat": self.lat,
            "lon": self.lon,
            "capacity": self.capacity,
            "peak_occupation": self.peak_occupation,
        }


class FullBusStorage:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def _file_for_date(self, dt: datetime) -> Path:
        return self.base_dir / f"{_to_utc(dt).date().isoformat()}.csv"

    def write_event(self, event: FullBusEvent) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        path = self._file_for_date(event.start_time)
        with path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(event.to_row())

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

    def _iter_files(self, start: datetime, end: datetime):
        current = _to_utc(start).date()
        end_date = _to_utc(end).date()
        while current <= end_date:
            yield self.base_dir / f"{current.isoformat()}.csv"
            current += timedelta(days=1)

    def query_events(
        self,
        start: datetime,
        end: datetime,
        route_ids: Optional[Set[str]] = None,
        stop_ids: Optional[Set[str]] = None,
    ) -> List[FullBusEvent]:
        start_utc = _to_utc(start)
        end_utc = _to_utc(end)
        if end_utc < start_utc:
            return []

        events: List[FullBusEvent] = []
        for path in self._iter_files(start_utc, end_utc):
            if not path.exists():
                continue
            with path.open("r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 13:
                        continue
                    try:
                        start_ts = parse_iso8601_utc(row[0])
                    except ValueError:
                        continue
                    if start_ts < start_utc or start_ts > end_utc:
                        continue

                    route_id = row[5] or ""
                    stop_id = row[7] or ""

                    if route_ids and route_id not in route_ids:
                        continue
                    if stop_ids and stop_id not in stop_ids:
                        continue

                    try:
                        end_ts = parse_iso8601_utc(row[1])
                    except ValueError:
                        continue

                    lat = None
                    lon = None
                    try:
                        lat = float(row[9]) if row[9] else None
                    except ValueError:
                        pass
                    try:
                        lon = float(row[10]) if row[10] else None
                    except ValueError:
                        pass

                    capacity = None
                    peak_occ = None
                    try:
                        capacity = int(row[11]) if row[11] else None
                    except ValueError:
                        pass
                    try:
                        peak_occ = int(row[12]) if row[12] else None
                    except ValueError:
                        pass

                    events.append(
                        FullBusEvent(
                            start_time=start_ts,
                            end_time=end_ts,
                            vehicle_id=row[2] or "",
                            vehicle_name=row[3] or "",
                            block=row[4] or "",
                            route_id=route_id,
                            route_name=row[6] or "",
                            nearest_stop_id=stop_id,
                            nearest_stop_name=row[8] or "",
                            lat=lat,
                            lon=lon,
                            capacity=capacity,
                            peak_occupation=peak_occ,
                        )
                    )
        events.sort(key=lambda e: e.start_time)
        return events
