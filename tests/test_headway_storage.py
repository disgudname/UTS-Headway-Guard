import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from headway_storage import HeadwayEvent, HeadwayStorage


def _event(ts: datetime) -> HeadwayEvent:
    return HeadwayEvent(
        timestamp=ts,
        route_id="1",
        stop_id="A",
        vehicle_id=None,
        vehicle_name=None,
        event_type="arrival",
        headway_arrival_arrival=None,
        headway_departure_arrival=None,
        dwell_seconds=None,
    )


def test_write_events_groups_by_service_day_and_query(tmp_path):
    storage = HeadwayStorage(tmp_path)
    day_one_ts = datetime(2024, 5, 1, 23, 30, tzinfo=timezone.utc)
    day_two_ts = datetime(2024, 5, 2, 0, 15, tzinfo=timezone.utc)

    storage.write_events([_event(day_one_ts), _event(day_two_ts)])

    day_one_file = tmp_path / "2024-05-01.csv"
    day_two_file = tmp_path / "2024-05-02.csv"

    assert day_one_file.exists()
    assert day_two_file.exists()

    with day_one_file.open() as f:
        rows_day_one = list(csv.reader(f))
    with day_two_file.open() as f:
        rows_day_two = list(csv.reader(f))

    assert [row[0] for row in rows_day_one] == ["2024-05-01T23:30:00Z"]
    assert [row[0] for row in rows_day_two] == ["2024-05-02T00:15:00Z"]

    queried = storage.query_events(day_one_ts - timedelta(hours=1), day_two_ts + timedelta(hours=1))
    assert [event.timestamp for event in queried] == [day_one_ts, day_two_ts]
