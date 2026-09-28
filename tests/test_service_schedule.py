from datetime import date

import pytest

from service_schedule import ServiceSchedule, parse_page_date, parse_table

HEADERS = ["Service Date", "UVA Transit", "UVA Ride", "Night Pilot", "UVA FlexRide", "Notes"]
ROWS = [
    ["Sep 28 - Monday", "Full Service", "10:00 PM - 5:00 AM", "10:00 PM - 2:00 AM", "In Service", ""],
    ["Oct 3 - Saturday", "No Service", "8:00 PM - 5:00 AM", "No Service", "No Service", "Fall Break"],
]


def test_year_is_the_nearest_one():
    assert parse_page_date("Jan 2 - Friday", date(2026, 12, 20)) == date(2027, 1, 2)
    assert parse_page_date("Dec 30 - Wednesday", date(2027, 1, 3)) == date(2026, 12, 30)
    assert parse_page_date("Sept 28 - Monday", date(2026, 9, 1)) == date(2026, 9, 28)
    assert parse_page_date("Service Date", date(2026, 9, 1)) is None


def test_columns_by_header_and_notes_split_out():
    days = parse_table(HEADERS, ROWS, date(2026, 9, 28))
    assert days["2026-10-03"] == {
        "label": "Oct 3 - Saturday",
        "services": {"UVA Transit": "No Service", "UVA Ride": "8:00 PM - 5:00 AM", "Night Pilot": "No Service",
                     "UVA FlexRide": "No Service"},
        "notes": "Fall Break",
    }


def test_bad_table_rejected():
    with pytest.raises(ValueError):
        parse_table(["Foo", "Bar"], [["a", "b"]], date(2026, 9, 28))


def test_bad_pull_keeps_last_good_and_history_is_merged(tmp_path):
    sched = ServiceSchedule(tmp_path)
    tables = [{"headers": HEADERS, "rows": ROWS[:1]}, {"headers": HEADERS, "rows": ROWS[1:]}]
    assert sched.apply(tables, date(2026, 9, 28)) == ["2026-09-28", "2026-10-03"]
    assert sched.apply([tables[0], {"headers": HEADERS, "rows": []}], date(2026, 9, 29)) == []
    assert sched.last_error
    assert sched.apply(tables[1:], date(2026, 9, 29)) == []
    reloaded = ServiceSchedule(tmp_path)
    assert set(reloaded.days) == {"2026-09-28", "2026-10-03"}  # Sep 28 kept after it left the page
    status = reloaded.status(date(2026, 10, 3), date(2026, 10, 3), 3)
    assert status["today"]["services"]["UVA Transit"] == "No Service"
    assert len(status["days"]) == 1
