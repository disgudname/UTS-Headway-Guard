import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import cat_gtfs as cg


# --- _Service.active_on ------------------------------------------------------------


def test_service_active_on_matches_weekday_within_range():
    # Mon, Tue active; Wed off. Range covers the whole week either way.
    svc = cg._Service(
        weekdays=(True, True, False, False, False, False, False),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
    )
    assert svc.active_on(date(2026, 9, 14)) is True  # a Monday
    assert svc.active_on(date(2026, 9, 16)) is False  # a Wednesday


def test_service_active_on_respects_date_range_bounds():
    svc = cg._Service(
        weekdays=(True,) * 7,
        start_date=date(2026, 9, 15),
        end_date=date(2026, 9, 20),
    )
    assert svc.active_on(date(2026, 9, 14)) is False  # before start
    assert svc.active_on(date(2026, 9, 15)) is True
    assert svc.active_on(date(2026, 9, 21)) is False  # after end


def test_service_active_on_calendar_dates_exceptions_override_the_weekday_mask():
    svc = cg._Service(
        weekdays=(False,) * 7,  # never runs by the normal weekday mask
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        added=frozenset({date(2026, 12, 25)}),  # a special holiday run
    )
    assert svc.active_on(date(2026, 12, 25)) is True
    assert svc.active_on(date(2026, 12, 26)) is False

    svc2 = cg._Service(
        weekdays=(True,) * 7,  # normally runs every day
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        removed=frozenset({date(2026, 12, 25)}),  # except this one
    )
    assert svc2.active_on(date(2026, 12, 25)) is False
    assert svc2.active_on(date(2026, 12, 24)) is True


# --- _parse_services ----------------------------------------------------------------


def test_parse_services_reads_calendar_and_calendar_dates():
    calendar_csv = (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        '"WE",1,1,1,1,1,0,0,"20260101","20261231"\n'
        '"SA",0,0,0,0,0,1,0,"20260101","20261231"\n'
    )
    calendar_dates_csv = (
        "service_id,date,exception_type\n"
        '"WE","20260904","2"\n'  # a weekday removed (e.g. a holiday)
        '"HOLIDAY","20260904","1"\n'  # a service that exists only via this addition
    )
    services = cg._parse_services(calendar_csv, calendar_dates_csv)
    assert set(services) == {"WE", "SA", "HOLIDAY"}
    assert services["WE"].active_on(date(2026, 9, 7)) is True  # an ordinary Monday
    assert services["WE"].active_on(date(2026, 9, 4)) is False  # removed via calendar_dates
    assert services["HOLIDAY"].active_on(date(2026, 9, 4)) is True
    assert services["HOLIDAY"].active_on(date(2026, 9, 5)) is False


# --- _parse_departures ---------------------------------------------------------------


def test_parse_departures_extracts_pattern_id_from_trip_short_name_suffix():
    trips_csv = (
        "route_id,service_id,trip_id,trip_short_name\n"
        '"1","WE","T1","06:30:00-50"\n'
        '"1","WE","T2","not-a-pattern-suffix"\n'  # no trailing -NN -- should be skipped
    )
    stop_times_csv = (
        "trip_id,stop_id,stop_sequence,arrival_time,departure_time\n"
        '"T1","12800",1,"06:30:00","06:30:00"\n'
        '"T1","14926",2,"06:31:00","06:31:00"\n'
        '"T2","99999",1,"07:00:00","07:00:00"\n'
    )
    departures = cg._parse_departures(trips_csv, stop_times_csv)
    assert set(departures) == {"50"}
    assert departures["50"]["12800"] == [("WE", 6 * 3600 + 30 * 60)]
    assert departures["50"]["14926"] == [("WE", 6 * 3600 + 31 * 60)]


# --- scheduled_departures_s (end-to-end against a hand-built _Schedule) -------------


def _install_fixture_schedule():
    services = {
        "WE": cg._Service(
            weekdays=(True, True, True, True, True, False, False),
            start_date=date(2026, 1, 1),
            end_date=date(2026, 12, 31),
        ),
    }
    departures = {
        "50": {
            "12800": [
                ("WE", 6 * 3600 + 30 * 60),  # 06:30:00
                ("WE", 25 * 3600 + 30 * 60),  # 25:30:00 -- 1:30am the NEXT calendar day
            ],
        },
    }
    cg._schedule = cg._Schedule(services=services, departures=departures, refreshed_at=cg.datetime.now(cg.NY_TZ))


def test_scheduled_departures_s_returns_seconds_since_midnight_for_the_target_date():
    _install_fixture_schedule()
    try:
        # 2026-09-14 is a Monday -- "WE" is active.
        deps = cg.scheduled_departures_s("50", "12800", date(2026, 9, 14))
        assert 6 * 3600 + 30 * 60 in deps
    finally:
        cg._schedule = None


def test_scheduled_departures_s_rolls_yesterdays_past_midnight_trip_into_today():
    _install_fixture_schedule()
    try:
        # 2026-09-15 is a Tuesday. The 25:30:00 trip belongs to Monday's ("WE")
        # service day but lands at 1:30am Tuesday -- it should surface as a
        # Tuesday departure at 1*3600+30*60 seconds since Tuesday's own midnight.
        deps = cg.scheduled_departures_s("50", "12800", date(2026, 9, 15))
        assert (1 * 3600 + 30 * 60) in deps
    finally:
        cg._schedule = None


def test_scheduled_departures_s_empty_when_service_not_active_that_day():
    _install_fixture_schedule()
    try:
        # 2026-09-20 is a Sunday -- "WE" (Mon-Fri) doesn't run that day, AND its
        # preceding day (Saturday) doesn't run either, so there's no same-day
        # trip and no rollover from the day before to worry about.
        assert cg.scheduled_departures_s("50", "12800", date(2026, 9, 20)) == []
    finally:
        cg._schedule = None


def test_scheduled_departures_s_empty_for_unknown_pattern_or_stop():
    _install_fixture_schedule()
    try:
        assert cg.scheduled_departures_s("999", "12800", date(2026, 9, 14)) == []
        assert cg.scheduled_departures_s("50", "no-such-stop", date(2026, 9, 14)) == []
    finally:
        cg._schedule = None


def test_scheduled_departures_s_empty_when_nothing_loaded():
    cg._schedule = None
    assert cg.scheduled_departures_s("50", "12800", date(2026, 9, 14)) == []
