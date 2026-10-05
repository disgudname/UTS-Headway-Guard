import json
import sys
from datetime import date
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import service_levels as sl


def test_calendar_text_maps_to_a_level():
    assert sl.level_from_calendar("Recess Service") == "recess"
    assert sl.level_from_calendar("Full Service") == "full"
    assert sl.level_from_calendar("No Service") == "none"
    assert sl.level_from_calendar("Summer Service") == "summer"
    assert sl.level_from_calendar("Exam Service") == "exam"
    assert sl.level_from_calendar("") is None
    assert sl.level_from_calendar("8:00 PM - 5:00 AM") is None


def test_defaults_for_days_nobody_logged(tmp_path):
    log = sl.ServiceLevelLog(tmp_path)
    assert log.level(date(2026, 4, 28)) is None          # before the known ranges: unknown
    assert log.level(date(2026, 4, 29)) == log.level(date(2026, 5, 8)) == "exam"
    assert log.level(date(2026, 5, 10)) is None          # the weekend between exams and summer
    assert sl.history_class("exam") is None              # noted, but regular history
    assert log.level(date(2026, 5, 11)) == "summer"
    assert log.level(date(2026, 8, 19)) == "summer"
    assert log.level(date(2026, 8, 20)) == "full"
    assert log.entry(date(2026, 7, 1))["source"] == "default"
    assert sl.history_class("summer") == sl.history_class("recess") == "recess"
    assert sl.history_class("full") is None and sl.history_class("none") is None and sl.history_class(None) is None


def test_logged_day_overrides_the_default_and_survives_a_restart(tmp_path):
    log = sl.ServiceLevelLog(tmp_path)
    entry = {"services": {"UVA Transit": "Recess Service", "Night Pilot": "No Service"}, "notes": "Fall Break"}
    assert log.record_calendar_day(date(2026, 10, 5), entry) is True
    assert log.record_calendar_day(date(2026, 10, 5), entry) is False   # unchanged: nothing rewritten
    assert log.record_calendar_day(date(2026, 10, 7), {"services": {"UVA Transit": "?"}}) is False
    again = sl.ServiceLevelLog(tmp_path)
    assert again.level(date(2026, 10, 5)) == "recess"
    assert again.entry(date(2026, 10, 5)) == {
        "date": "2026-10-05", "level": "recess", "label": "Recess Service", "notes": "Fall Break", "source": "calendar",
    }
    assert again.level(date(2026, 10, 7)) == "full"
    assert list(json.loads((tmp_path / "service_levels.json").read_text())["days"]) == ["2026-10-05"]
