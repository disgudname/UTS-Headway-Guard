import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import eta_health_check  # noqa: E402
import eta_health_scheduler as s  # noqa: E402


def _et(*args):
    return datetime(*args, tzinfo=s.ET)


def test_weekday_morning_slots():
    # Tue 2026-10-27 06:00: inside the 05:00 run (210 min), nothing else open.
    assert s.due_slots(_et(2026, 10, 27, 6, 0)) == [(_et(2026, 10, 27, 5, 0), 210)]


def test_back_to_back_runs_overlap_only_in_the_scoring_grace():
    # 08:35: the 05:00 run is scoring (ended 08:30), the 08:30 run is watching.
    assert s.due_slots(_et(2026, 10, 27, 8, 35)) == [(_et(2026, 10, 27, 5, 0), 210), (_et(2026, 10, 27, 8, 30), 240)]


def test_night_run_crosses_midnight():
    # Wed 00:10 is still Tuesday's 21:45 run (165 min, to 00:30).
    assert s.due_slots(_et(2026, 10, 28, 0, 10)) == [(_et(2026, 10, 27, 21, 45), 165)]


def test_weekend_slots_not_on_weekdays():
    assert s.due_slots(_et(2026, 10, 31, 7, 40)) == [(_et(2026, 10, 31, 7, 30), 30)]   # Saturday
    assert s.due_slots(_et(2026, 10, 27, 7, 40)) == [(_et(2026, 10, 27, 5, 0), 210)]   # Tuesday


def test_finished_starts(tmp_path):
    results = tmp_path / "health_results.jsonl"
    assert s.finished_starts(results) == set()
    results.write_text(json.dumps({"when": "2026-10-27T05:00:00-04:00"}) + "\n\n", encoding="utf-8")
    assert s.finished_starts(results) == {"2026-10-27T05:00"}


def test_read_polls_skips_a_line_cut_off_by_a_restart(tmp_path):
    log = tmp_path / "20261027-0500.jsonl"
    log.write_text('{"t": 1}\n{"t": 2, "ours": [\n{"t": 3}\n', encoding="utf-8")
    assert [p["t"] for p in eta_health_check.read_polls(log)] == [1, 3]


def test_claim_log_gives_each_run_its_own_file(tmp_path, monkeypatch):
    monkeypatch.setattr(eta_health_check, "OUT_DIR", tmp_path)
    now = _et(2026, 10, 26, 18, 39)
    assert eta_health_check.claim_log(now).name == "20261026-1839.jsonl"
    assert eta_health_check.claim_log(now).name == "20261026-1839-2.jsonl"
