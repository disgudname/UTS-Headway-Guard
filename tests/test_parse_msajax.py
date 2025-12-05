import datetime
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import parse_msajax


def test_parse_msajax_with_offset_negative():
    # Example from TransLoc with negative offset; offset should shift earlier
    raw = "/Date(1764922344000-0700)/"
    ts_ms = parse_msajax(raw)
    assert ts_ms is not None
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
    assert dt == datetime.datetime(2025, 12, 5, 1, 12, 24)


def test_parse_msajax_without_offset():
    raw = "/Date(1764922344000)/"
    ts_ms = parse_msajax(raw)
    assert ts_ms == 1764922344000
