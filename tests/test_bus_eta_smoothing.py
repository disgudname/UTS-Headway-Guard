import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app


def _reset():
    app._bus_eta_history.clear()


def _feed(key, readings, start=1000.0, step=12.0):
    """Feed successive raw readings (seconds-to-arrival) and return what each is published as."""
    out = []
    for i, raw in enumerate(readings):
        out.append(app._smooth_bus_eta_seconds(key, start + i * step, raw))
    return out


def test_a_single_spike_is_ignored():
    _reset()
    # Steady countdown (each poll 12s later, so the raw number drops by 12), with one
    # 3-minute spike in the middle -- the shape seen live at red lights.
    raw = [600, 588, 576 + 180, 564, 552]
    pub = _feed(("54", "1", "16"), raw)
    assert pub[:2] == [600, 588]            # window not full yet: passed through
    assert abs(pub[2] - 576) < 1.0          # spike removed
    assert abs(pub[3] - 564) < 1.0
    assert abs(pub[4] - 552) < 1.0


def test_a_real_trend_is_followed_without_lag():
    _reset()
    # Bus genuinely running 60s slower per poll from here on: the median tracks it
    # within one poll rather than smearing it out.
    raw = [600, 588, 576, 624, 672, 720]
    pub = _feed(("54", "2", "16"), raw)
    assert abs(pub[3] - 576) < 61   # one poll behind at most
    assert abs(pub[5] - 672) < 61


def test_arriving_readings_are_never_delayed():
    _reset()
    pub = _feed(("54", "3", "16"), [300, 100, 40, 8], step=12.0)
    assert pub[-1] == 8


def test_keys_do_not_share_history():
    _reset()
    a = _feed(("54", "4", "16"), [600, 588, 576])
    b = _feed(("54", "4", "17"), [90, 78, 66])
    assert abs(a[2] - 576) < 1.0 and abs(b[2] - 66) < 1.0
