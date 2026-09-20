import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "scripts"))

import eta_health_check as h  # noqa: E402


def _row(ours, tl, ours_s, past_m, n=0):
    return {
        "t": 1000.0 + n, "key": ("59", "737", "16"), "remaining": 1500.0, "past_m": past_m,
        "ours": ours, "tl": tl, "ours_s": ours_s, "tl_s": 1500.0 + tl, "source": "historical",
    }


def _filler(n=60):
    """Ordinary well-predicted rows so the run isn't 'too little data'."""
    return [{
        "t": 1000.0 + i, "key": ("59", f"{700 + i}", "16"), "remaining": 300.0, "past_m": -500.0,
        "ours": 10.0, "tl": 20.0, "ours_s": 300.0, "tl_s": 320.0, "source": "historical",
    } for i in range(n)]


def test_a_due_shown_just_after_the_bus_passed_is_not_a_full_lap_flip():
    # Night Pilot 2026-09-20 01:30: "Due" with the bus 24-216 m past the stop; the scorer matched it to
    # the next lap ~24 min away.
    rows = _filler() + [_row(-1500.0, 20.0, 0.0, 216.0), _row(-1420.0, 111.0, 0.0, 24.0)]
    out, problems = h.analyze(rows, {})
    assert out["full_lap_flips"] == 0
    assert out["flips_just_passed"] == 2
    assert not any("full-lap" in p for p in problems)


def test_a_real_full_lap_flip_is_still_flagged():
    # Ours says ~2600 s (a lap away) while TransLoc is right: not a display lag.
    rows = _filler() + [_row(2500.0, 30.0, 2600.0, -800.0)]
    out, problems = h.analyze(rows, {})
    assert out["full_lap_flips"] == 1
    assert any("full-lap" in p for p in problems)


def test_a_due_far_from_the_stop_is_still_flagged():
    # "Due" while the bus is nowhere near the stop (behind it, or 3 km past it) is a real error.
    rows = _filler() + [_row(-1500.0, 20.0, 0.0, -600.0, 1), _row(-1500.0, 20.0, 0.0, 3000.0, 2)]
    out, _ = h.analyze(rows, {})
    assert out["full_lap_flips"] == 2
    assert out["flips_just_passed"] == 0


def test_only_a_due_reading_gets_the_exemption():
    # The bus is 100 m past the stop but we predicted 20 minutes: a real miss, not display lag.
    rows = _filler() + [_row(2500.0, 30.0, 1200.0, 100.0)]
    out, _ = h.analyze(rows, {})
    assert out["full_lap_flips"] == 1
