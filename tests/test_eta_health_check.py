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


def _filler(n=60, historical_pct=100.0):
    """Ordinary well-predicted rows so the run isn't 'too little data'."""
    n_hist = round(n * historical_pct / 100.0)
    return [{
        "t": 1000.0 + i, "key": ("59", f"{700 + i}", "16"), "remaining": 300.0, "past_m": -500.0,
        "ours": 10.0, "tl": 20.0, "ours_s": 300.0, "tl_s": 320.0,
        "source": "historical" if i < n_hist else "projected",
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


def test_thin_history_is_a_note_not_a_breach():
    # Weekend daytime runs sit at ~10-15% (07:30 Sun 11.8%, 09:49 Sun 14.2%) while the weekend routes' history builds.
    out, problems = h.analyze(_filler(100, historical_pct=12.0), {})
    assert not problems
    assert any("thin history" in n for n in out["notes"])


def test_a_collapsed_history_share_is_still_a_breach():
    # The original bug (history grouped by an empty field) measured ~1%.
    out, problems = h.analyze(_filler(100, historical_pct=1.0), {})
    assert any("emptied" in p for p in problems)
    assert "notes" not in out


def test_healthy_history_share_has_neither():
    out, problems = h.analyze(_filler(100, historical_pct=60.0), {})
    assert not problems and "notes" not in out


# --- "visits TransLoc predicted that we didn't": warm-up and single-poll noise ---------------------------------

def _track(n_polls, missing, key=("57", "830", "39"), t0=2000.0):
    """One (route, stop, vehicle) scored at n_polls consecutive 15 s polls; `missing` = poll indexes where we
    had no prediction but TransLoc did."""
    return [{
        "t": t0 + 15.0 * i, "key": key, "remaining": 300.0, "past_m": -500.0,
        "ours": None if i in missing else 10.0, "tl": 20.0,
        "ours_s": None if i in missing else 300.0, "tl_s": 320.0, "source": "historical",
    } for i in range(n_polls)]


def test_gaps_in_the_first_polls_of_a_run_are_warmup_not_a_breach():
    rows = _filler() + _track(10, missing={0, 1})
    warmup_until = 2000.0 + 15.0  # the first two polls
    out, problems = h.analyze(rows, {}, warmup_until)
    assert out["only_transloc"] == 0
    assert out["only_transloc_ignored"] == {"warmup": 2, "single_poll": 0}
    assert not any("TransLoc predicted" in p for p in problems)


def test_a_single_poll_dropout_mid_run_is_ignored_but_still_counted():
    rows = _filler() + _track(10, missing={5})
    out, problems = h.analyze(rows, {}, None)
    assert out["only_transloc"] == 0
    assert out["only_transloc_ignored"] == {"warmup": 0, "single_poll": 1}
    assert not any("TransLoc predicted" in p for p in problems)


def test_a_gap_of_two_or_more_polls_is_still_a_breach():
    rows = _filler() + _track(10, missing={5, 6})
    out, problems = h.analyze(rows, {}, None)
    assert out["only_transloc"] == 2
    assert "only_transloc_ignored" not in out
    assert any("2 visit(s) TransLoc predicted" in p for p in problems)


def test_a_vehicle_we_never_predicted_is_still_a_breach():
    rows = _filler() + _track(6, missing={0, 1, 2, 3, 4, 5})
    out, problems = h.analyze(rows, {}, None)
    assert out["only_transloc"] == 6
    assert any("TransLoc predicted" in p for p in problems)


def test_warmup_and_single_poll_gaps_are_split_independently():
    rows = _filler() + _track(12, missing={0, 6}, key=("57", "830", "39")) + _track(12, missing={8, 9}, key=("57", "831", "39"))
    persistent, warmup, single, gaps = h.transloc_only_gaps(rows, warmup_until=2000.0 + 15.0)
    assert (persistent, warmup, single, gaps) == (2, 1, 1, 1)


def test_first_poll_on_a_new_route_is_a_one_poll_lag_not_a_breach():
    # TransLoc's arrivals moved Gold buses 18/39 to route 57 one poll before its vehicle feed did (2026-09-25 17:57):
    # nothing on that route for the bus the poll before, we have it the poll after.
    rows = _filler() + _track(10, missing={0}, t0=2500.0)
    out, problems = h.analyze(rows, {}, None)
    assert out["only_transloc"] == 0
    assert out["only_transloc_ignored"] == {"warmup": 0, "single_poll": 1}
    assert not any("TransLoc predicted" in p for p in problems)


def test_a_bus_missing_at_many_stops_is_one_gap():
    rows = _filler()
    for stop in ("700", "701", "702"):
        rows += _track(10, missing={4, 5, 6}, key=("55", stop, "13"))
    out, problems = h.analyze(rows, {}, None)
    assert out["only_transloc"] == 9
    assert out["only_transloc_gaps"] == 1
    assert any("9 visit(s) TransLoc predicted that we didn't (1 separate bus gap(s))" in p for p in problems)
