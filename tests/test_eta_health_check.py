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


# --- whose fault: buses slow vs the engine ---------------------------------------------------------------------

def _run(median, close=-10, when="2026-10-08T17:00:01-04:00", day="Thu", **extra):
    route = {"n": 500, "median_err_s": median, "median_abs_err_s": abs(median), "close_in_median_err_s": close}
    return {"when": when, "day": day, "minutes": 30.0, "service_level": "full", "routes": {"Gold": route}, **extra}


def _earlier(medians):
    return [_run(m, when=f"2026-09-{21 + i:02d}T17:00:01-04:00", day="Mon") for i, m in enumerate(medians)]


def test_slower_than_the_same_slot_usually_runs_is_the_buses():
    causes = h.diagnose(_run(-170), _earlier([-40, -60, -50, -70]))
    assert len(causes) == 1 and "BUSES SLOW today" in causes[0] and "not the engine" in causes[0]


def test_the_same_gap_most_days_is_the_engines_lean_not_traffic():
    causes = h.diagnose(_run(-95), _earlier([-80, -100, -90, -85]))
    assert len(causes) == 1 and "ENGINE LEAN" in causes[0]


def test_an_eta_that_is_off_close_in_is_the_engine():
    causes = h.diagnose(_run(-170, close=-120), _earlier([-40, -60, -50, -70]))
    assert len(causes) == 1 and causes[0].startswith("Gold: ENGINE --")


def test_buses_arriving_before_our_eta_is_the_engine():
    causes = h.diagnose(_run(80), _earlier([-40, -60, -50, -70]))
    assert "BEFORE our ETA" in causes[0] and "ENGINE" in causes[0]


def test_a_slot_with_no_past_runs_does_not_guess_today_vs_every_day():
    causes = h.diagnose(_run(-170), _earlier([-40]) + [_run(-40, when="2026-09-27T17:00:01-04:00", day="Sun")] * 5)
    assert "too few earlier runs" in causes[0]


def test_flips_and_missing_predictions_are_the_engine_and_a_route_on_target_says_nothing():
    run = _run(-30, breaches=["3 full-lap flip(s): our ETA >20 min off while TransLoc was within 2 min",
                              "Purple Line: 20.4% of predictions >2 min late (limit 12.0%)"])
    causes = h.diagnose(run, [])
    assert causes[0].startswith("ENGINE -- 3 full-lap") and causes[1].startswith("Purple staging")


def test_a_football_saturday_is_not_compared_with_ordinary_saturdays():
    sats = [_run(-40, when=f"2026-09-{d:02d}T12:30:01-04:00", day="Sat") for d in (5, 12, 19, 26)]
    game = _run(-170, when="2026-10-10T12:30:01-04:00", day="Sat",
                calendar={"service_date": "2026-10-10", "services": {}, "notes": "Home Football Game Route Service Ends at 4PM"})
    assert "too few earlier runs" in h.diagnose(game, sats)[0]
    assert "BUSES SLOW today" in h.diagnose(_run(-170, when="2026-10-17T12:30:01-04:00", day="Sat"), sats + [game])[0]


def test_an_after_midnight_run_reads_the_calendar_of_the_evening_before(monkeypatch):
    import datetime
    asked = []

    def fetch(path):
        asked.append(path)
        return {"days": [{"date": "2026-10-10", "services": {"Night Pilot": "No Service"}, "notes": "Home Football Game"}]}

    monkeypatch.setattr(h.eta_watch, "fetch", fetch)
    cal = h.service_calendar(datetime.datetime(2026, 10, 11, 1, 30))
    assert "start=2026-10-10" in asked[0]
    assert cal["service_date"] == "2026-10-10" and cal["services"]["Night Pilot"] == "No Service"
    assert h.is_game_day({"calendar": cal})


def test_a_breach_pinned_on_one_bus_is_not_called_the_engine():
    # 2026-10-13 08:30: [09] left Runk a minute before its sheet time; eta_suspects said ONE BUS.
    run = _run(-30, breaches=["4.5% of predictions >2 min late (limit 3.0%)",
                              "3 full-lap flip(s): our ETA >20 min off while TransLoc was within 2 min"],
               suspect_verdicts=[("late", "ONE BUS"), ("full-lap flip", "ENGINE, ONE BUS")])
    causes = h.diagnose(run, [])
    assert causes[0].startswith("ONE BUS (not the engine")
    assert causes[1].startswith("ENGINE, ONE BUS")


def test_a_few_explained_buses_together_are_not_the_engine():
    run = _run(-30, breaches=["9.5% of predictions >2 min late (limit 3.0%)"],
               suspect_verdicts=[("late", "ONE OF A FEW BUSES"), ("late", "A FEW BUSES")])
    assert h.diagnose(run, [])[0].startswith("ONE BUS (not the engine")


def test_late_spread_over_several_buses_is_still_the_engine():
    run = _run(-30, breaches=["9.5% of predictions >2 min late (limit 3.0%)"],
               suspect_verdicts=[("late", "SEVERAL BUSES")])
    assert h.diagnose(run, [])[0].startswith("ENGINE --")


def _polls(track, ours_block="[09]"):
    """Watch-log polls for one bus (vehicle 21, route 67) from [(t, lat, lon)]."""
    return [{"t": t, "veh": [["21", "67", lat, lon, 0.0]], "ours": [["67", "s1", "21", 60.0, "historical", ours_block]],
             "tl": []} for t, lat, lon in track]


def test_suspects_see_a_gps_freeze_and_not_a_stop():
    import eta_suspects as s
    # Same fix for 5 min, then 1.1 km on: the Emmet/McCormick dead spot (2026-10-13 12:30, 25231 [12]).
    track = [(1000.0 + 15 * i, 38.0300 + 0.0002 * i, -78.5) for i in range(5)]
    track += [(1075.0 + 15 * i, 38.0308, -78.5) for i in range(21)]
    track += [(1075.0 + 15 * 21, 38.0408, -78.5)]
    stops = {("67", "s1"): (38.0308, -78.5, "Emmet St @ Contemplative Commons", 0.0)}
    lines = {"67": ([(38.02, -78.5), (38.05, -78.5)], [0.0, 3336.0])}
    tags = s.bus_tags("21", "67", 1100.0, 1350.0, _polls(track), stops, lines)
    assert any(t.startswith("GPS froze") for t in tags)
    assert not any(t.startswith("sat still") for t in tags)


def test_suspects_see_a_bus_leave_a_timestop_before_its_sheet_time(monkeypatch):
    import eta_suspects as s
    # Sat at the stop 08:43:45-08:48:45, sheet 08:50 (2026-10-13 08:30, 20131 [09] at Runk).
    track = [(1000.0 + 15 * i, 38.0300, -78.5) for i in range(21)] + [(1315.0, 38.0320, -78.5)]
    stops = {("67", "s1"): (38.0300, -78.5, "Hereford Dr @ Runk Dining Hall", 1000.0)}
    lines = {"67": ([(38.02, -78.5), (38.05, -78.5)], [0.0, 3336.0])}

    class Blocks:
        @staticmethod
        def scheduled_hold_epoch(route, stop, block, ts):
            return 1375.0
    monkeypatch.setattr(s, "uts_blocks", Blocks)
    tags = s.bus_tags("21", "67", 1000.0, 1300.0, _polls(track), stops, lines, block="[09]")
    assert any("BEFORE its sheet time" in t for t in tags)
