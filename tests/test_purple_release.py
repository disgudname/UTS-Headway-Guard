import sys
import types
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.modules.setdefault("httpx", types.ModuleType("httpx"))

import bus_eta  # noqa: E402
import purple_release as pr  # noqa: E402

NY = ZoneInfo("America/New_York")
# Mon 2026-10-19 09:15 local: the West Complex -> Fontaine rule (offset 0 in the test rules) is in force.
NOW = datetime(2026, 10, 19, 9, 15, tzinfo=NY).timestamp()
RULES = [pr.Rule("West Complex", "Fontaine", 9, 9, 30.0)]
STOPS = [SimpleNamespace(id="987", name="Jefferson Park Ave @ West Complex"),
         SimpleNamespace(id="988", name="Jefferson Park Ave @ Cabell Hall"),
         SimpleNamespace(id="983", name="Ray C. Hunt Dr @ 400 Fontaine Bldg")]


def bus(vid, to_stage, to_fontaine, at=None, since=NOW):
    """A bus whose ETAs are fixed; a release at its staging stop pushes its Fontaine ETA out."""
    def eta(stop, release):
        base = {"987": to_stage, "988": None, "983": to_fontaine}[stop.id]
        if base is None or release is None or stop.id != "983":
            return base
        return max(base, release[1] - NOW + 300.0)  # 300 s West Complex -> Fontaine
    return pr.Bus(vid=vid, stops=STOPS, at_stop_id=at, since=since, eta=eta)


def test_staged_bus_leaves_offset_after_the_bus_ahead_reaches_the_trigger():
    leader = bus("A", to_stage=1500.0, to_fontaine=120.0)
    staged = bus("B", to_stage=0.0, to_fontaine=320.0, at="987")
    assert pr.release_epochs([leader, staged], NOW, RULES) == {"B": ("987", NOW + 150.0)}


def test_bus_ahead_is_the_one_that_left_the_staging_stop_last():
    far = bus("A", to_stage=900.0, to_fontaine=30.0)
    near = bus("C", to_stage=1500.0, to_fontaine=200.0)
    staged = bus("B", to_stage=0.0, to_fontaine=320.0, at="987")
    assert pr.release_epochs([far, near, staged], NOW, RULES)["B"][1] == NOW + 230.0


def test_a_bus_behind_the_staging_stop_is_not_the_bus_ahead():
    behind = bus("A", to_stage=60.0, to_fontaine=400.0)  # reaches the staging stop first
    staged = bus("B", to_stage=0.0, to_fontaine=320.0, at="987")
    assert pr.release_epochs([behind, staged], NOW, RULES) == {}


def test_second_bus_in_the_queue_follows_the_first():
    leader = bus("A", to_stage=1500.0, to_fontaine=120.0)
    first = bus("B", to_stage=0.0, to_fontaine=320.0, at="987", since=NOW - 200)
    second = bus("C", to_stage=0.0, to_fontaine=320.0, at="987", since=NOW - 60)
    out = pr.release_epochs([leader, first, second], NOW, RULES)
    assert out["B"][1] == NOW + 150.0
    # B leaves at +150 and reaches Fontaine at +450; C goes 30 s after that.
    assert out["C"][1] == NOW + 480.0


def test_no_rule_outside_its_hours_or_on_weekends():
    leader = bus("A", to_stage=1500.0, to_fontaine=120.0)
    staged = bus("B", to_stage=0.0, to_fontaine=320.0, at="987")
    assert pr.release_epochs([leader, staged], NOW + 3600, RULES) == {}
    saturday = datetime(2026, 10, 17, 9, 15, tzinfo=NY).timestamp()
    assert pr.release_epochs([leader, staged], saturday, RULES) == {}


def test_release_too_far_out_falls_back_to_history():
    leader = bus("A", to_stage=3000.0, to_fontaine=pr.MAX_RELEASE_S + 60)
    staged = bus("B", to_stage=0.0, to_fontaine=320.0, at="987")
    assert pr.release_epochs([leader, staged], NOW, RULES) == {}


def test_shipped_rules_load():
    rules = pr.load_rules()
    assert rules and all(r.stage and r.trigger and r.first_hour <= r.last_hour for r in rules)


def test_release_dwell_fn_holds_only_the_current_visit():
    dwell = bus_eta.release_dwell_fn(lambda r, s, w: 90.0, "987", NOW + 200.0, NOW)
    assert dwell("74", "987", NOW + 10) == 190.0
    assert dwell("74", "987", NOW + 1200) == 90.0       # same stop a lap later
    assert dwell("74", "988", NOW + 10) == 90.0
    assert dwell("74", "987", NOW + 10) >= bus_eta.TYPICAL_DWELL_S
    late = bus_eta.release_dwell_fn(lambda r, s, w: 90.0, "987", NOW - 60.0, NOW)
    assert late("74", "987", NOW) == bus_eta.TYPICAL_DWELL_S


# --- Fontaine spacing hold (gap_holds) ---
GAP = [pr.GapRule("Fontaine", 9, 9, 270.0, 60.0, 2, -75.0)]


def fbus(vid, to_fontaine, at=None, since=NOW):
    return pr.Bus(vid=vid, stops=STOPS, at_stop_id=at, since=since,
                  eta=lambda stop, release: 0.0 if at == stop.id else (to_fontaine if stop.id == "983" else None))


def seen_leaving(state, vid="L", left=NOW - 100.0):
    """The bus ahead sat at Fontaine, then was gone on the next call."""
    pr.gap_holds([fbus(vid, 0.0, at="983", since=left - 200)], left - 10, GAP, state=state)
    pr.gap_holds([fbus(vid, 900.0)], left + 10, GAP, state=state)


def test_bus_arriving_close_behind_waits_the_gap():
    state = {}
    seen_leaving(state)  # left NOW - 100
    out = pr.gap_holds([fbus("A", 0.0, at="983", since=NOW - 30), fbus("L", 900.0)], NOW, GAP, state=state)
    assert out == {"A": ("983", NOW + 170.0)}  # 270 s behind the bus ahead


def test_bus_arriving_with_a_big_gap_leaves_after_min_dwell():
    state = {}
    seen_leaving(state, left=NOW - 600)
    out = pr.gap_holds([fbus("A", 120.0), fbus("L", 900.0)], NOW, GAP, state=state)
    assert out["A"] == ("983", NOW + 180.0)  # arrives +120, min dwell 60


def test_queue_chains_and_third_bus_pushes_the_first_out():
    state = {}
    seen_leaving(state)
    buses = [fbus("A", 0.0, at="983", since=NOW - 30), fbus("B", 60.0), fbus("C", 120.0), fbus("L", 900.0)]
    out = pr.gap_holds(buses, NOW, GAP, state=state)
    # A would go at +170, but C is due at +120 with A and B there: A goes 75 s before C pulls in.
    assert out["A"][1] == NOW + 45.0
    assert out["B"][1] == NOW + 315.0  # 270 s behind A
    assert out["C"][1] == NOW + 585.0


def test_third_bus_due_after_the_gap_changes_nothing():
    state = {}
    seen_leaving(state)
    buses = [fbus("A", 0.0, at="983", since=NOW - 30), fbus("B", 60.0), fbus("C", 400.0), fbus("L", 900.0)]
    assert pr.gap_holds(buses, NOW, GAP, state=state)["A"][1] == NOW + 170.0


def test_no_hold_until_a_departure_has_been_seen():
    assert pr.gap_holds([fbus("A", 0.0, at="983"), fbus("B", 60.0)], NOW, GAP, state={}) == {}


def test_departure_across_a_long_gap_between_calls_is_not_recorded():
    state = {}
    pr.gap_holds([fbus("L", 0.0, at="983", since=NOW - 900)], NOW - 600, GAP, state=state)
    pr.gap_holds([fbus("L", 900.0)], NOW - 300, GAP, state=state)
    assert state["Fontaine"]["left"] is None


def test_gap_rule_only_in_its_hours_and_on_weekdays():
    state = {}
    seen_leaving(state)
    buses = [fbus("A", 0.0, at="983", since=NOW - 30), fbus("L", 900.0)]
    assert pr.gap_holds(buses, NOW + 3600, GAP, state={}) == {}
    saturday = datetime(2026, 10, 17, 9, 15, tzinfo=NY).timestamp()
    assert pr.gap_holds(buses, saturday, GAP, state=state) == {}


def test_held_until_dwell_fn_covers_the_next_visit_only():
    dwell = bus_eta.held_until_dwell_fn(lambda r, s, w: 90.0, "983", NOW + 400.0, NOW)
    assert dwell("74", "983", NOW + 120) == 280.0   # on its way: arrives +120, leaves +400
    assert dwell("74", "983", NOW + 600) == bus_eta.TYPICAL_DWELL_S  # arrives after the leave time
    assert dwell("74", "983", NOW + 1800) == 90.0   # a lap later
    assert dwell("74", "988", NOW + 120) == 90.0


def test_shipped_gap_rules_load():
    rules = pr.load_gap_rules()
    assert rules and all(r.stage and r.gap_s > 0 and r.capacity >= 2 for r in rules)
