import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _merge_uts_wait_lookups


def test_bus_eta_value_wins_outright_over_a_smaller_transloc_value():
    # Regression for a real bug: TransLoc's own predictor has no concept of a
    # scheduled timestop hold, so during an active one its number for a stop
    # downstream of the hold can be smaller (wrong) than bus_eta's correctly
    # held-adjusted one. The old union-then-pick-smallest merge would surface
    # TransLoc's wrong number and silently defeat hold detection entirely.
    transloc = {("67", "874"): [300.0]}  # TransLoc: unaware of the hold, too optimistic
    bus_eta = {("67", "874"): [780.0]}  # bus_eta: correctly reflects the hold
    merged = _merge_uts_wait_lookups(transloc, bus_eta)
    assert merged[("67", "874")] == [780.0]
    assert 300.0 not in merged[("67", "874")]


def test_falls_back_to_transloc_when_bus_eta_has_nothing_for_that_stop():
    # bus_eta can come up empty for a stop (e.g. missing shape data) -- TransLoc's
    # raw value must still be used rather than silently dropping the key.
    transloc = {("67", "999"): [120.0]}
    bus_eta: dict = {}
    merged = _merge_uts_wait_lookups(transloc, bus_eta)
    assert merged[("67", "999")] == [120.0]


def test_keys_only_in_one_source_are_preserved():
    transloc = {("67", "A"): [60.0]}
    bus_eta = {("67", "B"): [90.0]}
    merged = _merge_uts_wait_lookups(transloc, bus_eta)
    assert merged[("67", "A")] == [60.0]
    assert merged[("67", "B")] == [90.0]


def test_multiple_bus_eta_vehicles_for_the_same_stop_are_kept_and_sorted():
    transloc = {("67", "874"): [45.0]}
    bus_eta = {("67", "874"): [900.0, 200.0]}
    merged = _merge_uts_wait_lookups(transloc, bus_eta)
    # Still entirely bus_eta's own values (both vehicles), sorted -- TransLoc's
    # single value is dropped for this key since bus_eta has SOME coverage here.
    assert merged[("67", "874")] == [200.0, 900.0]


def test_empty_inputs_produce_empty_output():
    assert _merge_uts_wait_lookups({}, {}) == {}
