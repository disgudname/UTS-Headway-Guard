import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _onboard_apply_announce_rules, _onboard_rule_mode  # noqa: E402

ORANGE = {"name": "Orange Line", "color": "#f80"}
SILVER = {"name": "Silver Line", "color": "#ccc"}
CAT = {"key": "cat", "name": "CAT", "color": "#1f5aa6"}


def _row(name, transfers=(), connections=()):
    return {"id": "1", "name": name, "transfers": [dict(t) for t in transfers],
            "connections": [dict(c) for c in connections]}


def _apply(rows, rules, own="Gold Line"):
    _onboard_apply_announce_rules(rows, own, {"Orange Line": "#f80", "Silver Line": "#ccc"}, {"cat"}, rules)
    return rows


def test_no_rules_leaves_rows_alone():
    rows = _apply([_row("A", [ORANGE], [CAT])], [])
    assert rows[0]["transfers"] == [ORANGE] and rows[0]["connections"] == [CAT]


def test_off_removes_and_quiet_marks_silent():
    rules = [{"stop": "A", "route": "*", "target": "Orange Line", "mode": "off"},
             {"stop": "A", "route": "*", "target": "CAT", "mode": "quiet"}]
    row = _apply([_row("A", [ORANGE, SILVER], [CAT])], rules)[0]
    assert [t["name"] for t in row["transfers"]] == ["Silver Line"]
    assert row["connections"] == [{**CAT, "silent": True}]


def test_rules_only_hit_their_stop():
    rules = [{"stop": "A", "route": "*", "target": "*", "mode": "off"}]
    rows = _apply([_row("A", [ORANGE], [CAT]), _row("B", [ORANGE], [CAT])], rules)
    assert rows[0]["transfers"] == [] and rows[0]["connections"] == []
    assert rows[1]["transfers"] == [ORANGE] and rows[1]["connections"] == [CAT]


def test_always_adds_a_running_service_only():
    rules = [{"stop": "A", "route": "*", "target": "Silver Line", "mode": "always"},
             {"stop": "A", "route": "*", "target": "Blue Line", "mode": "always"},  # not running: nothing to add
             {"stop": "A", "route": "*", "target": "CAT", "mode": "always"}]
    row = _apply([_row("A", [ORANGE])], rules)[0]
    assert [t["name"] for t in row["transfers"]] == ["Orange Line", "Silver Line"]
    assert row["connections"] == [CAT]


def test_most_specific_rule_wins():
    rules = [{"stop": "*", "route": "*", "target": "CAT", "mode": "off"},
             {"stop": "A", "route": "*", "target": "CAT", "mode": "quiet"},
             {"stop": "A", "route": "Gold Line", "target": "CAT", "mode": "always"}]
    assert _onboard_rule_mode(rules, "A", "Gold Line", "CAT") == "always"
    assert _onboard_rule_mode(rules, "A", "Orange Line", "CAT") == "quiet"
    assert _onboard_rule_mode(rules, "B", "Gold Line", "CAT") == "off"
    assert _onboard_rule_mode(rules, "B", "Gold Line", "Orange Line") is None


def test_route_scoped_rule_only_on_that_route():
    rules = [{"stop": "A", "route": "Gold Line", "target": "Orange Line", "mode": "off"}]
    assert _apply([_row("A", [ORANGE])], rules, own="Gold Line")[0]["transfers"] == []
    assert _apply([_row("A", [ORANGE])], rules, own="Green Line")[0]["transfers"] == [ORANGE]
