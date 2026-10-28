import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import eta_health_notify as n  # noqa: E402


def _result(**kw):
    d = {"on_target_pct": 88.0, "tl_on_target_pct": 50.0, "late_over_2min_pct_excl_near_term_only": 5.0,
         "purple_in_service": True, "historical_pct": 90.0, "breaches": ["x"], "causes": [], "suspects": []}
    d.update(kw)
    return json.dumps(d)


def test_one_bus_is_all_good_not_problem():
    msg = n.fallback_summary(_result(
        causes=["ONE BUS (not the engine; see suspects) -- 5.6% of predictions >2 min late"],
        suspects=["ONE BUS: 17132 [08] on Orange Line — Pre-6PM, 17:30-17:57, left 170 s BEFORE its sheet time"]))
    assert msg.startswith("ALL GOOD")
    assert "17132 [08] Orange Line, left a timestop early" in msg


def test_purple_and_slow_buses_never_problem():
    msg = n.fallback_summary(_result(causes=[
        "Purple staging (buses leaving a hold early), known -- Purple Line: 15% >2 min late",
        "Gold Line — Detour Service - Pre-6PM: BUSES SLOW today -- arrived 94 s after our ETA"]))
    assert msg.startswith("HEADS UP")
    assert "Gold Line: buses ran slower" in msg


def test_engine_cause_is_problem():
    msg = n.fallback_summary(_result(causes=["ENGINE -- 4 full-lap flip(s)"]))
    assert msg.startswith("PROBLEM")


def test_inconclusive_and_error_are_heads_up():
    assert n.fallback_summary(json.dumps({"inconclusive": "no buses"})).startswith("HEADS UP")
    assert n.fallback_summary(json.dumps({"error": "timeout"})).startswith("HEADS UP")
