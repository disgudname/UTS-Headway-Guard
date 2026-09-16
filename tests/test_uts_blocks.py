import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import uts_blocks

NY_TZ = ZoneInfo("America/New_York")


def _epoch(y, mo, d, h, mi, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=NY_TZ).timestamp()


def _patch_data(monkeypatch, blocks, timestops):
    monkeypatch.setattr(uts_blocks, "_blocks", blocks)
    monkeypatch.setattr(uts_blocks, "_timestops", timestops)
    stop_id_to_code = {
        (str(route_id), str(stop_id)): code
        for code, by_route in timestops.items()
        for route_id, stop_id in by_route.items()
    }
    monkeypatch.setattr(uts_blocks, "_stop_id_to_code", stop_id_to_code)


# A Monday-Friday block that visits code "AAA" every 20 minutes starting 8:00am,
# and a Saturday-Sunday block reusing the same block id at different times --
# same pattern real Block Packages use (a block number is a reusable duty slot,
# not a unique-forever id -- see build_uts_blocks.py).
WEEKDAY_STOPS = [[28800, "AAA"], [30000, "AAA"], [31200, "AAA"]]  # 8:00, 8:20, 8:40
WEEKEND_STOPS = [[36000, "AAA"]]  # 10:00

SAMPLE_BLOCKS = {
    "[01]": {
        "weekday_groups": [
            {"weekdays": [0, 1, 2, 3, 4], "stops": WEEKDAY_STOPS},
            {"weekdays": [5, 6], "stops": WEEKEND_STOPS},
        ]
    }
}
SAMPLE_TIMESTOPS = {"AAA": {"99": "stop-1"}}


def test_timestop_code_for_stop_resolves_mapped_pair(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    assert uts_blocks.timestop_code_for_stop("99", "stop-1") == "AAA"
    assert uts_blocks.timestop_code_for_stop("99", "stop-2") is None
    assert uts_blocks.timestop_code_for_stop("other-route", "stop-1") is None


def test_scheduled_hold_epoch_matches_nearest_lap(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    # A Monday (2026-09-14 is a Monday) live estimate landing near the 8:20 lap.
    ref = _epoch(2026, 9, 14, 8, 19)
    result = uts_blocks.scheduled_hold_epoch("99", "stop-1", "[01]", ref)
    assert result == _epoch(2026, 9, 14, 8, 20)


def test_scheduled_hold_epoch_none_when_stop_not_mapped(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    ref = _epoch(2026, 9, 14, 8, 19)
    assert uts_blocks.scheduled_hold_epoch("99", "unmapped-stop", "[01]", ref) is None


def test_scheduled_hold_epoch_none_without_block_id(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    ref = _epoch(2026, 9, 14, 8, 19)
    assert uts_blocks.scheduled_hold_epoch("99", "stop-1", None, ref) is None


def test_scheduled_hold_epoch_none_when_reference_too_far_from_any_lap(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    # Nowhere near 8:00/8:20/8:40 -- must not match the wrong lap just because
    # it's the nearest one numerically.
    ref = _epoch(2026, 9, 14, 14, 0)
    assert uts_blocks.scheduled_hold_epoch("99", "stop-1", "[01]", ref) is None


def test_scheduled_hold_epoch_respects_weekday_group(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    # Same block, same stop, but a WEEKDAY reference time on a SATURDAY
    # (2026-09-19) -- the weekday group's 8:00-8:40 laps don't apply that day,
    # only the weekend group's 10:00 lap does.
    ref = _epoch(2026, 9, 19, 8, 19)
    assert uts_blocks.scheduled_hold_epoch("99", "stop-1", "[01]", ref) is None
    ref_weekend_lap = _epoch(2026, 9, 19, 9, 59)
    assert uts_blocks.scheduled_hold_epoch("99", "stop-1", "[01]", ref_weekend_lap) == _epoch(2026, 9, 19, 10, 0)


def test_next_scheduled_arrival_epoch_returns_earliest_at_or_after(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    after = _epoch(2026, 9, 14, 8, 5)
    result = uts_blocks.next_scheduled_arrival_epoch("99", "stop-1", after)
    assert result == _epoch(2026, 9, 14, 8, 20)


def test_next_scheduled_arrival_epoch_rolls_into_the_next_day(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    # Monday 2026-09-14, after all three weekday laps -- nothing left running
    # that day, next real service is Tuesday 8:00 (still weekday group).
    after = _epoch(2026, 9, 14, 20, 0)
    result = uts_blocks.next_scheduled_arrival_epoch("99", "stop-1", after)
    assert result == _epoch(2026, 9, 15, 8, 0)


def test_next_scheduled_arrival_epoch_none_when_stop_unmapped(monkeypatch):
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    after = _epoch(2026, 9, 14, 8, 5)
    assert uts_blocks.next_scheduled_arrival_epoch("99", "nope", after) is None


def test_midnight_rollover_entry_resolves_against_previous_day(monkeypatch):
    # A Night-Pilot-shaped block: Sunday-Wednesday group running from 22:00
    # through 25:20 (1:20am the next day, encoded by build_uts_blocks.py as
    # 91200s -- see that script's rollover-handling docstring).
    blocks = {
        "[03]": {
            "weekday_groups": [
                {"weekdays": [6, 0, 1, 2], "stops": [[79200, "LIB"], [91200, "LIB"]]},
            ]
        }
    }
    timestops = {"LIB": {"59": "stop-lib"}}
    _patch_data(monkeypatch, blocks, timestops)
    # 2026-09-14 is a Monday; 1:20am Tuesday 2026-09-15 is the rolled-over lap
    # whose service day is actually Monday's.
    ref = _epoch(2026, 9, 15, 1, 20)
    result = uts_blocks.scheduled_hold_epoch("59", "stop-lib", "[03]", ref)
    assert result == ref


# Two blocks that both visit "BBB" (mapped to stop-2), at different times --
# used to verify best_matching_block/hold_for_ride pin ONE plausible block from
# the nearest match and then keep following that same block, rather than
# drifting to whichever block happens to be nearest at each individual stop.
TWO_BLOCK_TIMESTOPS = {"AAA": {"99": "stop-1"}, "BBB": {"99": "stop-2"}}
TWO_BLOCKS = {
    "[01]": {
        "weekday_groups": [
            {"weekdays": [0, 1, 2, 3, 4], "stops": [[28800, "AAA"], [29400, "BBB"], [30000, "AAA"]]},
        ]
    },
    "[02]": {
        "weekday_groups": [
            {"weekdays": [0, 1, 2, 3, 4], "stops": [[29200, "BBB"], [30800, "AAA"]]},
        ]
    },
}


def test_best_matching_block_picks_nearest_candidate(monkeypatch):
    _patch_data(monkeypatch, TWO_BLOCKS, TWO_BLOCK_TIMESTOPS)
    # Monday 8:09am -- 60s from block [01]'s 8:10 BBB, 140s from block [02]'s
    # 8:06:40 BBB, so [01] should win.
    ref = _epoch(2026, 9, 14, 8, 9)
    assert uts_blocks.best_matching_block("99", "stop-2", ref) == "[01]"


def test_best_matching_block_none_when_nothing_within_tolerance(monkeypatch):
    _patch_data(monkeypatch, TWO_BLOCKS, TWO_BLOCK_TIMESTOPS)
    ref = _epoch(2026, 9, 14, 14, 0)
    assert uts_blocks.best_matching_block("99", "stop-2", ref) is None


def test_best_matching_block_none_when_stop_unmapped(monkeypatch):
    _patch_data(monkeypatch, TWO_BLOCKS, TWO_BLOCK_TIMESTOPS)
    assert uts_blocks.best_matching_block("99", "nope", _epoch(2026, 9, 14, 8, 9)) is None


def test_hold_for_ride_pins_a_block_then_keeps_following_it(monkeypatch):
    _patch_data(monkeypatch, TWO_BLOCKS, TWO_BLOCK_TIMESTOPS)
    ref1 = _epoch(2026, 9, 14, 8, 9)
    hold1, block_id = uts_blocks.hold_for_ride("99", "stop-2", None, ref1)
    assert block_id == "[01]"
    assert hold1 == _epoch(2026, 9, 14, 8, 10)  # BBB at 8:10 per block [01], not [02]'s 8:06:40

    # Same ride, next stop -- passing the pinned block_id must follow block
    # [01]'s OWN next entry (AAA at 8:20), never block [02]'s.
    ref2 = _epoch(2026, 9, 14, 8, 19)
    hold2, block_id2 = uts_blocks.hold_for_ride("99", "stop-1", block_id, ref2)
    assert block_id2 == "[01]"
    assert hold2 == _epoch(2026, 9, 14, 8, 20)


def test_hold_for_ride_none_when_stop_unmapped(monkeypatch):
    _patch_data(monkeypatch, TWO_BLOCKS, TWO_BLOCK_TIMESTOPS)
    hold, block_id = uts_blocks.hold_for_ride("99", "nope", None, _epoch(2026, 9, 14, 8, 9))
    assert hold is None and block_id is None


def test_is_loaded_reflects_whether_block_data_is_present(monkeypatch):
    _patch_data(monkeypatch, {}, {})
    assert uts_blocks.is_loaded() is False
    _patch_data(monkeypatch, SAMPLE_BLOCKS, SAMPLE_TIMESTOPS)
    assert uts_blocks.is_loaded() is True
