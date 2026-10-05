import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _backward_polls, _resolve_dir_sign


def test_stationary_transloc_speed_ignores_noisy_negative_along_mps():
    # Regression for a real bug: TransLoc reports the vehicle as stopped
    # (holding at a scheduled timestop -- now a minutes-long state thanks to
    # the schedule-hold feature), but our own arc-length projection briefly
    # jumped backward (a few metres of GPS jitter snapping the nearest-point
    # match to a different pass of a self-near road) producing a large
    # negative along_mps. Without the mps guard this incorrectly set
    # dir_sign=-1, which made bus_eta.estimate_stop_eta_s silently drop every
    # downstream ETA for the vehicle except the one within the arriving-now
    # radius -- confirmed live on Silver and Green Line vehicles sitting
    # still at a timestop.
    dir_sign, tiebreak = _resolve_dir_sign(mps=0.0, along_mps=-5.0, prev_sign=1)
    assert dir_sign == 1  # keeps the last known-good direction, not -1
    assert tiebreak is False


def test_stationary_transloc_speed_ignores_noisy_positive_along_mps_too():
    dir_sign, tiebreak = _resolve_dir_sign(mps=0.2, along_mps=8.0, prev_sign=-1)
    assert dir_sign == -1
    assert tiebreak is False


def test_genuinely_moving_forward_sets_positive_dir_sign():
    dir_sign, tiebreak = _resolve_dir_sign(mps=5.0, along_mps=4.0, prev_sign=-1)
    assert dir_sign == 1
    assert tiebreak is False


def test_genuinely_moving_backward_sets_negative_dir_sign():
    dir_sign, tiebreak = _resolve_dir_sign(mps=5.0, along_mps=-4.0, prev_sign=1)
    assert dir_sign == -1
    assert tiebreak is False


def test_moving_but_ambiguous_along_mps_keeps_previous_sign():
    # TransLoc says it's moving, but our own along_mps is within the noise
    # band (|along_mps| <= dir_eps) -- keep trusting the last known direction.
    dir_sign, tiebreak = _resolve_dir_sign(mps=3.0, along_mps=0.1, prev_sign=1)
    assert dir_sign == 1
    assert tiebreak is False


def test_ambiguous_along_mps_with_no_prior_direction_requests_heading_tiebreak():
    dir_sign, tiebreak = _resolve_dir_sign(mps=3.0, along_mps=0.1, prev_sign=0)
    assert dir_sign == 0
    assert tiebreak is True


def test_stationary_with_no_prior_direction_does_not_request_tiebreak():
    # A brand-new, already-stationary vehicle has no reliable heading signal
    # either -- stay at "unknown" (0) rather than forcing a heading guess.
    dir_sign, tiebreak = _resolve_dir_sign(mps=0.0, along_mps=-5.0, prev_sign=0)
    assert dir_sign == 0
    assert tiebreak is False


def test_far_off_route_keeps_previous_direction():
    # Bus pulling out of a layover 200 m off its route's shape (Orange [07] at Scott Stadium, 2026-09-25): the
    # projection hops between passes of the loop and reads as backward; the last direction must stand.
    dir_sign, tiebreak = _resolve_dir_sign(mps=5.0, along_mps=-78.0, prev_sign=1, off_route_m=250.0)
    assert dir_sign == 1
    assert tiebreak is False


def test_far_off_route_does_not_hold_a_backward_reading():
    # Green [01] (bus 15) turning off the Green Loop line toward Hereford @ Runk, 2026-09-30 17:47: one backward
    # reading at the 100 m edge was held for the 9 min it was off route, and a backward bus gets no ETAs.
    dir_sign, tiebreak = _resolve_dir_sign(mps=5.0, along_mps=-2.7, prev_sign=-1, off_route_m=250.0)
    assert dir_sign == 0
    assert tiebreak is False
    # ...including while it sits at the off-route stop.
    dir_sign, _ = _resolve_dir_sign(mps=0.0, along_mps=0.0, prev_sign=-1, off_route_m=271.0)
    assert dir_sign == 0


def test_near_route_still_detects_reversal():
    dir_sign, _ = _resolve_dir_sign(mps=5.0, along_mps=-4.0, prev_sign=1, off_route_m=30.0)
    assert dir_sign == -1


def test_single_backward_reading_does_not_survive_standing_still():
    # Silver [14] (bus 32) pulling into Pinn Hall, 2026-10-05 08:37: one fix snapped back 12 m at 6 mph, the next was
    # 0 mph, and the -1 was held for the 8 min hold, so the bus had no ETAs at its next 9 stops.
    sign, _ = _resolve_dir_sign(mps=2.7, along_mps=-2.4, prev_sign=1, off_route_m=11.0, backward_polls=0)
    assert sign == -1
    polls = _backward_polls(0, sign, 2.7, -2.4)
    assert polls == 1
    sign, tiebreak = _resolve_dir_sign(mps=0.0, along_mps=-0.7, prev_sign=sign, off_route_m=11.0, backward_polls=polls)
    assert sign == 0
    assert tiebreak is False
    assert _backward_polls(polls, sign, 0.0, -0.7) == 0


def test_confirmed_backward_reading_is_kept_while_stopped():
    # A bus really running against the shape (two moving polls in a row) that stops at a light stays backward.
    polls = _backward_polls(_backward_polls(0, -1, 5.0, -4.0), -1, 5.0, -4.0)
    assert polls == 2
    sign, _ = _resolve_dir_sign(mps=0.0, along_mps=0.0, prev_sign=-1, backward_polls=polls)
    assert sign == -1
    assert _backward_polls(polls, sign, 0.0, 0.0) == 2


def test_repeated_fix_does_not_confirm_a_backward_reading():
    # Moving, but the same GPS fix as last poll (along_mps 0): the sign carries over without counting.
    assert _backward_polls(1, -1, 5.0, 0.0) == 1
