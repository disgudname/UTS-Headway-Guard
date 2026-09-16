import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _resolve_dir_sign


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
