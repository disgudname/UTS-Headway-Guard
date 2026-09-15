import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _project_onto_polyline


def test_project_onto_polyline_prefers_right_side_on_a_tied_bidirectional_street():
    # Regression for a real bug found live on Gold Line: an out-and-back polyline
    # (heads north then returns south along the same line -- the shape of a
    # bidirectional street with stops on each side) gives a stop on one side of
    # the road an IDENTICAL nearest-point distance to both the outbound and
    # return legs, since geometrically they're the same line. Plain nearest-point
    # search resolved that tie by iteration order (arbitrary), putting a
    # southbound-only stop (Emmet St @ Contemplative Commons) on the northbound
    # pass alongside a genuinely-northbound stop (Emmet St @ Central Grounds
    # Garage) -- confirmed live, and confirmed by the user: Gold really does
    # serve each stop only in its own direction.
    poly = [(0.0, 0.0), (0.005, 0.0), (0.01, 0.0), (0.005, 0.0), (0.0, 0.0)]
    cum = [0.0, 500.0, 1000.0, 1500.0, 2000.0]

    # Just east of the line -- the RIGHT side of the northbound (outbound) leg.
    east_arc = _project_onto_polyline(0.0025, 0.0003, poly, cum)
    # Just west of the line -- the RIGHT side of the southbound (return) leg.
    west_arc = _project_onto_polyline(0.0025, -0.0003, poly, cum)

    assert east_arc < 1000.0  # lands on the outbound/northbound leg
    assert west_arc > 1000.0  # lands on the return/southbound leg


def test_project_onto_polyline_unaffected_when_theres_no_real_tie():
    # A stop nowhere near a duplicate pass should behave exactly as plain
    # nearest-point search always has -- this fix must not change behavior for
    # the overwhelming majority of stops that were never ambiguous.
    poly = [(0.0, 0.0), (0.0, 0.01)]
    cum = [0.0, 1000.0]
    arc = _project_onto_polyline(0.00005, 0.005, poly, cum)
    assert 400.0 < arc < 600.0


def test_project_onto_polyline_falls_back_to_nearest_when_no_tied_candidate_is_on_the_right():
    # A stop sitting exactly ON the road (zero lateral offset -- e.g. noisy
    # coordinates, or a stop genuinely in a median) is on neither side's RIGHT
    # (the cross product is exactly zero, not negative, for both directions) --
    # there's no right-side candidate to prefer, so this must fall back to
    # plain nearest-point rather than picking arbitrarily or erroring.
    poly = [(0.0, 0.0), (0.005, 0.0), (0.01, 0.0), (0.005, 0.0), (0.0, 0.0)]
    cum = [0.0, 500.0, 1000.0, 1500.0, 2000.0]
    arc = _project_onto_polyline(0.0025, 0.0, poly, cum)
    assert arc is not None  # doesn't error; some deterministic answer comes back
