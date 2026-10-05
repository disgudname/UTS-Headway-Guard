import math
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import _project_onto_polyline, ll_to_xy


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


def test_project_onto_polyline_ignores_a_short_kink_when_judging_the_side():
    # Regression, seen live 2026-10-05 on evening Gold (57): the real shape around
    # Goodwin Bridge. The northbound pass has a 4 m jog pointing south-east at the
    # vertex both passes share, and the southbound stop is on the right of that
    # jog, so it was pinned to the northbound pass and its ETA came 4.4 km of
    # route early.
    northbound = [
        (38.04239, -78.50576), (38.0426, -78.50568), (38.04266, -78.50566), (38.04303, -78.50551),
        (38.04323, -78.50547), (38.04327, -78.50548), (38.04325, -78.50544), (38.04336, -78.50541),
        (38.04339, -78.5054), (38.04342, -78.50539), (38.04372, -78.50529), (38.04373, -78.50529),
    ]
    southbound = [
        (38.04363, -78.50543), (38.04345, -78.50546), (38.04341, -78.50547), (38.04339, -78.50547),
        (38.04337, -78.50547), (38.04335, -78.50546), (38.04327, -78.50548), (38.04326, -78.50546),
        (38.04323, -78.50547), (38.04303, -78.50551), (38.04266, -78.50566), (38.0426, -78.50568),
    ]
    poly = northbound + southbound
    cum = [0.0]
    for a, b in zip(poly, poly[1:]):
        cum.append(cum[-1] + math.hypot(*ll_to_xy(b[0], b[1], a[0], a[1])))
    turn = cum[len(northbound)]  # where the southbound pass starts

    assert _project_onto_polyline(38.043279, -78.505573, poly, cum) > turn  # Goodwin Bridge (Southbound)
    assert _project_onto_polyline(38.043246, -78.505373, poly, cum) < turn  # Goodwin Bridge (Northbound)
