import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

pytest.importorskip("openpyxl")  # build-time tool only; not in requirements.txt

import build_uts_blocks as b  # noqa: E402

# Text boxes copied from the Fall 2026 block package workbooks (Gold weekday, Green weekday,
# Orange weekend, Night Pilot). The "EVENING ROUTE CHANGE" box has to be ignored.
GOLD = [
    "EVENING ROUTE CHANGE BLK 09: AFTER LEAVING HER AT 1750, FOLLOW POST-1800 ROUTE TO CHP.",
    "HOW TO GO OUT-OF-SERVICE BLK 09: LEAVE BAR AT 2200 AND STAY IN-SERVICE UNTIL LIB. TAKE PASSENGERS AS FAR AS "
    "McCORMICK RD DORMS AND RETURN TO LOT. BLK 10: LEAVE BAR AT 1955 AND STAY IN-SERVICE UNTIL LIB. TAKE PASSENGERS "
    "AS FAR AS McCORMICK RD DORMS AND RETURN TO LOT. BLK 11: LEAVE CHP AT 2200, STAY IN-SERVICE UNTIL BAR, AND "
    "RETURN TO LOT.",
]


def test_gold_notes_give_leave_until_and_the_dorms_cutoff():
    notes = b.parse_out_of_service_notes(GOLD)
    assert set(notes) == {"[09]", "[10]", "[11]"}
    assert notes["[10]"] == {
        "leave_code": "BAR", "leave_s": 19 * 3600 + 55 * 60, "until_code": "LIB", "last_code": "DORMS", "then": "lot",
    }
    assert notes["[11]"]["last_code"] is None and notes["[11]"]["until_code"] == "BAR"


def test_weekday_ranges_in_a_note_are_not_mistaken_for_a_last_stop():
    text = [
        "HOW TO GO OUT-OF-SERVICE BLK 01: LEAVE CHP AT 2200 AND STAY IN-SERVICE UNTIL MP. MONDAY THRU WEDNESDAY "
        "RETURN TO LOT. THURSDAY THRU FRIDAY GO OUT OF SERVICE AND TAKE UNIT TO LIB. PROCEED TO BLK 04 NIGHT PILOT "
        "ROUTE. INFORM PASSENGERS OF ROUTE CHANGE. BLK 02: LEAVE JPA AT 2200, STAY IN-SERVICE UNTIL CHAPEL, AND "
        "RETURN TO LOT."
    ]
    notes = b.parse_out_of_service_notes(text)
    assert notes["[01]"]["last_code"] is None
    assert notes["[01]"]["until_code"] == "MP" and notes["[01]"]["then"] == "night_pilot"
    assert notes["[02]"]["until_code"] == "CHP"  # "CHAPEL" spelled out


def test_final_loop_and_split_heading_and_as_far_as_the_leave_stop():
    text = [
        "HOW TO GO OUT-OF-SERVICE BLK 05: LEAVE LIB AT 2130, STAY IN-SERVICE, AND MAKE FINAL LOOP. PROCEED TO [03] "
        "NIGHT PILOT WITH 2200 LIB DEPARTURE.",
        "HOW TO GO OUT-OF-SERVICE BLK 14: LEAVE MCQ AT 2000, STAY IN-SERVICE UNTIL PIN, TAKE PASSENGERS AS FAR AS MCQ, "
        "AND RETURN TO LOT.",
    ]
    notes = b.parse_out_of_service_notes(text)
    # "final loop": the cut-off is the next time it is back at the stop it left (LIB), then it becomes Night Pilot
    assert notes["[05]"]["until_code"] == "LIB" and notes["[05]"]["then"] == "night_pilot"
    # "as far as" the stop it leaves from (MCQ) is a full lap too; recorded as-is, bus_eta reads cut-off == leave stop
    assert notes["[14]"]["last_code"] == "MCQ" and notes["[14]"]["until_code"] == "PIN"


def test_passengers_thru_names_the_last_stop():
    text = [
        "HOW TO GO OUT-OF-SERVICE BLK 11: LEAVE BAR AT 2200, STAY IN-SERVICE UNTIL LIB, DROP OFF ANY REMAINING "
        "PASSENGERS THRU MCCORMICK RD, AND RETURN TO LOT."
    ]
    assert b.parse_out_of_service_notes(text)["[11]"]["last_code"] == "DORMS"


def test_evening_route_change_box_alone_yields_nothing():
    assert b.parse_out_of_service_notes(GOLD[:1]) == {}


def test_evening_route_change_notes_give_the_leave_stop_and_time():
    text = [
        "EVENING ROUTE CHANGE BLK 09: AFTER LEAVING HER AT 1750, FOLLOW POST-1800 ROUTE TO CHP. BLK 12: AFTER LEAVING "
        "BAR AT 1800, FOLLOW POST-1800 ROUTE TO LIB.",
        "EVENING ROUTE CHANGE BLK 05: AFTER LEAVING MP AT 1750, FOLLOW POST-1800 ROUTE TO LIB. BLK 07: FOLLOW "
        "POST-1800 ROUTE STARTING WITH 1800 DEPARTURE FROM PIN.",
        "HOW TO GO OUT-OF-SERVICE BLK 06: LEAVE MP AT 1800, STAY IN-SERVICE UNTIL LIB, AND RETURN TO LOT.",
    ]
    notes = b.parse_route_change_notes(text)
    assert notes == {
        "[09]": {"leave_code": "HER", "leave_s": 17 * 3600 + 50 * 60},
        "[12]": {"leave_code": "BAR", "leave_s": 18 * 3600},
        "[05]": {"leave_code": "MP", "leave_s": 17 * 3600 + 50 * 60},
        "[07]": {"leave_code": "PIN", "leave_s": 18 * 3600},   # "starting with the 1800 departure from PIN"
    }
    # the out-of-service box is not an evening-change box, and vice versa
    assert b.parse_out_of_service_notes(text[:2]) == {}


def _sheet(title_cols, cells):
    """A minimal sheet: titles in row 0, weekday text in row 1, a header row, then time rows."""
    from datetime import time as dtime
    width = max(title_cols) + 3
    rows = [[None] * width for _ in range(3 + len(cells))]
    for col, text in title_cols.items():
        rows[0][col] = text
        rows[1][col] = "MONDAY through FRIDAY"
    for col in title_cols:
        rows[2][col], rows[2][col + 1], rows[2][col + 2] = "Time", "[05]", "Time"
    for i, row in enumerate(cells):
        for col, (hh, mm, code) in row:
            rows[3 + i][col] = dtime(hh, mm)
            rows[3 + i][col + 1] = code
            rows[3 + i][col + 2] = dtime(hh, mm)
    return [tuple(r) for r in rows]


def test_old_year_tables_on_the_same_sheet_are_ignored():
    # "OR Fall 2026 Wkday" carries two Fall 2023 tables beside the real Fall 2026 one
    rows = _sheet(
        {0: "*Full* Fall 2023 Service", 4: "*Full* Fall 2026 Service"},
        [[(0, (7, 30, "CSW")), (4, (7, 30, "MP"))], [(0, (7, 55, "MP")), (4, (7, 50, "CSW"))]],
    )
    weekdays, per = b.parse_sheet(rows)
    assert weekdays == [0, 1, 2, 3, 4]
    assert per["[05]"] == [(7 * 3600 + 30 * 60, "MP"), (7 * 3600 + 50 * 60, "CSW")]


def test_tables_of_the_same_year_are_all_kept():
    # Gold's "Part 1" / "Part 2" are both 2026
    rows = _sheet(
        {0: "*Full* Fall 2026 Service", 4: "*Full* Fall 2026 Service"},
        [[(0, (7, 30, "BAR")), (4, (18, 0, "CHP"))]],
    )
    _, per = b.parse_sheet(rows)
    assert [code for _, code in per["[05]"]] == ["BAR", "CHP"]
