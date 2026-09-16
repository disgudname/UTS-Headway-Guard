"""Local, occasional batch job: converts UTS's hand-maintained "Block Package"
Excel workbooks (one per route, e.g. "Gold Line 08.11.2026.xlsx") into
config/uts_blocks.json, the compact static schedule uts_blocks.py loads at
runtime to let bus_eta.py's live ETAs account for a bus intentionally holding
at a scheduled "timestop" (see uts_blocks.py's module docstring for how that's
used).

These workbooks are NOT checked into this repo -- they live wherever UTS ops
hands them to you locally. Re-run this script whenever a new Block Package
ships (a few times a year): update config/uts_active_sheets.json to point at
the new current sheet name(s) first, then run:

    python build_uts_blocks.py --blocks-dir "C:\\path\\to\\Block Packages"

Requires openpyxl (`pip install openpyxl`) -- a local build-time tool only,
deliberately not a runtime dependency of the deployed app (see requirements.txt).

Workbook layout (confirmed by hand against the real files, 2026-09-16): each
active sheet lays a whole day out as several side-by-side "clusters" purely for
print-page-width reasons -- a `Time` column, one or more `[NN]`-bracketed block-
ID columns, then a closing `Time` column (repeated with the same value, a built-
in sanity check), then usually a blank spacer column before the next cluster.
The SAME block ID can (and does) reappear in multiple clusters across the sheet
-- e.g. Gold Line's "[09]" shows up in both an AM cluster and a PM cluster --
so a block's full-day sequence has to be gathered from every cluster it
appears in, not just one. Block ID numbers are global across the whole UTS
system, not per-route (confirmed: Gold 09-12, Green 01-02, Night Pilot 03-04,
Orange 05-08, Silver 13-14 in the currently active sheets, no overlaps) --
matches how app.py's own vehicle_block_lookup() already treats them, with no
route qualifier. The same block ID DOES legitimately reappear across a single
route's own Weekday vs. Weekend sheets (a block number is a reusable duty slot,
not a unique-per-day-forever id) -- that's expected and handled by keeping a
separate weekday-group entry per sheet, not a collision.

Each sheet's own header text (row 2, e.g. "MONDAY through FRIDAY", "SATURDAY
and SUNDAY", "SUNDAY THRU WEDNESDAY") declares which weekdays it applies to --
parsed here rather than hand-curated, since Block Packages carry no GTFS-style
calendar.txt with real date ranges. There is no service-exception handling
(exam periods, breaks) -- config/uts_active_sheets.json's hand-picked "current
sheet" is the only calendar this system has; keep it pointed at the right
sheet for the time of year.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openpyxl

ROOT_DIR = Path(__file__).resolve().parent
ACTIVE_SHEETS_PATH = ROOT_DIR / "config" / "uts_active_sheets.json"
OUTPUT_PATH = ROOT_DIR / "config" / "uts_blocks.json"

# A real timestop code is 2-8 uppercase letters, no digits (digits are reserved
# for [NN] block labels) -- see the earlier hand-verification pass in this
# session's Block Package exploration. Words that happen to fit the same shape
# but aren't places get excluded explicitly.
CODE_RE = re.compile(r"^[A-Z]{2,8}$")
EXCLUDE_CODES = {
    "AM", "PM", "TIME", "END", "REV", "DATE",
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
    "WEEKDAY", "WEEKEND", "SPRING", "SUMMER", "FALL", "WINTER", "RECESS", "EXAM",
    "SERVICE", "SCHEDULE", "FULL", "REDUCED", "ACTIVE", "GOLD", "GREEN", "ORANGE",
    "SILVER", "LINE", "PILOT", "NIGHT", "PART", "THRU", "THROUGH", "AND", "DEST",
}

BLOCK_HEADER_RE = re.compile(r"^\[(\d{1,2})\]$")

_WEEKDAY_NAMES = {
    "MONDAY": 0, "TUESDAY": 1, "WEDNESDAY": 2, "THURSDAY": 3,
    "FRIDAY": 4, "SATURDAY": 5, "SUNDAY": 6,
}
_WEEKDAY_ORDER = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]


def _parse_weekday_range(text: str) -> Optional[List[int]]:
    """"MONDAY through FRIDAY" -> [0,1,2,3,4]; "SUNDAY THRU WEDNESDAY" -> [6,0,1,2]
    (wraps past Sunday); "SATURDAY and SUNDAY" -> [5,6]. None if the text doesn't
    look like a weekday-range declaration at all."""
    if not text:
        return None
    upper = text.upper()
    found = [(m.start(), _WEEKDAY_NAMES[name]) for name in _WEEKDAY_NAMES for m in re.finditer(name, upper)]
    if len(found) < 2:
        return None
    found.sort()
    start_day = found[0][1]
    end_day = found[-1][1]
    days = []
    d = start_day
    for _ in range(7):
        days.append(d)
        if d == end_day:
            break
        d = (d + 1) % 7
    return days


def _find_header_row(rows: List[Tuple[Any, ...]]) -> Optional[int]:
    """Row index (0-based) of the row that lays out `Time`/`[NN]` cluster headers."""
    for i, row in enumerate(rows):
        has_time = any(isinstance(c, str) and c.strip() == "Time" for c in row)
        has_block = any(isinstance(c, str) and BLOCK_HEADER_RE.match(c.strip()) for c in row)
        if has_time and has_block:
            return i
    return None


def _find_weekdays(rows: List[Tuple[Any, ...]], header_row: int) -> Optional[List[int]]:
    for row in rows[:header_row]:
        for cell in row:
            if isinstance(cell, str):
                days = _parse_weekday_range(cell)
                if days:
                    return days
    return None


def _clusters(header_row: Tuple[Any, ...]) -> List[Tuple[int, List[Tuple[int, str]]]]:
    """[(time_col, [(block_col, block_id), ...]), ...] -- one entry per Time..Time
    cluster found in the header row."""
    time_cols = [i for i, c in enumerate(header_row) if isinstance(c, str) and c.strip() == "Time"]
    clusters = []
    for start, end in zip(time_cols, time_cols[1:]):
        block_cols = []
        for i in range(start + 1, end):
            c = header_row[i]
            if isinstance(c, str):
                m = BLOCK_HEADER_RE.match(c.strip())
                if m:
                    block_cols.append((i, f"[{m.group(1)}]"))
        if block_cols:
            clusters.append((start, block_cols))
    return clusters


def _time_to_seconds(value: Any) -> Optional[int]:
    if isinstance(value, dtime):
        return value.hour * 3600 + value.minute * 60 + value.second
    if isinstance(value, datetime):
        return value.hour * 3600 + value.minute * 60 + value.second
    return None


def parse_sheet(rows: List[Tuple[Any, ...]]) -> Tuple[Optional[List[int]], Dict[str, List[Tuple[int, str]]]]:
    """Returns (weekdays, {block_id: [(time_s, code), ...]}) for one sheet.

    time_s is seconds since this weekday-group's own service-day midnight, and
    CAN exceed 86400 -- e.g. Night Pilot's blocks run from ~22:00 through past
    midnight, and Excel's time-of-day cells wrap back to 0:00 rather than
    continuing past 24:00:00 the way GTFS text does. Each cluster's rows are
    walked in their real top-to-bottom sheet order (never globally re-sorted
    until every cluster's own wrap is already resolved) specifically to catch
    that: a same-cluster decrease in time-of-day marks a midnight crossing, so
    every later entry in that cluster gets 86400 added. Clusters are handled
    independently (rollover state resets per cluster) since a single sheet row
    holds several unrelated clusters' values side by side purely for print
    layout (see module docstring) -- interleaving them before wrap-detection
    would see spurious "decreases" at every cluster boundary that have nothing
    to do with midnight."""
    header_row_idx = _find_header_row(rows)
    if header_row_idx is None:
        return None, {}
    weekdays = _find_weekdays(rows, header_row_idx)
    clusters = _clusters(rows[header_row_idx])
    data_rows = rows[header_row_idx + 1:]
    per_block: Dict[str, List[Tuple[int, str]]] = {}
    for time_col, block_cols in clusters:
        prev_time: Optional[int] = None
        rollover = 0
        for row in data_rows:
            if time_col >= len(row):
                continue
            time_s = _time_to_seconds(row[time_col])
            if time_s is None:
                continue
            if prev_time is not None and time_s < prev_time:
                rollover += 86400
            prev_time = time_s
            adj_time = time_s + rollover
            for block_col, block_id in block_cols:
                if block_col >= len(row):
                    continue
                cell = row[block_col]
                if not isinstance(cell, str):
                    continue
                code = cell.strip().upper()
                if not code or not CODE_RE.match(code) or code in EXCLUDE_CODES:
                    continue
                per_block.setdefault(block_id, []).append((adj_time, code))
    for seq in per_block.values():
        seq.sort()
    return weekdays, per_block


def build(blocks_dir: Path) -> Dict[str, Any]:
    active_sheets = json.loads(ACTIVE_SHEETS_PATH.read_text(encoding="utf-8"))
    blocks: Dict[str, Dict[str, Any]] = {}
    block_origin: Dict[str, str] = {}  # block_id -> which route file first defined it (collision check)
    source_files: Dict[str, str] = {}

    for route_name, cfg in active_sheets.items():
        fn = cfg["file"]
        path = blocks_dir / fn
        if not path.exists():
            print(f"[build_uts_blocks] WARNING: {path} not found, skipping {route_name}")
            continue
        source_files[route_name] = fn
        wb = openpyxl.load_workbook(path, data_only=True)
        for sheet_name in cfg["sheets"]:
            if sheet_name not in wb.sheetnames:
                print(f"[build_uts_blocks] WARNING: sheet {sheet_name!r} not found in {fn}, skipping")
                continue
            ws = wb[sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            weekdays, per_block = parse_sheet(rows)
            if weekdays is None:
                print(f"[build_uts_blocks] WARNING: couldn't find a header row in {fn}/{sheet_name}")
                continue
            for block_id, seq in per_block.items():
                if not seq:
                    continue
                if block_id in block_origin and block_origin[block_id] != route_name:
                    print(
                        f"[build_uts_blocks] WARNING: block {block_id} defined by both "
                        f"{block_origin[block_id]} and {route_name} -- block IDs are assumed "
                        f"globally unique across routes, this needs a human look"
                    )
                block_origin[block_id] = route_name
                entry = blocks.setdefault(block_id, {"weekday_groups": []})
                entry["weekday_groups"].append({"weekdays": weekdays, "stops": [list(t) for t in seq]})

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_files": source_files,
        "blocks": blocks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--blocks-dir",
        default=str(Path.home() / "Documents" / "Block Packages"),
        help="Directory containing the UTS Block Package .xlsx files",
    )
    args = parser.parse_args()
    blocks_dir = Path(args.blocks_dir)
    if not blocks_dir.exists():
        raise SystemExit(f"Block Packages directory not found: {blocks_dir}")

    result = build(blocks_dir)
    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    n_blocks = len(result["blocks"])
    n_stops = sum(len(g["stops"]) for b in result["blocks"].values() for g in b["weekday_groups"])
    print(f"[build_uts_blocks] wrote {OUTPUT_PATH} -- {n_blocks} blocks, {n_stops} scheduled timestop entries")


if __name__ == "__main__":
    main()
