"""Change log for the When To Work "Complete Schedule" iCal feed (the W2W_ICAL_URL secret).

W2W's read-only API (AssignedShiftList) never returns unassigned shifts, but the Google-Calendar copy of the full schedule
does: an unassigned shift is just a shift with no employee. The feed only shows the schedule as it is NOW, so this module
keeps the last snapshot and appends every difference (a shift added, removed, or changed hands/times/note) to a JSONL log,
which is the only way to know later who was on a shift, or that it was open, at a given time.

W2W does not push every unassigned shift to that calendar: the unassigned COPY it makes when a manager edits a shift
(a callout) only arrives once somebody saves the copy again. W2W's API does count unassigned shifts per position per day
(DailyPositionTotals), so `missing_open_shifts` compares that count with the feed and, where the feed is short, takes
the missing shift's times from the assigned shift it was copied from. See HANDOFF.md 2026-10-09.

Files (in the data directory): w2w_schedule_snapshot.json, w2w_schedule_changes.jsonl.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from itertools import combinations
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
SNAPSHOT_NAME = "w2w_schedule_snapshot.json"
LOG_NAME = "w2w_schedule_changes.jsonl"
# Only shifts starting within this many days back are tracked (the feed reaches a year back; old edits are just noise).
TRACK_DAYS_BACK = 60
# A fetch that suddenly has far fewer shifts than the last good one is treated as a bad response, not a mass deletion.
MIN_KEEP_FRACTION = 0.5
COMPARED_FIELDS = ("employee", "position", "start", "end", "note")
_BUS_BLOCK_RE = re.compile(r"\d{2}")
# /ob board: (side, service-day rollover, [(group, position matcher)]). Positions are W2W's exact names.
OB_SIDES = (
    ("bus", time(2, 30), [
        ("block", lambda p: bool(_BUS_BLOCK_RE.fullmatch(p))),
        ("charter", lambda p: p.startswith("Charter")),
        ("staff", lambda p: p in ("Sup", "FlexRide Dispatch")),
    ]),
    ("ondemand", time(5, 30), [
        ("driver", lambda p: p in ("OnDemand Driver", "OnDemand EB", "FlexRide Driver", "FlexRide EB")),
        ("staff", lambda p: p == "OnDemand Dispatch"),
    ]),
)
_BS_N = "\\" + "n"
_BS_COMMA = "\\" + ","


def _unfold(text: str) -> str:
    return re.sub(r"\r?\n[ \t]", "", text)


def _to_ny_iso(value: str) -> str:
    return (
        datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).astimezone(NY).isoformat(timespec="minutes")
    )


def parse_ics(text: str) -> Dict[str, Dict[str, Any]]:
    """{uid: {employee, position, start, end, note, modified}} for every shift in the feed. Times are New York local
    ISO strings. employee is "" for an unassigned shift. Events that don't parse are skipped."""
    shifts: Dict[str, Dict[str, Any]] = {}
    for block in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", _unfold(text), re.S):
        fields: Dict[str, str] = {}
        for line in block.strip().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.split(";")[0]] = value
        try:
            start, end = _to_ny_iso(fields["DTSTART"]), _to_ny_iso(fields["DTEND"])
        except (KeyError, ValueError):
            continue
        desc = re.sub(r"\(key:.*?\)", "", fields.get("DESCRIPTION", "").replace(_BS_N, "\n").replace(_BS_COMMA, ","))
        lines = desc.rstrip("\n ").split("\n")
        position_match = re.search(r"\[([^\]]+)\]", lines[1]) if len(lines) > 1 else None
        uid = fields.get("UID") or f"{start}|{fields.get('SUMMARY', '')}"
        shifts[uid] = {
            "employee": lines[0].strip() if lines else "",
            "position": position_match.group(1) if position_match else (lines[1].strip() if len(lines) > 1 else ""),
            "position_name": lines[1].strip() if len(lines) > 1 else "",
            "start": start,
            "end": end,
            "note": lines[4].strip() if len(lines) > 4 else "",
            "modified": fields.get("LAST-MODIFIED", ""),
        }
    return shifts


def _tracked(shift: Dict[str, Any], now: datetime) -> bool:
    return shift["start"][:10] >= (now.astimezone(NY).date() - timedelta(days=TRACK_DAYS_BACK)).isoformat()


def diff_shifts(old: Dict[str, Dict[str, Any]], new: Dict[str, Dict[str, Any]], now: datetime) -> List[Dict[str, Any]]:
    """Change events between two snapshots: added / removed / changed (which of COMPARED_FIELDS differ). The feed's own
    LAST-MODIFIED is ignored for the comparison (W2W re-stamps hundreds of shifts at once with no real change)."""
    ts = now.astimezone(timezone.utc).isoformat(timespec="seconds")
    events: List[Dict[str, Any]] = []

    def base(kind: str, uid: str, shift: Dict[str, Any]) -> Dict[str, Any]:
        return {"ts": ts, "kind": kind, "uid": uid, "date": shift["start"][:10], "position": shift["position"],
                "start": shift["start"], "end": shift["end"]}

    for uid, shift in new.items():
        if uid not in old:
            events.append({**base("added", uid, shift), "employee_after": shift["employee"], "note_after": shift["note"]})
            continue
        before = old[uid]
        changed = [f for f in COMPARED_FIELDS if before.get(f) != shift.get(f)]
        if changed:
            events.append({
                **base("changed", uid, shift), "fields": changed,
                "employee_before": before.get("employee", ""), "employee_after": shift["employee"],
                "note_before": before.get("note", ""), "note_after": shift["note"],
                "start_before": before.get("start"), "end_before": before.get("end"),
                "position_before": before.get("position"),
            })
    for uid, shift in old.items():
        if uid not in new:
            events.append({**base("removed", uid, shift), "employee_before": shift["employee"], "note_before": shift["note"]})
    return events


# Shifts filled in from W2W's API are dropped if the API has not been read successfully for this long.
API_FILL_MAX_AGE_S = 1200
_API_RED = "9"  # COLOR_ID dispatch gives a called-out shift; only ever used to choose between equal candidates


def _api_dt(day: str, clock: str) -> datetime:
    """W2W API date + time ("10/9/2026", "2:30pm") as a New York datetime."""
    month, dom, year = (int(part) for part in day.split("/"))
    match = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?\s*([ap])m?", clock.strip().lower())
    if not match:
        raise ValueError(f"unreadable W2W time {clock!r}")
    hour = int(match.group(1)) % 12 + (12 if match.group(3) == "p" else 0)
    return datetime(year, month, dom, hour, int(match.group(2) or 0), tzinfo=NY)


def _hours(shift: Dict[str, Any]) -> float:
    return (datetime.fromisoformat(shift["end"]) - datetime.fromisoformat(shift["start"])).total_seconds() / 3600.0


def missing_open_shifts(
    shifts: Dict[str, Dict[str, Any]], totals: List[Dict[str, Any]], assigned: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Unassigned shifts W2W has that the calendar feed does not, as (filled, unresolved).

    `totals` are DailyPositionTotals rows and `assigned` AssignedShiftList rows for the same days. For each position
    and day where W2W counts more unassigned shifts than the feed holds, the missing ones are unassigned copies of
    assigned shifts on that position that day, so their times are taken from the assigned shifts whose lengths add up
    to the missing hours (ignoring any whose copy is already in the feed). `filled` are snapshot-shaped shifts with
    "filled": True and an empty note (W2WScheduleLog.apply_api looks the note up in the change log). A gap that no single choice of shifts
    explains goes to `unresolved` and nothing is made up for it. Days W2W has not published are skipped: the feed
    only carries published shifts."""
    feed_open: Dict[tuple, List[Dict[str, Any]]] = {}
    for shift in shifts.values():
        if not shift["employee"]:
            feed_open.setdefault((shift["start"][:10], shift.get("position_name") or shift["position"]), []).append(shift)
    originals: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in assigned:
        if str(row.get("PUBLISHED", "Y")).upper() != "Y":
            continue
        try:
            start = _api_dt(row["START_DATE"], row["START_TIME"])
            end = _api_dt(row.get("END_DATE") or row["START_DATE"], row["END_TIME"])
        except (KeyError, ValueError):
            continue
        if end <= start:
            end += timedelta(days=1)
        originals.setdefault((start.date().isoformat(), row.get("POSITION_NAME", "")), []).append({
            "start": start.isoformat(timespec="minutes"), "end": end.isoformat(timespec="minutes"),
            "red": str(row.get("COLOR_ID", "")) == _API_RED,
        })
    published_days = {day for day, _ in originals}
    filled: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    for row in totals:
        try:
            month, dom, year = (int(part) for part in row["SCHEDULE_DATE"].split("/"))
            day, name = date(year, month, dom).isoformat(), row["POSITION_NAME"]
            want, want_hours = int(row["UNASSIGNED_SHIFTS"]), float(row["UNASSIGNED_HOURS"])
        except (KeyError, ValueError):
            continue
        have = feed_open.get((day, name), [])
        missing = want - len(have)
        if missing <= 0 or day not in published_days:
            continue
        missing_hours = want_hours - sum(_hours(s) for s in have)
        candidates = list(originals.get((day, name), []))
        for twin in have:  # an original whose copy already reached the feed is not the one that is missing
            match = next((c for c in candidates if (c["start"], c["end"]) == (twin["start"], twin["end"])), None)
            if match:
                candidates.remove(match)
        picks = [c for c in combinations(candidates, missing) if abs(sum(_hours(x) for x in c) - missing_hours) < 0.01]
        if len(picks) > 1:
            picks = [c for c in picks if all(x["red"] for x in c)]
        if len(picks) != 1:
            unresolved.append({"date": day, "position_name": name, "missing": missing, "hours": round(missing_hours, 2)})
            continue
        position = re.search(r"\[([^\]]+)\]", name)
        for original in picks[0]:
            filled.append({"employee": "", "position": position.group(1) if position else name, "position_name": name,
                           "start": original["start"], "end": original["end"], "note": "", "modified": "", "filled": True})
    return filled, unresolved


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class W2WScheduleLog:
    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.snapshot_path = self.directory / SNAPSHOT_NAME
        self.log_path = self.directory / LOG_NAME
        self.last_poll_ts: Optional[str] = None
        self.last_error: Optional[str] = None
        self._snapshot: Optional[Dict[str, Dict[str, Any]]] = None  # in-memory copy of the last good snapshot
        # Unassigned shifts W2W's API says exist but the feed has not delivered (see missing_open_shifts)
        self.filled: List[Dict[str, Any]] = []
        self._filled_at: Optional[datetime] = None
        self.unresolved: List[Dict[str, Any]] = []
        self.last_api_error: Optional[str] = None
        self._fill_notes: Dict[tuple, str] = {}  # (position_name, start, end) -> note, kept while the shift stays filled

    def _load_snapshot(self) -> Optional[Dict[str, Dict[str, Any]]]:
        if self._snapshot is not None:
            return self._snapshot
        try:
            self._snapshot = json.loads(self.snapshot_path.read_text(encoding="utf-8")).get("shifts")
        except (OSError, ValueError):
            return None
        return self._snapshot

    def apply(self, ics_text: str, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Diff a fetched feed against the saved snapshot, append the changes to the log and save the new snapshot.
        The first ever fetch just writes the snapshot and one "baseline" record. Returns the events written."""
        now = now or datetime.now(timezone.utc)
        parsed = {uid: s for uid, s in parse_ics(ics_text).items() if _tracked(s, now)}
        stamp = now.astimezone(timezone.utc).isoformat(timespec="seconds")
        if not parsed:
            self.last_error = "feed had no usable shifts"
            return []
        old = self._load_snapshot()
        if old is not None and len(parsed) < len(old) * MIN_KEEP_FRACTION:
            self.last_error = f"feed shrank from {len(old)} to {len(parsed)} shifts; ignored"
            return []
        events: List[Dict[str, Any]]
        if old is None:
            events = [{"ts": stamp, "kind": "baseline", "shifts": len(parsed)}]
        else:
            events = diff_shifts({u: s for u, s in old.items() if _tracked(s, now)}, parsed, now)
        if events:
            self.directory.mkdir(parents=True, exist_ok=True)
            with self.log_path.open("a", encoding="utf-8") as fh:
                fh.write("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in events))
        _atomic_write(self.snapshot_path, json.dumps({"saved_at": stamp, "shifts": parsed}, separators=(",", ":")))
        self._snapshot = parsed
        self.last_poll_ts, self.last_error = stamp, None
        return events

    def apply_api(self, totals: List[Dict[str, Any]], assigned: List[Dict[str, Any]], now: Optional[datetime] = None) -> None:
        """Take one read of W2W's API (DailyPositionTotals + AssignedShiftList rows for the same days) and work out which
        unassigned shifts the feed is missing. Memory only: the change log stays a record of the feed itself."""
        filled, self.unresolved = missing_open_shifts(self._load_snapshot() or {}, totals, assigned)
        notes: Dict[tuple, str] = {}
        for shift in filled:
            key = (shift["position_name"], shift["start"], shift["end"])
            # Looked up once and kept, so a later edit to the original's note cannot change what the board shows
            notes[key] = self._fill_notes[key] if key in self._fill_notes else self._note_before_last_edit(shift)
            shift["note"] = notes[key]
        self.filled, self._fill_notes = filled, notes
        self._filled_at = now or datetime.now(timezone.utc)
        self.last_api_error = None

    def _note_before_last_edit(self, shift: Dict[str, Any]) -> str:
        """The note an unassigned copy carries: what the original shift's note was BEFORE the edit that made the copy.
        W2W copies the shift as it stood, then dispatch often adds why it is open ("DNS(Sick)", "callout") to the
        original only; that reason must never reach the board. The feed shows the edit as the named shift being
        removed and added again, so this is the note on the latest such removal. "" when the log has none."""
        try:
            lines = self.log_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return ""
        for line in reversed(lines):
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if (event.get("kind") in ("removed", "changed") and event.get("employee_before")
                    and event.get("position") == shift["position"]
                    and event.get("start_before", event.get("start")) == shift["start"]
                    and event.get("end_before", event.get("end")) == shift["end"]):
                return event.get("note_before", "")
        return ""

    def _open_shifts(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Every unassigned shift: the feed's, plus the ones filled in from the API that the feed still lacks."""
        shifts = [s for s in (self._load_snapshot() or {}).values() if not s["employee"]]
        now = now or datetime.now(timezone.utc)
        if self._filled_at is None or (now - self._filled_at).total_seconds() > API_FILL_MAX_AGE_S:
            return shifts
        in_feed = {(s.get("position_name") or s["position"], s["start"], s["end"]) for s in shifts}
        return shifts + [s for s in self.filled if (s["position_name"], s["start"], s["end"]) not in in_feed]

    def recent_changes(self, limit: int = 200, on_date: Optional[str] = None, position: Optional[str] = None) -> List[Dict[str, Any]]:
        try:
            lines = self.log_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return []
        out: List[Dict[str, Any]] = []
        for line in reversed(lines):
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("kind") == "baseline":
                continue
            if on_date and event.get("date") != on_date:
                continue
            if position and str(event.get("position", "")).lstrip("0") != str(position).lstrip("0"):
                continue
            out.append(event)
            if len(out) >= limit:
                break
        return out

    def unassigned(self, start: date, days: int = 7) -> List[Dict[str, Any]]:
        """Unassigned bus-block shifts (position like "08") starting from `start` for `days` days."""
        end = start + timedelta(days=days)
        rows = [
            {"date": s["start"][:10], "position": s["position"], "start": s["start"], "end": s["end"], "note": s["note"]}
            for s in self._open_shifts()
            if _BUS_BLOCK_RE.fullmatch(s["position"] or "")
            and start.isoformat() <= s["start"][:10] < end.isoformat()
        ]
        rows.sort(key=lambda r: (r["start"], r["position"]))
        return rows

    def open_blocks(self, now: Optional[datetime] = None) -> Dict[str, Dict[str, Any]]:
        """Open shifts ("OB") for the current service day of each side, for the /ob board. Bus side: bus blocks, charters, Sup and
        FlexRide Dispatch, day 02:30 -> 02:30. OnDemand side: OnDemand/FlexRide drivers and EBs and OnDemand Dispatch,
        day 05:30 -> 05:30. A shift belongs to the day its START falls in; ended shifts are kept (flagged by the page)."""
        now = (now or datetime.now(timezone.utc)).astimezone(NY)
        shifts = self._open_shifts(now)
        result: Dict[str, Dict[str, Any]] = {}
        for side, rollover, groups in OB_SIDES:
            day_start = datetime.combine(now.date(), rollover, NY)
            if now < day_start:
                day_start -= timedelta(days=1)
            day_end = day_start + timedelta(days=1)
            rows = []
            for shift in shifts:
                group = next((g for g, match in groups if match(shift["position"] or "")), None)
                if group and day_start <= datetime.fromisoformat(shift["start"]) < day_end:
                    rows.append({"group": group, "position": shift["position"], "start": shift["start"],
                                 "end": shift["end"], "note": shift["note"]})
            rows.sort(key=lambda r: (r["start"], r["position"]))
            result[side] = {"day_start": day_start.isoformat(timespec="minutes"),
                            "day_end": day_end.isoformat(timespec="minutes"), "shifts": rows}
        return result

    def open_shift_rows(
        self, now: Optional[datetime] = None, first: Optional[date] = None, last: Optional[date] = None,
    ) -> List[Dict[str, str]]:
        """Unassigned shifts (any position: bus blocks, Sup, OnDemand, ...) that have not ended yet and START on a day
        from `first` to `last` inclusive, shaped like the rows of W2W's AssignedShiftList (empty FIRST_NAME/LAST_NAME) so
        the same builders that turn API shifts into per-block assignments can take them.

        The caller passes the SAME days it asked W2W's API for, because "today" is defined differently in different
        places (block-drivers: yesterday + today; on_duty: one service day that rolls over at 02:30) and a shift
        from another day must not show up as if it were today's. Default: yesterday..today, which is what block-drivers
        uses (yesterday so a shift running past midnight still counts). Empty until the first poll has succeeded."""
        now = (now or datetime.now(timezone.utc)).astimezone(NY)
        first_day = (first or (now.date() - timedelta(days=1))).isoformat()
        last_day = (last or now.date()).isoformat()
        rows: List[Dict[str, str]] = []
        for shift in self._open_shifts(now):
            if not (first_day <= shift["start"][:10] <= last_day):
                continue
            start, end = datetime.fromisoformat(shift["start"]), datetime.fromisoformat(shift["end"])
            if end <= now:
                continue
            rows.append({
                "POSITION_NAME": shift.get("position_name") or shift["position"],
                "FIRST_NAME": "", "LAST_NAME": "", "COLOR_ID": "0",
                "START_DATE": f"{start.month}/{start.day}/{start.year}", "START_TIME": _w2w_clock(start),
                "END_DATE": f"{end.month}/{end.day}/{end.year}", "END_TIME": _w2w_clock(end),
                "DURATION": str(round((end - start).total_seconds() / 3600.0, 2)),
                "DESCRIPTION": shift.get("note", ""),
            })
        rows.sort(key=lambda r: (r["START_DATE"], r["START_TIME"]))
        return rows


def _w2w_clock(moment: datetime) -> str:
    """W2W's own time style: 7am, 10:30am, 6:45pm."""
    hour12 = moment.hour % 12 or 12
    suffix = "am" if moment.hour < 12 else "pm"
    return f"{hour12}{suffix}" if moment.minute == 0 else f"{hour12}:{moment.minute:02d}{suffix}"
