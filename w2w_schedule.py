"""Change log for the When To Work "Complete Schedule" iCal feed (the W2W_ICAL_URL secret).

W2W's read-only API (AssignedShiftList) never returns unassigned shifts, but the Google-Calendar copy of the full schedule
does: an unassigned shift is just a shift with no employee. The feed only shows the schedule as it is NOW, so this module
keeps the last snapshot and appends every difference (a shift added, removed, or changed hands/times/note) to a JSONL log,
which is the only way to know later who was on a shift, or that it was open, at a given time.

Files (in the data directory): w2w_schedule_snapshot.json, w2w_schedule_changes.jsonl.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
SNAPSHOT_NAME = "w2w_schedule_snapshot.json"
LOG_NAME = "w2w_schedule_changes.jsonl"
# Only shifts starting within this many days back are tracked (the feed reaches a year back; old edits are just noise).
TRACK_DAYS_BACK = 60
# A fetch that suddenly has far fewer shifts than the last good one is treated as a bad response, not a mass deletion.
MIN_KEEP_FRACTION = 0.5
COMPARED_FIELDS = ("employee", "position", "start", "end", "note")
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
        """Unassigned bus-block shifts (position like "08" or "19 PM") starting from `start` for `days` days."""
        shifts = self._load_snapshot() or {}
        end = start + timedelta(days=days)
        rows = [
            {"date": s["start"][:10], "position": s["position"], "start": s["start"], "end": s["end"], "note": s["note"]}
            for s in shifts.values()
            if not s["employee"] and re.fullmatch(r"\d{1,2}( ?[AP]M)?", s["position"] or "")
            and start.isoformat() <= s["start"][:10] < end.isoformat()
        ]
        rows.sort(key=lambda r: (r["start"], r["position"]))
        return rows

    def open_shift_rows(self, now: Optional[datetime] = None, days_ahead: int = 2) -> List[Dict[str, str]]:
        """Unassigned shifts (any position: bus blocks, Sup, OnDemand, ...) that have not ended yet and start within
        `days_ahead` days, shaped like the rows of W2W's AssignedShiftList (empty FIRST_NAME/LAST_NAME) so the same
        builders that turn API shifts into per-block assignments can take them. Yesterday's shifts are included so a
        shift running past midnight still counts. Empty until the first poll has succeeded."""
        now = (now or datetime.now(timezone.utc)).astimezone(NY)
        shifts = self._load_snapshot() or {}
        first = (now.date() - timedelta(days=1)).isoformat()
        last = (now.date() + timedelta(days=days_ahead)).isoformat()
        rows: List[Dict[str, str]] = []
        for shift in shifts.values():
            if shift["employee"] or not (first <= shift["start"][:10] <= last):
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
