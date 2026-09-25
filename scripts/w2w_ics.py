"""Parses a W2W "Complete Schedule" Google-Calendar .ics export into shift rows.

Each VEVENT is one shift; DESCRIPTION is "<employee>\\n[<position>]\\n<times>\\n<date>\\n<shift note>...". An UNASSIGNED shift
has an empty employee line (the summary starts with the position), so it shows up here even though the API's
AssignedShiftList never returns it.
  python scripts/w2w_ics.py <file.ics>
"""
import datetime
import re
import sys
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
BACKSLASH_N = "\\" + "n"
BACKSLASH_COMMA = "\\" + ","


def unfold(text):
    return re.sub(r"\r?\n[ \t]", "", text)


def _utc_to_ny(value):
    return datetime.datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=datetime.timezone.utc).astimezone(NY)


def parse(path):
    text = unfold(open(path, encoding="utf-8", errors="ignore").read())
    out = []
    for block in re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", text, re.S):
        fields = {}
        for line in block.strip().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.split(";")[0]] = value
        desc = fields.get("DESCRIPTION", "").replace(BACKSLASH_N, "\n").replace(BACKSLASH_COMMA, ",")
        desc = re.sub(r"\(key:.*?\)", "", desc).rstrip("\n ")
        lines = desc.split("\n")
        out.append({
            "start": _utc_to_ny(fields["DTSTART"]),
            "end": _utc_to_ny(fields["DTEND"]),
            "summary": fields.get("SUMMARY", "").replace(BACKSLASH_COMMA, ","),
            "employee": lines[0].strip() if lines else "",
            "position": (re.search(r"\[([^\]]+)\]", lines[1]).group(1) if len(lines) > 1 and re.search(r"\[([^\]]+)\]", lines[1]) else (lines[1].strip() if len(lines) > 1 else "")),
            "note": lines[4].strip() if len(lines) > 4 else "",
            "modified": fields.get("LAST-MODIFIED"),
            "status": fields.get("STATUS"),
        })
    return out


if __name__ == "__main__":
    events = parse(sys.argv[1])
    print(len(events), min(e["start"] for e in events), max(e["start"] for e in events))
