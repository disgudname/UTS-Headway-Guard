"""UTS service levels ("Full Service", "Recess Service", "No Service", ...) from parking.virginia.edu/serviceschedule.

The page is a plain Drupal table (one row per day, today through roughly the end of next month; no pager), but it sits
behind a Cloudflare challenge that 403s httpx/curl. The headless Chromium already in the image (for PulsePoint's WAF)
gets through it in 1-2 s, from Fly's IP too (checked 2026-09-28), so a pull loads the page in Chromium and reads the table.

Columns are kept by their header text (on 2026-09-28: "UVA Transit", "UVA Ride", "Night Pilot", "UVA FlexRide", "Notes"),
not by position, so a renamed or added column shows up instead of silently shifting. Dates on the page have no year
("Oct 3 - Saturday"); the year is the one that puts the date closest to today.

Every pull is merged into <data dir>/service_schedule.json by date, so days that have scrolled off the page are kept as
history. A pull with no rows, or without a date column, is rejected and the last good copy keeps being served.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SCHEDULE_URL = "https://parking.virginia.edu/serviceschedule"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
KEEP_HISTORY_DAYS = 400
_DATE_RE = re.compile(r"^\s*([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2})\b")
_MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}

# Reads every table on the page (the schedule is split into more than one, e.g. by month), each as {headers, rows}.
_READ_TABLES_JS = """() => [...document.querySelectorAll('table')].map(t => ({
  headers: [...t.querySelectorAll('thead th')].map(th => th.innerText.trim()),
  rows: [...t.querySelectorAll('tbody tr')].map(tr => [...tr.querySelectorAll('td')].map(td => td.innerText.trim())),
}))"""


def parse_page_date(text: str, today: date) -> Optional[date]:
    """'Oct 3 - Saturday' -> the date nearest `today` with that month/day."""
    match = _DATE_RE.match(text or "")
    month = _MONTHS.get(match.group(1).lower()) if match else None
    if not month:
        return None
    candidates = []
    for year in (today.year - 1, today.year, today.year + 1):
        try:
            candidates.append(date(year, month, int(match.group(2))))
        except ValueError:
            pass
    return min(candidates, key=lambda d: abs((d - today).days)) if candidates else None


def parse_table(headers: List[str], rows: List[List[str]], today: date) -> Dict[str, Dict[str, Any]]:
    """{iso date: {"label", "services": {column: level}, "notes"}} from the page's table.
    Raises ValueError when the table doesn't look like the schedule."""
    if not rows or not headers or "date" not in headers[0].lower():
        raise ValueError(f"unexpected table: headers={headers[:8]} rows={len(rows)}")
    days: Dict[str, Dict[str, Any]] = {}
    for cells in rows:
        if not cells:
            continue
        day = parse_page_date(cells[0], today)
        if day is None:
            continue
        services: Dict[str, str] = {}
        notes = ""
        for name, value in zip(headers[1:], cells[1:]):
            value = " ".join(value.split())
            if name.lower() == "notes":
                notes = value
            elif name:
                services[name] = value
        days[day.isoformat()] = {"label": " ".join(cells[0].split()), "services": services, "notes": notes}
    if not days:
        raise ValueError(f"no dated rows in {len(rows)} rows")
    return days


async def fetch_tables(url: str = SCHEDULE_URL, channel: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load the page in headless Chromium (past Cloudflare) and return its tables as [{headers, rows}]."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, channel=channel)
        try:
            page = await browser.new_page(user_agent=USER_AGENT)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector("table tbody tr td", timeout=45000)
            tables = await page.evaluate(_READ_TABLES_JS)
        finally:
            await browser.close()
    if not tables:
        raise ValueError("no table on the page")
    return tables


def _write_atomic(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class ServiceSchedule:
    def __init__(self, data_dir: Path):
        self.path = Path(data_dir) / "service_schedule.json"
        self.days: Dict[str, Dict[str, Any]] = {}
        self.fetched_at: Optional[str] = None
        self.last_error: Optional[str] = None
        try:
            saved = json.loads(self.path.read_text(encoding="utf-8"))
            self.days = saved.get("days") or {}
            self.fetched_at = saved.get("fetched_at")
        except (OSError, ValueError):
            pass

    def apply(self, tables: List[Dict[str, Any]], today: date) -> List[str]:
        """Merge a pull ([{headers, rows}] per table) in; returns the dates whose entry changed.
        Tables not headed "...Date" are ignored; leaves everything as-is if a schedule table doesn't parse or there is none."""
        fresh: Dict[str, Dict[str, Any]] = {}
        try:
            tables = [t for t in tables if "date" in ((t.get("headers") or [""])[0]).lower()]
            if not tables:
                raise ValueError("no schedule table on the page")
            for table in tables:
                fresh.update(parse_table(table.get("headers") or [], table.get("rows") or [], today))
        except ValueError as exc:
            self.last_error = str(exc)
            return []
        changed = [d for d, entry in fresh.items() if self.days.get(d) != entry]
        cutoff = (today - timedelta(days=KEEP_HISTORY_DAYS)).isoformat()
        self.days = {d: e for d, e in {**self.days, **fresh}.items() if d >= cutoff}
        self.fetched_at = datetime.now(timezone.utc).isoformat()
        self.last_error = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(self.path, json.dumps(
            {"fetched_at": self.fetched_at, "source": SCHEDULE_URL, "days": dict(sorted(self.days.items()))}, indent=1
        ).encode("utf-8"))
        return changed

    def day(self, d: date) -> Optional[Dict[str, Any]]:
        entry = self.days.get(d.isoformat())
        return {"date": d.isoformat(), **entry} if entry else None

    def status(self, service_day: date, start: date, days: int) -> Dict[str, Any]:
        listed = []
        for i in range(max(0, days)):
            entry = self.day(start + timedelta(days=i))
            if entry:
                listed.append(entry)
        return {
            "today": self.day(service_day),
            "days": listed,
            "fetched_at": self.fetched_at,
            "last_error": self.last_error,
            "source": SCHEDULE_URL,
        }
