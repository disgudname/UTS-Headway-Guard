"""
UTS Service Level Scraper

Scrapes the official UVA Parking & Transportation service schedule page
to determine the current service level for University Transit Service (UTS).

Service Day Logic:
- A service day runs from 02:30 to 02:30 America/New_York
- If local time is 00:00-02:29:59, the service day is yesterday's date
- If local time is 02:30+, the service day is today's date
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

# Constants
SERVICE_SCHEDULE_URL = "https://parking.virginia.edu/serviceschedule"
NY_TZ = ZoneInfo("America/New_York")
SERVICE_DAY_CUTOFF = time(2, 30, 0)  # 02:30 AM

# Month name abbreviation to month number mapping
MONTH_ABBREV_TO_NUM = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Header text variations for column identification
HEADER_SERVICE_DATE = "service date"
HEADER_UTS = "university transit service"
HEADER_NOTES = "notes"


@dataclass
class ServiceLevelResult:
    """Result of a service level lookup."""
    service_date: str  # YYYY-MM-DD format
    service_level: str  # Exact text from webpage (trimmed), or "UNKNOWN"
    notes: Optional[str]  # Trimmed notes, None if empty
    source_url: str = SERVICE_SCHEDULE_URL
    scraped_at: str = ""  # ISO 8601 timestamp
    source_hash: str = ""  # Hash of the row HTML for change detection
    error: Optional[str] = None  # Error message when returning UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        result = {
            "service_date": self.service_date,
            "service_level": self.service_level,
            "notes": self.notes,
            "source_url": self.source_url,
            "scraped_at": self.scraped_at,
            "source_hash": self.source_hash,
        }
        if self.error is not None:
            result["error"] = self.error
        return result


@dataclass
class ServiceLevelCache:
    """Cache for service level data."""
    result: Optional[ServiceLevelResult] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    fetched_at: float = 0.0  # Unix timestamp


def get_service_date(now: Optional[datetime] = None) -> date:
    """
    Determine the current service date based on the 02:30 cutoff.

    Args:
        now: Current datetime in any timezone (defaults to now in NY timezone)

    Returns:
        The service date (calendar date in NY timezone, adjusted for cutoff)
    """
    if now is None:
        now = datetime.now(NY_TZ)
    else:
        now = now.astimezone(NY_TZ)

    current_time = now.time()
    current_date = now.date()

    # If before 02:30, the service day is yesterday
    if current_time < SERVICE_DAY_CUTOFF:
        return current_date - timedelta(days=1)
    return current_date


def parse_date_cell(date_text: str, target_year: int) -> Optional[date]:
    """
    Parse a date cell like "Dec 17 - Wednesday" into a date object.

    Args:
        date_text: The date text from the table cell
        target_year: The year to use for parsing (handles year boundaries)

    Returns:
        Parsed date or None if parsing fails
    """
    # Pattern: "Dec 17 - Wednesday" or "Dec 17-Wednesday" or similar
    # Extract month abbreviation and day number
    pattern = r"([A-Za-z]{3})\s*(\d{1,2})"
    match = re.search(pattern, date_text.strip())
    if not match:
        return None

    month_abbrev = match.group(1).lower()
    day = int(match.group(2))

    month = MONTH_ABBREV_TO_NUM.get(month_abbrev)
    if month is None:
        return None

    try:
        return date(target_year, month, day)
    except ValueError:
        return None


def find_column_indices(headers: list) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Find column indices for Service Date, UTS, and Notes columns.

    Args:
        headers: List of header cell text values

    Returns:
        Tuple of (date_idx, uts_idx, notes_idx), any can be None if not found
    """
    date_idx = None
    uts_idx = None
    notes_idx = None

    for i, header in enumerate(headers):
        header_lower = header.lower().strip()
        if HEADER_SERVICE_DATE in header_lower:
            date_idx = i
        elif HEADER_UTS in header_lower:
            uts_idx = i
        elif HEADER_NOTES in header_lower:
            notes_idx = i

    return date_idx, uts_idx, notes_idx


def extract_cell_text(cell) -> str:
    """
    Extract text content from a table cell, handling nested HTML.

    Args:
        cell: BeautifulSoup Tag object for the <td> element

    Returns:
        Trimmed text content
    """
    # Get text content, stripping HTML
    text = cell.get_text(separator=" ", strip=True)
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compute_row_hash(row_html: str) -> str:
    """Compute a hash of the row HTML for change detection."""
    return hashlib.sha256(row_html.encode("utf-8")).hexdigest()[:16]


def parse_service_schedule(
    html: str,
    target_date: date,
) -> ServiceLevelResult:
    """
    Parse the service schedule HTML and extract service level for a specific date.

    Args:
        html: The HTML content of the service schedule page
        target_date: The service date to look up

    Returns:
        ServiceLevelResult with extracted data or error
    """
    now_iso = datetime.now(NY_TZ).isoformat()
    target_date_str = target_date.isoformat()

    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as e:
        return ServiceLevelResult(
            service_date=target_date_str,
            service_level="UNKNOWN",
            notes=None,
            scraped_at=now_iso,
            error=f"HTML parse error: {e}",
        )

    # Find the main schedule table
    # The table has class "table table-hover table-striped"
    tables = soup.find_all("table", class_="table")
    if not tables:
        return ServiceLevelResult(
            service_date=target_date_str,
            service_level="UNKNOWN",
            notes=None,
            scraped_at=now_iso,
            error="No schedule table found in page",
        )

    # Try each table to find the one with our date
    for table in tables:
        thead = table.find("thead")
        tbody = table.find("tbody")

        if not thead or not tbody:
            continue

        # Extract header cells
        header_row = thead.find("tr")
        if not header_row:
            continue

        headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
        date_idx, uts_idx, notes_idx = find_column_indices(headers)

        if date_idx is None or uts_idx is None:
            continue

        # Search rows for the target date
        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) <= max(date_idx, uts_idx, notes_idx or 0):
                continue

            date_text = extract_cell_text(cells[date_idx])
            row_date = parse_date_cell(date_text, target_date.year)

            # Handle year boundary: if target is Jan and row shows Dec, try previous year
            if row_date is None:
                continue

            # Handle year boundary scenarios
            if row_date.month == 12 and target_date.month == 1:
                # Row shows December, target is January - row is from last year
                row_date = parse_date_cell(date_text, target_date.year - 1)
            elif row_date.month == 1 and target_date.month == 12:
                # Row shows January, target is December - row is from next year
                row_date = parse_date_cell(date_text, target_date.year + 1)

            if row_date != target_date:
                continue

            # Found the row - extract service level and notes
            service_level = extract_cell_text(cells[uts_idx])
            notes = None
            if notes_idx is not None and notes_idx < len(cells):
                notes_text = extract_cell_text(cells[notes_idx])
                notes = notes_text if notes_text else None

            # Compute row hash for change detection
            row_html = str(row)
            source_hash = compute_row_hash(row_html)

            return ServiceLevelResult(
                service_date=target_date_str,
                service_level=service_level,
                notes=notes,
                scraped_at=now_iso,
                source_hash=source_hash,
            )

    # Date not found in any table
    return ServiceLevelResult(
        service_date=target_date_str,
        service_level="UNKNOWN",
        notes=None,
        scraped_at=now_iso,
        error=f"Date {target_date_str} not found in schedule table",
    )


# For unit testing: expose internal functions
__all__ = [
    "SERVICE_SCHEDULE_URL",
    "ServiceLevelResult",
    "ServiceLevelCache",
    "get_service_date",
    "parse_date_cell",
    "find_column_indices",
    "extract_cell_text",
    "compute_row_hash",
    "parse_service_schedule",
]
