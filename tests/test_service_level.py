"""
Unit tests for the UTS Service Level scraper.

Tests cover:
- Service day boundary logic (02:30 cutoff)
- Date parsing from table cells
- Normal extraction of service level and notes
- Empty notes handling
- Scrape failure fallback
- Column identification
"""

import pytest
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from service_level import (
    SERVICE_SCHEDULE_URL,
    ServiceLevelResult,
    get_service_date,
    parse_date_cell,
    find_column_indices,
    extract_cell_text,
    compute_row_hash,
    parse_service_schedule,
)

NY_TZ = ZoneInfo("America/New_York")


class TestGetServiceDate:
    """Tests for the 02:30 service day cutoff logic."""

    def test_before_cutoff_uses_previous_day(self):
        """Before 02:30, service day should be yesterday."""
        # 2:00 AM on Dec 18 -> service day is Dec 17
        now = datetime(2025, 12, 18, 2, 0, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 12, 17)

    def test_at_cutoff_uses_current_day(self):
        """At exactly 02:30, service day should be today."""
        now = datetime(2025, 12, 18, 2, 30, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 12, 18)

    def test_after_cutoff_uses_current_day(self):
        """After 02:30, service day should be today."""
        now = datetime(2025, 12, 18, 10, 0, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 12, 18)

    def test_just_before_cutoff(self):
        """At 02:29:59, service day should still be yesterday."""
        now = datetime(2025, 12, 18, 2, 29, 59, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 12, 17)

    def test_midnight_uses_previous_day(self):
        """At midnight, service day should be yesterday."""
        now = datetime(2025, 12, 18, 0, 0, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 12, 17)

    def test_year_boundary_before_cutoff(self):
        """Jan 1 at 1:00 AM should return Dec 31 of previous year."""
        now = datetime(2025, 1, 1, 1, 0, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2024, 12, 31)

    def test_year_boundary_after_cutoff(self):
        """Jan 1 at 3:00 AM should return Jan 1."""
        now = datetime(2025, 1, 1, 3, 0, 0, tzinfo=NY_TZ)
        result = get_service_date(now)
        assert result == date(2025, 1, 1)

    def test_handles_utc_timezone(self):
        """Should correctly convert from UTC to NY time."""
        # 7:00 AM UTC on Dec 18 = 2:00 AM EST (before cutoff)
        utc_now = datetime(2025, 12, 18, 7, 0, 0, tzinfo=ZoneInfo("UTC"))
        result = get_service_date(utc_now)
        assert result == date(2025, 12, 17)

    def test_handles_pst_timezone(self):
        """Should correctly convert from PST to NY time."""
        # 11:00 PM PST on Dec 17 = 2:00 AM EST on Dec 18 (before cutoff)
        pst_now = datetime(2025, 12, 17, 23, 0, 0, tzinfo=ZoneInfo("America/Los_Angeles"))
        result = get_service_date(pst_now)
        # 11 PM PST = 2 AM EST next day, which is before cutoff
        assert result == date(2025, 12, 17)


class TestParseDateCell:
    """Tests for parsing date cells like 'Dec 17 - Wednesday'."""

    def test_standard_format(self):
        """Parse standard format 'Dec 17 - Wednesday'."""
        result = parse_date_cell("Dec 17 - Wednesday", 2025)
        assert result == date(2025, 12, 17)

    def test_format_without_dash(self):
        """Parse format without spaces around dash."""
        result = parse_date_cell("Dec 17-Wednesday", 2025)
        assert result == date(2025, 12, 17)

    def test_single_digit_day(self):
        """Parse single digit day."""
        result = parse_date_cell("Jan 5 - Friday", 2025)
        assert result == date(2025, 1, 5)

    def test_all_months(self):
        """Parse all month abbreviations."""
        months = [
            ("Jan 15", 1), ("Feb 15", 2), ("Mar 15", 3), ("Apr 15", 4),
            ("May 15", 5), ("Jun 15", 6), ("Jul 15", 7), ("Aug 15", 8),
            ("Sep 15", 9), ("Oct 15", 10), ("Nov 15", 11), ("Dec 15", 12),
        ]
        for text, month in months:
            result = parse_date_cell(text, 2025)
            assert result == date(2025, month, 15), f"Failed for {text}"

    def test_case_insensitive(self):
        """Month abbreviation should be case insensitive."""
        assert parse_date_cell("DEC 17", 2025) == date(2025, 12, 17)
        assert parse_date_cell("dec 17", 2025) == date(2025, 12, 17)

    def test_extra_whitespace(self):
        """Handle extra whitespace."""
        result = parse_date_cell("  Dec  17  -  Wednesday  ", 2025)
        assert result == date(2025, 12, 17)

    def test_invalid_month(self):
        """Invalid month should return None."""
        result = parse_date_cell("Xyz 17 - Wednesday", 2025)
        assert result is None

    def test_missing_day(self):
        """Missing day should return None."""
        result = parse_date_cell("Dec - Wednesday", 2025)
        assert result is None

    def test_invalid_day(self):
        """Invalid day (e.g., Feb 30) should return None."""
        result = parse_date_cell("Feb 30 - Friday", 2025)
        assert result is None


class TestFindColumnIndices:
    """Tests for finding column indices from header text."""

    def test_standard_headers(self):
        """Find indices for standard headers."""
        headers = [
            "Service Date",
            "University Transit Service (UTS)",
            "UTS OnDemand",
            "UTS Night Pilot",
            "DART",
            "Notes"
        ]
        date_idx, uts_idx, notes_idx = find_column_indices(headers)
        assert date_idx == 0
        assert uts_idx == 1
        assert notes_idx == 5

    def test_case_insensitive(self):
        """Headers should be matched case insensitively."""
        headers = [
            "SERVICE DATE",
            "UNIVERSITY TRANSIT SERVICE (UTS)",
            "NOTES"
        ]
        date_idx, uts_idx, notes_idx = find_column_indices(headers)
        assert date_idx == 0
        assert uts_idx == 1
        assert notes_idx == 2

    def test_missing_notes_column(self):
        """Handle missing notes column."""
        headers = [
            "Service Date",
            "University Transit Service (UTS)",
        ]
        date_idx, uts_idx, notes_idx = find_column_indices(headers)
        assert date_idx == 0
        assert uts_idx == 1
        assert notes_idx is None

    def test_missing_required_columns(self):
        """Handle missing required columns."""
        headers = ["Some Other Column", "Notes"]
        date_idx, uts_idx, notes_idx = find_column_indices(headers)
        assert date_idx is None
        assert uts_idx is None
        assert notes_idx == 1


class TestComputeRowHash:
    """Tests for row hash computation."""

    def test_deterministic(self):
        """Same input should produce same hash."""
        html = "<tr><td>Dec 17</td><td>Full Service</td></tr>"
        hash1 = compute_row_hash(html)
        hash2 = compute_row_hash(html)
        assert hash1 == hash2

    def test_different_content_different_hash(self):
        """Different content should produce different hash."""
        html1 = "<tr><td>Dec 17</td><td>Full Service</td></tr>"
        html2 = "<tr><td>Dec 17</td><td>Exam Service</td></tr>"
        assert compute_row_hash(html1) != compute_row_hash(html2)

    def test_hash_length(self):
        """Hash should be 16 characters."""
        html = "<tr><td>Test</td></tr>"
        result = compute_row_hash(html)
        assert len(result) == 16


class TestParseServiceSchedule:
    """Tests for full HTML parsing."""

    SAMPLE_HTML = """
    <!DOCTYPE html>
    <html>
    <body>
    <table class="table table-hover table-striped">
        <thead>
            <tr>
                <th>Service Date</th>
                <th>University Transit Service (UTS)</th>
                <th>Notes</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Dec 17 - Wednesday</td>
                <td><p>Exam Service</p></td>
                <td></td>
            </tr>
            <tr>
                <td>Dec 18 - Thursday</td>
                <td><p>Exam Service</p></td>
                <td><p>Last day of exams</p></td>
            </tr>
            <tr>
                <td>Dec 19 - Friday</td>
                <td><p class="text-danger"><strong>No Service</strong></p></td>
                <td><p>Winter break begins</p></td>
            </tr>
        </tbody>
    </table>
    </body>
    </html>
    """

    def test_normal_extraction(self):
        """Extract service level from a normal row."""
        result = parse_service_schedule(self.SAMPLE_HTML, date(2025, 12, 17))
        assert result.service_level == "Exam Service"
        assert result.notes is None
        assert result.service_date == "2025-12-17"
        assert result.error is None
        assert result.source_hash != ""

    def test_extraction_with_notes(self):
        """Extract service level and notes."""
        result = parse_service_schedule(self.SAMPLE_HTML, date(2025, 12, 18))
        assert result.service_level == "Exam Service"
        assert result.notes == "Last day of exams"
        assert result.service_date == "2025-12-18"
        assert result.error is None

    def test_extraction_with_styled_text(self):
        """Extract service level with HTML styling (bold, danger class)."""
        result = parse_service_schedule(self.SAMPLE_HTML, date(2025, 12, 19))
        assert result.service_level == "No Service"
        assert result.notes == "Winter break begins"
        assert result.error is None

    def test_date_not_found(self):
        """Return UNKNOWN when date is not in table."""
        result = parse_service_schedule(self.SAMPLE_HTML, date(2025, 12, 25))
        assert result.service_level == "UNKNOWN"
        assert result.notes is None
        assert result.error is not None
        assert "not found" in result.error.lower()

    def test_empty_html(self):
        """Handle empty HTML."""
        result = parse_service_schedule("", date(2025, 12, 17))
        assert result.service_level == "UNKNOWN"
        assert result.error is not None

    def test_no_table_found(self):
        """Handle HTML with no schedule table."""
        html = "<html><body><p>No table here</p></body></html>"
        result = parse_service_schedule(html, date(2025, 12, 17))
        assert result.service_level == "UNKNOWN"
        assert result.error is not None
        assert "no schedule table" in result.error.lower()

    def test_malformed_html(self):
        """Handle malformed HTML gracefully."""
        html = "<table><tr><td>Unclosed table"
        result = parse_service_schedule(html, date(2025, 12, 17))
        # lxml should still parse it, but no valid data
        assert result.service_level == "UNKNOWN"

    def test_empty_notes_become_null(self):
        """Empty notes should be None, not empty string."""
        result = parse_service_schedule(self.SAMPLE_HTML, date(2025, 12, 17))
        assert result.notes is None

    def test_whitespace_only_notes_become_null(self):
        """Whitespace-only notes should be None."""
        html = """
        <table class="table">
            <thead><tr><th>Service Date</th><th>University Transit Service (UTS)</th><th>Notes</th></tr></thead>
            <tbody><tr><td>Dec 17</td><td>Full Service</td><td>   </td></tr></tbody>
        </table>
        """
        result = parse_service_schedule(html, date(2025, 12, 17))
        assert result.notes is None


class TestServiceLevelResult:
    """Tests for ServiceLevelResult dataclass."""

    def test_to_dict_without_error(self):
        """Convert result to dict without error field."""
        result = ServiceLevelResult(
            service_date="2025-12-17",
            service_level="Exam Service",
            notes="Test notes",
            scraped_at="2025-12-17T10:00:00-05:00",
            source_hash="abc123",
        )
        d = result.to_dict()
        assert d["service_date"] == "2025-12-17"
        assert d["service_level"] == "Exam Service"
        assert d["notes"] == "Test notes"
        assert d["source_url"] == SERVICE_SCHEDULE_URL
        assert d["scraped_at"] == "2025-12-17T10:00:00-05:00"
        assert d["source_hash"] == "abc123"
        assert "error" not in d

    def test_to_dict_with_error(self):
        """Convert result to dict with error field."""
        result = ServiceLevelResult(
            service_date="2025-12-17",
            service_level="UNKNOWN",
            notes=None,
            scraped_at="2025-12-17T10:00:00-05:00",
            source_hash="",
            error="Network error",
        )
        d = result.to_dict()
        assert d["service_level"] == "UNKNOWN"
        assert d["notes"] is None
        assert d["error"] == "Network error"

    def test_to_dict_with_null_notes(self):
        """Null notes should be preserved in dict."""
        result = ServiceLevelResult(
            service_date="2025-12-17",
            service_level="Exam Service",
            notes=None,
            scraped_at="2025-12-17T10:00:00-05:00",
            source_hash="abc",
        )
        d = result.to_dict()
        assert d["notes"] is None


class TestRealWorldHTML:
    """Tests using the sample HTML from the examples directory."""

    @pytest.fixture
    def sample_html(self):
        """Load the sample HTML file if available."""
        import os
        sample_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "examples",
            "serviceschedule.html"
        )
        if os.path.exists(sample_path):
            with open(sample_path, "r", encoding="utf-8") as f:
                return f.read()
        pytest.skip("Sample HTML file not available")

    def test_parse_dec_17(self, sample_html):
        """Parse Dec 17 from real sample HTML."""
        result = parse_service_schedule(sample_html, date(2025, 12, 17))
        assert result.service_level == "Exam Service"
        assert result.notes is None
        assert result.error is None

    def test_parse_dec_19_with_notes(self, sample_html):
        """Parse Dec 19 which has notes."""
        result = parse_service_schedule(sample_html, date(2025, 12, 19))
        assert result.service_level == "Exam Service"
        assert result.notes == "Service Ends at 8:00PM"

    def test_parse_dec_20_no_service(self, sample_html):
        """Parse Dec 20 which has No Service."""
        result = parse_service_schedule(sample_html, date(2025, 12, 20))
        assert result.service_level == "No Service"
        assert result.notes == "Men's Basketball"

    def test_parse_dec_25_holiday(self, sample_html):
        """Parse Dec 25 (Christmas) - No Service."""
        result = parse_service_schedule(sample_html, date(2025, 12, 25))
        assert result.service_level == "No Service"
        assert result.notes is None

    def test_all_december_dates_parseable(self, sample_html):
        """All December dates in the sample should be parseable."""
        for day in range(16, 32):
            try:
                target = date(2025, 12, day)
            except ValueError:
                continue  # Skip invalid dates
            result = parse_service_schedule(sample_html, target)
            # Should not return error for dates in the table
            assert result.error is None, f"Error for Dec {day}: {result.error}"
            assert result.service_level != "UNKNOWN", f"Unknown for Dec {day}"
