"""
Test vehicle-drivers endpoint and block-to-driver mapping logic.
"""

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# Import the functions we're testing (they will be imported from app.py)
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app import (
    _split_interlined_blocks,
    _find_current_driver,
    _normalize_driver_name,
    _find_ondemand_driver_by_name,
    _extract_block_from_position_name,
)


class TestSplitInterlinedBlocks(unittest.TestCase):
    """Test the _split_interlined_blocks function."""

    def test_interlined_blocks(self):
        """Test splitting interlined blocks like [01]/[04]."""
        result = _split_interlined_blocks("[01]/[04]")
        self.assertEqual(result, ["01", "04"])

    def test_three_interlined_blocks(self):
        """Test splitting three interlined blocks."""
        result = _split_interlined_blocks("[01]/[04]/[05]")
        self.assertEqual(result, ["01", "04", "05"])

    def test_single_block(self):
        """Test a single non-interlined block."""
        result = _split_interlined_blocks("[01]")
        self.assertEqual(result, ["01"])

    def test_block_with_am_pm(self):
        """Test blocks with AM/PM designation."""
        result = _split_interlined_blocks("[16] AM")
        self.assertEqual(result, ["16"])

    def test_interlined_block_with_am_pm(self):
        """Test interlined blocks where one has AM/PM."""
        result = _split_interlined_blocks("[21]/[16] AM")
        self.assertEqual(result, ["21", "16"])

    def test_zero_padding(self):
        """Test that single-digit blocks get zero-padded."""
        result = _split_interlined_blocks("[1]/[4]")
        self.assertEqual(result, ["01", "04"])

    def test_blocks_20_26(self):
        """Test AM/PM blocks (20-26)."""
        result = _split_interlined_blocks("[20]/[10]")
        self.assertEqual(result, ["20", "10"])

    def test_empty_string(self):
        """Test empty string returns empty list."""
        result = _split_interlined_blocks("")
        self.assertEqual(result, [])

    def test_none(self):
        """Test None returns empty list."""
        result = _split_interlined_blocks(None)
        self.assertEqual(result, [])


class TestFindCurrentDriver(unittest.TestCase):
    """Test the _find_current_driver function."""

    def setUp(self):
        """Set up test data."""
        self.tz = ZoneInfo("America/New_York")
        self.now = datetime(2025, 12, 7, 10, 30, tzinfo=self.tz)  # 10:30 AM
        self.now_ts = int(self.now.timestamp() * 1000)

        # Create sample W2W assignments structure
        am_start = datetime(2025, 12, 7, 6, 0, tzinfo=self.tz)  # 6:00 AM
        am_end = datetime(2025, 12, 7, 12, 0, tzinfo=self.tz)   # 12:00 PM
        pm_start = datetime(2025, 12, 7, 13, 0, tzinfo=self.tz) # 1:00 PM
        pm_end = datetime(2025, 12, 7, 18, 0, tzinfo=self.tz)   # 6:00 PM

        self.assignments_by_block = {
            "01": {
                "am": [
                    {
                        "name": "John Doe",
                        "start_ts": int(am_start.timestamp() * 1000),
                        "end_ts": int(am_end.timestamp() * 1000),
                        "start_label": "6a",
                        "end_label": "12p",
                        "color_id": "0"
                    }
                ],
                "pm": [
                    {
                        "name": "Jane Smith",
                        "start_ts": int(pm_start.timestamp() * 1000),
                        "end_ts": int(pm_end.timestamp() * 1000),
                        "start_label": "1p",
                        "end_label": "6p",
                        "color_id": "1"
                    }
                ]
            },
            "04": {
                "any": [
                    {
                        "name": "Bob Johnson",
                        "start_ts": int(am_start.timestamp() * 1000),
                        "end_ts": int(pm_end.timestamp() * 1000),
                        "start_label": "6a",
                        "end_label": "6p",
                        "color_id": "2"
                    }
                ]
            }
        }

    def test_find_am_driver(self):
        """Test finding driver during AM shift."""
        driver = _find_current_driver("01", self.assignments_by_block, self.now_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "John Doe")

    def test_find_pm_driver(self):
        """Test finding driver during PM shift."""
        pm_time = datetime(2025, 12, 7, 14, 30, tzinfo=self.tz)  # 2:30 PM
        pm_ts = int(pm_time.timestamp() * 1000)
        driver = _find_current_driver("01", self.assignments_by_block, pm_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Jane Smith")

    def test_find_any_period_driver(self):
        """Test finding driver with 'any' period designation."""
        driver = _find_current_driver("04", self.assignments_by_block, self.now_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Bob Johnson")

    def test_no_driver_outside_shift(self):
        """Test that no driver is found outside shift times."""
        late_night = datetime(2025, 12, 7, 23, 0, tzinfo=self.tz)  # 11:00 PM
        late_ts = int(late_night.timestamp() * 1000)
        driver = _find_current_driver("01", self.assignments_by_block, late_ts)
        self.assertIsNone(driver)

    def test_no_driver_for_unassigned_block(self):
        """Test that no driver is found for a block with no assignments."""
        driver = _find_current_driver("99", self.assignments_by_block, self.now_ts)
        self.assertIsNone(driver)

    def test_boundary_start_time(self):
        """Test finding driver at exact shift start time."""
        start_time = datetime(2025, 12, 7, 6, 0, tzinfo=self.tz)
        start_ts = int(start_time.timestamp() * 1000)
        driver = _find_current_driver("01", self.assignments_by_block, start_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "John Doe")

    def test_boundary_end_time(self):
        """Test that driver is NOT found at exact shift end time (exclusive)."""
        end_time = datetime(2025, 12, 7, 12, 0, tzinfo=self.tz)
        end_ts = int(end_time.timestamp() * 1000)
        driver = _find_current_driver("01", self.assignments_by_block, end_ts)
        # Should get PM driver or None, not AM driver
        if driver:
            self.assertNotEqual(driver["name"], "John Doe")


class TestRawBlockMapping(unittest.TestCase):
    """Test the complete flow of mapping raw blocks to drivers."""

    def test_raw_block_scenario(self):
        """
        Test scenario where TransLoc returns raw block [01] (not interlined),
        and we need to find which driver is currently active.
        """
        # Scenario: It's 10:30 AM
        # - Block 01 AM driver: John Doe (6a-12p)
        # Vehicle is assigned to block [01], we should find John Doe

        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 10, 30, tzinfo=tz)
        now_ts = int(now.timestamp() * 1000)

        am_start = datetime(2025, 12, 7, 6, 0, tzinfo=tz)
        am_end = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        assignments_by_block = {
            "01": {
                "am": [{
                    "name": "John Doe",
                    "start_ts": int(am_start.timestamp() * 1000),
                    "end_ts": int(am_end.timestamp() * 1000),
                    "start_label": "6a",
                    "end_label": "12p",
                    "color_id": "0"
                }]
            }
        }

        # Extract block number from raw block
        block_name = "[01]"
        block_numbers = _split_interlined_blocks(block_name)
        self.assertEqual(block_numbers, ["01"])

        # Find driver for the block
        driver = _find_current_driver(block_numbers[0], assignments_by_block, now_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "John Doe")

    def test_different_raw_block_scenario(self):
        """
        Test scenario where TransLoc returns raw block [04] (not interlined).
        """
        # Scenario: It's 10:30 AM
        # - Block 04 driver: Bob Johnson (6a-6p, any period)
        # Vehicle is assigned to block [04], we should find Bob Johnson

        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 10, 30, tzinfo=tz)
        now_ts = int(now.timestamp() * 1000)

        all_day_start = datetime(2025, 12, 7, 6, 0, tzinfo=tz)
        all_day_end = datetime(2025, 12, 7, 18, 0, tzinfo=tz)

        assignments_by_block = {
            "04": {
                "any": [{
                    "name": "Bob Johnson",
                    "start_ts": int(all_day_start.timestamp() * 1000),
                    "end_ts": int(all_day_end.timestamp() * 1000),
                    "start_label": "6a",
                    "end_label": "6p",
                    "color_id": "2"
                }]
            }
        }

        # Extract block number from raw block
        block_name = "[04]"
        block_numbers = _split_interlined_blocks(block_name)
        self.assertEqual(block_numbers, ["04"])

        # Find driver for the block
        driver = _find_current_driver(block_numbers[0], assignments_by_block, now_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Bob Johnson")


class TestExtractBlockFromPositionName(unittest.TestCase):
    """Test the _extract_block_from_position_name function."""

    def test_regular_block(self):
        """Test regular block extraction."""
        block, period = _extract_block_from_position_name("[01]")
        self.assertEqual(block, "01")
        self.assertEqual(period, "")

    def test_block_with_am(self):
        """Test block with AM designation."""
        block, period = _extract_block_from_position_name("[16 AM]")
        self.assertEqual(block, "16")
        self.assertEqual(period, "am")

    def test_block_with_pm(self):
        """Test block with PM designation."""
        block, period = _extract_block_from_position_name("[20 PM]")
        self.assertEqual(block, "20")
        self.assertEqual(period, "pm")

    def test_ondemand_driver(self):
        """Test OnDemand Driver position."""
        block, period = _extract_block_from_position_name("OnDemand Driver")
        self.assertEqual(block, "OnDemand Driver")
        self.assertEqual(period, "any")

    def test_ondemand_eb(self):
        """Test OnDemand EB position."""
        block, period = _extract_block_from_position_name("OnDemand EB")
        self.assertEqual(block, "OnDemand EB")
        self.assertEqual(period, "any")

    def test_ondemand_case_insensitive(self):
        """Test that OnDemand matching is case-insensitive."""
        block, period = _extract_block_from_position_name("ondemand driver")
        self.assertEqual(block, "OnDemand Driver")
        self.assertEqual(period, "any")

        block, period = _extract_block_from_position_name("ONDEMAND EB")
        self.assertEqual(block, "OnDemand EB")
        self.assertEqual(period, "any")

    def test_none_input(self):
        """Test None input."""
        block, period = _extract_block_from_position_name(None)
        self.assertIsNone(block)
        self.assertEqual(period, "")


class TestNormalizeDriverName(unittest.TestCase):
    """Test the _normalize_driver_name function."""

    def test_basic_name(self):
        """Test basic name normalization."""
        result = _normalize_driver_name("John Doe")
        self.assertEqual(result, "john doe")

    def test_extra_whitespace(self):
        """Test name with extra whitespace."""
        result = _normalize_driver_name("John   Doe")
        self.assertEqual(result, "john doe")

    def test_leading_trailing_whitespace(self):
        """Test name with leading/trailing whitespace."""
        result = _normalize_driver_name("  John Doe  ")
        self.assertEqual(result, "john doe")

    def test_mixed_case(self):
        """Test mixed case name."""
        result = _normalize_driver_name("JoHn DoE")
        self.assertEqual(result, "john doe")

    def test_empty_string(self):
        """Test empty string."""
        result = _normalize_driver_name("")
        self.assertEqual(result, "")

    def test_none_input(self):
        """Test None input."""
        result = _normalize_driver_name(None)
        self.assertEqual(result, "")


class TestFindOndemandDriverByName(unittest.TestCase):
    """Test the _find_ondemand_driver_by_name function."""

    def setUp(self):
        """Set up test data."""
        self.tz = ZoneInfo("America/New_York")
        self.now = datetime(2025, 12, 7, 10, 30, tzinfo=self.tz)  # 10:30 AM
        self.now_ts = int(self.now.timestamp() * 1000)

        # Create sample W2W assignments structure
        shift_start = datetime(2025, 12, 7, 8, 0, tzinfo=self.tz)  # 8:00 AM
        shift_end = datetime(2025, 12, 7, 16, 0, tzinfo=self.tz)   # 4:00 PM

        self.assignments_by_block = {
            "OnDemand Driver": {
                "any": [
                    {
                        "name": "Alice Johnson",
                        "start_ts": int(shift_start.timestamp() * 1000),
                        "end_ts": int(shift_end.timestamp() * 1000),
                        "start_label": "8a",
                        "end_label": "4p",
                        "color_id": "0"
                    }
                ]
            },
            "OnDemand EB": {
                "any": [
                    {
                        "name": "Bob Smith",
                        "start_ts": int(shift_start.timestamp() * 1000),
                        "end_ts": int(shift_end.timestamp() * 1000),
                        "start_label": "8a",
                        "end_label": "4p",
                        "color_id": "1"
                    }
                ]
            }
        }

    def test_find_ondemand_driver(self):
        """Test finding an OnDemand Driver."""
        driver = _find_ondemand_driver_by_name(
            "Alice Johnson", self.assignments_by_block, self.now_ts
        )
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Alice Johnson")
        self.assertEqual(driver["block"], "OnDemand Driver")

    def test_find_ondemand_eb_driver(self):
        """Test finding an OnDemand EB driver."""
        driver = _find_ondemand_driver_by_name(
            "Bob Smith", self.assignments_by_block, self.now_ts
        )
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Bob Smith")
        self.assertEqual(driver["block"], "OnDemand EB")

    def test_case_insensitive_match(self):
        """Test that driver name matching is case-insensitive."""
        driver = _find_ondemand_driver_by_name(
            "alice johnson", self.assignments_by_block, self.now_ts
        )
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Alice Johnson")

    def test_whitespace_normalization(self):
        """Test that extra whitespace is normalized."""
        driver = _find_ondemand_driver_by_name(
            "Alice   Johnson", self.assignments_by_block, self.now_ts
        )
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Alice Johnson")

    def test_no_match(self):
        """Test when no driver matches."""
        driver = _find_ondemand_driver_by_name(
            "Charlie Brown", self.assignments_by_block, self.now_ts
        )
        self.assertIsNone(driver)

    def test_outside_shift_time(self):
        """Test when driver is not within shift time."""
        late_night = datetime(2025, 12, 7, 22, 0, tzinfo=self.tz)  # 10:00 PM
        late_ts = int(late_night.timestamp() * 1000)
        driver = _find_ondemand_driver_by_name(
            "Alice Johnson", self.assignments_by_block, late_ts
        )
        self.assertIsNone(driver)

    def test_empty_driver_name(self):
        """Test with empty driver name."""
        driver = _find_ondemand_driver_by_name(
            "", self.assignments_by_block, self.now_ts
        )
        self.assertIsNone(driver)

    def test_none_driver_name(self):
        """Test with None driver name."""
        driver = _find_ondemand_driver_by_name(
            None, self.assignments_by_block, self.now_ts
        )
        self.assertIsNone(driver)


if __name__ == "__main__":
    unittest.main()
