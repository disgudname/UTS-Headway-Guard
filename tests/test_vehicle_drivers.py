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

from app import _split_interlined_blocks, _find_current_driver


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


if __name__ == "__main__":
    unittest.main()
