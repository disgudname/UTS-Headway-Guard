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
    _find_current_drivers,
    _normalize_driver_name,
    _find_ondemand_driver_by_name,
    _extract_block_from_position_name,
    _parse_dotnet_date,
    _parse_block_time_today,
    _build_block_mapping_with_times,
    _select_current_or_next_block,
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
                        "color_id": "0",
                        "position_name": "[01]"
                    }
                ],
                "pm": [
                    {
                        "name": "Jane Smith",
                        "start_ts": int(pm_start.timestamp() * 1000),
                        "end_ts": int(pm_end.timestamp() * 1000),
                        "start_label": "1p",
                        "end_label": "6p",
                        "color_id": "1",
                        "position_name": "[01]"
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
                        "color_id": "2",
                        "position_name": "[04]"
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

    def test_overlapping_shifts(self):
        """Test handling of overlapping driver shifts (driver swap scenario)."""
        # Scenario: Two drivers overlap 9:45 AM - 10:00 AM
        # Outgoing driver: 6:00 AM - 10:00 AM
        # Incoming driver: 9:45 AM - 6:00 PM
        # At 9:50 AM, both should be active

        overlap_time = datetime(2025, 12, 7, 9, 50, tzinfo=self.tz)  # 9:50 AM
        overlap_ts = int(overlap_time.timestamp() * 1000)

        outgoing_start = datetime(2025, 12, 7, 6, 0, tzinfo=self.tz)
        outgoing_end = datetime(2025, 12, 7, 10, 0, tzinfo=self.tz)
        incoming_start = datetime(2025, 12, 7, 9, 45, tzinfo=self.tz)
        incoming_end = datetime(2025, 12, 7, 18, 0, tzinfo=self.tz)

        assignments_with_overlap = {
            "05": {
                "am": [
                    {
                        "name": "Outgoing Driver",
                        "start_ts": int(outgoing_start.timestamp() * 1000),
                        "end_ts": int(outgoing_end.timestamp() * 1000),
                        "start_label": "6a",
                        "end_label": "10a",
                        "color_id": "0",
                        "position_name": "[05]"
                    },
                    {
                        "name": "Incoming Driver",
                        "start_ts": int(incoming_start.timestamp() * 1000),
                        "end_ts": int(incoming_end.timestamp() * 1000),
                        "start_label": "9:45a",
                        "end_label": "6p",
                        "color_id": "1",
                        "position_name": "[05]"
                    }
                ]
            }
        }

        # Test the new plural function - should return both drivers
        drivers = _find_current_drivers("05", assignments_with_overlap, overlap_ts)
        self.assertEqual(len(drivers), 2)

        # Verify they're sorted by start time (outgoing first, incoming second)
        self.assertEqual(drivers[0]["name"], "Outgoing Driver")
        self.assertEqual(drivers[1]["name"], "Incoming Driver")

        # Verify shift times are preserved
        self.assertEqual(drivers[0]["start_ts"], int(outgoing_start.timestamp() * 1000))
        self.assertEqual(drivers[0]["end_ts"], int(outgoing_end.timestamp() * 1000))
        self.assertEqual(drivers[1]["start_ts"], int(incoming_start.timestamp() * 1000))
        self.assertEqual(drivers[1]["end_ts"], int(incoming_end.timestamp() * 1000))

        # Test backward compatibility - _find_current_driver should return first driver
        driver = _find_current_driver("05", assignments_with_overlap, overlap_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "Outgoing Driver")

    def test_no_overlap_sequential_shifts(self):
        """Test non-overlapping sequential shifts."""
        # Scenario: Clean handoff at exactly 12:00 PM
        # Morning driver: 6:00 AM - 12:00 PM
        # Afternoon driver: 12:00 PM - 6:00 PM
        # At 11:59 AM, only morning driver
        # At 12:00 PM, only afternoon driver

        just_before_noon = datetime(2025, 12, 7, 11, 59, tzinfo=self.tz)
        before_ts = int(just_before_noon.timestamp() * 1000)

        # At 11:59, should get morning driver
        driver = _find_current_driver("01", self.assignments_by_block, before_ts)
        self.assertIsNotNone(driver)
        self.assertEqual(driver["name"], "John Doe")


class TestInterlinedBlockDriverMatching(unittest.TestCase):
    """Test driver matching for interlined blocks like [05]/[03]."""

    def test_interlined_block_matches_both_drivers(self):
        """
        Test that interlined blocks like [05]/[03] match drivers from BOTH blocks.

        Scenario: TransLoc returns "[05]/[03]" (interlined)
        W2W has separate drivers for block 05 and block 03
        We should match drivers from BOTH blocks.
        """
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 10, 30, tzinfo=tz)  # 10:30 AM
        now_ts = int(now.timestamp() * 1000)

        am_start = datetime(2025, 12, 7, 6, 0, tzinfo=tz)
        am_end = datetime(2025, 12, 7, 14, 0, tzinfo=tz)

        # W2W has separate assignments for blocks 05 and 03
        assignments_by_block = {
            "05": {
                "am": [{
                    "name": "Driver Five",
                    "start_ts": int(am_start.timestamp() * 1000),
                    "end_ts": int(am_end.timestamp() * 1000),
                    "start_label": "6a",
                    "end_label": "2p",
                    "color_id": "0",
                    "position_name": "[05]"
                }]
            },
            "03": {
                "am": [{
                    "name": "Driver Three",
                    "start_ts": int(am_start.timestamp() * 1000),
                    "end_ts": int(am_end.timestamp() * 1000),
                    "start_label": "6a",
                    "end_label": "2p",
                    "color_id": "1",
                    "position_name": "[03]"
                }]
            }
        }

        # TransLoc returns interlined block
        block_name = "[05]/[03]"
        block_numbers = _split_interlined_blocks(block_name)
        self.assertEqual(block_numbers, ["05", "03"])

        # Collect drivers from ALL blocks (simulating the fixed logic)
        current_drivers = []
        for block_number in block_numbers:
            block_drivers = _find_current_drivers(block_number, assignments_by_block, now_ts)
            current_drivers.extend(block_drivers)

        # Should have drivers from BOTH blocks
        self.assertEqual(len(current_drivers), 2)
        driver_names = [d["name"] for d in current_drivers]
        self.assertIn("Driver Five", driver_names)
        self.assertIn("Driver Three", driver_names)

    def test_interlined_block_with_one_empty(self):
        """
        Test interlined block where only one block has a driver.

        Scenario: TransLoc returns "[05]/[03]"
        W2W only has a driver for block 05 (block 03 shift hasn't started)
        """
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 10, 30, tzinfo=tz)  # 10:30 AM
        now_ts = int(now.timestamp() * 1000)

        am_start = datetime(2025, 12, 7, 6, 0, tzinfo=tz)
        am_end = datetime(2025, 12, 7, 14, 0, tzinfo=tz)

        # Only block 05 has a driver (block 03 has no assignment or shift hasn't started)
        assignments_by_block = {
            "05": {
                "am": [{
                    "name": "Driver Five",
                    "start_ts": int(am_start.timestamp() * 1000),
                    "end_ts": int(am_end.timestamp() * 1000),
                    "start_label": "6a",
                    "end_label": "2p",
                    "color_id": "0",
                    "position_name": "[05]"
                }]
            }
            # Block 03 has no entry
        }

        # TransLoc returns interlined block
        block_name = "[05]/[03]"
        block_numbers = _split_interlined_blocks(block_name)

        # Collect drivers from ALL blocks
        current_drivers = []
        for block_number in block_numbers:
            block_drivers = _find_current_drivers(block_number, assignments_by_block, now_ts)
            current_drivers.extend(block_drivers)

        # Should only have driver from block 05
        self.assertEqual(len(current_drivers), 1)
        self.assertEqual(current_drivers[0]["name"], "Driver Five")


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
                    "color_id": "0",
                    "position_name": "[01]"
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
                    "color_id": "2",
                    "position_name": "[04]"
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
                        "color_id": "0",
                        "position_name": "OnDemand Driver"
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
                        "color_id": "1",
                        "position_name": "OnDemand EB"
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


class TestParseDotnetDate(unittest.TestCase):
    """Test the _parse_dotnet_date function."""

    def test_valid_date(self):
        """Test parsing a valid .NET JSON date."""
        result = _parse_dotnet_date("/Date(1757019600000)/")
        self.assertEqual(result, 1757019600000)

    def test_another_valid_date(self):
        """Test parsing another valid .NET JSON date."""
        result = _parse_dotnet_date("/Date(1757030700000)/")
        self.assertEqual(result, 1757030700000)

    def test_empty_string(self):
        """Test empty string returns None."""
        result = _parse_dotnet_date("")
        self.assertIsNone(result)

    def test_none_input(self):
        """Test None input returns None."""
        result = _parse_dotnet_date(None)
        self.assertIsNone(result)

    def test_invalid_format(self):
        """Test invalid format returns None."""
        result = _parse_dotnet_date("not a date")
        self.assertIsNone(result)


class TestParseBlockTimeToday(unittest.TestCase):
    """Test the _parse_block_time_today function."""

    def test_parse_morning_time(self):
        """Test parsing morning time like 06:00 AM."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("06:00 AM", reference_date)
        self.assertIsNotNone(result)

        # Convert back to datetime to verify
        result_dt = datetime.fromtimestamp(result / 1000, tz)
        self.assertEqual(result_dt.date(), reference_date.date())
        self.assertEqual(result_dt.hour, 6)
        self.assertEqual(result_dt.minute, 0)

    def test_parse_afternoon_time(self):
        """Test parsing afternoon time like 03:00 PM."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("03:00 PM", reference_date)
        self.assertIsNotNone(result)

        # Convert back to datetime to verify
        result_dt = datetime.fromtimestamp(result / 1000, tz)
        self.assertEqual(result_dt.date(), reference_date.date())
        self.assertEqual(result_dt.hour, 15)
        self.assertEqual(result_dt.minute, 0)

    def test_parse_time_with_minutes(self):
        """Test parsing time with non-zero minutes like 06:05 PM."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("06:05 PM", reference_date)
        self.assertIsNotNone(result)

        # Convert back to datetime to verify
        result_dt = datetime.fromtimestamp(result / 1000, tz)
        self.assertEqual(result_dt.date(), reference_date.date())
        self.assertEqual(result_dt.hour, 18)
        self.assertEqual(result_dt.minute, 5)

    def test_parse_midnight(self):
        """Test parsing midnight (12:00 AM)."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("12:00 AM", reference_date)
        self.assertIsNotNone(result)

        # Convert back to datetime to verify
        result_dt = datetime.fromtimestamp(result / 1000, tz)
        self.assertEqual(result_dt.date(), reference_date.date())
        self.assertEqual(result_dt.hour, 0)
        self.assertEqual(result_dt.minute, 0)

    def test_parse_noon(self):
        """Test parsing noon (12:00 PM)."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("12:00 PM", reference_date)
        self.assertIsNotNone(result)

        # Convert back to datetime to verify
        result_dt = datetime.fromtimestamp(result / 1000, tz)
        self.assertEqual(result_dt.date(), reference_date.date())
        self.assertEqual(result_dt.hour, 12)
        self.assertEqual(result_dt.minute, 0)

    def test_empty_string(self):
        """Test empty string returns None."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("", reference_date)
        self.assertIsNone(result)

    def test_none_input(self):
        """Test None input returns None."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today(None, reference_date)
        self.assertIsNone(result)

    def test_invalid_format(self):
        """Test invalid format returns None."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        result = _parse_block_time_today("not a time", reference_date)
        self.assertIsNone(result)


class TestBuildBlockMappingWithTimes(unittest.TestCase):
    """Test the _build_block_mapping_with_times function."""

    def test_single_vehicle_single_block(self):
        """Test a single vehicle with a single block."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)  # Noon on Dec 7

        block_groups = [
            {
                "BlockGroupId": "[01]",
                "VehicleId": 123,
                "Blocks": [
                    {
                        "BlockStartTime": "06:00 AM",
                        "BlockEndTime": "02:00 PM",
                        "Trips": [
                            {
                                "VehicleID": 123
                            }
                        ]
                    }
                ]
            }
        ]
        result = _build_block_mapping_with_times(block_groups, reference_date)

        self.assertIn("123", result)
        self.assertEqual(len(result["123"]), 1)
        block_name, start_ts, end_ts = result["123"][0]
        self.assertEqual(block_name, "[01]")

        # Verify the times are on Dec 7, 2025
        start_dt = datetime.fromtimestamp(start_ts / 1000, tz)
        end_dt = datetime.fromtimestamp(end_ts / 1000, tz)
        self.assertEqual(start_dt.date(), reference_date.date())
        self.assertEqual(start_dt.hour, 6)
        self.assertEqual(start_dt.minute, 0)
        self.assertEqual(end_dt.hour, 14)
        self.assertEqual(end_dt.minute, 0)

    def test_single_vehicle_multiple_blocks(self):
        """Test a single vehicle with multiple blocks (morning and afternoon shifts)."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        block_groups = [
            {
                "BlockGroupId": "[21]/[16] AM",
                "VehicleId": 30,
                "Blocks": [
                    {
                        "BlockStartTime": "05:00 AM",
                        "BlockEndTime": "10:00 AM",
                        "Trips": [
                            {
                                "VehicleID": 30
                            }
                        ]
                    }
                ]
            },
            {
                "BlockGroupId": "[16] PM",
                "VehicleId": 30,
                "Blocks": [
                    {
                        "BlockStartTime": "03:00 PM",
                        "BlockEndTime": "06:05 PM",
                        "Trips": [
                            {
                                "VehicleID": 30
                            }
                        ]
                    }
                ]
            }
        ]
        result = _build_block_mapping_with_times(block_groups, reference_date)

        self.assertIn("30", result)
        self.assertEqual(len(result["30"]), 2)

        # Should have both morning and afternoon blocks
        block_names = [block[0] for block in result["30"]]
        self.assertIn("[21]/[16] AM", block_names)
        self.assertIn("[16] PM", block_names)

    def test_interlined_block(self):
        """Test interlined block handling."""
        tz = ZoneInfo("America/New_York")
        reference_date = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        block_groups = [
            {
                "BlockGroupId": "[05]/[03]",
                "VehicleId": 456,
                "Blocks": [
                    {
                        "BlockStartTime": "03:00 PM",
                        "BlockEndTime": "06:05 PM",
                        "Trips": [
                            {
                                "VehicleID": 456
                            }
                        ]
                    }
                ]
            }
        ]
        result = _build_block_mapping_with_times(block_groups, reference_date)

        self.assertIn("456", result)
        self.assertEqual(len(result["456"]), 1)
        block_name, start_ts, end_ts = result["456"][0]
        self.assertEqual(block_name, "[05]/[03]")


class TestSelectCurrentOrNextBlock(unittest.TestCase):
    """Test the _select_current_or_next_block function."""

    def test_current_block(self):
        """Test selecting current active block."""
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 10, 0, tzinfo=tz)  # 10:00 AM
        now_ts = int(now.timestamp() * 1000)

        # Morning block: 5:00 AM - 10:00 AM
        morning_start = datetime(2025, 12, 7, 5, 0, tzinfo=tz)
        morning_end = datetime(2025, 12, 7, 12, 0, tzinfo=tz)

        # Afternoon block: 3:00 PM - 6:00 PM
        afternoon_start = datetime(2025, 12, 7, 15, 0, tzinfo=tz)
        afternoon_end = datetime(2025, 12, 7, 18, 0, tzinfo=tz)

        blocks_with_times = [
            ("[21]/[16] AM", int(morning_start.timestamp() * 1000), int(morning_end.timestamp() * 1000)),
            ("[16] PM", int(afternoon_start.timestamp() * 1000), int(afternoon_end.timestamp() * 1000))
        ]

        result = _select_current_or_next_block(blocks_with_times, now_ts)
        self.assertEqual(result, "[21]/[16] AM")

    def test_next_block(self):
        """Test that no block is returned when between shifts (no current block)."""
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 12, 30, tzinfo=tz)  # 12:30 PM (between shifts)
        now_ts = int(now.timestamp() * 1000)

        # Morning block: 5:00 AM - 10:00 AM (past)
        morning_start = datetime(2025, 12, 7, 5, 0, tzinfo=tz)
        morning_end = datetime(2025, 12, 7, 10, 0, tzinfo=tz)

        # Afternoon block: 3:00 PM - 6:00 PM (future)
        afternoon_start = datetime(2025, 12, 7, 15, 0, tzinfo=tz)
        afternoon_end = datetime(2025, 12, 7, 18, 0, tzinfo=tz)

        blocks_with_times = [
            ("[21]/[16] AM", int(morning_start.timestamp() * 1000), int(morning_end.timestamp() * 1000)),
            ("[16] PM", int(afternoon_start.timestamp() * 1000), int(afternoon_end.timestamp() * 1000))
        ]

        result = _select_current_or_next_block(blocks_with_times, now_ts)
        # Should return None since no block is currently active (prevents duplicate assignments)
        self.assertIsNone(result)

    def test_no_current_or_future_blocks(self):
        """Test when all blocks are in the past."""
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 20, 0, tzinfo=tz)  # 8:00 PM (after all blocks)
        now_ts = int(now.timestamp() * 1000)

        # Morning block: 5:00 AM - 10:00 AM (past)
        morning_start = datetime(2025, 12, 7, 5, 0, tzinfo=tz)
        morning_end = datetime(2025, 12, 7, 10, 0, tzinfo=tz)

        # Afternoon block: 3:00 PM - 6:00 PM (past)
        afternoon_start = datetime(2025, 12, 7, 15, 0, tzinfo=tz)
        afternoon_end = datetime(2025, 12, 7, 18, 0, tzinfo=tz)

        blocks_with_times = [
            ("[21]/[16] AM", int(morning_start.timestamp() * 1000), int(morning_end.timestamp() * 1000)),
            ("[16] PM", int(afternoon_start.timestamp() * 1000), int(afternoon_end.timestamp() * 1000))
        ]

        result = _select_current_or_next_block(blocks_with_times, now_ts)
        self.assertIsNone(result)

    def test_early_morning_before_first_block(self):
        """Test that no block is returned before first block starts."""
        tz = ZoneInfo("America/New_York")
        now = datetime(2025, 12, 7, 3, 0, tzinfo=tz)  # 3:00 AM (before morning block)
        now_ts = int(now.timestamp() * 1000)

        # Morning block: 5:00 AM - 10:00 AM
        morning_start = datetime(2025, 12, 7, 5, 0, tzinfo=tz)
        morning_end = datetime(2025, 12, 7, 10, 0, tzinfo=tz)

        blocks_with_times = [
            ("[21]/[16] AM", int(morning_start.timestamp() * 1000), int(morning_end.timestamp() * 1000))
        ]

        result = _select_current_or_next_block(blocks_with_times, now_ts)
        # Should return None since block hasn't started yet (prevents duplicate assignments)
        self.assertIsNone(result)

    def test_empty_blocks_list(self):
        """Test with empty blocks list."""
        now_ts = int(datetime.now().timestamp() * 1000)
        result = _select_current_or_next_block([], now_ts)
        self.assertIsNone(result)

    def test_blocks_without_times(self):
        """Test blocks without time information fall back to first block."""
        now_ts = int(datetime.now().timestamp() * 1000)
        blocks_with_times = [
            ("[01]", None, None),
            ("[02]", None, None)
        ]
        result = _select_current_or_next_block(blocks_with_times, now_ts)
        # Should fall back to first block when no time info available
        self.assertEqual(result, "[01]")


if __name__ == "__main__":
    unittest.main()
