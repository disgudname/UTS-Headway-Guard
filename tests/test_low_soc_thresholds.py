"""
Test when a bus's state of charge counts as crossing a low-battery threshold.
"""

import unittest
from unittest import mock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app


class LowSocThresholdTest(unittest.TestCase):
    def setUp(self):
        app._notified_soc_thresholds.clear()
        patcher = mock.patch.object(app, "_save_low_soc_state")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_35_fires_once_per_drop(self):
        self.assertIsNone(app._check_soc_threshold_crossed("25231", 40.0))
        self.assertEqual(app._check_soc_threshold_crossed("25231", 34.8), 35)
        self.assertIsNone(app._check_soc_threshold_crossed("25231", 34.5))

    def test_wobble_around_35_does_not_refire(self):
        app._check_soc_threshold_crossed("25231", 34.9)
        self.assertIsNone(app._check_soc_threshold_crossed("25231", 35.4))
        self.assertIsNone(app._check_soc_threshold_crossed("25231", 34.9))

    def test_rearms_after_recharge(self):
        app._check_soc_threshold_crossed("25231", 34.0)
        self.assertIsNone(app._check_soc_threshold_crossed("25231", 60.0))
        self.assertEqual(app._check_soc_threshold_crossed("25231", 34.0), 35)


if __name__ == "__main__":
    unittest.main()
