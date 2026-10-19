"""
Test when a TransLoc outage pushes "down" and "back" to the phone.
"""

import unittest

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app


class TranslocOutageWatchTest(unittest.TestCase):
    def test_one_or_two_failures_stay_quiet(self):
        w = app.TranslocOutageWatch(threshold=3)
        self.assertFalse(w.failed(100.0))
        self.assertFalse(w.failed(105.0))
        self.assertIsNone(w.succeeded())

    def test_down_once_then_back_with_start_time(self):
        w = app.TranslocOutageWatch(threshold=3)
        self.assertFalse(w.failed(100.0))
        self.assertFalse(w.failed(105.0))
        self.assertTrue(w.failed(110.0))
        self.assertFalse(w.failed(115.0))  # pushed once per outage
        self.assertEqual(w.succeeded(), (100.0, 4))
        self.assertIsNone(w.succeeded())

    def test_rearms_after_recovery(self):
        w = app.TranslocOutageWatch(threshold=2)
        w.failed(1.0)
        self.assertTrue(w.failed(2.0))
        w.succeeded()
        self.assertFalse(w.failed(10.0))
        self.assertTrue(w.failed(11.0))


if __name__ == "__main__":
    unittest.main()
