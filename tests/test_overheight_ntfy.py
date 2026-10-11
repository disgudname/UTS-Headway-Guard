"""
Test which over-height buses get a phone push when they reach the bridge.
"""

import unittest

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app


def bus(name, lat=app.BRIDGE_LAT, lon=app.BRIDGE_LON):
    return {"Name": name, "Latitude": lat, "Longitude": lon}


class OverheightNotifyTest(unittest.TestCase):
    def setUp(self):
        app._overheight_last_inside.clear()
        self.tall = str(app.OVERHEIGHT_BUSES[0])

    def tearDown(self):
        app._overheight_last_inside.clear()

    def test_tall_bus_at_bridge_is_pushed(self):
        out = app.overheight_buses_to_notify([bus(self.tall)], now=1000.0)
        self.assertEqual([b for b, _ in out], [self.tall])

    def test_other_bus_at_bridge_is_not(self):
        self.assertEqual(app.overheight_buses_to_notify([bus("12132")], now=1000.0), [])

    def test_tall_bus_far_away_is_not(self):
        far = bus(self.tall, lat=app.BRIDGE_LAT + 0.01)
        self.assertEqual(app.overheight_buses_to_notify([far], now=1000.0), [])

    def test_pushed_once_while_it_stays(self):
        self.assertEqual(len(app.overheight_buses_to_notify([bus(self.tall)], now=1000.0)), 1)
        for t in range(1010, 3000, 10):  # polled every 10 s, well past the re-arm time
            self.assertEqual(app.overheight_buses_to_notify([bus(self.tall)], now=t), [])

    def test_pushed_again_after_leaving_long_enough(self):
        app.overheight_buses_to_notify([bus(self.tall)], now=1000.0)
        far = bus(self.tall, lat=app.BRIDGE_LAT + 0.01)
        app.overheight_buses_to_notify([far], now=1100.0)
        later = 1000.0 + app.OVERHEIGHT_NTFY_REARM_S + 1
        self.assertEqual(len(app.overheight_buses_to_notify([bus(self.tall)], now=later)), 1)

    def test_bad_position_is_skipped(self):
        self.assertEqual(
            app.overheight_buses_to_notify([{"Name": self.tall, "Latitude": None}], now=1000.0), []
        )


if __name__ == "__main__":
    unittest.main()
