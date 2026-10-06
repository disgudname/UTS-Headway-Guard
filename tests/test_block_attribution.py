import unittest
from datetime import datetime

from block_attribution import Cover, block_for, blocks_by_family, family_of, place


class TestPlace(unittest.TestCase):
    def setUp(self):
        self.when = datetime(2026, 8, 26, 10, 0)
        self.sec = 10 * 3600
        self.pieces = {"09": [(7 * 3600, 18 * 3600)], "11": [(7 * 3600, 18 * 3600)], "12": [(16 * 3600, 22 * 3600)]}

    def test_one_block_is_left_alone(self):
        self.assertEqual(place(["09"], self.when, self.sec, {}, Cover(), None), (["09"], False))

    def test_only_blocks_scheduled_out(self):
        numbers, by_fit = place(["09", "12"], self.when, self.sec, self.pieces, Cover(), None)
        self.assertEqual((numbers, by_fit), (["09"], False))

    def test_block_another_bus_is_covering_is_dropped(self):
        cover = Cover()
        cover.add("09", datetime(2026, 8, 26, 10, 20))
        cover.sort()
        self.assertEqual(place(["09", "11"], self.when, self.sec, self.pieces, cover, None), (["11"], False))

    def test_cover_goes_before_the_timestop_fit(self):
        cover = Cover()
        cover.add("09", datetime(2026, 8, 26, 9, 50))
        cover.sort()
        self.assertEqual(place(["09", "11"], self.when, self.sec, self.pieces, cover, "09"), (["11"], False))

    def test_timestop_fit_settles_a_tie(self):
        self.assertEqual(place(["09", "11"], self.when, self.sec, self.pieces, Cover(), "11"), (["11"], True))

    def test_still_tied_is_shared(self):
        self.assertEqual(place(["09", "11"], self.when, self.sec, self.pieces, Cover(), None), (["09", "11"], False))

    def test_nothing_scheduled_or_everything_covered_keeps_all(self):
        cover = Cover()
        for block in ("09", "11"):
            cover.add(block, self.when)
        cover.sort()
        self.assertEqual(place(["09", "11"], self.when, 2 * 3600, self.pieces, cover, None), (["09", "11"], False))


class TestNames(unittest.TestCase):
    def test_lot_shuttle_is_not_the_purple_line(self):
        self.assertEqual(family_of("Purple Line"), "Purple")
        self.assertIsNone(family_of("Purple Lots Shuttle"))

    def test_interlined_group(self):
        self.assertEqual(block_for(["17", "10"], "Gold"), "10")
        self.assertEqual(blocks_by_family(["[17]/[10]", "[09]"]), {"Purple": ["17"], "Gold": ["09", "10"]})


if __name__ == "__main__":
    unittest.main()
