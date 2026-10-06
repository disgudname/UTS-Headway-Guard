import unittest
from datetime import date

import boxscore


def row(when, bus, route, entries=1, stop="Somewhere", route_id=67, rsid=1):
    return {"ClientTime": when, "Vehicle": bus, "Route": route, "Entries": entries, "RouteStop": stop,
            "RouteID": route_id, "RouteStopID": rsid}


def group(group_id, *trips):
    return {"BlockGroupId": group_id, "Blocks": [{"Trips": [
        {"RouteName": name, "StartTime": start, "EndTime": end} for name, start, end in trips]}]}


DAY = date(2026, 9, 30)  # a Wednesday
NEXT = date(2026, 10, 1)
GROUPS = {
    DAY: [group("[09]", ("Gold Line", "PT5H", "PT22H")), group("[11]", ("Gold Line", "PT5H", "PT22H")),
          group("[05]/[03]", ("Orange Line", "PT7H30M", "PT21H58M"), ("Night Pilot", "PT21H59M", "PT23H59M")),
          group("[03]", ("Night Pilot", "PT0H", "PT2H"))],
    NEXT: [group("[03]", ("Night Pilot", "PT0H", "PT2H"))],
}


class BoxScoreTests(unittest.TestCase):
    def test_family_ignores_lot_shuttles(self):
        self.assertEqual(boxscore.family_of("Purple Line"), "Purple")
        self.assertIsNone(boxscore.family_of("Purple Lots Shuttle"))
        self.assertEqual(boxscore.family_of("Green Loop DETOUR"), "Green")

    def test_night_pilot_after_midnight_stays_with_the_evening_before(self):
        pieces = boxscore.schedule_pieces(DAY, GROUPS)
        self.assertEqual(pieces["03"], [(21 * 3600 + 59 * 60, 23 * 3600 + 59 * 60), (86400, 86400 + 7200)])
        bus_days = {DAY: {"100": {"blocks": ["[05]/[03]"], "miles": 120}}, NEXT: {"100": {"blocks": ["[03]"], "miles": 20}}}
        rows = [row("9/30/2026 11:30:00 PM", "100", "Night Pilot", 4), row("10/1/2026 1:15:00 AM", "100", "Night Pilot", 3),
                row("10/1/2026 9:00:00 AM", "100", "Night Pilot", 50),   # the next service day
                row("9/30/2026 2:00:00 AM", "100", "Night Pilot", 60)]   # the one before
        box = boxscore.build(DAY, rows, bus_days, GROUPS)
        night = next(r for r in box["routes"] if r["family"] == "Night Pilot")
        self.assertEqual(night["riders"], 7)
        self.assertEqual(night["innings"][-1], 7)
        self.assertEqual(next(b for b in night["blocks"] if b["block"] == "03")["riders"], 7)

    def test_bus_listed_on_two_blocks_is_not_counted_on_both(self):
        # Bus 200 touched [09] and [11]; bus 300 is on [09] alone and is carrying riders, so 200's belong to [11].
        bus_days = {DAY: {"200": {"blocks": ["[09]", "[11]"], "miles": 150}, "300": {"blocks": ["[09]"], "miles": 150}}}
        rows = []
        for hour in range(8, 12):
            rows.append(row(f"9/30/2026 {hour}:10:00 AM", "300", "Gold Line", 10))
            rows.append(row(f"9/30/2026 {hour}:20:00 AM", "200", "Gold Line", 20))
        box = boxscore.build(DAY, rows, bus_days, GROUPS)
        gold = {b["block"]: b for b in next(r for r in box["routes"] if r["family"] == "Gold")["blocks"]}
        self.assertEqual(gold["09"]["riders"], 40)
        self.assertEqual(gold["11"]["riders"], 80)
        self.assertEqual(gold["11"]["buses"], ["200"])
        self.assertEqual(box["totals"]["riders"], 120)
        self.assertEqual(box["stars"][0]["block"], "11")
        self.assertEqual(gold["09"]["miles"], 150)
        self.assertEqual(gold["11"]["miles"], 150)

    def test_shuttle_riders_land_in_other(self):
        rows = [row("9/30/2026 10:00:00 AM", "900", "Purple Lots Shuttle", 30)]
        box = boxscore.build(DAY, rows, {DAY: {}}, GROUPS)
        self.assertEqual({r["family"]: r["riders"] for r in box["routes"] if r["riders"]}, {"Other": 30})

    def test_notes_rank_against_the_same_kind_of_day(self):
        history = [{"date": f"2026-09-{d:02d}", "level": "full", "riders": 1000 + d, "routes": {"Gold": 400}} for d in (1, 2, 3, 8, 9)]
        box = {"date": "2026-09-16", "level": "full", "totals": {"riders": 2000},
               "routes": [{"family": "Gold", "name": "Gold Line", "riders": 900}]}
        notes = boxscore.notes(box, history)
        self.assertIn("1st of 6", notes[0])
        self.assertTrue(any("Gold Line had its best weekday" in n for n in notes))
        self.assertEqual(boxscore.notes(box, history[:2]), [])


if __name__ == "__main__":
    unittest.main()
