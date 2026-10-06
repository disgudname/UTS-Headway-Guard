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

    def test_lot_shuttles_get_their_own_lines_and_make_an_event_day(self):
        rows = [row("9/30/2026 5:00:00 PM", "900", "Purple Lots Shuttle", 400), row("9/30/2026 10:30:00 PM", "901", "Post-Game Fan Shuttle", 300),
                row("9/30/2026 10:00:00 AM", "902", "Charter", 9), row("9/30/2026 10:00:00 AM", "903", "Training - Please Ignore", 50)]
        box = boxscore.build(DAY, rows, {DAY: {}}, GROUPS, game={"opponent": "Delaware", "kickoff": "19:00"})
        riders = {r["family"]: (r["kind"], r["riders"], r["before_kickoff"]) for r in box["routes"]}
        self.assertEqual(riders, {"Purple Lots Shuttle": ("shuttle", 400, 400), "Post-Game Fan Shuttle": ("shuttle", 300, 0),
                                  "Other": ("other", 9, 9)})
        self.assertEqual(box["event"], {"kind": "football", "name": None, "opponent": "Delaware", "kickoff": "19:00",
                                        "shuttle_riders": 700, "regular_riders": 0})
        self.assertEqual(box["totals"]["riders"], 709)  # training riders are not service
        # No game on the books, but the shuttles ran: still an event day.
        self.assertEqual(boxscore.build(DAY, rows, {DAY: {}}, GROUPS)["event"]["kind"], "event")
        self.assertIsNone(boxscore.build(DAY, rows[2:], {DAY: {}}, GROUPS)["event"])
        concert = boxscore.build(DAY, rows, {DAY: {}}, GROUPS, game={"name": "Luke Combs at Scott Stadium", "kickoff": None})["event"]
        self.assertEqual((concert["kind"], concert["name"]), ("event", "Luke Combs at Scott Stadium"))

    def test_game_log_keeps_home_football_only(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            log = boxscore.GameLog(tmp)
            self.assertEqual(log.game(date(2026, 9, 26))["opponent"], "Delaware")
            self.assertEqual(log.game(date(2026, 8, 29))["opponent"], "NC State")
            changed = log.record([
                {"sport": "Football -  Virginia Cavaliers", "opponent": "Syracuse", "start_time": "2026-10-10T19:30:00-04:00", "is_home": True},
                {"sport": "Football -  Virginia Cavaliers", "opponent": "Cal", "start_time": "2026-11-14T00:00:00-05:00", "is_home": True},
                {"sport": "Football -  Virginia Cavaliers", "opponent": "Florida State", "start_time": "2026-10-17T12:00:00-04:00", "is_home": False},
                {"sport": "Men's Soccer -  Virginia Cavaliers", "opponent": "Queens", "start_time": "2026-10-06T18:00:00-04:00", "is_home": True},
            ])
            self.assertEqual(changed, 2)
            self.assertEqual(log.game(date(2026, 10, 10)), {"opponent": "Syracuse", "kickoff": "19:30"})
            self.assertEqual(log.game(date(2026, 11, 14)), {"opponent": "Cal", "kickoff": None})
            self.assertIsNone(log.game(date(2026, 10, 17)))
            self.assertEqual(boxscore.GameLog(tmp).game(date(2026, 10, 10))["opponent"], "Syracuse")

    def test_notes_rank_against_the_same_kind_of_day(self):
        history = [{"date": f"2026-09-{d:02d}", "level": "full", "riders": 1000 + d, "routes": {"Gold": 400}} for d in (1, 2, 3, 8, 9)]
        box = {"date": "2026-09-16", "level": "full", "totals": {"riders": 2000},
               "routes": [{"family": "Gold", "name": "Gold Line", "riders": 900}]}
        notes = boxscore.notes(box, history)
        self.assertIn("1st of 6", notes[0])
        self.assertTrue(any("Gold Line had its best weekday" in n for n in notes))
        self.assertEqual(boxscore.notes(box, history[:2]), [])

    def test_event_days_are_ranked_only_against_each_other(self):
        history = [{"date": f"2026-09-{d:02d}", "level": "full", "riders": 1000 + d, "routes": {"Gold": 400}} for d in (1, 2, 3, 8, 9)]
        history += [{"date": "2026-08-29", "level": "full", "riders": 9000, "routes": {}, "event": True, "shuttle_riders": 8900}]
        game = {"date": "2026-09-26", "level": "full", "totals": {"riders": 8000}, "routes": [],
                "event": {"kind": "football", "shuttle_riders": 6269}}
        self.assertEqual(boxscore.notes(game, history), ["6,269 riders on the lot and fan shuttles, 2nd of 2 event days on record."])
        plain = {"date": "2026-09-16", "level": "full", "totals": {"riders": 2000}, "routes": []}
        self.assertIn("1st of 6", boxscore.notes(plain, history)[0])  # the event day is not a peer


if __name__ == "__main__":
    unittest.main()
