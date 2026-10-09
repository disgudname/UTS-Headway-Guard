import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import w2w_schedule as w

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)
BS = chr(92)   # iCal escapes a newline as backslash-n and a comma as backslash-comma inside DESCRIPTION
CRLF = chr(13) + chr(10)


def _event(uid, start_z, end_z, employee, position, times, day, note, modified="20260901T120000Z"):
    parts = [employee, f"[{position}]", times, day.replace(",", BS + ","), note, "", "", "", "", "", ""]
    desc = (BS + "n").join(parts) + BS + "n(key: X do not alter)"
    summary = f"{employee} [{position}] {times} {note}".strip()
    lines = ["BEGIN:VEVENT", f"DTSTART:{start_z}", f"DTEND:{end_z}", f"UID:{uid}", f"LAST-MODIFIED:{modified}",
             f"DESCRIPTION:{desc}", f"SUMMARY:{summary}", "END:VEVENT"]
    return CRLF.join(lines) + CRLF


def _feed(*events):
    return "BEGIN:VCALENDAR" + CRLF + "VERSION:2.0" + CRLF + "".join(events) + "END:VCALENDAR" + CRLF


# 2026-09-04 10:30-18:30 New York = 14:30Z-22:30Z
ASSIGNED = _event("a1", "20260904T143000Z", "20260904T223000Z", "Gene Kirby", "08", "10:30am-6:30pm", "Sep 4, 2026", "OFF - Relieve @ 1040 MP")
UNASSIGNED = _event("a1", "20260904T143000Z", "20260904T223000Z", "", "08", "10:30am-6:30pm", "Sep 4, 2026", "OFF - Relieve @ 1040 MP")


def test_parse_reads_employee_position_times_and_note_in_new_york_time():
    shifts = w.parse_ics(_feed(ASSIGNED))
    assert shifts["a1"] == {
        "employee": "Gene Kirby", "position": "08", "position_name": "[08]", "start": "2026-09-04T10:30-04:00",
        "end": "2026-09-04T18:30-04:00", "note": "OFF - Relieve @ 1040 MP", "modified": "20260901T120000Z",
    }


def test_an_unassigned_shift_has_an_empty_employee_and_is_listed_as_unassigned(tmp_path):
    shifts = w.parse_ics(_feed(UNASSIGNED))
    assert shifts["a1"]["employee"] == "" and shifts["a1"]["position"] == "08"
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(UNASSIGNED), NOW)
    rows = log.unassigned(date(2026, 9, 4), days=1)
    assert [(r["date"], r["position"], r["start"]) for r in rows] == [("2026-09-04", "08", "2026-09-04T10:30-04:00")]
    assert log.unassigned(date(2026, 9, 5), days=1) == []


def test_first_poll_is_a_baseline_then_changes_are_logged(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    first = log.apply(_feed(ASSIGNED), NOW)
    assert [e["kind"] for e in first] == ["baseline"]
    second = log.apply(_feed(UNASSIGNED), NOW)
    assert len(second) == 1
    e = second[0]
    assert (e["kind"], e["fields"], e["employee_before"], e["employee_after"], e["date"], e["position"]) == (
        "changed", ["employee"], "Gene Kirby", "", "2026-09-04", "08")
    assert [x["kind"] for x in log.recent_changes()] == ["changed"]          # baseline hidden from the reader
    assert log.recent_changes(on_date="2026-09-05") == []
    assert len(log.recent_changes(position="8")) == 1                       # "8" and "08" are the same position


def test_added_and_removed_shifts_are_logged(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    other = _event("b2", "20260904T110000Z", "20260904T150000Z", "Abinash G", "08", "7am-11am", "Sep 4, 2026", "")
    log.apply(_feed(ASSIGNED, other), NOW)
    added = _event("c3", "20260907T110000Z", "20260907T150000Z", "", "09", "7am-11am", "Sep 7, 2026", "")
    events = log.apply(_feed(ASSIGNED, added), NOW)
    assert sorted(e["kind"] for e in events) == ["added", "removed"]


def test_a_relabelled_last_modified_alone_is_not_a_change(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    restamped = ASSIGNED.replace("20260901T120000Z", "20260923T190000Z")
    log.apply(_feed(ASSIGNED), NOW)
    assert log.apply(_feed(restamped), NOW) == []


def test_a_feed_that_shrinks_to_almost_nothing_is_ignored_not_treated_as_mass_deletion(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    many = [_event(f"u{i}", "20260904T143000Z", "20260904T223000Z", "X Y", "08", "t", "Sep 4, 2026", "") for i in range(10)]
    log.apply(_feed(*many), NOW)
    assert log.apply(_feed(many[0]), NOW) == []
    assert "shrank" in log.last_error
    assert log.apply("<html>not a calendar</html>", NOW) == []
    assert len(w.W2WScheduleLog(tmp_path)._load_snapshot()) == 10          # snapshot untouched


def test_shifts_older_than_the_tracking_window_are_ignored(tmp_path):
    old = _event("old", "20250913T173000Z", "20250913T203000Z", "Z", "08", "t", "Sep 13, 2025", "")
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(ASSIGNED, old), NOW)
    assert list(json.loads((tmp_path / w.SNAPSHOT_NAME).read_text())["shifts"]) == ["a1"]


# ---- open shifts -> the per-block assignment structures the dispatch endpoints use ---------------------------------------
def test_open_shift_rows_are_api_shaped_and_skip_assigned_ended_and_far_future_shifts(tmp_path):
    ended = _event("e1", "20260904T110000Z", "20260904T150000Z", "", "09", "7am-11am", "Sep 4, 2026", "")
    later = _event("l1", "20260906T143000Z", "20260906T223000Z", "", "[x]", "10:30am-6:30pm", "Sep 6, 2026", "")
    far = _event("f1", "20260920T143000Z", "20260920T223000Z", "", "10", "10:30am-6:30pm", "Sep 20, 2026", "")
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(ASSIGNED, UNASSIGNED.replace("UID:a1", "UID:a2"), ended, later, far), NOW)
    now = datetime(2026, 9, 4, 16, 30, tzinfo=timezone.utc)  # 12:30 New York on 09-04: the 7-11am shift is over
    rows = log.open_shift_rows(now)
    assert [(r["POSITION_NAME"], r["START_DATE"], r["START_TIME"], r["END_TIME"]) for r in rows[:1]] == [
        ("[08]", "9/4/2026", "10:30am", "6:30pm")]
    assert rows[0]["FIRST_NAME"] == "" and rows[0]["LAST_NAME"] == "" and rows[0]["DURATION"] == "8.0"
    assert all(r["START_DATE"] != "9/20/2026" for r in rows)                     # a later day
    assert all(r["START_DATE"] != "9/6/2026" for r in rows)                      # tomorrow-and-after never counts as today
    assert all(r["END_TIME"] != "11am" for r in rows)                             # 7-11am already over


def test_w2w_clock_matches_the_api_style():
    assert [w._w2w_clock(datetime(2026, 9, 4, h, m)) for h, m in [(7, 0), (10, 30), (12, 0), (0, 15), (18, 45)]] == [
        "7am", "10:30am", "12pm", "12:15am", "6:45pm"]


def test_open_shifts_become_unassigned_entries_by_block(tmp_path, monkeypatch):
    import app
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(UNASSIGNED), NOW)
    monkeypatch.setattr(app.app.state, "w2w_schedule_log", log, raising=False)
    tz = app.ZoneInfo("America/New_York")
    now = datetime(2026, 9, 4, 14, 0, tzinfo=timezone.utc)
    by_block = app._open_shift_assignments(now, tz)
    entry = by_block["08"]["any"][0] if "any" in by_block["08"] else next(iter(by_block["08"].values()))[0]
    assert entry["name"] == "OPEN" and entry["unassigned"] is True
    assert (entry["start_label"], entry["end_label"]) == ("10:30a", "6:30p") or entry["start_label"].startswith("10")
    assert entry["end_ts"] - entry["start_ts"] == 8 * 3600 * 1000
    assert entry["position_name"] == "[08]"
    # a person-assigned shift must not be flagged
    assigned = app._build_driver_assignments(
        [{"POSITION_NAME": "[08]", "FIRST_NAME": "Gene", "LAST_NAME": "Kirby", "START_DATE": "9/4/2026", "START_TIME": "10:30am",
          "END_DATE": "9/4/2026", "END_TIME": "6:30pm", "COLOR_ID": "0"}], now, tz)
    person = next(iter(assigned["08"].values()))[0]
    assert person["name"] == "Gene Kirby" and "unassigned" not in person
    # no log / nothing polled yet -> no open shifts, never an error
    monkeypatch.setattr(app.app.state, "w2w_schedule_log", None, raising=False)
    assert app._open_shift_assignments(now, tz) == {}


# ---- /v1/dispatch/vehicle-drivers and /v1/uts/on_duty with open shifts --------------------------------------------------
def _shift(name, start_ts, end_ts, position_name="[08]", unassigned=False):
    entry = {"name": name, "start_ts": start_ts, "end_ts": end_ts, "start_label": "1a", "end_label": "2p",
             "color_id": "0", "position_name": position_name}
    if unassigned:
        entry["unassigned"] = True
    return entry


def _run_fetch_vehicle_drivers(monkeypatch, assigned, open_shifts, transloc_block="[08]", block_window=(-3600000, 3600000)):
    import asyncio
    import app
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    async def fake_groups(client, include_metadata=False):
        return []

    async def fake_w2w_get(_fetcher):
        return {"assignments_by_block": assigned(now_ms), "unassigned_by_block": open_shifts(now_ms)}

    monkeypatch.setattr(app, "fetch_block_groups", fake_groups)
    monkeypatch.setattr(app, "_build_block_mapping_with_times",
                        lambda groups, *a, **k: {"14": [(transloc_block, now_ms + block_window[0], now_ms + block_window[1])]})
    monkeypatch.setattr(app.w2w_assignments_cache, "get", fake_w2w_get)
    monkeypatch.setattr(app.app.state, "ondemand_client", None, raising=False)
    monkeypatch.setattr(app.state, "vehicles_raw", [{"VehicleID": 14, "Name": "18232", "RouteID": 0}], raising=False)
    monkeypatch.setattr(app.state, "vehicle_block_cache", {}, raising=False)
    return asyncio.run(app._fetch_vehicle_drivers())["vehicle_drivers"].get("14")


def test_a_bus_on_a_block_nobody_is_assigned_to_shows_the_open_shift(monkeypatch):
    entry = _run_fetch_vehicle_drivers(
        monkeypatch, lambda n: {},
        lambda n: {"08": {"any": [_shift("OPEN", n - 3600000, n + 3600000, unassigned=True)]}})
    assert entry["block"] == "[08]"
    assert [(d["name"], d.get("unassigned")) for d in entry["drivers"]] == [("OPEN", True)]


def test_an_open_shift_overlapping_an_assigned_driver_is_listed_beside_them(monkeypatch):
    entry = _run_fetch_vehicle_drivers(
        monkeypatch,
        lambda n: {"08": {"any": [_shift("Gene Kirby", n - 1800000, n + 3600000)]}},
        lambda n: {"08": {"any": [_shift("OPEN", n - 7200000, n + 1800000, unassigned=True)]}})
    assert [(d["name"], d.get("unassigned", False)) for d in entry["drivers"]] == [("OPEN", True), ("Gene Kirby", False)]


def test_an_open_shift_on_another_block_never_changes_a_bus_that_has_a_driver(monkeypatch):
    entry = _run_fetch_vehicle_drivers(
        monkeypatch,
        lambda n: {"08": {"any": [_shift("Gene Kirby", n - 1800000, n + 3600000)]}},
        lambda n: {"09": {"any": [_shift("OPEN", n - 7200000, n + 1800000, "[09]", unassigned=True)]}})   # different block
    assert [d["name"] for d in entry["drivers"]] == ["Gene Kirby"]
    nothing = _run_fetch_vehicle_drivers(monkeypatch, lambda n: {}, lambda n: {})
    assert nothing["drivers"] == []


def test_an_open_shift_keeps_a_bus_listed_outside_its_transloc_block_times(monkeypatch):
    # TransLoc's block ended an hour ago, but the block's W2W shift is still running and nobody is on it (EBs usually
    # drive these): the bus stays in the list, marked OPEN.
    window = (-7200000, -3600000)
    entry = _run_fetch_vehicle_drivers(
        monkeypatch, lambda n: {},
        lambda n: {"08": {"any": [_shift("OPEN", n - 7200000, n + 3600000, unassigned=True)]}}, block_window=window)
    assert entry is not None and entry["block"] == "[08]"
    assert [(d["name"], d.get("unassigned")) for d in entry["drivers"]] == [("OPEN", True)]
    # ...and with no shift at all, it stays out, as before
    assert _run_fetch_vehicle_drivers(monkeypatch, lambda n: {}, lambda n: {}, block_window=window) is None


def test_on_duty_lists_uncovered_supervisor_and_dispatch_shifts_separately(monkeypatch, tmp_path):
    import asyncio
    import app
    now_ny = datetime.now(app.ZoneInfo("America/New_York"))

    def when(delta_hours):
        moment = now_ny + __import__("datetime").timedelta(hours=delta_hours)
        return f"{moment.month}/{moment.day}/{moment.year}", w._w2w_clock(moment)

    def api_shift(position, first, last, start_h, end_h):
        (sd, st), (ed, et) = when(start_h), when(end_h)
        return {"POSITION_NAME": position, "FIRST_NAME": first, "LAST_NAME": last, "START_DATE": sd, "START_TIME": st,
                "END_DATE": ed, "END_TIME": et, "COLOR_ID": "0"}

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"AssignedShiftList": [api_shift("Sup", "Pat", "Lee", -1, 3)]}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, *a, **k):
            return FakeResponse()

    async def fake_w2w_get(_fetcher):
        return {"assignments_by_block": {}}

    log = w.W2WScheduleLog(tmp_path)
    open_rows = [api_shift("OnDemand Dispatch", "", "", -1, 2), api_shift("Sup", "", "", 3, 7), api_shift("[08]", "", "", -1, 2)]
    monkeypatch.setattr(log, "open_shift_rows", lambda now=None, first=None, last=None: open_rows)
    monkeypatch.setattr(app.app.state, "w2w_schedule_log", log, raising=False)
    monkeypatch.setattr(app, "W2W_KEY", "test-key")
    monkeypatch.setattr(app.httpx, "AsyncClient", lambda *a, **k: FakeClient())
    monkeypatch.setattr(app.w2w_assignments_cache, "get", fake_w2w_get)
    out = asyncio.run(app._fetch_on_duty_personnel())
    assert [p["name"] for p in out["supervisors"]] == ["Pat Lee"]                 # the real supervisor is unchanged
    assert [p["name"] for p in out["ondemand_dispatchers"]] == []                  # an open shift is never "on duty"
    assert [(p["name"], p["active"]) for p in out["ondemand_dispatchers_open"]] == [("OPEN", True)]   # uncovered right now
    assert [(p["name"], p["active"]) for p in out["supervisors_open"]] == [("OPEN", False)]           # the later, uncovered shift


def test_an_unassigned_block_is_labelled_with_the_w2w_name_not_transloc_s(monkeypatch):
    # TransLoc calls the block "[24] PM"; W2W (what dispatchers work from) calls the position "[24]". With nobody
    # assigned, the bus used to fall back to TransLoc's label; with the open shift known it shows W2W's.
    entry = _run_fetch_vehicle_drivers(
        monkeypatch, lambda n: {},
        lambda n: {"24": {"pm": [_shift("OPEN", n - 3600000, n + 3600000, "[24]", unassigned=True)]}},
        transloc_block="[24] PM")
    assert entry["block"] == "[24]"
    assert [(d["name"], d.get("unassigned")) for d in entry["drivers"]] == [("OPEN", True)]
    # no W2W shift at all (assigned or open): still TransLoc's label, as before
    nothing = _run_fetch_vehicle_drivers(monkeypatch, lambda n: {}, lambda n: {}, transloc_block="[24] PM")
    assert nothing["block"] == "[24] PM" and nothing["drivers"] == []


def test_open_shifts_only_come_from_the_days_the_caller_asked_for(tmp_path):
    def shift(uid, start_z, end_z, position, day):
        return _event(uid, start_z, end_z, "", position, "t", day, "")
    # New York: yesterday 22:00 -> 06:00 today (runs past midnight), today 16:00-22:30, tomorrow 07:00-11:00
    feed = _feed(
        shift("y", "20260904T020000Z", "20260904T100000Z", "01", "Sep 3, 2026"),
        shift("t", "20260904T200000Z", "20260905T023000Z", "02", "Sep 4, 2026"),
        shift("n", "20260905T110000Z", "20260905T150000Z", "03", "Sep 5, 2026"),
    )
    log = w.W2WScheduleLog(tmp_path)
    log.apply(feed, NOW)
    now = datetime(2026, 9, 4, 8, 0, tzinfo=timezone.utc)       # 04:00 New York on 09-04: the overnight shift is running
    names = lambda rows: sorted(r["POSITION_NAME"] for r in rows)
    assert names(log.open_shift_rows(now)) == ["[01]", "[02]"]                       # default: yesterday + today, never tomorrow
    assert names(log.open_shift_rows(now, first=date(2026, 9, 4), last=date(2026, 9, 4))) == ["[02]"]   # one service day
    assert names(log.open_shift_rows(now, first=date(2026, 9, 3), last=date(2026, 9, 3))) == ["[01]"]   # 02:30 rollover: still yesterday's
    assert names(log.open_shift_rows(now, first=date(2026, 9, 3), last=date(2026, 9, 5))) == ["[01]", "[02]", "[03]"]


# --- Open shifts the feed has not delivered, filled in from W2W's API (missing_open_shifts) ---

def _api_shift(position, start, end, hours, color="0", day="9/4/2026", end_day=None, published="Y"):
    return {"POSITION_NAME": position, "START_DATE": day, "START_TIME": start, "END_DATE": end_day or day, "END_TIME": end,
            "DURATION": str(hours), "COLOR_ID": color, "PUBLISHED": published, "FIRST_NAME": "Gene", "LAST_NAME": "Kirby"}


def _total(position, shifts, hours, day="9/4/2026"):
    return {"SCHEDULE_DATE": day, "POSITION_NAME": position, "UNASSIGNED_SHIFTS": str(shifts), "UNASSIGNED_HOURS": str(hours)}


def test_an_open_copy_the_feed_never_got_takes_its_times_from_the_shift_it_was_copied_from():
    feed = w.parse_ics(_feed(ASSIGNED))  # the called-out shift is in the feed with its driver; the open copy is not
    assigned = [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9"), _api_shift("[08]", "6am", "10:30am", 4.5)]
    filled, unresolved = w.missing_open_shifts(feed, [_total("[08]", 1, 8.0)], assigned)
    assert unresolved == []
    assert [(s["employee"], s["position"], s["position_name"], s["start"], s["end"], s["note"]) for s in filled] == [
        ("", "08", "[08]", "2026-09-04T10:30-04:00", "2026-09-04T18:30-04:00", "")]


def test_nothing_is_filled_when_the_feed_already_has_every_open_shift_w2w_counts():
    assigned = [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9")]
    assert w.missing_open_shifts(w.parse_ics(_feed(UNASSIGNED)), [_total("[08]", 1, 8.0)], assigned) == ([], [])


def test_an_overnight_open_copy_ends_on_the_next_day():
    assigned = [_api_shift("[03]", "10pm", "2:30am", 4.5, color="9", end_day="9/5/2026")]
    filled, _ = w.missing_open_shifts({}, [_total("[03]", 1, 4.5)], assigned)
    assert (filled[0]["start"], filled[0]["end"]) == ("2026-09-04T22:00-04:00", "2026-09-05T02:30-04:00")


def test_the_original_of_a_copy_already_in_the_feed_is_not_used_twice():
    # two 8 h shifts on the block were called out; one copy reached the feed, so the missing one is the other
    feed = w.parse_ics(_feed(UNASSIGNED))
    assigned = [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9"), _api_shift("[08]", "2:30pm", "10:30pm", 8.0, color="9")]
    filled, unresolved = w.missing_open_shifts(feed, [_total("[08]", 2, 16.0)], assigned)
    assert unresolved == [] and [s["start"] for s in filled] == ["2026-09-04T14:30-04:00"]


def test_between_two_shifts_of_the_same_length_the_red_one_is_the_copy_s_original():
    assigned = [_api_shift("[08]", "6am", "10am", 4.0), _api_shift("[08]", "2pm", "6pm", 4.0, color="9")]
    filled, unresolved = w.missing_open_shifts({}, [_total("[08]", 1, 4.0)], assigned)
    assert unresolved == [] and [s["start"] for s in filled] == ["2026-09-04T14:00-04:00"]


def test_a_gap_no_shift_explains_is_reported_and_nothing_is_made_up():
    assigned = [_api_shift("[08]", "6am", "10am", 4.0), _api_shift("[08]", "2pm", "6pm", 4.0)]
    for total in (_total("[08]", 1, 4.0), _total("[08]", 1, 6.5)):  # two equal candidates; no candidate of that length
        filled, unresolved = w.missing_open_shifts({}, [total], assigned)
        assert filled == [] and unresolved == [
            {"date": "2026-09-04", "position_name": "[08]", "missing": 1, "hours": float(total["UNASSIGNED_HOURS"])}]


def test_days_w2w_has_not_published_are_left_alone():
    assigned = [_api_shift("[08]", "10:30am", "6:30pm", 8.0, published="N")]
    assert w.missing_open_shifts({}, [_total("[08]", 1, 8.0)], assigned) == ([], [])


def test_a_filled_shift_shows_on_the_board_until_the_feed_has_it_or_the_api_goes_quiet(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(ASSIGNED), NOW)
    now = datetime(2026, 9, 4, 16, 0, tzinfo=timezone.utc)
    api = ([_total("[08]", 1, 8.0)], [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9")])

    def board(at):
        return [(s["position"], s["start"]) for s in log.open_blocks(at)["bus"]["shifts"]]

    assert board(now) == []
    log.apply_api(*api, now=now)
    assert board(now) == [("08", "2026-09-04T10:30-04:00")]
    assert [r["POSITION_NAME"] for r in log.open_shift_rows(now)] == ["[08]"]
    log.apply_api(*api)  # unassigned() has no clock argument, so this read has to be fresh by the real clock
    assert [r["position"] for r in log.unassigned(date(2026, 9, 4), days=1)] == ["08"]
    log.apply_api(*api, now=now)
    # the API has not answered for longer than API_FILL_MAX_AGE_S: stop trusting the fill
    assert board(datetime(2026, 9, 4, 16, 30, tzinfo=timezone.utc)) == []
    # the copy reaches the feed before the next API read: one row, not two
    log.apply(_feed(ASSIGNED.replace("UID:a1", "UID:a0"), UNASSIGNED), now)
    assert board(now) == [("08", "2026-09-04T10:30-04:00")]


def test_a_filled_shift_carries_the_note_from_before_the_callout_edit_never_the_reason(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(ASSIGNED), NOW)
    # the callout: W2W re-creates the named shift (new UID) with the reason added to its note
    sick = ASSIGNED.replace("UID:a1", "UID:a2").replace("1040 MP", "1040 MP - DNS(Sick)")
    log.apply(_feed(sick), NOW)
    api = ([_total("[08]", 1, 8.0)], [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9")])
    log.apply_api(*api)
    assert [s["note"] for s in log.filled] == ["OFF - Relieve @ 1040 MP"]
    board = log.open_blocks(datetime(2026, 9, 4, 16, 0, tzinfo=timezone.utc))["bus"]["shifts"]
    assert [s["note"] for s in board] == ["OFF - Relieve @ 1040 MP"]
    # a later edit to the original does not change the note already chosen
    log.apply(_feed(sick.replace("UID:a2", "UID:a3").replace("DNS(Sick)", "DNS(Sick) - spoke to driver")), NOW)
    log.apply_api(*api)
    assert [s["note"] for s in log.filled] == ["OFF - Relieve @ 1040 MP"]


def test_a_filled_shift_has_no_note_when_the_log_never_saw_the_original_edited(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    log.apply(_feed(ASSIGNED), NOW)
    log.apply_api([_total("[08]", 1, 8.0)], [_api_shift("[08]", "10:30am", "6:30pm", 8.0, color="9")])
    assert [s["note"] for s in log.filled] == [""]


# --- W2W's own "Unassigned Shifts" export CSV: the exact list of open shifts, checked against the API's count ---

EXPORT_HEAD = '"Shift ID","Schedule ID","Position ID","Position Name","Cat","Shift Description","Date","Start Time","End Time","Duration","Day Of Week"'
EXPORT = CRLF.join([
    EXPORT_HEAD,
    '2,9,7,"[08]",,"OFF - Relieve @ 1040 MP",9/4/2026,10:30 AM,06:30 PM,   8.0,4',
    '3,9,7,"[03]",,"OFF - Relieve [05] @ ~2210 HER",9/4/2026,10:00 PM,02:30 AM,   4.5,4',
    '4,9,7,"OnDemand Driver",,"OFF, JPJ staging",9/2/2026,09:30 PM,05:30 AM,   8.0,2',
]) + CRLF
EXPORT_DAYS = ["2026-09-02", "2026-09-03", "2026-09-04"]
EXPORT_TOTALS = [_total("[08]", 1, 8.0), _total("[03]", 1, 4.5), _total("OnDemand Driver", 1, 8.0, day="9/2/2026"),
                 _total("[01]", 0, 0.0, day="9/3/2026")]


def test_the_export_lists_unassigned_shifts_with_their_own_times_and_notes():
    shifts = w.parse_export_csv(EXPORT)
    assert [(s["position"], s["position_name"], s["start"], s["end"], s["note"], s["shift_id"]) for s in shifts] == [
        ("08", "[08]", "2026-09-04T10:30-04:00", "2026-09-04T18:30-04:00", "OFF - Relieve @ 1040 MP", "2"),
        ("03", "[03]", "2026-09-04T22:00-04:00", "2026-09-05T02:30-04:00", "OFF - Relieve [05] @ ~2210 HER", "3"),
        ("OnDemand Driver", "OnDemand Driver", "2026-09-02T21:30-04:00", "2026-09-03T05:30-04:00", "OFF, JPJ staging", "4"),
    ]
    assert w.parse_export_csv(EXPORT_HEAD + CRLF) == []  # a header with no rows: nothing unassigned
    # the all-shifts export has an Employee Name column: rows with a name are skipped
    both = EXPORT_HEAD + ',"Employee Name"' + CRLF + '1,9,7,"[08]",,"OFF",9/4/2026,06:00 AM,10:00 AM,4.0,4,"Gene Kirby"' + CRLF +         '2,9,7,"[08]",,"OFF",9/4/2026,10:00 AM,02:00 PM,4.0,4,' + CRLF
    assert [s["start"][11:16] for s in w.parse_export_csv(both)] == ["10:00"]


def test_something_that_is_not_the_export_is_an_error(tmp_path):
    import pytest
    for bad in ("<html><body>Sign in</body></html>", "Report type=exportunassigned is not authorized!",
                '"Shift ID","Date"' + CRLF + "1,9/4/2026" + CRLF, EXPORT_HEAD + CRLF + '2,9,7,"[08]",,"OFF",soon,10:30 AM,06:30 PM,8.0,4' + CRLF):
        with pytest.raises(ValueError):
            w.parse_export_csv(bad)
    log = w.W2WScheduleLog(tmp_path)
    log.apply_export(EXPORT, EXPORT_DAYS, EXPORT_TOTALS)
    with pytest.raises(ValueError):
        log.apply_export("<html>401</html>", EXPORT_DAYS, EXPORT_TOTALS)
    assert log.open_source() == "export" and len(log._export_open) == 3  # the last good read is kept


def test_a_day_is_only_taken_from_the_export_when_it_agrees_with_the_api_count(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    now = datetime(2026, 9, 4, 16, 0, tzinfo=timezone.utc)
    stale = _event("s1", "20260904T143000Z", "20260904T223000Z", "", "09", "10:30am-6:30pm", "Sep 4, 2026", "OFF")
    log.apply(_feed(ASSIGNED, stale), NOW)

    def board(at=now):
        return [(s["position"], s["start"][11:16]) for s in log.open_blocks(at)["bus"]["shifts"]]

    # an export that came back EMPTY while W2W counts open shifts must not read as "No OB": every day stays on the feed
    log.apply_export(EXPORT_HEAD + CRLF, EXPORT_DAYS, EXPORT_TOTALS, now=now)
    assert log.export_mismatch == ["2026-09-02", "2026-09-04"] and board() == [("09", "10:30")]
    # 9-03 agreed (0 = 0), so the export still counts as the source for that day
    assert log.open_source(now) == "export"
    # W2W counts one more [08] than the export lists: 9-04 stays on the feed, the other days come from the export
    short = EXPORT_TOTALS + [_total("[10]", 1, 3.5)]
    log.apply_export(EXPORT, EXPORT_DAYS, short, now=now)
    assert log.export_mismatch == ["2026-09-04"] and board() == [("09", "10:30")]
    # a day the API said nothing about is not trusted either
    log.apply_export(EXPORT, EXPORT_DAYS + ["2026-09-05"], EXPORT_TOTALS, now=now)
    assert log.export_mismatch == ["2026-09-05"]


def test_the_export_replaces_the_feed_on_the_days_it_covers_and_only_while_it_is_fresh(tmp_path):
    log = w.W2WScheduleLog(tmp_path)
    now = datetime(2026, 9, 4, 16, 0, tzinfo=timezone.utc)
    # feed: a nameless [09] on 9-04 that W2W no longer has, and a nameless [08] on 9-10 (outside the export's days)
    stale = _event("s1", "20260904T143000Z", "20260904T223000Z", "", "09", "10:30am-6:30pm", "Sep 4, 2026", "OFF")
    later = _event("s2", "20260910T143000Z", "20260910T223000Z", "", "08", "10:30am-6:30pm", "Sep 10, 2026", "OFF")
    log.apply(_feed(ASSIGNED, stale, later), NOW)

    def board(at):
        return [(s["position"], s["start"][11:16], s["note"]) for s in log.open_blocks(at)["bus"]["shifts"]]

    assert log.open_source(now) == "feed" and board(now) == [("09", "10:30", "OFF")]
    log.apply_export(EXPORT, EXPORT_DAYS, EXPORT_TOTALS, now=now)
    assert log.open_source(now) == "export" and log.export_mismatch == []
    assert board(now) == [("08", "10:30", "OFF - Relieve @ 1040 MP"), ("03", "22:00", "OFF - Relieve [05] @ ~2210 HER")]
    # the feed still supplies days the export does not cover
    log.apply_export(EXPORT, EXPORT_DAYS, EXPORT_TOTALS)
    assert [(r["date"], r["position"]) for r in log.unassigned(date(2026, 9, 10), days=1)] == [("2026-09-10", "08")]
    # export unread for longer than API_FILL_MAX_AGE_S: back to the feed
    late = datetime(2026, 9, 4, 16, 30, tzinfo=timezone.utc)
    log.apply_export(EXPORT, EXPORT_DAYS, EXPORT_TOTALS, now=now)
    assert log.open_source(late) == "feed" and board(late) == [("09", "10:30", "OFF")]


# --- W2W's own change log: the "Recent Shift History" export ---

HISTORY_HEAD = "ShiftID,ShiftDate,StartTime,EndTime,Position,ChangeDate,ChangedBy,NowAssignedTo,Change"
HISTORY = CRLF.join([
    HISTORY_HEAD,
    '1294214799,10/09/2026,13:00,15:00,"[12]",10/09/2026 12:33,"Pat Manager","Mike Driver","Description changedStart Time changedDuration changedWorker assigned"',
    '1291723793,10/09/2026,22:00,02:30,"[03]",10/08/2026 23:39,"Sam Manager","Art Driver","Color changed"',
    '1294167791,10/09/2026,22:00,02:30,"[03]",10/08/2026 23:39,"Sam Manager","","Shift Created"',
    '1293279095,10/12/2026,21:30,05:30,"OnDemand Driver",10/08/2026 23:57,"Sam Manager","","Worker unassigned"',
]) + CRLF


def test_shift_history_rows_become_change_events_oldest_first():
    events = w.parse_history_csv(HISTORY)
    assert [e["ts"] for e in events] == ["2026-10-08T23:39-04:00", "2026-10-08T23:39-04:00", "2026-10-08T23:57-04:00", "2026-10-09T12:33-04:00"]
    created = events[1]
    assert created == {
        "ts": "2026-10-08T23:39-04:00", "shift_id": "1294167791", "date": "2026-10-09", "position": "03", "position_name": "[03]",
        "start": "2026-10-09T22:00-04:00", "end": "2026-10-10T02:30-04:00", "changed_by": "Sam Manager", "assigned_to": "",
        "changes": ["Shift Created"],
    }
    assert events[3]["changes"] == ["Description changed", "Start Time changed", "Duration changed", "Worker assigned"]
    assert events[2]["position"] == "OnDemand Driver" and events[2]["end"] == "2026-10-13T05:30-04:00"
    # the same report on a 12-hour clock
    twelve = HISTORY_HEAD + CRLF + '7,10/09/2026,10:00 PM,02:30 AM,"[03]",10/08/2026 11:39 PM,"Sam Manager","","Shift Created"' + CRLF
    assert [(e["ts"], e["start"], e["end"]) for e in w.parse_history_csv(twelve)] == [
        ("2026-10-08T23:39-04:00", "2026-10-09T22:00-04:00", "2026-10-10T02:30-04:00")]


def test_shift_history_is_stored_once_and_served_newest_first(tmp_path):
    import pytest
    log = w.W2WScheduleLog(tmp_path)
    assert log.history_resume_date(date(2026, 10, 9), 60) == date(2026, 8, 10)  # nothing stored: backfill
    assert log.apply_history(HISTORY) == 4
    assert log.apply_history(HISTORY) == 0  # the same rows again (every poll re-reads today) add nothing
    assert w.W2WScheduleLog(tmp_path).apply_history(HISTORY) == 0  # ... also after a restart
    later = HISTORY + '1294167791,10/09/2026,22:00,02:30,"[03]",10/09/2026 15:10,"Sam Manager","New Driver","Worker assigned"' + CRLF
    assert log.apply_history(later) == 1
    assert log.history_resume_date(date(2026, 10, 12), 60) == date(2026, 10, 9)  # resume from the newest stored change
    assert [(e["ts"][5:16], e["assigned_to"]) for e in log.history(on_date="2026-10-09", position="3")] == [
        ("10-09T15:10", "New Driver"), ("10-08T23:39", "Art Driver"), ("10-08T23:39", "")]
    assert len(log.history(limit=2)) == 2 and log.history(on_date="2026-10-12")[0]["changes"] == ["Worker unassigned"]
    for bad in ("Report type=exportshifthistory is not authorized!", "<html>Sign in</html>",
                HISTORY_HEAD + CRLF + '1,soon,13:00,15:00,"[12]",10/09/2026 12:33,"A","B","Color changed"' + CRLF):
        with pytest.raises(ValueError):
            log.apply_history(bad)
    assert len(log.history()) == 5  # a bad file stores nothing
    assert log.apply_history(HISTORY_HEAD + CRLF) == 0  # a quiet day
