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
    assert all(r["START_DATE"] != "9/20/2026" for r in rows)                     # beyond the 2-day look-ahead
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
    monkeypatch.setattr(log, "open_shift_rows", lambda now=None, days_ahead=2: open_rows)
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
