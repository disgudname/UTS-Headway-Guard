import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from headway_storage import HeadwayEvent
import trip_planner_history as tph

NY_TZ = tph.NY_TZ


class FakeStorage:
    """Minimal stand-in for HeadwayStorage -- just enough of query_events's contract
    for these tests (real class reads CSV files, not needed here)."""

    def __init__(self, events):
        self._events = events

    def query_events(self, start, end, route_ids=None, stop_ids=None):
        return [e for e in self._events if start <= e.timestamp <= end]


def _event(dt, route_id, stop_id, block, event_type="arrival", address_id=None):
    return HeadwayEvent(
        timestamp=dt,
        route_id=route_id,
        stop_id=stop_id,
        vehicle_id="1",
        vehicle_name="1234",
        event_type=event_type,
        headway_arrival_arrival=None,
        headway_departure_arrival=None,
        dwell_seconds=None,
        block=block,
        address_id=address_id,
    )


def _wed_5pm(day_offset: int) -> datetime:
    # 2026-09-09 is a Wednesday; walk backward/forward in 7-day steps to stay on
    # Wednesdays while varying the sample.
    base = datetime(2026, 9, 9, 17, 0, tzinfo=NY_TZ)
    return base - timedelta(days=7 * day_offset)


def test_build_hop_time_samples_buckets_by_route_stop_weekday_hour():
    events = []
    # Three Wednesday 5pm runs of block "Gold_01": stop A -> stop B in 300/310/320s.
    for i, gap in enumerate([300, 310, 320]):
        start = _wed_5pm(i)
        events.append(_event(start, "67", "A", "Gold_01"))
        events.append(_event(start + timedelta(seconds=gap), "67", "B", "Gold_01"))

    storage = FakeStorage(events)
    samples = tph.build_hop_time_samples(storage, now=_wed_5pm(0) + timedelta(hours=1))
    key = tph._bucket_key("67", "A", "B", 2, 17)  # Wednesday == weekday() 2
    # Order isn't part of the contract (build_hop_time_samples processes one day at
    # a time, oldest first, to bound memory -- see its docstring) -- just the set.
    assert sorted(samples[key]) == [300.0, 310.0, 320.0]


def test_refresh_hop_time_cache_drops_sparse_buckets(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "CACHE_PATH", tmp_path / "hop_times.json")
    events = []
    # Only two samples -- below MIN_SAMPLES (3).
    for i, gap in enumerate([300, 310]):
        start = _wed_5pm(i)
        events.append(_event(start, "67", "A", "Gold_01"))
        events.append(_event(start + timedelta(seconds=gap), "67", "B", "Gold_01"))

    storage = FakeStorage(events)
    cache = tph.refresh_hop_time_cache(storage, now=_wed_5pm(0) + timedelta(hours=1))
    key = tph._bucket_key("67", "A", "B", 2, 17)
    assert key not in cache["buckets"]


def test_refresh_hop_time_cache_keeps_bucket_with_enough_samples(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "CACHE_PATH", tmp_path / "hop_times.json")
    events = []
    for i, gap in enumerate([300, 310, 320]):
        start = _wed_5pm(i)
        events.append(_event(start, "67", "A", "Gold_01"))
        events.append(_event(start + timedelta(seconds=gap), "67", "B", "Gold_01"))

    storage = FakeStorage(events)
    cache = tph.refresh_hop_time_cache(storage, now=_wed_5pm(0) + timedelta(hours=1))
    key = tph._bucket_key("67", "A", "B", 2, 17)
    assert cache["buckets"][key]["seconds"] == 306.6  # 33rd percentile of 300/310/320, just under the median
    assert cache["buckets"][key]["samples"] == 3


def test_quantile_interpolates_and_half_is_the_median():
    assert tph._quantile([320.0, 300.0, 310.0], 0.5) == 310.0
    assert tph._quantile([100.0, 200.0], 0.5) == 150.0
    assert tph._quantile([100.0, 200.0, 300.0, 400.0, 500.0], 0.25) == 200.0
    assert tph._quantile([42.0], 0.4) == 42.0


def test_hop_cache_built_with_another_quantile_is_rebuilt_at_once(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "CACHE_PATH", tmp_path / "hop_times.json")
    events = []
    for i, gap in enumerate([300, 310, 320]):
        start = _wed_5pm(i)
        events.append(_event(start, "67", "A", "Gold_01"))
        events.append(_event(start + timedelta(seconds=gap), "67", "B", "Gold_01"))
    now = _wed_5pm(0) + timedelta(hours=1)
    key = tph._bucket_key("67", "A", "B", 2, 17)
    # A fresh cache from before the setting existed (or from another value) must not be reused until 03:00.
    (tmp_path / "hop_times.json").write_text(
        json.dumps({"refreshed_at": now.isoformat(), "buckets": {key: {"seconds": 310.0, "samples": 3}}})
    )
    assert tph.ensure_hop_time_cache(FakeStorage(events), now=now)["buckets"][key]["seconds"] == 306.6
    # ...and once rebuilt it is reused as before (an empty store would otherwise empty it).
    assert tph.ensure_hop_time_cache(FakeStorage([]), now=now)["buckets"][key]["seconds"] == 306.6


def test_implausible_gap_is_excluded_as_a_layover_not_a_hop():
    start = _wed_5pm(0)
    events = [
        _event(start, "67", "A", "Gold_01"),
        # 2 hour gap -- clearly a layover/pull-in, not one continuous hop.
        _event(start + timedelta(hours=2), "67", "B", "Gold_01"),
    ]
    storage = FakeStorage(events)
    samples = tph.build_hop_time_samples(storage, now=start + timedelta(hours=3))
    key = tph._bucket_key("67", "A", "B", 2, 17)
    assert key not in samples


def test_different_blocks_are_not_treated_as_one_run():
    start = _wed_5pm(0)
    events = [
        _event(start, "67", "A", "Gold_01"),
        _event(start + timedelta(seconds=300), "67", "B", "Gold_02"),  # different block
    ]
    storage = FakeStorage(events)
    samples = tph.build_hop_time_samples(storage, now=start + timedelta(hours=1))
    assert samples == {}


def test_build_hop_time_samples_resolve_stop_id_overrides_recorded_stop_id():
    # Regression coverage for build_eta_model.py: an event's recorded stop_id can be
    # wrong (see StopPoint.route_stop_ids' docstring in headway_tracker.py) -- a
    # resolver keyed off address_id (untouched by that bug) should be used for
    # bucketing instead, with no change to callers that don't pass one.
    start = _wed_5pm(0)
    events = [
        _event(start, "67", "wrong-a", "Gold_01", address_id="addr-a"),
        _event(start + timedelta(seconds=300), "67", "wrong-b", "Gold_01", address_id="addr-b"),
    ]
    storage = FakeStorage(events)
    corrected = {"addr-a": "A", "addr-b": "B"}
    samples = tph.build_hop_time_samples(
        storage,
        now=start + timedelta(hours=1),
        resolve_stop_id=lambda ev: corrected[ev.address_id],
    )
    key = tph._bucket_key("67", "A", "B", 2, 17)
    assert samples[key] == [300.0]
    # Without a resolver, the (wrong) recorded stop_id is bucketed as-is.
    uncorrected = tph.build_hop_time_samples(storage, now=start + timedelta(hours=1))
    wrong_key = tph._bucket_key("67", "wrong-a", "wrong-b", 2, 17)
    assert uncorrected[wrong_key] == [300.0]


def test_build_hop_time_samples_respects_custom_lookback_days():
    # build_eta_model.py passes a much longer lookback than the live 60-day default
    # to reach the full archive -- an event just outside the default LOOKBACK_DAYS
    # must still be found when a longer window is requested.
    old = _wed_5pm(0) - timedelta(days=tph.LOOKBACK_DAYS + 10)
    events = [
        _event(old, "67", "A", "Gold_01"),
        _event(old + timedelta(seconds=300), "67", "B", "Gold_01"),
    ]
    storage = FakeStorage(events)
    key = tph._bucket_key("67", "A", "B", old.weekday(), old.hour)
    assert key not in tph.build_hop_time_samples(storage, now=_wed_5pm(0))
    deep = tph.build_hop_time_samples(storage, now=_wed_5pm(0), lookback_days=tph.LOOKBACK_DAYS + 30)
    assert deep[key] == [300.0]


def test_hop_time_model_lookup_returns_none_for_unknown_bucket():
    model = tph.HopTimeModel(buckets={})
    when = _wed_5pm(0).timestamp()
    assert model.lookup("67", "A", "B", when) is None


def test_hop_time_model_lookup_returns_known_bucket():
    when_dt = _wed_5pm(0)
    key = tph._bucket_key("67", "A", "B", when_dt.weekday(), when_dt.hour)
    model = tph.HopTimeModel(buckets={key: {"seconds": 310.0, "samples": 3}})
    assert model.lookup("67", "A", "B", when_dt.timestamp()) == 310.0


def test_hop_time_model_falls_back_to_deep_model_when_primary_misses():
    when_dt = _wed_5pm(0)
    key = tph._bucket_key("67", "A", "B", when_dt.weekday(), when_dt.hour)
    deep = tph.HopTimeModel(buckets={key: {"seconds": 500.0, "samples": 10}})
    primary = tph.HopTimeModel(buckets={}, fallback=deep)
    assert primary.lookup("67", "A", "B", when_dt.timestamp()) == 500.0


def test_hop_time_model_prefers_primary_over_fallback():
    when_dt = _wed_5pm(0)
    key = tph._bucket_key("67", "A", "B", when_dt.weekday(), when_dt.hour)
    deep = tph.HopTimeModel(buckets={key: {"seconds": 500.0, "samples": 10}})
    primary = tph.HopTimeModel(buckets={key: {"seconds": 310.0, "samples": 3}}, fallback=deep)
    assert primary.lookup("67", "A", "B", when_dt.timestamp()) == 310.0


def test_load_model_wires_the_deep_cache_as_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "CACHE_PATH", tmp_path / "hop_times.json")
    monkeypatch.setattr(tph, "DEEP_CACHE_PATH", tmp_path / "hop_times_deep.json")
    when_dt = _wed_5pm(0)
    deep_key = tph._bucket_key("67", "X", "Y", when_dt.weekday(), when_dt.hour)
    (tmp_path / "hop_times_deep.json").write_text(
        json.dumps({"refreshed_at": when_dt.isoformat(), "buckets": {deep_key: {"seconds": 900.0, "samples": 20}}})
    )
    storage = FakeStorage([])  # empty live archive -- the live 60-day cache stays empty
    model = tph.load_model(storage, now=when_dt + timedelta(hours=1))
    assert model.lookup("67", "X", "Y", when_dt.timestamp()) == 900.0


def test_is_cache_stale_true_when_never_refreshed():
    assert tph.is_cache_stale({}, now=_wed_5pm(0)) is True


def test_is_cache_stale_false_right_after_refresh():
    now = _wed_5pm(0).replace(hour=4)  # after today's 3am threshold
    cache = {"refreshed_at": now.isoformat()}
    assert tph.is_cache_stale(cache, now=now + timedelta(hours=1)) is False


def test_is_cache_stale_true_after_next_days_threshold():
    refreshed = _wed_5pm(0).replace(hour=4)
    next_day_after_threshold = refreshed + timedelta(days=1, hours=1)
    cache = {"refreshed_at": refreshed.isoformat()}
    assert tph.is_cache_stale(cache, now=next_day_after_threshold) is True


def _event_for_vehicle(dt, route_id, stop_id, vehicle_id, block=None):
    ev = _event(dt, route_id, stop_id, block)
    ev.vehicle_id = vehicle_id
    return ev


def test_runs_fall_back_to_vehicle_when_block_is_missing():
    # `block` is empty on almost every recent day in production; consecutive arrivals of
    # the same vehicle on the same route must still make hop samples.
    events = []
    for i, gap in enumerate([300, 310, 320]):
        start = _wed_5pm(i)
        events.append(_event_for_vehicle(start, "57", "A", "12"))
        events.append(_event_for_vehicle(start + timedelta(seconds=gap), "57", "B", "12"))
    samples = tph.build_hop_time_samples(FakeStorage(events), now=_wed_5pm(0) + timedelta(hours=1))
    assert sorted(samples[tph._bucket_key("57", "A", "B", 2, 17)]) == [300.0, 310.0, 320.0]


def test_different_vehicles_without_blocks_are_not_treated_as_one_run():
    start = _wed_5pm(0)
    events = [
        _event_for_vehicle(start, "57", "A", "12"),
        _event_for_vehicle(start + timedelta(seconds=100), "57", "B", "44"),
    ]
    samples = tph.build_hop_time_samples(FakeStorage(events), now=start + timedelta(hours=1))
    assert not samples


def test_events_with_neither_block_nor_vehicle_are_skipped():
    start = _wed_5pm(0)
    events = [
        _event_for_vehicle(start, "57", "A", None),
        _event_for_vehicle(start + timedelta(seconds=100), "57", "B", None),
    ]
    assert not tph.build_hop_time_samples(FakeStorage(events), now=start + timedelta(hours=1))


def test_lookup_falls_back_to_the_other_weekend_day_at_the_same_hour():
    sunday_6pm = tph._bucket_key("57", "A", "B", 6, 18)
    model = tph.HopTimeModel({sunday_6pm: {"seconds": 240.0, "samples": 5}})
    saturday_6pm = datetime(2026, 9, 19, 18, 30, tzinfo=NY_TZ).timestamp()  # a Saturday
    assert model.lookup("57", "A", "B", saturday_6pm) == 240.0


def test_other_day_group_is_only_a_last_resort():
    # Same route + hop on a weekday evening backs a Saturday evening only when nothing
    # in the weekend group is within +/-2h -- and a weekend bucket always wins.
    monday_6pm = tph._bucket_key("57", "A", "B", 0, 18)
    sunday_3pm = tph._bucket_key("57", "A", "B", 6, 15)  # 3 hours away: out of reach
    model = tph.HopTimeModel({monday_6pm: {"seconds": 240.0, "samples": 5}, sunday_3pm: {"seconds": 999.0, "samples": 5}})
    saturday_6pm = datetime(2026, 9, 19, 18, 30, tzinfo=NY_TZ).timestamp()
    assert model.lookup("57", "A", "B", saturday_6pm) == 240.0
    weekend_near = tph._bucket_key("57", "A", "B", 6, 17)
    model = tph.HopTimeModel({monday_6pm: {"seconds": 240.0, "samples": 5}, weekend_near: {"seconds": 300.0, "samples": 5}})
    assert model.lookup("57", "A", "B", saturday_6pm) == 300.0


def test_other_day_group_fallback_is_bounded_to_two_hours_and_the_same_route():
    monday_1pm = tph._bucket_key("57", "A", "B", 0, 13)
    other_route = tph._bucket_key("99", "A", "B", 0, 18)
    model = tph.HopTimeModel({monday_1pm: {"seconds": 240.0, "samples": 5}, other_route: {"seconds": 240.0, "samples": 5}})
    saturday_6pm = datetime(2026, 9, 19, 18, 30, tzinfo=NY_TZ).timestamp()
    assert model.lookup("57", "A", "B", saturday_6pm) is None


def test_exact_weekday_bucket_beats_the_same_group_fallback():
    exact = tph._bucket_key("57", "A", "B", 5, 18)
    other = tph._bucket_key("57", "A", "B", 6, 18)
    model = tph.HopTimeModel({exact: {"seconds": 200.0, "samples": 4}, other: {"seconds": 400.0, "samples": 9}})
    saturday_6pm = datetime(2026, 9, 19, 18, 30, tzinfo=NY_TZ).timestamp()
    assert model.lookup("57", "A", "B", saturday_6pm) == 200.0


def test_lookup_same_group_fallback_uses_the_median_of_the_other_days():
    keys = {tph._bucket_key("57", "A", "B", wd, 9): {"seconds": sec, "samples": 4} for wd, sec in ((0, 100.0), (1, 200.0), (3, 900.0))}
    model = tph.HopTimeModel(keys)
    wednesday_9am = datetime(2026, 9, 9, 9, 30, tzinfo=NY_TZ).timestamp()
    assert model.lookup("57", "A", "B", wednesday_9am) == 200.0


def _sat(hour):
    return datetime(2026, 9, 19, hour, 30, tzinfo=NY_TZ).timestamp()  # a Saturday


def test_lookup_widens_to_neighbouring_hours_within_the_same_day_group():
    # Nothing at Saturday 6pm or Sunday 6pm, but Saturday 5pm has a bucket.
    model = tph.HopTimeModel({tph._bucket_key("57", "A", "B", 5, 17): {"seconds": 300.0, "samples": 4}})
    assert model.lookup("57", "A", "B", _sat(18)) == 300.0


def test_same_hour_other_day_beats_neighbouring_hours():
    model = tph.HopTimeModel({
        tph._bucket_key("57", "A", "B", 6, 18): {"seconds": 250.0, "samples": 4},
        tph._bucket_key("57", "A", "B", 5, 17): {"seconds": 999.0, "samples": 9},
    })
    assert model.lookup("57", "A", "B", _sat(18)) == 250.0


def test_lookup_widening_stops_at_two_hours():
    model = tph.HopTimeModel({tph._bucket_key("57", "A", "B", 5, 15): {"seconds": 300.0, "samples": 4}})
    assert model.lookup("57", "A", "B", _sat(18)) is None
    assert model.lookup("57", "A", "B", _sat(17)) == 300.0  # 2 hours away


def test_neighbouring_hours_never_cross_midnight():
    model = tph.HopTimeModel({tph._bucket_key("57", "A", "B", 5, 23): {"seconds": 300.0, "samples": 4}})
    early = datetime(2026, 9, 19, 0, 30, tzinfo=NY_TZ).timestamp()
    assert model.lookup("57", "A", "B", early) is None


def test_drive_and_dwell_treats_repeat_arrivals_at_one_stop_as_one_visit():
    # A bus staged at A is logged arrive/depart/arrive/depart; the hold is first arrival -> last
    # departure (200 s) and the drive is last departure -> arrival at B (40 s).
    t0 = _wed_5pm(0)
    events = [
        _event(t0, "74", "A", "blk"),
        _event(t0 + timedelta(seconds=60), "74", "A", "blk", event_type="departure"),
        _event(t0 + timedelta(seconds=70), "74", "A", "blk"),
        _event(t0 + timedelta(seconds=200), "74", "A", "blk", event_type="departure"),
        _event(t0 + timedelta(seconds=240), "74", "B", "blk"),
        _event(t0 + timedelta(seconds=260), "74", "B", "blk", event_type="departure"),
    ]
    drive, dwell = tph.build_drive_and_dwell_samples(FakeStorage(events), now=t0 + timedelta(hours=1))
    assert dwell[tph._bucket_key("74", "A", tph.DWELL_KEY, 2, 17)] == [200.0]
    assert dwell[tph._bucket_key("74", "B", tph.DWELL_KEY, 2, 17)] == [20.0]
    assert drive == {tph._bucket_key("74", "A", "B", 2, 17): [40.0]}


def test_drive_and_dwell_needs_a_departure_to_time_a_visit():
    t0 = _wed_5pm(0)
    events = [_event(t0, "74", "A", "blk"), _event(t0 + timedelta(seconds=90), "74", "B", "blk")]
    drive, dwell = tph.build_drive_and_dwell_samples(FakeStorage(events), now=t0 + timedelta(hours=1))
    assert not drive and not dwell


def test_load_drive_dwell_models_builds_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "DRIVE_DWELL_CACHE_PATH", tmp_path / "dd.json")
    monkeypatch.setattr(tph, "_drive_dwell_memo", {})
    events = []
    for week in range(3):
        t0 = _wed_5pm(week)
        events += [
            _event(t0, "74", "A", f"blk{week}"),
            _event(t0 + timedelta(seconds=100 * (week + 1)), "74", "A", f"blk{week}", event_type="departure"),
            _event(t0 + timedelta(seconds=100 * (week + 1) + 40), "74", "B", f"blk{week}"),
        ]
    now = _wed_5pm(0) + timedelta(hours=1)
    drive, dwell, _ = tph.load_drive_dwell_models(FakeStorage(events), now=now)
    when = _wed_5pm(0).timestamp()
    assert drive.lookup("74", "A", "B", when) == 40.0
    assert dwell.lookup("74", "A", when) == 200.0  # 40th percentile of 100/200/300
    assert (tmp_path / "dd.json").exists()
    drive2, _, _ = tph.load_drive_dwell_models(FakeStorage([]), now=now)  # fresh cache: not rebuilt from the empty store
    assert drive2.lookup("74", "A", "B", when) == 40.0
    monkeypatch.setattr(tph, "_drive_dwell_memo", {})  # e.g. after a restart: read back from the file
    drive3, _, _ = tph.load_drive_dwell_models(FakeStorage([]), now=now)
    assert drive3.lookup("74", "A", "B", when) == 40.0


def test_timestop_drive_model_reads_the_low_side_of_each_drive_bucket(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "DRIVE_DWELL_CACHE_PATH", tmp_path / "dd.json")
    monkeypatch.setattr(tph, "_drive_dwell_memo", {})
    events = []
    for week, drive_s in enumerate([200, 300, 400, 500, 600]):
        t0 = _wed_5pm(week)
        events += [
            _event(t0, "58", "PIN", f"blk{week}"),
            _event(t0 + timedelta(seconds=60), "58", "PIN", f"blk{week}", event_type="departure"),
            _event(t0 + timedelta(seconds=60 + drive_s), "58", "MAD", f"blk{week}"),
        ]
    now = _wed_5pm(0) + timedelta(hours=1)
    drive, _, timestop_drive = tph.load_drive_dwell_models(FakeStorage(events), now=now)
    when = _wed_5pm(0).timestamp()
    assert drive.lookup("58", "PIN", "MAD", when) == 400.0  # median, as dwell-mode routes use it
    assert timestop_drive.lookup("58", "PIN", "MAD", when) == 300.0  # 25th percentile, for the hop cap


def test_drive_dwell_cache_without_the_low_side_is_rebuilt(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "DRIVE_DWELL_CACHE_PATH", tmp_path / "dd.json")
    monkeypatch.setattr(tph, "_drive_dwell_memo", {})
    now = _wed_5pm(0) + timedelta(hours=1)
    key = tph._bucket_key("58", "PIN", "MAD", 2, 17)
    (tmp_path / "dd.json").write_text(json.dumps(
        {"refreshed_at": now.isoformat(), "drive": {key: {"seconds": 400.0, "samples": 5}}, "dwell": {}}
    ))
    tph.load_drive_dwell_models(FakeStorage([]), now=now)
    assert json.loads((tmp_path / "dd.json").read_text())["quantiles"] == [tph.TIMESTOP_DRIVE_QUANTILE, tph.DWELL_QUANTILE]


# Stops on a line running east: LIB and CHP face each other (10 m apart), NEXT is 300 m on.
_COORDS = {"PREV": (38.0, -78.004), "LIB": (38.0, -78.0), "CHP": (38.00009, -78.0), "NEXT": (38.0, -77.99658)}


def _visit(t, stop, dwell_s=30, arrival_type="stopped"):
    arr = _event(t, "57", stop, "run")
    arr.arrival_type = arrival_type
    return [arr, _event(t + timedelta(seconds=dwell_s), "57", stop, "run", event_type="departure")]


def test_phantom_arrival_at_the_facing_stop_is_dropped(monkeypatch):
    monkeypatch.setattr(tph, "_stop_coords", dict(_COORDS))
    t = _wed_5pm(0)
    events = (_visit(t, "PREV") + _visit(t + timedelta(seconds=120), "LIB")
              + _visit(t + timedelta(seconds=120), "CHP", arrival_type="route_activation")
              + _visit(t + timedelta(seconds=240), "NEXT"))
    events.sort(key=lambda e: e.timestamp)
    kept = [(e.stop_id, e.event_type) for e in tph.drop_phantom_route_activations(events)]
    assert ("CHP", "arrival") not in kept and ("CHP", "departure") not in kept
    assert [s for s, kind in kept if kind == "arrival"] == ["PREV", "LIB", "NEXT"]

    samples = tph.build_hop_time_samples(FakeStorage(events), now=t + timedelta(hours=1))
    assert tph._bucket_key("57", "PREV", "LIB", 2, 17) in samples
    assert tph._bucket_key("57", "LIB", "NEXT", 2, 17) in samples
    assert not any("|CHP|" in k for k in samples)


def test_route_activation_at_the_next_stop_is_kept(monkeypatch):
    # 300 m on and 40 s later: a real stop whose bubble #1 was missed, not a phantom.
    monkeypatch.setattr(tph, "_stop_coords", dict(_COORDS))
    t = _wed_5pm(0)
    events = _visit(t, "LIB", dwell_s=20) + _visit(t + timedelta(seconds=40), "NEXT", arrival_type="route_activation")
    assert tph.drop_phantom_route_activations(events) == events


def test_without_stop_positions_nothing_is_dropped(monkeypatch):
    monkeypatch.setattr(tph, "_stop_coords", {})
    t = _wed_5pm(0)
    events = _visit(t, "LIB") + _visit(t, "CHP", arrival_type="route_activation")
    assert tph.drop_phantom_route_activations(events) == events


def test_stop_positions_are_merged_and_saved_not_replaced(tmp_path, monkeypatch):
    # TransLoc only lists running routes; the 03:00 rebuild must still know daytime stops.
    path = tmp_path / "coords.json"
    monkeypatch.setattr(tph, "STOP_COORDS_PATH", path)
    monkeypatch.setattr(tph, "_stop_coords", None)
    tph.set_stop_coords({"LIB": _COORDS["LIB"]})
    tph.set_stop_coords({"CHP": _COORDS["CHP"]})
    assert set(tph._known_stop_coords()) == {"LIB", "CHP"}
    monkeypatch.setattr(tph, "_stop_coords", None)  # e.g. after a restart
    assert set(tph._known_stop_coords()) == {"LIB", "CHP"}


# --- shared (AddressID) history: routes borrow hops other routes drove over the same stops ---

def _shared_run(t0, route, stops, gaps, block, dwell_s=20):
    """One run over (route_stop_id, address_id) pairs: arrive, dwell, leave, `gaps` seconds arrival to arrival."""
    events, t = [], t0
    for (stop, addr), gap in zip(stops, gaps + [0]):
        events.append(_event(t, route, stop, block, address_id=addr))
        events.append(_event(t + timedelta(seconds=dwell_s), route, stop, block, event_type="departure", address_id=addr))
        t += timedelta(seconds=gap)
    return events


def _old_route_history(gaps=(100, 110, 120), dwell_s=20):
    # Route 57 drove stop 820 (address 7) -> 821 (address 8) on three Wednesdays at 5 pm.
    events = []
    for week, gap in enumerate(gaps):
        events += _shared_run(_wed_5pm(week), "57", [("820", "7"), ("821", "8")], [gap], f"b{week}", dwell_s)
    return events


def test_new_route_borrows_the_shared_hop_for_the_same_stops(monkeypatch):
    monkeypatch.setattr(tph, "_stop_addresses", {})
    samples = tph.build_hop_time_samples(FakeStorage(_old_route_history()), now=_wed_5pm(0) + timedelta(hours=1))
    buckets = {k: {"seconds": tph._quantile(v, 0.5), "samples": len(v)} for k, v in samples.items() if len(v) >= 3}
    assert buckets[tph._shared_key("7", "8", 2, 17)]["seconds"] == 110.0
    # Route 67 is brand new: different RouteStopIDs (901/902) for the same two stops.
    model = tph.HopTimeModel(buckets, addresses={"67|901": "7", "67|902": "8"})
    assert model.lookup("67", "901", "902", _wed_5pm(0).timestamp()) == 110.0
    assert tph.HopTimeModel(buckets).lookup("67", "901", "902", _wed_5pm(0).timestamp()) is None  # no table, no borrowing


def test_a_layover_stays_out_of_the_shared_pool(monkeypatch):
    monkeypatch.setattr(tph, "_stop_addresses", {})
    events = _old_route_history(gaps=(300, 310, 320), dwell_s=tph.SHARED_MAX_DWELL_S + 60)
    samples = tph.build_hop_time_samples(FakeStorage(events), now=_wed_5pm(0) + timedelta(hours=1))
    assert len(samples[tph._bucket_key("57", "820", "821", 2, 17)]) == 3  # the route keeps its own
    assert tph._shared_key("7", "8", 2, 17) not in samples


def test_shared_address_resolves_a_two_stop_address_list(monkeypatch):
    monkeypatch.setattr(tph, "_stop_addresses", {"57|821": "8"})  # from TransLoc's route stop list
    events = _old_route_history()
    for ev in events:
        if ev.stop_id == "821" and ev.timestamp.date() == _wed_5pm(0).date():
            ev.address_id = "8,15"  # the tracker sometimes adds a neighbouring stop
    samples = tph.build_hop_time_samples(FakeStorage(events), now=_wed_5pm(0) + timedelta(hours=1))
    assert len(samples[tph._shared_key("7", "8", 2, 17)]) == 3


def test_own_route_history_wins_within_a_time_window():
    when = _wed_5pm(0).timestamp()
    buckets = {
        tph._bucket_key("67", "901", "902", 2, 17): {"seconds": 90.0, "samples": 3},
        tph._shared_key("7", "8", 2, 17): {"seconds": 150.0, "samples": 30},
    }
    model = tph.HopTimeModel(buckets, addresses={"67|901": "7", "67|902": "8"})
    assert model.lookup("67", "901", "902", when) == 90.0


def test_time_of_day_beats_route_specific_history():
    # The route only has 4 pm; the shared road history has this exact weekday and hour: use the hour.
    when = _wed_5pm(0).timestamp()
    buckets = {
        tph._bucket_key("67", "901", "902", 2, 16): {"seconds": 90.0, "samples": 3},
        tph._shared_key("7", "8", 2, 17): {"seconds": 150.0, "samples": 30},
    }
    model = tph.HopTimeModel(buckets, addresses={"67|901": "7", "67|902": "8"})
    assert model.lookup("67", "901", "902", when) == 150.0


def test_shared_drive_samples_back_the_timestop_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(tph, "DRIVE_DWELL_CACHE_PATH", tmp_path / "dd.json")
    monkeypatch.setattr(tph, "_drive_dwell_memo", {})
    monkeypatch.setattr(tph, "_stop_addresses", {"67|901": "7", "67|902": "8"})
    now = _wed_5pm(0) + timedelta(hours=1)
    drive, _, cap = tph.load_drive_dwell_models(FakeStorage(_old_route_history()), now=now)
    # drive = arrival gap - 20 s dwell: 80/90/100
    assert drive.lookup("67", "901", "902", _wed_5pm(0).timestamp()) == 90.0
    assert cap.lookup("67", "901", "902", _wed_5pm(0).timestamp()) == 85.0


def test_stop_addresses_are_merged_and_saved(tmp_path, monkeypatch):
    path = tmp_path / "addr.json"
    monkeypatch.setattr(tph, "STOP_ADDRESSES_PATH", path)
    monkeypatch.setattr(tph, "_stop_addresses", None)
    tph.set_stop_addresses({"57|820": 7})
    tph.set_stop_addresses({"67|901": "7"})
    assert json.loads(path.read_text()) == {"57|820": "7", "67|901": "7"}


def test_shared_history_never_crosses_weekday_and_weekend():
    when = _wed_5pm(0).timestamp()
    sunday_5pm = {tph._shared_key("7", "8", 6, 17): {"seconds": 60.0, "samples": 30}}
    model = tph.HopTimeModel(sunday_5pm, addresses={"67|901": "7", "67|902": "8"})
    assert model.lookup("67", "901", "902", when) is None
    # ...while the route's OWN weekend history is still a last resort, as before.
    own_sunday = {tph._bucket_key("67", "901", "902", 6, 17): {"seconds": 60.0, "samples": 3}}
    assert tph.HopTimeModel(own_sunday).lookup("67", "901", "902", when) == 60.0
