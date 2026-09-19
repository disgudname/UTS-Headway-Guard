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
    assert cache["buckets"][key]["seconds"] == 310.0  # median of 300/310/320
    assert cache["buckets"][key]["samples"] == 3


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
