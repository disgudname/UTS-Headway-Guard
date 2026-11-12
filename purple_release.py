"""Purple Line anti-bunching release: when a bus staged at Lower JPA / Lane will leave.

Purple has no block package. Its written anti-bunching rules say a bus waiting at a staging stop
leaves when the bus ahead of it reaches a set stop: Lower JPA (Jefferson Park Ave @ West Complex)
when the bus ahead gets to Scott Stadium (06h) or Fontaine (07-13h), Lane Rd @ MR-5 when it gets to Lee St
(afternoons). Drivers follow it loosely, but well enough to beat the dwell history: over the
weekday headway logs 2026-08-20..09-30, a staged bus left a median 0-90 s after the bus ahead
reached the trigger (middle half within ~1-2 min), and predicting the departure that way was off
by 40-65 s against 57-98 s for the typical hold. The per-hour lags in config/purple_release.json
are those measured medians minus 30 s; hours where the rule did no better (early morning, the
evening, PM holds at Lower JPA) have no rule and keep the dwell history. 06h uses the bus ahead
at Scott Stadium (route 72; measured -35 s, 30 s vs 114 s error on the late-September half). No
October log covers 06h, so that hour was not replayed.

Offline replay of the October weekday logs (scripts/eta_replay_purple.py, history before 10-01),
on predictions from the buses it released: median |error| 89 -> 71 s, within 2 min 59 -> 68%,
>2 min late 2.6 -> 5.7% (with the lags as measured: 68 s, 70%, 9.1%). The old dwell history
read staged buses ~75 s too slow. Tried and dropped: a "second bus pulled in, first one goes"
rule at Fontaine / Lane (0-15 s better than the elapsed-dwell history it would replace), and
Fontaine PM triggers (only fit 12-14 min ahead, too far to predict the bus ahead).

Fontaine (Ray C. Hunt Dr @ 400 Fontaine Bldg) is a spacing hold, not a trigger-stop one (`gap_rules`,
`gap_holds`): a bus leaves there at the later of arrival + min_dwell_s and gap_s after the bus ahead
left, and since only two buses fit, the first goes about cap_lag_s after a third pulls in. Measured
over seven weekday prod logs 11-04..11-11 (168 visits): a bus arriving with the bus ahead still there
sat a median 5.8 min, one arriving 3+ min after it left about 1 min (correlation -0.66); held buses
left a median 270 s behind the bus ahead (240 s 14-16h, 345-375 s mornings). The history's flat
~3 min was off a median 117 s; the rule 55-60 s. This applies to buses on their way to Fontaine
too (within GAP_HORIZON_S), not only to the one sitting there, because most of Purple's misses were
predictions through the Fontaine hold from a bus still driving toward it. Two buses fill the stop
(user, 2026-11-12); a third pulling in pushed the first out ~30 s later on all 3 occasions logged.
Replay (scripts/eta_replay_purple.py, history before 10-01), gap_s 210 / min_dwell_s 60: tuned on
the November logs, Purple >2 min late 12.0 -> 10.8%, median |error| 105 -> 103 s; held out on the
20 weekday 08:30/12:30/15:00/17:00 logs 10-19..10-23, 10.8 -> 9.8% (on predictions the rule
touched 10.7 -> 8.6%), median |error| about the same. Larger gaps (240-300 s) cut the |error| more
in November but put more buses ahead of their ETA.

The bus ahead is found from our own ETAs: of the other buses that will reach the trigger before
they next reach the staging stop (they have left it and are on their way), the one furthest from
the trigger. A second bus staged behind the first follows the first one's (already adjusted) time.
"""
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
CONFIG_PATH = Path(__file__).resolve().parent / "config" / "purple_release.json"
# A release this far out is not a staging hold any more (bus ahead stuck, GPS frozen): use the history.
MAX_RELEASE_S = 15 * 60.0


@dataclass(frozen=True)
class Rule:
    stage: str
    trigger: str
    first_hour: int
    last_hour: int
    offset_s: float


@dataclass(frozen=True)
class GapRule:
    stage: str
    first_hour: int
    last_hour: int
    gap_s: float
    min_dwell_s: float
    capacity: int = 2
    cap_lag_s: float = 30.0


@dataclass
class Bus:
    """One Purple bus. `eta` is (stop, release) -> seconds from now or None, release being an optional
    (stop id, epoch) this bus is held to at its staging stop."""
    vid: str
    stops: List[Any]                 # the bus's line's stops (need .id and .name)
    at_stop_id: Optional[str]        # stop it is sitting at, if any
    since: float                     # epoch it started sitting there
    eta: Callable[[Any, Optional[Tuple[str, float]]], Optional[float]]


_rules_cache: Optional[List[Rule]] = None


GAP_HORIZON_S = 15 * 60.0  # buses due at the gap stop within this are queued for it
LAST_DEPARTURE_MAX_AGE_S = 30 * 60.0  # older than this, the bus ahead's departure is not known
DEPARTURE_MAX_GAP_S = 120.0  # a departure seen across a longer gap between calls is not recorded
_gap_rules_cache: Optional[List[GapRule]] = None
# stage keyword -> {"at": {vid: (since, last seen there)}, "left": epoch the last bus left}
_gap_state: Dict[str, Dict[str, Any]] = {}


def load_gap_rules(path: Path = CONFIG_PATH) -> List[GapRule]:
    global _gap_rules_cache
    if _gap_rules_cache is None or path != CONFIG_PATH:
        try:
            raw = json.loads(path.read_text(encoding="utf-8")).get("gap_rules", [])
        except (OSError, json.JSONDecodeError):
            raw = []
        rules = [GapRule(r["stage"], int(r["hours"][0]), int(r["hours"][1]), float(r["gap_s"]), float(r["min_dwell_s"]),
                         int(r.get("capacity", 2)), float(r.get("cap_lag_s", 30.0))) for r in raw]
        if path != CONFIG_PATH:
            return rules
        _gap_rules_cache = rules
    return _gap_rules_cache


def load_rules(path: Path = CONFIG_PATH) -> List[Rule]:
    global _rules_cache
    if _rules_cache is None or path != CONFIG_PATH:
        try:
            raw = json.loads(path.read_text(encoding="utf-8")).get("rules", [])
        except (OSError, json.JSONDecodeError):
            raw = []
        rules = [Rule(r["stage"], r["trigger"], int(r["hours"][0]), int(r["hours"][1]), float(r["offset_s"])) for r in raw]
        if path != CONFIG_PATH:
            return rules
        _rules_cache = rules
    return _rules_cache


def active_rules(rules: List[Any], now: float) -> List[Any]:
    local = datetime.fromtimestamp(now, NY_TZ)
    if local.weekday() >= 5:
        return []
    return [r for r in rules if r.first_hour <= local.hour <= r.last_hour]


def _matching(stops: List[Any], keyword: str) -> List[Any]:
    return [s for s in stops if keyword.lower() in (getattr(s, "name", "") or "").lower()]


def _min_eta(bus: Bus, stops: List[Any], release: Optional[Tuple[str, float]] = None) -> Optional[float]:
    etas = [e for e in (bus.eta(s, release) for s in stops) if e is not None]
    return min(etas) if etas else None


def release_epochs(buses: List[Bus], now: float, rules: Optional[List[Rule]] = None) -> Dict[str, Tuple[str, float]]:
    """vid -> (staging stop id on its line, epoch it is expected to leave) for every bus sitting at a
    staging stop with a rule in force now and a bus ahead to follow."""
    rules = active_rules(load_rules() if rules is None else rules, now)
    out: Dict[str, Tuple[str, float]] = {}
    for rule in rules:
        staged: List[Tuple[Bus, Any]] = []
        for bus in buses:
            for stop in _matching(bus.stops, rule.stage):
                if bus.at_stop_id is not None and str(stop.id) == bus.at_stop_id:
                    staged.append((bus, stop))
        if not staged:
            continue
        staged.sort(key=lambda bs: bs[0].since)
        staged_vids = {b.vid for b, _ in staged}
        # Buses on their way from the staging stop to the trigger: seconds to the trigger.
        ahead: List[float] = []
        for bus in buses:
            if bus.vid in staged_vids:
                continue
            triggers, stage_stops = _matching(bus.stops, rule.trigger), _matching(bus.stops, rule.stage)
            if not triggers or not stage_stops:
                continue
            t, g = _min_eta(bus, triggers), _min_eta(bus, stage_stops)
            if t is not None and g is not None and t < g:
                ahead.append(t)
        leader_t = max(ahead) if ahead else None  # the one that left the staging stop last
        for bus, stop in staged:
            if leader_t is None:
                break
            release = now + leader_t + rule.offset_s
            if release - now > MAX_RELEASE_S:
                break
            out[bus.vid] = (str(stop.id), release)
            # The next bus in the queue follows this one.
            leader_t = _min_eta(bus, _matching(bus.stops, rule.trigger), out[bus.vid])
    return out


def gap_holds(buses: List[Bus], now: float, rules: Optional[List[GapRule]] = None,
              releases: Optional[Dict[str, Tuple[str, float]]] = None,
              state: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Tuple[str, float]]:
    """vid -> (gap stop id on its line, epoch it is expected to leave) for every bus sitting at, or due
    within GAP_HORIZON_S at, a gap-rule stop. Call once per poll, in time order: it remembers when
    each bus left (`state`, module-wide by default), and the first bus in the queue follows that.
    `releases` (release_epochs) are used for buses staged elsewhere on their way here."""
    rules = active_rules(load_gap_rules() if rules is None else rules, now)
    state = _gap_state if state is None else state
    releases = releases or {}
    out: Dict[str, Tuple[str, float]] = {}
    for rule in rules:
        st = state.setdefault(rule.stage, {"at": {}, "left": None})
        visible = {b.vid for b in buses}
        here: Dict[str, Tuple[Bus, Any]] = {}
        for bus in buses:
            for stop in _matching(bus.stops, rule.stage):
                if bus.at_stop_id is not None and str(stop.id) == bus.at_stop_id:
                    here[bus.vid] = (bus, stop)
        for vid in list(st["at"]):
            if vid not in here:
                last_seen = st["at"][vid][1]
                # Pulled out somewhere between the last two looks (a bus that vanished from the feed did not;
                # after a long gap between calls the time is not known).
                if vid in visible and now - last_seen <= DEPARTURE_MAX_GAP_S:
                    left = (last_seen + now) / 2.0
                    st["left"] = left if st["left"] is None else max(st["left"], left)
                del st["at"][vid]
        for vid, (bus, _) in here.items():
            st["at"][vid] = (st["at"][vid][0] if vid in st["at"] else bus.since, now)
        queue: List[Tuple[float, Bus, Any, bool]] = [(st["at"][vid][0], b, s, True) for vid, (b, s) in here.items()]
        for bus in buses:
            if bus.vid in here:
                continue
            stops = _matching(bus.stops, rule.stage)
            if not stops:
                continue
            e = _min_eta(bus, stops, releases.get(bus.vid))
            if e is not None and 0 < e <= GAP_HORIZON_S:
                stop = min(stops, key=lambda s: bus.eta(s, releases.get(bus.vid)) or 9e9)
                queue.append((now + e, bus, stop, False))
        queue.sort(key=lambda q: q[0])
        prev = st["left"]
        if prev is None or now - prev > LAST_DEPARTURE_MAX_AGE_S:
            continue  # the bus ahead's departure is not known (startup, or a long gap): keep the history
        for k, (arr, bus, stop, sitting) in enumerate(queue):
            dep = max(arr + rule.min_dwell_s, prev + rule.gap_s)
            # Only `capacity` buses fit: the first one goes once the one `capacity` places behind pulls in.
            j = k + rule.capacity
            if j < len(queue) and queue[j][0] < dep:
                dep = max(arr + 20.0, queue[j][0] + rule.cap_lag_s)
            if sitting:
                dep = max(dep, now)
            if dep - now > MAX_RELEASE_S:
                break
            out[bus.vid] = (str(stop.id), dep)
            prev = dep
    return out

