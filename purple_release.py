"""Purple Line anti-bunching release: when a bus staged at Lower JPA / Lane will leave.

Purple has no block package. Its written anti-bunching rules say a bus waiting at a staging stop
leaves when the bus ahead of it reaches a set stop: Lower JPA (Jefferson Park Ave @ West Complex)
when the bus ahead gets to Fontaine (mornings, middays), Lane Rd @ MR-5 when it gets to Lee St
(afternoons). Drivers follow it loosely, but well enough to beat the dwell history: over the
weekday headway logs 2026-08-20..09-30, a staged bus left a median 0-90 s after the bus ahead
reached the trigger (middle half within ~1-2 min), and predicting the departure that way was off
by 40-65 s against 57-98 s for the typical hold. The per-hour lags in config/purple_release.json
are those measured medians minus 30 s; hours where the rule did no better (early morning, the
evening, PM holds at Lower JPA) have no rule and keep the dwell history. 06h would fit (bus ahead
at Scott Stadium, -35 s) but route 72's Scott Stadium stop has no position on the shape.

Offline replay of the October weekday logs (scripts/eta_replay_purple.py, history before 10-01),
on predictions from the buses it released: median |error| 89 -> 71 s, within 2 min 59 -> 68%,
>2 min late 2.6 -> 5.7% (with the lags as measured: 68 s, 70%, 9.1%). The old dwell history
read staged buses ~75 s too slow. Tried and dropped: a "second bus pulled in, first one goes"
rule at Fontaine / Lane (0-15 s better than the elapsed-dwell history it would replace), and
Fontaine PM triggers (only fit 12-14 min ahead, too far to predict the bus ahead).

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


def active_rules(rules: List[Rule], now: float) -> List[Rule]:
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
