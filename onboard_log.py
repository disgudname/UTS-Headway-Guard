"""Log of what live /onboard signs were shown, so it can be scored against what the bus did
(scripts/onboard_score.py).

Every live GET /v1/onboard writes one line: the bus, where it was (TransLoc's fix and the tablet's own,
if sent), the stop the sign says it is at, and the ETAs it was given. Replays (?at=) are not logged.
Two tabs on the same bus would double the lines, so a bus is logged at most once per MIN_GAP_S.

One file per service day in `<data dir>/onboard_log/YYYY-MM-DD.jsonl`. A service day runs to 04:00
local, so a night bus stays in one file. Kept for KEEP_DAYS.
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
KEEP_DAYS = 60
MIN_GAP_S = 4.0
MAX_STOPS = 10
SERVICE_DAY_ROLLOVER_H = 4


def service_day(ts: float) -> str:
    return (datetime.fromtimestamp(ts, NY_TZ) - timedelta(hours=SERVICE_DAY_ROLLOVER_H)).strftime("%Y-%m-%d")


def _r(x: Any, nd: int) -> Any:
    return round(float(x), nd) if x is not None else None


def build_record(
    out: Dict[str, Any], now: float, veh: Any = None,
    device: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """One log line from a /v1/onboard answer. `veh` is the TransLoc Vehicle (or None), `device` the
    tablet's query params {lat, lon, heading, speed} (all None when it sent none)."""
    rec: Dict[str, Any] = {"t": round(now, 1), "bus": out.get("bus"), "status": out.get("status")}
    if veh is not None:
        rec["veh"] = str(veh.id)
        rec["tl"] = [_r(veh.lat, 6), _r(veh.lon, 6), _r(veh.ground_mps, 1), _r(getattr(veh, "age_s", None), 1)]
    if device and device.get("lat") is not None and device.get("lon") is not None:
        rec["dev"] = [_r(device["lat"], 6), _r(device["lon"], 6), _r(device.get("heading"), 0), _r(device.get("speed"), 1)]
    pos = out.get("position") or {}
    if pos.get("source"):
        rec["src"] = pos["source"]
    if pos.get("device_offset_m") is not None:
        rec["off_m"] = pos["device_offset_m"]
    if out.get("status") != "in_service":
        return rec
    rec["route"] = (out.get("route") or {}).get("id")
    rec["block"] = out.get("block")
    at = out.get("at_stop")
    if at:
        rec["at"] = [at.get("id"), bool(at.get("stopped"))]
    rec["eta"] = [[s.get("id"), s.get("seconds")] for s in (out.get("stops") or [])[:MAX_STOPS]]
    return rec


class OnboardLog:
    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self._last_by_bus: Dict[str, float] = {}
        self._pruned_for: Optional[str] = None

    def _path(self, day: str) -> Path:
        return self.directory / f"{day}.jsonl"

    def record(self, rec: Dict[str, Any]) -> bool:
        """Append one line unless this bus was logged under MIN_GAP_S ago. Returns whether it wrote."""
        now = float(rec["t"])
        bus = str(rec.get("bus") or "")
        last = self._last_by_bus.get(bus)
        if last is not None and 0.0 <= now - last < MIN_GAP_S:
            return False
        self._last_by_bus[bus] = now
        day = service_day(now)
        self.directory.mkdir(parents=True, exist_ok=True)
        with self._path(day).open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        if self._pruned_for != day:
            self._pruned_for = day
            self.prune(now)
        return True

    def prune(self, now: float) -> None:
        cutoff = (datetime.fromtimestamp(now, NY_TZ) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
        for p in self.directory.glob("*.jsonl"):
            if p.stem < cutoff:
                try:
                    p.unlink()
                except OSError:
                    pass

    def read(self, day: str) -> List[Dict[str, Any]]:
        path = self._path(day)
        if not path.exists():
            return []
        out = []
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out
