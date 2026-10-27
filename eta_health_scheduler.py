"""Runs the scheduled ETA health checks on prod (HANDOFF.md section 7).

They ran from Task Scheduler on the home server until 2026-10-26, when the user needed that machine back
(and it had been off all that Monday, so the day went unlogged). Same slots, same script
(scripts/eta_health_check.py), now started from app.py's startup as a low-priority child process so scoring
never holds up the event loop. Logs and health_results.jsonl go to /data/eta_watch/.

A slot's run is named by its scheduled start (`--start`), so a deploy that kills it mid-way just starts it again
on the same log and the run keeps its start time and length (which is how eta_health_check.diagnose finds
earlier runs of the same slot). A slot is done once health_results.jsonl has a line with its start time.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
WEEKDAYS = (0, 1, 2, 3, 4)
WEEKEND = (5, 6)
EVERY_DAY = WEEKDAYS + WEEKEND

# (days the slot starts on, start HH:MM Eastern, minutes) -- copied from the home server's ETA-Health-* tasks.
SLOTS: List[Tuple[Tuple[int, ...], str, int]] = [
    (EVERY_DAY, "01:30", 30),
    (WEEKDAYS, "05:00", 210),
    (WEEKDAYS, "08:30", 240),
    (WEEKDAYS, "12:30", 150),
    (WEEKDAYS, "15:00", 120),
    (WEEKDAYS, "17:00", 90),
    (WEEKDAYS, "17:30", 60),   # route-change run (ETA-RouteChange-Weekdays)
    (WEEKDAYS, "19:30", 30),
    (WEEKEND, "07:30", 30),
    (WEEKEND, "11:30", 60),
    (WEEKEND, "14:00", 30),
    (WEEKEND, "17:00", 30),
    (WEEKEND, "20:00", 30),
    (EVERY_DAY, "21:45", 165),
]
POLL_S = 15
CHECK_EVERY_S = 30
# After the watch window, a run cut off while scoring is still restarted (it scores the log it has) this long.
SCORE_GRACE_MIN = 20
MAX_LAUNCHES = 3          # per slot per process: a check that keeps crashing must not spam the phone
KEEP_DAYS = 30            # run logs older than this are deleted (health_results.jsonl is kept)

REPO = Path(__file__).resolve().parent
SCRIPT = REPO / "scripts" / "eta_health_check.py"


def enabled() -> bool:
    return os.getenv("ETA_HEALTH_CHECKS", "").strip().lower() in ("1", "true", "yes", "on")


def due_slots(now: datetime) -> List[Tuple[datetime, int]]:
    """(scheduled start, minutes) of every slot whose watch window or scoring grace contains `now`."""
    out = []
    for back in (0, 1):  # 21:45 runs past midnight, so yesterday's slots can still be open
        day = (now - timedelta(days=back)).date()
        for days, hhmm, minutes in SLOTS:
            if day.weekday() not in days:
                continue
            h, m = map(int, hhmm.split(":"))
            start = datetime(day.year, day.month, day.day, h, m, tzinfo=ET)
            if start <= now < start + timedelta(minutes=minutes + SCORE_GRACE_MIN):
                out.append((start, minutes))
    return out


def finished_starts(results: Path) -> Set[str]:
    """Start times ("YYYY-MM-DDTHH:MM") that already have a result line."""
    try:
        return {json.loads(l).get("when", "")[:16] for l in results.read_text(encoding="utf-8").splitlines() if l.strip()}
    except FileNotFoundError:
        return set()


def prune(out_dir: Path, keep_days: int = KEEP_DAYS) -> None:
    cutoff = time.time() - keep_days * 86400
    for p in out_dir.glob("2*.jsonl*"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except OSError:
            pass


async def run_forever(data_dir: Path, port: str) -> None:
    out_dir = data_dir / "eta_watch"
    out_dir.mkdir(parents=True, exist_ok=True)
    results = out_dir / "health_results.jsonl"
    # TZ: the machine runs on UTC, and the scripts name logs and print bus times in local (Eastern) time.
    env = {**os.environ, "ETA_WATCH_DIR": str(out_dir), "ETA_WATCH_BASE": f"http://127.0.0.1:{port}",
           "TZ": "America/New_York"}
    running: Dict[str, asyncio.subprocess.Process] = {}
    launches: Dict[str, int] = {}
    pruned_on = None
    await asyncio.sleep(60)  # let the updater fill state before the first poll
    while True:
        try:
            for key, proc in list(running.items()):
                if proc.returncode is not None:
                    del running[key]
            now = datetime.now(ET)
            done = finished_starts(results)
            for start, minutes in due_slots(now):
                key = start.strftime("%Y-%m-%dT%H:%M")
                if key in done or key in running or launches.get(key, 0) >= MAX_LAUNCHES:
                    continue
                log = out_dir / f"{start:%Y%m%d-%H%M}.jsonl"
                if now >= start + timedelta(minutes=minutes) and not log.exists():
                    continue  # missed the whole window: nothing to score
                launches[key] = launches.get(key, 0) + 1
                with open(out_dir / f"{start:%Y%m%d-%H%M}.out", "ab") as out:
                    running[key] = await asyncio.create_subprocess_exec(
                        "nice", "-n", "19", sys.executable, str(SCRIPT), str(minutes), str(POLL_S),
                        "--start", start.isoformat(), cwd=str(REPO), env=env, stdout=out, stderr=out,
                    )
                print(f"[eta-health] started {key} ({minutes} min), launch {launches[key]}")
            if pruned_on != now.date():
                prune(out_dir)
                pruned_on = now.date()
        except Exception as exc:
            print(f"[eta-health] scheduler error: {exc}")
        await asyncio.sleep(CHECK_EVERY_S)
