"""One-time "are stop ETAs done?" verdict, pushed to the phone via ntfy.

    python scripts/eta_done_check.py [--dry-run]

Run once by the Task Scheduler job ETA-Done-2026-10-14, after the last run of the test week (HANDOFF.md,
"ETA DONE DATE" in the open list). Collects every health-check result from SINCE to UNTIL, checks whether the
ETA engine files changed since BASE_COMMIT, has a headless `claude -p` judge the week against the three
conditions, and POSTs the verdict to https://ntfy.sh/<NTFY_TOPIC>. Read-only: it edits nothing.
--dry-run prints the message instead of sending it.
"""
import json
import os
import subprocess
import sys
import urllib.request

import eta_health_notify as notify

SINCE = "2026-10-08"
UNTIL = "2026-10-14"
BASE_COMMIT = "db376ba"
ENGINE_FILES = ["bus_eta.py", "trip_planner_history.py", "uts_blocks.py", "config/uts_blocks.json",
                "config/uts_timestops.json", "config/uts_active_sheets.json"]
LATE_TARGET_PCT = 3.0
NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)

PROMPT = """You are giving the final verdict on a one-week test of the UVA transit dashboard's stop ETAs.
Do NOT edit files, commit, or run anything except reading. In HANDOFF.md read the "ETA DONE DATE" item in
"Open right now" (the three conditions) and the message-board entries dated {since} to {until} (they explain
individual misses), then judge this week:

ETA engine commits since {base} (empty = none):
{commits}

Runs (late% = predictions more than 2 min late, Purple left out; target under {target}%):
{runs}

One engine change is exempt by the user's decision (2026-10-09) and does NOT count as "engine code changed":
the fix to when ETAs are blanked for a block's last trip (Orange [08]'s Stadium Rd stops, Night Pilot [04]'s
last ten minutes; bus_eta.py OOS_POSITION_TRUST_BEFORE_S and uts_blocks.route_change_plan). Any other engine
commit still counts.

Purple is not part of the test. Lot shuttles on game day Sat 2026-10-10 do not count. A run over the target
still passes if a board entry (or its breach list) pins it on one bus, one driver, or traffic.

Reply with ONLY the notification text, plain language, no markdown, max 5 short lines. Line 1 starts with
"DONE:" (all three conditions met), "NOT DONE:" (say which condition failed) or "NOTE:" (cannot tell, e.g.
runs missing). Then: how many runs, how many over the target and why, and whether engine code changed."""


def week_runs():
    rows = []
    for line in notify.RESULTS.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if SINCE <= r.get("when", "")[:10] <= UNTIL:
            rows.append(r)
    return rows


def late_pct(r):
    """Late share without Purple; the plain figure when Purple was not running in that run."""
    v = r.get("late_over_2min_pct_excl_near_term_only")
    return r.get("late_over_2min_pct") if v is None else v


def run_line(r):
    if r.get("error"):
        return f"{r['when'][:16]} ERROR {r['error']}"
    if r.get("inconclusive"):
        return f"{r['when'][:16]} inconclusive ({r['inconclusive']})"
    return (f"{r['when'][:16]} {r.get('day')} late {late_pct(r)}% median|err| {r.get('median_abs_err_s')}s "
            f"scored {r.get('scored')} breaches: {'; '.join(r.get('breaches') or []) or 'none'}")


def engine_commits():
    subprocess.run(["git", "fetch", "-q", "origin"], cwd=notify.REPO, timeout=60, creationflags=NO_WINDOW)
    out = subprocess.run(["git", "log", "--oneline", f"{BASE_COMMIT}..origin/main", "--", *ENGINE_FILES],
                         cwd=notify.REPO, capture_output=True, text=True, timeout=60, creationflags=NO_WINDOW)
    return out.stdout.strip()


def fallback(rows, commits):
    scored = [r for r in rows if late_pct(r) is not None and not r.get("inconclusive")]
    over = [r for r in scored if late_pct(r) > LATE_TARGET_PCT]
    head = "NOT DONE: engine code changed" if commits else ("NOTE: runs over target, read them" if over else "DONE:")
    return (f"{head} {len(scored)} scored runs {SINCE}..{UNTIL}, {len(over)} over {LATE_TARGET_PCT}% late"
            + (": " + ", ".join(f"{r['when'][5:16]} {late_pct(r)}%" for r in over) if over else "")
            + ". (Claude review unavailable.)")


def main():
    rows = week_runs()
    commits = engine_commits()
    prompt = PROMPT.format(since=SINCE, until=UNTIL, base=BASE_COMMIT, commits=commits or "(none)",
                           target=LATE_TARGET_PCT, runs="\n".join(run_line(r) for r in rows) or "(no runs found)")
    msg = None
    try:
        out = subprocess.run([str(notify.CLAUDE), "-p", prompt], cwd=notify.REPO, capture_output=True, text=True,
                             timeout=420, creationflags=NO_WINDOW)
        if out.returncode == 0 and out.stdout.strip():
            msg = out.stdout.strip()
    except Exception:
        pass
    msg = msg or fallback(rows, commits)
    if "--dry-run" in sys.argv:
        print(msg)
        return 0
    topic = os.environ.get("NTFY_TOPIC") or notify.user_env("NTFY_TOPIC")
    if not topic:
        print("NTFY_TOPIC not set; not sending")
        return 1
    done = msg.upper().startswith("DONE")
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}", data=msg.encode("utf-8"), method="POST",
        headers={"Title": "ETA test week: verdict", "Priority": "high",
                 "Tags": "checkered_flag" if done else "rotating_light"},
    )
    urllib.request.urlopen(req, timeout=20).read()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("done check failed:", exc)
        sys.exit(1)
