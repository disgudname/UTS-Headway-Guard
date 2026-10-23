"""Have Claude review the newest ETA health-check result and push a verdict to the phone via ntfy.

    python scripts/eta_health_notify.py [--dry-run] [--result N]

Called at the end of eta_health_check.py (HANDOFF.md section 7). Reads the last line of
data-local/eta_watch/health_results.jsonl, asks a headless `claude -p` for a 1-3 line verdict, and
POSTs it to https://ntfy.sh/<NTFY_TOPIC>. The topic is the "password" on ntfy's free tier, so it
lives in the NTFY_TOPIC environment variable, never in git. Never raises: a failed notify must not
fail the health check. --dry-run prints the message instead of sending it.
"""
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "data-local" / "eta_watch" / "health_results.jsonl"
CLAUDE = Path.home() / ".local" / "bin" / "claude.exe"

PROMPT = """You are writing a phone notification about one scheduled check of the UVA transit dashboard's bus arrival
times ("ETAs"). The reader runs transit operations and drives buses but is NOT a statistician or programmer: write the
way you would tell a coworker, in plain words. Do NOT edit files, commit, or run anything except reading. Read
HANDOFF.md section 7 for what the fields and limits mean, then write about this result:

{result}

FORMAT: plain text, no markdown, at most 5 short lines.
Line 1 starts with exactly one of:
  "ALL GOOD:"  nothing needs anyone's attention (including a run where one odd bus explains everything);
  "HEADS UP:"  worth knowing but not ours to fix, or the check could not tell (no buses, too little data, it failed);
  "PROBLEM:"   our ETAs went wrong in a way we should fix (see WHO below).
then a few words saying why, e.g. "ALL GOOD: Saturday evening".
Line 2 is the headline score, from on_target_pct / tl_on_target_pct: "On target: 89 of every 100 ETAs (TransLoc: 56)."
"On target" means the bus came within about a minute of the time shown when it was close, and within a few minutes when
it was 10-15 min away. If those fields are missing, say instead how far off a typical ETA was, in seconds or minutes
(median_abs_err_s), and leave TransLoc out unless tl_median_err_s is there.
Line 3 is the missed-bus risk, from late_over_2min_pct (use late_over_2min_pct_excl_near_term_only when present, which
leaves Purple out): it is the share of ETAs where the bus LEFT more than 2 minutes BEFORE the time shown, i.e. a rider
trusting it could have missed the bus. Say it as "1 in N" ETAs (round to a friendly number: 1 in 1,000, 1 in 50...),
or "none" at 0. The target is under 3 in 100. NEVER call this "late": in plain words it is the bus coming early.
Then at most two lines on anything off, by route name ("Gold", "Green", "Night Pilot"...), giving the route's own on
target score (routes[...].on_target_pct) and WHO is behind it, taken from the "causes" and "suspects" fields:
  the buses: they ran slower or faster than usual (traffic, long stops). Nothing to fix. ("causes" says BUSES SLOW);
  our lean: we deliberately plan on buses going a bit quicker than usual so nobody misses one, so some routes read a
    bit behind every day. Expected, nothing to fix. ("causes" says ENGINE LEAN);
  one bus: one bus did something nobody could predict; name the bus number, block in brackets like [09], the time,
    and what it did in plain words ("sat 5 min at Runk, then left a minute before its scheduled time"). Not ours to fix.
    ("suspects" verdict ONE BUS / ONE OF A FEW BUSES / A FEW BUSES);
  our system: the ETAs were wrong for a reason that is not the buses' pace, so it is ours to fix: say what went wrong
    in plain words. ("causes" says ENGINE, or a suspect verdict ENGINE, ONE BUS or RECURRING). Only this one makes
    line 1 PROBLEM. RECURRING means the same block had the same trouble at the same time on earlier days (it names
    them): say so ("every weekday since ..."), never call it one bus, whatever the bus did.
  planned: a block left the route by its own timetable (end of its service, or its evening route change) and TransLoc
    kept predicting stops the bus would no longer serve; our blank there is right. Not a problem: leave it out, or
    one short line at most. ("causes" / suspect verdict PLANNED). Never call it an odd bus.
Other things that make line 1 PROBLEM: "full_lap_flips" above 0 (an ETA was ~20 minutes off while TransLoc was right),
or "only_transloc" above 0 (stops where TransLoc showed a time and we showed nothing) UNLESS the suspects call those
rows PLANNED: say it as "we showed no time at N stops where TransLoc had one", with the bus numbers if the suspects
name them.
"historical_pct" below 5 is a PROBLEM too ("our trip-time history looks empty"); otherwise never mention it.
Purple Line staging holds change daily: judge Purple only on what the fields say about it, and never let Purple alone
make line 1 PROBLEM.
The "calendar" field is the public service calendar for the day (a service day runs to 04:00, so the 00:00-01:30 runs
belong to the day before). If there were few or no buses and the calendar says why ("No Service", service ended, a
game day running only lot shuttles), it is HEADS UP with that reason, not a PROBLEM; mention a home game whenever its
notes show one. Game-day lot shuttles never count against the ETAs: never quote a score for them.
The "notices" field lists the service alerts riders could see during the run (detours, closed stops, delays; "routes"
or "stop_ids" say what each one covers, none means system-wide). Ones with "source": "transloc" are TransLoc's own
rider alerts: they go up days early, so read the text for WHEN it applies ("On Friday, October 23 ... 7 AM to 9 AM")
and ignore it unless the run's "when" falls in that window. When one applies to a route that went wrong, say so on
that route's line ("Gold Line is on a detour today") and treat it as the likely reason: a detour the ETAs were not built
for is HEADS UP, not PROBLEM, unless the trouble is on routes the alert does not touch. A "SERVICE ALERT" entry in
"causes" points at the same thing. With no route trouble, mention an active alert in a few words at most.
Words to avoid: median, percentile, breach, threshold, scorer, slot, horizon, engine, model, history, flip, poll,
log, field names. Use whole numbers. Do not repeat a number twice."""


def review(last):
    try:
        out = subprocess.run(
            [str(CLAUDE), "-p", PROMPT.format(result=last)],
            cwd=REPO, capture_output=True, text=True, timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        text = out.stdout.strip()
        if out.returncode == 0 and text:
            return text
    except Exception:
        pass
    # Fallback: plain summary straight from the data if Claude is unavailable.
    d = json.loads(last)
    if d.get("error"):
        return f"NOTE: health check failed to run: {d['error']}"
    if d.get("breaches"):
        why = ("\nWHY: " + "; ".join(d["causes"])) if d.get("causes") else ""
        return ("PROBLEM: " + "; ".join(d["breaches"])[:300] + why)[:900]
    return "ALL CLEAR (Claude review unavailable)."


def user_env(name):
    """Read a user env var straight from the registry (long-running scheduler sessions can miss new ones)."""
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
            return winreg.QueryValueEx(k, name)[0]
    except Exception:
        return None


def main():
    lines = [l for l in RESULTS.read_text(encoding="utf-8").splitlines() if l.strip()]
    last = lines[-1]
    if "--result" in sys.argv:  # --result N: review line N of the results file instead (testing; -1 = the last)
        last = lines[int(sys.argv[sys.argv.index("--result") + 1])]
    msg = review(last)
    problem = msg.upper().startswith("PROBLEM")
    if "--dry-run" in sys.argv:
        print(msg)
        return 0
    topic = os.environ.get("NTFY_TOPIC") or user_env("NTFY_TOPIC")
    if not topic:
        print("NTFY_TOPIC not set; not sending")
        return 1
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}", data=msg.encode("utf-8"), method="POST",
        headers={
            "Title": "Bus ETAs: PROBLEM" if problem else "Bus ETAs",
            "Priority": "high" if problem else "default",
            "Tags": "rotating_light" if problem else "bus",
        },
    )
    urllib.request.urlopen(req, timeout=20).read()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("notify failed:", exc)
        sys.exit(1)
