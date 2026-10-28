"""Have Claude review the newest ETA health-check result and push a verdict to the phone via ntfy.

    python scripts/eta_health_notify.py [--dry-run] [--result N]

Called at the end of eta_health_check.py (HANDOFF.md section 7). Reads the last line of
data-local/eta_watch/health_results.jsonl (ETA_WATCH_DIR), asks Claude for a short verdict, and
POSTs it to https://ntfy.sh/<topic>. With ANTHROPIC_API_KEY set (prod, where eta_health_scheduler.py runs the
checks) Claude is called through the API with HANDOFF.md section 7 pasted in; otherwise through a headless
`claude -p` on this machine. The topic is the "password" on ntfy's free tier, so it lives in ETA_NTFY_TOPIC
(prod, where NTFY_TOPIC is the bridge-alert topic) or NTFY_TOPIC, never in git. Never raises: a failed notify must not
fail the health check. --dry-run prints the message instead of sending it.
"""
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = Path(os.environ.get("ETA_WATCH_DIR") or REPO / "data-local" / "eta_watch") / "health_results.jsonl"
CLAUDE = Path.home() / ".local" / "bin" / "claude.exe"
MODEL = "claude-opus-5-5"

PROMPT = """You are writing a phone notification about one scheduled check of the UVA transit dashboard's bus arrival
times ("ETAs"). The reader runs transit operations and drives buses but is NOT a statistician or programmer: write the
way you would tell a coworker, in plain words. Do NOT edit files, commit, or run anything except reading. Read
HANDOFF.md section 7 for what the fields and limits mean, then write about this result:

{result}

FORMAT: plain text, no markdown, at most 6 short lines.
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
Line 4, only when "early" shows any over_2min above 0: HOW early, from "early" (Purple and lot shuttles are already
out of it). Say how many came 5+ and 10+ minutes before the time shown, again as "1 in N" of "early.of" (skip a step
that is 0), then the worst bus from "early.worst": bus number, block in brackets, route, the stop and clock time, how
many minutes early against the time we showed, how many stops it hit ("stops_hit"), and how long riders could see the
wrong time ("shown_for_s": say it in seconds under 2 minutes, else whole minutes), e.g. "5+ min early: 1 in 9.
Worst: 14332 [10] Gold Line came 7 min before our time at JPJ South Lot (19:49), 19 stops, wrong time up for 30 sec."
The minutes early is how far off the worst moment was, not how long it lasted: never word it so it sounds like the
bus was early for a long time. When TransLoc had that same
one right (tl_early_min under 1), add "TransLoc had it right" (and if the suspects say the bus left a timestop before its sheet time, say so). Name a second
worst bus only if it was also 5+ min early. With over_2min at 0, leave this line out.
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


def handoff_section_7():
    """HANDOFF.md section 7 (what the fields and limits mean), for the API call, which cannot read files."""
    text = (REPO / "HANDOFF.md").read_text(encoding="utf-8")
    start = text.index("## 7.")
    end = text.find("\n## ", start + 5)
    return text[start:end if end > 0 else None]


def review_api(last):
    """The verdict through the Anthropic API; None if it can't be had (the caller falls back to a plain summary)."""
    import anthropic
    prompt = PROMPT.format(result=last).replace(
        "Read\nHANDOFF.md section 7 for what the fields and limits mean",
        "HANDOFF.md section 7, below, says what the\nfields and limits mean")
    content = f"<handoff_section_7>\n{handoff_section_7()}\n</handoff_section_7>\n\n{prompt}"
    request = dict(model=MODEL, max_tokens=16000, messages=[{"role": "user", "content": content}])
    client = anthropic.Anthropic(timeout=300)
    try:
        # A safety decline on the main model is re-run on another one inside the same call.
        msg = client.beta.messages.create(betas=["server-side-fallback-2026-07-01"], fallbacks="default", **request)
    except anthropic.BadRequestError:
        msg = client.messages.create(**request)
    if msg.stop_reason == "refusal":
        return None
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    return text or None


def review(last):
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            text = review_api(last)
            if text:
                return text
        except Exception as exc:
            print("Claude API review failed:", exc)
        return fallback_summary(last)
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
    return fallback_summary(last)


# Labels eta_health_check.py puts at the front of a "causes" entry (or a route's part of it), sorted by who it is on.
# Anything not listed (ENGINE, ENGINE, ONE BUS, ENGINE (RECURRING ...)) is ours to fix: PROBLEM.
_FINE = ("ONE BUS", "PROBABLY ONE BUS", "SERVICE ALERT", "PURPLE", "Purple staging", "LOT SHUTTLES", "SMALL", "PLANNED", "ENGINE LEAN")
_WORTH_KNOWING = ("BUSES SLOW", "BUSES SLOWER THAN HISTORY", "ONE OF A FEW BUSES", "A FEW BUSES",
                  "SEVERAL BUSES")
_SKIP_ROUTES = ("Purple", "Lots Shuttle", "Fan Shuttle")


def _cause_label(cause):
    """("Orange Line", "BUSES SLOW today") from "Orange Line — Pre-6PM: BUSES SLOW today -- ...", or (None, label)."""
    head = cause.split(" -- ")[0]
    for label in ("BUSES SLOW", "BUSES SLOWER THAN HISTORY", "ENGINE LEAN", "ENGINE"):
        if ": " + label in head:
            route = head.split(": " + label)[0].split(" — ")[0].strip()
            return route, head[len(head.split(": " + label)[0]) + 2:]
    return None, head


def _kind(label):
    if label.startswith("ENGINE LEAN"):
        return "fine"
    if label.startswith("ENGINE"):
        return "problem"
    if label.startswith(_WORTH_KNOWING):
        return "heads_up"
    if label.startswith(_FINE):
        return "fine"
    return "problem"


def _bus_line(suspect):
    """"One bus: 17132 [08] Orange Line, left a timestop early." from a suspects entry."""
    verdict, _, rest = suspect.partition(": ")
    who = rest.split(",")[0].replace(" on ", " ").split(" — ")[0]
    did = []
    if "BEFORE its sheet time" in rest:
        did.append("left a timestop early")
    if "GPS froze" in rest:
        did.append("its GPS kept freezing (the Emmet/McCormick dead spot if it's there)")
    if "pulling out" in rest or "joined the route" in rest:
        did.append("just pulling out")
    if "off the route" in rest or "against the route" in rest:
        did.append("went off its route")
    tl = " TransLoc had it right." if "TransLoc had these right" in rest else ""
    verdict = "One bus, worth a look" if verdict.startswith(("ENGINE", "RECURRING")) else verdict.capitalize()
    return f"{verdict}: {who}" + (", " + " and ".join(did[:2]) if did else "") + "." + tl


def fallback_summary(last):
    """Plain-language verdict straight from the data when Claude is unavailable, sorted the way PROMPT sorts it:
    PROBLEM only when the run's causes put it on our system, never for one bus, Purple, lot shuttles or slow buses."""
    d = json.loads(last)
    if d.get("error"):
        return f"HEADS UP: the check couldn't run ({str(d['error'])[:120]})."
    if d.get("inconclusive"):
        return f"HEADS UP: nothing to check ({d['inconclusive']})."
    lines = []
    ours, tl = d.get("on_target_pct"), d.get("tl_on_target_pct")
    if ours is not None and tl is not None:
        lines.append(f"Right on time: {round(ours)}% of our times, {round(tl)}% of TransLoc's.")
    late = d.get("late_over_2min_pct_excl_near_term_only")
    if late is None:
        late = d.get("late_over_2min_pct")
    if late:
        lines.append(f"Bus left before the time shown: 1 in {max(1, round(100 / late))}"
                     + (" (not counting Purple)." if d.get("purple_in_service") else "."))
    elif late is not None:
        lines.append("No bus left before the time shown" + (" (not counting Purple)." if d.get("purple_in_service") else "."))

    kinds, why = [], []
    for cause in d.get("causes") or []:
        route, label = _cause_label(cause)
        if route and route.startswith(_SKIP_ROUTES):
            continue
        kind = _kind(label)
        kinds.append(kind)
        if kind == "problem":
            breach = cause.partition(" -- ")[2]
            if "full-lap" in breach:
                what = "some times were about 20 min off while TransLoc was right"
            elif "TransLoc predicted that we didn't" in breach:
                what = f"we showed no time at {breach.split(' ')[0]} stops where TransLoc had one"
            elif ">2 min late" in breach:
                what = "buses left before our times too often"
            else:
                what = "our times were off"
            if "RECURRING" in label:
                what += ", same block at the same time on earlier days"
            why.append(f"{route}: {what}. Worth a look." if route else f"{what[0].upper()}{what[1:]}. Worth a look.")
        elif label.startswith("BUSES SLOW"):
            why.append(f"{route}: buses ran slower than usual (traffic or long stops).")
        elif label.startswith("Purple staging"):
            why.append("Purple Line: buses left their staging holds early, as usual.")
    if (d.get("historical_pct") or 100) < 5:
        kinds.append("problem")
        why.insert(0, "Our trip-time history looks empty.")
    for s in d.get("suspects") or []:
        if s.startswith(("ONE BUS:", "ENGINE, ONE BUS:", "RECURRING")) and " Purple" not in s.split(",")[0]:
            why.append(_bus_line(s))
    seen = set()
    why = [w for w in why if not (w in seen or seen.add(w))]

    if "problem" in kinds:
        head = "PROBLEM: our times went wrong"
    elif "heads_up" in kinds:
        head = "HEADS UP: buses had a rough stretch, not our times"
    elif d.get("breaches"):
        head = "ALL GOOD: one-off buses or Purple only"
    else:
        head = "ALL GOOD"
    return "\n".join([head] + lines + why[:3])[:900]


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
    topic = os.environ.get("ETA_NTFY_TOPIC") or os.environ.get("NTFY_TOPIC") or user_env("NTFY_TOPIC")
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
