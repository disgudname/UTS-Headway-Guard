"""Have Claude review the newest ETA health-check result and push a verdict to the phone via ntfy.

    python scripts/eta_health_notify.py [--dry-run]

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

PROMPT = """You are reviewing one scheduled ETA health check for the UVA transit dashboard.
Do NOT edit files, commit, or run anything except reading. Read HANDOFF.md section 7 for the
thresholds and baseline, then judge this result:

{result}

Reply with ONLY the notification text: 1-3 short plain-language lines for a phone screen, no markdown.
Start with "ALL CLEAR:" if fine, "PROBLEM:" if a real breach (not one delayed bus or a scorer
artifact), or "NOTE:" if inconclusive/odd (e.g. error, too little data, Purple off-hours)."""


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
        return "PROBLEM: " + "; ".join(d["breaches"])[:300]
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
            "Title": "ETA check: PROBLEM" if problem else "ETA check",
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
