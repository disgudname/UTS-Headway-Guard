"""Daily check that prod's PulsePoint proxy is serving live incidents; ntfy only on failure.

Run by the `PulsePoint-Daily-Check` Windows scheduled task on [home]. Silent when the feed has
incidents. Otherwise POSTs to https://ntfy.sh/<NTFY_TOPIC> (same topic as eta_health_notify.py,
never in git). PulsePoint sits behind an AWS WAF challenge the app solves with headless Chromium
(see _mint_pulsepoint_waf_token in app.py), so a failure here usually means that stopped working.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

from eta_health_notify import user_env

URL = "https://uts-headway-guard.fly.dev/v1/pulsepoint/incidents"


def check():
    """Return None if live data is present, else a short reason."""
    try:
        with urllib.request.urlopen(URL, timeout=60) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return f"HTTP {exc.code} from {URL}"
    except Exception as exc:
        return f"request failed: {exc}"
    incidents = data.get("incidents") if isinstance(data, dict) else None
    if not isinstance(incidents, dict):
        return "response has no incidents (still encrypted or unexpected shape)"
    active = len(incidents.get("active") or [])
    recent = len(incidents.get("recent") or [])
    if active + recent == 0:
        return "incidents list is empty (0 active, 0 recent)"
    print(f"ok: {active} active, {recent} recent")
    return None


def main():
    problem = check()
    if problem:
        # One retry: the fetch that mints a fresh WAF token can be slow or hit a blip.
        time.sleep(60)
        problem = check()
    if not problem:
        return 0
    print("PROBLEM:", problem)
    topic = os.environ.get("NTFY_TOPIC") or user_env("NTFY_TOPIC")
    if not topic:
        print("NTFY_TOPIC not set; not sending")
        return 1
    msg = f"No live PulsePoint data on prod: {problem}. Check fly logs for [pulsepoint]."
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}", data=msg.encode("utf-8"), method="POST",
        headers={"Title": "PulsePoint down", "Priority": "high", "Tags": "rotating_light"},
    )
    urllib.request.urlopen(req, timeout=20).read()
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("pulsepoint check failed:", exc)
        sys.exit(1)
