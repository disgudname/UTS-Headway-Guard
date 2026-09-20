# HANDOFF — living doc shared between the dev machine and the home server

This is how Claude sessions on different machines talk to each other. Claude's auto-memory is
per-machine and does NOT travel, so anything the other machine needs to know goes **here**, in git.

## How to use this doc

- **Read it at the start of a session. Update it before you finish**, then commit and push.
  (`CLAUDE.md` tells every session to do this.)
- **Pull before you edit, push right after.** Two machines editing one file is how conflicts happen.
  Keep edits small and push them straight away.
- **Machine tags.** Sign every message-board entry with `[dev]` (the Windows dev machine where the ETA work
  happened, `C:\Users\Pat Cox\...`) or `[home]` (the home server that runs Valhalla/Nominatim). If a new
  machine joins, add a tag for it in the list below.
- **§1 Message board is append-only, newest first.** Add an entry; don't rewrite someone else's. When an
  item is resolved, add a follow-up line under it (`↳ [home] done 2026-09-21, see commit abc123`) rather than
  deleting history. Prune only entries that are both resolved and older than ~a month.
- **§2–§7 are reference.** Edit those in place when the facts change, and put the date on what you changed.
  If a reference fact turns out to be stale, fix it here — don't just work around it.
- **Never put secrets here** (API keys, auth keys, passwords, cookies). Name where they live instead.
- **What belongs where:** decisions, state that isn't obvious from the code, warnings, and "I need X from the
  other machine" go here. Things the code or `git log` already say do not.

Machine tags: `[dev]` = Windows dev machine · `[home]` = home server (Windows box + VirtualBox VM `valhalla-server`).

---

## 1. Message board (newest first)

### 2026-09-19 · [dev] · NEW: regular ETA health checks (procedure in §7, automation not built)
- Added §7: how to run a check, when, what counts as "clearly broken," and where to record results. **Nothing is
  automated yet** — until the user asks, a check happens only when a session runs one by hand.
- Only Saturday evenings have been measured. The most valuable next data is a **weekday morning, midday and
  rush hour**, and a run while **Purple** is in service (Purple has never appeared in a test).
- Post a board entry only when a check breaches a threshold or covers a new kind of hour/day (see §7) — not for
  every routine pass.

### 2026-09-19 · [dev] · CORRECTION: geocoding IS deployed and live
- The entry below (and §5) said the geocoding Fly step was still open. **That was stale.** Verified today:
  `GEOCODE_URL` and `GEOCODE_PROXY_URL` are set on Fly (status Deployed), and
  `https://uts-headway-guard.fly.dev/v1/search/geocode?q=rugby road` returns real results, so prod reaches
  Nominatim over the tunnel. Nothing left to do for geocoding. `GEOCODING_SEARCH.md` updated to match.
- **Lesson:** the docs describe state at write time. Re-verify (`flyctl secrets list`, hit the live endpoint)
  before telling the user something is unfinished.

### 2026-09-19 · [dev] · ETA work is done and deployed; home-server work is next  *(geocoding item 2 below is superseded — see correction above)*
- **State:** everything from the ETA-accuracy session is committed, pushed, and deployed (last code commit
  `efec93e`). Details in §4. Nothing is half-finished on `[dev]`.
- **For `[home]` — suggested first steps:**
  1. Read `ROUTING_ENGINE.md` and `GEOCODING_SEARCH.md` (§5 summarizes them) and **re-verify the box**: is the VM
     up, are Valhalla (:8002) and Nominatim (:8003) answering, is Tailscale connected? Those docs describe
     state as of when they were written.
  2. The geocoding feature is **built but not deployed** (endpoint `/v1/search/geocode` + livemap "Places"
     search). The remaining step is Fly-side: set `GEOCODE_URL` / `GEOCODE_PROXY_URL` secrets and deploy —
     **deploy only when the user says "cpd"** (see §2).
  3. Then per the user's priorities: Tracklayer frontend (`/routeplanner`) and/or moving
     `/api/routes/leg` + `/api/ondemand/routes` off paid OpenRouteService.
- **Open question for the user (not blocking):** auth on `/routeplanner` — likely dispatcher-gated.
- **If you touch the ETA engine from `[home]`:** don't. It's tuned and the user chose to stop; read §4 first.
  Small exception: if the health-check scripts show something clearly broken (late misses common, a route badly
  late, a full-lap glitch), flag it here for `[dev]`.

---

## 2. Standing rules for working with this user (learned the hard way)

- **Plain language.** Explain like the user isn't a coder. When they say "dumb it down," go much
  simpler than feels necessary. Lead with the answer, then the reasons. (`AGENTS.md` also says the
  user may be typing quickly — infer intent from typos.)
- **Deploys are per-instance.** Never run `flyctl deploy` on your own. Committing/pushing to `main`
  is not a green light to deploy. The literal word **"cpd"** from the user means *commit, push, and
  deploy this now* — that occurrence only. After deploying, verify live (`/v1/health` + spot-check
  the change).
- **`flyctl`, not `fly`, and only via PowerShell** on the Windows box (`C:\Users\Pat Cox\.fly\bin\flyctl.exe`).
  A trailing `Error: The handle is invalid.` is cosmetic Windows noise, not a failure.
- **Never kill processes by name** (`taskkill /IM python.exe`, `pkill node`). Kill by the specific PID
  you started. This once killed an unrelated process of theirs.
- **Editing repo files from Python on Windows:** text-mode `open(p, "w")` flips the whole file LF→CRLF.
  Use the Edit tool, or read/write in binary (`"rb"`/`"wb"`).
- **The app runs on ONE CPU** (Fly, `dfw`). Anything expensive on a polled endpoint must be cached, or it
  stalls unrelated requests (kiosk check-ins, SSE). After any middleware/response-layer change, verify a
  streaming endpoint actually keeps delivering (a 200 + passing health check is not enough).
- **ETA preference (user, this session): slightly EARLY beats LATE.** An early ETA costs a rider a short
  wait; a late one costs them the bus. When scoring or tuning, report early and late errors separately and
  weigh late misses more. Don't "fix" a small early lean by pushing estimates later.
- **Don't chase perfection.** The user's own framing: it's a chaotic system. We agreed to stop tuning the
  ETA engine unless something is *clearly broken* (late misses common, one route badly late, a full-lap glitch).
- **HANDOFF.md edits never need permission.** The user said (2026-09-19) to just update this file whenever it's
  useful — don't ask first. Still commit and push right after (see "How to use this doc").
- Commit style: present-tense imperative subject, technical body. Commits made by Claude end with the
  `Co-Authored-By` / `Claude-Session` trailer lines the harness provides. Single branch (`main`).
- `PushNotification` only delivers when the terminal is *not* the active window; the user has the mobile
  app. If it says "not sent," say the update in the terminal instead.

## 3. Repo state (snapshot — update when it changes)

- Clean apart from untracked `data-local/` (the six ETA test logs, `run1`–`run6.jsonl` — raw data, left
  out on purpose) and `uva_style.json` (untracked before this session; not ours).
- **Recent commits, oldest first:** `0343709`, `2b5fce5`, `feba3dc`, `32cbbb8`, `e65ee8f`, `514f308`,
  `efec93e` (all ETA-related, all deployed).
- Baseline test failures that are **not ours** and predate this work: 8 in the full suite
  (`test_cache_concurrency`, `test_ondemand_filters`, …) and `tests/test_vehicle_drivers.py` fails to import.
  Compare against that baseline rather than expecting green.

## 4. Reference: bus ETA accuracy work (2026-09-19)

The ETA engine (`bus_eta.py`, exposed at `GET /v1/eta/uts_stop_arrivals`, shown beside TransLoc's own ETA
in `/livemap` popups, and feeding the trip planner's wait times) was measured against reality for the first
time and then fixed. UTS only (CAT has no history/schedule).

### How it works (short version)
Live position + smoothed speed for the bus's next stop; every later stop = historical hop time (from
`trip_planner_history.py`) scaled by a "pace factor" that fades with distance; scheduled **timestop holds**
(`uts_blocks.py`, `config/uts_timestops.json`) push ETAs later; a real-GPS "arriving now" override.
UTS routes are all loops. Key files: `bus_eta.py`, `trip_planner_history.py`, `uts_blocks.py`,
`app.py` (`project_vehicle_to_route`, `_compute_bus_eta_arrivals`, `_smooth_bus_eta_seconds`).

### Bugs found and fixed
1. **Two-way roads (Gold Line, Massie Rd / Emmet St).** The route retraces the same physical road on the way
   out and back. `project_vehicle_to_route` could snap a bus to the wrong pass (a float-noise tie-break; then
   GPS jitter on divided roads), which made its ETA a full lap away or dropped it entirely. Now cost =
   distance + heading-mismatch penalty + continuity-with-last-poll penalty; heading ignored when stopped.
   `bus_eta`'s "arriving now" override also now requires the stop to be arc-close (≤600 m ahead or just short
   of a full lap).
2. **Brief stops read as "running slow."** Predictions jumped later mostly when the bus was stopped (73% vs
   35% baseline). Speed floor 1.0→3.0 m/s, `PACE_RATIO_MIN` 0.35→0.5, plus a median-of-last-3 smoother per
   (route, stop, vehicle) in `_compute_bus_eta_arrivals`.
3. **The history table was nearly empty.** It grouped runs by the `block` field, which is empty on almost
   every recent day, so the live table had ~320 buckets, none for current routes, and ~91% of ETAs were the
   distance/speed guess. Now groups by (block **or** vehicle, day). Lookup widens: exact weekday+hour → other
   day(s) of the same group (Mon–Fri / Sat–Sun) → hour ±1 → ±2 → the other day group (same route) → deep cache.
   Table went 320 → 24,851 buckets. **The live table was rebuilt by hand on the machine** (see §6); it also
   self-refreshes nightly at 03:00 ET with the fixed code.
4. **Timestop layovers.** Buses leave a scheduled timestop ~1 min *after* the scheduled time (median ~1.1 min,
   n=11) → `SCHEDULED_DEPARTURE_LAG_S = 45`. History hops that *start* at a timestop include the layover
   (Green Chapel 267 s, Orange Shannon Library 362 s for ~60 s of driving) → the hop leaving **any mapped
   timestop** is capped at typical-speed driving + 30 s (`POST_HOLD_HOP_ALLOWANCE_S`, via `is_timestop_fn`).
   The "any mapped timestop" part was a fix for a regression I introduced: weekday-evening history leaked
   Pinn Hall's 550 s weekday layover into Saturday Orange ETAs (36% of Orange predictions >2 min late).

### Measured result (scored against real bus tracks; Saturday evenings)
Last test (run 6): median error **0 s**, median |error| 46 s, misses >2 min late 1.2% (TransLoc: 1.2%, but
its median error is −136 s — it runs 2–6 min *early*, worse the further out). Orange median |error| 21 s.
The leftover big misses were single buses (Gold bus 12 delayed — TransLoc equally early on 94% of those rows;
Green bus 16 sat still ~3 min and our ETA froze while it dwelled, then snapped back).

### The measurement tooling (in `scripts/`, the reusable part)
- `eta_watch.py [minutes=30] [poll_s=15] [out]` — polls prod for our ETAs, TransLoc's, and every bus's GPS;
  writes JSONL (`data-local/eta_watch/`). Stdlib only, hits public endpoints only.
- `eta_compare.py <log> [...]` — scores both against reality. **The scorer must be direction-aware**: it
  Viterbi-matches each bus's GPS to the route polyline (steady forward motion) and defines "actual arrival"
  as the route position crossing the stop's `arc_pos`. Two earlier GPS-closeness scorers gave convincing but
  *wrong* "worst misses" on two-way roads. **Any error cluster both engines share is a scorer artifact until
  proven otherwise.** Reports median/mean error by horizon, early vs late, by route, by estimate source
  (`live`/`historical`/`projected`; "historical" only if *every* hop had history).
- Signed error convention: `predicted − actual`; **positive = late (bad), negative = early**.

### Open items / known limits
- **Purple never appeared in any test.** It has no block package by design (anti-bunching software releases
  held buses dynamically), so held Purple buses will read too soon. No accuracy number exists for it.
- **Only Saturday evenings were tested** (5:30–8:40 PM, a day buses ran ~6% faster than a typical Saturday).
  Weekday mornings / midday / rush hour are unmeasured. Worth a run before trusting those hours.
- A **dwelling bus with no scheduled hold has a frozen ETA** (leans late while it sits, corrects when it moves).
- A late bus is assumed to leave the timestop after a 30 s allowance; observed dwell was 30–60 s. Slightly early.
- **Regular health checks:** procedure and thresholds are in §7; automating them is discussed but not built.
- Timestops are a hand-confirmed list (`config/uts_timestops.json`: only (route, code) pairs seen with a live
  bus). An unlisted stop where buses layover would still leak layover into history; add it to that file.

## 5. Reference: the home server (Valhalla / Nominatim)

The existing docs are current and detailed — **read them first, and re-verify anything stateful**:

- `ROUTING_ENGINE.md` — Valhalla is **live in production**: VirtualBox Ubuntu VM (`valhalla-server`) on the
  Windows home box, Docker, Charlottesville/Albemarle extract (`osmium extract --strategy simple`, because the
  default strategy OOM-killed the 8 GB box), port 8002, reached over Tailscale. Fly runs `tailscaled` in
  userspace mode with a local HTTP proxy (`WALK_ROUTER_PROXY_URL`). `trip_planner.estimate_walk_leg()` calls it
  (`source: "routed"`), falling back to straight-line on any failure.
- `GEOCODING_SEARCH.md` — Nominatim is up on the same VM (port 8003), and the backend endpoint
  `GET /v1/search/geocode` plus the livemap "Places" search UI are written. **Not yet done: the Fly side**
  (`GEOCODE_URL` / `GEOCODE_PROXY_URL` secrets unset, nothing deployed). Per that doc, `GEOCODE_PROXY_URL` can
  reuse the existing `http://localhost:1055`.
- Still to do from `ROUTING_ENGINE.md`: Tracklayer's frontend (`/routeplanner`, waypoint route sketching on the
  GES basemap; open decision = auth on that page), and migrating `/api/routes/leg` + `/api/ondemand/routes` off
  paid hosted OpenRouteService onto the same engine (`auto` costing).
- Gotchas already paid for: `tailscaled` state must live on `/data/tailscale` (ephemeral rootfs loses the
  login on restart); a freshly-set Fly secret sometimes needs `flyctl secrets deploy` before the running
  process sees it (diff `/proc/<pid>/environ`).
- **Update 2026-09-19:** the geocoding Fly side is done and live (see §1 correction); the "Not yet done" bullet above is stale.
- Reminder: deploy only on "cpd".

## 6. Useful recipes

**Inspect live prod state (run Python on the Fly machine with the app's real env).** From PowerShell:
```powershell
$b64 = [Convert]::ToBase64String([IO.File]::ReadAllBytes("probe.py"))
$cmd = "sh -c 'cd /app && echo $b64 | base64 -d | python3'"
& "$env:USERPROFILE\.fly\bin\flyctl.exe" ssh console --pty=false -C $cmd
```
Inside the probe: `os.chdir('/app'); sys.path.insert(0,'/app'); import app, trip_planner_history as tph`, and
call the local API at `http://127.0.0.1:8080/...` for live vehicle/graph data. Prefix `nice -n 19` before
`python3` for anything heavy (single CPU). This is how the history table was rebuilt (`tph.refresh_hop_time_cache`,
~14 s) and how block/timestop wiring was inspected.

**Local scratch server against real upstream data:** `DATA_DIRS=./relative-path uvicorn app:app --port 809N`.
`DATA_DIRS` is colon-split — an absolute Windows path (`C:\...`) silently creates a stray `C/` folder in the
repo root; always use a relative path. Poll `/v1/health`, then tear down by the specific PID.

**Useful public endpoints for ETA work (no auth):** `/v1/eta/uts_stop_arrivals`, `/v1/transloc/stop_arrivals`,
`/v1/testmap/transloc/vehicles`, `/v1/routes/{id}/vehicles_raw` (s_pos, ema_mps, dir_sign), `/v1/trip-planner/uts-graph`
(per-line polyline + ordered stops with `arc_pos`). Block assignments are dispatcher-gated; inspect them via the probe.

## 7. Regular ETA health checks

**Purpose:** catch regressions and drift in ETA accuracy over time — *not* to tune. The engine is tuned; the user
chose to stop (see §2). **Status: manual only.** Automation (below) is an idea the user hasn't asked to build yet.

### Run one check (~35 min, read-only against production)
Any machine with Python 3 and internet, from the repo root:
```
python scripts/eta_watch.py 30 15 data-local/eta_watch/<yyyymmdd-hhmm>.jsonl
python scripts/eta_compare.py data-local/eta_watch/<same-file>.jsonl
```
It only makes public GET requests (about 120 polls × 3 endpoints), so it's light on the one-CPU app. Logs live in
`data-local/` and are **not committed**. Details of what the scorer does: §4.

### When to run them
Vary the conditions — six Saturday-evening runs already exist, so more of the same adds little. Aim for:
weekday morning (~8–10), weekday midday, weekday rush/evening, Sunday, and **at least one with Purple in
service** (no Purple accuracy data exists at all). Several per week is plenty; a check after any change to
`bus_eta.py`, `trip_planner_history.py`, or the vehicle-tracking code in `app.py` is worth more than a scheduled one.

### What counts as "clearly broken" (starting thresholds — adjust once weekday data exists)
Baseline from the last Saturday run (run 6): overall median error 0 s, median |error| 46 s, misses >2 min **late**
1.2% (TransLoc: 1.2%, median −136 s). Investigate if any of these show up:
- misses >2 min **late** above ~3% of predictions overall (late is the costly direction — see §2);
- any single route with median |error| above ~90 s, or a median **late** bias above ~+60 s;
- a **full-lap flip**: one estimate ~20+ min off while TransLoc is within ~2 min for the same bus/stop, or a
  non-zero "only TransLoc" coverage count (TransLoc predicted a visit we didn't);
- the share of estimates using real history (`historical`) collapsing — that would mean the hop-time table has
  emptied again (check the nightly 03:00 rebuild and whether `block`/vehicle grouping still yields buckets).

**Before calling something a bug:** (1) check whether it's ONE bus — a delayed or parked bus hurts both engines
(look at the worst misses by bus, and whether TransLoc missed the same rows); (2) any cluster **both** engines
share is a scorer artifact until proven otherwise; (3) early misses are the lesser problem.

### Recording results
- Routine pass with nothing unusual: no board entry needed.
- Post a short `[machine]` entry on the message board (numbers + one line of interpretation) when a threshold is
  breached, or when the check covers a new kind of hour/day (first weekday morning, first Purple run, …).
- Real fixes go in code with a commit message that says what was measured, as before.

### Automation (idea only — ask the user before building)
- A script on an always-on machine (the home server is the natural host; Windows Task Scheduler or cron) runs the
  two commands several times a day at varied hours and appends one summary line per run to a local results file.
- Optionally a scheduled Claude routine reads that file once a day and pings the user — only on a threshold
  breach or a first-of-its-kind run, not every pass.
- Unknowns: whether a routine's notification reliably reaches the user's phone, and what scheduled cloud runs
  cost. The plain scheduled script (no AI) avoids both for the mechanical part.
