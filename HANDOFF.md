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

### 2026-09-20 · [home] · DEPLOYED: BlockId on the ETA feed + timestop lag 45->30 s; schedule holds verified working; Gold slowdown expected tonight
- **Deployed `ec3e979`:** every row in `/v1/eta/uts_stop_arrivals` now has `BlockId` (or null); `eta_watch` logs it as the 6th element of each `ours` row. Timestop departure lag `SCHEDULED_DEPARTURE_LAG_S`
  is now 30 s (was 45): ~20k headway-event departures at timestops the schedule really governs, median ~27 s (12-48 s per route/stop). Dispatch assigns blocks properly (user confirmed); all 4 live buses
  had blocks all evening ([01] bus16/Green, [05] bus13/Orange, [11] bus12 + [09] bus44/Gold).
- **Holds verified:** replaying Sun 18:00 with the real blocks, the schedule moves ~12% of predictions (almost all Gold) by a median +110 s and halves the error on those (350 s early -> 170 s early); live
  matches the with-holds replay. Holds only delay EARLY buses; a bus that is already late gets no help, and the live pace correction fades after a few stops.
- **Schedule adherence (headway data, fall semester):** at timestops the schedule really governs, buses leave a median 12-48 s after scheduled time; 55-97% within 2 min, wrong-lap risk ~0%. Some (route, stop, time) combos
  have huge offsets (e.g. Orange MP weekday evenings, Gold HER weekday evenings) because block route_ids are route FAMILIES with daytime entries; the 25-min match tolerance keeps holds from firing there.
- **Watching:** Gold (57) started slipping ~18:00 (both buses ~3-4 min behind schedule; predictions ~2.7 min early for us and TransLoc). Mumford & Sons at JPJ 19:30 (Gold passes Massie Rd @ JPJ) - expect worse
  Gold ETAs into the evening and after the ~22:00 exit. Possible fix if it persists: let the recent lap pace persist further downstream (engine tuning; test offline with `scripts/eta_replay.py` first). User is also
  looking for traffic data that might help. Nothing changed for this yet.

### 2026-09-20 · [home] · Offline ETA replay tool + a redesign that did NOT beat the current one (nothing deployed)
- **Tool:** `scripts/eta_replay.py <headway_dir> <cutoff> <logs...>` replays eta_watch bus positions through the real `bus_eta` engine under different histories and scores them with
  `eta_compare`. The full headway archive (258 daily CSVs, 2025-12-11 on, ~360 MB, arrival+departure events with dwell) can be pulled from the Fly machine (tar it in /tmp, `flyctl ssh sftp get`);
  a local copy is in `data-local/headway_archive/` (untracked). Replay of the CURRENT design + block schedule reproduces the live app (54 s / 6.4% late vs 52 s / 6.2%), so it is trustworthy.
- **Biggest lever = the block schedule:** current history WITHOUT schedule holds scores 76 s typical, 64% within 2 min, ~55 s early lean; WITH holds 54 s / 78% / -12 s. (TransLoc 85 s / 61%.)
- **Thin-history root cause found:** before 2026-09-14 (`9c2ec69`) headway_tracker stored a BORROWED stop_id from another route on shared stops, so most older events sit under IDs that don't match the
  route (Green weekend: 5 of 20 hops had a bucket). Events still carry `stop_name`; `trip_planner_history.stop_id_resolver()` re-keys by (route, name) -> 18 of 20. Live daily refresh does NOT do this yet
  (only the offline deep-cache script does), but the deep-cache fallback hides most of the gap, so the visible gain is small.
- **Tried: driving-only hops (departure(A)->arrival(B)) + separate dwell** (`build_drive_and_dwell_samples`, `DwellModel`, `bus_eta.estimate_stop_eta_s(dwell_fn=...)`, default OFF). With the schedule it ties the
  current design (55 s, 80% within 2 min, 5.9% late) ONLY when dwell uses the 25th percentile; with median dwell it runs late too often (18% >2 min late, mostly Gold). Better on Orange, worse on Green.
  Not deployed, not wired into app.py. Sunday data only, four buses; the offline schedule stand-in picks the nearest scheduled block (real app tracks blocks live).
- **Next most valuable thing:** make sure every live bus is matched to its block (Gold runs two buses) rather than changing the history; and re-run the replay on weekday logs once they exist.
- Also: the cap on hops leaving a timestop must stay day-independent (see the layover-cap entry below); this redesign would remove the need for it but did not win.

### 2026-09-20 · [home] · DON'T make the layover cap day/time-aware (tried, broke Orange, reverted)
- See the timestop-audit entry, item 4. `uts_blocks.is_timestop_active` was removed again. Keep `is_timestop_fn` = any mapped stop for the route.
- Lesson: check §4's bug list before touching layover logic; a change that looks like a pure cleanup reopened a leak the user had already paid for once.

### 2026-09-20 · [home] · Continuous Sunday logging (12 sessions), stale-"Due" fix deployed, CORRECTION on late misses, timestop audit
- **What ran:** back-to-back 30-min `eta_watch` sessions 10:29-15:51 (logs in `data-local/eta_watch/continuous/`, NOT committed except on the
  user's request). **Only FOUR buses were ever scored** (16 Green/54, 13 Orange/55, 12+44 Gold/57); GPS bus 17 (route 11) sat parked and had no predictions.
- **Deployed (`359e145`): stale "Due" fix.** The median-of-3 smoother kept publishing ~0 for 1-2 polls after a bus passed a stop (81% of 640 passes,
  usually 15-30 s). Now an "arriving" (<=20 s) previous reading bypasses the median. After deploy: 20-32% of passes, ~10% still >=30 s and ~4% >=45 s
  (a second cause remains, probably the arriving-now override). Tests: `tests/test_bus_eta_smoothing.py`.
- **CORRECTION (earlier summaries/pitch talk said "no late misses" - WRONG):** over 12 sessions, **4.1% of our predictions were >2 min late (TransLoc 3.4%)**,
  0% >5 min late. That is slightly over the ~3% line in §7. Median error still leans early overall (-3..-55 s) except Session 11 (+37 s).
- **Session 11 lateness (Sun 15:21):** one bus (16, Green): +79 s median, 29% >2 min late; other three buses fine. Error ramps up steadily with horizon (+23 s at ~6 min out
  to +170 s at ~15 min), no step at any timestop; bus ran ~10% faster than usual and TransLoc showed the same ramp (its early lean just masked it). Session 12 partial: gone.
- **Timestop audit (config/uts_timestops.json vs config/uts_blocks.json) - needs a decision:**
  1. The timestop config is per ROUTE only; the schedule is per day-group AND time of day (e.g. Green weekday: MP/HER 07:30-17:45, CHP/JPA 18:00-22:00; Green weekend: CHP only).
  2. Scheduled holds are already correct (weekend Green has only CHP entries). BUT the layover **hop cap** (`is_timestop_fn` = "any mapped stop for the route") ignores day/time,
     so it caps the hop leaving Green's JPA/MP on weekends, Orange's MP/PIN, Gold's MCQ, etc., where buses do not layover. Observed Sunday dwell (>=45 s at a stop): real layovers only at
     Green CHP (median 3 min), Orange LIB (4.6 min), Gold CHP (4.3 min) and LIB (2 min); Gold BAR/HER ~45-75 s (normal). Effect of the extra cap: slightly EARLY ETAs, not late.
  3. **CORRECTED (same day, user was right): every timestop pair is already mapped.** The "scheduled-but-unmapped" list I first wrote here was an artifact: block `route_ids`
     are route FAMILIES (e.g. block [05] = 53/55/70), so a route inherits codes for stops it never visits. Checked against TransLoc's live stop lists (`/v1/transloc/routes`, which
     includes the weekday route IDs 53/67/68/58): all 24 (route, code) pairs where the route really has that stop are mapped to the right RouteStopID. Nothing to add.
  4. **TRIED AND REVERTED (2026-09-20 [home]): making the cap schedule/day-aware was WRONG.** Deployed as `540f01f`; Session 13 showed Orange (route 55) median +232 s, 72% >2 min late (vs -12 s / 7% before), worst after MP and PIN. Reason: the 'any mapped stop' cap is ALSO what stops weekday layover time, pooled into weekend history, from leaking into weekend ETAs (§4 bug 4). It must stay day-independent. Reverted (`4c9fbf4`) and redeployed. Do not re-attempt without first changing how history is pooled across day groups.

### 2026-09-20 · [dev] · Thin weekend history is now a NOTE, not a PROBLEM (needs `git pull` on [home])
- I ran a manual 30-min check on the dev box (Sun 09:49–10:19): median error -2 s vs TransLoc -79 s, 0.5% >2 min late
  (TL 1.1%), Orange/Green/Gold all within limits — the ONLY flag was "14.2% of estimates use real history". Same thin-history
  cause as the 07:30 run (weekend route ids are new this semester), so every weekend daytime run would have sent a PROBLEM push
  that isn't actionable.
- Change (`scripts/eta_health_check.py`): history share < 30% is now an informational `notes` entry in the result line (no
  breach, exit code unaffected); a real collapse (< 5%, the original block-field bug measured ~1%) is still a breach. The
  notify prompt tells Claude a `notes` field is informational and must not turn an ALL CLEAR into a PROBLEM.
  Tests: `tests/test_eta_health_check.py`. Revisit the 30% NOTE line once weekend `historical_pct` climbs (weekday-evening/night
  runs are already 50–90%).
- The dev-box log isn't committed (`data-local/` only on the user's request).

### 2026-09-20 · [home] · `data-local/` is committed ONLY when the user asks
- The user had the ETA watch logs (`data-local/eta_watch/`, incl. `health_results.jsonl`) committed once (`25e4bbf`) and said
  this should keep happening **at their request, not automatically**. Don't add `data-local/` to routine commits; stage files by
  name (no `git add -A` / `git add .`). If it's the only change, mention it and ask.
- Clarification: the machine that runs the scheduled ETA checks (Task Scheduler, `ETA-Health-*`) is `[home]`, and the user
  confirmed on 2026-09-20 that this Windows box (`WATCHTOWER`) is the home server.

### 2026-09-20 · [dev] · Sunday 07:30 breaches investigated: mostly two buses' first loop (both engines late), plus thin weekend history
- **Late 11.1% / Orange +65 s:** all from Green bus 16 and Orange bus 34 (each route's only bus), concentrated in the
  ~10 min after they pulled out of the lot (~07:31) and ran their first loop; ours +111 s vs TransLoc +114 s in that window
  (TL >2 min late on 76% of the same rows). Errors are ~0 from ~07:45 on. Gold had 0.0% >2 min late. Neither bus stopped
  ≥30 s during the loop. Cause not pinned: on hops that DO have history the buses ran roughly normal (0.94–1.43× expected,
  4–16 hops each), so it isn't simply "buses were faster"; the late predictions came from the distance/speed fallback
  (85% of that run's estimates). Treat "first ~10 min of service each morning" as a known-hard window for both engines;
  one sample — wait for more weekend/weekday mornings (the scheduled runs will supply them) before tuning anything.
- **Only 11.8% history:** real. At Sunday 07:45, no usable history for 15/20 Green hops, 14/20 Orange, 7/30 Gold — and
  Green/Orange have ANY weekend history for only 5/20 and 6/20 adjacent hops at ANY hour (Gold 24/30). Not an ID problem in
  current data (yesterday's Green/Orange/Gold events were 100% recorded under stop IDs on today's route); ~450 older weekend
  buckets per route are filed under stop IDs no longer on the route and never match. Likely cause (unverified): the weekend
  route IDs 54/55 only started this semester, so only ~3 Saturdays/Sundays feed the "≥3 samples per hop-hour" rule.
  Saturday evening looked better only because weekday-evening data can be pooled; weekday MORNING service uses different
  route ids (53/66/67/68), so nothing pools in at 07:30. Expect it to improve on its own as weekend days accumulate — check
  `historical_pct` on weekend-morning runs over the next few weeks before touching `MIN_SAMPLES`.
- **Units gotcha:** the `veh` speed field in `eta_watch` logs (labelled `mps`) is TransLoc's `GroundSpeed` in **mph**, not m/s.
  (Some earlier speed numbers quoted in this session as m/s were really mph; conclusions from positions were unaffected.)
- No code change for this. Not deployed anything.

### 2026-09-20 · [dev] · Health check no longer cries wolf on "Due" a few seconds after a bus passes (needs `git pull` on [home])
- The 01:30 Night Pilot run flagged "6 full-lap flips." **None were real.** Bus 16 was passing five stops in three minutes
  (up to 27 m/s); each flagged row was our "Due" shown 3-40 s after the bus went by (24-216 m past the stop), which the
  scorer compared against the bus's next crossing a lap (~24 min) away. (Note: an earlier claim that the bus "skipped"
  Valley Rd / Brandon Ave was wrong — it passed them at 6 m and 5 m; that came from a once-a-minute table at 27 m/s.)
- Fix: `eta_compare.score_rows` now records `past_m` (how far past the stop the bus is at that moment); the health check
  exempts a "Due" (<= 30 s) with the bus 0-300 m past the stop and reports it as `flips_just_passed` (informational, never a
  breach). Real flips still trip: run 1 from before the engine fixes still shows 23; tests in `tests/test_eta_health_check.py`.
- **`[home]`: `git pull`** so the scheduled tasks pick it up. The 07:30 Sunday breaches (late %, Orange bias, low
  history share) were NOT affected by this and are still real signals.

### 2026-09-19 · [home] · Claude review + ntfy push is BUILT (needs the phone app to receive)
- `scripts/eta_health_notify.py` (called at the end of `eta_health_check.py`; failures never change its exit code) runs
  headless `claude -p` on the newest result line, gets a 1-3 line verdict (ALL CLEAR / PROBLEM / NOTE), and POSTs it to
  `https://ntfy.sh/<topic>`. PROBLEM goes out at high priority. If Claude is unavailable it falls back to a plain summary.
- Topic lives in the **`NTFY_TOPIC` user environment variable on [home]** (also read from the registry, since long-running
  scheduler sessions can miss new env vars). Not in git. Runs the same 7x/day schedule; no task changes were needed.
- Tested: Claude verdict works on a synthetic result; one test message was POSTed to ntfy. **Not yet verified on the phone:**
  Messages include an ETA accuracy summary (median error, % >2 min late, vs TransLoc, worst route, count). The user still has to install the ntfy app and subscribe to the topic (told to them in the session). Not yet seen in a real scheduled run.

### 2026-09-19 · [dev] · REQUEST (user): after each scheduled ETA check, Claude analyzes it and pushes the result via ntfy
- **What the user wants:** after every scheduled health-check run, **Claude is prompted to analyze the results
  and send the user a push notification through a plain web-push service (ntfy)** — not through Claude's own
  `PushNotification` tool, which was tested and doesn't reach the phone from unattended runs (see §7).
- **Status: approved by the user, not built yet.** Decisions (user, 2026-09-19): **a push after EVERY run** (7 a day —
  a short "all clear" is wanted, not just breaches), **Claude usage cost is not a concern**, and it **runs on `[home]`**.
  Spec and details are in §7 ("Requested next: Claude review + ntfy push"). `[home]`: you're clear to build it.
  The user still has to install the ntfy phone app and choose/receive the channel name — ask them for that.
- **Do not put the ntfy channel name in git.** On the free hosted service the channel name is effectively the
  password (anyone who knows it can read or send). Keep it in a git-ignored local file or a Windows environment
  variable on the machine that sends.
- **Cost:** $0 for ntfy (hosted free tier, or free to self-host); paid tiers exist but aren't needed. The user said Claude
  usage from running `claude -p` after each run is fine.

### 2026-09-19 · [home] · ETA health checks are now automated (on the home server)
- Built `scripts/eta_health_check.py` and registered Windows Task Scheduler jobs on the home server
  (`ETA-Health-*`; full time list in §7, expanded the same day to cover early AM, evenings and overnight). Each runs 30 min, read-only.
  Details and how to change/remove them: §7.
- **Nothing pings the user.** Results just accumulate in `data-local/eta_watch/health_results.jsonl` on the home
  server (not committed). Any session on `[home]` should glance at it (`breaches` non-empty = look closer) and post
  a board entry per §7. The optional "scheduled Claude routine" was NOT built (cost/notification unknowns).
- The scheduled runs will finally give weekday data; Purple only shows up if it happens to be running then
  (`purple_in_service` is recorded per run).

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
- **Routes change shape a lot by day of week and time of day (user, 2026-09-20).** Each variant is its own TransLoc RouteID (e.g. Green: 54 = post-6PM/weekends loop, 68 =
  pre-6PM detour service, 52/66/80/71 = other/older variants; Gold 57 vs 67/56/78; Orange 53 vs 55), each with its own stop IDs, stop order and polyline. Consequences to keep in mind:
  hop-time history is keyed per route, so every new variant starts with NO history (the reason weekend 54/55 history is thin); timestop mappings are per route ID (correct, since a
  route only has the stops it visits); block `route_ids` are route FAMILIES, not one variant; and an accuracy number is only meaningful for the variant that was running. Log which
  RouteIDs are active in each run before comparing across days/hours. Never assume weekend results carry over to a weekday variant.
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
chose to stop (see §2). **Status: automated on the home server (below); you can still run one by hand.**

### Run one check (~35 min, read-only against production)
Any machine with Python 3 and internet, from the repo root:
```
python scripts/eta_watch.py 30 15 data-local/eta_watch/<yyyymmdd-hhmm>.jsonl
python scripts/eta_compare.py data-local/eta_watch/<same-file>.jsonl
```
It only makes public GET requests (about 120 polls × 3 endpoints), so it's light on the one-CPU app. Logs live in
`data-local/` and are **not committed automatically** (only when the user asks; see the 2026-09-20 [home] entry). Details of what the scorer does: §4.

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
  non-zero "only TransLoc" coverage count (TransLoc predicted a visit we didn't). A "Due" shown within ~300 m / a few
  seconds AFTER the bus passed a stop is display lag, not a flip — the health check exempts it (`flips_just_passed`);
- the share of estimates using real history (`historical`) **collapsing below ~5%** — that means the hop-time table has
  emptied again (check the nightly 03:00 rebuild and whether `block`/vehicle grouping still yields buckets). A share of
  5–30% is only a `notes` entry (thin weekend history is expected for now).

**Before calling something a bug:** (1) check whether it's ONE bus — a delayed or parked bus hurts both engines
(look at the worst misses by bus, and whether TransLoc missed the same rows); (2) any cluster **both** engines
share is a scorer artifact until proven otherwise; (3) early misses are the lesser problem.

### Recording results
- Routine pass with nothing unusual: no board entry needed.
- Post a short `[machine]` entry on the message board (numbers + one line of interpretation) when a threshold is
  breached, or when the check covers a new kind of hour/day (first weekday morning, first Purple run, …).
- Real fixes go in code with a commit message that says what was measured, as before.

### Automation (BUILT 2026-09-19, on the home server)
- `scripts/eta_health_check.py [minutes=30] [poll_s=15]` = watch + score + one summary line appended to
  `data-local/eta_watch/health_results.jsonl` (fields: headline errors vs TransLoc, per-route numbers,
  `historical_pct`, `full_lap_flips`, `purple_in_service`, `breaches`, or `inconclusive` if buses weren't running /
  too little data — that's not a breach). Exit code 0 clean/inconclusive, 2 breach, 1 check couldn't run.
  Thresholds are the constants at the top of the script (the §7 numbers) — edit them there once weekday data exists.
- Scheduled via Windows Task Scheduler on the home server (local Eastern time, `ETA-Health-*` tasks):
  Mon–Fri 04:30, 08:30, 12:30, 17:00, 19:30 · Sat+Sun 07:30, 12:00, 14:00, 17:00, 20:00 ·
  every day 00:00 and 01:30. (Updated 2026-09-19 at the user's request; the earlier Saturday-only 12:00 task was
  folded into the Sat+Sun noon task; the Sunday-only 14:00 became Sat+Sun. Night Pilot runs until 2 AM every night while classes are in session, so the overnight runs should catch it.) They run under
  `pythonw.exe` from the repo root, only if the machine is awake/online (missed runs start when available).
  Manage: `Get-ScheduledTask ETA-Health-*` / `Unregister-ScheduledTask -TaskName ETA-Health-Sunday -Confirm:$false`.
  A `git pull` on the home server updates the script the tasks run.
- Claude review + ntfy push: **built 2026-09-19 [home]** (`scripts/eta_health_notify.py`, see board); "Requested next" below is the original spec.
- **Tested 2026-09-19 [home]: headless `claude -p` cannot ping the phone.** Run from Task Scheduler (nobody at the
  terminal), with and without `--remote-control`, `PushNotification` returned "Not sent - this terminal is active",
  and the user's phone received nothing. (`claude.exe` lives in the user's `.local\bin`; a headless run does
  follow CLAUDE.md's startup routine. A local scheduled `claude -p` review that writes to HANDOFF.md is feasible.)
  User said to drop pings for now; the only push route left would be a plain web push service (e.g. ntfy) called
  from the script.

### Requested next: Claude review + ntfy push after each run (2026-09-19, not built)
**The user's ask, in their words:** Claude should be prompted after each run to analyze the results and send the push
notification through this service (ntfy).

**Intended flow (per scheduled run):**
1. `eta_health_check.py` finishes and appends its summary line to `data-local/eta_watch/health_results.jsonl`.
2. A follow-up step starts a headless Claude session (`claude -p ...`, which the `[home]` test showed does follow
   CLAUDE.md's startup routine) with a prompt like: read the newest line(s) of the results file, compare against the
   thresholds and the run-6 baseline in this section, decide whether anything is off (one delayed bus vs. a real
   problem; scorer artifacts; `inconclusive` is not a breach), and write a short plain-language verdict.
3. That verdict is sent to the user's phone through **ntfy**, by a plain HTTP request (`curl`/`Invoke-RestMethod`
   POST to the channel URL). Claude's own `PushNotification` tool is NOT used — it returned "Not sent" from unattended
   runs and nothing reached the phone.
4. Per the board rules, a run that breaches a threshold or covers a new kind of hour/day also gets a message-board entry.

**Constraints / facts (checked 2026-09-19):**
- ntfy hosted service (ntfy.sh): free, no sign-up; paid tiers (~$5/$10/$20 per month per a third-party listing) add higher
  limits and reserved private names — not needed here. The server is open source and free to self-host, but reliable
  instant delivery to a phone from a self-hosted server is fiddlier (hosted ntfy.sh uses Firebase for Android push).
  The exact free-tier daily message cap wasn't found; irrelevant at our volume (7 messages/day).
- **The channel name is the password** on the free tier: use a long random string (20+ chars), keep it out of git,
  and treat the messages as non-secret (short ETA summaries only — never keys, cookies, or internal URLs).
- The user must install the ntfy app on their phone and subscribe to the channel before anything can be received.

**Decisions (user, 2026-09-19) — these settle the earlier open questions:**
- **Frequency: a push after every run.** The schedule above is 7 runs/day (Mon–Fri 04:30, 08:30, 12:30, 17:00, 19:30
  plus 00:00 and 01:30; Sat+Sun 07:30, 12:00, 14:00, 17:00, 20:00 plus 00:00 and 01:30), so 7 messages a day. A one-line
  "all clear" is fine and wanted; make a breach message stand out (e.g. higher ntfy priority or a clear "PROBLEM" prefix).
- **Claude usage cost: not a concern.** No need to optimize the prompt for cost, but keep it focused so runs finish quickly.
- **Where it runs: `[home]`** (owns Task Scheduler, always on). Headless runs need `claude.exe` reachable from the scheduled
  task (it lives in the user's `.localin`, per the earlier test).

**Still needed from the user:** install the ntfy app on their phone and subscribe to a channel. Generate the long random
channel name on `[home]`, store it locally (git-ignored file or Windows env var), and hand it to the user to type into the app.

**Deploys:** none involved. Nothing here touches the Fly app; adding it is a scheduler/script change on `[home]`.
