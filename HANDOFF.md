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

### 2026-09-24 · [home] · /livemap over-height bridge alert (DEPLOYED 2026-09-24; engage + release tested locally in Chrome) + /map-vs-/livemap audit
- **Built:** `core/layers/overheight.js` (+ `overheight-style.js`, popup CSS in `livemap.css`). When a bus on `OVERHEIGHT_BUSES` (from `/v1/config`, testmap defaults as fallback) is inside `BRIDGE_RADIUS` of the bridge: map eases to the bridge (zoom>=18), gestures locked, red disc, "OVERHEIGHT VEHICLE" popup glued to the bus; releases + returns to the prior view when none is in range. **Gate = ANY authenticated dispatcher (`isAuthed()`), plus `?dispatcher=true` / adminKiosk** (user asked 2026-09-24: too safety-critical to limit to the dispatch iframe; it seizes the camera for them by design; `?adminMode=false` does not veto it). The disc layers are baked into the style just under the vehicle layer. Verified locally in Chrome against a live bus (engage, disc, popup, locked handlers); release (popup gone, gestures back, view restored) also verified. `dispatcher.html` still iframes `/testmap?dispatcher=true`, so this only matters once that iframe moves to `/livemap?dispatcher=true`.
- **Audit result (user decisions 2026-09-24):** Amtrak/radar/aircraft/battery never coming to livemap; Traccar disused; OnDemand route lines unnecessary (vandispatch2 exists); headway bubbles no longer needed; agency switcher is deliberately absent (livemap is single-feed). **Headings checked:** `/v1/testmap/transloc/vehicles` already returns server-derived sticky headings for on-route buses, so livemap using raw `Heading` is fine there; only route-0 (out-of-service) buses get TransLoc's raw heading (0 when parked) vs testmap's client-side bearing-from-movement. Cosmetic, left as is.

### 2026-09-24 · [home] · /livemap off-route badge for dispatchers (DEPLOYED 2026-09-24, NOT visually verified)
- **Built** (`scripts/livemap/core/layers/vehicles.js`, `data/transloc.js` `getRouteShape`): UTS buses >60 m (same as /map) from their own route polyline get a red "!" disc, dispatcher-only (`isDispatcher()`, so `?adminMode=false` hides it). Distance is computed in `deriveProps` (flat-earth metres vs `getRouteShape(routeId).coords`); re-derived when route geometry loads (`onRoutes` -> reingest).
- **Layering:** the badge is baked into the bus's ONE composite image (same trick as the number/block pills), NOT a separate layer, so it can never land above/below the wrong things. Off-route buses get a composite even with labels off (empty pills). `sortKey` gets +1e8 so an off-route bus draws above all normal buses (testmap's +1200 z-offset equivalent).
- **Check on deploy:** load `/livemap` signed in as dispatcher, find/force an off-route bus (or `?dispatcher=1` with a mocked position), confirm badge sits right of the pin, clear of pills, at all zoom levels.

### 2026-09-24 · [home] · /livemap is now an installable PWA + has the push bell (DEPLOYED)
- **Built:** `livemap-manifest.json` served at `/livemap.webmanifest` (start_url `/livemap?source=pwa`; the old `/manifest.json` still starts at `/`); `livemap.html` links it, sets theme/iOS tags and registers `service-worker.js` (before, only the index pages registered it). The service-alert bell (`scripts/push-notifications.js`) now seats itself in `[data-push-bell-slot]` inside the map-controls cluster (`MapControls` loads the script after mounting; `css/livemap.css` `.map-ctrl-push-slot`). Other pages keep the floating bell. Push notification click target changed `/map` -> `/` (index is where service alerts show): service worker default + the low-battery and TransLoc alert pushes in `app.py`.
- **Verified:** layout in local Chrome with the bell forced visible (no VAPID keys locally). NOT verified: subscribe flow end to end, install on a phone.
- Context: the user floated a native Android/iOS app; feasibility dive concluded feasible but non-code blockers (App Store 4.2, licensing/branding, Fly capacity, FCM/APNs) so they shelved it ("pipe dream"). PWA is the cheap 80%.

### 2026-09-24 · [home] · /livemap trip planner MOBILE: list still needs an app switch to appear (FIXED: user confirmed on phone 2026-09-24; the click-outside-after-reparent hypothesis was right)
- User confirmed desktop fixed, phone unchanged (pages are `no-store`, so the phone has the new code). Can't reproduce a WebView bug in desktop Chrome (a 400px iframe of /livemap gets the real mobile layout and works fine).
- **Hypothesis:** the tap reparents the focused input into `.tp-search-overlay`, so the follow-up click lands outside `f.wrap` and the document click-outside handler hides the list. **Change:** ignore click-outside for 700 ms after the overlay opens, and re-assert the empty-state list 400 ms after open if missing/hidden (`trip-planner-panel.js`).
- If the phone STILL needs the app switch: next suspects are the `translateX` overlay transition/compositor bug (try dropping the slide animation entirely on mobile) or the reparent-blur; get a remote-debug console from the phone.

### 2026-09-24 · [home] · /livemap trip planner: empty-field list (Your Location / Choose on map / Recent) missing on DESKTOP (FIXED `8fd8949`, verified on desktop locally in Chrome, DEPLOYED to prod 2026-09-24; phone still unverified)
- **Root cause:** `18ccb38` moved the empty-state populate into `_maybeOpenOverlay()`, which returns early on desktop (`!_isMobileLayout()`), so the focus handler never populated on desktop. Same gap on mobile when the overlay was already open for that field.
- **Fix** (`scripts/livemap/ui/trip-planner-panel.js`): `_maybeOpenOverlay` now returns true when it populated; the focus handler populates itself otherwise; a click on an empty, already-focused field with a hidden list reopens it. Needs `fly deploy` + a live check on desktop AND a phone.

### 2026-09-24 · [home] · /vandispatch Active Trips card showed the wrong driver for a shared van (FIXED, committed `311d8bb`, deployed to prod)
- **Symptom:** Van 13 trip card showed Nidal while the roster card said Jaquan (Nidal's shift hadn't started). **Cause (found by reading, not reproduced live):** `trip-board.js` built `vehicleId -> driver` with last-duty-wins, so a van with 2+ duties in a day showed whichever duty came last in the list. **Fix:** trip cards now look the driver up by the trip's own `dutyId`; per-van fallback prefers the in-progress / current-window duty over completed/cancelled ones. Roster (`duty-roster.js`) untouched. Not covered: the map van label, if it derives its driver separately (unchecked).
- **Verified:** logic test (Van 13 scenario) AND the user confirmed on live data after deploy that the trip card shows the right driver. Done; nothing pending. (Map van label not checked, but no symptom reported.)

### 2026-09-24 · [home] · Evening route changes ("EVENING ROUTE CHANGE" text boxes): investigated, NOT built yet; a capture is scheduled for tonight 17:40
- **TODO (any session that reads this on/after 2026-09-24 18:20): CHECK THE RESULTS of the 17:40 capture.** Nobody reads them automatically. Run `python scripts/eta_phantoms.py data-local/eta_watch/20260924-1740.jsonl`, apply the verdict rule under "Next step" below, and tell the user. Then replace this line with the outcome.
- **User's idea:** like the out-of-service cut-off, use the block package's "EVENING ROUTE CHANGE" notes (text boxes on Gold/Green/Orange weekday sheets; `sheet_drawing_texts` already returns them, `parse_out_of_service_notes` skips them). Notes: Gold 09/11 "after leaving HER/BAR at 1750", Gold 10/12 "at 1800", Green 01 "after HER 1745 ... at McCormick/Alderman go straight onto McCormick", Green 02 "after MP 1745", Orange 05 "after MP 1750", Orange 07 "post-1800 route starting with the 1800 PIN departure". Post-1800 routes are different TransLoc route ids: Gold 67->57, Green 68->54, Orange 53->55.
- **The routes really do differ (from prod `/data/vehicle_logs/<day>_routes.json` + bus tracks):** post-6PM shapes are 21-50% off the pre-6PM polyline, and the served stops differ (Gold pre serves 4 Emmet St stops, post serves 4 McCormick Rd + 2 Newcomb stops; Green post goes to JPA/14th St, pre doesn't; Orange similar). BEWARE: `/v1/trip-planner/uts-graph` shows the not-currently-live variant with `shapeSource: "borrowed"` (the sibling's geometry and stops), so during the day 57/54/55 look identical to 67/68/53. Don't compare variants from that endpoint.
- **TransLoc already flips route ids in lockstep, three days running (09-21/22/23, identical to the minute):** Green 68->0 at 17:45:2x, ->54 at 17:47; Gold 67->57 and Orange 53->55 at 17:51:1x-3x (bus 39/17532 individually 17:55->0, 17:57->57); 18:01:xx three buses go to 0 (the 1800 cut-offs). So the flip is a scheduled trip boundary inside TransLoc, roughly 1 min after the package's change time. That is why I did NOT assume a big problem: the window where our engine holds a bus on the old route after it physically changed looks like about 1-2 min, and blocks whose change is 18:00 (Gold 10/12, Orange 07) sit at the HER/PIN layover when they flip at 17:51, where both routes share the stop.
- **What is NOT known:** what the ETA feed actually shows in 17:45-18:15 (no watch logs cover it; the 17:00 runs end 17:29 and the 19:30 runs start after). Phantom predictions (a stop we predict that the bus then never reaches) are invisible to the normal scorer. So: scheduled one-off `ETA-RouteChange-2026-09-24` (Task Scheduler, 17:40, `eta_health_check.py 40 15`), and added `scripts/eta_phantoms.py <log>` (per-minute count of predictions due within 10 min that never resolved; baseline on a normal morning log is ~5.7%, noisy and rising toward the log's end, so read it relative to the quiet minutes). Log will be `data-local/eta_watch/20260924-1740.jsonl` (+ `.graph.json`).
- **Next step:** run `python scripts/eta_phantoms.py data-local/eta_watch/20260924-1740.jsonl`, look at minutes 17:44-17:58 vs 17:40-17:44. If phantoms spike or errors are large right after the flips, build it (parse the change notes into `config/uts_blocks.json` next to `out_of_service`; hold/switch a bus's route by block+time). If flat, the notes add nothing beyond TransLoc's own flips and we skip it.
- Raw vehicle logs pulled to `data-local/route_change/` (not committed). Pulling them: `MSYS_NO_PATHCONV=1 fly ssh sftp get /data/vehicle_logs/<yyyymmdd>_<HH>.jsonl <dest>` from the repo root (file hour is ET; prod keeps 7 days).

### 2026-09-24 · [home] · ETA health logs can't be re-scored later (TransLoc renumbers stop IDs per service variant); fixed by saving the graph with each log
- **Found while digging into Wed 09-23 19:30 (14.6% late):** re-scoring that log today gave 1,009 rows instead of the 11,913 the live check saw. `eta_compare.load_graph()` fetches TODAY's `/v1/trip-planner/uts-graph`, and TransLoc gives every route new RouteStopIDs when the service variant changes (evening vs day, detours; e.g. Gold 57 was stops 814-844, now 874-901). So an old log only matches the graph from when it was recorded. Only route 58 survived the re-score, and it was NOT the source of the late misses (bus 26/38 median +18 / -69 s, 0% late), so the earlier "route-58 block change at 6 PM" guess is unfounded. The cause of that run's 14.6% is unknown and now unrecoverable.
- **Fix (scripts only, engine untouched):** `eta_compare.load_graph(log)` saves `<log>.graph.json` (~225 KB) next to the log on first use and re-reads it afterwards; `eta_health_check.py` and `eta_compare.py` main pass the log path. Logs recorded before this have no saved graph, so don't try to re-score them. Verified on the 09-24 08:30 log: same 14,138 rows as the live result.
- **Numbers, 12 runs 09-22 12:30 to 09-24 08:30:** unchanged picture (daytime median |err| 47-72 s, 17:00 rush 103-121 s early bias, Purple Fontaine over 90 s every morning, Night Pilot 15-24 s). 09-24 08:30 (first weekday day after the cut-off deploy): 2.3% late, only breach is Purple Fontaine. Evening 19:30 runs are the weakest slot both nights.
- Not committed: `data-local/` (standing rule).

### 2026-09-24 · [home] · /vandispatch "shifts with no vans / vans with no shifts": investigated live, NOT reproduced, no code changed
- **Checked on the Fly machine (read-only):** W2W shifts vs Spare duties at 11:06 ET. Names match exactly (Alsamman, Jones, Debruhl, Bowles) and every current Spare duty lines up with a FlexRide shift inside the 1 h slack. No OnDemand vans active. No `[spare]`/W2W fetch errors in the log buffer. Unrelated: `/v1/pulsepoint/incidents` returns 500 (JSONDecodeError) repeatedly.
- **Matching lives in** `scripts/livemap/apps/vandispatch2/duty-roster.js` `renderDuties()` (exact normalized-name + time window; nothing server-side). Latent weak spots, unverified as the user's symptom: (1) a Spare duty running past its W2W shift's end loses the shift (backend `_build_driver_assignments` drops ended shifts) and can grab the driver's NEXT shift via the 1 h slack; (2) `find()` takes the first overlapping shift, not the closest; (3) spare duties are matched only against `FlexRide Driver` shifts, so an OnDemand-position driver on a Spare van would show as two cards; (4) `api_spare_duties` caches `[]` for 60 s if the upstream call fails.
- **Next step:** need the specific driver/time it looks wrong for, ideally while it is wrong.

### 2026-09-24 · [home] · Kiosk app-version reporting (server + kiosk), and the f9fc0a5 update was seen landing
- **Confirmed:** after the `prod` push the user saw a kiosk restart (self-update applied it); check-in log showed both kiosks with a ~30 s gap at ~14:51 UTC (10:51 ET). Self-update wiring audited in `uvatransit-kiosk` and looks right (timer 3 min after boot then every 10 min, reads channel from the local status server on :8765, pulls the channel-named branch from the public repo, installs the app files, restarts `kiosk.service`).
- **Built:** kiosks now send `app_version` (contents of `/etc/kiosk/app-version.txt`, the commit the updater last installed; blank if it has never self-updated) in `POST /v1/kiosk-checkin`. `app.py` stores it per device only when the key is present (old kiosks don't clobber it, value clipped to 40 chars); `/kiosk-fleet` has a new "App Version" column (first 7 chars, full value on hover). A kiosk showing blank after it should have updated means the updater is not working there.
- **Known gaps in the updater (need a reflash to fix, NOT done):** it never replaces itself, its timer/service, `kiosk.service`, or the reboot cron; and it counts a half-install (missing file on the branch) as success, restarts anyway, and records the SHA. Fix idea: verify every source file exists before installing any, else skip and don't record the SHA.
- The kiosk-side change is in `uvatransit-kiosk` (same `kiosk-launch.sh`), pushed to master + prod.

### 2026-09-24 · [home] · Kiosk flapping SOLVED (root cause is in the kiosk repo, not the backend); fix PUSHED to `uvatransit-kiosk` master + prod (`f9fc0a5`)
- **Cause:** `uvatransit-kiosk` -> `stage-kiosk/01-kiosk-app/files/kiosk-launch.sh`. `checkin()` tried ONCE (5 s limit) and `decide_target()` swapped to `dashboard-down.html` ("Can't reach the ops dashboard") the instant one check-in failed, then back on the next success 15 s later. Any single dropped/slow request = flap. Flapping unit: MAC 88:A2:9E:8F:B3:50 (`url` override = `https://utsopsdashboard.com/downed?kioskMode=true`, channel prod). Only 2 kiosks currently check in (that one and B8:27:EB:05:67:CB, both prod, same build).
- **Backend is fine:** machine idle (load 0.2), no `[loop-lag]` stalls at a flap (10:38:11 ET), check-ins were 200 at 14:38:01 and 14:38:17, Google downed sheet always returns good CSV. Occasional connection resets/20 s hangs to prod exist from the [home] box (fly.io's own hosts show it too) so the network path does drop requests sometimes; the kiosk just never tolerated that.
- **Fix (local clone `Documents/GitHub/uvatransit-kiosk`, uncommitted, on master):** `checkin()` retries once after 1 s; new `CHECKIN_FAIL_STREAK`; `decide_target()` keeps the current page until 3 consecutive check-in failures (~45 s). Simulated: 1-2 misses never change the screen, 3+ shows the error page, recovery is immediate.
- **Rollout (DONE, user OK'd going straight to prod; only 2 test units):** kiosks self-update every 10 min from the branch named after their channel. `prod` was 5 weeks stale (d72e265, still the old iframe design, and lacking `dashboard-down.html` + `cdp-navigate.py` that self-update installs) while the kiosks' image (built 2026-08-25 16:05 ET) already runs master's navigate-in-place script. So a patch on prod's old script would have regressed them; instead I pushed master and fast-forwarded `prod` to master (`f9fc0a5` = the 3 unreleased 08-25 commits + the fix). `dev` is still d72e265 (stale; the dev unit B8:27:EB:43:C3:6E hasn't checked in for ~30 days). Each kiosk restarts its display once when it picks the update up. The clone at `Documents/GitHub/uvatransit-kiosk` has a repo-local git identity set.
- Other changes today (harmless, deployed): `[http_service.concurrency]` in `fly.toml` and the `[loop-lag]` watchdog in `app.py`. `data-local/kiosk_watch/prod_kiosk.log` capture was a background job; not needed anymore.

### 2026-09-23 · [home] · Cut-off live test 21:46-22:03: 6 of 7 buses exact; two Orange [05] bugs found and fixed live (3 deploys total); prod = `claude/oos-cutoff` b4eebf7
- **Verified against an independent expectation** (each bus should predict exactly the stops from its next stop through its cut-off): Green 01/02, Orange 07, Gold 09/11/12 all MATCHED (11, 5, 11, 11, 11, 17 stops). The only wrong bus was Orange [05] (vehicle 17432).
- **Bug 1 (deploy #2, `82eb544`):** [05] parked AT the Library (its cut-off) projected a few metres past the end of its last segment, so it stopped counting as on its last run and predicted 20/20 stops; and the "next stop is the target" shortcut returned the first stop past the cut-off before the check ran. Fixed with a 40 m tolerance (`OOS_CUTOFF_TOL_M`), moving the cut-off setup ahead of the shortcut, and `bus_eta.out_of_service_phase` / `out_of_service_finished` + per-bus memory in `app.py` (`_oos_run_seen`): a bus seen on its last run and later outside it gets no ETAs; never-seen buses are treated as finished 45 min after the scheduled departure (server restart).
- **Bug 2 (deploy #3):** at ~22:00 dispatch changed [05]'s block to Night Pilot [03] while TransLoc still listed it on route 55, so 20 Orange stops were predicted for a bus that no longer serves them. Fixed with `_oos_handover` (a bus seen on its last run that takes a different block on the same route is done) and the stateless `uts_blocks.block_mismatches_route()` (block's line family known and excluding the route; a route no family lists is never a mismatch, so stale config can't blank a bus). After the fix 17432 shows Night Pilot (route 59) with all 14 Night Pilot stops and no Orange stops.
- **Fact learned:** at their 22:00 last departures, Gold 09/11/12, Orange 07 and Green 01/02 were all set to **route 0 (out of service) in TransLoc within minutes**, even though the block package says they carry passengers on to their cut-off. So those buses leave our feed at 22:00 regardless; the cut-off only bites for buses that stay on their route (Orange [05] 21:40-22:00, Night Pilot handovers) or before dispatch flips them. Not investigated: whether riders can still board those buses after 22:00 (nothing we can predict for them under a route id).
- **Test gap to remember:** the `app.py` glue (`_oos_run_seen`, `_oos_handover`, the mismatch skip) has no unit test (it lives inside the big ETA loop); the pure pieces do. My first verify script hid Bug 2 by crashing on a bus with no plan; use `live_verify2`-style checks that handle that.
- **Merged:** `claude/oos-cutoff` is in `main` (`d893251`); the branch can be deleted. `claude/late-dwell` (opt-in `late_dwell_s`, not wired, not deployed) is still an unmerged branch. `ETA-CapReplay-2026-09-27` is still scheduled.

### 2026-09-23 · [home] · DEPLOYED (fly deploy, ~21:45 ET) the out-of-service cut-off from branch `claude/oos-cutoff` `0483c3f` (NOT merged to main)
- **What is live:** everything on `claude/oos-cutoff` (cut-off incl. full-lap and Night Pilot HER, schedule notes in `config/uts_blocks.json`, landmarks file). The layover cap is unchanged (day-independent) and `late_dwell_s` is NOT in this build. ~~Prod runs code that is not on `main`~~ **MERGED to `main` 2026-09-23 22:10 (`d893251`, byte-identical to what is deployed): prod == main.** Roll back with `fly deploy` from `main` (`a1300ec`-era code), or `fly releases` / `fly deploy --image` to the previous image `deployment-01M30PDYQTQD1PGVE63ERFBRVD`.
- **First live check (21:46, 7 buses with plans):** health ok. Orange block [05] (bus 17432, left PIN 21:40, final run) has predictions for only the 4 stops between it and the Library (Grady 14th 30 s, Grady Preston Pl 66 s, Rugby 134 s, Library 310 s) and NOTHING for the 14 stops past the Library: the cut-off works on live data. The other six plans (Green 01/02, Orange 07, Gold 09/11/12) were "not yet" (their last departures are 22:00).
- **Watch next:** after 22:00, Gold 09/12/11 and Orange 07/Green 01/02 should stop predicting past their cut-offs (Gold 09: DORMS stop 840; Gold 11/12: BAR 829; Orange 07: LIB 706; Green 01: MP 657; Green 02: CHP 653). `data-local/eta_watch/live_check` style: fetch `/v1/eta/uts_stop_arrivals` and count stops per (route, vehicle, block). Things that would be wrong: a bus on its final run still showing every stop, or (worse) a bus NOT yet on its final run losing stops it still serves (check a bus approaching its 22:00 departure keeps its full loop).

### 2026-09-23 · [home] · Out-of-service notes: all 19 block-days verified + user corrections applied; late-dwell prototype measured (small win, NOT wired)
- **Coverage (user asked "did you get them for every block?"):** yes. Re-read every active sheet's text boxes: 19 of 19 block-days in `config/uts_blocks.json` have a note; the 3 apparent mismatches were notes that mention Night Pilot blocks (03/04) from other lines' sheets, not missing notes. After the corrections below, all 19 resolve to real stops on at least one running route variant (the leftover "?" are only on pre-6PM variants 53/67/68 that never serve those stops).
- **User corrections, applied on `claude/oos-cutoff` (`c56b205`, pushed):** (1) Night Pilot [04] "until HER": on route 59 that is "Hereford Dr @ Runk Dining Hall" = RouteStopID 735 (the same name HER has elsewhere; there is no "McCormick Rd @ Runk" on route 59). Mapped as a cut-off-only LANDMARK in `config/uts_landmarks.json` (`HER: {59: 735}`), NOT in `uts_timestops.json`, so it does not switch on the layover cap. (2) Orange weekend [05] "make final loop" = the cut-off is the next time it is back at LIB, where it becomes Night Pilot [03]. Silver [14] "as far as MCQ" (its own leave stop) is the same shape. The parser now records cut-off == leave stop and `bus_eta` reads that as the NEXT arrival there (position-based "already on the lap" detection is time-gated so a bus still approaching its departure is not cut off at it). (3) Silver [13]/[14] come from the Spring 2026 sheet and that is fine: the Silver schedule doesn't change between spring and fall, nobody re-labels the sheet. Tests added for the full-lap walk, landmark resolution and the parser.
- **Late-dwell prototype (branch `claude/late-dwell`, merged with the cut-off branch, replay only):** `estimate_stop_eta_s(late_dwell_s=...)`, default 0 = today. A bus that arrives after its scheduled timestop time now sits `late_dwell_s` before leaving instead of leaving the instant it arrives. Replays with real block ids: weekday runs 09-21..23 (12 runs, 159,843 predictions) and Sunday 09-20 (3 runs, 14,912; Orange/Green blocks filled in). Overall median error / >2 min late / >2 min early: weekday 0 s = -53 s / 3.0% / 30.8%; 45 s = -48 s / 3.2% / 29.7%; 90 s = -45 s / 3.9% / 28.5%. Sunday 0 s = -47 s / 2.5% / 28.5%; 45 s = -44 s / 2.5% / 26.5%; 90 s = -39 s / 2.5% / 25.3%. It only moves Gold: weekday Gold daytime (route 67) 0 -> 45 s: median -68 -> -57 s, >2 min early 36.5% -> 33.6%, late 1.8% -> 2.2%; at 90 s route 67 late rises to 3.6% and Green (68) to 3.2%, so **45 s is the safe value**. Sunday Gold 57: -54 -> -48 s. Orange, Green (weekend), Purple, Silver: no change or negligible. **It is a small win (about +5 s of bias, +2 pts fewer early misses), NOT the main cause of the weekday early lean**, which stays at -50 s overall and grows with horizon (-30 s at 2-5 min out, -100 s at 10-20 min, -160 s at 20+ min). Not wired into `app.py`; worth deploying with the cut-off only if we want a tiny Gold improvement.
- **Where the big early bias probably lives instead (untested):** it grows with number of hops, which points at per-hop history medians being a little short (right-skewed hop times) or pace decay, not at timestop dwell.

### 2026-09-23 · [home] · Out-of-service cut-off COMMITTED on a branch (not merged, not deployed); health-check scorer noise fixed on main
- **Branch `claude/oos-cutoff` (`1b41dc9`, pushed):** the out-of-service cut-off + `is_timestop_at` (unwired) + `scripts/eta_replay_cap.py` + tests, described in the entry below. NOT merged to main, NOT deployed (no `fly deploy`). Plan: deploy after review; the first weekday evening after that is the real test (Gold blocks 09/12 leave BAR at 22:00, Orange 07 leaves PIN at 22:00, etc.).
- **UPDATE 2026-09-23: merged to `main`, and the [home] working tree is now ON `main`** (the scripts the Sunday replay task needs are all in `main`). Old note: the working tree was deliberately left ON that branch: the one-off Sunday replay task (`ETA-CapReplay-2026-09-27`) and the scheduled health checks run from this checkout, and the replay needs the branch's `uts_blocks.py`/`scripts/eta_replay_cap.py`. `scripts/eta_health_check.py` is identical on both, so the health tasks are unaffected. Switch back to main only after that task has run (or merge the branch).
- **Scorer fix on main (`eta_health_check.py`):** "visits TransLoc predicted that we didn't" now ignores (a) the first `WARMUP_POLLS = 2` polls of a run and (b) a bus whose predictions drop for exactly ONE poll and return (the vehicle/stop is present at the previous and the next poll). Both are still reported as `only_transloc_ignored: {warmup, single_poll}`. Gaps of 2+ polls still count and still breach. Rescoring Sep 21-23: 315 raw -> **93 persistent** (65 warm-up, 157 single-poll); runs flagged for this drop from 12 to 6. The `only_transloc` key in `health_results.jsonl` therefore means "persistent gaps" from now on (older lines are the raw count). Tests in `tests/test_eta_health_check.py`.
- **Still worth a look (not done):** the 93 persistent gaps, e.g. Mon 09-21 12:30 (24), 17:00 (32), Wed 08:30 (17). Multi-poll dropouts may be real.
- **Also started:** offline prototype of the late-dwell fix (see the "Timestop adherence" entry), replay only.

### 2026-09-23 · [home] · Sunday Orange: is 09-20 a one-off? (user's hypothesis) -> largely yes; the "time-aware cap regresses Sundays" verdict is now MIXED, decide after Sun 09-27 logs
- **Sunday Orange is schedule-locked to a 30-min loop.** Headway archive (Dec 2025-Sep 2026, 20 Sunday vehicle-days, 383 loops, MP-to-MP): median 29.9 min, day medians 29.3-30.9. 09-20 was 30.5 (30.0 in the 13:30-15:00 window the 14:00 replay covers, other Sundays' median 30.5, range 28.1-35.4). It was NOT a slow day.
- **What was unusual on 09-20 is the split, not the total:** Library layover 435 s (13:30-15:00) vs 95-204 s on the last four Sundays this semester (and 250-590 s last semester). Loop time stays 30 min, so the layover is just the slack the driving leaves; history's median LIB dwell (247 s) is a poor predictor of any one Sunday. Sunday schedule = LIB visit every :00/:30, and the hold fires (bus at LIB ~13:50 is held to 14:00), so holds are already doing the right thing.
- **Replay redone with block [05]/[01] filled in for the Sunday Orange/Green** (the 09-20 logs predate BlockId; Sunday has exactly one of each): time-aware cap vs the day-independent cap, Orange/55 only: 14:00 run WORSE (median +45 -> +90 s, >2 min late 24% -> 41%); 17:00 run BETTER (median -100 -> -60 s, early >2 min 46% -> 24%, late unchanged 1.5%); 20:00 run BETTER (-105 -> -58 s, early 47% -> 20%). All Sunday runs together: >2 min late 2.5% -> 3.7% (WORSE, crosses the 3% limit), median |err| 65 -> 62 s and >2 min early 28.5% -> 24.3% (BETTER). So it trades early misses for late ones, and 14:00 on 09-20 (long Library layover, faster driving) is the bad case. That fits the user's read that the bad window may be a one-off day, but ONE Sunday of prod logs cannot prove it.
- **Set up 2026-09-23 [home]:** one-off Task Scheduler task `ETA-CapReplay-2026-09-27` (Sun 2026-09-27 21:00, after the 20:00 health run) runs `python scripts/eta_replay_cap.py data-local/headway_archive/headway 2026-09-27T05:00 "data-local/eta_watch/20260927-*.jsonl"` and writes `data-local/eta_watch/cap_replay_20260927.txt`. Read-only. `scripts/eta_replay_cap.py` is UNTRACKED on [home] like the engine changes (it imports `uts_blocks.is_timestop_at`/`out_of_service_plan`, which exist only in the uncommitted working tree). Note the headway archive only runs through 09-20, so the frozen history is a week stale, fine for this comparison. Look at the `route 55` block per run. Remove with `Unregister-ScheduledTask -TaskName ETA-CapReplay-2026-09-27 -Confirm:$false`.
- **Next step (cheap, decisive):** the scheduled ETA-Health runs on Sun 09-27 give fresh Sunday logs (with real BlockIds). Replay them old vs `is_timestop_at`; if Orange is neutral-or-better across those, the time-aware cap is worth wiring. Until then it stays unwired (`app.py` day-independent). Correction to my earlier wording in the 09-23 out-of-service entry and the 09-20 DON'T entry: read "worse" as "mixed, one bad window".

### 2026-09-23 · [home] · Timestop adherence measured on weekday logs (171 visits, block ids from the logs): holds are right, late buses dwell longer than we assume
- **Method:** for each bus with a block id, every visit to a mapped timestop (110 m radius, gaps <=60 s merged: a bus creeping round a lot leaves a tight radius for a poll or two and a 60 m detector split one BAR visit into two, which briefly looked like "BAR is never held"), compared arrival/departure with `scheduled_hold_epoch`. Logs 09-21..09-23, 13 sessions. Script was scratch (not committed); n is small (4-17 per stop).
- **Buses wait for the schedule.** Early arrivals (dwell 3-6 min) leave a median +31-46 s after the scheduled time at EVERY timestop incl. Gold BAR (0% skipped the wait). Our assumption (schedule + 30 s) is within ~15 s there. Only 4% of visits (7/171) left >=2 min early; Wed 09-23 Gold block [10] (vehicle 17532) at CHP -3.7 min and BAR -3.5 min is the extreme case, one driver, not a pattern.
- **Late arrivals dwell longer than we assume.** A bus that arrives AFTER its scheduled time still sits 60-75 s (Gold BAR ~180 s, Gold HER 60 s) before leaving, but the walk lets it leave immediately (arrival + the 30 s post-hold hop allowance). Modelled departure error for late arrivals: about -40 to -55 s (too early) at HER/MP/CSW/MCQ, ~-160 s at Gold BAR (n=13). 35% of all visits leave >=2 min after schedule, so this matters. It fits the weekday early lean (median error -40..-105 s) and may explain part of the rush-hour bias. NOT acted on (constants like `SCHEDULED_DEPARTURE_LAG_S`/`POST_HOLD_HOP_ALLOWANCE_S` were tuned on Sunday data; needs a weekday replay WITH holds before changing anything).
- **Orange/Sunday (why a time-aware cap scores worse):** hop-by-hop, Sunday history matches real hop times within ~10 s except the Library layover (real 363 s vs history 247 s); the replayed Orange ETAs are still late for bus 13 and grow with horizon (+139 s median at 10+ min out, replay has no block ids for that day so no holds). Cause not found; replay fidelity vs live is a suspect (live was +47 s at 10+ min).

### 2026-09-23 · [home] · Out-of-service cut-off BUILT (uncommitted, NOT deployed); time-aware layover cap re-tested and STILL a regression
- **Trigger (Wed 19:30 run, bus 17532 = TransLoc VehicleID 39, block [10], Gold/57):** 65% of that run's >2-min-late predictions came from this one bus, a constant ~+283 s on every stop after each of two timestops. We held it to the scheduled departure at Chapel (~9 min assumed, bus paused 2:15) and then at Barracks Road (BAR) (hold to 19:55, bus left 19:51:31). Both holds were correct per schedule; the driver simply didn't wait. Not a bug. BAR is a real one-off scheduled departure: the block package's "HOW TO GO OUT-OF-SERVICE" note for Gold block 10 says "LEAVE BAR AT 1955 AND STAY IN-SERVICE UNTIL LIB. TAKE PASSENGERS AS FAR AS McCORMICK RD DORMS AND RETURN TO LOT". The workbook's `END` row is only a marker for "no more scheduled timestops", NOT a time (user).
- **Built (working tree on [home], uncommitted):** `build_uts_blocks.py` now parses those notes out of the workbooks' drawing text boxes (openpyxl can't see them; it reads the XLSX XML) into `out_of_service` on each weekday group in `config/uts_blocks.json` (19 groups; schedule data otherwise byte-identical). `uts_blocks.out_of_service_plan()` + `bus_eta.estimate_stop_eta_s(out_of_service_fn=...)`: once a block is on its last public trip (already between the last departure stop and the cut-off, or the walk reaches that scheduled departure) any target the walk can only reach by passing the cut-off returns None (no ETA). Cut-off = the note's "as far as" stop, else "stay in service until" stop. New `config/uts_landmarks.json` maps the non-timestop cut-off `DORMS` -> stop 840 on route 57 (708 on 55). Buses already past the cut-off and heading to the lot are deliberately untouched (by position alone they look like a bus still approaching its last departure). Tests: `tests/test_bus_eta.py`, `tests/test_uts_blocks.py`, new `tests/test_build_uts_blocks.py`.
- **Not measurable by the scorer:** it only scores stops whose real crossing appears in the log, and a bus is still inside the window when it reaches its cut-off, so the replay shows 0 dropped and identical numbers. Verified by direct call on the real 19:41 snapshot instead (stops up to 840 get ETAs; 841 and HER return None).
- **Time-aware layover cap: DON'T (again).** I also implemented `uts_blocks.is_timestop_at()` (timestop = mapped AND some block on the route has a visit within 30 min) and it matches the schedule well (Green weekend = CHP only, Gold flips at 18:00, Silver constant), BUT I had not read the 09-20 "DON'T make the layover cap day/time-aware" entry. Replay of Sunday logs 09-20 14:00 + 17:00 with the day-independent cap vs time-aware: >2 min late 3.1% -> 5.0% (14:00 alone 4.7% -> 8.2%), same direction as the 09-20 failure, all of it on Orange/55 (route 55 >2 min late 23% -> 41%, +80 s at every stop after Pinn Hall). On weekdays the two are numerically identical (the cap rarely bites), so there is no gain to trade for. **Mechanism, measured 09-23 (this corrects the 09-20 'weekday layover leaks into weekend history' explanation for today's data):** Sunday Orange has its own history buckets (n~8) for 15 of 16 hours, so nothing is leaking in. Real Sunday buses take a median 214 s (n=26, p25 165, p75 276) from Pinn Hall to the next stop, a 517 m hop that is ~80-130 s of driving, and the Sunday history says 112-389 s by hour (208 s at 14:30). The schedule has NO Sunday timestop at PIN or MP, but buses really spend 1-4 min around Pinn Hall on Sundays. The always-on cap clips that hop to 124 s (-84 s), so removing it makes the hop CORRECT. It scores worse only because the replayed Orange ETAs are already ~+50-80 s late upstream of Pinn Hall for a reason NOT pinned down (that log has no block ids, so no holds), and the cap was masking it. So the cap is right for the wrong reason at Pinn Hall on Sundays; fix the upstream lateness first, then a time-aware cap could be retested. The 09-20 live failure may still have been the pooling leak (production history was thinner that morning); not reproducible from what we have. It is left in the repo but NOT wired: `app.py` passes the old day-independent cap (now a 3-arg lambda). Revisit only after history pooling across day groups changes.
- **Mapping:** no new timestop pairs needed (agrees with the 09-20 audit: every pair where the route really has the stop is mapped; the "unmapped" ones are stops that route variant never visits, or variants not running now: 52/56/66/70/71/78/80/9). The only new stop is the `DORMS` landmark.
- **Known limits of the cut-off:** Silver block 14's "as far as MCQ" (its own leave stop) and Orange weekend block 05's "make final loop" are a full lap, so no cut-off is applied. Night Pilot (route 59) isn't in the uts-graph so its cut-off (PIN) is untested. Green Thu/Fri "then go to Night Pilot" only matters after the cut-off, so it is recorded (`then`) but unused.
- **Not done:** no commit, no `fly deploy`. Also `pip install -r requirements.txt` + pytest were needed on [home]; pre-existing unrelated failures: `tests/test_vehicle_drivers.py` (import error), `tests/test_cache_concurrency.py` (needs async plugin), 2 in `tests/test_ondemand_filters.py`.

### 2026-09-23 · [home] · ETA health checks, first 3 weekdays (Sep 20-23, 27 runs): engine healthy, nothing to fix; scorer noise identified (nothing changed)
- **Accuracy:** daytime median |error| 62-81 s (TransLoc's own predictions run ~150-230 s early). Late >2 min is 1-5% on weekdays (Sunday daytime 8-11%).
- **Rush-hour early bias is real:** 17:00 runs (Mon/Tue/Wed) median |err| 77/121/103 s, predicting too soon. Error grows with horizon (<2 min out: 20-60 s; 10+ min out: 2-5 min), i.e. buses run slower than history hop times, compounding per stop. Worst on route 73 (Purple, Scott Stadium variant): Purple has no block package, so held buses read too soon (already a known limit). History is already keyed weekday+hour, so this is thin/stale history, not a missing feature. Watch whether it shrinks as weekday history builds; only touch the engine if it persists (see the 09-20 DECISION).
- **"Visits TransLoc predicted that we didn't" is mostly scorer noise:** (a) first 1-2 polls of a run have no prediction from us yet (warmup; 45 of Wed 17:00's 66); (b) single-poll (15 s) flickers where one bus's whole ETA set drops to 0-2 entries as it pulls off a stop/layover, then returns unchanged. Suggested scorer fix (NOT done): skip the first 2 polls and ignore single-poll gaps in `eta_health_check.analyze`.
- **4 full-lap flips in ~350k predictions**, all single-poll glitches (bus at/just before the stop, we briefly say ~28 min); same class as the 09-20 "22 min late for one poll" note. Don't chase.
- **Mon 09-21 00:00 run scored 0: dispatcher error, not the engine.** Night Pilot bus 13 was moved to route 0 (not in service) 15 s into the run and the other route-2 bus sat parked; TransLoc had no predictions either. Ignore that "inconclusive". The script can't tell dispatch-caused emptiness from real end of service. 04:30 runs are always empty (no service).
- Nothing committed from `data-local/` (per standing rule).

### 2026-09-20 · [home] · DEPLOYED `57aea89`: scheduled-hold wrong-lap fix (a bus ~20 min late was held for the NEXT visit)
- **Bug (seen live 20:01, Gold bus 44 at Shannon Library, block [09], concert night):** `uts_blocks.scheduled_hold_epoch` picked the NEAREST scheduled visit within 25 min. Bus arrived 21 min after the 19:40
  visit / 19 min before 20:20, matched 20:20, and was held ~19 min -> ETAs for the rest of its loop ~20 min LATE (TransLoc within 3 min). Only bites when a bus is >~half a headway (20 min on Gold) behind schedule.
- **Fix:** an upcoming visit only matches if the bus is <= `EARLY_MATCH_LIMIT_S` (10 min) early for it (measured typical 2-6 min early); otherwise match the previous visit (no hold). First visit of the day keeps the
  old behavior. Replay of 6 sessions with real blocks: >10-min-late predictions 50 -> 0, other metrics unchanged. Tests in `tests/test_uts_blocks.py`.
- **Known, expected after every deploy:** the first poll after a restart has no previous position/smoothing history, so a bus on a two-way road (Emmet St) can snap to the wrong pass for one poll (seen 20:40:01, bus 12,
  ~22 min late for one 15 s poll, corrected next poll). Not the hold logic; don't chase it.
- **Context:** during the Mumford & Sons show both Gold buses ran 10-19 min behind schedule from ~19:00; bus 12 peaked +14 min, so it was ~6 min from tripping the same bug.

### 2026-09-20 · [home] · DECISION (user): stop changing the ETA engine for now; keep logging
- The user wants the engine left as is ("not changing a whole buncha stuff"). Live and staying: stale-Due fix, `BlockId` on the ETA feed, 30 s timestop lag. Rolled back: day-aware layover cap.
- **Parked, NOT built/deployed (revisit only with more data, or if Gold keeps slipping):** driving-only hops + dwell history (tied the current design; opt-in code + `scripts/eta_replay.py` are in the repo),
  an "event night" hop factor for JPJ (effect is real but modest, mostly weekday 7 PM events: Gold loops overrun 40/45-min targets by ~6 min on such nights, worst ~+14 min), persistent-pace carry-over for long predictions,
  and a nightly script that rebuilds the JPJ event list (arena site has no public feed; its event pages + sitemap work, 43 upcoming events saved in `data-local/jpj_upcoming_from_site.json`, untracked).
- Gold facts for later: real loop ~41 min by GPS (weekend blocks 40 min; weekday 40 with ~1 in 5 at 45). Headway-event Chapel arrivals occur twice per loop in current data, so use Massie Rd @ JPJ (West Entrance)
  arrivals to time loops. The Gold loop's shape/length in headway data changed on ~2026-05-04 (do not pool baselines across it).
- Continuous 30-min logging sessions continue until the user says stop; `data-local/` is committed only on request.

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
- **Re-tested 2026-09-23 with a schedule-derived `is_timestop_at`: worse on Sunday logs (>2 min late 3.1% -> 5.0%, all Orange). Do not wire it in yet -- but the reason is NOT clearly the pooling leak; see the 09-23 out-of-service entry for the measured mechanism.** Read this entry BEFORE building anything time-aware around the cap.

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
