# Off-Grounds place search — home-server handoff notes

**Nominatim is up and wired into the app; only the Fly-side deploy step is left.** Same
pattern as `ROUTING_ENGINE.md` (read that file too — it documents the box, the OSM extract,
and the Tailscale tunnel this work reuses).

## Current state (as of this writing)

- **Nominatim is running on the home server**, in the same VirtualBox VM as Valhalla
  (`valhalla-server`), not a new VM. `mediagis/nominatim:5.1` in Docker, `--restart
  unless-stopped`, container port 8080 mapped to host/VM port 8003. Postgres data lives in
  a named Docker volume (`nominatim-data`), not a bind mount.
- **Imported from the existing extract**, no re-download: `/home/valhalla/valhalla_data/
  charlottesville.osm.pbf` (7.3MB), the same Charlottesville/Albemarle bbox Valhalla's
  tiles were built from. User confirmed reusing that bbox as-is rather than padding it for
  places further out (e.g. Zion Crossroads/Ruckersville) — can revisit later if a real gap
  shows up.
- **RAM was not actually a problem.** The VM's existing 3GB allocation (2GB free at the
  time) was plenty for this tiny extract — the import finished in under 4 minutes without
  ever needing to stop the Valhalla container. The image's default Postgres tuning
  (`shared_buffers=2GB`, `maintenance_work_mem=10GB`, ...) is sized for real servers and
  was overridden down via env vars on `docker run` (`POSTGRES_SHARED_BUFFERS=512MB`, etc.)
  — reuse those same overrides if this ever gets re-imported with a larger bbox, but
  re-check headroom first since they were only validated against a 7MB extract.
- **Connectivity verified**: VirtualBox NAT port-forward added the same way as Valhalla's
  8002 (`VBoxManage controlvm valhalla-server natpf1 "nominatim,tcp,,8003,,8003"`, bound to
  all host interfaces). Confirmed reachable at `http://100.119.243.68:8003/search?q=...
  &format=jsonv2` over Tailscale from the host.
- **Data completeness spot-checked, not exhaustively verified**: well-known named places
  resolve fine ("Downtown Mall", "The Standard at Charlottesville"). Two other real
  Charlottesville student apartment complexes, "Grounds at Fifth" and "Lark at Ivy", return
  nothing — the surrounding streets geocode fine, so it's an OSM tagging gap, not a bbox or
  import problem. Per this doc's own philosophy (see below), the fix for a specific missing
  complex is adding it to OSM upstream, not working around it here. Also confirmed:
  Nominatim does **not** do fuzzy street-suffix correction — "Chesapeake Ave" (assumed suffix)
  returned nothing where "Chesapeake Street" (OSM's actual tag) resolved the exact address
  correctly. Real quality gap vs. a paid geocoder like Google Places, worth knowing going in;
  not something to fix now.
- **Backend endpoint done**: `GET /v1/search/geocode?q=` in `app.py` (next to
  `/v1/uva/facility_search`), `GEOCODE_URL`/`GEOCODE_PROXY_URL`/`GEOCODE_HTTP_TIMEOUT_S` env
  vars declared next to the `ORS_*` config block. Trims a Nominatim `jsonv2` hit down to
  `{name, address, lat, lon, bbox}` — `bbox` reordered from Nominatim's `[south, north,
  west, east]` to `facility_search`'s own `[minLon, minLat, maxLon, maxLat]` convention so
  the frontend's existing bbox-handling code works unchanged. Degrades to `{"results": []}`
  on any failure (unset env var, timeout, non-200, bad JSON) — never a 500, same philosophy
  as `estimate_walk_leg()`'s router fallback. Tested end-to-end against the live Nominatim
  instance (real FastAPI app, not just the trim logic in isolation) — see the git commit
  this line ships in for exact behavior.
- **Frontend done**: `scripts/livemap/ui/search.js` and `ui/trip-planner-panel.js` both now
  fetch `/v1/uva/facility_search` and `/v1/search/geocode` in parallel (`Promise.all`, each
  degrading to an empty list independently) and render a third "Places" section alongside
  "Buildings"/"Bus stops". A place pick already carries a direct lat/lon from Nominatim (no
  bbox-centroid derivation needed, unlike a building's ArcGIS-polygon-only row) — in
  `trip-planner-panel.js` it reuses the same point-apply path as a stop/recent pick; in
  `search.js` it gets its own `_pickPlace()` (camera fitBounds/flyTo + "Navigate here", same
  shape as `_pickBuilding()` but no polygon to highlight).
- **Not yet done: the Fly side.** `GEOCODE_URL`/`GEOCODE_PROXY_URL` secrets are not set on
  Fly, and none of this has been deployed. `GEOCODE_PROXY_URL` should just reuse
  `WALK_ROUTER_PROXY_URL`'s existing value (`http://localhost:1055`) — no new Dockerfile/
  start.sh changes needed, the outbound Tailscale proxy already runs. `GEOCODE_URL` should
  be `http://100.119.243.68:8003/search`.

## Why

`/livemap`'s search box and the trip planner's origin/destination fields currently only
resolve two things:

1. **UVA buildings** — `/v1/uva/facility_search` (`app.py` ~line 12941), sourced from UVA's
   own facility ArcGIS layer. On-Grounds only, by construction — that layer doesn't contain
   anything off-Grounds.
2. **UTS bus stops** — client-side, off the live TransLoc stop index (`scripts/livemap/core/
   data/transloc.js`'s `getStops()`).

There is currently no way to type an off-Grounds address or place name (an apartment
complex, a restaurant, a random street address) and get a pin. User wants that gap closed —
concrete example given: "an off grounds apartment complex."

## Plan: self-hosted OSM geocoder, on the same home server as Valhalla

Same reasoning as the routing engine: don't pay for/rely on a hosted geocoding API
(Google Places, Mapbox Geocoding, LocationIQ) when OSM data plus a self-hosted box already
does the job for routing. **Recommended engine: self-hosted Nominatim**, not Photon —
Photon's own indexer actually requires a Nominatim import as an intermediate step (it reads
from a Nominatim-populated Postgres DB, then builds its Elasticsearch/Lucene index from
that), so it's strictly more moving parts for no clear benefit here. Nominatim alone
("give it a `.osm.pbf`, get a `/search` HTTP API") is the standard, well-documented path —
most people reach for `mediagis/nominatim` on Docker Hub for exactly this.

- **Reuse the existing extract.** `ROUTING_ENGINE.md` already documents a Geofabrik
  Virginia extract cut to a Charlottesville/Albemarle bbox via `osmium extract --strategy
  simple`, used to build Valhalla's tiles. Nominatim's import wants the same kind of
  `.osm.pbf` input — check whether that intermediate file is still on the VM (it may have
  been deleted after Valhalla's tile build finished) before re-downloading from Geofabrik.
- **Confirm the bbox is wide enough for this use case before importing.** The Valhalla bbox
  was picked because "this app only ever routes within Charlottesville/Albemarle." Off-
  Grounds apartment searches may reach slightly further out (e.g. complexes along routes
  toward Zion Crossroads/Ruckersville that some UTS/CAT riders still care about) — confirm
  with the user whether the same bbox is fine or needs padding before spending import time
  on too-narrow an extract.
- **RAM is the open risk.** The home box is the same 8GB-RAM Windows machine already running
  the Valhalla VM (`ROUTING_ENGINE.md` notes Valhalla's default tile-build strategy OOM-killed
  on this box, which is why `--strategy simple` was used). Nominatim's import step is
  independently RAM-hungry — check Nominatim's own current sizing guidance for a *regional*
  (not country/planet) extract before committing, and plan to run the one-time import with
  the Valhalla container stopped if headroom is tight (ongoing query serving afterward should
  be much lighter than the import itself).
- **Data completeness is unverified.** Not every off-Grounds apartment complex is
  necessarily tagged as a named point/POI in OSM (some may only exist as untagged building
  footprints, or may be missing outright). Spot-check a handful of known complexes (e.g.
  search for a couple of well-known Charlottesville student apartment names) against the
  import before assuming this closes the gap completely — if a specific complex is missing
  or mistagged, the fix is adding/editing it in OSM itself (same philosophy as
  `ROUTING_ENGINE.md`'s "fix it upstream" note), not working around it in this app.

## Connectivity — reuse the existing tunnel, don't build a new one

No new infrastructure needed here: the Fly app already reaches the home box over Tailscale
via a userspace-networking outbound proxy (`localhost:1055` on the Fly side, per
`ROUTING_ENGINE.md`). Nominatim just needs its own port forwarded the same way Valhalla's
8002 is (VirtualBox NAT rebound to all host interfaces, not `127.0.0.1`-only) — e.g. port
8003 — and the existing Fly-side proxy already carries traffic to any host:port on that
tailnet peer, so no second proxy component should be required.

New env vars, following the `WALK_ROUTER_URL`/`WALK_ROUTER_PROXY_URL` naming convention
already established:
- `GEOCODE_URL` — the home server's Nominatim `/search` endpoint over Tailscale.
- `GEOCODE_PROXY_URL` — same Fly-side outbound proxy, reused.

## Backend seam (app.py) — mirrors `estimate_walk_leg()`'s pattern

~~A new endpoint, e.g. `GET /v1/search/geocode?q=`, that:~~ Done — see "Current state" above
for exactly what shipped. Kept as a separate endpoint from `/v1/uva/facility_search` rather
than blending results into one, per the reasoning below (their refresh/cache lifetimes are
too different to share a cache layer): UVA buildings are a 12h in-memory TTL; Nominatim is
just proxied live, no caching.

## Frontend

~~`scripts/livemap/ui/search.js` and `scripts/livemap/ui/trip-planner-panel.js` already group
results into labeled sections ("Buildings", "Bus stops"). Add a third section (something
like "Places") sourced from the new endpoint, debounced the same way the existing building
search is.~~ Done — see "Current state" above.

## Remaining: Fly deploy

1. Set Fly secrets: `GEOCODE_URL=http://100.119.243.68:8003/search` (the home server's
   Nominatim endpoint) and `GEOCODE_PROXY_URL` (reuse `WALK_ROUTER_PROXY_URL`'s value,
   `http://localhost:1055` — same outbound Tailscale proxy, no new infra).
2. `flyctl deploy`, then confirm the Fly machine can actually reach Nominatim over the
   tunnel (same `tailscale status` / process-environment checks `ROUTING_ENGINE.md`
   documents for `WALK_ROUTER_URL` — including the "secret set doesn't always mean the
   running process has it yet" gotcha noted there).
3. Live-check `/v1/search/geocode?q=` against production, then a real search-box/trip-planner
   query in the browser.
