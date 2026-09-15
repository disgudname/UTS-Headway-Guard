# Off-Grounds place search — home-server handoff notes

Not started. This is the pickup point for the next home-server session, same pattern as
`ROUTING_ENGINE.md` (read that file too — it documents the box, the OSM extract, and the
Tailscale tunnel this work reuses).

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

A new endpoint, e.g. `GET /v1/search/geocode?q=`, that:
1. Calls `GEOCODE_URL` (through `GEOCODE_PROXY_URL` if set) with the query.
2. Trims Nominatim's response down to a shape the frontend can merge alongside
   `/v1/uva/facility_search`'s existing row shape — same field names where they overlap
   (name, lat/lon, maybe a bbox) so `search.js`/`trip-planner-panel.js` don't need two
   separate result-rendering code paths.
3. On any failure (unset env var, timeout, non-200) returns an empty result rather than
   erroring the whole search box — same graceful-degrade philosophy as the walk router
   falling back to a straight line.

## Frontend

`scripts/livemap/ui/search.js` and `scripts/livemap/ui/trip-planner-panel.js` already group
results into labeled sections ("Buildings", "Bus stops"). Add a third section (something
like "Places") sourced from the new endpoint, debounced the same way the existing building
search is.

## Open decisions for the home-server session

1. Nominatim vs. confirm-and-proceed on RAM headroom — check actual free RAM/disk on the box
   first; this may just work, or may need the import run with Valhalla stopped.
2. Bbox: reuse Valhalla's Charlottesville/Albemarle extract as-is, or pad it? (confirm with
   user — see above)
3. Whether to fold "Places" results into the same `/v1/uva/facility_search` endpoint (one
   endpoint, blended results) or keep it a separate `/v1/search/geocode` endpoint the
   frontend calls in parallel — leaning separate endpoint since the two data sources have
   very different refresh/cache lifetimes (UVA buildings: 12h TTL in-memory; Nominatim: just
   proxy live).
