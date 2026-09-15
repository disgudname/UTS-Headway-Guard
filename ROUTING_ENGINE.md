# Routing engine — current state and the self-hosted handoff

This file is the pickup point for standing up a self-hosted OSM routing engine (Valhalla,
per the decision below) and wiring it into this app. It exists so that work can start on
a different machine — clone the repo, read this file, go — without re-deriving context
that already exists in this conversation history.

## Why this file exists

Two features in this app want real, sidewalk/road-following directions, not straight
lines:

1. **`/livemap`'s trip planner** (`trip_planner.py`) — walking legs between a rider's
   origin/destination and a bus stop. `trip_planner.estimate_walk_leg()` now calls the
   self-hosted router when `WALK_ROUTER_URL` is set (it is, in production — see "Current
   state" below), returning `source: "routed"` with a real polyline; it still falls back
   to the straight-line estimate (`source: "straight_line"`) on any failure.
2. **Tracklayer** (planned, not built — see the project's own notes) — a route-sketching
   tool for drafting new UTS routes, snapping dragged waypoints to roads.
3. **On-demand van routes** (`app.py`'s `/api/ondemand/routes`, `/api/routes/leg`) —
   already live, already using OSM-based routing, but via **paid hosted OpenRouteService**
   (`ORS_DIRECTIONS_URL`, default `api.openrouteservice.org/v2/directions/driving-car`),
   gated behind dispatcher auth specifically to keep that key from being an open proxy
   (`app.py:4215-4224`).

All three want the same thing: an OSM-based router. The decision (made together, in the
Tracklayer scoping conversation) is to **self-host Valhalla** on a home server rather than
keep paying for/relying on hosted ORS — Valhalla supports per-request costing profiles
(`pedestrian`, `auto`, `bicycle`) from one instance, so **one deployment can serve all
three use cases above**, not three separate engines.

## Current state (as of this writing)

- **Home server is up.** A VirtualBox VM (`valhalla-server`, Ubuntu 24.04, headless, on
  the Windows home box) runs Valhalla in Docker, tiled from a Geofabrik Virginia extract
  cut down to a Charlottesville/Albemarle bbox via `osmium extract --strategy simple`
  (`--strategy simple` specifically because the default strategy OOM-killed on the home
  box's 8GB RAM; `simple` trades multipolygon/admin-boundary completeness for much lower
  memory, which is fine since we don't build admin/timezone databases anyway). Verified
  serving real pedestrian routes on port 8002.
- Connectivity is Tailscale, installed on the Windows host itself (not inside the VM) --
  VirtualBox's NAT port-forward for 8002 is rebound to all host interfaces (was
  `127.0.0.1`-only) so it's reachable at the host's Tailscale IP.
- `trip_planner.py`'s `estimate_walk_leg()` now calls `WALK_ROUTER_URL` when set (Valhalla
  `/route`, pedestrian costing, decodes the returned polyline), falling back to the
  straight-line estimate on any failure, exactly per the seam described below.
  `app.py`'s call into `trip_planner.find_trips` is wrapped in `asyncio.to_thread` so the
  (now potentially network-calling) walk-leg estimation can't stall the shared event loop
  -- this app has a documented history of event-loop-stall bugs under load.
- The Fly side: `Dockerfile`/`start.sh` install and conditionally start Tailscale
  (`TS_AUTHKEY` env var/secret; no-op if unset) in userspace-networking mode with a local
  outbound HTTP proxy at `localhost:1055`, since Fly Machines don't grant a real tun
  device. `WALK_ROUTER_PROXY_URL` points `trip_planner.py`'s router calls at that proxy.
  **Deployed and verified end-to-end**: `TS_AUTHKEY`, `WALK_ROUTER_URL`, and
  `WALK_ROUTER_PROXY_URL` are all set as Fly secrets, the Fly machine shows up as a
  connected peer in the tailnet, and a live `/v1/trip-planner/plan` request against
  production returns a walk leg with `source: "routed"` and a real sidewalk-following
  polyline.
  - **Gotcha hit and fixed**: `tailscaled`'s state dir must live on the persistent `/data`
    volume (`start.sh` uses `/data/tailscale/tailscaled.state`), not the container's
    ephemeral root filesystem. The first attempt used the default ephemeral path and a
    non-reusable auth key; the very next machine restart (triggered by an unrelated
    `flyctl secrets set`) wiped the login and the one-time key couldn't re-auth. Confirmed
    live via `tailscale status` inside the machine showing "Logged out." after a restart
    that had nothing to do with Tailscale. Fixed by pointing `--state` at `/data/tailscale`
    and using a reusable key as a safety net. A side effect: the old consumed identity
    still shows up offline in the Tailscale admin console as a stale device
    (`login.tailscale.com/admin/machines`) — harmless, safe to delete whenever.
  - Also note: `flyctl secrets set`/`flyctl deploy` occasionally needs a follow-up
    `flyctl secrets deploy` before a newly-set secret actually shows up in the running
    process's environment — confirmed by diffing `/proc/<pid>/environ` before and after.
    If a router/Tailscale env var seems to have "not taken" after a deploy that reported
    success, try that before assuming the code is wrong.
- `/api/routes/leg` and `/api/ondemand/routes` (`app.py:4215-4243`, `4268-4349`) are
  still hosted-ORS, driving-car only, dispatcher-gated. Migrating these to the
  self-hosted engine is still a separate future task, not done as part of this.

## The seam: `trip_planner.estimate_walk_leg()`

```python
def estimate_walk_leg(start: Tuple[float, float], end: Tuple[float, float]) -> WalkLeg:
    ...
    return WalkLeg(coordinates=[...], distance_m=..., duration_s=..., source="straight_line")
```

This is the *only* place trip-planner walking geometry is computed. Every caller
downstream (itinerary ranking, the frontend's rendering) consumes the `WalkLeg` shape —
`{coordinates: [[lat, lon], ...], distance_m, duration_s, source}` — regardless of how it
was produced. Swapping in real routing means changing this one function's body (and
adding the env-var gate below), not touching the trip-finding logic or the frontend.

**When the self-hosted engine exists**, wire it up like this:

1. Add an env var, e.g. `WALK_ROUTER_URL` (unset today). When set, it should point at
   the home server's Valhalla `/route` endpoint (`costing: "pedestrian"`), reached over
   whatever connectivity is chosen (see below).
2. In `estimate_walk_leg()`: if `WALK_ROUTER_URL` is set, call it and return a `WalkLeg`
   with `source: "routed"` and the real polyline; on any failure (timeout, non-200),
   fall back to the existing straight-line estimate rather than failing the whole trip
   plan — a degraded walking line is better than no itinerary.
3. The frontend already renders `source: "straight_line"` as a plain dashed line between
   two points; `source: "routed"` should render the real returned polyline the same
   dashed style, just no longer a straight segment. No other frontend change needed.

## Engine choice: Valhalla

Leaning Valhalla over self-hosted ORS or OSRM:

- **Valhalla** — per-request costing (`pedestrian` vs `auto` vs `bicycle`) from one
  running instance, so it covers trip-planner walking, Tracklayer's road-snapping, and
  (eventually) on-demand van driving directions without standing up separate engines.
  Also has strong `trace_route` map-matching, useful if Tracklayer ever gets a freehand
  mode. Image: `ghcr.io/valhalla/valhalla`. Tiled — no full re-preprocess to retune costing.
- **Self-hosted ORS** — API-identical to what `/api/routes/leg` already calls today, so
  migrating that endpoint would be a pure URL repoint. Heaviest build (JVM + GraphHopper).
- **OSRM** — fastest queries, but rigid Lua costing profiles and a full re-preprocess to
  change anything; less natural fit for "one engine, three costing profiles."

## Data

- Geofabrik **Virginia** `.osm.pbf` extract. Optionally `osmium extract` down to a
  Charlottesville-area bounding box to cut RAM/build time — this app only ever routes
  within Charlottesville/Albemarle, no need for the whole state's tile graph in memory.
- Fits in Docker on 2-4 GB RAM for one state, per Valhalla's own sizing guidance.
- Cron a weekly re-pull + re-preprocess so OSM edits (a newly-mapped sidewalk, a fixed
  one-way) show up without manual intervention.
- If a road/path is wrong in OSM, fix it upstream in OSM itself — it improves this tool
  and everyone else using that data, and the engine picks it up on the next re-preprocess.

## Connectivity: home server -> Fly app

**Decided: Tailscale.** Installed on the Windows host (not inside the Valhalla VM) --
VirtualBox NAT forwards the VM's Valhalla port to the host, and the host's Tailscale IP
is what's reachable from outside. On the Fly side, `Dockerfile`/`start.sh` run
`tailscaled` in userspace-networking mode (Fly Machines don't grant a real tun device)
with a local outbound HTTP proxy; `trip_planner.py` routes its Valhalla calls through
that proxy via `WALK_ROUTER_PROXY_URL`. See "Current state" above for exactly what's
wired vs. still needs a real deploy to verify.

The goal is still one URL (`WALK_ROUTER_URL`, and later a similar var for
Tracklayer/ORS migration) that this app calls — the specific tunnel mechanism is an
implementation detail behind that URL, not something callers need to know about.

## Setup checklist for the home-server session

1. ~~Confirm Docker is installed on the home box (RAM/disk free, OS/distro).~~ Done --
   home box is an 8GB-RAM Windows 11 machine; Docker runs inside a VirtualBox Ubuntu VM
   rather than Docker Desktop directly (Windows' WSL2/Hyper-V component store was
   corrupted and not worth fighting; VirtualBox sidesteps it entirely).
2. ~~Pull `ghcr.io/valhalla/valhalla`, download the Virginia (or
   Charlottesville-extracted) `.osm.pbf` from Geofabrik, run the tile build.~~ Done --
   note the image is the bare Valhalla binaries, not a self-building wrapper: config via
   `valhalla_build_config`, tiles via `valhalla_build_tiles`, served via
   `valhalla_service config.json 1`. No `tiles.tar` extract was built (not required --
   `tile_dir` alone works fine), and no admin/timezone sqlite databases were built either
   (not needed for pedestrian/auto costing without time-dependent restrictions).
3. ~~Verify locally on the home box: `curl` the Valhalla `/route` endpoint with a
   `pedestrian` costing request between two known Charlottesville points, confirm it
   returns a sane polyline.~~ Done -- UVA Rotunda to the Downtown Mall returns real
   sidewalk/stairs-level turn-by-turn.
4. ~~Stand up connectivity back to the Fly app.~~ Done both sides -- Tailscale on the
   Windows host (VirtualBox NAT rebound to all interfaces) and `TS_AUTHKEY` set on Fly;
   the Fly machine shows up as a connected tailnet peer. See the state-persistence gotcha
   above if a redeploy ever comes back "Logged out."
5. ~~Wire `estimate_walk_leg()`'s router branch and set `WALK_ROUTER_URL`/
   `WALK_ROUTER_PROXY_URL` on Fly.~~ Done -- verified live against production
   (`/v1/trip-planner/plan` returns `source: "routed"` with a real polyline).
6. Next up: treat `/api/routes/leg` / `/api/ondemand/routes` migrating off hosted ORS to
   the same engine (`auto` costing) as a follow-up task, and Tracklayer's road-snapping
   (also `auto`, or a custom costing for "prefer roads a bus fits down") as another.
