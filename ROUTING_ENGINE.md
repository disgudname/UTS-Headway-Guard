# Routing engine — current state and the self-hosted handoff

This file is the pickup point for standing up a self-hosted OSM routing engine (Valhalla,
per the decision below) and wiring it into this app. It exists so that work can start on
a different machine — clone the repo, read this file, go — without re-deriving context
that already exists in this conversation history.

## Why this file exists

Two features in this app want real, sidewalk/road-following directions, not straight
lines:

1. **`/livemap`'s trip planner** (`trip_planner.py`) — walking legs between a rider's
   origin/destination and a bus stop. Currently a straight-line estimate
   (`trip_planner.estimate_walk_leg()`), explicitly marked `source: "straight_line"` in
   its output and labeled as an estimate in the UI. **Not wired to any router yet.**
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

- Nothing is self-hosted yet. The home server needs to be set up first.
- `trip_planner.py`'s walking legs are straight-line only, by deliberate choice for the
  initial trip-planner ship: it avoids putting new public-traffic load on the paid ORS
  key that only serves a handful of dispatcher-viewed vans today, and it doesn't block
  the whole feature on infrastructure that doesn't exist yet.
- `/api/routes/leg` and `/api/ondemand/routes` (`app.py:4215-4243`, `4268-4349`) are
  hosted-ORS, driving-car only, dispatcher-gated. Migrating these to the self-hosted
  engine is a separate future task, not part of standing up the engine itself.

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

Not yet decided — pick one when starting this work:

- **Tailscale** between the Fly machine and the home box (Fly has a documented pattern
  for this) — probably the simplest, no public exposure of the home box.
- **Port-forward + reverse proxy** (Caddy/nginx) on the home box, with TLS and an auth
  token, if Tailscale doesn't fit the home network setup.

Either way, the goal is one URL (`WALK_ROUTER_URL`, and later a similar var for
Tracklayer/ORS migration) that this app calls — the specific tunnel mechanism is an
implementation detail behind that URL, not something callers need to know about.

## Setup checklist for the home-server session

1. Confirm Docker is installed on the home box (RAM/disk free, OS/distro).
2. Pull `ghcr.io/valhalla/valhalla`, download the Virginia (or Charlottesville-extracted)
   `.osm.pbf` from Geofabrik, run the tile build.
3. Verify locally on the home box: `curl` the Valhalla `/route` endpoint with a
   `pedestrian` costing request between two known Charlottesville points, confirm it
   returns a sane polyline.
4. Stand up connectivity back to the Fly app (Tailscale recommended, see above).
5. Set `WALK_ROUTER_URL` on the Fly app (`fly secrets set` or `fly.toml`, per the user's
   own judgment on secret vs. plain env var) and implement the `estimate_walk_leg()`
   branch described above.
6. Once that's solid, treat `/api/routes/leg` / `/api/ondemand/routes` migrating off
   hosted ORS to the same engine (`auto` costing) as a separate follow-up task, and
   Tracklayer's road-snapping (also `auto`, or a custom costing for "prefer roads a bus
   fits down") as another.
