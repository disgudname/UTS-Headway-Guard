# UTS Headway Guard

UTS Headway Guard is a full real-time operations platform for the University Transit Service. It polls live vehicle and schedule data, highlights safety risks, persists telemetry for after-action review, and serves specialized web tools for drivers, dispatchers, service crews, and digital signage. The backend is a FastAPI service augmented by asyncio tasks and Server-Sent Events streams, while the frontend surfaces lightweight Leaflet dashboards and status boards tailored to each audience.

## System architecture

### External data sources
- **TransLoc JSONP Relay** — primary feeds for routes, live vehicle locations, anti-bunching recommendations, block assignments, ridership counts, vehicle capacities, stop arrivals, and alerts. The service proxies these endpoints so the UI can run without exposing credentials.【F:app.py†L335-L529】【F:ridership.html†L24-L109】【F:arrivalsdisplay.html†L67-L180】
- **OpenStreetMap Overpass API** — queried once per route to retrieve speed-limit tags and low-clearance structures around the 14th Street bridge. Results are cached in memory so an Overpass outage does not break guidance.【F:app.py†L360-L428】【F:app.py†L430-L478】
- **RideSystems client catalog** — the live map can enumerate partner agencies through `admin.ridesystems.net` and let dispatch staff monitor other systems from the same UI.【F:map.html†L224-L287】

### Headway computation and safety logic
- Vehicles are projected to the closest arc length on each route polyline, blending proximity, heading, and previous segment to stabilize the position even when the bus is stopped or reversing.【F:app.py†L430-L506】
- Segment-level speed caps from Overpass are blended with an exponential moving average of observed speeds so downstream tools always have a realistic reference for posted limits versus live performance.【F:app.py†L506-L614】
- Overheight vehicles and low-clearance structures are tracked so driver tools can warn operators as they approach the 14th Street bridge. The list of monitored buses and radii are configurable at runtime.【F:driver.html†L66-L184】【F:app.py†L685-L723】

### Background processing
- An asyncio startup task keeps the in-memory model fresh: it polls routes, vehicles (including unassigned units), block groups, and caches derived data structures (active routes, roster list, speed profiles, block-to-bus maps). Failures are surfaced through the `/v1/health` endpoint and UI banners.【F:app.py†L877-L1132】
- A separate vehicle logger captures the full TransLoc payload every few seconds, deduplicates stationary snapshots, annotates block assignments, writes hourly JSONL files, and prunes data beyond the configured retention window.【F:app.py†L1133-L1214】
- Mileage is accumulated per vehicle per service day (based on America/New_York), carrying the previous day’s odometer forward, recording block history, and exposing reset controls to service crew tools.【F:app.py†L733-L872】【F:app.py†L1564-L1608】

### Persistence and synchronization
- Data required across restarts is stored under `DATA_DIRS` (defaults to `/data`). The service persists `config.json`, `mileage.json`, and `vehicle_headings.json`, mirroring them to every configured data directory.【F:app.py†L685-L772】【F:app.py†L808-L934】
- Vehicle log volumes can span multiple mounts via `VEH_LOG_DIRS`; each entry is flushed and fsynced so replay data survives crashes. Hourly files are automatically pruned after the retention window.【F:app.py†L1133-L1178】
- Multi-machine deployments can list peer instances in `SYNC_PEERS`. When an admin updates configuration, mileage snapshots, or vehicle heading caches, the service POSTs the change to every peer’s `/sync` endpoint using an optional shared secret so volumes stay in sync.【F:app.py†L689-L723】【F:app.py†L846-L934】

## API reference

### Health and metadata
- `GET /v1/health` — exposes overall polling status plus the last encountered error and timestamp, enabling UI banners and monitoring.【F:app.py†L875-L883】

### Routes and geometry
- `GET /v1/routes` — active and inactive routes with length, live vehicle counts, and friendly labels assembled from TransLoc description/info text.【F:app.py†L1217-L1241】
- `GET /v1/routes_all` — complete roster of known routes with active flags for dropdowns.【F:app.py†L1243-L1254】
- `GET /v1/routes/{route_id}` — color and length metadata for a specific route.【F:app.py†L1256-L1263】
- `GET /v1/routes/{route_id}/shape` — encoded polyline and line color for map overlays.【F:app.py†L1265-L1272】
- `GET /v1/transloc/routes` — raw TransLoc route payload cached by the updater for diagnostic clients.【F:app.py†L1334-L1339】

### Vehicles and headway guidance
- `GET /v1/roster/vehicles` — deduplicated list of unit numbers encountered in the feed for driver dropdowns.【F:app.py†L1274-L1280】
- `GET /v1/vehicles` — on-demand proxy to TransLoc returning vehicles with optional stale/unassigned filtering for kiosk displays.【F:app.py†L1282-L1312】
- `GET /v1/routes/{route_id}/vehicles_raw` — detailed telemetry (position, EMA, segment speed limit, roadway name, direction) for every vehicle assigned to a route.【F:app.py†L1314-L1354】

### Dispatch utilities
- `GET /v1/dispatch/blocks` — aggregates block group schedules, plain-language block assignments, route colors, and route assignment by bus, cached between polls so tables render instantly.【F:app.py†L1743-L1773】
- `GET /v1/transloc/anti_bunching` — lightweight proxy to TransLoc’s native anti-bunching recommendations for side-by-side comparisons in the dispatcher view.【F:app.py†L1390-L1400】
- `GET /v1/stream/api_calls` — SSE feed of every outbound API request (method, status code, URL) for live observability dashboards.【F:app.py†L1475-L1486】

### Safety and signage
- `GET /v1/low_clearances` — low-clearance points filtered by maximum height and bridge radius, cached after the first Overpass fetch.【F:app.py†L1402-L1410】

### Service crew mileage
- `GET /v1/servicecrew` — returns per-bus mileage totals, block assignments, and reset baselines for the requested date (default today).【F:app.py†L1508-L1532】
- `POST /v1/servicecrew/reset/{bus}` — records a mileage reset for a specific bus at service start.【F:app.py†L1534-L1544】
- `POST /v1/servicecrew/refresh` & `GET /v1/stream/servicecrew_refresh` — manual refresh hook plus SSE broadcast so kiosks reload instantly after data corrections.【F:app.py†L1546-L1562】

### Configuration and synchronization
- `GET /v1/config` & `POST /v1/config` — expose and mutate runtime tunables (poll intervals, safety thresholds, vehicle lists, API hosts). Saved values persist under every data directory and replicate to peers.【F:app.py†L1488-L1506】
- `POST /sync` — internal endpoint accepting config, mileage, or vehicle-heading payloads from trusted peers using an optional shared secret.【F:app.py†L914-L934】

### Logs and static assets
- `GET /vehicle_log/{YYYYMMDD_HH}.jsonl` — download an hourly vehicle snapshot file for offline replay or investigations.【F:app.py†L1488-L1510】
- `GET /FGDC.ttf` — serves the custom FGDC typeface used across the UI suite.【F:app.py†L1475-L1483】
- Root-level routes (`/`, `/driver`, `/dispatcher`, `/map`, `/testmap`, `/madmap`, `/metromap`, `/debug`, `/admin`, `/servicecrew`, `/buses`, `/apicalls`, `/ridership`, `/replay`, `/arrivalsdisplay`, `/transloc_ticker`) render the corresponding HTML dashboards bundled in the repository.【F:app.py†L1488-L1611】

## Web clients

### Unified landing portal
`/` offers quick navigation cards to every major tool plus shortcuts to GitHub and the admin console so staff can jump between roles easily.【F:index.html†L1-L58】

### Driver anti-bunching console
The driver view walks operators through selecting their unit and route, then shows:
- Live guidance card with color-coded orders, countdown timers, and last-update indicators.
- Embedded clock, map with animated markers, per-vehicle name bubbles, and route outline.
- Automatic warnings when an overheight coach nears a low-clearance location, including a full-screen red overlay with audible alarms.
- Periodic health checks and light/dark theme toggle for night operations.【F:driver.html†L1-L414】

### Dispatcher operations board
Dispatchers receive a split-screen operations dashboard and live map:
- Block roster panels (current and future) with alias expansion, automatic highlighting of extra buses, and mileage-aware sorting.
- Extra- and downed-bus panels fed from shared spreadsheets so fleet availability is always up to date.
- Health-check driven status banner plus an embedded `/map` iframe for geographic context.【F:dispatcher.html†L1-L430】

### Live map suite
`/map`, `/madmap`, and `/metromap` power day-to-day UVA operations while `/testmap` and `/testmap-minimal` expose an experimental control panel for advanced monitoring:

#### Shared map experience (`/map`, `/madmap`, `/metromap`)
- Route overlays use SVG renderers with adaptive stroke weights so intertwined lines stay legible at every zoom level, and a loading overlay keeps kiosk deployments from flashing half-rendered states while feeds initialize.【F:map.html†L4300-L4382】
- The interface discovers every RideSystems agency on demand, prioritizes UVA, and persists the operator’s choice (with a consent banner) so dispatch can jump between partner systems without reconfiguring the map each time.【F:map.html†L4384-L4426】【F:map.html†L6707-L6741】
- Sliding control and route-selector panels reposition themselves based on viewport size and kiosk/admin modes, letting wall displays stay clutter-free while tablets retain quick access to filters and overlays.【F:map.html†L4431-L4545】【F:map.html†L4552-L4559】

#### Testbed features (`/testmap`)
- URL parameters toggle admin, kiosk, and radar behaviors so the same page can run as an analyst console, lobby display, or storm ops view without code changes.【F:testmap.js†L624-L655】
- The control panel orchestrates a multi-source data pipeline: TransLoc snapshots, PulsePoint incidents, Amtrak/VRE train feeds, FAA ADS-B traffic, and Charlottesville Area Transit (CAT) overlays refresh on independent cadences and cache aggressively to avoid vendor rate limits.【F:testmap.js†L658-L760】【F:testmap.js†L807-L845】
- Incident markers fan out with animated halos, while service-alert toggles summarize active notices and surface combined UVA + CAT alerts with accessibility-friendly status text and expandable panels.【F:testmap.js†L658-L706】【F:testmap.js†L3680-L3838】
- CAT integration downloads routes, stops, patterns, vehicles, and stop ETAs, then tracks selections so kiosk-safe routes stay visible even when the operator switches agencies.【F:testmap.js†L736-L760】【F:testmap.js†L820-L851】

#### Minimal embed (`/testmap-minimal`)
- A stripped-down Leaflet view accepts `baseURL` and `routeId` query parameters, renders a single route using the same adaptive stroke-weight logic, and audits pane transforms so third-party CMS embeds can’t accidentally warp the geometry.【F:testmap-minimal.html†L24-L148】

### Service crew dashboard
A widescreen table summarises miles driven per bus, current block assignments (with aliasing), high-mileage alerts, and links to an embedded kiosk-mode map. Data refreshes continuously and highlights overheight buses with distinctive colors.【F:servicecrew.html†L1-L176】

### Replay explorer
Investigators can review historical operations with:
- Date and time pickers, per-route and per-bus filters, and disclaimers clarifying the limitations of TransLoc data.
- Timeline scrubber with pause, play, and fast-forward controls from 1× to 1000×.
- Toggleable labels for speed and block numbers, route overlays, and dual selectors for routes and buses to focus playback on relevant fleets.【F:replay.html†L1-L360】

### Ridership daily rollups
The Red/Blue line dashboard downloads APC entries for a selected day, aggregates AM/PM buckets, tracks the newest datapoint timestamp, and lets staff annotate notes and export the entire table to CSV.【F:ridership.html†L1-L161】

### Bus overview
`/buses` lists every vehicle with its current block, roadway segment, posted speed limit, actual speed (highlighting speeding coaches), and stale data shading, alongside a live map for context.【F:buses.html†L1-L132】

### Debug and monitoring utilities
- `/debug` now focuses on the embedded live map and route list; anti-bunching debug tables have been retired.【F:debug.html†L1-L102】
- `/apicalls` subscribes to the API call SSE feed to audit outbound HTTP traffic in real time.【F:apicalls.html†L1-L38】
- `/transloc_ticker` renders a configurable alert ticker suitable for broadcast overlays, honoring URL parameters for duration, colors, and visibility.【F:transloc_ticker.html†L1-L200】

### Digital signage workflow
- `/arrivalsdisplay` drives stop displays with arrival predictions, vehicle capacity bars, scrolling alert ticker, spoken announcements (with optional audio primer for autoplay policies), and a stop name banner. Configure a display by appending `?stopid=STOP_ID` to the URL.【F:arrivalsdisplay.html†L1-L552】

### Page URL parameters

| Page(s) | Query parameters |
| --- | --- |
| `/`, `/index`, `/driver`, `/dispatcher`, `/servicecrew`, `/admin`, `/buses`, `/apicalls`, `/ridership`, `/replay`, `/debug`, `/madmap`, `/metromap`, `/404` | None – these dashboards ignore query strings and rely on in-app controls.【203a89†L1-L7】 |
| `/map` | `kioskMode` (`true`/`false`) hides the route selector and suppresses admin overlays for kiosk displays; `adminMode` (`true`/`false`) toggles block/speed bubbles and exposes non-public routes; `adminKioskMode` (`true`/`false`) hides the selector while retaining admin overlays.【F:map.html†L202-L230】 |
| `/testmap` | Same as `/map`: `kioskMode`, `adminMode`, and `adminKioskMode` drive whether the selector is visible and whether admin overlays render.【F:testmap.html†L763-L831】 |
| `/testmap-minimal` | `baseURL` forces a specific RideSystems host instead of auto-discovering UVA; `routeId` chooses a specific route (numeric) when multiple shapes are available.【F:testmap-minimal.html†L24-L303】 |
| `/arrivalsdisplay` | `stopid=12345` selects the TransLoc stop to monitor; `showInactiveAlerts=true` keeps expired alerts in the ticker; `voice` selects a Web Speech voice name for announcements when available.【F:arrivalsdisplay.html†L67-L552】 |
| `/transloc_ticker` | Styling knobs: `height`, `bg`, `fg`, `sepfg`, `size`, `sepsize`, `duration`, `pad`. Content switches: `showInactive`/`showInactiveAlerts` (truthy to include inactive alerts), `source=alerts|arrivals`, `stops` (TransLoc stop list when `source=arrivals`), `sep` (separator text), `refresh` (poll interval in ms).【F:transloc_ticker.html†L170-L515】 |

## Data and logging
- Hourly JSONL vehicle logs live under the first `VEH_LOG_DIR` (default `/data/vehicle_logs`). Files are named `YYYYMMDD_HH.jsonl`, flushed synchronously, and pruned based on `VEH_LOG_RETENTION_MS`. Each record embeds raw TransLoc vehicles plus block assignments and capture timestamp.【F:app.py†L1133-L1214】
- Daily mileage snapshots are stored in `mileage.json` and include cumulative miles, day totals, reset baselines, last known location, and block history.【F:app.py†L804-L852】
- Sample TransLoc payloads (`GetRoutes*.txt`, `GetVehicles*.txt`, etc.) are included at the repo root to facilitate offline UI development and testing.【F:GetRoutes.txt†L1-L5】

## Configuration and environment variables
Key tunables may be provided via environment variables or edited live through `/admin`:
- **TransLoc / Overpass**: `TRANSLOC_BASE`, `TRANSLOC_KEY`, `OVERPASS_EP`.
- **Polling & headway**: `VEH_REFRESH_S`, `ROUTE_REFRESH_S`, `BLOCK_REFRESH_S`, `STALE_FIX_S`, `ROUTE_GRACE_S`, `EMA_ALPHA`, `MIN_SPEED_FLOOR`, `MAX_SPEED_CEIL`.
- **Safety**: `DEFAULT_CAP_MPS`, `BRIDGE_LAT`, `BRIDGE_LON`, `LOW_CLEARANCE_SEARCH_M`, `LOW_CLEARANCE_LIMIT_FT`, `BRIDGE_IGNORE_RADIUS_M`, `OVERHEIGHT_BUSES`, `LOW_CLEARANCE_RADIUS`, `BRIDGE_RADIUS`, `ALL_BUSES`.
- **Data directories**: `DATA_DIRS`, `VEH_LOG_DIRS`, `VEH_LOG_DIR`, `VEH_LOG_INTERVAL_S`, `VEH_LOG_RETENTION_MS`, `VEH_LOG_MIN_MOVE_M`.
- **Sync & secrets**: `SYNC_PEERS`, `SYNC_SECRET`.
- **Miscellaneous**: timezone defaults (`TZ`) and standard FastAPI/Uvicorn settings via `PORT` when running in containers.【F:app.py†L35-L130】【F:app.py†L685-L872】【F:start.sh†L1-L18】

## Running locally
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the development server:
   ```bash
   uvicorn app:app --reload --port 8080
   ```
3. Open the role-specific pages (`/driver`, `/dispatcher`, `/servicecrew`, `/map`, etc.) on http://localhost:8080. The asynchronous updater will begin polling TransLoc immediately, and SSE clients will populate once the first snapshot completes.【F:app.py†L877-L1132】

## Docker and deployment
- The provided `Dockerfile` builds a slim Python 3.12 image, installs system dependencies (`build-essential`, `curl`), and runs the app under a non-root `appuser`. The container entrypoint delegates to `start.sh` to create/chown shared volumes before exec-ing Uvicorn on `$PORT` (default 8080).【F:Dockerfile†L1-L33】【F:start.sh†L1-L18】
- `fly.toml` defines a Fly.io deployment that mounts persistent volumes at `/data`, ensuring logs, config, and mileage survive restarts. Multi-machine deployments should configure `SYNC_PEERS` and `SYNC_SECRET` for eventual consistency across regions.【F:fly.toml†L1-L33】【F:app.py†L689-L723】

## Development aids
- Fonts (`FGDC.ttf`) and HTML assets live alongside `app.py` so the FastAPI static routes can serve them directly without additional build tooling.【F:app.py†L1475-L1510】
- HTML dashboards rely on Leaflet, polyline decoding, and minimal inline JavaScript so they can be edited without bundlers.
- Root-level `.txt` files capture historical API responses for regression testing and UI prototyping without hitting production services.【F:GetRoutes.txt†L1-L5】

---

Disclaimer: All code in this repository was written by OpenAI's Codex.
