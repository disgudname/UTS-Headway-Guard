# CLAUDE.md - AI Assistant Guide for UTS Operations Dashboard

This document provides comprehensive guidance for AI assistants working with the UTS Operations Dashboard codebase.

## Project Overview

**UTS Operations Dashboard** is a real-time transit operations management system for the University Transit Service at UVA. The system combines:

- **FastAPI backend** (Python) - Polls live vehicle/schedule data, manages headway tracking, provides SSE streams
- **Node.js/Express service** - Disk-backed maintenance ticketing system with JSONL event sourcing
- **HTML dashboards** - Leaflet-based web interfaces for drivers, dispatchers, service crews, kiosks, and admin

### Core Capabilities

- **Live headway monitoring** - Flags bunching risks and over-height vehicle alerts for low-clearance zones
- **Role-specific dashboards** - Tailored views for different operational roles
- **Shared data layer** - Async background tasks maintain route geometry, vehicle telemetry, block assignments
- **Persistent storage** - Configuration and history survive restarts via `/data` volume
- **Extensible signage** - Reusable pipeline for arrivals boards, alert tickers, kiosk displays

---

## Directory Structure

```
.
├── app.py                      # Main FastAPI application (290KB, core backend)
├── headway_tracker.py          # Headway tracking logic and vehicle state management
├── headway_storage.py          # Persistence layer for headway events
├── tickets_store.py            # Python ticket store interface
├── ondemand_client.py          # On-demand service client integration
├── uva_athletics.py            # UVA athletics event feed integration
├── requirements.txt            # Python dependencies (FastAPI, uvicorn, httpx, pycryptodome)
├── package.json                # Node.js dependencies (Express)
├── Dockerfile                  # Multi-stage build with Python 3.12-slim
├── fly.toml                    # Fly.io deployment configuration
├── start.sh                    # Container entrypoint (sets permissions, launches uvicorn)
├── AGENTS.md                   # AI assistant instructions (read this!)
├── config/
│   ├── headway_config.json     # Headway monitoring configuration
│   └── stop_approach.json      # Stop approach detection settings
├── src/                        # Node.js maintenance ticketing service
│   ├── server.js               # Express app with REST API
│   ├── state.js                # Event sourcing state management
│   ├── storage.js              # JSONL append-only log + snapshots
│   ├── config.js               # Node.js configuration
│   └── logger.js               # Logging utilities
├── html/                       # User-facing HTML dashboards (30+ pages)
│   ├── index.html              # Landing page
│   ├── map.html                # Main operations map
│   ├── dispatcher.html         # Dispatcher control panel
│   ├── driver.html             # Driver interface
│   ├── servicecrew.html        # Service crew dashboard
│   ├── repairs.html            # Maintenance ticket management
│   ├── repairsscreen.html      # Shop floor display
│   ├── replay.html             # Historical playback
│   ├── headway.html            # Headway visualization
│   ├── headway_diagnostics.html
│   ├── arrivalsdisplay.html    # Public arrivals board
│   ├── radar.html              # Vehicle tracking
│   ├── eink-block.html         # E-ink display
│   ├── ridership.html          # Ridership analytics
│   ├── admin.html              # Admin panel
│   ├── stop-approach.html      # Stop approach config editor
│   └── sitemap.html            # Navigation sitemap
├── scripts/                    # Frontend JavaScript modules
│   ├── markers.js              # Map marker rendering
│   ├── kioskmap.js             # Kiosk map logic
│   ├── planeObject.js          # Vehicle object models
│   ├── planes_integration.js   # Vehicle tracking integration
│   ├── nav-bar.js              # Navigation component
│   ├── stop-approach.js        # Stop approach editor
│   ├── testmap.js              # Test map utilities
│   └── acceptance.sh           # End-to-end test script
├── css/                        # Stylesheets
│   ├── testmap.css
│   ├── kioskmap.css
│   └── stop-approach.css
├── fonts/                      # Custom fonts
├── media/                      # Media assets
├── examples/                   # Sample API payloads for testing
└── tests/                      # Python test suite
    ├── test_headway_tracker.py
    ├── test_headway_storage.py
    ├── test_ondemand_client.py
    ├── test_ticket_export.py
    ├── test_ticket_purge.py
    ├── test_dispatch_auth.py
    └── test_pulsepoint_first_on_scene.py
```

---

## Architecture

### Python FastAPI Backend (Port 8080)

**Main application:** `app.py`

- **Polling loops:** Background tasks fetch TransLoc routes/vehicles, Amtrak data, PulsePoint incidents, UVA athletics events
- **Headway tracking:** `HeadwayTracker` monitors vehicle proximity, arrival/departure events, bunching detection
- **SSE streams:** Real-time updates for map clients
- **REST endpoints:** Vehicle data, route geometry, block assignments, mileage tracking, arrivals predictions
- **Authentication:** Cookie-based auth for dispatcher pages with secure flag support

**Key modules:**
- `headway_tracker.py` - Vehicle position tracking, stop arrival/departure detection, approach cone logic
- `headway_storage.py` - Append-only event log for headway events with ISO8601 timestamps
- `tickets_store.py` - Python interface to maintenance ticket data
- `ondemand_client.py` - Integration with on-demand service providers
- `uva_athletics.py` - ICS feed parser for UVA home games (cached daily at 03:00 ET)

### Node.js Express Service (Port 8080)

**Maintenance ticketing API:** `src/server.js`

- **Event sourcing:** Append-only JSONL log (`events.jsonl`) with periodic snapshots
- **REST API:** CRUD operations with idempotency keys, CSV export, soft/hard purges
- **Durable writes:** Every event is fsync'd before acknowledgment
- **State materialization:** In-memory state rebuilt from log on startup
- **Single-machine design:** No cross-machine coordination required

**Storage layout** (`DATA_DIR=/data/maint`):
- `events.jsonl` - Append-only event log
- `snapshot.json` - Periodic state snapshot + replay offset
- `meta.json` - Last replayed offset + hard-purge checkpoint

### Frontend Architecture

- **Leaflet-based maps:** Most dashboards use Leaflet for geospatial visualization
- **Server-sent events:** Real-time updates without polling
- **Role-specific views:** Each HTML page targets specific operational roles
- **No build step:** Static HTML/CSS/JS served directly by FastAPI

---

## Key Configuration & Constants

### Headway Tracking (`headway_tracker.py`)

```python
HEADWAY_DISTANCE_THRESHOLD_M = 60.0           # Stop proximity threshold
STOP_APPROACH_DEFAULT_RADIUS_M = 100.0        # Default approach radius
STOP_SPEED_THRESHOLD_MPS = 0.5                # Speed threshold for "stopped"
MOVEMENT_CONFIRMATION_DISPLACEMENT_M = 2.0    # Displacement for departure confirmation
MOVEMENT_CONFIRMATION_MIN_DURATION_S = 20.0   # Min duration for movement confirmation
QUICK_DEPARTURE_MIN_DURATION_S = 5.0          # Quick departure threshold
```

### Environment Variables (from `fly.toml` and `app.py`)

**Polling intervals:**
- `VEH_REFRESH_S=10` - Vehicle position refresh interval
- `ROUTE_REFRESH_S=60` - Route geometry refresh interval
- `STALE_FIX_S=90` - Stale vehicle detection threshold

**Headway parameters:**
- `URBAN_FACTOR=1.12` - Urban speed adjustment
- `GREEN_FRAC=0.75` - Green headway threshold
- `RED_FRAC=0.50` - Red headway threshold (bunching)
- `ONTARGET_TOL_SEC=30` - On-time tolerance
- `W_LIMIT=0.85` - Weight limit factor
- `EMA_ALPHA=0.40` - Exponential moving average alpha
- `MIN_SPEED_FLOOR=1.2` - Minimum speed (m/s)
- `MAX_SPEED_CEIL=22.0` - Maximum speed (m/s)

**API endpoints:**
- `TRANSLOC_BASE` - TransLoc API base URL
- `TRANSLOC_KEY` - TransLoc API key
- `OVERPASS_EP` - Overpass API endpoint for OSM data
- `CAT_API_BASE` - CAT (Charlottesville Area Transit) API
- `PULSEPOINT_ENDPOINT` - Emergency incident feed
- `AMTRAKER_URL` - Amtrak train tracking
- `RIDESYSTEMS_CLIENTS_URL` - RideSystems client API
- `W2W_ASSIGNED_SHIFT_URL` - WhenToWork shift assignments

**Storage:**
- `DATA_DIRS=/data` - Persistent storage location (colon-separated)
- `VEH_LOG_DIRS=/data/vehicle_logs` - Vehicle log storage

**Security:**
- `DISPATCH_COOKIE_SECURE=true` - Enforce secure cookies in production

---

## Development Workflow

### Local Development

**Python FastAPI service:**
```bash
# Install dependencies
pip install -r requirements.txt

# Run development server (with auto-reload)
uvicorn app:app --reload --port 8080

# Access dashboards at http://localhost:8080/
```

**Node.js maintenance ticketing service:**
```bash
# Install dependencies
npm install

# Run locally
DATA_DIR=./data-local PORT=8080 node src/server.js
```

### Testing

**Python tests:**
```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m pytest tests/test_headway_tracker.py -v

# Key test files:
# - test_headway_tracker.py - Arrival/departure detection logic
# - test_headway_storage.py - Event persistence
# - test_ticket_export.py - CSV export validation
# - test_ticket_purge.py - Soft/hard purge logic
# - test_dispatch_auth.py - Authentication flows
```

**Acceptance tests (Node.js service):**
```bash
# End-to-end verification
scripts/acceptance.sh
```

### Deployment (Fly.io)

**Configuration:** `fly.toml`
- Region: `dfw` (Dallas)
- Port: `8080`
- Health check: `GET /v1/health`
- Volume: `/data` (auto-extends from 80% threshold, max 10GB)

**Deployment steps:**
```bash
# Deploy to Fly.io
fly deploy

# Check status
fly status

# View logs
fly logs

# SSH into machine
fly ssh console

# Set secrets (not committed to repo)
fly secrets set TRANSLOC_KEY=xxx W2W_KEY=yyy
```

**Important:** The app uses `start.sh` to set `/data` permissions before dropping to non-root `appuser`.

---

## Data Persistence Patterns

### Headway Events (`headway_storage.py`)

**Event format (JSONL):**
```json
{
  "vehicle_id": "1234",
  "stop_id": "stop_123",
  "route_id": "route_1",
  "timestamp": "2025-12-07T12:34:56.789Z",
  "event_type": "arrival"
}
```

**Storage:**
- Default location: `/data/headway_events.jsonl`
- ISO8601 UTC timestamps
- Append-only writes

### Maintenance Tickets (`src/storage.js`)

**Event sourcing:**
- Append-only log with fsync after each write
- Periodic snapshots (max every 60s or 500 events)
- Hard purges rewrite log with temp file + atomic rename

**Event types:**
- `ticket_created`
- `ticket_updated`
- `ticket_purged` (soft)
- `hard_purge` (removes events from log)

### Configuration Files

**`config/headway_config.json`:**
```json
{
  "tracked_route_ids": ["route_1", "route_2"],
  "tracked_stop_ids": ["stop_123", "stop_456"]
}
```

**`config/stop_approach.json`:**
```json
{
  "stop_123": [38.123, -78.456, 100.0]  // [lat, lon, radius_m]
}
```

---

## API Endpoints Overview

### FastAPI Routes (Python)

**Core vehicle/route data:**
- `GET /` - Landing page
- `GET /v1/health` - Health check (Fly.io monitoring)
- `GET /api/vehicles` - Current vehicle positions
- `GET /api/routes` - Route geometry
- `GET /api/blocks` - Block assignments
- `GET /stream/vehicles` - SSE stream of vehicle updates

**Dashboards (HTML):**
- `GET /map` - Main operations map
- `GET /dispatcher` - Dispatcher control panel
- `GET /driver` - Driver dashboard
- `GET /servicecrew` - Service crew view
- `GET /repairs` - Maintenance ticket UI
- `GET /repairsscreen` - Shop floor signage
- `GET /replay` - Historical replay
- `GET /headway` - Headway visualization
- `GET /headway-diagnostics` - Diagnostic view
- `GET /arrivalsdisplay` - Arrivals board
- `GET /vdot-cams` - Multi-state traffic camera viewer
- `GET /sitemap` - Full page list

**Specialized endpoints:**
- `GET /api/uva_athletics/home` - UVA home games (query: `start_date`, `end_date`, `start_time`, `end_time`)
- `GET /api/mileage` - Vehicle mileage tracking
- `GET /api/pulsepoint` - Emergency incidents
- `GET /api/amtrak` - Train positions

### Node.js Maintenance Ticket API

**CRUD operations:**
- `POST /api/tickets` - Create ticket (supports `Idempotency-Key` header)
- `GET /api/tickets/:id` - Get ticket
- `PUT /api/tickets/:id` - Update ticket (validates status transitions)
- `GET /api/tickets` - List all tickets

**Special endpoints:**
- `GET /api/signage` - Open tickets for shop floor displays
- `GET /api/export.csv` - RFC4180 CSV export (grouped by vehicle)
- `POST /api/purge` - Soft/hard purge with durable compaction

**Status validation:**
- OPS statuses: `reported`, `en_route`, `ready_for_service`
- SHOP statuses: `pending`, `diagnosed`, `parts_ordered`, `in_progress`, `completed`

### Traffic Camera API (`/vdot-cams`)

Multi-state traffic camera dashboard with HLS video streams. Frontend at `html/vdot-cams.html`.

**Architecture:**
- States with coordinates use Leaflet map picker (VA, MD, TN, AR)
- States without coordinates use dropdown picker (WV)
- All streams proxied through backend to handle CORS
- Grid layout configurable (columns × rows), persisted in localStorage

#### Virginia (VDOT 511)
- **Camera API:** `https://www.511virginia.org/data/geojson/icons-702702.geojson`
- **Stream pattern:** Direct HLS URLs in GeoJSON `properties.https_url`
- **Proxy:** `/api/vdot/stream/{stream_path}` - not currently needed (CORS allowed)
- **Coords:** Yes (`geometry.coordinates = [lng, lat]`)
- **Count:** ~1,669 cameras

#### West Virginia (WV511)
- **Camera API:** `/api/wv511/cameras` (backend aggregates 24 routes)
- **Source:** `https://wv511.org/map/data/CameraListing.aspx?ROUTE={route}`
- **Data format:** JavaScript with `myCams[]` array, pipe-delimited: `name|?|?|CAM_ID|?|is_break|?|description`
- **Routes:** I-64, I-68, I-70, I-77, I-79, I-81, I-470, US-19, US-33, US-35, US-48, US-50, US-52, US-60, US-119, US-219, US-220, US-250, US-340, WV-2, WV-9, WV-10, WV-61, Statewide
- **Stream pattern:** `https://vtcN.roadsummary.com/rtplive/{CAM_ID}/playlist.m3u8`
- **Proxy:** `/api/wv511/stream/{cam_id}` - tries vtc1-5 servers with fallback
- **Coords:** No (dropdown picker only)
- **Count:** ~200+ cameras

#### Maryland (CHART)
- **Camera API:** `/api/mdchart/cameras`
- **Source:** `https://chart.maryland.gov/video/GetCCTVDataNew.aspx?callback=processCCTVs`
- **Data format:** JSONP or plain JSON (backend handles both)
- **Stream pattern:** `https://strmrN.sha.maryland.gov/rtplive/{cctv_id}/playlist.m3u8`
  - Server number (`N`) is per-camera in `cctvIp` field
- **Proxy:** `/api/mdchart/stream/{server}/{path}` - validates `strmrN.sha.maryland.gov` format
- **Coords:** Yes (`Latitude`, `Longitude` fields)
- **Count:** ~556 cameras

#### Tennessee (TDOT SmartWay)
- **Camera API:** `/api/tndot/cameras`
- **Source:** `https://www.tdot.tn.gov/opendata/api/public/RoadwayCameras`
- **Auth:** Requires `ApiKey: 8d3b7a82635d476795c09b2c41facc60` header
- **Data format:** JSON array with `httpsVideoUrl` field
- **Stream pattern:** `https://{server}:443/rtplive/{path}/playlist.m3u8`
  - Various servers like `mcleansfs1.us-east-1.skyvdn.com`
- **Proxy:** `/api/tndot/stream/{server}/{path}`
- **Coords:** Yes (`lat`, `lng` fields)
- **Count:** ~667 cameras

#### Arkansas (IDrive Arkansas)
- **Camera API:** `/api/ardot/cameras`
- **Source:** `https://layers.idrivearkansas.com/cameras.geojson`
- **Data format:** GeoJSON FeatureCollection
- **Stream flow (important!):**
  1. `hls_stream_protected` field contains `https://actis.idrivearkansas.com/index.php/api/cameras/feed/{id}.m3u8`
  2. This URL returns **302 redirect** to CDN: `https://7212406.r.worldssl.net/...?token=xxx`
  3. CDN serves actual HLS playlist with time-sensitive token
  4. **Tokens expire after ~30-60 seconds** - must be refreshed automatically
- **Proxy (primary):** `/api/ardot/cam/{camera_id}/{filename}`
  - Camera-based endpoint with **automatic token management**
  - Maintains per-camera token cache, refreshed every 25 seconds
  - Playlist URLs rewritten to `/api/ardot/cam/{id}/chunklist.m3u8` (no tokens in URLs)
  - On 403, force-refreshes token and retries automatically
  - Video playback is uninterrupted during token refresh
- **Proxy (legacy):** `/api/ardot/stream/{server}/{path}`
  - Kept for backwards compatibility
  - Actis requests redirect to new `/api/ardot/cam/` endpoint
- **Required headers:** `Origin: https://idrivearkansas.com`, `Referer: https://idrivearkansas.com/`
- **Coords:** Yes (GeoJSON Point `geometry.coordinates = [lng, lat]`)
- **Count:** ~544 cameras

**Common proxy patterns:**
```python
# Arkansas uses camera-based proxy for automatic token management:
# /api/ardot/cam/{camera_id}/playlist.m3u8 -> fetches with current token
# /api/ardot/cam/{camera_id}/chunklist.m3u8 -> uses same cached token
# /api/ardot/cam/{camera_id}/media_123.ts -> uses same cached token

# Other states rewrite m3u8 playlists to route sub-requests through proxy:
# Relative URLs: /api/{state}/stream/{server}/{base_path}/{chunk.m3u8}
# Absolute URLs: Extract server/path and rewrite to proxy path
```

---

## Conventions & Patterns

### Code Style (Python)

- **Type hints:** Use `from typing import` extensively
- **Dataclasses:** Prefer `@dataclass` for structured data (see `VehicleSnapshot`, `ApproachState`)
- **Async/await:** Background tasks use `asyncio`
- **Config via environment:** All secrets and tunables via env vars
- **UTC timestamps:** Always use `datetime.timezone.utc` or `ZoneInfo("UTC")`
- **Local timezone:** UVA operations use `America/New_York` (set in `app.py`)

### Code Style (Node.js)

- **CommonJS:** `require()` not ES6 imports (see `package.json`: `"type": "commonjs"`)
- **Event sourcing:** Append-only JSONL with in-memory materialized state
- **Idempotency:** Support `Idempotency-Key` headers for ticket creation
- **Atomic writes:** Use temp file + rename for hard purges
- **Logging:** Structured logging via `logger.js`

### Naming Conventions

- **Python files:** `snake_case.py`
- **JavaScript files:** `camelCase.js` or `kebab-case.js`
- **HTML files:** `lowercase.html` or `kebab-case.html`
- **CSS files:** Match corresponding HTML (e.g., `testmap.html` → `testmap.css`)
- **Routes:** Use underscores in route names (e.g., `tracked_route_ids`)
- **Stops:** Prefix with `stop_` (e.g., `stop_id`)
- **Vehicles:** Use `vehicle_id` and `vehicle_name`

### Git Workflow

**Observed patterns from commit history:**
- Feature branches: `codex/descriptive-feature-name` or `claude/claude-md-{session-id}`
- Merge commits: All PRs use merge commits (no squashing observed)
- Commit messages: Descriptive present-tense imperatives
  - Examples: "Add movement confirmation for departures", "Track arrivals using approach cone behavior"
- PR descriptions: Remain technical (per AGENTS.md)

**Common change types:**
- Headway logic refinements (arrival/departure detection)
- GPS drift tolerance adjustments
- Movement confirmation thresholds
- Diagnostic logging additions

---

## Testing Practices

### Python Test Structure

Tests use standard Python `unittest` or `pytest`:
- Arrange-Act-Assert pattern
- Mock external API calls (`httpx` requests)
- Test critical paths: arrival detection, departure confirmation, purge logic
- Validate timestamp handling (UTC vs local time)

### Node.js Acceptance Tests

`scripts/acceptance.sh` exercises:
1. Ticket creation with idempotency
2. Status transitions (OPS/SHOP flows)
3. CSV export formatting
4. Soft purge (filtering)
5. Hard purge (log compaction)

**Run before deployment:**
```bash
./scripts/acceptance.sh
```

---

## AI Assistant Guidelines (from AGENTS.md)

**Critical rules:**
1. **User may be intoxicated** - Look for typos and infer intent
2. **Keep things simple** - Avoid overcomplicating additions
3. **Update sitemap** - Add new user-facing pages to `html/sitemap.html`
4. **Layman explanations** - Explain concepts in simple terms to user
5. **Technical PR descriptions** - Keep GitHub PR descriptions technical

**Codex authorship:**
All code in this repo was originally written by OpenAI's Codex (see README disclaimer).

---

## Common Tasks & Patterns

### Adding a New Dashboard

1. Create `html/new-page.html`
2. Add corresponding CSS in `css/new-page.css` if needed
3. Add JavaScript logic in `scripts/new-page.js` if needed
4. Register route in `app.py`:
   ```python
   @app.get("/new-page")
   async def new_page():
       return FileResponse("html/new-page.html")
   ```
5. **Add to sitemap:** Update `html/sitemap.html`

### Modifying Headway Logic

**Key file:** `headway_tracker.py`

1. Update constants at top of file if needed
2. Modify `process_snapshot()` for arrival/departure detection
3. Update diagnostic logging in `recent_snapshot_diagnostics`
4. Add tests in `tests/test_headway_tracker.py`
5. Check impact on `/headway-diagnostics` dashboard

### Adjusting Stop Approach Configuration

**File:** `config/stop_approach.json`

Format: `{"stop_id": [lat, lon, radius_m]}`

**UI editor:** `/stop-approach` (admin page)

### Adding External API Integration

1. Add base URL and credentials to environment variables
2. Create async polling function in `app.py`
3. Register background task in startup event
4. Cache results in module-level variable
5. Expose via REST endpoint or SSE stream

Example pattern (see `uva_athletics.py`):
```python
_cached_events = []
_last_fetch = None

async def fetch_external_api():
    global _cached_events, _last_fetch
    async with httpx.AsyncClient() as client:
        resp = await client.get(API_URL)
        _cached_events = parse_response(resp)
        _last_fetch = datetime.now(timezone.utc)

@app.on_event("startup")
async def startup():
    asyncio.create_task(periodic_fetch())
```

### Debugging SSE Streams

**Python backend:**
- Check `StreamingResponse` generator functions
- Verify `Content-Type: text/event-stream`
- Ensure `\n\n` after each event

**Frontend:**
- Open browser DevTools → Network → EventStream
- Look for `data:` lines in response
- Check connection stays open (no 204/304)

---

## Security Considerations

### Authentication

**Dispatcher pages:** Cookie-based auth with configurable secure flag
- `DISPATCH_COOKIE_SECURE=true` in production (HTTPS-only)
- Session tokens stored in HTTP-only cookies

**No authentication required:**
- Public-facing dashboards (arrivals displays, kiosks)
- Read-only API endpoints

### Secrets Management

**Never commit:**
- `TRANSLOC_KEY`
- `W2W_KEY`
- `CAT_API_TOKEN`
- `PULSEPOINT_PASSPHRASE`

**Set via Fly.io secrets:**
```bash
fly secrets set TRANSLOC_KEY=xxx W2W_KEY=yyy
```

### Data Privacy

- Vehicle logs may contain operator PII - handle accordingly
- Maintenance tickets include mechanic names and vehicle IDs
- Do not expose raw incident data without filtering

---

## Performance Optimization Notes

### Polling Frequency

Balance freshness vs API rate limits:
- **Vehicles:** 10s (configurable via `VEH_REFRESH_S`)
- **Routes:** 60s (geometry changes infrequently)
- **UVA Athletics:** Daily at 03:00 ET

### Caching Strategies

- **Route geometry:** Cache until polyline hash changes
- **Speed limits:** Fetch from Overpass once per route, invalidate on geometry change
- **UVA athletics:** Daily refresh, serve from memory
- **Ticket snapshots:** Every 60s or 500 events

### Event Log Growth

**Headway events:**
- Append-only growth
- Consider periodic archival for long-term deployments

**Maintenance tickets:**
- Hard purges compact the log
- Snapshots reduce replay time on startup

---

## Troubleshooting

### Common Issues

**Health check failing (`/v1/health`):**
- Check uvicorn startup logs
- Verify port 8080 is listening
- Ensure `/data` volume is mounted

**Vehicles not updating:**
- Check `TRANSLOC_KEY` is set
- Verify `VEH_REFRESH_S` environment variable
- Look for HTTP errors in logs

**Headway events not recording:**
- Verify `config/headway_config.json` exists
- Check route/stop IDs match TransLoc data
- Review `/headway-diagnostics` page

**Ticket writes failing:**
- Check `/data/maint` directory permissions
- Verify `appuser` owns the directory
- Look for fsync errors in Node.js logs

**SSE stream disconnects:**
- Check for proxy timeouts (nginx, Cloudflare)
- Verify `keep-alive` headers
- Review browser console for errors

### Logging

**Python (FastAPI):**
- Stdout/stderr captured by Fly.io
- Use `print()` for debug (appears in uvicorn logs)
- Check `[headway]` prefixed messages

**Node.js (Express):**
- Structured JSON logs via `logger.js`
- `logger.info()`, `logger.error()`, etc.

---

## Quick Reference

### File Size Hotspots
- `app.py` - 290KB (main application, many endpoints)
- `headway_tracker.py` - 54KB (core tracking logic)
- `scripts/testmap.js` - 744KB (large test map script)
- `html/map.html` - 545KB (main map dashboard)
- `html/dispatcher.html` - 172KB (dispatcher panel)

### Critical Paths
- Vehicle position updates → `HeadwayTracker.process_snapshot()`
- Arrival detection → Approach cone logic + distance thresholds
- Departure confirmation → Movement confirmation with displacement check
- Ticket creation → Event append + fsync + state materialization

### When to Ask User for Clarification
- New dashboard layout/design preferences
- Headway threshold adjustments (requires domain knowledge)
- Integration with new external APIs (requires credentials)
- Authentication requirements for new pages
- Data retention/archival policies

---

## Recent Development Focus (from git history)

Recent commits show focus on:
- **Departure timing logic** - Movement confirmation, displacement-based detection
- **GPS drift tolerance** - Preventing false departures from GPS noise
- **Arrival logging** - Approach cone behavior improvements
- **Movement confirmation** - Refining thresholds for reliable event detection

This indicates the headway tracking system is under active refinement. Be cautious when modifying these areas and add diagnostic logging for validation.

---

## Further Reading

- FastAPI docs: https://fastapi.tiangolo.com/
- Leaflet.js: https://leafletjs.com/
- Event sourcing patterns: https://martinfowler.com/eaaDev/EventSourcing.html
- Fly.io deployment: https://fly.io/docs/

---

**Last updated:** 2025-12-07
**For questions:** Consult AGENTS.md and README.md
