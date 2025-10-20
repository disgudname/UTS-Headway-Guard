# UTS Operations Dashboard

UTS Operations Dashboard powers day-to-day transit operations for the University Transit Service. A FastAPI backend polls live vehicle and schedule data, while lightweight HTML dashboards help drivers, dispatchers, service crews, and signage stay aligned in real time.

## Quick overview
- **Live headway monitoring** – Combines TransLoc feeds with cached schedule data to flag bunching risks and highlight over-height vehicles approaching low-clearance zones.
- **Role-specific web tools** – Dedicated pages for drivers, dispatch, service crews, kiosks, and investigators provide maps, block rosters, mileage tables, replay timelines, and arrival boards.
- **Shared data layer** – Async background tasks keep route geometry, vehicle telemetry, block assignments, and mileage totals current. Key files persist under `/data` so restarts keep configuration and history intact.
- **Extensible signage** – Digital displays (arrivals boards, alert tickers, kiosk maps) reuse the same data pipeline, letting operations staff reuse the platform across lobbies, depots, and command posts.

## Components
- `app.py` – FastAPI application with background updaters, Server-Sent Events, configuration persistence, and mileage tracking.
- HTML dashboards (`html/*.html`) – Leaflet-based clients rendered directly by the backend for each audience (drivers, dispatchers, service crews, riders, admins, and replay analysts).
- Static assets – Stylesheets in `css/`, JavaScript in `scripts/`, fonts in `fonts/`, media in `media/`, and sample API payloads in `examples/` to support offline UI development and regression testing.

## Maintenance ticketing service

A disk-backed maintenance ticketing API now ships alongside the legacy Python dashboards. The service is implemented in Node.js/Express and is designed for Fly.io machines that replicate directly over HTTP using append-only JSONL event logs.

### Running locally

```bash
npm install
DATA_DIR=./data-local PORT=8080 node src/server.js
```

The server exposes the documented REST API on `PORT` (default `8080`). Set `FLY_MACHINE_ID` to any unique string when running more than once on the same host. Leave `PEERS` unset for standalone usage; supply a comma-separated list of peer base URLs to enable replication.

### Storage layout

State is persisted under `DATA_DIR` (default `/data/maint`):

- `events.jsonl` – append-only event log, fsync'd on every write.
- `snapshot.json` – periodic snapshot of the materialised state plus the replay offset.
- `meta.json` – durable replication metadata (`pending` acks, compaction checkpoints).
- `queue/` – reserved for retry bookkeeping.

Snapshots are emitted at most every 60 seconds or 500 events. Hard purges rewrite `events.jsonl` with a temp file + atomic rename and trigger a full rebuild of in-memory state.

### Replication on Fly.io

Each Fly.io machine should mount its own volume at `/data/maint` and set the following environment variables:

```toml
[env]
  DATA_DIR = "/data/maint"
  FLY_MACHINE_ID = "machine-a" # unique per machine
  PEERS = "http://10.0.0.2:8080,http://10.0.0.3:8080"
```

Writes return once they are persisted locally; a background replicator retries peers until two distinct machine IDs acknowledge each event. Pending replication state survives restarts. During startup the service fetches recent events from peers via `/internal/export` to achieve eventual consistency.

### REST highlights

- `POST /api/tickets` / `PUT /api/tickets/:id` – CRUD with status validation and optional `Idempotency-Key` headers.
- `GET /api/signage` – open tickets suitable for shop-floor displays.
- `GET /api/export.csv` – RFC4180 CSV grouped by vehicle label then chronologically.
- `POST /api/purge` – soft and hard purges with replication and durable compaction.

All read paths honour soft purges. Hard purges remove matching ticket events from the log and rebuild the snapshot so deleted data will not reappear after restarts.

### Acceptance checks

Run the scripted end-to-end verification (spins up two local machines, exercises replication, purges, and CSV export):

```bash
scripts/acceptance.sh
```

## Getting started
1. Install Python dependencies (for the legacy FastAPI dashboards):
   ```bash
   pip install -r requirements.txt
   ```
2. Run the development server:
   ```bash
   uvicorn app:app --reload --port 8080
   ```
3. Open the role-specific pages (e.g. `/driver`, `/dispatcher`, `/servicecrew`, `/map`) at <http://localhost:8080>.

## Deployment notes
- The provided `Dockerfile` builds a minimal Python image, creates a non-root user, installs dependencies, and launches Uvicorn via `start.sh`.
- `fly.toml` demonstrates a Fly.io deployment that mounts persistent storage at `/data` for configuration, mileage, and vehicle logs.

---

Disclaimer: All code in this repository was written by OpenAI's Codex.
