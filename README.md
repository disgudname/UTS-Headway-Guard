# UTS Headway Guard

UTS Headway Guard powers day-to-day transit operations for the University Transit Service. A FastAPI backend polls live vehicle and schedule data, while lightweight HTML dashboards help drivers, dispatchers, service crews, and signage stay aligned in real time.

## Quick overview
- **Live headway monitoring** – Combines TransLoc feeds with cached schedule data to flag bunching risks and highlight over-height vehicles approaching low-clearance zones.
- **Role-specific web tools** – Dedicated pages for drivers, dispatch, service crews, kiosks, and investigators provide maps, block rosters, mileage tables, replay timelines, and arrival boards.
- **Shared data layer** – Async background tasks keep route geometry, vehicle telemetry, block assignments, and mileage totals current. Key files persist under `/data` so restarts keep configuration and history intact.
- **Extensible signage** – Digital displays (arrivals boards, alert tickers, kiosk maps) reuse the same data pipeline, letting operations staff reuse the platform across lobbies, depots, and command posts.

## Components
- `app.py` – FastAPI application with background updaters, Server-Sent Events, configuration persistence, and mileage tracking.
- HTML dashboards (`*.html`) – Leaflet-based clients rendered directly by the backend for each audience (drivers, dispatchers, service crews, riders, admins, and replay analysts).
- Static assets – CSS, JavaScript, fonts, and sample API payloads that allow offline UI development and regression testing.

## Getting started
1. Install dependencies:
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
