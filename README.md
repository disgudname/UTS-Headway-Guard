# UTS Headway Guard

UTS Headway Guard is a proof-of-concept service and lightweight web UI for reducing bus bunching on transit routes.
It gathers live data from TransLoc and the OpenStreetMap Overpass API, computes target headways, and exposes the results through FastAPI for drivers and dispatchers.

## Features
- Polls TransLoc for route geometry and live vehicle positions.
- Fetches and caches OSM speed limits and low-clearance data via Overpass.
- Computes arc-based headways and recommended actions (OK/Ease off/HOLD).
- Exposes REST endpoints for health, routes, vehicles, per-vehicle instructions, and low-clearance warnings.
- Streams per-route status via Server-Sent Events for live updates.
- Serves minimal dispatcher and driver web clients with overheight alerts.

## Requirements
- Python 3.10+
- Dependencies listed in `requirements.txt`

Install dependencies:
```bash
pip install -r requirements.txt
```

## Running
Start the development server with:
```bash
uvicorn app:app --reload --port 8080
```

Open the [driver](http://localhost:8080/driver) and [dispatcher](http://localhost:8080/dispatcher) pages in a browser.

### Replay

A lightweight logger and replay page are included for reviewing past vehicle
positions. The application automatically polls TransLoc every few seconds and
appends snapshots to `vehicle_log.jsonl`, pruning entries older than one week.
Open `/replay` in a running server to view the logged data with a timeline and
playback controls (pause, play and fast forward).

## API

Key endpoints exposed by the service:
- `/v1/health` – service health and last error.
- `/v1/routes` – active routes with current status.
- `/v1/routes_all` – all known routes with active flags.
- `/v1/roster/vehicles` – roster of vehicle names.
- `/v1/vehicles` – live vehicle positions.
- `/v1/low_clearances` – low-clearance locations near the bridge.
- `/v1/routes/{route_id}/status` – headway status for a route.
- `/v1/routes/{route_id}/vehicles/{vehicle_name}/instruction` – driver instruction for a vehicle.
- `/v1/stream/routes/{route_id}` – Server-Sent Events stream.

## Configuration
Runtime settings can be tuned with environment variables such as `TRANSLOC_BASE`, `TRANSLOC_KEY` and `OVERPASS_EP`.
See `app.py` for the full list and default values.

## Docker
Build and run a containerised instance:
```bash
docker build -t uts-headway-guard .
docker run -p 8080:8080 uts-headway-guard
```

