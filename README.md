# UTS Headway Guard

UTS Headway Guard is a proof-of-concept service and lightweight web UI for reducing bus bunching on transit routes.
It uses the FastAPI framework to expose anti-bunching calculations to drivers and dispatchers.

## Features
- Polls TransLoc for routes and live vehicle positions.
- Fetches and caches speed limits from the Overpass API.
- Computes arc-based headways and targets between consecutive vehicles.
- Exposes REST endpoints and a Server-Sent Events stream for live updates.
- Includes minimal dispatcher and driver web clients.

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

## Configuration
Runtime settings can be tuned with environment variables such as `TRANSLOC_BASE`, `TRANSLOC_KEY` and `OVERPASS_EP`.
See `app.py` for the full list and default values.

## Docker
Build and run a containerised instance:
```bash
docker build -t uts-headway-guard .
docker run -p 8080:8080 uts-headway-guard
```

