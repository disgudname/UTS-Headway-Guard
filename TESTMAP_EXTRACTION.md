# Testmap Extraction Guide

Extract the public testmap page into a standalone Fly.io app that shows **UVA bus locations only**.

---

## FOR THE HUMAN: Files to Copy

Copy these files from `UTS-Headway-Guard` into your new repo, preserving directory structure:

### Frontend
```
html/testmap.html
css/testmap.css
css/marker-selection-menu.css
scripts/testmap.js
scripts/marker-selection-menu.js
```

### Static Assets
```
media/favicon.ico
media/favicon.svg
media/apple-touch-icon-120.png
media/apple-touch-icon-152.png
media/apple-touch-icon-180.png
media/busmarker.svg
media/client-logo.png          <-- YOU PROVIDE THIS
fonts/FGDC.ttf
```

---

## FOR THE AI: Build a Minimal FastAPI App

Assume all files listed above are present, including `media/client-logo.png`.

This is a **UVA-only** app - no agency switching. Hardcode the UVA TransLoc base URL.

### Required API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /v1/testmap/transloc/metadata` | Routes, stops, agency info |
| `GET /v1/stream/testmap/vehicles` | SSE stream - immediate snapshot + updates every 10s |
| `GET /v1/config` | Runtime config |
| `GET /v1/health` | Health check for Fly.io |

### Static File Routes

```python
GET /testmap                    -> html/testmap.html
GET /testmap.js                 -> scripts/testmap.js
GET /testmap.css                -> css/testmap.css
GET /marker-selection-menu.js   -> scripts/marker-selection-menu.js
GET /marker-selection-menu.css  -> css/marker-selection-menu.css
GET /busmarker.svg              -> media/busmarker.svg
GET /FGDC.ttf                   -> fonts/FGDC.ttf
GET /media/*                    -> media/* (favicons, client-logo.png)
```

### Environment Variables

```bash
TRANSLOC_KEY=xxx
TRANSLOC_BASE=https://uva.transloc.com  # Hardcoded for UVA
VEH_REFRESH_S=10
ROUTE_REFRESH_S=60
STALE_FIX_S=90
```

### What to EXCLUDE

- Agency switching (no RideSystems, no `base_url` query param)
- Separate REST endpoint for vehicles (use SSE only)
- CAT transit
- Amtrak trains
- PulsePoint incidents
- On-demand service
- Authentication
- Admin/dispatcher features
- All other pages

### Implementation Notes

1. **UVA only** - Hardcode the TransLoc base URL, ignore any `base_url` query params
2. **SSE-only for vehicles** - Send immediate snapshot on connection, then push updates every 10s
3. **No authentication** - Public map
4. **CORS** - Allow all origins
5. **Client logo** - Serve `media/client-logo.png` for the agency logo

### Minimal app.py

```python
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
import json
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TRANSLOC_KEY = os.environ.get("TRANSLOC_KEY", "")
TRANSLOC_BASE = os.environ.get("TRANSLOC_BASE", "https://uva.transloc.com")
VEH_REFRESH_S = int(os.environ.get("VEH_REFRESH_S", "10"))

# Static files
app.mount("/media", StaticFiles(directory="media"), name="media")

@app.get("/v1/health")
async def health():
    return {"status": "ok"}

@app.get("/testmap")
async def testmap_page():
    return FileResponse("html/testmap.html", media_type="text/html")

@app.get("/testmap.js")
async def testmap_js():
    return FileResponse("scripts/testmap.js", media_type="application/javascript")

@app.get("/testmap.css")
async def testmap_css():
    return FileResponse("css/testmap.css", media_type="text/css")

@app.get("/marker-selection-menu.js")
async def marker_menu_js():
    return FileResponse("scripts/marker-selection-menu.js", media_type="application/javascript")

@app.get("/marker-selection-menu.css")
async def marker_menu_css():
    return FileResponse("css/marker-selection-menu.css", media_type="text/css")

@app.get("/busmarker.svg")
async def busmarker():
    return FileResponse("media/busmarker.svg", media_type="image/svg+xml")

@app.get("/FGDC.ttf")
async def font():
    return FileResponse("fonts/FGDC.ttf", media_type="font/ttf")

@app.get("/v1/config")
async def config():
    return {
        "TRANSLOC_BASE": TRANSLOC_BASE,
        "VEH_REFRESH_S": VEH_REFRESH_S,
    }

@app.get("/v1/testmap/transloc/metadata")
async def transloc_metadata():
    # Proxy to TransLoc API - routes, stops, agency info
    async with httpx.AsyncClient() as client:
        # Fetch and return metadata
        pass

@app.get("/v1/stream/testmap/vehicles")
async def stream_vehicles():
    async def generate():
        # Send immediate snapshot on connection
        vehicles = await fetch_vehicles_from_transloc()
        yield f"data: {json.dumps({'vehicles': vehicles})}\n\n"

        # Then push updates every VEH_REFRESH_S seconds
        while True:
            await asyncio.sleep(VEH_REFRESH_S)
            vehicles = await fetch_vehicles_from_transloc()
            yield f"data: {json.dumps({'vehicles': vehicles})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

async def fetch_vehicles_from_transloc():
    # Fetch current vehicle positions from TransLoc API
    async with httpx.AsyncClient() as client:
        # Implementation here
        return []
```

### fly.toml

```toml
app = "uva-bus-map"
primary_region = "dfw"

[http_service]
  internal_port = 8080
  force_https = true

[[services.http_checks]]
  path = "/v1/health"
  interval = 30000
  timeout = 5000
```

---

## Summary

4 API endpoints:
- 1 TransLoc proxy (metadata - routes/stops)
- 1 SSE stream (vehicles - immediate snapshot + 10s updates)
- 1 config
- 1 health check

Plus static file serving. No agency switching. SSE-only for vehicle positions.
