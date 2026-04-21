# Van Dispatch — Implementation Planning

**Status:** Pre-implementation planning  
**Author:** Pat + Claude, April 2026  
**Goal:** Purpose-built dispatcher tooling for on-demand van operations, currently powered by TransLoc OnDemand and designed to transition seamlessly to Spare.

---

## Context

The existing `map.html` / `testmap.js` stack is bus-first. Vans and on-demand rides exist there but are secondary: rides are only visible by clicking specific markers, there's no flat ride list, and there are no dispatch actions. As UTS moves to in-house paratransit (and eventually migrates on-demand off TransLoc onto Spare), a dedicated van dispatch interface is needed.

The full Spare OpenAPI spec is saved at `spare_openapi_spec.json` for reference during implementation.

---

## Pages to Build

Three separate pages, all behind dispatcher auth (`_require_dispatcher_access`), all linked from `sitemap.html` and from each other.

### 1. `html/van-map.html`
The primary all-in-one dispatch view. Map on one side, ride list on the other — both always visible so a dispatcher can act without switching pages. Vans always on; buses off by default with a toggle. Clicking a van or a ride row cross-highlights the other. Common dispatch actions (cancel, rematch, edit notes) available directly from the ride list without navigating away.

### 2. `html/van-rides.html`
Deep-dive ride board for when the dispatcher needs more screen real estate for the table — detailed filters, extended columns, bulk views. Useful for planning ahead or reviewing history. Less time-pressure than `van-map.html`.

### 3. `html/van-duties.html`
Shift roster. One row per active/scheduled duty showing: driver name, vehicle, shift start/end, trip count, current lateness, matching status (paused/active). Action buttons for pausing matching and cancelling a shift.

---

## Backend API to Add (all in `app.py`)

All new endpoints require dispatcher auth. Proxy to whichever provider is active.

### Read endpoints

| Endpoint | Description |
|---|---|
| `GET /api/ondemand/rides` | Flat list of rides with status, pickup/dropoff info, van assignment, accessibility flags. Extracted from existing schedules/rides data. |
| `GET /api/ondemand/duties` | Active and scheduled duties with driver, vehicle, trip count, lateness. |

For now these just reshape existing `_collect_ondemand_data()` output into flat lists. When Spare is wired up, they switch to calling Spare's `/requests` and `/duties` endpoints.

### Write endpoints (Spare-only, return 503 when Spare not configured)

| Endpoint | Spare call | Description |
|---|---|---|
| `POST /api/ondemand/rides/{id}/cancel` | `POST /requests/{id}/cancellation` | Cancel a ride |
| `POST /api/ondemand/rides/{id}/rematch` | `POST /requests/{id}/rematch` | Re-dispatch to another van |
| `PATCH /api/ondemand/rides/{id}/notes` | `PATCH /requests/{id}` | Update driver notes |
| `POST /api/ondemand/rides` | `POST /requests` | Create a ride manually |
| `PATCH /api/ondemand/duties/{id}/matching` | `PATCH /duties/{id}` | Pause or resume trip matching |
| `POST /api/ondemand/duties/{id}/cancel` | `POST /duties/{id}/cancellation` | Cancel a shift early |

Write endpoints should return a clear JSON error (`{"error": "Dispatching requires Spare — not yet configured"}`) rather than a generic 503 so the frontend can show a useful message.

### Provider detection helper

Add a helper `_spare_client()` that returns an httpx client pre-configured with `SPARE_API_KEY` and the right base URL, or `None` if unconfigured. Write endpoints call this and branch accordingly. Keeps provider logic in one place.

---

## Spare API Reference

Full spec: `spare_openapi_spec.json` in project root (1.8 MB OpenAPI 3.0).  
Live docs: https://developers.sparelabs.com/docs/public/2bf42f837bf82-spare-open-api

### Authentication
All requests: `Authorization: Bearer <SPARE_API_KEY>` header. Key lives in Spare org admin settings.

### Base URLs
- US (likely UTS): `https://api.us.sparelabs.com/v1`
- US2: `https://api.us2.sparelabs.com/v1`

### Webhooks
Registered via Spare Platform UI (Org Settings → Webhooks) or `POST /webhooks`.  
Payload delivery: HTTP POST to our endpoint, JSON body, 5 retries w/ exponential backoff (1s → 10min).

### New environment variables to add to `fly.toml`

```
SPARE_API_KEY        # Bearer token for Spare API
SPARE_API_BASE_URL   # e.g. https://api.us.sparelabs.com/v1
SPARE_ORG_ID         # Organization ID for scoping queries (if needed)
SPARE_WEBHOOK_SECRET # For verifying incoming webhook signatures (see webhook-security docs)
```

---

## Spare API — Endpoint Details for Implementation

### Fetching rides: `GET /requests`

Query parameters useful for the ride board:
- No built-in date filter documented — fetch and filter by `scheduledPickupTs` client-side, or use `GET /dutySchedules/requests?dutyIds[]=...&fromRequestedPickupTs=...&toRequestedPickupTs=...` to scope to today's active duties.

Key response fields (full schema in `spare_openapi_spec.json` → `components/schemas/` or see `GET /requests/{id}`):

```
id                          string
status                      "processing"|"noDriversAvailable"|"serviceDisruption"|"accepted"|"arriving"|"inProgress"|"cancelled"|"completed"
dispatchStatus              "awaitingDispatch"|"offeredToFleet"|"unscheduled"|"onHold"
riderId / rider.firstName+lastName
driverId / driver.firstName+lastName
vehicleId / dutyId / dutyIdentifier
requestedPickupAddress      string
requestedPickupLocation     GeoJSON Point  {type:"Point", coordinates:[lng,lat]}
scheduledPickupTs           epoch seconds
scheduledPickupAddress      string
scheduledPickupLocation     GeoJSON Point
pickupEta                   epoch seconds (null once pickup complete)
pickupArrivedTs             epoch seconds
pickupCompletedTs           epoch seconds
requestedDropoffAddress     string
requestedDropoffLocation    GeoJSON Point
scheduledDropoffTs          epoch seconds
dropoffEta                  epoch seconds (null once dropoff complete)
dropoffArrivedTs            epoch seconds
dropoffCompletedTs          epoch seconds
numRiders                   {total, wheelchair, ...}
accessibilityFeatures       array of strings
notes                       string  (driver-facing instructions)
pickupNotes / dropoffNotes  strings
lateness                    object
metrics                     object (populated on completion)
cancellationDetails         object (populated if cancelled)
```

### Fetching duties: `GET /dutySchedules/duties`

Best endpoint for the duty roster — range-queryable, more efficient than paginating `/duties`.

Query parameters:
```
nowTs               current time in seconds (required)
withinRangeFromTs   range start (epoch seconds)
withinRangeToTs     range end (epoch seconds)
status              "scheduled"|"inProgress"|"completed"|"cancelled"  (repeatable)
fleetId             filter by fleet (repeatable)
```

Key response fields per duty:
```
id / identifier
driverId / driver.firstName+lastName
vehicleId / vehicle.identifier + vehicle.licensePlate
status              "scheduled"|"inProgress"|"completed"|"cancelled"
startRequestedTs / endRequestedTs   epoch seconds
startLocation / endLocation         GeoJSON Point
isMatchingEnabled   boolean
lateness            number (seconds)
scheduleLegs        array  (ordered stops/slots for this duty)
metrics             object
```

### Fetching slots (manifest per duty): `GET /dutySchedules/slots?dutyIds[]=...`

Returns ordered pickup/dropoff sequence for given duty IDs. Lighter than fetching each slot individually.

Key response fields per slot:
```
id
dutyId
type                "pickup"|"dropoff"|"startLocation"|"endLocation"|"routeStop"|"driverBreak"
requestId
scheduledTs         epoch seconds
scheduledAddress
scheduledLocation   GeoJSON Point
eta                 epoch seconds
arrivedTs / completedTs / cancelledTs / startedTs   epoch seconds
requestStatus       RequestStatus (see above)
rider.firstName+lastName
accessibilityFeatures
requestNotes
walkingDistance / walkingDuration
```

### Vehicle location: `GET /duties/{id}/vehicleLocation`

One-off fetch. For live tracking, use the `vehicleLocation` webhook instead.

```
vehicleId
dutyId
location            GeoJSON Point  {type:"Point", coordinates:[lng,lat]}
bearing             degrees (0–360)
isRoutable          boolean
latestLocationUpdatedTs   epoch seconds
```

### Write operations

#### Cancel a ride: `POST /requests/{id}/cancellation`
```json
// Request body (all optional):
{ "reason": "string", "cancelledByType": "dispatcher" }

// Response: updated request object
```

#### Rematch a ride: `POST /requests/{id}/rematch`
```json
// No body required
// Response: updated request object
```

#### Update ride notes: `PATCH /requests/{id}`
```json
{ "notes": "New driver instructions here" }
// Can also patch: pickupNotes, dropoffNotes, metadata
// Response: updated request object
```

#### Create a ride manually: `POST /requests`
Requires knowing the `serviceId` (from Spare org setup). Minimum body:
```json
{
  "riderId": "string",
  "serviceId": "string",
  "requestedPickupLocation": { "type": "Point", "coordinates": [lng, lat] },
  "requestedDropoffLocation": { "type": "Point", "coordinates": [lng, lat] },
  "requestedPickupTs": 1234567890
}
```
Full schema in spec — also supports `notes`, `accessibilityFeatures`, `numRiders`, `metadata`.

#### Pause/resume matching on a duty: `PATCH /duties/{id}`
```json
{ "isMatchingEnabled": false }   // pause
{ "isMatchingEnabled": true }    // resume
```

#### Cancel a duty: `POST /duties/{id}/cancellation`
```json
// No body required (or pass reason string)
// Response: updated duty object
```

---

### Driver Breaks — Full Detail

Breaks appear as `type: "driverBreak"` slots in a duty's slot sequence (from `GET /dutySchedules/slots`), so they show up automatically in the ordered manifest alongside pickups and dropoffs. No special handling needed to display them — they're already in the slot stream.

#### Break Policies: `GET/POST /breakPolicies`

Org-level rules for when breaks are required.
```
breakLength       seconds the break lasts
maxDrivingTime    max continuous driving time before a break is required (seconds)
id
```

#### List breaks for a duty: `GET /driverBreaks?dutyId={id}`

#### Schedule a break within a time window: `POST /driverBreaks`
```json
{
  "dutyId": "string",
  "breakLength": 900,
  "earliestStartTs": 1234567890,
  "latestStartTs":   1234568790,
  "address": "string (optional)",
  "location": { "type": "Point", "coordinates": [lng, lat] },
  "notes": "string",
  "breakReasonId": "string"
}
```

#### Insert a break as soon as possible: `POST /driverBreaks/asap`
Most useful for urgent breaks (driver medical, vehicle issue, etc.).
```json
{
  "dutyId": "string",
  "breakLength": 900,
  "conflictResolution": "none | afterDropoffs | rematchOnboardRiders",
  "address": "string (optional)",
  "location": { "type": "Point", "coordinates": [lng, lat] },
  "notes": "string",
  "breakReasonId": "string"
}
```

**`conflictResolution` options:**
- `none` — only insert if it doesn't conflict with current passengers (safest)
- `afterDropoffs` — wait until all current passengers are dropped off, then break
- `rematchOnboardRiders` — reassign any current passengers to other vans and break immediately (most disruptive, for genuine emergencies)

#### Update a break: `PATCH /driverBreaks/{id}`
Can update `breakLength`, `earliestStartTs`, `latestStartTs`, `location`, `address`, `notes`, `breakReasonId`.

#### Cancel a break: `POST /driverBreaks/{id}/cancellation`

#### Break status
No explicit status field on the break object — lifecycle is tracked through the corresponding slot (`type: "driverBreak"`), which has `status: scheduled | inProgress | completed | cancelled` like any other slot.

#### Break Reason taxonomy (`DriverBreakReasonType` enum)
Break reasons are configured per-org (which ones are active, whether they feed into revenue/vehicle metrics, whether they trigger an incident case in Spare Engage). The full type list:

| Category | Types |
|---|---|
| Routine | `break`, `adHocBreak` |
| Driver | `driverDAndATesting`, `driverMedical`, `driverCallout`, `driverPersonalEmergency`, `driverMisconduct`, `driverUnionBusiness` |
| Vehicle | `vehicleAccident`, `vehicleBreakdown`, `vehicleCharging`, `vehicleFueling`, `vehicleMaintenance` |
| Location | `locationClosed`, `locationIncorrect` |
| Rider | `riderBiohazard`, `riderDisorderly`, `riderMedical`, `riderNonpayment`, `doorToDoor` |
| General | `other` |

The `createIncidentCase` flag on a reason config means certain break reasons (e.g. `vehicleAccident`, `riderMedical`) can automatically open a case in **Spare Engage** — a separate CRM/incident management add-on within Spare's product suite (not part of core dispatch). Whether this does anything useful depends on whether UTS's Spare contract includes Engage. Worth asking the Spare rep — it could be valuable for paratransit compliance/documentation, but it's out of scope for the dashboard work either way.

#### Dashboard integration notes
- The Van Dispatch page should add a **"Schedule Break"** action to the duty row in the shift roster — opens a small form with length, time window, and reason
- **"Break ASAP"** should be a separate prominent button with a `conflictResolution` picker, since the choice has real operational consequences (especially `rematchOnboardRiders`)
- Break slots already appear in the van's slot sequence on the map popup — no extra work needed there
- The reason taxonomy is rich enough to be useful for incident reporting; worth surfacing the reason label in the shift roster when a break is active

### Webhook events (real-time)

Register webhook URL via Spare Platform UI. All events POST JSON to our endpoint.

#### `vehicleLocation` — fires on every GPS update from an active duty
```json
{
  "type": "vehicleLocation",
  "data": {
    "vehicleId": "...",
    "dutyId": "...",
    "location": { "type": "Point", "coordinates": [lng, lat] },
    "bearing": 45,
    "isRoutable": true,
    "latestLocationUpdatedTs": "1540233622"
  }
}
```
→ Fan out to SSE stream clients so `van-map.html` gets live position updates.

#### `requestStatus` — fires on every ride status change
```json
{
  "type": "requestStatus",
  "data": { /* full request object, same schema as GET /requests/{id} */ }
}
```
→ Update the ride board row in real-time.

#### `eta` — fires when pickup or dropoff ETA changes
```json
{
  "type": "eta",
  "data": {
    "updates": [
      { "requestId": "...", "pickupETA": 1549677553, "dropoffETA": 1549677553 }
    ]
  }
}
```
→ Update ETA column in ride board without a full re-fetch.

### Webhook security
Spare signs webhook payloads with HMAC-SHA256 using `SPARE_WEBHOOK_SECRET`. Verify before processing.  
See: https://developers.sparelabs.com/docs/public/webhook-security

---

## Data Model — Normalized Ride Object

The frontend should receive a consistent shape regardless of provider. Define this shape in the backend and populate it from whichever source is active.

```json
{
  "id": "string",
  "provider": "transloc | spare",
  "status": "scheduled | processing | accepted | arriving | inProgress | completed | cancelled | noDriversAvailable",
  "riderName": "string",
  "riderPhone": "string | null",
  "accessibilityFeatures": ["string"],
  "notes": "string | null",
  "numRiders": 1,
  "pickupAddress": "string",
  "pickupLocation": { "lat": 0, "lng": 0 },
  "scheduledPickupTs": 1234567890,
  "pickupEta": 1234567890,
  "dropoffAddress": "string",
  "dropoffLocation": { "lat": 0, "lng": 0 },
  "scheduledDropoffTs": 1234567890,
  "dropoffEta": 1234567890,
  "vehicleId": "string | null",
  "vehicleName": "string | null",
  "driverId": "string | null",
  "driverName": "string | null",
  "dutyId": "string | null",
  "lateness": "number | null",
  "canCancel": true,
  "canRematch": true,
  "canEditNotes": true
}
```

`canCancel` / `canRematch` / `canEditNotes` are set `false` when Spare is not configured, so the frontend can disable buttons without needing to know about the provider.

---

## Normalized Duty Object

```json
{
  "id": "string",
  "identifier": "string",
  "driverName": "string",
  "vehicleName": "string",
  "vehicleLicensePlate": "string | null",
  "status": "scheduled | inProgress | completed | cancelled",
  "startTs": 1234567890,
  "endTs": 1234567890,
  "tripCount": 5,
  "completedTripCount": 2,
  "isMatchingEnabled": true,
  "lateness": "number | null",
  "canPauseMatching": true,
  "canCancel": true
}
```

---

## van-map.html — Design Notes

This is the primary day-to-day dispatch page. The goal is to handle all common actions without leaving the page.

### Layout
Split pane: map left (~55%), ride list right (~45%). A collapse arrow lets the dispatcher go map-only or list-only if needed. Both panels scroll independently.

### Map panel
- Base: same Leaflet setup as `map.html` but stripped down
- Van markers always visible; directional arrows when bearing available (Spare provides bearing, TransLoc may not)
- Bus layer off by default, toggle button in map controls to enable
- Clicking a van marker opens a sidebar popup (not a Leaflet popup) showing:
  - Driver name, vehicle ID/plate
  - Upcoming slot sequence: ordered pickups/dropoffs with times and status
  - Quick link to that van's rows in the ride list (highlights them)
- Pickup and dropoff location markers shown on the map for active/arriving rides; clicking one highlights the corresponding ride row
- When Spare `vehicleLocation` webhook is wired up: positions update in real-time via SSE
- For now: polls `/api/ondemand` on existing TTL

### Ride list panel
Same table as `van-rides.html` (same columns, same sort/filter controls) but lives alongside the map. Clicking a ride row pans the map to that ride's pickup location and highlights the assigned van marker.

Common dispatch actions accessible inline per row (icon buttons, not hidden in a detail pane):
- **Notes** pencil icon — inline editable field, saves on blur/enter; Spare only
- **Cancel** × icon — one confirmation click; Spare only
- **Rematch** ↺ icon — one confirmation click; Spare only

Action buttons are visible at all times but dimmed with a tooltip when Spare is not yet configured, so the UI is already built and the dispatcher knows what's coming.

A slide-out detail drawer (right edge of the list panel) opens on row click for the full ride details — all fields, status history if available, links.

---

## van-rides.html — Design Notes

### Layout
Split pane: map left (~40%), ride table right (~60%). Collapsible. Map can be hidden to give full width to the table.

### Ride table columns
| Column | Notes |
|---|---|
| Status | Color-coded badge (scheduled=grey, accepted=blue, arriving=yellow, inProgress=green, completed=dimmed, cancelled=red) |
| Rider | Name, accessibility icons (wheelchair, etc.) if features present |
| Pickup | Address + scheduled time |
| Dropoff | Address + scheduled time |
| ETA | Live pickup ETA if available (Spare only) |
| Van / Driver | Vehicle name + driver name |
| Lateness | Spare only; minutes late |

### Sort options
- Scheduled pickup time (default, ascending)
- Van / driver
- Status (group by status phase)
- Lateness (Spare only)

### Filter options
- Status: checkboxes for each status (default: hide completed + cancelled)
- Van: multi-select (show all or filter to specific vehicles)
- Time window: today's rides (default), or custom range

### Detail pane (opens on row click)
Shows all ride fields. Action buttons:
- **Edit Notes** — inline textarea, PATCH on save; disabled with tooltip if Spare not configured
- **Cancel Ride** — confirmation dialog; disabled if not Spare
- **Rematch** — confirmation dialog; disabled if not Spare
- Link: "View on Spare Platform" (opens Spare admin UI for this request) — Spare only

### Real-time updates
- Poll `/api/ondemand/rides` every 15–30s as baseline
- When Spare `requestStatus` and `eta` webhooks are wired up: backend pushes updates via the existing SSE stream pattern, frontend reflects changes instantly without full reload

---

## van-duties.html — Design Notes

### Layout
Full-width table, no map needed (van-map.html is the spatial view).

### Duty table columns
| Column | Notes |
|---|---|
| Driver | Name |
| Vehicle | Name / license plate |
| Shift | Start → End times |
| Status | scheduled / inProgress / completed / cancelled |
| Trips | X completed / Y total |
| Matching | Active / Paused badge |
| Lateness | Spare only |

### Action buttons per row
- **Pause / Resume Matching** (toggle) — `PATCH /api/ondemand/duties/{id}/matching`; Spare only
- **Cancel Shift** — confirmation dialog; Spare only

---

## Sitemap Updates

Add all three pages to `html/sitemap.html` under a new "Van Dispatch" group:
- Van Map (`/van-map`)
- Ride Board (`/van-rides`)
- Duty Roster (`/van-duties`)

---

## Auth Notes

All three pages and all new API endpoints use `_require_dispatcher_access()`. No new auth logic needed — same cookie mechanism as `dispatcher.html`.

---

## Implementation Order (suggested)

1. Add `GET /api/ondemand/rides` and `GET /api/ondemand/duties` (read-only, reshape existing data)
2. Build `van-rides.html` against those endpoints (table, filters, sort — no action buttons yet)
3. Build `van-map.html` (relatively quick, mostly a stripped-down map.html)
4. Build `van-duties.html`
5. Add write endpoints (all return 503/clear error for now since Spare not yet configured)
6. Wire action buttons in `van-rides.html` and `van-duties.html` to write endpoints (buttons visible, disabled until Spare key present)
7. When Spare API key is available: implement `_spare_client()`, flip write endpoints to call Spare, enable buttons

---

---

## Implementation Context (existing codebase patterns)

Everything below is how the current codebase works. Reference this when writing new code so it stays consistent.

### Adding a new auth-gated HTML page

1. Load HTML at startup near line ~1418:
   ```python
   VAN_MAP_HTML = _load_html("van-map.html")
   ```
2. Add a route that redirects to login if not authed (same pattern as `/dispatcher`):
   ```python
   @app.get("/van-map")
   async def van_map_page(request: Request):
       _refresh_dispatch_passwords()
       if _has_dispatcher_access(request):
           return HTMLResponse(VAN_MAP_HTML)
       return _login_redirect(request)
   ```
3. Add to sitemap (`html/sitemap.html`).

### Dispatcher auth

- `_require_dispatcher_access(request)` — raises `HTTPException(401)` if not authed. Use for JSON API endpoints.
- `_has_dispatcher_access(request)` — returns bool. Use for HTML page routes (so you can redirect instead of 401).
- `_login_redirect(request)` — redirects to `/login?return=<path>`. Always use this for HTML routes.
- Cookie name: `DISPATCH_COOKIE_NAME = "dispatcher_auth"` (line 563).
- No new auth logic needed — all three van pages and all write endpoints just call these helpers.

### TTLCache pattern

Used for all polling-based data. Lives in `app.py` around line 1556. Usage:
```python
spare_rides_cache = TTLCache(30)   # 30-second TTL

async def fetch_spare_rides_raw() -> List[Dict]:
    # ... httpx call ...
    return data

# In an endpoint or background task:
rides = await spare_rides_cache.get(fetch_spare_rides_raw)
```
Includes singleflight deduplication — safe to call from multiple concurrent requests.

### SSE broadcast pattern

To push real-time updates to van-map clients (e.g., from Spare webhooks):

```python
# Module level (~line 1483):
VAN_DISPATCH_SUBS: set[asyncio.Queue] = set()

def broadcast_van_dispatch(payload: dict) -> None:
    if not VAN_DISPATCH_SUBS:
        return
    encoded = f"data: {json.dumps(payload)}\n\n"
    for q in list(VAN_DISPATCH_SUBS):
        try:
            q.put_nowait(encoded)
        except asyncio.QueueFull:
            pass

# SSE endpoint:
@app.get("/api/ondemand/stream")
async def van_dispatch_stream(request: Request):
    _require_dispatcher_access(request)
    async def gen():
        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        VAN_DISPATCH_SUBS.add(q)
        try:
            # Send current state on connect
            current = await spare_rides_cache.get(fetch_spare_rides_raw)
            yield f"data: {json.dumps({'type': 'rides', 'data': current})}\n\n"
            while True:
                encoded = await q.get()
                yield encoded
        finally:
            VAN_DISPATCH_SUBS.discard(q)
    return StreamingResponse(gen(), media_type="text/event-stream")
```

When a Spare webhook event arrives (vehicleLocation, requestStatus, eta), call `broadcast_van_dispatch(...)` to push to all connected clients immediately.

### Existing on-demand data functions

These are already in `app.py` and should be reused/reshaped rather than replaced:

- `fetch_ondemand_rides(client, schedules)` (~line 2776) — returns `Dict[ride_id, ride_info]` from `ONDEMAND_RIDES_URL`.
- `fetch_ondemand_schedules(client)` (~line 2647) — returns list of vehicle schedule dicts, each containing `stops[]` → `rides[]`.
- `_collect_ondemand_data(client)` — runs both in parallel, returns `{"vehicles": [...], ...}` with rides embedded per stop.
- `_build_ondemand_payload(request)` — the full assembled payload served by `GET /api/ondemand`. Returns vehicles with positions, stop plans, next-stop targets.
- `build_ondemand_vehicle_stop_plans(schedules, ...)` (~line 3044) — builds ordered slot sequences per vehicle.

For `GET /api/ondemand/rides`, the simplest approach is to call `_collect_ondemand_data(client)` and flatten the nested rides out into a list, enriching with van/driver info from the vehicle entries.

### Spare client helper (to write when key is available)

Add near the other client init code:
```python
SPARE_API_KEY = (os.getenv("SPARE_API_KEY") or "").strip()
SPARE_API_BASE_URL = (os.getenv("SPARE_API_BASE_URL") or "https://api.us.sparelabs.com/v1").strip()

def _spare_headers() -> Optional[dict]:
    """Returns auth headers for Spare API, or None if not configured."""
    if not SPARE_API_KEY:
        return None
    return {"Authorization": f"Bearer {SPARE_API_KEY}", "Accept": "application/json"}
```

Write endpoints check `_spare_headers()` and return a clear error if None:
```python
@app.post("/api/ondemand/rides/{ride_id}/cancel")
async def cancel_ride(ride_id: str, request: Request):
    _require_dispatcher_access(request)
    headers = _spare_headers()
    if headers is None:
        raise HTTPException(status_code=503, detail="Dispatching requires Spare — SPARE_API_KEY not configured")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SPARE_API_BASE_URL}/requests/{ride_id}/cancellation",
            headers=headers
        )
        resp.raise_for_status()
        return resp.json()
```

### nav-bar.js

The shared nav bar at `scripts/nav-bar.js` auto-generates nav links. If it reads from a sitemap or link list, verify how it works before adding new pages — may need a new entry there too, not just `sitemap.html`.

---

## Open Questions

- Will paratransit and on-demand eventually be on the **same** Spare organization, or separate orgs? This affects whether `GET /api/ondemand/rides` returns both service types or needs separate endpoints.
- Should `van-rides.html` include paratransit trips (future), or only on-demand? Probably both eventually — could add a "Service Type" filter column.
- Manual ride creation (`POST /requests`) requires knowing service ID, zone, and time rules. Will need Spare org setup details before implementing the "New Ride" form.
- Should there be a mobile-friendly view for drivers? Probably out of scope for this iteration.
