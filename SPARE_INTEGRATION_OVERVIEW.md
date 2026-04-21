# Spare Integration — Overview for Management

## What is Spare?

Spare is the software platform UTS has contracted to coordinate UVA FlexRide, our new in-house paratransit service (previously operated under the DART — Demand and Response Transportation — name). Drivers use a Spare app on department-provided tablets to receive trip assignments, navigate to passengers, and mark pickups and dropoffs complete. Riders (or staff booking on their behalf) request trips through Spare's system.

Spare also exposes an **API** — a structured data feed that allows other software systems to read information from Spare and send instructions back to it. Think of it like a two-way pipe: we can pull live data out of Spare to display on our operations dashboard, and we can push actions back in (like cancelling a trip or reassigning a driver) without anyone having to log into Spare's own interface.

---

## What Data Spare Can Share With Us

Because of this API, our operations dashboard will be able to display:

**Live vehicle locations**
Spare sends us a GPS position for every van every few seconds while a driver is on shift — including which direction the van is heading. This lets us show FlexRide vans on the same map our dispatchers already use for fixed-route buses.

**Trip status in real time**
Every paratransit trip moves through a series of stages: scheduled → driver en route to pickup → driver arrived at pickup → passenger on board → completed. Spare notifies us the moment any trip changes status, so our board stays current without anyone manually updating it.

**Estimated arrival times**
Spare continuously recalculates when each driver will reach a passenger's pickup address and when they'll complete the dropoff. If a driver is running late, those estimates update automatically and we see it immediately.

**The full trip list**
We can pull a list of all trips scheduled for today (or any date range), including passenger name, pickup and dropoff addresses, scheduled times, any special needs (wheelchair accessibility, etc.), and notes for the driver.

**Driver shifts**
We can see which drivers are on shift right now, what vehicle each is driving, how many trips they've been assigned, and whether they're running on time or behind.

---

## What We're Building

We are currently developing three new pages in the UTS Operations Dashboard specifically for FlexRide dispatch. All three require a dispatcher login — the same credentials already used for existing dispatcher tools.

### Van Dispatch (main page)

This is the primary screen a FlexRide dispatcher would have open throughout their shift. The left side of the screen shows a live map; the right side shows a list of today's trips. Both update automatically as things change — no refreshing required.

**On the map:** Every FlexRide van appears as a moving marker, updating in real time as drivers navigate their routes. A dispatcher can click on any van to see a quick summary — who's driving, what vehicle, and the sequence of upcoming pickups and dropoffs still ahead of them.

**In the trip list:** Every trip for the day is listed with its current status (color-coded — grey for scheduled, blue for a driver en route, yellow for driver on-site, green for passenger on board), the passenger's name, pickup and dropoff addresses, scheduled times, and live estimated arrival times. Special needs like wheelchair accessibility are flagged visibly so nothing gets missed.

**Cross-linking:** Clicking on a trip in the list highlights that van on the map. Clicking on a van on the map highlights its trips in the list. The two panels always stay in sync.

### What Dispatchers Can Do

A significant part of what makes this tool useful is that dispatchers can take action directly from the dashboard — without logging into a separate system, making a phone call, or waiting for someone else to make a change. Every action includes a confirmation step to prevent mistakes.

**Edit driver notes**
Dispatchers can add or update the instructions that appear on a driver's tablet for any trip. This is useful for communicating late-breaking information — a passenger who needs extra time to board, a note about a building entrance, a heads-up that a rider uses a wheelchair that wasn't recorded at booking. The driver sees the updated note immediately on their tablet.

**Cancel a trip**
A trip can be cancelled directly from the dashboard — for example, if a rider calls in to say they no longer need the ride, or if a situation arises that makes the trip impossible to fulfill. Cancelling through the dashboard updates Spare immediately, so the driver's tablet reflects the change and the time slot is freed for other assignments.

**Reassign a trip to a different van**
If a vehicle breaks down, a driver falls significantly behind schedule, or a trip needs to move to a different vehicle for any reason, dispatchers can request that Spare reassign the trip to another available van. Spare's system handles the logistics of finding the best match given what's available.

**Manually create a trip**
For passengers who call in rather than booking online, dispatchers can enter a new trip directly from the dashboard — passenger name, pickup address, dropoff address, and requested time — and Spare will assign it to an appropriate driver automatically.

**Pause a driver's new trip assignments**
If a driver needs to be temporarily taken out of rotation — handling a difficult passenger situation, dealing with a vehicle issue, or needing a break — a dispatcher can pause new trip assignments for that driver without ending their shift entirely. New trips won't be sent to their tablet until matching is resumed. This can be done from the shift roster page.

**Schedule a driver break**
A dispatcher can schedule a break for a driver within a specified time window. Spare fits the break into the driver's route automatically, scheduling it between trips so service disruption is minimized.

**Insert an immediate break**
For situations that can't wait — a driver feeling unwell, a vehicle issue, or anything requiring a stop right away — a dispatcher can request an immediate break. When doing so, they choose how to handle any passengers currently assigned to that driver: hold off until the driver finishes their current dropoffs, or reassign those passengers to other available vans immediately. Spare handles the reassignment automatically.

Breaks are logged with a reason category — routine break, vehicle maintenance, driver medical, rider incident, and so on. This means the break log functions as a record of what happened and why, which may be useful for reporting and compliance purposes.

**End a shift early**
If a driver needs to go off duty before their scheduled end time, their shift can be closed out from the dashboard.

Fixed-route buses can optionally be overlaid on the same map for supervisors who want a full fleet picture, but they're hidden by default to keep the view focused on FlexRide.

### Trip Board (detail view)

A full-screen version of the trip list for situations that require more depth — reviewing all trips across a wider time window, filtering by a specific driver or vehicle, sorting by lateness, or preparing for a debrief at the end of a day. All of the same dispatcher actions are available here. Less focused on moment-to-moment dispatch, more useful for planning and review.

### Shift Roster

A table listing all FlexRide driver shifts for the day: driver name, assigned vehicle, shift start and end times, number of trips assigned, trips completed, and whether automatic trip-matching is active or paused. This gives supervisors a quick read on how the operation is running at any given moment — who is on, what they're carrying, and whether anyone is falling behind.

---

## Development Status and Testing

Development on these pages is beginning now. The pages are being built against Spare's published specifications, so the interface will be complete and ready to connect as soon as FlexRide goes live.

Because DART/FlexRide is not yet operational, live testing — confirming that van locations update correctly, that trip status changes come through in real time, that dispatcher actions take effect as expected — will need to happen once Spare is actively in use for the service. The interface itself can be built and reviewed in full before then; it's the live data connection that has to wait for service launch.

---

## What If On-Demand Moves to Spare Too?

UTS currently operates an overnight on-demand van service on a separate system. There is a possibility that this service could also migrate to Spare in the future. If that happens, the FlexRide dispatcher tools described above would extend to cover **both** services in the same interface — no separate screens, no additional development work needed.

In practice that would mean:

- **One map** showing both FlexRide and on-demand vans together, with fixed-route buses available as an optional overlay
- **One trip list** covering both services, with a label on each trip indicating which service it belongs to and filters to focus on one or the other
- **The same set of dispatcher actions** available for on-demand trips that are available for FlexRide — editing notes, cancelling trips, reassigning vehicles, creating new trips, pausing a driver's assignments

For dispatchers, this would mean managing the entire van operation from one screen.

No commitment to that migration has been made — this is simply to note that the groundwork is already being laid for it, and if the decision is made to move on-demand to Spare, the operational tools will be ready.
