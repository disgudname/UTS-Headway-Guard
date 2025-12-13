# Setting Up Bus Stop Approach Bubbles

Approach bubbles define the sequence a bus follows as it approaches and serves a stop. They are used to determine arrivals, passthroughs, departures, and dwell time. Each stop may have one or more sets of approach bubbles.

---

## How Events Are Logged

Understanding how events are logged is essential before configuring approach bubbles.

**Arrival (Stopped):**  
Logged when the bus passes through each bubble in order and then stops inside the final (highest-numbered) bubble.

**Arrival (Passthrough):**  
Logged when the bus passes through each bubble in order and then exits the final bubble without stopping.

**Departure:**  
Logged when the bus passes through each bubble in order (regardless of arrival type) and then exits the final bubble.

**Important behavior:**  
If a bus passes through a stop without stopping, this results in:

- An Arrival (Passthrough) event
- A Departure event
- Both events share the same timestamp
- The resulting dwell time is 0 seconds

---

## Accessing the Stop Approach Tool

Go to https://utsopsdashboard.com/stop-approach  
(Log in if prompted.)

Select the stop you want to configure:

- Click the purple stop marker on the map, or
- Select the stop from the dropdown list in the right-hand panel.

---

## Creating an Approach Bubble Set

In the right-hand panel, locate the Approach Bubbles section and click “+ Add Set”.

Multiple sets may be created for a single stop if buses approach from different directions (for example, Madison Ave @ Grady Ave).

Click “+ Click map to add bubble”.

Click on the map to place the first bubble.

This bubble should cover the area where the bus actually stops.

**Important:**  
TransLoc frequently “glues” a bus’s reported position to the stop marker. Even if the marker is not directly on the roadway, a stopped bus may appear at that location. Ensure the final bubble includes the stop marker area.

---

## Adding Earlier Approach Bubbles

Click “+ Click map to add bubble” again.

Click on the map to place the next bubble.

The newly added bubble becomes Bubble #1, and existing bubbles are renumbered automatically.

Place earlier bubbles where buses naturally slow or pass through, such as:

- Stop signs
- Traffic lights
- Turns
- Lane merges or curves

Repeat as needed to create a logical approach sequence.

Bubbles may be placed anywhere, including before the final bubble of another set.

---

## Adjusting Bubble Size and Position

Adjust the radius of each bubble using the size field in the bubble list.

Larger bubbles are generally more reliable.

70 meters or larger is recommended when possible.

GPS updates can be sporadic, especially on older vehicles.

Adjust the position of a bubble by dragging it using the numbered center marker.

---

## Saving Your Changes (Required)

After making any changes, click “Save stop settings” at the bottom of the right-hand panel.

**Important:**  
Changes are not saved automatically.  
You must click “Save stop settings” before:

- Closing the page
- Selecting a different stop
- Navigating away from the tool

Unsaved changes will be lost.

---

## Placement Considerations

Be mindful of stop signs, traffic lights, or other features that may cause a bus to stop in the final bubble before reaching the stop.

When possible, place one edge of the final bubble near the downstream end of the stop, so passthroughs are logged promptly.

If a stop is very close to an intersection, it is acceptable - and often preferable - to position the final bubble so it does not cover the intersection.

---

## Testing and Validation

After saving a bubble set, it should be validated using live vehicle data.

Open https://utsopsdashboard.com/map?bubbles=true.

This view shows buses interacting with approach bubbles in real time.

Observe buses approaching and serving the stop:

- Confirm that bubbles are entered in the expected order.
- Verify arrivals, passthroughs, and departures occur at the correct locations.

**Live update behavior:**

Changes to bubble sets take effect immediately after saving.

However, if a bus is already interacting with a bubble set when changes are made:

- The updated configuration will not apply to that bus mid-sequence.
- The changes will take effect the next time a bus enters the first bubble in that set.

---

## Notes & Tips

- Larger bubbles reduce missed detections caused by GPS jitter or infrequent location updates.
- Avoid placing the final bubble where buses routinely stop for reasons unrelated to serving the stop.
- Use multiple approach sets when a stop is served from distinct directions or road geometries.
- Bubble order matters: buses must pass through the bubbles in sequence for events to be logged correctly.
