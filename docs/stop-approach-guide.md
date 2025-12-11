# Stop Approach Editor: approach bubble setup

Use the Stop Approach Editor (`/stop-approach`) to define the bubbles that detect vehicles as they near a stop. Follow the steps below in order; button labels match what you see on the page.

## 1) Load stops and pick your location
- Open the Stop Approach Editor from the sitemap or go directly to `/stop-approach`.
- Click **Reload stops** if the stop list might be out of date, then choose the stop in the **Stop** dropdown. The map will fly to that stop and show linked routes.

## 2) Create an approach set
- Click **+ Add Set** to start a configuration for the travel direction you are working on (for example, “Northbound”).
- Rename the set in the name box as needed. Use one set per direction.

## 3) Place bubbles in the correct order (start at the stop, work outward)
- Press **+ Click map to add bubble**, then click on the **stop first** to drop Bubble #1. Each new bubble you add becomes the new **#1** and pushes the rest down the list.
- After placing the first bubble on the stop, keep adding bubbles **moving away from the stop in the direction the bus approaches**. This preserves the intended sequence the system uses when deciding whether a vehicle is approaching.

## 4) Size and spacing guidelines
- Bubble radius: adjust in the number box beside each bubble, or tap the **25/40/50** presets that match typical approach speeds. Drag the circle on the map if you need to reposition it.
- Final bubble: place one edge as close to the back end of the stop as possible. If a bus stops, it will remain inside the bubble; if it keeps rolling, it exits the final bubble quickly so headway alerts advance.
- Overlap: with **only two bubbles**, avoid overlapping them. An overlap can let a bus heading the wrong way sit in both bubbles at once and trigger both stages. With three or more bubbles, light overlap is acceptable as long as the order still reflects the path a bus actually travels.

## 5) Clean up or reorder
- Drag the list handle (≰ icon) to reorder bubbles if you need to fix the sequence; numbers update automatically.
- Use the **✕** button next to a bubble to delete it. Use **Delete** beside the set name to remove an entire set.

## 6) Save and reset
- Click **Save stop settings** to publish your changes. The badge beside “Approach Bubbles” shows how many bubbles exist across all sets.
- **Reset all to defaults** restores every stop to the system defaults if you need a clean slate.
