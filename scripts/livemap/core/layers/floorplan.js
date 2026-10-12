// livemap/core/layers/floorplan.js
// -----------------------------------------------------------------------------
// The 2018 floor plan of Parking & Transit (1101 Millmont St), drawn inside the
// building when you zoom in. Dispatchers only: the plan comes from
// GET /v1/livemap/floorplan, which needs the dispatcher cookie, and is fetched
// once after sign-in. The layers and the GeoJSON source are baked into the style
// doc (see floorplan-style.js); this module only setData()s the source and
// re-applies it after a theme swap.
// -----------------------------------------------------------------------------

import { API_BASE } from '../config.js';
import { getMap, onStyleReady } from '../map.js';
import { onDispatcher, startSession } from '../data/session.js';
import { FLOORPLAN_SOURCE_ID } from './floorplan-style.js';

const URL = `${API_BASE}/v1/livemap/floorplan`;
const EMPTY = { type: 'FeatureCollection', features: [] };

let plan = null; // the fetched FeatureCollection
let show = false;
let requested = false;

export function installFloorplanLayer() {
  startSession(); // idempotent; makes sure the auth probe is running
  onStyleReady(push);
  onDispatcher((isDisp) => {
    show = isDisp;
    if (show && !requested) load();
    push();
  });
}

async function load() {
  requested = true;
  try {
    const r = await fetch(URL, { cache: 'no-store' });
    if (!r.ok) return; // 404: no plan on this server
    plan = await r.json();
    push();
  } catch {
    requested = false; // network hiccup: try again at the next sign-in check
  }
}

function push() {
  const map = getMap();
  const src = map && map.getSource(FLOORPLAN_SOURCE_ID);
  if (src) src.setData(show && plan ? plan : EMPTY);
}
