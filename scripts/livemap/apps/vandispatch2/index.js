// livemap/apps/vandispatch2/index.js
// -----------------------------------------------------------------------------
// Wires the vandispatch2 dispatcher panels + map overlays together: the polling
// data layer, the Active Trips board, the Duty Roster, the van <-> card
// selection sync, and the map overlays (pickup/drop-off markers, service-area
// outline, PulsePoint, route polylines). Called once from apps/vandispatch2.js
// after the shared map + van layer are up.
// -----------------------------------------------------------------------------

import { startVandispatchData, onChange, isWebhookConfigured } from './data.js';
import { installSelection } from './selection.js';
import { installTripBoard, renderTrips } from './trip-board.js';
import { installDutyRoster, renderDuties } from './duty-roster.js';
import { startMapOverlays } from './map-overlays.js';

export function startVandispatchPanels(map) {
  installSelection(map);
  installTripBoard();
  installDutyRoster();
  startMapOverlays(map);

  onChange(() => {
    renderTrips();
    renderDuties();
    const el = document.getElementById('map-no-webhooks');
    if (el) el.style.display = isWebhookConfigured() ? 'none' : 'block';
  });

  startVandispatchData();
}
