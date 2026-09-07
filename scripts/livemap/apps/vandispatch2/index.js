// livemap/apps/vandispatch2/index.js
// -----------------------------------------------------------------------------
// Wires the vandispatch2 dispatcher panels together: the polling data layer,
// the Active Trips board, the Duty Roster, and the van <-> card selection sync.
// Called once from apps/vandispatch2.js after the shared map + layers are up.
// -----------------------------------------------------------------------------

import { startVandispatchData, onChange, isWebhookConfigured } from './data.js';
import { installSelection } from './selection.js';
import { installTripBoard, renderTrips } from './trip-board.js';
import { installDutyRoster, renderDuties } from './duty-roster.js';

export function startVandispatchPanels(map) {
  installSelection(map);
  installTripBoard();
  installDutyRoster();

  onChange(() => {
    renderTrips();
    renderDuties();
    const el = document.getElementById('map-no-webhooks');
    if (el) el.style.display = isWebhookConfigured() ? 'none' : 'block';
  });

  startVandispatchData();
}
