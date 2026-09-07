// livemap/apps/vandispatch2/trip-board.js
// -----------------------------------------------------------------------------
// The "Active Trips" board — Spare ride requests + OnDemand ride cards in one
// chronological list. Ported from html/vandispatch.html's renderTrips(); the
// only change is that a card click frames that ride's pickup/drop-off on the
// map (fit-to-bounds) instead of drawing a route line, and card clicks are
// delegated off the list (data-* attributes) rather than inline onclick.
// -----------------------------------------------------------------------------

import {
  getSpareTrips,
  getSpareDuties,
  getVehicleData,
  getVanColor,
  getOdStops,
  getOdVehicles,
  computeStopOrderLookup,
  OD_ACTIVE,
} from './data.js';
import { frameTrip, applyHighlight } from './selection.js';
import {
  escHtml,
  svgIcon,
  fmtTime,
  TIME_24,
  formatPhone,
  formatLeadTime,
  rideIntentLabel,
  riderName,
  accessTags,
  stopOrderBadgeHtml,
} from './helpers.js';

const OD_STATUS_CLASS = {
  in_progress: 'inProgress',
  inProgress: 'inProgress',
  pending: 'accepted',
  scheduled: 'accepted',
  accepted: 'accepted',
};
const OD_STATUS_LABEL = {
  in_progress: 'In Progress',
  inProgress: 'In Progress',
  pending: 'Pending',
  scheduled: 'Scheduled',
  accepted: 'Accepted',
};

export function renderTrips() {
  const list = document.getElementById('trips-list');
  if (!list) return;
  const stamp = document.getElementById('trips-updated');
  if (stamp)
    stamp.textContent = new Date().toLocaleTimeString('en-US', { ...TIME_24, second: '2-digit' });

  const spareTrips = getSpareTrips();
  const spareDuties = getSpareDuties();
  const vehicleData = getVehicleData();
  const odStops = getOdStops();
  const odVehicles = getOdVehicles();

  const items = [];
  const tripSortTs = (req) =>
    (req.status === 'inProgress'
      ? req.dropoffEta || req.scheduledDropoffTs
      : req.pickupEta || req.scheduledPickupTs) || Infinity;

  // vehicleId -> driver name, from the duty roster.
  const spareDriverByVid = {};
  for (const d of spareDuties) {
    const vid = d.vehicleId || (d.vehicle && d.vehicle.id);
    const dn = [d.driver && d.driver.firstName, d.driver && d.driver.lastName]
      .filter(Boolean)
      .join(' ');
    if (vid && dn) spareDriverByVid[vid] = dn;
  }
  const stopOrder = computeStopOrderLookup();

  for (const req of spareTrips) {
    const name = riderName(req);
    const status = req.status || 'unknown';
    const lyftBrand =
      (req.serviceBrand && (req.serviceBrand.name || req.serviceBrand.externalName)) || '';
    const toLyft = req.isExternallyDispatched === true || /lyft/i.test(lyftBrand);
    const lyftOffered = toLyft && req.dispatchStatus === 'offeredToFleet';
    const pillText = lyftOffered ? 'offered to Lyft' : status;
    const lyftBadge = toLyft ? '<span class="source-badge source-badge--lyft">Lyft</span>' : '';
    const pickup = req.scheduledPickupAddress || req.requestedPickupAddress || '—';
    const dropoff = req.scheduledDropoffAddress || req.requestedDropoffAddress || '—';
    const pickedUp = Number.isFinite(req.pickupCompletedTs);
    const puLabel = pickedUp ? 'Picked up' : 'PU';
    const puEta = pickedUp
      ? fmtTime(req.pickupCompletedTs)
      : req.pickupEta
        ? fmtTime(req.pickupEta)
        : fmtTime(req.scheduledPickupTs);
    const doEta = req.dropoffEta ? fmtTime(req.dropoffEta) : fmtTime(req.scheduledDropoffTs);
    const notes = req.notes
      ? `<div class="addr trip-note" style="margin-top:.25rem">${svgIcon('note')} ${escHtml(
          req.notes,
        )}</div>`
      : '';
    const riders = req.numRiders ? ` · ${req.numRiders} rider${req.numRiders > 1 ? 's' : ''}` : '';
    const riderPhoneRaw =
      req.rider && req.rider.phoneNumber ? String(req.rider.phoneNumber).trim() : '';
    const phoneLine = riderPhoneRaw
      ? `<div class="addr trip-phone" style="font-size:.72rem;margin-bottom:.15rem">${svgIcon(
          'phone',
        )} <a href="tel:${escHtml(riderPhoneRaw.replace(/[^\d+]/g, ''))}">${escHtml(
          formatPhone(riderPhoneRaw),
        )}</a></div>`
      : '';
    const van = req.vehicleId ? vehicleData[req.vehicleId] || {} : null;
    const vanLabel = van ? van.identifier || 'Van' : null;
    const vanColor = van ? getVanColor(vanLabel, van.markerColor || '#E57200') : null;
    const vanDriver = req.vehicleId ? spareDriverByVid[req.vehicleId] : null;
    const vanLine = van
      ? `<div class="addr" style="color:#aaa;font-size:.72rem;margin-bottom:.15rem">${svgIcon(
          'van',
          vanColor,
        )} <span style="color:${vanColor};font-weight:600">${escHtml(vanLabel)}</span>${
          vanDriver ? ` · ${escHtml(vanDriver)}` : ''
        }</div>`
      : toLyft
        ? `<div class="addr" style="color:#EA0B8C;font-size:.72rem;margin-bottom:.15rem;font-weight:600">${escHtml(
            lyftBrand || 'Dispatched to Lyft',
          )}${lyftOffered ? ' — awaiting Lyft driver' : ''}</div>`
        : `<div class="addr" style="color:#666;font-size:.72rem;margin-bottom:.15rem;font-style:italic">Not yet assigned to a van</div>`;
    const bookedPickupTs = req.requestedPickupTs || req.scheduledPickupTs;
    const leadTime =
      req.createdAt && bookedPickupTs ? formatLeadTime(bookedPickupTs - req.createdAt) : null;
    const bookedLine = req.createdAt
      ? `<div class="addr" style="color:#777;font-size:.68rem;margin-top:.3rem">${svgIcon(
          'calendar',
        )} Booked ${leadTime ? `${leadTime} ahead` : `at ${fmtTime(req.createdAt)}`}</div>`
      : '';
    const intentLabel = rideIntentLabel(req);
    const intentLine = intentLabel
      ? `<div class="addr" style="color:#777;font-size:.68rem">${svgIcon('target')} ${escHtml(
          intentLabel,
        )}</div>`
      : '';
    const badgeColor = vanColor || '#E57200';
    const puBadge = stopOrderBadgeHtml(stopOrder[`${req.id}-Pickup`], badgeColor);
    const doBadge = stopOrderBadgeHtml(stopOrder[`${req.id}-Dropoff`], badgeColor);
    items.push({
      sortTs: tripSortTs(req),
      html: `<div class="trip-card ${status}" style="cursor:pointer" title="Click to frame this ride on the map" data-trip-kind="spare" data-trip-ref="${escHtml(
        req.id,
      )}" data-van-source="spare" data-vehicle-id="${escHtml(req.vehicleId || '')}">
        <div class="trip-header">
          <span class="status-pill ${status}">${escHtml(pillText)}</span>
          <span class="rider-name">${escHtml(name)}${riders}</span>
          <span style="margin-left:auto;display:inline-flex;gap:.3rem;align-items:center">${lyftBadge}<span class="source-badge source-badge--spare">Spare</span></span>
        </div>
        <div class="trip-body">
          ${vanLine}
          ${phoneLine}
          <div class="addr">${puBadge}▲ ${escHtml(pickup)}</div>
          <div class="addr">${doBadge}▼ ${escHtml(dropoff)}</div>
          <div class="trip-meta">
            <span><span class="eta-label">${puLabel}</span> <span class="eta-time">${puEta}</span></span>
            <span><span class="eta-label">DO</span> <span class="eta-time">${doEta}</span></span>
          </div>
          ${bookedLine}
          ${intentLine}
          ${notes}
          <div class="access-icons">${accessTags(req)}</div>
        </div>
      </div>`,
    });
  }

  // OnDemand: regroup per-stop rows into one card per ride.
  const odRides = new Map();
  for (const s of odStops) {
    const rec = (s.rides && s.rides[0]) || {};
    const rs = s.rideStatus || rec.rideStatus || '';
    if (!OD_ACTIVE.has(rs) && rs !== '') continue;
    const key = s.rideId || rec.rideId || `${s.vehicleId}|${(s.riders || []).join(',')}`;
    let r = odRides.get(key);
    if (!r) {
      const odv = odVehicles.find((x) => String(x.vehicleId) === String(s.vehicleId));
      r = {
        riders: (s.riders || []).slice(),
        vehicleId: s.vehicleId,
        vehicleName: s.vehicleName,
        vehicleColor: s.vehicleColor,
        driverName: (odv && odv.driverName) || '',
        status: rs || 'accepted',
        phone: '',
        pickupTs: null,
        dropoffTs: null,
        pickupAddr: '',
        dropoffAddr: '',
        pickupOrder: null,
        dropoffOrder: null,
      };
      odRides.set(key, r);
    }
    if (!r.riders.length && s.riders && s.riders.length) r.riders = s.riders.slice();
    if (rs) r.status = rs;
    if (!r.phone) r.phone = s.phone || rec.phone || '';
    if (s.stopType === 'pickup') {
      r.pickupAddr = r.pickupAddr || s.address;
      if (Number.isFinite(s.stopTs)) r.pickupTs = s.stopTs;
      if (Number.isFinite(s.order)) r.pickupOrder = s.order;
    }
    if (s.stopType === 'dropoff') {
      r.dropoffAddr = r.dropoffAddr || s.address;
      if (Number.isFinite(s.stopTs)) r.dropoffTs = s.stopTs;
      if (Number.isFinite(s.order)) r.dropoffOrder = s.order;
    }
    r.pickupAddr = r.pickupAddr || s.pickupAddress || rec.pickupAddress || '';
    r.dropoffAddr = r.dropoffAddr || s.dropoffAddress || rec.dropoffAddress || '';
  }
  for (const [rideKey, r] of odRides.entries()) {
    const cls = OD_STATUS_CLASS[r.status] || 'processing';
    const lbl = OD_STATUS_LABEL[r.status] || r.status || 'Active';
    const pickedUp = r.status === 'in_progress' || r.status === 'inProgress';
    const name = r.riders.join(', ') || 'Rider';
    const vanColor = r.vehicleColor || '#ec4899';
    const phoneRaw = r.phone ? String(r.phone).trim() : '';
    const phoneLine = phoneRaw
      ? `<div class="addr trip-phone" style="font-size:.72rem;margin-bottom:.15rem">${svgIcon(
          'phone',
        )} <a href="tel:${escHtml(phoneRaw.replace(/[^\d+]/g, ''))}">${escHtml(
          formatPhone(phoneRaw),
        )}</a></div>`
      : '';
    const puAddr = r.pickupAddr || (pickedUp ? 'Pickup complete' : '—');
    const puLabel = pickedUp ? 'Picked up' : 'PU';
    const puTime = r.pickupTs ? fmtTime(r.pickupTs) : pickedUp ? svgIcon('check') : '—';
    const doTime = r.dropoffTs ? fmtTime(r.dropoffTs) : '—';
    const puBadge = stopOrderBadgeHtml(r.pickupOrder, vanColor);
    const doBadge = stopOrderBadgeHtml(r.dropoffOrder, vanColor);
    const sortTs = (pickedUp ? r.dropoffTs : r.pickupTs) || r.pickupTs || r.dropoffTs || Infinity;
    items.push({
      sortTs,
      html: `<div class="trip-card ${cls}" style="cursor:pointer" title="Click to frame this ride on the map" data-trip-kind="od" data-trip-ref="${escHtml(
        rideKey,
      )}" data-van-source="od" data-vehicle-id="${escHtml(r.vehicleId || '')}">
        <div class="trip-header">
          <span class="status-pill ${cls}">${escHtml(lbl)}</span>
          <span class="rider-name">${escHtml(name)}</span>
          <span class="source-badge source-badge--w2w" style="margin-left:auto">OD</span>
        </div>
        <div class="trip-body">
          <div class="addr" style="color:#aaa;font-size:.72rem;margin-bottom:.15rem">${svgIcon(
            'van',
            vanColor,
          )} <span style="color:${vanColor};font-weight:600">${escHtml(
            r.vehicleName || 'Van',
          )}</span>${r.driverName ? ` · ${escHtml(r.driverName)}` : ''}</div>
          ${phoneLine}
          <div class="addr">${puBadge}▲ ${escHtml(puAddr)}</div>
          <div class="addr">${doBadge}▼ ${escHtml(r.dropoffAddr || '—')}</div>
          <div class="trip-meta">
            <span><span class="eta-label">${puLabel}</span> <span class="eta-time">${puTime}</span></span>
            <span><span class="eta-label">DO</span> <span class="eta-time">${doTime}</span></span>
          </div>
        </div>
      </div>`,
    });
  }

  items.sort((a, b) => a.sortTs - b.sortTs);
  const cards = items.map((i) => i.html);
  list.innerHTML = cards.length
    ? cards.join('')
    : '<div class="no-trips">No active trips right now</div>';
  applyHighlight(false);
}

let wired = false;
export function installTripBoard() {
  if (wired) return;
  wired = true;
  const list = document.getElementById('trips-list');
  if (!list) return;
  list.addEventListener('click', (e) => {
    const card = e.target.closest('.trip-card');
    if (!card || !card.dataset.tripKind) return;
    frameTrip(card.dataset.tripKind, card.dataset.tripRef || '');
  });
}
