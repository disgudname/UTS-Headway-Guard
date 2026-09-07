// livemap/apps/vandispatch2/duty-roster.js
// -----------------------------------------------------------------------------
// The "Duty Roster — Today" panel — Spare duties cross-referenced with the
// WhenToWork FlexRide / OnDemand shifts. Ported from html/vandispatch.html's
// renderDuties() + buildDutyCard() + the expandable itinerary. Clicking a card
// selects that van (highlight + frame), exactly like clicking its map marker.
// -----------------------------------------------------------------------------

import {
  getSpareDuties,
  getW2WShifts,
  getVehicleData,
  getOdVehicles,
  getOdStops,
  getVanColor,
  computeStopOrderLookup,
  computeVehicleStopGroups,
  OD_ACTIVE,
} from './data.js';
import {
  selectVan,
  applyHighlight,
  onSelectionChange,
  getSelected,
} from './selection.js';
import {
  escHtml,
  svgIcon,
  fmtTime,
  TIME_24,
  formatPhone,
  normalizeName,
  windowsOverlapWithSlack,
  stopOrderBadgeHtml,
} from './helpers.js';

const STATUS_LABELS = {
  inProgress: 'In Progress',
  scheduled: 'Scheduled',
  completed: 'Completed',
  cancelled: 'Cancelled',
};
const humanizeStatus = (s) => STATUS_LABELS[s] || s || '—';

// --- expandable itinerary ------------------------------------------
function buildItineraryRows(source, vehicleId) {
  if (!vehicleId) return '';
  const rowHtml = (order, color, isPickup, name, ts, address, phone) => {
    const ph = phone ? String(phone).trim() : '';
    return `<div class="itin-row">
      <div class="itin-line">
        ${stopOrderBadgeHtml(order, color)}<span>${isPickup ? '▲' : '▼'}</span>
        <span class="itin-name">${escHtml(name || 'Rider')}</span>
        ${
          ph
            ? `<a class="itin-phone" href="tel:${escHtml(ph.replace(/[^\d+]/g, ''))}">${escHtml(
                formatPhone(ph),
              )}</a>`
            : ''
        }
        ${ts ? `<span class="itin-time">${fmtTime(ts)}</span>` : ''}
      </div>
      <div class="itin-addr">${escHtml(address || '—')}</div>
    </div>`;
  };

  const rows = [];
  if (source === 'spare') {
    const v = getVehicleData()[vehicleId] || {};
    const color = getVanColor(v.identifier || 'Van', v.markerColor || '#E57200');
    const lookup = computeStopOrderLookup();
    const groups = computeVehicleStopGroups()[vehicleId] || [];
    groups.forEach((g) => {
      g.stops.forEach((s) =>
        rows.push(
          rowHtml(
            lookup[`${s.requestId}-${s.kind}`],
            color,
            s.kind === 'Pickup',
            s.name,
            s.ts,
            s.address,
            s.phone,
          ),
        ),
      );
    });
  } else {
    getOdStops()
      .filter((s) => {
        if (String(s.vehicleId) !== String(vehicleId)) return false;
        const rs = s.rideStatus || (s.rides && s.rides[0] && s.rides[0].rideStatus) || '';
        return OD_ACTIVE.has(rs) || rs === '';
      })
      .slice()
      .sort((a, b) => (a.order || 0) - (b.order || 0))
      .forEach((s) =>
        rows.push(
          rowHtml(
            s.order,
            s.vehicleColor || '#ec4899',
            s.stopType === 'pickup',
            (s.riders || []).join(', '),
            Number.isFinite(s.stopTs) ? s.stopTs : null,
            s.address,
            s.phone || (s.rides && s.rides[0] && s.rides[0].phone) || '',
          ),
        ),
      );
  }

  if (!rows.length) return '<div class="itin-empty">No remaining stops for this van</div>';
  return `<div class="itin-head">Itinerary — ${rows.length} stop${
    rows.length > 1 ? 's' : ''
  }</div>${rows.join('')}`;
}

/** Fill (or clear) one duty card's itinerary box. */
export function updateDutyItinerary(card, expanded) {
  const box = card.querySelector('.duty-itinerary');
  if (!box) return;
  const rows = expanded
    ? buildItineraryRows(card.dataset.vanSource, card.dataset.vehicleId || '')
    : '';
  if (!rows) {
    box.hidden = true;
    box.innerHTML = '';
    return;
  }
  box.innerHTML = rows;
  box.hidden = false;
}

/** Rebuild every visible duty card's itinerary against the current selection. */
function syncItineraries() {
  document.querySelectorAll('#duties-list .duty-card').forEach((el) => {
    updateDutyItinerary(el, el.classList.contains('card-selected'));
  });
}

// --- one duty card (ported buildDutyCard) -------------------------
function buildDutyCard(r) {
  const srcBadge =
    r.source === 'spare'
      ? `<span class="source-badge source-badge--spare">Spare</span>${
          r.detail
            ? ` <span style="color:var(--text-faint);font-size:.7rem">${escHtml(r.detail)}</span>`
            : ''
        }`
      : `<span class="source-badge source-badge--w2w">${
          r.odActive ? 'OD' : 'W2W'
        }</span>${
          r.detail
            ? ` <span style="color:var(--text-faint);font-size:.7rem">${escHtml(r.detail)}</span>`
            : ''
        }`;
  const statusPill =
    r.source === 'spare'
      ? r.onBreakSince
        ? `<span class="duty-status onBreak">On break</span>`
        : `<span class="duty-status ${r.status}">${escHtml(humanizeStatus(r.status))}</span>`
      : r.odActive
        ? `<span class="duty-status inProgress">Active</span>`
        : `<span class="duty-status scheduled" style="background:#0a4a4d;color:#C9F2F4">Shift</span>`;
  const assignLine = r.vanColor
    ? `${svgIcon('van', r.vanColor)} <span style="color:${r.vanColor};font-weight:600">${escHtml(
        r.assignment,
      )}</span>`
    : r.source === 'w2w'
      ? `<span style="color:var(--accent-label)">${escHtml(r.assignment)}</span>`
      : escHtml(r.assignment);

  const timeLines = [];
  if (r.shiftStr)
    timeLines.push(
      `<span><span class="eta-label">Shift</span> <span class="eta-time">${escHtml(
        r.shiftStr,
      )}</span></span>`,
    );
  if (r.dutyStr)
    timeLines.push(
      `<span><span class="eta-label">Duty</span> <span class="eta-time">${escHtml(
        r.dutyStr,
      )}</span></span>`,
    );

  let onTimeLine = '';
  if (r.lateness != null) {
    const lateMin = Math.round(r.lateness / 60);
    onTimeLine =
      lateMin > 1
        ? `<div class="addr" style="color:#ffb74d;font-size:.7rem;margin-top:.3rem">${svgIcon(
            'clock',
          )} ${lateMin} min late</div>`
        : `<div class="addr" style="color:#81c784;font-size:.7rem;margin-top:.3rem">${svgIcon(
            'check',
          )} On time</div>`;
  }

  const interruptions = [];
  if (r.hasDriverConflict) interruptions.push('Driver conflict');
  if (r.hasVehicleConflict) interruptions.push('Vehicle conflict');
  if (r.cancellationDetails) interruptions.push('Cancelled');
  if (r.onBreakSince) {
    const elapsedM = Math.max(0, Math.round((Date.now() / 1000 - r.onBreakSince) / 60));
    const capM = r.breakLenSec ? ` / ${Math.round(r.breakLenSec / 60)}m` : '';
    interruptions.push(`On break ${elapsedM}m${capM}`);
  }
  if (r.breakSec > 60) interruptions.push(`Break ${Math.round(r.breakSec / 60)}m`);
  if (r.pauseSec > 60) interruptions.push(`Paused ${Math.round(r.pauseSec / 60)}m`);
  const interruptLine = interruptions.length
    ? `<div class="access-icons">${interruptions
        .map(
          (t) =>
            `<span class="access-tag" style="background:#3a1f1f;color:#ffab91">${svgIcon(
              'warn',
            )} ${escHtml(t)}</span>`,
        )
        .join('')}</div>`
    : '';

  return `<div class="duty-card ${r.status}${
    r.onBreakSince ? ' onBreak' : ''
  }" title="Click to show this van on the map" data-van-source="${
    r.source === 'spare' ? 'spare' : 'od'
  }" data-vehicle-id="${escHtml(r.vehicleId || '')}" data-driver="${escHtml(r.driverNameNorm || '')}">
    <div class="trip-header">
      <span class="rider-name">${escHtml(r.driverName)}</span>
      ${statusPill}
      <span style="margin-left:auto">${srcBadge}</span>
    </div>
    <div class="trip-body">
      <div class="addr">${assignLine}</div>
      <div class="trip-meta">${timeLines.join('')}</div>
      ${onTimeLine}
      ${interruptLine}
    </div>
    <div class="duty-itinerary" hidden></div>
  </div>`;
}

// --- render (ported renderDuties) --------------------------------
export function renderDuties() {
  const list = document.getElementById('duties-list');
  if (!list) return;

  const spareDuties = getSpareDuties();
  const w2wShifts = getW2WShifts();
  const vehicleData = getVehicleData();
  const odVehicles = getOdVehicles();

  const spareRows = spareDuties.map((d) => {
    const driver = d.driver || {};
    const driverName = [driver.firstName, driver.lastName].filter(Boolean).join(' ') || '—';
    const vid = d.vehicleId || (d.vehicle && d.vehicle.id);
    const vehName =
      (d.vehicle && d.vehicle.identifier) ||
      (vid && vehicleData[vid] && vehicleData[vid].identifier) ||
      '—';
    const vObj = vid ? vehicleData[vid] || {} : {};
    const metrics = d.metrics || {};
    return {
      source: 'spare',
      startMs: (d.startRequestedTs || 0) * 1000,
      startTs: d.startRequestedTs || 0,
      endTs: d.endRequestedTs || 0,
      driverName,
      driverNameNorm: normalizeName(driverName),
      assignment: vehName,
      vanColor: vehName !== '—' ? getVanColor(vehName, vObj.markerColor || '#E57200') : null,
      status: d.status || 'unknown',
      vehicleId: vid || '',
      dutyStr: `${fmtTime(d.startRequestedTs)} – ${fmtTime(d.endRequestedTs)}`,
      shiftStr: null,
      detail: d.identifier || '',
      lateness: Number.isFinite(d.lateness) ? d.lateness : null,
      hasDriverConflict: !!d.hasDriverConflict,
      hasVehicleConflict: !!d.hasVehicleConflict,
      cancellationDetails: d.cancellationDetails || null,
      breakSec: Number.isFinite(metrics.totalBreakLengthS) ? metrics.totalBreakLengthS : 0,
      pauseSec: Number.isFinite(metrics.totalInProgressPauseLengthS)
        ? metrics.totalInProgressPauseLengthS
        : 0,
      onBreakSince:
        d.activeBreak && Number.isFinite(d.activeBreak.startedTs) ? d.activeBreak.startedTs : null,
      breakLenSec:
        d.activeBreak && Number.isFinite(d.activeBreak.breakLength)
          ? d.activeBreak.breakLength
          : null,
    };
  });

  const flexRideShifts = w2wShifts.filter((s) => s.position_name === 'FlexRide Driver');
  const consumedShifts = new Set();
  for (const row of spareRows) {
    const match = flexRideShifts.find(
      (s) =>
        !consumedShifts.has(s) &&
        normalizeName(s.name) === row.driverNameNorm &&
        windowsOverlapWithSlack(
          row.startTs,
          row.endTs,
          (s.start_ts || 0) / 1000,
          (s.end_ts || 0) / 1000,
          3600,
        ),
    );
    if (match) {
      consumedShifts.add(match);
      row.shiftStr = `${fmtTime(match.start_ts / 1000)} – ${fmtTime(match.end_ts / 1000)}`;
    }
  }

  const w2wRows = w2wShifts
    .filter((s) => !consumedShifts.has(s))
    .map((s) => {
      const nameNorm = normalizeName(s.name);
      const odVan = odVehicles.find(
        (v) => v.driverName && normalizeName(v.driverName) === nameNorm,
      );
      return {
        source: 'w2w',
        startMs: s.start_ts || 0,
        driverName: s.name || '—',
        driverNameNorm: nameNorm,
        assignment: odVan ? odVan.label : s.position_name || 'UVA Ride',
        vanColor: odVan ? odVan.color : null,
        vehicleId: odVan ? odVan.vehicleId : '',
        odActive: !!odVan,
        status: odVan ? 'inProgress' : 'shift',
        shiftStr: `${fmtTime(s.start_ts / 1000)} – ${fmtTime(s.end_ts / 1000)}`,
        dutyStr: null,
        detail: odVan && odVan.rideCount ? `${odVan.rideCount} ride${odVan.rideCount > 1 ? 's' : ''}` : '',
        lateness: null,
        hasDriverConflict: false,
        hasVehicleConflict: false,
        cancellationDetails: null,
        breakSec: 0,
        pauseSec: 0,
      };
    });

  const rows = [...spareRows, ...w2wRows];
  const stamp = document.getElementById('duties-updated');
  if (stamp)
    stamp.textContent = new Date().toLocaleTimeString('en-US', { ...TIME_24, second: '2-digit' });

  if (!rows.length) {
    list.innerHTML = '<div class="no-duties">No shifts or duties today</div>';
    return;
  }

  rows.sort((a, b) => a.startMs - b.startMs);
  list.innerHTML = rows.map(buildDutyCard).join('');
  applyHighlight(false);
  syncItineraries();
}

let wired = false;
export function installDutyRoster() {
  if (wired) return;
  wired = true;
  const list = document.getElementById('duties-list');
  if (!list) return;
  list.addEventListener('click', (e) => {
    const card = e.target.closest('.duty-card');
    if (!card) return;
    if (card.dataset.vanSource === 'spare') {
      selectVan('spare', card.dataset.vehicleId || null);
    } else {
      selectVan('od', card.dataset.vehicleId || null, card.dataset.driver || null);
    }
  });
  // When the selection changes (from a map click, a trip card, or here),
  // expand/collapse the matching duty card's itinerary.
  onSelectionChange(() => syncItineraries());
}
