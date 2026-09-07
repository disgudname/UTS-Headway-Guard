// livemap/apps/vandispatch2/helpers.js
// -----------------------------------------------------------------------------
// Pure formatting / small helper functions for the vandispatch2 panels, ported
// close to verbatim from the Leaflet html/vandispatch.html so the two pages
// present trips and duties identically. Nothing here touches the map.
// -----------------------------------------------------------------------------

export function escHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Small inline stroke SVGs — the panels used to lean on emoji for these labels.
// Sized to 1em so they sit inline with their text; stroke inherits `color`.
export function svgIcon(name, color) {
  const body =
    {
      van: '<path d="M1 11.5V4.5h8.5l3.5 3.5v3.5"/><path d="M1 11.5h13.5"/><circle cx="4.3" cy="11.7" r="1.6"/><circle cx="11.2" cy="11.7" r="1.6"/>',
      phone:
        '<path d="M3.5 2.2 6 2l1 3-1.4 1a8.5 8.5 0 0 0 3.4 3.4L10 9l3 1-.2 2.5c-.1.6-.6 1-1.2 1C6.4 13.2 2.8 9.6 2.3 4.4c-.1-.6.3-1.1.9-1.2z"/>',
      note: '<path d="M3.5 1.5h6L12.5 4.5v10h-9z"/><path d="M9 1.5V5h3.5"/><path d="M5.5 8h5M5.5 10.5h5M5.5 13h3"/>',
      calendar:
        '<rect x="2" y="3" width="12" height="11" rx="1.5"/><path d="M2 6.5h12M5.5 1.5v3M10.5 1.5v3"/>',
      target: '<circle cx="8" cy="8" r="5.5"/><circle cx="8" cy="8" r="2.3"/>',
      clock: '<circle cx="8" cy="8" r="6"/><path d="M8 4.5V8l2.6 1.8"/>',
      check: '<path d="M3 8.5 6.2 12 13 4.5"/>',
      warn: '<path d="M8 2 14.5 13.5h-13z"/><path d="M8 6.5v3.6M8 12v.05"/>',
    }[name] || '';
  return `<svg viewBox="0 0 16 16" width="1em" height="1em" fill="none" stroke="${
    color || 'currentColor'
  }" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-0.15em;flex-shrink:0" aria-hidden="true">${body}</svg>`;
}

// YIQ perceived-brightness black/white pick, threshold 150 (matches testmap's
// contrastBW — a touch higher than 128 for medium colours like orange).
export function contrastColor(hex) {
  if (typeof hex !== 'string') return '#ffffff';
  const h = hex.replace('#', '');
  if (h.length !== 6 && h.length !== 3) return '#ffffff';
  let r;
  let g;
  let b;
  if (h.length === 3) {
    r = parseInt(h[0] + h[0], 16);
    g = parseInt(h[1] + h[1], 16);
    b = parseInt(h[2] + h[2], 16);
  } else {
    r = parseInt(h.slice(0, 2), 16);
    g = parseInt(h.slice(2, 4), 16);
    b = parseInt(h.slice(4, 6), 16);
  }
  const yiq = (r * 299 + g * 587 + b * 114) / 1000;
  return yiq >= 150 ? '#000000' : '#ffffff';
}

// 24-hour clock everywhere (h23, zero-padded).
export const TIME_24 = { hour: '2-digit', minute: '2-digit', hourCycle: 'h23' };
export function fmtTime(ts) {
  if (!ts) return '—';
  return new Date(ts * 1000).toLocaleTimeString('en-US', TIME_24);
}

// Normalise a rider-typed phone number to plain dashes; keep a leading US "1-".
export function formatPhone(raw) {
  const s = String(raw ?? '').trim();
  const digits = s.replace(/\D/g, '');
  if (digits.length === 10) return `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits[0] === '1')
    return `1-${digits.slice(1, 4)}-${digits.slice(4, 7)}-${digits.slice(7)}`;
  return s;
}

// "45m", "2h 10m", "3d 4h" — how far ahead of the ride it was booked.
export function formatLeadTime(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return null;
  const mins = Math.round(seconds / 60);
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  const remMins = mins % 60;
  if (hrs < 24) return remMins ? `${hrs}h ${remMins}m` : `${hrs}h`;
  const days = Math.floor(hrs / 24);
  const remHrs = hrs % 24;
  return remHrs ? `${days}d ${remHrs}h` : `${days}d`;
}

// Spare intentType -> a rider-facing "Leave at" / "Arrive by" line.
export function rideIntentLabel(req) {
  if (req.intentType === 'arriveBy') {
    const t = req.requestedDropoffTs || req.scheduledDropoffTs;
    return t ? `Arrive by ${fmtTime(t)}` : null;
  }
  if (req.intentType === 'leaveAt') {
    const pu = req.requestedPickupTs || req.scheduledPickupTs;
    if (!pu) return null;
    if (req.createdAt && pu - req.createdAt <= 300) return 'Leave ASAP';
    return `Leave at ${fmtTime(pu)}`;
  }
  return null;
}

export function riderName(req) {
  const r = req.rider;
  if (!r) return 'Rider';
  return [r.firstName, r.lastName].filter(Boolean).join(' ') || 'Rider';
}

export function accessTags(req) {
  const features = req.accessibilityFeatures || [];
  if (!features.length) return '';
  return features
    .map((f) => {
      const label = typeof f === 'object' ? f.type || JSON.stringify(f) : f;
      return `<span class="access-tag">${escHtml(label)}</span>`;
    })
    .join('');
}

// OnDemand callName is always "VAN XX:" / "CAR XX:" — strip colon + suffix,
// title-case the prefix.
export function parseOdCallName(raw) {
  if (!raw) return raw;
  const name = raw.split(':')[0].trim();
  return name.replace(/^([A-Za-z]+)/, (m) => m.charAt(0).toUpperCase() + m.slice(1).toLowerCase());
}

export function normalizeName(s) {
  return (s || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

// Do a W2W shift window and a Spare duty window (both in seconds) plausibly
// refer to the same stretch of work? Allow an hour of slack each way.
export function windowsOverlapWithSlack(aStart, aEnd, bStart, bEnd, slackSec) {
  return aStart - slackSec <= bEnd && bStart - slackSec <= aEnd;
}

// The little numbered circle badge shown on trip cards, itinerary rows, and (as
// a real marker) on the map's stop pins — same colour as the assigned van.
export function stopOrderBadgeHtml(order, color) {
  if (!order) return '';
  const textColor = contrastColor(color);
  return `<span style="display:inline-flex;align-items:center;justify-content:center;width:16px;height:16px;border-radius:50%;background:${color};color:${textColor};font-size:10px;font-weight:700;border:1.5px solid white;flex-shrink:0;margin-right:4px;vertical-align:middle">${order}</span>`;
}

/** GeoJSON [lng,lat] point -> [lng, lat] number pair (MapLibre order), or null. */
export function lngLatFromGeoJson(loc) {
  if (!loc || !Array.isArray(loc.coordinates)) return null;
  const [lng, lat] = loc.coordinates;
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return [lng, lat];
}

/** Great-circle metres between two [lng,lat] points (for the same-spot merge). */
export function haversineM(a, b) {
  const R = 6371000;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b[1] - a[1]);
  const dLng = toRad(b[0] - a[0]);
  const s =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(a[1])) * Math.cos(toRad(b[1])) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)));
}
