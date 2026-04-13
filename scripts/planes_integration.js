(function (global) {
  'use strict';

  function adoptIfEmpty(key, candidate) {
    if (!candidate || typeof candidate !== 'object') {
      return;
    }
    const current = global[key];
    const hasEntries = current && typeof current === 'object' && Object.keys(current).length > 0;
    if (!hasEntries) {
      global[key] = candidate;
    }
  }

  adoptIfEmpty('shapes', typeof shapes !== 'undefined' ? shapes : undefined);
  adoptIfEmpty('TypeDesignatorIcons', typeof TypeDesignatorIcons !== 'undefined' ? TypeDesignatorIcons : undefined);
  adoptIfEmpty('CategoryIcons', typeof CategoryIcons !== 'undefined' ? CategoryIcons : undefined);
  adoptIfEmpty('TypeDescriptionIcons', typeof TypeDescriptionIcons !== 'undefined' ? TypeDescriptionIcons : undefined);

  const IDLE_INTERVAL_MS = 5000;
  const MOVE_DEBOUNCE_MS = 300;
  const FETCH_TIMEOUT_MS = 10000;
  const BACKOFF_MS = 2000;
  const STALE_REMOVE_MS = 60000;
  const MIN_RADIUS_NM = 5;
  const MAX_RADIUS_NM = 250;
  const DEFAULT_RADIUS_NM = 25;
  const NM_IN_METERS = 1852;
  const LATITUDE_MIN = -90;
  const LATITUDE_MAX = 90;
  const LONGITUDE_MIN = -180;
  const LONGITUDE_MAX = 180;
  const SUPERSEDED_REASON = 'PlaneLayerFetchSuperseded';
  const TIMEOUT_REASON = 'PlaneLayerFetchTimeout';
  const PLANE_PANE_NAME = 'planesPane';
  const PLANE_PANE_ZINDEX = 520;
  const FALLBACK_STROKE_WIDTH = 0.75;

  const state = {
    map: null,
    initialized: false,
    disposed: false,
    started: false,
    idleTimer: null,
    moveDebounceTimer: null,
    scheduledFetchTimer: null,
    inflightController: null,
    lastFetchAt: -Infinity,
    backoffUntil: 0,
    markers: new Map(),
    iconCache: new Map(),
    mapListeners: [],
    markerLayer: null,
    leafletPaneName: null,
    lastFetchCenter: null,
    lastFetchRadiusNM: DEFAULT_RADIUS_NM,
    pendingRotationRefresh: false,
    selectedAircraftId: null,
    aircraftPopupEl: null,
    aircraftPopupLatLng: null,
    aircraftPopupMoveCleanup: null,
    pinnedTrackId: null,
    trackPolyline: null,
  };
  function isLeafletMap(map) {
    return typeof global.L !== 'undefined' && map && typeof global.L.marker === 'function';
  }

  function clampNumber(value, min, max) {
    if (!Number.isFinite(value)) return min;
    return Math.min(max, Math.max(min, value));
  }

  function toFiniteNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : NaN;
  }

  function metersBetween(lat1, lon1, lat2, lon2) {
    const R = 6371000;
    const toRad = x => x * Math.PI / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(a));
  }

  function metersToNM(m) {
    return m / NM_IN_METERS;
  }

  function isValidLatitude(value) {
    return Number.isFinite(value) && value >= LATITUDE_MIN && value <= LATITUDE_MAX;
  }

  function isValidLongitude(value) {
    return Number.isFinite(value) && value >= LONGITUDE_MIN && value <= LONGITUDE_MAX;
  }

  function tryLat(candidate) {
    const numeric = toFiniteNumber(candidate);
    return isValidLatitude(numeric) ? numeric : NaN;
  }

  function tryLon(candidate) {
    const numeric = toFiniteNumber(candidate);
    return isValidLongitude(numeric) ? numeric : NaN;
  }

  function extractLatLon(row) {
    // Extract purely horizontal coordinates from the payload.  We intentionally
    // ignore altitude, velocity, or extrapolated metadata so zoom level changes
    // never influence the marker's geodetic position.
    if (!row || typeof row !== 'object') {
      return null;
    }

    let lat = tryLat(row.lat);
    if (!Number.isFinite(lat)) {
      lat = tryLat(row.latitude);
    }
    if (!Number.isFinite(lat)) {
      lat = tryLat(row.Latitude);
    }
    if (!Number.isFinite(lat)) {
      lat = tryLat(row.Lat);
    }
    if (!Number.isFinite(lat)) {
      lat = tryLat(row.latDeg ?? row.lat_deg ?? row.lat_deg_m);
    }

    let lon = tryLon(row.lon);
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.lng);
    }
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.long);
    }
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.longitude);
    }
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.Lon);
    }
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.Longitude);
    }
    if (!Number.isFinite(lon)) {
      lon = tryLon(row.lonDeg ?? row.lon_deg ?? row.lon_deg_m);
    }

    const fallbackSources = [
      row.position,
      row.pos,
      row.location,
      row.loc,
      row.coordinates,
      row.coord
    ];

    for (let i = 0; i < fallbackSources.length && (!Number.isFinite(lat) || !Number.isFinite(lon)); i += 1) {
      const source = fallbackSources[i];
      if (!source) continue;

      if (Array.isArray(source)) {
        if (source.length < 2) continue;
        const first = toFiniteNumber(source[0]);
        const second = toFiniteNumber(source[1]);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          if (!Number.isFinite(lat) && isValidLatitude(first) && isValidLongitude(second)) {
            lat = first;
            lon = second;
            continue;
          }
          if (!Number.isFinite(lat) && isValidLatitude(second) && isValidLongitude(first)) {
            lat = second;
            lon = first;
            continue;
          }
        }
      } else if (typeof source === 'object') {
        if (!Number.isFinite(lat)) {
          lat = tryLat(source.lat ?? source.latitude ?? source.Latitude ?? source.Lat ?? source.latDeg ?? source.lat_deg);
        }
        if (!Number.isFinite(lon)) {
          lon = tryLon(source.lon ?? source.lng ?? source.long ?? source.longitude ?? source.Lon ?? source.Longitude ?? source.lonDeg ?? source.lon_deg);
        }
      }
    }

    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      return { lat, lon };
    }
    return null;
  }

  function buildAdsbUrl(lat, lon, dist) {
    const customEndpointRaw = (typeof global.ADSB_PROXY_ENDPOINT === 'string') ? global.ADSB_PROXY_ENDPOINT.trim() : '';
    const latStr = String(lat);
    const lonStr = String(lon);
    const distStr = String(dist);
    if (customEndpointRaw) {
      const params = `lat=${encodeURIComponent(latStr)}&lon=${encodeURIComponent(lonStr)}&dist=${encodeURIComponent(distStr)}`;
      const hasQuery = customEndpointRaw.includes('?');
      let base = customEndpointRaw;
      if (hasQuery) {
        if (!base.endsWith('?') && !base.endsWith('&')) {
          base += '&';
        }
      } else {
        base += '?';
      }
      return `${base}${params}`;
    }
    return `https://opendata.adsb.fi/api/v2/lat/${latStr}/lon/${lonStr}/dist/${distStr}`;
  }

  function getMapCenterLatLng(map) {
    if (map && typeof map.getCenter === 'function') {
      const c = map.getCenter();
      if (c) {
        const lat = typeof c.lat === 'number' ? c.lat : (Array.isArray(c) ? c[0] : undefined);
        const lon = typeof c.lng === 'number' ? c.lng : (Array.isArray(c) ? c[1] : undefined);
        if (Number.isFinite(lat) && Number.isFinite(lon)) {
          return { lat, lon };
        }
      }
    }
    if (map && map.getCenter) {
      const c = map.getCenter();
      if (c && Number.isFinite(c.lat) && Number.isFinite(c.lng)) {
        return { lat: c.lat, lon: c.lng };
      }
    }
    if (map && typeof map.getView === 'function' && global.ol && global.ol.proj) {
      const view = map.getView();
      if (view && typeof view.getCenter === 'function') {
        const center3857 = view.getCenter();
        if (Array.isArray(center3857)) {
          const transformed = global.ol.proj.transform(center3857, 'EPSG:3857', 'EPSG:4326');
          if (Array.isArray(transformed) && Number.isFinite(transformed[1]) && Number.isFinite(transformed[0])) {
            return { lat: transformed[1], lon: transformed[0] };
          }
        }
      }
    }
    throw new Error('Unsupported map object');
  }

  function getMapBoundsCorners(map) {
    if (map && typeof map.getBounds === 'function') {
      const b = map.getBounds();
      if (b) {
        if (typeof b.getNorthEast === 'function') {
          const ne = b.getNorthEast();
          if (ne && Number.isFinite(ne.lat) && Number.isFinite(ne.lng)) {
            return { neLat: ne.lat, neLon: ne.lng };
          }
        } else if (b._northEast && Number.isFinite(b._northEast.lat) && Number.isFinite(b._northEast.lng)) {
          return { neLat: b._northEast.lat, neLon: b._northEast.lng };
        }
      }
    }
    if (map && typeof map.getBounds === 'function') {
      const b = map.getBounds();
      if (b && typeof b.getNorthEast === 'function') {
        const ne = b.getNorthEast();
        if (ne && Number.isFinite(ne.lat) && Number.isFinite(ne.lng)) {
          return { neLat: ne.lat, neLon: ne.lng };
        }
      }
    }
    return null;
  }

  function buildFetchContext(map) {
    const center = getMapCenterLatLng(map);
    const corners = getMapBoundsCorners(map);
    let distNM = DEFAULT_RADIUS_NM;
    if (corners) {
      const meters = metersBetween(center.lat, center.lon, corners.neLat, corners.neLon);
      const nm = metersToNM(meters);
      distNM = clampNumber(Math.ceil(nm * 1.10), MIN_RADIUS_NM, MAX_RADIUS_NM);
    }
    const url = buildAdsbUrl(center.lat, center.lon, distNM);
    return { url, center, distNM };
  }

  function normalizeHeading(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return 0;
    }
    const normalized = ((numeric % 360) + 360) % 360;
    return normalized;
  }

  function computeAltitude(row) {
    if (!row) return NaN;
    const altGeom = toFiniteNumber(row.alt_geom);
    if (Number.isFinite(altGeom)) return altGeom;
    const altBaro = toFiniteNumber(row.alt_baro);
    if (Number.isFinite(altBaro)) return altBaro;
    return NaN;
  }

  function fmtAlt(row) {
    const baro = toFiniteNumber(row && row.alt_baro);
    if (row && row.alt_baro === 'ground') return 'Ground';
    if (Number.isFinite(baro)) return baro.toLocaleString() + ' ft';
    return null;
  }

  function fmtSpeed(row) {
    const gs = toFiniteNumber(row && row.gs);
    if (Number.isFinite(gs)) return Math.round(gs) + ' kt';
    return null;
  }

  function fmtVRate(row) {
    let rate = toFiniteNumber(row && row.baro_rate);
    if (!Number.isFinite(rate)) rate = toFiniteNumber(row && row.geom_rate);
    if (!Number.isFinite(rate)) return null;
    const sign = rate >= 0 ? '+' : '';
    return sign + rate.toLocaleString() + ' fpm';
  }

  function fmtSquawk(row) {
    const sq = row && typeof row.squawk === 'string' ? row.squawk.trim() : '';
    return sq || null;
  }

  function buildAircraftPopupHTML(row) {
    if (!row) return '';
    const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    const callsign  = (typeof row.flight === 'string' ? row.flight.trim() : '') || null;
    const reg       = (typeof row.r === 'string' ? row.r.trim() : '') || null;
    const typeCode  = (typeof row.t === 'string' ? row.t.trim() : '') || null;
    const desc      = (typeof row.desc === 'string' ? row.desc.trim() : '') || null;
    const operator  = (typeof row.ownOp === 'string' ? row.ownOp.trim() : '') || null;
    const year      = (typeof row.year === 'string' ? row.year.trim() : '') || null;
    const rawHex    = (typeof row.hex === 'string' ? row.hex.trim() : '') || null;
    const hex       = rawHex ? rawHex.toUpperCase() : null;
    const emergency = (typeof row.emergency === 'string' && row.emergency !== 'none') ? row.emergency : null;
    const squawk    = fmtSquawk(row);
    const alt       = fmtAlt(row);
    const spd       = fmtSpeed(row);
    const vrate     = fmtVRate(row);
    const srcType   = (typeof row.type === 'string' ? row.type.replace(/_/g, ' ') : '') || null;
    const dbFlags   = typeof row.dbFlags === 'number' ? row.dbFlags : 0;

    // Derived flags
    const isMilitary  = !!(dbFlags & 1);
    const isPIA       = !!(dbFlags & 4);   // Privacy ICAO Address — no DB info by design
    const isLADD      = !!(dbFlags & 8);   // FAA-restricted display
    const isSynthHex  = rawHex && rawHex.startsWith('~'); // TIS-B / track-file, no real ICAO

    // Primary display label: callsign > reg > hex > "Unknown"
    const primaryLabel = callsign || reg || (hex && !isSynthHex ? hex : null) || 'Unknown';
    const primaryIsCallsign = !!callsign;
    const primaryIsReg      = !callsign && !!reg;
    const primaryIsHex      = !callsign && !reg;

    const isEmergencySquawk = squawk === '7700' || squawk === '7600' || squawk === '7500';
    const typeLabel = desc
      ? `${esc(desc)}${typeCode ? ` (${esc(typeCode)})` : ''}`
      : (typeCode ? esc(typeCode) : '');
    const climbClass = vrate
      ? (vrate.startsWith('+') ? ' aircraft-popup__climb-up' : ' aircraft-popup__climb-down')
      : '';

    // Badges row under the primary label
    const badges = [];
    if (primaryIsCallsign && reg)         badges.push(`<span class="aircraft-popup__badge aircraft-popup__badge--reg">${esc(reg)}</span>`);
    if (primaryIsCallsign && typeCode)    badges.push(`<span class="aircraft-popup__badge">${esc(typeCode)}</span>`);
    if (primaryIsReg && typeCode)         badges.push(`<span class="aircraft-popup__badge">${esc(typeCode)}</span>`);
    if (primaryIsHex && hex && !isSynthHex) { /* hex is already the primary label, skip badge */ }
    if (isMilitary)  badges.push(`<span class="aircraft-popup__badge aircraft-popup__badge--military">Military</span>`);
    if (isPIA)       badges.push(`<span class="aircraft-popup__badge aircraft-popup__badge--dim">Privacy addr</span>`);
    if (isLADD)      badges.push(`<span class="aircraft-popup__badge aircraft-popup__badge--dim">LADD</span>`);
    if (isSynthHex)  badges.push(`<span class="aircraft-popup__badge aircraft-popup__badge--dim">No transponder</span>`);
    const badgesHTML = badges.length
      ? `<div class="aircraft-popup__badges">${badges.join('')}</div>`
      : '';

    let statRows = '';
    if (alt)    statRows += `<div class="aircraft-popup__stat"><span class="aircraft-popup__stat-label">Altitude</span><span class="aircraft-popup__stat-value">${esc(alt)}</span></div>`;
    if (spd)    statRows += `<div class="aircraft-popup__stat"><span class="aircraft-popup__stat-label">Speed</span><span class="aircraft-popup__stat-value">${esc(spd)}</span></div>`;
    if (vrate)  statRows += `<div class="aircraft-popup__stat"><span class="aircraft-popup__stat-label">Climb</span><span class="aircraft-popup__stat-value${climbClass}">${esc(vrate)}</span></div>`;
    if (squawk) statRows += `<div class="aircraft-popup__stat"><span class="aircraft-popup__stat-label">Squawk</span><span class="aircraft-popup__stat-value${isEmergencySquawk ? ' aircraft-popup__squawk-alert' : ''}">${esc(squawk)}</span></div>`;

    const metaParts = [];
    if (operator) metaParts.push(esc(operator));
    if (year)     metaParts.push(`Mfr ${esc(year)}`);
    // Show ICAO hex in meta only if it wasn't used as the primary label
    if (hex && !isSynthHex && !primaryIsHex) metaParts.push(`ICAO ${esc(hex)}`);
    if (srcType)  metaParts.push(esc(srcType));

    const emergencyBanner = emergency
      ? `<div class="aircraft-popup__emergency">${esc(emergency.toUpperCase())}</div>`
      : '';

    const isPinned = state.pinnedTrackId === rawHex;
    const pinBtn = `<button class="aircraft-popup__pin-btn${isPinned ? ' aircraft-popup__pin-btn--active' : ''}">${isPinned ? 'Unpin track' : 'Pin track'}</button>`;

    const primaryClass = `aircraft-popup__callsign${primaryIsHex ? ' aircraft-popup__callsign--hex' : ''}`;

    return `<button class="custom-popup-close">&times;</button>${emergencyBanner}<div class="aircraft-popup"><div class="aircraft-popup__header"><div class="aircraft-popup__icon"><i class="ti ti-plane-inflight"></i></div><div class="aircraft-popup__title-block"><div class="${primaryClass}">${esc(primaryLabel)}</div>${badgesHTML}</div></div>${typeLabel ? `<div class="aircraft-popup__type-line">${typeLabel}</div>` : ''}${statRows ? `<div class="aircraft-popup__divider"></div><div class="aircraft-popup__stats">${statRows}</div>` : ''}${metaParts.length ? `<div class="aircraft-popup__divider"></div><div class="aircraft-popup__meta">${metaParts.map(p => `<span class="aircraft-popup__meta-item">${p}</span>`).join('')}</div>` : ''}<div class="aircraft-popup__divider"></div><div class="aircraft-popup__actions">${pinBtn}</div></div><div class="custom-popup-arrow"></div>`;
  }

  function updateAircraftPopupPosition() {
    const el = state.aircraftPopupEl;
    const latlng = state.aircraftPopupLatLng;
    if (!el || !latlng || !state.map) return;
    if (typeof state.map.latLngToContainerPoint !== 'function') return;
    const mapContainer = typeof state.map.getContainer === 'function' ? state.map.getContainer() : null;
    if (!mapContainer || typeof mapContainer.getBoundingClientRect !== 'function') return;
    const mapRect = mapContainer.getBoundingClientRect();
    const pt = state.map.latLngToContainerPoint(latlng);
    el.style.left = `${mapRect.left + pt.x}px`;
    el.style.top  = `${mapRect.top  + pt.y}px`;
  }

  function centerAircraftPopupOnMap() {
    const el = state.aircraftPopupEl;
    if (!el || !state.map || typeof state.map.panBy !== 'function') return;
    const mapContainer = typeof state.map.getContainer === 'function' ? state.map.getContainer() : null;
    if (!mapContainer) return;
    const mapRect   = mapContainer.getBoundingClientRect();
    const popupRect = el.getBoundingClientRect();
    if (!mapRect.width || !mapRect.height || !popupRect.width || !popupRect.height) return;
    const dx = (popupRect.left - mapRect.left + popupRect.width  / 2) - mapRect.width  / 2;
    const dy = (popupRect.top  - mapRect.top  + popupRect.height / 2) - mapRect.height / 2;
    if (Math.abs(dx) < 1 && Math.abs(dy) < 1) return;
    state.map.panBy([dx, dy], { animate: true, duration: 0.35, easeLinearity: 0.25 });
  }

  function openAircraftPopup(id, lat, lon, row) {
    closeAircraftPopup();
    state.selectedAircraftId = id;
    state.aircraftPopupLatLng = [lat, lon];

    const el = typeof document !== 'undefined' ? document.createElement('div') : null;
    if (!el) return;
    el.className = 'custom-popup';
    el.dataset.position = `${lat},${lon}`;
    el.innerHTML = buildAircraftPopupHTML(row);
    document.body.appendChild(el);
    state.aircraftPopupEl = el;

    const closeBtn = el.querySelector('.custom-popup-close');
    if (closeBtn) closeBtn.addEventListener('click', closeAircraftPopup);

    const pinBtn = el.querySelector('.aircraft-popup__pin-btn');
    if (pinBtn) pinBtn.addEventListener('click', () => pinTrack(id));

    renderTrackPolyline(id);
    updateAircraftPopupPosition();
    // Fetch full flight track and rich metadata in the background
    const hex = row && typeof row.hex === 'string' ? row.hex : null;
    if (hex) {
      fetchAndMergeOpenSkyTrack(id, hex);
      fetchAircraftMetadata(id, hex);
    }

    if (typeof requestAnimationFrame === 'function') {
      requestAnimationFrame(centerAircraftPopupOnMap);
    } else {
      centerAircraftPopupOnMap();
    }

    if (state.map && typeof state.map.on === 'function') {
      const onMove = () => updateAircraftPopupPosition();
      state.map.on('zoom move zoomend moveend', onMove);
      state.aircraftPopupMoveCleanup = () => {
        if (state.map && typeof state.map.off === 'function') {
          state.map.off('zoom move zoomend moveend', onMove);
        }
      };
    }
  }

  function closeAircraftPopup() {
    if (state.aircraftPopupMoveCleanup) {
      state.aircraftPopupMoveCleanup();
      state.aircraftPopupMoveCleanup = null;
    }
    if (state.aircraftPopupEl) {
      state.aircraftPopupEl.remove();
      state.aircraftPopupEl = null;
    }
    state.aircraftPopupLatLng = null;
    const closingId = state.selectedAircraftId;
    state.selectedAircraftId = null;
    // Only remove the track if this aircraft's track isn't pinned
    if (closingId !== state.pinnedTrackId) {
      removeTrackPolyline();
    }
  }

  function refreshOpenPopup(id, lat, lon, row) {
    // Refresh pinned track even if popup is closed
    if (id === state.pinnedTrackId && state.trackPolyline) {
      renderTrackPolyline(id);
    }
    if (state.selectedAircraftId !== id || !state.aircraftPopupEl) return;
    state.aircraftPopupLatLng = [lat, lon];
    state.aircraftPopupEl.dataset.position = `${lat},${lon}`;
    updateAircraftPopupPosition();
    // Also refresh track for selected (non-pinned) aircraft
    if (id !== state.pinnedTrackId) {
      renderTrackPolyline(id);
    }
    const inner = state.aircraftPopupEl.querySelector('.aircraft-popup');
    if (inner) {
      const tmp = document.createElement('div');
      tmp.innerHTML = buildAircraftPopupHTML(row);
      const updated = tmp.querySelector('.aircraft-popup');
      if (updated) {
        inner.replaceWith(updated);
        const pinBtn = state.aircraftPopupEl.querySelector('.aircraft-popup__pin-btn');
        if (pinBtn) pinBtn.addEventListener('click', () => pinTrack(id));
      }
    }
  }

  function renderTrackPolyline(id) {
    removeTrackPolyline();
    if (!isLeafletMap(state.map) || !global.L) return;
    const entry = state.markers.get(id);
    if (!entry || !entry.track || entry.track.length < 2) return;
    const polyline = global.L.polyline(entry.track, {
      color: '#7eb8f0',
      weight: 2,
      opacity: 0.75,
      dashArray: '5 4',
      pane: state.leafletPaneName || undefined,
      interactive: false,
    });
    if (state.markerLayer && typeof state.markerLayer.addLayer === 'function') {
      state.markerLayer.addLayer(polyline);
    } else if (typeof polyline.addTo === 'function') {
      polyline.addTo(state.map);
    }
    state.trackPolyline = polyline;
  }

  async function fetchAndMergeOpenSkyTrack(id, hex) {
    if (!hex) return;
    const url = `/api/opensky/track?icao24=${encodeURIComponent(hex.toLowerCase())}`;
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) return;
      const data = await resp.json();
      const path = data && Array.isArray(data.path) ? data.path : null;
      if (!path || path.length < 2) return;
      // path entries: [unix_ts, lat, lon, alt_m, heading, on_ground]
      const entry = state.markers.get(id);
      if (!entry) return;
      const historical = path
        .filter(p => Array.isArray(p) && p.length >= 3 && Number.isFinite(p[1]) && Number.isFinite(p[2]))
        .map(p => [p[1], p[2]]);
      // Merge: historical points go first, then any live points already accumulated
      // that are newer than the last historical timestamp
      const lastHistTs = path[path.length - 1][0];
      const livePts = (entry.trackTimestamps || [])
        .map((ts, i) => ({ ts, pt: entry.track[i] }))
        .filter(x => x.ts > lastHistTs)
        .map(x => x.pt);
      entry.track = historical.concat(livePts);
      // If this aircraft is currently selected or pinned, refresh the polyline
      if (state.selectedAircraftId === id || state.pinnedTrackId === id) {
        renderTrackPolyline(id);
      }
    } catch (e) {
      // Silently ignore - fall back to live accumulation
    }
  }

  async function fetchAircraftMetadata(id, hex) {
    if (!hex || hex.startsWith('~')) return; // no real ICAO — nothing to look up
    const url = `/api/adsbfi/aircraft?icao24=${encodeURIComponent(hex.toLowerCase())}`;
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) return;
      const data = await resp.json();
      // adsb.fi /v2/icao/{hex} returns { aircraft: [...] }
      const ac = data && Array.isArray(data.aircraft) ? data.aircraft[0] : null;
      if (!ac) return;
      const entry = state.markers.get(id);
      if (!entry) return;
      // Merge metadata fields that OpenSky doesn't supply into the live row
      const metaFields = ['r', 't', 'desc', 'ownOp', 'year', 'dbFlags'];
      let changed = false;
      for (const field of metaFields) {
        if (ac[field] !== undefined && ac[field] !== null) {
          entry.row[field] = ac[field];
          changed = true;
        }
      }
      if (changed && (state.selectedAircraftId === id || state.pinnedTrackId === id)) {
        refreshOpenPopup(id, entry.lat, entry.lon, entry.row);
      }
    } catch (e) {
      // silently ignore
    }
  }

  function removeTrackPolyline() {
    if (!state.trackPolyline) return;
    try {
      if (typeof state.trackPolyline.remove === 'function') {
        state.trackPolyline.remove();
      } else if (state.map && typeof state.map.removeLayer === 'function') {
        state.map.removeLayer(state.trackPolyline);
      }
    } catch (e) { /* ignore */ }
    state.trackPolyline = null;
  }

  function pinTrack(id) {
    if (state.pinnedTrackId === id) {
      // Unpin
      state.pinnedTrackId = null;
      if (state.aircraftPopupEl) {
        const btn = state.aircraftPopupEl.querySelector('.aircraft-popup__pin-btn');
        if (btn) {
          btn.textContent = 'Pin track';
          btn.classList.remove('aircraft-popup__pin-btn--active');
          btn.disabled = false;
        }
      }
      // If popup is also closed, remove the dangling polyline
      if (!state.selectedAircraftId) {
        removeTrackPolyline();
      }
    } else {
      state.pinnedTrackId = id;
      if (state.aircraftPopupEl) {
        const btn = state.aircraftPopupEl.querySelector('.aircraft-popup__pin-btn');
        if (btn) {
          btn.textContent = 'Unpin track';
          btn.classList.add('aircraft-popup__pin-btn--active');
          btn.disabled = false;
        }
      }
    }
  }

  function updateTrackForEntry(entry, lat, lon) {
    if (!entry.track) entry.track = [];
    if (!entry.trackTimestamps) entry.trackTimestamps = [];
    const last = entry.track[entry.track.length - 1];
    // Only append if position changed by at least ~10m
    if (!last || metersBetween(last[0], last[1], lat, lon) > 10) {
      const ts = Math.floor(Date.now() / 1000);
      entry.track.push([lat, lon]);
      entry.trackTimestamps.push(ts);
      if (entry.track.length > 500) {
        entry.track.shift();
        entry.trackTimestamps.shift();
      }
    }
  }

  function computeFillColor(row, altitudeInfo) {
    const isGround = altitudeInfo.isGround;
    let h;
    let s;
    let l;
    if (isGround) {
      h = 120;
      s = 25;
      l = 60;
    } else {
      const rounded = Math.round(altitudeInfo.altitude / 500) * 500;
      const clamped = clampNumber(rounded, 0, 45000);
      const t = clamped / 45000;
      h = 220 + (0 - 220) * t;
      s = 80;
      l = 45;
    }

    const seenPos = toFiniteNumber(row && row.seen_pos);
    const staleThreshold = (row && row.type === 'adsc') ? 1200 : 15;
    if (Number.isFinite(seenPos) && seenPos > staleThreshold) {
      s -= 10;
      l -= 10;
    }

    if (isGround) {
      l += 15;
    }

    if (row && row.type === 'mlat') {
      h = (h - 10 + 360) % 360;
    }

    const squawk = row && typeof row.squawk === 'string' ? row.squawk.trim() : '';
    if (global.atcStyle && (squawk === '7700' || squawk === '7600' || squawk === '7500')) {
      h = 0;
      s = 100;
      l = 40;
    }

    h = ((h % 360) + 360) % 360;
    s = clampNumber(s, 0, 95);
    l = clampNumber(l, 0, 95);

    return { h, s, l, css: hslToCss(h, s, l) };
  }

  function hslToCss(h, s, l) {
    if (typeof global.hslToRgb === 'function') {
      const rgb = global.hslToRgb(h, s, l);
      if (rgb && Number.isFinite(rgb.r) && Number.isFinite(rgb.g) && Number.isFinite(rgb.b)) {
        return `rgb(${rgb.r}, ${rgb.g}, ${rgb.b})`;
      }
    }
    return `hsl(${h}, ${s}%, ${l}%)`;
  }

  function getAircraftId(row) {
    if (!row) return null;
    const candidates = [row.hex, row.icao, row.icao24, row.icao_address, row.addr, row.address, row.id];
    for (let i = 0; i < candidates.length; i += 1) {
      const candidate = candidates[i];
      if (candidate === null || candidate === undefined) continue;
      const str = String(candidate).trim();
      if (str) return str;
    }
    return null;
  }

  function pickMarkerDescriptor(row, altitudeToken, eastbound) {
    const getBase = typeof global.getBaseMarker === 'function' ? global.getBaseMarker : null;
    if (getBase) {
      try {
        const descriptor = getBase(row ? row.category : undefined, row ? row.t : undefined, row ? row.desc : undefined, row ? row.wtc : undefined, row ? row.type : undefined, altitudeToken, eastbound);
        if (Array.isArray(descriptor) && descriptor.length > 0) {
          return descriptor;
        }
      } catch (error) {
        console.warn('PlaneLayer: getBaseMarker failed, falling back', error);
      }
    }
    return fallbackMarkerDescriptor(row, altitudeToken);
  }

  function fallbackMarkerDescriptor(row, altitudeToken) {
    const typeDesignator = row && typeof row.t === 'string' ? row.t.toUpperCase() : '';
    if (typeDesignator && global.TypeDesignatorIcons && Array.isArray(global.TypeDesignatorIcons[typeDesignator])) {
      return global.TypeDesignatorIcons[typeDesignator];
    }
    const descRaw = row && typeof row.desc === 'string' ? row.desc : '';
    const desc = descRaw.toUpperCase();
    if (desc) {
      if (/MD-?11/.test(desc)) return ['md11', 1];
      if (/CRJ|REGIONAL\s+JET/.test(desc)) return ['jet_swept', 0.92];
      if (/ERJ|EMBRAER/.test(desc)) return ['jet_swept', 0.92];
      if (/AIRBUS|A3\d{2}/.test(desc)) return ['airliner', 1];
      if (/BOEING|B7\d{2}/.test(desc)) return ['airliner', 1];
      if (/CESSNA/.test(desc)) return ['cessna', 1];
      if (/HELI|ROTOR/.test(desc)) return ['helicopter', 1];
    }
    if (row && row.category && global.CategoryIcons && Array.isArray(global.CategoryIcons[row.category])) {
      return global.CategoryIcons[row.category];
    }
    if (altitudeToken === 'ground') {
      const sourceType = row && typeof row.type === 'string' ? row.type.toLowerCase() : '';
      if (sourceType === 'adsb_icao_nt' || sourceType === 'tisb_other' || sourceType === 'tisb_trackfile') {
        return ['ground_square', 1];
      }
    }
    return ['unknown', 1];
  }

  function buildIconInfo(row, altitudeInfo, eastbound) {
    const altitudeToken = altitudeInfo.isGround ? 'ground' : 'air';
    const descriptor = pickMarkerDescriptor(row, altitudeToken, eastbound);
    const shapeName = Array.isArray(descriptor) && descriptor.length > 0 ? descriptor[0] : 'unknown';
    const scaleRaw = Array.isArray(descriptor) && descriptor.length > 1 ? descriptor[1] : 1;
    const scale = Number.isFinite(scaleRaw) && scaleRaw > 0 ? scaleRaw : 1;
    const shapesSource = (typeof global.shapes !== 'undefined' && global.shapes)
      || (typeof shapes !== 'undefined' ? shapes : undefined)
      || {};
    const shape = shapesSource[shapeName] || shapesSource.unknown;
    if (!shape) {
      return null;
    }

    const strokeColor = typeof global.OutlineADSBColor === 'string' ? global.OutlineADSBColor : '#000000';
    const strokeWidth = Number.isFinite(global.outlineWidth) ? global.outlineWidth : FALLBACK_STROKE_WIDTH;
    const fill = computeFillColor(row, altitudeInfo).css;
    const iconKey = `${shapeName}|${scale}|${fill}|${strokeColor}|${strokeWidth}`;

    let cached = state.iconCache.get(iconKey);
    if (!cached) {
      if (typeof global.svgShapeToURI !== 'function') {
        return null;
      }
      const iconUrl = global.svgShapeToURI(shape, fill, strokeColor, strokeWidth, scale);
      if (!iconUrl) {
        return null;
      }
      let leafletIcon = null;
      if (isLeafletMap(state.map) && global.L && typeof global.L.icon === 'function') {
        const width = Math.max(1, Math.round((shape.w || 32) * scale));
        const height = Math.max(1, Math.round((shape.h || 32) * scale));
        leafletIcon = global.L.icon({
          iconUrl,
          iconSize: [width, height],
          iconAnchor: [width / 2, height / 2],
          className: 'plane-layer-icon'
        });
      }
      cached = { iconUrl, leafletIcon };
      state.iconCache.set(iconKey, cached);
    }

    const headingDeg = normalizeHeading(row && row.track);
    const rotationRadians = (shape && shape.noRotate) ? 0 : (headingDeg * Math.PI / 180);

    return {
      shapeName,
      scale,
      fill,
      iconKey,
      rotationRadians,
      resources: cached,
      headingDeg,
      altitudeToken
    };
  }

  function applyLeafletMarkerRotation(marker, rotationDeg) {
    if (!marker) return false;
    const element = (typeof marker.getElement === 'function') ? marker.getElement() : marker._icon;
    if (!element) return false;

    const rotation = Number.isFinite(rotationDeg) ? rotationDeg : 0;
    const existing = element.style.transform || '';
    const base = existing.replace(/(?:\s*rotate\([^)]*\))+/g, '').trim();

    if (rotation === 0) {
      element.style.transform = base;
    } else if (base) {
      element.style.transform = `${base} rotate(${rotation}deg)`;
    } else {
      element.style.transform = `rotate(${rotation}deg)`;
    }

    if (!element.style.transformOrigin) {
      element.style.transformOrigin = 'center center';
    }
    element.style.willChange = 'transform';
    return true;
  }

  function scheduleLeafletMarkerRotation(marker, rotationDeg) {
    if (!marker) return;
    if (applyLeafletMarkerRotation(marker, rotationDeg)) {
      return;
    }
    if (typeof marker.once === 'function') {
      marker.once('add', () => {
        applyLeafletMarkerRotation(marker, rotationDeg);
      });
    } else if (typeof requestAnimationFrame === 'function') {
      requestAnimationFrame(() => applyLeafletMarkerRotation(marker, rotationDeg));
    }
  }

  function refreshAllMarkerRotations() {
    if (!isLeafletMap(state.map)) {
      return;
    }
    if (state.pendingRotationRefresh) {
      return;
    }

    state.pendingRotationRefresh = true;

    const applyAll = () => {
      state.pendingRotationRefresh = false;
      if (!isLeafletMap(state.map) || state.disposed) {
        return;
      }
      state.markers.forEach(entry => {
        if (!entry || !entry.marker) return;
        scheduleLeafletMarkerRotation(entry.marker, entry.rotationDeg);
      });
    };

    if (typeof requestAnimationFrame === 'function') {
      requestAnimationFrame(applyAll);
    } else {
      setTimeout(applyAll, 0);
    }
  }

  function ensureLeafletPane(map) {
    if (!isLeafletMap(map)) {
      return;
    }
    if (!state.leafletPaneName) {
      if (typeof map.getPane === 'function' && map.getPane(PLANE_PANE_NAME)) {
        state.leafletPaneName = PLANE_PANE_NAME;
      } else if (typeof map.createPane === 'function') {
        map.createPane(PLANE_PANE_NAME);
        const pane = typeof map.getPane === 'function' ? map.getPane(PLANE_PANE_NAME) : null;
        if (pane) {
          pane.style.zIndex = String(PLANE_PANE_ZINDEX);
          pane.style.pointerEvents = 'auto';
        }
        state.leafletPaneName = PLANE_PANE_NAME;
      }
    }
    if (!state.markerLayer && global.L && typeof global.L.layerGroup === 'function') {
      state.markerLayer = global.L.layerGroup();
      if (typeof state.markerLayer.addTo === 'function') {
        state.markerLayer.addTo(map);
      } else if (typeof map.addLayer === 'function') {
        map.addLayer(state.markerLayer);
      }
    }
  }

  function addMapListener(map, type, handler) {
    if (!map || typeof handler !== 'function') return;
    let remover = null;
    if (typeof map.on === 'function') {
      map.on(type, handler);
      if (typeof map.off === 'function') {
        remover = () => map.off(type, handler);
      } else if (typeof map.un === 'function') {
        remover = () => map.un(type, handler);
      }
    } else if (typeof map.addEventListener === 'function') {
      map.addEventListener(type, handler);
      remover = () => map.removeEventListener(type, handler);
    } else if (typeof map.addListener === 'function') {
      const token = map.addListener(type, handler);
      remover = () => {
        if (token && typeof token.remove === 'function') {
          token.remove();
        } else if (typeof map.removeListener === 'function') {
          map.removeListener(type, handler);
        }
      };
    }
    if (remover) {
      state.mapListeners.push(remover);
    }
  }

  function removeAllMapListeners() {
    state.mapListeners.forEach(remover => {
      try {
        remover();
      } catch (error) {
        console.warn('PlaneLayer: failed to remove map listener', error);
      }
    });
    state.mapListeners = [];
  }

  function pauseIdle() {
    if (state.idleTimer) {
      clearInterval(state.idleTimer);
      state.idleTimer = null;
    }
  }

  function resumeIdle() {
    if (!state.started || state.disposed) return;
    if (!state.idleTimer) {
      state.idleTimer = setInterval(() => {
        requestFetch('idle');
      }, IDLE_INTERVAL_MS);
    }
  }

  function clearMoveDebounce() {
    if (state.moveDebounceTimer) {
      clearTimeout(state.moveDebounceTimer);
      state.moveDebounceTimer = null;
    }
  }

  function clearScheduledFetch() {
    if (state.scheduledFetchTimer) {
      clearTimeout(state.scheduledFetchTimer);
      state.scheduledFetchTimer = null;
    }
  }

  function abortInflight(reason) {
    const controller = state.inflightController;
    if (controller && !controller.signal.aborted) {
      if (typeof controller.abort === 'function') {
        controller.__planeAbortReason = reason;
        controller.abort(reason);
      }
    }
    state.inflightController = null;
  }

  function handleInteractionStart() {
    pauseIdle();
    clearMoveDebounce();
  }

  function handleInteractionEnd() {
    clearMoveDebounce();
    if (!state.started || state.disposed) {
      return;
    }
    state.moveDebounceTimer = setTimeout(() => {
      state.moveDebounceTimer = null;
      requestFetch('viewport');
      resumeIdle();
    }, MOVE_DEBOUNCE_MS);
  }

  function removeMarkerEntry(entry) {
    if (!entry) return;
    if (entry.marker) {
      try {
        if (typeof entry.marker.remove === 'function') {
          entry.marker.remove();
        } else if (state.map && typeof state.map.removeLayer === 'function') {
          state.map.removeLayer(entry.marker);
        }
      } catch (error) {
        console.warn('PlaneLayer: failed to remove marker', error);
      }
    }
  }

  function cleanupMarkers(now, referenceCenter, referenceRadiusNM) {
    const center = referenceCenter || state.lastFetchCenter;
    const radiusNM = Number.isFinite(referenceRadiusNM) ? referenceRadiusNM : state.lastFetchRadiusNM;
    state.markers.forEach((entry, id) => {
      const tooOld = now - entry.lastSeen > STALE_REMOVE_MS;
      let outside = false;
      if (center && Number.isFinite(radiusNM)) {
        const distMeters = metersBetween(center.lat, center.lon, entry.lat, entry.lon);
        const distNM = metersToNM(distMeters);
        if (Number.isFinite(distNM) && distNM > radiusNM + 5) {
          outside = true;
        }
      }
      if (tooOld || outside) {
        removeMarkerEntry(entry);
        state.markers.delete(id);
      }
    });
  }

  function ensureMarkerForRow(row, lat, lon, iconInfo, timestamp) {
    const id = getAircraftId(row);
    if (!id) return;
    let entry = state.markers.get(id);
    if (!entry) {
      entry = { marker: null, iconKey: null, lastSeen: timestamp, lat, lon, row };
      state.markers.set(id, entry);
    }
    entry.lastSeen = timestamp;
    entry.lat = lat;
    entry.lon = lon;
    entry.row = row;
    updateTrackForEntry(entry, lat, lon);

    if (!isLeafletMap(state.map)) {
      return;
    }

    const leafletIcon = iconInfo.resources ? iconInfo.resources.leafletIcon : null;
    if (!leafletIcon) {
      return;
    }

    const rotationDeg = Number.isFinite(iconInfo.headingDeg) ? iconInfo.headingDeg : 0;
    const markerOptions = { icon: leafletIcon, interactive: true };
    if (state.leafletPaneName) {
      markerOptions.pane = state.leafletPaneName;
    }

    if (!entry.marker) {
      const marker = global.L.marker([lat, lon], markerOptions);
      if (state.markerLayer && typeof state.markerLayer.addLayer === 'function') {
        state.markerLayer.addLayer(marker);
      } else if (typeof marker.addTo === 'function') {
        marker.addTo(state.map);
      }
      marker.on('click', () => {
        const currentEntry = state.markers.get(id);
        const r = currentEntry ? currentEntry.row : row;
        openAircraftPopup(id, currentEntry ? currentEntry.lat : lat, currentEntry ? currentEntry.lon : lon, r);
      });
      entry.marker = marker;
      entry.iconKey = iconInfo.iconKey;
      entry.rotationDeg = rotationDeg;
      scheduleLeafletMarkerRotation(marker, rotationDeg);
      return;
    }

    if (entry.iconKey !== iconInfo.iconKey) {
      if (typeof entry.marker.setIcon === 'function') {
        entry.marker.setIcon(leafletIcon);
      }
      entry.iconKey = iconInfo.iconKey;
      entry.rotationDeg = rotationDeg;
      scheduleLeafletMarkerRotation(entry.marker, rotationDeg);
    }

    if (typeof entry.marker.setLatLng === 'function') {
      entry.marker.setLatLng([lat, lon]);
    }
    entry.rotationDeg = rotationDeg;
    scheduleLeafletMarkerRotation(entry.marker, rotationDeg);

    refreshOpenPopup(id, lat, lon, row);
  }

  async function doFetch(reason) {
    if (!state.map || state.disposed || !state.started) {
      return;
    }
    state.lastFetchAt = Date.now();
    let context;
    try {
      context = buildFetchContext(state.map);
    } catch (error) {
      console.error('PlaneLayer: unable to determine map bounds', error);
      state.backoffUntil = Date.now() + BACKOFF_MS;
      return;
    }

    state.lastFetchCenter = context.center;
    state.lastFetchRadiusNM = context.distNM;

    const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
    if (controller) {
      state.inflightController = controller;
    } else {
      state.inflightController = null;
    }

    let timeoutId = null;
    if (controller && typeof setTimeout === 'function') {
      timeoutId = setTimeout(() => {
        controller.__planeAbortReason = TIMEOUT_REASON;
        controller.abort(TIMEOUT_REASON);
      }, FETCH_TIMEOUT_MS);
    }

    try {
      const response = await fetch(context.url, {
        signal: controller ? controller.signal : undefined,
        cache: 'no-store'
      });
      if (!response || !response.ok) {
        throw new Error(response ? `HTTP ${response.status}` : 'No response');
      }
      let payload;
      try {
        payload = await response.json();
      } catch (parseError) {
        throw new Error('Invalid JSON payload');
      }
      state.backoffUntil = 0;
      const rows = extractRowsFromPayload(payload);
      processPayload(rows, context);
    } catch (error) {
      const abortReason = controller ? (controller.signal && 'reason' in controller.signal ? controller.signal.reason : controller.__planeAbortReason) : null;
      if (abortReason === SUPERSEDED_REASON) {
        return;
      }
      if (abortReason === TIMEOUT_REASON) {
        console.warn('PlaneLayer: ADS-B request timed out');
      } else if (error && error.name === 'AbortError' && abortReason === undefined) {
        return;
      } else {
        console.error('PlaneLayer: ADS-B fetch failed', error);
      }
      state.backoffUntil = Date.now() + BACKOFF_MS;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      state.inflightController = null;
    }
  }

  function extractRowsFromPayload(payload) {
    if (Array.isArray(payload)) {
      return payload;
    }
    if (!payload || typeof payload !== 'object') {
      return [];
    }

    if (Array.isArray(payload.ac)) {
      return payload.ac;
    }
    if (Array.isArray(payload.aircraft)) {
      return payload.aircraft;
    }
    if (Array.isArray(payload.rows)) {
      return payload.rows;
    }
    if (Array.isArray(payload.data)) {
      return payload.data;
    }

    for (const key in payload) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) {
        const value = payload[key];
        if (Array.isArray(value) && value.length > 0 && typeof value[0] === 'object') {
          return value;
        }
      }
    }

    return [];
  }

  function processPayload(rows, context) {
    if (!Array.isArray(rows)) {
      return;
    }
    const now = Date.now();
    ensureLeafletPane(state.map);
    rows.forEach(row => {
      if (!row) return;
      const coords = extractLatLon(row);
      if (!coords) {
        return;
      }
      const { lat, lon } = coords;
      const altitude = computeAltitude(row);
      const altitudeInfo = { altitude, isGround: !Number.isFinite(altitude) };
      const headingDeg = normalizeHeading(row.track);
      const eastbound = headingDeg < 180;
      const iconInfo = buildIconInfo(row, altitudeInfo, eastbound);
      if (!iconInfo) {
        return;
      }
      ensureMarkerForRow(row, lat, lon, iconInfo, now);
    });
    const cleanupCenter = context && context.center ? context.center : null;
    const cleanupRadius = context && Number.isFinite(context.distNM) ? context.distNM : undefined;
    cleanupMarkers(now, cleanupCenter, cleanupRadius);
  }

  function requestFetch(reason) {
    if (!state.map || state.disposed || !state.started) {
      return;
    }
    const now = Date.now();
    const earliest = Math.max(state.lastFetchAt + 1000, state.backoffUntil);
    const wait = Math.max(0, earliest - now);
    clearScheduledFetch();
    abortInflight(SUPERSEDED_REASON);
    if (wait === 0) {
      doFetch(reason);
    } else {
      state.scheduledFetchTimer = setTimeout(() => {
        state.scheduledFetchTimer = null;
        doFetch(reason);
      }, wait);
    }
  }

  function startInternal() {
    if (!state.map || state.disposed) return;
    if (state.started) return;
    state.started = true;
    resumeIdle();
    requestFetch('start');
  }

  const PlaneLayer = {
    init(map) {
      if (!map) {
        throw new Error('PlaneLayer.init requires a map instance');
      }
      if (state.initialized && state.map !== map) {
        PlaneLayer.dispose();
      }
      state.map = map;
      state.initialized = true;
      state.disposed = false;
      ensureLeafletPane(map);
      addMapListener(map, 'zoomstart', handleInteractionStart);
      addMapListener(map, 'movestart', handleInteractionStart);
      addMapListener(map, 'zoomend', handleInteractionEnd);
      addMapListener(map, 'moveend', handleInteractionEnd);
      addMapListener(map, 'zoomanim', refreshAllMarkerRotations);
      addMapListener(map, 'zoom', refreshAllMarkerRotations);
      addMapListener(map, 'zoomend', refreshAllMarkerRotations);
      startInternal();
      return PlaneLayer;
    },
    start() {
      startInternal();
      return PlaneLayer;
    },
    stop() {
      if (!state.started) return PlaneLayer;
      state.started = false;
      pauseIdle();
      clearMoveDebounce();
      clearScheduledFetch();
      abortInflight(SUPERSEDED_REASON);
      return PlaneLayer;
    },
    dispose() {
      PlaneLayer.stop();
      state.pinnedTrackId = null;
      closeAircraftPopup();
      removeTrackPolyline();
      removeAllMapListeners();
      state.markers.forEach(entry => removeMarkerEntry(entry));
      state.markers.clear();
      if (state.markerLayer && typeof state.markerLayer.remove === 'function') {
        state.markerLayer.remove();
      } else if (state.markerLayer && state.map && typeof state.map.removeLayer === 'function') {
        state.map.removeLayer(state.markerLayer);
      }
      state.markerLayer = null;
      state.leafletPaneName = null;
      state.iconCache.clear();
      state.pendingRotationRefresh = false;
      state.map = null;
      state.initialized = false;
      state.disposed = true;
      return PlaneLayer;
    }
  };

  Object.defineProperty(PlaneLayer, 'isStarted', {
    get() {
      return state.started && !state.disposed;
    }
  });

  global.PlaneLayer = PlaneLayer;
})(typeof window !== 'undefined' ? window : globalThis);
