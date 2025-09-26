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
    return `https://opendata.adsb.fi/api//v2/lat/${latStr}/lon/${lonStr}/dist/${distStr}`;
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

  function reapplyLeafletMarkerRotations() {
    if (!isLeafletMap(state.map)) {
      return;
    }
    state.markers.forEach(entry => {
      if (!entry || !entry.marker) {
        return;
      }
      const rotationDeg = Number.isFinite(entry.rotationDeg) ? entry.rotationDeg : 0;
      scheduleLeafletMarkerRotation(entry.marker, rotationDeg);
    });
  }

  function handleInteractionEnd() {
    reapplyLeafletMarkerRotations();
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
      entry = { marker: null, iconKey: null, lastSeen: timestamp, lat, lon };
      state.markers.set(id, entry);
    }
    entry.lastSeen = timestamp;
    entry.lat = lat;
    entry.lon = lon;

    if (!isLeafletMap(state.map)) {
      return;
    }

    const leafletIcon = iconInfo.resources ? iconInfo.resources.leafletIcon : null;
    if (!leafletIcon) {
      return;
    }

    const rotationDeg = Number.isFinite(iconInfo.headingDeg) ? iconInfo.headingDeg : 0;
    const markerOptions = { icon: leafletIcon, interactive: false };
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
