/* global L, polyline */
(function () {
  'use strict';

  const REFRESH_INTERVAL_MS = 5000;
  const UVA_DEFAULT_CENTER = [38.0336, -78.508];
  const UVA_DEFAULT_ZOOM = 14;
  const DEFAULT_ROUTE_COLOR = '#38bdf8';
  const ROUTE_STROKE_WEIGHT = 6;
  const ROUTE_STRIPE_DASH_LENGTH = 16;
  const STOP_ICON_SIZE_PX = 24;

  const BUS_MARKER_SVG_URL = 'busmarker.svg';
  const BUS_MARKER_VIEWBOX_WIDTH = 52.99;
  const BUS_MARKER_VIEWBOX_HEIGHT = 86.99;
  const BUS_MARKER_PIVOT_X = 26.5;
  const BUS_MARKER_PIVOT_Y = 43.49;
  const BUS_MARKER_ASPECT_RATIO = BUS_MARKER_VIEWBOX_HEIGHT / BUS_MARKER_VIEWBOX_WIDTH;
  const BUS_MARKER_WIDTH_PX = 36;
  const BUS_MARKER_HEIGHT_PX = Math.round(BUS_MARKER_WIDTH_PX * BUS_MARKER_ASPECT_RATIO);
  const BUS_MARKER_ANCHOR_X = BUS_MARKER_WIDTH_PX * (BUS_MARKER_PIVOT_X / BUS_MARKER_VIEWBOX_WIDTH);
  const BUS_MARKER_ANCHOR_Y = BUS_MARKER_HEIGHT_PX * (BUS_MARKER_PIVOT_Y / BUS_MARKER_VIEWBOX_HEIGHT);
  const BUS_MARKER_DEFAULT_HEADING = 0;

  function parseAdminMode() {
    try {
      const params = new URLSearchParams(window.location.search);
      if (!params.has('adminMode')) {
        return true;
      }
      const raw = params.get('adminMode');
      if (raw === null) {
        return true;
      }
      const normalized = raw.trim().toLowerCase();
      if (['0', 'false', 'no', 'off'].includes(normalized)) {
        return false;
      }
      return true;
    } catch (error) {
      console.warn('Failed to parse adminMode parameter, defaulting to admin mode.', error);
      return true;
    }
  }

  const adminMode = parseAdminMode();
  if (document && document.body) {
    document.body.dataset.adminMode = adminMode ? 'true' : 'false';
  }

  const map = L.map('map', {
    zoomControl: false,
    scrollWheelZoom: false,
    doubleClickZoom: false,
    boxZoom: false,
    keyboard: false,
    dragging: false,
    touchZoom: false,
    tap: false,
    inertia: false
  }).setView(UVA_DEFAULT_CENTER, UVA_DEFAULT_ZOOM);

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    subdomains: 'abcd',
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
  }).addTo(map);

  const routeLayerGroup = L.layerGroup().addTo(map);
  const stopLayerGroup = L.layerGroup().addTo(map);
  const vehicleLayerGroup = L.layerGroup().addTo(map);
  const labelLayerGroup = L.layerGroup();
  if (adminMode) {
    labelLayerGroup.addTo(map);
  }

  const vehicleMarkers = new Map();
  const vehicleLabels = new Map();
  const routeColors = new Map();
  const stopIconCache = new Map();
  let busMarkerSvgText = null;
  let busMarkerSvgPromise = null;

  function createFallbackBusIcon(color, isStale) {
    const circleClasses = ['bus-icon__circle'];
    if (isStale) {
      circleClasses.push('bus-icon__circle--stale');
    }
    return L.divIcon({
      className: 'bus-icon',
      html: `<div class="${circleClasses.join(' ')}" style="--bus-color:${color}"></div>`,
      iconSize: [BUS_MARKER_WIDTH_PX, BUS_MARKER_WIDTH_PX],
      iconAnchor: [BUS_MARKER_WIDTH_PX / 2, BUS_MARKER_WIDTH_PX / 2]
    });
  }

  function ensureStopIcon(routeIds) {
    const sorted = Array.from(routeIds).sort((a, b) => {
      const aNum = Number(a);
      const bNum = Number(b);
      if (Number.isFinite(aNum) && Number.isFinite(bNum)) {
        return aNum - bNum;
      }
      return String(a).localeCompare(String(b));
    });
    const gradient = buildStopGradient(sorted);
    const cacheKey = `${sorted.join('|')}|${gradient}`;
    if (stopIconCache.has(cacheKey)) {
      return stopIconCache.get(cacheKey);
    }
    const html = [
      '<div class="stop-icon__circle">',
      `<span class="stop-icon__pie" style="background:${gradient}"></span>`,
      '<span class="stop-icon__core"></span>',
      '</div>'
    ].join('');
    const icon = L.divIcon({
      className: 'stop-icon',
      html,
      iconSize: [STOP_ICON_SIZE_PX, STOP_ICON_SIZE_PX],
      iconAnchor: [STOP_ICON_SIZE_PX / 2, STOP_ICON_SIZE_PX / 2]
    });
    stopIconCache.set(cacheKey, icon);
    return icon;
  }

  function buildStopGradient(routeIds) {
    if (!Array.isArray(routeIds) || routeIds.length === 0) {
      return 'radial-gradient(circle at 35% 35%, #ffffff, #e2e8f0)';
    }
    const colors = routeIds.map(id => getRouteColorById(id));
    const slice = 100 / colors.length;
    const segments = colors.map((color, index) => {
      const start = (slice * index).toFixed(2);
      const end = (slice * (index + 1)).toFixed(2);
      return `${color} ${start}% ${end}%`;
    });
    return `conic-gradient(${segments.join(', ')})`;
  }

  async function ensureBusMarkerSvg() {
    if (typeof busMarkerSvgText === 'string' && busMarkerSvgText.trim()) {
      return true;
    }
    if (busMarkerSvgPromise) {
      try {
        await busMarkerSvgPromise;
        return typeof busMarkerSvgText === 'string' && busMarkerSvgText.trim();
      } catch (error) {
        return false;
      }
    }
    busMarkerSvgPromise = fetch(BUS_MARKER_SVG_URL, { cache: 'no-store' })
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        return response.text();
      })
      .then(text => {
        busMarkerSvgText = text;
        return text;
      })
      .catch(error => {
        console.error('Failed to load bus marker SVG asset.', error);
        busMarkerSvgText = null;
        throw error;
      });
    try {
      await busMarkerSvgPromise;
      return typeof busMarkerSvgText === 'string' && busMarkerSvgText.trim();
    } catch (error) {
      return false;
    }
  }

  function createBusMarkerIcon(routeColor, headingDeg, isStale) {
    if (typeof busMarkerSvgText !== 'string' || !busMarkerSvgText.trim()) {
      return null;
    }
    const template = document.createElement('template');
    template.innerHTML = busMarkerSvgText.trim();
    const svg = template.content.firstElementChild;
    if (!svg || svg.tagName.toLowerCase() !== 'svg') {
      return null;
    }
    const normalizedColor = normalizeColor(routeColor);
    const routeShape = svg.querySelector('#route_color');
    if (routeShape) {
      routeShape.setAttribute('fill', normalizedColor);
    }
    const container = document.createElement('div');
    container.className = 'bus-marker-icon';
    container.dataset.routeColor = normalizedColor;
    if (isStale) {
      container.classList.add('bus-marker-icon--stale');
    }
    const heading = Number.isFinite(headingDeg) ? headingDeg : BUS_MARKER_DEFAULT_HEADING;
    const svgWrapper = document.createElement('div');
    svgWrapper.className = 'bus-marker-icon__svg';
    svgWrapper.style.setProperty('--bus-heading', `${heading}deg`);
    svgWrapper.appendChild(svg);
    container.appendChild(svgWrapper);
    return L.divIcon({
      className: 'bus-icon',
      html: container.outerHTML,
      iconSize: [BUS_MARKER_WIDTH_PX, BUS_MARKER_HEIGHT_PX],
      iconAnchor: [BUS_MARKER_ANCHOR_X, BUS_MARKER_ANCHOR_Y]
    });
  }

  function createLabelIcon(lines, color) {
    if (!Array.isArray(lines) || lines.length === 0) {
      return null;
    }
    const primary = escapeHtml(lines[0]);
    const secondary = lines.slice(1).map(segment => `<span class="bus-label__segment">${escapeHtml(segment)}</span>`).join('');
    const textHtml = [`<span class="bus-label__primary">${primary}</span>`, secondary].join('');
    const html = `<div class="bus-label__container" style="--bus-color:${color}"><span class="bus-label__dot"></span><span class="bus-label__text">${textHtml}</span></div>`;
    return L.divIcon({
      className: 'bus-label',
      html,
      iconSize: [0, 0],
      iconAnchor: [0, 0]
    });
  }

  const loadingOverlay = document.getElementById('loadingOverlay');
  const loadingText = loadingOverlay ? loadingOverlay.querySelector('.loading-overlay__text') : null;
  const routeLegendElement = document.getElementById('routeLegend');

  function setLoadingVisible(visible, message) {
    if (!loadingOverlay) {
      return;
    }
    if (typeof message === 'string' && loadingText) {
      loadingText.textContent = message;
    }
    if (visible) {
      loadingOverlay.removeAttribute('hidden');
      loadingOverlay.setAttribute('aria-busy', 'true');
    } else {
      loadingOverlay.setAttribute('hidden', '');
      loadingOverlay.setAttribute('aria-busy', 'false');
    }
  }

  function normalizeColor(candidate) {
    if (typeof candidate !== 'string') {
      return DEFAULT_ROUTE_COLOR;
    }
    const trimmed = candidate.trim();
    if (!trimmed) {
      return DEFAULT_ROUTE_COLOR;
    }
    if (/^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(trimmed)) {
      return trimmed;
    }
    if (/^([0-9a-f]{6})$/i.test(trimmed)) {
      return `#${trimmed}`;
    }
    return DEFAULT_ROUTE_COLOR;
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function toNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function renderRoutes(routes) {
    routeLayerGroup.clearLayers();
    routeColors.clear();
    if (!Array.isArray(routes) || routes.length === 0) {
      return [];
    }

    const boundsPoints = [];
    const routeGeometries = new Map();
    const canDecode = typeof polyline === 'object' && typeof polyline.decode === 'function';

    routes.forEach(route => {
      if (!route || typeof route !== 'object' || route.IsVisibleOnMap === false) {
        return;
      }
      const idRaw = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
      if (idRaw === undefined || idRaw === null) {
        return;
      }
      const id = Number(idRaw);
      const color = normalizeColor(route.MapLineColor || route.Color);
      routeColors.set(String(id), color);

      const encoded = route.EncodedPolyline || route.Polyline || route.encodedPolyline;
      if (!encoded || !canDecode) {
        return;
      }

      let decoded = [];
      try {
        decoded = polyline.decode(encoded);
      } catch (error) {
        console.warn('Failed to decode route polyline', id, error);
        decoded = [];
      }
      if (!decoded || decoded.length < 2) {
        return;
      }

      const latlngs = decoded.map(pair => [pair[0], pair[1]]);
      routeGeometries.set(id, latlngs);
      boundsPoints.push(...latlngs);
    });

    const groups = buildSegmentGroups(routeGeometries);
    groups.forEach(group => {
      const segments = group.segments
        .map(segment => {
          const start = segment.start;
          const end = segment.end;
          if (!start || !end) {
            return null;
          }
          return [
            [start[0], start[1]],
            [end[0], end[1]]
          ];
        })
        .filter(Boolean);
      if (segments.length === 0) {
        return;
      }

      if (group.routes.length === 1) {
        const routeId = group.routes[0];
        const layer = L.polyline(segments, {
          color: getRouteColorById(routeId),
          weight: ROUTE_STROKE_WEIGHT,
          opacity: 0.95,
          lineCap: 'round',
          lineJoin: 'round'
        });
        layer.addTo(routeLayerGroup);
        return;
      }

      const stripeCount = group.routes.length;
      const dashLength = ROUTE_STRIPE_DASH_LENGTH;
      const gapLength = dashLength * (stripeCount - 1);
      group.routes.forEach((routeId, index) => {
        const layer = L.polyline(segments, {
          color: getRouteColorById(routeId),
          weight: ROUTE_STROKE_WEIGHT,
          opacity: 1,
          dashArray: `${dashLength} ${gapLength}`,
          dashOffset: `${dashLength * index}`,
          lineCap: 'butt',
          lineJoin: 'round'
        });
        layer.addTo(routeLayerGroup);
      });
    });

    return boundsPoints;
  }

  function renderStops(stops, activeRouteIds) {
    stopLayerGroup.clearLayers();
    if (!Array.isArray(stops) || stops.length === 0) {
      return;
    }

    const activeSet = activeRouteIds instanceof Set ? activeRouteIds : new Set();
    const requireActiveRoutes = activeSet.size > 0;
    if (!requireActiveRoutes) {
      return;
    }
    const aggregated = new Map();

    stops.forEach(stop => {
      if (!stop || typeof stop !== 'object') {
        return;
      }
      const lat = toNumber(stop.Latitude ?? stop.lat ?? stop.Lat);
      const lon = toNumber(stop.Longitude ?? stop.lon ?? stop.Lon ?? stop.Lng);
      if (lat === null || lon === null) {
        return;
      }

      const stopIdRaw = stop.StopID ?? stop.StopId ?? stop.RouteStopID ?? stop.RouteStopId ?? stop.id;
      const key = stopIdRaw !== undefined && stopIdRaw !== null
        ? String(stopIdRaw)
        : `${lat.toFixed(6)},${lon.toFixed(6)}`;

      let entry = aggregated.get(key);
      if (!entry) {
        entry = {
          lat,
          lon,
          routes: new Set()
        };
        aggregated.set(key, entry);
      }

      const routeIds = extractStopRouteIds(stop);
      routeIds.forEach(routeId => {
        entry.routes.add(routeId);
      });
    });

    aggregated.forEach(entry => {
      if (!entry || entry.lat === null || entry.lon === null) {
        return;
      }
      const routeList = Array.from(entry.routes);
      const relevantRoutes = requireActiveRoutes
        ? routeList.filter(routeId => activeSet.has(routeId))
        : routeList;
      if (relevantRoutes.length === 0) {
        return;
      }
      const icon = ensureStopIcon(new Set(relevantRoutes));
      L.marker([entry.lat, entry.lon], {
        icon,
        interactive: false,
        keyboard: false
      }).addTo(stopLayerGroup);
    });
  }

  function extractStopRouteIds(stop) {
    const ids = new Set();
    if (!stop || typeof stop !== 'object') {
      return ids;
    }

    const direct = stop.RouteID ?? stop.RouteId ?? stop.routeID ?? stop.routeId ?? stop.rid ?? stop.Route;
    if (direct !== undefined && direct !== null) {
      const numeric = toNumber(direct);
      if (numeric !== null) {
        ids.add(numeric);
      }
    }

    const candidates = [stop.RouteIds, stop.RouteIDs, stop.routeIds, stop.routeIDs];
    candidates.forEach(list => {
      if (Array.isArray(list)) {
        list.forEach(value => {
          const numeric = toNumber(value);
          if (numeric !== null) {
            ids.add(numeric);
          }
        });
      }
    });

    const routesArray = stop.Routes ?? stop.routes;
    if (Array.isArray(routesArray)) {
      routesArray.forEach(item => {
        if (typeof item === 'object' && item !== null) {
          const numeric = toNumber(item.RouteID ?? item.RouteId ?? item.routeID ?? item.routeId ?? item.id);
          if (numeric !== null) {
            ids.add(numeric);
          }
        } else {
          const numeric = toNumber(item);
          if (numeric !== null) {
            ids.add(numeric);
          }
        }
      });
    }

    return ids;
  }

  function getRouteColorById(routeId) {
    const key = routeId !== undefined && routeId !== null ? String(routeId) : '';
    if (key && routeColors.has(key)) {
      return routeColors.get(key);
    }
    return DEFAULT_ROUTE_COLOR;
  }

  function buildSegmentGroups(routeGeometries) {
    const segmentMap = new Map();

    routeGeometries.forEach((latlngs, routeId) => {
      if (!Array.isArray(latlngs) || latlngs.length < 2) {
        return;
      }
      for (let index = 0; index < latlngs.length - 1; index += 1) {
        const start = latlngs[index];
        const end = latlngs[index + 1];
        const normalized = normalizeSegmentKey(start, end);
        if (!normalized) {
          continue;
        }
        let entry = segmentMap.get(normalized.baseKey);
        if (!entry) {
          entry = {
            routes: new Set(),
            segments: []
          };
          segmentMap.set(normalized.baseKey, entry);
        }
        entry.routes.add(routeId);
        entry.segments.push({
          routeId,
          start: normalized.forward ? start : end,
          end: normalized.forward ? end : start
        });
      }
    });

    const groups = new Map();

    segmentMap.forEach(entry => {
      if (!entry || entry.segments.length === 0) {
        return;
      }
      const sortedRoutes = Array.from(entry.routes).sort((a, b) => a - b);
      if (sortedRoutes.length === 0) {
        return;
      }
      const signature = sortedRoutes.join('|');
      let group = groups.get(signature);
      if (!group) {
        group = {
          routes: sortedRoutes,
          segments: [],
          seen: new Set()
        };
        groups.set(signature, group);
      }

      const referenceRouteId = sortedRoutes[0];
      let chosen = entry.segments.find(segment => segment.routeId === referenceRouteId);
      if (!chosen) {
        chosen = entry.segments[0];
      }
      if (!chosen || !chosen.start || !chosen.end) {
        return;
      }
      const segmentKey = directionalSegmentKey(chosen.start, chosen.end);
      if (group.seen.has(segmentKey)) {
        return;
      }
      group.seen.add(segmentKey);
      group.segments.push({
        start: chosen.start,
        end: chosen.end
      });
    });

    return Array.from(groups.values());
  }

  function normalizeSegmentKey(start, end) {
    if (!Array.isArray(start) || !Array.isArray(end)) {
      return null;
    }
    const aLat = roundCoord(start[0]);
    const aLng = roundCoord(start[1]);
    const bLat = roundCoord(end[0]);
    const bLng = roundCoord(end[1]);
    if (!Number.isFinite(aLat) || !Number.isFinite(aLng) || !Number.isFinite(bLat) || !Number.isFinite(bLng)) {
      return null;
    }
    if (aLat === bLat && aLng === bLng) {
      return null;
    }
    const forward = aLat < bLat || (aLat === bLat && aLng <= bLng);
    const baseKey = forward
      ? `${aLat},${aLng}|${bLat},${bLng}`
      : `${bLat},${bLng}|${aLat},${aLng}`;
    return { baseKey, forward };
  }

  function directionalSegmentKey(start, end) {
    const aLat = roundCoord(start[0]);
    const aLng = roundCoord(start[1]);
    const bLat = roundCoord(end[0]);
    const bLng = roundCoord(end[1]);
    return `${aLat},${aLng}|${bLat},${bLng}`;
  }

  function roundCoord(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return NaN;
    }
    return Math.round(numeric * 1e6) / 1e6;
  }

  function buildVehicleLabelLines(vehicle, blocks) {
    const lines = [];
    if (vehicle && typeof vehicle.Name === 'string' && vehicle.Name.trim()) {
      lines.push(vehicle.Name.trim());
    }
    if (vehicle && typeof vehicle.VehicleName === 'string' && vehicle.VehicleName.trim() && lines.length === 0) {
      lines.push(vehicle.VehicleName.trim());
    }
    if (blocks && typeof blocks === 'object') {
      const block = blocks[String(vehicle?.VehicleID ?? vehicle?.VehicleId ?? vehicle?.id ?? '')];
      if (typeof block === 'string' && block.trim()) {
        lines.push(block.trim());
      }
    }
    if (lines.length === 0) {
      const routeId = vehicle?.RouteID ?? vehicle?.RouteId ?? vehicle?.routeID;
      if (routeId !== undefined && routeId !== null) {
        lines.push(`Route ${routeId}`);
      }
    }
    return lines;
  }

  function deriveActiveRouteIds(vehicles) {
    const active = new Set();
    if (!Array.isArray(vehicles)) {
      return active;
    }
    vehicles.forEach(vehicle => {
      if (!vehicle || typeof vehicle !== 'object') {
        return;
      }
      const routeIdRaw = vehicle.RouteID ?? vehicle.RouteId ?? vehicle.routeID ?? vehicle.routeId;
      const routeId = toNumber(routeIdRaw);
      if (routeId !== null) {
        active.add(routeId);
      }
    });
    return active;
  }

  async function updateVehicleMarkers(vehicles, blocks) {
    const activeVehicles = new Set();
    if (!Array.isArray(vehicles)) {
      vehicles = [];
    }

    let hasSvgMarker = false;
    if (vehicles.length > 0) {
      try {
        hasSvgMarker = await ensureBusMarkerSvg();
      } catch (error) {
        hasSvgMarker = false;
      }
    }

    vehicles.forEach(vehicle => {
      if (!vehicle || typeof vehicle !== 'object') {
        return;
      }
      const idRaw = vehicle.VehicleID ?? vehicle.VehicleId ?? vehicle.id;
      if (idRaw === undefined || idRaw === null) {
        return;
      }
      const id = String(idRaw);
      const lat = toNumber(vehicle.Latitude ?? vehicle.lat ?? vehicle.Lat);
      const lon = toNumber(vehicle.Longitude ?? vehicle.lon ?? vehicle.Lon ?? vehicle.Lng);
      if (lat === null || lon === null) {
        return;
      }
      activeVehicles.add(id);
      const routeId = vehicle.RouteID ?? vehicle.RouteId ?? vehicle.routeID;
      const color = getRouteColorById(routeId);
      const marker = vehicleMarkers.get(id);
      const heading = toNumber(vehicle.Heading ?? vehicle.heading ?? vehicle.h);
      let icon = null;
      if (hasSvgMarker) {
        icon = createBusMarkerIcon(color, heading ?? BUS_MARKER_DEFAULT_HEADING, Boolean(vehicle.IsStale));
      }
      if (!icon) {
        icon = createFallbackBusIcon(color, Boolean(vehicle.IsStale));
      }
      if (marker) {
        marker.setLatLng([lat, lon]);
        marker.setIcon(icon);
      } else {
        const newMarker = L.marker([lat, lon], {
          icon,
          interactive: false,
          keyboard: false
        });
        newMarker.addTo(vehicleLayerGroup);
        vehicleMarkers.set(id, newMarker);
      }

      if (adminMode) {
        const lines = buildVehicleLabelLines(vehicle, blocks);
        const labelIcon = createLabelIcon(lines, color);
        const existingLabel = vehicleLabels.get(id);
        if (labelIcon) {
          if (existingLabel) {
            existingLabel.setLatLng([lat, lon]);
            existingLabel.setIcon(labelIcon);
          } else {
            const labelMarker = L.marker([lat, lon], {
              icon: labelIcon,
              interactive: false,
              keyboard: false,
              zIndexOffset: 500
            });
            labelMarker.addTo(labelLayerGroup);
            vehicleLabels.set(id, labelMarker);
          }
        } else if (existingLabel) {
          map.removeLayer(existingLabel);
          vehicleLabels.delete(id);
        }
      }
    });

    vehicleMarkers.forEach((marker, id) => {
      if (!activeVehicles.has(id)) {
        map.removeLayer(marker);
        vehicleMarkers.delete(id);
      }
    });

    if (adminMode) {
      vehicleLabels.forEach((marker, id) => {
        if (!activeVehicles.has(id)) {
          map.removeLayer(marker);
          vehicleLabels.delete(id);
        }
      });
    } else if (vehicleLabels.size > 0) {
      vehicleLabels.forEach(marker => {
        map.removeLayer(marker);
      });
      vehicleLabels.clear();
    }
  }

  function describeRoute(route) {
    if (!route || typeof route !== 'object') {
      return { name: 'Route', detail: '' };
    }
    const shortName = typeof route.ShortName === 'string' && route.ShortName.trim() ? route.ShortName.trim() : '';
    const description = typeof route.Description === 'string' && route.Description.trim() ? route.Description.trim() : '';
    const longName = typeof route.LongName === 'string' && route.LongName.trim() ? route.LongName.trim() : '';
    const routeName = typeof route.RouteName === 'string' && route.RouteName.trim() ? route.RouteName.trim() : '';

    let primary = shortName || routeName || description || longName || 'Route';
    let detail = '';
    if (primary === shortName && (description || longName)) {
      detail = description || longName;
    } else if (primary === routeName && description && description !== routeName) {
      detail = description;
    } else if (!shortName && description && longName && description !== longName) {
      primary = description;
      detail = longName;
    }
    return { name: primary, detail };
  }

  function updateRouteLegend(routes) {
    if (!routeLegendElement) {
      return;
    }
    if (!Array.isArray(routes) || routes.length === 0) {
      routeLegendElement.innerHTML = '';
      routeLegendElement.style.display = 'none';
      return;
    }
    const entries = routes
      .filter(route => route && route.IsVisibleOnMap !== false)
      .map(route => {
        const id = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
        const color = routeColors.get(String(id ?? '')) || normalizeColor(route.MapLineColor || route.Color);
        const { name, detail } = describeRoute(route);
        return {
          id: String(id ?? name),
          color,
          name,
          detail
        };
      })
      .sort((a, b) => a.name.localeCompare(b.name));
    const html = entries
      .map(entry => {
        const detailHtml = entry.detail ? `<span class="route-legend__details">${escapeHtml(entry.detail)}</span>` : '';
        return `<div class="route-legend__item"><span class="route-legend__swatch" style="--route-color:${entry.color}"></span><span class="route-legend__label"><span class="route-legend__name">${escapeHtml(entry.name)}</span>${detailHtml}</span></div>`;
      })
      .join('');
    routeLegendElement.innerHTML = html;
    routeLegendElement.style.display = html ? '' : 'none';
  }

  let hasInitialView = false;
  let refreshTimerId = null;
  let hasRenderedSnapshot = false;

  async function applySnapshot(snapshot) {
    const routes = Array.isArray(snapshot?.routes) ? snapshot.routes : [];
    const stops = Array.isArray(snapshot?.stops) ? snapshot.stops : [];
    const vehicles = Array.isArray(snapshot?.vehicles) ? snapshot.vehicles : [];
    const blocks = snapshot && typeof snapshot.blocks === 'object' ? snapshot.blocks : {};

    const activeRouteIds = deriveActiveRouteIds(vehicles);
    const filteredRoutes = routes.filter(route => {
      if (!route || typeof route !== 'object') {
        return false;
      }
      if (route.IsVisibleOnMap === false) {
        return false;
      }
      const idRaw = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
      const routeId = toNumber(idRaw);
      if (routeId === null) {
        return false;
      }
      if (activeRouteIds.size === 0) {
        return false;
      }
      return activeRouteIds.has(routeId);
    });

    const boundsPoints = renderRoutes(filteredRoutes);
    renderStops(stops, activeRouteIds);
    await updateVehicleMarkers(vehicles, blocks);
    updateRouteLegend(filteredRoutes);

    if (!hasInitialView) {
      if (boundsPoints.length > 0) {
        const bounds = L.latLngBounds(boundsPoints);
        map.fitBounds(bounds, { padding: [48, 48], maxZoom: 16 });
      } else {
        map.setView(UVA_DEFAULT_CENTER, UVA_DEFAULT_ZOOM);
      }
      hasInitialView = true;
    }
  }

  async function fetchSnapshot() {
    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    const timeoutId = controller ? window.setTimeout(() => controller.abort(), 15000) : null;
    try {
      const response = await fetch('/v1/testmap/transloc', {
        cache: 'no-store',
        signal: controller ? controller.signal : undefined
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const snapshot = await response.json();
      await applySnapshot(snapshot);
      if (!hasRenderedSnapshot) {
        setLoadingVisible(false);
      }
      hasRenderedSnapshot = true;
    } catch (error) {
      console.error('Failed to refresh UVA kiosk snapshot', error);
      if (!hasRenderedSnapshot) {
        setLoadingVisible(true, 'Unable to load data. Retrying…');
      }
    } finally {
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
      scheduleNextRefresh();
    }
  }

  function scheduleNextRefresh() {
    if (refreshTimerId !== null) {
      window.clearTimeout(refreshTimerId);
    }
    refreshTimerId = window.setTimeout(fetchSnapshot, REFRESH_INTERVAL_MS);
  }

  setLoadingVisible(true, 'Loading UVA buses…');
  fetchSnapshot();
})();
