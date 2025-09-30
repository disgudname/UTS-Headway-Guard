/* global L, polyline */
(function () {
  'use strict';

  const REFRESH_INTERVAL_MS = 5000;
  const UVA_DEFAULT_CENTER = [38.0336, -78.508];
  const UVA_DEFAULT_ZOOM = 14;
  const DEFAULT_ROUTE_COLOR = '#38bdf8';

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

  const stopIcon = L.divIcon({
    className: 'stop-icon',
    html: '<div class="stop-icon__circle"></div>',
    iconSize: [20, 20],
    iconAnchor: [10, 10]
  });

  function createBusIcon(color, isStale) {
    const circleClasses = ['bus-icon__circle'];
    if (isStale) {
      circleClasses.push('bus-icon__circle--stale');
    }
    return L.divIcon({
      className: 'bus-icon',
      html: `<div class="${circleClasses.join(' ')}" style="--bus-color:${color}"></div>`,
      iconSize: [32, 32],
      iconAnchor: [16, 16]
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
    const canDecode = typeof polyline === 'object' && typeof polyline.decode === 'function';
    routes.forEach(route => {
      if (!route || typeof route !== 'object') {
        return;
      }
      const id = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
      const color = normalizeColor(route.MapLineColor || route.Color);
      if (id !== undefined && id !== null) {
        routeColors.set(String(id), color);
      }
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
      if (!decoded || decoded.length === 0) {
        return;
      }
      const latlngs = decoded.map(pair => [pair[0], pair[1]]);
      boundsPoints.push(...latlngs);
      L.polyline(latlngs, {
        color,
        weight: 6,
        opacity: 0.92,
        lineCap: 'round',
        lineJoin: 'round'
      }).addTo(routeLayerGroup);
    });
    return boundsPoints;
  }

  function renderStops(stops) {
    stopLayerGroup.clearLayers();
    if (!Array.isArray(stops) || stops.length === 0) {
      return;
    }
    const seen = new Set();
    stops.forEach(stop => {
      if (!stop || typeof stop !== 'object') {
        return;
      }
      const lat = toNumber(stop.Latitude ?? stop.lat ?? stop.Lat);
      const lon = toNumber(stop.Longitude ?? stop.lon ?? stop.Lon ?? stop.Lng);
      if (lat === null || lon === null) {
        return;
      }
      const stopKeyRaw = stop.StopID ?? stop.StopId ?? stop.RouteStopID ?? stop.RouteStopId;
      const stopKey = stopKeyRaw !== undefined && stopKeyRaw !== null ? String(stopKeyRaw) : `${lat.toFixed(6)},${lon.toFixed(6)}`;
      if (seen.has(stopKey)) {
        return;
      }
      seen.add(stopKey);
      L.marker([lat, lon], {
        icon: stopIcon,
        interactive: false,
        keyboard: false
      }).addTo(stopLayerGroup);
    });
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

  function updateVehicleMarkers(vehicles, blocks) {
    const activeVehicles = new Set();
    if (!Array.isArray(vehicles)) {
      vehicles = [];
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
      const color = routeColors.get(String(routeId ?? '')) || DEFAULT_ROUTE_COLOR;
      const marker = vehicleMarkers.get(id);
      const icon = createBusIcon(color, Boolean(vehicle.IsStale));
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

  function applySnapshot(snapshot) {
    const routes = Array.isArray(snapshot?.routes) ? snapshot.routes : [];
    const stops = Array.isArray(snapshot?.stops) ? snapshot.stops : [];
    const vehicles = Array.isArray(snapshot?.vehicles) ? snapshot.vehicles : [];
    const blocks = snapshot && typeof snapshot.blocks === 'object' ? snapshot.blocks : {};

    const boundsPoints = renderRoutes(routes);
    renderStops(stops);
    updateVehicleMarkers(vehicles, blocks);
    updateRouteLegend(routes);

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
      applySnapshot(snapshot);
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
