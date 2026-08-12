/* global L, SunCalc */
(function () {
  'use strict';

  // ---------------------------
  // Query params
  // ---------------------------

  const params = new URLSearchParams(window.location.search);
  const kioskParam = params.get('kioskMode');
  const kioskMode = kioskParam !== null && kioskParam.toLowerCase() === 'true';

  // ---------------------------
  // Constants
  // ---------------------------

  const ALEXANDRIA_CENTER = [38.8048, -77.0469];
  const ALEXANDRIA_ZOOM = 13;

  const METADATA_REFRESH_MS = 10 * 60 * 1000;
  const VEHICLE_REFRESH_MS = 10 * 1000;
  // Vehicles with no fix in this window are dropped, not just dimmed. DASH's OBA
  // feed was measured (live, 6 polls over 75s) to genuinely refresh individual
  // vehicles' positions only every ~25-50s, not sub-15s like typical AVL systems
  // — a tighter cutoff makes buses flicker in and out of view every poll cycle.
  const STALE_THRESHOLD_MS = 90 * 1000;
  const PANEL_COLLAPSE_BREAKPOINT = 600;

  const ROUTES_ENDPOINT = '/v1/testmap/dash/routes';
  const SHAPES_ENDPOINT = '/v1/testmap/dash/shapes';
  const VEHICLES_ENDPOINT = '/v1/testmap/dash/vehicles';

  const DEFAULT_ROUTE_COLOR = '#000000';
  const DEFAULT_ROUTE_STROKE_WEIGHT = 6;
  const MIN_ROUTE_STROKE_WEIGHT = 3;
  const MAX_ROUTE_STROKE_WEIGHT = 12;
  const ROUTE_WEIGHT_ZOOM_DELTA_LIMIT = 3;
  const ROUTE_WEIGHT_BASE_ZOOM = 15;
  const ROUTE_WEIGHT_STEP_PER_ZOOM = 1;
  const STOP_MARKER_ICON_SIZE = 24;
  const STOP_MARKER_OUTLINE_COLOR = '#FFFFFF';
  const STOP_MARKER_OUTLINE_WIDTH = 2;
  const STOP_MARKER_BORDER_COLOR = 'rgba(15,23,42,0.55)';
  const STOP_MARKER_BORDER_WIDTH = 2;

  const BUS_MARKER_SVG_URL = 'busmarker.svg';
  const BUS_MARKER_VIEWBOX_WIDTH = 52.99;
  const BUS_MARKER_VIEWBOX_HEIGHT = 86.99;
  const BUS_MARKER_ASPECT_RATIO = BUS_MARKER_VIEWBOX_HEIGHT / BUS_MARKER_VIEWBOX_WIDTH;
  const BUS_MARKER_BASE_WIDTH_PX = 26;
  const BUS_MARKER_ICON_ANCHOR_X_RATIO = 0.5;
  const BUS_MARKER_ICON_ANCHOR_Y_RATIO = 0.5;
  const BUS_MARKER_TRANSFORM_ORIGIN = '50% 50%';
  const BUS_MARKER_DEFAULT_ROUTE_COLOR = '#0B7A26';
  const BUS_MARKER_DEFAULT_CONTRAST_COLOR = '#FFFFFF';

  const MAP_THEMES = Object.freeze({ LIGHT: 'light', DARK: 'dark', AUTO: 'auto' });
  const MAP_THEME_STORAGE_KEY = 'dashmap_theme_preference';
  const SECTION_STORAGE_PREFIX = 'dashmap:section:';
  const SOLAR_THEME_LAT = ALEXANDRIA_CENTER[0];
  const SOLAR_THEME_LON = ALEXANDRIA_CENTER[1];

  const POPUP_OPTIONS = {
    className: 'ondemand-driver-popup',
    closeButton: true,
    autoClose: true,
    autoPan: true,
  };

  // ---------------------------
  // Small helpers
  // ---------------------------

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function normalizeColor(candidate, fallback) {
    const fb = fallback || DEFAULT_ROUTE_COLOR;
    if (typeof candidate !== 'string') return fb;
    const trimmed = candidate.trim();
    if (!trimmed) return fb;
    if (/^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(trimmed)) return trimmed;
    if (/^[0-9a-f]{6}$/i.test(trimmed)) return `#${trimmed}`;
    return fb;
  }

  function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max);
  }

  function computeRouteStrokeWeight(zoom) {
    const targetZoom = Number.isFinite(zoom) ? zoom : map.getZoom();
    if (!Number.isFinite(targetZoom)) {
      return clamp(DEFAULT_ROUTE_STROKE_WEIGHT, MIN_ROUTE_STROKE_WEIGHT, MAX_ROUTE_STROKE_WEIGHT);
    }
    const zoomDeltaRaw = targetZoom - ROUTE_WEIGHT_BASE_ZOOM;
    const limitedDelta = clamp(zoomDeltaRaw, -ROUTE_WEIGHT_ZOOM_DELTA_LIMIT, ROUTE_WEIGHT_ZOOM_DELTA_LIMIT);
    const computed = DEFAULT_ROUTE_STROKE_WEIGHT + ROUTE_WEIGHT_STEP_PER_ZOOM * limitedDelta;
    return clamp(Number.isFinite(computed) ? computed : DEFAULT_ROUTE_STROKE_WEIGHT, MIN_ROUTE_STROKE_WEIGHT, MAX_ROUTE_STROKE_WEIGHT);
  }

  // ---------------------------
  // Map init
  // ---------------------------

  const map = L.map('map').setView(ALEXANDRIA_CENTER, ALEXANDRIA_ZOOM);

  const tileAttribution = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>';
  const lightTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', { attribution: tileAttribution });
  const darkTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', { attribution: tileAttribution });
  lightTileLayer.addTo(map);

  const routePane = map.createPane('routes');
  routePane.style.zIndex = '200';
  const stopPane = map.createPane('stops');
  stopPane.style.zIndex = '300';
  const vehiclePane = map.createPane('vehicles');
  vehiclePane.style.zIndex = '400';

  const stopLayerGroup = L.layerGroup().addTo(map);
  const vehicleLayerGroup = L.layerGroup().addTo(map);

  const routePaneName = 'routes';
  let sharedRouteRenderer = null;
  if (typeof L.svg === 'function') {
    try {
      sharedRouteRenderer = L.svg({ padding: 0, pane: routePaneName });
      map.addLayer(sharedRouteRenderer);
    } catch (error) {
      console.warn('Failed to initialize shared SVG renderer for routes.', error);
      sharedRouteRenderer = null;
    }
  }

  const ROUTE_LAYER_BASE_OPTIONS = Object.freeze({
    updateWhenZooming: true,
    updateWhenIdle: true,
    interactive: false,
  });

  function mergeRouteLayerOptions(overrides) {
    const base = Object.assign({}, ROUTE_LAYER_BASE_OPTIONS);
    if (sharedRouteRenderer) base.renderer = sharedRouteRenderer;
    base.pane = routePaneName;
    return Object.assign(base, overrides || {});
  }

  function createSpatialIndex() {
    if (typeof rbush === 'function') {
      try {
        return rbush();
      } catch (error) {
        console.error('Failed to create rbush index via rbush().', error);
      }
    }
    if (typeof RBush === 'function') {
      try {
        return new RBush();
      } catch (error) {
        console.error('Failed to create rbush index via new RBush().', error);
      }
    }
    console.error('RBush spatial index library is not available. Route overlap rendering will be disabled.');
    return null;
  }

  function isSpatialIndexAvailable() {
    return typeof rbush === 'function' || typeof RBush === 'function';
  }

  // A "route key" here is `${routeId}__${polylineIndex}` — DASH routes can have
  // several genuinely distinct (deduped) polyline segments, and this renderer
  // (ported from testmap.js's OverlapRouteRenderer) only accepts one continuous
  // geometry per key, so each segment is registered as its own key. getRouteColor
  // strips the suffix back to the real route id for coloring, and the renderer's
  // self-overlap guard is patched to compare real route ids (not raw keys) so two
  // segments belonging to the SAME route are never mistaken for two different
  // routes sharing a corridor.
  function routeIdFromKey(key) {
    const idx = String(key).lastIndexOf('__');
    return idx === -1 ? key : String(key).slice(0, idx);
  }

  function getRouteColor(routeKey) {
    const route = routesById.get(routeIdFromKey(routeKey));
    return normalizeColor(route && route.color, DEFAULT_ROUTE_COLOR);
  }

  // ---------------------------
  // Overlap-aware route renderer (ported from testmap.js's OverlapRouteRenderer,
  // adapted for string route keys instead of numeric TransLoc route ids, and
  // for multiple polyline segments per route via composite keys — see above)
  // ---------------------------

  class OverlapRouteRenderer {
    constructor(mapInstance, options = {}) {
      this.map = mapInstance;
      this.options = Object.assign({
        sampleStepPx: 8,
        dashLengthPx: 16,
        minDashLengthPx: 0.5,
        matchTolerancePx: 6,
        headingToleranceDeg: 20,
        simplifyTolerancePx: 0.75,
        latLngEqualityMargin: 1e-9,
        strokeWeight: DEFAULT_ROUTE_STROKE_WEIGHT,
        minStrokeWeight: MIN_ROUTE_STROKE_WEIGHT,
        maxStrokeWeight: MAX_ROUTE_STROKE_WEIGHT,
      }, options);
      this.layers = [];
      this.routeGeometries = new Map();
      this.selectedRoutes = [];
      this.currentZoom = typeof mapInstance?.getZoom === 'function' ? mapInstance.getZoom() : null;
      this.renderer = options.renderer || null;
      this.routePaneName = typeof options.pane === 'string' && options.pane ? options.pane : routePaneName;
      this.routeGeometrySignatures = new Map();
      this.lastRenderState = null;
    }

    reset() {
      this.clearLayers();
      this.routeGeometries.clear();
      this.selectedRoutes = [];
      this.routeGeometrySignatures.clear();
      this.lastRenderState = null;
    }

    clearLayers() {
      this.layers.forEach((layer) => {
        if (layer && this.map.hasLayer(layer)) this.map.removeLayer(layer);
      });
      this.layers = [];
    }

    updateRoutes(routeGeometryMap, selectedRouteIds) {
      if (!Array.isArray(selectedRouteIds) || selectedRouteIds.length === 0) {
        this.reset();
        return this.getLayers();
      }

      const geometryEntries = routeGeometryMap instanceof Map
        ? Array.from(routeGeometryMap.entries())
        : Object.entries(routeGeometryMap || {});

      const desiredIds = new Set(selectedRouteIds.filter((id) => id !== undefined && id !== null && id !== ''));

      const nextGeometries = new Map();
      geometryEntries.forEach(([key, value]) => {
        if (desiredIds.has(key) && Array.isArray(value)) nextGeometries.set(key, value);
      });

      const geometrySignatures = new Map();
      this.routeGeometries = nextGeometries;
      this.selectedRoutes = Array.from(this.routeGeometries.keys()).sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));

      this.routeGeometries.forEach((latlngs, routeId) => {
        geometrySignatures.set(routeId, this.computeRouteGeometrySignature(latlngs));
      });
      this.routeGeometrySignatures = geometrySignatures;

      const mapZoom = typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null;
      if (Number.isFinite(mapZoom)) this.currentZoom = mapZoom;

      this.render();
      return this.getLayers();
    }

    handleZoomFrame(targetZoom) {
      if (this.routeGeometries.size === 0 || this.selectedRoutes.length === 0) return this.getLayers();
      const zoom = Number.isFinite(targetZoom) ? targetZoom : (typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null);
      if (!Number.isFinite(zoom)) return this.getLayers();
      this.currentZoom = zoom;
      this.render();
      return this.getLayers();
    }

    handleZoomEnd() {
      const zoom = typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null;
      return this.handleZoomFrame(zoom);
    }

    getLayers() {
      return this.layers.slice();
    }

    computeStrokeWeight(zoom = this.currentZoom) {
      const minWeight = Number.isFinite(this.options.minStrokeWeight) ? this.options.minStrokeWeight : MIN_ROUTE_STROKE_WEIGHT;
      const maxWeight = Number.isFinite(this.options.maxStrokeWeight) ? this.options.maxStrokeWeight : MAX_ROUTE_STROKE_WEIGHT;
      const computed = computeRouteStrokeWeight(zoom);
      if (!Number.isFinite(computed)) return clamp(DEFAULT_ROUTE_STROKE_WEIGHT, minWeight, maxWeight);
      return clamp(computed, minWeight, maxWeight);
    }

    computeRouteGeometrySignature(latlngs) {
      if (!Array.isArray(latlngs) || latlngs.length === 0) return 'empty';
      const totalPoints = latlngs.length;
      const sampleCount = Math.min(totalPoints, 10);
      const step = Math.max(1, Math.floor(totalPoints / sampleCount));
      const parts = [totalPoints];
      const extractCoordinate = (point, key) => {
        if (Array.isArray(point) && point.length >= 2) {
          const index = key === 'lat' ? 0 : 1;
          const value = Number(point[index]);
          return Number.isFinite(value) ? value : Number.NaN;
        }
        return point && typeof point[key] === 'number' ? point[key] : Number.NaN;
      };
      const appendPoint = (point) => {
        const lat = extractCoordinate(point, 'lat');
        const lng = extractCoordinate(point, 'lng');
        const format = (value) => (Number.isFinite(value) ? value.toFixed(6) : 'nan');
        parts.push(`${format(lat)},${format(lng)}`);
      };
      for (let i = 0; i < totalPoints; i += step) appendPoint(latlngs[i]);
      const lastPoint = latlngs[totalPoints - 1];
      if (lastPoint && (totalPoints - 1) % step !== 0) appendPoint(lastPoint);
      return parts.join('|');
    }

    render() {
      if (!this.map) return;
      const zoom = Number.isFinite(this.currentZoom) ? this.currentZoom : (typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null);

      const selectionKey = this.selectedRoutes.join(',');
      const geometrySignature = this.selectedRoutes.map((routeId) => `${routeId}:${this.routeGeometrySignatures.get(routeId) || ''}`).join('|');
      const colorSignature = this.selectedRoutes.map((routeId) => `${routeId}:${getRouteColor(routeId) || ''}`).join('|');
      const zoomKey = Number.isFinite(zoom) ? zoom.toFixed(6) : 'NaN';
      const nextRenderState = { selectionKey, geometrySignature, colorSignature, zoomKey, didRender: false };

      if (this.routeGeometries.size === 0 || this.selectedRoutes.length === 0) {
        this.clearLayers();
        nextRenderState.didRender = true;
        this.lastRenderState = nextRenderState;
        return;
      }
      if (!Number.isFinite(zoom)) {
        this.clearLayers();
        this.lastRenderState = nextRenderState;
        return;
      }

      const lastState = this.lastRenderState;
      if (lastState && lastState.didRender
        && lastState.selectionKey === selectionKey
        && lastState.geometrySignature === geometrySignature
        && lastState.colorSignature === colorSignature
        && lastState.zoomKey === zoomKey) {
        return;
      }

      this.clearLayers();

      const step = Number.isFinite(this.options.sampleStepPx) && this.options.sampleStepPx > 0 ? this.options.sampleStepPx : 8;
      const tolerance = Number.isFinite(this.options.matchTolerancePx) ? this.options.matchTolerancePx : 6;
      const headingToleranceRad = (Number.isFinite(this.options.headingToleranceDeg) ? this.options.headingToleranceDeg : 20) * Math.PI / 180;

      const segmentsByRoute = new Map();
      const spatialItems = [];

      this.routeGeometries.forEach((latlngs, routeId) => {
        if (!Array.isArray(latlngs) || latlngs.length < 2) return;
        const segments = this.resampleRoute(routeId, latlngs, zoom, step);
        if (!Array.isArray(segments) || segments.length === 0) return;
        segmentsByRoute.set(routeId, segments);
        segments.forEach((segment) => {
          spatialItems.push({
            minX: segment.bounds.minX - tolerance,
            minY: segment.bounds.minY - tolerance,
            maxX: segment.bounds.maxX + tolerance,
            maxY: segment.bounds.maxY + tolerance,
            segment,
          });
        });
      });

      if (spatialItems.length === 0) {
        nextRenderState.didRender = true;
        this.lastRenderState = nextRenderState;
        return;
      }

      const tree = createSpatialIndex();
      if (!tree || typeof tree.load !== 'function' || typeof tree.search !== 'function') {
        console.error('RBush spatial index instance is invalid; skipping overlap rendering.');
        this.lastRenderState = nextRenderState;
        return;
      }

      tree.load(spatialItems);
      this.populateSharedRoutes(spatialItems, tree, tolerance, headingToleranceRad);

      const groups = this.buildGroups(segmentsByRoute, zoom);
      this.drawGroups(groups);
      nextRenderState.didRender = true;
      this.lastRenderState = nextRenderState;
    }

    populateSharedRoutes(spatialItems, tree, tolerance, headingToleranceRad) {
      const processedPairs = new Set();
      spatialItems.forEach((item) => {
        const segment = item.segment;
        if (!segment) return;
        const candidates = tree.search(item);
        candidates.forEach((candidate) => {
          const other = candidate.segment;
          if (!other || other === segment) return;
          // Compare the REAL route id, not the raw composite key — two polyline
          // segments belonging to the same DASH route (e.g. "1_35__0" and
          // "1_35__1") must never be treated as two different routes sharing a
          // corridor, or every route with >1 deduped segment stripes against
          // itself, fragmenting into hundreds of tiny groups.
          if (routeIdFromKey(other.routeId) === routeIdFromKey(segment.routeId)) return;
          const pairKey = String(segment.routeId) < String(other.routeId)
            ? `${segment.routeId}:${segment.index}|${other.routeId}:${other.index}`
            : `${other.routeId}:${other.index}|${segment.routeId}:${segment.index}`;
          if (processedPairs.has(pairKey)) return;
          processedPairs.add(pairKey);
          if (!this.segmentsOverlap(segment, other, tolerance, headingToleranceRad)) return;
          segment.sharedRoutes.add(other.routeId);
          other.sharedRoutes.add(segment.routeId);
          this.applyRouteOffset(segment, other);
          this.applyRouteOffset(other, segment);
        });
      });
    }

    applyRouteOffset(target, source) {
      if (!target || !source) return;
      if (!target.routeOffsets) target.routeOffsets = {};
      const sourceOffset = this.extractRouteOffset(source, source.routeId);
      if (!Number.isFinite(sourceOffset)) return;
      const existing = target.routeOffsets[source.routeId];
      const candidate = Number.isFinite(existing?.min) ? Math.min(existing.min, sourceOffset) : sourceOffset;
      target.routeOffsets[source.routeId] = { min: candidate };
    }

    extractRouteOffset(segment, routeId) {
      if (!segment) return null;
      const offsets = segment.routeOffsets || {};
      const direct = offsets[routeId];
      if (direct && Number.isFinite(direct.min)) return direct.min;
      const values = [];
      const startVal = Number(segment.start?.cumulativeLength);
      if (Number.isFinite(startVal)) values.push(startVal);
      const endVal = Number(segment.end?.cumulativeLength);
      if (Number.isFinite(endVal)) values.push(endVal);
      return values.length > 0 ? Math.min(...values) : null;
    }

    buildGroups(segmentsByRoute, zoom) {
      const groups = [];
      segmentsByRoute.forEach((segments, routeId) => {
        const ordered = segments.slice().sort((a, b) => (Number(a.start?.cumulativeLength) || 0) - (Number(b.start?.cumulativeLength) || 0));
        let current = null;
        ordered.forEach((segment) => {
          const sharedRoutes = Array.from(segment.sharedRoutes || []).sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));
          if (sharedRoutes.length === 0) return;
          const primary = sharedRoutes[0];
          if (primary !== routeId) return;

          const needsNewGroup = !current
            || !this.sameRouteSet(current.routes, sharedRoutes)
            || !this.latLngsClose(current.lastLatLng, segment.start.latlng);

          if (needsNewGroup) {
            if (current) {
              const finalized = this.finalizeGroup(current, zoom);
              if (finalized) groups.push(finalized);
            }
            current = { routes: sharedRoutes, segments: [], points: [], offsets: new Map(), lastLatLng: null };
          }

          if (current.points.length === 0) {
            current.points.push(segment.start.latlng);
          } else if (!this.latLngsClose(current.points[current.points.length - 1], segment.start.latlng)) {
            current.points.push(segment.start.latlng);
          }
          current.points.push(segment.end.latlng);
          current.lastLatLng = segment.end.latlng;
          current.segments.push(segment);

          const routeOffsets = segment.routeOffsets || {};
          current.routes.forEach((routeKey) => {
            const candidate = Number(routeOffsets?.[routeKey]?.min ?? routeOffsets?.[routeKey]);
            if (Number.isFinite(candidate)) {
              const existing = current.offsets.get(routeKey);
              if (!Number.isFinite(existing) || candidate < existing) current.offsets.set(routeKey, candidate);
            }
          });
        });
        if (current) {
          const finalized = this.finalizeGroup(current, zoom);
          if (finalized) groups.push(finalized);
          current = null;
        }
      });
      return groups;
    }

    finalizeGroup(group) {
      const points = this.collapsePoints(group.points || []);
      if (points.length < 2) return null;
      const lengthPx = group.segments.reduce((sum, segment) => sum + (Number.isFinite(segment.lengthPx) ? segment.lengthPx : 0), 0);
      const primaryRoute = group.routes[0];
      const offsetCandidates = group.segments
        .map((segment) => Number(segment.routeOffsets?.[primaryRoute]?.min ?? segment.routeOffsets?.[primaryRoute]))
        .filter((value) => Number.isFinite(value));
      const offsetPx = offsetCandidates.length > 0 ? Math.min(...offsetCandidates) : 0;
      const offsetMap = new Map();
      group.offsets.forEach((value, key) => {
        if (Number.isFinite(value)) offsetMap.set(key, value);
      });
      return { routes: group.routes.slice(), points, lengthPx, offsetPx, routeOffsets: offsetMap };
    }

    collapsePoints(points) {
      const collapsed = [];
      points.forEach((point) => {
        if (collapsed.length === 0 || !this.latLngsClose(collapsed[collapsed.length - 1], point)) collapsed.push(point);
      });
      return collapsed;
    }

    sameRouteSet(a, b) {
      if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
      for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
      return true;
    }

    latLngsClose(a, b) {
      if (!a || !b) return false;
      const tolerance = this.options.latLngEqualityMargin || 1e-9;
      const latA = a.lat ?? a?.latlng?.lat ?? 0;
      const lngA = a.lng ?? a?.latlng?.lng ?? 0;
      const latB = b.lat ?? b?.latlng?.lat ?? 0;
      const lngB = b.lng ?? b?.latlng?.lng ?? 0;
      return Math.abs(latA - latB) <= tolerance && Math.abs(lngA - lngB) <= tolerance;
    }

    drawGroups(groups) {
      const newLayers = [];
      const dashBase = this.options.dashLengthPx;
      const minDash = this.options.minDashLengthPx;
      const weight = this.computeStrokeWeight();

      groups.forEach((group) => {
        if (!group || !Array.isArray(group.routes) || group.routes.length === 0) return;
        if (!Array.isArray(group.points) || group.points.length < 2) return;

        const coords = group.points.map((latlng) => [latlng.lat, latlng.lng]);
        const sortedRoutes = group.routes.slice().sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));
        const offsetsByRoute = new Map();
        group.routeOffsets.forEach((value, routeId) => {
          const numericValue = Number(value);
          if (Number.isFinite(numericValue)) {
            const existing = offsetsByRoute.get(routeId);
            if (!Number.isFinite(existing) || numericValue < existing) offsetsByRoute.set(routeId, numericValue);
          }
        });

        if (sortedRoutes.length === 1) {
          const routeId = sortedRoutes[0];
          const layer = L.polyline(coords, mergeRouteLayerOptions({
            color: getRouteColor(routeId),
            weight,
            opacity: 1,
            lineCap: 'round',
            lineJoin: 'round',
          })).addTo(this.map);
          newLayers.push(layer);
          return;
        }

        const groupLength = group.lengthPx || 0;
        if (!(groupLength > 0)) return;
        const stripeCount = sortedRoutes.length;

        // A group shorter than roughly one full dash pattern can only produce
        // sub-pixel, invisible dashes (dashLength shrinks to groupLength /
        // stripeCount) — that's pure render cost for nothing. Draw it as one
        // plain line in the "primary" route's color instead of a degenerate
        // stripe too small to actually read as striped.
        if (groupLength < dashBase * stripeCount) {
          const layer = L.polyline(coords, mergeRouteLayerOptions({
            color: getRouteColor(sortedRoutes[0]),
            weight,
            opacity: 1,
            lineCap: 'round',
            lineJoin: 'round',
          })).addTo(this.map);
          newLayers.push(layer);
          return;
        }

        let dashLength = dashBase;
        if (dashLength * stripeCount > groupLength) dashLength = groupLength / stripeCount;
        if (!(dashLength > 0)) dashLength = minDash;

        const gapLength = dashLength * (stripeCount - 1);
        const patternLength = dashLength + gapLength;

        let baseOffsetValue;
        const tolerance = 1e-9;
        let anchorRouteId = null;
        let anchorOffset = -Infinity;

        sortedRoutes.forEach((routeId) => {
          const offsetValue = offsetsByRoute.get(routeId);
          if (Number.isFinite(offsetValue)) {
            if (anchorRouteId === null
              || offsetValue > anchorOffset + tolerance
              || (Math.abs(offsetValue - anchorOffset) <= tolerance && String(routeId) < String(anchorRouteId))) {
              anchorRouteId = routeId;
              anchorOffset = offsetValue;
            }
          }
        });

        if (anchorRouteId !== null && Number.isFinite(anchorOffset)) {
          const anchorIndex = sortedRoutes.indexOf(anchorRouteId);
          baseOffsetValue = anchorOffset - dashLength * anchorIndex;
        } else {
          const rawOffset = Number(group.offsetPx);
          baseOffsetValue = Number.isFinite(rawOffset) ? rawOffset : 0;
        }

        sortedRoutes.forEach((routeId, index) => {
          let dashOffsetValue = baseOffsetValue + dashLength * index;
          if (patternLength > 0) {
            const targetOffset = offsetsByRoute.get(routeId);
            if (Number.isFinite(targetOffset)) {
              const diff = targetOffset - dashOffsetValue;
              const adjustment = Math.round(diff / patternLength);
              if (Number.isFinite(adjustment) && adjustment !== 0) dashOffsetValue += adjustment * patternLength;
            }
            dashOffsetValue = ((dashOffsetValue % patternLength) + patternLength) % patternLength;
          }

          const layer = L.polyline(coords, mergeRouteLayerOptions({
            color: getRouteColor(routeId),
            weight,
            opacity: 1,
            dashArray: `${dashLength} ${gapLength}`,
            dashOffset: `${dashOffsetValue}`,
            lineCap: 'butt',
            lineJoin: 'round',
          })).addTo(this.map);
          newLayers.push(layer);
        });
      });

      this.layers = newLayers;
    }

    simplifyLatLngs(latlngs, zoom) {
      if (!Array.isArray(latlngs) || latlngs.length === 0) return [];
      const projected = latlngs.map((latlng) => this.map.project(latlng, zoom));
      let simplified = projected;
      if (projected.length > 2 && this.options.simplifyTolerancePx > 0 && L.LineUtil && L.LineUtil.simplify) {
        simplified = L.LineUtil.simplify(projected, this.options.simplifyTolerancePx);
      }
      return simplified.map((pt) => ({ point: L.point(pt.x, pt.y), latlng: this.map.unproject(pt, zoom) }));
    }

    resampleRoute(routeId, latlngs, zoom, step) {
      const simplified = this.simplifyLatLngs(latlngs, zoom);
      if (simplified.length < 2) return [];

      const samples = [];
      const first = simplified[0];
      samples.push({ latlng: first.latlng, point: first.point, cumulativeLength: 0 });

      let traversed = 0;
      let distanceSinceLast = 0;

      for (let i = 1; i < simplified.length; i++) {
        const prev = simplified[i - 1];
        const curr = simplified[i];
        const segmentLength = this.distance(prev.point, curr.point);
        if (segmentLength === 0) continue;

        let consumed = 0;
        while (distanceSinceLast + (segmentLength - consumed) >= step) {
          const remaining = step - distanceSinceLast;
          consumed += remaining;
          const ratio = consumed / segmentLength;
          const samplePoint = this.interpolatePoint(prev.point, curr.point, ratio);
          const sampleLatLng = this.map.unproject(samplePoint, zoom);
          traversed += remaining;
          samples.push({ latlng: sampleLatLng, point: samplePoint, cumulativeLength: traversed });
          distanceSinceLast = 0;
        }

        const leftover = segmentLength - consumed;
        traversed += leftover;
        distanceSinceLast += leftover;
      }

      const last = simplified[simplified.length - 1];
      const lastSample = samples[samples.length - 1];
      if (!this.latLngsClose(lastSample.latlng, last.latlng)) {
        samples.push({ latlng: last.latlng, point: last.point, cumulativeLength: traversed });
      } else {
        lastSample.cumulativeLength = traversed;
      }

      const segments = [];
      for (let i = 0; i < samples.length - 1; i++) {
        const start = samples[i];
        const end = samples[i + 1];
        const lengthPx = this.distance(start.point, end.point);
        if (!(lengthPx > 0)) continue;

        const bounds = {
          minX: Math.min(start.point.x, end.point.x),
          minY: Math.min(start.point.y, end.point.y),
          maxX: Math.max(start.point.x, end.point.x),
          maxY: Math.max(start.point.y, end.point.y),
        };
        const midpoint = L.point((start.point.x + end.point.x) / 2, (start.point.y + end.point.y) / 2);
        const heading = Math.atan2(end.point.y - start.point.y, end.point.x - start.point.x);
        const offsetValues = [];
        const startOffset = Number(start.cumulativeLength);
        if (Number.isFinite(startOffset)) offsetValues.push(startOffset);
        const endOffset = Number(end.cumulativeLength);
        if (Number.isFinite(endOffset)) offsetValues.push(endOffset);

        const routeOffsets = {};
        if (offsetValues.length > 0) routeOffsets[routeId] = { min: Math.min(...offsetValues) };

        segments.push({
          routeId, index: segments.length, start, end, lengthPx, bounds, midpoint, heading, routeOffsets,
          sharedRoutes: new Set([routeId]),
        });
      }
      return segments;
    }

    interpolatePoint(a, b, t) {
      return L.point(a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t);
    }

    distance(a, b) {
      const ax = a?.x ?? 0;
      const ay = a?.y ?? 0;
      const bx = b?.x ?? 0;
      const by = b?.y ?? 0;
      const dx = bx - ax;
      const dy = by - ay;
      return Math.sqrt(dx * dx + dy * dy);
    }

    segmentsOverlap(a, b, tolerance, headingToleranceRad) {
      const midpointDistance = this.distance(a.midpoint, b.midpoint);
      if (midpointDistance > tolerance) return false;
      const headingDiff = this.smallestHeadingDifference(a.heading, b.heading);
      if (headingDiff > headingToleranceRad && Math.abs(Math.PI - headingDiff) > headingToleranceRad) return false;
      const startDistance = this.distance(a.start.point, b.start.point);
      const endDistance = this.distance(a.end.point, b.end.point);
      const crossStart = this.distance(a.start.point, b.end.point);
      const crossEnd = this.distance(a.end.point, b.start.point);
      return Math.min(startDistance, endDistance, crossStart, crossEnd) <= tolerance * 2;
    }

    smallestHeadingDifference(a, b) {
      let diff = Math.abs(a - b);
      diff = diff % (Math.PI * 2);
      if (diff > Math.PI) diff = (Math.PI * 2) - diff;
      return diff;
    }
  }

  let overlapRenderer = null;
  if (isSpatialIndexAvailable()) {
    overlapRenderer = new OverlapRouteRenderer(map, {
      sampleStepPx: 8,
      dashLengthPx: 16,
      minDashLengthPx: 0.5,
      // DASH's OBA-sourced polylines come from several distinct shapeIds per
      // road rather than TransLoc's one-shape-per-route source, so two routes
      // running down the "same" street don't trace pixel-identical paths the
      // way these tolerances (tuned for testmap's cleaner data) assume.
      // Loosened here so real shared corridors keep matching instead of
      // flickering in and out of "overlapping" every few pixels.
      matchTolerancePx: 20,
      headingToleranceDeg: 30,
      strokeWeight: DEFAULT_ROUTE_STROKE_WEIGHT,
      minStrokeWeight: MIN_ROUTE_STROKE_WEIGHT,
      maxStrokeWeight: MAX_ROUTE_STROKE_WEIGHT,
      renderer: sharedRouteRenderer,
      pane: routePaneName,
    });
    map.on('zoomend', () => overlapRenderer.handleZoomEnd());
  } else {
    console.error('rbush not loaded; DASH map will fall back to plain route polylines with no overlap striping.');
  }

  // ---------------------------
  // State
  // ---------------------------

  const routesById = new Map(); // routeId -> {id, shortName, longName, color, textColor}
  const stopsById = new Map(); // stopId -> {id, lat, lon, name, code, direction, routeIds}
  const vehicleMarkers = new Map(); // vehicleId -> L.Marker
  const routeGeometryMap = new Map(); // "routeId__idx" -> [{lat,lng}, ...] fed to OverlapRouteRenderer
  const routeKeysByRouteId = new Map(); // routeId -> ["routeId__0", "routeId__1", ...]
  const stopMarkers = []; // [{routeIds, layer}] — persists independent of layer-group membership
  const routeSelections = new Map(); // routeId -> boolean (defaults to true if unset)
  const activeRouteIds = new Set(); // routeIds with at least one live vehicle right now
  let lastActiveRouteSignature = '';
  let allRouteBounds = null;
  let busMarkerSvgText = null;
  let busMarkerSvgPromise = null;

  function isRouteSelected(routeId) {
    return routeSelections.has(routeId) ? routeSelections.get(routeId) : true;
  }

  function setLoadingVisible(visible, text) {
    const overlay = document.getElementById('loadingOverlay');
    if (!overlay) return;
    if (text) {
      const textEl = overlay.querySelector('.loading-overlay__text');
      if (textEl) textEl.textContent = text;
    }
    overlay.classList.toggle('is-visible', !!visible);
    overlay.setAttribute('aria-busy', visible ? 'true' : 'false');
  }

  function updateKioskStatusMessage(hasActiveVehicles) {
    const el = document.getElementById('kioskStatusMessage');
    if (!el) return;
    if (!isKioskExperienceActive()) {
      el.classList.remove('is-visible');
      el.setAttribute('aria-hidden', 'true');
      return;
    }
    el.classList.toggle('is-visible', !hasActiveVehicles);
    el.setAttribute('aria-hidden', hasActiveVehicles ? 'true' : 'false');
  }

  // ===========================================================
  // Theme (Light / Auto / Dark)
  // ===========================================================

  let systemPrefersDark = false;
  try {
    systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
      systemPrefersDark = e.matches;
      applyMapTheme();
    });
  } catch (error) {
    systemPrefersDark = false;
  }

  function loadThemePreference() {
    try {
      const stored = localStorage.getItem(MAP_THEME_STORAGE_KEY);
      if (stored === MAP_THEMES.LIGHT || stored === MAP_THEMES.DARK || stored === MAP_THEMES.AUTO) {
        return stored;
      }
    } catch (error) {
      // ignore
    }
    return MAP_THEMES.AUTO;
  }

  function storeThemePreference(theme) {
    try {
      localStorage.setItem(MAP_THEME_STORAGE_KEY, theme);
    } catch (error) {
      // ignore
    }
  }

  let currentMapTheme = loadThemePreference();
  let effectiveMapTheme = null;

  function isSolarDark() {
    if (typeof SunCalc === 'undefined') return null;
    try {
      const now = new Date();
      const times = SunCalc.getTimes(now, SOLAR_THEME_LAT, SOLAR_THEME_LON);
      const dawn = times.dawn;
      const dusk = times.dusk;
      if (!dawn || !dusk || isNaN(dawn.getTime()) || isNaN(dusk.getTime())) return null;
      return now < dawn || now > dusk;
    } catch (error) {
      return null;
    }
  }

  function resolveEffectiveTheme(preference) {
    if (preference === MAP_THEMES.AUTO) {
      if (kioskMode) {
        const solarDark = isSolarDark();
        if (solarDark !== null) return solarDark ? MAP_THEMES.DARK : MAP_THEMES.LIGHT;
      }
      return systemPrefersDark ? MAP_THEMES.DARK : MAP_THEMES.LIGHT;
    }
    return preference;
  }

  function applyPanelTheme(isDark) {
    document.body.classList.toggle('theme-dark', isDark);
  }

  function applyMapTheme() {
    const newEffective = resolveEffectiveTheme(currentMapTheme);
    applyPanelTheme(newEffective === MAP_THEMES.DARK);
    if (newEffective === effectiveMapTheme) return;
    effectiveMapTheme = newEffective;
    if (effectiveMapTheme === MAP_THEMES.DARK) {
      if (map.hasLayer(lightTileLayer)) map.removeLayer(lightTileLayer);
      if (!map.hasLayer(darkTileLayer)) darkTileLayer.addTo(map);
    } else {
      if (map.hasLayer(darkTileLayer)) map.removeLayer(darkTileLayer);
      if (!map.hasLayer(lightTileLayer)) lightTileLayer.addTo(map);
    }
    const activeLayer = effectiveMapTheme === MAP_THEMES.DARK ? darkTileLayer : lightTileLayer;
    if (activeLayer && typeof activeLayer.bringToBack === 'function') activeLayer.bringToBack();
  }

  function updateThemeToggleButtons() {
    document.querySelectorAll('.theme-mode-button').forEach((btn) => {
      btn.classList.toggle('is-active', btn.getAttribute('data-theme') === currentMapTheme);
    });
  }

  function setMapTheme(theme) {
    if (theme !== MAP_THEMES.LIGHT && theme !== MAP_THEMES.DARK && theme !== MAP_THEMES.AUTO) {
      theme = MAP_THEMES.AUTO;
    }
    currentMapTheme = theme;
    storeThemePreference(theme);
    applyMapTheme();
    updateThemeToggleButtons();
  }
  window.setMapTheme = setMapTheme;

  function centerMapOnRoutes() {
    if (allRouteBounds && typeof map.fitBounds === 'function') {
      try {
        map.fitBounds(allRouteBounds, { padding: [20, 20] });
        return true;
      } catch (error) {
        console.warn('Failed to fit DASH route bounds.', error);
      }
    }
    map.setView(ALEXANDRIA_CENTER, ALEXANDRIA_ZOOM);
    return true;
  }
  window.centerMapOnRoutes = centerMapOnRoutes;

  // ===========================================================
  // Panel toggle / positioning
  // ===========================================================

  let kioskUiSuppressed = false;

  function isKioskExperienceActive() {
    return kioskMode;
  }

  function setPanelToggleArrow(tab, direction) {
    if (!tab) return;
    tab.setAttribute('data-arrow-direction', direction);
  }

  function togglePanelVisibility(panelId, tabId, expandedArrow, collapsedArrow) {
    if (isKioskExperienceActive()) return;
    const panel = document.getElementById(panelId);
    const tab = document.getElementById(tabId);
    if (!panel || !tab) return;
    const isHidden = panel.classList.toggle('hidden');
    setPanelToggleArrow(tab, isHidden ? collapsedArrow : expandedArrow);
    positionAllPanelTabs();
  }

  function toggleControlPanel() {
    togglePanelVisibility('controlPanel', 'controlPanelTab', 'in', 'out');
  }
  window.toggleControlPanel = toggleControlPanel;

  function toggleRoutePanel() {
    const column = document.getElementById('rightPanelColumn');
    const tab = document.getElementById('routeSelectorTab');
    if (!column || !tab) return;
    const isHidden = column.classList.toggle('hidden');
    setPanelToggleArrow(tab, isHidden ? 'out' : 'in');
    positionAllPanelTabs();
  }
  window.toggleRoutePanel = toggleRoutePanel;

  function positionPanelTab(panelId, tabId, side) {
    const panel = document.getElementById(panelId);
    const tab = document.getElementById(tabId);
    if (!panel || !tab) return;

    const panelRect = panel.getBoundingClientRect();
    const tabRect = tab.getBoundingClientRect();
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    const tabHeight = tabRect.height || tab.offsetHeight || 0;
    if (Number.isFinite(panelRect.top) && Number.isFinite(panelRect.height)) {
      const panelCenter = panelRect.top + panelRect.height / 2;
      const halfTab = tabHeight / 2;
      let targetTop = panelCenter;
      if (Number.isFinite(viewportHeight) && halfTab > 0) {
        const minTop = halfTab + 8;
        const maxTop = viewportHeight - halfTab - 8;
        targetTop = Math.min(Math.max(panelCenter, minTop), Math.max(minTop, maxTop));
      }
      tab.style.top = `${targetTop}px`;
    }

    const panelStyle = window.getComputedStyle(panel);
    const gap = side === 'right' ? (parseFloat(panelStyle.right) || 0) : (parseFloat(panelStyle.left) || 0);
    const offset = panel.offsetWidth + gap;
    const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
    const tabWidth = tabRect.width || tab.offsetWidth || 0;

    let navOffset = 0;
    try {
      const rootStyle = window.getComputedStyle(document.documentElement);
      const rawOffset = parseFloat(rootStyle.getPropertyValue('--hg-nav-left-offset'));
      if (Number.isFinite(rawOffset)) navOffset = Math.max(0, rawOffset);
    } catch (error) {
      navOffset = 0;
    }

    if (side === 'right') {
      if (panel.classList.contains('hidden')) {
        tab.style.right = '0';
      } else {
        const maxRight = Math.max(0, viewportWidth - tabWidth);
        tab.style.right = `${Math.min(offset, maxRight)}px`;
      }
      tab.style.left = '';
    } else {
      if (panel.classList.contains('hidden')) {
        tab.style.left = `${navOffset}px`;
      } else {
        const maxLeft = Math.max(0, viewportWidth - tabWidth);
        tab.style.left = `${Math.min(offset, maxLeft)}px`;
      }
      tab.style.right = '';
    }
  }

  function isCompactViewport() {
    const width = window.innerWidth || document.documentElement.clientWidth || 0;
    const height = window.innerHeight || document.documentElement.clientHeight || 0;
    const candidates = [width, height].filter((v) => Number.isFinite(v) && v > 0);
    const smallest = candidates.length > 0 ? Math.min(...candidates) : width;
    return Number.isFinite(smallest) && smallest <= PANEL_COLLAPSE_BREAKPOINT;
  }

  function isPanelVisibleForMobileBehavior(panel) {
    if (!panel) return false;
    if (panel.classList.contains('hidden')) return false;
    if (window.getComputedStyle(panel).display === 'none') return false;
    return true;
  }

  function updatePanelTabVisibility() {
    const controlTab = document.getElementById('controlPanelTab');
    const routeTab = document.getElementById('routeSelectorTab');
    if (!controlTab || !routeTab) return;
    if (!isCompactViewport()) {
      controlTab.classList.remove('is-hidden-mobile');
      routeTab.classList.remove('is-hidden-mobile');
      return;
    }
    const controlPanel = document.getElementById('controlPanel');
    const rightColumn = document.getElementById('rightPanelColumn');
    const controlVisible = isPanelVisibleForMobileBehavior(controlPanel);
    const routeVisible = isPanelVisibleForMobileBehavior(rightColumn);
    if (controlVisible && !routeVisible) {
      routeTab.classList.add('is-hidden-mobile');
      controlTab.classList.remove('is-hidden-mobile');
    } else if (routeVisible && !controlVisible) {
      controlTab.classList.add('is-hidden-mobile');
      routeTab.classList.remove('is-hidden-mobile');
    } else {
      controlTab.classList.remove('is-hidden-mobile');
      routeTab.classList.remove('is-hidden-mobile');
    }
  }

  function ensurePanelsHiddenForKioskExperience() {
    if (!isKioskExperienceActive() || kioskUiSuppressed) return;
    const hide = (panel) => {
      if (!panel) return;
      panel.classList.add('hidden');
      panel.style.display = 'none';
      panel.setAttribute('aria-hidden', 'true');
      panel.innerHTML = '';
    };
    const hideTab = (tab) => {
      if (tab) tab.style.display = 'none';
    };
    hide(document.getElementById('controlPanel'));
    hide(document.getElementById('routeSelector'));
    hide(document.getElementById('rightPanelColumn'));
    hideTab(document.getElementById('controlPanelTab'));
    hideTab(document.getElementById('routeSelectorTab'));
    kioskUiSuppressed = true;
  }

  function positionAllPanelTabs() {
    if (isKioskExperienceActive()) {
      ensurePanelsHiddenForKioskExperience();
      return;
    }
    positionPanelTab('rightPanelColumn', 'routeSelectorTab', 'right');
    positionPanelTab('controlPanel', 'controlPanelTab', 'left');
    updatePanelTabVisibility();
  }

  function throttleAnimationFrame(fn) {
    let scheduled = false;
    return function throttled(...args) {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(() => {
        scheduled = false;
        fn.apply(null, args);
      });
    };
  }
  const positionAllPanelTabsThrottled = throttleAnimationFrame(positionAllPanelTabs);

  function initializePanelStateForViewport() {
    if (isKioskExperienceActive()) {
      ensurePanelsHiddenForKioskExperience();
      return;
    }
    if (!isCompactViewport()) return;
    const controlPanel = document.getElementById('controlPanel');
    const controlTab = document.getElementById('controlPanelTab');
    const rightPanelColumn = document.getElementById('rightPanelColumn');
    const routeTab = document.getElementById('routeSelectorTab');
    if (controlPanel) controlPanel.classList.add('hidden');
    if (controlTab) setPanelToggleArrow(controlTab, 'out');
    if (rightPanelColumn) rightPanelColumn.classList.add('hidden');
    if (routeTab) setPanelToggleArrow(routeTab, 'out');
    positionAllPanelTabs();
  }

  // ===========================================================
  // Collapsible sections (control panel)
  // ===========================================================

  function isSectionCollapsed(id) {
    try {
      const stored = localStorage.getItem(SECTION_STORAGE_PREFIX + id + ':collapsed');
      return stored === null ? true : stored === 'true';
    } catch (error) {
      return true;
    }
  }

  function toggleSectionCollapse(id) {
    const el = document.getElementById('section-' + id);
    if (!el) return;
    const nowCollapsed = el.classList.toggle('is-collapsed');
    try {
      localStorage.setItem(SECTION_STORAGE_PREFIX + id + ':collapsed', String(nowCollapsed));
    } catch (error) {
      // ignore
    }
  }
  window.toggleSectionCollapse = toggleSectionCollapse;

  function buildSection(id, label, innerHtml) {
    if (!innerHtml || !innerHtml.trim()) return '';
    const collapsed = isSectionCollapsed(id);
    const chevron = '<svg class="section-chevron" viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M2 4l4 4 4-4" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    return `
      <div class="selector-group selector-group--collapsible${collapsed ? ' is-collapsed' : ''}" id="section-${id}">
        <button type="button" class="section-header" onclick="toggleSectionCollapse('${id}')">
          <span class="section-header__label">${escapeHtml(label)}</span>
          ${chevron}
        </button>
        <div class="section-body">${innerHtml}</div>
      </div>
    `;
  }

  // ===========================================================
  // Control panel (Map View only)
  // ===========================================================

  function updateControlPanel() {
    if (isKioskExperienceActive()) {
      ensurePanelsHiddenForKioskExperience();
      return;
    }
    const panel = document.getElementById('controlPanel');
    if (!panel) return;

    const mapViewSectionInner = `
      <div class="theme-mode-group" id="themeModeButtons">
        <button type="button" class="pill-button theme-mode-button ${currentMapTheme === MAP_THEMES.LIGHT ? 'is-active' : ''}" data-theme="${MAP_THEMES.LIGHT}" onclick="setMapTheme('${MAP_THEMES.LIGHT}')">
          Light
        </button>
        <button type="button" class="pill-button theme-mode-button ${currentMapTheme === MAP_THEMES.AUTO ? 'is-active' : ''}" data-theme="${MAP_THEMES.AUTO}" onclick="setMapTheme('${MAP_THEMES.AUTO}')">
          Auto
        </button>
        <button type="button" class="pill-button theme-mode-button ${currentMapTheme === MAP_THEMES.DARK ? 'is-active' : ''}" data-theme="${MAP_THEMES.DARK}" onclick="setMapTheme('${MAP_THEMES.DARK}')">
          Dark
        </button>
      </div>
      <button type="button" id="centerMapButton" class="pill-button center-map-button" onclick="centerMapOnRoutes()">
        Center Map
      </button>
    `;

    const contentHtml = buildSection('mapview', 'Map View', mapViewSectionInner);

    const fullHtml = `
      <div class="selector-header">
        <div class="selector-header-text">
          <div class="selector-title">System Controls</div>
        </div>
      </div>
      <div class="selector-content">
        ${contentHtml}
      </div>
    `;
    panel.innerHTML = fullHtml;
    updateThemeToggleButtons();
    positionAllPanelTabs();
  }

  // ===========================================================
  // Route selector panel
  // ===========================================================

  function applyRouteOptionState(inputElement) {
    if (!inputElement) return;
    const parentLabel = inputElement.closest('label.route-option');
    if (!parentLabel) return;
    parentLabel.classList.toggle('is-active', inputElement.checked);
  }

  function applyRouteSelection() {
    if (!overlapRenderer) return;
    const selectedKeys = [];
    routeKeysByRouteId.forEach((keys, routeId) => {
      if (isRouteSelected(routeId)) selectedKeys.push(...keys);
    });
    overlapRenderer.updateRoutes(routeGeometryMap, selectedKeys);
  }

  function refreshMapVisibility() {
    applyRouteSelection();
    vehicleMarkers.forEach((marker) => {
      const routeId = marker.options && marker.options.dashRouteId;
      const shouldShow = routeId === undefined ? true : isRouteSelected(routeId);
      const hasLayer = vehicleLayerGroup.hasLayer(marker);
      if (shouldShow && !hasLayer) vehicleLayerGroup.addLayer(marker);
      if (!shouldShow && hasLayer) vehicleLayerGroup.removeLayer(marker);
    });
    stopMarkers.forEach(({ routeIds, layer }) => {
      const shouldShow = !routeIds || routeIds.length === 0 || routeIds.some((rid) => isRouteSelected(rid));
      const hasLayer = stopLayerGroup.hasLayer(layer);
      if (shouldShow && !hasLayer) stopLayerGroup.addLayer(layer);
      if (!shouldShow && hasLayer) stopLayerGroup.removeLayer(layer);
    });
  }

  function selectAllRoutes() {
    routesById.forEach((route, id) => routeSelections.set(id, true));
    updateRouteSelector();
    refreshMapVisibility();
  }
  window.selectAllRoutes = selectAllRoutes;

  function selectActiveRoutes() {
    routesById.forEach((route, id) => routeSelections.set(id, activeRouteIds.has(id)));
    updateRouteSelector();
    refreshMapVisibility();
  }
  window.selectActiveRoutes = selectActiveRoutes;

  function deselectAllRoutes() {
    routesById.forEach((route, id) => routeSelections.set(id, false));
    updateRouteSelector();
    refreshMapVisibility();
  }
  window.deselectAllRoutes = deselectAllRoutes;

  function toggleRoutePanelCollapse() {
    const routePanel = document.getElementById('routeSelector');
    if (!routePanel) return;
    routePanel.classList.toggle('is-collapsed');
    positionAllPanelTabs();
  }
  window.toggleRoutePanelCollapse = toggleRoutePanelCollapse;

  function updateRouteSelector() {
    if (isKioskExperienceActive()) {
      ensurePanelsHiddenForKioskExperience();
      return;
    }
    const container = document.getElementById('routeSelector');
    if (!container) return;

    const routeIds = Array.from(routesById.keys()).sort((a, b) => {
      const aActive = activeRouteIds.has(a);
      const bActive = activeRouteIds.has(b);
      if (aActive !== bActive) return aActive ? -1 : 1;
      const nameA = (routesById.get(a).shortName || a).toString().toUpperCase();
      const nameB = (routesById.get(b).shortName || b).toString().toUpperCase();
      return nameA.localeCompare(nameB, undefined, { numeric: true });
    });

    const routeToggleSvg = '<svg class="selector-header__toggle" viewBox="0 0 20 20" aria-hidden="true"><path d="M6 8l4 4 4-4"/></svg>';
    let html = `
      <div class="selector-header selector-header--collapsible" onclick="toggleRoutePanelCollapse()">
        <div class="selector-header-text">
          <div class="selector-title">Route Controls</div>
          <div class="selector-subtitle">Tailor the map to the DASH routes you care about.</div>
        </div>
        ${routeToggleSvg}
      </div>
      <div class="selector-content">
        <div class="selector-section">
          <div class="selector-group selector-group--route-actions">
            <div class="selector-label">Select Routes</div>
            <div class="display-mode-group route-action-buttons">
              <button type="button" class="pill-button" onclick="selectAllRoutes()">Select All</button>
              <button type="button" class="pill-button" onclick="selectActiveRoutes()">Select Active</button>
              <button type="button" class="pill-button" onclick="deselectAllRoutes()">Deselect All</button>
            </div>
          </div>
          <div class="route-list">
    `;

    routeIds.forEach((routeId) => {
      const route = routesById.get(routeId);
      const checked = isRouteSelected(routeId);
      const hasActiveVehicle = activeRouteIds.has(routeId);
      const routeName = route.longName || route.shortName || routeId;
      const color = normalizeColor(route.color, DEFAULT_ROUTE_COLOR);
      const detailHtml = hasActiveVehicle ? '' : '<span class="route-option-detail">No buses currently assigned</span>';
      html += `
          <label class="route-option${checked ? ' is-active' : ''}">
            <input type="checkbox" id="dashroute_${escapeHtml(routeId)}" value="${escapeHtml(routeId)}" ${checked ? 'checked' : ''}>
            <span class="color-box route-option-swatch" style="background:${color};"></span>
            <span class="route-option-text">
              <span class="route-option-name">${escapeHtml(route.shortName || routeId)} — ${escapeHtml(routeName)}</span>
              ${detailHtml}
            </span>
          </label>
      `;
    });

    html += `
          </div>
        </div>
      </div>
    `;

    container.innerHTML = html;

    routeIds.forEach((routeId) => {
      const chk = document.getElementById('dashroute_' + routeId);
      if (!chk) return;
      chk.addEventListener('change', () => {
        routeSelections.set(routeId, chk.checked);
        applyRouteOptionState(chk);
        refreshMapVisibility();
      });
    });

    positionAllPanelTabs();
  }

  // ===========================================================
  // Route legend (kiosk-visible, always shown)
  // ===========================================================

  function renderRouteLegend() {
    const legend = document.getElementById('routeLegend');
    if (!legend) return;
    if (!isKioskExperienceActive()) {
      legend.style.display = 'none';
      legend.innerHTML = '';
      return;
    }
    const routes = Array.from(routesById.values()).sort((a, b) =>
      String(a.shortName || '').localeCompare(String(b.shortName || ''), undefined, { numeric: true })
    );
    legend.style.display = 'block';
    legend.innerHTML = [
      '<div class="legend-title">DASH Routes</div>',
      routes
        .map((route) => {
          const color = normalizeColor(route.color, DEFAULT_ROUTE_COLOR);
          const name = `${route.shortName || route.id} — ${route.longName || ''}`;
          return [
            '<div class="legend-item">',
            `<span class="legend-color" style="background-color:${color}"></span>`,
            '<div class="legend-text">',
            `<div class="legend-name">${escapeHtml(name)}</div>`,
            '</div>',
            '</div>',
          ].join('');
        })
        .join(''),
    ].join('');
  }

  // ===========================================================
  // Bus icon
  // ===========================================================

  async function ensureBusMarkerSvg() {
    if (typeof busMarkerSvgText === 'string' && busMarkerSvgText.trim()) return true;
    if (!busMarkerSvgPromise) {
      busMarkerSvgPromise = fetch(BUS_MARKER_SVG_URL, { cache: 'no-store' })
        .then((response) => {
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          return response.text();
        })
        .then((text) => {
          busMarkerSvgText = text;
          return text;
        })
        .catch((error) => {
          console.error('Failed to load bus marker SVG asset.', error);
          busMarkerSvgText = null;
          throw error;
        });
    }
    try {
      await busMarkerSvgPromise;
      return typeof busMarkerSvgText === 'string' && busMarkerSvgText.trim().length > 0;
    } catch (error) {
      return false;
    }
  }

  function normalizeGlyphColor(candidate) {
    return normalizeColor(candidate, BUS_MARKER_DEFAULT_CONTRAST_COLOR);
  }

  function buildBusMarkerIcon(vehicleId, routeColor, textColor, headingDeg) {
    if (typeof busMarkerSvgText !== 'string' || !busMarkerSvgText.trim()) return null;
    const template = document.createElement('template');
    template.innerHTML = busMarkerSvgText.trim();
    const svgEl = template.content.firstElementChild;
    if (!svgEl || svgEl.tagName.toLowerCase() !== 'svg') return null;

    svgEl.classList.add('bus-marker__svg');
    svgEl.setAttribute('viewBox', `0 0 ${BUS_MARKER_VIEWBOX_WIDTH} ${BUS_MARKER_VIEWBOX_HEIGHT}`);
    svgEl.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    svgEl.setAttribute('focusable', 'false');
    svgEl.style.width = '100%';
    svgEl.style.height = '100%';
    svgEl.style.transformOrigin = BUS_MARKER_TRANSFORM_ORIGIN;
    svgEl.style.transformBox = 'fill-box';
    const heading = Number.isFinite(headingDeg) ? headingDeg : 0;
    svgEl.style.transform = `rotate(${heading.toFixed(2)}deg)`;

    const fillColor = normalizeColor(routeColor, BUS_MARKER_DEFAULT_ROUTE_COLOR);
    const glyphColor = normalizeGlyphColor(textColor);

    const root = document.createElement('div');
    root.className = 'bus-marker__root';
    root.style.setProperty('--bus-marker-fill', fillColor);
    root.style.setProperty('--bus-marker-glyph', glyphColor);
    root.setAttribute('role', 'img');
    root.setAttribute('aria-label', `Bus ${vehicleId}`);
    root.style.pointerEvents = 'none';
    root.appendChild(svgEl);

    const wrapper = document.createElement('div');
    wrapper.className = 'bus-marker__wrapper';
    wrapper.appendChild(root);

    const outerWrapper = document.createElement('div');
    outerWrapper.appendChild(wrapper);

    const width = BUS_MARKER_BASE_WIDTH_PX;
    const height = width * BUS_MARKER_ASPECT_RATIO;
    return L.divIcon({
      html: outerWrapper.innerHTML,
      className: 'leaflet-div-icon bus-marker',
      iconSize: [width, height],
      iconAnchor: [width * BUS_MARKER_ICON_ANCHOR_X_RATIO, height * BUS_MARKER_ICON_ANCHOR_Y_RATIO],
    });
  }

  // ===========================================================
  // Stop icon
  // ===========================================================

  const stopIconCache = new Map();

  function collectStopMarkerColors(routeIds) {
    const colors = (routeIds || [])
      .map((id) => normalizeColor(routesById.get(id) && routesById.get(id).color, null))
      .filter((c) => c !== null);
    return colors.length > 0 ? colors : [DEFAULT_ROUTE_COLOR];
  }

  function ensureStopIcon(routeIds) {
    const sorted = Array.from(routeIds || []).sort((a, b) => String(a).localeCompare(String(b)));
    const colors = collectStopMarkerColors(sorted);
    const colorKey = colors.slice().sort((a, b) => a.localeCompare(b, undefined, { numeric: true })).join('|');
    const size = STOP_MARKER_ICON_SIZE;
    const outline = Math.max(0, STOP_MARKER_OUTLINE_WIDTH);
    const borderWidth = Math.max(0, STOP_MARKER_BORDER_WIDTH);
    const devicePixelRatio = Number.isFinite(window.devicePixelRatio) && window.devicePixelRatio > 0 ? window.devicePixelRatio : 1;
    const cacheKey = `${colorKey}|${size}|${outline}|${borderWidth}|${devicePixelRatio}`;
    if (stopIconCache.has(cacheKey)) return stopIconCache.get(cacheKey);

    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d');
    const scaledSize = Math.ceil(size * devicePixelRatio);
    canvas.width = scaledSize;
    canvas.height = scaledSize;
    context.scale(devicePixelRatio, devicePixelRatio);

    const center = size / 2;
    const outlineThickness = Math.min(outline, center);
    const outlineRadius = center;
    const borderRadius = Math.max(0, outlineRadius - outlineThickness);
    const fillRadius = Math.max(0, borderRadius - borderWidth);

    context.clearRect(0, 0, size, size);

    context.beginPath();
    context.arc(center, center, outlineRadius, 0, Math.PI * 2);
    context.fillStyle = STOP_MARKER_OUTLINE_COLOR;
    context.fill();

    context.beginPath();
    context.arc(center, center, borderRadius, 0, Math.PI * 2);
    context.fillStyle = STOP_MARKER_BORDER_COLOR;
    context.fill();

    if (fillRadius > 0) {
      if (colors.length <= 1) {
        context.beginPath();
        context.arc(center, center, fillRadius, 0, Math.PI * 2);
        context.fillStyle = colors[0] || '#FFFFFF';
        context.fill();
      } else {
        const segmentAngle = (Math.PI * 2) / colors.length;
        let currentAngle = -Math.PI / 2;
        colors.forEach((color) => {
          context.beginPath();
          context.moveTo(center, center);
          context.arc(center, center, fillRadius, currentAngle, currentAngle + segmentAngle);
          context.closePath();
          context.fillStyle = color;
          context.fill();
          currentAngle += segmentAngle;
        });
      }
    }

    const icon = L.icon({
      iconUrl: canvas.toDataURL('image/png'),
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
      className: 'leaflet-marker-icon stop-marker-image-icon',
    });
    stopIconCache.set(cacheKey, icon);
    return icon;
  }

  // ===========================================================
  // Popups
  // ===========================================================

  function routePillHtml(routeId) {
    const route = routesById.get(routeId);
    if (!route) return '';
    const bg = normalizeColor(route.color, DEFAULT_ROUTE_COLOR);
    const fg = normalizeColor(route.textColor, BUS_MARKER_DEFAULT_CONTRAST_COLOR);
    const label = route.shortName || route.longName || routeId;
    return `<span class="ondemand-driver-popup__value" style="display:inline-flex;padding:2px 10px;border-radius:999px;background:${bg};color:${fg};">${escapeHtml(label)}</span>`;
  }

  function formatScheduleDeviation(seconds) {
    if (!Number.isFinite(seconds)) return null;
    const minutes = Math.round(Math.abs(seconds) / 60);
    if (minutes === 0) return 'On time';
    return seconds > 0 ? `${minutes} min late` : `${minutes} min early`;
  }

  function popupSection(label, valueHtml) {
    return [
      '<div class="ondemand-driver-popup__section">',
      `<div class="ondemand-driver-popup__label">${escapeHtml(label)}</div>`,
      `<div class="ondemand-driver-popup__value">${valueHtml}</div>`,
      '</div>',
    ].join('');
  }

  function buildVehiclePopupHtml(vehicle) {
    const route = routesById.get(vehicle.routeId);
    const nextStop = vehicle.nextStopId ? stopsById.get(vehicle.nextStopId) : null;
    const deviationText = formatScheduleDeviation(vehicle.scheduleDeviationSec);
    const sections = [];
    if (vehicle.routeId) {
      sections.push(popupSection('Route', routePillHtml(vehicle.routeId)));
    } else {
      const bg = normalizeColor(vehicle.routeColor, DEFAULT_ROUTE_COLOR);
      const fg = normalizeColor(vehicle.routeTextColor, DEFAULT_ROUTE_COLOR);
      sections.push(popupSection('Route', `<span class="ondemand-driver-popup__value" style="display:inline-flex;padding:2px 10px;border-radius:999px;background:${bg};color:${fg};">Unknown</span>`));
    }
    sections.push(popupSection('Headsign', escapeHtml(vehicle.tripHeadsign || (route ? route.longName : '') || 'DASH Bus')));
    if (nextStop) sections.push(popupSection('Next stop', escapeHtml(nextStop.name || nextStop.id)));
    if (deviationText) sections.push(popupSection('Schedule', escapeHtml(deviationText)));
    return `<div class="ondemand-driver-popup__content">${sections.join('<div class="ondemand-driver-popup__divider"></div>')}</div>`;
  }

  function buildStopPopupHtml(stop) {
    const pills = (stop.routeIds || []).map((rid) => routePillHtml(rid)).join(' ');
    const sections = [popupSection('Stop', escapeHtml(stop.name || 'Stop'))];
    if (stop.code) sections.push(popupSection('Stop code', escapeHtml(stop.code)));
    if (pills) sections.push(popupSection('Routes', pills));
    return `<div class="ondemand-driver-popup__content">${sections.join('<div class="ondemand-driver-popup__divider"></div>')}</div>`;
  }

  // ===========================================================
  // Data fetch + render
  // ===========================================================

  async function fetchJson(url) {
    const response = await fetch(url, { cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
    return response.json();
  }

  async function refreshMetadata() {
    try {
      const [routesPayload, shapesPayload] = await Promise.all([
        fetchJson(ROUTES_ENDPOINT),
        fetchJson(SHAPES_ENDPOINT),
      ]);

      routesById.clear();
      (routesPayload.routes || []).forEach((route) => {
        if (route && route.id) routesById.set(route.id, route);
      });

      stopsById.clear();
      (shapesPayload.stops || []).forEach((stop) => {
        if (stop && stop.id) stopsById.set(stop.id, stop);
      });

      routeGeometryMap.clear();
      routeKeysByRouteId.clear();
      if (overlapRenderer) overlapRenderer.reset();
      const bounds = L.latLngBounds([]);
      const shapesByRoute = shapesPayload.routes || {};
      Object.keys(shapesByRoute).forEach((routeId) => {
        const shape = shapesByRoute[routeId];
        const keys = [];
        (shape.polylines || []).forEach((path, idx) => {
          if (!Array.isArray(path) || path.length < 2) return;
          const key = `${routeId}__${idx}`;
          routeGeometryMap.set(key, path);
          keys.push(key);
          path.forEach((pt) => bounds.extend(pt));
        });
        if (keys.length > 0) routeKeysByRouteId.set(routeId, keys);
      });
      allRouteBounds = bounds.isValid() ? bounds : null;

      stopLayerGroup.clearLayers();
      stopMarkers.length = 0;
      stopsById.forEach((stop) => {
        if (!Number.isFinite(stop.lat) || !Number.isFinite(stop.lon)) return;
        const marker = L.marker([stop.lat, stop.lon], {
          icon: ensureStopIcon(stop.routeIds),
          pane: 'stops',
        });
        marker.bindPopup(buildStopPopupHtml(stop), POPUP_OPTIONS);
        marker.addTo(stopLayerGroup);
        stopMarkers.push({ routeIds: stop.routeIds || [], layer: marker });
      });

      refreshMapVisibility();

      renderRouteLegend();
      updateControlPanel();
      updateRouteSelector();
    } catch (error) {
      console.error('Failed to refresh DASH route/stop metadata.', error);
    }
  }

  async function refreshVehicles() {
    try {
      await ensureBusMarkerSvg();
      const payload = await fetchJson(VEHICLES_ENDPOINT);
      const vehicles = payload.vehicles || [];
      const seenIds = new Set();
      const now = Date.now();

      activeRouteIds.clear();

      vehicles.forEach((vehicle) => {
        if (!Number.isFinite(vehicle.lat) || !Number.isFinite(vehicle.lon)) return;
        if (Number.isFinite(vehicle.lastLocationUpdateTime) && now - vehicle.lastLocationUpdateTime > STALE_THRESHOLD_MS) return;
        seenIds.add(vehicle.vehicleId);
        if (vehicle.routeId) activeRouteIds.add(vehicle.routeId);
        const icon = buildBusMarkerIcon(vehicle.vehicleId, vehicle.routeColor, vehicle.routeTextColor, vehicle.heading);
        const popupHtml = buildVehiclePopupHtml(vehicle);
        let marker = vehicleMarkers.get(vehicle.vehicleId);
        const shouldShow = isRouteSelected(vehicle.routeId);
        if (!marker) {
          marker = L.marker([vehicle.lat, vehicle.lon], { icon, pane: 'vehicles', dashRouteId: vehicle.routeId });
          marker.bindPopup(popupHtml, POPUP_OPTIONS);
          if (shouldShow) marker.addTo(vehicleLayerGroup);
          vehicleMarkers.set(vehicle.vehicleId, marker);
        } else {
          marker.setLatLng([vehicle.lat, vehicle.lon]);
          if (icon) marker.setIcon(icon);
          if (marker.isPopupOpen()) {
            marker.setPopupContent(popupHtml);
          } else {
            marker.bindPopup(popupHtml, POPUP_OPTIONS);
          }
          const hasLayer = map.hasLayer(marker);
          if (shouldShow && !hasLayer) vehicleLayerGroup.addLayer(marker);
          if (!shouldShow && hasLayer) vehicleLayerGroup.removeLayer(marker);
        }
      });

      vehicleMarkers.forEach((marker, vehicleId) => {
        if (!seenIds.has(vehicleId)) {
          vehicleLayerGroup.removeLayer(marker);
          vehicleMarkers.delete(vehicleId);
        }
      });

      const activeSignature = Array.from(activeRouteIds).sort().join('|');
      if (activeSignature !== lastActiveRouteSignature) {
        lastActiveRouteSignature = activeSignature;
        updateRouteSelector();
      }

      updateKioskStatusMessage(seenIds.size > 0);
      setLoadingVisible(false);
    } catch (error) {
      console.error('Failed to refresh DASH vehicle positions.', error);
    }
  }

  function scheduleMetadataRefresh() {
    setTimeout(() => {
      refreshMetadata().finally(scheduleMetadataRefresh);
    }, METADATA_REFRESH_MS);
  }

  function scheduleVehicleRefresh() {
    setTimeout(() => {
      refreshVehicles().finally(scheduleVehicleRefresh);
    }, VEHICLE_REFRESH_MS);
  }

  // ===========================================================
  // Boot
  // ===========================================================

  applyMapTheme();
  ensurePanelsHiddenForKioskExperience();
  initializePanelStateForViewport();
  if (!isKioskExperienceActive()) {
    window.addEventListener('load', positionAllPanelTabsThrottled);
    window.addEventListener('resize', positionAllPanelTabsThrottled);
  }

  setLoadingVisible(true, 'Loading DASH buses…');
  refreshMetadata().then(() => {
    scheduleMetadataRefresh();
    refreshVehicles().then(scheduleVehicleRefresh);
  });
})();
