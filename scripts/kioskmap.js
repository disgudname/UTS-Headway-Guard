/* global L, polyline */
(function () {
  'use strict';

  const REFRESH_INTERVAL_MS = 5000;
  const mapDefaults = (typeof window !== 'undefined' && window.HeadwayMapDefaults)
    ? window.HeadwayMapDefaults
    : null;
  const FALLBACK_CENTER = [38.03799212281404, -78.50981502838886];
  const rawCenter = Array.isArray(mapDefaults?.center) && mapDefaults.center.length === 2
    ? [Number(mapDefaults.center[0]), Number(mapDefaults.center[1])]
    : [NaN, NaN];
  const UVA_DEFAULT_CENTER = rawCenter.every(value => Number.isFinite(value))
    ? rawCenter
    : FALLBACK_CENTER;
  const rawZoom = mapDefaults?.zoom;
  const UVA_DEFAULT_ZOOM = Number.isFinite(Number(rawZoom)) ? Number(rawZoom) : 15;
  const DEFAULT_ROUTE_COLOR = '#000000';
  const ROUTE_STROKE_WEIGHT = 6;
  const ROUTE_STRIPE_DASH_LENGTH = 16;
  const DEFAULT_ROUTE_STROKE_WEIGHT = ROUTE_STROKE_WEIGHT;
  const MIN_ROUTE_STROKE_WEIGHT = 3;
  const MAX_ROUTE_STROKE_WEIGHT = 12;
  const ROUTE_WEIGHT_BASE_ZOOM = 15;
  const ROUTE_WEIGHT_STEP_PER_ZOOM = 1;
  const ROUTE_WEIGHT_ZOOM_DELTA_LIMIT = 3;
  const ENABLE_OVERLAP_DASH_RENDERING = true;
  const STOP_ICON_SIZE_PX = 24;
  const STOP_GROUPING_PIXEL_DISTANCE = 20;

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
  const BUS_MARKER_BASE_WIDTH_PX = BUS_MARKER_WIDTH_PX;
  const BUS_MARKER_DEFAULT_ROUTE_COLOR = DEFAULT_ROUTE_COLOR;
  const BUS_MARKER_DEFAULT_CONTRAST_COLOR = '#ffffff';
  const BUS_MARKER_LABEL_FONT_FAMILY = 'FGDC, sans-serif';
  const BUS_MARKER_LABEL_MIN_FONT_PX = 10;
  const LABEL_VERTICAL_CLEARANCE_PX = -7;
  const LABEL_VERTICAL_ALIGNMENT_BONUS_PX = 6;
  const LABEL_VERTICAL_ALIGNMENT_EXPONENT = 4;
  const LABEL_HORIZONTAL_ALIGNMENT_BONUS_PX = 1;
  const LABEL_TEXT_VERTICAL_ADJUSTMENT_RATIO = 0.06;
  const NAME_BUBBLE_BASE_FONT_PX = 14;
  const NAME_BUBBLE_HORIZONTAL_PADDING = 14;
  const NAME_BUBBLE_VERTICAL_PADDING = 3;
  const NAME_BUBBLE_MIN_WIDTH = 40;
  const NAME_BUBBLE_MIN_HEIGHT = 20;
  const NAME_BUBBLE_CORNER_RADIUS = 10;
  const NAME_BUBBLE_FRAME_INSET = 5;
  const BLOCK_BUBBLE_BASE_FONT_PX = 14;
  const BLOCK_BUBBLE_HORIZONTAL_PADDING = 14;
  const BLOCK_BUBBLE_VERTICAL_PADDING = 3;
  const BLOCK_BUBBLE_MIN_WIDTH = 40;
  const BLOCK_BUBBLE_MIN_HEIGHT = 20;
  const BLOCK_BUBBLE_CORNER_RADIUS = 10;
  const BLOCK_BUBBLE_FRAME_INSET = 5;
  const LABEL_BASE_STROKE_WIDTH = 3;

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

  function parseDebugMode() {
    try {
      const params = new URLSearchParams(window.location.search);
      if (!params.has('debugMode')) {
        return false;
      }
      const raw = params.get('debugMode');
      if (raw === null) {
        return false;
      }
      const normalized = raw.trim().toLowerCase();
      if (['0', 'false', 'no', 'off'].includes(normalized)) {
        return false;
      }
      if (['1', 'true', 'yes', 'on'].includes(normalized)) {
        return true;
      }
      return true;
    } catch (error) {
      console.warn('Failed to parse debugMode parameter, defaulting to disabled.', error);
      return false;
    }
  }

  const adminMode = parseAdminMode();
  const debugMode = parseDebugMode();
  if (document && document.body) {
    document.body.dataset.adminMode = adminMode ? 'true' : 'false';
    document.body.dataset.debugMode = debugMode ? 'true' : 'false';
  }

  let sharedRouteRenderer = null;

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

  const ROUTE_PANE = 'routes';
  const STOP_PANE = 'stops';
  const VEHICLE_PANE = 'vehicles';
  const VEHICLE_LABEL_PANE = 'vehicle-labels';

  const routePane = map.createPane(ROUTE_PANE);
  routePane.style.zIndex = '200';
  routePane.style.pointerEvents = 'none';

  if (typeof L === 'object' && typeof L.svg === 'function') {
    try {
      sharedRouteRenderer = L.svg({ padding: 0, pane: ROUTE_PANE });
      if (sharedRouteRenderer) {
        map.addLayer(sharedRouteRenderer);
      }
    } catch (error) {
      console.warn('Failed to initialize shared SVG renderer for routes.', error);
      sharedRouteRenderer = null;
    }
  }

  const stopPane = map.createPane(STOP_PANE);
  stopPane.style.zIndex = '300';
  stopPane.style.pointerEvents = 'none';

  const vehiclePane = map.createPane(VEHICLE_PANE);
  vehiclePane.style.zIndex = '400';
  vehiclePane.style.pointerEvents = 'none';

  const vehicleLabelPane = map.createPane(VEHICLE_LABEL_PANE);
  vehicleLabelPane.style.zIndex = '450';
  vehicleLabelPane.style.pointerEvents = 'none';

  const overlapRenderer = createOverlapRenderer(map);
  if (overlapRenderer) {
    map.on('zoomend', () => {
      overlapRenderer.handleZoomEnd();
      if (!lastRouteRenderState.useOverlapRenderer) {
        updateFallbackRouteWeights();
      }
    });
  } else {
    map.on('zoomend', () => {
      updateFallbackRouteWeights();
    });
  }
  const stopLayerGroup = L.layerGroup().addTo(map);
  const vehicleLayerGroup = L.layerGroup().addTo(map);
  const labelLayerGroup = L.layerGroup();
  if (adminMode) {
    labelLayerGroup.addTo(map);
  }

  const vehicleMarkers = new Map();
  const vehicleLabels = new Map();
  const routeColors = new Map();
  let routeLayers = [];
  let routePolylineCache = new Map();
  let lastRouteRenderState = {
    selectionKey: '',
    colorSignature: '',
    geometrySignature: '',
    useOverlapRenderer: !!(ENABLE_OVERLAP_DASH_RENDERING && overlapRenderer)
  };
  const stopIconCache = new Map();
  let busMarkerSvgText = null;
  let busMarkerSvgPromise = null;
  const markerAnimationHandles = new WeakMap();
  let textMeasurementCanvas = null;
  let busMarkerVisibleExtents = null;

  const MARKER_ANIMATION_DURATION_MS = 1000;

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
        busMarkerVisibleExtents = null;
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
      if (routeShape.style && typeof routeShape.style.setProperty === 'function') {
        routeShape.style.setProperty('fill', normalizedColor);
      }
      if (typeof routeShape.classList === 'object' && typeof routeShape.classList.remove === 'function') {
        routeShape.classList.remove('st1');
      }
    }
    const container = document.createElement('div');
    container.className = 'bus-marker-icon';
    container.dataset.routeColor = normalizedColor;
    if (container.style && typeof container.style.setProperty === 'function') {
      container.style.setProperty('--bus-color', normalizedColor);
    }
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

  function getTextMeasurementContext() {
    if (!textMeasurementCanvas && typeof document !== 'undefined') {
      textMeasurementCanvas = document.createElement('canvas');
    }
    return textMeasurementCanvas ? textMeasurementCanvas.getContext('2d') : null;
  }

  function measureLabelTextWidth(text, fontSizePx, fontWeight = 'bold') {
    const ctx = getTextMeasurementContext();
    const normalizedFontSize = Math.max(1, Number(fontSizePx) || 0);
    if (!ctx) {
      return (typeof text === 'string' ? text.length : 0) * normalizedFontSize * 0.6;
    }
    ctx.font = `${fontWeight} ${normalizedFontSize}px ${BUS_MARKER_LABEL_FONT_FAMILY}`;
    const metrics = ctx.measureText(text || '');
    return metrics && Number.isFinite(metrics.width)
      ? metrics.width
      : (typeof text === 'string' ? text.length : 0) * normalizedFontSize * 0.6;
  }

  function roundToTwoDecimals(value) {
    return Math.round((Number(value) || 0) * 100) / 100;
  }

  function getBusMarkerVisibleExtents() {
    if (busMarkerVisibleExtents) {
      return busMarkerVisibleExtents;
    }

    const fallbackExtents = {
      top: BUS_MARKER_PIVOT_Y,
      bottom: BUS_MARKER_VIEWBOX_HEIGHT - BUS_MARKER_PIVOT_Y,
      left: BUS_MARKER_PIVOT_X,
      right: BUS_MARKER_VIEWBOX_WIDTH - BUS_MARKER_PIVOT_X
    };

    if (typeof document === 'undefined' || typeof busMarkerSvgText !== 'string' || !busMarkerSvgText.trim()) {
      busMarkerVisibleExtents = fallbackExtents;
      return busMarkerVisibleExtents;
    }

    try {
      const template = document.createElement('template');
      template.innerHTML = busMarkerSvgText.trim();
      const svg = template.content.firstElementChild;
      if (!svg || svg.tagName.toLowerCase() !== 'svg') {
        busMarkerVisibleExtents = fallbackExtents;
        return busMarkerVisibleExtents;
      }

      const clone = svg.cloneNode(true);
      clone.style.position = 'absolute';
      clone.style.visibility = 'hidden';
      clone.style.pointerEvents = 'none';
      clone.style.left = '-9999px';
      clone.style.top = '-9999px';
      const host = document.body || document.documentElement;
      if (!host) {
        busMarkerVisibleExtents = fallbackExtents;
        return busMarkerVisibleExtents;
      }

      host.appendChild(clone);
      let bbox = null;
      try {
        bbox = clone.getBBox();
      } finally {
        clone.remove();
      }

      if (bbox && Number.isFinite(bbox.x) && Number.isFinite(bbox.y) && Number.isFinite(bbox.width) && Number.isFinite(bbox.height)) {
        busMarkerVisibleExtents = {
          top: BUS_MARKER_PIVOT_Y - bbox.y,
          bottom: bbox.y + bbox.height - BUS_MARKER_PIVOT_Y,
          left: BUS_MARKER_PIVOT_X - bbox.x,
          right: (bbox.x + bbox.width) - BUS_MARKER_PIVOT_X
        };
        return busMarkerVisibleExtents;
      }
    } catch (error) {
      console.error('Failed to compute bus marker visible extents:', error);
    }

    busMarkerVisibleExtents = fallbackExtents;
    return busMarkerVisibleExtents;
  }

  function computeBusMarkerVerticalExtentsForHeading(headingDeg) {
    const extents = getBusMarkerVisibleExtents();
    if (!extents) {
      return null;
    }

    const normalizedHeading = normalizeHeadingDegrees(Number.isFinite(headingDeg) ? headingDeg : BUS_MARKER_DEFAULT_HEADING);
    const radians = normalizedHeading * Math.PI / 180;
    const sin = Math.sin(radians);
    const cos = Math.cos(radians);

    const corners = [
      { x: -extents.left, y: -extents.top },
      { x: extents.right, y: -extents.top },
      { x: extents.right, y: extents.bottom },
      { x: -extents.left, y: extents.bottom }
    ];

    let minY = Infinity;
    let maxY = -Infinity;
    for (const corner of corners) {
      const rotatedY = corner.x * sin + corner.y * cos;
      if (rotatedY < minY) {
        minY = rotatedY;
      }
      if (rotatedY > maxY) {
        maxY = rotatedY;
      }
    }

    if (!Number.isFinite(minY) || !Number.isFinite(maxY)) {
      return null;
    }

    return {
      top: Math.abs(minY),
      bottom: Math.abs(maxY)
    };
  }

  function computeLabelLeaderOffset(scale, headingDeg, position = 'above') {
    const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
    const normalizedHeading = normalizeHeadingDegrees(
      Number.isFinite(headingDeg) ? headingDeg : BUS_MARKER_DEFAULT_HEADING
    );
    const conversionFactor = (BUS_MARKER_BASE_WIDTH_PX * safeScale) / BUS_MARKER_VIEWBOX_WIDTH;
    const fallbackWidth = BUS_MARKER_BASE_WIDTH_PX * safeScale;
    const fallbackHeight = fallbackWidth * BUS_MARKER_ASPECT_RATIO;
    const fallbackHalfDiagonal = Math.sqrt(fallbackWidth * fallbackWidth + fallbackHeight * fallbackHeight) / 2;
    const clearance = LABEL_VERTICAL_CLEARANCE_PX * safeScale;

    const headingRelativeToVertical = normalizedHeading % 180;
    const deviationFromVertical = Math.min(headingRelativeToVertical, 180 - headingRelativeToVertical);
    const verticality = Math.pow(
      Math.max(0, Math.cos(deviationFromVertical * Math.PI / 180)),
      LABEL_VERTICAL_ALIGNMENT_EXPONENT
    );
    const verticalAlignmentBonus = LABEL_VERTICAL_ALIGNMENT_BONUS_PX * safeScale * verticality;
    const horizontalAlignmentBonus = LABEL_HORIZONTAL_ALIGNMENT_BONUS_PX * safeScale * (1 - verticality);

    const verticalExtents = computeBusMarkerVerticalExtentsForHeading(normalizedHeading);
    if (!verticalExtents) {
      return Math.max(0, fallbackHalfDiagonal + clearance + verticalAlignmentBonus + horizontalAlignmentBonus);
    }

    let extentSvgUnits;
    if (position === 'below') {
      extentSvgUnits = verticalExtents.bottom;
    } else if (position === 'above') {
      extentSvgUnits = verticalExtents.top;
    } else {
      extentSvgUnits = Math.max(verticalExtents.top, verticalExtents.bottom);
    }

    if (!Number.isFinite(extentSvgUnits)) {
      return Math.max(0, fallbackHalfDiagonal + clearance + verticalAlignmentBonus + horizontalAlignmentBonus);
    }

    const extentPx = extentSvgUnits * conversionFactor;
    const totalOffset = extentPx + clearance + verticalAlignmentBonus + horizontalAlignmentBonus;
    return totalOffset > 0 ? totalOffset : 0;
  }

  function normalizeHeadingDegrees(degrees) {
    const normalized = Number.isFinite(degrees) ? degrees : BUS_MARKER_DEFAULT_HEADING;
    return ((normalized % 360) + 360) % 360;
  }

  function computeBusMarkerGlyphColor(routeColor) {
    const fallback = BUS_MARKER_DEFAULT_CONTRAST_COLOR;
    const candidate = typeof routeColor === 'string' && routeColor.trim().length > 0
      ? routeColor.trim()
      : BUS_MARKER_DEFAULT_ROUTE_COLOR;
    const contrast = contrastBW(candidate);
    return contrast || fallback;
  }

  function contrastBW(hex) {
    if (typeof hex !== 'string' || hex.trim().length === 0) {
      return '#ffffff';
    }
    let normalized = hex.trim().replace(/^#/, '');
    if (normalized.length === 3) {
      normalized = normalized.split('').map(ch => ch + ch).join('');
    }
    if (normalized.length !== 6 || /[^0-9a-f]/i.test(normalized)) {
      return '#ffffff';
    }
    const r = parseInt(normalized.substring(0, 2), 16) / 255;
    const g = parseInt(normalized.substring(2, 4), 16) / 255;
    const b = parseInt(normalized.substring(4, 6), 16) / 255;
    const L = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    return L > 0.55 ? '#000000' : '#ffffff';
  }

  function createNameBubbleDivIcon(busName, routeColor, scale, headingDeg) {
    if (typeof busName !== 'string' || busName.trim().length === 0) {
      return null;
    }
    const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
    const name = busName.trim();
    const fillColor = typeof routeColor === 'string' && routeColor.trim().length > 0
      ? routeColor
      : BUS_MARKER_DEFAULT_ROUTE_COLOR;
    const fontSize = Math.max(BUS_MARKER_LABEL_MIN_FONT_PX, NAME_BUBBLE_BASE_FONT_PX * safeScale);
    const horizontalPadding = NAME_BUBBLE_HORIZONTAL_PADDING * safeScale;
    const verticalPadding = NAME_BUBBLE_VERTICAL_PADDING * safeScale;
    const frameInset = NAME_BUBBLE_FRAME_INSET * safeScale;
    const textWidth = measureLabelTextWidth(name, fontSize);
    const rectWidth = Math.max(NAME_BUBBLE_MIN_WIDTH * safeScale, textWidth + horizontalPadding * 2);
    const rectHeight = Math.max(NAME_BUBBLE_MIN_HEIGHT * safeScale, fontSize + verticalPadding * 2);
    const svgWidth = roundToTwoDecimals(rectWidth);
    const svgHeight = roundToTwoDecimals(rectHeight + frameInset * 2);
    const radius = NAME_BUBBLE_CORNER_RADIUS * safeScale;
    const strokeWidth = Math.max(1, LABEL_BASE_STROKE_WIDTH * safeScale);
    const radiusRounded = roundToTwoDecimals(radius);
    const strokeWidthRounded = roundToTwoDecimals(strokeWidth);
    const rectY = roundToTwoDecimals(frameInset);
    const rectHeightRounded = roundToTwoDecimals(rectHeight);
    const textX = roundToTwoDecimals(svgWidth / 2);
    const baselineShift = fontSize * LABEL_TEXT_VERTICAL_ADJUSTMENT_RATIO;
    const textY = roundToTwoDecimals(rectY + rectHeight / 2 + baselineShift);
    const anchorX = textX;
    const leaderOffset = roundToTwoDecimals(computeLabelLeaderOffset(safeScale, headingDeg, 'above'));
    const anchorY = svgHeight + leaderOffset;
    const textColor = computeBusMarkerGlyphColor(fillColor);
    const fontSizeRounded = roundToTwoDecimals(fontSize);
    const svg = `
      <svg width="${svgWidth}" height="${svgHeight}" viewBox="0 0 ${svgWidth} ${svgHeight}" xmlns="http://www.w3.org/2000/svg" style="pointer-events: none;">
        <g>
          <rect x="0" y="${rectY}" width="${svgWidth}" height="${rectHeightRounded}" rx="${radiusRounded}" ry="${radiusRounded}" fill="${fillColor}" stroke="white" stroke-width="${strokeWidthRounded}" />
          <text x="${textX}" y="${textY}" dominant-baseline="middle" alignment-baseline="middle" text-anchor="middle" font-size="${fontSizeRounded}" font-weight="bold" fill="${textColor}" font-family="${BUS_MARKER_LABEL_FONT_FAMILY}">${escapeHtml(name)}</text>
        </g>
      </svg>`;
    return L.divIcon({
      html: svg,
      className: 'leaflet-div-icon bus-label-icon',
      iconSize: [svgWidth, svgHeight],
      iconAnchor: [anchorX, anchorY]
    });
  }

  function createBlockBubbleDivIcon(blockName, routeColor, scale, headingDeg) {
    if (typeof blockName !== 'string' || blockName.trim() === '') {
      return null;
    }
    const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
    const name = blockName.trim();
    const fillColor = typeof routeColor === 'string' && routeColor.trim().length > 0
      ? routeColor
      : BUS_MARKER_DEFAULT_ROUTE_COLOR;
    const fontSize = Math.max(BUS_MARKER_LABEL_MIN_FONT_PX, BLOCK_BUBBLE_BASE_FONT_PX * safeScale);
    const horizontalPadding = BLOCK_BUBBLE_HORIZONTAL_PADDING * safeScale;
    const verticalPadding = BLOCK_BUBBLE_VERTICAL_PADDING * safeScale;
    const frameInset = BLOCK_BUBBLE_FRAME_INSET * safeScale;
    const textWidth = measureLabelTextWidth(name, fontSize);
    const rectWidth = Math.max(BLOCK_BUBBLE_MIN_WIDTH * safeScale, textWidth + horizontalPadding * 2);
    const rectHeight = Math.max(BLOCK_BUBBLE_MIN_HEIGHT * safeScale, fontSize + verticalPadding * 2);
    const svgWidth = roundToTwoDecimals(rectWidth);
    const svgHeight = roundToTwoDecimals(rectHeight + frameInset * 2);
    const radius = BLOCK_BUBBLE_CORNER_RADIUS * safeScale;
    const strokeWidth = Math.max(1, LABEL_BASE_STROKE_WIDTH * safeScale);
    const radiusRounded = roundToTwoDecimals(radius);
    const strokeWidthRounded = roundToTwoDecimals(strokeWidth);
    const rectY = roundToTwoDecimals(frameInset);
    const rectHeightRounded = roundToTwoDecimals(rectHeight);
    const textX = roundToTwoDecimals(svgWidth / 2);
    const baselineShift = fontSize * LABEL_TEXT_VERTICAL_ADJUSTMENT_RATIO;
    const textY = roundToTwoDecimals(rectY + rectHeight / 2 + baselineShift);
    const anchorX = textX;
    const leaderOffset = roundToTwoDecimals(computeLabelLeaderOffset(safeScale, headingDeg, 'below'));
    const anchorY = -leaderOffset;
    const textColor = computeBusMarkerGlyphColor(fillColor);
    const fontSizeRounded = roundToTwoDecimals(fontSize);
    const svg = `
      <svg width="${svgWidth}" height="${svgHeight}" viewBox="0 0 ${svgWidth} ${svgHeight}" xmlns="http://www.w3.org/2000/svg" style="pointer-events: none;">
        <g>
          <rect x="0" y="${rectY}" width="${svgWidth}" height="${rectHeightRounded}" rx="${radiusRounded}" ry="${radiusRounded}" fill="${fillColor}" stroke="white" stroke-width="${strokeWidthRounded}" />
          <text x="${textX}" y="${textY}" dominant-baseline="middle" alignment-baseline="middle" text-anchor="middle" font-size="${fontSizeRounded}" font-weight="bold" fill="${textColor}" font-family="${BUS_MARKER_LABEL_FONT_FAMILY}">${escapeHtml(name)}</text>
        </g>
      </svg>`;
    return L.divIcon({
      html: svg,
      className: 'leaflet-div-icon bus-label-icon',
      iconSize: [svgWidth, svgHeight],
      iconAnchor: [anchorX, anchorY]
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

  function normalizeLabelSegment(value) {
    if (value === null || value === undefined) {
      return '';
    }
    const raw = typeof value === 'string' ? value : String(value);
    const trimmed = raw.trim();
    if (!trimmed) {
      return '';
    }
    return trimmed.replace(/\s+/g, ' ');
  }

  function selectFirstString(values) {
    if (!Array.isArray(values)) {
      return '';
    }
    for (let index = 0; index < values.length; index += 1) {
      const candidate = normalizeLabelSegment(values[index]);
      if (candidate) {
        return candidate;
      }
    }
    return '';
  }

  function extractVehicleName(vehicle) {
    if (!vehicle || typeof vehicle !== 'object') {
      return '';
    }
    return selectFirstString([
      vehicle.Name,
      vehicle.VehicleName,
      vehicle.Vehicle,
      vehicle.VehicleNumber,
      vehicle.VehicleNum,
      vehicle.VehicleIdentifier
    ]);
  }

  function extractBlockLabel(vehicle, blocks) {
    if (!vehicle || typeof vehicle !== 'object' || !blocks || typeof blocks !== 'object') {
      return '';
    }
    const rawId = vehicle.VehicleID ?? vehicle.VehicleId ?? vehicle.id;
    if (rawId === undefined || rawId === null) {
      return '';
    }
    const key = String(rawId);
    const value = blocks[key];
    if (typeof value === 'string' && value.trim()) {
      return value;
    }
    if (value && typeof value === 'object') {
      if (Array.isArray(value)) {
        return selectFirstString(value);
      }
      return selectFirstString([
        value.Name,
        value.BlockName,
        value.BlockId,
        value.BlockID,
        value.label,
        value.title
      ]);
    }
    return '';
  }

  function formatBlockBubbleText(blockLabel) {
    const normalized = normalizeLabelSegment(blockLabel);
    if (!normalized) {
      return '';
    }
    const lower = normalized.toLowerCase();
    if (lower === 'block') {
      return '';
    }
    const blockPrefix = 'block ';
    if (lower.startsWith(blockPrefix)) {
      const withoutPrefix = normalizeLabelSegment(normalized.slice(blockPrefix.length));
      return withoutPrefix;
    }
    return normalized;
  }

  const ROUTE_LAYER_BASE_OPTIONS = Object.freeze({
    updateWhenZooming: true,
    updateWhenIdle: true,
    interactive: false
  });
  const routePaneName = ROUTE_PANE;

  function createSpatialIndex(options = {}) {
    if (typeof rbush === 'function') {
      try {
        return rbush(options.maxEntries);
      } catch (error) {
        console.error('Failed to create rbush index via rbush()', error);
      }
    }
    if (typeof RBush === 'function') {
      try {
        return new RBush(options.maxEntries);
      } catch (error) {
        console.error('Failed to create rbush index via new RBush()', error);
      }
    }
    console.error('RBush spatial index library is not available. Route overlap rendering will be disabled.');
    return null;
  }

  function mergeRouteLayerOptions(overrides = {}, rendererOverride = null, paneOverride = null) {
    const base = Object.assign({}, ROUTE_LAYER_BASE_OPTIONS);
    const renderer = rendererOverride || sharedRouteRenderer;
    if (renderer) {
      base.renderer = renderer;
    }
    const pane = paneOverride || routePaneName;
    if (typeof pane === 'string' && pane) {
      base.pane = pane;
    }
    return Object.assign(base, overrides || {});
  }

  function computeRouteStrokeWeight(zoom) {
    const minWeight = MIN_ROUTE_STROKE_WEIGHT;
    const maxWeight = MAX_ROUTE_STROKE_WEIGHT;
    const baseWeight = DEFAULT_ROUTE_STROKE_WEIGHT;
    const targetZoom = Number.isFinite(zoom)
      ? zoom
      : (typeof map?.getZoom === 'function' ? map.getZoom() : null);
    if (!Number.isFinite(targetZoom)) {
      return Math.max(minWeight, Math.min(maxWeight, baseWeight));
    }
    const zoomDeltaRaw = targetZoom - ROUTE_WEIGHT_BASE_ZOOM;
    const limitedDelta = Math.max(-ROUTE_WEIGHT_ZOOM_DELTA_LIMIT, Math.min(ROUTE_WEIGHT_ZOOM_DELTA_LIMIT, zoomDeltaRaw));
    const computed = baseWeight + ROUTE_WEIGHT_STEP_PER_ZOOM * limitedDelta;
    if (!Number.isFinite(computed)) {
      return Math.max(minWeight, Math.min(maxWeight, baseWeight));
    }
    return Math.max(minWeight, Math.min(maxWeight, computed));
  }

      class OverlapRouteRenderer {
        constructor(map, options = {}) {
          this.map = map;
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
            maxStrokeWeight: MAX_ROUTE_STROKE_WEIGHT
          }, options);
          this.layers = [];
          this.routeGeometries = new Map();
          this.selectedRoutes = [];
          this.currentZoom = typeof map?.getZoom === 'function' ? map.getZoom() : null;
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
          this.layers.forEach(layer => {
            if (layer && this.map.hasLayer(layer)) {
              this.map.removeLayer(layer);
            }
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

          const desiredIds = new Set(
            selectedRouteIds
              .map(id => Number(id))
              .filter(id => !Number.isNaN(id))
          );

          const nextGeometries = new Map();
          geometryEntries.forEach(([key, value]) => {
            const numericKey = Number(key);
            if (!Number.isNaN(numericKey) && desiredIds.has(numericKey) && Array.isArray(value)) {
              nextGeometries.set(numericKey, value);
            }
          });

          const geometrySignatures = new Map();

          this.routeGeometries = nextGeometries;
          this.selectedRoutes = Array.from(this.routeGeometries.keys()).sort((a, b) => a - b);

          this.routeGeometries.forEach((latlngs, routeId) => {
            geometrySignatures.set(routeId, this.computeRouteGeometrySignature(latlngs));
          });
          this.routeGeometrySignatures = geometrySignatures;

          const mapZoom = typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null;
          if (Number.isFinite(mapZoom)) {
            this.currentZoom = mapZoom;
          }

          this.render();
          return this.getLayers();
        }

        handleZoomFrame(targetZoom) {
          if (this.routeGeometries.size === 0 || this.selectedRoutes.length === 0) {
            return this.getLayers();
          }

          const zoom = Number.isFinite(targetZoom)
            ? targetZoom
            : (typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null);
          if (!Number.isFinite(zoom)) {
            return this.getLayers();
          }

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

        hasPersistentPixelCache() {
          return false;
        }

        computeStrokeWeight(zoom = this.currentZoom) {
          const minWeight = Number.isFinite(this.options.minStrokeWeight)
            ? this.options.minStrokeWeight
            : MIN_ROUTE_STROKE_WEIGHT;
          const maxWeight = Number.isFinite(this.options.maxStrokeWeight)
            ? this.options.maxStrokeWeight
            : MAX_ROUTE_STROKE_WEIGHT;
          const computed = computeRouteStrokeWeight(zoom);
          if (!Number.isFinite(computed)) {
            return Math.max(minWeight, Math.min(maxWeight, DEFAULT_ROUTE_STROKE_WEIGHT));
          }
          return Math.max(minWeight, Math.min(maxWeight, computed));
        }

        computeRouteGeometrySignature(latlngs) {
          if (!Array.isArray(latlngs) || latlngs.length === 0) {
            return 'empty';
          }

          const totalPoints = latlngs.length;
          const sampleCount = Math.min(totalPoints, 10);
          const step = Math.max(1, Math.floor(totalPoints / sampleCount));
          const parts = [totalPoints];

          const extractCoordinate = (point, key) => {
            if (!point) {
              return Number.NaN;
            }
            if (typeof point[key] === 'number') {
              return point[key];
            }
            if (point.latlng && typeof point.latlng[key] === 'number') {
              return point.latlng[key];
            }
            if (Array.isArray(point) && point.length >= 2) {
              const index = key === 'lat' ? 0 : 1;
              const value = Number(point[index]);
              return Number.isFinite(value) ? value : Number.NaN;
            }
            const altKey = key === 'lat' ? 'latitude' : 'longitude';
            if (typeof point[altKey] === 'number') {
              return point[altKey];
            }
            const upperKey = key === 'lat' ? 'Latitude' : 'Longitude';
            if (typeof point[upperKey] === 'number') {
              return point[upperKey];
            }
            return Number.NaN;
          };

          const appendPoint = (point) => {
            const lat = extractCoordinate(point, 'lat');
            const lng = extractCoordinate(point, 'lng');
            const format = value => (Number.isFinite(value) ? value.toFixed(6) : 'nan');
            parts.push(`${format(lat)},${format(lng)}`);
          };

          for (let i = 0; i < totalPoints; i += step) {
            appendPoint(latlngs[i]);
          }

          const lastPoint = latlngs[totalPoints - 1];
          if (lastPoint && (totalPoints - 1) % step !== 0) {
            appendPoint(lastPoint);
          }

          return parts.join('|');
        }

        render() {
          if (!this.map) return;

          const zoom = Number.isFinite(this.currentZoom)
            ? this.currentZoom
            : (typeof this.map?.getZoom === 'function' ? this.map.getZoom() : null);

          const selectionKey = this.selectedRoutes.join(',');
          const geometrySignature = this.selectedRoutes
            .map(routeId => `${routeId}:${this.routeGeometrySignatures.get(routeId) || ''}`)
            .join('|');
          const colorSignature = this.selectedRoutes
            .map(routeId => {
              const color = getRouteColor(routeId);
              return `${routeId}:${typeof color === 'string' ? color : ''}`;
            })
            .join('|');
          const zoomKey = Number.isFinite(zoom) ? zoom.toFixed(6) : 'NaN';
          const nextRenderState = {
            selectionKey,
            geometrySignature,
            colorSignature,
            zoomKey,
            didRender: false
          };

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
          if (lastState
            && lastState.didRender
            && lastState.selectionKey === selectionKey
            && lastState.geometrySignature === geometrySignature
            && lastState.colorSignature === colorSignature
            && lastState.zoomKey === zoomKey) {
            return;
          }

          this.clearLayers();

          const step = Number.isFinite(this.options.sampleStepPx) && this.options.sampleStepPx > 0
            ? this.options.sampleStepPx
            : 8;
          const tolerance = Number.isFinite(this.options.matchTolerancePx)
            ? this.options.matchTolerancePx
            : 6;
          const headingToleranceRad = (Number.isFinite(this.options.headingToleranceDeg)
            ? this.options.headingToleranceDeg
            : 20) * Math.PI / 180;

          const segmentsByRoute = new Map();
          const spatialItems = [];

          this.routeGeometries.forEach((latlngs, routeId) => {
            if (!Array.isArray(latlngs) || latlngs.length < 2) {
              return;
            }

            const segments = this.resampleRoute(routeId, latlngs, zoom, step);
            if (!Array.isArray(segments) || segments.length === 0) {
              return;
            }

            segmentsByRoute.set(routeId, segments);

            segments.forEach(segment => {
              spatialItems.push({
                minX: segment.bounds.minX - tolerance,
                minY: segment.bounds.minY - tolerance,
                maxX: segment.bounds.maxX + tolerance,
                maxY: segment.bounds.maxY + tolerance,
                segment
              });
            });
          });

          if (spatialItems.length === 0) {
            nextRenderState.didRender = true;
            this.lastRenderState = nextRenderState;
            return;
          }

          const tree = createSpatialIndex({ maxEntries: this.options.maxEntries });
          if (!tree || typeof tree.load !== 'function' || typeof tree.search !== 'function') {
            console.error('RBush spatial index instance is invalid; skipping overlap rendering.');
            this.lastRenderState = nextRenderState;
            return;
          }

          tree.clear?.();
          tree.load(spatialItems);
          this.populateSharedRoutes(spatialItems, tree, tolerance, headingToleranceRad);

          const groups = this.buildGroups(segmentsByRoute, zoom);
          this.drawGroups(groups);
          nextRenderState.didRender = true;
          this.lastRenderState = nextRenderState;
        }

        populateSharedRoutes(spatialItems, tree, tolerance, headingToleranceRad) {
          const processedPairs = new Set();

          spatialItems.forEach(item => {
            const segment = item.segment;
            if (!segment) return;

            const candidates = tree.search(item);
            candidates.forEach(candidate => {
              const other = candidate.segment;
              if (!other || other === segment) return;
              if (other.routeId === segment.routeId) return;

              const pairKey = segment.routeId < other.routeId
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
          if (!target.routeOffsets) {
            target.routeOffsets = {};
          }

          const sourceOffset = this.extractRouteOffset(source, source.routeId);
          if (!Number.isFinite(sourceOffset)) {
            return;
          }

          const existing = target.routeOffsets[source.routeId];
          const candidate = Number.isFinite(existing?.min) ? Math.min(existing.min, sourceOffset) : sourceOffset;
          target.routeOffsets[source.routeId] = { min: candidate };
        }

        extractRouteOffset(segment, routeId) {
          if (!segment) return null;
          const offsets = segment.routeOffsets || {};
          const direct = offsets[routeId];
          if (direct && Number.isFinite(direct.min)) {
            return direct.min;
          }

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
            const ordered = segments.slice().sort((a, b) => {
              const aOffset = Number(a.start?.cumulativeLength) || 0;
              const bOffset = Number(b.start?.cumulativeLength) || 0;
              return aOffset - bOffset;
            });

            let current = null;

            ordered.forEach(segment => {
              const sharedRoutes = Array.from(segment.sharedRoutes || []).sort((a, b) => a - b);
              if (sharedRoutes.length === 0) return;

              const primary = sharedRoutes[0];
              if (primary !== routeId) {
                return;
              }

              const needsNewGroup = !current
                || !this.sameRouteSet(current.routes, sharedRoutes)
                || !this.latLngsClose(current.lastLatLng, segment.start.latlng);

              if (needsNewGroup) {
                if (current) {
                  const finalized = this.finalizeGroup(current, zoom);
                  if (finalized) {
                    groups.push(finalized);
                  }
                }

                current = {
                  routes: sharedRoutes,
                  segments: [],
                  points: [],
                  offsets: new Map(),
                  lastLatLng: null
                };
              }

              current.segments.push(segment);

              if (current.points.length === 0) {
                current.points.push(segment.start.latlng);
              } else if (!this.latLngsClose(current.points[current.points.length - 1], segment.start.latlng)) {
                current.points.push(segment.start.latlng);
              }
              current.points.push(segment.end.latlng);
              current.lastLatLng = segment.end.latlng;

              const routeOffsets = segment.routeOffsets || {};
              current.routes.forEach(routeKey => {
                const candidate = Number(routeOffsets?.[routeKey]?.min ?? routeOffsets?.[routeKey]);
                if (Number.isFinite(candidate)) {
                  const existing = current.offsets.get(routeKey);
                  if (!Number.isFinite(existing) || candidate < existing) {
                    current.offsets.set(routeKey, candidate);
                  }
                }
              });
            });

            if (current) {
              const finalized = this.finalizeGroup(current, zoom);
              if (finalized) {
                groups.push(finalized);
              }
              current = null;
            }
          });

          return groups;
        }

        finalizeGroup(group, zoom) {
          const points = this.collapsePoints(group.points || []);
          if (points.length < 2) {
            return null;
          }

          const lengthPx = group.segments.reduce((sum, segment) => {
            const value = Number(segment.lengthPx);
            return sum + (Number.isFinite(value) ? value : 0);
          }, 0);

          const primaryRoute = group.routes[0];
          const offsetCandidates = group.segments
            .map(segment => Number(segment.routeOffsets?.[primaryRoute]?.min ?? segment.routeOffsets?.[primaryRoute]))
            .filter(value => Number.isFinite(value));
          const offsetPx = offsetCandidates.length > 0 ? Math.min(...offsetCandidates) : 0;

          const offsetMap = new Map();
          group.offsets.forEach((value, key) => {
            if (Number.isFinite(value)) {
              offsetMap.set(key, value);
            }
          });

          return {
            routes: group.routes.slice(),
            points,
            lengthPx,
            offsetPx,
            routeOffsets: offsetMap
          };
        }

        collapsePoints(points) {
          const collapsed = [];
          points.forEach(point => {
            if (collapsed.length === 0 || !this.latLngsClose(collapsed[collapsed.length - 1], point)) {
              collapsed.push(point);
            }
          });
          return collapsed;
        }

        sameRouteSet(a, b) {
          if (!Array.isArray(a) || !Array.isArray(b)) return false;
          if (a.length !== b.length) return false;
          for (let i = 0; i < a.length; i++) {
            if (a[i] !== b[i]) return false;
          }
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

          groups.forEach(group => {
            if (!group || !Array.isArray(group.routes) || group.routes.length === 0) return;
            if (!Array.isArray(group.points) || group.points.length < 2) return;

            const coords = group.points.map(latlng => [latlng.lat, latlng.lng]);
            const sortedRoutes = group.routes.slice().sort((a, b) => a - b);
            const offsetsByRoute = new Map();

            if (group.routeOffsets instanceof Map) {
              group.routeOffsets.forEach((value, routeId) => {
                const numericRoute = Number(routeId);
                const numericValue = Number(value);
                if (Number.isFinite(numericRoute) && Number.isFinite(numericValue)) {
                  const existing = offsetsByRoute.get(numericRoute);
                  if (!Number.isFinite(existing) || numericValue < existing) {
                    offsetsByRoute.set(numericRoute, numericValue);
                  }
                }
              });
            } else if (group.routeOffsets && typeof group.routeOffsets === 'object') {
              Object.entries(group.routeOffsets).forEach(([routeKey, info]) => {
                const numericRoute = Number(routeKey);
                const numericValue = Number(info?.min ?? info);
                if (Number.isFinite(numericRoute) && Number.isFinite(numericValue)) {
                  const existing = offsetsByRoute.get(numericRoute);
                  if (!Number.isFinite(existing) || numericValue < existing) {
                    offsetsByRoute.set(numericRoute, numericValue);
                  }
                }
              });
            }

            if (sortedRoutes.length === 1) {
              const routeId = sortedRoutes[0];
              const layer = L.polyline(coords, mergeRouteLayerOptions({
                color: getRouteColor(routeId),
                weight,
                opacity: 1,
                lineCap: 'round',
                lineJoin: 'round'
              }, this.renderer, this.routePaneName)).addTo(this.map);
              newLayers.push(layer);
              return;
            }

            const groupLength = group.lengthPx || 0;
            if (!(groupLength > 0)) return;
            const stripeCount = sortedRoutes.length;
            let dashLength = dashBase;
            if (dashLength * stripeCount > groupLength) {
              dashLength = groupLength / stripeCount;
            }
            if (!(dashLength > 0)) {
              dashLength = minDash;
            }

            const gapLength = dashLength * (stripeCount - 1);
            const patternLength = dashLength + gapLength;

            let baseOffsetValue;
            const tolerance = 1e-9;
            let anchorRouteId = null;
            let anchorOffset = -Infinity;

            sortedRoutes.forEach(routeId => {
              const offsetValue = offsetsByRoute.get(routeId);
              if (Number.isFinite(offsetValue)) {
                if (
                  anchorRouteId === null ||
                  offsetValue > anchorOffset + tolerance ||
                  (Math.abs(offsetValue - anchorOffset) <= tolerance && routeId < anchorRouteId)
                ) {
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
                  if (Number.isFinite(adjustment) && adjustment !== 0) {
                    dashOffsetValue += adjustment * patternLength;
                  }
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
                lineJoin: 'round'
              }, this.renderer, this.routePaneName)).addTo(this.map);
              newLayers.push(layer);
            });
          });

          this.layers = newLayers;
        }

        toLatLng(candidate) {
          if (!candidate) {
            return null;
          }

          if (candidate instanceof L.LatLng) {
            return candidate;
          }

          if (Array.isArray(candidate)) {
            if (candidate.length < 2) {
              return null;
            }
            const latValue = candidate[0];
            const lngValue = candidate[1];
            if (latValue === null || latValue === undefined || lngValue === null || lngValue === undefined) {
              return null;
            }
            const lat = toNumber(latValue);
            const lng = toNumber(lngValue);
            if (lat === null || lng === null) {
              return null;
            }
            return L.latLng(lat, lng);
          }

          if (typeof candidate === 'object') {
            const wrappers = ['latlng', 'latLng', 'LatLng'];
            for (let index = 0; index < wrappers.length; index += 1) {
              const key = wrappers[index];
              if (candidate[key]) {
                return this.toLatLng(candidate[key]);
              }
            }

            const latKeys = ['lat', 'latitude', 'Latitude', 'Lat'];
            const lngKeys = ['lng', 'lon', 'longitude', 'Longitude', 'Lng', 'Long', 'Lon'];

            let lat = null;
            for (let index = 0; index < latKeys.length; index += 1) {
              const value = candidate[latKeys[index]];
              if (value === null || value === undefined) {
                continue;
              }
              lat = toNumber(value);
              if (lat !== null) {
                break;
              }
            }

            let lng = null;
            for (let index = 0; index < lngKeys.length; index += 1) {
              const value = candidate[lngKeys[index]];
              if (value === null || value === undefined) {
                continue;
              }
              lng = toNumber(value);
              if (lng !== null) {
                break;
              }
            }

            if (lat !== null && lng !== null) {
              return L.latLng(lat, lng);
            }
          }

          return null;
        }

        simplifyLatLngs(latlngs, zoom) {
          if (!Array.isArray(latlngs) || latlngs.length === 0) {
            return [];
          }

          const normalized = [];
          for (let index = 0; index < latlngs.length; index += 1) {
            const latlng = this.toLatLng(latlngs[index]);
            if (latlng) {
              normalized.push(latlng);
            }
          }

          if (normalized.length === 0) {
            return [];
          }

          const projected = normalized.map(latlng => this.map.project(latlng, zoom));
          let simplified = projected;
          if (projected.length > 2 && this.options.simplifyTolerancePx > 0 && L.LineUtil && L.LineUtil.simplify) {
            simplified = L.LineUtil.simplify(projected, this.options.simplifyTolerancePx);
          }

          return simplified.map(pt => ({
            point: L.point(pt.x, pt.y),
            latlng: this.map.unproject(pt, zoom)
          }));
        }

        resampleRoute(routeId, latlngs, zoom, step) {
          const simplified = this.simplifyLatLngs(latlngs, zoom);
          if (simplified.length < 2) {
            return [];
          }

          const samples = [];
          const first = simplified[0];
          samples.push({
            latlng: first.latlng,
            point: first.point,
            cumulativeLength: 0
          });

          let traversed = 0;
          let distanceSinceLast = 0;

          for (let i = 1; i < simplified.length; i++) {
            const prev = simplified[i - 1];
            const curr = simplified[i];
            const segmentLength = this.distance(prev.point, curr.point);
            if (segmentLength === 0) {
              continue;
            }

            let consumed = 0;
            while (distanceSinceLast + (segmentLength - consumed) >= step) {
              const remaining = step - distanceSinceLast;
              consumed += remaining;
              const ratio = consumed / segmentLength;
              const samplePoint = this.interpolatePoint(prev.point, curr.point, ratio);
              const sampleLatLng = this.map.unproject(samplePoint, zoom);
              traversed += remaining;
              samples.push({
                latlng: sampleLatLng,
                point: samplePoint,
                cumulativeLength: traversed
              });
              distanceSinceLast = 0;
            }

            const leftover = segmentLength - consumed;
            traversed += leftover;
            distanceSinceLast += leftover;
          }

          const last = simplified[simplified.length - 1];
          const lastSample = samples[samples.length - 1];
          if (!this.latLngsClose(lastSample.latlng, last.latlng)) {
            samples.push({
              latlng: last.latlng,
              point: last.point,
              cumulativeLength: traversed
            });
          } else {
            lastSample.cumulativeLength = traversed;
          }

          const segments = [];
          for (let i = 0; i < samples.length - 1; i++) {
            const start = samples[i];
            const end = samples[i + 1];
            const lengthPx = this.distance(start.point, end.point);
            if (!(lengthPx > 0)) {
              continue;
            }

            const bounds = {
              minX: Math.min(start.point.x, end.point.x),
              minY: Math.min(start.point.y, end.point.y),
              maxX: Math.max(start.point.x, end.point.x),
              maxY: Math.max(start.point.y, end.point.y)
            };
            const midpoint = L.point(
              (start.point.x + end.point.x) / 2,
              (start.point.y + end.point.y) / 2
            );
            const heading = Math.atan2(end.point.y - start.point.y, end.point.x - start.point.x);
            const offsetValues = [];
            const startOffset = Number(start.cumulativeLength);
            if (Number.isFinite(startOffset)) offsetValues.push(startOffset);
            const endOffset = Number(end.cumulativeLength);
            if (Number.isFinite(endOffset)) offsetValues.push(endOffset);

            const routeOffsets = {};
            if (offsetValues.length > 0) {
              routeOffsets[routeId] = { min: Math.min(...offsetValues) };
            }

            segments.push({
              routeId,
              index: segments.length,
              start,
              end,
              lengthPx,
              bounds,
              midpoint,
              heading,
              routeOffsets,
              sharedRoutes: new Set([routeId])
            });
          }

          return segments;
        }

        interpolatePoint(a, b, t) {
          return L.point(
            a.x + (b.x - a.x) * t,
            a.y + (b.y - a.y) * t
          );
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
          if (midpointDistance > tolerance) {
            return false;
          }

          const headingDiff = this.smallestHeadingDifference(a.heading, b.heading);
          if (headingDiff > headingToleranceRad && Math.abs(Math.PI - headingDiff) > headingToleranceRad) {
            return false;
          }

          const startDistance = this.distance(a.start.point, b.start.point);
          const endDistance = this.distance(a.end.point, b.end.point);
          const crossStart = this.distance(a.start.point, b.end.point);
          const crossEnd = this.distance(a.end.point, b.start.point);
          const closeEnough = Math.min(startDistance, endDistance, crossStart, crossEnd) <= tolerance * 2;

          return closeEnough;
        }

        smallestHeadingDifference(a, b) {
          let diff = Math.abs(a - b);
          diff = diff % (Math.PI * 2);
          if (diff > Math.PI) diff = (Math.PI * 2) - diff;
          return diff;
        }
      }

  function createOverlapRenderer(mapInstance) {
    if (!ENABLE_OVERLAP_DASH_RENDERING) {
      return null;
    }
    if (!mapInstance || typeof mapInstance !== 'object') {
      return null;
    }
    try {
      return new OverlapRouteRenderer(mapInstance, {
        sampleStepPx: 8,
        dashLengthPx: ROUTE_STRIPE_DASH_LENGTH,
        minDashLengthPx: 0.5,
        matchTolerancePx: 6,
        strokeWeight: DEFAULT_ROUTE_STROKE_WEIGHT,
        minStrokeWeight: MIN_ROUTE_STROKE_WEIGHT,
        maxStrokeWeight: MAX_ROUTE_STROKE_WEIGHT,
        renderer: sharedRouteRenderer,
        pane: ROUTE_PANE
      });
    } catch (error) {
      console.error('Failed to initialize overlap route renderer:', error);
      return null;
    }
  }

  function clearRouteLayers() {
    if (!Array.isArray(routeLayers) || routeLayers.length === 0) {
      routeLayers = [];
      return;
    }
    routeLayers.forEach(layer => {
      if (!layer) {
        return;
      }
      if (map && typeof map.removeLayer === 'function' && map.hasLayer(layer)) {
        map.removeLayer(layer);
        return;
      }
      if (typeof layer.remove === 'function') {
        try {
          layer.remove();
        } catch (error) {
          console.warn('Failed to remove route layer cleanly', error);
        }
      }
    });
    routeLayers = [];
  }

  function updateFallbackRouteWeights() {
    if (!Array.isArray(routeLayers) || routeLayers.length === 0) {
      return;
    }
    const weight = computeRouteStrokeWeight(typeof map?.getZoom === 'function' ? map.getZoom() : null);
    routeLayers.forEach(layer => {
      if (layer && typeof layer.setStyle === 'function') {
        layer.setStyle({ weight });
      }
    });
  }

  function buildLatLngSignature(latLngs) {
    if (!Array.isArray(latLngs) || latLngs.length === 0) {
      return '';
    }
    const parts = [];
    const limit = Math.min(latLngs.length, 256);
    for (let i = 0; i < limit; i += 1) {
      const point = latLngs[i];
      if (!point) {
        continue;
      }
      const lat = Number(point.lat ?? point.latitude ?? (Array.isArray(point) ? point[0] : null));
      const lng = Number(point.lng ?? point.lon ?? point.longitude ?? (Array.isArray(point) ? point[1] : null));
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        continue;
      }
      parts.push(`${lat.toFixed(6)},${lng.toFixed(6)}`);
    }
    return parts.join(';');
  }

  function getRouteGeometrySignature(routeId, geometryMap = null) {
    const numeric = Number(routeId);
    if (!Number.isFinite(numeric)) {
      return '';
    }
    if (routePolylineCache.has(numeric)) {
      const cacheEntry = routePolylineCache.get(numeric);
      if (cacheEntry && typeof cacheEntry.encoded === 'string') {
        return cacheEntry.encoded;
      }
    }
    if (geometryMap instanceof Map && geometryMap.has(numeric)) {
      return buildLatLngSignature(geometryMap.get(numeric));
    }
    return '';
  }

  function renderRoutes(routes) {
    routeColors.clear();

    if (!Array.isArray(routes) || routes.length === 0) {
      clearRouteLayers();
      if (overlapRenderer) {
        overlapRenderer.reset();
      }
      lastRouteRenderState = {
        selectionKey: '',
        colorSignature: '',
        geometrySignature: '',
        useOverlapRenderer: false
      };
      return [];
    }

    const boundsPoints = [];
    const selectedRouteIds = [];
    const rendererGeometries = new Map();
    const simpleGeometries = [];
    const seenRouteIds = new Set();
    let geometryChanged = false;
    const canDecode = typeof polyline === 'object' && typeof polyline.decode === 'function';

    routes.forEach(route => {
      if (!route || typeof route !== 'object' || route.IsVisibleOnMap === false) {
        return;
      }
      const idRaw = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
      const numericId = toNumber(idRaw);
      if (numericId === null) {
        return;
      }
      if (seenRouteIds.has(numericId)) {
        return;
      }
      const color = normalizeColor(route.MapLineColor || route.Color);
      routeColors.set(String(numericId), color);

      const encoded = route.EncodedPolyline || route.Polyline || route.encodedPolyline;
      if (!encoded || !canDecode) {
        return;
      }

      let cacheEntry = routePolylineCache.get(numericId);
      let latLngPath = Array.isArray(cacheEntry?.latLngPath) ? cacheEntry.latLngPath : null;
      let decodedSuccessfully = true;
      if (!cacheEntry || cacheEntry.encoded !== encoded || !latLngPath || latLngPath.length < 2) {
        decodedSuccessfully = false;
        let decoded = [];
        try {
          decoded = polyline.decode(encoded);
        } catch (error) {
          console.warn('Failed to decode route polyline', numericId, error);
          decoded = [];
        }
        latLngPath = decoded
          .map(pair => {
            if (!Array.isArray(pair) || pair.length < 2) {
              return null;
            }
            const lat = Number(pair[0]);
            const lng = Number(pair[1]);
            if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
              return null;
            }
            return L.latLng(lat, lng);
          })
          .filter(value => value instanceof L.LatLng);
        if (latLngPath.length < 2) {
          return;
        }
        cacheEntry = { encoded, latLngPath };
        routePolylineCache.set(numericId, cacheEntry);
        geometryChanged = true;
        decodedSuccessfully = true;
      }

      if (!decodedSuccessfully || !Array.isArray(latLngPath) || latLngPath.length < 2) {
        return;
      }

      seenRouteIds.add(numericId);
      boundsPoints.push(...latLngPath);
      selectedRouteIds.push(numericId);

      if (ENABLE_OVERLAP_DASH_RENDERING && overlapRenderer) {
        rendererGeometries.set(numericId, latLngPath);
      }

      simpleGeometries.push({
        routeId: numericId,
        latLngPath,
        routeColor: getRouteColorById(numericId)
      });
    });

    const previousSelectedIds = new Set(lastRouteRenderState.selectionKey
      ? lastRouteRenderState.selectionKey.split('|').filter(Boolean).map(id => Number(id))
      : []);

    routePolylineCache.forEach((_, routeId) => {
      if (!seenRouteIds.has(routeId)) {
        if (previousSelectedIds.has(routeId)) {
          geometryChanged = true;
        }
        routePolylineCache.delete(routeId);
      }
    });

    const selectedRouteIdsSorted = selectedRouteIds.slice().sort((a, b) => a - b);
    const selectionKey = selectedRouteIdsSorted.join('|');
    const colorSignature = selectedRouteIdsSorted
      .map(id => `${id}:${getRouteColorById(id)}`)
      .join('|');
    const geometrySignature = selectedRouteIdsSorted
      .map(id => `${id}:${getRouteGeometrySignature(id, rendererGeometries)}`)
      .join('|');

    let overlapRendered = false;
    const shouldAttemptOverlap = ENABLE_OVERLAP_DASH_RENDERING
      && overlapRenderer
      && rendererGeometries.size > 0
      && selectedRouteIdsSorted.length > 0;

    if (shouldAttemptOverlap) {
      const layers = overlapRenderer.updateRoutes(rendererGeometries, selectedRouteIdsSorted);
      overlapRendered = Array.isArray(layers) && layers.length > 0;
      if (!overlapRendered) {
        overlapRenderer.clearLayers();
      }
    } else if (overlapRenderer) {
      overlapRenderer.reset();
    }

    const rendererFlag = overlapRendered;
    let shouldRender = routeLayers.length === 0
      || rendererFlag !== lastRouteRenderState.useOverlapRenderer
      || selectionKey !== lastRouteRenderState.selectionKey
      || colorSignature !== lastRouteRenderState.colorSignature
      || geometrySignature !== lastRouteRenderState.geometrySignature
      || geometryChanged;

    if (rendererFlag) {
      if (routeLayers.length > 0) {
        clearRouteLayers();
      }
      shouldRender = false;
    }

    if (!rendererFlag && shouldRender) {
      clearRouteLayers();
      const strokeWeight = computeRouteStrokeWeight(typeof map?.getZoom === 'function' ? map.getZoom() : null);
      simpleGeometries.forEach(entry => {
        if (!entry || !Array.isArray(entry.latLngPath) || entry.latLngPath.length < 2) {
          return;
        }
        const layer = L.polyline(entry.latLngPath, mergeRouteLayerOptions({
          color: entry.routeColor,
          weight: strokeWeight,
          opacity: 1,
          lineCap: 'round',
          lineJoin: 'round'
        })).addTo(map);
        routeLayers.push(layer);
      });
    }

    lastRouteRenderState = {
      selectionKey,
      colorSignature,
      geometrySignature,
      useOverlapRenderer: rendererFlag
    };

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
          routes: new Set(),
          count: 0
        };
        aggregated.set(key, entry);
      }

      entry.count = (entry.count ?? 0) + 1;
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        if (entry.count === 1) {
          entry.lat = lat;
          entry.lon = lon;
        } else {
          const previousWeight = entry.count - 1;
          entry.lat = ((entry.lat ?? lat) * previousWeight + lat) / entry.count;
          entry.lon = ((entry.lon ?? lon) * previousWeight + lon) / entry.count;
        }
      }

      const routeIds = extractStopRouteIds(stop);
      routeIds.forEach(routeId => {
        entry.routes.add(routeId);
      });
    });

    const aggregatedEntries = [];
    aggregated.forEach(entry => {
      if (!entry) {
        return;
      }
      const lat = Number(entry.lat);
      const lon = Number(entry.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return;
      }
      const routes = entry.routes instanceof Set ? entry.routes : new Set(entry.routes);
      const routeList = Array.from(routes);
      const relevantRoutes = requireActiveRoutes
        ? routeList.filter(routeId => activeSet.has(routeId))
        : routeList;
      if (relevantRoutes.length === 0) {
        return;
      }
      aggregatedEntries.push({
        lat,
        lon,
        routes: new Set(relevantRoutes)
      });
    });

    const groupedStops = groupStopsByPixelDistance(aggregatedEntries, STOP_GROUPING_PIXEL_DISTANCE);
    groupedStops.forEach(group => {
      if (!group) {
        return;
      }
      const lat = Number(group.lat);
      const lon = Number(group.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return;
      }
      const routes = group.routes instanceof Set ? group.routes : new Set(group.routes);
      if (routes.size === 0) {
        return;
      }
      const icon = ensureStopIcon(routes);
      L.marker([lat, lon], {
        icon,
        interactive: false,
        keyboard: false,
        pane: STOP_PANE
      }).addTo(stopLayerGroup);
    });
  }

  function groupStopsByPixelDistance(entries, thresholdPx) {
    if (!Array.isArray(entries) || entries.length === 0) {
      return [];
    }

    const groups = [];
    const threshold = Number(thresholdPx);
    const allowGrouping = Number.isFinite(threshold) && threshold >= 0;
    const thresholdSq = allowGrouping ? threshold * threshold : 0;

    entries.forEach(entry => {
      if (!entry) {
        return;
      }

      const lat = Number(entry.lat);
      const lon = Number(entry.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
        return;
      }

      const routes = entry.routes instanceof Set ? entry.routes : new Set(entry.routes);
      if (!(routes instanceof Set) || routes.size === 0) {
        return;
      }

      let point = null;
      try {
        point = map.latLngToLayerPoint([lat, lon]);
      } catch (error) {
        point = null;
      }

      let targetGroup = null;
      let smallestDistanceSq = Infinity;
      if (allowGrouping && point) {
        for (const group of groups) {
          if (!group || !group.point) {
            continue;
          }
          const dx = group.point.x - point.x;
          const dy = group.point.y - point.y;
          const distanceSq = dx * dx + dy * dy;
          if (distanceSq <= thresholdSq && distanceSq < smallestDistanceSq) {
            smallestDistanceSq = distanceSq;
            targetGroup = group;
          }
        }
      }

      if (targetGroup) {
        const previousCount = targetGroup.count ?? 1;
        const newCount = previousCount + 1;
        targetGroup.lat = ((targetGroup.lat ?? lat) * previousCount + lat) / newCount;
        targetGroup.lon = ((targetGroup.lon ?? lon) * previousCount + lon) / newCount;
        targetGroup.count = newCount;
        routes.forEach(routeId => {
          targetGroup.routes.add(routeId);
        });
        try {
          const updatedPoint = map.latLngToLayerPoint([targetGroup.lat, targetGroup.lon]);
          if (updatedPoint) {
            targetGroup.point = updatedPoint;
          }
        } catch (error) {
          targetGroup.point = null;
        }
      } else {
        groups.push({
          lat,
          lon,
          routes: new Set(routes),
          point,
          count: 1
        });
      }
    });

    return groups.map(group => ({
      lat: group.lat,
      lon: group.lon,
      routes: group.routes
    }));
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

  function getRouteColor(routeId) {
    return getRouteColorById(routeId);
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

  function deriveRouteIdsFromRoutes(routes) {
    const ids = new Set();
    if (!Array.isArray(routes)) {
      return ids;
    }
    routes.forEach(route => {
      if (!route || typeof route !== 'object') {
        return;
      }
      const idRaw = route.RouteID ?? route.RouteId ?? route.routeID ?? route.id;
      const id = toNumber(idRaw);
      if (id !== null) {
        ids.add(id);
      }
    });
    return ids;
  }

  function ensureVehicleLabelState(id) {
    let state = vehicleLabels.get(id);
    if (!state) {
      state = { nameMarker: null, blockMarker: null };
      vehicleLabels.set(id, state);
    }
    return state;
  }

  function removeLabelMarker(marker) {
    if (!marker) {
      return;
    }
    cancelMarkerAnimation(marker);
    if (map && typeof map.removeLayer === 'function') {
      map.removeLayer(marker);
    } else if (typeof marker.remove === 'function') {
      marker.remove();
    }
  }

  function disposeVehicleLabelState(id) {
    const state = vehicleLabels.get(id);
    if (!state) {
      return;
    }
    if (state.nameMarker) {
      removeLabelMarker(state.nameMarker);
      state.nameMarker = null;
    }
    if (state.blockMarker) {
      removeLabelMarker(state.blockMarker);
      state.blockMarker = null;
    }
    vehicleLabels.delete(id);
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
      const headingDeg = Number.isFinite(heading) ? heading : BUS_MARKER_DEFAULT_HEADING;
      let icon = null;
      if (hasSvgMarker) {
        icon = createBusMarkerIcon(color, heading ?? BUS_MARKER_DEFAULT_HEADING, Boolean(vehicle.IsStale));
      }
      if (!icon) {
        icon = createFallbackBusIcon(color, Boolean(vehicle.IsStale));
      }
      const targetPosition = [lat, lon];
      if (marker) {
        setMarkerPosition(marker, targetPosition);
        marker.setIcon(icon);
      } else {
        const newMarker = L.marker([lat, lon], {
          icon,
          interactive: false,
          keyboard: false,
          pane: VEHICLE_PANE
        });
        newMarker.addTo(vehicleLayerGroup);
        vehicleMarkers.set(id, newMarker);
      }

      if (adminMode) {
        const labelScale = 1;
        const vehicleName = extractVehicleName(vehicle);
        const blockRaw = extractBlockLabel(vehicle, blocks);
        const blockText = formatBlockBubbleText(blockRaw);
        const nameIcon = createNameBubbleDivIcon(vehicleName, color, labelScale, headingDeg);
        const blockIcon = blockText ? createBlockBubbleDivIcon(blockText, color, labelScale, headingDeg) : null;
        let labelState = vehicleLabels.get(id) || null;

        if (nameIcon || blockIcon) {
          if (!labelState) {
            labelState = ensureVehicleLabelState(id);
          }
        }

        if (labelState && nameIcon) {
          if (labelState.nameMarker) {
            setMarkerPosition(labelState.nameMarker, targetPosition);
            labelState.nameMarker.setIcon(nameIcon);
          } else {
            labelState.nameMarker = L.marker([lat, lon], {
              icon: nameIcon,
              interactive: false,
              keyboard: false,
              zIndexOffset: 500,
              pane: VEHICLE_LABEL_PANE
            });
            labelState.nameMarker.addTo(labelLayerGroup);
          }
        } else if (labelState && labelState.nameMarker) {
          removeLabelMarker(labelState.nameMarker);
          labelState.nameMarker = null;
        }

        if (labelState && blockIcon) {
          if (labelState.blockMarker) {
            setMarkerPosition(labelState.blockMarker, targetPosition);
            labelState.blockMarker.setIcon(blockIcon);
          } else {
            labelState.blockMarker = L.marker([lat, lon], {
              icon: blockIcon,
              interactive: false,
              keyboard: false,
              zIndexOffset: 500,
              pane: VEHICLE_LABEL_PANE
            });
            labelState.blockMarker.addTo(labelLayerGroup);
          }
        } else if (labelState && labelState.blockMarker) {
          removeLabelMarker(labelState.blockMarker);
          labelState.blockMarker = null;
        }

        if (labelState && !labelState.nameMarker && !labelState.blockMarker) {
          vehicleLabels.delete(id);
        }
      }
    });

    vehicleMarkers.forEach((marker, id) => {
      if (!activeVehicles.has(id)) {
        cancelMarkerAnimation(marker);
        map.removeLayer(marker);
        vehicleMarkers.delete(id);
      }
    });

    if (adminMode) {
      vehicleLabels.forEach((state, id) => {
        if (!activeVehicles.has(id)) {
          disposeVehicleLabelState(id);
        }
      });
    } else if (vehicleLabels.size > 0) {
      const ids = Array.from(vehicleLabels.keys());
      ids.forEach(disposeVehicleLabelState);
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

    const activeRouteIds = debugMode
      ? deriveRouteIdsFromRoutes(routes)
      : deriveActiveRouteIds(vehicles);
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
      if (debugMode) {
        return true;
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
      map.setView(UVA_DEFAULT_CENTER, UVA_DEFAULT_ZOOM);
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

  function setMarkerPosition(marker, position) {
    if (!marker) {
      return;
    }
    const target = resolveLatLng(position);
    if (!target) {
      return;
    }
    const current = typeof marker.getLatLng === 'function' ? marker.getLatLng() : null;
    if (current && latLngsAreClose(current, target)) {
      cancelMarkerAnimation(marker);
      marker.setLatLng(target);
      return;
    }
    if (!canAnimateMarkers()) {
      cancelMarkerAnimation(marker);
      marker.setLatLng(target);
      return;
    }
    animateMarkerTo(marker, target);
  }

  function resolveLatLng(position) {
    if (!position) {
      return null;
    }
    if (Array.isArray(position) && position.length >= 2) {
      const lat = Number(position[0]);
      const lng = Number(position[1]);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        return L.latLng(lat, lng);
      }
      return null;
    }
    if (position instanceof L.LatLng) {
      return position;
    }
    if (typeof position === 'object') {
      const lat = Number(position.lat ?? position.Latitude ?? position.Lat);
      const lng = Number(position.lng ?? position.Longitude ?? position.Lon ?? position.Lng);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        return L.latLng(lat, lng);
      }
    }
    return null;
  }

  function canAnimateMarkers() {
    return typeof window !== 'undefined'
      && typeof window.requestAnimationFrame === 'function'
      && typeof window.cancelAnimationFrame === 'function';
  }

  function animateMarkerTo(marker, destination) {
    const end = resolveLatLng(destination);
    if (!marker || !end) {
      return;
    }
    const start = marker.getLatLng();
    if (!start) {
      cancelMarkerAnimation(marker);
      marker.setLatLng(end);
      return;
    }
    if (latLngsAreClose(start, end)) {
      cancelMarkerAnimation(marker);
      marker.setLatLng(end);
      return;
    }

    const previousHandle = markerAnimationHandles.get(marker);
    if (typeof previousHandle === 'number') {
      window.cancelAnimationFrame(previousHandle);
    }

    let startTimestamp = null;

    function step(currentTimestamp) {
      if (startTimestamp === null) {
        startTimestamp = currentTimestamp;
      }
      const elapsed = currentTimestamp - startTimestamp;
      const progress = Math.min(Math.max(elapsed / MARKER_ANIMATION_DURATION_MS, 0), 1);
      const interpolatedLat = start.lat + (end.lat - start.lat) * progress;
      const interpolatedLng = start.lng + (end.lng - start.lng) * progress;
      marker.setLatLng([interpolatedLat, interpolatedLng]);
      if (progress < 1) {
        const handle = window.requestAnimationFrame(step);
        markerAnimationHandles.set(marker, handle);
      } else {
        marker.setLatLng(end);
        markerAnimationHandles.delete(marker);
      }
    }

    const handle = window.requestAnimationFrame(step);
    markerAnimationHandles.set(marker, handle);
  }

  function latLngsAreClose(a, b) {
    if (!a || !b) {
      return false;
    }
    if (typeof a.equals === 'function') {
      return a.equals(b, 1e-7);
    }
    const latDiff = Math.abs(Number(a.lat) - Number(b.lat));
    const lngDiff = Math.abs(Number(a.lng) - Number(b.lng));
    return latDiff < 1e-7 && lngDiff < 1e-7;
  }

  function cancelMarkerAnimation(marker) {
    if (!markerAnimationHandles.has(marker)) {
      return;
    }
    const handle = markerAnimationHandles.get(marker);
    if (typeof handle === 'number' && typeof window !== 'undefined' && typeof window.cancelAnimationFrame === 'function') {
      window.cancelAnimationFrame(handle);
    }
    markerAnimationHandles.delete(marker);
  }

  setLoadingVisible(true, 'Loading UVA buses…');
  fetchSnapshot();
})();
