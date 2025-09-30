/* global L, polyline */
(function () {
  'use strict';

  const REFRESH_INTERVAL_MS = 5000;
  const UVA_DEFAULT_CENTER = [38.03799212281404, -78.50981502838886];
  const UVA_DEFAULT_ZOOM = 15;
  const DEFAULT_ROUTE_COLOR = '#000000';
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

  const stopPane = map.createPane(STOP_PANE);
  stopPane.style.zIndex = '300';
  stopPane.style.pointerEvents = 'none';

  const vehiclePane = map.createPane(VEHICLE_PANE);
  vehiclePane.style.zIndex = '400';
  vehiclePane.style.pointerEvents = 'none';

  const vehicleLabelPane = map.createPane(VEHICLE_LABEL_PANE);
  vehicleLabelPane.style.zIndex = '450';
  vehicleLabelPane.style.pointerEvents = 'none';

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
          lineJoin: 'round',
          pane: ROUTE_PANE
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
          lineJoin: 'round',
          pane: ROUTE_PANE
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
        keyboard: false,
        pane: STOP_PANE
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
