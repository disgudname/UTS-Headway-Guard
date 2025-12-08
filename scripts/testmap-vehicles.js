'use strict';

/**
 * TestMap Vehicles Module
 * Contains: Bus marker creation, vehicle state management, animations, heading calculations
 * Dependencies: testmap-core.js
 */

(function(TM) {
    if (!TM) {
        console.error('[TestMap] Core module not loaded');
        return;
    }

    const { utils, state, CONSTANTS } = TM;

    // ============================================================
    // VEHICLE STATE MANAGEMENT
    // ============================================================

    // Vehicle markers and state
    TM.vehicles = {
        markers: {},                    // Map of vehicleID -> Leaflet marker
        busMarkerStates: {},            // Map of vehicleID -> state object
        nameBubbles: {},                // Map of markerKey -> bubble markers
        busBlocks: {},                  // Vehicle block assignments
        previousBusData: {},            // Previous state for delta detection
        cachedVehicleDrivers: {},       // Driver information cache
        cachedEtas: {},                 // Stop arrival time estimates
        cachedNextStops: {},            // Next stops for vehicles
        vehicleHeadingCache: new Map(), // Heading values keyed by vehicleID
        customPopups: [],               // Array of custom popups
        busPopupRefreshIntervals: {},   // Popup refresh timers
        // Animation state
        vehicleSmoothingTemporarilyDisabled: false,
        vehicleSmoothingDisableClaims: 0,
        // Follow state
        vehicleFollowState: {
            vehicleKey: null,
            active: false,
            toast: null,
            bounds: null
        },
        // Pending updates for batching
        pendingBusVisualUpdates: new Map()
    };

    // ============================================================
    // VEHICLE MARKER CONSTANTS
    // ============================================================

    const BUS_MARKER_SVG_URL = '/media/bus_marker.svg';
    let BUS_MARKER_SVG_TEXT = null;
    let busSvgLoadPromise = null;

    const BUS_MARKER_BASE_ZOOM = 15;
    const BUS_MARKER_BASE_WIDTH_PX = 36;
    const BUS_MARKER_BASE_HEIGHT_PX = 36;
    const BUS_MARKER_MIN_SCALE = 0.6;
    const BUS_MARKER_MAX_SCALE = 1.2;
    const BUS_MARKER_DEFAULT_ROUTE_COLOR = '#0f172a';
    const BUS_MARKER_DEFAULT_CONTRAST_COLOR = '#ffffff';
    const BUS_MARKER_CENTER_RING_ID = 'center_ring';
    const BUS_MARKER_STOPPED_SQUARE_ID = 'stopped_square';
    const BUS_MARKER_STOPPED_SQUARE_SIZE_PX = 8;
    const BUS_MARKER_CENTER_RING_CENTER_X = 18;
    const BUS_MARKER_CENTER_RING_CENTER_Y = 18;

    // ============================================================
    // SVG LOADING
    // ============================================================

    function loadBusSVG() {
        if (BUS_MARKER_SVG_TEXT) {
            return Promise.resolve(BUS_MARKER_SVG_TEXT);
        }
        if (busSvgLoadPromise) {
            return busSvgLoadPromise;
        }
        busSvgLoadPromise = fetch(BUS_MARKER_SVG_URL)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Failed to load SVG: ${response.status}`);
                }
                return response.text();
            })
            .then(text => {
                BUS_MARKER_SVG_TEXT = text;
                return text;
            })
            .catch(error => {
                console.error('Error loading bus marker SVG:', error);
                busSvgLoadPromise = null;
                return null;
            });
        return busSvgLoadPromise;
    }

    // ============================================================
    // MARKER STATE MANAGEMENT
    // ============================================================

    function ensureBusMarkerState(vehicleID) {
        if (!TM.vehicles.busMarkerStates[vehicleID]) {
            TM.vehicles.busMarkerStates[vehicleID] = {
                vehicleID,
                busName: '',
                routeID: null,
                position: null,
                heading: null,
                headingDeg: 0,
                speed: null,
                isStale: false,
                isStopped: false,
                isOffRoute: false,
                offRouteDistanceMeters: null,
                fillColor: BUS_MARKER_DEFAULT_ROUTE_COLOR,
                glyphColor: BUS_MARKER_DEFAULT_CONTRAST_COLOR,
                size: null,
                isHovered: false,
                isSelected: false,
                elements: null,
                marker: null,
                markerEventsBound: false
            };
        }
        return TM.vehicles.busMarkerStates[vehicleID];
    }

    function clearBusMarkerState(vehicleID) {
        const state = TM.vehicles.busMarkerStates[vehicleID];
        if (state) {
            state.elements = null;
            state.marker = null;
            state.markerEventsBound = false;
        }
        delete TM.vehicles.busMarkerStates[vehicleID];
    }

    // ============================================================
    // MARKER METRICS & SIZING
    // ============================================================

    function computeBusMarkerMetrics(zoom) {
        const clampedZoom = Math.max(10, Math.min(20, zoom || BUS_MARKER_BASE_ZOOM));
        const zoomDiff = clampedZoom - BUS_MARKER_BASE_ZOOM;
        const scale = Math.max(BUS_MARKER_MIN_SCALE, Math.min(BUS_MARKER_MAX_SCALE, 1 + zoomDiff * 0.08));
        return {
            widthPx: Math.round(BUS_MARKER_BASE_WIDTH_PX * scale),
            heightPx: Math.round(BUS_MARKER_BASE_HEIGHT_PX * scale),
            scale
        };
    }

    function setBusMarkerSize(markerState, metrics) {
        if (!markerState || !metrics) return;
        markerState.size = {
            widthPx: metrics.widthPx,
            heightPx: metrics.heightPx,
            scale: metrics.scale
        };
    }

    // ============================================================
    // MARKER COLOR MANAGEMENT
    // ============================================================

    function computeBusMarkerGlyphColor(routeColor) {
        const fallback = BUS_MARKER_DEFAULT_CONTRAST_COLOR;
        const candidate = typeof routeColor === 'string' && routeColor.trim().length > 0
            ? routeColor
            : BUS_MARKER_DEFAULT_ROUTE_COLOR;
        return utils.contrastBW(candidate) || fallback;
    }

    function normalizeRouteColor(color) {
        if (typeof color === 'string') {
            const trimmed = color.trim();
            if (trimmed.length > 0) return trimmed;
        }
        return BUS_MARKER_DEFAULT_ROUTE_COLOR;
    }

    function normalizeGlyphColor(color, routeColor) {
        if (typeof color === 'string') {
            const trimmed = color.trim();
            if (trimmed.length > 0) return trimmed;
        }
        return computeBusMarkerGlyphColor(normalizeRouteColor(routeColor));
    }

    function updateBusMarkerColorElements(markerState) {
        if (!markerState) return;

        const normalizedFill = normalizeRouteColor(markerState.fillColor);
        const normalizedGlyph = normalizeGlyphColor(markerState.glyphColor, normalizedFill);
        markerState.fillColor = normalizedFill;
        markerState.glyphColor = normalizedGlyph;

        // Performance optimization: Use CSS custom properties instead of setAttribute
        if (markerState.elements?.root) {
            markerState.elements.root.style.setProperty('--bus-marker-fill', normalizedFill);
            markerState.elements.root.style.setProperty('--bus-marker-glyph', normalizedGlyph);
        } else {
            // Fallback for elements without root reference
            if (markerState.elements?.routeColor) {
                markerState.elements.routeColor.setAttribute('fill', normalizedFill);
                markerState.elements.routeColor.style.fill = normalizedFill;
            }
            if (markerState.elements?.centerRing) {
                markerState.elements.centerRing.setAttribute('fill', normalizedGlyph);
                markerState.elements.centerRing.style.fill = normalizedGlyph;
            }
            if (markerState.elements?.centerSquare) {
                markerState.elements.centerSquare.setAttribute('fill', normalizedGlyph);
                markerState.elements.centerSquare.style.fill = normalizedGlyph;
            }
            if (markerState.elements?.heading) {
                markerState.elements.heading.setAttribute('fill', normalizedGlyph);
                markerState.elements.heading.style.fill = normalizedGlyph;
            }
        }
    }

    // ============================================================
    // HEADING & BEARING CALCULATIONS
    // ============================================================

    function computeBearingDegrees(lat1, lon1, lat2, lon2) {
        const toRad = deg => deg * Math.PI / 180;
        const toDeg = rad => rad * 180 / Math.PI;
        const dLon = toRad(lon2 - lon1);
        const y = Math.sin(dLon) * Math.cos(toRad(lat2));
        const x = Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
                  Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dLon);
        const bearing = toDeg(Math.atan2(y, x));
        return (bearing + 360) % 360;
    }

    function normalizeHeadingDegrees(heading) {
        if (!Number.isFinite(heading)) return 0;
        return ((heading % 360) + 360) % 360;
    }

    function bearingToDirection(bearing) {
        const directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
        const index = Math.round(bearing / 45) % 8;
        return directions[index];
    }

    function updateBusMarkerHeading(markerState, headingDeg) {
        if (!markerState || !Number.isFinite(headingDeg)) return;
        markerState.headingDeg = normalizeHeadingDegrees(headingDeg);

        if (markerState.elements?.svg) {
            const transform = `rotate(${markerState.headingDeg}deg)`;
            markerState.elements.svg.style.transform = transform;
        }
    }

    // ============================================================
    // VEHICLE HEADING CACHE
    // ============================================================

    function rememberCachedVehicleHeading(vehicleID, heading) {
        if (!vehicleID || !Number.isFinite(heading)) return;
        TM.vehicles.vehicleHeadingCache.set(vehicleID, {
            heading: normalizeHeadingDegrees(heading),
            timestamp: Date.now()
        });
    }

    function getCachedVehicleHeading(vehicleID) {
        const entry = TM.vehicles.vehicleHeadingCache.get(vehicleID);
        if (!entry) return null;
        // Cache entries older than 5 minutes are stale
        if (Date.now() - entry.timestamp > 300000) {
            TM.vehicles.vehicleHeadingCache.delete(vehicleID);
            return null;
        }
        return entry.heading;
    }

    // ============================================================
    // MARKER ANIMATION
    // ============================================================

    function syncMarkerPopupPosition(marker) {
        if (!marker || typeof marker.getPopup !== 'function') return;
        const popup = marker.getPopup();
        if (!popup || typeof popup.setLatLng !== 'function' || typeof popup.isOpen !== 'function') return;
        if (!popup.isOpen()) return;
        const markerLatLng = typeof marker.getLatLng === 'function' ? marker.getLatLng() : null;
        if (!markerLatLng) return;
        popup.setLatLng(markerLatLng);
    }

    function animateMarkerTo(marker, newPosition, options = {}) {
        if (!marker || !newPosition) return;

        const disableSmoothing = Boolean(options?.disableSmoothing) || TM.vehicles.vehicleSmoothingTemporarilyDisabled;
        const hasArrayPosition = Array.isArray(newPosition) && newPosition.length >= 2;
        const endPos = hasArrayPosition ? L.latLng(newPosition) : L.latLng(newPosition?.lat, newPosition?.lng);

        if (!endPos || Number.isNaN(endPos.lat) || Number.isNaN(endPos.lng)) return;

        const startPos = marker.getLatLng();
        if (!startPos) {
            marker.setLatLng(endPos);
            syncMarkerPopupPosition(marker);
            return;
        }

        // Performance: Skip animations when tab is hidden or in low performance mode
        if (disableSmoothing || state.lowPerformanceMode || !TM.pageIsVisible || typeof requestAnimationFrame !== 'function') {
            marker.setLatLng(endPos);
            syncMarkerPopupPosition(marker);
            return;
        }

        const positionsMatch = typeof startPos.equals === 'function'
            ? startPos.equals(endPos, 1e-7)
            : (Math.abs(startPos.lat - endPos.lat) < 1e-7 && Math.abs(startPos.lng - endPos.lng) < 1e-7);

        if (positionsMatch) {
            marker.setLatLng(endPos);
            syncMarkerPopupPosition(marker);
            return;
        }

        const duration = 1000;
        const startTime = performance.now();

        function animate(time) {
            const elapsed = time - startTime;
            const t = Math.min(elapsed / duration, 1);
            const currentPos = L.latLng(
                startPos.lat + t * (endPos.lat - startPos.lat),
                startPos.lng + t * (endPos.lng - startPos.lng)
            );
            marker.setLatLng(currentPos);
            syncMarkerPopupPosition(marker);
            if (t < 1) requestAnimationFrame(animate);
        }
        requestAnimationFrame(animate);
    }

    // ============================================================
    // SMOOTHING CONTROL
    // ============================================================

    function requestVehicleSmoothingDisable() {
        TM.vehicles.vehicleSmoothingDisableClaims++;
        TM.vehicles.vehicleSmoothingTemporarilyDisabled = true;
        return () => {
            TM.vehicles.vehicleSmoothingDisableClaims--;
            if (TM.vehicles.vehicleSmoothingDisableClaims <= 0) {
                TM.vehicles.vehicleSmoothingDisableClaims = 0;
                TM.vehicles.vehicleSmoothingTemporarilyDisabled = false;
            }
        };
    }

    // ============================================================
    // MARKER VISUAL UPDATE BATCHING
    // ============================================================

    let visualUpdateFrameId = null;

    function queueBusMarkerVisualUpdate(vehicleID, update) {
        if (!vehicleID) return;
        const existing = TM.vehicles.pendingBusVisualUpdates.get(vehicleID) || {};
        TM.vehicles.pendingBusVisualUpdates.set(vehicleID, Object.assign(existing, update));

        if (visualUpdateFrameId === null) {
            visualUpdateFrameId = requestAnimationFrame(flushBusMarkerVisualUpdates);
        }
    }

    function flushBusMarkerVisualUpdates() {
        visualUpdateFrameId = null;
        TM.vehicles.pendingBusVisualUpdates.forEach((update, vehicleID) => {
            applyBusMarkerVisualUpdate(vehicleID, update);
        });
        TM.vehicles.pendingBusVisualUpdates.clear();
    }

    function applyBusMarkerVisualUpdate(vehicleID, update) {
        const markerState = TM.vehicles.busMarkerStates[vehicleID];
        if (!markerState) return;

        if (update && Object.prototype.hasOwnProperty.call(update, 'fillColor')) {
            markerState.fillColor = update.fillColor;
        }
        if (update && Object.prototype.hasOwnProperty.call(update, 'glyphColor')) {
            markerState.glyphColor = update.glyphColor;
        }
        updateBusMarkerColorElements(markerState);
    }

    // ============================================================
    // SPEED FORMATTING
    // ============================================================

    function formatBusSpeed(speedMph) {
        if (!Number.isFinite(speedMph)) return '--';
        return Math.round(speedMph).toString();
    }

    // ============================================================
    // EXPORT TO NAMESPACE
    // ============================================================

    TM.vehicleUtils = {
        loadBusSVG,
        ensureBusMarkerState,
        clearBusMarkerState,
        computeBusMarkerMetrics,
        setBusMarkerSize,
        computeBusMarkerGlyphColor,
        normalizeRouteColor,
        normalizeGlyphColor,
        updateBusMarkerColorElements,
        computeBearingDegrees,
        normalizeHeadingDegrees,
        bearingToDirection,
        updateBusMarkerHeading,
        rememberCachedVehicleHeading,
        getCachedVehicleHeading,
        syncMarkerPopupPosition,
        animateMarkerTo,
        requestVehicleSmoothingDisable,
        queueBusMarkerVisualUpdate,
        flushBusMarkerVisualUpdates,
        formatBusSpeed
    };

    // Export constants
    TM.vehicleConstants = {
        BUS_MARKER_SVG_URL,
        BUS_MARKER_BASE_ZOOM,
        BUS_MARKER_BASE_WIDTH_PX,
        BUS_MARKER_BASE_HEIGHT_PX,
        BUS_MARKER_MIN_SCALE,
        BUS_MARKER_MAX_SCALE,
        BUS_MARKER_DEFAULT_ROUTE_COLOR,
        BUS_MARKER_DEFAULT_CONTRAST_COLOR,
        BUS_MARKER_CENTER_RING_ID,
        BUS_MARKER_STOPPED_SQUARE_ID
    };

    console.log('[TestMap] Vehicles module loaded');

})(window.TestMap);
