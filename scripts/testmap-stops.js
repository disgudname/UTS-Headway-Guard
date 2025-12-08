'use strict';

/**
 * TestMap Stops Module
 * Contains: Stop marker rendering, stop caching, ETA display, marker pooling
 * Dependencies: testmap-core.js
 */

(function(TM) {
    if (!TM) {
        console.error('[TestMap] Core module not loaded');
        return;
    }

    const { utils, state, CONSTANTS } = TM;

    // ============================================================
    // STOP STATE MANAGEMENT
    // ============================================================

    TM.stops = {
        stopMarkers: [],                    // Array of Leaflet markers for stops
        stopMarkerCache: new Map(),         // Cache of groupKey -> marker data
        stopMarkerIconCache: new Map(),     // Cache of rendered stop icons
        stopDataCache: [],                  // Cached stop data from API
        catStopDataCache: [],               // Cached CAT stops
        routeStopAddressMap: {},            // Stop ID -> address mapping
        routeStopRouteMap: {},              // Stop ID -> route IDs mapping
        lastStopDisplaySignature: null,     // For caching rendered set of stops
        // Marker pooling
        stopMarkerPool: [],
        // Scheduling state
        scheduledStopRenderFrame: null,
        scheduledStopRenderTimeout: null,
        stopRenderDebounceTimer: null,
        // Progressive loading
        progressiveStopQueue: [],
        progressiveStopIdleCallback: null
    };

    // ============================================================
    // STOP MARKER CONSTANTS
    // ============================================================

    const STOP_MARKER_ICON_SIZE = CONSTANTS.STOP_MARKER_ICON_SIZE || 18;
    const STOP_MARKER_OUTLINE_COLOR = '#FFFFFF';
    const STOP_MARKER_OUTLINE_WIDTH = 2;
    const STOP_MARKER_BORDER_COLOR = '#232D4B';
    const STOP_MARKER_BORDER_WIDTH = 2;
    const STOP_RENDER_BOUNDS_PADDING = CONSTANTS.STOP_RENDER_BOUNDS_PADDING || 0.1;
    const STOP_GROUPING_PIXEL_DISTANCE = CONSTANTS.STOP_GROUPING_PIXEL_DISTANCE || 20;
    const STOP_MARKER_POOL_MAX_SIZE = CONSTANTS.STOP_MARKER_POOL_MAX_SIZE || 100;
    const STOP_RENDER_DEBOUNCE_MS = CONSTANTS.STOP_RENDER_DEBOUNCE_MS || 150;
    const PROGRESSIVE_STOP_BATCH_SIZE = CONSTANTS.PROGRESSIVE_STOP_BATCH_SIZE || 20;

    // ============================================================
    // MARKER POOLING
    // ============================================================

    function getPooledStopMarker() {
        if (TM.stops.stopMarkerPool.length > 0) {
            return TM.stops.stopMarkerPool.pop();
        }
        return null;
    }

    function returnStopMarkerToPool(marker) {
        if (!marker) return;
        // Remove all event listeners
        if (typeof marker.off === 'function') {
            marker.off('click');
        }
        // Hide the marker (remove from map but don't destroy)
        const map = state.map;
        if (map && typeof map.hasLayer === 'function' && map.hasLayer(marker)) {
            map.removeLayer(marker);
        }
        // Only pool if we haven't exceeded the limit
        if (TM.stops.stopMarkerPool.length < STOP_MARKER_POOL_MAX_SIZE) {
            TM.stops.stopMarkerPool.push(marker);
        }
    }

    function clearStopMarkerCache() {
        TM.stops.stopMarkerCache.forEach(entry => {
            if (!entry || !entry.marker) return;
            try {
                if (typeof entry.marker.off === 'function' && entry.clickHandler) {
                    entry.marker.off('click', entry.clickHandler);
                }
                const map = state.map;
                if (map && typeof map.removeLayer === 'function') {
                    map.removeLayer(entry.marker);
                }
            } catch (error) {
                console.warn('Failed to remove stop marker from map:', error);
            }
        });
        TM.stops.stopMarkerCache.clear();
        TM.stops.stopMarkers = [];
        TM.stops.lastStopDisplaySignature = null;
    }

    // ============================================================
    // STOP MARKER ICON CREATION
    // ============================================================

    function buildStopMarkerGradient(routeIds, catRouteKeys, options = {}) {
        const colors = collectStopMarkerColors(routeIds, catRouteKeys, options);
        if (colors.length === 0) {
            return STOP_MARKER_BORDER_COLOR;
        }
        if (colors.length === 1) {
            return colors[0];
        }
        // Create conic gradient for multiple colors
        const step = 360 / colors.length;
        const segments = colors.map((color, i) => {
            const start = i * step;
            const end = (i + 1) * step;
            return `${color} ${start}deg ${end}deg`;
        });
        return `conic-gradient(${segments.join(', ')})`;
    }

    function collectStopMarkerColors(routeIds, catRouteKeys, options = {}) {
        const colors = [];
        const getRouteColor = TM.getRouteColor || (() => STOP_MARKER_BORDER_COLOR);
        const getCatRouteColor = TM.getCatRouteColor || (() => null);

        // Collect TransLoc route colors
        if (Array.isArray(routeIds)) {
            routeIds.forEach(routeId => {
                const color = getRouteColor(routeId);
                if (color && !colors.includes(color)) {
                    colors.push(color);
                }
            });
        }

        // Collect CAT route colors
        if (Array.isArray(catRouteKeys)) {
            catRouteKeys.forEach(routeKey => {
                const color = getCatRouteColor(routeKey);
                if (color && !colors.includes(color)) {
                    colors.push(color);
                }
            });
        }

        // OnDemand segments
        if (options.onDemandSegments) {
            options.onDemandSegments.forEach(segment => {
                if (segment.color && !colors.includes(segment.color)) {
                    colors.push(segment.color);
                }
            });
        }

        return colors.length > 0 ? colors : [STOP_MARKER_BORDER_COLOR];
    }

    function createStopMarkerIcon(routeIds, catRouteKeys = [], options = {}) {
        const colors = collectStopMarkerColors(routeIds, catRouteKeys, options);
        const colorKey = colors.slice().sort((a, b) => a.localeCompare(b, undefined, { numeric: true })).join('|');
        const size = STOP_MARKER_ICON_SIZE;
        const outline = Math.max(0, Number(STOP_MARKER_OUTLINE_WIDTH) || 0);

        // Check cache
        const cacheKey = `${colorKey}__${size}__${outline}`;
        if (TM.stops.stopMarkerIconCache.has(cacheKey)) {
            return TM.stops.stopMarkerIconCache.get(cacheKey);
        }

        const gradient = buildStopMarkerGradient(routeIds, catRouteKeys, options);
        const icon = L.divIcon({
            className: 'stop-marker-container leaflet-div-icon',
            html: `<div class="stop-marker-outer" style="--stop-marker-size:${size}px;--stop-marker-border-color:${STOP_MARKER_BORDER_COLOR};--stop-marker-outline-size:${outline}px;--stop-marker-outline-color:${STOP_MARKER_OUTLINE_COLOR};--stop-marker-gradient:${gradient};"></div>`,
            iconSize: [size, size],
            iconAnchor: [size / 2, size / 2]
        });

        TM.stops.stopMarkerIconCache.set(cacheKey, icon);
        return icon;
    }

    // ============================================================
    // STOP RENDERING SCHEDULING
    // ============================================================

    function scheduleStopRendering() {
        if (TM.stops.scheduledStopRenderFrame !== null || TM.stops.scheduledStopRenderTimeout !== null) {
            return;
        }

        const run = () => {
            TM.stops.scheduledStopRenderFrame = null;
            TM.stops.scheduledStopRenderTimeout = null;
            const hasTranslocStops = Array.isArray(TM.stops.stopDataCache) && TM.stops.stopDataCache.length > 0;
            const hasCatStops = TM.stops.catStopDataCache && Array.isArray(TM.stops.catStopDataCache) && TM.stops.catStopDataCache.length > 0;
            if (hasTranslocStops || hasCatStops) {
                if (typeof TM.renderBusStops === 'function') {
                    TM.renderBusStops(TM.stops.stopDataCache);
                }
            }
        };

        if (!state.lowPerformanceMode && typeof requestAnimationFrame === 'function') {
            TM.stops.scheduledStopRenderFrame = requestAnimationFrame(run);
            return;
        }

        const delay = state.lowPerformanceMode ? 75 : 16;
        TM.stops.scheduledStopRenderTimeout = setTimeout(run, delay);
    }

    // Debounced version for map movements
    function debouncedScheduleStopRendering() {
        if (TM.stops.stopRenderDebounceTimer !== null) {
            clearTimeout(TM.stops.stopRenderDebounceTimer);
        }
        TM.stops.stopRenderDebounceTimer = setTimeout(() => {
            TM.stops.stopRenderDebounceTimer = null;
            scheduleStopRendering();
        }, STOP_RENDER_DEBOUNCE_MS);
    }

    // ============================================================
    // PROGRESSIVE STOP LOADING
    // ============================================================

    function cancelProgressiveStopLoading() {
        if (TM.stops.progressiveStopIdleCallback !== null) {
            if (typeof cancelIdleCallback === 'function') {
                cancelIdleCallback(TM.stops.progressiveStopIdleCallback);
            } else {
                clearTimeout(TM.stops.progressiveStopIdleCallback);
            }
            TM.stops.progressiveStopIdleCallback = null;
        }
        TM.stops.progressiveStopQueue = [];
    }

    function processProgressiveStopBatch(deadline) {
        const hasTimeRemaining = typeof deadline?.timeRemaining === 'function'
            ? () => deadline.timeRemaining() > 0
            : () => true;

        let processed = 0;
        while (TM.stops.progressiveStopQueue.length > 0 && processed < PROGRESSIVE_STOP_BATCH_SIZE && hasTimeRemaining()) {
            const task = TM.stops.progressiveStopQueue.shift();
            if (task && typeof task === 'function') {
                try {
                    task();
                } catch (error) {
                    console.warn('Progressive stop loading task failed:', error);
                }
            }
            processed++;
        }

        if (TM.stops.progressiveStopQueue.length > 0) {
            scheduleProgressiveStopBatch();
        } else {
            TM.stops.progressiveStopIdleCallback = null;
        }
    }

    function scheduleProgressiveStopBatch() {
        if (TM.stops.progressiveStopIdleCallback !== null) {
            return;
        }
        if (typeof requestIdleCallback === 'function') {
            TM.stops.progressiveStopIdleCallback = requestIdleCallback(processProgressiveStopBatch, { timeout: 100 });
        } else {
            TM.stops.progressiveStopIdleCallback = setTimeout(() => processProgressiveStopBatch(null), 16);
        }
    }

    function queueProgressiveStopTask(task) {
        if (typeof task === 'function') {
            TM.stops.progressiveStopQueue.push(task);
            scheduleProgressiveStopBatch();
        }
    }

    // ============================================================
    // STOP GROUPING UTILITIES
    // ============================================================

    function createStopGroupKey(routeStopIds, fallbackStopId) {
        const routeStopKey = Array.isArray(routeStopIds) ? routeStopIds.join(',') : '';
        const fallbackKey = typeof fallbackStopId === 'string' ? fallbackStopId : '';
        return `${routeStopKey}__${fallbackKey}`;
    }

    function normalizeIdentifier(value) {
        if (typeof value === 'string') {
            return value.trim().toLowerCase();
        }
        if (typeof value === 'number') {
            return String(value);
        }
        return '';
    }

    // ============================================================
    // EXPORT TO NAMESPACE
    // ============================================================

    TM.stopUtils = {
        getPooledStopMarker,
        returnStopMarkerToPool,
        clearStopMarkerCache,
        createStopMarkerIcon,
        buildStopMarkerGradient,
        collectStopMarkerColors,
        scheduleStopRendering,
        debouncedScheduleStopRendering,
        cancelProgressiveStopLoading,
        queueProgressiveStopTask,
        createStopGroupKey,
        normalizeIdentifier
    };

    TM.stopConstants = {
        STOP_MARKER_ICON_SIZE,
        STOP_MARKER_OUTLINE_COLOR,
        STOP_MARKER_OUTLINE_WIDTH,
        STOP_MARKER_BORDER_COLOR,
        STOP_MARKER_BORDER_WIDTH,
        STOP_RENDER_BOUNDS_PADDING,
        STOP_GROUPING_PIXEL_DISTANCE,
        STOP_MARKER_POOL_MAX_SIZE,
        STOP_RENDER_DEBOUNCE_MS,
        PROGRESSIVE_STOP_BATCH_SIZE
    };

    console.log('[TestMap] Stops module loaded');

})(window.TestMap);
