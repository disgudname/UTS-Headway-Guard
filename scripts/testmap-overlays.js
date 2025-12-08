'use strict';

/**
 * TestMap Overlays Module
 * Contains: Incidents (PulsePoint), service alerts, radar, trains, CAT overlay utilities
 * Dependencies: testmap-core.js
 */

(function(TM) {
    if (!TM) {
        console.error('[TestMap] Core module not loaded');
        return;
    }

    const { utils, state, CONSTANTS } = TM;

    // ============================================================
    // INCIDENTS STATE
    // ============================================================

    TM.incidents = {
        visible: false,
        visibilityPreference: null,
        markers: new Map(),              // incidentID -> {marker, haloMarker, data}
        iconCache: new Map(),            // icon URL -> Leaflet icon
        haloIconCache: new Map(),
        layerGroup: null,
        haloLayerGroup: null,
        nearRoutesLookup: new Map(),     // Route proximity lookup
        lastFetchTime: 0,
        fetchPromise: null
    };

    // ============================================================
    // INCIDENT CONSTANTS
    // ============================================================

    const PULSEPOINT_ENDPOINT = '/v1/pulsepoint/incidents';
    const INCIDENT_REFRESH_INTERVAL_MS = CONSTANTS.INCIDENT_REFRESH_INTERVAL_MS || 45000;
    const FALLBACK_INCIDENT_ICON_SIZE = 36;
    const INCIDENT_ICON_SCALE = 1.0;
    const HALO_COLOR_RGB = '239, 68, 68';
    const HALO_BASE_OPACITY = 0.5;
    const HALO_DURATION_MS = 2000;
    const HALO_MAX_RADIUS_PX = 60;
    const HALO_MIN_RADIUS_PX = 15;

    // ============================================================
    // INCIDENT UTILITIES
    // ============================================================

    function looksLikePulsePointIncident(record) {
        if (!record || typeof record !== 'object') return false;
        const hasCoordinates = (
            (typeof record.Latitude === 'number' || typeof record.latitude === 'number') &&
            (typeof record.Longitude === 'number' || typeof record.longitude === 'number')
        );
        const hasId = record.ID || record.IncidentID || record.incidentId;
        return hasCoordinates && hasId;
    }

    function getIncidentCoordinates(incident) {
        if (!incident) return null;
        const lat = Number(incident.Latitude ?? incident.latitude ?? incident.lat);
        const lng = Number(incident.Longitude ?? incident.longitude ?? incident.lon ?? incident.lng);
        if (Number.isFinite(lat) && Number.isFinite(lng)) {
            return [lat, lng];
        }
        return null;
    }

    function getIncidentId(incident) {
        if (!incident) return null;
        return incident.ID || incident.IncidentID || incident.incidentId || incident.id || null;
    }

    function createIncidentLeafletIcon(iconUrl, width, height) {
        const scale = Number.isFinite(INCIDENT_ICON_SCALE) && INCIDENT_ICON_SCALE > 0
            ? INCIDENT_ICON_SCALE
            : 1;
        const scaledWidth = Math.round(width * scale);
        const scaledHeight = Math.round(height * scale);
        return L.icon({
            iconUrl,
            iconSize: [scaledWidth, scaledHeight],
            iconAnchor: [scaledWidth / 2, scaledHeight]
        });
    }

    function getIncidentHaloIcon(markerHeight) {
        const safeHeight = Math.max(20, Number(markerHeight) || FALLBACK_INCIDENT_ICON_SIZE);
        const cacheKey = `${safeHeight}`;

        if (TM.incidents.haloIconCache.has(cacheKey)) {
            return TM.incidents.haloIconCache.get(cacheKey);
        }

        const diameter = HALO_MAX_RADIUS_PX * 2;
        const anchorX = diameter / 2;
        const anchorY = diameter / 2 + safeHeight / 2;
        const minScale = HALO_MAX_RADIUS_PX > 0
            ? Math.max(0, Math.min(1, HALO_MIN_RADIUS_PX / HALO_MAX_RADIUS_PX))
            : 0.25;

        const html = `<div class="incident-halo" style="--incident-halo-diameter:${diameter}px;--incident-halo-base-opacity:${HALO_BASE_OPACITY};--incident-halo-duration:${HALO_DURATION_MS}ms;--incident-halo-start-scale:${minScale};--incident-halo-color-rgb:${HALO_COLOR_RGB};"></div>`;

        const icon = L.divIcon({
            className: 'incident-halo-icon',
            iconSize: [diameter, diameter],
            iconAnchor: [anchorX, anchorY],
            html
        });

        TM.incidents.haloIconCache.set(cacheKey, icon);
        return icon;
    }

    // ============================================================
    // SERVICE ALERTS STATE
    // ============================================================

    TM.serviceAlerts = {
        alerts: [],
        loading: false,
        error: null,
        expanded: false,
        lastFetchAgency: '',
        lastFetchTime: 0,
        fetchPromise: null,
        hasLoaded: false
    };

    // ============================================================
    // SERVICE ALERT FIELD PARSING (OPTIMIZED)
    // ============================================================

    const SERVICE_ALERT_START_FIELDS = Object.freeze([
        'StartDateText', 'StartDateDisplay', 'StartDateLocalText', 'StartDateLocal',
        'StartDate', 'StartDateUtc', 'StartDateTime', 'StartDateISO',
        'StartTimestamp', 'StartTime', 'Start', 'BeginDateText',
        'BeginDate', 'BeginDateUtc', 'BeginTime', 'EffectiveStart',
        'EffectiveStartDate', 'EffectiveStartUtc'
    ]);

    const SERVICE_ALERT_END_FIELDS = Object.freeze([
        'EndDateText', 'EndDateDisplay', 'EndDateLocalText', 'EndDateLocal',
        'EndDate', 'EndDateUtc', 'EndDateTime', 'EndDateISO',
        'EndTimestamp', 'EndTime', 'End', 'StopDateText',
        'StopDate', 'StopDateUtc', 'StopTime', 'ExpirationDate',
        'ExpirationDateUtc', 'ExpireDate', 'ExpireDateUtc',
        'EffectiveEnd', 'EffectiveEndDate', 'EffectiveEndUtc'
    ]);

    // Pre-computed lowercase lookup maps for O(1) field matching
    const SERVICE_ALERT_START_FIELDS_LOWER = Object.freeze(
        SERVICE_ALERT_START_FIELDS.reduce((acc, field, index) => {
            acc[field.toLowerCase()] = { field, priority: index };
            return acc;
        }, {})
    );

    const SERVICE_ALERT_END_FIELDS_LOWER = Object.freeze(
        SERVICE_ALERT_END_FIELDS.reduce((acc, field, index) => {
            acc[field.toLowerCase()] = { field, priority: index };
            return acc;
        }, {})
    );

    function formatServiceAlertTimeValue(value) {
        if (!value) return { display: '', raw: '' };

        let dateObj = null;
        const raw = String(value);

        if (typeof value === 'number') {
            // Unix timestamp (seconds or milliseconds)
            dateObj = new Date(value > 1e12 ? value : value * 1000);
        } else if (typeof value === 'string') {
            const trimmed = value.trim();
            if (!trimmed) return { display: '', raw: '' };
            dateObj = new Date(trimmed);
        }

        if (!dateObj || isNaN(dateObj.getTime())) {
            return { display: raw, raw };
        }

        try {
            const formatter = new Intl.DateTimeFormat('en-US', {
                dateStyle: 'medium',
                timeStyle: 'short',
                timeZoneName: 'short'
            });
            return { display: formatter.format(dateObj), raw };
        } catch (error) {
            return { display: dateObj.toLocaleString(), raw };
        }
    }

    function extractServiceAlertTime(record, type) {
        if (!record || typeof record !== 'object') {
            return { display: '', raw: '' };
        }

        const fieldsLower = type === 'end' ? SERVICE_ALERT_END_FIELDS_LOWER : SERVICE_ALERT_START_FIELDS_LOWER;

        // Optimized: Single pass through record keys with O(1) field lookup
        let bestMatch = null;
        let bestPriority = Infinity;

        const keys = Object.keys(record);
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const lowerKey = key.toLowerCase();
            const mapping = fieldsLower[lowerKey];
            if (mapping && mapping.priority < bestPriority) {
                const info = formatServiceAlertTimeValue(record[key]);
                if (info.display) {
                    if (mapping.priority === 0) {
                        return info; // Can't get better than first priority
                    }
                    bestMatch = info;
                    bestPriority = mapping.priority;
                }
            }
        }

        return bestMatch || { display: '', raw: '' };
    }

    // ============================================================
    // RADAR STATE
    // ============================================================

    TM.radar = {
        layer: null,
        product: 'composite',
        opacity: 0.6,
        enabled: false,
        configuredProduct: null,
        configuredOpacity: null,
        refreshTimerId: null,
        suppressionTimeoutId: null,
        temporarilyUnavailable: false,
        suppressedForErrors: false,
        tileErrorCount: 0,
        lastFailedUrl: null
    };

    // Radar products and configuration
    const RADAR_PRODUCTS = Object.freeze(['composite', 'precipitation', 'velocity']);
    const RADAR_DEFAULT_OPACITY = 0.6;
    const RADAR_MIN_OPACITY = 0.1;
    const RADAR_MAX_OPACITY = 1.0;
    const RADAR_REFRESH_INTERVAL_MS = 300000; // 5 minutes

    function normalizeRadarProduct(product) {
        if (typeof product === 'string') {
            const lower = product.toLowerCase().trim();
            if (RADAR_PRODUCTS.includes(lower)) {
                return lower;
            }
        }
        return 'composite';
    }

    function clampRadarOpacity(opacity) {
        const num = Number(opacity);
        if (!Number.isFinite(num)) return RADAR_DEFAULT_OPACITY;
        return Math.max(RADAR_MIN_OPACITY, Math.min(RADAR_MAX_OPACITY, num));
    }

    // ============================================================
    // CAT OVERLAY STATE
    // ============================================================

    TM.cat = {
        enabled: false,
        priorityMode: false,
        routesFitToView: false,
        activeRouteIds: new Set(),
        layerGroup: null,
        vehicleMarkers: new Map(),
        vehicles: [],
        routes: [],
        stops: [],
        patterns: [],
        serviceAlerts: [],
        stopEtaCache: new Map(),
        routeCheckboxStates: {},
        overlapInfoByNumericId: new Map(),
        // Fetching state
        routesFetchPromise: null,
        stopsFetchPromise: null,
        vehiclesFetchPromise: null,
        // Refresh intervals
        refreshIntervals: []
    };

    // CAT constants
    const CAT_VEHICLE_MARKER_DEFAULT_COLOR = '#232D4B';
    const CAT_VEHICLE_MARKER_MIN_LABEL = '?';
    const CAT_OUT_OF_SERVICE_ROUTE_ID = 'out-of-service';

    // ============================================================
    // CAT UTILITIES
    // ============================================================

    function catRouteKey(routeId) {
        if (!routeId) return '';
        return `cat-${routeId}`;
    }

    function isCatOverlapRouteId(routeId) {
        if (typeof routeId === 'string') {
            return routeId.startsWith('cat-');
        }
        if (typeof routeId === 'number') {
            return TM.cat.overlapInfoByNumericId.has(routeId);
        }
        return false;
    }

    function catStopKey(stopId) {
        if (!stopId) return '';
        return `cat-stop-${stopId}`;
    }

    function buildCatVehicleIcon(color, label) {
        const fallbackLabel = label || CAT_VEHICLE_MARKER_MIN_LABEL;
        const safeColor = utils.escapeAttribute(color || CAT_VEHICLE_MARKER_DEFAULT_COLOR);
        const safeLabel = utils.escapeHtml(fallbackLabel);
        const html = `<div class="cat-vehicle-marker" style="--cat-marker-color:${safeColor};"><span class="cat-vehicle-marker__label">${safeLabel}</span></div>`;
        return L.divIcon({
            className: 'cat-vehicle-icon',
            html,
            iconSize: [38, 38],
            iconAnchor: [19, 19]
        });
    }

    // ============================================================
    // TRAINS STATE
    // ============================================================

    TM.trains = {
        visible: false,
        layerGroup: null,
        markers: new Map(),
        nameBubbles: {},
        pollIntervalId: null,
        lastFetchTime: 0,
        fetchPromise: null
    };

    const TRAINS_ENDPOINT = '/v1/testmap/trains';
    const TRAIN_REFRESH_INTERVAL_MS = 30000;

    // ============================================================
    // TRAIN UTILITIES
    // ============================================================

    function getTrainIdentifier(train) {
        if (!train) return null;
        return train.trainID || train.TrainID || train.trainNum || train.TrainNum || train.id || null;
    }

    function getTrainHeadingDegrees(headingValue, fallback = 0) {
        if (Number.isFinite(headingValue)) {
            return headingValue;
        }
        if (typeof headingValue === 'string') {
            const parsed = parseFloat(headingValue);
            if (Number.isFinite(parsed)) {
                return parsed;
            }
        }
        return fallback;
    }

    // ============================================================
    // EXPORT TO NAMESPACE
    // ============================================================

    TM.incidentUtils = {
        looksLikePulsePointIncident,
        getIncidentCoordinates,
        getIncidentId,
        createIncidentLeafletIcon,
        getIncidentHaloIcon
    };

    TM.incidentConstants = {
        PULSEPOINT_ENDPOINT,
        INCIDENT_REFRESH_INTERVAL_MS,
        FALLBACK_INCIDENT_ICON_SIZE,
        INCIDENT_ICON_SCALE,
        HALO_COLOR_RGB,
        HALO_BASE_OPACITY,
        HALO_DURATION_MS,
        HALO_MAX_RADIUS_PX,
        HALO_MIN_RADIUS_PX
    };

    TM.alertUtils = {
        formatServiceAlertTimeValue,
        extractServiceAlertTime,
        SERVICE_ALERT_START_FIELDS,
        SERVICE_ALERT_END_FIELDS
    };

    TM.radarUtils = {
        normalizeRadarProduct,
        clampRadarOpacity,
        RADAR_PRODUCTS,
        RADAR_DEFAULT_OPACITY,
        RADAR_MIN_OPACITY,
        RADAR_MAX_OPACITY,
        RADAR_REFRESH_INTERVAL_MS
    };

    TM.catUtils = {
        catRouteKey,
        isCatOverlapRouteId,
        catStopKey,
        buildCatVehicleIcon,
        CAT_VEHICLE_MARKER_DEFAULT_COLOR,
        CAT_VEHICLE_MARKER_MIN_LABEL,
        CAT_OUT_OF_SERVICE_ROUTE_ID
    };

    TM.trainUtils = {
        getTrainIdentifier,
        getTrainHeadingDegrees,
        TRAINS_ENDPOINT,
        TRAIN_REFRESH_INTERVAL_MS
    };

    console.log('[TestMap] Overlays module loaded');

})(window.TestMap);
