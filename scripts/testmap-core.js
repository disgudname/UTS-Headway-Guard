'use strict';

/**
 * TestMap Core Module
 * Contains: Constants, utilities, map initialization, shared state management
 * Dependencies: None (must load first)
 */

// Global namespace for TestMap modules
window.TestMap = window.TestMap || {};

(function(TM) {
    // ============================================================
    // PLATFORM DETECTION & FONT HANDLING
    // ============================================================

    const DEFAULT_MAP_FONT_STACK = `FGDC, sans-serif`;
    const IOS_MAP_FONT_STACK = `system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`;
    const IOS_BODY_CLASS = 'ios-font';

    function detectIOSPlatform() {
        if (typeof navigator !== 'object' || navigator === null) {
            return false;
        }
        const userAgent = typeof navigator.userAgent === 'string' ? navigator.userAgent : '';
        const platform = typeof navigator.platform === 'string' ? navigator.platform : '';
        const maxTouchPoints = typeof navigator.maxTouchPoints === 'number' ? navigator.maxTouchPoints : 0;

        if (/iPad|iPhone|iPod/i.test(userAgent) || /iPad|iPhone|iPod/i.test(platform)) {
            return true;
        }
        if (platform === 'MacIntel' && maxTouchPoints > 1) {
            return true;
        }
        if (typeof navigator.userAgentData === 'object' && navigator.userAgentData !== null) {
            try {
                const brands = Array.isArray(navigator.userAgentData.brands) ? navigator.userAgentData.brands : [];
                if (brands.some(brand => typeof brand.brand === 'string' && /iOS/i.test(brand.brand))) {
                    return true;
                }
            } catch (error) {
                // Ignore structured UA parsing failures
            }
        }
        return false;
    }

    const IS_IOS_PLATFORM = detectIOSPlatform();
    const ACTIVE_MAP_FONT_STACK = IS_IOS_PLATFORM ? IOS_MAP_FONT_STACK : DEFAULT_MAP_FONT_STACK;

    // Apply iOS class to body if needed
    if (typeof document !== 'undefined' && IS_IOS_PLATFORM) {
        const applyIOSClass = () => {
            if (document.body) {
                document.body.classList.add(IOS_BODY_CLASS);
                return true;
            }
            return false;
        };
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', applyIOSClass, { once: true });
        } else if (!applyIOSClass()) {
            document.addEventListener('DOMContentLoaded', applyIOSClass, { once: true });
        }
    }

    // ============================================================
    // GLOBAL CONFIGURATION
    // ============================================================

    window.ADSB_PROXY_ENDPOINT = window.ADSB_PROXY_ENDPOINT || '/adsb';

    // Plane style configuration
    function applyPlaneStyleOptions() {
        if (typeof window.setPlaneStyleOptions === 'function') {
            window.setPlaneStyleOptions({ atcStyle: false });
        } else {
            window.atcStyle = false;
        }
    }

    function schedulePlaneStyleOverride() {
        if (typeof window.setPlaneStyleOptions === 'function') {
            applyPlaneStyleOptions();
            return;
        }
        const tryApply = () => {
            applyPlaneStyleOptions();
            window.removeEventListener('load', tryApply);
        };
        if (document.readyState === 'loading' || document.readyState === 'interactive') {
            document.addEventListener('DOMContentLoaded', tryApply, { once: true });
            window.addEventListener('load', tryApply, { once: true });
        } else if (document.readyState === 'complete') {
            if (typeof queueMicrotask === 'function') {
                queueMicrotask(applyPlaneStyleOptions);
            } else {
                setTimeout(applyPlaneStyleOptions, 0);
            }
        }
    }
    schedulePlaneStyleOverride();

    // ============================================================
    // CORE STATE MANAGEMENT
    // ============================================================

    // Visibility state
    TM.pageIsVisible = !document.hidden;
    TM.refreshIntervalsPaused = false;

    function handleVisibilityChange() {
        TM.pageIsVisible = !document.hidden;
        if (TM.pageIsVisible && TM.refreshIntervalsPaused) {
            TM.refreshIntervalsPaused = false;
            // Notify modules to resume
            if (typeof TM.onVisibilityResume === 'function') {
                TM.onVisibilityResume();
            }
        } else if (!TM.pageIsVisible) {
            TM.refreshIntervalsPaused = true;
        }
    }

    if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
        document.addEventListener('visibilitychange', handleVisibilityChange, { passive: true });
    }

    // ============================================================
    // DOM UTILITIES
    // ============================================================

    const domElementCache = new Map();

    function getCachedElementById(id) {
        if (typeof document === 'undefined' || typeof id !== 'string') {
            return null;
        }
        const trimmedId = id.trim();
        if (trimmedId === '') return null;
        const cached = domElementCache.get(trimmedId);
        if (cached && cached.isConnected) {
            return cached;
        }
        const element = document.getElementById(trimmedId);
        if (element) {
            domElementCache.set(trimmedId, element);
            return element;
        }
        domElementCache.delete(trimmedId);
        return null;
    }

    // ============================================================
    // THROTTLER / DEBOUNCER UTILITIES
    // ============================================================

    function createAnimationFrameThrottler(callback) {
        if (typeof callback !== 'function') {
            return () => {};
        }
        let scheduled = false;
        let lastArgs = null;
        return (...args) => {
            lastArgs = args;
            if (scheduled) return;
            scheduled = true;
            requestAnimationFrame(() => {
                scheduled = false;
                callback(...lastArgs);
            });
        };
    }

    function createDebouncer(callback, wait = 100) {
        if (typeof callback !== 'function') {
            return () => {};
        }
        let timeoutId = null;
        return (...args) => {
            if (timeoutId !== null) {
                clearTimeout(timeoutId);
            }
            timeoutId = setTimeout(() => {
                timeoutId = null;
                callback(...args);
            }, wait);
        };
    }

    // ============================================================
    // SCRIPT LOADING
    // ============================================================

    const loadedScripts = new Set();

    function loadScriptOnce(src) {
        if (loadedScripts.has(src)) {
            return Promise.resolve();
        }
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = src;
            script.async = true;
            script.onload = () => {
                loadedScripts.add(src);
                resolve();
            };
            script.onerror = () => reject(new Error(`Failed to load script: ${src}`));
            document.head.appendChild(script);
        });
    }

    // ============================================================
    // COLOR & FORMATTING UTILITIES
    // ============================================================

    function sanitizeCssColor(color) {
        if (typeof color !== 'string') return '';
        const trimmed = color.trim();
        if (!trimmed) return '';
        // Basic validation for hex colors and named colors
        if (/^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$/.test(trimmed)) {
            return trimmed;
        }
        if (/^[a-zA-Z]+$/.test(trimmed)) {
            return trimmed;
        }
        if (/^rgb\(/.test(trimmed) || /^rgba\(/.test(trimmed) || /^hsl\(/.test(trimmed)) {
            return trimmed;
        }
        return '';
    }

    function escapeHtml(text) {
        if (typeof text !== 'string') return '';
        return text
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    function escapeAttribute(value) {
        if (typeof value !== 'string') return '';
        return value.replace(/"/g, '&quot;').replace(/'/g, '&#039;');
    }

    function contrastBW(hexColor) {
        if (typeof hexColor !== 'string') return '#ffffff';
        const hex = hexColor.replace('#', '');
        if (hex.length !== 6 && hex.length !== 3) return '#ffffff';
        let r, g, b;
        if (hex.length === 3) {
            r = parseInt(hex[0] + hex[0], 16);
            g = parseInt(hex[1] + hex[1], 16);
            b = parseInt(hex[2] + hex[2], 16);
        } else {
            r = parseInt(hex.substring(0, 2), 16);
            g = parseInt(hex.substring(2, 4), 16);
            b = parseInt(hex.substring(4, 6), 16);
        }
        // YIQ formula for perceived brightness
        const yiq = (r * 299 + g * 587 + b * 114) / 1000;
        return yiq >= 128 ? '#000000' : '#ffffff';
    }

    // ============================================================
    // DISTANCE CALCULATIONS
    // ============================================================

    const EARTH_RADIUS_METERS = 6371000;

    function computeGreatCircleDistanceMeters(lat1, lon1, lat2, lon2) {
        const toRad = deg => deg * Math.PI / 180;
        const dLat = toRad(lat2 - lat1);
        const dLon = toRad(lon2 - lon1);
        const a = Math.sin(dLat / 2) ** 2 +
                  Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
        return EARTH_RADIUS_METERS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    // ============================================================
    // SHARED CONSTANTS
    // ============================================================

    TM.CONSTANTS = Object.freeze({
        DEFAULT_MAP_FONT_STACK,
        IOS_MAP_FONT_STACK,
        ACTIVE_MAP_FONT_STACK,
        EARTH_RADIUS_METERS,
        // Map defaults
        INITIAL_MAP_VIEW: Object.freeze({
            center: [38.0336, -78.5080],
            zoom: 14
        }),
        // Display modes
        DISPLAY_MODES: Object.freeze({
            DEFAULT: 'default',
            SPEED: 'speed',
            BLOCK: 'block',
            NAME: 'name'
        }),
        // Performance
        PANEL_COLLAPSE_BREAKPOINT: 768,
        STOP_RENDER_BOUNDS_PADDING: 0.1,
        STOP_GROUPING_PIXEL_DISTANCE: 20,
        STOP_MARKER_ICON_SIZE: 18,
        STOP_MARKER_POOL_MAX_SIZE: 100,
        STOP_RENDER_DEBOUNCE_MS: 150,
        PROGRESSIVE_STOP_BATCH_SIZE: 20,
        // Polling intervals
        VEHICLE_POLL_INTERVAL_MS: 7000,
        STOP_POLL_INTERVAL_MS: 60000,
        ROUTE_PATH_POLL_INTERVAL_MS: 20000,
        TRANSLOC_VEHICLES_TTL_MS: 4000,
        TRANSLOC_METADATA_TTL_MS: 60000,
        SERVICE_ALERT_REFRESH_INTERVAL_MS: 60000,
        INCIDENT_REFRESH_INTERVAL_MS: 45000
    });

    // ============================================================
    // SHARED STATE (to be populated by other modules)
    // ============================================================

    TM.state = {
        map: null,
        mapContainer: null,
        baseURL: '',
        adminMode: false,
        kioskMode: false,
        adminKioskMode: false,
        dispatcherMode: false,
        displayMode: 'default',
        lowPerformanceMode: false,
        activeRoutes: [],
        routeSelections: {},
        allRoutes: [],
        routeColors: {},
        refreshIntervals: [],
        refreshIntervalsActive: false
    };

    // ============================================================
    // EXPORT TO NAMESPACE
    // ============================================================

    TM.utils = {
        getCachedElementById,
        createAnimationFrameThrottler,
        createDebouncer,
        loadScriptOnce,
        sanitizeCssColor,
        escapeHtml,
        escapeAttribute,
        contrastBW,
        computeGreatCircleDistanceMeters
    };

    TM.IS_IOS_PLATFORM = IS_IOS_PLATFORM;

    // Provide a hook for modules to register visibility resume handlers
    TM._visibilityResumeHandlers = [];
    TM.onVisibilityResume = function() {
        TM._visibilityResumeHandlers.forEach(handler => {
            try {
                handler();
            } catch (e) {
                console.error('Visibility resume handler error:', e);
            }
        });
    };
    TM.registerVisibilityResumeHandler = function(handler) {
        if (typeof handler === 'function') {
            TM._visibilityResumeHandlers.push(handler);
        }
    };

    console.log('[TestMap] Core module loaded');

})(window.TestMap);
