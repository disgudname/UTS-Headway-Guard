'use strict';

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
      const hasIOSBrand = brands.some(brand => typeof brand.brand === 'string' && /iOS/i.test(brand.brand));
      if (hasIOSBrand) {
        return true;
      }
    } catch (error) {
      // Ignore structured UA parsing failures.
    }
  }

  return false;
}

const IS_IOS_PLATFORM = detectIOSPlatform();
const ACTIVE_MAP_FONT_STACK = IS_IOS_PLATFORM ? IOS_MAP_FONT_STACK : DEFAULT_MAP_FONT_STACK;

if (typeof document !== 'undefined' && IS_IOS_PLATFORM) {
  const applyIOSClass = () => {
    if (document.body) {
      document.body.classList.add(IOS_BODY_CLASS);
      return true;
    }
    return false;
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      applyIOSClass();
    }, { once: true });
  } else {
    if (!applyIOSClass()) {
      document.addEventListener('DOMContentLoaded', () => {
        applyIOSClass();
      }, { once: true });
    }
  }
}

window.ADSB_PROXY_ENDPOINT = window.ADSB_PROXY_ENDPOINT || '/adsb';

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

      function createAnimationFrameThrottler(callback) {
        if (typeof callback !== 'function') {
          return () => {};
        }
        let scheduled = false;
        let lastArgs = null;
        return (...args) => {
          lastArgs = args;
          if (scheduled) {
            return;
          }
          scheduled = true;
          const runner = () => {
            scheduled = false;
            callback(...lastArgs);
          };
          if (typeof requestAnimationFrame === 'function') {
            requestAnimationFrame(runner);
          } else {
            setTimeout(runner, 16);
          }
        };
      }

      const loadedScriptPromises = new Map();

      function loadScriptOnce(url) {
        if (typeof document === 'undefined') {
          return Promise.reject(new Error('Document is not available'));
        }
        if (typeof url !== 'string' || url.trim() === '') {
          return Promise.reject(new Error('Invalid script URL'));
        }
        const normalizedUrl = url.trim();
        if (loadedScriptPromises.has(normalizedUrl)) {
          return loadedScriptPromises.get(normalizedUrl);
        }
        const promise = new Promise((resolve, reject) => {
          const script = document.createElement('script');
          script.src = normalizedUrl;
          script.async = true;
          script.onload = () => resolve();
          script.onerror = event => {
            loadedScriptPromises.delete(normalizedUrl);
            const error = new Error(`Failed to load script: ${normalizedUrl}`);
            error.event = event;
            reject(error);
          };
          document.head.appendChild(script);
        });
        loadedScriptPromises.set(normalizedUrl, promise);
        return promise;
      }

// Manually set these variables.
      // adminMode: true for admin view (with speed/block bubbles and unit numbers).
      //            Can be disabled via URL param `adminMode=false`.
      //            In public mode (adminMode=false) the route selector is still shown
      //            but only for routes that are public-facing.
      // kioskMode: true to hide the route selector/tab and suppress vehicle overlays for a public display.
      // adminKioskMode: true to hide the route selector/tab while retaining admin overlays (previous kiosk behavior).
      // displayMode selects whether admin overlays show speed, block numbers, or neither.
      if (typeof window !== 'undefined' && typeof window.usp === 'undefined') {
        let searchParams = null;
        if (typeof URLSearchParams === 'function' && typeof window.location === 'object' && typeof window.location.search === 'string') {
          try {
            searchParams = new URLSearchParams(window.location.search || '');
          } catch (error) {
            searchParams = null;
          }
        }
        window.usp = {
          has(name) {
            if (!searchParams) return false;
            return searchParams.has(name);
          },
          getInt(name, defaultValue = 0) {
            if (!searchParams) return defaultValue;
            const raw = searchParams.get(name);
            if (raw === null) return defaultValue;
            const parsed = Number.parseInt(raw, 10);
            return Number.isFinite(parsed) ? parsed : defaultValue;
          },
          getBoolean(name, defaultValue = false) {
            if (!searchParams) return defaultValue;
            if (!searchParams.has(name)) return defaultValue;
            const raw = searchParams.get(name);
            if (raw === null) return defaultValue;
            const normalized = raw.trim().toLowerCase();
            if (normalized === '') return true;
            if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
            if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
            return defaultValue;
          }
        };
      }

      const DISPLAY_MODES = Object.freeze({
        SPEED: 'speed',
        BLOCK: 'block',
        NONE: 'none'
      });
      let adminMode = false; // shows unit numbers and speed/block bubbles
      let adminModeExplicitlySet = false;
      let kioskMode = false;
      let adminKioskMode = false;
      let kioskExperienceActive = false;
      let adminKioskUiSuppressed = false;
      let kioskUiSuppressed = false;
      let kioskVehicleStatusKnown = false;
      let displayMode = DISPLAY_MODES.BLOCK;

      const PANEL_COLLAPSE_BREAKPOINT = 600;

      const enableOverlapDashRendering = true;

      function adminFeaturesAllowed() {
        return (adminMode && !kioskMode) || adminKioskMode;
      }

      function radarFeaturesAllowed() {
        return adminFeaturesAllowed() && !adminKioskMode;
      }

      function trainsFeatureAllowed() {
        return adminFeaturesAllowed() && !adminKioskMode;
      }

      function isKioskExperienceActive() {
        return kioskExperienceActive;
      }

      function updateKioskExperienceState() {
        kioskExperienceActive = Boolean(kioskMode) || Boolean(adminKioskMode);
        return kioskExperienceActive;
      }

      const ADMIN_AUTH_ENDPOINT = '/api/dispatcher/auth';
      const ADMIN_LOGOUT_ENDPOINT = '/api/dispatcher/logout';
      let adminAuthCheckPromise = null;
      let adminAuthInitialized = false;
      let urlAdminPassword = '';
      let urlAdminAuthAttempted = false;
      let urlAdminAuthSucceeded = false;
      let adminLogoutInProgress = false;
      let navAuthorized = false;

      function updateUserAuthorizationState(authorized) {
        const normalized = authorized === true;
        if (navAuthorized === normalized) {
          return navAuthorized;
        }
        navAuthorized = normalized;
        if (!navAuthorized) {
          setOnDemandVehiclesEnabled(false);
          setOnDemandStopsEnabled(false);
        } else {
          updateOnDemandButton();
          updateOnDemandStopsButton();
        }
        return navAuthorized;
      }

      function userIsAuthorizedForOnDemand() {
        return navAuthorized === true;
      }

      function setAdminModeEnabled(enabled, options = {}) {
        const normalized = Boolean(enabled);
        const clearExplicitFlag = options && options.clearExplicitFlag === true;
        const modeChanged = adminMode !== normalized;
        adminMode = normalized;
        if (clearExplicitFlag) {
          adminModeExplicitlySet = false;
        }
        if (!modeChanged) {
          return adminMode;
        }
        updateControlPanel();
        updateRouteSelector(activeRoutes, true);
        updateRouteLegend([], { preserveOnEmpty: true });
        updateDisplayModeOverlays();
        refreshMap();
        return adminMode;
      }

      function getAdminAuthElements() {
        return {
          overlay: getCachedElementById('adminAuthOverlay'),
          form: getCachedElementById('adminAuthForm'),
          passwordInput: getCachedElementById('adminAuthPassword'),
          status: getCachedElementById('adminAuthStatus'),
          submitButton: getCachedElementById('adminAuthSubmit'),
          closeButton: getCachedElementById('adminAuthClose')
        };
      }

      function closeAdminPasswordPrompt() {
        const { overlay, status, passwordInput, submitButton } = getAdminAuthElements();
        if (!overlay) {
          return;
        }
        overlay.classList.remove('is-visible');
        overlay.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('admin-auth-open');
        if (status) {
          status.textContent = '';
        }
        if (passwordInput) {
          passwordInput.value = '';
        }
        if (submitButton) {
          submitButton.disabled = false;
        }
      }

      async function checkAdminAuthorization(options = {}) {
        const { silent = false, forceEnable = false, forceRefresh = false } = options || {};
        if (!forceEnable && adminModeExplicitlySet && adminMode === false) {
          return false;
        }
        if (adminAuthCheckPromise && !forceRefresh) {
          return adminAuthCheckPromise;
        }
        const checkPromise = (async () => {
          try {
            const response = await fetch(ADMIN_AUTH_ENDPOINT);
            if (!response.ok) {
              return false;
            }
            let data = null;
            try {
              data = await response.json();
            } catch (error) {
              data = null;
            }
            const authorized = data && data.authorized === true;
            updateUserAuthorizationState(authorized);
            const requiresPassword = data && typeof data.required === 'boolean' ? data.required : true;
            if (authorized || requiresPassword === false) {
              if (forceEnable) {
                setAdminModeEnabled(true, { clearExplicitFlag: true });
              } else if (!adminMode) {
                setAdminModeEnabled(true);
              }
              return true;
            }
          } catch (error) {
            if (!silent) {
              console.warn('Failed to verify admin authorization', error);
            }
          }
          return false;
        })().finally(() => {
          adminAuthCheckPromise = null;
        });
        adminAuthCheckPromise = checkPromise;
        return checkPromise;
      }

      async function submitAdminPassword(password, options = {}) {
        const { silent = false, enableAdminMode = true } = options || {};
        if (typeof password !== 'string') {
          return { ok: false, error: new Error('Invalid password value') };
        }
        const trimmed = password.trim();
        if (!trimmed) {
          return { ok: false, error: new Error('Password is empty') };
        }
        try {
          const response = await fetch(ADMIN_AUTH_ENDPOINT, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json'
            },
            credentials: 'same-origin',
            body: JSON.stringify({ password: trimmed })
          });
          let data = null;
          try {
            data = await response.json();
          } catch (_error) {
            data = null;
          }
          if (response.ok) {
            if (enableAdminMode) {
              setAdminModeEnabled(true, { clearExplicitFlag: true });
            }
            return { ok: true, data };
          }
          return { ok: false, data };
        } catch (error) {
          if (!silent) {
            console.error('Password verification failed', error);
          }
          return { ok: false, error };
        }
      }

      async function logoutAdminTools(event) {
        if (event && typeof event.preventDefault === 'function') {
          event.preventDefault();
        }
        if (adminLogoutInProgress) {
          return false;
        }
        adminLogoutInProgress = true;
        const trigger = event && event.currentTarget && typeof event.currentTarget === 'object'
          ? event.currentTarget
          : null;
        if (trigger && typeof trigger.disabled === 'boolean') {
          trigger.disabled = true;
        }
        try {
          const response = await fetch(ADMIN_LOGOUT_ENDPOINT, {
            method: 'POST',
            credentials: 'same-origin'
          });
          if (!response.ok) {
            throw new Error(`Logout request failed with status ${response.status}`);
          }
          urlAdminPassword = '';
          urlAdminAuthSucceeded = false;
          urlAdminAuthAttempted = false;
          adminModeExplicitlySet = true;
          setAdminModeEnabled(false);
          updateUserAuthorizationState(false);
          return true;
        } catch (error) {
          console.error('Failed to log out of admin tools', error);
          if (trigger && typeof trigger.disabled === 'boolean') {
            trigger.disabled = false;
          }
          return false;
        } finally {
          adminLogoutInProgress = false;
        }
      }

      function handleNavBarAuthChange(event) {
        if (!event || !event.detail) {
          return;
        }
        const authorized = event.detail.authorized === true;
        updateUserAuthorizationState(authorized);
        if (authorized) {
          return;
        }
        const wasAuthorized = adminMode || urlAdminAuthSucceeded;
        if (!wasAuthorized) {
          return;
        }
        urlAdminPassword = '';
        urlAdminAuthSucceeded = false;
        urlAdminAuthAttempted = false;
        adminModeExplicitlySet = true;
        setAdminModeEnabled(false);
      }

      async function attemptAdminAuthorizationFromUrl() {
        if (urlAdminAuthAttempted) {
          return urlAdminAuthSucceeded;
        }
        urlAdminAuthAttempted = true;
        if (!urlAdminPassword) {
          urlAdminAuthSucceeded = false;
          return false;
        }
        const result = await submitAdminPassword(urlAdminPassword, { silent: true });
        urlAdminAuthSucceeded = Boolean(result && result.ok);
        return urlAdminAuthSucceeded;
      }

      function initializeAdminAuthUI() {
        if (adminAuthInitialized) {
          return;
        }
        const { overlay, form, passwordInput, status, submitButton, closeButton } = getAdminAuthElements();
        if (!overlay || !form || !passwordInput) {
          return;
        }
        adminAuthInitialized = true;

        const handleSubmit = async event => {
          event.preventDefault();
          if (!passwordInput) {
            return;
          }
          const password = passwordInput.value.trim();
          if (password.length === 0) {
            if (status) {
              status.textContent = 'Please enter the password.';
            }
            passwordInput.focus();
            return;
          }
          if (submitButton) {
            submitButton.disabled = true;
          }
          if (status) {
            status.textContent = '';
          }
          try {
            const result = await submitAdminPassword(password, { silent: true });
            if (result.ok) {
              closeAdminPasswordPrompt();
              await checkAdminAuthorization({ silent: true, forceEnable: true, forceRefresh: true });
              return;
            }
            if (result && result.error) {
              console.error('Password verification failed', result.error);
              if (status) {
                status.textContent = 'Unable to verify password. Try again.';
              }
              return;
            }
            let detail = 'Incorrect password.';
            if (result && result.data && typeof result.data.detail === 'string' && result.data.detail.trim() !== '') {
              detail = result.data.detail.trim();
            }
            if (status) {
              status.textContent = detail;
            }
          } finally {
            if (submitButton) {
              submitButton.disabled = false;
            }
            if (passwordInput) {
              passwordInput.value = '';
              if (overlay && overlay.classList.contains('is-visible')) {
                passwordInput.focus();
              }
            }
          }
        };

        form.addEventListener('submit', handleSubmit);
        overlay.addEventListener('click', event => {
          if (event.target === overlay) {
            closeAdminPasswordPrompt();
          }
        });
        if (closeButton) {
          closeButton.addEventListener('click', event => {
            event.preventDefault();
            closeAdminPasswordPrompt();
          });
        }
        document.addEventListener('keydown', event => {
          if (event.key === 'Escape') {
            const { overlay: currentOverlay } = getAdminAuthElements();
            if (currentOverlay && currentOverlay.classList.contains('is-visible')) {
              closeAdminPasswordPrompt();
            }
          }
        });
      }

      async function openAdminPasswordPrompt() {
        const alreadyAuthorized = await checkAdminAuthorization({ silent: true, forceEnable: true });
        if (alreadyAuthorized) {
          return;
        }
        if (typeof window === 'undefined' || typeof window.location === 'undefined') {
          return;
        }
        const path = typeof window.location.pathname === 'string' ? window.location.pathname : '/';
        const search = typeof window.location.search === 'string' ? window.location.search : '';
        const hash = typeof window.location.hash === 'string' ? window.location.hash : '';
        const target = `${path}${search}${hash}`;
        const loginUrl = `/login?return=${encodeURIComponent(target)}`;
        window.location.href = loginUrl;
      }

      if (typeof window !== 'undefined') {
        window.openAdminPasswordPrompt = openAdminPasswordPrompt;
        window.closeAdminPasswordPrompt = closeAdminPasswordPrompt;
        window.logoutAdminTools = logoutAdminTools;
        if (typeof window.addEventListener === 'function') {
          window.addEventListener('hg-nav-auth-changed', handleNavBarAuthChange);
        }
      }

      function suppressAdminKioskPanels() {
        if (!adminKioskMode || adminKioskUiSuppressed) {
          return;
        }
        if (typeof document === 'undefined') {
          return;
        }

        const controlPanel = getCachedElementById('controlPanel');
        const routeSelector = getCachedElementById('routeSelector');
        const controlTab = getCachedElementById('controlPanelTab');
        const routeTab = getCachedElementById('routeSelectorTab');
        const elementsReady = controlPanel || routeSelector || controlTab || routeTab;
        if (!elementsReady) {
          return;
        }

        const hidePanel = panel => {
          if (!panel) return;
          if (panel.classList && typeof panel.classList.add === 'function') {
            panel.classList.add('hidden');
          }
          if (panel.style) {
            panel.style.display = 'none';
          }
          if (typeof panel.setAttribute === 'function') {
            panel.setAttribute('aria-hidden', 'true');
          }
          try {
            panel.innerHTML = '';
          } catch (error) {
            // Ignore failures to clear panel content in environments without innerHTML.
          }
        };

        hidePanel(controlPanel);
        hidePanel(routeSelector);

        const hideTab = tab => {
          if (!tab) return;
          if (tab.style) {
            tab.style.display = 'none';
          }
        };

        hideTab(controlTab);
        hideTab(routeTab);

        adminKioskUiSuppressed = true;
      }

      function ensurePanelsHiddenForKioskExperience() {
        if (!isKioskExperienceActive()) {
          return false;
        }

        if (adminKioskMode) {
          const wasSuppressed = adminKioskUiSuppressed;
          suppressAdminKioskPanels();
          if (!wasSuppressed && adminKioskUiSuppressed) {
            kioskUiSuppressed = true;
          }
          return true;
        }

        if (!kioskMode || kioskUiSuppressed) {
          return true;
        }

        if (typeof document === 'undefined') {
          return kioskMode;
        }

        const hidePanelElement = panel => {
          if (!panel) return;
          if (panel.classList && typeof panel.classList.add === 'function') {
            panel.classList.add('hidden');
          }
          if (panel.style) {
            panel.style.display = 'none';
          }
          if (typeof panel.setAttribute === 'function') {
            panel.setAttribute('aria-hidden', 'true');
          }
          try {
            panel.innerHTML = '';
          } catch (error) {
            // Ignore DOM write failures.
          }
        };

        const hideTabElement = tab => {
          if (!tab) return;
          if (tab.style) {
            tab.style.display = 'none';
          }
        };

        hidePanelElement(getCachedElementById('controlPanel'));
        hidePanelElement(getCachedElementById('routeSelector'));
        hideTabElement(getCachedElementById('controlPanelTab'));
        hideTabElement(getCachedElementById('routeSelectorTab'));

        kioskUiSuppressed = true;
        return true;
      }

      const RADAR_PRODUCTS = Object.freeze({
        BASE: 'base',
        COMPOSITE: 'composite'
      });
      const RADAR_PRODUCT_ORDER = Object.freeze([RADAR_PRODUCTS.BASE, RADAR_PRODUCTS.COMPOSITE]);
      const RADAR_PRODUCT_INFO = Object.freeze({
        [RADAR_PRODUCTS.BASE]: {
          label: "Base Reflectivity (0.5° Tilt)",
          urlTemplate: "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-900913/{z}/{x}/{y}.png",
          cacheBustingStrategy: "none"
        },
        [RADAR_PRODUCTS.COMPOSITE]: {
          label: "Composite Reflectivity",
          urlTemplate: "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0r-900913/{z}/{x}/{y}.png",
          cacheBustingStrategy: "none"
        }
      });
      const RADAR_DEFAULT_PRODUCT = RADAR_PRODUCTS.BASE;
      const RADAR_DEFAULT_OPACITY = 0.5;
      const RADAR_MIN_OPACITY = 0.3;
      const RADAR_MAX_OPACITY = 0.8;
      const RADAR_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
      const RADAR_SESSION_STORAGE_KEY = "hg.radar.state";
      const RADAR_UNAVAILABLE_MESSAGE = "Radar temporarily unavailable.";
      const RADAR_CONSECUTIVE_ERROR_THRESHOLD = 3;
      const RADAR_PANE_NAME = "radarPane";
      const RADAR_CACHE_BUST_PLACEHOLDER = "{cacheBust}";

      let showRadar = false;
      let radarConfiguredProduct = RADAR_DEFAULT_PRODUCT;
      let radarConfiguredOpacity = RADAR_DEFAULT_OPACITY;

      let radarEnabled = false;
      let radarProduct = RADAR_DEFAULT_PRODUCT;
      let radarOpacity = RADAR_DEFAULT_OPACITY;
      let radarLayer = null;
      let radarLayerProduct = null;
      let radarRefreshTimerId = null;
      let radarCacheBustKey = "";
      let radarTileErrorCount = 0;
      let radarTemporarilyUnavailable = false;
      let radarLastFailedUrl = "";
      let radarSuppressedForErrors = false;
      let radarSuppressionTimeoutId = null;

      const CONFIG_ENDPOINT = '/v1/config';
      const DISPATCHER_DEFAULT_BRIDGE = Object.freeze({
        lat: 38.03404931117353,
        lng: -78.4995922309842
      });
      const DISPATCHER_DEFAULT_RADIUS_METERS = 117;
      const DISPATCHER_DEFAULT_OVERHEIGHT_IDS = Object.freeze([
        '25131', '25231', '25331', '25431',
        '17132', '14132', '12432', '18532'
      ]);
      const DISPATCHER_MIN_FOCUS_ZOOM = 18;

      const dispatcherMode = window.usp.getBoolean('dispatcher', false);
      let dispatcherConfig = null;
      let dispatcherConfigPromise = null;
      const dispatcherLockState = {
        active: false,
        vehicleKey: null,
        circle: null,
        popup: null,
        pendingPopupVehicleKey: null,
        mapInteractionBackup: null,
        targetZoom: null,
        lockedCenter: null,
        originalView: null,
        popupCloseSuppressionDepth: 0
      };

      function dispatcherFeaturesAllowed() {
        return dispatcherMode || adminKioskMode;
      }

      function buildDispatcherDefaultConfig() {
        return {
          bridgeLat: DISPATCHER_DEFAULT_BRIDGE.lat,
          bridgeLng: DISPATCHER_DEFAULT_BRIDGE.lng,
          radiusMeters: DISPATCHER_DEFAULT_RADIUS_METERS,
          overheightBusIds: new Set(DISPATCHER_DEFAULT_OVERHEIGHT_IDS)
        };
      }

      function normalizeDispatcherConfigPayload(payload) {
        const defaults = buildDispatcherDefaultConfig();
        if (!payload || typeof payload !== 'object') {
          return defaults;
        }
        const normalized = {
          bridgeLat: Number(payload.BRIDGE_LAT),
          bridgeLng: Number(payload.BRIDGE_LON),
          radiusMeters: Number(payload.BRIDGE_RADIUS),
          overheightBusIds: new Set()
        };
        if (!Number.isFinite(normalized.bridgeLat)) {
          normalized.bridgeLat = defaults.bridgeLat;
        }
        if (!Number.isFinite(normalized.bridgeLng)) {
          normalized.bridgeLng = defaults.bridgeLng;
        }
        if (!Number.isFinite(normalized.radiusMeters) || normalized.radiusMeters <= 0) {
          normalized.radiusMeters = defaults.radiusMeters;
        }
        const sourceList = Array.isArray(payload.OVERHEIGHT_BUSES)
          ? payload.OVERHEIGHT_BUSES
          : DISPATCHER_DEFAULT_OVERHEIGHT_IDS;
        sourceList.forEach(value => {
          const text = value === undefined || value === null ? '' : `${value}`.trim();
          if (text) {
            normalized.overheightBusIds.add(text);
          }
        });
        if (normalized.overheightBusIds.size === 0) {
          defaults.overheightBusIds.forEach(id => normalized.overheightBusIds.add(id));
        }
        return normalized;
      }

      function ensureDispatcherConfigLoaded() {
        if (!dispatcherFeaturesAllowed()) {
          return Promise.resolve(null);
        }
        if (dispatcherConfig) {
          return Promise.resolve(dispatcherConfig);
        }
        if (dispatcherConfigPromise) {
          return dispatcherConfigPromise;
        }
        dispatcherConfigPromise = fetch(CONFIG_ENDPOINT, { cache: 'no-store' })
          .then(response => {
            if (!response || !response.ok) {
              throw new Error(response ? `HTTP ${response.status}` : 'No response');
            }
            return response.json();
          })
          .then(data => {
            dispatcherConfig = normalizeDispatcherConfigPayload(data);
            return dispatcherConfig;
          })
          .catch(error => {
            console.error('Failed to load dispatcher configuration:', error);
            dispatcherConfig = buildDispatcherDefaultConfig();
            return dispatcherConfig;
          })
          .finally(() => {
            dispatcherConfigPromise = null;
          });
        return dispatcherConfigPromise;
      }

      function computeGreatCircleDistanceMeters(lat1, lon1, lat2, lon2) {
        const toRadians = value => (Number.isFinite(value) ? value * (Math.PI / 180) : NaN);
        const phi1 = toRadians(lat1);
        const phi2 = toRadians(lat2);
        const deltaPhi = toRadians(lat2 - lat1);
        const deltaLambda = toRadians(lon2 - lon1);
        if ([phi1, phi2, deltaPhi, deltaLambda].some(value => Number.isNaN(value))) {
          return NaN;
        }
        const a = Math.sin(deltaPhi / 2) ** 2
          + Math.cos(phi1) * Math.cos(phi2) * Math.sin(deltaLambda / 2) ** 2;
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        const earthRadiusMeters = 6371000;
        return earthRadiusMeters * c;
      }

      function isDispatcherLockActive() {
        return dispatcherFeaturesAllowed() && !!dispatcherLockState.active;
      }

      function disableMapInteractionsForDispatcher() {
        if (!map) {
          return;
        }
        const backup = dispatcherLockState.mapInteractionBackup || {};
        const controls = ['dragging', 'scrollWheelZoom', 'doubleClickZoom', 'boxZoom', 'touchZoom', 'keyboard'];
        controls.forEach(name => {
          const control = map[name];
          if (!control || typeof control.disable !== 'function') {
            return;
          }
          if (!Object.prototype.hasOwnProperty.call(backup, name) && typeof control.enabled === 'function') {
            try {
              backup[name] = control.enabled();
            } catch (error) {
              backup[name] = undefined;
            }
          } else if (!Object.prototype.hasOwnProperty.call(backup, name)) {
            backup[name] = undefined;
          }
          try {
            control.disable();
          } catch (error) {
            console.warn(`Unable to disable map control ${name}:`, error);
          }
        });
        dispatcherLockState.mapInteractionBackup = backup;
      }

      function restoreMapInteractionsForDispatcher() {
        if (!map) {
          return;
        }
        const backup = dispatcherLockState.mapInteractionBackup || {};
        const controls = ['dragging', 'scrollWheelZoom', 'doubleClickZoom', 'boxZoom', 'touchZoom', 'keyboard'];
        controls.forEach(name => {
          const control = map[name];
          if (!control) {
            return;
          }
          const wasEnabled = backup[name];
          try {
            if (typeof control.enable === 'function' && typeof control.disable === 'function') {
              if (wasEnabled === false) {
                control.disable();
              } else {
                control.enable();
              }
            } else if (typeof control.enable === 'function' && wasEnabled !== false) {
              control.enable();
            }
          } catch (error) {
            console.warn(`Unable to restore map control ${name}:`, error);
          }
        });
        dispatcherLockState.mapInteractionBackup = null;
      }

      function ensureDispatcherCircle(config) {
        if (!dispatcherFeaturesAllowed() || !map || typeof L === 'undefined' || typeof L.circle !== 'function') {
          return null;
        }
        if (dispatcherLockState.circle) {
          return dispatcherLockState.circle;
        }
        const circle = L.circle([config.bridgeLat, config.bridgeLng], {
          radius: config.radiusMeters,
          color: '#dc2626',
          weight: 2,
          fillColor: '#dc2626',
          fillOpacity: 0.15,
          interactive: false,
          pane: 'busesPane'
        });
        dispatcherLockState.circle = circle;
        if (map && typeof circle.addTo === 'function') {
          circle.addTo(map);
        }
        return circle;
      }

      function suppressDispatcherPopupCloseHandling(callback) {
        dispatcherLockState.popupCloseSuppressionDepth = (dispatcherLockState.popupCloseSuppressionDepth || 0) + 1;
        try {
          return callback();
        } finally {
          dispatcherLockState.popupCloseSuppressionDepth = Math.max(
            0,
            (dispatcherLockState.popupCloseSuppressionDepth || 0) - 1
          );
        }
      }

      function clearDispatcherPopupForVehicle(vehicleKey) {
        if (!vehicleKey) {
          return;
        }
        const marker = markers && markers[vehicleKey];
        if (!marker) {
          return;
        }
        suppressDispatcherPopupCloseHandling(() => {
          try {
            const popup = typeof marker.getPopup === 'function' ? marker.getPopup() : null;
            if (popup) {
              if (map && typeof map.closePopup === 'function') {
                map.closePopup(popup);
              }
              if (typeof popup.remove === 'function') {
                popup.remove();
              }
            }
            if (typeof marker.closePopup === 'function') {
              marker.closePopup();
            }
            if (typeof marker.unbindPopup === 'function') {
              marker.unbindPopup();
            }
            if (dispatcherLockState.popup && popup && dispatcherLockState.popup === popup) {
              dispatcherLockState.popup = null;
            }
          } catch (error) {
            console.warn('Failed to clear dispatcher popup for vehicle', vehicleKey, error);
          }
        });
      }

      function getDispatcherPopupVehicleLabel(vehicleKey) {
        if (!vehicleKey) {
          return '';
        }
        const state = busMarkerStates ? busMarkerStates[vehicleKey] : null;
        const name = state && typeof state.busName === 'string' ? state.busName.trim() : '';
        if (name) {
          return name;
        }
        return `Vehicle ${vehicleKey}`.trim();
      }

      function getDispatcherPopupBlockLabel(vehicleKey) {
        if (!vehicleKey || !busBlocks) {
          return '';
        }
        const direct = typeof busBlocks[vehicleKey] === 'string' ? busBlocks[vehicleKey].trim() : '';
        if (direct) {
          return direct;
        }
        const numericKey = Number(vehicleKey);
        if (Number.isFinite(numericKey)) {
          const numericValue = typeof busBlocks[numericKey] === 'string' ? busBlocks[numericKey].trim() : '';
          if (numericValue) {
            return numericValue;
          }
        }
        return '';
      }

      function buildDispatcherPopupContent(vehicleKey) {
        const pieces = [];
        const vehicleLabel = getDispatcherPopupVehicleLabel(vehicleKey);
        const blockLabel = getDispatcherPopupBlockLabel(vehicleKey);
        if (vehicleLabel) {
          pieces.push(`<div class="dispatcher-overheight-popup__vehicle">${escapeHtml(vehicleLabel)}</div>`);
        }
        if (blockLabel) {
          const normalized = blockLabel.trim().toLowerCase();
          const needsPrefix = !normalized.startsWith('block ');
          const displayText = needsPrefix ? `Block ${blockLabel}` : blockLabel;
          pieces.push(`<div class="dispatcher-overheight-popup__block">${escapeHtml(displayText)}</div>`);
        }
        pieces.push('<div class="dispatcher-overheight-popup__content">OVERHEIGHT VEHICLE</div>');
        return pieces.join('');
      }

      function openDispatcherPopupForVehicle(vehicleKey) {
        if (!dispatcherFeaturesAllowed()) {
          return;
        }
        if (!vehicleKey && dispatcherLockState.vehicleKey) {
          vehicleKey = dispatcherLockState.vehicleKey;
        }
        if (!vehicleKey) {
          return;
        }
        const marker = markers && markers[vehicleKey];
        if (!marker) {
          dispatcherLockState.pendingPopupVehicleKey = vehicleKey;
          return;
        }
        dispatcherLockState.pendingPopupVehicleKey = null;
        suppressDispatcherPopupCloseHandling(() => {
          try {
            clearDispatcherPopupForVehicle(vehicleKey);
            const popupContent = buildDispatcherPopupContent(vehicleKey);
            if (typeof marker.bindPopup === 'function') {
              marker.bindPopup(popupContent, {
                closeButton: false,
                closeOnClick: false,
                autoClose: false,
                className: 'dispatcher-overheight-popup'
              });
            }
            if (typeof marker.openPopup === 'function') {
              marker.openPopup();
            }
            dispatcherLockState.popup = typeof marker.getPopup === 'function' ? marker.getPopup() : null;
          } catch (error) {
            console.error('Failed to open dispatcher popup for vehicle', vehicleKey, error);
          }
        });
      }

      function handleDispatcherPopupClosed(event) {
        if (!isDispatcherLockActive()) {
          return;
        }
        if (!event || !event.popup) {
          return;
        }
        if ((dispatcherLockState.popupCloseSuppressionDepth || 0) > 0) {
          return;
        }
        if (dispatcherLockState.popup && event.popup !== dispatcherLockState.popup) {
          return;
        }
        openDispatcherPopupForVehicle(dispatcherLockState.vehicleKey);
      }

      function selectNearestOverheightCandidate(currentCandidate, vehicleKey, vehicleName, lat, lon, config) {
        if (!config || !config.overheightBusIds || config.overheightBusIds.size === 0) {
          return currentCandidate;
        }
        const normalizedVehicleId = vehicleKey ? `${vehicleKey}`.trim() : '';
        const normalizedVehicleName = vehicleName ? `${vehicleName}`.trim() : '';
        const isOverheight = (normalizedVehicleId && config.overheightBusIds.has(normalizedVehicleId))
          || (normalizedVehicleName && config.overheightBusIds.has(normalizedVehicleName));
        if (!isOverheight) {
          return currentCandidate;
        }
        const distance = computeGreatCircleDistanceMeters(lat, lon, config.bridgeLat, config.bridgeLng);
        if (!Number.isFinite(distance) || distance > config.radiusMeters) {
          return currentCandidate;
        }
        if (!currentCandidate || distance < currentCandidate.distance) {
          return {
            vehicleKey,
            distance
          };
        }
        return currentCandidate;
      }

      function updateDispatcherPendingPopup() {
        if (!dispatcherLockState.pendingPopupVehicleKey) {
          return;
        }
        openDispatcherPopupForVehicle(dispatcherLockState.pendingPopupVehicleKey);
      }

      function handleDispatcherLock(candidate, config) {
        if (!dispatcherFeaturesAllowed() || !map) {
          return;
        }
        if (!candidate || !config) {
          if (!dispatcherLockState.active) {
            return;
          }
          const previousVehicleKey = dispatcherLockState.vehicleKey;
          const originalView = dispatcherLockState.originalView;
          restoreMapInteractionsForDispatcher();
          if (dispatcherLockState.circle && typeof map.removeLayer === 'function') {
            map.removeLayer(dispatcherLockState.circle);
          }
          if (previousVehicleKey) {
            clearDispatcherPopupForVehicle(previousVehicleKey);
          }
          if (originalView && originalView.center) {
            const { center, zoom } = originalView;
            const hasValidCenter = center
              && Number.isFinite(center.lat)
              && Number.isFinite(center.lng);
            const hasValidZoom = Number.isFinite(zoom);
            if (hasValidCenter) {
              try {
                if (typeof map.setView === 'function') {
                  const fallbackZoom = typeof map.getZoom === 'function' ? map.getZoom() : undefined;
                  const viewZoom = hasValidZoom ? zoom : fallbackZoom;
                  if (typeof viewZoom === 'number' && Number.isFinite(viewZoom)) {
                    map.setView([center.lat, center.lng], viewZoom, { animate: true });
                  } else if (typeof map.panTo === 'function') {
                    map.panTo([center.lat, center.lng]);
                  } else {
                    map.setView([center.lat, center.lng]);
                  }
                } else if (typeof map.panTo === 'function') {
                  map.panTo([center.lat, center.lng]);
                }
              } catch (error) {
                console.warn('Failed to restore original dispatcher map view:', error);
              }
            }
          }
          dispatcherLockState.circle = null;
          dispatcherLockState.popup = null;
          dispatcherLockState.vehicleKey = null;
          dispatcherLockState.pendingPopupVehicleKey = null;
          dispatcherLockState.active = false;
          dispatcherLockState.targetZoom = null;
          dispatcherLockState.lockedCenter = null;
          dispatcherLockState.originalView = null;
          return;
        }

        const { bridgeLat, bridgeLng, radiusMeters } = config;
        if (!Number.isFinite(bridgeLat) || !Number.isFinite(bridgeLng) || !Number.isFinite(radiusMeters) || radiusMeters <= 0) {
          return;
        }

        const circle = ensureDispatcherCircle(config);
        if (circle) {
          try {
            circle.setLatLng([bridgeLat, bridgeLng]);
            circle.setRadius(radiusMeters);
            if (map && typeof map.hasLayer === 'function' && !map.hasLayer(circle) && typeof circle.addTo === 'function') {
              circle.addTo(map);
            }
          } catch (error) {
            console.warn('Unable to update dispatcher circle:', error);
          }
        }

        const mapZoom = typeof map.getZoom === 'function' ? map.getZoom() : null;
        const targetZoom = Number.isFinite(mapZoom)
          ? Math.max(mapZoom, DISPATCHER_MIN_FOCUS_ZOOM)
          : DISPATCHER_MIN_FOCUS_ZOOM;
        dispatcherLockState.targetZoom = targetZoom;

        if (!dispatcherLockState.active) {
          disableMapInteractionsForDispatcher();
          if (!dispatcherLockState.originalView) {
            const currentCenter = typeof map.getCenter === 'function' ? map.getCenter() : null;
            const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : null;
            if (currentCenter && Number.isFinite(currentCenter.lat) && Number.isFinite(currentCenter.lng)) {
              dispatcherLockState.originalView = {
                center: { lat: currentCenter.lat, lng: currentCenter.lng },
                zoom: Number.isFinite(currentZoom) ? currentZoom : null
              };
            } else {
              dispatcherLockState.originalView = null;
            }
          }
        }

        const previousVehicleKey = dispatcherLockState.vehicleKey;
        if (previousVehicleKey && previousVehicleKey !== candidate.vehicleKey) {
          clearDispatcherPopupForVehicle(previousVehicleKey);
        }

        dispatcherLockState.active = true;
        dispatcherLockState.vehicleKey = candidate.vehicleKey;

        const currentCenter = typeof map.getCenter === 'function' ? map.getCenter() : null;
        const lockedCenter = dispatcherLockState.lockedCenter;
        const needsRecentering = !dispatcherLockState.active
          || !lockedCenter
          || !Number.isFinite(lockedCenter.lat)
          || !Number.isFinite(lockedCenter.lng)
          || Math.abs(lockedCenter.lat - bridgeLat) > 1e-6
          || Math.abs(lockedCenter.lng - bridgeLng) > 1e-6
          || !currentCenter
          || !Number.isFinite(currentCenter.lat)
          || !Number.isFinite(currentCenter.lng);
        const needsZoomIncrease = Number.isFinite(targetZoom)
          && (!Number.isFinite(mapZoom) || mapZoom < targetZoom);

        if (needsRecentering && typeof map.setView === 'function') {
          try {
            map.setView([bridgeLat, bridgeLng], targetZoom, { animate: true });
            dispatcherLockState.lockedCenter = { lat: bridgeLat, lng: bridgeLng };
          } catch (error) {
            console.warn('Failed to center map on bridge for dispatcher mode:', error);
          }
        } else if (needsZoomIncrease && typeof map.setZoom === 'function') {
          try {
            map.setZoom(targetZoom);
          } catch (error) {
            console.warn('Failed to adjust map zoom for dispatcher mode:', error);
          }
        }

        if (!needsRecentering && !dispatcherLockState.lockedCenter) {
          dispatcherLockState.lockedCenter = { lat: bridgeLat, lng: bridgeLng };
        }

        openDispatcherPopupForVehicle(candidate.vehicleKey);
      }

      function isRadarInteractiveMode() {
        return !kioskMode && !adminKioskMode;
      }

      function normalizeRadarProduct(value) {
        if (typeof value !== 'string') {
          return RADAR_DEFAULT_PRODUCT;
        }
        const key = value.trim().toLowerCase();
        return Object.prototype.hasOwnProperty.call(RADAR_PRODUCT_INFO, key) ? key : RADAR_DEFAULT_PRODUCT;
      }

      function clampRadarOpacity(value) {
        const numeric = Number.parseFloat(value);
        if (!Number.isFinite(numeric)) {
          return RADAR_DEFAULT_OPACITY;
        }
        return Math.min(RADAR_MAX_OPACITY, Math.max(RADAR_MIN_OPACITY, numeric));
      }

      function formatRadarOpacity(value) {
        const numeric = Number.parseFloat(value);
        if (!Number.isFinite(numeric)) {
          return `${Math.round(RADAR_DEFAULT_OPACITY * 100)}%`;
        }
        const percentage = Math.round(Math.min(1, Math.max(0, numeric)) * 100);
        return `${percentage}%`;
      }

      function buildRadarCacheBustKey(date = new Date()) {
        const year = date.getUTCFullYear();
        const month = String(date.getUTCMonth() + 1).padStart(2, "0");
        const day = String(date.getUTCDate()).padStart(2, "0");
        const hours = String(date.getUTCHours()).padStart(2, "0");
        const minutes = String(date.getUTCMinutes()).padStart(2, "0");
        return `${year}${month}${day}${hours}${minutes}`;
      }

      function removeCacheBustPlaceholder(template) {
        if (typeof template !== "string" || !template.includes(RADAR_CACHE_BUST_PLACEHOLDER)) {
          return template;
        }
        return template.split(RADAR_CACHE_BUST_PLACEHOLDER).join("");
      }

      function getRadarTileUrlTemplate(productKey, cacheBustKey) {
        const info = RADAR_PRODUCT_INFO[productKey] || RADAR_PRODUCT_INFO[RADAR_DEFAULT_PRODUCT];
        if (!info || typeof info.urlTemplate !== "string" || !info.urlTemplate) {
          return "";
        }
        const template = info.urlTemplate;
        const strategy = info.cacheBustingStrategy || "query";
        if (!cacheBustKey || strategy === "none") {
          return removeCacheBustPlaceholder(template);
        }
        if (strategy === "path" && template.includes(RADAR_CACHE_BUST_PLACEHOLDER)) {
          return template.split(RADAR_CACHE_BUST_PLACEHOLDER).join(cacheBustKey);
        }
        const separator = template.includes("?") ? "&" : "?";
        return `${template}${separator}t=${encodeURIComponent(cacheBustKey)}`;
      }

      function loadRadarSessionState() {
        if (!isRadarInteractiveMode()) {
          return null;
        }
        try {
          if (typeof window === "undefined" || !window.sessionStorage) {
            return null;
          }
          const stored = window.sessionStorage.getItem(RADAR_SESSION_STORAGE_KEY);
          if (!stored) {
            return null;
          }
          const parsed = JSON.parse(stored);
          if (!parsed || typeof parsed !== "object") {
            return null;
          }
          const sanitizedState = {
            product: normalizeRadarProduct(parsed.product),
            opacity: clampRadarOpacity(parsed.opacity)
          };
          if (Object.prototype.hasOwnProperty.call(parsed, "enabled")) {
            try {
              window.sessionStorage.setItem(
                RADAR_SESSION_STORAGE_KEY,
                JSON.stringify(sanitizedState)
              );
            } catch (error) {
              console.warn("Failed to sanitize radar session state:", error);
            }
          }
          return {
            enabled: false,
            product: sanitizedState.product,
            opacity: sanitizedState.opacity
          };
        } catch (error) {
          console.warn("Failed to load radar session state:", error);
          return null;
        }
      }

      function saveRadarSessionState() {
        if (!isRadarInteractiveMode()) {
          return;
        }
        try {
          if (typeof window === "undefined" || !window.sessionStorage) {
            return;
          }
          const payload = {
            product: radarProduct,
            opacity: radarOpacity
          };
          window.sessionStorage.setItem(RADAR_SESSION_STORAGE_KEY, JSON.stringify(payload));
        } catch (error) {
          console.warn("Failed to save radar session state:", error);
        }
      }

      function initializeRadarPreferences() {
        radarConfiguredProduct = normalizeRadarProduct(radarConfiguredProduct);
        radarConfiguredOpacity = clampRadarOpacity(radarConfiguredOpacity);
        radarProduct = radarConfiguredProduct;
        radarOpacity = radarConfiguredOpacity;
        radarEnabled = false;
        radarTemporarilyUnavailable = false;
        radarTileErrorCount = 0;
        radarLastFailedUrl = "";
        radarSuppressedForErrors = false;
        if (radarSuppressionTimeoutId !== null) {
          clearTimeout(radarSuppressionTimeoutId);
          radarSuppressionTimeoutId = null;
        }
        if (!radarFeaturesAllowed()) {
          radarEnabled = false;
          return;
        }
        if (adminKioskMode) {
          radarEnabled = false;
        } else if (kioskMode) {
          radarEnabled = !!showRadar;
        } else {
          const storedState = loadRadarSessionState();
          if (storedState) {
            radarEnabled = !!storedState.enabled;
            radarProduct = storedState.product;
            radarOpacity = storedState.opacity;
          }
        }
      }

      function createRadarTileLayer(productKey) {
        const normalizedProduct = normalizeRadarProduct(productKey);
        const template = getRadarTileUrlTemplate(normalizedProduct, radarCacheBustKey);
        if (!template) {
          return null;
        }
        const layer = L.tileLayer(template, {
          pane: RADAR_PANE_NAME,
          opacity: radarOpacity,
          attribution: "Radar tiles © NOAA/NWS.",
          updateWhenIdle: true,
          updateWhenZooming: true,
          keepBuffer: 0,
          crossOrigin: true
        });
        layer.on("tileload", handleRadarTileLoad);
        layer.on("tileerror", handleRadarTileError);
        return layer;
      }

      function applyRadarState() {
        if (!radarFeaturesAllowed()) {
          removeRadarLayer();
          return;
        }
        if (!map) {
          return;
        }
        const shouldDisplay = radarEnabled && !radarTemporarilyUnavailable && !radarSuppressedForErrors;
        if (!shouldDisplay) {
          removeRadarLayer();
          return;
        }
        radarProduct = normalizeRadarProduct(radarProduct);
        radarOpacity = clampRadarOpacity(radarOpacity);
        if (!radarLayer) {
          radarCacheBustKey = buildRadarCacheBustKey();
          const layer = createRadarTileLayer(radarProduct);
          if (!layer) {
            return;
          }
          radarLayer = layer;
          radarLayerProduct = radarProduct;
          radarLayer.setOpacity(radarOpacity);
          radarLayer.addTo(map);
        } else {
          if (radarLayerProduct !== radarProduct) {
            radarLayerProduct = radarProduct;
            radarCacheBustKey = buildRadarCacheBustKey();
            const template = getRadarTileUrlTemplate(radarProduct, radarCacheBustKey);
            if (template) {
              radarLayer.setUrl(template);
            }
          }
          radarLayer.setOpacity(radarOpacity);
        }
        restartRadarRefreshTimer();
      }

      function removeRadarLayer() {
        if (radarLayer) {
          radarLayer.off("tileload", handleRadarTileLoad);
          radarLayer.off("tileerror", handleRadarTileError);
          if (map) {
            map.removeLayer(radarLayer);
          }
          radarLayer = null;
        }
        radarLayerProduct = null;
        radarCacheBustKey = "";
        clearRadarRefreshTimer();
      }

      function refreshRadarTiles() {
        if (!map || !radarLayer) {
          return;
        }
        radarLayerProduct = normalizeRadarProduct(radarProduct);
        radarCacheBustKey = buildRadarCacheBustKey();
        const template = getRadarTileUrlTemplate(radarLayerProduct, radarCacheBustKey);
        if (template) {
          radarLayer.setUrl(template);
        }
      }

      function startRadarRefreshTimer() {
        if (radarRefreshTimerId !== null) {
          return;
        }
        radarRefreshTimerId = window.setInterval(() => {
          refreshRadarTiles();
        }, RADAR_REFRESH_INTERVAL_MS);
      }

      function clearRadarRefreshTimer() {
        if (radarRefreshTimerId !== null) {
          clearInterval(radarRefreshTimerId);
          radarRefreshTimerId = null;
        }
      }

      function restartRadarRefreshTimer() {
        clearRadarRefreshTimer();
        if (radarLayer && radarEnabled && !radarTemporarilyUnavailable && !radarSuppressedForErrors) {
          startRadarRefreshTimer();
        }
      }

      function handleRadarTileLoad() {
        radarTileErrorCount = 0;
      }

      function handleRadarTileError(event) {
        const tile = event && event.tile ? event.tile : null;
        const url = tile && (tile.src || tile.currentSrc) ? (tile.src || tile.currentSrc) : getRadarTileUrlTemplate(radarLayerProduct || radarProduct, radarCacheBustKey);
        if (url) {
          radarLastFailedUrl = url;
        }
        console.error("Radar tile fetch failed:", radarLastFailedUrl || "Unknown URL");
        radarTileErrorCount += 1;
        if (radarTileErrorCount < RADAR_CONSECUTIVE_ERROR_THRESHOLD) {
          return;
        }
        radarTileErrorCount = 0;
        if (isRadarInteractiveMode()) {
          radarTemporarilyUnavailable = true;
          radarEnabled = false;
          removeRadarLayer();
          saveRadarSessionState();
          refreshRadarControlsUI();
        } else {
          radarSuppressedForErrors = true;
          removeRadarLayer();
          scheduleRadarSuppressionRecovery();
        }
      }

      function scheduleRadarSuppressionRecovery() {
        if (radarSuppressionTimeoutId !== null) {
          clearTimeout(radarSuppressionTimeoutId);
        }
        radarSuppressionTimeoutId = window.setTimeout(() => {
          radarSuppressionTimeoutId = null;
          radarSuppressedForErrors = false;
          radarTileErrorCount = 0;
          if (radarEnabled) {
            applyRadarState();
          }
        }, RADAR_REFRESH_INTERVAL_MS);
      }

      function setRadarEnabled(nextEnabled) {
        const allowRadar = radarFeaturesAllowed();
        const shouldEnable = allowRadar && !!nextEnabled;
        if (shouldEnable && radarTemporarilyUnavailable) {
          refreshRadarControlsUI();
          return;
        }
        if (radarEnabled === shouldEnable) {
          refreshRadarControlsUI();
          return;
        }
        radarEnabled = shouldEnable;
        if (!radarEnabled) {
          removeRadarLayer();
          radarSuppressedForErrors = false;
          radarTileErrorCount = 0;
          radarCacheBustKey = "";
          if (radarSuppressionTimeoutId !== null) {
            clearTimeout(radarSuppressionTimeoutId);
            radarSuppressionTimeoutId = null;
          }
        }
        applyRadarState();
        if (isRadarInteractiveMode()) {
          saveRadarSessionState();
        }
        refreshRadarControlsUI();
      }

      function setRadarProduct(nextProduct) {
        if (!radarFeaturesAllowed()) {
          refreshRadarControlsUI();
          return;
        }
        const normalized = normalizeRadarProduct(nextProduct);
        const productChanged = radarProduct !== normalized;
        radarProduct = normalized;
        radarTileErrorCount = 0;
        radarLastFailedUrl = "";
        if (radarTemporarilyUnavailable && productChanged) {
          radarTemporarilyUnavailable = false;
        }
        if (radarSuppressedForErrors) {
          radarSuppressedForErrors = false;
          if (radarSuppressionTimeoutId !== null) {
            clearTimeout(radarSuppressionTimeoutId);
            radarSuppressionTimeoutId = null;
          }
        }
        if (radarEnabled && map) {
          radarCacheBustKey = buildRadarCacheBustKey();
          if (radarLayer) {
            radarLayerProduct = radarProduct;
            const template = getRadarTileUrlTemplate(radarProduct, radarCacheBustKey);
            if (template) {
              radarLayer.setUrl(template);
            }
            radarLayer.setOpacity(radarOpacity);
          } else {
            applyRadarState();
          }
          restartRadarRefreshTimer();
        }
        if (isRadarInteractiveMode()) {
          saveRadarSessionState();
        }
        refreshRadarControlsUI();
      }

      function setRadarOpacity(nextOpacity) {
        if (!radarFeaturesAllowed()) {
          refreshRadarControlsUI();
          return;
        }
        const clamped = clampRadarOpacity(nextOpacity);
        if (Math.abs(clamped - radarOpacity) < 0.0001) {
          refreshRadarControlsUI();
          return;
        }
        radarOpacity = clamped;
        if (radarLayer) {
          radarLayer.setOpacity(radarOpacity);
        }
        if (isRadarInteractiveMode()) {
          saveRadarSessionState();
        }
        refreshRadarControlsUI();
      }

      function initializeRadarControls() {
        const toggleButton = document.getElementById("radarToggleButton");
        if (toggleButton) {
          toggleButton.addEventListener("click", event => {
            event.preventDefault();
            if (toggleButton.disabled) {
              return;
            }
            setRadarEnabled(!radarEnabled);
          });
        }
        const productSelect = document.getElementById("radarProductSelect");
        if (productSelect) {
          productSelect.addEventListener("change", event => {
            setRadarProduct(event.target.value);
          });
        }
        const opacityRange = document.getElementById("radarOpacityRange");
        if (opacityRange) {
          const handleOpacityChange = event => {
            const value = Number.parseFloat(event.target.value);
            if (Number.isFinite(value)) {
              setRadarOpacity(value);
            }
          };
          opacityRange.addEventListener("input", handleOpacityChange);
          opacityRange.addEventListener("change", handleOpacityChange);
        }
        refreshRadarControlsUI();
      }

      function refreshRadarControlsUI() {
        const allowRadar = radarFeaturesAllowed();
        const toggleButton = document.getElementById("radarToggleButton");
        const isActive = allowRadar && radarEnabled && !radarTemporarilyUnavailable;
        if (toggleButton) {
          toggleButton.classList.toggle("is-active", isActive);
          toggleButton.setAttribute("aria-pressed", isActive ? "true" : "false");
          toggleButton.disabled = !allowRadar || radarTemporarilyUnavailable;
          const indicator = toggleButton.querySelector(".toggle-indicator");
          if (indicator) {
            indicator.textContent = isActive ? "On" : "Off";
          }
        }
        const productSelect = document.getElementById("radarProductSelect");
        if (productSelect) {
          productSelect.value = radarProduct;
          productSelect.disabled = !allowRadar;
        }
        const opacityRange = document.getElementById("radarOpacityRange");
        if (opacityRange) {
          opacityRange.value = radarOpacity.toFixed(2);
          opacityRange.disabled = !allowRadar;
        }
        const opacityValue = document.getElementById("radarOpacityValue");
        if (opacityValue) {
          opacityValue.textContent = formatRadarOpacity(radarOpacity);
        }
        const statusMessage = document.getElementById("radarStatusMessage");
        if (statusMessage) {
          if (radarTemporarilyUnavailable) {
            statusMessage.textContent = RADAR_UNAVAILABLE_MESSAGE;
            statusMessage.style.display = "";
          } else {
            statusMessage.textContent = "";
            statusMessage.style.display = "none";
          }
        }
      }

      const ROUTE_LAYER_BASE_OPTIONS = Object.freeze({
        updateWhenZooming: true,
        updateWhenIdle: true,
        interactive: false
      });
      let sharedRouteRenderer = null;
      let routePaneName = 'overlayPane';
      let lastRenderedLegendRoutes = [];
      let lastRenderedLegendSignature = '';

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

      const params = new URLSearchParams(window.location.search);
      const kioskParam = params.get('kioskMode');
      if (kioskParam !== null) {
        kioskMode = kioskParam.toLowerCase() === 'true';
      }
      const adminKioskParam = params.get('adminKioskMode');
      if (adminKioskParam !== null) {
        adminKioskMode = adminKioskParam.toLowerCase() === 'true';
      }
      updateKioskExperienceState();
      ensurePanelsHiddenForKioskExperience();
      updateKioskStatusMessage({ known: false, hasActiveVehicles: false });
      const adminParam = params.get('adminMode');
      if (adminParam !== null) {
        adminModeExplicitlySet = true;
        adminMode = adminParam.toLowerCase() === 'true';
      }
      const passParam = params.get('pass');
      if (typeof passParam === 'string') {
        urlAdminPassword = passParam.trim();
      }

      const showRadarParam = params.get('showRadar');
      if (showRadarParam !== null) {
        showRadar = showRadarParam.toLowerCase() === 'true';
      }
      const radarProductParam = params.get('radarProduct');
      if (radarProductParam) {
        radarConfiguredProduct = normalizeRadarProduct(radarProductParam);
      }
      const radarOpacityParam = params.get('radarOpacity');
      if (radarOpacityParam !== null) {
        const parsedOpacity = Number.parseFloat(radarOpacityParam);
        if (Number.isFinite(parsedOpacity)) {
          radarConfiguredOpacity = clampRadarOpacity(parsedOpacity);
        }
      }

      initializeRadarPreferences();

      const outOfServiceRouteColor = '#000000';

      const TRANSLOC_SNAPSHOT_ENDPOINT = '/v1/testmap/transloc';
      const TRANSLOC_SNAPSHOT_TTL_MS = 2000;
      const PULSEPOINT_ENDPOINT = '/v1/testmap/pulsepoint';
      const INCIDENT_REFRESH_INTERVAL_MS = 45000;
      const FALLBACK_INCIDENT_ICON_SIZE = 36;
      const INCIDENT_ICON_SCALE = 0.25;
      const DEFAULT_ICON_SCALE = Number.isFinite(INCIDENT_ICON_SCALE) && INCIDENT_ICON_SCALE > 0
        ? INCIDENT_ICON_SCALE
        : 1;
      const DEFAULT_SCALED_INCIDENT_ICON_HEIGHT = Math.max(1, Math.round(FALLBACK_INCIDENT_ICON_SIZE * DEFAULT_ICON_SCALE));
      const HALO_MIN_RADIUS_PX = 16;
      const HALO_MAX_RADIUS_PX = 64;
      const HALO_BASE_OPACITY = 0.5;
      const HALO_DURATION_MS = 1600;
      const HALO_COLOR_DEFAULT = '#FF5A3C';
      const HALO_COLOR_RGB = (() => {
        if (typeof HALO_COLOR_DEFAULT !== 'string') return '255, 90, 60';
        const hex = HALO_COLOR_DEFAULT.replace(/[^0-9a-f]/gi, '').trim();
        if (hex.length === 3) {
          const r = parseInt(hex[0] + hex[0], 16);
          const g = parseInt(hex[1] + hex[1], 16);
          const b = parseInt(hex[2] + hex[2], 16);
          if ([r, g, b].every(value => Number.isFinite(value))) {
            return `${r}, ${g}, ${b}`;
          }
        } else if (hex.length === 6) {
          const r = parseInt(hex.slice(0, 2), 16);
          const g = parseInt(hex.slice(2, 4), 16);
          const b = parseInt(hex.slice(4, 6), 16);
          if ([r, g, b].every(value => Number.isFinite(value))) {
            return `${r}, ${g}, ${b}`;
          }
        }
        return '255, 90, 60';
      })();
      const INCIDENT_HALO_ANIMATED_LIMIT = 60;
      // To tweak the centroid offset without editing this file, set
      // window.HeadwayGuardIncidentMarkerOffset = { xFactor: <number>, yFactor: <number> }
      // before this script runs. Factors are multiplied by the scaled marker height.
      const markerCentroidOverride = (typeof window !== 'undefined' && window.HeadwayGuardIncidentMarkerOffset)
        ? window.HeadwayGuardIncidentMarkerOffset
        : null;
      const MARKER_CENTROID_OFFSET_Y_FACTOR = typeof markerCentroidOverride?.yFactor === 'number'
        ? markerCentroidOverride.yFactor
        : -0.18;
      const MARKER_CENTROID_OFFSET_X_FACTOR = typeof markerCentroidOverride?.xFactor === 'number'
        ? markerCentroidOverride.xFactor
        : 0;
      const INCIDENTS_ALLOWED_AGENCY_NAMES = ['University of Virginia', 'University of Virginia Health'];
      const CAT_ALLOWED_AGENCY_NAMES = INCIDENTS_ALLOWED_AGENCY_NAMES;
      const TRAINS_ENDPOINT = '/v1/testmap/trains';
      const TRAIN_POLL_INTERVAL_MS = 30000;
      const TRAIN_TARGET_STATION_CODE = '';
      const TRAIN_CARDINAL_HEADING_DEGREES = Object.freeze({
        N: 0,
        NORTH: 0,
        NORTHBOUND: 0,
        NE: 45,
        NORTHEAST: 45,
        NORTHEASTBOUND: 45,
        E: 90,
        EAST: 90,
        EASTBOUND: 90,
        SE: 135,
        SOUTHEAST: 135,
        SOUTHEASTBOUND: 135,
        S: 180,
        SOUTH: 180,
        SOUTHBOUND: 180,
        SW: 225,
        SOUTHWEST: 225,
        SOUTHWESTBOUND: 225,
        W: 270,
        WEST: 270,
        WESTBOUND: 270,
        NW: 315,
        NORTHWEST: 315,
        NORTHWESTBOUND: 315
      });
      const CAT_ROUTES_ENDPOINT = '/v1/testmap/cat/routes';
      const CAT_STOPS_ENDPOINT = '/v1/testmap/cat/stops';
      const CAT_PATTERNS_ENDPOINT = '/v1/testmap/cat/patterns';
      const CAT_VEHICLES_ENDPOINT = '/v1/testmap/cat/vehicles';
      const CAT_SERVICE_ALERTS_ENDPOINT = '/v1/testmap/cat/service-alerts';
      const CAT_STOP_ETAS_ENDPOINT = '/v1/testmap/cat/stop-etas';
      const RIDESYSTEMS_CLIENTS_ENDPOINT = '/v1/testmap/ridesystems/clients';
      const CAT_VEHICLE_FETCH_INTERVAL_MS = 5000;
      const CAT_METADATA_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
      const CAT_SERVICE_ALERT_REFRESH_INTERVAL_MS = 60000;
      const CAT_SERVICE_ALERT_UNAVAILABLE_MESSAGE = 'CAT service alerts are unavailable.';
      const CAT_VEHICLE_MARKER_DEFAULT_COLOR = '#0f172a';
      const CAT_VEHICLE_MARKER_MIN_LABEL = 'CAT';
      const CAT_MAX_TOOLTIP_ETAS = 3;
      const CAT_VEHICLE_ETA_CACHE_TTL_MS = 30000;
      const CAT_STOP_ETA_CACHE_TTL_MS = 30000;
      const ONDEMAND_ENDPOINT = '/api/ondemand';
      const ONDEMAND_MARKER_PREFIX = 'ondemand:';
      const ONDEMAND_MARKER_DEFAULT_COLOR = '#ec4899';
      const ONDEMAND_REFRESH_INTERVAL_MS = 5000;
      const ONDEMAND_STOP_ROUTE_PREFIX = 'ondemand-stop:';
      const ONDEMAND_STOP_TOOLTIP_CLASS = 'ondemand-stop-tooltip';

      let map;
      let markers = {};
      let busMarkerStates = {};
      const INITIAL_MAP_VIEW = Object.freeze({
          center: [38.03799212281404, -78.50981502838886],
          zoom: 15
      });
      const dispatcherMessageSource = 'dispatcher';
      const dispatcherMessageTypes = Object.freeze({
          focusBus: 'dispatcher:focusBus',
          centerMap: 'dispatcher:centerMap'
      });
      const dispatcherResponseSource = 'testmap';
      const dispatcherResponseTypes = Object.freeze({
          vehicleUnavailable: 'testmap:vehicleUnavailable'
      });
      const vehicleFollowState = {
          active: false,
          vehicleId: '',
          desiredZoom: null,
          pendingInitialCenter: false,
          displayLabel: '',
          visibilityGraceDeadline: 0
      };
      let vehicleFollowInteractionHandlersBound = false;
      const VEHICLE_FOLLOW_CENTER_EPSILON = 0.00001;
      const VEHICLE_FOLLOW_VISIBILITY_GRACE_MS = 4000;

      function normalizeDispatcherVehicleKey(value) {
          if (value == null) {
              return '';
          }
          const text = `${value}`.trim();
          if (!text || text === '—') {
              return '';
          }
          return text;
      }

      function normalizeDispatcherIdentifierForComparison(value) {
          const key = normalizeDispatcherVehicleKey(value);
          if (!key) {
              return '';
          }
          return key.replace(/[\s_-]+/g, '').toLowerCase();
      }

      function findVehicleKeyByIdentifier(identifier) {
          const normalized = normalizeDispatcherIdentifierForComparison(identifier);
          if (!normalized || !busMarkerStates) {
              return '';
          }
          let partialMatch = '';
          const identifierDigits = normalized.replace(/[^0-9]/g, '');
          for (const [vehicleKey, state] of Object.entries(busMarkerStates)) {
              if (!state) {
                  continue;
              }
              const busName = normalizeDispatcherVehicleKey(state.busName);
              if (!busName) {
                  continue;
              }
              const comparison = normalizeDispatcherIdentifierForComparison(busName);
              if (!comparison) {
                  continue;
              }
              if (comparison === normalized) {
                  return vehicleKey;
              }
              if (!partialMatch) {
                  const comparisonDigits = comparison.replace(/[^0-9]/g, '');
                  if (identifierDigits && identifierDigits.length > 0 && identifierDigits === comparisonDigits) {
                      partialMatch = vehicleKey;
                  } else if (!identifierDigits && (comparison.includes(normalized) || normalized.includes(comparison))) {
                      partialMatch = vehicleKey;
                  }
              }
          }
          return partialMatch;
      }

      function resolveDispatcherVehicleKey(vehicleId, fallbackIdentifiers = []) {
          const normalizedVehicleId = normalizeDispatcherVehicleKey(vehicleId);
          const identifiers = [];
          if (normalizedVehicleId) {
              identifiers.push(normalizedVehicleId);
          }
          if (Array.isArray(fallbackIdentifiers)) {
              for (const value of fallbackIdentifiers) {
                  const normalized = normalizeDispatcherVehicleKey(value);
                  if (normalized) {
                      identifiers.push(normalized);
                  }
              }
          }
          if (identifiers.length === 0) {
              return { vehicleKey: '', displayLabel: '' };
          }
          const uniqueIdentifiers = Array.from(new Set(identifiers));
          for (const candidate of uniqueIdentifiers) {
              if ((markers && markers[candidate]) || (busMarkerStates && busMarkerStates[candidate])) {
                  return { vehicleKey: candidate, displayLabel: identifiers[0] || candidate, resolvedFromFallback: false };
              }
          }
          for (const candidate of uniqueIdentifiers) {
              const matchedKey = findVehicleKeyByIdentifier(candidate);
              if (matchedKey) {
                  return { vehicleKey: matchedKey, displayLabel: candidate, resolvedFromFallback: false };
              }
          }
          const fallbackLabel = identifiers[0] || '';
          const fallbackKey = normalizeDispatcherVehicleKey(normalizedVehicleId) || normalizeDispatcherVehicleKey(fallbackLabel);
          return {
              vehicleKey: fallbackKey,
              displayLabel: fallbackLabel || fallbackKey || '',
              resolvedFromFallback: true
          };
      }

      function getVehicleDisplayName(vehicleId) {
          const normalizedKey = normalizeDispatcherVehicleKey(vehicleId);
          if (!normalizedKey) {
              return '';
          }
          const name = busMarkerStates?.[normalizedKey]?.busName;
          if (typeof name === 'string') {
              const trimmed = name.trim();
              if (trimmed) {
                  return trimmed;
              }
          }
          return normalizedKey;
      }

      function notifyDispatcherVehicleUnavailable(details = {}) {
          if (typeof window === 'undefined' || !window.parent || window.parent === window) {
              return;
          }
          const payload = {
              source: dispatcherResponseSource,
              type: dispatcherResponseTypes.vehicleUnavailable
          };
          const labelCandidates = [];
          if (details && typeof details === 'object') {
              const normalizedVehicleId = normalizeDispatcherVehicleKey(details.vehicleId);
              if (normalizedVehicleId) {
                  payload.vehicleId = normalizedVehicleId;
                  labelCandidates.push(normalizedVehicleId);
              }
              const normalizedBus = normalizeDispatcherVehicleKey(details.bus);
              if (normalizedBus) {
                  payload.bus = normalizedBus;
                  labelCandidates.push(normalizedBus);
              }
              if (typeof details.label === 'string') {
                  const trimmedLabel = details.label.trim();
                  if (trimmedLabel) {
                      payload.label = trimmedLabel;
                      labelCandidates.unshift(trimmedLabel);
                  }
              }
              if (typeof details.reason === 'string') {
                  const trimmedReason = details.reason.trim();
                  if (trimmedReason) {
                      payload.reason = trimmedReason;
                  }
              }
          }
          if (!payload.label && labelCandidates.length > 0) {
              payload.label = labelCandidates[0];
          }
          try {
              window.parent.postMessage(payload, window.location.origin);
          } catch (error) {
              console.warn('Failed to notify dispatcher about unavailable vehicle:', error);
          }
      }

      function showVehicleFollowToast(message) {
          const toast = getCachedElementById('mapToast');
          if (!toast) {
              return;
          }
          toast.textContent = message;
          toast.setAttribute('aria-hidden', 'false');
          toast.classList.add('is-visible');
      }

      function hideVehicleFollowToast() {
          const toast = getCachedElementById('mapToast');
          if (!toast) {
              return;
          }
          toast.classList.remove('is-visible');
          toast.setAttribute('aria-hidden', 'true');
      }

      function isVehicleFollowVisibilityGraceActive() {
          const deadline = vehicleFollowState.visibilityGraceDeadline;
          if (!Number.isFinite(deadline) || deadline <= 0) {
              return false;
          }
          return Date.now() < deadline;
      }

      function isVehicleMarkerReady(vehicleId) {
          const normalizedKey = normalizeDispatcherVehicleKey(vehicleId);
          if (!normalizedKey) {
              return false;
          }
          const marker = markers && markers[normalizedKey];
          if (!marker || typeof marker.getLatLng !== 'function') {
              return false;
          }
          const latLng = marker.getLatLng();
          if (!latLng) {
              return false;
          }
          const { lat, lng } = latLng;
          return Number.isFinite(lat) && Number.isFinite(lng);
      }

      function updateVehicleFollowToast() {
          if (!vehicleFollowState.active) {
              hideVehicleFollowToast();
              return;
          }
          const vehicleName = getVehicleDisplayName(vehicleFollowState.vehicleId);
          const fallbackLabel = normalizeDispatcherVehicleKey(vehicleFollowState.displayLabel || vehicleFollowState.vehicleId);
          const displayName = vehicleName || fallbackLabel;
          const markerReady = isVehicleMarkerReady(vehicleFollowState.vehicleId);
          let message;
          if (markerReady) {
              message = displayName ? `Following ${displayName}` : 'Following vehicle';
          } else {
              message = displayName
                  ? `Waiting for ${displayName} to appear on the map`
                  : 'Waiting for vehicle to appear on the map';
          }
          showVehicleFollowToast(message);
      }

      function stopFollowingVehicle() {
          if (!vehicleFollowState.active) {
              hideVehicleFollowToast();
              return;
          }
          vehicleFollowState.active = false;
          vehicleFollowState.vehicleId = '';
          vehicleFollowState.desiredZoom = null;
          vehicleFollowState.pendingInitialCenter = false;
          vehicleFollowState.displayLabel = '';
          vehicleFollowState.visibilityGraceDeadline = 0;
          hideVehicleFollowToast();
      }

      function updateVehicleFollowPosition(force = false) {
          if (!vehicleFollowState.active || !map) {
              return;
          }
          const vehicleId = vehicleFollowState.vehicleId;
          if (!vehicleId) {
              stopFollowingVehicle();
              return;
          }
          const marker = markers && markers[vehicleId];
          if (!marker || typeof marker.getLatLng !== 'function') {
              return;
          }
          const latLng = marker.getLatLng();
          if (!latLng) {
              return;
          }

          if (vehicleFollowState.pendingInitialCenter) {
              vehicleFollowState.pendingInitialCenter = false;
              if (Number.isFinite(vehicleFollowState.desiredZoom)) {
                  if (typeof map.flyTo === 'function') {
                      map.flyTo(latLng, vehicleFollowState.desiredZoom, { animate: true, duration: 0.75, easeLinearity: 0.25 });
                  } else if (typeof map.setView === 'function') {
                      map.setView([latLng.lat, latLng.lng], vehicleFollowState.desiredZoom);
                  } else if (typeof map.panTo === 'function') {
                      map.panTo(latLng);
                  }
              } else if (typeof map.panTo === 'function') {
                  map.panTo(latLng);
              } else if (typeof map.setView === 'function') {
                  const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : undefined;
                  if (currentZoom !== undefined) {
                      map.setView([latLng.lat, latLng.lng], currentZoom);
                  } else {
                      map.setView([latLng.lat, latLng.lng]);
                  }
              }
              return;
          }

          const center = typeof map.getCenter === 'function' ? map.getCenter() : null;
          let needsPan = force;
          if (!needsPan) {
              if (!center) {
                  needsPan = true;
              } else {
                  const deltaLat = Math.abs(center.lat - latLng.lat);
                  const deltaLng = Math.abs(center.lng - latLng.lng);
                  needsPan = deltaLat > VEHICLE_FOLLOW_CENTER_EPSILON || deltaLng > VEHICLE_FOLLOW_CENTER_EPSILON;
              }
          }

          if (!needsPan) {
              return;
          }

          if (typeof map.panTo === 'function') {
              map.panTo(latLng, { animate: true });
          } else if (typeof map.setView === 'function') {
              const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : undefined;
              if (currentZoom !== undefined) {
                  map.setView([latLng.lat, latLng.lng], currentZoom);
              } else {
                  map.setView([latLng.lat, latLng.lng]);
              }
          }
      }

      function startVehicleFollow(vehicleId, options = {}) {
          if (!map) {
              return;
          }
          const normalizedKey = normalizeDispatcherVehicleKey(vehicleId);
          if (!normalizedKey) {
              stopFollowingVehicle();
              return;
          }
          vehicleFollowState.active = true;
          vehicleFollowState.vehicleId = normalizedKey;
          const desiredZoom = Number.isFinite(options.zoom) ? options.zoom : null;
          vehicleFollowState.desiredZoom = desiredZoom;
          vehicleFollowState.pendingInitialCenter = !options.immediate;
          const displayLabelCandidate = typeof options.displayLabel === 'string' ? options.displayLabel : '';
          const normalizedLabel = displayLabelCandidate.trim();
          vehicleFollowState.displayLabel = normalizedLabel || normalizedKey;
          if (VEHICLE_FOLLOW_VISIBILITY_GRACE_MS > 0) {
              const now = Date.now();
              vehicleFollowState.visibilityGraceDeadline = Number.isFinite(now)
                  ? now + VEHICLE_FOLLOW_VISIBILITY_GRACE_MS
                  : VEHICLE_FOLLOW_VISIBILITY_GRACE_MS;
          } else {
              vehicleFollowState.visibilityGraceDeadline = 0;
          }
          updateVehicleFollowToast();
          if (options.forcePan) {
              updateVehicleFollowPosition(true);
          }
      }

      function maintainVehicleFollowAfterUpdate() {
          if (!vehicleFollowState.active) {
              hideVehicleFollowToast();
              return;
          }
          updateVehicleFollowToast();
          updateVehicleFollowPosition(false);
      }

      function registerVehicleFollowInteractionHandlers() {
          if (vehicleFollowInteractionHandlersBound) {
              return;
          }
          if (!map || typeof map.getContainer !== 'function') {
              return;
          }
          const container = map.getContainer();
          if (!container) {
              return;
          }
          const handler = () => {
              stopFollowingVehicle();
          };
          ['mousedown', 'touchstart', 'wheel', 'click', 'keydown'].forEach(eventName => {
              container.addEventListener(eventName, handler, { passive: true });
          });
          vehicleFollowInteractionHandlersBound = true;
      }

      function focusDispatcherVehicle(vehicleId, details = {}) {
          if (!map) {
              return false;
          }
          const fallbackIdentifiers = [];
          if (details && typeof details === 'object') {
              if (details.bus) {
                  fallbackIdentifiers.push(details.bus);
              }
              if (details.label) {
                  fallbackIdentifiers.push(details.label);
              }
          }
          const resolution = resolveDispatcherVehicleKey(vehicleId, fallbackIdentifiers);
          const candidateLabel = typeof resolution.displayLabel === 'string' && resolution.displayLabel.trim().length > 0
              ? resolution.displayLabel.trim()
              : '';
          const fallbackLabel = typeof details.label === 'string' ? details.label.trim() : '';
          const busLabel = typeof details.bus === 'string' ? details.bus.trim() : '';
          const normalizedKey = normalizeDispatcherVehicleKey(resolution.vehicleKey);
          const fallbackIdentifier = normalizeDispatcherVehicleKey(vehicleId);
          const displayLabel = candidateLabel || fallbackLabel || busLabel || normalizedKey || fallbackIdentifier;
          if (!normalizedKey) {
              stopFollowingVehicle();
              notifyDispatcherVehicleUnavailable({
                  vehicleId: fallbackIdentifier,
                  bus: busLabel,
                  label: displayLabel,
                  reason: 'unresolved'
              });
              return false;
          }
          const marker = markers && markers[normalizedKey];
          if (!marker || typeof marker.getLatLng !== 'function') {
              notifyDispatcherVehicleUnavailable({
                  vehicleId: normalizedKey,
                  bus: busLabel,
                  label: displayLabel,
                  reason: 'not_visible'
              });
              startVehicleFollow(normalizedKey, { zoom: 17, immediate: false, forcePan: true, displayLabel });
              return false;
          }
          const latLng = marker.getLatLng();
          if (!latLng) {
              notifyDispatcherVehicleUnavailable({
                  vehicleId: normalizedKey,
                  bus: busLabel,
                  label: displayLabel,
                  reason: 'not_visible'
              });
              startVehicleFollow(normalizedKey, { zoom: 17, immediate: false, forcePan: true, displayLabel });
              return false;
          }
          const { lat, lng } = latLng;
          if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
              notifyDispatcherVehicleUnavailable({
                  vehicleId: normalizedKey,
                  bus: busLabel,
                  label: displayLabel,
                  reason: 'not_visible'
              });
              startVehicleFollow(normalizedKey, { zoom: 17, immediate: false, forcePan: true, displayLabel });
              return false;
          }
          try {
              const targetLatLng = L.latLng(lat, lng);
              const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : null;
              const desiredZoom = Number.isFinite(currentZoom) ? Math.max(currentZoom, 17) : 17;
              if (typeof map.flyTo === 'function') {
                  map.flyTo(targetLatLng, desiredZoom, { animate: true, duration: 0.75, easeLinearity: 0.25 });
              } else if (typeof map.panTo === 'function') {
                  map.panTo(targetLatLng);
              } else if (typeof map.setView === 'function') {
                  map.setView([lat, lng], desiredZoom);
              }
              startVehicleFollow(normalizedKey, { zoom: desiredZoom, immediate: true, displayLabel });
              return true;
          } catch (error) {
              console.error('Failed to focus dispatcher vehicle on map:', error);
              notifyDispatcherVehicleUnavailable({
                  vehicleId: normalizedKey,
                  bus: busLabel,
                  label: displayLabel,
                  reason: 'error'
              });
              startVehicleFollow(normalizedKey, { zoom: 17, immediate: false, forcePan: true, displayLabel });
              return false;
          }
      }

      function centerDispatcherMapOnRoutes() {
          stopFollowingVehicle();
          if (!map) {
              return false;
          }
          if (allRouteBounds && typeof map.fitBounds === 'function') {
              try {
                  map.fitBounds(allRouteBounds, { padding: [20, 20] });
                  return true;
              } catch (error) {
                  console.warn('Failed to fit all route bounds for dispatcher:', error);
              }
          }
          if (typeof map.setView === 'function') {
              map.setView(INITIAL_MAP_VIEW.center, INITIAL_MAP_VIEW.zoom);
              return true;
          }
          return false;
      }

      function centerMapOnRoutes() {
          return centerDispatcherMapOnRoutes();
      }

      function handleDispatcherMessage(event) {
          if (!event || !event.data || typeof event.data !== 'object') {
              return;
          }
          if (event.origin && event.origin !== window.location.origin) {
              return;
          }
          const { source, type, vehicleId, bus, label } = event.data;
          if (source !== dispatcherMessageSource) {
              return;
          }
          if (type === dispatcherMessageTypes.focusBus) {
              focusDispatcherVehicle(vehicleId, { bus, label });
          } else if (type === dispatcherMessageTypes.centerMap) {
              centerDispatcherMapOnRoutes();
          }
      }

      if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
          window.addEventListener('message', handleDispatcherMessage);
      }
      const trainsFeature = {
          markers: {},
          markerStates: {},
          nameBubbles: {},
          visible: false,
          fetchPromise: null,
          module: null,
          loadPromise: null,
          pollIntervalId: null
      };
      const PLANE_DEPENDENCY_SCRIPTS = Object.freeze([
          '/plane_globals.js',
          '/markers.js',
          '/planeObject.js',
          '/planes_integration.js'
      ]);
      const planesFeature = {
          visible: false,
          module: null,
          loadPromise: null,
          dependenciesPromise: null
      };

      function loadPlaneDependencies() {
          if (planesFeature.dependenciesPromise) {
              return planesFeature.dependenciesPromise;
          }
          const promise = PLANE_DEPENDENCY_SCRIPTS.reduce(
              (chain, scriptUrl) => chain.then(() => loadScriptOnce(scriptUrl)),
              Promise.resolve()
          )
              .then(() => {
                  applyPlaneStyleOptions();
              })
              .catch(error => {
                  planesFeature.dependenciesPromise = null;
                  throw error;
              });
          planesFeature.dependenciesPromise = promise;
          return promise;
      }

      function ensurePlanesFeatureLoaded() {
          if (planesFeature.module) {
              return Promise.resolve(planesFeature.module);
          }
          if (planesFeature.loadPromise) {
              return planesFeature.loadPromise;
          }
          const promise = loadPlaneDependencies()
              .then(() => loadScriptOnce('/testmap-planes.js'))
              .then(() => {
                  if (typeof window.initializePlanesFeature !== 'function') {
                      throw new Error('Planes feature module is unavailable');
                  }
                  const module = window.initializePlanesFeature({
                      getMap: () => map,
                      getPlaneLayer: () => window.PlaneLayer,
                      updateToggleButton: updateAircraftToggleButton,
                      onVisibilityChange(visible) {
                          planesFeature.visible = !!visible;
                          updateAircraftToggleButton();
                      },
                      initialVisibility: planesFeature.visible
                  });
                  planesFeature.module = module;
                  return module;
              })
              .catch(error => {
                  console.error('Failed to initialize aircraft feature:', error);
                  planesFeature.loadPromise = null;
                  throw error;
              });
          planesFeature.loadPromise = promise;
          return promise;
      }

      function ensureTrainsFeatureLoaded() {
          if (trainsFeature.module) {
              return Promise.resolve(trainsFeature.module);
          }
          if (trainsFeature.loadPromise) {
              return trainsFeature.loadPromise;
          }
          const promise = loadScriptOnce('/testmap-trains.js')
              .then(() => {
                  if (typeof window.initializeTrainsFeature !== 'function') {
                      throw new Error('Trains feature module is unavailable');
                  }
                  const module = window.initializeTrainsFeature({
                      getMap: () => map,
                      state: trainsFeature,
                      adminFeaturesAllowed: trainsFeatureAllowed,
                      updateToggleButton: updateTrainToggleButton,
                      onVisibilityChange(visible) {
                          trainsFeature.visible = !!visible;
                          updateTrainToggleButton();
                          if (trainsFeature.visible) {
                              startTrainPolling().catch(error => console.error('Failed to refresh trains:', error));
                          } else {
                              stopTrainPolling();
                          }
                      },
                      onFetchPromiseChange(promise) {
                          trainsFeature.fetchPromise = promise || null;
                      },
                      TRAINS_ENDPOINT,
                      TRAIN_TARGET_STATION_CODE
                  });
                  trainsFeature.module = module;
                  return module;
              })
              .catch(error => {
                  console.error('Failed to initialize trains feature:', error);
                  trainsFeature.loadPromise = null;
                  throw error;
              });
          trainsFeature.loadPromise = promise;
          return promise;
      }

      function setPlanesVisibility(visible) {
          const desiredVisibility = !!visible;
          const allowPlanes = adminFeaturesAllowed();
          const effectiveVisibility = allowPlanes && desiredVisibility;
          if (!effectiveVisibility && !planesFeature.module) {
              planesFeature.visible = false;
              updateAircraftToggleButton();
              return Promise.resolve();
          }
          planesFeature.visible = effectiveVisibility;
          updateAircraftToggleButton();
          return ensurePlanesFeatureLoaded()
              .then(module => {
                  if (module && typeof module.setVisibility === 'function') {
                      return module.setVisibility(effectiveVisibility);
                  }
                  return undefined;
              })
              .catch(error => {
                  console.error('Error setting aircraft visibility:', error);
                  planesFeature.visible = false;
                  updateAircraftToggleButton();
              });
      }

      function toggleAircraftVisibility() {
          setPlanesVisibility(!planesFeature.visible);
      }

      function stopTrainPolling() {
          if (trainsFeature.pollIntervalId !== null) {
              clearInterval(trainsFeature.pollIntervalId);
              trainsFeature.pollIntervalId = null;
          }
      }

      function startTrainPolling() {
          if (!trainsFeatureAllowed() || !trainsFeature.visible) {
              stopTrainPolling();
              return Promise.resolve();
          }
          if (trainsFeature.pollIntervalId !== null) {
              return trainsFeature.fetchPromise || Promise.resolve();
          }
          const runFetch = () => {
              fetchTrains().catch(error => console.error('Failed to fetch trains:', error));
          };
          if (typeof window === 'undefined' || typeof window.setInterval !== 'function') {
              return fetchTrains();
          }
          const initialFetch = fetchTrains();
          trainsFeature.pollIntervalId = window.setInterval(runFetch, TRAIN_POLL_INTERVAL_MS);
          return initialFetch;
      }

      function setTrainsVisibility(visible) {
          const desiredVisibility = !!visible;
          const allowTrains = trainsFeatureAllowed();
          const effectiveVisibility = allowTrains && desiredVisibility;
          if (!effectiveVisibility && !trainsFeature.module) {
              trainsFeature.visible = false;
              trainsFeature.markers = {};
              trainsFeature.markerStates = {};
              trainsFeature.nameBubbles = {};
              updateTrainToggleButton();
              stopTrainPolling();
              return Promise.resolve();
          }
          trainsFeature.visible = effectiveVisibility;
          updateTrainToggleButton();
          return ensureTrainsFeatureLoaded()
              .then(module => {
                  if (module && typeof module.setVisibility === 'function') {
                      return module.setVisibility(effectiveVisibility);
                  }
                  return undefined;
              })
              .then(() => {
                  if (effectiveVisibility) {
                      return startTrainPolling();
                  }
                  stopTrainPolling();
                  return undefined;
              })
              .catch(error => {
                  console.error('Error setting train visibility:', error);
                  trainsFeature.visible = false;
                  updateTrainToggleButton();
                  stopTrainPolling();
              });
      }

      function toggleTrainsVisibility() {
          setTrainsVisibility(!trainsFeature.visible);
      }

      function updateTrainMarkersVisibility() {
          if (!trainsFeature.module || typeof trainsFeature.module.updateTrainMarkersVisibility !== 'function') {
              return Promise.resolve();
          }
          try {
              const result = trainsFeature.module.updateTrainMarkersVisibility();
              return result || Promise.resolve();
          } catch (error) {
              return Promise.reject(error);
          }
      }

      function fetchTrains() {
          if (!trainsFeature.visible) {
              return Promise.resolve();
          }
          if (!trainsFeature.module) {
              return ensureTrainsFeatureLoaded()
                  .then(module => {
                      if (module && typeof module.fetchTrains === 'function') {
                          return module.fetchTrains();
                      }
                      return undefined;
                  });
          }
          if (typeof trainsFeature.module.fetchTrains === 'function') {
              return trainsFeature.module.fetchTrains();
          }
          return Promise.resolve();
      }

      function clearAllTrainMarkers() {
          if (trainsFeature.module && typeof trainsFeature.module.clearAllMarkers === 'function') {
              try {
                  trainsFeature.module.clearAllMarkers();
              } catch (error) {
                  console.error('Error clearing train markers:', error);
              }
          }
          trainsFeature.markers = {};
          trainsFeature.markerStates = {};
          trainsFeature.nameBubbles = {};
      }
      const vehicleHeadingCache = new Map();
      let vehicleHeadingCachePromise = null;
      const VEHICLE_HEADING_CACHE_ENDPOINT = '/v1/vehicle_headings';
      let pendingBusVisualUpdates = new Map();
      let busMarkerVisualUpdateFrame = null;
      let selectedVehicleId = null;
      let markerScaleUpdateFrame = null;
      let pendingMarkerScaleMetrics = null;
      let textMeasurementCanvas = null;

      let agencies = [];
      let baseURL = '';
      let includeStaleVehicles = false;
      let onDemandVehiclesEnabled = false;
      let onDemandStopsEnabled = true;
      let onDemandPollingTimerId = null;
      let onDemandPollingPausedForVisibility = false;
      let onDemandFetchPromise = null;
      const onDemandVehicleColorMap = new Map();
      let onDemandStopDataCache = [];
      const onDemandStopMarkerCache = new Map();
      let onDemandStopMarkers = [];
      const ADMIN_KIOSK_UVA_HEALTH_NAME = 'University of Virginia Health';
      const ADMIN_KIOSK_UVA_HEALTH_START_MINUTES = 2 * 60 + 30;
      const ADMIN_KIOSK_UVA_HEALTH_END_MINUTES = 4 * 60 + 30;
      const ADMIN_KIOSK_UVA_NAME = 'University of Virginia';
      const MS_PER_MINUTE = 60 * 1000;
      const MS_PER_DAY = 24 * 60 * MS_PER_MINUTE;
      const ADMIN_KIOSK_SCHEDULE_MIN_DELAY_MS = 1000;
      const ADMIN_KIOSK_SCHEDULE_MAX_DELAY_MS = 30 * 60 * 1000;
      const ADMIN_KIOSK_UVA_HEALTH_START_MS = ADMIN_KIOSK_UVA_HEALTH_START_MINUTES * MS_PER_MINUTE;
      const ADMIN_KIOSK_UVA_HEALTH_END_MS = ADMIN_KIOSK_UVA_HEALTH_END_MINUTES * MS_PER_MINUTE;
      const ADMIN_KIOSK_ONDEMAND_START_MINUTES = 19 * 60 + 30;
      const ADMIN_KIOSK_ONDEMAND_END_MINUTES = 5 * 60;
      const ADMIN_KIOSK_ONDEMAND_START_MS = ADMIN_KIOSK_ONDEMAND_START_MINUTES * MS_PER_MINUTE;
      const ADMIN_KIOSK_ONDEMAND_END_MS = ADMIN_KIOSK_ONDEMAND_END_MINUTES * MS_PER_MINUTE;

      let adminKioskOnDemandTimerId = null;

      function shouldForceAdminKioskUvaHealth() {
        if (!adminKioskMode) {
          return false;
        }
        try {
          const now = new Date();
          if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
            return false;
          }
          const hours = typeof now.getHours === 'function' ? now.getHours() : NaN;
          const minutes = typeof now.getMinutes === 'function' ? now.getMinutes() : NaN;
          if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
            return false;
          }
          const totalMinutes = hours * 60 + minutes;
          return totalMinutes >= ADMIN_KIOSK_UVA_HEALTH_START_MINUTES
            && totalMinutes < ADMIN_KIOSK_UVA_HEALTH_END_MINUTES;
        } catch (error) {
          return false;
        }
      }

      let adminKioskScheduledAgencyTimerId = null;

      function getAgencyByName(name) {
        if (!name || !Array.isArray(agencies)) {
          return null;
        }
        const normalizedName = `${name}`.trim().toLowerCase();
        if (!normalizedName) {
          return null;
        }
        return agencies.find(entry => {
          const entryName = typeof entry?.name === 'string' ? entry.name.trim().toLowerCase() : '';
          return entryName === normalizedName;
        }) || null;
      }

      function getAdminKioskScheduledAgencyUrl() {
        if (!adminKioskMode) {
          return null;
        }
        const targetName = shouldForceAdminKioskUvaHealth()
          ? ADMIN_KIOSK_UVA_HEALTH_NAME
          : ADMIN_KIOSK_UVA_NAME;
        const agency = getAgencyByName(targetName);
        const url = typeof agency?.url === 'string' ? agency.url.trim() : '';
        return url || null;
      }

      function clearAdminKioskScheduledAgencyTimer() {
        if (adminKioskScheduledAgencyTimerId === null) {
          return;
        }
        if (typeof window !== 'undefined' && typeof window.clearTimeout === 'function') {
          window.clearTimeout(adminKioskScheduledAgencyTimerId);
        } else {
          clearTimeout(adminKioskScheduledAgencyTimerId);
        }
        adminKioskScheduledAgencyTimerId = null;
      }

      function computeNextAdminKioskScheduleDelayMs() {
        if (!adminKioskMode) {
          return null;
        }
        try {
          const now = new Date();
          if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
            return null;
          }
          const hours = typeof now.getHours === 'function' ? now.getHours() : NaN;
          const minutes = typeof now.getMinutes === 'function' ? now.getMinutes() : NaN;
          const seconds = typeof now.getSeconds === 'function' ? now.getSeconds() : NaN;
          const milliseconds = typeof now.getMilliseconds === 'function' ? now.getMilliseconds() : NaN;
          if (![hours, minutes, seconds, milliseconds].every(Number.isFinite)) {
            return null;
          }
          const currentMs = (((hours * 60) + minutes) * 60 + seconds) * 1000 + milliseconds;
          let targetMs;
          if (currentMs < ADMIN_KIOSK_UVA_HEALTH_START_MS) {
            targetMs = ADMIN_KIOSK_UVA_HEALTH_START_MS;
          } else if (currentMs < ADMIN_KIOSK_UVA_HEALTH_END_MS) {
            targetMs = ADMIN_KIOSK_UVA_HEALTH_END_MS;
          } else {
            targetMs = ADMIN_KIOSK_UVA_HEALTH_START_MS + 24 * 60 * MS_PER_MINUTE;
          }
          const delta = targetMs - currentMs;
          return Number.isFinite(delta) ? delta : null;
        } catch (error) {
          return null;
        }
      }

      function scheduleNextAdminKioskAgencyCheck() {
        clearAdminKioskScheduledAgencyTimer();
        if (!adminKioskMode || typeof window === 'undefined' || typeof window.setTimeout !== 'function') {
          return;
        }
        let delay = computeNextAdminKioskScheduleDelayMs();
        if (!Number.isFinite(delay) || delay <= 0) {
          delay = ADMIN_KIOSK_SCHEDULE_MAX_DELAY_MS;
        }
        const normalizedDelay = Math.min(
          Math.max(delay, ADMIN_KIOSK_SCHEDULE_MIN_DELAY_MS),
          ADMIN_KIOSK_SCHEDULE_MAX_DELAY_MS
        );
        adminKioskScheduledAgencyTimerId = window.setTimeout(() => {
          adminKioskScheduledAgencyTimerId = null;
          enforceAdminKioskAgencySchedule({ force: true });
        }, normalizedDelay);
      }

      function enforceAdminKioskAgencySchedule({ force = false } = {}) {
        if (!adminKioskMode) {
          clearAdminKioskScheduledAgencyTimer();
          return;
        }
        const targetUrl = getAdminKioskScheduledAgencyUrl();
        if (targetUrl) {
          const trimmedTarget = targetUrl.trim();
          if (trimmedTarget && (force || baseURL !== trimmedTarget)) {
            if (!map) {
              baseURL = trimmedTarget;
            } else if (baseURL !== trimmedTarget) {
              changeAgency(trimmedTarget);
            }
          }
        }
        scheduleNextAdminKioskAgencyCheck();
      }

      function initializeAdminKioskAgencySchedule() {
        if (!adminKioskMode) {
          clearAdminKioskScheduledAgencyTimer();
          return;
        }
        enforceAdminKioskAgencySchedule({ force: true });
      }

      function shouldEnableAdminKioskOnDemand() {
        try {
          const now = new Date();
          if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
            return false;
          }
          const hours = typeof now.getHours === 'function' ? now.getHours() : NaN;
          const minutes = typeof now.getMinutes === 'function' ? now.getMinutes() : NaN;
          if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
            return false;
          }
          const totalMinutes = (hours * 60) + minutes;
          if (ADMIN_KIOSK_ONDEMAND_START_MINUTES <= ADMIN_KIOSK_ONDEMAND_END_MINUTES) {
            return totalMinutes >= ADMIN_KIOSK_ONDEMAND_START_MINUTES
              && totalMinutes < ADMIN_KIOSK_ONDEMAND_END_MINUTES;
          }
          return totalMinutes >= ADMIN_KIOSK_ONDEMAND_START_MINUTES
            || totalMinutes < ADMIN_KIOSK_ONDEMAND_END_MINUTES;
        } catch (error) {
          return false;
        }
      }

      function clearAdminKioskOnDemandTimer() {
        if (adminKioskOnDemandTimerId === null) {
          return;
        }
        if (typeof window !== 'undefined' && typeof window.clearTimeout === 'function') {
          window.clearTimeout(adminKioskOnDemandTimerId);
        } else {
          clearTimeout(adminKioskOnDemandTimerId);
        }
        adminKioskOnDemandTimerId = null;
      }

      function computeNextAdminKioskOnDemandDelayMs() {
        try {
          const now = new Date();
          if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
            return null;
          }
          const hours = typeof now.getHours === 'function' ? now.getHours() : NaN;
          const minutes = typeof now.getMinutes === 'function' ? now.getMinutes() : NaN;
          const seconds = typeof now.getSeconds === 'function' ? now.getSeconds() : NaN;
          const milliseconds = typeof now.getMilliseconds === 'function' ? now.getMilliseconds() : NaN;
          if (![hours, minutes, seconds, milliseconds].every(Number.isFinite)) {
            return null;
          }
          const currentMs = (((hours * 60) + minutes) * 60 + seconds) * 1000 + milliseconds;
          const nextStart = currentMs < ADMIN_KIOSK_ONDEMAND_START_MS
            ? ADMIN_KIOSK_ONDEMAND_START_MS
            : ADMIN_KIOSK_ONDEMAND_START_MS + MS_PER_DAY;
          const nextEnd = currentMs < ADMIN_KIOSK_ONDEMAND_END_MS
            ? ADMIN_KIOSK_ONDEMAND_END_MS
            : ADMIN_KIOSK_ONDEMAND_END_MS + MS_PER_DAY;
          const currentlyEnabled = shouldEnableAdminKioskOnDemand();
          const targetMs = currentlyEnabled ? nextEnd : nextStart;
          const delta = targetMs - currentMs;
          return Number.isFinite(delta) ? delta : null;
        } catch (error) {
          return null;
        }
      }

      function scheduleNextAdminKioskOnDemandCheck() {
        clearAdminKioskOnDemandTimer();
        if (typeof window === 'undefined' || typeof window.setTimeout !== 'function') {
          return;
        }
        let delay = computeNextAdminKioskOnDemandDelayMs();
        if (!Number.isFinite(delay) || delay <= 0) {
          delay = ADMIN_KIOSK_SCHEDULE_MAX_DELAY_MS;
        }
        const normalizedDelay = Math.min(
          Math.max(delay, ADMIN_KIOSK_SCHEDULE_MIN_DELAY_MS),
          ADMIN_KIOSK_SCHEDULE_MAX_DELAY_MS
        );
        adminKioskOnDemandTimerId = window.setTimeout(() => {
          adminKioskOnDemandTimerId = null;
          enforceAdminKioskOnDemandSchedule({ force: true });
        }, normalizedDelay);
      }

      function enforceAdminKioskOnDemandSchedule({ force = false } = {}) {
        if (!userIsAuthorizedForOnDemand()) {
          if (onDemandVehiclesEnabled) {
            setOnDemandVehiclesEnabled(false);
          } else {
            updateOnDemandButton();
          }
          if (onDemandStopsEnabled) {
            setOnDemandStopsEnabled(false);
          } else {
            updateOnDemandStopsButton();
          }
          scheduleNextAdminKioskOnDemandCheck();
          return;
        }
        const shouldEnable = shouldEnableAdminKioskOnDemand();
        if (force || onDemandVehiclesEnabled !== shouldEnable) {
          setOnDemandVehiclesEnabled(shouldEnable);
        }
        if (force || onDemandStopsEnabled !== shouldEnable) {
          setOnDemandStopsEnabled(shouldEnable);
        }
        scheduleNextAdminKioskOnDemandCheck();
      }

      function initializeAdminKioskOnDemandSchedule() {
        clearAdminKioskOnDemandTimer();
        enforceAdminKioskOnDemandSchedule({ force: true });
      }
      let catOverlayEnabled = false;
      let catLayerGroup = null;
      const catVehicleMarkers = new Map();
      const catVehicleEtaCache = new Map();
      const catStopEtaCache = new Map();
      const catStopEtaRequests = new Map();
      let catActiveVehicleTooltip = null;
      const catRoutesById = new Map();
      const catStopsById = new Map();
      const CAT_OUT_OF_SERVICE_ROUTE_KEY = '__CAT_OUT_OF_SERVICE__';
      const CAT_OUT_OF_SERVICE_NUMERIC_ROUTE_ID = 777;
      const kioskModeAllowedCatRouteIds = new Set();
      const catRouteSelections = new Map();
      let catActiveRouteKeys = new Set();
      const catVehiclesById = new Map();
      let catRefreshIntervals = [];
      let catRoutesLastFetchTime = 0;
      let catStopsLastFetchTime = 0;
      const catRoutePatternGeometries = new Map();
      const catRoutePatternLayers = new Map();
      const catOverlapPatternIdMap = new Map();
      const catOverlapInfoByNumericId = new Map();
      let nextCatOverlapNumericId = 1000000;
      let catRoutePatternsLastFetchTime = 0;
      let catRoutePatternsCache = [];
      let catVehiclesPaneName = 'catVehiclesPane';
      let catServiceAlerts = [];
      let catServiceAlertsLoading = false;
      let catServiceAlertsError = null;
      let catServiceAlertsFetchPromise = null;
      let catServiceAlertsLastFetchTime = 0;
      let catBusMarkerSvgPromise = null;

      let incidentsVisible = false;
      let incidentsVisibilityPreference = false;
      let incidentLayerGroup = null;
      const incidentMarkers = new Map();
      const incidentIconCache = new Map();
      const incidentHaloIconCache = new Map();
      let incidentHaloLayerGroup = null;
      let incidentsNearRoutesLookup = new Map();
      const reduceMotionMediaQuery = (typeof window !== 'undefined' && typeof window.matchMedia === 'function')
        ? window.matchMedia('(prefers-reduced-motion: reduce)')
        : null;
      let isFetchingIncidents = false;

      const LOW_PERFORMANCE_QUERY_PARAM = 'lowperf';
      const LOW_PERFORMANCE_DISABLE_PARAM = 'highperf';
      let lowPerformanceMode = false;

      function getUrlBooleanOverride(paramName, defaultValue) {
        if (!window.usp || typeof window.usp.getBoolean !== 'function') {
          return defaultValue;
        }
        if (!window.usp.has(paramName)) {
          return defaultValue;
        }
        return window.usp.getBoolean(paramName, defaultValue);
      }

      function detectLowPerformanceDevice() {
        if (getUrlBooleanOverride(LOW_PERFORMANCE_DISABLE_PARAM, false)) {
          return false;
        }
        if (getUrlBooleanOverride(LOW_PERFORMANCE_QUERY_PARAM, false)) {
          return true;
        }

        const nav = typeof navigator === 'object' && navigator !== null ? navigator : null;
        const hardwareConcurrency = nav && typeof nav.hardwareConcurrency === 'number'
          ? nav.hardwareConcurrency
          : Number.parseInt(nav && nav.hardwareConcurrency, 10);
        const deviceMemory = nav && typeof nav.deviceMemory === 'number'
          ? nav.deviceMemory
          : Number.parseInt(nav && nav.deviceMemory, 10);
        const userAgent = nav && typeof nav.userAgent === 'string' ? nav.userAgent.toLowerCase() : '';
        const lowPowerUserAgent = /raspberry pi|armv[0-9]+l|aarch64/.test(userAgent);
        const lowThreadCount = Number.isFinite(hardwareConcurrency) && hardwareConcurrency > 0 && hardwareConcurrency <= 4;
        const lowMemory = Number.isFinite(deviceMemory) && deviceMemory > 0 && deviceMemory <= 4;

        return lowPowerUserAgent || lowThreadCount || lowMemory || isReducedMotionPreferred();
      }

      function updateLowPerformanceMode() {
        lowPerformanceMode = detectLowPerformanceDevice();
      }

      updateLowPerformanceMode();

      const TRANSLOC_SNAPSHOT_DEFAULT_CACHE_KEY = '__default__';
      const translocSnapshotCache = new Map();

      function getTranslocSnapshotCacheKey(base, includeStale = includeStaleVehicles) {
        const baseKey = base || TRANSLOC_SNAPSHOT_DEFAULT_CACHE_KEY;
        return includeStale ? `${baseKey}::stale` : `${baseKey}::fresh`;
      }

      function getOrCreateTranslocSnapshotEntry(cacheKey) {
        let entry = translocSnapshotCache.get(cacheKey);
        if (!entry) {
          entry = { data: null, promise: null, timestamp: 0 };
          translocSnapshotCache.set(cacheKey, entry);
        }
        return entry;
      }

      function resetTranslocSnapshotCache() {
        translocSnapshotCache.clear();
      }

      function loadTranslocSnapshot(force = false) {
        const sanitizedBaseURL = sanitizeBaseUrl(baseURL);
        const cacheKey = getTranslocSnapshotCacheKey(sanitizedBaseURL, includeStaleVehicles);
        const entry = getOrCreateTranslocSnapshotEntry(cacheKey);
        const now = Date.now();
        if (!force && entry.data && (now - entry.timestamp) < TRANSLOC_SNAPSHOT_TTL_MS) {
          return Promise.resolve(entry.data);
        }
        if (entry.promise) {
          return entry.promise;
        }
        const queryParts = [];
        if (sanitizedBaseURL) {
          queryParts.push(`base_url=${encodeURIComponent(sanitizedBaseURL)}`);
        }
        if (includeStaleVehicles) {
          queryParts.push('stale=true');
        }
        const endpoint = queryParts.length > 0
          ? `${TRANSLOC_SNAPSHOT_ENDPOINT}?${queryParts.join('&')}`
          : TRANSLOC_SNAPSHOT_ENDPOINT;
        entry.promise = fetch(endpoint, { cache: 'no-store' })
          .then(response => {
            if (!response || !response.ok) {
              throw new Error(response ? `HTTP ${response.status}` : 'No response');
            }
            return response.json();
          })
          .then(data => {
            entry.data = data || {};
            entry.timestamp = Date.now();
            return entry.data;
          })
          .catch(error => {
            console.error('Failed to load TransLoc snapshot:', error);
            throw error;
          })
          .finally(() => {
            entry.promise = null;
          });
        return entry.promise;
      }

      const SERVICE_ALERT_REFRESH_INTERVAL_MS = 60000;
      const SERVICE_ALERT_START_FIELDS = Object.freeze([
        'StartDateText',
        'StartDateDisplay',
        'StartDateLocalText',
        'StartDateLocal',
        'StartDate',
        'StartDateUtc',
        'StartDateTime',
        'StartDateISO',
        'StartTimestamp',
        'StartTime',
        'Start',
        'BeginDateText',
        'BeginDate',
        'BeginDateUtc',
        'BeginTime',
        'EffectiveStart',
        'EffectiveStartDate',
        'EffectiveStartUtc'
      ]);
      const SERVICE_ALERT_END_FIELDS = Object.freeze([
        'EndDateText',
        'EndDateDisplay',
        'EndDateLocalText',
        'EndDateLocal',
        'EndDate',
        'EndDateUtc',
        'EndDateTime',
        'EndDateISO',
        'EndTimestamp',
        'EndTime',
        'End',
        'StopDateText',
        'StopDate',
        'StopDateUtc',
        'StopTime',
        'ExpirationDate',
        'ExpirationDateUtc',
        'ExpireDate',
        'ExpireDateUtc',
        'EffectiveEnd',
        'EffectiveEndDate',
        'EffectiveEndUtc'
      ]);
      const SERVICE_ALERT_UNAVAILABLE_MESSAGE = 'Service alerts unavailable.';
      const SERVICE_ALERT_STATUS_NO_ALERTS = 'No Active Alerts';
      const SERVICE_ALERT_STATUS_LOADING = 'Loading…';
      const SERVICE_ALERT_STATUS_ERROR = 'Unavailable';
      const SERVICE_ALERT_DATE_FORMATTER = (() => {
        if (typeof Intl === 'undefined' || typeof Intl.DateTimeFormat !== 'function') {
          return null;
        }
        try {
          return new Intl.DateTimeFormat('en-US', {
            dateStyle: 'medium',
            timeStyle: 'short',
            timeZoneName: 'short'
          });
        } catch (error) {
          try {
            return new Intl.DateTimeFormat('en-US', {
              year: 'numeric',
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: '2-digit',
              timeZoneName: 'short'
            });
          } catch (fallbackError) {
            return null;
          }
        }
      })();
      let serviceAlerts = [];
      let serviceAlertsLoading = false;
      let serviceAlertsError = null;
      let serviceAlertsExpanded = false;
      let serviceAlertsLastFetchAgency = '';
      let serviceAlertsLastFetchTime = 0;
      let serviceAlertsFetchPromise = null;
      let serviceAlertsHasLoaded = false;

      function hasIncidentsRequiringVisibility() {
        return incidentsNearRoutesLookup instanceof Map && incidentsNearRoutesLookup.size > 0;
      }

      function shouldShowIncidentLayer() {
        if (!incidentsAreAvailable()) {
          return false;
        }
        return incidentsVisible || hasIncidentsRequiringVisibility();
      }

      function maintainIncidentLayers() {
        if (!map) return;
        const shouldShow = shouldShowIncidentLayer();
        if (!incidentLayerGroup) {
          incidentLayerGroup = L.layerGroup();
        }
        if (shouldShow) {
          if (!map.hasLayer(incidentLayerGroup)) {
            incidentLayerGroup.addTo(map);
          }
        } else if (map.hasLayer(incidentLayerGroup)) {
          map.removeLayer(incidentLayerGroup);
        }
        const shouldShowHalos = shouldShow && hasIncidentsRequiringVisibility();
        if (!incidentHaloLayerGroup) {
          incidentHaloLayerGroup = L.layerGroup();
        }
        if (shouldShowHalos) {
          if (!map.hasLayer(incidentHaloLayerGroup)) {
            incidentHaloLayerGroup.addTo(map);
          }
        } else if (map.hasLayer(incidentHaloLayerGroup)) {
          map.removeLayer(incidentHaloLayerGroup);
        }
      }

      const INCIDENT_ROUTE_PROXIMITY_THRESHOLD_METERS = 150;
      const INCIDENT_TIME_ZONE = 'America/New_York';
      const INCIDENT_LIST_ICON_BASE_URL = 'https://web.pulsepoint.org/images/respond_icons/';
      const INCIDENT_TYPE_LABELS = Object.freeze({
        AED: 'AED Alarm',
        AC: 'Aircraft Crash',
        AE: 'Aircraft Emergency',
        AES: 'Aircraft Emergency Standby',
        OA: 'Alarm',
        AR: 'Animal Rescue',
        AF: 'Appliance Fire',
        AI: 'Arson Investigation',
        AA: 'Auto Aid',
        BT: 'Bomb Threat',
        BP: 'Burn Permit',
        CMA: 'Carbon Monoxide',
        CHIM: 'Chimney Fire',
        CR: 'Cliff Rescue',
        TCP: 'Collision Involving Pedestrian',
        TCS: 'Collision Involving Structure',
        TCT: 'Collision Involving Train',
        CF: 'Commercial Fire',
        CL: 'Commercial Lockout',
        CA: 'Community Activity',
        CP: 'Community Paramedicine',
        CSR: 'Confined Space Rescue',
        WF: 'Confirmed Fire',
        WSF: 'Confirmed Structure Fire',
        WVEG: 'Confirmed Vegetation Fire',
        CB: 'Controlled Burn/Prescribed Fire',
        EQ: 'Earthquake',
        EE: 'Electrical Emergency',
        ELF: 'Electrical Fire',
        ELR: 'Elevator Rescue',
        EER: 'Elevator/Escalator Rescue',
        EM: 'Emergency',
        ER: 'Emergency Response',
        TCE: 'Expanded Traffic Collision',
        EX: 'Explosion',
        EF: 'Extinguished Fire',
        FIRE: 'Fire',
        FA: 'Fire Alarm',
        FW: 'Fire Watch',
        FWI: 'Fireworks Investigation',
        FLW: 'Flood Warning',
        FL: 'Flooding',
        FULL: 'Full Assignment',
        GAS: 'Gas Leak',
        HC: 'Hazardous Condition',
        HMR: 'Hazardous Response',
        HMI: 'Hazmat Investigation',
        IR: 'Ice Rescue',
        IF: 'Illegal Fire',
        IA: 'Industrial Accident',
        IFT: 'Interfacility Transfer',
        INV: 'Investigation',
        LR: 'Ladder Request',
        LZ: 'Landing Zone',
        LA: 'Lift Assist',
        LO: 'Lockout',
        MA: 'Manual Alarm',
        MF: 'Marine Fire',
        ME: 'Medical Emergency',
        MC: 'Move-up/Cover',
        MCI: 'Multi Casualty',
        MU: 'Mutual Aid',
        NO: 'Notification',
        OI: 'Odor Investigation',
        OF: 'Outside Fire',
        PE: 'Pipeline Emergency',
        PF: 'Pole Fire',
        PA: 'Police Assist',
        PLE: 'Powerline Emergency',
        PS: 'Public Service',
        RTE: 'Railroad/Train Emergency',
        GF: 'Refuse/Garbage Fire',
        RES: 'Rescue',
        RF: 'Residential Fire',
        RL: 'Residential Lockout',
        RR: 'Rope Rescue',
        SH: 'Sheared Hydrant',
        SD: 'Smoke Detector',
        SI: 'Smoke Investigation',
        STBY: 'Standby',
        ST: 'Strike Team/Task Force',
        SC: 'Structural Collapse',
        SF: 'Structure Fire',
        TF: 'Tank Fire',
        TR: 'Technical Rescue',
        TEST: 'Test',
        TOW: 'Tornado Warning',
        TC: 'Traffic Collision',
        TRNG: 'Training',
        TE: 'Transformer Explosion',
        TD: 'Tree Down',
        TNR: 'Trench Rescue',
        TRBL: 'Trouble Alarm',
        TSW: 'Tsunami Warning',
        USAR: 'Urban Search and Rescue',
        VEG: 'Vegetation Fire',
        VF: 'Vehicle Fire',
        VL: 'Vehicle Lockout',
        VS: 'Vessel Sinking',
        WE: 'Water Emergency',
        WR: 'Water Rescue',
        WFA: 'Waterflow Alarm',
        WX: 'Weather Incident',
        WA: 'Wires Arching',
        WD: 'Wires Down',
        WDA: 'Wires Down/Arcing',
        WCF: 'Working Commercial Fire',
        WRF: 'Working Residential Fire'
      });
      const INCIDENT_RECEIVED_FIELDS = Object.freeze([
        'CallReceivedDateTime',
        'ReceivedDateTime',
        'Received',
        'CallReceived',
        'FirstReceived',
        'CreateDate',
        'CreatedDateTime',
        'DispatchDateTime'
      ]);
      const INCIDENT_UNIT_STATUS_INFO = Object.freeze({
        DP: { label: 'Dispatched', color: '#f57c00', background: 'rgba(245, 124, 0, 0.16)', border: 'rgba(245, 124, 0, 0.38)' },
        AK: { label: 'Acknowledged', color: '#f57c00', background: 'rgba(245, 124, 0, 0.16)', border: 'rgba(245, 124, 0, 0.38)' },
        ER: { label: 'En Route', color: '#00cc00', background: 'rgba(0, 204, 0, 0.16)', border: 'rgba(0, 204, 0, 0.38)' },
        SG: { label: 'Staged', color: '#cc0000', background: 'rgba(204, 0, 0, 0.16)', border: 'rgba(204, 0, 0, 0.38)' },
        OS: { label: 'On Scene', color: '#cc0000', background: 'rgba(204, 0, 0, 0.16)', border: 'rgba(204, 0, 0, 0.38)' },
        AE: { label: 'Available On Scene', color: '#cc0000', background: 'rgba(204, 0, 0, 0.16)', border: 'rgba(204, 0, 0, 0.38)' },
        TR: { label: 'Transport', color: '#ffc107', background: 'rgba(255, 193, 7, 0.16)', border: 'rgba(255, 193, 7, 0.38)' },
        TA: { label: 'Transport Arrived', color: '#1976d2', background: 'rgba(25, 118, 210, 0.16)', border: 'rgba(25, 118, 210, 0.38)' },
        AR: { label: 'Cleared From Incident', color: '#494949', background: 'rgba(73, 73, 73, 0.16)', border: 'rgba(73, 73, 73, 0.38)' }
      });
      const INCIDENT_UNIT_STATUS_ALIASES = Object.freeze({
        DP: 'DP',
        DISPATCHED: 'DP',
        DISPATCH: 'DP',
        AK: 'AK',
        ACK: 'AK',
        ACKNOWLEDGED: 'AK',
        ER: 'ER',
        'EN ROUTE': 'ER',
        ENROUTE: 'ER',
        SG: 'SG',
        STAGED: 'SG',
        OS: 'OS',
        'ON SCENE': 'OS',
        'ON-SCENE': 'OS',
        ONSCENE: 'OS',
        AE: 'AE',
        'AVAILABLE ON SCENE': 'AE',
        'AVAILABLE ONSCENE': 'AE',
        'AVAILABLE ON-SCENE': 'AE',
        'AVAIL ON SCENE': 'AE',
        TR: 'TR',
        TRANSPORT: 'TR',
        TRANSPORTING: 'TR',
        TA: 'TA',
        'TRANSPORT ARRIVED': 'TA',
        'TRANSPORT-ARRIVED': 'TA',
        'TRANSPORT ARRVD': 'TA',
        AR: 'AR',
        CLEARED: 'AR',
        'CLEARED FROM INCIDENT': 'AR'
      });
      const INCIDENT_UNIT_STATUS_SECTION_ORDER = Object.freeze([
        'OS',
        'AE',
        'SG',
        'ER',
        'TR',
        'TA',
        'DP',
        'AK',
        'AR'
      ]);
      const INCIDENT_UNIT_ACTIVE_STATUS_KEYS = Object.freeze([
        'OS',
        'AE',
        'ER'
      ]);
      const INCIDENT_UNIT_STATUS_FALLBACK_LABEL = 'Status Unknown';

      let latestActiveIncidents = [];
      let incidentsNearRoutes = [];
      let incidentRouteAlertSignature = '';
      const incidentFirstOnSceneTimes = new Map();
      // Demo incident preview state (delete when demo button is removed).
      let demoIncidentActive = false;
      let demoIncidentEntry = null;
      let demoIncidentPreviousVisibility = null;
      const DEMO_INCIDENT_STATIC_ROW = Object.freeze({
        Marker: 'https://web.pulsepoint.org/images/respond_icons/me_map_active.png',
        Category: 'active',
        ID: '2296541797',
        Type: 'ME',
        Address: 'HILLSDALE DR, CHARLOTTESVILLE, VA',
        Received: '2025-09-22T02:25:48Z',
        Agency: '00300',
        Latitude: '38.0739216008',
        Longitude: '-78.4735028808',
        Units: 'E83 (OS), RS18 (OS)'
      });

      let demoIncidentCsvRow = null;

      function getSelectedAgencyRecord() {
        if (!Array.isArray(agencies) || agencies.length === 0) {
          return null;
        }
        const sanitizedBaseURL = typeof baseURL === 'string' ? baseURL.trim().replace(/\/+$/, '') : '';
        const match = agencies.find(agency => {
          if (!agency || typeof agency.url !== 'string') return false;
          const candidateUrl = agency.url.trim().replace(/\/+$/, '');
          return candidateUrl === sanitizedBaseURL;
        });
        return match && typeof match === 'object' ? match : null;
      }

      function doesSelectedAgencyMatchNames(allowedNames) {
        if (!Array.isArray(allowedNames) || allowedNames.length === 0) {
          return false;
        }
        const selectedAgency = getSelectedAgencyRecord();
        if (!selectedAgency || typeof selectedAgency.name !== 'string') {
          return false;
        }
        const normalizedName = selectedAgency.name.trim().toLowerCase();
        return allowedNames.some(name => typeof name === 'string' && name.trim().toLowerCase() === normalizedName);
      }

      function incidentsAreAvailable() {
        if (!adminFeaturesAllowed()) {
          return false;
        }
        if (!Array.isArray(agencies) || agencies.length === 0) {
          return true;
        }
        return doesSelectedAgencyMatchNames(INCIDENTS_ALLOWED_AGENCY_NAMES);
      }

      function catOverlayIsAvailable() {
        if (!Array.isArray(agencies) || agencies.length === 0) {
          return false;
        }
        return doesSelectedAgencyMatchNames(CAT_ALLOWED_AGENCY_NAMES);
      }

      if (!incidentsAreAvailable()) {
        incidentsVisible = false;
      }
      incidentsVisibilityPreference = incidentsVisible;

      function looksLikePulsePointIncident(obj) {
        if (!obj || typeof obj !== 'object') return false;
        const keys = ['ID', 'FullDisplayAddress', 'PulsePointIncidentCallType', 'CallReceivedDateTime', 'Latitude', 'Longitude'];
        return keys.filter(key => key in obj).length >= 2;
      }

      function inferPulsePointMarkerType(rec) {
        const candidates = [
          rec.PulsePointIncidentCallTypePrimaryCode,
          rec.PulsePointIncidentCallTypeCode,
          rec.PulsePointIncidentCallTypeID,
          rec.PulsePointIncidentTypeCode,
          rec.PulsePointIncidentType,
          rec.CallTypeCode,
          rec.TypeCode,
          rec.CallType,
          rec.Type,
          rec.IncidentType,
          rec.PulsePointIncidentCallType
        ];
        for (const value of candidates) {
          if (value == null) continue;
          const raw = typeof value === 'number' ? value.toString() : String(value);
          const trimmed = raw.trim();
          if (!trimmed) continue;
          if (/^[A-Za-z0-9]{1,6}$/.test(trimmed)) return trimmed.toUpperCase();
          const firstToken = trimmed.split(/[\s/-]+/)[0];
          if (firstToken && /^[A-Za-z0-9]{1,4}$/.test(firstToken)) return firstToken.toUpperCase();
          const words = trimmed.match(/[A-Za-z0-9]+/g);
          if (words && words.length >= 2) {
            const acronym = words.map(word => word[0]).join('');
            if (acronym && /^[A-Za-z0-9]{1,4}$/.test(acronym)) return acronym.toUpperCase();
          }
        }
        return '';
      }

      function buildPulsePointMarkerUrl(type, category) {
        const categoryLower = (category || '').toLowerCase();
        if (!type || (categoryLower !== 'active' && categoryLower !== 'recent')) return '';
        return `https://web.pulsepoint.org/images/respond_icons/${type.toLowerCase()}_map_${categoryLower}.png`;
      }

      function pulsePointMarkerAltText(type, category, fallback) {
        const parts = [];
        if (type) parts.push(type);
        if (category) parts.push(category);
        if (!parts.length && fallback) parts.push(fallback);
        if (!parts.length) return 'Marker icon';
        parts.push('marker icon');
        return parts.join(' ');
      }

      function decoratePulsePointIncident(rec, category) {
        const copy = { ...(rec || {}), _category: category };
        if (Array.isArray(copy.Unit)) {
          copy._units = copy.Unit.map(u => {
            const id = u.UnitID || u.Unit || '';
            const status = u.PulsePointDispatchStatus || u.Status || '';
            return status ? `${id} (${status})` : id;
          }).join(', ');
        } else {
          copy._units = '';
        }
        const markerType = inferPulsePointMarkerType(copy);
        const markerCategory = (category || '').toLowerCase();
        const markerUrl = buildPulsePointMarkerUrl(markerType, markerCategory);
        copy._markerType = markerType;
        copy._markerCategory = markerUrl ? markerCategory : '';
        copy._markerUrl = markerUrl;
        copy._markerAlt = markerUrl ? pulsePointMarkerAltText(markerType, markerCategory, copy.PulsePointIncidentCallType) : '';
        return copy;
      }

      function normalizePulsePointIncidents(root) {
        const out = [];
        const incidentsRoot = root && root.incidents;
        const categories = [['active', 'active'], ['recent', 'recent'], ['alerts', 'alerts']];
        let pulled = 0;
        if (incidentsRoot && typeof incidentsRoot === 'object') {
          for (const [key, label] of categories) {
            const arr = Array.isArray(incidentsRoot[key]) ? incidentsRoot[key] : [];
            arr.forEach(rec => out.push(decoratePulsePointIncident(rec, label)));
            pulled += arr.length;
          }
          if (pulled) return out;
        }
        const rootKeys = root && typeof root === 'object' ? Object.keys(root) : [];
        for (const key of rootKeys) {
          const value = root[key];
          if (Array.isArray(value) && value.every(looksLikePulsePointIncident)) {
            value.forEach(rec => out.push(decoratePulsePointIncident(rec, key)));
          } else if (value && typeof value === 'object') {
            Object.keys(value).forEach(innerKey => {
              const innerValue = value[innerKey];
              if (Array.isArray(innerValue) && innerValue.every(looksLikePulsePointIncident)) {
                innerValue.forEach(rec => out.push(decoratePulsePointIncident(rec, innerKey)));
              }
            });
          }
        }
        if (out.length) return out;
        (function dig(x, label = 'misc') {
          if (!x) return;
          if (Array.isArray(x) && x.length && looksLikePulsePointIncident(x[0])) {
            x.forEach(rec => out.push(decoratePulsePointIncident(rec, label)));
            return;
          }
          if (typeof x === 'object') {
            Object.keys(x).forEach(childKey => dig(x[childKey], childKey));
          }
        })(root);
        return out;
      }

      function resetIncidentAlertState() {
        latestActiveIncidents = [];
        updateIncidentsNearRoutes([], '');
        incidentFirstOnSceneTimes.clear();
      }

      function parseIncidentDate(value) {
        if (value === undefined || value === null || value === '') return null;
        if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
        if (typeof value === 'number' && Number.isFinite(value)) {
          const fromNumber = new Date(value);
          return Number.isNaN(fromNumber.getTime()) ? null : fromNumber;
        }
        const str = String(value).trim();
        if (!str) return null;
        let parsed = new Date(str);
        if (!Number.isNaN(parsed.getTime())) return parsed;
        if (!/[zZ]|[+-]\d{2}:?\d{2}$/.test(str)) {
          parsed = new Date(`${str}Z`);
          if (!Number.isNaN(parsed.getTime())) return parsed;
        }
        return null;
      }

      function normalizeIncidentTimestampValue(value) {
        if (value === undefined || value === null || value === '') return null;
        if (value instanceof Date && !Number.isNaN(value.getTime())) {
          return value.getTime();
        }
        if (typeof value === 'number' && Number.isFinite(value)) {
          if (value > 1e12) return Math.round(value);
          if (value > 1e9) return Math.round(value * 1000);
          return null;
        }
        const str = String(value).trim();
        if (!str) return null;
        const numeric = Number.parseFloat(str);
        if (Number.isFinite(numeric)) {
          if (numeric > 1e12) return Math.round(numeric);
          if (numeric > 1e9) return Math.round(numeric * 1000);
        }
        const parsed = parseIncidentDate(str);
        return parsed ? parsed.getTime() : null;
      }

      function formatIncidentTimestamp(date) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
        try {
          const display = new Intl.DateTimeFormat('en-US', {
            hour: 'numeric',
            minute: '2-digit',
            hour12: true,
            timeZone: INCIDENT_TIME_ZONE
          }).format(date);
          const full = new Intl.DateTimeFormat('en-US', {
            dateStyle: 'medium',
            timeStyle: 'short',
            timeZone: INCIDENT_TIME_ZONE
          }).format(date);
          return { display, full };
        } catch (error) {
          const fallback = date.toLocaleString();
          return { display: fallback, full: fallback };
        }
      }

      function getIncidentReceivedTimeInfo(incident) {
        if (!incident) return null;
        for (const field of INCIDENT_RECEIVED_FIELDS) {
          if (!Object.prototype.hasOwnProperty.call(incident, field)) continue;
          const date = parseIncidentDate(incident[field]);
          const info = formatIncidentTimestamp(date);
          if (info) return info;
        }
        return null;
      }

      function getIncidentTimestamp(incident) {
        if (!incident) return null;
        for (const field of INCIDENT_RECEIVED_FIELDS) {
          if (!Object.prototype.hasOwnProperty.call(incident, field)) continue;
          const date = parseIncidentDate(incident[field]);
          if (date) {
            return date.getTime();
          }
        }
        return null;
      }

      // === Demo incident CSV helpers (remove alongside the demo button) ===
      function parseCsvRows(text) {
        if (typeof text !== 'string') return [];
        const rows = [];
        let current = [];
        let field = '';
        let inQuotes = false;
        for (let i = 0; i < text.length; i += 1) {
          const char = text[i];
          if (inQuotes) {
            if (char === '"') {
              if (text[i + 1] === '"') {
                field += '"';
                i += 1;
              } else {
                inQuotes = false;
              }
            } else {
              field += char;
            }
            continue;
          }
          if (char === '"') {
            inQuotes = true;
            continue;
          }
          if (char === ',') {
            current.push(field);
            field = '';
            continue;
          }
          if (char === '\r') {
            continue;
          }
          if (char === '\n') {
            current.push(field);
            rows.push(current);
            current = [];
            field = '';
            continue;
          }
          field += char;
        }
        if (field !== '' || current.length > 0) {
          current.push(field);
        }
        if (current.length > 0) {
          rows.push(current);
        }
        return rows.filter(row => Array.isArray(row) && row.some(value => String(value ?? '').trim() !== ''));
      }

      function extractFirstIncidentFromCsv(text) {
        const rows = parseCsvRows(text);
        if (!rows.length) return null;
        const header = rows[0].map(cell => String(cell ?? '').trim());
        for (let i = 1; i < rows.length; i += 1) {
          const row = rows[i];
          if (!row || !row.some(value => String(value ?? '').trim() !== '')) continue;
          const obj = {};
          header.forEach((key, index) => {
            if (!key) return;
            obj[key] = row[index] !== undefined ? row[index] : '';
          });
          const hasValue = Object.keys(obj).some(key => String(obj[key] ?? '').trim() !== '');
          if (hasValue) {
            return obj;
          }
        }
        return null;
      }

      function buildDemoIncidentEntryFromRow(row) {
        if (!row || typeof row !== 'object') return null;
        const sanitize = value => (typeof value === 'string' ? value.trim() : value);
        const typeValue = sanitize(row.Type) || 'INC';
        const typeCode = String(typeValue || '').trim().toUpperCase();
        const idRaw = sanitize(row.ID) || 'DEMO_INCIDENT';
        const id = String(idRaw).trim() || 'DEMO_INCIDENT';
        const normalizedId = getNormalizedIncidentId(id) || 'DEMO_INCIDENT';
        const markerUrl = sanitize(row.Marker) || '';
        const category = sanitize(row.Category) || 'active';
        const address = sanitize(row.Address) || 'Demo Address';
        const received = sanitize(row.Received) || new Date().toISOString();
        const agency = sanitize(row.Agency) || '';
        const units = sanitize(row.Units) || '';
        const lat = parseIncidentCoordinate(sanitize(row.Latitude));
        const lon = parseIncidentCoordinate(sanitize(row.Longitude));
        const label = (typeCode && Object.prototype.hasOwnProperty.call(INCIDENT_TYPE_LABELS, typeCode))
          ? INCIDENT_TYPE_LABELS[typeCode]
          : (typeValue ? String(typeValue) : 'Incident');
        const incident = {
          _markerUrl: markerUrl,
          _markerType: typeCode || 'INC',
          _category: category,
          _demo: true,
          ID: normalizedId,
          IncidentID: normalizedId,
          Type: typeCode || typeValue,
          TypeCode: typeCode || typeValue,
          PulsePointIncidentCallType: label,
          PulsePointIncidentCallTypeCode: typeCode || typeValue,
          CallType: label,
          CallTypeCode: typeCode || typeValue,
          Received: received,
          DisplayAddress: address,
          FullDisplayAddress: address,
          Address: address,
          Latitude: lat,
          Longitude: lon,
          Units: units,
          Agency: agency
        };
        const timestamp = getIncidentTimestamp(incident) ?? Date.now();
        const demoRouteColor = sanitizeCssColor(routeColors?.[404] || '#F97316');
        return {
          id: normalizedId,
          incident,
          routes: [
            { routeId: 404, name: 'Demo Route 404', distance: 42, color: demoRouteColor }
          ],
          closestDistance: 42,
          timestamp,
          _demo: true,
          _demoSignature: `demo-${normalizedId}`
        };
      }

      async function ensureDemoIncidentRow() {
        if (demoIncidentCsvRow) return demoIncidentCsvRow;
        if (!DEMO_INCIDENT_STATIC_ROW) return null;
        demoIncidentCsvRow = { ...DEMO_INCIDENT_STATIC_ROW };
        return demoIncidentCsvRow;
      }

      function createDemoIncidentEntry() {
        if (!demoIncidentCsvRow) return null;
        const entry = buildDemoIncidentEntryFromRow(demoIncidentCsvRow);
        if (!entry) return null;
        entry.incident = entry.incident ? { ...entry.incident } : null;
        entry.routes = Array.isArray(entry.routes)
          ? entry.routes.map(route => ({ ...route }))
          : [];
        if (entry.incident) {
          ensureIncidentFirstOnSceneTracking(entry.incident, entry.id);
          entry.firstOnSceneTimestamp = getIncidentFirstOnSceneTimestamp(entry.incident, entry.id);
          entry.detailSignature = buildIncidentAlertDetailSignature(entry.incident);
        }
        return entry;
      }

      function normalizeUnitStatus(value) {
        if (value === undefined || value === null || value === '') {
          return { key: '', raw: '' };
        }
        const raw = String(value).trim();
        if (!raw) {
          return { key: '', raw: '' };
        }
        const upper = raw.toUpperCase();
        const canonical = INCIDENT_UNIT_STATUS_ALIASES[upper] || '';
        const info = canonical ? INCIDENT_UNIT_STATUS_INFO[canonical] || null : null;
        const label = info?.label || raw;
        return { key: canonical, raw, info, label };
      }

      function parseUnitString(text) {
        if (typeof text !== 'string') {
          return { name: '', status: '', raw: '' };
        }
        const trimmed = text.trim();
        if (!trimmed) {
          return { name: '', status: '', raw: '' };
        }
        const match = trimmed.match(/^(.*?)\s*\(([^)]*)\)\s*$/);
        if (match) {
          return {
            name: match[1].trim(),
            status: match[2].trim(),
            raw: trimmed
          };
        }
        return { name: trimmed, status: '', raw: trimmed };
      }

      function extractIncidentUnits(incident) {
        const units = [];
        if (incident && Array.isArray(incident.Unit)) {
          incident.Unit.forEach(entry => {
            if (!entry) return;
            const nameCandidates = [
              entry.UnitID,
              entry.Unit,
              entry.Name,
              entry.ApparatusID,
              entry.VehicleID
            ];
            let name = nameCandidates.find(value => typeof value === 'string' && value.trim());
            const statusCandidates = [
              entry.PulsePointDispatchStatus,
              entry.DispatchStatus,
              entry.Status,
              entry.UnitStatus
            ];
            let status = statusCandidates.find(value => typeof value === 'string' && value.trim());
            let parsed = null;
            if (typeof entry === 'string') {
              parsed = parseUnitString(entry);
              if (!status && parsed.status) {
                status = parsed.status;
              }
              if (!name && parsed.name) {
                name = parsed.name;
              }
            }
            const normalized = normalizeUnitStatus(status);
            const statusKey = normalized.key || '';
            const normalizedLabel = typeof normalized.label === 'string' ? normalized.label.trim() : '';
            const statusLabel = normalizedLabel || (typeof normalized.raw === 'string' ? normalized.raw.trim() : '');
            const unitName = typeof name === 'string' ? name.trim() : '';
            const rawText = parsed?.raw && parsed.raw.trim()
              ? parsed.raw.trim()
              : (typeof entry === 'string' ? entry.trim() : '');
            const displayText = unitName || statusLabel || rawText;
            if (!displayText) return;
            let tooltip = '';
            if (statusLabel) {
              tooltip = statusLabel;
            }
            if (normalized.raw && normalized.raw !== statusLabel && (!tooltip || tooltip.toLowerCase() !== normalized.raw.toLowerCase())) {
              tooltip = tooltip ? `${tooltip} • ${normalized.raw}` : normalized.raw;
            }
            units.push({
              displayText,
              statusKey,
              statusLabel,
              colorInfo: normalized.info || null,
              tooltip,
              rawStatus: normalized.raw,
              name: unitName
            });
          });
        }
        if (units.length === 0) {
          const stringCandidates = [
            incident?._units,
            incident?.Units,
            incident?.Apparatus,
            incident?.UnitString
          ];
          const source = stringCandidates.find(value => typeof value === 'string' && value.trim());
          if (source) {
            source.split(',').map(part => part.trim()).filter(Boolean).forEach(part => {
              const parsed = parseUnitString(part);
              const normalized = normalizeUnitStatus(parsed.status);
              const statusKey = normalized.key || '';
              const normalizedLabel = typeof normalized.label === 'string' ? normalized.label.trim() : '';
              const statusLabel = normalizedLabel || (typeof normalized.raw === 'string' ? normalized.raw.trim() : '');
              const baseName = parsed.name ? parsed.name.trim() : '';
              const displayText = baseName
                ? baseName
                : (statusLabel || (parsed.raw ? parsed.raw.trim() : ''));
              if (!displayText) return;
              let tooltip = '';
              if (statusLabel) {
                tooltip = statusLabel;
              }
              if (normalized.raw && normalized.raw !== statusLabel && (!tooltip || tooltip.toLowerCase() !== normalized.raw.toLowerCase())) {
                tooltip = tooltip ? `${tooltip} • ${normalized.raw}` : normalized.raw;
              }
              units.push({
                displayText,
                statusKey,
                statusLabel,
                colorInfo: normalized.info || null,
                tooltip,
                rawStatus: normalized.raw,
                name: baseName
              });
            });
          }
        }
        return units;
      }

      function unitHasOnSceneStatus(unit) {
        if (!unit) return false;
        const statusKey = typeof unit.statusKey === 'string' ? unit.statusKey.trim().toUpperCase() : '';
        if (statusKey === 'OS' || statusKey === 'AE') {
          return true;
        }
        const labelCandidates = [unit.statusLabel, unit.rawStatus, unit.tooltip, unit.displayText];
        for (const candidate of labelCandidates) {
          if (typeof candidate !== 'string') continue;
          const normalized = candidate.trim().toLowerCase();
          if (!normalized) continue;
          if (normalized.includes('on scene') || normalized.includes('onscene')) {
            return true;
          }
        }
        return false;
      }

      function unitHasOnSceneOrEnRouteStatus(unit) {
        if (!unit) return false;
        const statusKey = typeof unit.statusKey === 'string' ? unit.statusKey.trim().toUpperCase() : '';
        if (unitHasOnSceneStatus(unit)) {
          return true;
        }
        if (statusKey === 'ER') {
          return true;
        }
        const labelCandidates = [unit.statusLabel, unit.rawStatus, unit.tooltip, unit.displayText];
        for (const candidate of labelCandidates) {
          if (typeof candidate !== 'string') continue;
          const normalized = candidate.trim().toLowerCase();
          if (!normalized) continue;
          if (normalized.includes('en route') || normalized.includes('enroute')) {
            return true;
          }
        }
        return false;
      }

      function incidentHasOnSceneOrEnRouteUnits(incident) {
        const units = extractIncidentUnits(incident);
        if (!Array.isArray(units) || units.length === 0) {
          return false;
        }
        return units.some(unit => unitHasOnSceneOrEnRouteStatus(unit));
      }

      function incidentHasOnSceneUnits(incident) {
        const units = extractIncidentUnits(incident);
        if (!Array.isArray(units) || units.length === 0) {
          return false;
        }
        return units.some(unit => unitHasOnSceneStatus(unit));
      }

      function extractIncidentFirstOnSceneDate(incident) {
        if (!incident || typeof incident !== 'object') return null;
        const incidentFields = [
          'FirstUnitOnSceneDateTime',
          'FirstOnSceneDateTime',
          'FirstUnitOnScene',
          'FirstOnScene',
          'FirstUnitArrivedDateTime',
          'FirstArrivedDateTime',
          'FirstArrivalDateTime',
          'CallFirstUnitOnSceneDateTime',
          'CallFirstOnSceneDateTime',
          'CallFirstUnitArrivedDateTime'
        ];
        for (const field of incidentFields) {
          if (!Object.prototype.hasOwnProperty.call(incident, field)) continue;
          const date = parseIncidentDate(incident[field]);
          if (date) return date;
        }
        const timeline = incident.Timeline || incident.timeline;
        if (timeline && typeof timeline === 'object') {
          for (const key of Object.keys(timeline)) {
            if (!key || typeof key !== 'string') continue;
            if (!/scene/i.test(key)) continue;
            const date = parseIncidentDate(timeline[key]);
            if (date) return date;
          }
        }
        const units = Array.isArray(incident?.Unit) ? incident.Unit : [];
        const unitFieldCandidates = [
          'UnitOnSceneDateTime',
          'UnitArrivedDateTime',
          'UnitAtSceneDateTime',
          'OnSceneDateTime',
          'ArrivedDateTime',
          'ArrivalDateTime',
          'OnSceneTime',
          'OnSceneTimestamp',
          'Arrived'
        ];
        let earliest = null;
        units.forEach(unit => {
          if (!unit || typeof unit !== 'object') return;
          unitFieldCandidates.forEach(field => {
            if (!Object.prototype.hasOwnProperty.call(unit, field)) return;
            const date = parseIncidentDate(unit[field]);
            if (!date) return;
            if (!earliest || date.getTime() < earliest.getTime()) {
              earliest = date;
            }
          });
        });
        return earliest;
      }

      function ensureIncidentFirstOnSceneTracking(incident, incidentId) {
        if (!incident || typeof incident !== 'object') return null;
        const id = incidentId ? getNormalizedIncidentId(incidentId) : deriveIncidentLookupId(incident);
        if (!id) return null;
        const hasOnScene = incidentHasOnSceneUnits(incident);
        const existing = incidentFirstOnSceneTimes.get(id) || null;
        let timestamp = Number.isFinite(existing?.timestamp) ? existing.timestamp : null;
        let source = typeof existing?.source === 'string' ? existing.source : '';
        const serverSourceCandidates = [
          incident?._firstOnSceneTimestampSource,
          incident?.firstOnSceneTimestampSource,
          incident?.FirstOnSceneTimestampSource
        ];
        const serverSource = serverSourceCandidates.find(value => typeof value === 'string' && value.trim()) || '';
        const serverTimestampCandidates = [
          incident?._firstOnSceneTimestamp,
          incident?.firstOnSceneTimestamp,
          incident?.FirstOnSceneTimestamp
        ];
        let serverTimestamp = null;
        for (const candidate of serverTimestampCandidates) {
          const normalized = normalizeIncidentTimestampValue(candidate);
          if (Number.isFinite(normalized)) {
            serverTimestamp = normalized;
            break;
          }
        }
        if (Number.isFinite(serverTimestamp)) {
          const preferServer = !Number.isFinite(timestamp)
            || source !== 'data'
            || (typeof serverSource === 'string'
              && serverSource.trim().toLowerCase() === 'data'
              && (!Number.isFinite(timestamp) || serverTimestamp <= timestamp));
          if (preferServer) {
            timestamp = serverTimestamp;
            source = serverSource || source || '';
          }
        }
        const dataDate = extractIncidentFirstOnSceneDate(incident);
        if (dataDate instanceof Date && !Number.isNaN(dataDate.getTime())) {
          const ms = dataDate.getTime();
          if (!Number.isFinite(timestamp) || ms < timestamp || source !== 'data') {
            timestamp = ms;
            source = 'data';
          }
        }
        if (hasOnScene && Number.isFinite(timestamp)) {
          incidentFirstOnSceneTimes.set(id, { timestamp, source });
          incident._firstOnSceneTimestamp = timestamp;
          if (source) {
            incident._firstOnSceneTimestampSource = source;
          } else {
            delete incident._firstOnSceneTimestampSource;
          }
          return timestamp;
        }
        incidentFirstOnSceneTimes.delete(id);
        delete incident._firstOnSceneTimestamp;
        delete incident._firstOnSceneTimestampSource;
        return null;
      }

      function getIncidentFirstOnSceneTimestamp(incident, incidentId) {
        if (!incident || typeof incident !== 'object') return null;
        if (typeof incident._firstOnSceneTimestamp === 'number' && Number.isFinite(incident._firstOnSceneTimestamp)) {
          return incident._firstOnSceneTimestamp;
        }
        const id = incidentId ? getNormalizedIncidentId(incidentId) : deriveIncidentLookupId(incident);
        if (!id) return null;
        const entry = incidentFirstOnSceneTimes.get(id) || null;
        return entry ? entry.timestamp : null;
      }

      function getIncidentFirstOnSceneTimeInfo(incident, incidentId) {
        const timestamp = getIncidentFirstOnSceneTimestamp(incident, incidentId);
        if (!Number.isFinite(timestamp)) return null;
        const date = new Date(timestamp);
        return formatIncidentTimestamp(date);
      }

      function buildIncidentAlertDetailSignature(incident) {
        if (!incident || typeof incident !== 'object') return '';
        const parts = [];
        const identifier = getIncidentIdentifier(incident);
        if (identifier) parts.push(`id:${identifier}`);
        const typeCode = getIncidentTypeCode(incident);
        if (typeCode) parts.push(`type:${typeCode}`);
        const statusCandidates = [incident.Status, incident.IncidentStatus, incident.Stage];
        statusCandidates.forEach(value => {
          if (typeof value !== 'string') return;
          const trimmed = value.trim();
          if (trimmed) {
            parts.push(`status:${trimmed}`);
          }
        });
        const locationText = getIncidentLocationText(incident);
        if (locationText) parts.push(`loc:${locationText}`);
        const received = getIncidentTimestamp(incident);
        if (Number.isFinite(received)) parts.push(`recv:${received}`);
        const firstOnScene = getIncidentFirstOnSceneTimestamp(incident);
        if (Number.isFinite(firstOnScene)) parts.push(`firstScene:${firstOnScene}`);
        const units = extractIncidentUnits(incident);
        if (Array.isArray(units) && units.length) {
          const unitParts = units.map(unit => {
            if (!unit) return '';
            const name = typeof unit.name === 'string' ? unit.name.trim().toLowerCase() : '';
            const key = typeof unit.statusKey === 'string' ? unit.statusKey.trim().toLowerCase() : '';
            const label = typeof unit.statusLabel === 'string' ? unit.statusLabel.trim().toLowerCase() : '';
            const raw = typeof unit.rawStatus === 'string' ? unit.rawStatus.trim().toLowerCase() : '';
            return `${name}|${key}|${label}|${raw}`;
          }).filter(Boolean).sort();
          parts.push(`units:${unitParts.join(';')}`);
        }
        return parts.join('||');
      }

      function getIncidentTypeCode(incident) {
        if (!incident) return '';
        const candidates = [
          incident._markerType,
          incident.PulsePointIncidentCallTypePrimaryCode,
          incident.PulsePointIncidentCallTypeCode,
          incident.PulsePointIncidentTypeCode,
          incident.CallTypeCode,
          incident.TypeCode,
          incident.CallType,
          incident.Type,
          incident.IncidentType
        ];
        for (const value of candidates) {
          if (value === undefined || value === null) continue;
          const raw = String(value).trim();
          if (!raw) continue;
          const normalized = raw.replace(/[^A-Za-z0-9]/g, '').toUpperCase();
          if (normalized) return normalized;
        }
        return '';
      }

      function buildPulsePointListIconUrl(typeCode) {
        if (!typeCode) return '';
        const normalized = String(typeCode).trim().toLowerCase();
        if (!normalized) return '';
        return `${INCIDENT_LIST_ICON_BASE_URL}${normalized}_list.png`;
      }

      function getIncidentTypeLabel(incident) {
        const code = getIncidentTypeCode(incident);
        if (code && Object.prototype.hasOwnProperty.call(INCIDENT_TYPE_LABELS, code)) {
          return INCIDENT_TYPE_LABELS[code];
        }
        const fallbackCandidates = [
          incident?.PulsePointIncidentCallType,
          incident?.CallType,
          incident?.Type,
          incident?.IncidentType
        ];
        const fallback = fallbackCandidates.find(value => typeof value === 'string' && value.trim());
        if (fallback) return fallback.trim();
        if (code) return code;
        return 'Incident';
      }

      async function fetchPulsePointIncidents() {
        try {
          const response = await fetch(PULSEPOINT_ENDPOINT, { cache: 'no-store' });
          if (!response || !response.ok) {
            throw new Error(response ? `PulsePoint HTTP ${response.status}` : 'PulsePoint request failed');
          }
          const payload = await response.json();
          if (Array.isArray(payload)) {
            return payload;
          }
          return normalizePulsePointIncidents(payload);
        } catch (error) {
          console.error('Failed to fetch PulsePoint incidents:', error);
          return [];
        }
      }

      function parseIncidentCoordinate(value) {
        if (value === undefined || value === null || value === '') return null;
        const numeric = typeof value === 'number' ? value : Number.parseFloat(value);
        return Number.isFinite(numeric) ? numeric : null;
      }

      function getNormalizedIncidentId(value) {
        if (value === undefined || value === null) return '';
        const str = typeof value === 'string' ? value : String(value);
        const trimmed = str.trim();
        return trimmed;
      }

      function getIncidentIdentifier(rec) {
        if (!rec || typeof rec !== 'object') return null;
        const candidateKeys = [
          'ID',
          'IncidentID',
          'IncidentNumber',
          'PulsePointIncidentID',
          'PulsePointIncidentCallNumber',
          'CadIncidentNumber',
          'CADIncidentNumber'
        ];
        for (const key of candidateKeys) {
          const value = rec[key];
          if (value === undefined || value === null) continue;
          const str = String(value).trim();
          if (str) return str;
        }
        const lat = parseIncidentCoordinate(rec.Latitude ?? rec.latitude ?? rec.lat);
        const lon = parseIncidentCoordinate(rec.Longitude ?? rec.longitude ?? rec.lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
        const received = typeof rec.CallReceivedDateTime === 'string'
          ? rec.CallReceivedDateTime
          : (typeof rec.ReceivedDateTime === 'string' ? rec.ReceivedDateTime : '');
        return `${lat.toFixed(6)}_${lon.toFixed(6)}_${received}`;
      }

      function deriveIncidentLookupId(incident) {
        if (!incident || typeof incident !== 'object') return '';
        let id = getNormalizedIncidentId(getIncidentIdentifier(incident));
        if (id) return id;
        const lat = parseIncidentCoordinate(incident.Latitude ?? incident.latitude ?? incident.lat);
        const lon = parseIncidentCoordinate(incident.Longitude ?? incident.longitude ?? incident.lon);
        if (Number.isFinite(lat) && Number.isFinite(lon)) {
          id = getNormalizedIncidentId(`${lat.toFixed(6)}_${lon.toFixed(6)}`);
        }
        return id;
      }

      function createIncidentLeafletIcon(iconUrl, width, height) {
        const scale = Number.isFinite(INCIDENT_ICON_SCALE) && INCIDENT_ICON_SCALE > 0
          ? INCIDENT_ICON_SCALE
          : 1;
        const baseWidth = Number.isFinite(width) && width > 0 ? width : FALLBACK_INCIDENT_ICON_SIZE;
        const baseHeight = Number.isFinite(height) && height > 0 ? height : FALLBACK_INCIDENT_ICON_SIZE;
        const scaledWidth = Math.max(1, Math.round(baseWidth * scale));
        const scaledHeight = Math.max(1, Math.round(baseHeight * scale));
        const anchorX = Math.round(scaledWidth / 2);
        const anchorY = scaledHeight;
        return L.icon({
          iconUrl,
          iconSize: [scaledWidth, scaledHeight],
          iconAnchor: [anchorX, anchorY],
          className: 'incident-marker-icon'
        });
      }

      function getIncidentIconEntry(iconUrl) {
        if (!iconUrl) return null;
        let entry = incidentIconCache.get(iconUrl);
        if (!entry) {
          const fallback = FALLBACK_INCIDENT_ICON_SIZE;
          entry = {
            icon: createIncidentLeafletIcon(iconUrl, fallback, fallback),
            markers: new Set(),
            loaded: false
          };
          incidentIconCache.set(iconUrl, entry);
          const img = new Image();
          img.decoding = 'async';
          img.addEventListener('load', () => {
            const width = img.naturalWidth || fallback;
            const height = img.naturalHeight || fallback;
            entry.icon = createIncidentLeafletIcon(iconUrl, width, height);
            entry.loaded = true;
            entry.markers.forEach(marker => {
              if (marker && typeof marker.setIcon === 'function') {
                marker.setIcon(entry.icon);
              }
            });
            applyIncidentHaloStates();
          });
          img.addEventListener('error', () => {
            entry.loaded = true;
          });
          img.src = iconUrl;
        }
        return entry;
      }

      function assignIncidentIcon(marker, iconUrl) {
        if (!marker || !iconUrl) return;
        const entry = getIncidentIconEntry(iconUrl);
        if (!entry) return;
        entry.markers.add(marker);
        marker.setIcon(entry.icon);
      }

      function releaseIncidentIcon(marker, iconUrl) {
        if (!marker || !iconUrl) return;
        const entry = incidentIconCache.get(iconUrl);
        if (entry && entry.markers) {
          entry.markers.delete(marker);
        }
      }

      function getIncidentMarkerIconSize(marker) {
        if (!marker) return null;
        const icon = marker.options && marker.options.icon ? marker.options.icon : null;
        if (!icon || !icon.options) return null;
        const size = icon.options.iconSize;
        if (Array.isArray(size) && size.length >= 2) {
          const width = Number(size[0]);
          const height = Number(size[1]);
          if (Number.isFinite(width) && Number.isFinite(height)) {
            return { width, height };
          }
        }
        return null;
      }

      function getIncidentHaloIcon(markerHeight) {
        const diameter = HALO_MAX_RADIUS_PX * 2;
        const safeHeight = Number.isFinite(markerHeight) && markerHeight > 0
          ? markerHeight
          : DEFAULT_SCALED_INCIDENT_ICON_HEIGHT;
        const key = safeHeight.toFixed(2);
        let icon = incidentHaloIconCache.get(key);
        if (!icon) {
          const offsetX = Number.isFinite(MARKER_CENTROID_OFFSET_X_FACTOR)
            ? MARKER_CENTROID_OFFSET_X_FACTOR * safeHeight
            : 0;
          const offsetY = Number.isFinite(MARKER_CENTROID_OFFSET_Y_FACTOR)
            ? MARKER_CENTROID_OFFSET_Y_FACTOR * safeHeight
            : 0;
          const anchorX = diameter / 2 - offsetX;
          const anchorY = diameter / 2 - offsetY;
          const minScale = HALO_MAX_RADIUS_PX > 0
            ? Math.max(0, Math.min(1, HALO_MIN_RADIUS_PX / HALO_MAX_RADIUS_PX))
            : 0.25;
          const html = `<div class="incident-halo" style="--incident-halo-diameter:${diameter}px;--incident-halo-base-opacity:${HALO_BASE_OPACITY};--incident-halo-duration:${HALO_DURATION_MS}ms;--incident-halo-start-scale:${minScale};--incident-halo-color-rgb:${HALO_COLOR_RGB};"></div>`;
          icon = L.divIcon({
            className: 'incident-halo-icon',
            iconSize: [diameter, diameter],
            iconAnchor: [anchorX, anchorY],
            html
          });
          incidentHaloIconCache.set(key, icon);
        }
        return icon;
      }

      function createIncidentHaloMarker(latLng, markerHeight) {
        const haloIcon = getIncidentHaloIcon(markerHeight);
        return L.marker(latLng, {
          icon: haloIcon,
          pane: 'incidentHalosPane',
          interactive: false,
          keyboard: false,
          bubblingMouseEvents: false
        });
      }

      function ensureIncidentHaloLayerGroup() {
        if (!map) return null;
        if (!incidentHaloLayerGroup) {
          incidentHaloLayerGroup = L.layerGroup();
        }
        const shouldShowHalos = shouldShowIncidentLayer() && hasIncidentsRequiringVisibility();
        if (shouldShowHalos) {
          if (!map.hasLayer(incidentHaloLayerGroup)) {
            incidentHaloLayerGroup.addTo(map);
          }
        } else if (map.hasLayer(incidentHaloLayerGroup)) {
          map.removeLayer(incidentHaloLayerGroup);
        }
        return incidentHaloLayerGroup;
      }

      function isReducedMotionPreferred() {
        return !!(reduceMotionMediaQuery && typeof reduceMotionMediaQuery.matches === 'boolean' && reduceMotionMediaQuery.matches);
      }

      function applyHaloAnimationState(entry, animated) {
        if (!entry || !entry.haloMarker) return;
        const haloMarker = entry.haloMarker;
        const update = () => {
          const element = haloMarker.getElement();
          if (!element) return;
          const haloElement = element.querySelector('.incident-halo');
          if (!haloElement) return;
          if (animated) {
            haloElement.classList.add('incident-halo--animated');
            haloElement.classList.remove('incident-halo--static');
          } else {
            haloElement.classList.add('incident-halo--static');
            haloElement.classList.remove('incident-halo--animated');
          }
        };
        update();
        setTimeout(update, 0);
      }

      function removeIncidentHalo(entry) {
        if (!entry || !entry.haloMarker) return;
        const haloMarker = entry.haloMarker;
        if (incidentHaloLayerGroup && incidentHaloLayerGroup.hasLayer(haloMarker)) {
          incidentHaloLayerGroup.removeLayer(haloMarker);
        } else if (haloMarker && typeof haloMarker.remove === 'function' && haloMarker._map) {
          haloMarker.remove();
        }
        entry.haloMarker = null;
        entry.haloAnimated = false;
      }

      function syncIncidentHaloForEntry(id, entry) {
        if (!entry || !entry.marker) return;
        if (!incidentsNearRoutesLookup.has(id)) {
          removeIncidentHalo(entry);
          return;
        }
        const haloGroup = ensureIncidentHaloLayerGroup();
        if (!haloGroup) return;
        const latLng = entry.marker.getLatLng();
        if (!latLng) return;
        const size = getIncidentMarkerIconSize(entry.marker);
        const markerHeight = size && Number.isFinite(size.height) ? size.height : DEFAULT_SCALED_INCIDENT_ICON_HEIGHT;
        const desiredIcon = getIncidentHaloIcon(markerHeight);
        if (entry.haloMarker) {
          entry.haloMarker.setLatLng(latLng);
          if (entry.haloMarker.options && entry.haloMarker.options.icon !== desiredIcon) {
            entry.haloMarker.setIcon(desiredIcon);
          }
          if (!haloGroup.hasLayer(entry.haloMarker)) {
            haloGroup.addLayer(entry.haloMarker);
          }
        } else {
          const haloMarker = createIncidentHaloMarker(latLng, markerHeight);
          entry.haloMarker = haloMarker;
          haloGroup.addLayer(haloMarker);
        }
      }

      function applyIncidentHaloStates() {
        if (!map) return;
        const haloGroup = ensureIncidentHaloLayerGroup();
        if (!haloGroup) return;
        const nearRouteList = Array.isArray(incidentsNearRoutes) ? incidentsNearRoutes : [];
        const orderedIds = [];
        nearRouteList.forEach(entry => {
          if (!entry) return;
          const candidateId = typeof entry.id === 'string' ? entry.id : getNormalizedIncidentId(entry.id);
          if (candidateId && !orderedIds.includes(candidateId)) {
            orderedIds.push(candidateId);
          }
        });
        const reduceMotion = isReducedMotionPreferred();
        const animatedIds = reduceMotion
          ? new Set()
          : new Set(orderedIds.slice(0, Math.max(0, INCIDENT_HALO_ANIMATED_LIMIT)));
        incidentMarkers.forEach((entry, id) => {
          if (!incidentsNearRoutesLookup.has(id)) {
            removeIncidentHalo(entry);
            return;
          }
          syncIncidentHaloForEntry(id, entry);
          const animate = animatedIds.has(id) && !reduceMotion;
          entry.haloAnimated = animate;
          applyHaloAnimationState(entry, animate);
        });
      }

      if (reduceMotionMediaQuery) {
        const handleReduceMotionChange = () => {
          updateLowPerformanceMode();
          applyIncidentHaloStates();
        };
        if (typeof reduceMotionMediaQuery.addEventListener === 'function') {
          reduceMotionMediaQuery.addEventListener('change', handleReduceMotionChange);
        } else if (typeof reduceMotionMediaQuery.addListener === 'function') {
          reduceMotionMediaQuery.addListener(handleReduceMotionChange);
        }
      }

      function updateIncidentMarkerTooltip(marker, incident) {
        if (!marker || !incident) return;
        const lines = [];
        const callType = incident.PulsePointIncidentCallType || incident.CallType || incident.Type || '';
        const address = incident.FullDisplayAddress || incident.DisplayAddress || incident.Address || '';
        if (callType) lines.push(callType);
        if (address) {
          if (!lines.length || address.trim().toLowerCase() !== lines[0].trim().toLowerCase()) {
            lines.push(address);
          }
        }
        if (!lines.length) {
          if (typeof marker.unbindTooltip === 'function') {
            marker.unbindTooltip();
          }
          return;
        }
        const tooltipText = lines.join('\n');
        if (typeof marker.getTooltip === 'function') {
          const tooltip = marker.getTooltip();
          if (tooltip && typeof tooltip.setContent === 'function') {
            tooltip.setContent(tooltipText);
            return;
          }
        }
        if (typeof marker.bindTooltip === 'function') {
          marker.bindTooltip(tooltipText, { direction: 'top', offset: [0, -4], sticky: true });
        }
      }

      function updateIncidentsNearRoutes(matches, signature = '') {
        const list = Array.isArray(matches) ? matches.slice() : [];
        incidentsNearRoutes = list;
        const lookup = new Map();
        list.forEach(entry => {
          if (!entry) return;
          const candidateId = typeof entry.id === 'string' ? entry.id : getNormalizedIncidentId(entry.id);
          if (candidateId) {
            if (entry.incident) {
              ensureIncidentFirstOnSceneTracking(entry.incident, candidateId);
              if (!Number.isFinite(entry.firstOnSceneTimestamp)) {
                entry.firstOnSceneTimestamp = getIncidentFirstOnSceneTimestamp(entry.incident, candidateId);
              }
              if (typeof entry.detailSignature !== 'string' || !entry.detailSignature) {
                entry.detailSignature = buildIncidentAlertDetailSignature(entry.incident);
              }
            }
            lookup.set(candidateId, entry);
          }
        });
        const previousSignature = incidentRouteAlertSignature;
        incidentsNearRoutesLookup = lookup;
        incidentRouteAlertSignature = typeof signature === 'string' ? signature : '';
        const shouldRefreshIncidentMarkers = previousSignature !== incidentRouteAlertSignature
          || (lookup && lookup.size > 0);
        if (shouldRefreshIncidentMarkers) {
          applyIncidentMarkers(latestActiveIncidents);
        }
        applyIncidentHaloStates();
        refreshOpenIncidentPopups();
        maintainIncidentLayers();
      }

      function applyIncidentMarkers(incidents) {
        if (!map) return;
        if (!incidentLayerGroup) {
          incidentLayerGroup = L.layerGroup();
        }
        const layerGroup = incidentLayerGroup;
        const shouldShowLayer = shouldShowIncidentLayer();
        if (shouldShowLayer) {
          if (!map.hasLayer(layerGroup)) {
            layerGroup.addTo(map);
          }
        } else if (map.hasLayer(layerGroup)) {
          map.removeLayer(layerGroup);
        }
        const activeIds = new Set();
        (Array.isArray(incidents) ? incidents : []).forEach(incident => {
          if (!incident) return;
          const lat = parseIncidentCoordinate(incident.Latitude ?? incident.latitude ?? incident.lat);
          const lon = parseIncidentCoordinate(incident.Longitude ?? incident.longitude ?? incident.lon);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
          const fallbackId = getNormalizedIncidentId(`${lat.toFixed(6)}_${lon.toFixed(6)}`);
          let id = getNormalizedIncidentId(getIncidentIdentifier(incident));
          if (!id) {
            id = fallbackId;
          }
          if (!id) return;
          const firstOnSceneTimestamp = ensureIncidentFirstOnSceneTracking(incident, id);
          const markerUrl = incident._markerUrl;
          if (!markerUrl) return;
          const isPinned = incidentsNearRoutesLookup.has(id);
          if (!incidentsVisible) {
            if (!shouldShowLayer || !isPinned) {
              return;
            }
          }
          activeIds.add(id);
          const existing = incidentMarkers.get(id);
          if (existing && existing.marker) {
            existing.marker.setLatLng([lat, lon]);
            if (existing.iconUrl !== markerUrl) {
              releaseIncidentIcon(existing.marker, existing.iconUrl);
              assignIncidentIcon(existing.marker, markerUrl);
              existing.iconUrl = markerUrl;
            }
            updateIncidentMarkerTooltip(existing.marker, incident);
            existing.data = incident;
            existing.firstOnSceneTimestamp = Number.isFinite(firstOnSceneTimestamp)
              ? firstOnSceneTimestamp
              : getIncidentFirstOnSceneTimestamp(incident, id);
            refreshIncidentPopup(id);
          } else {
            const marker = L.marker([lat, lon], {
              pane: 'incidentsPane',
              keyboard: false,
              zIndexOffset: 200
            });
            assignIncidentIcon(marker, markerUrl);
            updateIncidentMarkerTooltip(marker, incident);
            marker.addTo(layerGroup);
            incidentMarkers.set(id, {
              marker,
              data: incident,
              iconUrl: markerUrl,
              haloMarker: null,
              haloAnimated: false,
              firstOnSceneTimestamp: Number.isFinite(firstOnSceneTimestamp)
                ? firstOnSceneTimestamp
                : getIncidentFirstOnSceneTimestamp(incident, id)
            });
            marker.on('click', () => {
              const config = buildIncidentPopupConfig(id);
              if (config) {
                createCustomPopup(config);
              }
            });
            refreshIncidentPopup(id);
          }
        });
        const idsToRemove = [];
        incidentMarkers.forEach((entry, id) => {
          if (!activeIds.has(id)) {
            idsToRemove.push(id);
          }
        });
        idsToRemove.forEach(id => {
          const entry = incidentMarkers.get(id);
          if (!entry) return;
          releaseIncidentIcon(entry.marker, entry.iconUrl);
          if (incidentLayerGroup && entry.marker) {
            incidentLayerGroup.removeLayer(entry.marker);
          } else if (map && entry.marker && map.hasLayer(entry.marker)) {
            map.removeLayer(entry.marker);
          }
          removeIncidentHalo(entry);
          removeIncidentPopupById(id);
          incidentMarkers.delete(id);
          incidentFirstOnSceneTimes.delete(id);
        });
        applyIncidentHaloStates();
        maintainIncidentLayers();
      }

      function ensureRouteProjectedPath(entry) {
        if (!entry) return null;
        const latLngPath = Array.isArray(entry.latLngPath) ? entry.latLngPath : null;
        if (!latLngPath || latLngPath.length < 2) return null;
        const existing = Array.isArray(entry.projectedPath) ? entry.projectedPath : null;
        if (existing && existing.length === latLngPath.length && existing.length >= 2) {
          return existing;
        }
        if (typeof L === 'undefined' || !L.Projection || !L.Projection.SphericalMercator) return null;
        entry.projectedPath = latLngPath.map(point => L.Projection.SphericalMercator.project(point));
        return entry.projectedPath;
      }

      function ensureCatProjectedPath(geometry) {
        if (!geometry) return null;
        const latLngs = Array.isArray(geometry.latLngs) ? geometry.latLngs : null;
        if (!latLngs || latLngs.length < 2) return null;
        const existing = Array.isArray(geometry.projectedPath) ? geometry.projectedPath : null;
        if (existing && existing.length === latLngs.length && existing.length >= 2) {
          return existing;
        }
        if (typeof L === 'undefined' || !L.Projection || !L.Projection.SphericalMercator) return null;
        geometry.projectedPath = latLngs.map(point => L.Projection.SphericalMercator.project(point));
        return geometry.projectedPath;
      }

      function computeDistanceFromProjectedPointToSegmentMeters(point, a, b) {
        if (!point || !a || !b) return Infinity;
        const ax = a.x;
        const ay = a.y;
        const bx = b.x;
        const by = b.y;
        if (!Number.isFinite(ax) || !Number.isFinite(ay) || !Number.isFinite(bx) || !Number.isFinite(by)) {
          return Infinity;
        }
        const dx = bx - ax;
        const dy = by - ay;
        if (dx === 0 && dy === 0) {
          const diffX = point.x - ax;
          const diffY = point.y - ay;
          return Math.sqrt(diffX * diffX + diffY * diffY);
        }
        const t = ((point.x - ax) * dx + (point.y - ay) * dy) / (dx * dx + dy * dy);
        const clamped = Math.max(0, Math.min(1, t));
        const projX = ax + clamped * dx;
        const projY = ay + clamped * dy;
        const diffX = point.x - projX;
        const diffY = point.y - projY;
        return Math.sqrt(diffX * diffX + diffY * diffY);
      }

      function computeDistanceFromProjectedPointToPathMeters(point, projectedPath) {
        if (!point || !Array.isArray(projectedPath) || projectedPath.length < 2) {
          return Infinity;
        }
        let minDistance = Infinity;
        for (let i = 0; i < projectedPath.length - 1; i += 1) {
          const segmentStart = projectedPath[i];
          const segmentEnd = projectedPath[i + 1];
          const distance = computeDistanceFromProjectedPointToSegmentMeters(point, segmentStart, segmentEnd);
          if (!Number.isFinite(distance)) continue;
          if (distance < minDistance) {
            minDistance = distance;
          }
        }
        return minDistance;
      }

      function getRouteDisplayName(routeId) {
        const numericRouteId = Number(routeId);
        const candidates = [];
        if (Number.isFinite(numericRouteId) && allRoutes && allRoutes[numericRouteId]) {
          candidates.push(allRoutes[numericRouteId]);
        }
        if (allRoutes && Object.prototype.hasOwnProperty.call(allRoutes, routeId)) {
          candidates.push(allRoutes[routeId]);
        }
        const record = candidates.find(Boolean) || null;
        if (record) {
          const nameCandidates = [
            record.Description,
            record.RouteName,
            record.Name,
            record.LongName,
            record.ShortName
          ];
          const name = nameCandidates.find(value => typeof value === 'string' && value.trim());
          if (name) return name.trim();
        }
        if (Number.isFinite(numericRouteId)) {
          return `Route ${numericRouteId}`;
        }
        if (typeof routeId === 'string' && routeId.trim()) {
          return routeId.trim();
        }
        return 'Route';
      }

      function evaluateIncidentRouteAlerts() {
        if (demoIncidentActive && demoIncidentEntry) {
          return;
        }
        const hadAlerts = incidentRouteAlertSignature !== '' || (Array.isArray(incidentsNearRoutes) && incidentsNearRoutes.length > 0);
        if (!incidentsAreAvailable()) {
          if (hadAlerts) {
            resetIncidentAlertState();
            updateControlPanel();
          } else {
            resetIncidentAlertState();
          }
          return;
        }
        if (typeof L === 'undefined' || !L.Projection || !L.Projection.SphericalMercator) {
          return;
        }
        const activeRecords = Array.isArray(latestActiveIncidents) ? latestActiveIncidents : [];
        if (activeRecords.length === 0) {
          if (hadAlerts) {
            resetIncidentAlertState();
            updateControlPanel();
          } else {
            resetIncidentAlertState();
          }
          return;
        }
        const routeEntries = [];
        routePolylineCache.forEach((entry, key) => {
          const numericRouteId = Number(key);
          if (!Number.isFinite(numericRouteId) || numericRouteId === 0) return;
          const projectedPath = ensureRouteProjectedPath(entry);
          if (!projectedPath || projectedPath.length < 2) return;
          routeEntries.push({
            routeId: numericRouteId,
            rawRouteId: numericRouteId,
            projectedPath,
            source: 'transloc'
          });
        });
        if (catOverlayEnabled) {
          catRoutePatternGeometries.forEach(geometry => {
            if (!geometry || !Array.isArray(geometry.latLngs) || geometry.latLngs.length < 2) {
              return;
            }
            const routeKey = catRouteKey(geometry.routeKey);
            if (!routeKey || routeKey === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
              return;
            }
            if (!isCatRouteAllowedInCurrentMode(routeKey)) {
              return;
            }
            const projectedPath = ensureCatProjectedPath(geometry);
            if (!projectedPath || projectedPath.length < 2) {
              return;
            }
            const color = sanitizeCssColor(geometry.color)
              || sanitizeCssColor(getCatRouteColor(routeKey))
              || '';
            const info = getCatRouteInfo(routeKey) || null;
            const nameCandidates = [
              info?.displayName,
              info?.shortName,
              info?.longName,
              routeKey ? `Route ${routeKey}` : ''
            ];
            let displayName = '';
            for (const candidate of nameCandidates) {
              if (typeof candidate !== 'string') continue;
              const trimmed = candidate.trim();
              if (trimmed) {
                displayName = trimmed;
                break;
              }
            }
            routeEntries.push({
              routeId: `cat:${routeKey}`,
              rawRouteId: routeKey,
              projectedPath,
              source: 'cat',
              routeKey,
              color,
              name: displayName
            });
          });
        }
        if (routeEntries.length === 0) {
          if (hadAlerts) {
            resetIncidentAlertState();
            updateControlPanel();
          }
          return;
        }
        const projection = L.Projection.SphericalMercator;
        const threshold = Number.isFinite(INCIDENT_ROUTE_PROXIMITY_THRESHOLD_METERS) && INCIDENT_ROUTE_PROXIMITY_THRESHOLD_METERS >= 0
          ? INCIDENT_ROUTE_PROXIMITY_THRESHOLD_METERS
          : 0;
        const matches = [];
        activeRecords.forEach(incident => {
          const lat = parseIncidentCoordinate(incident?.Latitude ?? incident?.latitude ?? incident?.lat);
          const lon = parseIncidentCoordinate(incident?.Longitude ?? incident?.longitude ?? incident?.lon);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
          if (!incidentHasOnSceneOrEnRouteUnits(incident)) return;
          const incidentLatLng = L.latLng(lat, lon);
          const projectedPoint = projection.project(incidentLatLng);
          if (!projectedPoint) return;
          const matchedRoutes = [];
          const seenRoutes = new Set();
          let closestDistance = Infinity;
          routeEntries.forEach(entry => {
            if (!entry || !Array.isArray(entry.projectedPath) || entry.projectedPath.length < 2) {
              return;
            }
            const routeId = entry.routeId;
            const rawRouteId = entry.rawRouteId;
            const projectedPath = entry.projectedPath;
            const source = entry.source;
            const routeKey = entry.routeKey;
            const presetColor = entry.color;
            const presetName = entry.name;
            const distance = computeDistanceFromProjectedPointToPathMeters(projectedPoint, projectedPath);
            if (!Number.isFinite(distance)) return;
            if (distance < closestDistance) {
              closestDistance = distance;
            }
            if (distance <= threshold && !seenRoutes.has(routeId)) {
              seenRoutes.add(routeId);
              const colorCandidates = [];
              if (source === 'cat') {
                if (presetColor) {
                  colorCandidates.push(presetColor);
                }
                const fallbackCatColor = sanitizeCssColor(getCatRouteColor(routeKey || rawRouteId));
                if (fallbackCatColor) {
                  colorCandidates.push(fallbackCatColor);
                }
              } else {
                const lookupCandidates = [];
                if (rawRouteId !== undefined && rawRouteId !== null) {
                  lookupCandidates.push(rawRouteId);
                }
                if (routeId !== undefined && routeId !== null && routeId !== rawRouteId) {
                  lookupCandidates.push(routeId);
                }
                lookupCandidates.forEach(idCandidate => {
                  if (routeColors && typeof routeColors[idCandidate] === 'string') {
                    colorCandidates.push(routeColors[idCandidate]);
                  }
                  const numericCandidate = Number(idCandidate);
                  if (Number.isFinite(numericCandidate) && routeColors && typeof routeColors[numericCandidate] === 'string') {
                    colorCandidates.push(routeColors[numericCandidate]);
                  }
                });
                const numericRouteId = Number(rawRouteId);
                const storedRouteCandidates = [];
                if (Number.isFinite(numericRouteId) && allRoutes) {
                  storedRouteCandidates.push(allRoutes[numericRouteId], allRoutes[`${numericRouteId}`]);
                }
                if (rawRouteId !== undefined && rawRouteId !== null && allRoutes && Object.prototype.hasOwnProperty.call(allRoutes, rawRouteId)) {
                  storedRouteCandidates.push(allRoutes[rawRouteId]);
                }
                storedRouteCandidates.forEach(storedRoute => {
                  if (!storedRoute) return;
                  const storedColorCandidates = [storedRoute.MapLineColor, storedRoute.RouteColor, storedRoute.Color, storedRoute.color];
                  storedColorCandidates.forEach(value => {
                    if (typeof value === 'string') {
                      colorCandidates.push(value);
                    }
                  });
                });
              }
              let matchedColor = '';
              for (const candidate of colorCandidates) {
                const sanitized = sanitizeCssColor(candidate);
                if (sanitized) {
                  matchedColor = sanitized;
                  break;
                }
              }
              let routeName = '';
              if (source === 'cat') {
                if (typeof presetName === 'string' && presetName.trim()) {
                  routeName = presetName.trim();
                } else {
                  const info = getCatRouteInfo(routeKey || rawRouteId);
                  if (info) {
                    const infoNameCandidates = [info.displayName, info.shortName, info.longName];
                    for (const value of infoNameCandidates) {
                      if (typeof value !== 'string') continue;
                      const trimmed = value.trim();
                      if (trimmed) {
                        routeName = trimmed;
                        break;
                      }
                    }
                  }
                  if (!routeName && rawRouteId) {
                    routeName = `Route ${rawRouteId}`;
                  }
                }
              } else {
                routeName = getRouteDisplayName(rawRouteId ?? routeId);
              }
              matchedRoutes.push({
                routeId,
                rawRouteId: rawRouteId ?? routeId,
                routeKey: routeKey || null,
                name: routeName,
                distance,
                color: matchedColor
              });
            }
          });
          if (!matchedRoutes.length) return;
          matchedRoutes.sort((a, b) => (a.distance || 0) - (b.distance || 0));
          let id = getIncidentIdentifier(incident);
          if (!id) {
            id = `${lat.toFixed(6)}_${lon.toFixed(6)}`;
          }
          id = getNormalizedIncidentId(id);
          if (!id) return;
          const timestamp = getIncidentTimestamp(incident) ?? 0;
          const firstOnSceneTimestamp = ensureIncidentFirstOnSceneTracking(incident, id);
          const normalizedFirstOnScene = Number.isFinite(firstOnSceneTimestamp)
            ? firstOnSceneTimestamp
            : getIncidentFirstOnSceneTimestamp(incident, id);
          const detailSignature = buildIncidentAlertDetailSignature(incident);
          matches.push({
            id,
            incident,
            routes: matchedRoutes,
            closestDistance,
            timestamp,
            firstOnSceneTimestamp: normalizedFirstOnScene,
            detailSignature
          });
        });
        if (!matches.length) {
          if (hadAlerts) {
            resetIncidentAlertState();
            updateControlPanel();
          }
          return;
        }
        matches.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
        const signature = matches.map(match => {
          const routePart = match.routes.map(route => route.routeId).join(',');
          const distancePart = Number.isFinite(match.closestDistance) ? Math.round(match.closestDistance) : 'x';
          const detailPart = typeof match.detailSignature === 'string' ? match.detailSignature : '';
          return `${match.id || ''}:${routePart}:${distancePart}:${detailPart}`;
        }).join('|');
        if (signature !== incidentRouteAlertSignature) {
          updateIncidentsNearRoutes(matches, signature);
          updateControlPanel();
        }
      }

      async function refreshIncidents() {
        if (!incidentsAreAvailable()) {
          setIncidentsVisibility(false);
          return;
        }
        if (demoIncidentActive && demoIncidentEntry) {
          return;
        }
        if (!map || isFetchingIncidents) return;
        isFetchingIncidents = true;
        try {
          const records = await fetchPulsePointIncidents();
          const activeRecords = Array.isArray(records)
            ? records.filter(record => (record._category || '').toLowerCase() === 'active')
            : [];
          latestActiveIncidents = activeRecords;
          evaluateIncidentRouteAlerts();
          applyIncidentMarkers(activeRecords);
          updateControlPanel();
        } catch (error) {
          console.error('Failed to refresh PulsePoint incidents', error);
        } finally {
          isFetchingIncidents = false;
        }
      }

      function setIncidentsVisibility(visible) {
        const allowIncidents = incidentsAreAvailable();
        const hadAlerts = incidentRouteAlertSignature !== '' || (Array.isArray(incidentsNearRoutes) && incidentsNearRoutes.length > 0);
        incidentsVisible = allowIncidents && !!visible;

        if (!incidentsVisible && demoIncidentActive) {
          deactivateDemoIncidentPreview({ preserveVisibility: true });
        }

        if (!allowIncidents) {
          if (hadAlerts || (Array.isArray(latestActiveIncidents) && latestActiveIncidents.length > 0)) {
            resetIncidentAlertState();
            updateControlPanel();
          } else {
            resetIncidentAlertState();
          }
          removeAllIncidentPopups();
          maintainIncidentLayers();
          updateIncidentToggleButton();
          return;
        }

        incidentsVisibilityPreference = incidentsVisible;

        applyIncidentMarkers(latestActiveIncidents);
        if (incidentsVisible && incidentMarkers.size === 0 && !isFetchingIncidents) {
          refreshIncidents();
        } else {
          maintainIncidentLayers();
        }
        updateIncidentToggleButton();
        applyIncidentHaloStates();
        if (!incidentsVisible) {
          removeAllIncidentPopups();
        }
      }

      async function activateDemoIncidentPreview() {
        if (demoIncidentActive) return;
        const row = await ensureDemoIncidentRow();
        if (!row) {
          if (typeof window !== 'undefined' && typeof window.alert === 'function') {
            window.alert('Demo incident data is unavailable.');
          }
          return;
        }
        const entry = createDemoIncidentEntry();
        if (!entry || !entry.incident) {
          console.warn('Demo incident data is missing required fields.', entry);
          if (typeof window !== 'undefined' && typeof window.alert === 'function') {
            window.alert('Demo incident data is missing required fields.');
          }
          return;
        }
        demoIncidentEntry = entry;
        demoIncidentPreviousVisibility = incidentsVisible;
        demoIncidentActive = true;
        updateIncidentsNearRoutes([entry], entry._demoSignature || entry.id || 'demo');
        incidentsVisible = true;
        incidentsVisibilityPreference = true;
        if (!incidentLayerGroup && typeof L !== 'undefined' && typeof L.layerGroup === 'function') {
          incidentLayerGroup = L.layerGroup();
        }
        if (map && incidentLayerGroup && typeof incidentLayerGroup.addTo === 'function') {
          incidentLayerGroup.addTo(map);
        }
        if (entry.incident) {
          applyIncidentMarkers([entry.incident]);
        }
        if (!isDispatcherLockActive() && entry.incident && Number.isFinite(entry.incident.Latitude) && Number.isFinite(entry.incident.Longitude) && map && typeof map.setView === 'function') {
          try {
            const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : 0;
            const targetZoom = Number.isFinite(currentZoom) ? Math.max(currentZoom, 15) : 15;
            map.setView([entry.incident.Latitude, entry.incident.Longitude], targetZoom, { animate: true });
          } catch (error) {
            console.warn('Unable to move map to the demo incident location.', error);
          }
        }
        updateControlPanel();
        updateIncidentToggleButton();
      }

      function deactivateDemoIncidentPreview(options = {}) {
        if (!demoIncidentActive) {
          demoIncidentPreviousVisibility = null;
          return;
        }
        const preserveVisibility = options && options.preserveVisibility;
        demoIncidentActive = false;
        demoIncidentEntry = null;
        resetIncidentAlertState();
        applyIncidentMarkers([]);
        if (!preserveVisibility) {
          if (demoIncidentPreviousVisibility !== null) {
            incidentsVisible = !!demoIncidentPreviousVisibility;
            incidentsVisibilityPreference = incidentsVisible;
            if (map && incidentLayerGroup) {
              if (incidentsVisible) {
                incidentLayerGroup.addTo(map);
              } else {
                map.removeLayer(incidentLayerGroup);
              }
            }
          }
        }
        demoIncidentPreviousVisibility = null;
        updateControlPanel();
        updateIncidentToggleButton();
      }

      async function toggleDemoIncident() {
        if (demoIncidentActive) {
          deactivateDemoIncidentPreview();
        } else {
          await activateDemoIncidentPreview();
        }
      }

      function enforceIncidentVisibilityForCurrentAgency() {
        if (incidentsAreAvailable()) {
          setIncidentsVisibility(incidentsVisibilityPreference);
        } else {
          setIncidentsVisibility(false);
        }
      }

      function toggleIncidentsVisibility() {
        if (!incidentsAreAvailable()) return;
        setIncidentsVisibility(!incidentsVisible);
      }

      function setIncludeStaleVehicles(value) {
        const nextValue = !!value;
        if (includeStaleVehicles === nextValue) {
          updateStaleVehiclesButton();
          return;
        }
        includeStaleVehicles = nextValue;
        resetTranslocSnapshotCache();
        busLocationsFetchPromise = null;
        busLocationsFetchBaseURL = null;
        routePathsFetchPromise = null;
        routePathsFetchBaseURL = null;
        fetchRouteColors();
        fetchBusStops();
        fetchBlockAssignments();
        fetchBusLocations().then(() => fetchRoutePaths());
        fetchStopArrivalTimes()
          .then(allEtas => {
            cachedEtas = allEtas || {};
            updateCustomPopups();
          });
        if (shouldPollOnDemandData()) {
          fetchOnDemandVehicles();
        }
        updateStaleVehiclesButton();
      }

      function toggleStaleVehicles() {
        setIncludeStaleVehicles(!includeStaleVehicles);
      }

      function updateOnDemandButton() {
        const button = document.getElementById('onDemandToggleButton');
        if (!button) return;
        const authorized = userIsAuthorizedForOnDemand();
        if (typeof button.disabled === 'boolean') {
          button.disabled = !authorized;
        }
        if (!authorized) {
          button.setAttribute('aria-disabled', 'true');
        } else {
          button.removeAttribute('aria-disabled');
        }
        const isActive = !!onDemandVehiclesEnabled;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function updateOnDemandStopsButton() {
        const button = document.getElementById('onDemandStopsToggleButton');
        if (!button) return;
        const authorized = userIsAuthorizedForOnDemand();
        if (typeof button.disabled === 'boolean') {
          button.disabled = !authorized;
        }
        if (!authorized) {
          button.setAttribute('aria-disabled', 'true');
        } else {
          button.removeAttribute('aria-disabled');
        }
        const isActive = !!onDemandStopsEnabled;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function shouldPollOnDemandData() {
        if (!userIsAuthorizedForOnDemand()) {
          return false;
        }
        return !!(onDemandVehiclesEnabled || onDemandStopsEnabled);
      }

      function updateOnDemandVehicleColorMap(vehicles) {
        onDemandVehicleColorMap.clear();
        if (!Array.isArray(vehicles)) {
          return;
        }
        vehicles.forEach(vehicle => {
          if (!vehicle) {
            return;
          }
          const rawId = vehicle.vehicleId ?? vehicle.vehicleID;
          const vehicleId = typeof rawId === 'string' ? rawId.trim() : `${rawId || ''}`.trim();
          if (!vehicleId) {
            return;
          }
          const color = sanitizeCssColor(vehicle.markerColor) || ONDEMAND_MARKER_DEFAULT_COLOR;
          onDemandVehicleColorMap.set(vehicleId, color);
        });
      }

      function getOnDemandVehicleColor(vehicleId) {
        const normalized = typeof vehicleId === 'string' ? vehicleId.trim() : `${vehicleId || ''}`.trim();
        if (!normalized) {
          return ONDEMAND_MARKER_DEFAULT_COLOR;
        }
        return onDemandVehicleColorMap.get(normalized) || ONDEMAND_MARKER_DEFAULT_COLOR;
      }

      function isOnDemandVehicleId(vehicleID) {
        if (vehicleID === undefined || vehicleID === null) {
          return false;
        }
        return `${vehicleID}`.startsWith(ONDEMAND_MARKER_PREFIX);
      }

      function extractOnDemandDisplayName(callName) {
        if (typeof callName !== 'string') {
          return '';
        }
        const trimmed = callName.trim();
        if (!trimmed) {
          return '';
        }
        const colonIndex = trimmed.indexOf(':');
        if (colonIndex > 0) {
          return trimmed.slice(0, colonIndex).trim();
        }
        return trimmed;
      }

      function clearOnDemandVehicles() {
        if (!map || typeof map.removeLayer !== 'function') {
          Object.keys(markers).forEach(vehicleID => {
            if (isOnDemandVehicleId(vehicleID)) {
              delete markers[vehicleID];
              clearBusMarkerState(vehicleID);
            }
          });
          Object.keys(nameBubbles).forEach(vehicleID => {
            if (isOnDemandVehicleId(vehicleID)) {
              delete nameBubbles[vehicleID];
            }
          });
          return;
        }

        Object.keys(markers).forEach(vehicleID => {
          if (!isOnDemandVehicleId(vehicleID)) {
            return;
          }
          const marker = markers[vehicleID];
          if (marker) {
            map.removeLayer(marker);
          }
          delete markers[vehicleID];
          clearBusMarkerState(vehicleID);
        });

        Object.keys(nameBubbles).forEach(vehicleID => {
          if (!isOnDemandVehicleId(vehicleID)) {
            return;
          }
          const bubble = nameBubbles[vehicleID];
          if (bubble) {
            if (bubble.speedMarker) map.removeLayer(bubble.speedMarker);
            if (bubble.nameMarker) map.removeLayer(bubble.nameMarker);
            if (bubble.blockMarker) map.removeLayer(bubble.blockMarker);
            if (bubble.routeMarker) map.removeLayer(bubble.routeMarker);
          }
          delete nameBubbles[vehicleID];
        });

        Object.keys(busMarkerStates).forEach(vehicleID => {
          if (isOnDemandVehicleId(vehicleID)) {
            clearBusMarkerState(vehicleID);
          }
        });

        purgeOrphanedBusMarkers();
      }

      function stopOnDemandPolling() {
        if (onDemandPollingTimerId !== null) {
          clearInterval(onDemandPollingTimerId);
          onDemandPollingTimerId = null;
        }
      }

      function startOnDemandPolling() {
        if (!shouldPollOnDemandData() || onDemandPollingTimerId !== null) {
          return;
        }
        if (typeof document !== 'undefined' && document.hidden) {
          return;
        }
        fetchOnDemandVehicles().catch(error => {
          console.error('Failed to fetch OnDemand vehicles:', error);
        });
        onDemandPollingTimerId = setInterval(() => {
          fetchOnDemandVehicles().catch(error => {
            console.error('Failed to refresh OnDemand vehicles:', error);
          });
        }, ONDEMAND_REFRESH_INTERVAL_MS);
      }

      function setOnDemandVehiclesEnabled(value) {
        const authorized = userIsAuthorizedForOnDemand();
        const requestedEnable = !!value;
        if (requestedEnable && !authorized) {
          updateOnDemandButton();
          return;
        }
        const nextValue = requestedEnable;
        if (onDemandVehiclesEnabled === nextValue) {
          updateOnDemandButton();
          return;
        }
        const pollingBefore = shouldPollOnDemandData();
        onDemandVehiclesEnabled = nextValue;
        if (onDemandVehiclesEnabled) {
          onDemandPollingPausedForVisibility = false;
        } else {
          clearOnDemandVehicles();
          onDemandPollingPausedForVisibility = false;
        }
        const pollingAfter = shouldPollOnDemandData();
        if (pollingAfter && !pollingBefore) {
          startOnDemandPolling();
        } else if (!pollingAfter && pollingBefore) {
          stopOnDemandPolling();
        }
        updateOnDemandButton();
      }

      function toggleOnDemandVehicles() {
        if (!userIsAuthorizedForOnDemand()) {
          updateOnDemandButton();
          return;
        }
        setOnDemandVehiclesEnabled(!onDemandVehiclesEnabled);
      }

      function setOnDemandStopsEnabled(value) {
        const authorized = userIsAuthorizedForOnDemand();
        const requestedEnable = !!value;
        if (requestedEnable && !authorized) {
          updateOnDemandStopsButton();
          return;
        }
        if (onDemandStopsEnabled === requestedEnable) {
          updateOnDemandStopsButton();
          return;
        }
        const pollingBefore = shouldPollOnDemandData();
        onDemandStopsEnabled = requestedEnable;
        if (!onDemandStopsEnabled) {
          clearOnDemandStops();
        }
        const pollingAfter = shouldPollOnDemandData();
        if (pollingAfter && !pollingBefore) {
          onDemandPollingPausedForVisibility = false;
          startOnDemandPolling();
        } else if (!pollingAfter && pollingBefore) {
          stopOnDemandPolling();
          onDemandPollingPausedForVisibility = false;
        } else if (onDemandStopsEnabled) {
          renderOnDemandStops();
        }
        updateOnDemandStopsButton();
      }

      function toggleOnDemandStops() {
        if (!userIsAuthorizedForOnDemand()) {
          updateOnDemandStopsButton();
          return;
        }
        setOnDemandStopsEnabled(!onDemandStopsEnabled);
      }

      function normalizeOnDemandStopsForClustering(stops) {
        if (!Array.isArray(stops)) {
          return [];
        }
        const normalizedStops = [];
        stops.forEach((stop, index) => {
          if (!stop || typeof stop !== 'object') {
            return;
          }
          const lat = Number(stop.lat ?? stop.latitude);
          const lon = Number(stop.lng ?? stop.lon ?? stop.longitude);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
            return;
          }
          const rawVehicleId = stop.vehicleId ?? stop.vehicleID;
          const vehicleId = typeof rawVehicleId === 'string'
            ? rawVehicleId.trim()
            : `${rawVehicleId || ''}`.trim();
          if (!vehicleId) {
            return;
          }
          const routeId = `${ONDEMAND_STOP_ROUTE_PREFIX}${vehicleId}`;
          const stopTimestamp = typeof stop.stopTimestamp === 'string' ? stop.stopTimestamp : '';
          const uniqueSuffix = stopTimestamp || `${index}`;
          const routeStopId = `${routeId}:${uniqueSuffix}`;
          const capacityRaw = Number(stop.capacity);
          const capacity = Number.isFinite(capacityRaw) && capacityRaw > 0 ? capacityRaw : 1;
          const stopType = (stop.stopType || '').toLowerCase() === 'dropoff' ? 'dropoff' : 'pickup';
          const addressValue = typeof stop.address === 'string' ? stop.address.trim() : '';
          const serviceIdRaw = stop.serviceId ?? stop.serviceID;
          const serviceIdText = typeof serviceIdRaw === 'string'
            ? serviceIdRaw.trim()
            : `${serviceIdRaw || ''}`.trim();
          const serviceId = serviceIdText || null;
          const stopName = addressValue || `OnDemand ${stopType === 'dropoff' ? 'Dropoff' : 'Pickup'}`;
          normalizedStops.push({
            Latitude: lat,
            Longitude: lon,
            Name: stopName,
            RouteStopID: routeStopId,
            StopID: routeStopId,
            RouteIDs: [routeId],
            Routes: [{ RouteID: routeId }],
            isOnDemandStop: true,
            onDemandStopDetails: {
              vehicleId,
              routeId,
              capacity,
              stopType,
              address: addressValue,
              serviceId,
              stopTimestamp
            }
          });
        });
        return normalizedStops;
      }

      function summarizeOnDemandStopEntries(stopEntries) {
        const routeStopIds = new Set();
        const fallbackStopIds = new Set();
        const markerRouteIds = new Set();
        const addressSet = new Set();
        let latestStopTimestampMs = 0;
        const segmentsByVehicle = new Map();

        stopEntries.forEach(entry => {
          if (!entry) {
            return;
          }
          const entryRouteStopIds = Array.isArray(entry.routeStopIds) ? entry.routeStopIds : [];
          entryRouteStopIds.forEach(id => {
            if (id !== undefined && id !== null) {
              routeStopIds.add(`${id}`);
            }
          });
          const entryStopIds = Array.isArray(entry.stopIds) ? entry.stopIds : [];
          entryStopIds.forEach(id => {
            if (id === undefined || id === null) {
              return;
            }
            const text = `${id}`.trim();
            if (text) {
              fallbackStopIds.add(text);
            }
          });
          const entryRouteIds = Array.isArray(entry.routeIds) ? entry.routeIds : [];
          entryRouteIds.forEach(routeId => {
            const normalized = normalizeRouteIdentifier(routeId);
            if (typeof normalized === 'string' && normalized.startsWith(ONDEMAND_STOP_ROUTE_PREFIX)) {
              markerRouteIds.add(normalized);
            }
          });
          const onDemandStops = Array.isArray(entry.onDemandStops) ? entry.onDemandStops : [];
          onDemandStops.forEach(details => {
            if (!details) {
              return;
            }
            const vehicleId = typeof details.vehicleId === 'string'
              ? details.vehicleId.trim()
              : `${details.vehicleId || ''}`.trim();
            if (!vehicleId) {
              return;
            }
            let segment = segmentsByVehicle.get(vehicleId);
            if (!segment) {
              const routeId = details.routeId || `${ONDEMAND_STOP_ROUTE_PREFIX}${vehicleId}`;
              segment = {
                vehicleId,
                routeId,
                totalCapacity: 0,
                pickupCount: 0,
                dropoffCount: 0,
                serviceIds: new Set(),
                addresses: new Set(),
                latestTimestamp: 0
              };
              segmentsByVehicle.set(vehicleId, segment);
            }
            const capacityRaw = Number(details.capacity);
            const safeCapacity = Number.isFinite(capacityRaw) && capacityRaw > 0 ? capacityRaw : 1;
            segment.totalCapacity += safeCapacity;
            if ((details.stopType || '').toLowerCase() === 'dropoff') {
              segment.dropoffCount += safeCapacity;
            } else {
              segment.pickupCount += safeCapacity;
            }
            if (details.serviceId) {
              segment.serviceIds.add(`${details.serviceId}`);
            }
            if (details.address) {
              segment.addresses.add(details.address);
              addressSet.add(details.address);
            }
            const timestampMs = Date.parse(details.stopTimestamp || '');
            if (Number.isFinite(timestampMs)) {
              if (timestampMs > segment.latestTimestamp) {
                segment.latestTimestamp = timestampMs;
              }
              if (timestampMs > latestStopTimestampMs) {
                latestStopTimestampMs = timestampMs;
              }
            }
          });
        });

        const segments = Array.from(segmentsByVehicle.values()).map(segment => ({
          vehicleId: segment.vehicleId,
          routeId: segment.routeId,
          totalCapacity: segment.totalCapacity,
          pickupCount: segment.pickupCount,
          dropoffCount: segment.dropoffCount,
          color: sanitizeCssColor(getOnDemandVehicleColor(segment.vehicleId)) || ONDEMAND_MARKER_DEFAULT_COLOR,
          serviceIds: Array.from(segment.serviceIds).filter(Boolean).sort((a, b) => a.localeCompare(b, undefined, { numeric: true })),
          latestTimestamp: segment.latestTimestamp
        })).filter(segment => segment.totalCapacity > 0);

        const fallbackStopIdText = fallbackStopIds.size > 0
          ? Array.from(fallbackStopIds).sort((a, b) => a.localeCompare(b, undefined, { numeric: true })).join(', ')
          : '';

        const markerRouteIdList = markerRouteIds.size > 0
          ? Array.from(markerRouteIds).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
          : segments.map(segment => segment.routeId).filter(Boolean).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

        const representativeAddress = addressSet.size === 1 ? Array.from(addressSet)[0] : '';

        return {
          segments,
          routeStopIds: Array.from(routeStopIds),
          fallbackStopIdText,
          markerRouteIds: markerRouteIdList,
          address: representativeAddress,
          latestStopTimestamp: Number.isFinite(latestStopTimestampMs) && latestStopTimestampMs > 0
            ? new Date(latestStopTimestampMs).toISOString()
            : ''
        };
      }

      function formatOnDemandStopTimestamp(timestamp) {
        if (!timestamp) {
          return '';
        }
        try {
          const date = new Date(timestamp);
          if (Number.isNaN(date.getTime())) {
            return '';
          }
          if (typeof Intl !== 'undefined' && typeof Intl.DateTimeFormat === 'function') {
            const formatter = new Intl.DateTimeFormat(undefined, {
              month: 'short',
              day: 'numeric',
              hour: 'numeric',
              minute: '2-digit'
            });
            return formatter.format(date);
          }
          return date.toLocaleString();
        } catch (error) {
          return '';
        }
      }

      function clearOnDemandStops() {
        if (!map || typeof map.removeLayer !== 'function') {
          onDemandStopMarkerCache.clear();
          onDemandStopMarkers = [];
          return;
        }
        onDemandStopMarkerCache.forEach(entry => {
          if (!entry || !entry.marker) {
            return;
          }
          try {
            if (typeof entry.marker.unbindTooltip === 'function') {
              entry.marker.unbindTooltip();
            }
            map.removeLayer(entry.marker);
          } catch (error) {
            console.warn('Failed to remove OnDemand stop marker:', error);
          }
        });
        onDemandStopMarkerCache.clear();
        onDemandStopMarkers = [];
      }

      function buildOnDemandStopTooltip(groupInfo) {
        if (!groupInfo || !Array.isArray(groupInfo.segments) || groupInfo.segments.length === 0) {
          return '';
        }
        const sections = [];
        if (groupInfo.address) {
          sections.push(`<div class="ondemand-stop-tooltip__address">${escapeHtml(groupInfo.address)}</div>`);
        }
        const timestampText = formatOnDemandStopTimestamp(groupInfo.stopTimestamp);
        if (timestampText) {
          sections.push(`<div class="ondemand-stop-tooltip__timestamp">${escapeHtml(timestampText)}</div>`);
        }
        const lines = groupInfo.segments.map(segment => {
          if (!segment) {
            return '';
          }
          const color = sanitizeCssColor(segment.color) || ONDEMAND_MARKER_DEFAULT_COLOR;
          const swatch = `<span class="ondemand-stop-tooltip__swatch" style="background-color:${color};"></span>`;
          const vehicleLabel = `Vehicle ${segment.vehicleId}`;
          const pickupText = segment.pickupCount === 1 ? '1 pickup' : `${segment.pickupCount} pickups`;
          const dropoffText = segment.dropoffCount === 1 ? '1 dropoff' : `${segment.dropoffCount} dropoffs`;
          const counts = segment.dropoffCount > 0 ? `${pickupText}, ${dropoffText}` : pickupText;
          const serviceText = Array.isArray(segment.serviceIds) && segment.serviceIds.length > 0
            ? ` — Service ${escapeHtml(segment.serviceIds.join(', '))}`
            : '';
          return `<div class="ondemand-stop-tooltip__entry">${swatch}<div><strong>${escapeHtml(vehicleLabel)}</strong><div>${escapeHtml(counts)}${serviceText}</div></div></div>`;
        }).filter(Boolean);
        sections.push(...lines);
        return `<div class="ondemand-stop-tooltip__content">${sections.join('')}</div>`;
      }

      function renderOnDemandStops() {
        if (!onDemandStopsEnabled) {
          clearOnDemandStops();
          return;
        }
        if (!map) {
          return;
        }
        const stops = normalizeOnDemandStopsForClustering(onDemandStopDataCache);
        if (stops.length === 0) {
          clearOnDemandStops();
          return;
        }
        const groupedStops = groupStopsByPixelDistance(stops, STOP_GROUPING_PIXEL_DISTANCE);
        if (groupedStops.length === 0) {
          clearOnDemandStops();
          return;
        }

        const activeKeys = new Set();

        groupedStops.forEach(group => {
          if (!group || !Array.isArray(group.stops) || group.stops.length === 0) {
            return;
          }
          const stopEntries = buildStopEntriesFromStops(group.stops);
          if (stopEntries.length === 0) {
            return;
          }
          const summary = summarizeOnDemandStopEntries(stopEntries);
          if (!summary.segments || summary.segments.length === 0) {
            return;
          }

          const fallbackKey = summary.fallbackStopIdText
            || `${group.latitude.toFixed(5)},${group.longitude.toFixed(5)}`;
          const markerRouteIds = summary.markerRouteIds.length > 0
            ? summary.markerRouteIds.slice()
            : summary.segments.map(segment => segment.routeId).filter(Boolean);
          if (markerRouteIds.length === 0) {
            return;
          }
          markerRouteIds.sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

          const groupKey = createStopGroupKey(summary.routeStopIds, fallbackKey);
          const icon = createStopMarkerIcon(markerRouteIds, [], { onDemandSegments: summary.segments });
          if (!icon) {
            return;
          }

          const groupInfo = {
            position: [group.latitude, group.longitude],
            segments: summary.segments,
            markerRouteIds,
            groupKey,
            address: summary.address,
            stopTimestamp: summary.latestStopTimestamp
          };

          const tooltipHtml = buildOnDemandStopTooltip(groupInfo);
          let markerEntry = onDemandStopMarkerCache.get(groupKey) || null;
          const iconSignature = `${markerRouteIds.join('|')}__${summary.segments
            .map(segment => `${segment.vehicleId}:${segment.totalCapacity}`)
            .join('|')}`;
          if (!markerEntry || !markerEntry.marker) {
            const marker = L.marker(groupInfo.position, { icon, pane: 'ondemandStopsPane', interactive: false, keyboard: false });
            marker.addTo(map);
            if (typeof marker.bindTooltip === 'function') {
              marker.bindTooltip(tooltipHtml, { direction: 'top', opacity: 0.95, className: ONDEMAND_STOP_TOOLTIP_CLASS });
            }
            markerEntry = { marker, iconSignature, groupInfo };
          } else {
            if (typeof markerEntry.marker.setLatLng === 'function') {
              markerEntry.marker.setLatLng(groupInfo.position);
            }
            if (markerEntry.iconSignature !== iconSignature && typeof markerEntry.marker.setIcon === 'function') {
              markerEntry.marker.setIcon(icon);
            }
            const tooltip = typeof markerEntry.marker.getTooltip === 'function' ? markerEntry.marker.getTooltip() : null;
            if (tooltip && typeof tooltip.setContent === 'function') {
              tooltip.setContent(tooltipHtml);
            } else if (typeof markerEntry.marker.bindTooltip === 'function') {
              markerEntry.marker.bindTooltip(tooltipHtml, { direction: 'top', opacity: 0.95, className: ONDEMAND_STOP_TOOLTIP_CLASS });
            }
            markerEntry.iconSignature = iconSignature;
            markerEntry.groupInfo = groupInfo;
          }

          onDemandStopMarkerCache.set(groupKey, markerEntry);
          activeKeys.add(groupKey);
        });

        const removalKeys = [];
        onDemandStopMarkerCache.forEach((entry, key) => {
          if (!activeKeys.has(key)) {
            removalKeys.push(key);
          }
        });

        removalKeys.forEach(key => {
          const entry = onDemandStopMarkerCache.get(key);
          if (entry && entry.marker && map && typeof map.removeLayer === 'function') {
            try {
              if (typeof entry.marker.unbindTooltip === 'function') {
                entry.marker.unbindTooltip();
              }
              map.removeLayer(entry.marker);
            } catch (error) {
              console.warn('Failed to remove stale OnDemand stop marker:', error);
            }
          }
          onDemandStopMarkerCache.delete(key);
        });

        onDemandStopMarkers = Array.from(onDemandStopMarkerCache.values())
          .map(entry => entry && entry.marker)
          .filter(marker => !!marker);

        onDemandStopMarkers.forEach(marker => {
          if (marker && typeof marker.bringToFront === 'function') {
            marker.bringToFront();
          }
        });
      }

      function getOnDemandMarkerColor(entry) {
        if (!entry || typeof entry !== 'object') {
          return ONDEMAND_MARKER_DEFAULT_COLOR;
        }
        const candidates = [];
        if (typeof entry.markerColor === 'string') {
          candidates.push(entry.markerColor.trim());
        }
        if (typeof entry.color_hex === 'string') {
          candidates.push(entry.color_hex.trim());
        }
        if (typeof entry.color === 'string') {
          const trimmed = entry.color.trim();
          if (trimmed) {
            candidates.push(trimmed.startsWith('#') ? trimmed : `#${trimmed}`);
          }
        }
        for (const candidate of candidates) {
          const normalized = sanitizeCssColor(candidate);
          if (normalized) {
            return normalized;
          }
        }
        return ONDEMAND_MARKER_DEFAULT_COLOR;
      }

      async function fetchOnDemandVehicles() {
        if (!shouldPollOnDemandData()) {
          return [];
        }
        if (!map || typeof fetch !== 'function') {
          return [];
        }
        if (onDemandFetchPromise) {
          return onDemandFetchPromise;
        }
        const fetchPromise = (async () => {
          try {
            const response = await fetch(ONDEMAND_ENDPOINT, { cache: 'no-store' });
            if (!response.ok) {
              throw new Error(`HTTP ${response.status}`);
            }
            const payload = await response.json();
            if (!shouldPollOnDemandData()) {
              return [];
            }
            const vehicles = Array.isArray(payload?.vehicles) ? payload.vehicles : [];
            const ondemandStops = Array.isArray(payload?.ondemandStops) ? payload.ondemandStops : [];
            updateOnDemandVehicleColorMap(vehicles);
            onDemandStopDataCache = ondemandStops.slice();
            if (onDemandStopsEnabled) {
              renderOnDemandStops();
            }
            if (!onDemandVehiclesEnabled) {
              clearOnDemandVehicles();
              return [];
            }
            const seen = new Set();
            const zoom = typeof map.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM;
            const markerMetricsForZoom = computeBusMarkerMetrics(zoom);
            for (const vehicle of vehicles) {
              if (!vehicle || typeof vehicle !== 'object') {
                continue;
              }
              const isStale = vehicle.stale === true;
              if (isStale && !includeStaleVehicles) {
                continue;
              }
              if (vehicle.enabled === false) {
                continue;
              }
              const lat = Number(vehicle.lat ?? vehicle.latitude);
              const lon = Number(vehicle.lng ?? vehicle.lon ?? vehicle.longitude);
              if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
                continue;
              }
              const rawId = vehicle.vehicleId ?? vehicle.deviceUuid ?? vehicle.deviceId ?? vehicle.callName ?? '';
              const normalizedId = `${rawId}`.trim();
              if (!normalizedId) {
                continue;
              }
              const markerKey = `${ONDEMAND_MARKER_PREFIX}${normalizedId}`;
              seen.add(markerKey);
              const state = ensureBusMarkerState(markerKey);
              state.isOnDemand = true;
              const newPosition = [lat, lon];
              const speedRaw = Number(vehicle.speed);
              const speedMph = Number.isFinite(speedRaw) ? Math.max(0, speedRaw) : 0;
              const fallbackHeading = getVehicleHeadingFallback(markerKey, vehicle.heading);
              const headingDeg = updateBusMarkerHeading(state, newPosition, fallbackHeading, speedMph);
              const displayName = extractOnDemandDisplayName(vehicle.callName) || `Vehicle ${normalizedId}`;
              state.busName = displayName;
              state.routeID = null;
              const fillColor = sanitizeCssColor(vehicle.markerColor) || getOnDemandVehicleColor(normalizedId);
              state.fillColor = fillColor;
              const glyphColor = computeBusMarkerGlyphColor(fillColor);
              state.glyphColor = glyphColor;
              state.isStale = isStale;
              state.isStopped = isBusConsideredStopped(speedMph);
              state.groundSpeed = speedMph;
              state.lastUpdateTimestamp = Date.now();
              const accessibleName = `${displayName} OnDemand`;
              state.accessibleLabel = buildBusMarkerAccessibleLabel(accessibleName, headingDeg, speedMph);
              rememberCachedVehicleHeading(markerKey, headingDeg, state.lastUpdateTimestamp);

              if (markers[markerKey]) {
                animateMarkerTo(markers[markerKey], newPosition);
                markers[markerKey].routeID = null;
                markers[markerKey].isOnDemand = true;
                state.marker = markers[markerKey];
                queueBusMarkerVisualUpdate(markerKey, {
                  fillColor,
                  glyphColor,
                  headingDeg,
                  accessibleLabel: state.accessibleLabel,
                  stopped: state.isStopped,
                  stale: isStale
                });
              } else {
                const icon = await createBusMarkerDivIcon(markerKey, state);
                if (!icon) {
                  continue;
                }
                const marker = L.marker(newPosition, { icon, pane: 'busesPane', interactive: false, keyboard: false });
                marker.routeID = null;
                marker.isOnDemand = true;
                marker.addTo(map);
                markers[markerKey] = marker;
                state.marker = marker;
                removeDuplicateBusMarkerLayers(markerKey, marker);
                registerBusMarkerElements(markerKey);
                attachBusMarkerInteractions(markerKey);
                updateBusMarkerRootClasses(state);
                updateBusMarkerZIndex(state);
                applyBusMarkerOutlineWidth(state);
              }

              if (adminMode && !kioskMode) {
                const nameIcon = createNameBubbleDivIcon(displayName, fillColor, markerMetricsForZoom.scale, headingDeg);
                nameBubbles[markerKey] = nameBubbles[markerKey] || {};
                if (nameIcon) {
                  if (nameBubbles[markerKey].nameMarker) {
                    animateMarkerTo(nameBubbles[markerKey].nameMarker, newPosition);
                    nameBubbles[markerKey].nameMarker.setIcon(nameIcon);
                  } else {
                    nameBubbles[markerKey].nameMarker = L.marker(newPosition, { icon: nameIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                  }
                } else if (nameBubbles[markerKey].nameMarker) {
                  map.removeLayer(nameBubbles[markerKey].nameMarker);
                  delete nameBubbles[markerKey].nameMarker;
                }
              } else if (nameBubbles[markerKey] && nameBubbles[markerKey].nameMarker) {
                map.removeLayer(nameBubbles[markerKey].nameMarker);
                delete nameBubbles[markerKey].nameMarker;
              }

              if (nameBubbles[markerKey]) {
                const bubble = nameBubbles[markerKey];
                const hasMarkers = Boolean(bubble.speedMarker || bubble.nameMarker || bubble.blockMarker || bubble.routeMarker);
                if (hasMarkers) {
                  bubble.lastScale = markerMetricsForZoom.scale;
                } else {
                  delete nameBubbles[markerKey];
                }
              }
            }

            Object.keys(markers).forEach(vehicleID => {
              if (!isOnDemandVehicleId(vehicleID) || seen.has(vehicleID)) {
                return;
              }
              const marker = markers[vehicleID];
              if (marker) {
                map.removeLayer(marker);
              }
              delete markers[vehicleID];
              clearBusMarkerState(vehicleID);
              if (nameBubbles[vehicleID]) {
                const bubble = nameBubbles[vehicleID];
                if (bubble.speedMarker) map.removeLayer(bubble.speedMarker);
                if (bubble.nameMarker) map.removeLayer(bubble.nameMarker);
                if (bubble.blockMarker) map.removeLayer(bubble.blockMarker);
                if (bubble.routeMarker) map.removeLayer(bubble.routeMarker);
                delete nameBubbles[vehicleID];
              }
            });
            purgeOrphanedBusMarkers();
            return vehicles;
          } catch (error) {
            console.error('Failed to fetch OnDemand vehicles:', error);
            return [];
          }
        })();
        onDemandFetchPromise = fetchPromise;
        fetchPromise.finally(() => {
          if (onDemandFetchPromise === fetchPromise) {
            onDemandFetchPromise = null;
          }
        });
        return fetchPromise;
      }

      const BUS_MARKER_SVG_URL = 'busmarker.svg';

      const BUS_MARKER_VIEWBOX_WIDTH = 52.99;
      const BUS_MARKER_VIEWBOX_HEIGHT = 86.99;
      const BUS_MARKER_PIVOT_X = BUS_MARKER_VIEWBOX_WIDTH / 2;
      const BUS_MARKER_PIVOT_Y = BUS_MARKER_VIEWBOX_HEIGHT / 2;
      const BUS_MARKER_ASPECT_RATIO = BUS_MARKER_VIEWBOX_HEIGHT / BUS_MARKER_VIEWBOX_WIDTH;
      const BUS_MARKER_BASE_WIDTH_PX = 26;
      const BUS_MARKER_MIN_WIDTH_PX = 18;
      const BUS_MARKER_MAX_WIDTH_PX = 48;
      const BUS_MARKER_BASE_ZOOM = 15;
      const BUS_MARKER_MIN_SCALE = BUS_MARKER_MIN_WIDTH_PX / BUS_MARKER_BASE_WIDTH_PX;
      const BUS_MARKER_MAX_SCALE = BUS_MARKER_MAX_WIDTH_PX / BUS_MARKER_BASE_WIDTH_PX;
      const BUS_MARKER_SCALE_ZOOM_FACTOR = 5;
      const BUS_MARKER_ICON_ANCHOR_X_RATIO = BUS_MARKER_PIVOT_X / BUS_MARKER_VIEWBOX_WIDTH;
      const BUS_MARKER_ICON_ANCHOR_Y_RATIO = BUS_MARKER_PIVOT_Y / BUS_MARKER_VIEWBOX_HEIGHT;
      const BUS_MARKER_TRANSFORM_ORIGIN = '50% 50%';
      const BUS_MARKER_DEFAULT_HEADING = 0;
      const BUS_MARKER_DEFAULT_ROUTE_COLOR = '#0B7A26';
      const BUS_MARKER_DEFAULT_CONTRAST_COLOR = '#FFFFFF';
      const BUS_MARKER_CENTER_RING_CENTER_X = 26.5;
      const BUS_MARKER_CENTER_RING_CENTER_Y = 43.49;
      const BUS_MARKER_STOPPED_SQUARE_SIZE_PX = 20;
      const BUS_MARKER_STOPPED_SQUARE_ID = 'center_square';
      const BUS_MARKER_CENTER_RING_ID = 'center_ring';
      let BUS_MARKER_SVG_TEXT = null;
      let BUS_MARKER_SVG_LOAD_PROMISE = null;
      let busMarkerVisibleExtents = null;
      const BUS_MARKER_LABEL_FONT_FAMILY = ACTIVE_MAP_FONT_STACK;
      const BUS_MARKER_LABEL_MIN_FONT_PX = 10;
      const SPEED_BUBBLE_BASE_FONT_PX = 12;
      const SPEED_BUBBLE_HORIZONTAL_PADDING = 12;
      const SPEED_BUBBLE_VERTICAL_PADDING = 4;
      const SPEED_BUBBLE_MIN_WIDTH = 60;
      const SPEED_BUBBLE_MIN_HEIGHT = 20;
      const SPEED_BUBBLE_CORNER_RADIUS = 10;
      const LABEL_VERTICAL_CLEARANCE_PX = -7; // pull labels ~7px closer to the vehicle while relying on the half-diagonal for rotation safety
      const LABEL_VERTICAL_ALIGNMENT_BONUS_PX = 6; // push labels a little farther away when the vehicle is nearly north/south
      const LABEL_VERTICAL_ALIGNMENT_EXPONENT = 4; // emphasize the bonus for headings close to due north/south
      const LABEL_HORIZONTAL_ALIGNMENT_BONUS_PX = 1; // give east/west headings extra breathing room
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
      const MIN_HEADING_DISTANCE_METERS = 2;
      const MIN_POSITION_UPDATE_METERS = 0.5;
      const MIN_HEADING_SPEED_METERS_PER_SECOND = 1;
      const METERS_PER_SECOND_PER_MPH = 0.44704;
      const GPS_STALE_THRESHOLD_SECONDS = 60;
      let busMarkerContrastOverrideColor = null;

      let routeColors = {};
      let routeLayers = [];
      let stopMarkers = [];
      const stopMarkerCache = new Map();
      let lastStopDisplaySignature = null;
      let stopDataCache = [];
      let catStopDataCache = [];
      let routeStopAddressMap = {};
      let routeStopRouteMap = {};
      let nameBubbles = {};
      let busBlocks = {};
      let previousBusData = {};
      let cachedEtas = {};
      let customPopups = [];
      let allRouteBounds = null;
      let mapHasFitAllRoutes = false;
      let refreshIntervals = [];
      let refreshIntervalsActive = false;
      let refreshSuspendedForVisibility = false;
      let busLocationsFetchPromise = null;
      let busLocationsFetchBaseURL = null;
      let routePathsFetchPromise = null;
      let routePathsFetchBaseURL = null;
      let scheduledStopRenderFrame = null;
      let scheduledStopRenderTimeout = null;

      let overlapRenderer = null;

      let currentTranslocRendererGeometries = new Map();
      let currentTranslocSelectedRouteIds = [];

      let activeAgencyLoadCount = 0;

      function showLoadingOverlay() {
        const overlay = document.getElementById('loadingOverlay');
        if (!overlay) return;
        overlay.classList.add('is-visible');
        overlay.setAttribute('aria-busy', 'true');
      }

      function hideLoadingOverlay() {
        const overlay = document.getElementById('loadingOverlay');
        if (!overlay) return;
        overlay.classList.remove('is-visible');
        overlay.setAttribute('aria-busy', 'false');
      }

      function beginAgencyLoad() {
        activeAgencyLoadCount += 1;
        showLoadingOverlay();
      }

      function completeAgencyLoad() {
        activeAgencyLoadCount = Math.max(0, activeAgencyLoadCount - 1);
        if (activeAgencyLoadCount === 0) {
          hideLoadingOverlay();
        }
      }

      const STOP_GROUPING_PIXEL_DISTANCE = 20;
      const STOP_RENDER_BOUNDS_PADDING = 0.2;
      const STOP_MARKER_ICON_SIZE = 24;
      const STOP_MARKER_BORDER_COLOR = 'rgba(15,23,42,0.55)';
      const STOP_MARKER_OUTLINE_COLOR = '#FFFFFF';
      const STOP_MARKER_OUTLINE_WIDTH = 2;
      const STOP_MARKER_BORDER_WIDTH = 2;
      const stopMarkerIconCache = new Map();

      let routePolylineCache = new Map();
      let lastRouteRenderState = {
        selectionKey: '',
        colorSignature: '',
        geometrySignature: '',
        useOverlapRenderer: false
      };
      let lastRouteSelectorSignature = null;

      const DEFAULT_ROUTE_STROKE_WEIGHT = 6;
      const MIN_ROUTE_STROKE_WEIGHT = 3;
      const MAX_ROUTE_STROKE_WEIGHT = 12;
      const ROUTE_WEIGHT_ZOOM_DELTA_LIMIT = 3;
      const ROUTE_WEIGHT_BASE_ZOOM = 15;
      const ROUTE_WEIGHT_STEP_PER_ZOOM = 1;

      function computeRouteStrokeWeight(zoom) {
        const baseWeight = DEFAULT_ROUTE_STROKE_WEIGHT;
        const minWeight = MIN_ROUTE_STROKE_WEIGHT;
        const maxWeight = MAX_ROUTE_STROKE_WEIGHT;
        const targetZoom = Number.isFinite(zoom)
          ? zoom
          : (map && typeof map?.getZoom === 'function' ? map.getZoom() : null);
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

      async function loadAgencies() {
        try {
          const response = await fetch(RIDESYSTEMS_CLIENTS_ENDPOINT, { cache: 'no-store' });
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const payload = await response.json();
          const clients = Array.isArray(payload?.clients) ? payload.clients : [];
          agencies = clients.map(client => {
            const name = typeof client?.name === 'string' ? client.name.trim() : '';
            const url = typeof client?.url === 'string' ? client.url.trim() : '';
            if (!name || !url) return null;
            return { name, url };
          }).filter(Boolean);
          agencies.sort((a, b) => a.name.localeCompare(b.name));
          const uvaIndex = agencies.findIndex(a => a.name === 'University of Virginia');
          if (uvaIndex > -1) {
            const uva = agencies.splice(uvaIndex, 1)[0];
            agencies.unshift(uva);
          }
          const consent = localStorage.getItem('agencyConsent') === 'true';
          const storedAgency = consent ? localStorage.getItem('selectedAgency') : null;
          const scheduledAdminKioskUrl = getAdminKioskScheduledAgencyUrl();
          if (scheduledAdminKioskUrl) {
            baseURL = scheduledAdminKioskUrl;
          } else if (storedAgency && agencies.some(a => a.url === storedAgency)) {
            baseURL = storedAgency;
          } else {
            baseURL = agencies[0]?.url || '';
          }
          resetServiceAlertsState();
          updateControlPanel();
          enforceIncidentVisibilityForCurrentAgency();
          updateRouteSelector(activeRoutes, true);
        } catch (e) {
          console.error('Failed to load agencies', e);
        }
      }

      function positionPanelTab(panelId, tabId, side = 'right') {
        const panel = getCachedElementById(panelId);
        const tab = getCachedElementById(tabId);
        if (!panel || !tab) return;

        const panelRect = panel.getBoundingClientRect();
        const tabRect = tab.getBoundingClientRect();
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
        const tabHeight = tabRect.height || tab.offsetHeight || parseFloat(window.getComputedStyle(tab).height) || 0;
        if (Number.isFinite(panelRect?.top) && Number.isFinite(panelRect?.height)) {
          const panelCenter = panelRect.top + panelRect.height / 2;
          if (Number.isFinite(panelCenter)) {
            const halfTab = Number.isFinite(tabHeight) ? tabHeight / 2 : 0;
            let targetTop = panelCenter;
            if (Number.isFinite(viewportHeight) && halfTab > 0) {
              const minTop = halfTab + 8;
              const maxTop = viewportHeight - halfTab - 8;
              if (Number.isFinite(minTop) && Number.isFinite(maxTop)) {
                targetTop = Math.min(Math.max(panelCenter, minTop), Math.max(minTop, maxTop));
              }
            }
            if (Number.isFinite(targetTop)) {
              tab.style.top = `${targetTop}px`;
            }
          }
        }

        const panelStyle = window.getComputedStyle(panel);
        const gap = side === 'right'
          ? (parseFloat(panelStyle.right) || 0)
          : (parseFloat(panelStyle.left) || 0);
        const offset = panel.offsetWidth + gap;
        const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
        const tabWidth = tabRect.width || tab.offsetWidth || parseFloat(window.getComputedStyle(tab).width) || 0;

        let navOffset = 0;
        try {
          const root = document.documentElement;
          if (root && typeof window.getComputedStyle === 'function') {
            const rootStyle = window.getComputedStyle(root);
            if (rootStyle) {
              const rawOffset = parseFloat(rootStyle.getPropertyValue('--hg-nav-left-offset'));
              if (Number.isFinite(rawOffset)) {
                navOffset = Math.max(0, rawOffset);
              }
            }
          }
        } catch (error) {
          navOffset = 0;
        }

        const hiddenLeftPosition = navOffset > 0 ? navOffset : 0;

        if (side === 'right') {
          if (panel.classList.contains('hidden')) {
            tab.style.right = '0';
          } else {
            const maxRight = Math.max(0, viewportWidth - tabWidth);
            const clampedOffset = Math.min(offset, maxRight);
            tab.style.right = `${clampedOffset}px`;
          }
          tab.style.left = '';
        } else {
          if (panel.classList.contains('hidden')) {
            if (Number.isFinite(hiddenLeftPosition)) {
              tab.style.left = `${hiddenLeftPosition}px`;
            } else {
              tab.style.left = '0';
            }
          } else {
            const maxLeft = Math.max(0, viewportWidth - tabWidth);
            const clampedOffset = Math.min(offset, maxLeft);
            tab.style.left = `${clampedOffset}px`;
          }
          tab.style.right = '';
        }
      }

      function isCompactViewport() {
        const width = window.innerWidth || document.documentElement?.clientWidth || document.body?.clientWidth || 0;
        const height = window.innerHeight || document.documentElement?.clientHeight || document.body?.clientHeight || 0;
        const dimensionCandidates = [width, height].filter(value => Number.isFinite(value) && value > 0);
        const smallestDimension = dimensionCandidates.length > 0 ? Math.min(...dimensionCandidates) : width;
        return Number.isFinite(smallestDimension) && smallestDimension <= PANEL_COLLAPSE_BREAKPOINT;
      }

      function isPanelVisibleForMobileBehavior(panel) {
        if (!panel) return false;
        if (panel.classList && panel.classList.contains('hidden')) return false;
        if (panel.style && panel.style.display === 'none') return false;
        if (typeof window.getComputedStyle === 'function') {
          const computed = window.getComputedStyle(panel);
          if (computed && computed.display === 'none') {
            return false;
          }
        }
        return true;
      }

      function updatePanelTabVisibility() {
        const controlTab = getCachedElementById('controlPanelTab');
        const routeTab = getCachedElementById('routeSelectorTab');

        if (!controlTab || !routeTab) return;

        if (!isCompactViewport()) {
          controlTab.classList.remove('is-hidden-mobile');
          routeTab.classList.remove('is-hidden-mobile');
          return;
        }

        const controlPanel = getCachedElementById('controlPanel');
        const routePanel = getCachedElementById('routeSelector');

        const controlVisible = isPanelVisibleForMobileBehavior(controlPanel);
        const routeVisible = isPanelVisibleForMobileBehavior(routePanel);

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

      function positionAllPanelTabs() {
        if (isKioskExperienceActive()) {
          ensurePanelsHiddenForKioskExperience();
          return;
        }
        positionPanelTab('routeSelector', 'routeSelectorTab', 'right');
        positionPanelTab('controlPanel', 'controlPanelTab', 'left');
        updatePanelTabVisibility();
      }

      const positionAllPanelTabsThrottled = createAnimationFrameThrottler(positionAllPanelTabs);
      if (!kioskMode && !adminKioskMode) {
        window.addEventListener('load', positionAllPanelTabsThrottled);
        window.addEventListener('resize', positionAllPanelTabsThrottled);
      } else {
        ensurePanelsHiddenForKioskExperience();
      }

      // Global storage for routes from GetRoutes.
      let allRoutes = {};
      // Global object to store user selections (for admin mode).
      let routeSelections = {};
      // Tracks routes that currently have at least one vehicle assigned.
      let activeRoutes = new Set();
      // Tracks which routes the API designates as public-facing.
      let routeVisibility = {};
      // Routes that should be forced visible in kiosk mode when they have vehicles.
      const kioskModeAlwaysVisibleRoutes = new Set();

      // Routes default to visible if they currently have vehicles unless the user
      // overrides the selection via the route selector.
      function isRouteSelected(routeID) {
        if (!canDisplayRoute(routeID)) return false;
        const id = Number(routeID);
        if (Number.isNaN(id)) return false;
        if (routeSelections.hasOwnProperty(id)) return routeSelections[id];
        return activeRoutes.has(id);
      }

      function normalizeRouteIdForComparison(routeId) {
        if (routeId === undefined || routeId === null) return null;
        const numericId = Number(routeId);
        if (Number.isFinite(numericId)) {
          return `${numericId}`;
        }
        if (typeof routeId === 'string') {
          const trimmed = routeId.trim().toLowerCase();
          return trimmed !== '' ? trimmed : null;
        }
        const stringValue = String(routeId).trim().toLowerCase();
        return stringValue !== '' ? stringValue : null;
      }

      function routeHasActiveVehicles(routeId) {
        const normalizedRouteId = normalizeRouteIdForComparison(routeId);
        if (!normalizedRouteId) return false;
        let activeValues = [];
        if (activeRoutes instanceof Set) {
          activeValues = Array.from(activeRoutes);
        } else if (Array.isArray(activeRoutes)) {
          activeValues = activeRoutes.slice();
        } else {
          return false;
        }
        return activeValues.some(activeRouteId => {
          const normalizedActive = normalizeRouteIdForComparison(activeRouteId);
          return normalizedActive !== null && normalizedActive === normalizedRouteId;
        });
      }

      function getKioskStatusMessageElement() {
        return getCachedElementById('kioskStatusMessage');
      }

      function setKioskStatusMessageVisibility(visible) {
        const element = getKioskStatusMessageElement();
        if (!element) return;
        if (visible) {
          element.classList.add('is-visible');
          element.setAttribute('aria-hidden', 'false');
        } else {
          element.classList.remove('is-visible');
          element.setAttribute('aria-hidden', 'true');
        }
      }

      function hasAnyActiveRoutes(collection = activeRoutes) {
        if (collection instanceof Set) {
          return collection.size > 0;
        }
        if (Array.isArray(collection)) {
          return collection.length > 0;
        }
        return false;
      }

      function updateKioskStatusMessage(options = {}) {
        if (!kioskMode && !adminKioskMode) {
          kioskVehicleStatusKnown = false;
          setKioskStatusMessageVisibility(false);
          return;
        }

        if (Object.prototype.hasOwnProperty.call(options, 'known')) {
          kioskVehicleStatusKnown = Boolean(options.known);
        }

        const known = kioskVehicleStatusKnown;
        const hasActiveVehicles = Object.prototype.hasOwnProperty.call(options, 'hasActiveVehicles')
          ? Boolean(options.hasActiveVehicles)
          : hasAnyActiveRoutes();

        if (!known) {
          setKioskStatusMessageVisibility(false);
          return;
        }

        setKioskStatusMessageVisibility(!hasActiveVehicles);
      }

      function setRouteVisibility(route) {
        if (!route || typeof route.RouteID === 'undefined') return;
        const id = Number(route.RouteID);
        if (Number.isNaN(id)) return;
        routeVisibility[id] = route.IsVisibleOnMap !== false;
      }

      function isRoutePublicById(routeID) {
        const id = Number(routeID);
        if (Number.isNaN(id) || id === 0) return false;
        if (Object.prototype.hasOwnProperty.call(routeVisibility, id)) {
          return routeVisibility[id];
        }
        return true;
      }

      function canDisplayRoute(routeID) {
        const id = Number(routeID);
        if (Number.isNaN(id)) return false;
        if (id === 0) {
          return adminKioskMode || (!kioskMode && adminMode);
        }
        if (adminKioskMode) return true;
        if (kioskMode) {
          if (kioskModeAlwaysVisibleRoutes.has(id) && routeHasActiveVehicles(id)) {
            return true;
          }
          return isRoutePublicById(id);
        }
        if (adminMode) return true;
        return isRoutePublicById(id);
      }

      function setDisplayMode(mode) {
        const normalizedMode = typeof mode === 'string' ? mode.toLowerCase() : '';
        const validModes = Object.values(DISPLAY_MODES);
        if (!validModes.includes(normalizedMode)) return;
        const modeChanged = displayMode !== normalizedMode;
        displayMode = normalizedMode;
        updateDisplayModeButtons();
        if (modeChanged) {
          updateDisplayModeOverlays();
          refreshMap();
        }
      }

      function updateDisplayModeButtons() {
        const buttonContainer = document.getElementById('displayModeButtons');
        if (!buttonContainer) return;
        const buttons = buttonContainer.querySelectorAll('button[data-mode]');
        buttons.forEach(button => {
          const buttonMode = (button.dataset.mode || '').toLowerCase();
          const isActive = buttonMode === displayMode;
          button.classList.toggle('is-active', isActive);
        });
      }

      function updateTrainToggleButton() {
        const button = document.getElementById('trainToggleButton');
        if (!button) return;
        const allowTrains = trainsFeatureAllowed();
        const isActive = allowTrains && !!trainsFeature.visible;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        button.disabled = !allowTrains;
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function updateAircraftToggleButton() {
        const button = document.getElementById('aircraftToggleButton');
        if (!button) return;
        const allowPlanes = adminFeaturesAllowed();
        const planeLayer = window.PlaneLayer;
        const planeLayerStarted = !!(planeLayer && planeLayer.isStarted);
        const isActive = allowPlanes && !!planesFeature.visible && planeLayerStarted;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        button.disabled = !allowPlanes;
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function updateIncidentToggleButton() {
        const button = document.getElementById('incidentToggleButton');
        if (!button) return;
        const isActive = !!incidentsVisible;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function updateStaleVehiclesButton() {
        const button = document.getElementById('staleVehiclesButton');
        if (!button) return;
        const isActive = !!includeStaleVehicles;
        button.classList.toggle('is-active', isActive);
        button.setAttribute('aria-pressed', isActive ? 'true' : 'false');
        const indicator = button.querySelector('.toggle-indicator');
        if (indicator) {
          indicator.textContent = isActive ? 'On' : 'Off';
        }
      }

      function updateDisplayModeOverlays() {
        if (!map || typeof map.removeLayer !== 'function') return;
        Object.keys(nameBubbles).forEach(vehicleID => {
          const bubble = nameBubbles[vehicleID];
          if (!bubble) return;
          if (bubble.speedMarker && displayMode !== DISPLAY_MODES.SPEED) {
            map.removeLayer(bubble.speedMarker);
            delete bubble.speedMarker;
          }
          if (bubble.blockMarker && displayMode !== DISPLAY_MODES.BLOCK) {
            map.removeLayer(bubble.blockMarker);
            delete bubble.blockMarker;
          }
          if (bubble.routeMarker && displayMode !== DISPLAY_MODES.BLOCK) {
            map.removeLayer(bubble.routeMarker);
            delete bubble.routeMarker;
          }
        });
        purgeOrphanedBusMarkers();
      }

      function purgeOrphanedBusMarkers() {
        if (!map || typeof map.eachLayer !== 'function') {
          return;
        }
        const trackedIds = new Set();
        const trackedLayers = new Set();
        if (markers && typeof markers === 'object') {
          Object.keys(markers).forEach(id => {
            trackedIds.add(`${id}`);
            const layer = markers[id];
            if (layer) {
              trackedLayers.add(layer);
            }
          });
        }
        if (catVehicleMarkers instanceof Map) {
          catVehicleMarkers.forEach((marker, key) => {
            if (typeof key === 'string' && key) {
              trackedIds.add(key);
            }
            if (marker) {
              trackedLayers.add(marker);
            }
          });
        }
        map.eachLayer(layer => {
          if (!layer || typeof layer.getElement !== 'function') {
            return;
          }
          const element = layer.getElement();
          if (!element || !element.classList || !element.classList.contains('bus-marker')) {
            return;
          }
          const root = element.querySelector('.bus-marker__root');
          const datasetId = root && root.dataset ? root.dataset.vehicleId : undefined;
          const normalizedId = typeof datasetId === 'string' ? datasetId : '';
          if ((normalizedId && trackedIds.has(normalizedId)) || trackedLayers.has(layer)) {
            return;
          }
          try {
            map.removeLayer(layer);
          } catch (error) {
            console.warn('Failed to remove orphaned bus marker layer:', error);
          }
        });
      }

      function removeDuplicateBusMarkerLayers(vehicleID, keepLayer = null) {
        if (!map || typeof map.eachLayer !== 'function') {
          return;
        }
        const normalizedId = `${vehicleID}`;
        map.eachLayer(layer => {
          if (!layer || layer === keepLayer || typeof layer.getElement !== 'function') {
            return;
          }
          const element = layer.getElement();
          if (!element || !element.classList || !element.classList.contains('bus-marker')) {
            return;
          }
          const root = element.querySelector('.bus-marker__root');
          const datasetId = root && root.dataset ? root.dataset.vehicleId : undefined;
          const layerVehicleId = typeof datasetId === 'string' ? datasetId : '';
          if (layerVehicleId !== normalizedId) {
            return;
          }
          if (markers && typeof markers === 'object') {
            const trackedMarker = markers[normalizedId];
            if (trackedMarker && trackedMarker === layer) {
              return;
            }
          }
          try {
            map.removeLayer(layer);
          } catch (error) {
            console.warn('Failed to remove duplicate bus marker layer:', error);
          }
        });
      }

      function removeNameBubbleForKey(key) {
        if (!key || !Object.prototype.hasOwnProperty.call(nameBubbles, key)) return;
        const bubble = nameBubbles[key];
        if (bubble) {
          if (bubble.speedMarker) {
            if (map && typeof map.removeLayer === 'function') {
              map.removeLayer(bubble.speedMarker);
            } else if (typeof bubble.speedMarker.remove === 'function') {
              bubble.speedMarker.remove();
            }
          }
          if (bubble.nameMarker) {
            if (map && typeof map.removeLayer === 'function') {
              map.removeLayer(bubble.nameMarker);
            } else if (typeof bubble.nameMarker.remove === 'function') {
              bubble.nameMarker.remove();
            }
          }
          if (bubble.blockMarker) {
            if (map && typeof map.removeLayer === 'function') {
              map.removeLayer(bubble.blockMarker);
            } else if (typeof bubble.blockMarker.remove === 'function') {
              bubble.blockMarker.remove();
            }
          }
          if (bubble.routeMarker) {
            if (map && typeof map.removeLayer === 'function') {
              map.removeLayer(bubble.routeMarker);
            } else if (typeof bubble.routeMarker.remove === 'function') {
              bubble.routeMarker.remove();
            }
          }
          if (bubble.catRouteMarker) {
            if (map && typeof map.removeLayer === 'function') {
              map.removeLayer(bubble.catRouteMarker);
            } else if (typeof bubble.catRouteMarker.remove === 'function') {
              bubble.catRouteMarker.remove();
            }
          }
        }
        delete nameBubbles[key];
        purgeOrphanedBusMarkers();
      }

      function applyRouteOptionState(inputElement) {
        if (!inputElement || typeof inputElement.closest !== 'function') return;
        const parentLabel = inputElement.closest('label.route-option');
        if (!parentLabel) return;
        if (inputElement.checked) {
          parentLabel.classList.add('is-active');
        } else {
          parentLabel.classList.remove('is-active');
        }
      }

      function renderIncidentUnit(unit) {
        if (!unit) return '';
        const text = typeof unit.displayText === 'string' ? unit.displayText.trim() : '';
        if (!text) return '';
        const classes = ['incident-unit'];
        if (unit.statusKey) {
          classes.push(`incident-unit--${unit.statusKey.toLowerCase()}`);
        }
        const classAttr = classes.join(' ');
        const styleParts = [];
        if (unit.colorInfo) {
          if (unit.colorInfo.color) styleParts.push(`color:${unit.colorInfo.color}`);
          if (unit.colorInfo.background) styleParts.push(`background:${unit.colorInfo.background}`);
          if (unit.colorInfo.border) styleParts.push(`border-color:${unit.colorInfo.border}`);
        }
        const styleAttr = styleParts.length ? ` style="${styleParts.join(';')}"` : '';
        const tooltipValue = typeof unit.tooltip === 'string' ? unit.tooltip.trim() : '';
        const titleAttr = tooltipValue ? ` title="${escapeAttribute(tooltipValue)}"` : '';
        return `<span class="${classAttr}"${styleAttr}${titleAttr}>${escapeHtml(text)}</span>`;
      }

      function getIncidentUnitStatusLabel(unit) {
        if (!unit) return INCIDENT_UNIT_STATUS_FALLBACK_LABEL;
        const candidates = [
          typeof unit.statusLabel === 'string' ? unit.statusLabel.trim() : '',
          typeof unit.rawStatus === 'string' ? unit.rawStatus.trim() : '',
          typeof unit.statusKey === 'string' ? unit.statusKey.trim() : ''
        ];
        const label = candidates.find(value => value);
        return label || INCIDENT_UNIT_STATUS_FALLBACK_LABEL;
      }

      function getIncidentUnitStatusSortIndex(statusKey) {
        if (typeof statusKey === 'string' && statusKey) {
          const index = INCIDENT_UNIT_STATUS_SECTION_ORDER.indexOf(statusKey);
          if (index !== -1) return index;
        }
        return INCIDENT_UNIT_STATUS_SECTION_ORDER.length;
      }

      function buildIncidentUnitStatusGroups(units) {
        if (!Array.isArray(units) || units.length === 0) return [];
        const groupsMap = new Map();
        units.forEach((unit, index) => {
          if (!unit) return;
          const text = typeof unit.displayText === 'string' ? unit.displayText.trim() : '';
          if (!text) return;
          const rawLabel = getIncidentUnitStatusLabel(unit);
          const trimmedLabel = rawLabel ? rawLabel.trim() : '';
          const label = trimmedLabel || INCIDENT_UNIT_STATUS_FALLBACK_LABEL;
          const mapKey = unit.statusKey
            ? `key:${unit.statusKey}`
            : `label:${label.toLowerCase()}`;
          let group = groupsMap.get(mapKey);
          if (!group) {
            group = {
              key: unit.statusKey || '',
              label,
              units: [],
              sortIndex: getIncidentUnitStatusSortIndex(unit.statusKey || ''),
              firstUnitIndex: index
            };
            groupsMap.set(mapKey, group);
          }
          group.units.push(unit);
          if (index < group.firstUnitIndex) {
            group.firstUnitIndex = index;
          }
        });
        return Array.from(groupsMap.values()).sort((a, b) => {
          if (a.sortIndex !== b.sortIndex) return a.sortIndex - b.sortIndex;
          if (a.firstUnitIndex !== b.firstUnitIndex) return a.firstUnitIndex - b.firstUnitIndex;
          return a.label.localeCompare(b.label);
        });
      }

      function renderIncidentPopupUnitsSection(units) {
        const validUnits = Array.isArray(units)
          ? units.filter(unit => unit && typeof unit.displayText === 'string' && unit.displayText.trim())
          : [];
        if (!validUnits.length) return '';
        const groups = buildIncidentUnitStatusGroups(validUnits);
        if (!groups.length) return '';
        const groupsHtml = groups.map(group => {
          const unitsHtml = group.units.map(renderIncidentUnit).filter(Boolean).join('');
          if (!unitsHtml) return '';
          const safeLabel = escapeHtml(group.label);
          return `<div class="incident-popup__unit-status-group"><div class="incident-popup__unit-status-title">${safeLabel}</div><div class="incident-popup__unit-list">${unitsHtml}</div></div>`;
        }).filter(Boolean).join('');
        if (!groupsHtml) return '';
        return `<div class="incident-popup__section incident-popup__units"><div class="incident-popup__section-title">Units</div>${groupsHtml}</div>`;
      }

      function renderIncidentAlertUnitsSection(units) {
        const validUnits = Array.isArray(units)
          ? units.filter(unit => unit && typeof unit.displayText === 'string' && unit.displayText.trim())
          : [];
        if (!validUnits.length) return '';
        const groups = buildIncidentUnitStatusGroups(validUnits);
        if (!groups.length) return '';
        const groupsHtml = groups.map(group => {
          const unitsHtml = group.units.map(renderIncidentUnit).filter(Boolean).join('');
          if (!unitsHtml) return '';
          const safeLabel = escapeHtml(group.label);
          return `<div class="incident-alert__unit-status-group"><div class="incident-alert__unit-status-title">${safeLabel}</div><div class="incident-alert__unit-list">${unitsHtml}</div></div>`;
        }).filter(Boolean).join('');
        if (!groupsHtml) return '';
        return `<div class="incident-alert__units"><div class="incident-alert__units-label">Units</div>${groupsHtml}</div>`;
      }

      function renderIncidentAlertItem(entry) {
        if (!entry || !entry.incident) return '';
        const incident = entry.incident;
        const typeLabel = getIncidentTypeLabel(incident) || 'Incident';
        const safeTypeLabel = escapeHtml(typeLabel);
        const typeCode = getIncidentTypeCode(incident);
        const iconUrl = buildPulsePointListIconUrl(typeCode);
        const altText = typeLabel ? `${typeLabel} icon` : 'Incident icon';
        const safeAltText = escapeAttribute(altText);
        const iconHtml = iconUrl
          ? `<div class="incident-alert__media"><img src="${escapeAttribute(iconUrl)}" alt="${safeAltText}" loading="lazy" onerror="this.style.display='none';"></div>`
          : '';
        let incidentIdValue = typeof entry.id === 'string' ? entry.id : '';
        if (!incidentIdValue) {
          incidentIdValue = getIncidentIdentifier(incident) || '';
        }
        const normalizedIncidentId = getNormalizedIncidentId(incidentIdValue);
        if (!normalizedIncidentId) return '';
        ensureIncidentFirstOnSceneTracking(incident, normalizedIncidentId);
        const safeIncidentId = escapeAttribute(normalizedIncidentId);
        const locationText = getIncidentLocationText(incident);
        const locationHtml = locationText
          ? `<div class="incident-alert__location"><span class="incident-alert__location-label">Location:</span><span class="incident-alert__location-text">${escapeHtml(locationText)}</span></div>`
          : '';
        const timeInfo = getIncidentReceivedTimeInfo(incident);
        const units = extractIncidentUnits(incident);
        const hasOnSceneUnits = Array.isArray(units) && units.some(unit => unitHasOnSceneStatus(unit));
        const storedFirstOnScene = Number.isFinite(entry.firstOnSceneTimestamp)
          ? entry.firstOnSceneTimestamp
          : getIncidentFirstOnSceneTimestamp(incident, normalizedIncidentId);
        if (!Number.isFinite(entry.firstOnSceneTimestamp) && Number.isFinite(storedFirstOnScene)) {
          entry.firstOnSceneTimestamp = storedFirstOnScene;
        }
        let onSceneInfo = null;
        if (hasOnSceneUnits) {
          if (Number.isFinite(storedFirstOnScene)) {
            onSceneInfo = formatIncidentTimestamp(new Date(storedFirstOnScene));
          }
          if (!onSceneInfo) {
            onSceneInfo = getIncidentFirstOnSceneTimeInfo(incident, normalizedIncidentId);
          }
        }
        const metaParts = [];
        if (timeInfo) {
          metaParts.push(`<span class="incident-alert__received" title="${escapeAttribute(timeInfo.full)}">Received ${escapeHtml(timeInfo.display)}</span>`);
        }
        if (hasOnSceneUnits && onSceneInfo) {
          const onSceneTitle = escapeAttribute(`First unit on-scene ${onSceneInfo.full}`);
          metaParts.push(`<span class="incident-alert__on-scene" title="${onSceneTitle}">First unit on-scene ${escapeHtml(onSceneInfo.display)}</span>`);
        }
        const metaHtml = metaParts.length ? `<div class="incident-alert__meta">${metaParts.join('')}</div>` : '';
        const routeNames = Array.isArray(entry.routes)
          ? entry.routes.map(route => (typeof route?.name === 'string' ? route.name.trim() : '')).filter(Boolean)
          : [];
        const routesHtml = routeNames.length
          ? `<div class="incident-alert__routes-line"><span class="incident-alert__routes-label">Routes:</span><span class="incident-alert__routes-list">${routeNames.map(name => escapeHtml(name)).join(', ')}</span></div>`
          : '';
        const unitsHtml = renderIncidentAlertUnitsSection(units);
        const buttonTitleParts = [];
        if (typeLabel) {
          buttonTitleParts.push(`View ${typeLabel}`);
        } else {
          buttonTitleParts.push('View incident');
        }
        if (locationText) {
          buttonTitleParts.push(`at ${locationText}`);
        }
        buttonTitleParts.push('on the map');
        const safeButtonTitle = escapeAttribute(buttonTitleParts.join(' '));
        return `
          <button type="button" class="incident-alert__item incident-alert__item-button" data-incident-id="${safeIncidentId}" onclick="handleIncidentAlertClick(this)" title="${safeButtonTitle}">
            ${iconHtml}
            <div class="incident-alert__content">
              <div class="incident-alert__type">${safeTypeLabel}</div>
              ${metaHtml}
              ${locationHtml}
              ${routesHtml}
              ${unitsHtml}
            </div>
          </button>
        `;
      }

      function renderIncidentAlertsHtml() {
        const hasDemo = demoIncidentActive && demoIncidentEntry && demoIncidentEntry.incident;
        if (!hasDemo && !incidentsAreAvailable()) return '';
        const sourceEntries = hasDemo
          ? [demoIncidentEntry]
          : (Array.isArray(incidentsNearRoutes) ? incidentsNearRoutes : []);
        if (!Array.isArray(sourceEntries) || sourceEntries.length === 0) return '';
        const itemsHtml = sourceEntries.map(renderIncidentAlertItem).filter(Boolean).join('');
        if (!itemsHtml) return '';
        const multiple = sourceEntries.length > 1;
        const heading = hasDemo
          ? 'Demo Incident Near a Route'
          : (multiple ? 'Active Incidents Near Routes' : 'Active Incident Near a Route');
        const subheading = hasDemo
          ? 'Preview of an active incident alert using built-in sample data.'
          : (multiple
            ? 'Emergency responses are active on or near multiple transit corridors.'
            : 'An emergency response is active on or near a transit corridor.');
        return `
          <div class="selector-section incident-alert-block">
            <div class="incident-alert__header">
              <div class="incident-alert__title">${escapeHtml(heading)}</div>
              <div class="incident-alert__subtitle">${escapeHtml(subheading)}</div>
            </div>
            <div class="incident-alert__list">
              ${itemsHtml}
            </div>
          </div>
        `;
      }

      function handleIncidentAlertClick(element) {
        if (!element) return;
        const incidentId = element.getAttribute('data-incident-id');
        if (!incidentId) return;
        focusIncidentOnMap(incidentId);
      }

      function focusIncidentOnMap(incidentId) {
        if (!map) return;
        const normalizedId = getNormalizedIncidentId(incidentId);
        if (!normalizedId) return;
        maintainIncidentLayers();
        const entry = incidentMarkers.get(normalizedId);
        let latLng = null;
        if (entry && entry.marker && typeof entry.marker.getLatLng === 'function') {
          latLng = entry.marker.getLatLng();
        }
        if (!latLng) {
          const match = incidentsNearRoutesLookup.get(normalizedId);
          const incident = match?.incident || entry?.data || null;
          if (incident) {
            const lat = parseIncidentCoordinate(incident.Latitude ?? incident.latitude ?? incident.lat);
            const lon = parseIncidentCoordinate(incident.Longitude ?? incident.longitude ?? incident.lon);
            if (Number.isFinite(lat) && Number.isFinite(lon)) {
              if (typeof L !== 'undefined' && typeof L.latLng === 'function') {
                latLng = L.latLng(lat, lon);
              } else {
                latLng = { lat, lng: lon };
              }
            }
          }
        }
        if (!latLng || !Number.isFinite(latLng.lat) || !Number.isFinite(latLng.lng)) {
          return;
        }
        const targetLat = Number(latLng.lat);
        const targetLng = Number(latLng.lng);
        if (!Number.isFinite(targetLat) || !Number.isFinite(targetLng)) {
          return;
        }
        if (isDispatcherLockActive()) {
          return;
        }
        const currentZoom = typeof map.getZoom === 'function' ? map.getZoom() : null;
        const targetZoom = Number.isFinite(currentZoom) ? Math.max(currentZoom, 16) : 16;
        if (typeof map.flyTo === 'function') {
          map.flyTo([targetLat, targetLng], targetZoom, { animate: true, duration: 0.75, easeLinearity: 0.25 });
        } else if (typeof map.setView === 'function') {
          map.setView([targetLat, targetLng], targetZoom, { animate: true });
        }
        if (entry && entry.marker) {
          if (typeof entry.marker.bringToFront === 'function') {
            entry.marker.bringToFront();
          } else if (typeof entry.marker.setZIndexOffset === 'function') {
            entry.marker.setZIndexOffset(500);
          }
          if (typeof entry.marker.fire === 'function') {
            entry.marker.fire('click');
          }
        } else {
          const match = incidentsNearRoutesLookup.get(normalizedId);
          const incident = match?.incident || null;
          if (incident) {
            const routes = Array.isArray(match?.routes) ? match.routes : [];
            createCustomPopup({
              popupType: 'incident',
              position: [targetLat, targetLng],
              incident,
              id: normalizedId,
              routes
            });
          }
        }
      }

      function formatServiceAlertDate(date) {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
          return '';
        }
        if (SERVICE_ALERT_DATE_FORMATTER) {
          try {
            return SERVICE_ALERT_DATE_FORMATTER.format(date);
          } catch (error) {
            // fall through to native formatting
          }
        }
        try {
          return date.toLocaleString();
        } catch (error) {
          return date.toString();
        }
      }

      function formatServiceAlertTimeValue(value) {
        if (value === null || value === undefined) {
          return { display: '', raw: '' };
        }
        if (value instanceof Date && !Number.isNaN(value.getTime())) {
          return {
            display: formatServiceAlertDate(value),
            raw: value.toISOString()
          };
        }
        if (typeof value === 'number' && Number.isFinite(value)) {
          const date = new Date(value);
          if (!Number.isNaN(date.getTime())) {
            return {
              display: formatServiceAlertDate(date),
              raw: String(value)
            };
          }
          return { display: String(value), raw: String(value) };
        }
        if (typeof value === 'string') {
          const trimmed = value.trim();
          if (!trimmed) {
            return { display: '', raw: '' };
          }
          const unixMatch = trimmed.match(/\/Date\((\d+)\)\/?/i);
          if (unixMatch && unixMatch[1]) {
            const millis = Number(unixMatch[1]);
            if (Number.isFinite(millis)) {
              const date = new Date(millis);
              if (!Number.isNaN(date.getTime())) {
                return {
                  display: formatServiceAlertDate(date),
                  raw: trimmed
                };
              }
            }
          }
          const parsed = new Date(trimmed);
          if (!Number.isNaN(parsed.getTime())) {
            return {
              display: formatServiceAlertDate(parsed),
              raw: trimmed
            };
          }
          return { display: trimmed, raw: trimmed };
        }
        if (typeof value === 'object') {
          const stringValue = String(value);
          if (stringValue && stringValue !== '[object Object]') {
            return formatServiceAlertTimeValue(stringValue);
          }
        }
        return { display: '', raw: '' };
      }

      function extractServiceAlertTime(record, type) {
        if (!record || typeof record !== 'object') {
          return { display: '', raw: '' };
        }
        const fields = type === 'end' ? SERVICE_ALERT_END_FIELDS : SERVICE_ALERT_START_FIELDS;
        for (const field of fields) {
          if (Object.prototype.hasOwnProperty.call(record, field)) {
            const info = formatServiceAlertTimeValue(record[field]);
            if (info.display) {
              return info;
            }
          }
        }
        const lowerKeyMap = Object.keys(record).reduce((acc, key) => {
          acc[key.toLowerCase()] = key;
          return acc;
        }, {});
        for (const field of fields) {
          const originalKey = lowerKeyMap[field.toLowerCase()];
          if (originalKey && Object.prototype.hasOwnProperty.call(record, originalKey)) {
            const info = formatServiceAlertTimeValue(record[originalKey]);
            if (info.display) {
              return info;
            }
          }
        }
        return { display: '', raw: '' };
      }

      function normalizeServiceAlertRoutes(record) {
        const collected = [];
        const candidateKeys = ['Routes', 'routes', 'RoutesAffected', 'routesAffected', 'AffectedRoutes', 'affectedRoutes', 'RouteNames', 'routeNames'];
        for (const key of candidateKeys) {
          if (!Object.prototype.hasOwnProperty.call(record, key)) continue;
          const value = record[key];
          if (Array.isArray(value)) {
            value.forEach(entry => {
              if (!entry) return;
              if (typeof entry === 'string') {
                const trimmed = entry.trim();
                if (trimmed) collected.push(trimmed);
                return;
              }
              if (typeof entry === 'object') {
                const nameCandidates = [
                  typeof entry.Name === 'string' ? entry.Name.trim() : '',
                  typeof entry.RouteName === 'string' ? entry.RouteName.trim() : '',
                  typeof entry.Description === 'string' ? entry.Description.trim() : '',
                  typeof entry.Title === 'string' ? entry.Title.trim() : '',
                  typeof entry.label === 'string' ? entry.label.trim() : ''
                ];
                const label = nameCandidates.find(candidate => candidate);
                if (label) collected.push(label);
              }
            });
          } else if (typeof value === 'string') {
            const trimmed = value.trim();
            if (trimmed) {
              trimmed.split(/[,;]+/).map(part => part.trim()).filter(Boolean).forEach(part => collected.push(part));
            }
          }
          if (collected.length) break;
        }
        if (!collected.length) {
          return [];
        }
        const seen = new Set();
        return collected.filter(route => {
          const key = route.toLowerCase();
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
      }

      function normalizeServiceAlertRow(row) {
        if (!row || typeof row !== 'object') return null;
        const titleCandidates = [
          typeof row.MessageTitle === 'string' ? row.MessageTitle.trim() : '',
          typeof row.Title === 'string' ? row.Title.trim() : '',
          typeof row.Subject === 'string' ? row.Subject.trim() : ''
        ];
        const messageCandidates = [
          typeof row.MessageText === 'string' ? row.MessageText.trim() : '',
          typeof row.MessageBody === 'string' ? row.MessageBody.trim() : '',
          typeof row.Text === 'string' ? row.Text.trim() : '',
          typeof row.Description === 'string' ? row.Description.trim() : '',
          typeof row.Details === 'string' ? row.Details.trim() : '',
          typeof row.Body === 'string' ? row.Body.trim() : ''
        ];
        const title = titleCandidates.find(candidate => candidate) || '';
        const message = messageCandidates.find(candidate => candidate) || '';
        const startInfo = extractServiceAlertTime(row, 'start');
        const endInfo = extractServiceAlertTime(row, 'end');
        const idCandidates = [
          row.MessageID,
          row.MessageId,
          row.MessageGuid,
          row.Guid,
          row.ID,
          row.Id,
          row.AlertId,
          row.AlertID,
          row.RecordId,
          row.RecordID
        ];
        const rawId = idCandidates.find(value => value !== null && value !== undefined);
        const statusValue = typeof row.Status === 'string' ? row.Status.trim().toLowerCase() : '';
        let isActive = true;
        if (Object.prototype.hasOwnProperty.call(row, 'IsActive')) {
          isActive = !!row.IsActive;
        } else if (Object.prototype.hasOwnProperty.call(row, 'Active')) {
          isActive = !!row.Active;
        } else if (Object.prototype.hasOwnProperty.call(row, 'Visible')) {
          isActive = !!row.Visible;
        } else if (statusValue) {
          isActive = !(statusValue === 'inactive' || statusValue === 'expired' || statusValue === 'inactive alert');
        }
        const routes = normalizeServiceAlertRoutes(row);
        return {
          id: rawId !== undefined && rawId !== null ? String(rawId) : (title || message || null),
          title,
          message,
          startDisplay: startInfo.display,
          startRaw: startInfo.raw,
          endDisplay: endInfo.display,
          endRaw: endInfo.raw,
          isActive,
          routes
        };
      }

      function extractServiceAlertRows(data) {
        if (!data) return [];
        if (Array.isArray(data)) return data;
        if (Array.isArray(data.Rows)) return data.Rows;
        if (Array.isArray(data.rows)) return data.rows;
        if (Array.isArray(data.Result?.Rows)) return data.Result.Rows;
        if (Array.isArray(data.Result)) return data.Result;
        if (Array.isArray(data.Data)) return data.Data;
        if (Array.isArray(data.data)) return data.data;
        if (Array.isArray(data.Messages)) return data.Messages;
        if (Array.isArray(data.messages)) return data.messages;
        if (data.d) return extractServiceAlertRows(data.d);
        return [];
      }

      function getVisibleServiceAlerts() {
        const baseAlerts = Array.isArray(serviceAlerts) ? serviceAlerts : [];
        if (!catOverlayEnabled) {
          return baseAlerts;
        }
        const catAlerts = Array.isArray(catServiceAlerts) ? catServiceAlerts : [];
        return baseAlerts.concat(catAlerts);
      }

      function getActiveServiceAlertCount() {
        const alerts = getVisibleServiceAlerts();
        if (!Array.isArray(alerts) || alerts.length === 0) {
          return 0;
        }
        return alerts.reduce((total, alert) => {
          if (!alert) return total;
          return total + (alert.isActive === false ? 0 : 1);
        }, 0);
      }

      function isAnyServiceAlertsLoading() {
        return !!serviceAlertsLoading || (catOverlayEnabled && !!catServiceAlertsLoading);
      }

      function getCombinedServiceAlertsError() {
        const alerts = getVisibleServiceAlerts();
        const hasVisibleAlerts = Array.isArray(alerts) && alerts.length > 0;
        if (serviceAlertsError && !hasVisibleAlerts) {
          return serviceAlertsError;
        }
        if (catOverlayEnabled && catServiceAlertsError && !hasVisibleAlerts && !isAnyServiceAlertsLoading()) {
          return catServiceAlertsError;
        }
        return null;
      }

      function buildServiceAlertsStatusText() {
        if (isAnyServiceAlertsLoading()) {
          return SERVICE_ALERT_STATUS_LOADING;
        }
        const errorMessage = getCombinedServiceAlertsError();
        if (errorMessage) {
          return SERVICE_ALERT_STATUS_ERROR;
        }
        if (!serviceAlertsHasLoaded && !(catOverlayEnabled && Array.isArray(catServiceAlerts) && catServiceAlerts.length > 0)) {
          return SERVICE_ALERT_STATUS_LOADING;
        }
        const count = getActiveServiceAlertCount();
        if (count > 0) {
          return count === 1 ? '1 Active Alert' : `${count} Active Alerts`;
        }
        return SERVICE_ALERT_STATUS_NO_ALERTS;
      }

      function renderServiceAlertItem(alert) {
        if (!alert) return '';
        const itemClasses = ['service-alerts-item'];
        if (alert.isActive === false) {
          itemClasses.push('is-inactive');
        }
        const idAttr = alert.id ? ` data-alert-id="${escapeAttribute(alert.id)}"` : '';
        const titleTextRaw = typeof alert.title === 'string' ? alert.title.trim() : '';
        const messageTextRaw = typeof alert.message === 'string' ? alert.message.trim() : '';
        const titleText = titleTextRaw || 'Service Alert';
        const statusText = alert.isActive === false ? 'Inactive' : 'Active';
        const headerHtml = `<div class="service-alerts-item__header"><div class="service-alerts-item__title">${escapeHtml(titleText)}</div><span class="service-alerts-item__status">${escapeHtml(statusText)}</span></div>`;
        const messageHtml = messageTextRaw ? `<div class="service-alerts-item__message">${escapeHtml(messageTextRaw)}</div>` : '';
        const metaRows = [];
        if (alert.startDisplay) {
          const titleAttr = alert.startRaw ? ` title="${escapeAttribute(alert.startRaw)}"` : '';
          metaRows.push(`<div class="service-alerts-item__meta-row"><span class="service-alerts-item__meta-label">Start</span><span class="service-alerts-item__meta-value"${titleAttr}>${escapeHtml(alert.startDisplay)}</span></div>`);
        }
        if (alert.endDisplay) {
          const titleAttr = alert.endRaw ? ` title="${escapeAttribute(alert.endRaw)}"` : '';
          metaRows.push(`<div class="service-alerts-item__meta-row"><span class="service-alerts-item__meta-label">End</span><span class="service-alerts-item__meta-value"${titleAttr}>${escapeHtml(alert.endDisplay)}</span></div>`);
        }
        if (Array.isArray(alert.routes) && alert.routes.length > 0) {
          const routesHtml = alert.routes.map(route => escapeHtml(route)).join(', ');
          metaRows.push(`<div class="service-alerts-item__meta-row"><span class="service-alerts-item__meta-label">Routes</span><span class="service-alerts-item__meta-value">${routesHtml}</span></div>`);
        }
        const metaHtml = metaRows.length ? `<div class="service-alerts-item__meta">${metaRows.join('')}</div>` : '';
        return `<li class="${itemClasses.join(' ')}"${idAttr}>${headerHtml}${messageHtml}${metaHtml}</li>`;
      }

      function renderServiceAlertsPanelContentHtml() {
        const alerts = getVisibleServiceAlerts();
        const loading = isAnyServiceAlertsLoading();
        const errorMessage = getCombinedServiceAlertsError();
        if (loading && (!Array.isArray(alerts) || alerts.length === 0)) {
          return `<div class="service-alerts-state service-alerts-state--loading">Loading service alerts…</div>`;
        }
        if (errorMessage && (!Array.isArray(alerts) || alerts.length === 0)) {
          return `<div class="service-alerts-state service-alerts-state--error">${escapeHtml(errorMessage)}</div>`;
        }
        if (!Array.isArray(alerts) || alerts.length === 0) {
          return '';
        }
        const itemsHtml = alerts.map(renderServiceAlertItem).filter(Boolean).join('');
        if (!itemsHtml) {
          return '';
        }
        return `<ul class="service-alerts-list">${itemsHtml}</ul>`;
      }

      function renderServiceAlertsSectionHtml() {
        const buttonClasses = ['pill-button', 'service-alerts-toggle'];
        if (serviceAlertsExpanded) buttonClasses.push('is-expanded');
        if (isAnyServiceAlertsLoading()) buttonClasses.push('is-loading');
        if (getCombinedServiceAlertsError()) buttonClasses.push('has-error');
        if (getActiveServiceAlertCount() > 0) buttonClasses.push('has-active-alerts');
        const panelClasses = ['service-alerts-panel'];
        if (serviceAlertsExpanded) panelClasses.push('is-expanded');
        if (isAnyServiceAlertsLoading()) panelClasses.push('is-loading');
        if (getCombinedServiceAlertsError()) panelClasses.push('has-error');
        const panelContentHtml = renderServiceAlertsPanelContentHtml();
        const panelHasContent = typeof panelContentHtml === 'string' && panelContentHtml.trim().length > 0;
        if (!panelHasContent) panelClasses.push('is-empty');
        const statusText = escapeHtml(buildServiceAlertsStatusText());
        const hiddenAttr = serviceAlertsExpanded ? '' : ' hidden';
        const ariaHiddenAttr = ` aria-hidden="${serviceAlertsExpanded ? 'false' : 'true'}"`;
        return `
          <div class="selector-group service-alerts-group">
            <button type="button" id="serviceAlertsToggle" class="${buttonClasses.join(' ')}" aria-expanded="${serviceAlertsExpanded ? 'true' : 'false'}" aria-controls="serviceAlertsPanel" onclick="toggleServiceAlertsPanel(event)">
              <span class="service-alerts-toggle__text">
                <span class="service-alerts-toggle__label">Service Alerts</span>
                <span class="service-alerts-toggle__status">${statusText}</span>
              </span>
              <span class="service-alerts-toggle__chevron" aria-hidden="true"></span>
            </button>
            <div id="serviceAlertsPanel" class="${panelClasses.join(' ')}"${hiddenAttr}${ariaHiddenAttr}>${panelContentHtml}</div>
          </div>
        `;
      }

      function updateServiceAlertsButtonState() {
        const button = document.getElementById('serviceAlertsToggle');
        if (!button) return;
        button.setAttribute('aria-expanded', serviceAlertsExpanded ? 'true' : 'false');
        button.classList.toggle('is-expanded', !!serviceAlertsExpanded);
        button.classList.toggle('is-loading', isAnyServiceAlertsLoading());
        button.classList.toggle('has-error', !!getCombinedServiceAlertsError());
        button.classList.toggle('has-active-alerts', getActiveServiceAlertCount() > 0);
        const statusEl = button.querySelector('.service-alerts-toggle__status');
        if (statusEl) {
          statusEl.textContent = buildServiceAlertsStatusText();
        }
      }

      function updateServiceAlertsPanelVisibility() {
        const panel = document.getElementById('serviceAlertsPanel');
        if (!panel) return;
        panel.hidden = !serviceAlertsExpanded;
        panel.setAttribute('aria-hidden', serviceAlertsExpanded ? 'false' : 'true');
        if (panel.style) {
          if (serviceAlertsExpanded) {
            if (typeof panel.style.removeProperty === 'function') {
              panel.style.removeProperty('display');
            } else {
              panel.style.display = '';
            }
          } else {
            panel.style.display = 'none';
          }
        }
        panel.classList.toggle('is-expanded', !!serviceAlertsExpanded);
        panel.classList.toggle('is-loading', isAnyServiceAlertsLoading());
        panel.classList.toggle('has-error', !!getCombinedServiceAlertsError());
        const panelHasContent = typeof panel.innerHTML === 'string' && panel.innerHTML.trim().length > 0;
        panel.classList.toggle('is-empty', !panelHasContent && !isAnyServiceAlertsLoading() && !getCombinedServiceAlertsError());
      }

      function updateServiceAlertsPanelContent() {
        const panel = document.getElementById('serviceAlertsPanel');
        if (!panel) return;
        const contentHtml = renderServiceAlertsPanelContentHtml();
        panel.innerHTML = contentHtml;
        const hasContent = typeof contentHtml === 'string' && contentHtml.trim().length > 0;
        panel.classList.toggle('is-empty', !hasContent);
      }

      function refreshServiceAlertsUI() {
        const hasVisibleAlerts = getVisibleServiceAlerts().length > 0;
        if (serviceAlertsExpanded && !hasVisibleAlerts && !isAnyServiceAlertsLoading() && !getCombinedServiceAlertsError() && (serviceAlertsHasLoaded || (catOverlayEnabled && !catServiceAlertsLoading))) {
          serviceAlertsExpanded = false;
        }
        updateServiceAlertsButtonState();
        updateServiceAlertsPanelContent();
        updateServiceAlertsPanelVisibility();
      }

      function shouldFetchServiceAlerts() {
        if (adminKioskMode) {
          return false;
        }
        if (serviceAlertsLoading || serviceAlertsFetchPromise) {
          return false;
        }
        if (!baseURL) {
          return true;
        }
        if (serviceAlertsLastFetchAgency !== baseURL) {
          return true;
        }
        if (!serviceAlertsLastFetchTime) {
          return true;
        }
        return (Date.now() - serviceAlertsLastFetchTime) > SERVICE_ALERT_REFRESH_INTERVAL_MS;
      }

        async function fetchServiceAlertsForCurrentAgency() {
          if (serviceAlertsFetchPromise) {
            return serviceAlertsFetchPromise;
          }
          const fetchBaseURL = baseURL;
          const sanitizedBase = sanitizeBaseUrl(fetchBaseURL);
          if (!sanitizedBase) {
            serviceAlerts = [];
            serviceAlertsError = SERVICE_ALERT_UNAVAILABLE_MESSAGE;
            serviceAlertsLoading = false;
            serviceAlertsLastFetchAgency = fetchBaseURL;
            serviceAlertsLastFetchTime = Date.now();
            serviceAlertsHasLoaded = true;
            refreshServiceAlertsUI();
            return [];
          }
        serviceAlertsLoading = true;
        serviceAlertsError = null;
        refreshServiceAlertsUI();
        const query = new URLSearchParams({
          showInactive: 'false',
          includeDeleted: 'false',
          messageTypeId: '1',
          search: 'false',
          rows: '10',
          page: '1',
          sortIndex: 'StartDateUtc',
          sortOrder: 'asc'
        });
        const endpoint = `${sanitizedBase}/Secure/Services/RoutesService.svc/GetMessagesPaged?${query.toString()}`;
        const requestPromise = (async () => {
          const response = await fetch(endpoint, { cache: 'no-store' });
          if (!response.ok) {
            throw new Error(`Service alerts request failed: ${response.status}`);
          }
          const text = await response.text();
          let payload = {};
          if (text) {
            try {
              payload = JSON.parse(text);
            } catch (parseError) {
              console.error('Failed to parse service alerts response:', parseError);
              throw parseError;
            }
          }
          let rows = extractServiceAlertRows(payload);
          if (!rows.length && payload && typeof payload === 'object' && payload.d) {
            rows = extractServiceAlertRows(payload.d);
          }
          return rows.map(normalizeServiceAlertRow).filter(Boolean);
        })();
          serviceAlertsFetchPromise = requestPromise;
          try {
            const alerts = await requestPromise;
            if (baseURL !== fetchBaseURL) {
              return alerts;
            }
            serviceAlerts = alerts;
            serviceAlertsError = null;
            serviceAlertsLoading = false;
            serviceAlertsLastFetchAgency = fetchBaseURL;
            serviceAlertsLastFetchTime = Date.now();
            serviceAlertsHasLoaded = true;
            return alerts;
          } catch (error) {
            console.error('Failed to fetch service alerts:', error);
            if (baseURL !== fetchBaseURL) {
              return [];
            }
            serviceAlerts = [];
            serviceAlertsError = SERVICE_ALERT_UNAVAILABLE_MESSAGE;
            serviceAlertsLoading = false;
            serviceAlertsLastFetchAgency = fetchBaseURL;
            serviceAlertsLastFetchTime = Date.now();
            serviceAlertsHasLoaded = true;
            return [];
          } finally {
            if (serviceAlertsFetchPromise === requestPromise) {
              serviceAlertsFetchPromise = null;
            }
            if (baseURL === fetchBaseURL) {
              refreshServiceAlertsUI();
            }
          }
        }

      function toggleServiceAlertsPanel(event) {
        if (event && typeof event.preventDefault === 'function') {
          event.preventDefault();
        }
        const hasVisibleAlerts = getVisibleServiceAlerts().length > 0;
        const loading = isAnyServiceAlertsLoading();
        const errorMessage = getCombinedServiceAlertsError();
        if (!serviceAlertsExpanded && !hasVisibleAlerts && !loading && !errorMessage && (serviceAlertsHasLoaded || (catOverlayEnabled && !catServiceAlertsLoading))) {
          return;
        }
        serviceAlertsExpanded = !serviceAlertsExpanded;
        refreshServiceAlertsUI();
        if (serviceAlertsExpanded && shouldFetchServiceAlerts()) {
          fetchServiceAlertsForCurrentAgency();
        }
      }

      function resetServiceAlertsState() {
        serviceAlerts = [];
        serviceAlertsLoading = false;
        serviceAlertsError = null;
        serviceAlertsExpanded = false;
        serviceAlertsLastFetchAgency = '';
        serviceAlertsLastFetchTime = 0;
        serviceAlertsFetchPromise = null;
        serviceAlertsHasLoaded = false;
        refreshServiceAlertsUI();
      }

      function escapeAttribute(value) {
        return String(value || '')
          .replace(/&/g, '&amp;')
          .replace(/"/g, '&quot;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;');
      }


      function sanitizeBaseUrl(url) {
        if (typeof url !== 'string') return '';
        return url.trim().replace(/\/+$/, '');
      }

      function updateControlPanel() {
        if (isKioskExperienceActive()) {
          ensurePanelsHiddenForKioskExperience();
          return;
        }
        const panel = document.getElementById('controlPanel');
        if (!panel) return;

        const selectedAgency = agencies.find(a => a.url === baseURL);
        const sanitizedBaseURL = sanitizeBaseUrl(baseURL);
        let logoHtml = '';
        if (sanitizedBaseURL) {
          const agencyLogoUrl = `${sanitizedBaseURL}/Images/clientLogo.jpg`;
          const safeLogoSrc = escapeAttribute(agencyLogoUrl);
          const logoAltText = selectedAgency?.name ? `${selectedAgency.name} logo` : 'Agency logo';
          const safeLogoAltText = escapeAttribute(logoAltText);
          logoHtml = `
            <div class="selector-logo">
              <img src="${safeLogoSrc}" alt="${safeLogoAltText}" loading="lazy" onerror="this.closest('.selector-logo').style.display='none';">
            </div>
          `;
        }

        const incidentAlertsHtml = renderIncidentAlertsHtml();
        const serviceAlertsSectionHtml = renderServiceAlertsSectionHtml();
        const allowAdminFeatures = adminFeaturesAllowed();
        const allowTrainControls = trainsFeatureAllowed();
        const allowRadarControls = radarFeaturesAllowed();
        const catOverlayAvailable = catOverlayIsAvailable();
        if (!allowAdminFeatures && planesFeature.visible) {
          setPlanesVisibility(false);
        }
        if (!allowTrainControls && trainsFeature.visible) {
          setTrainsVisibility(false);
        }
        if (!allowRadarControls && radarEnabled) {
          setRadarEnabled(false);
        }
        let trainToggleHtml = '';
        const trainPlaneButtons = [];
        if (allowTrainControls) {
          trainPlaneButtons.push(
            `<button type="button" id="trainToggleButton" class="pill-button train-toggle-button${trainsFeature.visible ? ' is-active' : ''}" aria-pressed="${trainsFeature.visible ? 'true' : 'false'}" onclick="toggleTrainsVisibility()">
                Amtrak<span class="toggle-indicator">${trainsFeature.visible ? 'On' : 'Off'}</span>
              </button>`
          );
        }
        if (allowAdminFeatures) {
          trainPlaneButtons.push(
            `<button type="button" id="aircraftToggleButton" class="pill-button aircraft-toggle-button${planesFeature.visible ? ' is-active' : ''}" aria-pressed="${planesFeature.visible ? 'true' : 'false'}" onclick="toggleAircraftVisibility()">
                Aircraft<span class="toggle-indicator">${planesFeature.visible ? 'On' : 'Off'}</span>
              </button>`
          );
        }
        if (trainPlaneButtons.length > 0) {
          const trainPlaneLabel = allowTrainControls && allowAdminFeatures
            ? 'Trains and Planes'
            : allowTrainControls
              ? 'Trains'
              : 'Planes';
          const trainPlaneMarkup = trainPlaneButtons
            .map(button => `              ${button}`)
            .join('\n');
          trainToggleHtml = `
            <div class="selector-group">
              <div class="selector-label">${trainPlaneLabel}</div>
${trainPlaneMarkup}
            </div>
          `;
        }
        let radarControlsHtml = '';
        if (allowRadarControls) {
          const toggleActive = radarEnabled && !radarTemporarilyUnavailable;
          const toggleDisabledAttr = radarTemporarilyUnavailable ? ' disabled' : '';
          const productOptions = RADAR_PRODUCT_ORDER.map(productKey => {
            const info = RADAR_PRODUCT_INFO[productKey] || {};
            const label = typeof info.label === 'string' ? info.label : productKey;
            const safeLabel = escapeHtml(label);
            const selected = radarProduct === productKey ? ' selected' : '';
            return `<option value="${productKey}"${selected}>${safeLabel}</option>`;
          }).join('');
          const statusText = radarTemporarilyUnavailable ? escapeHtml(RADAR_UNAVAILABLE_MESSAGE) : '';
          radarControlsHtml = `
            <div class="selector-group radar-control-group">
              <div class="selector-label">Radar</div>
              <button type="button" id="radarToggleButton" class="pill-button radar-toggle${toggleActive ? ' is-active' : ''}" aria-pressed="${toggleActive ? 'true' : 'false'}"${toggleDisabledAttr}>
                Radar<span class="toggle-indicator">${toggleActive ? 'On' : 'Off'}</span>
              </button>
              <div class="radar-control">
                <label class="radar-control-label" for="radarProductSelect">Product</label>
                <div class="selector-control">
                  <select id="radarProductSelect">
                    ${productOptions}
                  </select>
                </div>
              </div>
              <div class="radar-control">
                <label class="radar-control-label" for="radarOpacityRange">Opacity</label>
                <div class="radar-opacity-row">
                  <input type="range" id="radarOpacityRange" min="${RADAR_MIN_OPACITY}" max="${RADAR_MAX_OPACITY}" step="0.01" value="${radarOpacity.toFixed(2)}">
                  <span id="radarOpacityValue" class="radar-opacity-value">${formatRadarOpacity(radarOpacity)}</span>
                </div>
              </div>
              <div id="radarStatusMessage" class="radar-status-message" role="status" aria-live="polite"${radarTemporarilyUnavailable ? '' : ' style="display:none;"'}>${statusText}</div>
            </div>
          `;
        }
        let demoButtonHtml = '';
        if (adminMode) {
          const demoButtonLabel = demoIncidentActive ? 'Hide Demo Incident' : 'Show Demo Incident';
          const demoButtonPressed = demoIncidentActive ? 'true' : 'false';
          const demoNoteText = demoIncidentActive
            ? 'Showing sample alert using built-in data.'
            : 'Load a sample alert using built-in data.';
          demoButtonHtml = `
            <!-- Demo incident preview controls (remove when the demo is finished) -->
            <div class="selector-section demo-incident-section">
              <button type="button" id="demoIncidentButton" class="demo-incident-button${demoIncidentActive ? ' is-active' : ''}" aria-pressed="${demoButtonPressed}" onclick="toggleDemoIncident()">
                ${escapeHtml(demoButtonLabel)}
              </button>
              <div class="demo-incident-note">${escapeHtml(demoNoteText)}</div>
            </div>
          `;
        }
        const incidentToggleHtml = incidentsAreAvailable() ? `
          <div class="selector-group">
            <div class="selector-label">Incidents</div>
            <button type="button" id="incidentToggleButton" class="pill-button incident-toggle-button${incidentsVisible ? ' is-active' : ''}" aria-pressed="${incidentsVisible ? 'true' : 'false'}" onclick="toggleIncidentsVisibility()">
              Incidents<span class="toggle-indicator">${incidentsVisible ? 'On' : 'Off'}</span>
            </button>
          </div>
        ` : '';
        const adminAccessHtml = `
          <div class="admin-auth-control">
            <div class="admin-auth-actions">
              <button type="button" class="admin-auth-link"${adminMode ? ' disabled aria-disabled="true"' : ' onclick="openAdminPasswordPrompt()"'}>
                ${adminMode ? 'Admin tools unlocked' : 'Unlock admin tools'}
              </button>
              ${adminMode ? '<button type="button" class="pill-button admin-auth-logout" onclick="logoutAdminTools(event)">Log out</button>' : ''}
            </div>
            <div class="admin-auth-note">${adminMode ? 'Admin tools stay on until you log out or clear your cookies.' : 'Dispatch password required.'}</div>
          </div>
        `;
        let html = `
          <div class="selector-header">
            <div class="selector-header-text">
              <div class="selector-title">System Controls</div>
              <div class="selector-subtitle">Choose a transit system and label style.</div>
            </div>
            ${logoHtml}
          </div>
          <div class="selector-content">
            <div class="selector-group">
              <label class="selector-label" for="agencySelect">Select System</label>
              <div class="selector-control">
                <select id="agencySelect" onchange="changeAgency(this.value)">
        `;
        agencies.forEach(a => {
          html += `<option value="${a.url}" ${a.url === baseURL ? 'selected' : ''}>${a.name}</option>`;
        });
        html += `
                </select>
              </div>
            </div>
        `;
        const centerMapButtonHtml = `
            <button type="button" id="centerMapButton" class="pill-button center-map-button" onclick="centerMapOnRoutes()">
              Center Map
            </button>
        `;
        if (catOverlayAvailable) {
          html += `
            <div class="selector-group">
              <button type="button" id="catToggleButton" class="pill-button cat-toggle-button${catOverlayEnabled ? ' is-active' : ''}" aria-pressed="${catOverlayEnabled ? 'true' : 'false'}" onclick="toggleCatOverlay()">
                CAT<span class="toggle-indicator">${catOverlayEnabled ? 'On' : 'Off'}</span>
              </button>
              ${centerMapButtonHtml}
            </div>
          `;
        } else {
          html += `
            <div class="selector-group">
              ${centerMapButtonHtml}
            </div>
          `;
        }
        html += serviceAlertsSectionHtml;
        html += incidentAlertsHtml;

        if (adminMode) {
          html += `
            <div class="selector-group">
              <div class="selector-label">Vehicle Labels</div>
              <div class="display-mode-group" id="displayModeButtons">
                <button type="button" class="pill-button display-mode-button ${displayMode === DISPLAY_MODES.SPEED ? 'is-active' : ''}" data-mode="${DISPLAY_MODES.SPEED}" onclick="setDisplayMode('${DISPLAY_MODES.SPEED}')">
                  Speed
                </button>
                <button type="button" class="pill-button display-mode-button ${displayMode === DISPLAY_MODES.BLOCK ? 'is-active' : ''}" data-mode="${DISPLAY_MODES.BLOCK}" onclick="setDisplayMode('${DISPLAY_MODES.BLOCK}')">
                  Block
                </button>
                <button type="button" class="pill-button display-mode-button ${displayMode === DISPLAY_MODES.NONE ? 'is-active' : ''}" data-mode="${DISPLAY_MODES.NONE}" onclick="setDisplayMode('${DISPLAY_MODES.NONE}')">
                  None
                </button>
              </div>
            </div>
            <div class="selector-group">
              <button type="button" id="staleVehiclesButton" class="pill-button stale-vehicles-button${includeStaleVehicles ? ' is-active' : ''}" aria-pressed="${includeStaleVehicles ? 'true' : 'false'}" onclick="toggleStaleVehicles()">
                Stale Vehicles<span class="toggle-indicator">${includeStaleVehicles ? 'On' : 'Off'}</span>
              </button>
            </div>
            <div class="selector-group">
              <button type="button" id="onDemandToggleButton" class="pill-button ondemand-toggle-button${onDemandVehiclesEnabled ? ' is-active' : ''}" aria-pressed="${onDemandVehiclesEnabled ? 'true' : 'false'}" onclick="toggleOnDemandVehicles()">
                OnDemand<span class="toggle-indicator">${onDemandVehiclesEnabled ? 'On' : 'Off'}</span>
              </button>
              <button type="button" id="onDemandStopsToggleButton" class="pill-button ondemand-stops-toggle-button${onDemandStopsEnabled ? ' is-active' : ''}" aria-pressed="${onDemandStopsEnabled ? 'true' : 'false'}" onclick="toggleOnDemandStops()">
                Demand Stops<span class="toggle-indicator">${onDemandStopsEnabled ? 'On' : 'Off'}</span>
              </button>
            </div>
          `;
        }

        html += incidentToggleHtml;
        html += radarControlsHtml;
        html += trainToggleHtml;
        html += demoButtonHtml;
        html += adminAccessHtml;

        html += `
          </div>
        `;

        panel.innerHTML = html;
        updateCatToggleButtonState();
        initializeRadarControls();
        updateDisplayModeButtons();
        updateTrainToggleButton();
        updateAircraftToggleButton();
        updateIncidentToggleButton();
        updateStaleVehiclesButton();
        updateOnDemandButton();
        updateOnDemandStopsButton();
        refreshServiceAlertsUI();
        positionAllPanelTabs();
      }

      // updateRouteSelector rebuilds the route selector panel.
      // The list (excluding Out of Service) is alphabetized and defaults to
      // checking only routes that currently have vehicles.
      function updateRouteSelector(activeRoutesParam, forceUpdate = false) {
        if (isKioskExperienceActive()) {
          ensurePanelsHiddenForKioskExperience();
          return;
        }
        const container = document.getElementById("routeSelector");
        if (!container) return;

        const activeRoutesSet = activeRoutesParam instanceof Set
          ? activeRoutesParam
          : new Set(Array.isArray(activeRoutesParam) ? activeRoutesParam : []);

        if (forceUpdate) {
          lastRouteSelectorSignature = null;
        }

        const agencyDropdown = document.getElementById('agencySelect');
        if (!forceUpdate && agencyDropdown && document.activeElement === agencyDropdown) {
          return;
        }

        let routeIDs = Object.keys(allRoutes)
          .map(id => Number(id))
          .filter(id => !Number.isNaN(id) && id !== 0 && canDisplayRoute(id));

        routeIDs.sort((a, b) => {
          const aHasVehicle = activeRoutesSet.has(a);
          const bHasVehicle = activeRoutesSet.has(b);
          if (aHasVehicle !== bHasVehicle) {
            return aHasVehicle ? -1 : 1;
          }
          const routeA = allRoutes[a] || {};
          const routeB = allRoutes[b] || {};
          const nameA = (routeA.Description || routeA.RouteName || `Route ${routeA.RouteID || a}` || '').trim().toUpperCase();
          const nameB = (routeB.Description || routeB.RouteName || `Route ${routeB.RouteID || b}` || '').trim().toUpperCase();
          if (nameA < nameB) return -1;
          if (nameA > nameB) return 1;
          return 0;
        });

        const agenciesSignature = agencies
          .map(a => `${a.url || ''}::${a.name || ''}`)
          .join('|');

        const routeSignatureParts = routeIDs.map(routeID => {
          const route = allRoutes[routeID] || {};
          const checked = Object.prototype.hasOwnProperty.call(routeSelections, routeID)
            ? routeSelections[routeID]
            : activeRoutesSet.has(routeID);
          const infoText = typeof route.InfoText === 'string' ? route.InfoText.trim() : '';
          const desc = typeof route.Description === 'string' ? route.Description.trim() : '';
          const color = route.MapLineColor || '';
          const hasActiveVehicle = activeRoutesSet.has(routeID);
          return `${routeID}:${checked ? 1 : 0}:${color}:${desc}:${infoText}:${hasActiveVehicle ? 1 : 0}`;
        });

        const catRoutesList = catOverlayEnabled ? getSortedCatRoutes() : [];
        const activeCatRouteKeysSet = catOverlayEnabled
          ? (catActiveRouteKeys instanceof Set ? new Set(catActiveRouteKeys) : new Set())
          : new Set();
        const catRouteRenderData = catRoutesList.map(route => {
          const key = catRouteKey(route.idKey);
          if (!key || key === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
            return null;
          }
          const checked = getCatRouteSelectionState(key, activeCatRouteKeysSet);
          const hasActiveVehicle = activeCatRouteKeysSet.has(key);
          const displayNameRaw = (route.displayName || route.shortName || route.longName || key || '').trim();
          const displayName = displayNameRaw || `Route ${key}`;
          const longName = typeof route.longName === 'string' ? route.longName.trim() : '';
          const detailLines = [];
          if (longName && longName.toUpperCase() !== displayName.toUpperCase()) {
            detailLines.push(longName);
          }
          if (!hasActiveVehicle) {
            detailLines.push('No buses currently assigned');
          }
          const color = route.color || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
          return {
            route,
            key,
            checked,
            hasActiveVehicle,
            displayName,
            detailLines,
            color
          };
        }).filter(Boolean);
        const catRouteSignatureParts = catRouteRenderData.map(entry => {
          const detailSignature = entry.detailLines.join('||');
          return `${entry.key}:${entry.checked ? 1 : 0}:${entry.color || ''}:${entry.displayName || ''}:${detailSignature}:${entry.hasActiveVehicle ? 1 : 0}`;
        });
        const catActiveSignature = Array.from(activeCatRouteKeysSet).sort().join('|');

        const outOfServiceChecked = adminMode && canDisplayRoute(0)
          ? (Object.prototype.hasOwnProperty.call(routeSelections, 0)
            ? routeSelections[0]
            : activeRoutesSet.has(0))
          : null;

        const signatureParts = [
          baseURL,
          adminMode ? '1' : '0',
          kioskMode ? '1' : '0',
          adminKioskMode ? '1' : '0',
          displayMode || '',
          agenciesSignature,
          outOfServiceChecked === null ? 'na' : (outOfServiceChecked ? '1' : '0'),
          routeSignatureParts.join('|'),
          catOverlayEnabled ? '1' : '0',
          `${catRouteRenderData.length}:${catRouteSignatureParts.join('|')}::${catActiveSignature}`
        ];

        const signature = signatureParts.join('||');
        if (!forceUpdate && signature === lastRouteSelectorSignature) {
          positionAllPanelTabs();
          return;
        }
        lastRouteSelectorSignature = signature;

        const previousContent = container.querySelector('.selector-content');
        const previousScrollTop = previousContent ? previousContent.scrollTop : 0;
        const activeElement = document.activeElement;
        const focusedElementId = activeElement && container.contains(activeElement) && activeElement.id
          ? activeElement.id
          : null;

        let html = `
          <div class="selector-header">
            <div class="selector-header-text">
              <div class="selector-title">Route Controls</div>
              <div class="selector-subtitle">Tailor the live map to the routes you care about.</div>
            </div>
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

        if (adminMode && canDisplayRoute(0)) {
          let outChecked = Object.prototype.hasOwnProperty.call(routeSelections, 0) ? routeSelections[0] : activeRoutesSet.has(0);
          html += `
                <label class="route-option route-option--out">
                  <input type="checkbox" id="route_0" value="0" ${outChecked ? "checked" : ""}>
                  <span class="color-box route-option-swatch" style="background:${outOfServiceRouteColor};"></span>
                  <span class="route-option-text">
                    <span class="route-option-name">Out of Service</span>
                    <span class="route-option-detail">Vehicles without an assigned route</span>
                  </span>
                </label>
          `;
        }

        routeIDs.forEach(routeID => {
          const route = allRoutes[routeID] || {};
          const checked = Object.prototype.hasOwnProperty.call(routeSelections, routeID)
            ? routeSelections[routeID]
            : activeRoutesSet.has(routeID);
          const routeNameRaw = (route.Description || route.RouteName || '').trim();
          const routeName = routeNameRaw !== '' ? routeNameRaw : `Route ${route.RouteID || routeID}`;
          const infoText = typeof route.InfoText === 'string' ? route.InfoText.trim() : '';
          const hasActiveVehicle = activeRoutesSet.has(routeID);
          const detailLines = [];
          if (infoText) {
            detailLines.push(infoText);
          }
          if (!hasActiveVehicle) {
            detailLines.push('No buses currently assigned');
          }
          const detailHtml = detailLines.map(text => `<span class="route-option-detail">${text}</span>`).join('');
          const color = route.MapLineColor || '#A0AEC0';
          html += `
                <label class="route-option">
                  <input type="checkbox" id="route_${routeID}" value="${routeID}" ${checked ? "checked" : ""}>
                  <span class="color-box route-option-swatch" style="background:${color};"></span>
                  <span class="route-option-text">
                    <span class="route-option-name">${routeName}</span>
                    ${detailHtml}
                  </span>
                </label>
          `;
        });

        if (catRouteRenderData.length > 0) {
          html += `
                <div class="selector-label">CAT Routes</div>
          `;
          catRouteRenderData.forEach(entry => {
            const detailHtml = entry.detailLines.map(text => `<span class="route-option-detail">${escapeHtml(text)}</span>`).join('');
            const checkboxId = getCatRouteCheckboxId(entry.key);
            const safeValue = escapeAttribute(entry.key);
            const safeColor = escapeAttribute(entry.color || CAT_VEHICLE_MARKER_DEFAULT_COLOR);
            const safeName = escapeHtml(entry.displayName);
            html += `
                <label class="route-option route-option--cat">
                  <input type="checkbox" id="${checkboxId}" value="${safeValue}" ${entry.checked ? "checked" : ""}>
                  <span class="color-box route-option-swatch" style="background:${safeColor};"></span>
                  <span class="route-option-text">
                    <span class="route-option-name">${safeName}</span>
                    ${detailHtml}
                  </span>
                </label>
            `;
          });
        }

        html += `
              </div>
            </div>
          </div>
        `;

        container.innerHTML = html;

        const newContent = container.querySelector('.selector-content');
        if (newContent) {
          newContent.scrollTop = previousScrollTop;
        }
        if (focusedElementId) {
          const replacementElement = document.getElementById(focusedElementId);
          if (replacementElement && typeof replacementElement.focus === 'function') {
            try {
              replacementElement.focus({ preventScroll: true });
            } catch (error) {
              replacementElement.focus();
            }
          }
        }
        let outChk = document.getElementById("route_0");
        if (outChk) {
          outChk.addEventListener("change", function() {
            routeSelections[0] = outChk.checked;
            applyRouteOptionState(outChk);
            refreshMap();
          });
          applyRouteOptionState(outChk);
        }
        routeIDs.forEach(routeID => {
          if (!canDisplayRoute(routeID) || Number(routeID) === 0) return;
          let chk = document.getElementById("route_" + routeID);
          if (chk) {
            chk.addEventListener("change", function() {
              routeSelections[routeID] = chk.checked;
              applyRouteOptionState(chk);
              refreshMap();
            });
            applyRouteOptionState(chk);
          }
        });

        if (catRouteRenderData.length > 0) {
          catRouteRenderData.forEach(entry => {
            const checkboxId = getCatRouteCheckboxId(entry.key);
            const catCheckbox = document.getElementById(checkboxId);
            if (!catCheckbox) {
              return;
            }
            catCheckbox.checked = entry.checked;
            applyRouteOptionState(catCheckbox);
            catCheckbox.addEventListener('change', function() {
              catRouteSelections.set(entry.key, catCheckbox.checked);
              applyRouteOptionState(catCheckbox);
              renderCatVehiclesUsingCache();
              renderCatRoutes();
              renderBusStops(stopDataCache);
            });
          });
        }

        positionAllPanelTabs();
      }

      function selectAllRoutes() {
        if (adminMode && canDisplayRoute(0)) {
          let outChk = document.getElementById("route_0");
          if (outChk) {
            outChk.checked = true;
            applyRouteOptionState(outChk);
          }
          routeSelections[0] = true;
        }
        for (let routeID in allRoutes) {
          if (!canDisplayRoute(routeID) || Number(routeID) === 0) continue;
          let chk = document.getElementById("route_" + routeID);
          if (chk) {
            chk.checked = true;
            applyRouteOptionState(chk);
          }
          routeSelections[routeID] = true;
        }
        if (catOverlayEnabled) {
          const catRoutes = getUniqueCatRoutes();
          catRoutes.forEach(route => {
            const key = catRouteKey(route.idKey);
            if (!key) {
              return;
            }
            const checkboxId = getCatRouteCheckboxId(key);
            const catCheckbox = document.getElementById(checkboxId);
            if (catCheckbox) {
              catCheckbox.checked = true;
              applyRouteOptionState(catCheckbox);
            }
            catRouteSelections.set(key, true);
          });
          renderCatVehiclesUsingCache();
          renderCatRoutes();
        }
        refreshMap();
      }

      function selectActiveRoutes() {
        const activeSet = activeRoutes instanceof Set
          ? activeRoutes
          : new Set(Array.isArray(activeRoutes) ? activeRoutes : []);

        if (adminMode && canDisplayRoute(0)) {
          const outChk = document.getElementById("route_0");
          const shouldSelectOut = activeSet.has(0);
          if (outChk) {
            outChk.checked = shouldSelectOut;
            applyRouteOptionState(outChk);
          }
          routeSelections[0] = shouldSelectOut;
        }

        for (let routeID in allRoutes) {
          if (!canDisplayRoute(routeID) || Number(routeID) === 0) continue;
          const numericRouteId = Number(routeID);
          const chk = document.getElementById("route_" + routeID);
          const shouldSelect = activeSet.has(numericRouteId);
          if (chk) {
            chk.checked = shouldSelect;
            applyRouteOptionState(chk);
          }
          routeSelections[numericRouteId] = shouldSelect;
        }

        if (catOverlayEnabled) {
          const activeCatKeys = catActiveRouteKeys instanceof Set ? new Set(catActiveRouteKeys) : new Set();
          const catRoutes = getUniqueCatRoutes();
          catRoutes.forEach(route => {
            const key = catRouteKey(route.idKey);
            if (!key) {
              return;
            }
            const shouldSelectCat = activeCatKeys.has(key);
            const checkboxId = getCatRouteCheckboxId(key);
            const catCheckbox = document.getElementById(checkboxId);
            if (catCheckbox) {
              catCheckbox.checked = shouldSelectCat;
              applyRouteOptionState(catCheckbox);
            }
            catRouteSelections.set(key, shouldSelectCat);
          });
          renderCatVehiclesUsingCache();
          renderCatRoutes();
        }

        refreshMap();
      }

      function deselectAllRoutes() {
        if (adminMode && canDisplayRoute(0)) {
          let outChk = document.getElementById("route_0");
          if (outChk) {
            outChk.checked = false;
            applyRouteOptionState(outChk);
          }
          routeSelections[0] = false;
        }
        for (let routeID in allRoutes) {
          if (!canDisplayRoute(routeID) || Number(routeID) === 0) continue;
          let chk = document.getElementById("route_" + routeID);
          if (chk) {
            chk.checked = false;
            applyRouteOptionState(chk);
          }
          routeSelections[routeID] = false;
        }
        if (catOverlayEnabled) {
          const catRoutes = getUniqueCatRoutes();
          catRoutes.forEach(route => {
            const key = catRouteKey(route.idKey);
            if (!key) {
              return;
            }
            const checkboxId = getCatRouteCheckboxId(key);
            const catCheckbox = document.getElementById(checkboxId);
            if (catCheckbox) {
              catCheckbox.checked = false;
              applyRouteOptionState(catCheckbox);
            }
            catRouteSelections.set(key, false);
          });
          renderCatVehiclesUsingCache();
          renderCatRoutes();
        }
        refreshMap();
      }

      function setPanelToggleArrow(tab, direction) {
        if (!tab) return;
        tab.setAttribute('data-arrow-direction', direction);
        const arrowElement = tab.querySelector('.panel-toggle__arrow');
        const isRightTab = tab.classList.contains('panel-toggle--right');
        const inChar = isRightTab ? '▶' : '◄';
        const outChar = isRightTab ? '◄' : '▶';
        const fallbackChar = direction === 'in' ? inChar : outChar;
        if (!arrowElement) {
          tab.textContent = fallbackChar;
          return;
        }
        const tagName = typeof arrowElement.tagName === 'string' ? arrowElement.tagName.toLowerCase() : '';
        if (tagName !== 'svg') {
          arrowElement.textContent = fallbackChar;
        }
      }

      // togglePanelVisibility toggles the provided panel's visibility and updates its tab arrow.
      function togglePanelVisibility(panelId, tabId, expandedArrow, collapsedArrow) {
        if (isKioskExperienceActive()) {
          return;
        }
        const panel = getCachedElementById(panelId);
        const tab = getCachedElementById(tabId);
        if (!panel || !tab) return;
        const isHidden = panel.classList.toggle('hidden');
        setPanelToggleArrow(tab, isHidden ? collapsedArrow : expandedArrow);
        positionAllPanelTabs();
      }

      function toggleRoutePanel() {
        togglePanelVisibility('routeSelector', 'routeSelectorTab', 'in', 'out');
      }

      function toggleControlPanel() {
        togglePanelVisibility('controlPanel', 'controlPanelTab', 'in', 'out');
      }

      function shouldCollapsePanelsOnLoad() {
        return dispatcherMode || isCompactViewport();
      }

      function initializePanelStateForViewport() {
        if (isKioskExperienceActive()) {
          ensurePanelsHiddenForKioskExperience();
          return;
        }
        if (!shouldCollapsePanelsOnLoad()) return;

        const controlPanel = getCachedElementById('controlPanel');
        const controlTab = getCachedElementById('controlPanelTab');
        const routePanel = getCachedElementById('routeSelector');
        const routeTab = getCachedElementById('routeSelectorTab');

        if (controlPanel && !controlPanel.classList.contains('hidden')) {
          controlPanel.classList.add('hidden');
        }
        if (controlTab) {
          setPanelToggleArrow(controlTab, 'out');
        }

        if (routePanel && !routePanel.classList.contains('hidden')) {
          routePanel.classList.add('hidden');
        }
        if (routeTab) {
          setPanelToggleArrow(routeTab, 'out');
        }

        positionAllPanelTabs();
      }

      function renderRouteLegendContent(legendElement, routes) {
        if (!legendElement) return;
        legendElement.style.display = "block";
        if (typeof legendElement.replaceChildren === 'function') {
          legendElement.replaceChildren();
        } else {
          legendElement.innerHTML = "";
        }

        const fragment = typeof document !== 'undefined' && typeof document.createDocumentFragment === 'function'
          ? document.createDocumentFragment()
          : null;
        const target = fragment || legendElement;

        routes.forEach(route => {
          const item = document.createElement("div");
          item.className = "legend-item";

          const color = document.createElement("span");
          color.className = "legend-color";
          color.style.backgroundColor = route.color;
          item.appendChild(color);

          const textContainer = document.createElement("div");
          textContainer.className = "legend-text";

          const name = document.createElement("div");
          name.className = "legend-name";
          name.textContent = route.name;
          textContainer.appendChild(name);

          if (route.description) {
            const description = document.createElement("div");
            description.className = "legend-description";
            description.textContent = route.description;
            textContainer.appendChild(description);
          }

          item.appendChild(textContainer);
          target.appendChild(item);
        });

        if (fragment) {
          legendElement.appendChild(fragment);
        }
      }

      function createOutOfServiceLegendEntry() {
        return {
          routeId: 0,
          name: 'Out of Service',
          description: 'Vehicles without an assigned route',
          color: outOfServiceRouteColor
        };
      }

      function extractLegendRouteIdentifiers(route) {
        const rawRouteId = route && typeof route === 'object'
          ? (route.routeId ?? route.routeID ?? route.id ?? null)
          : null;
        const numericId = Number(rawRouteId);
        if (Number.isFinite(numericId)) {
          return {
            rawRouteId,
            numericId,
            stringId: null
          };
        }
        const stringId = rawRouteId !== null && rawRouteId !== undefined
          ? String(rawRouteId).trim()
          : '';
        return {
          rawRouteId,
          numericId: null,
          stringId
        };
      }

      function buildLegendRouteKey(route) {
        if (route !== null && route !== undefined) {
          const rawLegendKey = route.legendKey !== undefined && route.legendKey !== null
            ? `${route.legendKey}`.trim()
            : '';
          if (rawLegendKey !== '') {
            return `custom:${rawLegendKey.toLowerCase()}`;
          }
        }
        const identifiers = extractLegendRouteIdentifiers(route);
        if (Number.isFinite(identifiers.numericId)) {
          return `num:${identifiers.numericId}`;
        }
        if (identifiers.stringId) {
          return `str:${identifiers.stringId.toLowerCase()}`;
        }
        if (route && typeof route.name === 'string' && route.name.trim() !== '') {
          return `name:${route.name.trim().toLowerCase()}`;
        }
        return null;
      }

      function compareLegendRoutes(a, b) {
        const aIdentifiers = extractLegendRouteIdentifiers(a);
        const bIdentifiers = extractLegendRouteIdentifiers(b);
        const aHasNumeric = Number.isFinite(aIdentifiers.numericId);
        const bHasNumeric = Number.isFinite(bIdentifiers.numericId);
        if (aHasNumeric && bHasNumeric) {
          return aIdentifiers.numericId - bIdentifiers.numericId;
        }
        if (aHasNumeric) return -1;
        if (bHasNumeric) return 1;
        const aLabel = aIdentifiers.stringId || (typeof a?.name === 'string' ? a.name.trim() : '');
        const bLabel = bIdentifiers.stringId || (typeof b?.name === 'string' ? b.name.trim() : '');
        return aLabel.localeCompare(bLabel, undefined, { numeric: true, sensitivity: 'base' });
      }

      function mergeLegendRoutes(primaryRoutes, additionalRoutes) {
        const mergedMap = new Map();
        let autoKeyCounter = 0;

        const appendRoute = (route, shouldOverride = false) => {
          if (!route || typeof route !== 'object') return;
          const key = buildLegendRouteKey(route);
          const mapKey = key !== null ? key : `auto:${autoKeyCounter++}`;
          if (mergedMap.has(mapKey)) {
            if (shouldOverride) {
              mergedMap.set(mapKey, route);
            }
            return;
          }
          mergedMap.set(mapKey, route);
        };

        (Array.isArray(primaryRoutes) ? primaryRoutes : []).forEach(route => appendRoute(route, true));
        (Array.isArray(additionalRoutes) ? additionalRoutes : []).forEach(route => appendRoute(route, false));

        const mergedRoutes = Array.from(mergedMap.values());
        mergedRoutes.sort(compareLegendRoutes);
        return mergedRoutes;
      }

      function buildCatLegendEntry(routeKey) {
        if (!catOverlayEnabled) {
          return null;
        }

        const normalizedKey = catRouteKey(routeKey);
        if (!normalizedKey || isCatOutOfServiceRouteValue(normalizedKey)) {
          return null;
        }

        const routeInfo = getCatRouteInfo(normalizedKey);
        const nameCandidates = [
          routeInfo?.displayName,
          routeInfo?.shortName,
          routeInfo?.longName,
          normalizedKey
        ];
        const legendName = nameCandidates.find(value => typeof value === 'string' && value.trim() !== '');
        const name = legendName ? legendName.trim() : `CAT Route ${normalizedKey}`;

        let description = '';
        const longName = typeof routeInfo?.longName === 'string' ? routeInfo.longName.trim() : '';
        if (longName && longName.toUpperCase() !== name.toUpperCase()) {
          description = longName;
        }

        const colorCandidates = [
          routeInfo?.color,
          getCatRouteColor(normalizedKey),
          CAT_VEHICLE_MARKER_DEFAULT_COLOR
        ];
        const colorCandidate = colorCandidates.find(value => typeof value === 'string' && value.trim() !== '');
        const color = sanitizeCssColor(colorCandidate) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;

        return {
          routeId: Number.isFinite(routeInfo?.id) ? routeInfo.id : normalizedKey,
          routeKey: normalizedKey,
          legendKey: `cat:${normalizedKey}`,
          name,
          description,
          color,
          isCatRoute: true
        };
      }

      function deriveCatLegendRoutes() {
        if (!catOverlayEnabled) {
          return [];
        }

        const visibleKeys = new Set();
        const addKeyIfVisible = candidate => {
          const normalized = catRouteKey(candidate);
          if (!normalized || isCatOutOfServiceRouteValue(normalized)) {
            return;
          }
          if (!isCatRouteVisible(normalized)) {
            return;
          }
          visibleKeys.add(normalized);
        };

        if (catRouteSelections instanceof Map) {
          catRouteSelections.forEach((selected, key) => {
            if (selected) {
              addKeyIfVisible(key);
            }
          });
        }

        if (catActiveRouteKeys instanceof Set) {
          catActiveRouteKeys.forEach(addKeyIfVisible);
        }

        catRoutePatternGeometries.forEach(geometry => {
          if (!geometry) {
            return;
          }
          addKeyIfVisible(geometry.routeKey);
        });

        catVehiclesById.forEach(vehicle => {
          if (!vehicle) {
            return;
          }
          const candidateKey = vehicle.catEffectiveRouteKey ?? vehicle.routeKey ?? vehicle.routeId;
          addKeyIfVisible(candidateKey);
        });

        if (visibleKeys.size === 0) {
          return [];
        }

        const legendEntries = [];
        visibleKeys.forEach(key => {
          const entry = buildCatLegendEntry(key);
          if (entry) {
            legendEntries.push(entry);
          }
        });

        legendEntries.sort(compareLegendRoutes);
        return legendEntries;
      }

      function buildLegendEntryFromState(routeId) {
        const numericRouteId = Number(routeId);
        if (!Number.isFinite(numericRouteId)) return null;
        if (numericRouteId === 0) {
          return createOutOfServiceLegendEntry();
        }

        const storedRoute = allRoutes?.[numericRouteId] || allRoutes?.[`${numericRouteId}`] || {};
        const routeIdLabel = `${numericRouteId}`;

        const nameCandidates = [
          storedRoute.Description,
          storedRoute.Name,
          storedRoute.RouteName
        ];
        const legendName = nameCandidates.find(value => typeof value === 'string' && value.trim() !== '');
        const name = legendName ? legendName.trim() : (routeIdLabel ? `Route ${routeIdLabel}` : 'Route');

        const descriptionCandidates = [
          storedRoute.InfoText,
          storedRoute.Description,
          storedRoute.RouteDescription
        ];
        const legendDescription = descriptionCandidates.find(value => typeof value === 'string' && value.trim() !== '');
        const description = legendDescription ? legendDescription.trim() : '';

        const color = getRouteColor(numericRouteId);

        return {
          routeId: numericRouteId,
          name,
          description,
          color
        };
      }

      function deriveLegendRoutesFromState(options = {}) {
        const { includeAllAvailableRoutes = false } = options || {};
        const legendEntries = [];
        const seenRouteIds = new Set();

        const addRouteId = candidateId => {
          const numericRouteId = Number(candidateId);
          if (!Number.isFinite(numericRouteId)) return;
          if (seenRouteIds.has(numericRouteId)) return;
          if (!canDisplayRoute(numericRouteId)) return;
          if (!includeAllAvailableRoutes && !isRouteSelected(numericRouteId)) return;

          const legendEntry = buildLegendEntryFromState(numericRouteId);
          if (!legendEntry) return;

          seenRouteIds.add(numericRouteId);
          legendEntries.push(legendEntry);
        };

        if (includeAllAvailableRoutes) {
          Object.keys(allRoutes).forEach(routeIdKey => {
            if (!Object.prototype.hasOwnProperty.call(allRoutes, routeIdKey)) return;
            addRouteId(routeIdKey);
          });
        } else {
          if (activeRoutes instanceof Set) {
            activeRoutes.forEach(addRouteId);
          } else if (Array.isArray(activeRoutes)) {
            activeRoutes.forEach(addRouteId);
          }

          Object.keys(routeSelections).forEach(routeIdKey => {
            if (!Object.prototype.hasOwnProperty.call(routeSelections, routeIdKey)) return;
            if (!routeSelections[routeIdKey]) return;
            addRouteId(routeIdKey);
          });
        }

        legendEntries.sort((a, b) => a.routeId - b.routeId);

        return legendEntries;
      }

      function computeLegendSignature(routes) {
        if (!Array.isArray(routes) || routes.length === 0) {
          return '';
        }
        return routes
          .map(route => {
            const routeId = route?.routeId ?? route?.routeID ?? route?.id ?? '';
            const name = route?.name ?? '';
            const description = route?.description ?? '';
            const color = route?.color ?? '';
            const isCatRoute = route?.isCatRoute ? '1' : '0';
            const routeKey = route?.routeKey ?? '';
            return [routeId, name, description, color, isCatRoute, routeKey]
              .map(value => (value === null || value === undefined ? '' : String(value)))
              .join('|');
          })
          .join(';');
      }

      function updateRouteLegend(displayedRoutes = [], options = {}) {
        const legend = getCachedElementById("routeLegend");
        if (!legend) return;

        const { forceHide = false, preserveOnEmpty = false } = options || {};
        const shouldShowLegend = isKioskExperienceActive();

        if (!shouldShowLegend || forceHide) {
          legend.style.display = "none";
          if (typeof legend.replaceChildren === 'function') {
            legend.replaceChildren();
          } else {
            legend.innerHTML = "";
          }
          lastRenderedLegendRoutes = [];
          lastRenderedLegendSignature = '';
          return;
        }

        const normalizedRoutes = Array.isArray(displayedRoutes) ? displayedRoutes : [];
        const filteredRoutes = normalizedRoutes.filter(route => {
          const candidateId = route?.routeId ?? route?.routeID ?? route?.id;
          if (adminKioskMode) {
            return routeHasActiveVehicles(candidateId);
          }
          return isRoutePublicById(candidateId);
        });

        const sanitizedRoutes = filteredRoutes.map(route => {
          const rawRouteId = route.routeId ?? route.routeID ?? route.id;
          const routeIdLabel = rawRouteId === undefined || rawRouteId === null ? "" : `${rawRouteId}`;
          const rawName = typeof route.name === "string" ? route.name : "";
          const name = rawName.trim() !== "" ? rawName.trim() : (routeIdLabel ? `Route ${routeIdLabel}` : "Route");
          const rawDescription = typeof route.description === "string" ? route.description : "";
          const description = rawDescription.trim();
          const color = typeof route.color === "string" && route.color.trim() !== "" ? route.color : "#000000";
          return {
            routeId: rawRouteId,
            name,
            description,
            color
          };
        });

        const catLegendRoutes = deriveCatLegendRoutes();
        const combinedSanitizedRoutes = mergeLegendRoutes(sanitizedRoutes, catLegendRoutes);

        const shouldIncludeOutOfServiceLegend = isRouteSelected(0) && routeHasActiveVehicles(0);
        if (shouldIncludeOutOfServiceLegend) {
          const hasOutOfServiceEntry = combinedSanitizedRoutes.some(route => {
            const candidateId = route?.routeId ?? route?.routeID ?? route?.id;
            return Number(candidateId) === 0;
          });
          if (!hasOutOfServiceEntry) {
            combinedSanitizedRoutes.push(createOutOfServiceLegendEntry());
          }
        }

        const filterAdminLegendRoutes = routes => {
          if (!adminKioskMode) {
            return Array.isArray(routes) ? routes : [];
          }
          if (!Array.isArray(routes)) return [];
          return routes.filter(route => {
            if (route?.isCatRoute) {
              const candidateKey = catRouteKey(route.routeKey ?? route.routeId ?? route.routeID ?? route.id);
              if (!candidateKey) {
                return false;
              }
              if (isCatOutOfServiceRouteValue(candidateKey)) {
                return isOutOfServiceRouteVisible();
              }
              return isCatRouteVisible(candidateKey);
            }
            const candidateId = route?.routeId ?? route?.routeID ?? route?.id;
            return routeHasActiveVehicles(candidateId);
          });
        };

        let routesToRender = filterAdminLegendRoutes(combinedSanitizedRoutes);

        if (routesToRender.length === 0) {
          let fallbackRoutes = deriveLegendRoutesFromState({
            includeAllAvailableRoutes: adminKioskMode
          });
          fallbackRoutes = mergeLegendRoutes(fallbackRoutes, catLegendRoutes);
          fallbackRoutes = filterAdminLegendRoutes(fallbackRoutes);
          if (fallbackRoutes.length > 0) {
            routesToRender = fallbackRoutes;
          } else if (preserveOnEmpty && lastRenderedLegendRoutes.length > 0) {
            renderRouteLegendContent(legend, lastRenderedLegendRoutes);
            return;
          } else {
            legend.style.display = "none";
            legend.innerHTML = "";
            lastRenderedLegendRoutes = [];
            lastRenderedLegendSignature = '';
            return;
          }
        } else if (adminKioskMode) {
          let additionalRoutes = deriveLegendRoutesFromState({ includeAllAvailableRoutes: true });
          additionalRoutes = filterAdminLegendRoutes(additionalRoutes);
          if (additionalRoutes.length > 0) {
            routesToRender = mergeLegendRoutes(routesToRender, additionalRoutes);
          }
          routesToRender = filterAdminLegendRoutes(routesToRender);
        }

        if (routesToRender.length === 0) {
          if (preserveOnEmpty && lastRenderedLegendRoutes.length > 0) {
            renderRouteLegendContent(legend, lastRenderedLegendRoutes);
          } else {
            legend.style.display = "none";
            legend.innerHTML = "";
            lastRenderedLegendRoutes = [];
            lastRenderedLegendSignature = '';
          }
          return;
        }

        const nextSignature = computeLegendSignature(routesToRender);
        lastRenderedLegendRoutes = routesToRender;
        if (nextSignature === lastRenderedLegendSignature) {
          legend.style.display = 'block';
          return;
        }
        lastRenderedLegendSignature = nextSignature;
        renderRouteLegendContent(legend, routesToRender);
      }

      // refreshMap updates route paths and bus locations.
      function refreshMap() {
        fetchBusLocations().then(fetchRoutePaths);
        const hasTranslocStops = Array.isArray(stopDataCache) && stopDataCache.length > 0;
        const hasCatStops = catOverlayEnabled && Array.isArray(catStopDataCache) && catStopDataCache.length > 0;
        if (hasTranslocStops || hasCatStops) {
          renderStopsIfDisplayChanged();
        }
        if (trainsFeature.visible && trainsFeatureAllowed()) {
          fetchTrains().catch(error => console.error('Error refreshing trains:', error));
        }
        if (catOverlayEnabled) {
          renderCatVehiclesUsingCache();
        }
        if (shouldPollOnDemandData()) {
          fetchOnDemandVehicles().catch(error => console.error('Failed to refresh OnDemand vehicles:', error));
        }
      }

      function clearRefreshIntervals() {
        refreshIntervals.forEach(clearInterval);
        refreshIntervals = [];
        refreshIntervalsActive = false;
        stopTrainPolling();
      }

      function startRefreshIntervals() {
        if (refreshIntervalsActive) {
          return;
        }
        refreshIntervals.push(setInterval(fetchBusLocations, 4000));
        refreshIntervals.push(setInterval(fetchBusStops, 60000));
        refreshIntervals.push(setInterval(fetchBlockAssignments, 60000));
        refreshIntervals.push(setInterval(() => {
          fetchStopArrivalTimes().then(allEtas => {
            cachedEtas = allEtas;
            updateCustomPopups();
          });
        }, 15000));
        refreshIntervals.push(setInterval(fetchRoutePaths, 15000));
        refreshIntervals.push(setInterval(() => {
          if (shouldFetchServiceAlerts()) {
            fetchServiceAlertsForCurrentAgency();
          }
        }, SERVICE_ALERT_REFRESH_INTERVAL_MS));
        if (incidentsAreAvailable()) {
          refreshIntervals.push(setInterval(refreshIncidents, INCIDENT_REFRESH_INTERVAL_MS));
          refreshIncidents();
        } else {
          setIncidentsVisibility(false);
        }
        if (trainsFeature.visible && trainsFeatureAllowed()) {
          startTrainPolling().catch(error => console.error('Failed to fetch trains:', error));
        } else {
          stopTrainPolling();
        }
        if (shouldFetchServiceAlerts()) {
          fetchServiceAlertsForCurrentAgency();
        }
        if (shouldPollOnDemandData()) {
          startOnDemandPolling();
        }
        refreshIntervalsActive = true;
      }

      function handleVisibilityChange() {
        if (typeof document === 'undefined') {
          return;
        }
        const hidden = document.hidden;
        if (hidden && !refreshSuspendedForVisibility) {
          refreshSuspendedForVisibility = true;
          if (shouldPollOnDemandData() && onDemandPollingTimerId !== null) {
            stopOnDemandPolling();
            onDemandPollingPausedForVisibility = true;
          }
          clearRefreshIntervals();
          return;
        }
        if (!hidden && refreshSuspendedForVisibility) {
          refreshSuspendedForVisibility = false;
          refreshMap();
          startRefreshIntervals();
          if (shouldPollOnDemandData() && (onDemandPollingPausedForVisibility || onDemandPollingTimerId === null)) {
            onDemandPollingPausedForVisibility = false;
            startOnDemandPolling();
          }
        }
      }

      if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
        document.addEventListener('visibilitychange', handleVisibilityChange);
      }

      function showCookieBanner() {
        if (isKioskExperienceActive()) {
          return;
        }
        if (localStorage.getItem('agencyConsent') !== 'true') {
          const banner = document.getElementById('cookieBanner');
          banner.style.display = 'block';
          document.getElementById('cookieAccept').addEventListener('click', () => {
            localStorage.setItem('agencyConsent', 'true');
            localStorage.setItem('selectedAgency', baseURL);
            banner.style.display = 'none';
          });
        }
      }

      function loadAgencyData() {
        resetTranslocSnapshotCache();
        return fetchRouteColors().then(() => {
          const stopArrivalsPromise = fetchStopArrivalTimes().then(allEtas => {
            cachedEtas = allEtas || {};
            updateCustomPopups();
            return allEtas;
          });
          const headingCachePromise = loadVehicleHeadingCache();
          const tasks = [
            fetchBusStops(),
            fetchBlockAssignments(),
            headingCachePromise.then(() => fetchBusLocations().then(() => fetchRoutePaths())),
            stopArrivalsPromise
          ];
          if (shouldFetchServiceAlerts()) {
            tasks.push(fetchServiceAlertsForCurrentAgency());
          }
          return Promise.allSettled(tasks);
        });
      }

      function changeAgency(url) {
        if (localStorage.getItem('agencyConsent') === 'true') {
          localStorage.setItem('selectedAgency', url);
        }
        beginAgencyLoad();
        clearRefreshIntervals();
        resetTranslocSnapshotCache();
        const previousBaseURL = baseURL;
        baseURL = url;
        if (catOverlayEnabled && !catOverlayIsAvailable()) {
          disableCatOverlay();
        }
        if (previousBaseURL !== baseURL) {
          busLocationsFetchPromise = null;
          busLocationsFetchBaseURL = null;
          routePathsFetchPromise = null;
          routePathsFetchBaseURL = null;
        }
        resetIncidentAlertState();
        resetServiceAlertsState();
        updateControlPanel();
        enforceIncidentVisibilityForCurrentAgency();
        Object.values(markers).forEach(m => map.removeLayer(m));
        markers = {};
        clearAllTrainMarkers();
        Object.values(nameBubbles).forEach(b => {
          if (b.speedMarker) map.removeLayer(b.speedMarker);
          if (b.nameMarker) map.removeLayer(b.nameMarker);
          if (b.blockMarker) map.removeLayer(b.blockMarker);
          if (b.routeMarker) map.removeLayer(b.routeMarker);
          if (b.catRouteMarker) map.removeLayer(b.catRouteMarker);
        });
        nameBubbles = {};
        clearStopMarkerCache();
        routeLayers.forEach(l => map.removeLayer(l));
        routeLayers = [];
        routePolylineCache.clear();
        lastRouteRenderState = {
          selectionKey: '',
          colorSignature: '',
          geometrySignature: '',
          useOverlapRenderer: !!(enableOverlapDashRendering && overlapRenderer)
        };
        lastRouteSelectorSignature = null;
        if (overlapRenderer) {
          overlapRenderer.reset();
        }
        currentTranslocRendererGeometries = new Map();
        currentTranslocSelectedRouteIds = [];
        resetCatOverlapRenderingState();
        busBlocks = {};
        previousBusData = {};
        cachedEtas = {};
        catStopEtaCache.clear();
        catStopEtaRequests.clear();
        customPopups.forEach(p => p.remove());
        customPopups = [];
        allRoutes = {};
        routeSelections = {};
        routeStopAddressMap = {};
        routeStopRouteMap = {};
        activeRoutes = new Set();
        kioskVehicleStatusKnown = false;
        updateKioskStatusMessage({ known: false, hasActiveVehicles: false });
        routeColors = {};
        routeVisibility = {};
        allRouteBounds = null;
        mapHasFitAllRoutes = false;
        updateRouteLegend([], { forceHide: true });
        updateRouteSelector(new Set(), true);
        loadAgencyData()
          .then(() => {
            startRefreshIntervals();
          })
          .catch(error => {
            console.error('Error loading agency data:', error);
          })
          .finally(() => {
            completeAgencyLoad();
          });
      }

      function getRouteColor(routeID) {
        if (routeID === 0) return outOfServiceRouteColor;
        const numeric = Number(routeID);
        if (!Number.isNaN(numeric) && isCatOverlapRouteId(numeric)) {
          const info = catOverlapInfoByNumericId.get(numeric);
          if (info && info.color) {
            return info.color;
          }
          if (info && info.routeKey) {
            const fallbackColor = sanitizeCssColor(getCatRouteColor(info.routeKey));
            if (fallbackColor) {
              return fallbackColor;
            }
          }
          return CAT_VEHICLE_MARKER_DEFAULT_COLOR;
        }
        return routeColors[routeID] || '#000000';
      }

      function initMap() {
          map = L.map('map', {
              zoomControl: false,
              crs: L.CRS.EPSG3857,
              zoomAnimation: true,
              markerZoomAnimation: true
          }).setView(INITIAL_MAP_VIEW.center, INITIAL_MAP_VIEW.zoom);
          map.on('popupclose', handleDispatcherPopupClosed);
          map.on('click', () => {
              closeCatVehicleTooltip();
          });
          map.on('moveend', renderOnDemandStops);
          map.on('zoomend', renderOnDemandStops);
          map.createPane(RADAR_PANE_NAME);
          const radarPane = map.getPane(RADAR_PANE_NAME);
          if (radarPane) {
              radarPane.style.zIndex = 350;
              radarPane.style.pointerEvents = 'none';
          }
          sharedRouteRenderer = L.svg({ padding: 0 });
          if (sharedRouteRenderer) {
              map.addLayer(sharedRouteRenderer);
          }
          map.createPane('stopsPane');
          const stopsPane = map.getPane('stopsPane');
          if (stopsPane) {
              stopsPane.style.zIndex = 450;
              stopsPane.style.pointerEvents = 'auto';
          }
          map.createPane('ondemandStopsPane');
          const ondemandStopsPane = map.getPane('ondemandStopsPane');
          if (ondemandStopsPane) {
              ondemandStopsPane.style.zIndex = 455;
              ondemandStopsPane.style.pointerEvents = 'auto';
          }
          map.createPane('busesPane');
          const busesPane = map.getPane('busesPane');
          if (busesPane) {
              busesPane.style.zIndex = 500;
              busesPane.style.pointerEvents = 'auto';
          }
          map.createPane(catVehiclesPaneName);
          const catPane = map.getPane(catVehiclesPaneName);
          if (catPane) {
              catPane.style.zIndex = 505;
              catPane.style.pointerEvents = 'auto';
          }
          map.createPane('incidentHalosPane');
          const incidentHalosPane = map.getPane('incidentHalosPane');
          if (incidentHalosPane) {
              incidentHalosPane.style.zIndex = 540;
              incidentHalosPane.style.pointerEvents = 'none';
          }
          map.createPane('incidentsPane');
          const incidentsPane = map.getPane('incidentsPane');
          if (incidentsPane) {
              incidentsPane.style.zIndex = 550;
              incidentsPane.style.pointerEvents = 'auto';
          }
          incidentLayerGroup = L.layerGroup();
          incidentMarkers.forEach(entry => {
              if (entry && entry.marker) {
                  incidentLayerGroup.addLayer(entry.marker);
              }
          });
          if (incidentsVisible) {
              incidentLayerGroup.addTo(map);
          }
          if (!incidentHaloLayerGroup) {
              incidentHaloLayerGroup = L.layerGroup();
          }
          incidentMarkers.forEach(entry => {
              if (entry && entry.haloMarker) {
                  incidentHaloLayerGroup.addLayer(entry.haloMarker);
              }
          });
          if (incidentsVisible && incidentHaloLayerGroup) {
              incidentHaloLayerGroup.addTo(map);
          }
          const cartoLight = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
              attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
          });
          cartoLight.addTo(map);
          applyRadarState();

          if (enableOverlapDashRendering) {
            overlapRenderer = new OverlapRouteRenderer(map, {
              sampleStepPx: 8,
              dashLengthPx: 16,
              minDashLengthPx: 0.5,
              matchTolerancePx: 6,
              strokeWeight: DEFAULT_ROUTE_STROKE_WEIGHT,
              minStrokeWeight: MIN_ROUTE_STROKE_WEIGHT,
              maxStrokeWeight: MAX_ROUTE_STROKE_WEIGHT,
              renderer: sharedRouteRenderer,
              pane: routePaneName
            });
            map.on('zoomend', () => {
              if (overlapRenderer) {
                overlapRenderer.handleZoomEnd();
              }
            });
          }

          if (isKioskExperienceActive()) {
            ensurePanelsHiddenForKioskExperience();
          }
          map.on('zoom', () => {
              scheduleMarkerScaleUpdate();
              updatePopupPositions();
          });
          map.on('move', () => {
              updatePopupPositions();
          });
          map.on('moveend', () => {
              scheduleStopRendering();
              updateTrainMarkersVisibility().catch(error => console.error('Error updating train markers visibility:', error));
          });
          map.on('zoomend', () => {
              scheduleStopRendering();
              scheduleMarkerScaleUpdate();
              updatePopupPositions();
              renderCatRoutes();
              updateTrainMarkersVisibility().catch(error => console.error('Error updating train markers visibility:', error));
          });
          if (planesFeature.visible && window.PlaneLayer && typeof window.PlaneLayer.init === 'function') {
              try {
                  window.PlaneLayer.init(map);
              } catch (error) {
                  console.error('PlaneLayer init failed:', error);
                  planesFeature.visible = false;
                  updateAircraftToggleButton();
              }
          }
          applyIncidentHaloStates();
          registerVehicleFollowInteractionHandlers();
      }

      async function fetchBusStops() {
          const currentBaseURL = baseURL;
          try {
              const snapshot = await loadTranslocSnapshot();
              if (currentBaseURL !== baseURL) return;
              const stopsArray = Array.isArray(snapshot?.stops) ? snapshot.stops : [];
              if (stopsArray.length > 0) {
                  stopDataCache = stopsArray;
                  renderBusStops(stopDataCache);
              }
          } catch (error) {
              console.error('Error fetching bus stops:', error);
          }
      }

      function groupStopsByPixelDistance(stops, thresholdPx) {
          if (!Array.isArray(stops) || stops.length === 0) {
              return [];
          }

          const validStops = stops.map(stop => {
              const latitude = parseFloat(stop.Latitude ?? stop.latitude ?? stop.lat);
              const longitude = parseFloat(stop.Longitude ?? stop.longitude ?? stop.lon);
              if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
                  return null;
              }
              return { latitude, longitude, stop };
          }).filter(entry => entry !== null);

          if (!map) {
              return validStops.map(entry => ({
                  latitude: entry.latitude,
                  longitude: entry.longitude,
                  stops: [entry.stop]
              }));
          }

          const bucketSize = Math.max(1, Number.isFinite(thresholdPx) ? thresholdPx : 1);
          const bucketMap = new Map();
          const groups = [];

          const getBucketKey = (x, y) => `${x}:${y}`;

          const registerGroupInBucket = (bucketKey, index) => {
              if (!bucketMap.has(bucketKey)) {
                  bucketMap.set(bucketKey, new Set());
              }
              bucketMap.get(bucketKey).add(index);
          };

          const unregisterGroupFromBucket = (bucketKey, index) => {
              const bucket = bucketMap.get(bucketKey);
              if (!bucket) {
                  return;
              }
              bucket.delete(index);
              if (bucket.size === 0) {
                  bucketMap.delete(bucketKey);
              }
          };

          validStops.forEach(({ latitude, longitude, stop }) => {
              const stopPoint = map.latLngToLayerPoint([latitude, longitude]);
              if (!stopPoint) {
                  return;
              }

              const cellX = Math.round(stopPoint.x / bucketSize);
              const cellY = Math.round(stopPoint.y / bucketSize);

              let targetGroupIndex = null;
              for (let dx = -1; dx <= 1 && targetGroupIndex === null; dx++) {
                  for (let dy = -1; dy <= 1 && targetGroupIndex === null; dy++) {
                      const neighborKey = getBucketKey(cellX + dx, cellY + dy);
                      const neighborBucket = bucketMap.get(neighborKey);
                      if (!neighborBucket) {
                          continue;
                      }
                      for (const groupIndex of neighborBucket) {
                          const group = groups[groupIndex];
                          if (!group || !group.point) {
                              continue;
                          }
                          if (stopPoint.distanceTo(group.point) <= thresholdPx) {
                              targetGroupIndex = groupIndex;
                              break;
                          }
                      }
                  }
              }

              if (targetGroupIndex !== null) {
                  const group = groups[targetGroupIndex];
                  group.stops.push(stop);
                  const totalStops = group.stops.length;
                  group.latitude = (group.latitude * (totalStops - 1) + latitude) / totalStops;
                  group.longitude = (group.longitude * (totalStops - 1) + longitude) / totalStops;
                  const newPoint = map.latLngToLayerPoint([group.latitude, group.longitude]);
                  if (newPoint) {
                      const oldBucketKey = group.bucketKey;
                      const newCellX = Math.round(newPoint.x / bucketSize);
                      const newCellY = Math.round(newPoint.y / bucketSize);
                      const newBucketKey = getBucketKey(newCellX, newCellY);
                      group.point = newPoint;
                      if (newBucketKey !== oldBucketKey) {
                          if (oldBucketKey) {
                              unregisterGroupFromBucket(oldBucketKey, targetGroupIndex);
                          }
                          registerGroupInBucket(newBucketKey, targetGroupIndex);
                          group.bucketKey = newBucketKey;
                      }
                  }
              } else {
                  const groupPoint = map.latLngToLayerPoint([latitude, longitude]);
                  if (!groupPoint) {
                      return;
                  }
                  const bucketKey = getBucketKey(cellX, cellY);
                  const groupIndex = groups.length;
                  groups.push({
                      latitude,
                      longitude,
                      stops: [stop],
                      point: groupPoint,
                      bucketKey
                  });
                  registerGroupInBucket(bucketKey, groupIndex);
              }
          });

          return groups.map(group => ({
              latitude: group.latitude,
              longitude: group.longitude,
              stops: group.stops
          }));
      }

      function sanitizeStopName(name) {
          if (typeof name !== 'string') {
              return '';
          }
          return name.replace(/^Stop Name:\s*/i, '').trim();
      }

      function normalizeIdentifier(value) {
          if (value === undefined || value === null) {
              return null;
          }
          const str = `${value}`.trim();
          return str === '' ? null : str;
      }

      function getSelectedRouteIdSet() {
          const selected = new Set();
          Object.keys(allRoutes).forEach(routeId => {
              const numericId = Number(routeId);
              if (!Number.isNaN(numericId) && isRouteSelected(numericId)) {
                  selected.add(numericId);
              }
          });
          return selected;
      }

      function normalizeRouteIdentifier(value) {
          if (value === undefined || value === null) {
              return null;
          }
          if (typeof value === 'string') {
              const trimmed = value.trim();
              if (!trimmed) {
                  return null;
              }
              if (trimmed.startsWith(ONDEMAND_STOP_ROUTE_PREFIX)) {
                  return trimmed;
              }
              const numeric = Number(trimmed);
              return Number.isNaN(numeric) ? null : numeric;
          }
          if (typeof value === 'number') {
              return Number.isNaN(value) ? null : value;
          }
          return null;
      }

      function buildStopEntriesFromStops(stops) {
          if (!Array.isArray(stops)) {
              return [];
          }

          const entriesByKey = new Map();
          stops.forEach(stop => {
              if (!stop) {
                  return;
              }

              const latitude = stop.Latitude ?? stop.latitude ?? stop.lat;
              const longitude = stop.Longitude ?? stop.longitude ?? stop.lon;
              const routeStopId = normalizeIdentifier(stop.RouteStopID ?? stop.RouteStopId);
              const addressIdFromStop = normalizeIdentifier(stop.AddressID ?? stop.AddressId);
              const addressIdFromMap = routeStopId ? normalizeIdentifier(routeStopAddressMap[routeStopId]) : null;
              const fallbackStopId = normalizeIdentifier(stop.StopID ?? stop.StopId);

              const key = addressIdFromStop
                  || addressIdFromMap
                  || (routeStopId ? `ROUTESTOP_${routeStopId}`
                      : (fallbackStopId ? `STOP_${fallbackStopId}`
                          : `LOC_${latitude}_${longitude}`));

              if (!entriesByKey.has(key)) {
                  entriesByKey.set(key, {
                      addressId: addressIdFromStop || addressIdFromMap || null,
                      routeStopIds: new Set(),
                      stopIds: new Set(),
                      catStopIds: new Set(),
                      names: new Set(),
                      routeIds: new Set(),
                      catRouteKeys: new Set(),
                      onDemandStops: []
                  });
              }

              const entry = entriesByKey.get(key);

              if (routeStopId) {
                  entry.routeStopIds.add(routeStopId);
              }

              const catRouteStopIds = stop.catRouteStopIds ?? stop.CatRouteStopIds;
              if (Array.isArray(catRouteStopIds)) {
                  catRouteStopIds.forEach(value => {
                      const normalized = normalizeIdentifier(value);
                      if (normalized) {
                          entry.routeStopIds.add(normalized);
                      }
                  });
              }

              if (fallbackStopId) {
                  entry.stopIds.add(fallbackStopId);
              }

              const addCatStopId = value => {
                  const keyValue = catStopKey(value);
                  if (keyValue) {
                      entry.catStopIds.add(keyValue);
                  }
              };

              if (stop.isCatStop) {
                  if (fallbackStopId) {
                      addCatStopId(fallbackStopId);
                  }
                  const explicitCatStopId = stop.catStopId ?? stop.CatStopId;
                  if (explicitCatStopId !== undefined && explicitCatStopId !== null) {
                      addCatStopId(explicitCatStopId);
                  }
                  const explicitCatStopIds = stop.catStopIds ?? stop.CatStopIds;
                  if (Array.isArray(explicitCatStopIds)) {
                      explicitCatStopIds.forEach(addCatStopId);
                  }
              }

              const descriptionCandidates = [
                  stop.Description,
                  stop.Name,
                  stop.StopName,
                  stop.Line1,
                  stop.SignVerbiage
              ];
              const name = descriptionCandidates.find(value => typeof value === 'string' && value.trim() !== '');
              if (name) {
                  entry.names.add(sanitizeStopName(name));
              }

              const explicitRouteId = normalizeRouteIdentifier(stop.RouteID ?? stop.RouteId);
              if (explicitRouteId !== null) {
                  entry.routeIds.add(explicitRouteId);
              }

              const routesArray = Array.isArray(stop.Routes) ? stop.Routes : [];
              routesArray.forEach(routeInfo => {
                  const candidateRouteId = normalizeRouteIdentifier(
                      routeInfo?.RouteID ?? routeInfo?.RouteId ?? routeInfo?.Id
                  );
                  if (candidateRouteId !== null) {
                      entry.routeIds.add(candidateRouteId);
                  }
              });

              const routeIdsList = Array.isArray(stop.RouteIDs ?? stop.RouteIds)
                  ? (stop.RouteIDs ?? stop.RouteIds)
                  : [];
              routeIdsList.forEach(routeIdValue => {
                  const normalizedRouteId = normalizeRouteIdentifier(routeIdValue);
                  if (normalizedRouteId !== null) {
                      entry.routeIds.add(normalizedRouteId);
                  }
              });

              const singleRoute = stop.Route ?? stop.route;
              if (singleRoute && typeof singleRoute === 'object') {
                  const singleRouteId = normalizeRouteIdentifier(
                      singleRoute.RouteID ?? singleRoute.RouteId ?? singleRoute.Id
                  );
                  if (singleRouteId !== null) {
                      entry.routeIds.add(singleRouteId);
                  }
              }

              if (stop.isOnDemandStop && stop.onDemandStopDetails) {
                  const details = stop.onDemandStopDetails;
                  entry.onDemandStops.push({
                      vehicleId: details.vehicleId ?? '',
                      routeId: details.routeId ?? '',
                      capacity: details.capacity ?? 1,
                      stopType: details.stopType ?? 'pickup',
                      address: typeof details.address === 'string' ? details.address : '',
                      serviceId: details.serviceId ?? null,
                      stopTimestamp: details.stopTimestamp ?? ''
                  });
                  const onDemandRouteId = normalizeRouteIdentifier(details.routeId);
                  if (onDemandRouteId !== null) {
                      entry.routeIds.add(onDemandRouteId);
                  }
              }

              const catKeysFromStop = extractCatRouteKeys(stop);
              catKeysFromStop.forEach(routeKey => entry.catRouteKeys.add(routeKey));
          });

          return Array.from(entriesByKey.values()).map(entry => {
              const stopIdsArray = Array.from(entry.stopIds);
              const catStopIdsArray = Array.from(entry.catStopIds);

              const normalizeTextId = id => {
                  if (id === undefined || id === null) {
                      return '';
                  }
                  const text = `${id}`;
                  return typeof text === 'string' ? text.trim() : '';
              };

              const normalizedCatStopIds = catStopIdsArray
                  .map(catStopKey)
                  .filter(id => typeof id === 'string' && id !== '');
              const remainingCatStopIds = new Set(normalizedCatStopIds);
              const formattedStopIds = [];

              stopIdsArray.forEach(rawId => {
                  const trimmedId = normalizeTextId(rawId);
                  if (!trimmedId) {
                      return;
                  }
                  const key = catStopKey(rawId);
                  if (key && remainingCatStopIds.has(key)) {
                      formattedStopIds.push(`${trimmedId}-CAT`);
                      remainingCatStopIds.delete(key);
                  } else {
                      formattedStopIds.push(trimmedId);
                  }
              });

              catStopIdsArray.forEach(rawId => {
                  const key = catStopKey(rawId);
                  if (!key || !remainingCatStopIds.has(key)) {
                      return;
                  }
                  const trimmedId = normalizeTextId(rawId);
                  if (!trimmedId) {
                      remainingCatStopIds.delete(key);
                      return;
                  }
                  formattedStopIds.push(`${trimmedId}-CAT`);
                  remainingCatStopIds.delete(key);
              });

              return {
                  addressId: entry.addressId,
                  routeStopIds: Array.from(entry.routeStopIds),
                  stopIds: stopIdsArray,
                  catStopIds: catStopIdsArray,
                  stopIdText: formattedStopIds.join(', '),
                  displayName: entry.names.size > 0 ? Array.from(entry.names).join(' / ') : 'Stop',
                  routeIds: Array.from(entry.routeIds),
                  catRouteKeys: Array.from(entry.catRouteKeys),
                  onDemandStops: Array.isArray(entry.onDemandStops) ? entry.onDemandStops.slice() : []
              };
          });
      }

      function collectRouteIdsForEntry(entry) {
          const routeIds = new Set();
          const catRouteKeys = new Set();
          if (!entry) {
              return { routeIds, catRouteKeys };
          }
          const entryRouteIds = entry.routeIds instanceof Set
              ? Array.from(entry.routeIds)
              : (Array.isArray(entry.routeIds) ? entry.routeIds : []);
          entryRouteIds.forEach(routeId => {
              const normalizedRouteId = normalizeRouteIdentifier(routeId);
              if (normalizedRouteId !== null) {
                  routeIds.add(normalizedRouteId);
              }
          });
          if (Array.isArray(entry.routeStopIds)) {
              entry.routeStopIds.forEach(routeStopId => {
                  const mapped = routeStopRouteMap[routeStopId];
                  const normalizedRouteId = normalizeRouteIdentifier(mapped);
                  if (normalizedRouteId !== null) {
                      routeIds.add(normalizedRouteId);
                  }
              });
          }

          const addCatRouteKey = value => {
              if (value === undefined || value === null) {
                  return;
              }
              if (value instanceof Set) {
                  value.forEach(addCatRouteKey);
                  return;
              }
              if (Array.isArray(value)) {
                  value.forEach(addCatRouteKey);
                  return;
              }
              const normalized = catRouteKey(value);
              if (normalized && normalized !== CAT_OUT_OF_SERVICE_ROUTE_KEY) {
                  catRouteKeys.add(normalized);
              }
          };

          addCatRouteKey(entry.catRouteKeys);

          return { routeIds, catRouteKeys };
      }

      function extractCatRouteKeys(stop) {
          const keys = new Set();
          if (!stop) {
              return keys;
          }

          const addKey = value => {
              if (value === undefined || value === null) {
                  return;
              }
              if (value instanceof Set) {
                  value.forEach(addKey);
                  return;
              }
              if (Array.isArray(value)) {
                  value.forEach(addKey);
                  return;
              }
              const normalized = catRouteKey(value);
              if (normalized && normalized !== CAT_OUT_OF_SERVICE_ROUTE_KEY) {
                  keys.add(normalized);
              }
          };

          addKey(stop.catRouteKeys);
          addKey(stop.CatRouteKeys);
          addKey(stop.catRouteKey);
          addKey(stop.CatRouteKey);
          addKey(stop.routeKeys);
          addKey(stop.RouteKeys);

          if (stop.isCatStop) {
              addKey(stop.rid ?? stop.Rid ?? stop.RID);
              addKey(stop.routeKey ?? stop.RouteKey);
              addKey(stop.RouteID ?? stop.RouteId ?? stop.routeID ?? stop.routeId);
          }

          return keys;
      }

      function collectRouteIdsForStop(stop) {
          const routeIds = new Set();
          const catRouteKeys = extractCatRouteKeys(stop);
          if (!stop) {
              return { routeIds, catRouteKeys };
          }

          const isCatStop = !!stop.isCatStop;

        const addRouteId = value => {
            if (value === undefined || value === null) {
                return;
            }
            if (value instanceof Set || Array.isArray(value)) {
                value.forEach(addRouteId);
                return;
            }
            const normalizedRouteId = normalizeRouteIdentifier(value);
            if (normalizedRouteId !== null) {
                routeIds.add(normalizedRouteId);
            }
        };

          if (!isCatStop) {
              addRouteId(stop.RouteID ?? stop.RouteId);

              const routeStopId = normalizeIdentifier(stop.RouteStopID ?? stop.RouteStopId);
              if (routeStopId) {
                  addRouteId(routeStopRouteMap[routeStopId]);
              }

              const routeIdsList = Array.isArray(stop.RouteIDs ?? stop.RouteIds)
                  ? (stop.RouteIDs ?? stop.RouteIds)
                  : [];
              routeIdsList.forEach(routeIdValue => addRouteId(routeIdValue));

              const routesArray = Array.isArray(stop.Routes) ? stop.Routes : [];
              routesArray.forEach(routeInfo => {
                  addRouteId(routeInfo?.RouteID ?? routeInfo?.RouteId ?? routeInfo?.Id ?? routeInfo);
              });

              const singleRoute = stop.Route ?? stop.route;
              if (singleRoute && typeof singleRoute === 'object') {
                  addRouteId(singleRoute.RouteID ?? singleRoute.RouteId ?? singleRoute.Id);
              }
          }

          return { routeIds, catRouteKeys };
      }

      function collectStopMarkerColors(routeIds, catRouteKeys = [], options = {}) {
          const weightedColors = [];
          const colorSet = new Set();

          const addWeightedColor = (color, weight = 1) => {
              const normalized = sanitizeCssColor(color);
              if (!normalized) {
                  return;
              }
              const safeWeight = Math.max(1, Math.round(weight));
              for (let i = 0; i < safeWeight; i += 1) {
                  weightedColors.push(normalized);
              }
          };

          const onDemandSegments = Array.isArray(options?.onDemandSegments) ? options.onDemandSegments : [];
          onDemandSegments.forEach(segment => {
              const weight = Number(segment?.totalCapacity ?? segment?.capacity ?? segment?.weight ?? 1);
              const color = segment?.color || getOnDemandVehicleColor(segment?.vehicleId);
              addWeightedColor(color, Number.isFinite(weight) && weight > 0 ? weight : 1);
          });

          const skipOnDemandRoutes = onDemandSegments.length > 0;
          const normalizedRouteIds = Array.isArray(routeIds) ? routeIds : [];
          normalizedRouteIds.forEach(routeId => {
              if (skipOnDemandRoutes && typeof routeId === 'string' && routeId.startsWith(ONDEMAND_STOP_ROUTE_PREFIX)) {
                  return;
              }
              const color = sanitizeCssColor(getRouteColor(routeId));
              if (color) {
                  colorSet.add(color);
              }
          });

          const catKeysArray = catRouteKeys instanceof Set
              ? Array.from(catRouteKeys)
              : (Array.isArray(catRouteKeys) ? catRouteKeys : [catRouteKeys]);
          catKeysArray.forEach(routeKey => {
              const normalized = catRouteKey(routeKey);
              if (!normalized || normalized === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
                  return;
              }
              const color = sanitizeCssColor(getCatRouteColor(normalized));
              if (color) {
                  colorSet.add(color);
              }
          });

          if (weightedColors.length > 0) {
              return weightedColors;
          }

          const colors = Array.from(colorSet);
          if (colors.length === 0) {
              return ['#FFFFFF'];
          }
          return colors;
      }

      function buildStopMarkerGradient(routeIds, catRouteKeys = [], options = {}) {
          const colors = collectStopMarkerColors(routeIds, catRouteKeys, options);
          if (colors.length <= 1) {
              return colors[0] || '#FFFFFF';
          }

          const segmentSize = 360 / colors.length;
          const segments = colors.map((color, index) => {
              const start = segmentSize * index;
              const end = segmentSize * (index + 1);
              return `${color} ${start}deg ${end}deg`;
          });
          return `conic-gradient(${segments.join(', ')})`;
      }

      function createStopMarkerIcon(routeIds, catRouteKeys = [], options = {}) {
          const colors = collectStopMarkerColors(routeIds, catRouteKeys, options);
          const colorKey = colors.slice().sort((a, b) => a.localeCompare(b, undefined, { numeric: true })).join('|');
          const size = STOP_MARKER_ICON_SIZE;
          const outline = Math.max(0, Number(STOP_MARKER_OUTLINE_WIDTH) || 0);
          const borderWidth = Math.max(0, Number(STOP_MARKER_BORDER_WIDTH) || 0);
          const devicePixelRatio = (typeof window !== 'undefined' && Number.isFinite(window.devicePixelRatio) && window.devicePixelRatio > 0)
              ? window.devicePixelRatio
              : 1;
          const cacheKey = `${colorKey}|${size}|${outline}|${borderWidth}|${devicePixelRatio}`;

          if (stopMarkerIconCache.has(cacheKey)) {
              return stopMarkerIconCache.get(cacheKey);
          }

          if (typeof document === 'undefined' || typeof document.createElement !== 'function') {
              const gradient = buildStopMarkerGradient(routeIds, catRouteKeys, options);
              const fallbackIcon = L.divIcon({
                  className: 'stop-marker-container leaflet-div-icon',
                  html: `<div class="stop-marker-outer" style="--stop-marker-size:${size}px;--stop-marker-border-color:${STOP_MARKER_BORDER_COLOR};--stop-marker-outline-size:${outline}px;--stop-marker-outline-color:${STOP_MARKER_OUTLINE_COLOR};--stop-marker-gradient:${gradient};"></div>`,
                  iconSize: [size, size],
                  iconAnchor: [size / 2, size / 2]
              });
              stopMarkerIconCache.set(cacheKey, fallbackIcon);
              return fallbackIcon;
          }

          const canvas = document.createElement('canvas');
          const context = canvas.getContext('2d');

          if (!context) {
              const gradient = buildStopMarkerGradient(routeIds, catRouteKeys, options);
              const fallbackIcon = L.divIcon({
                  className: 'stop-marker-container leaflet-div-icon',
                  html: `<div class="stop-marker-outer" style="--stop-marker-size:${size}px;--stop-marker-border-color:${STOP_MARKER_BORDER_COLOR};--stop-marker-outline-size:${outline}px;--stop-marker-outline-color:${STOP_MARKER_OUTLINE_COLOR};--stop-marker-gradient:${gradient};"></div>`,
                  iconSize: [size, size],
                  iconAnchor: [size / 2, size / 2]
              });
              stopMarkerIconCache.set(cacheKey, fallbackIcon);
              return fallbackIcon;
          }

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
                  colors.forEach(color => {
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
              className: 'leaflet-marker-icon stop-marker-image-icon'
          });

          stopMarkerIconCache.set(cacheKey, icon);
          return icon;
      }

      function createStopGroupKey(routeStopIds, fallbackStopIdText) {
          const normalizedIds = Array.isArray(routeStopIds)
              ? Array.from(new Set(routeStopIds
                  .map(id => `${id}`)
                  .map(value => value.trim())
                  .filter(value => value !== '' && value.toLowerCase() !== 'undefined' && value.toLowerCase() !== 'null')))
                  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
              : [];
          return `${JSON.stringify(normalizedIds)}|${fallbackStopIdText || ''}`;
      }

      function sanitizeCssColor(color) {
          if (typeof color !== 'string') {
              return '';
          }
          let trimmed = color.trim();
          if (trimmed.length === 0) {
              return '';
          }
          if (/^#[0-9a-fA-F]{3,8}$/.test(trimmed)) {
              return trimmed;
          }
          if (/^[0-9a-fA-F]{3,8}$/.test(trimmed)) {
              return `#${trimmed}`;
          }
          if (/^rgba?\(\s*\d+(?:\.\d+)?\s*,\s*\d+(?:\.\d+)?\s*,\s*\d+(?:\.\d+)?(?:\s*,\s*(?:0|1|0?\.\d+))?\s*\)$/i.test(trimmed)) {
              return trimmed.replace(/\s+/g, ' ');
          }
          if (/^hsla?\(\s*\d+(?:\.\d+)?(?:deg|rad|turn)?\s*,\s*\d+(?:\.\d+)?%\s*,\s*\d+(?:\.\d+)?%(?:\s*,\s*(?:0|1|0?\.\d+))?\s*\)$/i.test(trimmed)) {
              return trimmed.replace(/\s+/g, ' ');
          }
          if (/^[a-zA-Z]+$/.test(trimmed)) {
              return trimmed;
          }
          return '';
      }

      function getColorWithAlpha(color, alpha) {
          const safeAlpha = Math.min(1, Math.max(0, Number(alpha) || 0));
          if (typeof color !== 'string' || color.trim() === '') {
              return `rgba(0, 0, 0, ${safeAlpha})`;
          }

          const trimmed = color.trim();
          if (trimmed.startsWith('#')) {
              let hex = trimmed.slice(1);
              if (hex.length === 3 || hex.length === 4) {
                  hex = hex.split('').map(char => char + char).join('');
              }
              if (hex.length === 6 || hex.length === 8) {
                  const r = parseInt(hex.slice(0, 2), 16);
                  const g = parseInt(hex.slice(2, 4), 16);
                  const b = parseInt(hex.slice(4, 6), 16);
                  const baseAlpha = hex.length === 8 ? parseInt(hex.slice(6, 8), 16) / 255 : 1;
                  if ([r, g, b, baseAlpha].some(value => Number.isNaN(value))) {
                      return `rgba(0, 0, 0, ${safeAlpha})`;
                  }
                  const combinedAlpha = Math.round(Math.min(1, Math.max(0, baseAlpha * safeAlpha)) * 1000) / 1000;
                  return `rgba(${r}, ${g}, ${b}, ${combinedAlpha})`;
              }
          } else {
              const rgbaMatch = trimmed.match(/rgba?\(([^)]+)\)/i);
              if (rgbaMatch) {
                  const parts = rgbaMatch[1].split(',').map(part => part.trim());
                  if (parts.length >= 3) {
                      const r = parseFloat(parts[0]);
                      const g = parseFloat(parts[1]);
                      const b = parseFloat(parts[2]);
                      if ([r, g, b].some(value => Number.isNaN(value))) {
                          return `rgba(0, 0, 0, ${safeAlpha})`;
                      }
                      let baseAlpha = parts.length >= 4 ? parseFloat(parts[3]) : 1;
                      if (Number.isNaN(baseAlpha)) {
                          baseAlpha = 1;
                      }
                      const combinedAlpha = Math.round(Math.min(1, Math.max(0, baseAlpha * safeAlpha)) * 1000) / 1000;
                      return `rgba(${Math.round(r)}, ${Math.round(g)}, ${Math.round(b)}, ${combinedAlpha})`;
                  }
              }
          }

          return `rgba(0, 0, 0, ${safeAlpha})`;
      }

      function buildEtaTableHtml(routeStopIds, catStopIds = [], stopIds = []) {
          const normalizedRouteStopIds = Array.isArray(routeStopIds) ? routeStopIds : [];
          const etaEntries = [];

          normalizedRouteStopIds.forEach(routeStopId => {
              const key = typeof routeStopId === 'string' ? routeStopId.trim() : `${routeStopId}`.trim();
              if (!key) {
                  return;
              }
              const stopEtas = cachedEtas[key];
              if (!Array.isArray(stopEtas)) {
                  return;
              }
              stopEtas.forEach(eta => {
                  if (!eta) {
                      return;
                  }
                  const etaMinutes = Number(eta.etaMinutes);
                  etaEntries.push({
                      isCat: false,
                      routeStopId: key,
                      routeDescription: eta.routeDescription,
                      RouteId: eta.RouteId,
                      etaMinutes: Number.isFinite(etaMinutes) ? etaMinutes : null,
                      text: typeof eta.text === 'string' ? eta.text : ''
                  });
              });
          });

          const catStopIdSet = new Set();
          const addCatStopCandidate = value => {
              const normalized = catStopKey(value);
              if (normalized) {
                  catStopIdSet.add(normalized);
              }
          };
          if (Array.isArray(catStopIds)) {
              catStopIds.forEach(addCatStopCandidate);
          }
          if (Array.isArray(stopIds)) {
              stopIds.forEach(addCatStopCandidate);
          }

          const catStopIdList = Array.from(catStopIdSet);
          let hasCatEtaData = false;
          catStopIdList.forEach(stopId => {
              const cacheEntry = catStopEtaCache.get(stopId);
              if (!cacheEntry || !Array.isArray(cacheEntry.etas)) {
                  return;
              }
              if (cacheEntry.etas.length > 0) {
                  hasCatEtaData = true;
              }
              cacheEntry.etas.forEach(eta => {
                  if (!eta) {
                      return;
                  }
                  const etaMinutes = Number(eta.etaMinutes);
                  etaEntries.push(Object.assign({}, eta, {
                      isCat: true,
                      stopId,
                      etaMinutes: Number.isFinite(etaMinutes) ? etaMinutes : null,
                      text: typeof eta.text === 'string' ? eta.text : ''
                  }));
              });
          });

          const hasPendingCatRequest = catStopIdList.some(stopId => catStopEtaRequests.has(stopId));

          if (etaEntries.length === 0) {
              if (catStopIdList.length > 0 && hasPendingCatRequest && !hasCatEtaData) {
                  return '<div style="margin-top: 10px;">Loading arrival times…</div>';
              }
              return '<div style="margin-top: 10px;">No upcoming arrivals</div>';
          }

          const sortedEtas = etaEntries.slice().sort((a, b) => {
              const aMinutes = Number.isFinite(a.etaMinutes) ? a.etaMinutes : Number.POSITIVE_INFINITY;
              const bMinutes = Number.isFinite(b.etaMinutes) ? b.etaMinutes : Number.POSITIVE_INFINITY;
              if (aMinutes !== bMinutes) {
                  return aMinutes - bMinutes;
              }
              const aLabel = toNonEmptyString(a.routeDescription) || '';
              const bLabel = toNonEmptyString(b.routeDescription) || '';
              return aLabel.localeCompare(bLabel, undefined, { numeric: true, sensitivity: 'base' });
          });

          const etaRows = sortedEtas.map(eta => {
              const isCatEta = !!eta.isCat;
              let routeLabel = toNonEmptyString(eta.routeDescription) || '';
              let routeColor;
              if (isCatEta) {
                  const routeKey = catRouteKey(eta.routeKey ?? eta.routeId);
                  const routeInfo = getCatRouteInfo(routeKey);
                  if (!routeLabel) {
                      if (routeInfo) {
                          routeLabel = routeInfo.displayName || routeInfo.shortName || routeInfo.longName || '';
                      }
                      if (!routeLabel) {
                          if (routeKey) {
                              routeLabel = `Route ${routeKey}`;
                          } else if (Number.isFinite(Number(eta.routeId))) {
                              routeLabel = `Route ${Number(eta.routeId)}`;
                          }
                      }
                  }
                  routeColor = sanitizeCssColor(getCatRouteColor(routeKey || eta.routeId)) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
              } else {
                  if (!routeLabel) {
                      if (Number.isFinite(Number(eta.RouteId))) {
                          routeLabel = `Route ${Number(eta.RouteId)}`;
                      } else {
                          routeLabel = 'Route';
                      }
                  }
                  routeColor = sanitizeCssColor(getRouteColor(eta.RouteId)) || '#1f2937';
              }
              const textColor = contrastBW(routeColor);
              const shadowColor = getColorWithAlpha(routeColor, 0.35);
              const etaText = getEtaDisplayText(eta);
              return `<tr><td style="padding: 5px; text-align: center;"><div class="route-pill" style="background: ${routeColor}; color: ${textColor}; --route-pill-shadow-color: ${shadowColor};">${escapeHtml(routeLabel)}</div></td><td style="padding: 5px; text-align: center;">${escapeHtml(etaText)}</td></tr>`;
          }).join('');

          return `
            <table style="width: 100%; margin-top: 10px; border-collapse: collapse;">
              <thead>
                <tr>
                  <th style="border-bottom: 1px solid white; padding: 5px;">Route</th>
                  <th style="border-bottom: 1px solid white; padding: 5px;">ETA</th>
                </tr>
              </thead>
              <tbody>
                ${etaRows}
              </tbody>
            </table>
          `;
      }

      function getEtaDisplayText(eta) {
          if (!eta) {
              return '';
          }
          const textCandidate = toNonEmptyString(eta.text);
          if (textCandidate) {
              return textCandidate;
          }
          const minutes = Number(eta.etaMinutes);
          if (Number.isFinite(minutes)) {
              const rounded = Math.max(0, Math.round(minutes));
              return rounded <= 0 ? 'Arriving' : `${rounded} min`;
          }
          return 'Scheduled';
      }

      function buildStopEntriesSectionHtml(stopEntries, multipleStops) {
          if (!Array.isArray(stopEntries) || stopEntries.length === 0) {
              return '<div style="margin-top: 10px;">No upcoming arrivals</div>';
          }

          if (!multipleStops) {
              const entry = stopEntries[0];
              return buildEtaTableHtml(entry?.routeStopIds || [], entry?.catStopIds || [], entry?.stopIds || []);
          }

          return stopEntries.map(entry => {
              const entryTitle = entry.displayName ? `<span class="stop-entry-title">${sanitizeStopName(entry.displayName)}</span>` : '';
              const entryIdLine = entry.stopIdText ? `<span class="stop-entry-id">Stop ID: ${entry.stopIdText}</span>` : '';
              const entryAddressIdText = normalizeIdentifier(entry?.addressId);
              const entryAddressLine = entryAddressIdText ? `<span class="stop-entry-id">Stop ID: ${entryAddressIdText}</span>` : '';
              const tableHtml = buildEtaTableHtml(entry.routeStopIds || [], entry.catStopIds || [], entry.stopIds || []);
              return `<div class="stop-entry">${entryTitle}${entryIdLine}${entryAddressLine}${tableHtml}</div>`;
          }).join('');
      }

      function attachPopupCloseHandler(popupElement) {
          const closeButton = popupElement.querySelector('.custom-popup-close');
          if (!closeButton) {
              return;
          }
          closeButton.addEventListener('click', () => {
              popupElement.remove();
              customPopups = customPopups.filter(popup => popup !== popupElement);
          });
      }

      function setStopPopupContent(popupElement, groupInfo) {
          if (!popupElement || !groupInfo) {
              return;
          }

          popupElement.dataset.popupType = 'stop';
          const stopEntries = Array.isArray(groupInfo.stopEntries) ? groupInfo.stopEntries : [];
          const aggregatedRouteStopIds = Array.isArray(groupInfo.aggregatedRouteStopIds)
              ? groupInfo.aggregatedRouteStopIds
              : [];
          const fallbackStopIdText = typeof groupInfo.fallbackStopId === 'string'
              ? groupInfo.fallbackStopId
              : normalizeIdentifier(groupInfo.fallbackStopId) || '';
          const sanitizedStopName = sanitizeStopName(groupInfo.stopName || '');
          const multipleStops = stopEntries.length > 1;
          const primaryStopIdText = !multipleStops
              ? (stopEntries[0]?.stopIdText || fallbackStopIdText)
              : '';
          const entriesHtml = buildStopEntriesSectionHtml(stopEntries, multipleStops);
          const groupKey = groupInfo.groupKey || createStopGroupKey(aggregatedRouteStopIds, fallbackStopIdText);
          const primaryAddressIdText = !multipleStops
              ? normalizeIdentifier(stopEntries[0]?.addressId)
              : '';

          popupElement.dataset.routeStopIds = JSON.stringify(aggregatedRouteStopIds);
          popupElement.dataset.stopEntries = JSON.stringify(stopEntries);
          popupElement.dataset.stopName = sanitizedStopName;
          popupElement.dataset.fallbackStopId = fallbackStopIdText;
          popupElement.dataset.stopId = primaryStopIdText || '';
          popupElement.dataset.addressId = primaryAddressIdText || '';
          popupElement.dataset.groupKey = groupKey;

          const stopNameLine = (!multipleStops && sanitizedStopName)
              ? `<span class="stop-entry-title">${sanitizedStopName}</span><br>`
              : '';
          const addressIdLine = primaryAddressIdText ? `<span class="stop-entry-id">Address ID: ${primaryAddressIdText}</span><br>` : '';
          const stopIdLine = primaryStopIdText ? `<span class="stop-entry-id">Stop ID: ${primaryStopIdText}</span><br>` : '';

          popupElement.innerHTML = `
            <button class="custom-popup-close">&times;</button>
            ${stopNameLine}
            ${addressIdLine}
            ${stopIdLine}
            ${entriesHtml}
            <div class="custom-popup-arrow"></div>
          `;

          if (catOverlayEnabled) {
              const catStopIdsToFetch = new Set();
              stopEntries.forEach(entry => {
                  if (!entry || !Array.isArray(entry.catStopIds)) {
                      return;
                  }
                  entry.catStopIds.forEach(value => {
                      const key = catStopKey(value);
                      if (key) {
                          catStopIdsToFetch.add(key);
                      }
                  });
              });
              const fallbackCatStopId = catStopKey(fallbackStopIdText);
              if (fallbackCatStopId && (catStopIdsToFetch.size === 0 || catStopsById.has(fallbackCatStopId))) {
                  catStopIdsToFetch.add(fallbackCatStopId);
              }
              if (catStopIdsToFetch.size > 0) {
                  ensureCatStopEtas(Array.from(catStopIdsToFetch));
              }
          }

          attachPopupCloseHandler(popupElement);
      }

      function getIncidentLocationText(incident) {
          if (!incident) return '';
          const candidates = [
              incident.FullDisplayAddress,
              incident.DisplayAddress,
              incident.Address,
              incident.AddressName,
              incident.IncidentAddress,
              incident.LocationDescription,
              incident.Location,
              incident.CrossStreet,
              incident.Intersection,
              incident.NearestIntersection,
              incident.CommonName,
              incident.CommonLocation
          ];
          for (const value of candidates) {
              if (typeof value !== 'string') continue;
              const trimmed = value.trim();
              if (trimmed) {
                  return trimmed;
              }
          }
          return '';
      }

      function setIncidentPopupContent(popupElement, config) {
          if (!popupElement) {
              return;
          }

          popupElement.dataset.popupType = 'incident';
          const incident = config && config.incident ? config.incident : null;
          const idValue = typeof config?.id === 'string'
              ? config.id
              : (typeof config?.incidentId === 'string' ? config.incidentId : '');
          if (idValue) {
              popupElement.dataset.incidentId = idValue;
          } else {
              delete popupElement.dataset.incidentId;
          }

          if (!incident) {
              popupElement.innerHTML = `
                <button class="custom-popup-close">&times;</button>
                <div class="incident-popup">
                  <div>Incident information is unavailable.</div>
                </div>
                <div class="custom-popup-arrow"></div>
              `;
              attachPopupCloseHandler(popupElement);
              return;
          }

          const typeLabel = getIncidentTypeLabel(incident) || 'Incident';
          const safeTypeLabel = escapeHtml(typeLabel);
          const iconUrl = buildPulsePointListIconUrl(getIncidentTypeCode(incident));
          const iconAlt = typeLabel ? `${typeLabel} icon` : 'Incident icon';
          const iconHtml = iconUrl
              ? `<div class="incident-popup__icon"><img src="${escapeAttribute(iconUrl)}" alt="${escapeAttribute(iconAlt)}" onerror="this.style.display='none';"></div>`
              : `<div class="incident-popup__icon"><span class="incident-popup__icon-fallback">${escapeHtml((typeLabel || 'I').charAt(0))}</span></div>`;

          ensureIncidentFirstOnSceneTracking(incident, idValue);
          const timeInfo = getIncidentReceivedTimeInfo(incident);
          const receivedLine = timeInfo
              ? `<div class="incident-popup__meta-line" title="${escapeAttribute(timeInfo.full)}">Received ${escapeHtml(timeInfo.display)}</div>`
              : '';
          const onSceneInfo = incidentHasOnSceneUnits(incident)
              ? getIncidentFirstOnSceneTimeInfo(incident, idValue)
              : null;
          const onSceneLine = onSceneInfo
              ? `<div class="incident-popup__meta-line" title="${escapeAttribute(`First unit on-scene ${onSceneInfo.full}`)}">First unit on-scene ${escapeHtml(onSceneInfo.display)}</div>`
              : '';
          const statusCandidates = [incident.Status, incident.IncidentStatus, incident.Stage];
          let statusText = '';
          for (const value of statusCandidates) {
              if (typeof value !== 'string') continue;
              const trimmed = value.trim();
              if (trimmed) {
                  statusText = trimmed;
                  break;
              }
          }
          const statusLine = statusText
              ? `<div class="incident-popup__meta-line">Status: ${escapeHtml(statusText)}</div>`
              : '';
          const locationText = getIncidentLocationText(incident);
          const locationLine = locationText
              ? `<div class="incident-popup__meta-line">Location: ${escapeHtml(locationText)}</div>`
              : '';
          const metaLines = [receivedLine, onSceneLine, statusLine, locationLine].filter(Boolean).join('');
          const metaHtml = metaLines ? `<div class="incident-popup__meta">${metaLines}</div>` : '';

          const routes = Array.isArray(config?.routes) ? config.routes : [];
          const routeBadges = Array.isArray(routes)
              ? routes
                  .map(route => {
                      if (!route) return '';
                      if (typeof route === 'string') {
                          const trimmed = route.trim();
                          if (!trimmed) return '';
                          return `<span class="incident-popup__route">${escapeHtml(trimmed)}</span>`;
                      }
                      const nameCandidates = [route.name, route.RouteName, route.Description, route.Label];
                      let routeName = '';
                      for (const value of nameCandidates) {
                          if (typeof value !== 'string') continue;
                          const trimmed = value.trim();
                          if (trimmed) {
                              routeName = trimmed;
                              break;
                          }
                      }
                      if (!routeName) return '';
                      const colorCandidates = [route.color, route.Color, route.routeColor, route.RouteColor, route.fillColor, route.FillColor];
                      let routeColor = '';
                      for (const candidate of colorCandidates) {
                          const sanitized = sanitizeCssColor(candidate);
                          if (sanitized) {
                              routeColor = sanitized;
                              break;
                          }
                      }
                      if (!routeColor) {
                          const idCandidates = [route.routeId, route.RouteId, route.RouteID, route.rawRouteId, route.routeKey];
                          for (const idCandidate of idCandidates) {
                              if (idCandidate === undefined || idCandidate === null) continue;
                              const directColor = sanitizeCssColor(routeColors ? routeColors[idCandidate] : '');
                              if (directColor) {
                                  routeColor = directColor;
                                  break;
                              }
                              const numericId = Number(idCandidate);
                              if (!Number.isFinite(numericId)) continue;
                              const numericColor = sanitizeCssColor(routeColors ? routeColors[numericId] : '');
                              if (numericColor) {
                                  routeColor = numericColor;
                                  break;
                              }
                              const storedRoute = allRoutes ? (allRoutes[numericId] || allRoutes[`${numericId}`] || null) : null;
                              if (storedRoute) {
                                  const storedColor = sanitizeCssColor(storedRoute.MapLineColor || storedRoute.Color || storedRoute.RouteColor);
                                  if (storedColor) {
                                      routeColor = storedColor;
                                      break;
                                  }
                              }
                          }
                      }
                      const styleParts = [];
                      if (routeColor) {
                          styleParts.push(`background:${escapeAttribute(routeColor)}`);
                          styleParts.push(`border-color:${escapeAttribute(routeColor)}`);
                          const textColor = contrastBW(routeColor);
                          if (textColor) {
                              styleParts.push(`color:${escapeAttribute(textColor)}`);
                          }
                          const shadowColor = getColorWithAlpha(routeColor, 0.35);
                          if (shadowColor) {
                              styleParts.push(`box-shadow:0 10px 24px ${escapeAttribute(shadowColor)}`);
                          }
                      }
                      const styleAttr = styleParts.length ? ` style="${styleParts.join(';')}"` : '';
                      return `<span class="incident-popup__route"${styleAttr}>${escapeHtml(routeName)}</span>`;
                  })
                  .filter(Boolean)
              : [];
          const routesHtml = routeBadges.length
              ? `<div class="incident-popup__section"><div class="incident-popup__section-title">Routes Nearby</div><div class="incident-popup__routes-list">${routeBadges.join('')}</div></div>`
              : '';

          const units = extractIncidentUnits(incident);
          const unitsHtml = renderIncidentPopupUnitsSection(units);

          popupElement.innerHTML = `
            <button class="custom-popup-close">&times;</button>
            <div class="incident-popup">
              <div class="incident-popup__header">
                ${iconHtml}
                <div class="incident-popup__details">
                  <div class="incident-popup__title">${safeTypeLabel}</div>
                  ${metaHtml}
                </div>
              </div>
              ${routesHtml}
              ${unitsHtml}
            </div>
            <div class="custom-popup-arrow"></div>
          `;

          attachPopupCloseHandler(popupElement);
      }

      function getIncidentPopupElementById(id) {
          const normalizedId = getNormalizedIncidentId(id);
          if (!normalizedId) {
              return null;
          }
          for (const popupElement of customPopups) {
              if (!popupElement) continue;
              if (popupElement.dataset.popupType !== 'incident') continue;
              const popupId = popupElement.dataset.incidentId || '';
              if (popupId && getNormalizedIncidentId(popupId) === normalizedId) {
                  return popupElement;
              }
          }
          return null;
      }

      function buildIncidentPopupConfig(id) {
          const normalizedId = getNormalizedIncidentId(id);
          if (!normalizedId) {
              return null;
          }
          const entry = incidentMarkers.get(normalizedId);
          if (!entry || !entry.marker || typeof entry.marker.getLatLng !== 'function') {
              return null;
          }
          const latLng = entry.marker.getLatLng();
          if (!latLng) {
              return null;
          }
          const lat = Number(latLng.lat);
          const lng = Number(latLng.lng);
          if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
              return null;
          }
          const routesEntry = incidentsNearRoutesLookup.get(normalizedId);
          const routes = routesEntry && Array.isArray(routesEntry.routes) ? routesEntry.routes : [];
          return {
              popupType: 'incident',
              position: [lat, lng],
              incident: entry.data || null,
              id: normalizedId,
              routes
          };
      }

      function refreshIncidentPopup(id) {
          const popupElement = getIncidentPopupElementById(id);
          if (!popupElement) {
              return;
          }
          const config = buildIncidentPopupConfig(id);
          if (!config) {
              popupElement.remove();
              customPopups = customPopups.filter(popup => popup !== popupElement);
              return;
          }
          popupElement.dataset.position = `${config.position[0]},${config.position[1]}`;
          setIncidentPopupContent(popupElement, config);
          updatePopupPosition(popupElement, config.position);
      }

      function refreshOpenIncidentPopups() {
          const ids = customPopups
              .filter(popupElement => popupElement && popupElement.dataset.popupType === 'incident')
              .map(popupElement => popupElement.dataset.incidentId || '')
              .filter(Boolean);
          ids.forEach(id => {
              refreshIncidentPopup(id);
          });
      }

      function removeIncidentPopupById(id) {
          const popupElement = getIncidentPopupElementById(id);
          if (!popupElement) {
              return;
          }
          popupElement.remove();
          customPopups = customPopups.filter(popup => popup !== popupElement);
      }

      function removeAllIncidentPopups() {
          customPopups = customPopups.filter(popupElement => {
              if (!popupElement) {
                  return false;
              }
              if (popupElement.dataset.popupType === 'incident') {
                  popupElement.remove();
                  return false;
              }
              return true;
          });
      }

      function clearStopMarkerCache() {
          stopMarkerCache.forEach(entry => {
              if (!entry || !entry.marker) {
                  return;
              }
              try {
                  if (typeof entry.marker.off === 'function' && entry.clickHandler) {
                      entry.marker.off('click', entry.clickHandler);
                  }
                  if (map && typeof map.removeLayer === 'function') {
                      map.removeLayer(entry.marker);
                  }
              } catch (error) {
                  console.warn('Failed to remove stop marker from map:', error);
              }
          });
          stopMarkerCache.clear();
          stopMarkers = [];
          lastStopDisplaySignature = null;
      }

      function computeStopDisplaySignature() {
          const selectedRouteIds = Array.from(getSelectedRouteIdSet())
              .map(id => {
                  const numeric = Number(id);
                  return Number.isFinite(numeric) ? `${numeric}` : `${id}`;
              })
              .sort()
              .join(',');

          const parts = [`transloc:${selectedRouteIds}`];

          const outOfServiceVisible = typeof isOutOfServiceRouteVisible === 'function'
              ? isOutOfServiceRouteVisible()
              : false;

          if (!catOverlayEnabled) {
              parts.push('cat:off');
              parts.push(`out:${outOfServiceVisible ? '1' : '0'}`);
              return parts.join('|');
          }

          const explicitCatSelectionSet = new Set();
          if (catRouteSelections instanceof Map) {
              catRouteSelections.forEach((selected, key) => {
                  if (!selected) {
                      return;
                  }
                  const normalized = catRouteKey(key);
                  if (normalized) {
                      explicitCatSelectionSet.add(normalized);
                  }
              });
          }
          const explicitCatSelections = Array.from(explicitCatSelectionSet).sort();

          const activeCatKeySet = new Set();
          if (catActiveRouteKeys instanceof Set) {
              catActiveRouteKeys.forEach(key => {
                  const normalized = catRouteKey(key);
                  if (normalized) {
                      activeCatKeySet.add(normalized);
                  }
              });
          }
          const activeCatKeys = Array.from(activeCatKeySet).sort();

          parts.push(`cat:on:${explicitCatSelections.join(',')}`);
          parts.push(`catActive:${activeCatKeys.join(',')}`);
          parts.push(`out:${outOfServiceVisible ? '1' : '0'}`);

          return parts.join('|');
      }

      function renderBusStops(stopsArray) {
          const currentDisplaySignature = computeStopDisplaySignature();

          if (!map) {
              lastStopDisplaySignature = currentDisplaySignature;
              return;
          }

          if (scheduledStopRenderFrame !== null && typeof cancelAnimationFrame === 'function') {
              cancelAnimationFrame(scheduledStopRenderFrame);
              scheduledStopRenderFrame = null;
          }
          if (scheduledStopRenderTimeout !== null) {
              clearTimeout(scheduledStopRenderTimeout);
              scheduledStopRenderTimeout = null;
          }

          const baseStops = Array.isArray(stopsArray) ? stopsArray.slice() : [];
          const includeCatStops = catOverlayEnabled && Array.isArray(catStopDataCache) && catStopDataCache.length > 0;
          if (includeCatStops) {
              baseStops.push(...catStopDataCache);
          }

          if (baseStops.length === 0) {
              clearStopMarkerCache();
              lastStopDisplaySignature = currentDisplaySignature;
              return;
          }

          const bounds = typeof map?.getBounds === 'function' ? map.getBounds() : null;
          const paddedBounds = bounds && typeof bounds.pad === 'function'
              ? bounds.pad(STOP_RENDER_BOUNDS_PADDING)
              : bounds;
          const selectedRouteIdsSet = getSelectedRouteIdSet();
          const requiredRouteStopIds = new Set();
          const requiredFallbackStopIds = new Set();

          if (Array.isArray(customPopups) && customPopups.length > 0) {
              customPopups.forEach(popupElement => {
                  if (!popupElement || popupElement.dataset.popupType === 'incident') {
                      return;
                  }
                  let parsedRouteStopIds = [];
                  try {
                      parsedRouteStopIds = JSON.parse(popupElement.dataset.routeStopIds || '[]');
                  } catch (error) {
                      parsedRouteStopIds = [];
                  }
                  parsedRouteStopIds.forEach(id => {
                      const normalized = normalizeIdentifier(id);
                      if (normalized) {
                          requiredRouteStopIds.add(normalized);
                      }
                  });
                  const fallbackStopText = popupElement.dataset.fallbackStopId || '';
                  if (typeof fallbackStopText === 'string' && fallbackStopText) {
                      fallbackStopText.split(',').forEach(value => {
                          const normalized = normalizeIdentifier(value);
                          if (normalized) {
                              requiredFallbackStopIds.add(normalized);
                          }
                      });
                  }
              });
          }

          const stopsForVisibleRoutes = baseStops.filter(stop => {
              if (!stop) {
                  return false;
              }
              const latitude = Number.parseFloat(stop.Latitude ?? stop.latitude ?? stop.lat);
              const longitude = Number.parseFloat(stop.Longitude ?? stop.longitude ?? stop.lon);
              const routeStopId = normalizeIdentifier(stop.RouteStopID ?? stop.RouteStopId);
              const fallbackStopIdRaw = normalizeIdentifier(stop.StopID ?? stop.StopId);
              const { routeIds, catRouteKeys } = collectRouteIdsForStop(stop);
              let matchesTransloc = false;
              if (selectedRouteIdsSet.size > 0 && routeIds.size > 0) {
                  for (const routeId of routeIds) {
                      if (selectedRouteIdsSet.has(routeId)) {
                          matchesTransloc = true;
                          break;
                      }
                  }
              }
              let matchesCat = false;
              if (catOverlayEnabled && catRouteKeys.size > 0) {
                  for (const routeKey of catRouteKeys) {
                      if (isCatRouteVisible(routeKey)) {
                          matchesCat = true;
                          break;
                      }
                  }
              }
              if (!(matchesTransloc || matchesCat)) {
                  return false;
              }
              if (!paddedBounds || typeof paddedBounds.contains !== 'function') {
                  return true;
              }
              const inBounds = Number.isFinite(latitude) && Number.isFinite(longitude)
                  ? paddedBounds.contains([latitude, longitude])
                  : false;
              if (inBounds) {
                  return true;
              }
              if (routeStopId && requiredRouteStopIds.has(routeStopId)) {
                  return true;
              }
              if (fallbackStopIdRaw) {
                  const parts = fallbackStopIdRaw.split(',');
                  for (let i = 0; i < parts.length; i += 1) {
                      const normalized = normalizeIdentifier(parts[i]);
                      if (normalized && requiredFallbackStopIds.has(normalized)) {
                          return true;
                      }
                  }
              }
              return false;
          });

          if (stopsForVisibleRoutes.length === 0) {
              clearStopMarkerCache();
              return;
          }

          const groupedStops = groupStopsByPixelDistance(stopsForVisibleRoutes, STOP_GROUPING_PIXEL_DISTANCE);
          const groupedData = [];
          const activeMarkerKeys = new Set();

          groupedStops.forEach(group => {
              const stopEntries = buildStopEntriesFromStops(group.stops);
              if (stopEntries.length === 0) {
                  return;
              }

              const aggregatedRouteIds = new Set();
              const aggregatedCatRouteKeys = new Set();

              stopEntries.forEach(entry => {
                  const entryRoutes = collectRouteIdsForEntry(entry);
                  entryRoutes.routeIds.forEach(routeId => aggregatedRouteIds.add(routeId));
                  entryRoutes.catRouteKeys.forEach(routeKey => aggregatedCatRouteKeys.add(routeKey));
              });

              if (aggregatedRouteIds.size === 0 && aggregatedCatRouteKeys.size === 0) {
                  group.stops.forEach(stop => {
                      const stopRoutes = collectRouteIdsForStop(stop);
                      stopRoutes.routeIds.forEach(routeId => aggregatedRouteIds.add(routeId));
                      stopRoutes.catRouteKeys.forEach(routeKey => aggregatedCatRouteKeys.add(routeKey));
                  });
              }

              const servesTranslocSelection = selectedRouteIdsSet.size > 0
                  ? Array.from(aggregatedRouteIds).some(routeId => selectedRouteIdsSet.has(routeId))
                  : false;
              const servesCatSelection = catOverlayEnabled
                  ? Array.from(aggregatedCatRouteKeys).some(routeKey => isCatRouteVisible(routeKey))
                  : false;

              if (!servesTranslocSelection && !servesCatSelection) {
                  return;
              }

              const stopPosition = [group.latitude, group.longitude];
              const aggregatedRouteStopIds = Array.from(new Set(stopEntries.flatMap(entry => entry.routeStopIds)))
                  .map(id => `${id}`)
                  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
              const fallbackStopIdSet = new Set();
              stopEntries.forEach(entry => {
                  const entryStopIds = Array.isArray(entry?.stopIds) ? entry.stopIds : [];
                  entryStopIds.forEach(value => {
                      const normalized = normalizeIdentifier(value);
                      if (normalized) {
                          fallbackStopIdSet.add(normalized);
                      }
                  });
              });
              const fallbackStopIdText = Array.from(fallbackStopIdSet)
                  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
                  .join(', ');
              const displayStopName = Array.from(new Set(stopEntries
                  .map(entry => sanitizeStopName(entry.displayName))
                  .filter(Boolean)))
                  .join(' / ') || 'Stop';
              const groupKey = createStopGroupKey(aggregatedRouteStopIds, fallbackStopIdText);
              const markerRouteIds = Array.from(aggregatedRouteIds)
                  .filter(routeId => selectedRouteIdsSet.has(routeId))
                  .sort((a, b) => a - b);
              const markerCatRouteKeys = Array.from(aggregatedCatRouteKeys)
                  .filter(routeKey => isCatRouteVisible(routeKey))
                  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

              const groupInfo = {
                  position: stopPosition,
                  stopName: displayStopName,
                  fallbackStopId: fallbackStopIdText,
                  stopEntries,
                  aggregatedRouteStopIds,
                  groupKey,
                  catRouteKeys: Array.from(aggregatedCatRouteKeys).sort((a, b) => a.localeCompare(b, undefined, { numeric: true
}))
              };

              let shouldRenderMarker = true;
              if (paddedBounds && typeof paddedBounds.contains === 'function') {
                  if (Number.isFinite(stopPosition[0]) && Number.isFinite(stopPosition[1])) {
                      shouldRenderMarker = paddedBounds.contains(stopPosition);
                  } else {
                      shouldRenderMarker = false;
                  }
              }

              if (shouldRenderMarker) {
                  const markerIconSignature = `${markerRouteIds.join('|')}__${markerCatRouteKeys.join('|')}`;
                  const cachedEntry = stopMarkerCache.get(groupKey) || null;
                  let markerEntry = cachedEntry;

                  if (!markerEntry || !markerEntry.marker) {
                      const markerIcon = createStopMarkerIcon(markerRouteIds, markerCatRouteKeys);
                      const stopMarker = L.marker(stopPosition, {
                          icon: markerIcon,
                          pane: 'stopsPane'
                      }).addTo(map);

                      const handleClick = () => {
                          const latestEntry = stopMarkerCache.get(groupKey);
                          const latestInfo = latestEntry && latestEntry.groupInfo ? latestEntry.groupInfo : groupInfo;
                          createCustomPopup(Object.assign({ popupType: 'stop' }, latestInfo));
                      };

                      stopMarker.on('click', handleClick);

                      markerEntry = {
                          marker: stopMarker,
                          iconSignature: markerIconSignature,
                          groupInfo,
                          clickHandler: handleClick
                      };
                  } else {
                      const { marker } = markerEntry;
                      if (marker && typeof marker.setLatLng === 'function') {
                          marker.setLatLng(stopPosition);
                      }
                      if (markerEntry.iconSignature !== markerIconSignature) {
                          const markerIcon = createStopMarkerIcon(markerRouteIds, markerCatRouteKeys);
                          if (marker && typeof marker.setIcon === 'function') {
                              marker.setIcon(markerIcon);
                          }
                      }
                      markerEntry.iconSignature = markerIconSignature;
                      markerEntry.groupInfo = groupInfo;
                  }

                  stopMarkerCache.set(groupKey, markerEntry);
                  activeMarkerKeys.add(groupKey);
              }

              groupedData.push(groupInfo);
          });

          const removalKeys = [];
          stopMarkerCache.forEach((entry, key) => {
              if (!activeMarkerKeys.has(key)) {
                  removalKeys.push(key);
              }
          });

          removalKeys.forEach(key => {
              const entry = stopMarkerCache.get(key);
              if (entry && entry.marker) {
                  try {
                      if (typeof entry.marker.off === 'function' && entry.clickHandler) {
                          entry.marker.off('click', entry.clickHandler);
                      }
                      if (map && typeof map.removeLayer === 'function') {
                          map.removeLayer(entry.marker);
                      }
                  } catch (error) {
                      console.warn('Failed to clean up obsolete stop marker:', error);
                  }
              }
              stopMarkerCache.delete(key);
          });

          stopMarkers = Array.from(stopMarkerCache.values())
              .map(entry => entry && entry.marker)
              .filter(marker => !!marker);

          stopMarkers.forEach(marker => {
              if (!marker) return;
              if (typeof marker.bringToFront === 'function') {
                  marker.bringToFront();
                  return;
              }
              if (typeof marker.setZIndexOffset === 'function') {
                  marker.setZIndexOffset(1000);
              }
          });

          if (customPopups.length > 0) {
              const groupByKey = new Map();
              groupedData.forEach(groupInfo => {
                  groupByKey.set(groupInfo.groupKey, groupInfo);
              });

              customPopups = customPopups.filter(popupElement => {
                  if (!popupElement) {
                      return false;
                  }
                  if (popupElement.dataset.popupType === 'incident') {
                      return true;
                  }
                  let parsedRouteStopIds = [];
                  try {
                      parsedRouteStopIds = JSON.parse(popupElement.dataset.routeStopIds || '[]');
                  } catch (error) {
                      parsedRouteStopIds = [];
                  }
                  const fallbackId = popupElement.dataset.fallbackStopId || '';
                  const key = popupElement.dataset.groupKey || createStopGroupKey(parsedRouteStopIds, fallbackId);
                  const matchingGroup = groupByKey.get(key);
                  if (matchingGroup) {
                      popupElement.dataset.position = `${matchingGroup.position[0]},${matchingGroup.position[1]}`;
                      setStopPopupContent(popupElement, matchingGroup);
                      updatePopupPosition(popupElement, matchingGroup.position);
                      return true;
                  }
                  popupElement.remove();
                  return false;
              });
          }

          lastStopDisplaySignature = currentDisplaySignature;
      }

      function renderStopsIfDisplayChanged() {
          const nextSignature = computeStopDisplaySignature();
          if (lastStopDisplaySignature === nextSignature) {
              return;
          }
          renderBusStops(stopDataCache);
      }

      function scheduleStopRendering() {
          if (scheduledStopRenderFrame !== null || scheduledStopRenderTimeout !== null) {
              return;
          }

          const run = () => {
              scheduledStopRenderFrame = null;
              scheduledStopRenderTimeout = null;
              const hasTranslocStops = Array.isArray(stopDataCache) && stopDataCache.length > 0;
              const hasCatStops = catOverlayEnabled && Array.isArray(catStopDataCache) && catStopDataCache.length > 0;
              if (hasTranslocStops || hasCatStops) {
                  renderBusStops(stopDataCache);
              }
          };

          if (!lowPerformanceMode && typeof requestAnimationFrame === 'function') {
              scheduledStopRenderFrame = requestAnimationFrame(run);
              return;
          }

          const delay = lowPerformanceMode ? 75 : 16;
          scheduledStopRenderTimeout = setTimeout(run, delay);
      }

      function createCustomPopup(config) {
          if (!config || !Array.isArray(config.position) || config.position.length !== 2) {
              return;
          }
          const popupType = typeof config.popupType === 'string' ? config.popupType : 'stop';
          const position = config.position;
          customPopups.forEach(popup => popup.remove());
          customPopups = [];
          const popupElement = document.createElement('div');
          popupElement.className = 'custom-popup';
          document.body.appendChild(popupElement);
          popupElement.dataset.position = `${position[0]},${position[1]}`;
          popupElement.dataset.popupType = popupType;
          if (popupType === 'incident') {
              setIncidentPopupContent(popupElement, config);
          } else {
              setStopPopupContent(popupElement, config);
          }
          updatePopupPosition(popupElement, position);
          customPopups.push(popupElement);
          if (typeof requestAnimationFrame === 'function') {
              requestAnimationFrame(() => centerPopupOnMap(popupElement));
          } else {
              centerPopupOnMap(popupElement);
          }
      }

      function updatePopupPosition(popupElement, position) {
          if (!map || typeof map?.latLngToContainerPoint !== 'function') {
              return;
          }
          const mapPos = map.latLngToContainerPoint(position);
          popupElement.style.left = `${mapPos.x}px`;
          popupElement.style.top = `${mapPos.y}px`;
      }

      function centerPopupOnMap(popupElement) {
          if (!popupElement || !map || typeof map?.panBy !== 'function') {
              return;
          }
          const mapContainer = typeof map.getContainer === 'function' ? map.getContainer() : null;
          if (!mapContainer) {
              return;
          }
          const mapRect = mapContainer.getBoundingClientRect();
          const popupRect = popupElement.getBoundingClientRect();
          if (mapRect.width === 0 || mapRect.height === 0 || popupRect.width === 0 || popupRect.height === 0) {
              return;
          }
          const mapCenterX = mapRect.width / 2;
          const mapCenterY = mapRect.height / 2;
          const popupCenterX = (popupRect.left - mapRect.left) + (popupRect.width / 2);
          const popupCenterY = (popupRect.top - mapRect.top) + (popupRect.height / 2);
          const deltaX = popupCenterX - mapCenterX;
          const deltaY = popupCenterY - mapCenterY;
          if (Math.abs(deltaX) < 1 && Math.abs(deltaY) < 1) {
              return;
          }
          map.panBy([deltaX, deltaY], { animate: true, duration: 0.35, easeLinearity: 0.25 });
      }

      function updatePopupPositions() {
          if (!map || typeof map?.latLngToContainerPoint !== 'function') {
              return;
          }
          const zooming = !!(map?._animatingZoom || (map?._zoomAnimated && map?._zooming));
          if (zooming) {
              return;
          }
          customPopups.forEach(popupElement => {
              const position = popupElement.dataset.position;
              if (position) {
                  const [latitude, longitude] = position.split(',').map(Number);
                  updatePopupPosition(popupElement, [latitude, longitude]);
              }
          });
      }

      function updateCustomPopups() {
          customPopups.forEach(popupElement => {
              if (!popupElement) {
                  return;
              }
              if (popupElement.dataset.popupType === 'incident') {
                  const popupId = popupElement.dataset.incidentId || '';
                  if (popupId) {
                      refreshIncidentPopup(popupId);
                  }
                  return;
              }
              const position = popupElement.dataset.position;
              if (position) {
                  let routeStopIds = [];
                  let stopEntries = [];
                  try {
                      routeStopIds = JSON.parse(popupElement.dataset.routeStopIds || '[]');
                  } catch (error) {
                      routeStopIds = [];
                  }
                  try {
                      stopEntries = JSON.parse(popupElement.dataset.stopEntries || '[]');
                  } catch (error) {
                      stopEntries = [];
                  }
                  const fallbackStopId = popupElement.dataset.fallbackStopId || '';
                  const stopName = popupElement.dataset.stopName || '';
                  const groupKey = popupElement.dataset.groupKey || createStopGroupKey(routeStopIds, fallbackStopId);
                  const groupInfo = {
                      position: position.split(',').map(Number),
                      stopName,
                      fallbackStopId,
                      stopEntries,
                      aggregatedRouteStopIds: routeStopIds,
                      groupKey
                  };
                  setStopPopupContent(popupElement, groupInfo);
              }
          });
      }

      async function fetchStopArrivalTimes() {
          const currentBaseURL = baseURL;
          try {
              const snapshot = await loadTranslocSnapshot();
              if (currentBaseURL !== baseURL) return {};
              const arrivals = Array.isArray(snapshot?.arrivals) ? snapshot.arrivals : [];
              const allEtas = {};
              arrivals.forEach(arrival => {
                  const routeStopId = arrival?.RouteStopId ?? arrival?.RouteStopID;
                  if (!routeStopId) return;
                  if (!allEtas[routeStopId]) {
                      allEtas[routeStopId] = [];
                  }
                  const times = Array.isArray(arrival?.Times) ? arrival.Times : [];
                  times.forEach(time => {
                      const seconds = Number(time?.Seconds);
                      if (!Number.isFinite(seconds)) return;
                      const etaMinutes = Math.round(seconds / 60);
                      allEtas[routeStopId].push({
                          routeDescription: arrival.RouteDescription === 'Night Pilot' ? arrival.RouteDescription : arrival.RouteDescription,
                          etaMinutes,
                          RouteId: arrival.RouteId ?? arrival.RouteID
                      });
                  });
              });
              return allEtas;
          } catch (error) {
              console.error('Error fetching stop arrival times:', error);
              return {};
          }
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

        simplifyLatLngs(latlngs, zoom) {
          if (!Array.isArray(latlngs) || latlngs.length === 0) {
            return [];
          }

          const projected = latlngs.map(latlng => this.map.project(latlng, zoom));
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
      // Fetch routes from cached snapshot.
      async function fetchRouteColors() {
        try {
          const snapshot = await loadTranslocSnapshot();
          const routes = Array.isArray(snapshot?.routes) ? snapshot.routes : [];
          routes.forEach(route => {
            if (!route || typeof route !== 'object') return;
            setRouteVisibility(route);
            const routeId = route.RouteID;
            allRoutes[routeId] = Object.assign(allRoutes[routeId] || {}, route);
            if (canDisplayRoute(routeId)) {
              routeColors[routeId] = route.MapLineColor;
            } else if (Object.prototype.hasOwnProperty.call(routeColors, routeId)) {
              delete routeColors[routeId];
            }
          });
        } catch (error) {
          console.error('Error fetching route colors:', error);
        }
      }

      // Fetch route paths from GetRoutesForMapWithSchedule and center map on relevant routes.
      async function runFetchRoutePaths() {
          const currentBaseURL = baseURL;
          try {
              const snapshot = await loadTranslocSnapshot();
              if (currentBaseURL !== baseURL) return;
              const data = Array.isArray(snapshot?.routes) ? snapshot.routes : [];
                  const activeRoutesForBounds = new Set();
                  activeRoutes.forEach(routeId => {
                      const numericRouteId = Number(routeId);
                      if (Number.isFinite(numericRouteId) && numericRouteId !== 0) {
                          activeRoutesForBounds.add(numericRouteId);
                      }
                  });
                  const hasActiveServiceRoutes = activeRoutesForBounds.size > 0;
                  let bounds = null;
                  let fallbackBounds = null;
                  const displayedRoutes = new Map();
                  const rendererGeometries = new Map();
                  const simpleGeometries = [];
                  const selectedRouteIds = [];
                  const updatedRouteStopAddressMap = {};
                  const updatedRouteStopRouteMap = {};
                  const useOverlapRenderer = enableOverlapDashRendering && overlapRenderer;
                  const seenRouteIds = new Set();
                  let geometryChanged = false;

                  if (Array.isArray(data)) {
                      data.forEach(route => {
                          setRouteVisibility(route);
                          allRoutes[route.RouteID] = Object.assign(allRoutes[route.RouteID] || {}, route);
                  const numericRouteId = Number(route.RouteID);
                  const isNumericRoute = !Number.isNaN(numericRouteId);

                  if (Array.isArray(route.Stops)) {
                      route.Stops.forEach(stop => {
                          const routeStopId = Number(stop.RouteStopID ?? stop.RouteStopId);
                          const addressId = stop.AddressID ?? stop.AddressId;
                          if (!Number.isNaN(routeStopId)) {
                              if (isNumericRoute) {
                                  updatedRouteStopRouteMap[routeStopId] = numericRouteId;
                              }
                              if (addressId !== undefined && addressId !== null && `${addressId}`.trim() !== '') {
                                  updatedRouteStopAddressMap[routeStopId] = `${addressId}`;
                              }
                          }
                      });
                  }

                  const routeAllowed = canDisplayRoute(route.RouteID);
                          if (isNumericRoute && route.EncodedPolyline) {
                              seenRouteIds.add(numericRouteId);
                          }

                          if (!routeAllowed) {
                              return;
                          }

                          const isSelected = isRouteSelected(route.RouteID);
                          if (route.EncodedPolyline && isNumericRoute) {
                              const shouldIncludeInBounds = !hasActiveServiceRoutes || activeRoutesForBounds.has(numericRouteId);
                              let cacheEntry = routePolylineCache.get(numericRouteId);
                              let latLngPath;
                              let polyBounds = null;

                              if (!cacheEntry || cacheEntry.encoded !== route.EncodedPolyline) {
                                  const decodedPolyline = polyline.decode(route.EncodedPolyline);
                                  latLngPath = decodedPolyline.map(coords => L.latLng(coords[0], coords[1]));
                                  if (Array.isArray(latLngPath) && latLngPath.length >= 2) {
                                      polyBounds = L.latLngBounds(latLngPath);
                                  }
                                  const entry = {
                                      encoded: route.EncodedPolyline,
                                      latLngPath,
                                      bounds: polyBounds
                                  };
                                  routePolylineCache.set(numericRouteId, entry);
                                  ensureRouteProjectedPath(entry);
                                  cacheEntry = routePolylineCache.get(numericRouteId);
                                  if (isSelected) {
                                      geometryChanged = true;
                                  }
                              } else {
                                  latLngPath = cacheEntry.latLngPath;
                                  polyBounds = cacheEntry.bounds || null;
                                  if (!polyBounds && Array.isArray(latLngPath) && latLngPath.length >= 2) {
                                      polyBounds = L.latLngBounds(latLngPath);
                                      cacheEntry.bounds = polyBounds;
                                  }
                                  ensureRouteProjectedPath(cacheEntry);
                              }

                              let candidateBounds = polyBounds;
                              if (!candidateBounds && Array.isArray(latLngPath) && latLngPath.length >= 2) {
                                  candidateBounds = L.latLngBounds(latLngPath);
                                  if (cacheEntry) {
                                      cacheEntry.bounds = candidateBounds;
                                  } else {
                                      const existing = routePolylineCache.get(numericRouteId);
                                      if (existing) {
                                          existing.bounds = candidateBounds;
                                      }
                                  }
                              }

                              if (candidateBounds) {
                                  fallbackBounds = fallbackBounds
                                      ? fallbackBounds.extend(candidateBounds)
                                      : L.latLngBounds(candidateBounds);
                                  if (shouldIncludeInBounds) {
                                      bounds = bounds
                                          ? bounds.extend(candidateBounds)
                                          : L.latLngBounds(candidateBounds);
                                  }
                              }

                              if (isSelected && Array.isArray(latLngPath) && latLngPath.length >= 2) {
                                  const routeColor = getRouteColor(route.RouteID);
                                  selectedRouteIds.push(numericRouteId);
                                  if (useOverlapRenderer) {
                                      rendererGeometries.set(numericRouteId, latLngPath);
                                  } else {
                                      simpleGeometries.push({ routeId: numericRouteId, latLngPath, routeColor });
                                  }

                                  const storedRoute = allRoutes[route.RouteID] || {};
                                  const legendNameCandidates = [
                                      storedRoute.Description,
                                      route.Description,
                                      storedRoute.Name,
                                      route.Name,
                                      storedRoute.RouteName,
                                      route.RouteName
                                  ];
                                  let legendName = legendNameCandidates.find(value => typeof value === 'string' && value.trim() !== '');
                                  legendName = legendName ? legendName.trim() : `Route ${route.RouteID}`;
                                  const rawDescription = storedRoute.InfoText ?? route.InfoText ?? '';
                                  const legendDescription = typeof rawDescription === 'string' ? rawDescription.trim() : '';
                                  const legendRouteId = isNumericRoute ? numericRouteId : route.RouteID;
                                  displayedRoutes.set(route.RouteID, {
                                      routeId: legendRouteId,
                                      color: routeColor,
                                      name: legendName,
                                      description: legendDescription
                                  });
                              }
                          } else if (isSelected && isNumericRoute) {
                              if (routePolylineCache.has(numericRouteId)) {
                                  routePolylineCache.delete(numericRouteId);
                              }
                              geometryChanged = true;
                          }
                      });

                      const previousSelectedIds = new Set(lastRouteRenderState.selectionKey
                          ? lastRouteRenderState.selectionKey.split('|').filter(Boolean).map(id => Number(id))
                          : []);
                      Array.from(routePolylineCache.keys()).forEach(routeId => {
                          if (!seenRouteIds.has(routeId)) {
                              if (previousSelectedIds.has(routeId)) {
                                  geometryChanged = true;
                              }
                              routePolylineCache.delete(routeId);
                          }
                      });

                      const selectedRouteIdsSorted = selectedRouteIds.slice().sort((a, b) => a - b);
                      currentTranslocSelectedRouteIds = selectedRouteIdsSorted.slice();
                      currentTranslocRendererGeometries = new Map(rendererGeometries);
                      const selectionKey = selectedRouteIdsSorted.join('|');
                      const colorSignature = selectedRouteIdsSorted.map(id => `${id}:${getRouteColor(id)}`).join('|');
                      const geometrySignature = selectedRouteIdsSorted
                          .map(id => `${id}:${getRouteGeometrySignature(id, rendererGeometries)}`)
                          .join('|');
                      const rendererFlag = !!useOverlapRenderer;

                      let shouldRender = routeLayers.length === 0 ||
                        rendererFlag !== lastRouteRenderState.useOverlapRenderer ||
                        selectionKey !== lastRouteRenderState.selectionKey ||
                        colorSignature !== lastRouteRenderState.colorSignature ||
                        geometrySignature !== lastRouteRenderState.geometrySignature ||
                        geometryChanged;

                      if (useOverlapRenderer) {
                          updateOverlapRendererWithCatRoutes();
                      } else if (shouldRender) {
                          routeLayers.forEach(layer => map.removeLayer(layer));
                          routeLayers = [];
                          const currentStrokeWeight = computeRouteStrokeWeight(typeof map?.getZoom === 'function' ? map.getZoom() : null);
                          simpleGeometries.forEach(({ routeId, latLngPath, routeColor }) => {
                              const routeLayer = L.polyline(latLngPath, mergeRouteLayerOptions({
                                      color: routeColor,
                                      weight: currentStrokeWeight,
                                      opacity: 1,
                                      lineCap: 'round',
                                      lineJoin: 'round'
                                  })).addTo(map);
                                  routeLayers.push(routeLayer);
                          });
                      }

                      if (!rendererFlag) {
                          lastRouteRenderState = {
                              selectionKey,
                              colorSignature,
                              geometrySignature,
                              useOverlapRenderer: rendererFlag
                          };
                      }

                      routeStopAddressMap = updatedRouteStopAddressMap;
                      routeStopRouteMap = updatedRouteStopRouteMap;
                      updateCustomPopups();
                      const hasTranslocStops = Array.isArray(stopDataCache) && stopDataCache.length > 0;
                      const hasCatStops = catOverlayEnabled && Array.isArray(catStopDataCache) && catStopDataCache.length > 0;
                      if (hasTranslocStops || hasCatStops) {
                          renderBusStops(stopDataCache);
                      }
                      if (!bounds && fallbackBounds) {
                          bounds = fallbackBounds;
                      }
                      if (bounds) {
                          allRouteBounds = bounds;
                          if (!mapHasFitAllRoutes) {
                              if (!kioskMode && !adminKioskMode && !isDispatcherLockActive()) {
                                  map.fitBounds(allRouteBounds, { padding: [20, 20] });
                              }
                              mapHasFitAllRoutes = true;
                          }
                      }
                      evaluateIncidentRouteAlerts();
                      if (!isKioskExperienceActive()) {
                          updateRouteSelector(activeRoutes);
                      }
                      stopMarkers.forEach(stopMarker => {
                          if (!stopMarker) {
                              return;
                          }
                          if (typeof stopMarker.bringToFront === 'function') {
                              stopMarker.bringToFront();
                              return;
                          }
                          if (typeof stopMarker.setZIndexOffset === 'function') {
                              stopMarker.setZIndexOffset(1000);
                          }
                      });
                  }
                  if (isKioskExperienceActive()) {
                      const legendRoutes = Array.from(displayedRoutes.values());
                      updateRouteLegend(legendRoutes, { preserveOnEmpty: true });
                  } else {
                      updateRouteLegend([], { forceHide: true });
                  }
          } catch (error) {
              console.error('Error fetching route paths:', error);
              if (isKioskExperienceActive()) {
                  updateRouteLegend(lastRenderedLegendRoutes, { preserveOnEmpty: true });
              } else {
                  updateRouteLegend([], { forceHide: true });
              }
          }
      }

      async function fetchRoutePaths() {
          if (routePathsFetchPromise && routePathsFetchBaseURL === baseURL) {
              return routePathsFetchPromise;
          }
          const currentBaseURL = baseURL;
          const promise = runFetchRoutePaths();
          routePathsFetchBaseURL = currentBaseURL;
          routePathsFetchPromise = promise.finally(() => {
              if (routePathsFetchBaseURL === currentBaseURL) {
                  routePathsFetchPromise = null;
                  routePathsFetchBaseURL = null;
              }
          });
          return routePathsFetchPromise;
      }

      async function fetchBlockAssignments() {
          const currentBaseURL = baseURL;
          try {
              const snapshot = await loadTranslocSnapshot();
              if (currentBaseURL !== baseURL) return;
              const mapping = snapshot?.blocks && typeof snapshot.blocks === 'object'
                  ? snapshot.blocks
                  : {};
              busBlocks = Object.assign({}, mapping);
          } catch (error) {
              console.error('Error fetching block assignments:', error);
          }
      }

      async function runFetchBusLocations() {
          try {
              const headingPromise = loadVehicleHeadingCache();
              if (headingPromise && typeof headingPromise.then === 'function') {
                  await headingPromise;
              }
          } catch (error) {
              console.info('Proceeding without cached vehicle headings.', error);
          }
          const currentBaseURL = baseURL;
          try {
              await loadBusSVG();
          } catch (error) {
              console.error('Failed to load bus marker SVG before fetching locations:', error);
          }
          try {
              const snapshot = await loadTranslocSnapshot();
              if (currentBaseURL !== baseURL) {
                  return;
              }
              let dispatcherConfigLocal = null;
              if (dispatcherFeaturesAllowed()) {
                  try {
                      dispatcherConfigLocal = await ensureDispatcherConfigLoaded();
                  } catch (configError) {
                      console.error('Failed to resolve dispatcher configuration:', configError);
                  }
              }
              const data = Array.isArray(snapshot?.vehicles) ? snapshot.vehicles : [];
              const currentBusData = {};
              const activeRoutesSet = new Set();
              const vehicles = [];
              let dispatcherCandidate = null;

              data.forEach(vehicle => {
                  if (!vehicle) return;
                  const vehicleID = vehicle.VehicleID;
                  const lat = Number(vehicle.Latitude);
                  const lon = Number(vehicle.Longitude);
                  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
                  const newPosition = [lat, lon];
                  const groundSpeed = Number(vehicle.GroundSpeed) || 0;
                  const isMoving = groundSpeed > 0;
                  const busName = vehicle.Name;
                  let routeID = vehicle.RouteID;
                  if (!routeID && adminMode) {
                      routeID = 0;
                  } else if (!routeID) {
                      return;
                  }
                  const numericRouteId = Number(routeID);
                  const effectiveRouteId = Number.isNaN(numericRouteId) ? routeID : numericRouteId;
                  if (!canDisplayRoute(effectiveRouteId)) return;
                  if (!adminMode && !routeColors.hasOwnProperty(effectiveRouteId)) return;
                  activeRoutesSet.add(effectiveRouteId);
                  if (dispatcherFeaturesAllowed() && dispatcherConfigLocal) {
                      const vehicleKey = vehicleID === undefined || vehicleID === null ? '' : `${vehicleID}`.trim();
                      dispatcherCandidate = selectNearestOverheightCandidate(
                          dispatcherCandidate,
                          vehicleKey,
                          busName,
                          lat,
                          lon,
                          dispatcherConfigLocal
                      );
                  }
                  vehicles.push({
                      vehicleID,
                      newPosition,
                      isMoving,
                      busName,
                      routeID: effectiveRouteId,
                      heading: vehicle.Heading,
                      groundSpeed
                  });
              });

              activeRoutes = activeRoutesSet;
              updateRouteSelector(activeRoutesSet);
              updateKioskStatusMessage({ known: true, hasActiveVehicles: activeRoutesSet.size > 0 });

              const markerMetricsForZoom = computeBusMarkerMetrics(map && typeof map?.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM);

              for (const v of vehicles) {
                  const { vehicleID, newPosition, busName, routeID, heading, groundSpeed } = v;
                  if (!isRouteSelected(routeID)) continue;
                  currentBusData[vehicleID] = true;
                  const state = ensureBusMarkerState(vehicleID);
                  const routeColor = getRouteColor(routeID) || outOfServiceRouteColor;
                  const glyphColor = computeBusMarkerGlyphColor(routeColor);
                  const fallbackHeading = getVehicleHeadingFallback(vehicleID, heading);
                  const headingDeg = updateBusMarkerHeading(state, newPosition, fallbackHeading, groundSpeed);
                  const accessibleLabel = buildBusMarkerAccessibleLabel(busName, headingDeg, groundSpeed);
                  const gpsIsStale = isVehicleGpsStale(v);
                  const isStopped = isBusConsideredStopped(groundSpeed);

                  state.busName = busName;
                  state.routeID = routeID;
                  state.fillColor = routeColor;
                  state.glyphColor = glyphColor;
                  state.headingDeg = headingDeg;
                  state.accessibleLabel = accessibleLabel;
                  state.isStale = gpsIsStale;
                  state.isStopped = isStopped;
                  state.groundSpeed = groundSpeed;
                  state.lastUpdateTimestamp = Date.now();
                  rememberCachedVehicleHeading(vehicleID, headingDeg, state.lastUpdateTimestamp);

                  if (!state.size) {
                      setBusMarkerSize(state, markerMetricsForZoom);
                  }

                  if (markers[vehicleID]) {
                      animateMarkerTo(markers[vehicleID], newPosition);
                      markers[vehicleID].routeID = routeID;
                      queueBusMarkerVisualUpdate(vehicleID, {
                          fillColor: routeColor,
                          glyphColor,
                          headingDeg,
                          stale: gpsIsStale,
                          accessibleLabel,
                          stopped: isStopped
                      });
                  } else {
                      try {
                          const icon = await createBusMarkerDivIcon(vehicleID, state);
                          if (!icon) {
                              continue;
                          }
                          const marker = L.marker(newPosition, { icon, pane: 'busesPane', interactive: false, keyboard: false });
                          marker.routeID = routeID;
                          marker.addTo(map);
                          markers[vehicleID] = marker;
                          state.marker = marker;
                          removeDuplicateBusMarkerLayers(vehicleID, marker);
                          registerBusMarkerElements(vehicleID);
                          attachBusMarkerInteractions(vehicleID);
                          updateBusMarkerRootClasses(state);
                          updateBusMarkerZIndex(state);
                          applyBusMarkerOutlineWidth(state);
                      } catch (error) {
                          console.error(`Failed to create bus marker icon for vehicle ${vehicleID}:`, error);
                      }
                  }

                  if (adminMode && displayMode === DISPLAY_MODES.SPEED && !kioskMode) {
                      const speedIcon = createSpeedBubbleDivIcon(routeColor, groundSpeed, markerMetricsForZoom.scale, headingDeg);
                      if (speedIcon) {
                          nameBubbles[vehicleID] = nameBubbles[vehicleID] || {};
                          if (nameBubbles[vehicleID].speedMarker) {
                              animateMarkerTo(nameBubbles[vehicleID].speedMarker, newPosition);
                              nameBubbles[vehicleID].speedMarker.setIcon(speedIcon);
                          } else {
                              nameBubbles[vehicleID].speedMarker = L.marker(newPosition, { icon: speedIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                          }
                      } else if (nameBubbles[vehicleID] && nameBubbles[vehicleID].speedMarker) {
                          map.removeLayer(nameBubbles[vehicleID].speedMarker);
                          delete nameBubbles[vehicleID].speedMarker;
                      }
                  } else if (nameBubbles[vehicleID] && nameBubbles[vehicleID].speedMarker) {
                      map.removeLayer(nameBubbles[vehicleID].speedMarker);
                      delete nameBubbles[vehicleID].speedMarker;
                  }

                  if (adminMode && !kioskMode) {
                      const nameIcon = createNameBubbleDivIcon(busName, routeColor, markerMetricsForZoom.scale, headingDeg);
                      if (nameIcon) {
                          nameBubbles[vehicleID] = nameBubbles[vehicleID] || {};
                          if (nameBubbles[vehicleID].nameMarker) {
                              animateMarkerTo(nameBubbles[vehicleID].nameMarker, newPosition);
                              nameBubbles[vehicleID].nameMarker.setIcon(nameIcon);
                          } else {
                              nameBubbles[vehicleID].nameMarker = L.marker(newPosition, { icon: nameIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                          }
                      } else if (nameBubbles[vehicleID] && nameBubbles[vehicleID].nameMarker) {
                          map.removeLayer(nameBubbles[vehicleID].nameMarker);
                          delete nameBubbles[vehicleID].nameMarker;
                      }

                      const blockName = busBlocks[vehicleID];
                      if (displayMode === DISPLAY_MODES.BLOCK && blockName && blockName.includes('[')) {
                          const blockIcon = createBlockBubbleDivIcon(blockName, routeColor, markerMetricsForZoom.scale, headingDeg);
                          if (blockIcon) {
                              nameBubbles[vehicleID] = nameBubbles[vehicleID] || {};
                              if (nameBubbles[vehicleID].blockMarker) {
                                  animateMarkerTo(nameBubbles[vehicleID].blockMarker, newPosition);
                                  nameBubbles[vehicleID].blockMarker.setIcon(blockIcon);
                              } else {
                                  nameBubbles[vehicleID].blockMarker = L.marker(newPosition, { icon: blockIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                              }
                          } else if (nameBubbles[vehicleID] && nameBubbles[vehicleID].blockMarker) {
                              map.removeLayer(nameBubbles[vehicleID].blockMarker);
                              delete nameBubbles[vehicleID].blockMarker;
                          }
                      } else if (nameBubbles[vehicleID] && nameBubbles[vehicleID].blockMarker) {
                          map.removeLayer(nameBubbles[vehicleID].blockMarker);
                          delete nameBubbles[vehicleID].blockMarker;
                      }
                  } else {
                      if (nameBubbles[vehicleID] && nameBubbles[vehicleID].nameMarker) {
                          map.removeLayer(nameBubbles[vehicleID].nameMarker);
                          delete nameBubbles[vehicleID].nameMarker;
                      }
                      if (nameBubbles[vehicleID] && nameBubbles[vehicleID].blockMarker) {
                          map.removeLayer(nameBubbles[vehicleID].blockMarker);
                          delete nameBubbles[vehicleID].blockMarker;
                      }
                      if (nameBubbles[vehicleID] && nameBubbles[vehicleID].routeMarker) {
                          map.removeLayer(nameBubbles[vehicleID].routeMarker);
                          delete nameBubbles[vehicleID].routeMarker;
                      }
                  }

                  if (nameBubbles[vehicleID]) {
                      const hasMarkers = Boolean(nameBubbles[vehicleID].speedMarker || nameBubbles[vehicleID].nameMarker || nameBubbles[vehicleID].blockMarker || nameBubbles[vehicleID].routeMarker);
                      if (hasMarkers) {
                          nameBubbles[vehicleID].lastScale = markerMetricsForZoom.scale;
                      } else {
                          delete nameBubbles[vehicleID];
                      }
                  }
              }

              if (dispatcherFeaturesAllowed()) {
                  if (dispatcherConfigLocal) {
                      handleDispatcherLock(dispatcherCandidate, dispatcherConfigLocal);
                      updateDispatcherPendingPopup();
                  } else {
                      handleDispatcherLock(null, null);
                  }
              } else {
                  handleDispatcherLock(null, null);
              }

              Object.keys(markers).forEach(vehicleID => {
                  if (isOnDemandVehicleId(vehicleID)) {
                      return;
                  }
                  if (!currentBusData[vehicleID] || !isRouteSelected(markers[vehicleID].routeID)) {
                      map.removeLayer(markers[vehicleID]);
                      delete markers[vehicleID];
                      clearBusMarkerState(vehicleID);
                      if (nameBubbles[vehicleID]) {
                          if (nameBubbles[vehicleID].speedMarker) map.removeLayer(nameBubbles[vehicleID].speedMarker);
                          if (nameBubbles[vehicleID].nameMarker) map.removeLayer(nameBubbles[vehicleID].nameMarker);
                          if (nameBubbles[vehicleID].blockMarker) map.removeLayer(nameBubbles[vehicleID].blockMarker);
                          if (nameBubbles[vehicleID].routeMarker) map.removeLayer(nameBubbles[vehicleID].routeMarker);
                          delete nameBubbles[vehicleID];
                      }
                  }
              });
              purgeOrphanedBusMarkers();
              if (vehicleFollowState.active) {
                  const followedVehicleId = vehicleFollowState.vehicleId;
                  if (followedVehicleId) {
                      const markerExists = Boolean(markers && markers[followedVehicleId]);
                      const vehicleVisible = Boolean(currentBusData[followedVehicleId]);
                      if (vehicleVisible) {
                          vehicleFollowState.visibilityGraceDeadline = 0;
                      }
                      const waitingForInitialCenter = vehicleFollowState.pendingInitialCenter;
                      const waitingForVisibility = isVehicleFollowVisibilityGraceActive();
                      if (!waitingForInitialCenter && !waitingForVisibility && (!markerExists || !vehicleVisible)) {
                          stopFollowingVehicle();
                      }
                  }
              }
              maintainVehicleFollowAfterUpdate();
              previousBusData = currentBusData;
          } catch (error) {
              console.error("Error fetching bus locations:", error);
          }
      }

      async function fetchBusLocations() {
          if (busLocationsFetchPromise && busLocationsFetchBaseURL === baseURL) {
              return busLocationsFetchPromise;
          }
          const currentBaseURL = baseURL;
          const promise = runFetchBusLocations();
          busLocationsFetchBaseURL = currentBaseURL;
          busLocationsFetchPromise = promise.finally(() => {
              if (busLocationsFetchBaseURL === currentBaseURL) {
                  busLocationsFetchPromise = null;
                  busLocationsFetchBaseURL = null;
              }
          });
          return busLocationsFetchPromise;
      }

      function clamp(value, min, max) {
          return Math.min(Math.max(value, min), max);
      }

      function computeMarkerScale(zoom) {
          return 1;
      }

      function computeBusMarkerMetrics(zoom) {
          const scale = computeMarkerScale(zoom);
          const width = BUS_MARKER_BASE_WIDTH_PX * scale;
          const height = width * BUS_MARKER_ASPECT_RATIO;
          return { scale, widthPx: width, heightPx: height };
      }

      function rememberCachedVehicleHeading(vehicleID, headingDeg, timestamp) {
          if (!Number.isFinite(headingDeg)) {
              return;
          }
          if (vehicleID === undefined || vehicleID === null) {
              return;
          }
          const key = `${vehicleID}`;
          if (!key || key === 'undefined' || key === 'null') {
              return;
          }
          const normalizedHeading = normalizeHeadingDegrees(headingDeg);
          const updatedAt = Number.isFinite(timestamp) ? Number(timestamp) : Date.now();
          vehicleHeadingCache.set(key, { heading: normalizedHeading, updatedAt });
      }

      function getCachedVehicleHeading(vehicleID) {
          if (vehicleID === undefined || vehicleID === null) {
              return null;
          }
          const key = `${vehicleID}`;
          const entry = vehicleHeadingCache.get(key);
          if (!entry) {
              return null;
          }
          const heading = Number(entry.heading ?? entry.Heading);
          if (!Number.isFinite(heading)) {
              return null;
          }
          return normalizeHeadingDegrees(heading);
      }

      function getVehicleHeadingFallback(vehicleID, headingFromFeed) {
          const cached = getCachedVehicleHeading(vehicleID);
          if (cached !== null) {
              return cached;
          }
          const fromFeed = Number(headingFromFeed);
          if (Number.isFinite(fromFeed)) {
              return normalizeHeadingDegrees(fromFeed);
          }
          return BUS_MARKER_DEFAULT_HEADING;
      }

      function loadVehicleHeadingCache() {
          if (vehicleHeadingCachePromise) {
              return vehicleHeadingCachePromise;
          }
          if (typeof fetch !== 'function') {
              vehicleHeadingCachePromise = Promise.resolve(vehicleHeadingCache);
              return vehicleHeadingCachePromise;
          }
          vehicleHeadingCachePromise = (async () => {
              try {
                  const response = await fetch(VEHICLE_HEADING_CACHE_ENDPOINT, { cache: 'no-store' });
                  if (!response.ok) {
                      throw new Error(`HTTP ${response.status}`);
                  }
                  const data = await response.json();
                  if (data && typeof data === 'object' && data.headings && typeof data.headings === 'object') {
                      Object.entries(data.headings).forEach(([vehicleID, entry]) => {
                          if (!vehicleID) {
                              return;
                          }
                          let headingValue = entry;
                          let updatedAt = undefined;
                          if (entry && typeof entry === 'object') {
                              headingValue = entry.heading ?? entry.Heading;
                              const tsCandidate = entry.updated_at ?? entry.updatedAt ?? entry.timestamp ?? entry.ts_ms ?? entry.ts;
                              if (Number.isFinite(Number(tsCandidate))) {
                                  updatedAt = Number(tsCandidate);
                              }
                          }
                          const headingNumber = Number(headingValue);
                          if (!Number.isFinite(headingNumber)) {
                              return;
                          }
                          rememberCachedVehicleHeading(vehicleID, headingNumber, updatedAt);
                      });
                  }
              } catch (error) {
                  console.info('Vehicle heading cache unavailable; continuing without it.', error);
              }
              return vehicleHeadingCache;
          })();
          return vehicleHeadingCachePromise;
      }

      function toggleCatOverlay() {
          if (catOverlayEnabled) {
              disableCatOverlay();
          } else {
              enableCatOverlay();
          }
      }

      function enableCatOverlay() {
          catOverlayEnabled = true;
          ensureCatLayerGroup();
          ensureCatBusMarkerSvgLoaded();
          const restoredCatStops = restoreCatStopDataCacheFromStoredStops();
          updateCatToggleButtonState();
          refreshServiceAlertsUI();
          updateRouteSelector(activeRoutes, true);
          renderCatRoutes();
          if (restoredCatStops) {
              renderBusStops(stopDataCache);
          }
          fetchCatRoutes().catch(error => console.error('Failed to fetch CAT routes:', error));
          fetchCatStops().catch(error => console.error('Failed to fetch CAT stops:', error));
          fetchCatRoutePatterns().catch(error => console.error('Failed to fetch CAT route patterns:', error));
          fetchCatVehicles().catch(error => console.error('Failed to fetch CAT vehicles:', error));
          fetchCatServiceAlerts().catch(error => console.error('Failed to fetch CAT service alerts:', error));
          startCatRefreshIntervals();
          evaluateIncidentRouteAlerts();
          updateControlPanel();
      }

      function disableCatOverlay() {
          catOverlayEnabled = false;
          stopCatRefreshIntervals();
          clearCatVehicleMarkers();
          if (catLayerGroup && map && map.hasLayer(catLayerGroup)) {
              map.removeLayer(catLayerGroup);
          }
          catActiveRouteKeys = new Set();
          catRouteSelections.clear();
          catServiceAlerts = [];
          catServiceAlertsLoading = false;
          catServiceAlertsError = null;
          catServiceAlertsFetchPromise = null;
          catServiceAlertsLastFetchTime = 0;
          resetCatOverlapRenderingState();
          catStopDataCache = [];
          catStopEtaCache.clear();
          catStopEtaRequests.clear();
          renderBusStops(stopDataCache);
          updateCatToggleButtonState();
          refreshServiceAlertsUI();
          updateRouteSelector(activeRoutes, true);
          if (enableOverlapDashRendering && overlapRenderer) {
              updateOverlapRendererWithCatRoutes();
          }
          evaluateIncidentRouteAlerts();
          updateControlPanel();
      }

      function ensureCatLayerGroup() {
          if (!map) {
              return null;
          }
          if (!catLayerGroup) {
              catLayerGroup = L.layerGroup();
          }
          if (!map.hasLayer(catLayerGroup)) {
              catLayerGroup.addTo(map);
          }
          return catLayerGroup;
      }

      function startCatRefreshIntervals() {
          stopCatRefreshIntervals();
          if (!catOverlayEnabled) {
              return;
          }
          catRefreshIntervals.push(setInterval(() => {
              fetchCatVehicles().catch(error => console.error('Failed to refresh CAT vehicles:', error));
          }, CAT_VEHICLE_FETCH_INTERVAL_MS));
          catRefreshIntervals.push(setInterval(() => {
              fetchCatRoutes().catch(error => console.error('Failed to refresh CAT routes:', error));
              fetchCatStops().catch(error => console.error('Failed to refresh CAT stops:', error));
              fetchCatRoutePatterns().catch(error => console.error('Failed to refresh CAT route patterns:', error));
          }, CAT_METADATA_REFRESH_INTERVAL_MS));
          catRefreshIntervals.push(setInterval(() => {
              fetchCatServiceAlerts().catch(error => console.error('Failed to refresh CAT service alerts:', error));
          }, CAT_SERVICE_ALERT_REFRESH_INTERVAL_MS));
      }

      function stopCatRefreshIntervals() {
          if (Array.isArray(catRefreshIntervals)) {
              catRefreshIntervals.forEach(intervalId => clearInterval(intervalId));
          }
          catRefreshIntervals = [];
      }

      if (!catOverlayEnabled && kioskMode && !adminKioskMode && kioskModeAllowedCatRouteIds.size > 0) {
          enableCatOverlay();
      }

      function ensureCatBusMarkerSvgLoaded() {
          if (BUS_MARKER_SVG_TEXT) {
              return Promise.resolve(true);
          }
          if (!catBusMarkerSvgPromise && typeof loadBusSVG === 'function') {
              catBusMarkerSvgPromise = loadBusSVG()
                  .then(() => {
                      catBusMarkerSvgPromise = null;
                      return !!BUS_MARKER_SVG_TEXT;
                  })
                  .catch(error => {
                      catBusMarkerSvgPromise = null;
                      console.error('Failed to load bus marker SVG for CAT overlay:', error);
                      return false;
                  });
          }
          return catBusMarkerSvgPromise || Promise.resolve(false);
      }

      function isCatOutOfServiceRouteValue(value) {
          if (value === undefined || value === null) {
              return false;
          }
          const text = `${value}`.trim();
          if (text === '') {
              return false;
          }
          if (text === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
              return true;
          }
          const numeric = Number(text);
          if (Number.isFinite(numeric) && numeric === CAT_OUT_OF_SERVICE_NUMERIC_ROUTE_ID) {
              return true;
          }
          return false;
      }

      function catRouteKey(routeId) {
          if (routeId === undefined || routeId === null) {
              return '';
          }
          const text = `${routeId}`.trim();
          if (text === '') {
              return '';
          }
          if (isCatOutOfServiceRouteValue(text)) {
              return CAT_OUT_OF_SERVICE_ROUTE_KEY;
          }
          return text;
      }

      function isCatRouteAllowedInCurrentMode(routeKeyOrId) {
          if (!kioskMode || adminKioskMode) {
              return true;
          }
          const normalized = catRouteKey(routeKeyOrId);
          if (!normalized || isCatOutOfServiceRouteValue(normalized)) {
              return false;
          }
          const numeric = Number(normalized);
          if (!Number.isFinite(numeric)) {
              return false;
          }
          return kioskModeAllowedCatRouteIds.has(numeric);
      }

      function catStopKey(stopId) {
          if (stopId === undefined || stopId === null) {
              return '';
          }
          return `${stopId}`.trim();
      }

      function isCatOverlapRouteId(routeId) {
          const numeric = Number(routeId);
          if (Number.isNaN(numeric)) {
              return false;
          }
          return catOverlapInfoByNumericId.has(numeric);
      }

      function resetCatOverlapRenderingState() {
          catOverlapPatternIdMap.clear();
          catOverlapInfoByNumericId.clear();
          nextCatOverlapNumericId = 1000000;
      }

      function mergeNumericRouteIds(...lists) {
          const merged = new Set();
          lists.forEach(list => {
              if (!Array.isArray(list)) {
                  return;
              }
              list.forEach(id => {
                  const numeric = Number(id);
                  if (!Number.isNaN(numeric)) {
                      merged.add(numeric);
                  }
              });
          });
          return Array.from(merged).sort((a, b) => a - b);
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

      function ensureCatOverlapPatternEntry(patternKey, geometry) {
          if (!patternKey || !geometry) {
              return null;
          }
          let numericId = catOverlapPatternIdMap.get(patternKey);
          if (!Number.isFinite(numericId)) {
              numericId = nextCatOverlapNumericId;
              nextCatOverlapNumericId += 1;
              catOverlapPatternIdMap.set(patternKey, numericId);
          }
          const normalizedRouteKey = catRouteKey(geometry.routeKey);
          const color = sanitizeCssColor(geometry.color)
              || sanitizeCssColor(getCatRouteColor(normalizedRouteKey))
              || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
          const encoded = typeof geometry.encoded === 'string' ? geometry.encoded : '';
          const latLngs = Array.isArray(geometry.latLngs) ? geometry.latLngs : [];
          const geometrySignature = encoded || buildLatLngSignature(latLngs);
          catOverlapInfoByNumericId.set(numericId, {
              patternKey,
              routeKey: normalizedRouteKey,
              color,
              geometrySignature,
              encoded
          });
          return numericId;
      }

      function buildCatOverlapRendererData() {
          const geometries = new Map();
          const routeIds = [];
          if (!catOverlayEnabled) {
              return { geometries, routeIds };
          }
          catRoutePatternGeometries.forEach((geometry, patternKey) => {
              if (!geometry || !Array.isArray(geometry.latLngs) || geometry.latLngs.length < 2) {
                  return;
              }
              if (!isCatRouteVisible(geometry.routeKey)) {
                  return;
              }
              const numericId = ensureCatOverlapPatternEntry(patternKey, geometry);
              if (!Number.isFinite(numericId)) {
                  return;
              }
              geometries.set(numericId, geometry.latLngs);
              routeIds.push(numericId);
          });
          return {
              geometries,
              routeIds: mergeNumericRouteIds(routeIds)
          };
      }

      function getRouteGeometrySignature(routeId, geometryMap = currentTranslocRendererGeometries) {
          const numeric = Number(routeId);
          if (!Number.isNaN(numeric) && routePolylineCache.has(numeric)) {
              const cacheEntry = routePolylineCache.get(numeric);
              if (cacheEntry && typeof cacheEntry.encoded === 'string') {
                  return cacheEntry.encoded;
              }
          }
          if (!Number.isNaN(numeric) && catOverlapInfoByNumericId.has(numeric)) {
              const info = catOverlapInfoByNumericId.get(numeric);
              if (info && info.geometrySignature) {
                  return info.geometrySignature;
              }
          }
          if (geometryMap instanceof Map && geometryMap.has(numeric)) {
              return buildLatLngSignature(geometryMap.get(numeric));
          }
          return '';
      }

      function getUniqueCatRoutes() {
          const unique = new Map();
          catRoutesById.forEach(route => {
              if (!route || typeof route !== 'object') {
                  return;
              }
              const key = route.idKey ? `${route.idKey}`.trim() : '';
              if (!key || key === CAT_OUT_OF_SERVICE_ROUTE_KEY || unique.has(key)) {
                  return;
              }
              const routeIdentifier = Number.isFinite(route?.id) ? route.id : key;
              if (!isCatRouteAllowedInCurrentMode(routeIdentifier)) {
                  return;
              }
              unique.set(key, route);
          });
          return Array.from(unique.values());
      }

      function getCatRouteSortKey(route) {
          if (!route || typeof route !== 'object') {
              return '';
          }
          const candidates = [
              route.displayName,
              route.shortName,
              route.longName,
              route.idKey,
              Number.isFinite(route?.id) ? `${route.id}` : ''
          ];
          for (const value of candidates) {
              if (typeof value === 'string') {
                  const trimmed = value.trim();
                  if (trimmed) {
                      return trimmed;
                  }
              }
          }
          return '';
      }

      function compareCatRouteSortKeys(a, b) {
          const keyA = getCatRouteSortKey(a);
          const keyB = getCatRouteSortKey(b);
          if (keyA && keyB) {
              const comparison = keyA.localeCompare(keyB, undefined, { numeric: true, sensitivity: 'base' });
              if (comparison !== 0) {
                  return comparison;
              }
          } else if (keyA) {
              return -1;
          } else if (keyB) {
              return 1;
          }
          const fallbackA = (a?.idKey || '').trim().toUpperCase();
          const fallbackB = (b?.idKey || '').trim().toUpperCase();
          if (fallbackA < fallbackB) return -1;
          if (fallbackA > fallbackB) return 1;
          const numericA = Number.isFinite(a?.id) ? a.id : Number.POSITIVE_INFINITY;
          const numericB = Number.isFinite(b?.id) ? b.id : Number.POSITIVE_INFINITY;
          if (numericA < numericB) return -1;
          if (numericA > numericB) return 1;
          return 0;
      }

      function getSortedCatRoutes() {
          const routes = getUniqueCatRoutes();
          routes.sort(compareCatRouteSortKeys);
          return routes;
      }

      function getCatRouteCheckboxId(routeKey) {
          const normalized = catRouteKey(routeKey);
          const safeId = normalized ? normalized.replace(/[^A-Za-z0-9_-]/g, '_') : 'unassigned';
          return `cat_route_${safeId}`;
      }

      function getCatRouteSelectionState(routeKey, activeFallbackSet = catActiveRouteKeys) {
          const normalized = catRouteKey(routeKey);
          if (!normalized) {
              return false;
          }
          if (normalized === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
              return isOutOfServiceRouteVisible();
          }
          if (catRouteSelections.has(normalized)) {
              return !!catRouteSelections.get(normalized);
          }
          const fallbackSet = activeFallbackSet instanceof Set ? activeFallbackSet : null;
          if (catOverlayEnabled && catRouteSelections.size === 0 && (!fallbackSet || fallbackSet.size === 0)) {
              return true;
          }
          return fallbackSet ? fallbackSet.has(normalized) : false;
      }

      function isOutOfServiceRouteVisible() {
          if (!canDisplayRoute(0)) {
              return false;
          }
          if (Object.prototype.hasOwnProperty.call(routeSelections, 0)) {
              return !!routeSelections[0];
          }
          return activeRoutes instanceof Set ? activeRoutes.has(0) : false;
      }

      function getEffectiveCatRouteKey(vehicle) {
          if (!vehicle || typeof vehicle !== 'object') {
              return CAT_OUT_OF_SERVICE_ROUTE_KEY;
          }
          const candidates = [vehicle.routeKey, vehicle.routeId];
          for (let i = 0; i < candidates.length; i += 1) {
              const normalized = catRouteKey(candidates[i]);
              if (normalized) {
                  return normalized;
              }
          }
          return CAT_OUT_OF_SERVICE_ROUTE_KEY;
      }

      function isCatRouteVisible(routeKey, activeFallbackSet = catActiveRouteKeys) {
          if (isCatOutOfServiceRouteValue(routeKey)) {
              return isOutOfServiceRouteVisible();
          }
          const normalized = catRouteKey(routeKey);
          if (!normalized) {
              return false;
          }
          if (!isCatRouteAllowedInCurrentMode(normalized)) {
              return false;
          }
          if (catRouteSelections.has(normalized)) {
              return !!catRouteSelections.get(normalized);
          }
          const fallbackSet = activeFallbackSet instanceof Set ? activeFallbackSet : null;
          if (catOverlayEnabled && catRouteSelections.size === 0 && (!fallbackSet || fallbackSet.size === 0)) {
              return true;
          }
          return fallbackSet ? fallbackSet.has(normalized) : false;
      }

      function toNonEmptyString(value) {
          if (value === undefined || value === null) {
              return '';
          }
          const text = `${value}`.trim();
          return text;
      }

      function toNumberOrNull(value) {
          const num = Number(value);
          return Number.isFinite(num) ? num : null;
      }

      function getFirstDefined(source, keys) {
          if (!source || typeof source !== 'object' || !Array.isArray(keys)) {
              return undefined;
          }
          for (const key of keys) {
              if (Object.prototype.hasOwnProperty.call(source, key) && source[key] !== undefined && source[key] !== null) {
                  return source[key];
              }
          }
          return undefined;
      }

      function toCatLatLng(latCandidate, lonCandidate) {
          let lat = Number(latCandidate);
          let lon = Number(lonCandidate);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
              return null;
          }
          if (Math.abs(lat) > 90 && Math.abs(lon) <= 90) {
              const swappedLat = Number(lonCandidate);
              const swappedLon = Number(latCandidate);
              if (Number.isFinite(swappedLat) && Number.isFinite(swappedLon)) {
                  lat = swappedLat;
                  lon = swappedLon;
              }
          }
          if (Math.abs(lat) > 90 || Math.abs(lon) > 180) {
              return null;
          }
          try {
              return L.latLng(lat, lon);
          } catch (error) {
              return null;
          }
      }

      function decodeCatPolyline(encoded) {
          if (typeof encoded !== 'string') {
              return [];
          }
          const trimmed = encoded.trim();
          if (trimmed.length === 0) {
              return [];
          }
          try {
              const coords = polyline.decode(trimmed);
              if (!Array.isArray(coords)) {
                  return [];
              }
              return coords
                  .map(pair => {
                      if (!Array.isArray(pair) || pair.length < 2) {
                          return null;
                      }
                      const lat = Number(pair[0]);
                      const lon = Number(pair[1]);
                      if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
                          return null;
                      }
                      try {
                          return L.latLng(lat, lon);
                      } catch (error) {
                          return null;
                      }
                  })
                  .filter(Boolean);
          } catch (error) {
              console.warn('Failed to decode CAT pattern polyline:', error);
              return [];
          }
      }

      function normalizeCatPatternCoordinates(entry) {
          const encoded = toNonEmptyString(getFirstDefined(entry, [
              'encLine',
              'EncLine',
              'encodedPolyline',
              'EncodedPolyline',
              'polyline',
              'Polyline'
          ]));
          if (encoded) {
              const encodedCoords = decodeCatPolyline(encoded);
              if (encodedCoords.length >= 2) {
                  return { latLngs: encodedCoords, encoded };
              }
          }

          const decodedLine = entry && typeof entry === 'object'
              ? (entry.decLine || entry.DecLine || entry.decodedLine || entry.DecodedLine)
              : null;
          const latLngs = [];
          if (Array.isArray(decodedLine)) {
              decodedLine.forEach(point => {
                  let candidate = null;
                  if (Array.isArray(point) && point.length >= 2) {
                      candidate = toCatLatLng(point[0], point[1]) || toCatLatLng(point[1], point[0]);
                  } else if (point && typeof point === 'object') {
                      const latValue = getFirstDefined(point, ['lat', 'Lat', 'latitude', 'Latitude', 'y', 'Y']);
                      const lonValue = getFirstDefined(point, ['lon', 'Lon', 'lng', 'Lng', 'long', 'Long', 'longitude', 'Longitude', 'x', 'X']);
                      candidate = toCatLatLng(latValue, lonValue);
                  }
                  if (candidate) {
                      latLngs.push(candidate);
                  }
              });
          }

          if (latLngs.length >= 2) {
              return { latLngs, encoded: encoded || '' };
          }

          return { latLngs: [], encoded: encoded || '' };
      }

      function extractCatArray(root, candidateKeys = []) {
          if (Array.isArray(root)) {
              return root;
          }
          if (!root || typeof root !== 'object') {
              return [];
          }
          const candidateList = Array.isArray(candidateKeys) ? candidateKeys : (candidateKeys ? [candidateKeys] : []);
          const extendedCandidateKeys = candidateList.concat([
                  'get_routes',
                  'get_stops',
                  'get_vehicles',
                  'get_patterns',
                  'get_service_announcements',
                  'GetRoutes',
                  'GetStops',
                  'GetVehicles',
                  'GetPatterns',
                  'GetServiceAnnouncements'
              ]);
          for (const key of extendedCandidateKeys) {
              const value = root[key];
              if (Array.isArray(value)) {
                  return value;
              }
          }
          const fallbackKeys = ['data', 'Data', 'result', 'Result', 'results', 'Results', 'items', 'Items', 'values', 'Values'];
          for (const key of fallbackKeys) {
              const value = root[key];
              if (Array.isArray(value)) {
                  return value;
              }
          }
          for (const key of Object.keys(root)) {
              const value = root[key];
              if (Array.isArray(value)) {
                  return value;
              }
          }
          return [];
      }

      function normalizeCatRoute(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }
          const rawId = getFirstDefined(entry, ['RouteID', 'routeID', 'RouteId', 'routeId', 'ID', 'Id', 'id']);
          const idKey = catRouteKey(rawId);
          if (!idKey || idKey === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
              return null;
          }
          const numericId = toNumberOrNull(rawId);
          const colorValue = getFirstDefined(entry, ['Color', 'color', 'RouteColor', 'RouteHexColor', 'HexColor', 'DisplayColor', 'MapColor', 'RGB']);
          const color = sanitizeCssColor(colorValue) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
          const shortName = toNonEmptyString(getFirstDefined(entry, ['RouteAbbreviation', 'routeAbbreviation', 'RouteShortName', 'routeShortName', 'ShortName', 'shortName', 'Abbreviation', 'abbreviation', 'abbr']));
          const longName = toNonEmptyString(getFirstDefined(entry, ['RouteName', 'routeName', 'Description', 'description', 'LongName', 'longName', 'Name', 'name']));
          const displayName = shortName || longName || `Route ${idKey}`;
          return {
              id: numericId,
              idKey,
              color,
              shortName,
              longName,
              displayName
          };
      }

      function normalizeCatRoutes(root) {
          const entries = extractCatArray(root, ['routes', 'Routes']);
          const routes = [];
          entries.forEach(entry => {
              const normalized = normalizeCatRoute(entry);
              if (normalized) {
                  routes.push(normalized);
              }
          });
          return routes;
      }

      function normalizeCatPattern(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }

          const coordinates = normalizeCatPatternCoordinates(entry);
          if (!Array.isArray(coordinates.latLngs) || coordinates.latLngs.length < 2) {
              return null;
          }

          const routeCandidates = [];
          [entry.routes, entry.Routes, entry.routeIDs, entry.RouteIDs, entry.routeIds, entry.RouteIds]
              .filter(Array.isArray)
              .forEach(list => {
                  list.forEach(value => routeCandidates.push(value));
              });
          [
              entry.routeID,
              entry.RouteID,
              entry.routeId,
              entry.RouteId,
              entry.route,
              entry.Route
          ].forEach(value => {
              if (value !== undefined && value !== null) {
                  routeCandidates.push(value);
              }
          });

          const extIdText = toNonEmptyString(getFirstDefined(entry, ['extID', 'ExtID', 'externalId', 'ExternalId']));
          if (extIdText) {
              const extMatch = extIdText.match(/^(\s*\d+)/);
              if (extMatch && extMatch[1]) {
                  routeCandidates.push(extMatch[1]);
              }
          }

          const nameText = toNonEmptyString(getFirstDefined(entry, ['name', 'Name']));
          if (nameText) {
              const nameMatch = nameText.match(/^(\s*\d+)/);
              if (nameMatch && nameMatch[1]) {
                  routeCandidates.push(nameMatch[1]);
              }
          }

          const uniqueRouteKeys = new Set();
          routeCandidates.forEach(value => {
              const key = catRouteKey(value);
              if (key && key !== CAT_OUT_OF_SERVICE_ROUTE_KEY) {
                  uniqueRouteKeys.add(key);
              }
          });

          if (uniqueRouteKeys.size === 0) {
              return null;
          }

          const colorValue = sanitizeCssColor(getFirstDefined(entry, [
              'color',
              'Color',
              'routeColor',
              'RouteColor',
              'lineColor',
              'LineColor',
              'displayColor',
              'DisplayColor'
          ]));

          const idText = toNonEmptyString(getFirstDefined(entry, ['id', 'Id', 'patternId', 'PatternId', 'PatternID', 'patternID']));

          return {
              id: idText,
              extId: extIdText,
              name: nameText,
              routeKeys: Array.from(uniqueRouteKeys.values()),
              latLngs: coordinates.latLngs,
              color: colorValue || '',
              encoded: coordinates.encoded || ''
          };
      }

      function normalizeCatPatterns(root) {
          const entries = extractCatArray(root, ['patterns', 'Patterns']);
          const patterns = [];
          entries.forEach(entry => {
              const normalized = normalizeCatPattern(entry);
              if (normalized) {
                  patterns.push(normalized);
              }
          });
          return patterns;
      }

      async function fetchCatRoutes(force = false) {
          if (!catOverlayEnabled && !force) {
              return [];
          }
          const now = Date.now();
          if (!force && catRoutesById.size > 0 && (now - catRoutesLastFetchTime) < CAT_METADATA_REFRESH_INTERVAL_MS) {
              return Array.from(catRoutesById.values());
          }
          try {
              const response = await fetch(CAT_ROUTES_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              const routes = normalizeCatRoutes(payload);
              if (!catOverlayEnabled && !force) {
                  return routes;
              }
              catRoutesById.clear();
              routes.forEach(route => {
                  if (!route || route.idKey === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
                      return;
                  }
                  catRoutesById.set(route.idKey, route);
                  catRoutesById.set(`${route.idKey}`, route);
                  if (Number.isFinite(route.id)) {
                      catRoutesById.set(`${route.id}`, route);
                  }
              });
              catRoutesLastFetchTime = now;
              if (catOverlayEnabled) {
                  updateRouteSelector(activeRoutes);
                  renderCatRoutes();
                  if (Array.isArray(catStopDataCache) && catStopDataCache.length > 0) {
                      renderBusStops(stopDataCache);
                  }
              }
              return routes;
          } catch (error) {
              console.error('Failed to fetch CAT routes:', error);
              return [];
          }
      }

      function normalizeCatStop(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }
          const rawId = getFirstDefined(entry, ['StopID', 'stopID', 'StopId', 'stopId', 'ID', 'Id', 'id']);
          const idKey = catStopKey(rawId);
          if (!idKey) {
              return null;
          }
          const name = toNonEmptyString(getFirstDefined(entry, ['StopName', 'stopName', 'Name', 'name', 'Description', 'description'])) || `Stop ${idKey}`;
          const lat = toNumberOrNull(getFirstDefined(entry, ['Latitude', 'latitude', 'Lat', 'lat']));
          const lon = toNumberOrNull(getFirstDefined(entry, ['Longitude', 'longitude', 'Lon', 'lon', 'Lng', 'lng']));
          const rawRouteId = getFirstDefined(entry, ['RouteID', 'routeID', 'RouteId', 'routeId', 'rid', 'Rid', 'RID', 'Route', 'route']);
          const routeKey = catRouteKey(rawRouteId);
          const rawRouteStopId = getFirstDefined(entry, ['RouteStopID', 'RouteStopId', 'rsid', 'Rsid', 'RSID']);
          const routeStopId = normalizeIdentifier(rawRouteStopId);
          return {
              id: idKey,
              rawId,
              name,
              latitude: lat,
              longitude: lon,
              routeKey: routeKey && routeKey !== CAT_OUT_OF_SERVICE_ROUTE_KEY ? routeKey : null,
              routeStopId: routeStopId || null
          };
      }

      function normalizeCatStops(root) {
          const entries = extractCatArray(root, ['stops', 'Stops']);
          const stopsById = new Map();
          entries.forEach(entry => {
              const normalized = normalizeCatStop(entry);
              if (!normalized) {
                  return;
              }
              const stopId = normalized.id;
              if (!stopsById.has(stopId)) {
                  stopsById.set(stopId, {
                      id: stopId,
                      rawId: normalized.rawId,
                      name: normalized.name,
                      latitude: Number.isFinite(normalized.latitude) ? normalized.latitude : null,
                      longitude: Number.isFinite(normalized.longitude) ? normalized.longitude : null,
                      routeKeys: new Set(),
                      routeStopIds: new Set()
                  });
              }
              const stop = stopsById.get(stopId);
              if (toNonEmptyString(normalized.name)) {
                  stop.name = normalized.name;
              }
              if (Number.isFinite(normalized.latitude) && Number.isFinite(normalized.longitude)) {
                  stop.latitude = normalized.latitude;
                  stop.longitude = normalized.longitude;
              }
              if (normalized.routeKey) {
                  stop.routeKeys.add(normalized.routeKey);
              }
              if (normalized.routeStopId) {
                  stop.routeStopIds.add(normalized.routeStopId);
              }
          });
          return Array.from(stopsById.values()).map(stop => ({
              id: stop.id,
              rawId: stop.rawId,
              name: toNonEmptyString(stop.name) || `Stop ${stop.id}`,
              latitude: Number.isFinite(stop.latitude) ? stop.latitude : null,
              longitude: Number.isFinite(stop.longitude) ? stop.longitude : null,
              routeKeys: Array.from(stop.routeKeys),
              routeStopIds: Array.from(stop.routeStopIds),
              isCatStop: true
          })).filter(stop => Number.isFinite(stop.latitude) && Number.isFinite(stop.longitude));
      }

      function buildCatStopDataForRendering(catStops) {
          if (!Array.isArray(catStops)) {
              return [];
          }
          return catStops.map(stop => {
              const stopId = catStopKey(stop?.id ?? stop?.rawId);
              const lat = Number(stop?.latitude);
              const lon = Number(stop?.longitude);
              if (!stopId || !Number.isFinite(lat) || !Number.isFinite(lon)) {
                  return null;
              }
              const name = toNonEmptyString(stop?.name) || `Stop ${stopId}`;
              const routeKeysSource = stop?.routeKeys;
              const routeKeysIterable = Array.isArray(routeKeysSource)
                  ? routeKeysSource
                  : (routeKeysSource instanceof Set ? Array.from(routeKeysSource) : []);
              const normalizedRouteKeys = Array.from(new Set(routeKeysIterable
                  .map(routeKey => catRouteKey(routeKey))
                  .filter(key => key && key !== CAT_OUT_OF_SERVICE_ROUTE_KEY)))
                  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
              const filteredRouteKeys = normalizedRouteKeys.filter(routeKey => isCatRouteAllowedInCurrentMode(routeKey));
              if (kioskMode && !adminKioskMode && filteredRouteKeys.length === 0) {
                  return null;
              }
              const routeStopIdsSource = stop?.routeStopIds ?? stop?.catRouteStopIds;
              const routeStopIdsIterable = Array.isArray(routeStopIdsSource)
                  ? routeStopIdsSource
                  : (routeStopIdsSource instanceof Set ? Array.from(routeStopIdsSource) : []);
              const normalizedRouteStopIds = routeStopIdsIterable
                  .map(value => `${value}`.trim())
                  .filter(value => value !== '' && value.toLowerCase() !== 'null' && value.toLowerCase() !== 'undefined');
              return {
                  isCatStop: true,
                  StopID: stopId,
                  StopId: stopId,
                  StopName: name,
                  Name: name,
                  Latitude: lat,
                  Longitude: lon,
                  catRouteKeys: filteredRouteKeys,
                  CatRouteKeys: filteredRouteKeys.slice(),
                  catRouteStopIds: normalizedRouteStopIds
              };
          }).filter(Boolean);
      }

      function decorateCatEtaForStop(rawEta, fallbackStopId) {
          if (!rawEta || typeof rawEta !== 'object') {
              return null;
          }
          const stopId = catStopKey(rawEta.stopId || fallbackStopId);
          if (!stopId) {
              return null;
          }
          const stopInfo = catStopsById.get(stopId);
          const stopName = toNonEmptyString(rawEta.stopName) || (stopInfo ? stopInfo.name : '');
          const routeKey = catRouteKey(rawEta.routeKey || rawEta.routeId);
          const routeInfo = getCatRouteInfo(routeKey);
          const numericRouteId = Number.isFinite(rawEta.routeId) ? Number(rawEta.routeId) : (routeInfo && Number.isFinite(routeInfo.id) ? Number(routeInfo.id) : null);
          let routeDescription = toNonEmptyString(rawEta.routeDescription);
          if (!routeDescription && routeInfo) {
              routeDescription = routeInfo.displayName || routeInfo.shortName || routeInfo.longName || '';
          }
          if (!routeDescription) {
              if (routeKey) {
                  routeDescription = `Route ${routeKey}`;
              } else if (Number.isFinite(numericRouteId)) {
                  routeDescription = `Route ${numericRouteId}`;
              } else {
                  routeDescription = 'Route';
              }
          }
          let etaMinutes = Number.isFinite(rawEta.minutes) ? Number(rawEta.minutes) : null;
          if (!Number.isFinite(etaMinutes) && Number.isFinite(rawEta.seconds)) {
              etaMinutes = Number(rawEta.seconds) / 60;
          }
          let text = toNonEmptyString(rawEta.text);
          if (!text && Number.isFinite(etaMinutes)) {
              const rounded = Math.max(0, Math.round(etaMinutes));
              text = rounded <= 0 ? 'Arriving' : `${rounded} min`;
          }
          return {
              isCat: true,
              stopId,
              stopName,
              routeId: Number.isFinite(numericRouteId) ? numericRouteId : null,
              routeKey: routeKey || '',
              routeDescription,
              etaMinutes: Number.isFinite(etaMinutes) ? etaMinutes : null,
              text: text || ''
          };
      }

      function ensureCatStopEtas(stopIds) {
          if (!catOverlayEnabled || typeof fetch !== 'function') {
              return Promise.resolve([]);
          }
          if (!Array.isArray(stopIds) || stopIds.length === 0) {
              return Promise.resolve([]);
          }
          const normalizedIds = Array.from(new Set(stopIds.map(catStopKey).filter(id => typeof id === 'string' && id !== '')));
          if (normalizedIds.length === 0) {
              return Promise.resolve([]);
          }
          const now = Date.now();
          const pending = [];
          normalizedIds.forEach(stopId => {
              const cacheEntry = catStopEtaCache.get(stopId);
              if (cacheEntry && (now - cacheEntry.timestamp) <= CAT_STOP_ETA_CACHE_TTL_MS) {
                  return;
              }
              if (catStopEtaRequests.has(stopId)) {
                  pending.push(catStopEtaRequests.get(stopId));
                  return;
              }
              const url = `${CAT_STOP_ETAS_ENDPOINT}?stop_id=${encodeURIComponent(stopId)}`;
              const request = fetch(url, { cache: 'no-store' })
                  .then(response => {
                      if (!response.ok) {
                          throw new Error(`HTTP ${response.status}`);
                      }
                      return response.json();
                  })
                  .then(payload => {
                      const entries = Array.isArray(payload?.etas)
                          ? payload.etas
                          : extractCatArray(payload, ['get_stop_etas']);
                      const timestamp = Date.now();
                      let updated = false;
                      if (Array.isArray(entries) && entries.length > 0) {
                          entries.forEach(entry => {
                              const entryStopId = catStopKey(entry?.id ?? entry?.stopID ?? entry?.StopID ?? entry?.StopId ?? stopId);
                              const targetStopId = entryStopId || stopId;
                              const rawEtas = normalizeCatEtas(entry);
                              const decorated = rawEtas.map(rawEta => decorateCatEtaForStop(rawEta, targetStopId)).filter(Boolean);
                              catStopEtaCache.set(targetStopId, { timestamp, etas: decorated });
                              updated = true;
                          });
                      } else {
                          catStopEtaCache.set(stopId, { timestamp, etas: [] });
                          updated = true;
                      }
                      return updated;
                  })
                  .catch(error => {
                      console.error('Failed to fetch CAT stop ETAs:', error);
                      return false;
                  })
                  .finally(() => {
                      catStopEtaRequests.delete(stopId);
                  });
              catStopEtaRequests.set(stopId, request);
              pending.push(request);
          });
          if (pending.length === 0) {
              return Promise.resolve([]);
          }
          return Promise.allSettled(pending).then(results => {
              const shouldRefresh = results.some(result => result.status === 'fulfilled' && result.value);
              if (shouldRefresh) {
                  updateCustomPopups();
              }
              return results;
          });
      }

      function restoreCatStopDataCacheFromStoredStops() {
          if (Array.isArray(catStopDataCache) && catStopDataCache.length > 0) {
              return true;
          }
          if (!(catStopsById instanceof Map) || catStopsById.size === 0) {
              return false;
          }
          const cachedStops = Array.from(catStopsById.values());
          if (!Array.isArray(cachedStops) || cachedStops.length === 0) {
              return false;
          }
          catStopDataCache = buildCatStopDataForRendering(cachedStops);
          return Array.isArray(catStopDataCache) && catStopDataCache.length > 0;
      }

      async function fetchCatStops(force = false) {
          if (!catOverlayEnabled && !force) {
              return [];
          }
          const now = Date.now();
          if (!force && catStopsById.size > 0 && (now - catStopsLastFetchTime) < CAT_METADATA_REFRESH_INTERVAL_MS) {
              const cachedStops = Array.from(catStopsById.values());
              if (!Array.isArray(catStopDataCache) || catStopDataCache.length === 0) {
                  catStopDataCache = buildCatStopDataForRendering(cachedStops);
                  if (catOverlayEnabled) {
                      renderBusStops(stopDataCache);
                  }
              }
              return cachedStops;
          }
          try {
              const response = await fetch(CAT_STOPS_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              const stops = normalizeCatStops(payload);
              if (!catOverlayEnabled && !force) {
                  return stops;
              }
              catStopsById.clear();
              stops.forEach(stop => {
                  catStopsById.set(stop.id, stop);
              });
              catStopDataCache = buildCatStopDataForRendering(stops);
              catStopsLastFetchTime = now;
              if (catOverlayEnabled) {
                  renderBusStops(stopDataCache);
              }
              return stops;
          } catch (error) {
              console.error('Failed to fetch CAT stops:', error);
              return [];
          }
      }

      async function fetchCatRoutePatterns(force = false) {
          if (!catOverlayEnabled && !force) {
              return catRoutePatternsCache.slice();
          }
          const now = Date.now();
          if (!force && catRoutePatternGeometries.size > 0 && (now - catRoutePatternsLastFetchTime) < CAT_METADATA_REFRESH_INTERVAL_MS) {
              return catRoutePatternsCache.slice();
          }
          try {
              const response = await fetch(CAT_PATTERNS_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              const patterns = normalizeCatPatterns(payload);
              if (!catOverlayEnabled && !force) {
                  return patterns;
              }
              const newGeometries = new Map();
              patterns.forEach((pattern, index) => {
                  if (!pattern || !Array.isArray(pattern.routeKeys) || pattern.routeKeys.length === 0) {
                      return;
                  }
                  const baseKey = pattern.id || pattern.extId || pattern.name || (pattern.encoded ? `enc:${pattern.encoded}` : `idx:${index}`);
                  pattern.routeKeys.forEach(routeKey => {
                      const key = `${routeKey}::${baseKey}`;
                      newGeometries.set(key, {
                          routeKey,
                          color: sanitizeCssColor(pattern.color) || '',
                          latLngs: Array.isArray(pattern.latLngs) ? pattern.latLngs.slice() : [],
                          encoded: pattern.encoded || ''
                      });
                  });
              });

              const keysToRemove = new Set(catRoutePatternGeometries.keys());
              newGeometries.forEach((geometry, key) => {
                  catRoutePatternGeometries.set(key, geometry);
                  keysToRemove.delete(key);
              });
              keysToRemove.forEach(key => {
                  catRoutePatternGeometries.delete(key);
                  const numericId = catOverlapPatternIdMap.get(key);
                  if (Number.isFinite(numericId)) {
                      catOverlapPatternIdMap.delete(key);
                      catOverlapInfoByNumericId.delete(numericId);
                  }
              });

              catRoutePatternsLastFetchTime = now;
              catRoutePatternsCache = patterns;
              renderCatRoutes();
              return patterns;
          } catch (error) {
              console.error('Failed to fetch CAT route patterns:', error);
              return [];
          }
      }

      function normalizeCatEta(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }
          const rawStopId = getFirstDefined(entry, ['StopID', 'stopID', 'StopId', 'stopId', 'Stop', 'stop']);
          const stopId = catStopKey(rawStopId);
          const stopInfo = stopId ? catStopsById.get(stopId) : null;
          const stopName = toNonEmptyString(getFirstDefined(entry, ['StopName', 'stopName', 'Name', 'name', 'Description', 'description'])) || (stopInfo ? stopInfo.name : '');
          const rawRouteId = getFirstDefined(entry, ['RouteID', 'routeID', 'RouteId', 'routeId', 'Route', 'route', 'rid', 'Rid', 'RID']);
          const routeId = toNumberOrNull(rawRouteId);
          const routeKey = catRouteKey(rawRouteId);
          const routeInfo = getCatRouteInfo(routeKey);
          let routeDescription = toNonEmptyString(getFirstDefined(entry, ['RouteDescription', 'routeDescription', 'RouteName', 'routeName', 'RouteLabel', 'routeLabel', 'RouteAbbreviation', 'routeAbbreviation']));
          if (!routeDescription && routeInfo) {
              routeDescription = routeInfo.displayName || routeInfo.shortName || routeInfo.longName || '';
          }
          let minutes = toNumberOrNull(getFirstDefined(entry, ['Minutes', 'minutes', 'Min', 'min', 'EtaMinutes', 'etaMinutes']));
          const seconds = toNumberOrNull(getFirstDefined(entry, ['Seconds', 'seconds', 'Sec', 'sec', 'EtaSeconds', 'etaSeconds']));
          if (!Number.isFinite(minutes) && Number.isFinite(seconds)) {
              minutes = seconds / 60;
          }
          let text = toNonEmptyString(getFirstDefined(entry, ['DisplayTime', 'displayTime', 'Display', 'display', 'Text', 'text', 'Formatted', 'formatted']));
          if (!text) {
              if (Number.isFinite(minutes)) {
                  const rounded = Math.max(0, Math.round(minutes));
                  text = rounded <= 0 ? 'Due' : `${rounded} min`;
              } else if (Number.isFinite(seconds)) {
                  const roundedSeconds = Math.max(0, Math.round(seconds));
                  text = roundedSeconds <= 30 ? 'Due' : `${Math.round(roundedSeconds / 60)} min`;
              }
          }
          if (!text) {
              const fallbackTime = toNonEmptyString(getFirstDefined(entry, ['Time', 'time', 'ArrivalTime', 'arrivalTime', 'Scheduled', 'scheduled']));
              if (fallbackTime) {
                  text = fallbackTime;
              }
          }
          return {
              stopId,
              stopName,
              minutes: Number.isFinite(minutes) ? minutes : null,
              seconds: Number.isFinite(seconds) ? seconds : null,
              text: text || '',
              routeId: Number.isFinite(routeId) ? routeId : null,
              routeKey: routeKey || '',
              routeDescription: routeDescription || '',
              isCat: true
          };
      }

      function normalizeCatEtas(root) {
          const entries = extractCatArray(root, ['ETAs', 'etas', 'Eta', 'eta', 'Predictions', 'predictions', 'MinutesToNextStops', 'minutesToNextStops', 'MinutesToStops', 'minutesToStops', 'EnRoute', 'enRoute', 'Enroute', 'enroute']);
          const etas = [];
          entries.forEach(entry => {
              const normalized = normalizeCatEta(entry);
              if (normalized) {
                  etas.push(normalized);
              }
          });
          return etas;
      }

      function normalizeCatVehicle(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }
          const rawId = getFirstDefined(entry, ['VehicleID', 'vehicleID', 'VehicleId', 'vehicleId', 'ID', 'Id', 'id', 'Name', 'name', 'EquipmentID', 'equipmentID', 'EquipmentId', 'equipmentId']);
          const vehicleId = toNonEmptyString(rawId);
          if (!vehicleId) {
              return null;
          }
          const equipmentId = toNonEmptyString(getFirstDefined(entry, ['EquipmentID', 'equipmentID', 'EquipmentId', 'equipmentId']));
          const latitude = toNumberOrNull(getFirstDefined(entry, ['Latitude', 'latitude', 'Lat', 'lat']));
          const longitude = toNumberOrNull(getFirstDefined(entry, ['Longitude', 'longitude', 'Lon', 'lon', 'Lng', 'lng']));
          if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
              return null;
          }
          const heading = toNumberOrNull(getFirstDefined(entry, ['h', 'H', 'Heading', 'heading', 'Direction', 'direction']));
          const speed = toNumberOrNull(getFirstDefined(entry, ['Speed', 'speed', 'Velocity', 'velocity', 'GpsSpeed', 'GPSSpeed', 'GroundSpeed', 'groundSpeed']));
          const rawRouteId = getFirstDefined(entry, ['RouteID', 'routeID', 'RouteId', 'routeId', 'Route', 'route']);
          const routeKey = catRouteKey(rawRouteId);
          const routeAbbrev = toNonEmptyString(getFirstDefined(entry, ['RouteAbbreviation', 'routeAbbreviation', 'RouteShortName', 'routeShortName', 'ShortName', 'shortName']));
          const routeName = toNonEmptyString(getFirstDefined(entry, ['RouteName', 'routeName', 'Description', 'description']));
          const displayName = toNonEmptyString(getFirstDefined(entry, ['VehicleName', 'vehicleName', 'Name', 'name', 'Label', 'label']))
              || equipmentId
              || `Vehicle ${vehicleId}`;
          const etaSource = getFirstDefined(entry, [
              'ETAs',
              'etas',
              'Eta',
              'eta',
              'Predictions',
              'predictions',
              'MinutesToNextStops',
              'minutesToNextStops',
              'MinutesToStops',
              'minutesToStops'
          ]);
          const etas = normalizeCatEtas(etaSource);
          return {
              id: vehicleId,
              equipmentId,
              latitude,
              longitude,
              heading,
              speed,
              routeKey,
              routeId: rawRouteId,
              routeAbbrev,
              routeName,
              displayName,
              etas
          };
      }

      function normalizeCatVehicles(root) {
          const entries = extractCatArray(root, ['vehicles', 'Vehicles']);
          const vehicles = [];
          entries.forEach(entry => {
              const normalized = normalizeCatVehicle(entry);
              if (normalized) {
                  vehicles.push(normalized);
              }
          });
          return vehicles;
      }

      function formatCatRouteBubbleLabel(routeId) {
          if (routeId === undefined || routeId === null) {
              return null;
          }
          if (isCatOutOfServiceRouteValue(routeId)) {
              return null;
          }
          const text = `${routeId}`.trim();
          if (text === '') {
              return null;
          }
          const numeric = Number(text);
          if (Number.isFinite(numeric)) {
              const rounded = Math.trunc(numeric) === numeric ? Math.trunc(numeric) : numeric;
              if (rounded === 777 || numeric === 777) {
                  return null;
              }
              if (rounded === 12 || numeric === 12) {
                  return 'T';
              }
              return `Rte. ${rounded}`;
          }
          if (text === '777') {
              return null;
          }
          if (text === '12') {
              return 'T';
          }
          return `Rte. ${text}`;
      }

      function getCatRouteInfo(routeKey) {
          if (!routeKey) {
              return null;
          }
          const key = `${routeKey}`.trim();
          if (key === '' || key === CAT_OUT_OF_SERVICE_ROUTE_KEY) {
              return null;
          }
          if (catRoutesById.has(key)) {
              return catRoutesById.get(key);
          }
          if (Number.isFinite(Number(routeKey))) {
              const numericKey = `${Number(routeKey)}`;
              return catRoutesById.get(numericKey) || null;
          }
          return null;
      }

      function getCatRouteColor(routeKey) {
          const routeInfo = getCatRouteInfo(routeKey);
          if (routeInfo && routeInfo.color) {
              return routeInfo.color;
          }
          return CAT_VEHICLE_MARKER_DEFAULT_COLOR;
      }

      function buildLegacyCatVehicleIcon(label, color) {
          const fallbackLabel = label || CAT_VEHICLE_MARKER_MIN_LABEL;
          const safeColor = escapeAttribute(color || CAT_VEHICLE_MARKER_DEFAULT_COLOR);
          const safeLabel = escapeHtml(fallbackLabel);
          const html = `<div class="cat-vehicle-marker" style="--cat-marker-color:${safeColor};"><span class="cat-vehicle-marker__label">${safeLabel}</span></div>`;
          return L.divIcon({ className: 'cat-vehicle-icon', html, iconSize: [38, 38], iconAnchor: [19, 19] });
      }

      function buildCatVehicleIcon(vehicle, routeKeyOverride) {
          if (!vehicle || !vehicle.id) {
              return null;
          }
          const effectiveRouteKey = routeKeyOverride || vehicle.catEffectiveRouteKey || getEffectiveCatRouteKey(vehicle);
          const routeInfo = getCatRouteInfo(effectiveRouteKey);
          const label = vehicle.routeAbbrev
              || routeInfo?.shortName
              || routeInfo?.displayName
              || vehicle.routeName
              || vehicle.id
              || CAT_VEHICLE_MARKER_MIN_LABEL;
          const routeColor = sanitizeCssColor(getCatRouteColor(effectiveRouteKey)) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
          const headingDeg = normalizeHeadingDegrees(Number(vehicle.heading));
          const groundSpeed = Number(vehicle.speed);
          const accessibleName = toNonEmptyString(vehicle.displayName)
              || toNonEmptyString(vehicle.equipmentId)
              || label;
          const accessibleLabel = buildBusMarkerAccessibleLabel(accessibleName, headingDeg, groundSpeed);
          const glyphColor = computeBusMarkerGlyphColor(routeColor);
          const markerMetrics = computeBusMarkerMetrics(map && typeof map?.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM);
          const state = {
              fillColor: routeColor,
              glyphColor,
              accessibleLabel,
              headingDeg,
              isStopped: isBusConsideredStopped(groundSpeed),
              isStale: false,
              isSelected: false,
              isHovered: false,
              size: {
                  widthPx: markerMetrics.widthPx,
                  heightPx: markerMetrics.heightPx,
                  scale: markerMetrics.scale
              }
          };
          const icon = BUS_MARKER_SVG_TEXT ? buildBusMarkerDivIconSync(`cat-${vehicle.id}`, state) : null;
          if (icon) {
              return icon;
          }
          return buildLegacyCatVehicleIcon(label, routeColor);
      }

      function buildCatVehicleTooltip(vehicle, routeKeyOverride, options = {}) {
          const parts = [];
          const headerPieces = [];
          const effectiveRouteKey = routeKeyOverride || vehicle.catEffectiveRouteKey || getEffectiveCatRouteKey(vehicle);
          const routeInfo = getCatRouteInfo(effectiveRouteKey);
          const routeLabel = vehicle.routeAbbrev || routeInfo?.shortName || routeInfo?.displayName || vehicle.routeName;
          if (routeLabel) {
              headerPieces.push(routeLabel);
          }
          if (vehicle.displayName && vehicle.displayName !== routeLabel) {
              headerPieces.push(vehicle.displayName);
          }
          if (headerPieces.length) {
              parts.push(`<strong>${escapeHtml(headerPieces.join(' • '))}</strong>`);
          }
          const statusMessage = toNonEmptyString(options?.statusMessage);
          if (statusMessage) {
              parts.push(`<span>${escapeHtml(statusMessage)}</span>`);
          }
          const etaLines = [];
          const rawEtas = Array.isArray(options?.etas)
              ? options.etas
              : (Array.isArray(vehicle.etas) ? vehicle.etas : []);
          const etas = rawEtas.slice(0, CAT_MAX_TOOLTIP_ETAS);
          etas.forEach(eta => {
              const stopLabel = eta.stopName || (eta.stopId ? `Stop ${eta.stopId}` : 'Stop');
              const text = eta.text || (Number.isFinite(eta.minutes) ? `${Math.max(0, Math.round(eta.minutes))} min` : 'Scheduled');
              etaLines.push(`<span>${escapeHtml(stopLabel)}: ${escapeHtml(text)}</span>`);
          });
          if (etaLines.length) {
              parts.push(`<div class="cat-vehicle-tooltip__etas">${etaLines.join('')}</div>`);
          } else if (!statusMessage) {
              parts.push(`<span>${escapeHtml('No upcoming ETAs')}</span>`);
          }
          return parts.join('');
      }

      function closeCatVehicleTooltip(preserveIntent = false) {
          if (!catActiveVehicleTooltip) {
              return;
          }
          const { marker, tooltip } = catActiveVehicleTooltip;
          if (!preserveIntent && marker) {
              marker._catTooltipShouldRemainOpen = false;
          }
          if (tooltip && map && typeof map.removeLayer === 'function') {
              map.removeLayer(tooltip);
          }
          catActiveVehicleTooltip = null;
      }

      function ensureCatVehicleTooltipForMarker(marker) {
          if (!marker) {
              return null;
          }
          if (!marker._catVehicleTooltipInstance) {
              marker._catVehicleTooltipInstance = L.tooltip({
                  direction: 'top',
                  offset: [0, -26],
                  className: 'cat-vehicle-tooltip'
              });
          }
          return marker._catVehicleTooltipInstance;
      }

      function openCatVehicleTooltip(marker, options = {}) {
          if (!marker || !map) {
              return;
          }
          const vehicle = marker.catVehicleData;
          if (!vehicle) {
              return;
          }
          const tooltip = ensureCatVehicleTooltipForMarker(marker);
          if (!tooltip) {
              return;
          }
          tooltip.setContent(buildCatVehicleTooltip(vehicle, marker.catEffectiveRouteKey, options));
          tooltip.setLatLng(marker.getLatLng());
          const preserveIntent = catActiveVehicleTooltip?.marker === marker;
          closeCatVehicleTooltip(preserveIntent);
          tooltip.addTo(map);
          catActiveVehicleTooltip = { marker, tooltip };
      }

      async function fetchCatVehicleEtasForVehicle(vehicle) {
          if (!vehicle) {
              return [];
          }
          const equipmentId = toNonEmptyString(vehicle.equipmentId);
          const vehicleId = toNonEmptyString(vehicle.id);
          const cacheKey = (equipmentId || vehicleId || '').toLowerCase();
          if (!cacheKey) {
              return [];
          }
          const now = Date.now();
          const cached = catVehicleEtaCache.get(cacheKey);
          if (cached && now - cached.timestamp <= CAT_VEHICLE_ETA_CACHE_TTL_MS) {
              return cached.etas;
          }
          try {
              const response = await fetch(CAT_VEHICLES_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              const vehicles = normalizeCatVehicles(payload);
              let target = null;
              if (Array.isArray(vehicles) && vehicles.length) {
                  target = vehicles.find(entry => {
                      const entryEquipmentId = toNonEmptyString(entry.equipmentId);
                      const entryVehicleId = toNonEmptyString(entry.id);
                      return (equipmentId && entryEquipmentId === equipmentId) || (vehicleId && entryVehicleId === vehicleId);
                  }) || vehicles[0];
              }
              const etas = Array.isArray(target?.etas) ? target.etas : [];
              catVehicleEtaCache.set(cacheKey, { etas, timestamp: now });
              return etas;
          } catch (error) {
              catVehicleEtaCache.delete(cacheKey);
              throw error;
          }
      }

      async function handleCatVehicleMarkerClick(event) {
          if (!event || !event.target) {
              return;
          }
          const marker = event.target;
          if (!marker.catVehicleData) {
              return;
          }
          if (event.originalEvent) {
              if (typeof event.originalEvent.stopPropagation === 'function') {
                  event.originalEvent.stopPropagation();
              }
              if (typeof event.originalEvent.preventDefault === 'function') {
                  event.originalEvent.preventDefault();
              }
          }
          if (marker._catTooltipLoading) {
              return;
          }
          if (catActiveVehicleTooltip && catActiveVehicleTooltip.marker === marker) {
              closeCatVehicleTooltip();
          }
          marker._catTooltipShouldRemainOpen = true;
          marker._catTooltipLoading = true;
          marker._catTooltipRequestId = (marker._catTooltipRequestId || 0) + 1;
          const requestId = marker._catTooltipRequestId;
          openCatVehicleTooltip(marker, { statusMessage: 'Loading arrival times…' });
          try {
              const etas = await fetchCatVehicleEtasForVehicle(marker.catVehicleData);
              if (marker._catTooltipRequestId !== requestId || marker._catTooltipShouldRemainOpen === false) {
                  return;
              }
              if (Array.isArray(etas)) {
                  marker.catVehicleData = Object.assign({}, marker.catVehicleData, { etas });
              }
              if (marker._catTooltipShouldRemainOpen !== false) {
                  openCatVehicleTooltip(marker);
              }
          } catch (error) {
              console.error('Failed to load CAT vehicle ETAs:', error);
              if (marker._catTooltipRequestId === requestId && marker._catTooltipShouldRemainOpen !== false) {
                  openCatVehicleTooltip(marker, { statusMessage: 'Unable to load ETAs' });
              }
          } finally {
              if (marker._catTooltipRequestId === requestId) {
                  marker._catTooltipLoading = false;
              }
          }
      }

      function updateCatVehicleCache(vehicles) {
          catVehiclesById.clear();
          const activeKeys = new Set();
          if (!Array.isArray(vehicles)) {
              catActiveRouteKeys = activeKeys;
              return;
          }
          vehicles.forEach(vehicle => {
              if (!vehicle || typeof vehicle !== 'object') {
                  return;
              }
              const vehicleId = vehicle.id;
              if (!vehicleId) {
                  return;
              }
              const effectiveRouteKey = getEffectiveCatRouteKey(vehicle);
              const cachedVehicle = Object.assign({}, vehicle, { catEffectiveRouteKey: effectiveRouteKey });
              catVehiclesById.set(vehicleId, cachedVehicle);
              if (effectiveRouteKey !== CAT_OUT_OF_SERVICE_ROUTE_KEY && isCatRouteAllowedInCurrentMode(effectiveRouteKey)) {
                  activeKeys.add(effectiveRouteKey);
              }
          });
          catActiveRouteKeys = activeKeys;
          renderCatRoutes();
          if (catOverlayEnabled) {
              renderBusStops(stopDataCache);
          }
      }

      function removeCatRouteMarkerForBubble(bubbleKey, bubbleEntry = undefined) {
          const key = typeof bubbleKey === 'string' ? bubbleKey : null;
          const bubble = bubbleEntry || (key && Object.prototype.hasOwnProperty.call(nameBubbles, key) ? nameBubbles[key] : null);
          if (!bubble) {
              if (key && Object.prototype.hasOwnProperty.call(nameBubbles, key)) {
                  delete nameBubbles[key];
              }
              return;
          }
          if (bubble.catRouteMarker) {
              if (map && typeof map.removeLayer === 'function') {
                  map.removeLayer(bubble.catRouteMarker);
              } else if (typeof bubble.catRouteMarker.remove === 'function') {
                  bubble.catRouteMarker.remove();
              }
              delete bubble.catRouteMarker;
          }
          if (!bubble.speedMarker && !bubble.nameMarker && !bubble.blockMarker && !bubble.routeMarker && !bubble.catRouteMarker) {
              if (key) {
                  delete nameBubbles[key];
              }
          }
      }

      function renderCatVehiclesUsingCache() {
          if (!catOverlayEnabled) {
              return;
          }
          const busMarkerReady = !!BUS_MARKER_SVG_TEXT;
          if (!busMarkerReady) {
              ensureCatBusMarkerSvgLoaded().then(loaded => {
                  if (loaded && catOverlayEnabled) {
                      renderCatVehiclesUsingCache();
                  }
              });
          }
          const layerGroup = ensureCatLayerGroup();
          if (!layerGroup) {
              return;
          }
          const markerMetricsForZoom = computeBusMarkerMetrics(map && typeof map?.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM);
          const seen = new Set();
          catVehiclesById.forEach(vehicle => {
              if (!vehicle || !vehicle.id) {
                  return;
              }
              const markerKey = `cat-${vehicle.id}`;
              const lat = Number(vehicle.latitude);
              const lng = Number(vehicle.longitude);
              if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
                  removeNameBubbleForKey(markerKey);
                  return;
              }
              const newPosition = [lat, lng];
              const effectiveRouteKey = vehicle.catEffectiveRouteKey || getEffectiveCatRouteKey(vehicle);
              const shouldDisplay = isCatRouteVisible(effectiveRouteKey);
              let marker = catVehicleMarkers.get(markerKey);
              if (!shouldDisplay) {
                  if (marker) {
                      if (layerGroup.hasLayer(marker)) {
                          layerGroup.removeLayer(marker);
                      }
                      if (typeof marker.remove === 'function') {
                          marker.remove();
                      }
                      catVehicleMarkers.delete(markerKey);
                  }
                  removeNameBubbleForKey(markerKey);
                  return;
              }
              seen.add(markerKey);
              const icon = buildCatVehicleIcon(vehicle, effectiveRouteKey);
              if (!marker) {
                  marker = L.marker(newPosition, { icon, pane: catVehiclesPaneName, keyboard: false });
                  marker.addTo(layerGroup);
                  catVehicleMarkers.set(markerKey, marker);
              } else {
                  animateMarkerTo(marker, newPosition);
                  marker.setIcon(icon);
                  if (!layerGroup.hasLayer(marker)) {
                      layerGroup.addLayer(marker);
                  }
              }
              if (typeof marker.unbindTooltip === 'function') {
                  marker.unbindTooltip();
              }
              const existingMarkerEtas = Array.isArray(marker.catVehicleData?.etas)
                  ? marker.catVehicleData.etas.slice()
                  : [];
              const incomingEtas = Array.isArray(vehicle.etas) ? vehicle.etas.slice() : [];
              const combinedEtas = incomingEtas.length ? incomingEtas : existingMarkerEtas;
              marker.catEffectiveRouteKey = effectiveRouteKey;
              marker.catVehicleData = Object.assign({}, vehicle, {
                  etas: combinedEtas
              });
              if (!marker._catVehicleClickHandlerAttached) {
                  marker.on('click', handleCatVehicleMarkerClick);
                  marker._catVehicleClickHandlerAttached = true;
              }
              if (catActiveVehicleTooltip && catActiveVehicleTooltip.marker === marker && !marker._catTooltipLoading) {
                  openCatVehicleTooltip(marker);
              }

              const routeColor = sanitizeCssColor(getCatRouteColor(effectiveRouteKey)) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
              const headingDeg = normalizeHeadingDegrees(Number(vehicle.heading));
              const groundSpeed = Number(vehicle.speed);
              const bubbleKey = markerKey;

              if (adminMode && displayMode === DISPLAY_MODES.SPEED && !kioskMode) {
                  const speedIcon = createSpeedBubbleDivIcon(routeColor, groundSpeed, markerMetricsForZoom.scale, headingDeg);
                  if (speedIcon) {
                      nameBubbles[bubbleKey] = nameBubbles[bubbleKey] || {};
                      if (nameBubbles[bubbleKey].speedMarker) {
                          animateMarkerTo(nameBubbles[bubbleKey].speedMarker, newPosition);
                          nameBubbles[bubbleKey].speedMarker.setIcon(speedIcon);
                      } else if (map) {
                          nameBubbles[bubbleKey].speedMarker = L.marker(newPosition, { icon: speedIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                      }
                  } else if (nameBubbles[bubbleKey] && nameBubbles[bubbleKey].speedMarker) {
                      if (map && typeof map.removeLayer === 'function') {
                          map.removeLayer(nameBubbles[bubbleKey].speedMarker);
                      }
                      delete nameBubbles[bubbleKey].speedMarker;
                  }
              } else if (nameBubbles[bubbleKey] && nameBubbles[bubbleKey].speedMarker) {
                  if (map && typeof map.removeLayer === 'function') {
                      map.removeLayer(nameBubbles[bubbleKey].speedMarker);
                  }
                  delete nameBubbles[bubbleKey].speedMarker;
              }

              if (adminMode && !kioskMode) {
                  const labelText = toNonEmptyString(vehicle.equipmentId)
                      || toNonEmptyString(vehicle.displayName)
                      || toNonEmptyString(vehicle.id);
                  const nameIcon = labelText
                      ? createNameBubbleDivIcon(labelText, routeColor, markerMetricsForZoom.scale, headingDeg)
                      : null;
                  if (nameIcon) {
                      nameBubbles[bubbleKey] = nameBubbles[bubbleKey] || {};
                      if (nameBubbles[bubbleKey].nameMarker) {
                          animateMarkerTo(nameBubbles[bubbleKey].nameMarker, newPosition);
                          nameBubbles[bubbleKey].nameMarker.setIcon(nameIcon);
                      } else if (map) {
                          nameBubbles[bubbleKey].nameMarker = L.marker(newPosition, { icon: nameIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                      }
                  } else if (nameBubbles[bubbleKey] && nameBubbles[bubbleKey].nameMarker) {
                      if (map && typeof map.removeLayer === 'function') {
                          map.removeLayer(nameBubbles[bubbleKey].nameMarker);
                      }
                      delete nameBubbles[bubbleKey].nameMarker;
                  }
              } else {
                  if (nameBubbles[bubbleKey] && nameBubbles[bubbleKey].nameMarker) {
                      if (map && typeof map.removeLayer === 'function') {
                          map.removeLayer(nameBubbles[bubbleKey].nameMarker);
                      }
                      delete nameBubbles[bubbleKey].nameMarker;
                  }
              }

              const rawRouteIdForLabel = toNonEmptyString(vehicle.routeId);
              const fallbackRouteId = rawRouteIdForLabel !== '' ? rawRouteIdForLabel : effectiveRouteKey;
              const routeLabel = formatCatRouteBubbleLabel(fallbackRouteId);
              const routeIcon = routeLabel
                  ? createBlockBubbleDivIcon(routeLabel, routeColor, markerMetricsForZoom.scale, headingDeg)
                  : null;
              if (routeIcon) {
                  nameBubbles[bubbleKey] = nameBubbles[bubbleKey] || {};
                  const bubbleEntry = nameBubbles[bubbleKey];
                  if (bubbleEntry.catRouteMarker) {
                      animateMarkerTo(bubbleEntry.catRouteMarker, newPosition);
                      bubbleEntry.catRouteMarker.setIcon(routeIcon);
                  } else if (map) {
                      const marker = L.marker(newPosition, { icon: routeIcon, interactive: false, pane: 'busesPane' }).addTo(map);
                      marker._isCatRouteLabel = true;
                      bubbleEntry.catRouteMarker = marker;
                  }
              } else if (nameBubbles[bubbleKey] && nameBubbles[bubbleKey].catRouteMarker) {
                  removeCatRouteMarkerForBubble(bubbleKey, nameBubbles[bubbleKey]);
              }

              if (nameBubbles[bubbleKey]) {
                  const bubbleEntry = nameBubbles[bubbleKey];
                  const hasMarkers = Boolean(bubbleEntry.speedMarker || bubbleEntry.nameMarker || bubbleEntry.blockMarker || bubbleEntry.routeMarker || bubbleEntry.catRouteMarker);
                  if (hasMarkers) {
                      bubbleEntry.lastScale = markerMetricsForZoom.scale;
                  } else {
                      delete nameBubbles[bubbleKey];
                  }
              }
          });
          catVehicleMarkers.forEach((marker, key) => {
              if (!seen.has(key)) {
                  if (catLayerGroup && catLayerGroup.hasLayer(marker)) {
                      catLayerGroup.removeLayer(marker);
                  }
                  if (marker && typeof marker.remove === 'function') {
                      marker.remove();
                  }
                  if (catActiveVehicleTooltip && catActiveVehicleTooltip.marker === marker) {
                      closeCatVehicleTooltip();
                  }
                  catVehicleMarkers.delete(key);
                  removeNameBubbleForKey(key);
              }
          });
      }

      function removeCatRouteLayer(layer) {
          if (!layer) {
              return;
          }
          if (catLayerGroup && catLayerGroup.hasLayer(layer)) {
              catLayerGroup.removeLayer(layer);
          }
          if (typeof layer.remove === 'function') {
              layer.remove();
          }
      }

      function clearCatRouteLayers() {
          catRoutePatternLayers.forEach(layer => {
              removeCatRouteLayer(layer);
          });
          catRoutePatternLayers.clear();
      }

      function renderCatRoutes() {
          if (!catOverlayEnabled) {
              clearCatRouteLayers();
              if (enableOverlapDashRendering && overlapRenderer) {
                  updateOverlapRendererWithCatRoutes();
              }
              return;
          }
          if (enableOverlapDashRendering && overlapRenderer) {
              clearCatRouteLayers();
              updateOverlapRendererWithCatRoutes();
              return;
          }
          const layerGroup = ensureCatLayerGroup();
          if (!layerGroup || !map) {
              return;
          }
          const zoom = typeof map?.getZoom === 'function' ? map.getZoom() : null;
          const strokeWeight = computeRouteStrokeWeight(zoom);
          const seenKeys = new Set();

          catRoutePatternGeometries.forEach((geometry, key) => {
              if (!geometry || !Array.isArray(geometry.latLngs) || geometry.latLngs.length < 2) {
                  const existingLayer = catRoutePatternLayers.get(key);
                  if (existingLayer) {
                      removeCatRouteLayer(existingLayer);
                      catRoutePatternLayers.delete(key);
                  }
                  return;
              }
              const routeKey = geometry.routeKey;
              if (!isCatRouteVisible(routeKey)) {
                  const existingLayer = catRoutePatternLayers.get(key);
                  if (existingLayer) {
                      removeCatRouteLayer(existingLayer);
                      catRoutePatternLayers.delete(key);
                  }
                  return;
              }
              const color = sanitizeCssColor(geometry.color) || sanitizeCssColor(getCatRouteColor(routeKey)) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
              const options = {
                  color,
                  weight: strokeWeight,
                  opacity: 1,
                  lineCap: 'round',
                  lineJoin: 'round'
              };
              let layer = catRoutePatternLayers.get(key);
              if (!layer) {
                  layer = L.polyline(geometry.latLngs, mergeRouteLayerOptions(options));
                  layer.addTo(layerGroup);
                  catRoutePatternLayers.set(key, layer);
              } else {
                  if (typeof layer.setLatLngs === 'function') {
                      layer.setLatLngs(geometry.latLngs);
                  }
                  if (typeof layer.setStyle === 'function') {
                      layer.setStyle(options);
                  }
                  if (!layerGroup.hasLayer(layer)) {
                      layerGroup.addLayer(layer);
                  }
              }
              seenKeys.add(key);
          });

          catRoutePatternLayers.forEach((layer, key) => {
              if (!seenKeys.has(key)) {
                  removeCatRouteLayer(layer);
                  catRoutePatternLayers.delete(key);
              }
          });
      }

      function clearCatVehicleMarkers() {
          closeCatVehicleTooltip();
          catVehicleMarkers.forEach(marker => {
              if (catLayerGroup) {
                  catLayerGroup.removeLayer(marker);
              }
              if (marker && typeof marker.remove === 'function') {
                  marker.remove();
              }
          });
          catVehicleMarkers.clear();
          catVehiclesById.clear();
          catActiveRouteKeys = new Set();
          catVehicleEtaCache.clear();
          Object.keys(nameBubbles).forEach(key => {
              if (typeof key === 'string' && key.startsWith('cat-')) {
                  removeNameBubbleForKey(key);
              }
          });
          clearCatRouteLayers();
      }

      function updateCatVehicleMarkers(vehicles) {
          if (!catOverlayEnabled) {
              return;
          }
          updateCatVehicleCache(vehicles);
          renderCatVehiclesUsingCache();
          updateRouteSelector(activeRoutes);
      }

      function updateOverlapRendererWithCatRoutes() {
          if (!enableOverlapDashRendering || !overlapRenderer) {
              return;
          }
          const baseGeometries = currentTranslocRendererGeometries instanceof Map
              ? new Map(currentTranslocRendererGeometries)
              : new Map();
          const baseRouteIds = Array.isArray(currentTranslocSelectedRouteIds)
              ? currentTranslocSelectedRouteIds.slice()
              : [];
          const catData = buildCatOverlapRendererData();
          catData.geometries.forEach((latLngs, routeId) => {
              baseGeometries.set(routeId, latLngs);
          });
          const combinedRouteIds = mergeNumericRouteIds(baseRouteIds, catData.routeIds);
          const selectionKey = combinedRouteIds.join('|');
          const colorSignature = combinedRouteIds.map(id => `${id}:${getRouteColor(id)}`).join('|');
          const geometrySignature = combinedRouteIds
              .map(id => `${id}:${getRouteGeometrySignature(id, baseGeometries)}`)
              .join('|');
          const rendererFlag = true;
          const shouldUpdate = routeLayers.length === 0
              || !lastRouteRenderState.useOverlapRenderer
              || lastRouteRenderState.selectionKey !== selectionKey
              || lastRouteRenderState.colorSignature !== colorSignature
              || lastRouteRenderState.geometrySignature !== geometrySignature;
          if (!shouldUpdate) {
              return;
          }
          routeLayers.forEach(layer => {
              if (layer && map && typeof map.hasLayer === 'function' && map.hasLayer(layer)) {
                  map.removeLayer(layer);
              }
          });
          const layers = overlapRenderer.updateRoutes(baseGeometries, combinedRouteIds);
          routeLayers = layers;
          lastRouteRenderState = {
              selectionKey,
              colorSignature,
              geometrySignature,
              useOverlapRenderer: rendererFlag
          };
      }

      async function fetchCatVehicles() {
          if (!catOverlayEnabled) {
              return [];
          }
          try {
              const response = await fetch(CAT_VEHICLES_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              const vehicles = normalizeCatVehicles(payload);
              updateCatVehicleMarkers(vehicles);
              return vehicles;
          } catch (error) {
              console.error('Failed to fetch CAT vehicles:', error);
              return [];
          }
      }

      function normalizeCatServiceAlert(entry) {
          if (!entry || typeof entry !== 'object') {
              return null;
          }
          const id = toNonEmptyString(getFirstDefined(entry, ['ID', 'Id', 'id', 'AlertID', 'alertID', 'alertId', 'Guid', 'guid']));
          const title = toNonEmptyString(getFirstDefined(entry, ['Title', 'title', 'Name', 'name', 'Headline', 'headline'])) || 'Service Alert';
          const message = toNonEmptyString(getFirstDefined(entry, ['Message', 'message', 'Description', 'description', 'Details', 'details', 'Text', 'text']));
          const routesRaw = getFirstDefined(entry, ['Routes', 'routes', 'Route', 'route', 'RouteNames', 'routeNames']);
          let routes = [];
          if (Array.isArray(routesRaw)) {
              routes = routesRaw.map(value => toNonEmptyString(value)).filter(Boolean);
          } else if (typeof routesRaw === 'string') {
              routes = routesRaw.split(/[,;]+/).map(part => part.trim()).filter(Boolean);
          }
          const startRaw = toNonEmptyString(getFirstDefined(entry, ['StartDate', 'startDate', 'Start', 'start', 'Effective', 'effective', 'EffectiveDate', 'effectiveDate']));
          const endRaw = toNonEmptyString(getFirstDefined(entry, ['EndDate', 'endDate', 'End', 'end', 'Expiration', 'expiration', 'Expires', 'expires']));
          const isActiveField = getFirstDefined(entry, ['IsActive', 'isActive', 'Active', 'active', 'Status', 'status']);
          let isActive = undefined;
          if (typeof isActiveField === 'string') {
              isActive = !/^false$/i.test(isActiveField) && !/^inactive$/i.test(isActiveField);
          } else if (typeof isActiveField === 'boolean') {
              isActive = isActiveField;
          } else if (typeof isActiveField === 'number') {
              isActive = isActiveField !== 0;
          }
          if (isActive === undefined && endRaw) {
              const endTime = Date.parse(endRaw);
              if (Number.isFinite(endTime)) {
                  isActive = endTime > Date.now();
              }
          }
          return {
              id,
              title,
              message,
              routes,
              startDisplay: startRaw || '',
              startRaw,
              endDisplay: endRaw || '',
              endRaw,
              isActive: isActive !== undefined ? !!isActive : true
          };
      }

      function normalizeCatServiceAlerts(root) {
          const entries = extractCatArray(root, ['announcements', 'Announcements', 'alerts', 'Alerts']);
          const alerts = [];
          const processEntry = (entry) => {
              if (!entry || typeof entry !== 'object') {
                  return;
              }
              const nested = extractCatArray(entry, ['announcements', 'Announcements', 'alerts', 'Alerts']);
              if (Array.isArray(nested) && nested.length > 0) {
                  nested.forEach(processEntry);
                  return;
              }
              const normalized = normalizeCatServiceAlert(entry);
              if (normalized) {
                  alerts.push(normalized);
              }
          };
          entries.forEach(processEntry);
          return alerts;
      }

      async function fetchCatServiceAlerts() {
          if (!catOverlayEnabled) {
              return [];
          }
          if (catServiceAlertsFetchPromise) {
              return catServiceAlertsFetchPromise;
          }
          const now = Date.now();
          if (catServiceAlerts.length > 0 && (now - catServiceAlertsLastFetchTime) < CAT_SERVICE_ALERT_REFRESH_INTERVAL_MS) {
              return catServiceAlerts;
          }
          catServiceAlertsLoading = true;
          catServiceAlertsError = null;
          refreshServiceAlertsUI();
          const requestPromise = (async () => {
              const response = await fetch(CAT_SERVICE_ALERTS_ENDPOINT, { cache: 'no-store' });
              if (!response.ok) {
                  throw new Error(`HTTP ${response.status}`);
              }
              const payload = await response.json();
              return normalizeCatServiceAlerts(payload);
          })();
          catServiceAlertsFetchPromise = requestPromise;
          try {
              const alerts = await requestPromise;
              if (!catOverlayEnabled) {
                  return alerts;
              }
              catServiceAlerts = alerts;
              catServiceAlertsError = null;
              catServiceAlertsLoading = false;
              catServiceAlertsLastFetchTime = Date.now();
              refreshServiceAlertsUI();
              return alerts;
          } catch (error) {
              console.error('Failed to fetch CAT service alerts:', error);
              if (catOverlayEnabled) {
                  catServiceAlerts = [];
                  catServiceAlertsError = CAT_SERVICE_ALERT_UNAVAILABLE_MESSAGE;
                  catServiceAlertsLoading = false;
                  catServiceAlertsLastFetchTime = Date.now();
                  refreshServiceAlertsUI();
              }
              return [];
          } finally {
              if (catServiceAlertsFetchPromise === requestPromise) {
                  catServiceAlertsFetchPromise = null;
              }
          }
      }

      function updateCatToggleButtonState() {
          const button = document.getElementById('catToggleButton');
          if (!button) {
              return;
          }
          button.classList.toggle('is-active', !!catOverlayEnabled);
          button.setAttribute('aria-pressed', catOverlayEnabled ? 'true' : 'false');
          const indicator = button.querySelector('.toggle-indicator');
          if (indicator) {
              indicator.textContent = catOverlayEnabled ? 'On' : 'Off';
          }
      }

      function ensureBusMarkerState(vehicleID) {
          if (!busMarkerStates[vehicleID]) {
              const defaultRouteColor = BUS_MARKER_DEFAULT_ROUTE_COLOR;
              const cachedHeading = getCachedVehicleHeading(vehicleID);
              busMarkerStates[vehicleID] = {
                  vehicleID,
                  positionHistory: [],
                  headingDeg: Number.isFinite(cachedHeading) ? cachedHeading : BUS_MARKER_DEFAULT_HEADING,
                  fillColor: defaultRouteColor,
                  glyphColor: computeBusMarkerGlyphColor(defaultRouteColor),
                  accessibleLabel: '',
                  isStale: false,
                  isStopped: false,
                  isSelected: false,
                  isHovered: false,
                  lastUpdateTimestamp: 0,
                  size: null,
                  elements: null,
                  marker: null,
                  markerEventsBound: false
              };
          }
          return busMarkerStates[vehicleID];
      }

      function setBusMarkerSize(state, metrics) {
          if (!state || !metrics) {
              return;
          }
          state.size = {
              widthPx: metrics.widthPx,
              heightPx: metrics.heightPx,
              scale: metrics.scale
          };
      }

      function computeBusMarkerGlyphColor(routeColor) {
          if (typeof busMarkerContrastOverrideColor === 'string' && busMarkerContrastOverrideColor.trim().length > 0) {
              return busMarkerContrastOverrideColor;
          }
          const fallback = BUS_MARKER_DEFAULT_CONTRAST_COLOR;
          const candidate = typeof routeColor === 'string' && routeColor.trim().length > 0 ? routeColor : BUS_MARKER_DEFAULT_ROUTE_COLOR;
          const contrast = contrastBW(candidate);
          return contrast || fallback;
      }

      function normalizeRouteColor(color) {
          if (typeof color === 'string') {
              const trimmed = color.trim();
              if (trimmed.length > 0) {
                  return trimmed;
              }
          } else if (color !== undefined && color !== null) {
              const stringValue = `${color}`.trim();
              if (stringValue.length > 0) {
                  return stringValue;
              }
          }
          return BUS_MARKER_DEFAULT_ROUTE_COLOR;
      }

      function normalizeGlyphColor(color, routeColor) {
          if (typeof color === 'string') {
              const trimmed = color.trim();
              if (trimmed.length > 0) {
                  return trimmed;
              }
          } else if (color !== undefined && color !== null) {
              const stringValue = `${color}`.trim();
              if (stringValue.length > 0) {
                  return stringValue;
              }
          }
          const fallbackRouteColor = normalizeRouteColor(routeColor);
          return computeBusMarkerGlyphColor(fallbackRouteColor);
      }

      function applyColorsToBusMarkerSvg(svgEl, routeColor, glyphColor) {
          if (!svgEl) {
              return;
          }
          const fillColor = normalizeRouteColor(routeColor);
          const contrastColor = normalizeGlyphColor(glyphColor, fillColor);
          const routeShape = svgEl.querySelector('#route_color');
          const centerRing = svgEl.querySelector(`#${BUS_MARKER_CENTER_RING_ID}`);
          const centerSquare = svgEl.querySelector(`#${BUS_MARKER_STOPPED_SQUARE_ID}`);
          const heading = svgEl.querySelector('#heading');
          if (routeShape) {
              routeShape.setAttribute('fill', fillColor);
              routeShape.style.fill = fillColor;
          }
          if (centerRing) {
              centerRing.setAttribute('fill', contrastColor);
              centerRing.style.fill = contrastColor;
          }
          if (centerSquare) {
              centerSquare.setAttribute('fill', contrastColor);
              centerSquare.style.fill = contrastColor;
          }
          if (heading) {
              heading.setAttribute('fill', contrastColor);
              heading.style.fill = contrastColor;
          }
      }

      function updateBusMarkerColorElements(state) {
          if (!state) {
              return;
          }
          const normalizedFill = normalizeRouteColor(state.fillColor);
          const normalizedGlyph = normalizeGlyphColor(state.glyphColor, normalizedFill);
          state.fillColor = normalizedFill;
          state.glyphColor = normalizedGlyph;
          if (state.elements?.routeColor) {
              state.elements.routeColor.setAttribute('fill', normalizedFill);
              state.elements.routeColor.style.fill = normalizedFill;
          }
          if (state.elements?.centerRing) {
              state.elements.centerRing.setAttribute('fill', normalizedGlyph);
              state.elements.centerRing.style.fill = normalizedGlyph;
          }
          if (state.elements?.centerSquare) {
              state.elements.centerSquare.setAttribute('fill', normalizedGlyph);
              state.elements.centerSquare.style.fill = normalizedGlyph;
          }
          if (state.elements?.heading) {
              state.elements.heading.setAttribute('fill', normalizedGlyph);
              state.elements.heading.style.fill = normalizedGlyph;
          }
      }

      function ensureCenterSquareElement(svgEl) {
          if (!svgEl) {
              return null;
          }
          let square = svgEl.querySelector(`#${BUS_MARKER_STOPPED_SQUARE_ID}`);
          if (square) {
              return square;
          }
          const namespace = 'http://www.w3.org/2000/svg';
          square = document.createElementNS(namespace, 'rect');
          square.setAttribute('id', BUS_MARKER_STOPPED_SQUARE_ID);
          square.setAttribute('width', `${BUS_MARKER_STOPPED_SQUARE_SIZE_PX}`);
          square.setAttribute('height', `${BUS_MARKER_STOPPED_SQUARE_SIZE_PX}`);
          const halfSize = BUS_MARKER_STOPPED_SQUARE_SIZE_PX / 2;
          const x = BUS_MARKER_CENTER_RING_CENTER_X - halfSize;
          const y = BUS_MARKER_CENTER_RING_CENTER_Y - halfSize;
          square.setAttribute('x', x.toFixed(2));
          square.setAttribute('y', y.toFixed(2));
          square.style.display = 'none';
          square.style.pointerEvents = 'none';
          const centerRing = svgEl.querySelector(`#${BUS_MARKER_CENTER_RING_ID}`);
          const centerRingClass = centerRing?.getAttribute('class');
          if (centerRingClass) {
              square.setAttribute('class', centerRingClass);
          }
          const heading = svgEl.querySelector('#heading');
          if (heading && heading.parentNode) {
              heading.parentNode.insertBefore(square, heading);
          } else if (centerRing && centerRing.parentNode) {
              centerRing.parentNode.insertBefore(square, centerRing.nextSibling);
          } else {
              svgEl.appendChild(square);
          }
          return square;
      }

      function setCenterShapeDisplay(centerRing, centerSquare, isStopped) {
          const showSquare = Boolean(isStopped);
          if (centerRing) {
              centerRing.style.display = showSquare ? 'none' : 'inline';
          }
          if (centerSquare) {
              centerSquare.style.display = showSquare ? 'inline' : 'none';
          }
      }

      function applyStoppedVisualStateToSvg(svgEl, isStopped) {
          if (!svgEl) {
              return;
          }
          const centerSquare = ensureCenterSquareElement(svgEl);
          const centerRing = svgEl.querySelector(`#${BUS_MARKER_CENTER_RING_ID}`);
          setCenterShapeDisplay(centerRing, centerSquare, isStopped);
      }

      function ensureBusMarkerStoppedElements(state) {
          if (!state?.elements?.svg) {
              return;
          }
          const square = ensureCenterSquareElement(state.elements.svg);
          if (square) {
              state.elements.centerSquare = square;
          }
          if (!state.elements.centerRing || !state.elements.centerRing.isConnected) {
              const ring = state.elements.svg.querySelector(`#${BUS_MARKER_CENTER_RING_ID}`);
              if (ring) {
                  state.elements.centerRing = ring;
              }
          }
      }

      function applyBusMarkerStoppedVisualState(state) {
          if (!state?.elements?.svg) {
              return;
          }
          ensureBusMarkerStoppedElements(state);
          setCenterShapeDisplay(state.elements.centerRing, state.elements.centerSquare, state.isStopped);
      }

      function setBusMarkerContrastOverrideColor(color) {
          if (typeof color === 'string' && color.trim().length > 0) {
              busMarkerContrastOverrideColor = color.trim();
          } else {
              busMarkerContrastOverrideColor = null;
          }
          Object.keys(busMarkerStates).forEach(vehicleID => {
              const state = busMarkerStates[vehicleID];
              if (!state) {
                  return;
              }
              const routeColor = state.fillColor || BUS_MARKER_DEFAULT_ROUTE_COLOR;
              const glyphColor = computeBusMarkerGlyphColor(routeColor);
              state.glyphColor = glyphColor;
              queueBusMarkerVisualUpdate(vehicleID, { glyphColor });
          });
      }

      if (typeof window !== 'undefined') {
          window.setBusMarkerContrastOverrideColor = setBusMarkerContrastOverrideColor;
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

          if (!BUS_MARKER_SVG_TEXT || typeof document === 'undefined') {
              return fallbackExtents;
          }

          try {
              const template = document.createElement('template');
              template.innerHTML = BUS_MARKER_SVG_TEXT.trim();
              const svgEl = template.content.firstElementChild;
              if (!svgEl) {
                  throw new Error('Failed to parse bus marker SVG for bounds computation.');
              }
              const clone = svgEl.cloneNode(true);

              clone.querySelectorAll('rect').forEach(rect => {
                  const width = Number(rect.getAttribute('width'));
                  const height = Number(rect.getAttribute('height'));
                  if (width === 0 && height === 0) {
                      rect.remove();
                  }
              });

              clone.querySelectorAll('circle').forEach(circle => {
                  const radius = Number(circle.getAttribute('r'));
                  if (radius === 0) {
                      circle.remove();
                  }
              });

              clone.setAttribute('width', `${BUS_MARKER_VIEWBOX_WIDTH}`);
              clone.setAttribute('height', `${BUS_MARKER_VIEWBOX_HEIGHT}`);
              clone.style.position = 'absolute';
              clone.style.visibility = 'hidden';
              clone.style.pointerEvents = 'none';
              clone.style.left = '-9999px';
              clone.style.top = '-9999px';

              const host = document.body || document.documentElement;
              if (!host) {
                  throw new Error('Document does not have an attachable host element for bounds computation.');
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
                      bottom: (bbox.y + bbox.height) - BUS_MARKER_PIVOT_Y,
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

      function createSpeedBubbleDivIcon(routeColor, groundSpeed, scale, headingDeg) {
          if (!Number.isFinite(groundSpeed)) {
              return null;
          }
          const safeScale = Number.isFinite(scale) && scale > 0 ? scale : 1;
          const fillColor = typeof routeColor === 'string' && routeColor.trim().length > 0
              ? routeColor
              : BUS_MARKER_DEFAULT_ROUTE_COLOR;
          const textColor = computeBusMarkerGlyphColor(fillColor);
          const normalizedSpeed = Math.max(0, Math.round(groundSpeed));
          const label = `${normalizedSpeed} MPH`;
          const fontSize = Math.max(BUS_MARKER_LABEL_MIN_FONT_PX, SPEED_BUBBLE_BASE_FONT_PX * safeScale);
          const horizontalPadding = SPEED_BUBBLE_HORIZONTAL_PADDING * safeScale;
          const verticalPadding = SPEED_BUBBLE_VERTICAL_PADDING * safeScale;
          const textWidth = measureLabelTextWidth(label, fontSize);
          const width = Math.max(SPEED_BUBBLE_MIN_WIDTH * safeScale, textWidth + horizontalPadding * 2);
          const height = Math.max(SPEED_BUBBLE_MIN_HEIGHT * safeScale, fontSize + verticalPadding * 2);
          const radius = SPEED_BUBBLE_CORNER_RADIUS * safeScale;
          const strokeWidth = Math.max(1, LABEL_BASE_STROKE_WIDTH * safeScale);
          const svgWidth = roundToTwoDecimals(width);
          const svgHeight = roundToTwoDecimals(height);
          const radiusRounded = roundToTwoDecimals(radius);
          const strokeWidthRounded = roundToTwoDecimals(strokeWidth);
          const textX = roundToTwoDecimals(svgWidth / 2);
          const baselineShift = fontSize * LABEL_TEXT_VERTICAL_ADJUSTMENT_RATIO;
          const textY = roundToTwoDecimals(svgHeight / 2 + baselineShift);
          const anchorX = roundToTwoDecimals(svgWidth / 2);
          const leaderOffset = roundToTwoDecimals(computeLabelLeaderOffset(safeScale, headingDeg, 'below'));
          const anchorY = -leaderOffset;
          const svg = `
              <svg width="${svgWidth}" height="${svgHeight}" viewBox="0 0 ${svgWidth} ${svgHeight}" xmlns="http://www.w3.org/2000/svg" style="pointer-events: none;">
                  <g>
                      <rect x="0" y="0" width="${svgWidth}" height="${svgHeight}" rx="${radiusRounded}" ry="${radiusRounded}" fill="${fillColor}" stroke="white" stroke-width="${strokeWidthRounded}" />
                      <text x="${textX}" y="${textY}" dominant-baseline="middle" alignment-baseline="middle" text-anchor="middle" font-size="${roundToTwoDecimals(fontSize)}" font-weight="bold" fill="${textColor}" font-family="${BUS_MARKER_LABEL_FONT_FAMILY}">${escapeHtml(label)}</text>
                  </g>
              </svg>`;
          return L.divIcon({
              html: svg,
              className: 'leaflet-div-icon bus-label-icon',
              iconSize: [svgWidth, svgHeight],
              iconAnchor: [anchorX, anchorY]
          });
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

      function updateBusMarkerHeading(state, newPosition, fallbackHeading, groundSpeedMph) {
          if (!state) {
              return BUS_MARKER_DEFAULT_HEADING;
          }
          const lat = Array.isArray(newPosition) ? Number(newPosition[0]) : Number(newPosition?.lat ?? newPosition?.Latitude);
          const lng = Array.isArray(newPosition) ? Number(newPosition[1]) : Number(newPosition?.lng ?? newPosition?.Longitude);
          if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
              return state.headingDeg ?? BUS_MARKER_DEFAULT_HEADING;
          }
          const current = L.latLng(lat, lng);
          const history = Array.isArray(state.positionHistory) ? state.positionHistory : [];
          const previous = history.length > 0 ? history[history.length - 1] : null;
          let heading = Number.isFinite(state.headingDeg) ? state.headingDeg : BUS_MARKER_DEFAULT_HEADING;
          const sanitizedSpeedMph = Number.isFinite(groundSpeedMph) ? Math.max(0, groundSpeedMph) : null;
          const speedMetersPerSecond = sanitizedSpeedMph === null ? null : sanitizedSpeedMph * METERS_PER_SECOND_PER_MPH;
          const hasSufficientSpeed = speedMetersPerSecond === null || speedMetersPerSecond >= MIN_HEADING_SPEED_METERS_PER_SECOND;
          if (previous) {
              const distance = previous.distanceTo(current);
              const shouldUpdateHeading = distance >= MIN_HEADING_DISTANCE_METERS && hasSufficientSpeed;
              if (shouldUpdateHeading) {
                  const computed = computeBearingDegrees(previous, current);
                  if (Number.isFinite(computed)) {
                      heading = computed;
                  }
              } else if (!Number.isFinite(heading)) {
                  const fallback = Number.isFinite(fallbackHeading) ? fallbackHeading : BUS_MARKER_DEFAULT_HEADING;
                  heading = fallback;
              }
              if (distance >= MIN_POSITION_UPDATE_METERS) {
                  history.push(current);
                  if (history.length > 2) {
                      history.shift();
                  }
              } else {
                  history[history.length - 1] = current;
              }
          } else {
              const fallback = Number.isFinite(fallbackHeading) ? fallbackHeading : heading;
              heading = Number.isFinite(fallback) ? fallback : BUS_MARKER_DEFAULT_HEADING;
              history.push(current);
          }
          state.positionHistory = history;
          state.headingDeg = normalizeHeadingDegrees(heading);
          return state.headingDeg;
      }

      function computeBearingDegrees(fromLatLng, toLatLng) {
          if (!fromLatLng || !toLatLng) {
              return BUS_MARKER_DEFAULT_HEADING;
          }
          const lat1 = fromLatLng.lat * Math.PI / 180;
          const lat2 = toLatLng.lat * Math.PI / 180;
          const dLon = (toLatLng.lng - fromLatLng.lng) * Math.PI / 180;
          const y = Math.sin(dLon) * Math.cos(lat2);
          const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
          const theta = Math.atan2(y, x);
          const bearing = theta * 180 / Math.PI;
          return normalizeHeadingDegrees(bearing);
      }

      function normalizeHeadingDegrees(degrees) {
          const normalized = Number.isFinite(degrees) ? degrees : BUS_MARKER_DEFAULT_HEADING;
          return ((normalized % 360) + 360) % 360;
      }

      function setMarkerSvgRotation(svgElement, rotationDeg) {
          if (!svgElement) {
              return;
          }
          const normalizedRotation = normalizeHeadingDegrees(
              Number.isFinite(rotationDeg) ? rotationDeg : BUS_MARKER_DEFAULT_HEADING
          );
          svgElement.style.transformOrigin = BUS_MARKER_TRANSFORM_ORIGIN;
          svgElement.style.transformBox = 'fill-box';
          svgElement.style.transform = `rotate(${normalizedRotation.toFixed(2)}deg)`;
      }

      function buildBusMarkerAccessibleLabel(busName, headingDeg, groundSpeed) {
          const name = busName && `${busName}`.trim().length > 0 ? `${busName}`.trim() : 'Vehicle';
          const direction = bearingToDirection(headingDeg);
          const speedText = formatBusSpeed(groundSpeed);
          return `${name} — ${direction} — ${speedText}`;
      }

      function isBusConsideredStopped(groundSpeed) {
          if (!Number.isFinite(groundSpeed)) {
              return false;
          }
          const speed = Math.max(0, Math.round(groundSpeed));
          return speed <= 1;
      }

      function bearingToDirection(headingDeg) {
          if (!Number.isFinite(headingDeg)) {
              return 'Unknown direction';
          }
          const compass = [
              'Northbound',
              'Northeastbound',
              'Eastbound',
              'Southeastbound',
              'Southbound',
              'Southwestbound',
              'Westbound',
              'Northwestbound'
          ];
          const normalized = normalizeHeadingDegrees(headingDeg);
          const index = Math.round(normalized / 45) % compass.length;
          return compass[index];
      }

      function formatBusSpeed(groundSpeed) {
          if (!Number.isFinite(groundSpeed)) {
              return 'Speed unavailable';
          }
          const speed = Math.max(0, Math.round(groundSpeed));
          if (speed <= 1) {
              return 'Stopped';
          }
          return `${speed} mph`;
      }

      function escapeHtml(value) {
          if (value === null || value === undefined) {
              return '';
          }
          return `${value}`
              .replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;')
              .replace(/'/g, '&#39;');
      }

      function contrastBW(hex) {
          if (typeof hex !== 'string' || hex.trim().length === 0) {
              return '#FFFFFF';
          }
          let normalized = hex.trim().replace(/^#/, '');
          if (normalized.length === 3) {
              normalized = normalized.split('').map(ch => ch + ch).join('');
          }
          if (normalized.length !== 6 || /[^0-9a-fA-F]/.test(normalized)) {
              return '#FFFFFF';
          }
          const r = parseInt(normalized.substring(0, 2), 16) / 255;
          const g = parseInt(normalized.substring(2, 4), 16) / 255;
          const b = parseInt(normalized.substring(4, 6), 16) / 255;
          const L = 0.2126 * r + 0.7152 * g + 0.0722 * b;
          return L > 0.55 ? '#000000' : '#FFFFFF';
      }

      async function loadBusSVG() {
          if (BUS_MARKER_SVG_TEXT) {
              return BUS_MARKER_SVG_TEXT;
          }
          if (!BUS_MARKER_SVG_LOAD_PROMISE) {
              BUS_MARKER_SVG_LOAD_PROMISE = fetch(BUS_MARKER_SVG_URL)
                  .then(response => {
                      if (!response.ok) {
                          throw new Error(`Failed to load bus marker SVG: ${response.status} ${response.statusText}`);
                      }
                      return response.text();
                  })
                  .then(text => {
                      const template = document.createElement('template');
                      template.innerHTML = text.trim();
                      const parsedSvg = template.content.firstElementChild;
                      if (!parsedSvg || parsedSvg.tagName.toLowerCase() !== 'svg') {
                          throw new Error('Loaded bus marker asset is not a valid SVG.');
                      }
                      BUS_MARKER_SVG_TEXT = parsedSvg.outerHTML;
                      busMarkerVisibleExtents = null;
                      BUS_MARKER_SVG_LOAD_PROMISE = null;
                      return BUS_MARKER_SVG_TEXT;
                  })
                  .catch(error => {
                      BUS_MARKER_SVG_LOAD_PROMISE = null;
                      throw error;
                  });
          }
          return BUS_MARKER_SVG_LOAD_PROMISE;
      }

      function buildBusMarkerDivIconSync(vehicleID, state) {
          if (!state || !BUS_MARKER_SVG_TEXT) {
              return null;
          }
          const width = state.size?.widthPx ?? BUS_MARKER_BASE_WIDTH_PX;
          const height = state.size?.heightPx ?? width * BUS_MARKER_ASPECT_RATIO;
          const anchorX = width * BUS_MARKER_ICON_ANCHOR_X_RATIO;
          const anchorY = height * BUS_MARKER_ICON_ANCHOR_Y_RATIO;
          const headingDeg = Number.isFinite(state?.headingDeg) ? state.headingDeg : BUS_MARKER_DEFAULT_HEADING;
          const label = state?.accessibleLabel && state.accessibleLabel.trim().length > 0
              ? state.accessibleLabel.trim()
              : `Vehicle ${vehicleID}`;
          const template = document.createElement('template');
          template.innerHTML = BUS_MARKER_SVG_TEXT.trim();
          const svgEl = template.content.firstElementChild;
          if (!svgEl || svgEl.tagName.toLowerCase() !== 'svg') {
              return null;
          }
          svgEl.classList.add('bus-marker__svg');
          svgEl.setAttribute('viewBox', `0 0 ${BUS_MARKER_VIEWBOX_WIDTH} ${BUS_MARKER_VIEWBOX_HEIGHT}`);
          svgEl.setAttribute('preserveAspectRatio', 'xMidYMid meet');
          svgEl.setAttribute('focusable', 'false');
          svgEl.setAttribute('role', 'img');
          svgEl.setAttribute('aria-label', label);
          svgEl.setAttribute('overflow', 'visible');
          svgEl.style.width = '100%';
          svgEl.style.height = '100%';
          setMarkerSvgRotation(svgEl, headingDeg);

          const routeFillColor = normalizeRouteColor(state.fillColor);
          const glyphFillColor = normalizeGlyphColor(state.glyphColor, routeFillColor);
          state.fillColor = routeFillColor;
          state.glyphColor = glyphFillColor;
          ensureCenterSquareElement(svgEl);
          applyColorsToBusMarkerSvg(svgEl, routeFillColor, glyphFillColor);
          applyStoppedVisualStateToSvg(svgEl, state.isStopped);

          const existingTitle = svgEl.querySelector('title');
          let title = existingTitle;
          if (!title) {
              title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
              svgEl.insertBefore(title, svgEl.firstChild);
          }
          title.textContent = label;

          const rootClasses = ['bus-marker__root'];
          if (state?.isStale) rootClasses.push('is-stale');
          if (state?.isSelected) rootClasses.push('is-selected');
          if (state?.isHovered) rootClasses.push('is-hover');
          const root = document.createElement('div');
          root.className = rootClasses.join(' ');
          root.dataset.vehicleId = `${vehicleID}`;
          root.setAttribute('role', 'img');
          root.setAttribute('aria-label', label);
          root.style.pointerEvents = 'none';
          root.style.touchAction = 'none';
          root.style.cursor = 'default';
          root.appendChild(svgEl);

          const wrapper = document.createElement('div');
          wrapper.appendChild(root);

          return L.divIcon({
              html: wrapper.innerHTML,
              className: 'leaflet-div-icon bus-marker',
              iconSize: [width, height],
              iconAnchor: [anchorX, anchorY]
          });
      }

      async function createBusMarkerDivIcon(vehicleID, state) {
          if (!state) {
              return null;
          }
          try {
              await loadBusSVG();
          } catch (error) {
              console.error('Failed to load bus marker SVG:', error);
              return null;
          }
          if (!BUS_MARKER_SVG_TEXT) {
              return null;
          }
          if (!state.size) {
              const zoom = map && typeof map?.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM;
              setBusMarkerSize(state, computeBusMarkerMetrics(zoom));
          }
          return buildBusMarkerDivIconSync(vehicleID, state);
      }

      function registerBusMarkerElements(vehicleID) {
          const state = busMarkerStates[vehicleID];
          const marker = markers[vehicleID];
          if (!state || !marker) {
              return null;
          }
          removeDuplicateBusMarkerLayers(vehicleID, marker);
          const iconElement = marker.getElement();
          if (!iconElement) {
              return null;
          }
          iconElement.style.pointerEvents = 'none';
          const root = iconElement.querySelector('.bus-marker__root');
          const svg = root ? root.querySelector('.bus-marker__svg') : null;
          const title = svg ? svg.querySelector('title') : null;
          const routeShape = svg ? svg.querySelector('#route_color') : null;
          const centerSquare = svg ? ensureCenterSquareElement(svg) : null;
          const centerRing = svg ? svg.querySelector(`#${BUS_MARKER_CENTER_RING_ID}`) : null;
          const heading = svg ? svg.querySelector('#heading') : null;
          state.elements = {
              icon: iconElement,
              root,
              svg,
              title,
              routeColor: routeShape,
              centerRing,
              centerSquare,
              heading
          };
          if (root) {
              root.dataset.vehicleId = `${vehicleID}`;
              root.style.pointerEvents = 'none';
              root.style.touchAction = 'none';
              root.style.cursor = 'default';
              if (root.hasAttribute('tabindex')) {
                  root.removeAttribute('tabindex');
              }
              if (root.dataset && root.dataset.busMarkerFocusBound) {
                  delete root.dataset.busMarkerFocusBound;
              }
          }
          if (svg) {
              svg.style.pointerEvents = 'none';
              svg.style.transformOrigin = BUS_MARKER_TRANSFORM_ORIGIN;
              svg.style.transformBox = 'fill-box';
              setMarkerSvgRotation(svg, state.headingDeg);
          }
          updateBusMarkerColorElements(state);
          applyBusMarkerStoppedVisualState(state);
          return state.elements;
      }

      function queueBusMarkerVisualUpdate(vehicleID, update = {}) {
          if (!vehicleID) {
              return;
          }
          const existing = pendingBusVisualUpdates.get(vehicleID) || {};
          Object.assign(existing, update);
          pendingBusVisualUpdates.set(vehicleID, existing);
          if (busMarkerVisualUpdateFrame === null) {
              busMarkerVisualUpdateFrame = requestAnimationFrame(flushBusMarkerVisualUpdates);
          }
      }

      function flushBusMarkerVisualUpdates() {
          busMarkerVisualUpdateFrame = null;
          pendingBusVisualUpdates.forEach((update, vehicleID) => {
              applyBusMarkerVisualUpdate(vehicleID, update);
          });
          pendingBusVisualUpdates.clear();
      }

      function applyBusMarkerVisualUpdate(vehicleID, update) {
          const state = busMarkerStates[vehicleID];
          if (!state) {
              return;
          }
          const elements = state.elements || registerBusMarkerElements(vehicleID);
          if (!elements || !elements.root) {
              return;
          }
          if (update && Object.prototype.hasOwnProperty.call(update, 'fillColor')) {
              state.fillColor = update.fillColor;
          }
          if (update && Object.prototype.hasOwnProperty.call(update, 'glyphColor')) {
              state.glyphColor = update.glyphColor;
          }
          updateBusMarkerColorElements(state);
          if (update && typeof update.stale === 'boolean') {
              state.isStale = update.stale;
          }
          if (update && typeof update.accessibleLabel === 'string') {
              state.accessibleLabel = update.accessibleLabel;
          }
          if (update && Number.isFinite(update.headingDeg)) {
              state.headingDeg = normalizeHeadingDegrees(update.headingDeg);
          }
          if (update && Object.prototype.hasOwnProperty.call(update, 'stopped')) {
              state.isStopped = Boolean(update.stopped);
          }
          applyBusMarkerStoppedVisualState(state);
          const rotationDeg = normalizeHeadingDegrees(Number.isFinite(state.headingDeg) ? state.headingDeg : BUS_MARKER_DEFAULT_HEADING);
          if (elements.svg) {
              setMarkerSvgRotation(elements.svg, rotationDeg);
              if (state.accessibleLabel) {
                  elements.svg.setAttribute('aria-label', state.accessibleLabel);
              }
          }
          if (elements.root && state.accessibleLabel) {
              elements.root.setAttribute('aria-label', state.accessibleLabel);
          }
          if (elements.title && state.accessibleLabel) {
              elements.title.textContent = state.accessibleLabel;
          }
          updateBusMarkerRootClasses(state);
          updateBusMarkerZIndex(state);
          applyBusMarkerOutlineWidth(state);
      }

      function applyBusMarkerOutlineWidth(state) {
          if (!state?.elements?.svg) {
              return;
          }
          state.elements.svg.style.opacity = state.isStale ? '0.6' : '1';
      }

      function updateBusMarkerRootClasses(state) {
          if (!state?.elements?.root) {
              return;
          }
          const root = state.elements.root;
          root.classList.toggle('is-stale', Boolean(state.isStale));
          root.classList.toggle('is-selected', Boolean(state.isSelected));
          root.classList.toggle('is-hover', Boolean(state.isHovered));
      }

      function updateBusMarkerZIndex(state) {
          if (!state?.marker) {
              return;
          }
          let offset = 0;
          if (state.isSelected) {
              offset = 800;
          }
          if (state.isHovered) {
              offset = Math.max(offset, 1000);
          }
          state.marker.setZIndexOffset(offset);
      }

      function setBusMarkerHovered(vehicleID, isHovered) {
          const state = busMarkerStates[vehicleID];
          if (!state) {
              return;
          }
          const next = Boolean(isHovered);
          if (state.isHovered === next) {
              return;
          }
          state.isHovered = next;
          updateBusMarkerRootClasses(state);
          updateBusMarkerZIndex(state);
          applyBusMarkerOutlineWidth(state);
      }

      function setBusMarkerSelected(vehicleID, isSelected) {
          const state = busMarkerStates[vehicleID];
          if (!state) {
              return;
          }
          const next = Boolean(isSelected);
          if (state.isSelected === next) {
              return;
          }
          state.isSelected = next;
          updateBusMarkerRootClasses(state);
          updateBusMarkerZIndex(state);
          applyBusMarkerOutlineWidth(state);
      }

      function attachBusMarkerInteractions(vehicleID) {
          const state = busMarkerStates[vehicleID];
          const marker = markers[vehicleID];
          if (!state || !marker) {
              return;
          }
          if (typeof marker.off === 'function') {
              marker.off();
          }
          if (marker.options) {
              marker.options.interactive = false;
              marker.options.keyboard = false;
          }
          if (selectedVehicleId === vehicleID) {
              selectedVehicleId = null;
          }
          if (state.isHovered || state.isSelected) {
              state.isHovered = false;
              state.isSelected = false;
              updateBusMarkerRootClasses(state);
              updateBusMarkerZIndex(state);
              applyBusMarkerOutlineWidth(state);
          }
          const elements = state.elements || registerBusMarkerElements(vehicleID);
          const icon = elements?.icon;
          const root = elements?.root;
          const svg = elements?.svg;
          if (icon) {
              icon.style.pointerEvents = 'none';
          }
          if (root) {
              root.style.pointerEvents = 'none';
              root.style.touchAction = 'none';
              root.style.cursor = 'default';
              if (root.hasAttribute('tabindex')) {
                  root.removeAttribute('tabindex');
              }
              if (root.dataset && root.dataset.busMarkerFocusBound) {
                  delete root.dataset.busMarkerFocusBound;
              }
          }
          if (svg) {
              svg.style.pointerEvents = 'none';
          }
          state.markerEventsBound = false;
      }

      async function updateBusMarkerSizes(metricsOverride = null) {
          if (!map) {
              return;
          }
          const zoom = typeof map.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM;
          const metrics = metricsOverride || computeBusMarkerMetrics(zoom);
          try {
              await loadBusSVG();
          } catch (error) {
              console.error('Failed to load bus marker SVG while updating sizes:', error);
          }
          for (const vehicleID of Object.keys(markers)) {
              const marker = markers[vehicleID];
              const state = busMarkerStates[vehicleID];
              if (!marker || !state) {
                  continue;
              }
              const currentWidth = state.size?.widthPx;
              if (currentWidth && Math.abs(currentWidth - metrics.widthPx) < 0.1) {
                  continue;
              }
              setBusMarkerSize(state, metrics);
              try {
                  const icon = await createBusMarkerDivIcon(vehicleID, state);
                  if (!icon) {
                      continue;
                  }
                  marker.setIcon(icon);
                  registerBusMarkerElements(vehicleID);
                  attachBusMarkerInteractions(vehicleID);
                  updateBusMarkerRootClasses(state);
                  updateBusMarkerZIndex(state);
                  applyBusMarkerOutlineWidth(state);
              } catch (error) {
                  console.error(`Failed to resize bus marker for vehicle ${vehicleID}:`, error);
              }
          }
          updateLabelIconsForMetrics(metrics);
      }

      function updateLabelIconsForMetrics(metrics) {
          if (!metrics || !Number.isFinite(metrics.scale) || !map) {
              return;
          }
          const scale = metrics.scale;
          Object.keys(nameBubbles).forEach(vehicleID => {
              const bubble = nameBubbles[vehicleID];
              const state = busMarkerStates[vehicleID];
              const marker = markers[vehicleID];
              if (!bubble || !state || !marker) {
                  return;
              }
              const routeColor = state.fillColor || getRouteColor(state.routeID) || outOfServiceRouteColor;
              const speedMarker = bubble.speedMarker;
              const nameMarker = bubble.nameMarker;
              const blockMarker = bubble.blockMarker;
              const routeMarker = bubble.routeMarker;

              if (speedMarker) {
                  if (adminMode && displayMode === DISPLAY_MODES.SPEED && !kioskMode && Number.isFinite(state.groundSpeed)) {
                      const speedIcon = createSpeedBubbleDivIcon(routeColor, state.groundSpeed, scale, state.headingDeg);
                      if (speedIcon) {
                          speedMarker.setIcon(speedIcon);
                      } else {
                          map.removeLayer(speedMarker);
                          delete bubble.speedMarker;
                      }
                  } else {
                      map.removeLayer(speedMarker);
                      delete bubble.speedMarker;
                  }
              }

              if (nameMarker) {
                  if (adminMode && !kioskMode) {
                      const nameIcon = createNameBubbleDivIcon(state.busName, routeColor, scale, state.headingDeg);
                      if (nameIcon) {
                          nameMarker.setIcon(nameIcon);
                      } else {
                          map.removeLayer(nameMarker);
                          delete bubble.nameMarker;
                      }
                  } else {
                      map.removeLayer(nameMarker);
                      delete bubble.nameMarker;
                  }
              }

              if (blockMarker) {
                  const blockName = busBlocks[vehicleID];
                  if (adminMode && !kioskMode && displayMode === DISPLAY_MODES.BLOCK && blockName && blockName.includes('[')) {
                      const blockIcon = createBlockBubbleDivIcon(blockName, routeColor, scale, state.headingDeg);
                      if (blockIcon) {
                          blockMarker.setIcon(blockIcon);
                      } else {
                          map.removeLayer(blockMarker);
                          delete bubble.blockMarker;
                      }
                  } else {
                      map.removeLayer(blockMarker);
                      delete bubble.blockMarker;
                  }
              }

              if (routeMarker) {
                  const routeLabel = formatCatRouteBubbleLabel(state.routeID);
                  if (adminMode && !kioskMode && routeLabel) {
                      const routeIcon = createBlockBubbleDivIcon(routeLabel, routeColor, scale, state.headingDeg);
                      if (routeIcon) {
                          routeMarker.setIcon(routeIcon);
                      } else {
                          map.removeLayer(routeMarker);
                          delete bubble.routeMarker;
                      }
                  } else {
                      map.removeLayer(routeMarker);
                      delete bubble.routeMarker;
                  }
              }

              const hasMarkers = Boolean(bubble.speedMarker || bubble.nameMarker || bubble.blockMarker || bubble.routeMarker || bubble.catRouteMarker);
              if (hasMarkers) {
                  bubble.lastScale = scale;
              } else {
                  delete nameBubbles[vehicleID];
              }
          });

          if (catOverlayEnabled) {
              catVehiclesById.forEach((vehicle, vehicleId) => {
                  if (!vehicleId) {
                      return;
                  }
                  const markerKey = `cat-${vehicleId}`;
                  const bubble = nameBubbles[markerKey];
                  if (!bubble || !bubble.catRouteMarker) {
                      return;
                  }
                  const catMarker = catVehicleMarkers.get(markerKey);
                  const latLng = catMarker && typeof catMarker.getLatLng === 'function' ? catMarker.getLatLng() : null;
                  if (!catMarker || !latLng) {
                      removeCatRouteMarkerForBubble(markerKey, bubble);
                      return;
                  }
                  const effectiveRouteKey = vehicle.catEffectiveRouteKey || getEffectiveCatRouteKey(vehicle);
                  if (!isCatRouteVisible(effectiveRouteKey)) {
                      removeCatRouteMarkerForBubble(markerKey, bubble);
                      return;
                  }
                  const rawRouteIdForLabel = toNonEmptyString(vehicle.routeId);
                  const fallbackRouteId = rawRouteIdForLabel !== '' ? rawRouteIdForLabel : effectiveRouteKey;
                  const routeLabel = formatCatRouteBubbleLabel(fallbackRouteId);
                  if (!routeLabel) {
                      removeCatRouteMarkerForBubble(markerKey, bubble);
                      return;
                  }
                  const routeColor = sanitizeCssColor(getCatRouteColor(effectiveRouteKey)) || CAT_VEHICLE_MARKER_DEFAULT_COLOR;
                  const headingDeg = normalizeHeadingDegrees(Number(vehicle.heading));
                  const routeIcon = createBlockBubbleDivIcon(routeLabel, routeColor, scale, headingDeg);
                  if (!routeIcon) {
                      removeCatRouteMarkerForBubble(markerKey, bubble);
                      return;
                  }
                  animateMarkerTo(bubble.catRouteMarker, latLng);
                  bubble.catRouteMarker.setIcon(routeIcon);
                  bubble.lastScale = scale;
              });
          }

          Object.keys(trainsFeature.nameBubbles).forEach(key => {
              const bubble = trainsFeature.nameBubbles[key];
              if (!bubble) {
                  return;
              }
              const trainID = bubble.trainID || (key.startsWith('train:') ? key.slice(6) : key);
              const state = trainsFeature.markerStates[trainID];
              const latLng = state?.lastLatLng;
              if (!state || !latLng || !Number.isFinite(latLng.lat) || !Number.isFinite(latLng.lng)) {
                  if (trainsFeature.module && typeof trainsFeature.module.removeTrainNameBubble === 'function') {
                      trainsFeature.module.removeTrainNameBubble(trainID ?? key);
                  }
                  return;
              }
              const labelText = typeof state.routeName === 'string' ? state.routeName.trim() : '';
              if (!(adminMode && !kioskMode && labelText)) {
                  if (trainsFeature.module && typeof trainsFeature.module.removeTrainNameBubble === 'function') {
                      trainsFeature.module.removeTrainNameBubble(trainID ?? key);
                  }
                  return;
              }
              const routeColor = state.fillColor || BUS_MARKER_DEFAULT_ROUTE_COLOR;
              const nameIcon = createNameBubbleDivIcon(labelText, routeColor, scale, state.headingDeg);
              if (!nameIcon) {
                  if (trainsFeature.module && typeof trainsFeature.module.removeTrainNameBubble === 'function') {
                      trainsFeature.module.removeTrainNameBubble(trainID ?? key);
                  }
                  return;
              }
              if (bubble.nameMarker) {
                  animateMarkerTo(bubble.nameMarker, latLng);
                  bubble.nameMarker.setIcon(nameIcon);
              } else {
                  bubble.nameMarker = L.marker(latLng, { icon: nameIcon, interactive: false, pane: 'busesPane' }).addTo(map);
              }
              bubble.lastScale = scale;
              bubble.trainID = trainID;
              trainsFeature.nameBubbles[key] = bubble;
          });
      }

      function scheduleMarkerScaleUpdate() {
          if (!map) {
              return;
          }
          const zoom = typeof map.getZoom === 'function' ? map.getZoom() : BUS_MARKER_BASE_ZOOM;
          const metrics = computeBusMarkerMetrics(zoom);
          pendingMarkerScaleMetrics = Object.assign({}, metrics);
          if (markerScaleUpdateFrame !== null) {
              return;
          }
          markerScaleUpdateFrame = requestAnimationFrame(async () => {
              markerScaleUpdateFrame = null;
              const metricsToApply = pendingMarkerScaleMetrics;
              pendingMarkerScaleMetrics = null;
              if (!metricsToApply) {
                  return;
              }
              try {
                  await updateBusMarkerSizes(metricsToApply);
              } catch (error) {
                  console.error('Failed to update bus marker sizes:', error);
              }
          });
      }

      function clearBusMarkerState(vehicleID) {
          pendingBusVisualUpdates.delete(vehicleID);
          if (selectedVehicleId === vehicleID) {
              selectedVehicleId = null;
          }
          if (busMarkerStates[vehicleID]) {
              delete busMarkerStates[vehicleID];
          }
      }

      function isVehicleGpsStale(vehicle) {
          if (!vehicle) {
              return false;
          }
          if (vehicle.IsStale === true || vehicle.Stale === true || vehicle.StaleGPS === true) {
              return true;
          }
          if (vehicle.HasValidGps === false || vehicle.IsRealtime === false) {
              return true;
          }
          const ageFields = [
              vehicle.SecondsSinceReport,
              vehicle.SecondsSinceLastReport,
              vehicle.SecondsSinceLastUpdate,
              vehicle.SecondsSinceUpdate,
              vehicle.SecondsSinceLastGps,
              vehicle.LastGpsAgeSeconds,
              vehicle.LocationAge,
              vehicle.GPSSignalAge,
              vehicle.Age,
              vehicle.AgeInSeconds
          ];
          for (let i = 0; i < ageFields.length; i += 1) {
              const value = Number(ageFields[i]);
              if (Number.isFinite(value) && value > GPS_STALE_THRESHOLD_SECONDS) {
                  return true;
              }
          }
          return false;
      }

      function getTrainIdentifier(train) {
          if (!train) {
              return null;
          }
          const candidateFields = ['trainID', 'trainId', 'trainNumRaw', 'trainNum'];
          for (let i = 0; i < candidateFields.length; i += 1) {
              const value = train?.[candidateFields[i]];
              if (value !== undefined && value !== null) {
                  const text = `${value}`.trim();
                  if (text.length > 0) {
                      return text;
                  }
              }
          }
          if (typeof train?.routeName === 'string' && train.routeName.trim().length > 0) {
              return train.routeName.trim();
          }
          return null;
      }

      function trainIncludesStation(train, stationCode) {
          if (!train || !Array.isArray(train?.stations)) {
              return false;
          }
          if (typeof stationCode !== 'string' || stationCode.trim().length === 0) {
              return false;
          }
          const target = stationCode.trim().toUpperCase();
          return train.stations.some(stop => {
              const code = typeof stop?.code === 'string' ? stop.code.trim().toUpperCase() : '';
              return code === target;
          });
      }

      function buildTrainAccessibleLabel(train) {
          if (!train) {
              return 'Amtrak train';
          }
          const parts = [];
          const numberFields = ['trainNumRaw', 'trainNum'];
          for (let i = 0; i < numberFields.length; i += 1) {
              const value = train?.[numberFields[i]];
              if (value !== undefined && value !== null) {
                  const text = `${value}`.trim();
                  if (text.length > 0) {
                      parts.push(`Train ${text}`);
                      break;
                  }
              }
          }
          const routeName = typeof train?.routeName === 'string' ? train.routeName.trim() : '';
          if (routeName.length > 0) {
              parts.push(routeName);
          }
          const status = typeof train?.trainTimely === 'string' ? train.trainTimely.trim() : '';
          if (status.length > 0) {
              parts.push(status);
          }
          return parts.length > 0 ? parts.join(' — ') : 'Amtrak train';
      }

      function getTrainHeadingValue(train) {
          if (!train || typeof train !== 'object') {
              return null;
          }
          const candidateFields = ['heading', 'Heading', 'direction', 'Direction', 'dir', 'Dir'];
          for (let i = 0; i < candidateFields.length; i += 1) {
              const field = candidateFields[i];
              if (train[field] !== undefined && train[field] !== null) {
                  return train[field];
              }
          }
          return null;
      }

      function getTrainHeadingDegrees(headingValue, fallbackHeading) {
          const fallback = Number.isFinite(fallbackHeading)
              ? fallbackHeading
              : BUS_MARKER_DEFAULT_HEADING;
          if (Number.isFinite(headingValue)) {
              return normalizeHeadingDegrees(headingValue);
          }
          if (typeof headingValue === 'string') {
              const trimmed = headingValue.trim();
              if (trimmed.length > 0) {
                  const numericCandidate = Number(trimmed);
                  if (Number.isFinite(numericCandidate)) {
                      return normalizeHeadingDegrees(numericCandidate);
                  }
                  const normalized = trimmed.toUpperCase();
                  const sanitized = normalized.replace(/[^A-Z]/g, '');
                  const candidates = [];
                  if (sanitized.length > 0) {
                      candidates.push(sanitized);
                      const withoutBound = sanitized.replace(/BOUND$/, '');
                      if (withoutBound !== sanitized && withoutBound.length > 0) {
                          candidates.push(withoutBound);
                      }
                      const withoutWard = sanitized.replace(/WARD$/, '');
                      if (withoutWard !== sanitized && withoutWard.length > 0) {
                          candidates.push(withoutWard);
                      }
                  }
                  if (normalized !== sanitized && normalized.length > 0) {
                      candidates.push(normalized);
                  }
                  for (let i = 0; i < candidates.length; i += 1) {
                      const key = candidates[i];
                      const degrees = TRAIN_CARDINAL_HEADING_DEGREES[key];
                      if (Number.isFinite(degrees)) {
                          return normalizeHeadingDegrees(degrees);
                      }
                  }
              }
          }
          return normalizeHeadingDegrees(fallback);
      }

      function updateTrainMarkerHeading(state, newPosition, headingValue) {
          if (!state) {
              return BUS_MARKER_DEFAULT_HEADING;
          }
          const fallbackHeading = Number.isFinite(state.headingDeg)
              ? state.headingDeg
              : BUS_MARKER_DEFAULT_HEADING;
          let lat = Number.NaN;
          let lng = Number.NaN;
          if (Array.isArray(newPosition)) {
              if (newPosition.length >= 2) {
                  lat = Number(newPosition[0]);
                  lng = Number(newPosition[1]);
              }
          } else if (newPosition && typeof newPosition === 'object') {
              lat = Number(newPosition.lat ?? newPosition.Latitude);
              lng = Number(newPosition.lng ?? newPosition.Longitude);
          }
          if (Number.isFinite(lat) && Number.isFinite(lng)) {
              const current = L.latLng(lat, lng);
              const history = Array.isArray(state.positionHistory) ? state.positionHistory : [];
              if (history.length === 0) {
                  history.push(current);
              } else {
                  const previous = history[history.length - 1];
                  if (previous && typeof previous.distanceTo === 'function') {
                      const distance = previous.distanceTo(current);
                      if (distance >= MIN_POSITION_UPDATE_METERS) {
                          history.push(current);
                          if (history.length > 2) {
                              history.shift();
                          }
                      } else {
                          history[history.length - 1] = current;
                      }
                  } else {
                      history[history.length - 1] = current;
                  }
              }
              state.positionHistory = history;
          }
          const headingDeg = getTrainHeadingDegrees(headingValue, fallbackHeading);
          state.headingDeg = normalizeHeadingDegrees(headingDeg);
          return state.headingDeg;
      }

      function animateMarkerTo(marker, newPosition) {
        if (!marker || !newPosition) return;
        const hasArrayPosition = Array.isArray(newPosition) && newPosition.length >= 2;
        const endPos = hasArrayPosition ? L.latLng(newPosition) : L.latLng(newPosition?.lat, newPosition?.lng);
        if (!endPos || Number.isNaN(endPos.lat) || Number.isNaN(endPos.lng)) return;

        const startPos = marker.getLatLng();
        if (!startPos) {
          marker.setLatLng(endPos);
          return;
        }

        if (lowPerformanceMode || typeof requestAnimationFrame !== 'function') {
          marker.setLatLng(endPos);
          return;
        }

        const positionsMatch = typeof startPos.equals === 'function'
          ? startPos.equals(endPos, 1e-7)
          : (Math.abs(startPos.lat - endPos.lat) < 1e-7 && Math.abs(startPos.lng - endPos.lng) < 1e-7);

        if (positionsMatch) {
          marker.setLatLng(endPos);
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
          if (t < 1) requestAnimationFrame(animate);
        }
        requestAnimationFrame(animate);
      }

      document.addEventListener("DOMContentLoaded", async () => {
        initializeAdminAuthUI();
        const authorizedViaUrl = await attemptAdminAuthorizationFromUrl();
        if (authorizedViaUrl) {
          await checkAdminAuthorization({ silent: true, forceEnable: true, forceRefresh: true });
        } else {
          await checkAdminAuthorization({ silent: true });
        }
        ensurePanelsHiddenForKioskExperience();
        initializePanelStateForViewport();
        beginAgencyLoad();
        loadAgencies()
          .then(() => {
            initializeAdminKioskAgencySchedule();
            initializeAdminKioskOnDemandSchedule();
            initMap();
            showCookieBanner();
            enforceIncidentVisibilityForCurrentAgency();
            return loadAgencyData()
              .then(() => {
                startRefreshIntervals();
              });
          })
          .catch(error => {
            console.error('Error during initial load:', error);
          })
          .finally(() => {
            completeAgencyLoad();
          });
      });
