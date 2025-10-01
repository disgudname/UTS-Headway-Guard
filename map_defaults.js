(function initHeadwayMapDefaults() {
  const globalScope = (typeof window !== 'undefined' ? window : globalThis);
  const DEFAULT_CENTER = [38.03799212281404, -78.50981502838886];
  const DEFAULT_ZOOM = 15;

  const existing = globalScope.HeadwayMapDefaults && typeof globalScope.HeadwayMapDefaults === 'object'
    ? globalScope.HeadwayMapDefaults
    : {};

  const toNumber = (value, fallback) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
  };

  const resolveCenter = () => {
    if (Array.isArray(existing.center) && existing.center.length === 2) {
      const lat = toNumber(existing.center[0], DEFAULT_CENTER[0]);
      const lon = toNumber(existing.center[1], DEFAULT_CENTER[1]);
      return [lat, lon];
    }
    return DEFAULT_CENTER.slice();
  };

  const resolveZoom = () => toNumber(existing.zoom, DEFAULT_ZOOM);

  globalScope.HeadwayMapDefaults = Object.assign({}, existing, {
    center: resolveCenter(),
    zoom: resolveZoom(),
  });
})();
