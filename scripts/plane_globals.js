/*!
 * plane_globals.js
 * Minimal global defaults for integrating markers.js + planeObject.js
 * Safe, idempotent, and overrideable at runtime.
 */
(function initPlaneGlobals() {
  const W = (typeof window !== "undefined" ? window : globalThis);

  // -------- Rendering mode & feature flags (set sane defaults) ----------
  if (typeof W.webgl === "undefined") W.webgl = false;                // sprite/WebGL path off by default
  if (typeof W.pTracks === "undefined") W.pTracks = false;            // per-track rendering nuances
  if (typeof W.SelectedAllPlanes === "undefined") W.SelectedAllPlanes = false;
  if (typeof W.onlySelected === "undefined") W.onlySelected = false;
  if (typeof W.globeIndex === "undefined") W.globeIndex = false;      // affects "stale" thresholds
  if (typeof W.halloween === "undefined") W.halloween = false;        // seasonal overrides off
  if (typeof W.squareMania === "undefined") W.squareMania = false;    // force-square style off
  if (typeof W.atcStyle === "undefined") W.atcStyle = true;           // enable 7700/7600/7500 override
  if (typeof W.options === "undefined") W.options = {};               // misc flags (e.g., options.noRound=false)
  if (typeof W.monochromeMarkers === "undefined") W.monochromeMarkers = null; // e.g. "#00FF00" to force a single color
  if (typeof W.darkerColors === "undefined") W.darkerColors = false;  // global darkening toggle

  // -------- Outline/size defaults used by icon builders -----------------
  if (typeof W.outlineWidth === "undefined") W.outlineWidth = 1.0;    // base stroke width passed to SVG
  if (typeof W.OutlineADSBColor === "undefined") W.OutlineADSBColor = "#000000"; // icon stroke color

  // -------- Tables usually provided by markers.js -----------------------
  // If markers.js is loaded, these will already exist; keep as fallbacks to avoid ReferenceErrors.
  if (typeof W.shapes === "undefined") W.shapes = {};                 // shape registry (SVG paths)
  if (typeof W.TypeDesignatorIcons === "undefined") W.TypeDesignatorIcons = {};   // "A320" -> [shape, scale]
  if (typeof W.TypeDescriptionIcons === "undefined") W.TypeDescriptionIcons = {}; // "L2J", "L2J-M" -> [shape, scale]
  if (typeof W.CategoryIcons === "undefined") W.CategoryIcons = {};               // "A1","A3","A5","A7" -> [shape, scale]

  // -------- Altitude colour model (matches planeObject expectations) ----
  if (typeof W.ColorByAlt === "undefined") {
    // Compact, production-safe defaults. Tune to taste.
    W.ColorByAlt = {
      unknown: { h: 210, s: 10, l: 55 },         // muted steel blue
      ground:  { h: 120, s: 25, l: 60 },         // calm greenish for on-ground
      air: {
        s: 80,                                   // baseline saturation aloft
        // Hue vs Altitude control points (feet → hue degrees)
        h: [
          { alt:    0,  val: 220 },              // low = blue
          { alt: 45000, val:   0 }               // high = red
        ],
        // Lightness vs Hue control points (hue → L%)
        l: [
          { h:   0, val: 45 },
          { h: 220, val: 45 }
        ]
      },
      // Additive deltas applied after base colour (component-wise)
      stale:    { h:   0, s: -10, l: -10 },      // older positions = duller/darker
      selected: { h:   0, s: +10, l:  +5 },      // selected = slightly richer/brighter
      mlat:     { h: -10, s:   0, l:   0 }       // MLAT hue nudge
    };
  }

  // -------- Utility shims (only if upstream didn’t provide them) --------
  if (typeof W.adjust_baro_alt === "undefined") {
    // Pass-through shim; replace if you want pressure correction.
    W.adjust_baro_alt = function adjust_baro_alt(a) { return a; };
  }

  // Basic HEX→HSL and HSL→RGB shims (used if monochromeMarkers is set)
  if (typeof W.hexToHSL === "undefined") {
    W.hexToHSL = function hexToHSL(hex) {
      if (!hex) return { h: 0, s: 0, l: 50 };
      const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      if (!m) return { h: 0, s: 0, l: 50 };
      const r = parseInt(m[1], 16) / 255, g = parseInt(m[2], 16) / 255, b = parseInt(m[3], 16) / 255;
      const max = Math.max(r, g, b), min = Math.min(r, g, b);
      let h, s, l = (max + min) / 2;
      const d = max - min;
      if (d === 0) { h = 0; s = 0; }
      else {
        s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
        switch (max) {
          case r: h = (g - b) / d + (g < b ? 6 : 0); break;
          case g: h = (b - r) / d + 2; break;
          default: h = (r - g) / d + 4;
        }
        h *= 60;
      }
      return { h: (h + 360) % 360, s: Math.round(s * 100), l: Math.round(l * 100) };
    };
  }

  if (typeof W.hslToRgb === "undefined") {
    W.hslToRgb = function hslToRgb(h, s, l) {
      // Return {r,g,b} 0..255 for consumers that set sprite RGB directly.
      s /= 100; l /= 100;
      const a = s * Math.min(l, 1 - l);
      const f = n => {
        const k = (n + h / 30) % 12;
        const c = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
        return Math.round(255 * c);
      };
      return { r: f(0), g: f(8), b: f(4) };
    };
  }

  // -------- Convenience: runtime overrides for your app/Codex ----------
  if (typeof W.setPlaneStyleOptions === "undefined") {
    W.setPlaneStyleOptions = function setPlaneStyleOptions(opts) {
      if (!opts) return;
      const keys = [
        "webgl","pTracks","SelectedAllPlanes","onlySelected","globeIndex",
        "halloween","squareMania","atcStyle","darkerColors","outlineWidth","OutlineADSBColor","monochromeMarkers"
      ];
      keys.forEach(k => { if (k in opts) W[k] = opts[k]; });
      if (opts.options && typeof opts.options === "object") {
        W.options = Object.assign(W.options || {}, opts.options);
      }
    };
  }

  // -------- Diagnostics helper (optional) -------------------------------
  if (typeof W.planeGlobalsInfo === "undefined") {
    W.planeGlobalsInfo = function planeGlobalsInfo() {
      return {
        webgl: W.webgl, pTracks: W.pTracks, SelectedAllPlanes: W.SelectedAllPlanes, onlySelected: W.onlySelected,
        globeIndex: W.globeIndex, halloween: W.halloween, squareMania: W.squareMania, atcStyle: W.atcStyle,
        darkerColors: W.darkerColors, outlineWidth: W.outlineWidth, OutlineADSBColor: W.OutlineADSBColor,
        monochromeMarkers: W.monochromeMarkers, options: W.options, hasShapes: !!W.shapes && !!Object.keys(W.shapes).length
      };
    };
  }
})();
