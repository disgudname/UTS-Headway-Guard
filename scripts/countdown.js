/*
 * Countdown Clock -- browser port of the NYC-subway-style LED countdown sign
 * (see the standalone "Countdown Clock" project: driver/ + shared/ in Python).
 * Renders onto a 256x32 virtual LED grid (8 chained 32x32 panels) using the
 * same layout/font/color logic, drawn as round LEDs on a <canvas>.
 *
 * Config via query string:
 *   ?code=NGPG        feed code from /v1/feed-codes (preferred; stable across
 *                      TransLoc stop ID renumbering)
 *   ?stopIDs=26,113    raw TransLoc stop IDs, comma-separated (legacy form)
 *   ?poll=15000        poll interval ms (default 15000)
 *   ?page=4000         bottom-row page duration ms (default 4000)
 */
(function () {
  "use strict";

  var PANEL_SIZE = 32;
  var NUM_PANELS = 8;
  var CANVAS_W = PANEL_SIZE * NUM_PANELS; // 256
  var CANVAS_H = PANEL_SIZE; // 32
  var UNLIT_COLOR = [18, 18, 18];

  var ROW_HEIGHT = 16;
  var ROW_FONT_CAP_HEIGHT = 11;
  var PILL_HEIGHT = 13;
  var PILL_FONT_CAP_HEIGHT = 11;
  var BLINK_PERIOD_MS = 500;
  var TICK_MS = 200;

  // Real MTA countdown clocks show one pinned next-arrival plus a rotating set of the
  // next 5 after it (6 total) -- see "Tailoring information design for NYC Subway
  // countdown clocks" (https://www.adamfishercox.com/writing/countdown-clocks-for-the-mta/):
  // "the next upcoming train on the first line, and rotates the second line through the
  // following five trains to arrive." Capping here keeps a merged multi-route stop
  // (especially one mixing UTS + CAT arrivals via a feed code) from ballooning the
  // rotation list well past what the real hardware this sign emulates ever shows.
  var MAX_ARRIVALS = 6;

  var TEXT_COLOR = [0, 255, 170];
  var URGENT_COLOR = [255, 0, 0];

  // Orange/amber, matching the real NYC countdown clocks' scrolling service-alert line --
  // their signs are single-color amber/yellow LED displays, so this isn't a special
  // "alert" accent color distinct from some other "normal" color; it's just what all text
  // on those signs looks like, used here specifically for the alert line to read as a
  // clear departure from this sign's own cyan-green arrival rows.
  var ALERT_COLOR = [255, 140, 0];
  var ALERT_SCROLL_SPEED_PX_S = 30;
  var ALERT_TOKEN_GAP = 4; // px between scrolling tokens (both plain words and pills)

  // Route colors as of 2026-07-18, pulled from live TransLoc route data and hardcoded
  // here rather than derived from the current arrivals list: UTS route colors rarely
  // change, and this needs to resolve a route name mentioned in alert text even when
  // that route has no active arrival at this particular stop right now. Keys are the
  // same short/uppercased form shortRouteLabel() produces, matching the pill labels
  // already shown on arrival rows -- update this table if UTS repaints a route.
  var KNOWN_ROUTE_COLORS = {
    "GOLD": "#ffdd00",
    "GREEN": "#0c8103",
    "ORANGE": "#ff7300",
    "SILVER": "#5f6367",
    "PURPLE": "#662c90",
    "NIGHT PILOT": "#232d48",
  };
  var MAX_ROUTE_PHRASE_WORDS = Math.max.apply(null, Object.keys(KNOWN_ROUTE_COLORS).map(function (k) {
    return k.split(" ").length;
  }));

  function qp(name, def) {
    var params = new URLSearchParams(window.location.search);
    return params.has(name) ? params.get(name) : def;
  }

  // ---------------------------------------------------------------------
  // BDF font parsing (mirrors shared/graphics.py's Font.LoadFont/DrawText)
  // ---------------------------------------------------------------------

  function parseBdf(text) {
    var lines = text.split(/\r?\n/);
    var glyphs = {};
    var i = 0;
    var n = lines.length;

    function parseChar(i) {
      var encoding = null;
      var dwidth = 0;
      var bbw = 0, bbh = 0, bbxoff = 0, bbyoff = 0;
      var rows = [];
      i += 1; // past STARTCHAR
      while (i < n && lines[i].indexOf("ENDCHAR") !== 0) {
        var line = lines[i];
        if (line.indexOf("ENCODING") === 0) {
          encoding = parseInt(line.split(/\s+/)[1], 10);
        } else if (line.indexOf("DWIDTH") === 0) {
          dwidth = parseInt(line.split(/\s+/)[1], 10);
        } else if (line.indexOf("BBX") === 0) {
          var parts = line.split(/\s+/);
          bbw = parseInt(parts[1], 10);
          bbh = parseInt(parts[2], 10);
          bbxoff = parseInt(parts[3], 10);
          bbyoff = parseInt(parts[4], 10);
        } else if (line.indexOf("BITMAP") === 0) {
          i += 1;
          var hexRows = [];
          while (i < n && lines[i].indexOf("ENDCHAR") !== 0) {
            hexRows.push(lines[i].trim());
            i += 1;
          }
          var nibbleBits = hexRows.length ? hexRows[0].length * 4 : bbw;
          for (var h = 0; h < hexRows.length; h += 1) {
            var hrow = hexRows[h];
            var val = hrow ? parseInt(hrow, 16) : 0;
            if (nibbleBits > bbw) {
              val = val >>> (nibbleBits - bbw);
            }
            rows.push(val);
          }
          continue;
        }
        i += 1;
      }
      return { i: i, glyph: { bbw: bbw, bbh: bbh, bbxoff: bbxoff, bbyoff: bbyoff, dwidth: dwidth, rows: rows }, encoding: encoding };
    }

    while (i < n) {
      var line = lines[i];
      if (line.indexOf("STARTCHAR") === 0) {
        var parsed = parseChar(i);
        i = parsed.i;
        if (parsed.encoding !== null && parsed.encoding >= 0) {
          glyphs[parsed.encoding] = parsed.glyph;
        }
        continue;
      }
      i += 1;
    }

    var defaultAdvance = 0;
    for (var code in glyphs) {
      if (glyphs[code].dwidth > defaultAdvance) defaultAdvance = glyphs[code].dwidth;
    }

    return {
      glyphs: glyphs,
      defaultAdvance: defaultAdvance,
      characterWidth: function (codepoint) {
        var g = glyphs[codepoint];
        return g ? g.dwidth : defaultAdvance;
      },
    };
  }

  function textWidth(font, text) {
    var w = 0;
    for (var i = 0; i < text.length; i += 1) {
      w += font.characterWidth(text.charCodeAt(i));
    }
    return w;
  }

  // Returns the true lit-pixel bounding box of `text` (left/right relative to the
  // draw-origin cursor, plus width), as opposed to textWidth()'s advance-width sum.
  // The font's glyphs carry built-in trailing space for letter-spacing in running text
  // (e.g. digits are BBX width 6 at DWIDTH 8 — 2px of blank space baked onto the right),
  // so sizing/centering a pill off textWidth() leaves the visible ink looking shifted
  // left. Centering off the ink bounds instead fixes that regardless of label length.
  function textInkBounds(font, text) {
    var cursor = 0;
    var left = null, right = null;
    for (var i = 0; i < text.length; i += 1) {
      var glyph = font.glyphs[text.charCodeAt(i)];
      if (!glyph) {
        cursor += font.defaultAdvance;
        continue;
      }
      if (glyph.bbw > 0) {
        var glyphLeft = cursor + glyph.bbxoff;
        var glyphRight = glyphLeft + glyph.bbw;
        if (left === null || glyphLeft < left) left = glyphLeft;
        if (right === null || glyphRight > right) right = glyphRight;
      }
      cursor += glyph.dwidth;
    }
    if (left === null) { left = 0; right = 0; }
    return { left: left, right: right, width: right - left };
  }

  // Draws with baseline at row y. Returns total advance width.
  function drawText(canvas, font, x, y, color, text) {
    var cursor = x;
    for (var i = 0; i < text.length; i += 1) {
      var glyph = font.glyphs[text.charCodeAt(i)];
      if (!glyph) {
        cursor += font.defaultAdvance;
        continue;
      }
      var topRow = y - glyph.bbyoff - glyph.bbh + 1;
      for (var r = 0; r < glyph.rows.length; r += 1) {
        var bits = glyph.rows[r];
        var canvasY = topRow + r;
        for (var c = 0; c < glyph.bbw; c += 1) {
          if (bits & (1 << (glyph.bbw - 1 - c))) {
            canvas.setPixel(cursor + glyph.bbxoff + c, canvasY, color);
          }
        }
      }
      cursor += glyph.dwidth;
    }
    return cursor - x;
  }

  function fillRoundedRect(canvas, x, y, w, h, radius, color) {
    var r = Math.min(radius, Math.floor(w / 2), Math.floor(h / 2));
    for (var dy = 0; dy < h; dy += 1) {
      for (var dx = 0; dx < w; dx += 1) {
        if (r > 0) {
          var inCornerX = dx < r || dx >= w - r;
          var inCornerY = dy < r || dy >= h - r;
          if (inCornerX && inCornerY) {
            var cx = dx < r ? r - 1 : w - r;
            var cy = dy < r ? r - 1 : h - r;
            var dist2 = (dx - cx) * (dx - cx) + (dy - cy) * (dy - cy);
            if (dist2 > r * r) continue;
          }
        }
        canvas.setPixel(x + dx, y + dy, color);
      }
    }
  }

  // ---------------------------------------------------------------------
  // Color utils (mirrors shared/color_utils.py)
  // ---------------------------------------------------------------------

  function hexToRgb(hex) {
    var h = hex.replace(/^#/, "");
    return [parseInt(h.substr(0, 2), 16), parseInt(h.substr(2, 2), 16), parseInt(h.substr(4, 2), 16)];
  }

  function relativeLuminance(r, g, b) {
    function channel(c) {
      c = c / 255;
      return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
    }
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
  }

  function contrastTextColor(hexBg) {
    var rgb = hexToRgb(hexBg);
    return relativeLuminance(rgb[0], rgb[1], rgb[2]) > 0.5 ? [0, 0, 0] : [255, 255, 255];
  }

  // ---------------------------------------------------------------------
  // Arrivals normalization (mirrors shared/eta.py)
  // ---------------------------------------------------------------------

  function minutesFor(seconds) {
    return Math.max(0, Math.round(seconds / 60));
  }

  function isUrgent(arrival) {
    return arrival.isArriving || minutesFor(arrival.seconds) <= 0;
  }

  function displayEta(arrival) {
    return minutesFor(arrival.seconds) + "min";
  }

  function normalize(rawRoutes) {
    var arrivals = [];
    for (var i = 0; i < rawRoutes.length; i += 1) {
      var route = rawRoutes[i] || {};
      var colorHex = route.Color || "#808080";
      var routeName = route.RouteDescription || "Route";
      var destination = route.Destination || "";
      var times = route.Times || [];
      for (var t = 0; t < times.length; t += 1) {
        var time = times[t] || {};
        var text = (time.Text || "").trim().toLowerCase();
        var isArriving = !!time.IsArriving || text === "arriving";
        arrivals.push({
          routeName: routeName,
          colorHex: colorHex,
          seconds: parseInt(time.Seconds || 0, 10),
          isArriving: isArriving,
          destination: destination,
        });
      }
    }
    arrivals.sort(function (a, b) {
      return a.seconds - b.seconds;
    });
    return arrivals.slice(0, MAX_ARRIVALS);
  }

  // Drops a trailing " Line" or " Loop" word from a route name, e.g.
  // "Purple Line" -> "Purple", but leaves names like "Night Pilot" alone
  // since "Pilot" isn't one of those words.
  function shortRouteLabel(name) {
    var m = name.match(/^(.*\S)\s+(?:line|loop)$/i);
    return m ? m[1] : name;
  }

  // ---------------------------------------------------------------------
  // Layout (mirrors shared/layout.py)
  // ---------------------------------------------------------------------

  function pageCount(nArrivals) {
    return Math.max(1, nArrivals - 1);
  }

  // Shared by every row (arrival rows and the scrolling alert line) so their text and
  // pills all sit at the identical vertical position. Shifted 2px above a naive vertical
  // center: the deepest descenders (g/p/q/j drop 3px below baseline) would otherwise
  // land exactly 1px past the bottom edge of the canvas on the second row (rowTop +
  // ROW_HEIGHT == canvas height there, leaving zero margin) and get silently clipped by
  // setPixel's bounds check. A 1px shift alone fixes the clipping but leaves the
  // descender's tip flush against the very last row with no breathing room; the extra
  // 1px gives it a visible 1px margin instead. This still costs no headroom at the top --
  // every glyph's ascent still clears rowTop, even the tallest (!) at exactly row 0.
  function rowTopPad() {
    return Math.max(0, Math.floor((ROW_HEIGHT - ROW_FONT_CAP_HEIGHT) / 2)) - 2;
  }

  var PILL_PAD_X = 3;

  function pillWidth(font, text) {
    var ink = textInkBounds(font, text);
    return ink.width + PILL_PAD_X * 2;
  }

  // Draws a route-color pill with its top-left at (x, y) and returns its width. Shared
  // by arrival rows and the scrolling alert line so a route name renders identically in
  // both places.
  function drawPill(canvas, font, x, y, text, colorHex) {
    var ink = textInkBounds(font, text);
    var pillW = ink.width + PILL_PAD_X * 2;

    var rgb = hexToRgb(colorHex);
    fillRoundedRect(canvas, x, y, pillW, PILL_HEIGHT, Math.floor(PILL_HEIGHT / 2), rgb);

    var pillTopPad = Math.floor((PILL_HEIGHT - PILL_FONT_CAP_HEIGHT) / 2);
    var pillBaseline = y + pillTopPad + PILL_FONT_CAP_HEIGHT - 1;
    var contrast = contrastTextColor(colorHex);
    // Shift the draw-origin by -ink.left so the label's leftmost lit pixel lands
    // exactly PILL_PAD_X in from the pill's edge, regardless of the glyph's own bearing.
    drawText(canvas, font, x + PILL_PAD_X - ink.left, pillBaseline, contrast, text);
    return pillW;
  }

  function drawRow(canvas, font, rowTop, rank, arrival, blinkOn) {
    var urgent = isUrgent(arrival);
    var topPad = rowTopPad();
    var baseline = rowTop + topPad + ROW_FONT_CAP_HEIGHT;
    // PILL_HEIGHT is exactly 2px taller than ROW_FONT_CAP_HEIGHT, so deriving pillY
    // from the same topPad as the row's text baseline (rather than independently off
    // ROW_HEIGHT) puts the pill 1px above and 1px below the row's own cap-height text,
    // and keeps the two in sync if topPad/baseline ever shifts again.
    var pillY = rowTop + topPad;

    var x = 2;
    var label = rank + ".";
    var labelColor = urgent ? URGENT_COLOR : TEXT_COLOR;
    x += drawText(canvas, font, x, baseline, labelColor, label);
    x += 3;

    var routeLabel = shortRouteLabel(arrival.routeName).toUpperCase();
    x += drawPill(canvas, font, x, pillY, routeLabel, arrival.colorHex);

    if (arrival.destination) {
      x += 4;
      var destColor = urgent ? URGENT_COLOR : TEXT_COLOR;
      drawText(canvas, font, x, baseline, destColor, arrival.destination);
    }

    if (!urgent || blinkOn) {
      var etaText = displayEta(arrival);
      var etaW = textWidth(font, etaText);
      var etaX = canvas.width - 2 - etaW;
      var etaColor = urgent ? URGENT_COLOR : TEXT_COLOR;
      drawText(canvas, font, etaX, baseline, etaColor, etaText);
    }
  }

  // Splits alert text into a list of {type:"text", value} and {type:"pill", value,
  // colorHex} segments, greedily matching runs of ALL-CAPS words against known route
  // names (see KNOWN_ROUTE_COLORS) -- longest match wins, e.g. "NIGHT PILOT" matches as
  // one pill rather than "NIGHT" and "PILOT" as two unmatched words. Matching requires
  // the words in the alert text to already be all-caps, so an admin signals a route
  // bullet by writing the route name in caps, and ordinary capitalized text in the
  // message isn't accidentally swallowed.
  function tokenizeAlert(text) {
    var words = text.split(" ").filter(function (w) { return w.length > 0; });
    var segments = [];
    var i = 0;
    var n = words.length;
    while (i < n) {
      var matched = null;
      for (var span = Math.min(MAX_ROUTE_PHRASE_WORDS, n - i); span >= 1; span -= 1) {
        var phraseWords = words.slice(i, i + span);
        var allCaps = phraseWords.every(function (w) { return /^[A-Z]+$/.test(w); });
        if (!allCaps) continue;
        var phrase = phraseWords.join(" ");
        if (Object.prototype.hasOwnProperty.call(KNOWN_ROUTE_COLORS, phrase)) {
          matched = { phrase: phrase, span: span };
          break;
        }
      }
      if (matched) {
        segments.push({ type: "pill", value: matched.phrase, colorHex: KNOWN_ROUTE_COLORS[matched.phrase] });
        i += matched.span;
      } else {
        segments.push({ type: "text", value: words[i] });
        i += 1;
      }
    }
    return segments;
  }

  function segmentWidth(font, seg) {
    return seg.type === "pill" ? pillWidth(font, seg.value) : textWidth(font, seg.value);
  }

  // Renders `text` as a single continuous strip scrolling right-to-left across rowTop's
  // row, replacing the normal rotating second line while a service alert is active --
  // mirrors the real NYC countdown clocks' behavior of taking over the second line with
  // scrolling alert text. Route names written in ALL CAPS within the text (see
  // tokenizeAlert/KNOWN_ROUTE_COLORS) render as the same colored pills used on arrival
  // rows instead of plain text, mirroring the real signs' colored line bullets.
  function drawScrollingAlert(canvas, font, rowTop, text, nowMs) {
    var segments = tokenizeAlert(text);
    if (!segments.length) return;

    var topPad = rowTopPad();
    var baseline = rowTop + topPad + ROW_FONT_CAP_HEIGHT;
    var pillY = rowTop + topPad;

    var widths = segments.map(function (seg) { return segmentWidth(font, seg); });
    var stripWidth = widths.reduce(function (a, b) { return a + b; }, 0) + ALERT_TOKEN_GAP * segments.length;

    // Scrolls from just off the right edge to fully off the left edge, then loops.
    // Adding canvas.width to the cycle gives a breather of blank space before the text
    // repeats, rather than the strip immediately chasing its own tail.
    var cycleWidth = stripWidth + canvas.width;
    var offset = Math.floor((nowMs / 1000 * ALERT_SCROLL_SPEED_PX_S) % cycleWidth);
    var x = canvas.width - offset;

    for (var i = 0; i < segments.length; i += 1) {
      var seg = segments[i];
      var w = widths[i];
      if (x + w >= 0 && x <= canvas.width) {
        if (seg.type === "pill") {
          drawPill(canvas, font, x, pillY, seg.value, seg.colorHex);
        } else {
          drawText(canvas, font, x, baseline, ALERT_COLOR, seg.value);
        }
      }
      x += w + ALERT_TOKEN_GAP;
    }
  }

  function renderPage(canvas, font, arrivals, pageIndex, nowMs, alertText) {
    canvas.clear();

    if (!arrivals.length) {
      var baseline = ROW_HEIGHT + ROW_FONT_CAP_HEIGHT;
      drawText(canvas, font, 2, baseline, TEXT_COLOR, "No arrivals");
      return;
    }

    var blinkOn = Math.floor(nowMs / BLINK_PERIOD_MS) % 2 === 0;

    drawRow(canvas, font, 0, 1, arrivals[0], blinkOn);

    if (alertText) {
      drawScrollingAlert(canvas, font, ROW_HEIGHT, alertText, nowMs);
      return;
    }

    var remaining = arrivals.slice(1);
    if (remaining.length) {
      var idx = pageIndex % remaining.length;
      drawRow(canvas, font, ROW_HEIGHT, idx + 2, remaining[idx], blinkOn);
    }
  }

  // ---------------------------------------------------------------------
  // Virtual LED canvas + real <canvas> renderer
  // ---------------------------------------------------------------------

  function VirtualCanvas(width, height) {
    this.width = width;
    this.height = height;
    this.pixels = new Uint8Array(width * height * 3);
  }
  VirtualCanvas.prototype.setPixel = function (x, y, rgb) {
    if (x < 0 || x >= this.width || y < 0 || y >= this.height) return;
    var idx = (y * this.width + x) * 3;
    this.pixels[idx] = rgb[0];
    this.pixels[idx + 1] = rgb[1];
    this.pixels[idx + 2] = rgb[2];
  };
  VirtualCanvas.prototype.clear = function () {
    this.pixels.fill(0);
  };
  VirtualCanvas.prototype.getPixel = function (x, y) {
    var idx = (y * this.width + x) * 3;
    return [this.pixels[idx], this.pixels[idx + 1], this.pixels[idx + 2]];
  };

  function LedRenderer(canvasEl, virtualCanvas) {
    this.el = canvasEl;
    this.ctx = canvasEl.getContext("2d");
    this.vc = virtualCanvas;
    this.scale = 1;
    this.resize();
    var self = this;
    window.addEventListener("resize", function () {
      self.resize();
    });
  }
  LedRenderer.prototype.resize = function () {
    var dpr = window.devicePixelRatio || 1;
    var maxW = window.innerWidth;
    var maxH = window.innerHeight;
    var scale = Math.max(1, Math.floor(Math.min(maxW / this.vc.width, maxH / this.vc.height)));
    this.scale = scale;
    var cssW = this.vc.width * scale;
    var cssH = this.vc.height * scale;
    this.el.style.width = cssW + "px";
    this.el.style.height = cssH + "px";
    this.el.width = Math.round(cssW * dpr);
    this.el.height = Math.round(cssH * dpr);
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  };
  LedRenderer.prototype.draw = function () {
    var ctx = this.ctx;
    var scale = this.scale;
    var radius = Math.max(1, scale / 2 - 1);
    ctx.fillStyle = "#000000";
    ctx.fillRect(0, 0, this.vc.width * scale, this.vc.height * scale);
    for (var y = 0; y < this.vc.height; y += 1) {
      for (var x = 0; x < this.vc.width; x += 1) {
        var rgb = this.vc.getPixel(x, y);
        var isOff = rgb[0] === 0 && rgb[1] === 0 && rgb[2] === 0;
        var color = isOff ? UNLIT_COLOR : rgb;
        ctx.beginPath();
        ctx.fillStyle = "rgb(" + color[0] + "," + color[1] + "," + color[2] + ")";
        ctx.arc(x * scale + scale / 2, y * scale + scale / 2, radius, 0, Math.PI * 2);
        ctx.fill();
      }
    }
  };

  // ---------------------------------------------------------------------
  // Data fetching
  // ---------------------------------------------------------------------

  // Resolves straight to a URL rather than pre-fetching stop IDs client-side: the
  // code-keyed endpoint (/v1/transloc/stop_arrivals/{code}) resolves the code to its
  // current stop_ids server-side on every poll, so a code repointed at /feed-codes
  // takes effect immediately without reloading this page, and there's no separate
  // /v1/feed-codes round trip to do it.
  function resolveArrivalsUrl() {
    var code = qp("code", null);
    var stopIDsParam = qp("stopIDs", null) || qp("stopIds", null);

    if (stopIDsParam) {
      return "/v1/transloc/stop_arrivals?stopIDs=" + encodeURIComponent(stopIDsParam);
    }
    if (code) {
      return "/v1/transloc/stop_arrivals/" + encodeURIComponent(code.trim());
    }
    return null;
  }

  function fetchArrivals(onDone, onError) {
    var url = resolveArrivalsUrl();
    if (!url) {
      console.error("[countdown] no ?code= or ?stopIDs= given");
      onDone([]);
      return;
    }
    fetch(url)
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        onDone(normalize(Array.isArray(data) ? data : []));
      })
      .catch(onError);
  }

  // Derives the matching /v1/transloc/stop_alert(/{code} or ?stopIDs=...) URL from the
  // arrivals URL by swapping the path segment -- both endpoints accept the same /{code}
  // or ?stopIDs= form, so this is a straightforward substring replace.
  function fetchAlert(onDone) {
    var arrivalsUrl = resolveArrivalsUrl();
    if (!arrivalsUrl) {
      onDone(null);
      return;
    }
    var alertUrl = arrivalsUrl.replace("stop_arrivals", "stop_alert");
    fetch(alertUrl)
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        onDone((data && data.alert) || null);
      })
      .catch(function (err) {
        // Swallowed, not surfaced via onError: a broken alert fetch shouldn't affect
        // the arrivals display -- the sign just shows no alert that poll.
        console.error("[countdown] alert fetch failed, ignoring", err);
        onDone(null);
      });
  }

  // ---------------------------------------------------------------------
  // Main loop (mirrors driver/app.py's CountdownApp)
  // ---------------------------------------------------------------------

  function main() {
    var pollIntervalMs = parseInt(qp("poll", "15000"), 10);
    var pageDurationMs = parseInt(qp("page", "4000"), 10);

    var canvasEl = document.getElementById("sign");
    var vc = new VirtualCanvas(CANVAS_W, CANVAS_H);
    var renderer = new LedRenderer(canvasEl, vc);

    var state = {
      arrivals: [],
      alert: null,
      page: 0,
      lastFetch: -Infinity,
      lastPageChange: -Infinity,
      font: null,
    };

    function refetch(now) {
      state.lastFetch = now;
      fetchArrivals(
        function (arrivals) {
          state.arrivals = arrivals;
        },
        function (err) {
          console.error("[countdown] fetch failed, keeping last known data", err);
        }
      );
      fetchAlert(function (alert) {
        state.alert = alert;
      });
    }

    function tick() {
      if (!state.font) return;
      var now = Date.now();

      if (now - state.lastFetch >= pollIntervalMs) {
        refetch(now);
      }

      var totalPages = pageCount(state.arrivals.length);
      if (state.lastPageChange === -Infinity) {
        state.lastPageChange = now;
      } else if (now - state.lastPageChange >= pageDurationMs) {
        state.page = (state.page + 1) % totalPages;
        state.lastPageChange = now;
      }
      if (state.page >= totalPages) state.page = 0;

      renderPage(vc, state.font, state.arrivals, state.page, now, state.alert);
      renderer.draw();
    }

    fetch("/fonts/mta-sign.bdf")
      .then(function (r) { return r.text(); })
      .then(function (text) {
        state.font = parseBdf(text);
        refetch(Date.now());
        tick();
        setInterval(tick, TICK_MS);
      })
      .catch(function (err) {
        console.error("[countdown] failed to load font", err);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
