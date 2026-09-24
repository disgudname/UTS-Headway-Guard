// livemap/ui/trip-planner-panel.js
// -----------------------------------------------------------------------------
// Trip planner UI: a collapsed pill (bottom-left, clear of the search box at
// top-centre and the Panels columns at the left/right edges) that expands into
// an origin/destination picker + ranked itinerary results.
//
// Four ways to set a point, all feeding core/trip-planner.js's setOrigin /
// setDestination:
//   * type a building name (autocomplete via /v1/uva/facility_search, same
//     lookup ui/search.js already uses)
//   * type an off-Grounds address/place name (autocomplete via /v1/search/geocode,
//     same self-hosted Nominatim lookup ui/search.js already uses)
//   * "Use my location" (browser geolocation) -- origin field only
//   * "Click map" mode -- toggles a one-shot map click listener
// -----------------------------------------------------------------------------

import { API_BASE } from '../core/config.js';
import { getMap } from '../core/map.js';
import { debounce, parseColor, luminance, lsGet, lsSet } from '../core/util.js';
import { onCatEnabled } from '../core/data/cat.js';
import { getStops as getUtsStops, getRouteName } from '../core/data/transloc.js';
import { getRouteVisibility, setRouteHidden } from '../core/layers/routes.js';
import * as TripPlanner from '../core/trip-planner.js';

const MIN_CHARS = 2;
const DEBOUNCE_MS = 220;
const NEGLIGIBLE_WALK_M = 20;
const MAX_FIELD_STOPS = 6;

// Small inline icon set (stroke-based, currentColor) -- kept as raw markup rather than
// CSS ::before content so they scale/recolor cleanly and don't need a font/sprite sheet.
const ICONS = {
  locate:
    '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><circle cx="8" cy="8" r="2.3"/><path d="M8 1v2.6M8 12.4V15M1 8h2.6M12.4 8H15"/></svg>',
  pin:
    '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"><path d="M8 15S3 10.2 3 6.5a5 5 0 0 1 10 0C13 10.2 8 15 8 15Z"/><circle cx="8" cy="6.5" r="1.6"/></svg>',
  swap:
    '<svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 3v10M5 3 2.6 5.5M5 3l2.4 2.5"/><path d="M11 13V3M11 13l2.4-2.5M11 13 8.6 10.5"/></svg>',
  warn:
    '<svg viewBox="0 0 16 16" width="11" height="11" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"><path d="M8 1.6 14.8 14H1.2L8 1.6Z"/><path d="M8 6.2v3.4"/><circle cx="8" cy="11.6" r="0.15" fill="currentColor" stroke="none"/></svg>',
  back:
    '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M10 3 5 8l5 5"/></svg>',
  close:
    '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M4 4l8 8M12 4l-8 8"/></svg>',
  recent:
    '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="8.5" r="5.5"/><path d="M8 5.5V8.5L10.2 10"/></svg>',
  // Dot + three arcs growing outward toward the upper-right, each pulsing in
  // sequence -- the "this is a live GPS-based estimate" signal, not shown for
  // a scheduled/estimated one (see waitSourceBadgeHtml). Path data (not a
  // dasharray-clipped <circle>, which an earlier version of this tried --
  // produced straight bar-like shapes instead of arcs when actually rendered)
  // verified visually before shipping: for a quarter-circle from directly
  // right of the dot to directly above it, large-arc-flag=0 sweep-flag=0 is
  // the combination that bulges outward (away from the dot) the way a wifi
  // icon does -- sweep=1 produces a completely different, wrong shape.
  live:
    '<svg viewBox="0 0 16 16" width="10" height="10" fill="none" aria-hidden="true">' +
    '<circle cx="4" cy="13" r="1.5" fill="currentColor"/>' +
    '<path class="tp-live-arc tp-live-arc--1" d="M7 13A3 3 0 0 0 4 10" stroke="currentColor" stroke-width="1.6" fill="none" stroke-linecap="round"/>' +
    '<path class="tp-live-arc tp-live-arc--2" d="M9 13A5 5 0 0 0 4 8" stroke="currentColor" stroke-width="1.6" fill="none" stroke-linecap="round"/>' +
    '<path class="tp-live-arc tp-live-arc--3" d="M11 13A7 7 0 0 0 4 6" stroke="currentColor" stroke-width="1.6" fill="none" stroke-linecap="round"/>' +
    '</svg>',
};

// Locally-cached previous picks, most-recent-first -- shown in a field's dropdown
// when it's focused with nothing typed, Google-Maps style (see _showEmptyStateResults).
// Shared between the origin and destination fields: a place picked as a
// destination once is just as likely to be searched as an origin later.
const RECENTS_KEY = 'livemap.tripplanner.recents.v1';
const MAX_RECENTS = 6;

function loadRecentPoints() {
  try {
    const arr = JSON.parse(lsGet(RECENTS_KEY, '[]'));
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

/** Bumps `point` to the front, dedupes on label+coords, caps the list. Skips
 *  the synthetic "My location"/"Dropped pin" labels -- those describe how a
 *  point was set, not a place worth resurfacing as a searchable recent. */
function saveRecentPoint(point) {
  if (!point || !point.label || point.label === 'My location' || point.label === 'Dropped pin') return;
  const list = loadRecentPoints().filter(
    (p) => !(p.label === point.label && p.lat === point.lat && p.lng === point.lng),
  );
  list.unshift({ lat: point.lat, lng: point.lng, label: point.label });
  lsSet(RECENTS_KEY, JSON.stringify(list.slice(0, MAX_RECENTS)));
}

export class TripPlannerPanel {
  /** @param {HTMLElement} [parent] where to mount just the collapsed toggle button --
   *  normally ui/map-controls.js's `tripPlannerSlot`, so it renders as part of that
   *  bottom-right cluster. The card and full-screen search overlay always mount to
   *  document.body regardless: both are `position: fixed` against the viewport (see
   *  below for why), and a `transform` on any ancestor -- which the cluster needs
   *  for its own slide-with-the-right-panel animation -- becomes the containing
   *  block for `position: fixed` descendants, confirmed live, the card once
   *  collapsed to a sliver positioned relative to the toggle instead of the
   *  viewport when it shared a transformed ancestor with it. */
  mount(parent = document.body) {
    const el = document.createElement('div');
    el.className = 'tp-widget';
    el.innerHTML = `
      <button type="button" class="tp-toggle" aria-expanded="false" aria-label="Plan a trip">
        <span class="tp-toggle-icon" aria-hidden="true">${ICONS.pin}</span>
        <span class="tp-toggle-label">Plan a trip</span>
      </button>`;

    const cardEl = document.createElement('div');
    cardEl.className = 'tp-card';
    cardEl.hidden = true;
    cardEl.innerHTML = `
      <button type="button" class="tp-drag-handle" aria-label="Expand or collapse"><span></span></button>
      <div class="tp-card-head">
        <button type="button" class="tp-back" aria-label="Back to trip options" hidden>${ICONS.back}</button>
        <span class="tp-card-title">Plan a trip</span>
        <button type="button" class="tp-close" aria-label="Close">${ICONS.close}</button>
      </div>
      <div class="tp-fields">
        <div class="tp-field" data-field="origin">
          <span class="tp-field-badge tp-field-badge--origin">A</span>
          <input type="text" placeholder="Origin — building, stop, or address" autocomplete="off" spellcheck="false" />
          <div class="tp-field-results" hidden></div>
        </div>
        <div class="tp-field" data-field="destination">
          <span class="tp-field-badge tp-field-badge--destination">B</span>
          <input type="text" placeholder="Destination — building, stop, or address" autocomplete="off" spellcheck="false" />
          <div class="tp-field-results" hidden></div>
        </div>
        <button type="button" class="tp-swap" title="Swap origin and destination" aria-label="Swap origin and destination">${ICONS.swap}</button>
      </div>
      <div class="tp-when">
        <button type="button" class="tp-when-btn is-active" data-when="now">Now</button>
        <button type="button" class="tp-when-btn" data-when="later">Later…</button>
        <input type="datetime-local" class="tp-when-input" hidden />
      </div>
      <div class="tp-results" hidden></div>
      <div class="tp-detail-footer" hidden></div>`;

    // Mobile-only, full-screen search takeover (see MOBILE_BREAKPOINT below): on a
    // phone-width bottom sheet there's no room to show more than a sliver of a
    // field's autocomplete dropdown (confirmed live -- the sheet's own overflow:
    // hidden clipped all but ~1 row of it), and cramming a real destination-picking
    // flow into that little space reads nothing like it does on every map app
    // people already know. Matches Google Maps: tapping a field goes to a
    // dedicated full-screen search-only view; picking a result returns to the
    // previous screen with both fields visible. Implemented by RELOCATING the
    // tapped field's whole `.tp-field` element (input + buttons + its results
    // dropdown, which is already a child of it) into this overlay, then moving it
    // straight back on close -- reuses every bit of existing field logic
    // (search/pick/locate/pin) instead of duplicating it for a second UI.
    const overlayEl = document.createElement('div');
    overlayEl.className = 'tp-search-overlay';
    overlayEl.hidden = true;
    overlayEl.innerHTML = `
      <div class="tp-search-overlay-head">
        <button type="button" class="tp-search-overlay-back" aria-label="Back">${ICONS.back}</button>
      </div>
      <div class="tp-search-overlay-body"></div>`;
    overlayEl.querySelector('.tp-search-overlay-back').addEventListener('click', () => this._closeOverlay());

    this._el = el;
    this._cardEl = cardEl;
    this._toggle = el.querySelector('.tp-toggle');
    this._card = cardEl;
    this._overlayEl = overlayEl;
    this._overlayHead = overlayEl.querySelector('.tp-search-overlay-head');
    this._overlayBody = overlayEl.querySelector('.tp-search-overlay-body');
    this._overlayField = null;
    this._overlayCloseFinish = null; // pending close-animation finisher, if one is in flight
    this._results = cardEl.querySelector('.tp-results');
    this._detailFooter = cardEl.querySelector('.tp-detail-footer');
    this._backBtn = cardEl.querySelector('.tp-back');
    this._cardTitle = cardEl.querySelector('.tp-card-title');
    this._whenInput = cardEl.querySelector('.tp-when-input');
    this._pickMode = null; // 'origin' | 'destination' | null
    this._sheetDragging = false; // true while a mobile sheet drag owns the gesture -- see _wireDragHandle
    this._when = null; // Date | null ("now")
    this._view = 'list'; // 'list' | 'detail' -- which content _results/_detailFooter show
    this._routesHiddenForPlanning = null; // route IDs hidden while planning, to restore
    this._itineraries = [];

    this._fields = {};
    for (const field of ['origin', 'destination']) {
      const wrap = cardEl.querySelector(`.tp-field[data-field="${field}"]`);
      this._fields[field] = {
        wrap,
        input: wrap.querySelector('input'),
        results: wrap.querySelector('.tp-field-results'),
        items: [],
        reqSeq: 0,
      };
      this._wireField(field);
    }

    cardEl.querySelector('.tp-swap').addEventListener('click', () => this._swap());

    // Mobile-only bottom sheet: the handle is a real drag, not just a toggle -- see
    // _wireDragHandle. Harmless on desktop, where the handle itself is hidden via CSS
    // and pointer events on it never fire.
    this._wireDragHandle(cardEl.querySelector('.tp-drag-handle'), cardEl.querySelector('.tp-card-head'));

    this._toggle.addEventListener('click', () => this._setExpanded(true));
    cardEl.querySelector('.tp-close').addEventListener('click', () => {
      this._closeOverlay();
      this._stopPicking(false);
      // clearAll() BEFORE _setExpanded(false): it synchronously fires the 'idle'
      // status, which _renderStatus resets the sheet height for (_setView calls
      // _resetDragHeight()) -- confirmed live, running it AFTER _setExpanded set
      // up the close (slide-down-then-hide) animation wiped the just-set height:0
      // back to '' before the browser ever painted that frame, so the sheet just
      // snapped shut with no visible animation at all. Settling every OTHER state
      // change first means _setExpanded's own height assignment is the last word.
      TripPlanner.clearAll();
      this._setExpanded(false);
    });
    this._backBtn.addEventListener('click', () => this._showList());
    // Tapping the collapsed synopsis bar expands it, same as tapping a selected
    // option's summary in Maps to reveal its full step-by-step breakdown.
    this._detailFooter.addEventListener('click', () => {
      this._card.classList.add('is-expanded');
    });

    for (const btn of cardEl.querySelectorAll('.tp-when-btn')) {
      btn.addEventListener('click', () => this._setWhenMode(btn.dataset.when));
    }
    this._whenInput.addEventListener('change', () => {
      const v = this._whenInput.value;
      this._when = v ? new Date(v) : null;
      this._replan();
    });

    this._unsubOrigin = TripPlanner.onOrigin((p) => this._syncFieldValue('origin', p));
    this._unsubDestination = TripPlanner.onDestination((p) => this._syncFieldValue('destination', p));
    this._unsubStatus = TripPlanner.onStatus((s) => this._renderStatus(s));
    this._unsubItineraries = TripPlanner.onItineraries((its) => this._renderItineraries(its));
    this._unsubSelected = TripPlanner.onSelected((i) => this._syncSelected(i));

    // Re-run a shown plan when the rider flips the map's CAT toggle mid-session, so
    // results stay consistent with what "CAT on/off" actually means instead of
    // leaving a stale plan from before the toggle changed. onCatEnabled replays the
    // current value immediately on subscribe, which must NOT trigger a plan itself.
    let firstCatState = true;
    this._unsubCatEnabled = onCatEnabled(() => {
      if (firstCatState) {
        firstCatState = false;
        return;
      }
      this._replan();
    });

    // The desktop sidebar and mobile bottom sheet permanently cover part of
    // the map -- fitToItinerary needs to know how much so a selected route
    // isn't fit against the FULL canvas and end up with a chunk of that fit
    // hidden behind this panel (confirmed live: routes always looked too
    // zoomed in, since the actually-visible remainder was smaller than what
    // the fit assumed).
    TripPlanner.setBoundsPadding(() => this._computeFitPadding());

    (parent || document.body).appendChild(el);
    document.body.appendChild(cardEl);
    document.body.appendChild(overlayEl);
    return this;
  }

  unmount() {
    this._setRoutesHiddenForPlanning(false); // don't leave routes hidden behind us
    this._unsubOrigin?.();
    this._unsubDestination?.();
    this._unsubStatus?.();
    this._unsubItineraries?.();
    this._unsubSelected?.();
    this._unsubCatEnabled?.();
    this._closeOverlay();
    this._stopPicking(false);
    this._el?.remove();
    this._cardEl?.remove();
    this._overlayEl?.remove();
  }

  /** Registers a single callback fired whenever the panel opens (toggle click,
   *  or openForDestination below) -- e.g. search.js clears its contextual
   *  "Navigate here" button once the planner it points at is actually up. */
  onExpand(fn) {
    this._onExpand = fn;
  }

  /** Public entry point for ui/search.js's "Navigate here" button on a selected
   *  building: pre-fills the destination and puts the rider straight into
   *  picking an origin, mirroring Maps' "Directions" action from a place card. */
  openForDestination(point) {
    if (this._cardEl.hidden) this._setExpanded(true);
    else this._onExpand?.(); // already open -- _setExpanded(true) (and its hook call) won't run
    TripPlanner.setDestination(point);
    // Let the just-opened sheet/sidebar finish laying out before moving focus --
    // particularly on mobile, where _setExpanded's slide-up animation is still
    // running and the origin field may not even be at its final size yet.
    requestAnimationFrame(() => this._fields.origin.input.focus());
  }

  /** Mobile bottom sheet only (no-op everywhere the handle is display:none, i.e.
   *  desktop). Real drag-to-resize -- the sheet follows the pointer 1:1 while
   *  dragging, then snaps to whichever "detent" (candidate resting height) is
   *  closest once released. Which detents apply depends on what's currently
   *  showing, per the user's ask for detents "based on what you're interacting
   *  with": the list view (fields + itinerary results) gets a peek/half/full set,
   *  while the detail view (one itinerary's step-by-step breakdown) only ever had
   *  two states to begin with, so it keeps just peek/full. A near-zero-movement
   *  pointerdown+up (a plain tap, not a drag) jumps straight between the smallest
   *  and largest detent -- the same gesture the old toggle-only handle supported,
   *  so tapping still works for anyone who doesn't drag at all. */
  /** `triggers` are every element that should act as the drag surface -- not just
   *  the thin grip strip. User's ask: the whole navy top bar (grip strip + "Plan a
   *  trip"/"Trip details" header) should be draggable, matching how it already
   *  reads as one continuous bar visually (see the drag-handle navy-background fix
   *  above) -- not just a sliver of it functionally. The back/close buttons living
   *  inside that header still need to work as plain taps, so a pointerdown that
   *  lands on one of them is left alone entirely (untouched by drag or the
   *  tap-toggle below) rather than trying to distinguish "tap the button" from
   *  "tap-not-drag the header" after the fact. */
  _wireDragHandle(...triggers) {
    const TAP_THRESHOLD_PX = 6;
    let drag = null;
    for (const trigger of triggers) {
      trigger.addEventListener('pointerdown', (e) => {
        // .tp-card-head is also the always-visible desktop sidebar title bar (no
        // drag/detent concept there -- see _isMobileLayout's comment) -- without
        // this guard, grabbing it on desktop would still kick off a drag and pin
        // an inline height onto a sidebar that's meant to just stretch full height.
        if (!this._isMobileLayout()) return;
        if (e.target.closest('.tp-back, .tp-close')) return;
        e.currentTarget.setPointerCapture(e.pointerId);
        this._sheetDragging = true;
        drag = {
          pointerId: e.pointerId,
          captureEl: e.currentTarget,
          startY: e.clientY,
          startHeight: this._cardEl.getBoundingClientRect().height,
          detents: this._computeSheetDetents(),
        };
        this._cardEl.classList.add('tp-card--dragging');
        // The base (non-.is-expanded) rule caps height at max-height:46vh -- fine
        // for the plain two-state toggle, but it silently clamps a dragged/snapped
        // height that's meant to land anywhere between the peek and full detents
        // (confirmed live: dragged to the ~50vh "half" detent, inline height was
        // set correctly, but the box still rendered at 46vh because max-height is
        // a separate constraint that setting `height` doesn't override).
        // Neutralized for as long as a manual height is in effect;
        // _resetDragHeight() puts it back.
        this._cardEl.style.maxHeight = 'none';
      });
      trigger.addEventListener('pointermove', (e) => {
        if (!drag || e.pointerId !== drag.pointerId) return;
        const dy = e.clientY - drag.startY; // dragging UP (negative dy) grows the sheet
        const min = drag.detents[0];
        const max = drag.detents[drag.detents.length - 1];
        const next = Math.min(max, Math.max(min, drag.startHeight - dy));
        this._cardEl.style.height = `${next}px`;
      });
      const endDrag = (e) => {
        if (!drag || e.pointerId !== drag.pointerId) return;
        drag.captureEl.releasePointerCapture(drag.pointerId);
        const current = this._cardEl.getBoundingClientRect().height;
        const detents = drag.detents;
        const moved = Math.abs(current - drag.startHeight);
        let target;
        if (moved < TAP_THRESHOLD_PX) {
          const mid = (detents[0] + detents[detents.length - 1]) / 2;
          target = current < mid ? detents[detents.length - 1] : detents[0];
        } else {
          target = detents.reduce((best, d) => (Math.abs(d - current) < Math.abs(best - current) ? d : best));
        }
        this._cardEl.classList.remove('tp-card--dragging');
        this._cardEl.style.height = `${target}px`;
        this._card.classList.toggle('is-expanded', target === detents[detents.length - 1]);
        drag = null;
        // Deferred, not immediate: see the input 'focus' listener's comment -- a
        // stray native focus tied to this gesture can land AFTER pointerup fires,
        // so the guard needs to still be up for one more tick.
        setTimeout(() => { this._sheetDragging = false; }, 0);
      };
      trigger.addEventListener('pointerup', endDrag);
      trigger.addEventListener('pointercancel', endDrag);
    }
  }

  /** Detent heights in px, largest last. Measured from the actual content of the
   *  current view rather than guessed viewport fractions where that content's size
   *  is what defines a sensible "peek" -- e.g. detail view's peek is exactly the
   *  drag handle + synopsis footer (the only things shown there), whatever their
   *  real rendered height happens to be, not an arbitrary number that could drift
   *  out of sync with a font-size or content change. */
  _computeSheetDetents() {
    const vh = window.innerHeight;
    const full = vh * 0.85;
    const top = this._cardEl.querySelector('.tp-drag-handle').getBoundingClientRect().top;
    // The direct pixel span from the handle's top to the last visible element's
    // bottom -- NOT a sum of each child's own offsetHeight. offsetHeight excludes
    // an element's external margin, and .tp-when (like .tp-fields) carries a
    // top margin for spacing between rows -- summing offsetHeights silently
    // dropped that gap, confirmed live: the Now/Later row was clipped on first
    // open because the computed "peek" came out ~10px shorter than the content
    // actually needed. A bounding-rect span can't miss a gap like that; it's
    // measuring the real rendered layout, not reconstructing it from parts.
    const bottomEl = this._view === 'detail' ? this._detailFooter : this._cardEl.querySelector('.tp-when');
    const bottom = bottomEl.getBoundingClientRect().bottom;
    // +4px: a small cushion for the card's own border/padding below the last
    // element, which this span (deliberately) doesn't otherwise account for.
    const peek = bottom - top + 4;
    // Sorted + deduped rather than a flat [peek, half, full]: `peek` used to be
    // capped at a flat 46vh, which on a short viewport could be SMALLER than the
    // content it's meant to fit. Only capped against `full` now (a detent taller
    // than the sheet's own max makes no sense); if that still leaves peek bigger
    // than the nominal "half" checkpoint, sorting keeps the three detents in
    // valid ascending order instead of an invalid peek > half.
    const half = vh * 0.5;
    return [...new Set([Math.min(peek, full), half, full].sort((a, b) => a - b))];
  }

  /** How much of each map edge this panel's own chrome is covering right now,
   *  for TripPlanner.setBoundsPadding -- see the registration comment in
   *  mount() for why fitToItinerary needs this.
   *
   *  Deliberately does NOT read `.tp-card`'s own getBoundingClientRect(): its
   *  height is what's mid-transition right when a fit is triggered (adding/
   *  removing .is-expanded happens the same tick as selecting an itinerary),
   *  and a synchronous read in that same tick still reflects the PRE-change
   *  height, not the target one -- the same reason _setExpanded has to pin a
   *  frame and force a reflow rather than just trusting a plain read. Instead
   *  this reuses _computeSheetDetents' peek/full heights above, which measure
   *  fixed content elements (handle/fields/when/detail-footer) that don't
   *  themselves resize as the card animates, so they're valid immediately
   *  regardless of transition state. */
  _computeFitPadding() {
    if (this._isMobileLayout()) {
      if (this._cardEl.hidden) return { top: 24, bottom: 24, left: 24, right: 24 };
      const detents = this._computeSheetDetents();
      // A fit is triggered from exactly two places: auto-selecting the first
      // result right as it lands (list view, sheet expands to full) or
      // tapping a result for its detail (sheet collapses to peek) -- use
      // whichever this._view implies instead of the mid-transition height.
      const target = this._view === 'detail' ? detents[0] : detents[detents.length - 1];
      return { top: 24, bottom: target + 16, left: 24, right: 24 };
    }
    // Desktop: the sidebar is a fixed-width, always-full-height dock (see
    // .tp-card's own CSS) -- no measurement needed, it covers the same strip
    // of the left edge regardless of view state.
    return { top: 70, bottom: 70, left: 350, right: 70 };
  }

  /** Drops any height a manual drag pinned inline, so the next programmatic state
   *  change (a fresh plan landing, switching to detail, etc.) goes back to sizing
   *  itself off the plain is-expanded class/CSS instead of being stuck at wherever
   *  the sheet was last dragged to. */
  _resetDragHeight() {
    this._cardEl.style.height = '';
    this._cardEl.style.maxHeight = '';
  }

  _setExpanded(open) {
    this._setRoutesHiddenForPlanning(open); // real state change happens immediately either way
    if (open) {
      this._onExpand?.(); // e.g. search.js clearing its contextual "Navigate here" button
      this._resetDragHeight(); // reopen at the plain default, not wherever it was last dragged to
      this._toggle.setAttribute('aria-expanded', 'true');
      this._toggle.hidden = true; // hide right away -- the sheet is what's animating in now
      this._card.hidden = false;
      if (this._isMobileLayout()) {
        // CSS's static max-height:46vh is a guess that doesn't necessarily fit
        // this content on a shorter viewport -- confirmed live, it clipped the
        // Now/Later row on first open. Un-hidden above, so offsetHeight reads
        // inside _computeSheetDetents() now return real numbers -- measure the
        // real target BEFORE collapsing to 0 below, since a 0-height flex column
        // can shrink its children along with it and throw the measurement off.
        const targetHeight = this._computeSheetDetents()[0];
        // Slide up from nothing rather than snapping straight to full height: a
        // freshly-unhidden element has no previous rendered frame for .tp-card's
        // own `transition: height` to interpolate from, so setting the target
        // height in the same tick as un-hiding just appears instantly at that
        // height instead of animating in. Pin height to 0 with the transition
        // suspended, force a layout flush so the browser commits that as a real
        // frame, then hand back to the CSS transition for the actual target --
        // the standard way to animate an element in from a hidden state.
        this._cardEl.style.transition = 'none';
        this._cardEl.style.maxHeight = 'none';
        this._cardEl.style.height = '0px';
        void this._cardEl.offsetHeight; // force reflow -- commits the 0px frame
        this._cardEl.style.transition = '';
        this._cardEl.style.height = `${targetHeight}px`;
      }
      return;
    }

    this._toggle.setAttribute('aria-expanded', 'false');
    if (!this._isMobileLayout() || this._cardEl.hidden) {
      // Desktop has no slide concept for this sidebar; already-hidden needs no
      // animation either.
      this._card.hidden = true;
      this._toggle.hidden = false;
      return;
    }
    // Mirror the open animation on the way out -- an instant vanish read as
    // jarring right next to a sheet that now visibly slides in. Pin the CURRENT
    // rendered height first (it may be CSS-driven/"none" rather than an explicit
    // inline px value) so there's a real starting frame for the transition down
    // to 0, then actually hide once that transition finishes.
    this._cardEl.style.maxHeight = 'none';
    this._cardEl.style.height = `${this._cardEl.getBoundingClientRect().height}px`;
    void this._cardEl.offsetHeight;
    this._cardEl.style.height = '0px';
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      this._card.hidden = true;
      this._toggle.hidden = false;
      this._resetDragHeight();
    };
    this._cardEl.addEventListener('transitionend', finish, { once: true });
    // Fallback in case transitionend never fires (e.g. reduced-motion collapses
    // --dur low enough that the browser treats it as a no-op transition).
    setTimeout(finish, 300);
  }

  /** While trip planning is open, hide every ambient route (and, as a side
   *  effect, the ambient stop layer -- see core/layers/stops.js, a stop is
   *  dropped once every route serving it is hidden) so the only thing on the
   *  map is the itinerary being planned -- core/trip-planner.js draws that
   *  itinerary's own route + stops on its own dedicated layer regardless of
   *  this. Remembers exactly which routes were shown so closing restores that
   *  same set (including an idle route the rider had pinned on), rather than
   *  just reverting to "show everything." */
  _setRoutesHiddenForPlanning(hide) {
    if (hide) {
      if (this._routesHiddenForPlanning) return; // already applied
      this._routesHiddenForPlanning = getRouteVisibility()
        .filter((r) => r.shown)
        .map((r) => r.id);
      for (const id of this._routesHiddenForPlanning) setRouteHidden(id, true);
    } else {
      if (!this._routesHiddenForPlanning) return;
      for (const id of this._routesHiddenForPlanning) setRouteHidden(id, false);
      this._routesHiddenForPlanning = null;
    }
  }

  _wireField(field) {
    const f = this._fields[field];
    f.debouncedSearch = debounce((q) => this._searchField(field, q), DEBOUNCE_MS);
    f.input.addEventListener('input', () => {
      const q = f.input.value.trim();
      if (!q) {
        // Cancel rather than let a just-superseded debounced search call land
        // a beat later and clobber this render with stale stop/building results.
        f.debouncedSearch.cancel();
        this._showEmptyStateResults(field);
        return;
      }
      f.debouncedSearch(q);
    });
    f.input.addEventListener('focus', () => {
      // Confirmed live: a real (non-synthetic) drag on the header bar can make the
      // browser fire a genuine, un-scripted 'focus' event on this input mid-drag --
      // stack trace showed a bare 2-frame native origin, not a call from anywhere
      // in this file, even though every pointer event for the whole gesture still
      // correctly targeted .tp-card-head throughout (capture was never lost). A
      // synthetic PointerEvent-dispatch reproduction of the same gesture did NOT
      // trigger it, so this looks like a platform/input-stack quirk specific to a
      // real held-button drag, not a bug in the drag code's own event handling.
      // Whatever the exact cause, focus landing here while a sheet drag owns the
      // gesture is never something a user actually meant -- blur it back out
      // rather than opening the full-screen search mid-drag.
      if (this._sheetDragging) {
        f.input.blur();
        return;
      }
      // _maybeOpenOverlay does the move AND the empty-state populate itself now
      // (see its own comment) -- still reported missing live even after moving
      // the DOM before populating, so the populate call moved again, further in.
      const populated = this._maybeOpenOverlay(field);
      // Desktop (no overlay) and an already-open mobile overlay both bail out of
      // _maybeOpenOverlay before its populate step, so do it here -- otherwise the
      // Your Location / Choose on map / Recent list never appears on desktop.
      if (!populated && !f.input.value.trim()) this._showEmptyStateResults(field);
    });
    // Clicking an already-focused, still-empty field after its list was dismissed
    // (click-outside, Escape) fires no new 'focus' -- reopen the list on click.
    f.input.addEventListener('click', () => {
      if (!f.input.value.trim() && f.results.hidden) this._showEmptyStateResults(field);
    });
    f.input.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (this._overlayField === field) this._closeOverlay();
      else this._closeFieldResults(field);
    });
    // f.wrap and f.results are siblings once the mobile overlay is open (see
    // _maybeOpenOverlay -- wrap goes into the overlay head, results into its
    // body, two separate containers), so a click landing on a *result row*
    // isn't inside f.wrap at all. Checking f.wrap alone means the click that
    // bubbles up from tapping a row (after that row's own handler already ran)
    // reads as "outside" and closes the list right as it's being used --
    // the likely explanation for the empty-state list reportedly flashing
    // closed and reopening on its own.
    document.addEventListener('click', (e) => {
      if (!f.wrap.contains(e.target) && !f.results.contains(e.target)) this._closeFieldResults(field);
    });
  }

  /** Phone-width bottom sheet only -- see mount()'s comment on the overlay. Desktop's
   *  sidebar is tall enough for the inline dropdown as-is, so this is a deliberate
   *  no-op there (matches the same 768px breakpoint the CSS already uses throughout
   *  this widget). */
  _isMobileLayout() {
    return window.matchMedia('(max-width: 768px)').matches;
  }

  _maybeOpenOverlay(field) {
    if (!this._isMobileLayout() || this._overlayField === field) return false;
    // A close animation for the OTHER field might still be in flight (rapid tap
    // from one field to another) -- finish it synchronously (no animation, just
    // the cleanup) rather than leaving two fields' worth of DOM parented inside
    // the overlay at once.
    if (this._overlayCloseFinish) this._overlayCloseFinish();
    const f = this._fields[field];
    this._overlayField = field;
    this._overlayReturn = { parent: f.wrap.parentNode, next: f.wrap.nextSibling };
    f.wrap.classList.add('tp-field--overlay');
    f.results.classList.add('tp-field-results--overlay');
    this._overlayHead.appendChild(f.wrap);
    this._overlayBody.appendChild(f.results);
    this._overlayEl.hidden = false;
    // Slide in from the right (a screen being pushed, matching the back arrow) --
    // same "pin a start frame, flush, hand off to the CSS transition" pattern as
    // the bottom sheet's own open animation, needed for the same reason: a
    // freshly-unhidden element has no previous frame to transition from.
    this._overlayEl.style.transition = 'none';
    this._overlayEl.style.transform = 'translateX(100%)';
    // Populate HERE -- while the overlay is parked off-screen with its transition
    // disabled, not after kicking off the slide-in below. Moving the DOM before
    // populating (the previous fix) wasn't enough: it was still reported missing
    // live on a field's first mobile open, keyboard up, nothing switched apps to
    // force a repaint. Root cause looks like a real WebView/Chrome-mobile compositor
    // bug, not a plain paint-order issue -- mutating a descendant's content while
    // its ancestor has an in-flight CSS transform *transition* can fail to composite
    // until something external forces a fresh frame (an app switch is a textbook
    // trigger for that). Painting the field's final content while fully off-screen
    // and untransitioning, THEN animating that already-rendered layer into view via
    // transform alone, sidesteps the bug class entirely instead of adding another
    // forced-reflow band-aid on top of the one already in _renderFieldResults.
    if (!f.input.value.trim()) this._showEmptyStateResults(field);
    void this._overlayEl.offsetHeight;
    this._overlayEl.style.transition = '';
    this._overlayEl.style.transform = 'translateX(0)';
    f.input.focus();
    return true;
  }

  _closeOverlay() {
    const field = this._overlayField;
    if (!field) return;
    const f = this._fields[field];
    f.input.blur();
    if (!this._isMobileLayout() || this._overlayEl.hidden) {
      this._finishCloseOverlay(field, f);
      return;
    }
    // Slide back out to the right (a screen being popped), then actually detach
    // once that finishes -- mirrors the open animation, and keeps the field's
    // DOM visually leaving WITH the overlay instead of jumping back into the
    // main sheet a beat before the overlay has finished sliding away.
    this._overlayEl.style.transform = 'translateX(100%)';
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      this._overlayCloseFinish = null;
      this._finishCloseOverlay(field, f);
    };
    this._overlayCloseFinish = finish;
    this._overlayEl.addEventListener('transitionend', finish, { once: true });
    // Fallback in case transitionend never fires (e.g. reduced-motion collapses
    // --dur low enough that the browser treats it as a no-op transition).
    setTimeout(finish, 300);
  }

  _finishCloseOverlay(field, f) {
    f.wrap.classList.remove('tp-field--overlay');
    f.results.classList.remove('tp-field-results--overlay');
    const { parent, next } = this._overlayReturn || {};
    parent?.insertBefore(f.wrap, next || null);
    this._overlayEl.hidden = true;
    this._overlayEl.style.transform = '';
    this._overlayField = null;
    this._overlayReturn = null;
  }

  async _searchField(field, q) {
    const f = this._fields[field];
    if (q.length < MIN_CHARS) {
      f.items = [];
      this._renderFieldResults(field);
      return;
    }
    const seq = ++f.reqSeq;
    // Stops resolve instantly off the same live client-side index the search
    // box and stops.js use (core/data/transloc.js's getStops()) -- show them
    // right away, then fold buildings + places in once those fetches land, same
    // two-phase pattern as ui/search.js (and, like there, the two upstreams are
    // fetched in parallel so one being slow/down doesn't hold up the other).
    const stopItems = matchFieldStops(q);
    f.items = stopItems;
    this._renderFieldResults(field);
    try {
      const [buildingData, placeData] = await Promise.all([
        fetch(`${API_BASE}/v1/uva/facility_search?q=${encodeURIComponent(q)}`, { cache: 'no-store' })
          .then((r) => (r.ok ? r.json() : { results: [] }))
          .catch(() => ({ results: [] })),
        fetch(`${API_BASE}/v1/search/geocode?q=${encodeURIComponent(q)}`, { cache: 'no-store' })
          .then((r) => (r.ok ? r.json() : { results: [] }))
          .catch(() => ({ results: [] })),
      ]);
      if (seq !== f.reqSeq) return;
      const buildings = (Array.isArray(buildingData.results) ? buildingData.results : []).map((b) => ({
        kind: 'building',
        name: b.name,
        number: b.number,
        address: b.address,
        bbox: b.bbox,
      }));
      const places = (Array.isArray(placeData.results) ? placeData.results : []).map((p) => ({
        kind: 'place',
        name: p.name,
        address: p.address,
        lat: p.lat,
        lng: p.lon,
        bbox: p.bbox,
      }));
      f.items = [...stopItems, ...buildings, ...places];
      this._renderFieldResults(field);
    } catch {
      if (seq === f.reqSeq) {
        f.items = stopItems;
        this._renderFieldResults(field);
      }
    }
  }

  /** Shown when a field is focused with nothing typed: a pinned "Your Location"
   *  row (origin only -- there's no "current location" equivalent for a
   *  destination), a "Choose on map" row (both fields), then locally-cached
   *  recent picks, most-recent-first. Google Maps shows this same shape for an
   *  empty origin/destination field -- these two rows are what replaced the
   *  field's old standalone locate/pin buttons. */
  _showEmptyStateResults(field) {
    const f = this._fields[field];
    const items = [];
    if (field === 'origin') items.push({ kind: 'location', name: 'Your Location' });
    items.push({ kind: 'map', name: 'Choose on map' });
    for (const p of loadRecentPoints()) items.push({ kind: 'recent', name: p.label, lat: p.lat, lng: p.lng });
    f.items = items;
    this._renderFieldResults(field);
  }

  _renderFieldResults(field) {
    const f = this._fields[field];
    if (!f.items.length) {
      f.results.hidden = true;
      f.results.innerHTML = '';
      return;
    }
    // Mobile bottom sheet: the dropdown is an absolutely-positioned child of the
    // sheet, which clips its own content to .tp-card's box (needed elsewhere so the
    // itinerary list scrolls internally instead of spilling past the sheet). At the
    // sheet's default "peek" height that box is only ~46vh tall, so a dropdown
    // opened right after tapping a field (before anything else has expanded the
    // sheet) was rendering almost entirely off-screen -- confirmed live, only the
    // section header and a sliver of the first result were visible. Reuse the same
    // .is-expanded (85vh) state already used once results come back, which is a
    // no-op on desktop (that class only does anything inside the mobile media
    // query) -- see _renderStatus('idle')'s classList.remove for where this gets
    // cleared back out once the widget is closed/reset.
    this._card.classList.add('is-expanded');
    const KIND_HEADS = { stop: 'Bus stops', building: 'Buildings', place: 'Places', recent: 'Recent' };
    let html = '';
    let prevKind = null;
    f.items.forEach((it, i) => {
      // "Your Location" and "Choose on map" are single pinned rows, not a
      // group -- no header, same as Google Maps.
      if (it.kind !== prevKind && it.kind !== 'location' && it.kind !== 'map') {
        html += `<div class="tp-field-head">${KIND_HEADS[it.kind] || 'Buildings'}</div>`;
      }
      prevKind = it.kind;
      // Every row gets a leading icon now, same "full" rhythm as Google Maps'
      // own list (a bare icon-less row for stops/buildings read sparse next
      // to Your Location/Choose on map/Recent, which all have one).
      const icon = it.kind === 'location' ? ICONS.locate : it.kind === 'recent' ? ICONS.recent : ICONS.pin;
      const pinned = it.kind === 'location' || it.kind === 'map';
      html += `
      <button type="button" class="tp-field-item${pinned ? ' tp-field-item--pinned' : ''}" data-i="${i}">
        <span class="tp-field-item-icon">${icon}</span>
        <span class="tp-field-item-text">
          <span class="tp-field-item-name"></span>
          <span class="tp-field-item-meta"></span>
        </span>
      </button>`;
    });
    f.results.innerHTML = html;
    [...f.results.querySelectorAll('.tp-field-item')].forEach((btn) => {
      const it = f.items[Number(btn.dataset.i)];
      btn.querySelector('.tp-field-item-name').textContent = it.name;
      btn.querySelector('.tp-field-item-meta').textContent =
        it.kind === 'building' || it.kind === 'place'
          ? [it.number && `#${it.number}`, it.address].filter(Boolean).join(' · ')
          : it.meta || '';
      btn.addEventListener('click', () => this._pickResult(field, it));
    });
    f.results.hidden = false;
    // Force a synchronous layout right after un-hiding -- cheap insurance
    // against the same class of stale-paint bug the reordering in the focus
    // listener above targets, on the offchance content lands here through
    // some other path while f.results is mid-reparent.
    void f.results.offsetHeight;
  }

  _closeFieldResults(field) {
    this._fields[field].results.hidden = true;
  }

  _pickResult(field, it) {
    if (it.kind === 'location') {
      this._useMyLocation(field);
      return;
    }
    if (it.kind === 'map') {
      this._startPickingOnMap(field);
      return;
    }
    if (it.kind === 'stop' || it.kind === 'recent' || it.kind === 'place') {
      // Places already carry a lat/lng point from Nominatim (no bbox-centroid
      // derivation needed, unlike a building's ArcGIS polygon-only row).
      this._applyPoint(field, { lat: it.lat, lng: it.lng, label: it.name });
      return;
    }
    this._pickBuilding(field, it);
  }

  _pickBuilding(field, b) {
    if (!b.bbox || b.bbox.length < 4) return;
    const point = {
      lat: (b.bbox[1] + b.bbox[3]) / 2,
      lng: (b.bbox[0] + b.bbox[2]) / 2,
      label: b.name,
    };
    this._applyPoint(field, point);
  }

  _useMyLocation(field) {
    if (!navigator.geolocation) return;
    const f = this._fields[field];
    f.input.value = 'Locating…';
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        this._applyPoint(field, {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          label: 'My location',
        });
      },
      () => {
        f.input.value = '';
        f.input.placeholder = 'Could not get your location';
      },
      { enableHighAccuracy: true, timeout: 8000 },
    );
  }

  /** "Choose on map" (in a field's empty-state dropdown): pin fixed at the
   *  map's screen centre while the rider pans the map underneath it (the
   *  classic Uber/Maps "set pickup" pattern) -- more precise than a raw click,
   *  especially on mobile where a fingertip covers far more than the point
   *  being tapped. Desktop's sidebar already leaves the map mostly visible, so
   *  it gets a lightweight pin + floating confirm bar over it; mobile takes
   *  over almost the entire screen the way Google Maps' own "choose on map"
   *  does, since there's no spare screen space next to a bottom sheet. */
  _startPickingOnMap(field) {
    if (this._isMobileLayout()) this._enterMapPicker(field);
    else this._startAdjusting(field);
  }

  _startAdjusting(field) {
    this._stopPicking(true); // in case the OTHER field was mid-adjust
    const map = getMap();
    if (!map) return;
    this._pickMode = field;
    TripPlanner.setMarkerVisible(field, false);
    this._showCenterPin(field);
    this._showConfirmBar(field);
    this._recenterOnExisting(field, map);
  }

  _enterMapPicker(field) {
    // The full-screen field search overlay covers the map entirely -- close it
    // first so the map (and the pin about to go on it) is actually visible.
    this._closeOverlay();
    this._stopPicking(true); // in case the OTHER field was mid-adjust
    const map = getMap();
    if (!map) return;
    this._pickMode = field;
    this._mapPickerActive = true;
    TripPlanner.setMarkerVisible(field, false);
    this._showCenterPin(field);
    this._showMapPickerBar(field);
    // Hide the sheet entirely rather than just collapsing it -- "almost
    // entirely map", matching Google Maps' own dedicated pin-drop screen,
    // with the confirm action living in this top bar instead of a bar
    // floating over a still-visible sheet.
    this._cardEl.hidden = true;
    this._recenterOnExisting(field, map);
  }

  /** Start the pin exactly where the field's current point is (if it has one)
   *  so adjusting reads as "fine-tune", not "start over from nowhere". */
  _recenterOnExisting(field, map) {
    const existing = field === 'origin' ? TripPlanner.getOrigin() : TripPlanner.getDestination();
    if (existing) map.easeTo({ center: [existing.lng, existing.lat], duration: 300 });
    map.getCanvas()?.classList.add('tp-picking-cursor');
  }

  _showCenterPin(field) {
    const map = getMap();
    const container = map?.getContainer();
    if (!container) return;
    const pinEl = document.createElement('div');
    pinEl.className = `tp-center-pin tp-center-pin--${field}`;
    pinEl.innerHTML = `<span class="tp-center-pin-badge">${field === 'origin' ? 'A' : 'B'}</span>`;
    container.appendChild(pinEl);
    this._centerPinEl = pinEl;
  }

  /** Desktop only (see _startPickingOnMap) -- a small floating bar over the
   *  already-visible map, since there's no dedicated full-screen mode there. */
  _showConfirmBar(field) {
    const bar = document.createElement('div');
    bar.className = 'tp-center-pin-bar';
    bar.innerHTML = `
      <span class="tp-center-pin-hint">Drag the map to move the pin</span>
      <button type="button" class="tp-center-pin-confirm">Set ${field === 'origin' ? 'origin' : 'destination'}</button>`;
    bar.querySelector('.tp-center-pin-confirm').addEventListener('click', () => this._confirmAdjusting());
    document.body.appendChild(bar);
    this._centerPinBar = bar;
  }

  /** Mobile only -- a thin top bar (back / title+hint / OK) replacing the
   *  sheet while it's hidden, same shape as Google Maps' own "choose on map"
   *  screen. Back cancels and reopens the field's full-screen search list
   *  (mirrors Maps); OK confirms, same as the desktop bar's button. */
  _showMapPickerBar(field) {
    const bar = document.createElement('div');
    bar.className = 'tp-map-picker-bar';
    bar.innerHTML = `
      <button type="button" class="tp-map-picker-back" aria-label="Back">${ICONS.back}</button>
      <span class="tp-map-picker-text">
        <span class="tp-map-picker-title">Choose ${field === 'origin' ? 'origin' : 'destination'} location</span>
        <span class="tp-map-picker-sub">Pan &amp; zoom map under pin</span>
      </span>
      <button type="button" class="tp-map-picker-ok">OK</button>`;
    bar.querySelector('.tp-map-picker-back').addEventListener('click', () => {
      this._stopPicking(true);
      this._maybeOpenOverlay(field);
    });
    bar.querySelector('.tp-map-picker-ok').addEventListener('click', () => this._confirmAdjusting());
    document.body.appendChild(bar);
    this._mapPickerBar = bar;
  }

  _confirmAdjusting() {
    const field = this._pickMode;
    if (!field) return;
    const center = getMap()?.getCenter();
    this._stopPicking(false); // the point is about to be replaced below, no need to restore
    if (center) this._applyPoint(field, { lat: center.lat, lng: center.lng, label: 'Dropped pin' });
  }

  _stopPicking(restoreMarker) {
    if (!this._pickMode) return;
    const field = this._pickMode;
    this._pickMode = null;
    const map = getMap();
    map?.getCanvas()?.classList.remove('tp-picking-cursor');
    this._centerPinEl?.remove();
    this._centerPinEl = null;
    this._centerPinBar?.remove();
    this._centerPinBar = null;
    this._mapPickerBar?.remove();
    this._mapPickerBar = null;
    if (this._mapPickerActive) {
      this._cardEl.hidden = false;
      this._mapPickerActive = false;
    }
    if (restoreMarker) TripPlanner.setMarkerVisible(field, true);
  }

  _applyPoint(field, point) {
    this._closeFieldResults(field);
    if (this._overlayField === field) this._closeOverlay(); // back to the field-pair screen, Maps-style
    saveRecentPoint(point);
    if (field === 'origin') TripPlanner.setOrigin(point);
    else TripPlanner.setDestination(point);
    this._maybePlan();
  }

  _swap() {
    const origin = TripPlanner.getOrigin();
    const destination = TripPlanner.getDestination();
    TripPlanner.setOrigin(destination);
    TripPlanner.setDestination(origin);
    this._maybePlan();
  }

  _syncFieldValue(field, point) {
    const f = this._fields[field];
    f.input.value = point ? point.label || `${point.lat.toFixed(5)}, ${point.lng.toFixed(5)}` : '';
  }

  _setWhenMode(mode) {
    for (const btn of this._cardEl.querySelectorAll('.tp-when-btn')) {
      btn.classList.toggle('is-active', btn.dataset.when === mode);
    }
    if (mode === 'now') {
      this._when = null;
      this._whenInput.hidden = true;
      this._replan();
    } else {
      this._whenInput.hidden = false;
      if (!this._whenInput.value) this._whenInput.showPicker?.();
    }
  }

  _maybePlan() {
    if (TripPlanner.getOrigin() && TripPlanner.getDestination()) this._replan();
  }

  _replan() {
    if (TripPlanner.getOrigin() && TripPlanner.getDestination()) {
      TripPlanner.planTrip(this._when || undefined);
    }
  }

  _renderStatus(status) {
    this._cardEl.dataset.status = status;
    // Every one of these statuses means "not looking at a specific itinerary's detail
    // right now" (a fresh search just started, came back empty/failed, or was cleared
    // entirely) -- drop back to the list view and hide its synopsis footer so a stale
    // one from a previous plan can't linger behind the newly-empty/loading results.
    this._detailFooter.hidden = true;
    this._detailFooter.innerHTML = '';
    this._setView('list');
    if (status === 'loading') {
      this._results.hidden = false;
      this._results.innerHTML = '<div class="tp-status"><span class="tp-spinner" aria-hidden="true"></span>Finding trips…</div>';
    } else if (status === 'empty') {
      this._results.hidden = false;
      this._results.innerHTML =
        '<div class="tp-status tp-status--empty">No scheduled service can make this trip right now. ' +
        'Consider requesting an on-demand ride.</div>';
    } else if (status === 'error') {
      this._results.hidden = false;
      this._results.innerHTML = '<div class="tp-status tp-status--error">Couldn’t load trip options. Try again.</div>';
    } else if (status === 'idle') {
      this._results.hidden = true;
      this._results.innerHTML = '';
      this._card.classList.remove('is-expanded');
    }
  }

  _renderItineraries(itineraries) {
    this._itineraries = itineraries;
    if (!itineraries.length) return; // status handler already covers empty/error
    // A fresh set of results always lands back on the options list, even if the rider
    // was looking at a previous plan's detail view.
    this._setView('list');
    this._results.hidden = false;
    // Mobile bottom sheet: pop open to the "full" height once there's something to
    // show, same as Maps auto-expanding its sheet when directions come back, rather
    // than leaving results collapsed behind a peek-height sheet. No-op on desktop.
    this._card.classList.add('is-expanded');
    this._renderList();
  }

  _renderList() {
    this._results.innerHTML = this._itineraries.map((it, i) => this._itineraryCardHtml(it, i)).join('');
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.addEventListener('click', () => this._showDetail(Number(card.dataset.i)));
    });
    this._syncSelected(this._selectedIndex);
  }

  /** Switches between the ranked-options list and a single itinerary's full
   *  step-by-step breakdown -- mirrors Maps: tapping an option in the list shows
   *  just that one, collapsed to a small peek with its whole route visible on the
   *  map; tapping that peek (or the drag handle) expands it into the scrollable
   *  detail seen here. */
  _setView(view) {
    this._resetDragHeight(); // don't let a stale manual-drag height outlive a real state change
    this._view = view;
    const inDetail = view === 'detail';
    this._cardEl.dataset.view = view;
    this._backBtn.hidden = !inDetail;
    this._cardTitle.textContent = inDetail ? 'Trip details' : 'Plan a trip';
    this._cardEl.querySelector('.tp-fields').hidden = inDetail;
    this._cardEl.querySelector('.tp-when').hidden = inDetail;
  }

  _showList() {
    this._setView('list');
    this._card.classList.add('is-expanded');
    this._results.hidden = false;
    this._detailFooter.hidden = true;
    this._renderList();
  }

  _showDetail(index) {
    const itinerary = this._itineraries[index];
    if (!itinerary) return;
    this._setView('detail');
    this._selectedItinerary = itinerary;
    this._detailFooter.hidden = false;
    this._detailFooter.innerHTML = this._detailFooterHtml(itinerary);
    this._results.innerHTML = this._detailStepsHtml(itinerary);
    // Land on the small peek, same as Maps: the map (now fit to this one route) is
    // the main event until the rider explicitly asks for the full breakdown. Desktop
    // has no peek concept (see the [data-view="detail"] CSS, mobile-only) -- there,
    // the step list just stays visible in the always-full-height sidebar.
    this._card.classList.remove('is-expanded');
    // Selected LAST, after this._view/_detailFooter above are already set to
    // their detail-view values -- this is what triggers the map's fitBounds
    // (via TripPlanner.selectItinerary -> fitToItinerary -> the
    // _computeFitPadding callback), which reads both to size its padding.
    TripPlanner.selectItinerary(index);
  }

  _itineraryCardHtml(itinerary, i) {
    const mins = Math.round(itinerary.totalDurationS / 60);
    const warn = itinerary.legs.some((l) => l.kind === 'ride' && l.lastRideWarning);
    // A walk leg under this distance is "you're already there" -- most often the
    // origin/destination sitting right at a stop, or (confirmed live) a same-physical-
    // stop transfer where TransLoc happens to carry separate StopIDs per route for one
    // shelter. Showing "Walk 1 min" for a 1-metre gap actively misleads, so these are
    // dropped from the timeline entirely rather than rounded up to a fake minute.
    const visibleLegs = itinerary.legs.filter(
      (leg) => !(leg.kind === 'walk' && leg.distanceM != null && leg.distanceM < NEGLIGIBLE_WALK_M),
    );
    // Waiting at the stop is its own timeline step -- Walk, then Wait, then
    // Ride -- not a parenthetical tucked inside the ride row (which read like
    // the wait happened AFTER the ride, not before boarding).
    const steps = timelineSteps(visibleLegs);
    const legsHtml = steps.map((step, si) => this._stepRowHtml(step, si === steps.length - 1)).join('');
    return `
      <button type="button" class="tp-itin" data-i="${i}">
        <div class="tp-itin-top">
          <span class="tp-itin-time">${mins} min</span>
          ${itinerary.durationIsEstimate ? '<span class="tp-itin-tag">Estimated</span>' : ''}
          ${warn ? `<span class="tp-itin-tag tp-itin-tag--warn">${ICONS.warn}Last bus soon</span>` : ''}
        </div>
        <div class="tp-itin-timeline">${legsHtml}</div>
      </button>`;
  }

  _stepRowHtml(step, isLast) {
    if (step.type === 'wait') return this._waitRowHtml(step, isLast);
    if (step.type === 'walk') return this._walkRowHtml(step.leg, isLast);
    return this._rideRowHtml(step.leg, isLast);
  }

  _walkRowHtml(leg, isLast) {
    const lastClass = isLast ? ' tp-leg--last' : '';
    // Honest sub-minute wording instead of flooring up to a misleading "1 min" --
    // paired with the NEGLIGIBLE_WALK_M filter above, which drops the truly-zero
    // case (same-stop transfers, origin/destination right at a stop) entirely.
    const rawMins = Math.round(leg.durationS / 60);
    const timeLabel = rawMins < 1 ? '&lt;1 min' : `${rawMins} min`;
    const dist = leg.distanceM != null ? ` · ${formatWalkDistance(leg.distanceM)}` : '';
    return `
      <div class="tp-leg tp-leg--walk${lastClass}">
        <span class="tp-leg-dot tp-leg-dot--walk"></span>
        <span class="tp-leg-text">Walk <b>${timeLabel}</b><span class="tp-leg-sub">${dist}</span></span>
      </div>`;
  }

  _waitRowHtml(step, isLast) {
    const lastClass = isLast ? ' tp-leg--last' : '';
    const known = step.waitS != null;
    const mins = known ? Math.max(0, Math.round(step.waitS / 60)) : null;
    const timeLabel = !known ? '' : mins < 1 ? '&lt;1 min' : `${mins} min`;
    // "extrapolated" -- no live arrival reaches this far out (common on a
    // sparsely-vehicled route like Silver), so this is projected from the route's
    // own recently-observed headway rather than a directly-seen upcoming bus. Same
    // "estimated" language as a heuristic ride duration, not a separate concept --
    // not confident enough to promote to a badge. "live"/"scheduled" get a real
    // badge instead (see waitSourceBadgeHtml).
    const estimated = step.waitSSource === 'extrapolated' ? ' · estimated' : '';
    const badge = waitSourceBadgeHtml(step.waitSSource);
    return `
      <div class="tp-leg tp-leg--wait${lastClass}">
        <span class="tp-leg-dot tp-leg-dot--wait"></span>
        <span class="tp-leg-text">${
          known
            ? `Wait <b>${timeLabel}</b><span class="tp-leg-sub">${estimated}</span>${badge}`
            : '<span class="tp-leg-sub--muted">Wait time unknown</span>'
        }</span>
      </div>`;
  }

  _rideRowHtml(leg, isLast) {
    const lastClass = isLast ? ' tp-leg--last' : '';
    const color = normalizeColor(leg.color);
    const textColor = readableTextColor(color);
    const mins = Math.round((leg.rideS || 0) / 60);
    // Compact heads-up in the collapsed itinerary preview -- names the actual
    // timestop and duration right here (a bare "scheduled hold" label with no
    // specifics isn't worth showing), with the richer "until HH:MM" version in
    // _detailRideStepHtml. A block-level div, not an inline span, so it gets
    // its own line with real spacing instead of crowding the route badge.
    const holdTag = leg.holds && leg.holds[0] ? holdTagHtml(leg.holds[0]) : '';
    return `
      <div class="tp-leg tp-leg--ride${lastClass}">
        <span class="tp-leg-dot" style="background:${color}"></span>
        <div class="tp-leg-text">
          <span class="tp-route-badge" style="background:${color};color:${textColor}">${esc(leg.lineName || leg.lineId)}</span>
          <span class="tp-leg-sub">${mins} min ride</span>
          ${holdTag}
        </div>
      </div>`;
  }

  _syncSelected(index) {
    this._selectedIndex = index;
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.classList.toggle('is-selected', Number(card.dataset.i) === index);
    });
  }

  /** Clock time at the start of each leg, plus one trailing entry for the final
   *  arrival -- i.e. `marks.length === itinerary.legs.length + 1`. Walked forward
   *  from `_when` (or "now" if the rider didn't pick a specific time) by each leg's
   *  real elapsed time (wait included for a ride leg), the same arithmetic the
   *  backend uses for totalDurationS so these clocks and that total always agree. */
  _legStartClocks(itinerary) {
    let t = (this._when instanceof Date ? this._when : new Date()).getTime();
    const marks = [t];
    for (const leg of itinerary.legs) {
      const segS = leg.kind === 'walk' ? leg.durationS || 0 : (leg.waitS || 0) + (leg.rideS || 0);
      t += segS * 1000;
      marks.push(t);
    }
    return marks;
  }

  _detailStepsHtml(itinerary) {
    const clocks = this._legStartClocks(itinerary);
    const origin = TripPlanner.getOrigin();
    const destination = TripPlanner.getDestination();
    const visibleLegs = itinerary.legs.filter(
      (leg) => !(leg.kind === 'walk' && leg.distanceM != null && leg.distanceM < NEGLIGIBLE_WALK_M),
    );
    const rows = [
      `<div class="tp-detail-point">
        <span class="tp-field-badge tp-field-badge--origin">A</span>
        <span class="tp-detail-point-text">${esc(origin?.label || 'Origin')}</span>
        <span class="tp-detail-clock">${formatClock(clocks[0])}</span>
      </div>`,
    ];
    // clocks is indexed against the FULL leg list (including negligible walks that
    // got filtered out of visibleLegs above), so look each leg's clock up by its
    // position in the original list rather than assuming a 1:1 index match.
    for (const leg of visibleLegs) {
      const i = itinerary.legs.indexOf(leg);
      const startMs = clocks[i];
      if (leg.kind === 'walk') {
        rows.push(this._detailWalkStepHtml(leg));
      } else {
        // Waiting at the stop is its own step, in order -- shown BEFORE the
        // board/ride/alight step, not tucked inside it, so the sequence reads
        // "arrive, wait, board" instead of implying the wait happens after
        // you've already boarded.
        rows.push(this._detailWaitStepHtml(leg));
        rows.push(this._detailRideStepHtml(leg, startMs));
      }
    }
    rows.push(
      `<div class="tp-detail-point">
        <span class="tp-field-badge tp-field-badge--destination">B</span>
        <span class="tp-detail-point-text">${esc(destination?.label || 'Destination')}</span>
        <span class="tp-detail-clock">${formatClock(clocks[clocks.length - 1])}</span>
      </div>`,
    );
    return `<div class="tp-detail-steps">${rows.join('')}</div>`;
  }

  _detailWalkStepHtml(leg) {
    const mins = Math.round(leg.durationS / 60);
    const timeLabel = mins < 1 ? '<1 min' : `${mins} min`;
    const dist = leg.distanceM != null ? ` (${formatWalkDistance(leg.distanceM)})` : '';
    return `
      <div class="tp-detail-step tp-detail-step--walk">
        <span class="tp-leg-dot tp-leg-dot--walk"></span>
        <div class="tp-detail-step-body">
          <div class="tp-detail-step-main">Walk ${timeLabel}${dist}</div>
          ${leg.source === 'straight_line' ? '<div class="tp-detail-step-sub tp-leg-sub--muted">Estimated route</div>' : ''}
        </div>
      </div>`;
  }

  /** Its own step (own dot, own row) between the walk-in and the ride -- not a
   *  note buried inside the ride step, which used to read as if the wait
   *  happened AFTER boarding instead of before it. */
  _detailWaitStepHtml(leg) {
    const known = leg.waitS != null;
    const mins = known ? Math.max(0, Math.round(leg.waitS / 60)) : null;
    const timeLabel = known ? (mins < 1 ? '<1 min' : `${mins} min`) : null;
    const estimated = leg.waitSSource === 'extrapolated' ? ' · estimated' : '';
    const badge = waitSourceBadgeHtml(leg.waitSSource);
    return `
      <div class="tp-detail-step tp-detail-step--wait">
        <span class="tp-leg-dot tp-leg-dot--wait"></span>
        <div class="tp-detail-step-body">
          <div class="tp-detail-step-main">
            ${known ? `Wait ${timeLabel}${estimated}${badge}` : '<span class="tp-leg-sub--muted">Wait time unknown</span>'}
          </div>
        </div>
      </div>`;
  }

  _detailRideStepHtml(leg, startMs) {
    const color = normalizeColor(leg.color);
    const textColor = readableTextColor(color);
    // stopCount, not coordinates.length -- coordinates is now the line's real
    // road-following shape (dense polyline vertices), not one point per stop.
    const stops = leg.stopCount || 1;
    const rideMins = Math.round((leg.rideS || 0) / 60);
    const warn = leg.lastRideWarning
      ? `<span class="tp-itin-tag tp-itin-tag--warn">${ICONS.warn}Last bus soon</span>`
      : '';
    const boardClock = formatClock(startMs + (leg.waitS || 0) * 1000);
    const alightClock = formatClock(startMs + ((leg.waitS || 0) + (leg.rideS || 0)) * 1000);
    // Board stop, the ride itself, and the alight stop are three visually distinct
    // rows -- not one run-on sentence -- so the rider can scan "where do I stand" /
    // "what am I riding" / "where do I get off" at a glance, matching how Maps lays
    // this out (bold stop-name rows with the time flush right, the ride itself as its
    // own connecting chip in between).
    return `
      <div class="tp-detail-step tp-detail-step--ride">
        <span class="tp-leg-dot" style="background:${color}"></span>
        <div class="tp-detail-step-body">
          <div class="tp-detail-stop-row">
            <span class="tp-detail-stop-name">${esc(leg.boardStop?.name || 'stop')}</span>
            <span class="tp-detail-clock">${boardClock}</span>
          </div>
          <div class="tp-detail-ride-chip" style="border-left-color:${color}">
            <span class="tp-route-badge" style="background:${color};color:${textColor}">${esc(leg.lineName || leg.lineId)}</span>
            <span class="tp-detail-ride-chip-text">${stops} stop${stops === 1 ? '' : 's'} · ${rideMins} min${leg.rideSSource === 'heuristic' ? ' · estimated' : ''}</span>
            ${warn}
          </div>
          ${(leg.holds || []).map(holdNoteHtml).join('')}
          <div class="tp-detail-stop-row">
            <span class="tp-detail-stop-name">${esc(leg.alightStop?.name || 'stop')}</span>
            <span class="tp-detail-clock">${alightClock}</span>
          </div>
        </div>
      </div>`;
  }

  _detailFooterHtml(itinerary) {
    const mins = Math.round(itinerary.totalDurationS / 60);
    const clocks = this._legStartClocks(itinerary);
    const range = `${formatClock(clocks[0])} – ${formatClock(clocks[clocks.length - 1])}`;
    const tag = itinerary.durationIsEstimate ? '<span class="tp-itin-tag">Estimated</span>' : '';
    return `
      <div class="tp-detail-footer-time">
        <span class="tp-itin-time">${mins} min</span>
        <span class="tp-leg-sub">${range}</span>
      </div>
      ${tag}`;
  }
}

/** Match the live UTS stop index against a query for an origin/destination
 *  field -- same matching approach as ui/search.js's matchStops. CAT stops
 *  aren't included; there's no equivalent live client-side index for them. */
function matchFieldStops(q) {
  const ql = q.toLowerCase();
  const scored = [];
  for (const s of getUtsStops()) {
    if (!Number.isFinite(s.lng) || !Number.isFinite(s.lat)) continue;
    const name = String(s.name || '').toLowerCase();
    let score = -1;
    if (name === ql) score = 0;
    else if (name.startsWith(ql)) score = 1;
    else if (name.includes(ql)) score = 2;
    if (score < 0) continue;
    scored.push({ s, score });
  }
  scored.sort(
    (a, b) => a.score - b.score || String(a.s.name).localeCompare(String(b.s.name), undefined, { numeric: true }),
  );
  return scored.slice(0, MAX_FIELD_STOPS).map(({ s }) => ({
    kind: 'stop',
    name: s.name,
    meta: s.routeIds.map((rid) => getRouteName(rid)).filter(Boolean).join(', '),
    lat: s.lat,
    lng: s.lng,
  }));
}

/** Flattens legs into card-timeline steps, splitting each ride leg into its
 *  own "wait" step followed by its own "ride" step -- waiting at the stop is
 *  a real step in the trip (Walk, then Wait, then Ride), not a parenthetical
 *  folded into the ride's sub-label. */
function timelineSteps(legs) {
  const steps = [];
  for (const leg of legs) {
    if (leg.kind === 'walk') {
      steps.push({ type: 'walk', leg });
    } else {
      steps.push({ type: 'wait', waitS: leg.waitS, waitSSource: leg.waitSSource });
      steps.push({ type: 'ride', leg });
    }
  }
  return steps;
}

function normalizeColor(c) {
  const s = String(c || '888888').trim();
  return s.startsWith('#') ? s : `#${s}`;
}

/** White or dark text, whichever reads better on a route's own color (route colors
 *  span the whole brightness range -- Gold Line's #ffdd00 needs dark text, Purple
 *  Line's #662c90 needs white -- so this can't be a fixed choice). */
function readableTextColor(hex) {
  const parsed = parseColor(hex);
  if (!parsed) return '#1a1a1a';
  return luminance(parsed) > 0.55 ? '#1a1a1a' : '#ffffff';
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}

function formatClock(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

/** US customary units, not metres -- riders here think in feet/miles, not the
 *  metres the backend computes in (walk routing is metric internally). Below
 *  0.1 mile shows feet rounded to the nearest 10 (matches Google Maps' own
 *  cutover); at or above that, miles to one decimal place. */
const WALK_FEET_PER_METER = 3.28084;
const WALK_MILE_CUTOVER_FT = 528; // 0.1 mile
function formatWalkDistance(meters) {
  if (meters == null) return '';
  const feet = meters * WALK_FEET_PER_METER;
  if (feet < WALK_MILE_CUTOVER_FT) return `${Math.round(feet / 10) * 10} ft`;
  return `${(feet / 5280).toFixed(1)} mi`;
}

/** Live/Scheduled badge for a wait time -- "extrapolated" stays the existing
 *  plain "· estimated" text suffix (not confident enough to badge the way a
 *  real live position or a published schedule time is). Only "live" gets the
 *  animated icon (see ICONS.live) -- that's specifically a "this is moving,
 *  GPS-based data" signal, not a generic wait-time decoration. */
function waitSourceBadgeHtml(source) {
  if (source === 'live') return `<span class="tp-source-badge tp-source-badge--live">${ICONS.live}Live</span>`;
  if (source === 'scheduled') return `<span class="tp-source-badge tp-source-badge--scheduled">${ICONS.recent}Scheduled</span>`;
  return '';
}

/** One scheduled mid-ride hold (see trip_planner.py's _estimate_ride_seconds) --
 *  the bus intentionally sitting at a UTS "timestop" (never "timepoint") for a
 *  scheduled departure, which riders otherwise mistake for a driver taking an
 *  unscheduled break. Compact version (collapsed itinerary preview) and full
 *  version (expanded detail, with the actual release time) share the same
 *  underlying info -- a bare "scheduled hold" label with no specifics isn't
 *  worth showing at all. */
function holdTagHtml(hold) {
  if (!hold) return '';
  const mins = Math.max(1, Math.round((hold.holdS || 0) / 60));
  return `
    <div class="tp-hold-tag">
      ${ICONS.recent}
      <span>${mins} min timestop at <b>${esc(hold.stopName || 'a stop')}</b></span>
    </div>`;
}

function holdNoteHtml(hold) {
  if (!hold) return '';
  const mins = Math.max(1, Math.round((hold.holdS || 0) / 60));
  const until = hold.untilTs ? formatClock(hold.untilTs * 1000) : null;
  return `
    <div class="tp-hold-note">
      ${ICONS.recent}
      <span><b>${esc(hold.stopName || 'a stop')}</b> is a scheduled timestop — bus holds here ~${mins} min${until ? `, until ${until}` : ''}. This is expected, not a delay.</span>
    </div>`;
}
