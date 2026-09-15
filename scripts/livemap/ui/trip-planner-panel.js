// livemap/ui/trip-planner-panel.js
// -----------------------------------------------------------------------------
// Trip planner UI: a collapsed pill (bottom-left, clear of the search box at
// top-centre and the Panels columns at the left/right edges) that expands into
// an origin/destination picker + ranked itinerary results.
//
// Three ways to set a point, all feeding core/trip-planner.js's setOrigin /
// setDestination:
//   * type a building name (autocomplete via /v1/uva/facility_search, same
//     lookup ui/search.js already uses)
//   * "Use my location" (browser geolocation) -- origin field only
//   * "Click map" mode -- toggles a one-shot map click listener
// -----------------------------------------------------------------------------

import { API_BASE } from '../core/config.js';
import { getMap } from '../core/map.js';
import { debounce, parseColor, luminance } from '../core/util.js';
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
};

export class TripPlannerPanel {
  mount(parent = document.body) {
    // Two separate top-level elements, not one nested inside the other: .tp-widget
    // (just the small bottom-centre toggle) has its own `transform` for centring,
    // and a `transform` on any ancestor becomes the containing block for
    // `position: fixed` descendants -- confirmed live, the card collapsed to a
    // sliver positioned relative to the tiny toggle pill instead of the viewport
    // when it lived inside .tp-widget. Keeping them as siblings avoids that trap.
    const el = document.createElement('div');
    el.className = 'tp-widget';
    el.innerHTML = `
      <button type="button" class="tp-toggle" aria-expanded="false">
        <span class="tp-toggle-icon" aria-hidden="true"></span>
        <span>Plan a trip</span>
      </button>`;

    const cardEl = document.createElement('div');
    cardEl.className = 'tp-card';
    cardEl.hidden = true;
    cardEl.innerHTML = `
      <button type="button" class="tp-drag-handle" aria-label="Expand or collapse"><span></span></button>
      <div class="tp-card-head">
        <button type="button" class="tp-back" aria-label="Back to trip options" hidden>${ICONS.back}</button>
        <span class="tp-card-title">Plan a trip</span>
        <button type="button" class="tp-close" aria-label="Close">&times;</button>
      </div>
      <div class="tp-fields">
        <div class="tp-field" data-field="origin">
          <span class="tp-field-badge tp-field-badge--origin">A</span>
          <input type="text" placeholder="Origin — building, stop, or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location">${ICONS.locate}</button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Drag the map to set" aria-label="Drag the map to set">${ICONS.pin}</button>
          <div class="tp-field-results" hidden></div>
        </div>
        <div class="tp-field" data-field="destination">
          <span class="tp-field-badge tp-field-badge--destination">B</span>
          <input type="text" placeholder="Destination — building, stop, or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location" hidden>${ICONS.locate}</button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Drag the map to set" aria-label="Drag the map to set">${ICONS.pin}</button>
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
    this._results = cardEl.querySelector('.tp-results');
    this._detailFooter = cardEl.querySelector('.tp-detail-footer');
    this._backBtn = cardEl.querySelector('.tp-back');
    this._cardTitle = cardEl.querySelector('.tp-card-title');
    this._whenInput = cardEl.querySelector('.tp-when-input');
    this._pickMode = null; // 'origin' | 'destination' | null
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
        locateBtn: wrap.querySelector('.tp-field-btn--locate'),
        pinBtn: wrap.querySelector('.tp-field-btn--pin'),
        items: [],
        reqSeq: 0,
      };
      this._wireField(field);
    }

    cardEl.querySelector('.tp-swap').addEventListener('click', () => this._swap());

    // Mobile-only bottom sheet: the handle is a real drag, not just a toggle -- see
    // _wireDragHandle. Harmless on desktop, where the handle itself is hidden via CSS
    // and pointer events on it never fire.
    this._wireDragHandle(cardEl.querySelector('.tp-drag-handle'));

    this._toggle.addEventListener('click', () => this._setExpanded(true));
    cardEl.querySelector('.tp-close').addEventListener('click', () => {
      this._closeOverlay();
      this._stopPicking(false);
      this._setExpanded(false);
      TripPlanner.clearAll();
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

    const root = parent || document.body;
    root.appendChild(el);
    root.appendChild(cardEl);
    root.appendChild(overlayEl);
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
  _wireDragHandle(handle) {
    const TAP_THRESHOLD_PX = 6;
    let drag = null;
    handle.addEventListener('pointerdown', (e) => {
      handle.setPointerCapture(e.pointerId);
      drag = {
        pointerId: e.pointerId,
        startY: e.clientY,
        startHeight: this._cardEl.getBoundingClientRect().height,
        detents: this._computeSheetDetents(),
      };
      this._cardEl.classList.add('tp-card--dragging');
      // The base (non-.is-expanded) rule caps height at max-height:46vh -- fine for
      // the plain two-state toggle, but it silently clamps a dragged/snapped height
      // that's meant to land anywhere between the peek and full detents (confirmed
      // live: dragged to the ~50vh "half" detent, inline height was set correctly,
      // but the box still rendered at 46vh because max-height is a separate
      // constraint that setting `height` doesn't override). Neutralized for as long
      // as a manual height is in effect; _resetDragHeight() puts it back.
      this._cardEl.style.maxHeight = 'none';
    });
    handle.addEventListener('pointermove', (e) => {
      if (!drag || e.pointerId !== drag.pointerId) return;
      const dy = e.clientY - drag.startY; // dragging UP (negative dy) grows the sheet
      const min = drag.detents[0];
      const max = drag.detents[drag.detents.length - 1];
      const next = Math.min(max, Math.max(min, drag.startHeight - dy));
      this._cardEl.style.height = `${next}px`;
    });
    const endDrag = (e) => {
      if (!drag || e.pointerId !== drag.pointerId) return;
      handle.releasePointerCapture(drag.pointerId);
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
    };
    handle.addEventListener('pointerup', endDrag);
    handle.addEventListener('pointercancel', endDrag);
  }

  /** Detent heights in px, largest last. Measured from the actual content of the
   *  current view rather than guessed viewport fractions where that content's size
   *  is what defines a sensible "peek" -- e.g. detail view's peek is exactly the
   *  drag handle + synopsis footer (the only things shown there), whatever their
   *  real rendered height happens to be, not an arbitrary number that could drift
   *  out of sync with a font-size or content change. */
  _computeSheetDetents() {
    const vh = window.innerHeight;
    const handleH = this._cardEl.querySelector('.tp-drag-handle').offsetHeight;
    if (this._view === 'detail') {
      return [handleH + (this._detailFooter.offsetHeight || 0), vh * 0.85];
    }
    const peek =
      handleH +
      this._cardEl.querySelector('.tp-card-head').offsetHeight +
      this._cardEl.querySelector('.tp-fields').offsetHeight +
      this._cardEl.querySelector('.tp-when').offsetHeight;
    return [Math.min(peek, vh * 0.46), vh * 0.5, vh * 0.85];
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
    if (open) this._resetDragHeight(); // reopen at the plain default, not wherever it was last dragged to
    this._toggle.setAttribute('aria-expanded', String(open));
    this._card.hidden = !open;
    this._toggle.hidden = open;
    this._setRoutesHiddenForPlanning(open);
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
    f.input.addEventListener('input', () => f.debouncedSearch(f.input.value.trim()));
    f.input.addEventListener('focus', () => this._maybeOpenOverlay(field));
    f.input.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      if (this._overlayField === field) this._closeOverlay();
      else this._closeFieldResults(field);
    });
    document.addEventListener('click', (e) => {
      if (!f.wrap.contains(e.target)) this._closeFieldResults(field);
    });
    f.locateBtn?.addEventListener('click', () => this._useMyLocation(field));
    f.pinBtn.addEventListener('click', () => this._togglePicking(field));
  }

  /** Phone-width bottom sheet only -- see mount()'s comment on the overlay. Desktop's
   *  sidebar is tall enough for the inline dropdown as-is, so this is a deliberate
   *  no-op there (matches the same 768px breakpoint the CSS already uses throughout
   *  this widget). */
  _isMobileLayout() {
    return window.matchMedia('(max-width: 768px)').matches;
  }

  _maybeOpenOverlay(field) {
    if (!this._isMobileLayout() || this._overlayField === field) return;
    const f = this._fields[field];
    this._overlayField = field;
    this._overlayReturn = { parent: f.wrap.parentNode, next: f.wrap.nextSibling };
    f.wrap.classList.add('tp-field--overlay');
    f.results.classList.add('tp-field-results--overlay');
    this._overlayHead.appendChild(f.wrap);
    this._overlayBody.appendChild(f.results);
    this._overlayEl.hidden = false;
    f.input.focus();
  }

  _closeOverlay() {
    const field = this._overlayField;
    if (!field) return;
    const f = this._fields[field];
    f.input.blur();
    f.wrap.classList.remove('tp-field--overlay');
    f.results.classList.remove('tp-field-results--overlay');
    const { parent, next } = this._overlayReturn || {};
    parent?.insertBefore(f.wrap, next || null);
    this._overlayEl.hidden = true;
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
    // right away, then fold buildings in once that fetch lands, same
    // two-phase pattern as ui/search.js.
    const stopItems = matchFieldStops(q);
    f.items = stopItems;
    this._renderFieldResults(field);
    try {
      const r = await fetch(`${API_BASE}/v1/uva/facility_search?q=${encodeURIComponent(q)}`, {
        cache: 'no-store',
      });
      if (seq !== f.reqSeq) return;
      const data = r.ok ? await r.json() : { results: [] };
      const buildings = (Array.isArray(data.results) ? data.results : []).map((b) => ({
        kind: 'building',
        name: b.name,
        number: b.number,
        address: b.address,
        bbox: b.bbox,
      }));
      f.items = [...stopItems, ...buildings];
      this._renderFieldResults(field);
    } catch {
      if (seq === f.reqSeq) {
        f.items = stopItems;
        this._renderFieldResults(field);
      }
    }
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
    let html = '';
    let prevKind = null;
    f.items.forEach((it, i) => {
      if (it.kind !== prevKind) {
        html += `<div class="tp-field-head">${it.kind === 'stop' ? 'Bus stops' : 'Buildings'}</div>`;
        prevKind = it.kind;
      }
      html += `
      <button type="button" class="tp-field-item" data-i="${i}">
        <span class="tp-field-item-name"></span>
        <span class="tp-field-item-meta"></span>
      </button>`;
    });
    f.results.innerHTML = html;
    [...f.results.querySelectorAll('.tp-field-item')].forEach((btn) => {
      const it = f.items[Number(btn.dataset.i)];
      btn.querySelector('.tp-field-item-name').textContent = it.name;
      btn.querySelector('.tp-field-item-meta').textContent =
        it.kind === 'stop' ? it.meta : [it.number && `#${it.number}`, it.address].filter(Boolean).join(' · ');
      btn.addEventListener('click', () => this._pickResult(field, it));
    });
    f.results.hidden = false;
  }

  _closeFieldResults(field) {
    this._fields[field].results.hidden = true;
  }

  _pickResult(field, it) {
    if (it.kind === 'stop') {
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

  /** Drag-to-adjust: a pin fixed at the map's screen centre while the rider
   *  pans the map underneath it (the classic Uber/Maps "set pickup" pattern) --
   *  more precise than a raw click, especially on mobile where a fingertip
   *  covers far more than the point being tapped. Toggling the same field's
   *  pin button again cancels; the overlay's own "Set location" button
   *  confirms. */
  _togglePicking(field) {
    // The drag-to-adjust pin lives on the map, which the full-screen search overlay
    // covers entirely -- close it first so the pin (and the map underneath it) is
    // actually visible instead of silently starting off-screen.
    if (this._overlayField === field) this._closeOverlay();
    if (this._pickMode === field) {
      this._stopPicking(true); // cancel: leave the point as it was
      return;
    }
    this._startAdjusting(field);
  }

  _startAdjusting(field) {
    this._stopPicking(true); // in case the OTHER field was mid-adjust
    const map = getMap();
    if (!map) return;
    this._pickMode = field;
    for (const f of Object.keys(this._fields)) {
      this._fields[f].pinBtn.classList.toggle('is-active', f === field);
    }
    TripPlanner.setMarkerVisible(field, false);
    this._showCenterPin(field);
    // Start the pin exactly where the field's current point is (if it has
    // one) so adjusting reads as "fine-tune", not "start over from nowhere".
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

    const bar = document.createElement('div');
    bar.className = 'tp-center-pin-bar';
    bar.innerHTML = `
      <span class="tp-center-pin-hint">Drag the map to move the pin</span>
      <button type="button" class="tp-center-pin-confirm">Set ${field === 'origin' ? 'origin' : 'destination'}</button>`;
    bar.querySelector('.tp-center-pin-confirm').addEventListener('click', () => this._confirmAdjusting());
    container.appendChild(bar);
    this._centerPinBar = bar;
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
    for (const f of Object.values(this._fields)) f.pinBtn.classList.remove('is-active');
    const map = getMap();
    map?.getCanvas()?.classList.remove('tp-picking-cursor');
    this._centerPinEl?.remove();
    this._centerPinEl = null;
    this._centerPinBar?.remove();
    this._centerPinBar = null;
    if (restoreMarker) TripPlanner.setMarkerVisible(field, true);
  }

  _applyPoint(field, point) {
    this._closeFieldResults(field);
    if (this._overlayField === field) this._closeOverlay(); // back to the field-pair screen, Maps-style
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
    TripPlanner.selectItinerary(index);
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
    const dist = leg.distanceM != null ? ` · ${Math.round(leg.distanceM)}m` : '';
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
    return `
      <div class="tp-leg tp-leg--wait${lastClass}">
        <span class="tp-leg-dot tp-leg-dot--wait"></span>
        <span class="tp-leg-text">${
          known ? `Wait <b>${timeLabel}</b>` : '<span class="tp-leg-sub--muted">Wait time unknown</span>'
        }</span>
      </div>`;
  }

  _rideRowHtml(leg, isLast) {
    const lastClass = isLast ? ' tp-leg--last' : '';
    const color = normalizeColor(leg.color);
    const textColor = readableTextColor(color);
    const mins = Math.round((leg.rideS || 0) / 60);
    return `
      <div class="tp-leg tp-leg--ride${lastClass}">
        <span class="tp-leg-dot" style="background:${color}"></span>
        <span class="tp-leg-text">
          <span class="tp-route-badge" style="background:${color};color:${textColor}">${esc(leg.lineName || leg.lineId)}</span>
          <span class="tp-leg-sub">${mins} min ride</span>
        </span>
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
    const dist = leg.distanceM != null ? ` (${Math.round(leg.distanceM)}m)` : '';
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
    return `
      <div class="tp-detail-step tp-detail-step--wait">
        <span class="tp-leg-dot tp-leg-dot--wait"></span>
        <div class="tp-detail-step-body">
          <div class="tp-detail-step-main">
            ${known ? `Wait ${timeLabel}` : '<span class="tp-leg-sub--muted">Wait time unknown</span>'}
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
      steps.push({ type: 'wait', waitS: leg.waitS });
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
