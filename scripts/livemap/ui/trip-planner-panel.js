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
import * as TripPlanner from '../core/trip-planner.js';

const MIN_CHARS = 2;
const DEBOUNCE_MS = 220;
const NEGLIGIBLE_WALK_M = 20;

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
        <span class="tp-card-title">Plan a trip</span>
        <button type="button" class="tp-close" aria-label="Close">&times;</button>
      </div>
      <div class="tp-fields">
        <div class="tp-field" data-field="origin">
          <span class="tp-field-badge tp-field-badge--origin">A</span>
          <input type="text" placeholder="Origin — building or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location">${ICONS.locate}</button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Click the map to set" aria-label="Click the map to set">${ICONS.pin}</button>
          <div class="tp-field-results" hidden></div>
        </div>
        <div class="tp-field" data-field="destination">
          <span class="tp-field-badge tp-field-badge--destination">B</span>
          <input type="text" placeholder="Destination — building or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location" hidden>${ICONS.locate}</button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Click the map to set" aria-label="Click the map to set">${ICONS.pin}</button>
          <div class="tp-field-results" hidden></div>
        </div>
        <button type="button" class="tp-swap" title="Swap origin and destination" aria-label="Swap origin and destination">${ICONS.swap}</button>
      </div>
      <div class="tp-when">
        <button type="button" class="tp-when-btn is-active" data-when="now">Now</button>
        <button type="button" class="tp-when-btn" data-when="later">Later…</button>
        <input type="datetime-local" class="tp-when-input" hidden />
      </div>
      <div class="tp-results" hidden></div>`;

    this._el = el;
    this._cardEl = cardEl;
    this._toggle = el.querySelector('.tp-toggle');
    this._card = cardEl;
    this._results = cardEl.querySelector('.tp-results');
    this._whenInput = cardEl.querySelector('.tp-when-input');
    this._pickMode = null; // 'origin' | 'destination' | null
    this._when = null; // Date | null ("now")

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

    // Mobile-only bottom sheet: tapping the handle toggles between a short "peek"
    // height and a tall "full" one (see the (max-width: 768px) CSS) -- a fixed
    // two-state toggle rather than real drag-to-resize, but reads the same way at
    // rest. Harmless on desktop, where the handle itself is hidden via CSS.
    cardEl.querySelector('.tp-drag-handle').addEventListener('click', () => {
      this._card.classList.toggle('is-expanded');
    });

    this._toggle.addEventListener('click', () => this._setExpanded(true));
    cardEl.querySelector('.tp-close').addEventListener('click', () => {
      this._setExpanded(false);
      TripPlanner.clearAll();
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

    this._mapClickHandler = (e) => this._onMapClick(e);

    const root = parent || document.body;
    root.appendChild(el);
    root.appendChild(cardEl);
    return this;
  }

  unmount() {
    this._unsubOrigin?.();
    this._unsubDestination?.();
    this._unsubStatus?.();
    this._unsubItineraries?.();
    this._unsubSelected?.();
    this._unsubCatEnabled?.();
    this._stopPicking();
    this._el?.remove();
    this._cardEl?.remove();
  }

  _setExpanded(open) {
    this._toggle.setAttribute('aria-expanded', String(open));
    this._card.hidden = !open;
    this._toggle.hidden = open;
  }

  _wireField(field) {
    const f = this._fields[field];
    f.debouncedSearch = debounce((q) => this._searchField(field, q), DEBOUNCE_MS);
    f.input.addEventListener('input', () => f.debouncedSearch(f.input.value.trim()));
    f.input.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') this._closeFieldResults(field);
    });
    document.addEventListener('click', (e) => {
      if (!f.wrap.contains(e.target)) this._closeFieldResults(field);
    });
    f.locateBtn?.addEventListener('click', () => this._useMyLocation(field));
    f.pinBtn.addEventListener('click', () => this._togglePicking(field));
  }

  async _searchField(field, q) {
    const f = this._fields[field];
    if (q.length < MIN_CHARS) {
      f.items = [];
      this._renderFieldResults(field);
      return;
    }
    const seq = ++f.reqSeq;
    try {
      const r = await fetch(`${API_BASE}/v1/uva/facility_search?q=${encodeURIComponent(q)}`, {
        cache: 'no-store',
      });
      if (seq !== f.reqSeq) return;
      const data = r.ok ? await r.json() : { results: [] };
      f.items = (Array.isArray(data.results) ? data.results : []).map((b) => ({
        name: b.name,
        number: b.number,
        address: b.address,
        bbox: b.bbox,
      }));
      this._renderFieldResults(field);
    } catch {
      if (seq === f.reqSeq) {
        f.items = [];
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
    f.results.innerHTML = f.items
      .map(
        (it, i) => `
      <button type="button" class="tp-field-item" data-i="${i}">
        <span class="tp-field-item-name"></span>
        <span class="tp-field-item-meta"></span>
      </button>`,
      )
      .join('');
    [...f.results.querySelectorAll('.tp-field-item')].forEach((btn) => {
      const it = f.items[Number(btn.dataset.i)];
      btn.querySelector('.tp-field-item-name').textContent = it.name;
      btn.querySelector('.tp-field-item-meta').textContent = [
        it.number && `#${it.number}`,
        it.address,
      ]
        .filter(Boolean)
        .join(' · ');
      btn.addEventListener('click', () => this._pickBuilding(field, it));
    });
    f.results.hidden = false;
  }

  _closeFieldResults(field) {
    this._fields[field].results.hidden = true;
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

  _togglePicking(field) {
    if (this._pickMode === field) {
      this._stopPicking();
      return;
    }
    this._pickMode = field;
    for (const other of Object.keys(this._fields)) {
      this._fields[other].pinBtn.classList.toggle('is-active', other === field);
    }
    const map = getMap();
    map?.getCanvas()?.classList.add('tp-picking-cursor');
    map?.on('click', this._mapClickHandler);
  }

  _stopPicking() {
    if (!this._pickMode) return;
    this._pickMode = null;
    for (const f of Object.values(this._fields)) f.pinBtn.classList.remove('is-active');
    const map = getMap();
    map?.getCanvas()?.classList.remove('tp-picking-cursor');
    map?.off('click', this._mapClickHandler);
  }

  _onMapClick(e) {
    const field = this._pickMode;
    if (!field) return;
    this._applyPoint(field, { lat: e.lngLat.lat, lng: e.lngLat.lng, label: 'Dropped pin' });
    this._stopPicking();
  }

  _applyPoint(field, point) {
    this._closeFieldResults(field);
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
    if (!itineraries.length) return; // status handler already covers empty/error
    this._results.hidden = false;
    // Mobile bottom sheet: pop open to the "full" height once there's something to
    // show, same as Maps auto-expanding its sheet when directions come back, rather
    // than leaving results collapsed behind a peek-height sheet. No-op on desktop.
    this._card.classList.add('is-expanded');
    this._results.innerHTML = itineraries.map((it, i) => this._itineraryCardHtml(it, i)).join('');
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.addEventListener('click', () => TripPlanner.selectItinerary(Number(card.dataset.i)));
    });
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
    const legsHtml = visibleLegs.map((leg, li) => this._legRowHtml(leg, li === visibleLegs.length - 1)).join('');
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

  _legRowHtml(leg, isLast) {
    const lastClass = isLast ? ' tp-leg--last' : '';
    if (leg.kind === 'walk') {
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
    const color = normalizeColor(leg.color);
    const textColor = readableTextColor(color);
    const mins = Math.round((leg.rideS || 0) / 60);
    const waitSub =
      leg.waitS != null
        ? `<span class="tp-leg-sub"> · ${Math.max(0, Math.round(leg.waitS / 60))} min wait</span>`
        : '<span class="tp-leg-sub tp-leg-sub--muted"> · wait unknown</span>';
    return `
      <div class="tp-leg tp-leg--ride${lastClass}">
        <span class="tp-leg-dot" style="background:${color}"></span>
        <span class="tp-leg-text">
          <span class="tp-route-badge" style="background:${color};color:${textColor}">${esc(leg.lineName || leg.lineId)}</span>
          <span class="tp-leg-sub">${mins} min ride${waitSub}</span>
        </span>
      </div>`;
  }

  _syncSelected(index) {
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.classList.toggle('is-selected', Number(card.dataset.i) === index);
    });
  }
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
