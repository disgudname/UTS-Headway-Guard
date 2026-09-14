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
import { debounce } from '../core/util.js';
import { onCatEnabled } from '../core/data/cat.js';
import * as TripPlanner from '../core/trip-planner.js';

const MIN_CHARS = 2;
const DEBOUNCE_MS = 220;

export class TripPlannerPanel {
  mount(parent = document.body) {
    const el = document.createElement('div');
    el.className = 'tp-widget';
    el.innerHTML = `
      <button type="button" class="tp-toggle" aria-expanded="false">
        <span class="tp-toggle-icon" aria-hidden="true"></span>
        <span>Plan a trip</span>
      </button>
      <div class="tp-card" hidden>
        <div class="tp-card-head">
          <span class="tp-card-title">Plan a trip</span>
          <button type="button" class="tp-close" aria-label="Close">&times;</button>
        </div>
        <div class="tp-field" data-field="origin">
          <span class="tp-field-badge tp-field-badge--origin">A</span>
          <input type="text" placeholder="Origin — building or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location"></button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Click the map to set" aria-label="Click the map to set"></button>
          <div class="tp-field-results" hidden></div>
        </div>
        <div class="tp-field" data-field="destination">
          <span class="tp-field-badge tp-field-badge--destination">B</span>
          <input type="text" placeholder="Destination — building or address" autocomplete="off" spellcheck="false" />
          <button type="button" class="tp-field-btn tp-field-btn--locate" title="Use my location" aria-label="Use my location" hidden></button>
          <button type="button" class="tp-field-btn tp-field-btn--pin" title="Click the map to set" aria-label="Click the map to set"></button>
          <div class="tp-field-results" hidden></div>
        </div>
        <div class="tp-when">
          <button type="button" class="tp-when-btn is-active" data-when="now">Now</button>
          <button type="button" class="tp-when-btn" data-when="later">Later…</button>
          <input type="datetime-local" class="tp-when-input" hidden />
        </div>
        <div class="tp-results" hidden></div>
      </div>`;

    this._el = el;
    this._toggle = el.querySelector('.tp-toggle');
    this._card = el.querySelector('.tp-card');
    this._results = el.querySelector('.tp-results');
    this._whenInput = el.querySelector('.tp-when-input');
    this._pickMode = null; // 'origin' | 'destination' | null
    this._when = null; // Date | null ("now")

    this._fields = {};
    for (const field of ['origin', 'destination']) {
      const wrap = el.querySelector(`.tp-field[data-field="${field}"]`);
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

    this._toggle.addEventListener('click', () => this._setExpanded(true));
    el.querySelector('.tp-close').addEventListener('click', () => {
      this._setExpanded(false);
      TripPlanner.clearAll();
    });

    for (const btn of el.querySelectorAll('.tp-when-btn')) {
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

    (parent || document.body).appendChild(el);
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

  _syncFieldValue(field, point) {
    const f = this._fields[field];
    f.input.value = point ? point.label || `${point.lat.toFixed(5)}, ${point.lng.toFixed(5)}` : '';
  }

  _setWhenMode(mode) {
    for (const btn of this._el.querySelectorAll('.tp-when-btn')) {
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
    this._el.dataset.status = status;
    if (status === 'loading') {
      this._results.hidden = false;
      this._results.innerHTML = '<div class="tp-status">Finding trips…</div>';
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
    }
  }

  _renderItineraries(itineraries) {
    if (!itineraries.length) return; // status handler already covers empty/error
    this._results.hidden = false;
    this._results.innerHTML = itineraries.map((it, i) => this._itineraryCardHtml(it, i)).join('');
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.addEventListener('click', () => TripPlanner.selectItinerary(Number(card.dataset.i)));
    });
  }

  _itineraryCardHtml(itinerary, i) {
    const mins = Math.round(itinerary.totalDurationS / 60);
    const legsHtml = itinerary.legs.map((leg) => this._legChipHtml(leg)).join('');
    const warn = itinerary.legs.some((l) => l.kind === 'ride' && l.lastRideWarning);
    return `
      <button type="button" class="tp-itin" data-i="${i}">
        <div class="tp-itin-top">
          <span class="tp-itin-time">${mins} min</span>
          ${itinerary.durationIsEstimate ? '<span class="tp-itin-tag">estimated</span>' : ''}
          ${warn ? '<span class="tp-itin-tag tp-itin-tag--warn">last bus soon</span>' : ''}
        </div>
        <div class="tp-itin-legs">${legsHtml}</div>
      </button>`;
  }

  _legChipHtml(leg) {
    if (leg.kind === 'walk') {
      const mins = Math.max(1, Math.round(leg.durationS / 60));
      return `<span class="tp-chip tp-chip--walk">🚶 ${mins}m</span>`;
    }
    const mins = Math.round((leg.rideS || 0) / 60);
    const wait = leg.waitS != null ? ` · ${Math.round(leg.waitS / 60)}m wait` : '';
    return `<span class="tp-chip tp-chip--ride" style="--chip-color:#${String(leg.color || '888').replace(/^#/, '')}">
      ${esc(leg.lineName || leg.lineId)} · ${mins}m${wait}
    </span>`;
  }

  _syncSelected(index) {
    [...this._results.querySelectorAll('.tp-itin')].forEach((card) => {
      card.classList.toggle('is-selected', Number(card.dataset.i) === index);
    });
  }
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
