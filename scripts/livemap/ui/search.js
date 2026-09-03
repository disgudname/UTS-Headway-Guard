// livemap/ui/search.js
// -----------------------------------------------------------------------------
// Search (top-centre). One box over two indexes:
//
//   * Vehicles — every bus / CAT bus / van currently on the map, matched
//     client-side on its number (and, for a dispatcher, its block). Picking one
//     flies to it and locks the follow camera (same as the follow chip).
//   * Buildings — UVA building footprints via `/v1/uva/facility_search` (a proxy
//     of UVA Facilities' public search, the same service the Visitor Map uses).
//     Picking one flies to it and highlights the footprint.
//
// Vehicle matches are shown first (they're the live, operational thing); results
// are grouped under "Vehicles" / "Buildings" headers when both are present.
// -----------------------------------------------------------------------------

import { API_BASE } from '../core/config.js';
import { getMap } from '../core/map.js';
import { debounce } from '../core/util.js';
import { highlightBuilding, clearBuildingHighlight } from '../core/layers/building-highlight.js';
import { listVehicles, followVehicle, unitDisplayName } from '../core/layers/vehicles.js';

const MIN_CHARS = 2;
const DEBOUNCE_MS = 220;
const MAX_VEHICLES = 8;

export class SearchBox {
  mount() {
    const el = document.createElement('div');
    el.className = 'livemap-search';
    el.innerHTML = `
      <div class="lsb-field">
        <span class="lsb-icon" aria-hidden="true"></span>
        <input type="text" class="lsb-input" placeholder="Search vehicles or buildings…"
               autocomplete="off" spellcheck="false" aria-label="Search vehicles or buildings" />
        <button type="button" class="lsb-clear" aria-label="Clear" hidden>&times;</button>
      </div>
      <div class="lsb-results" role="listbox" hidden></div>`;

    this._el = el;
    this._input = el.querySelector('.lsb-input');
    this._clear = el.querySelector('.lsb-clear');
    this._results = el.querySelector('.lsb-results');
    this._items = [];
    this._active = -1;
    this._reqSeq = 0;
    this._loading = false; // a building fetch is in flight for the current query

    this._input.addEventListener('input', () => {
      this._clear.hidden = !this._input.value;
      this._debouncedSearch(this._input.value.trim());
    });
    this._input.addEventListener('keydown', (e) => this._onKey(e));
    this._input.addEventListener('focus', () => {
      if (this._items.length) this._openResults();
    });
    this._clear.addEventListener('click', () => this._reset(true));
    document.addEventListener('click', (e) => {
      if (!el.contains(e.target)) this._closeResults();
    });

    this._debouncedSearch = debounce((q) => this._search(q), DEBOUNCE_MS);
    document.body.appendChild(el);
    return this;
  }

  unmount() {
    this._el?.remove();
  }

  async _search(q) {
    if (q.length < MIN_CHARS) {
      this._loading = false;
      this._render([]);
      return;
    }
    const seq = ++this._reqSeq;

    // Vehicles resolve instantly off the in-memory index — show them right away,
    // then fold the building results in when the fetch lands. Until it does we're
    // still "Searching…" — never say "No matches" while the building lookup is in
    // flight, or a building-only query flashes a wrong empty state first.
    const vehicles = matchVehicles(q);
    this._loading = true;
    this._render(vehicles);

    this._el.classList.add('is-loading');
    try {
      const r = await fetch(`${API_BASE}/v1/uva/facility_search?q=${encodeURIComponent(q)}`, {
        cache: 'no-store',
      });
      if (seq !== this._reqSeq) return; // superseded by a newer keystroke
      const data = r.ok ? await r.json() : { results: [] };
      const buildings = (Array.isArray(data.results) ? data.results : []).map((b) => ({
        kind: 'building',
        name: b.name,
        number: b.number,
        address: b.address,
        geometry: b.geometry,
        bbox: b.bbox,
      }));
      this._loading = false;
      this._render([...vehicles, ...buildings]);
    } catch {
      if (seq === this._reqSeq) {
        this._loading = false;
        this._render(vehicles);
      }
    } finally {
      if (seq === this._reqSeq) this._el.classList.remove('is-loading');
    }
  }

  _render(items) {
    this._items = items;
    this._active = -1;

    if (!items.length) {
      const long = this._input.value.trim().length >= MIN_CHARS;
      this._results.innerHTML = long
        ? `<div class="lsb-empty">${this._loading ? 'Searching…' : 'No matches'}</div>`
        : '';
      this._results.hidden = !this._results.innerHTML;
      return;
    }

    const rowHtml = (it, i) => `
      <button type="button" class="lsb-item" role="option" data-i="${i}">
        ${
          it.kind === 'vehicle'
            ? `<span class="lsb-swatch" style="background:${esc(it.color || '#000')}"></span>`
            : ''
        }
        <span class="lsb-text">
          <span class="lsb-name"></span>
          <span class="lsb-meta"></span>
        </span>
      </button>`;

    let html = '';
    let prevKind = null;
    items.forEach((it, i) => {
      if (it.kind !== prevKind) {
        html += `<div class="lsb-head">${it.kind === 'vehicle' ? 'Vehicles' : 'Buildings'}</div>`;
        prevKind = it.kind;
      }
      html += rowHtml(it, i);
    });
    this._results.innerHTML = html;

    [...this._results.querySelectorAll('.lsb-item')].forEach((btn) => {
      const i = Number(btn.dataset.i);
      const it = items[i];
      btn.querySelector('.lsb-name').textContent = it.name;
      btn.querySelector('.lsb-meta').textContent =
        it.kind === 'vehicle'
          ? it.meta
          : [it.number && `#${it.number}`, it.address].filter(Boolean).join(' · ');
      btn.addEventListener('click', () => this._pick(i));
    });
    this._openResults();
  }

  _openResults() {
    if (this._results.innerHTML) this._results.hidden = false;
  }

  _closeResults() {
    this._results.hidden = true;
  }

  _onKey(e) {
    if (e.key === 'Escape') {
      if (this._input.value) this._reset(true);
      else this._closeResults();
      return;
    }
    if (!this._items.length) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      const dir = e.key === 'ArrowDown' ? 1 : -1;
      this._active = (this._active + dir + this._items.length) % this._items.length;
      this._syncActive();
    } else if (e.key === 'Enter') {
      e.preventDefault();
      this._pick(this._active >= 0 ? this._active : 0);
    }
  }

  _syncActive() {
    [...this._results.querySelectorAll('.lsb-item')].forEach((b) => {
      const i = Number(b.dataset.i);
      b.classList.toggle('is-active', i === this._active);
      if (i === this._active) b.scrollIntoView({ block: 'nearest' });
    });
  }

  _pick(i) {
    const it = this._items[i];
    if (!it) return;
    this._input.value = it.name;
    this._clear.hidden = false;
    this._closeResults();
    this._input.blur();
    if (it.kind === 'vehicle') this._pickVehicle(it);
    else this._pickBuilding(it);
  }

  _pickVehicle(v) {
    clearBuildingHighlight();
    const map = getMap();
    if (!map) {
      followVehicle(v.id);
      return;
    }
    const zoom = Math.max(map.getZoom(), 15.5);
    map.flyTo({ center: [v.lng, v.lat], zoom, duration: 850 });
    // Lock the follow camera once the fly-to settles (following snaps the centre
    // every frame, which would otherwise fight the animation).
    map.once('moveend', () => followVehicle(v.id));
  }

  _pickBuilding(b) {
    highlightBuilding(b.geometry);
    const map = getMap();
    if (map && b.bbox) {
      map.fitBounds(
        [
          [b.bbox[0], b.bbox[1]],
          [b.bbox[2], b.bbox[3]],
        ],
        { padding: 90, maxZoom: 18, duration: 850 },
      );
    }
  }

  _reset(focus) {
    this._input.value = '';
    this._clear.hidden = true;
    this._items = [];
    this._loading = false;
    this._reqSeq++; // abandon any in-flight building fetch
    this._render([]);
    this._closeResults();
    clearBuildingHighlight();
    if (focus) this._input.focus();
  }
}

// --- vehicle matching --------------------------------------------------------

// Number words for "block four" etc. Blocks realistically top out in the teens.
const NUM_WORDS = {
  zero: 0, one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9,
  ten: 10, eleven: 11, twelve: 12, thirteen: 13, fourteen: 14, fifteen: 15, sixteen: 16,
  seventeen: 17, eighteen: 18, nineteen: 19, twenty: 20,
};

/** The number(s) inside a bracketed block value: "[04]" -> {4}, "[01]/[04]" -> {1,4}. */
function blockNums(block) {
  const out = new Set();
  const re = /\[\s*0*(\d+)\s*\]/g;
  let m;
  while ((m = re.exec(String(block || '')))) out.add(Number(m[1]));
  return out;
}

/**
 * Read a block number out of a query — "block 4", "block four", "blk4", "b 4",
 * "b four", "block #4", "[4]", "[04]", or a bare 1–2 digit number. Returns the
 * number, or null when the query isn't block-shaped.
 */
function blockQueryNum(q) {
  const s = q.toLowerCase().trim();
  let m = s.match(/^(?:block|blk|b)\s*(?:no\.?|number|#)?\s*\[?\s*0*(\d{1,3})\s*\]?$/);
  if (m) return Number(m[1]);
  m = s.match(/^(?:block|blk|b)\s+([a-z]+)$/);
  if (m && m[1] in NUM_WORDS) return NUM_WORDS[m[1]];
  m = s.match(/^\[\s*0*(\d{1,3})\s*\]$/); // "[4]" / "[04]" on its own
  if (m) return Number(m[1]);
  if (/^\d{1,2}$/.test(s)) return Number(s); // bare short number could be a block
  return null;
}

/** Match the live vehicle index against a query; best matches first, capped. */
function matchVehicles(q) {
  const ql = q.toLowerCase();
  const qDigits = ql.replace(/\D/g, '');
  const qBareBlock = ql.replace(/[[\]]/g, '');
  const qBlock = blockQueryNum(ql);
  const qBlockExplicit = qBlock != null && !/^\d+$/.test(ql.trim()); // worded / "block N" form

  const scored = [];
  for (const v of listVehicles()) {
    if (!Number.isFinite(v.lng) || !Number.isFinite(v.lat)) continue;
    const label = String(v.label).toLowerCase();
    const block = String(v.block).toLowerCase();
    const blockBare = block.replace(/[[\]]/g, '');
    const bNums = blockNums(v.block);

    let score = -1;
    if (label === ql) score = 0;
    else if (label.startsWith(ql)) score = 1;
    else if (qBlockExplicit && bNums.has(qBlock)) score = 2; // "block 4" -> [04]
    else if (qDigits.length >= 2 && label.replace(/\D/g, '').startsWith(qDigits)) score = 3;
    else if (qBlock != null && bNums.has(qBlock)) score = 4; // bare "4" -> [04]
    else if (label.includes(ql)) score = 5;
    else if (block && (block.includes(ql) || (blockBare && blockBare === qBareBlock))) score = 6;
    if (score < 0) continue;

    scored.push({ v, score });
  }

  scored.sort(
    (a, b) =>
      a.score - b.score ||
      String(a.v.label).localeCompare(String(b.v.label), undefined, { numeric: true }),
  );

  return scored.slice(0, MAX_VEHICLES).map(({ v }) => ({
    kind: 'vehicle',
    id: v.id,
    lng: v.lng,
    lat: v.lat,
    color: v.routeColor,
    name: unitDisplayName(v.agency, v.label),
    meta: [v.block, v.routeName].filter(Boolean).join(' · '),
  }));
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
