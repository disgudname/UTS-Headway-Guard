// livemap/ui/route-legend.js
// -----------------------------------------------------------------------------
// The kiosk shells' one static readout: a small top-left panel listing the
// routes that currently have a bus on them, each with its colour swatch. It is
// read-only (no eye toggles, no collapse) — the interactive route picker lives
// only on the full page. A plain kiosk also gets a QR-code footer pointing the
// public at the live tracker; an adminKiosk does not.
//
// Matches the legacy testmap `#routeLegend` behaviour: shown whenever a kiosk
// experience is active, populated from the same "routes with active vehicles"
// set the map lines are drawn from.
// -----------------------------------------------------------------------------

import { onRouteVisibility } from '../core/layers/routes.js';
import { isCatEnabled, onCatEnabled, onCatRouteVisibility } from '../core/data/cat.js';

// Verbatim from testmap: a pre-rendered QR to https://utsopsdashboard.com/.
const QR_SVG =
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 27 27" aria-hidden="true">' +
  '<path fill="none" stroke="#000" stroke-width="1" d="M1 1.5h7m1 0h2m2 0h1m1 0h3m1 0h7m-25 1h1m5 0h1m1 0h3m1 0h3m1 0h1m1 0h1m5 0h1m-25 1h1m1 0h3m1 0h1m1 0h1m1 0h2m4 0h1m1 0h1m1 0h3m1 0h1m-25 1h1m1 0h3m1 0h1m1 0h2m1 0h1m1 0h3m2 0h1m1 0h3m1 0h1m-25 1h1m1 0h3m1 0h1m5 0h5m1 0h1m1 0h3m1 0h1m-25 1h1m5 0h1m1 0h1m5 0h1m1 0h1m1 0h1m5 0h1m-25 1h7m1 0h1m1 0h1m1 0h1m1 0h1m1 0h1m1 0h7m-16 1h1m2 0h1m2 0h2m-17 1h2m2 0h3m4 0h2m6 0h1m1 0h4m-25 1h3m4 0h1m1 0h4m1 0h4m2 0h2m1 0h1m-23 1h1m3 0h3m3 0h6m2 0h1m1 0h2m-21 1h1m1 0h1m2 0h2m1 0h3m1 0h2m1 0h1m1 0h1m2 0h2m-23 1h1m2 0h1m1 0h1m1 0h1m1 0h3m4 0h2m2 0h4m-25 1h2m1 0h1m3 0h2m2 0h1m1 0h5m2 0h1m2 0h1m-18 1h1m2 0h4m2 0h1m1 0h1m1 0h4m-15 1h1m1 0h1m1 0h1m1 0h1m1 0h1m1 0h3m1 0h2m-24 1h5m1 0h2m1 0h1m1 0h2m3 0h7m-15 1h2m1 0h1m1 0h1m1 0h2m3 0h1m-21 1h7m3 0h4m2 0h1m1 0h1m1 0h1m-21 1h1m5 0h1m1 0h2m6 0h1m3 0h4m-24 1h1m1 0h3m1 0h1m1 0h1m1 0h1m1 0h1m3 0h8m-24 1h1m1 0h3m1 0h1m2 0h1m1 0h1m1 0h2m2 0h3m2 0h3m-25 1h1m1 0h3m1 0h1m2 0h3m2 0h5m2 0h1m1 0h1m-24 1h1m5 0h1m1 0h1m3 0h1m5 0h6m-24 1h7m1 0h2m1 0h2m1 0h2m1 0h1m4 0h3"/></svg>';
const QR_URL = 'https://utsopsdashboard.com/';

export class RouteLegend {
  /** @param {{ qr?: boolean }} [opts] — qr: append the "track your bus" footer. */
  constructor(opts = {}) {
    this._qr = opts.qr !== false;
  }

  mount() {
    this._el = document.createElement('div');
    this._el.className = 'livemap-route-legend';
    this._el.setAttribute('aria-live', 'polite');
    this._el.hidden = true;
    document.body.appendChild(this._el);

    this._uts = [];
    this._cat = [];
    this._catOn = isCatEnabled();

    this._offUts = onRouteVisibility((groups) => {
      this._uts = groups || [];
      this._render();
    });
    this._offCat = onCatRouteVisibility((groups) => {
      this._cat = groups || [];
      this._render();
    });
    this._offCatOn = onCatEnabled((on) => {
      this._catOn = on;
      this._render();
    });
    this._render();
    return this;
  }

  unmount() {
    this._offUts?.();
    this._offCat?.();
    this._offCatOn?.();
    this._el?.remove();
  }

  _rows() {
    const rows = [];
    for (const g of this._uts) if (g && !g.hidden) rows.push({ name: g.name, color: g.color });
    if (this._catOn) {
      for (const g of this._cat) if (g && !g.hidden) rows.push({ name: g.name, color: g.color });
    }
    return rows;
  }

  _render() {
    if (!this._el) return;
    const rows = this._rows();
    if (!rows.length) {
      this._el.hidden = true;
      this._el.textContent = '';
      return;
    }

    const frag = document.createDocumentFragment();
    for (const r of rows) {
      const item = document.createElement('div');
      item.className = 'lrl-item';

      const dot = document.createElement('span');
      dot.className = 'lrl-dot';
      dot.style.backgroundColor = r.color || '#000000';
      item.appendChild(dot);

      const name = document.createElement('span');
      name.className = 'lrl-name';
      name.textContent = r.name || 'Route';
      item.appendChild(name);

      frag.appendChild(item);
    }

    if (this._qr) frag.appendChild(this._qrFooter());

    this._el.replaceChildren(frag);
    this._el.hidden = false;
  }

  _qrFooter() {
    const wrap = document.createElement('div');
    wrap.className = 'lrl-qr';

    const sep = document.createElement('div');
    sep.className = 'lrl-qr-sep';
    wrap.appendChild(sep);

    const row = document.createElement('div');
    row.className = 'lrl-qr-row';

    const link = document.createElement('a');
    link.className = 'lrl-qr-code';
    link.href = QR_URL;
    link.target = '_blank';
    link.rel = 'noopener noreferrer';
    link.setAttribute('aria-label', 'Open the UVATransit Operations Dashboard');
    link.innerHTML = QR_SVG;
    row.appendChild(link);

    const text = document.createElement('div');
    text.className = 'lrl-qr-text';
    const label = document.createElement('div');
    label.className = 'lrl-qr-label';
    label.textContent = 'Track your bus live';
    const url = document.createElement('div');
    url.className = 'lrl-qr-url';
    url.textContent = 'utsopsdashboard.com';
    text.append(label, url);
    row.appendChild(text);

    wrap.appendChild(row);
    return wrap;
  }
}
