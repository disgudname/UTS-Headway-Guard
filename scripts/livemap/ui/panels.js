// livemap/ui/panels.js
// -----------------------------------------------------------------------------
// The map chrome, laid out like testmap: a LEFT control panel (collapsible
// sections) and a RIGHT column (dispatcher status panel over the route
// selector), each able to slide off-screen via a screen-edge tab.
//
//   LEFT   feed-status chip · Map View (theme · satellite · center) · Layers
//   RIGHT  Status (dispatcher-auth only) · Routes
//
// Per-section collapse, per-panel hidden state, layer toggles and hidden routes
// all persist to localStorage.
// -----------------------------------------------------------------------------

import { DEFAULT_VIEW } from '../core/config.js';
import { getMap } from '../core/map.js';
import { lsGet, lsSet } from '../core/util.js';
import { onStatus, onVehicles } from '../core/data/transloc.js';
import { getThemeMode, setThemeMode, onThemeChange } from '../core/theme.js';
import { onRouteVisibility, setGroupHidden, setAllHidden } from '../core/layers/routes.js';
import { areStopsVisible, setStopsVisible } from '../core/layers/stops.js';
import { areLabelsVisible, setLabelsVisible } from '../core/layers/vehicles.js';
import { isSatelliteVisible, setSatelliteVisible } from '../core/layers/satellite.js';
import {
  isCatEnabled,
  setCatEnabled,
  onCatEnabled,
  onCatRouteVisibility,
  setCatGroupHidden,
  setCatAllHidden,
} from '../core/data/cat.js';
import {
  isMicroAvailable,
  isRideOn,
  isFlexOn,
  setMicroEnabled,
  onMicroAvailable,
  onMicroEnabled,
} from '../core/data/microtransit.js';
import { isSafetyOn, setSafety, onSafety } from '../core/data/safety.js';
import { isAuthed, onDispatcher, refresh as refreshSession } from '../core/data/session.js';
import { dispatcherOverlaysAllowed } from '../core/modes.js';

const K = {
  left: 'livemap.panel.left.away',
  right: 'livemap.panel.right.away',
  section: (id) => `livemap.section.${id}.collapsed`,
};

const STATUS_META = {
  live: { label: 'Live', cls: 'is-live' },
  polling: { label: 'Polling', cls: 'is-polling' },
  down: { label: 'Offline', cls: 'is-down' },
};

const STATUS_POLL_MS = 60_000;
const ALERTS_POLL_MS = 60_000;
const THEME_MODES = [
  ['auto', 'Auto'],
  ['light', 'Day'],
  ['dark', 'Night'],
];

export class Panels {
  mount() {
    this._left = buildLeft();
    this._right = buildRight();
    document.body.append(this._left.el, this._right.el, this._left.tab, this._right.tab);

    applyAway(this._left, K.left, lsGet(K.left, '0') === '1');
    applyAway(this._right, K.right, lsGet(K.right, '0') === '1');
    if (isCompactViewport()) {
      applyAway(this._left, K.left, true);
      applyAway(this._right, K.right, true);
    }

    this._offRoutes = onRouteVisibility((groups) => this._right.renderRoutes(groups));
    this._offCatRoutes = onCatRouteVisibility((groups) => this._right.renderCatRoutes(groups));
    this._offCatShown = onCatEnabled((on) => this._right.setCatRoutesVisible(on));
    this._offStatus = onStatus((s) => this._left.renderFeed(s));
    this._offTheme = onThemeChange(() => this._left.syncTheme());
    this._left.syncTheme();

    this._staleCount = 0;
    this._offVehicles = onVehicles((list) => {
      this._staleCount = countStaleInService(list);
      this._right.renderStatus(this._statusData, this._staleCount);
    });
    this._pollStatus();
    this._statusTimer = setInterval(() => this._pollStatus(), STATUS_POLL_MS);

    this._pollAlerts();
    this._alertsTimer = setInterval(() => this._pollAlerts(), ALERTS_POLL_MS);
    this._offCat = onCatEnabled(() => this._pollAlerts()); // CAT alerts in/out with the overlay
    return this;
  }

  unmount() {
    this._offRoutes?.();
    this._offCatRoutes?.();
    this._offCatShown?.();
    this._offStatus?.();
    this._offTheme?.();
    this._offVehicles?.();
    this._offCat?.();
    clearInterval(this._statusTimer);
    clearInterval(this._alertsTimer);
    for (const n of [this._left.el, this._right.el, this._left.tab, this._right.tab]) n.remove();
  }

  async _pollAlerts() {
    const reqs = [fetch('/v1/transloc/alerts?rows=25', { cache: 'no-store' })];
    if (isCatEnabled()) reqs.push(fetch('/v1/testmap/cat/service-alerts', { cache: 'no-store' }));
    const [utsRes, catRes] = await Promise.allSettled(reqs);
    const out = [];
    try {
      if (utsRes.status === 'fulfilled' && utsRes.value.ok) {
        const data = await utsRes.value.json();
        const rows = Array.isArray(data?.Rows) ? data.Rows : Array.isArray(data) ? data : [];
        out.push(...rows.map(normalizeAlert).filter(Boolean));
      }
      if (catRes && catRes.status === 'fulfilled' && catRes.value.ok) {
        const data = await catRes.value.json();
        const list = Array.isArray(data?.alerts) ? data.alerts : [];
        out.push(...list.map(normalizeCatAlert).filter(Boolean));
      }
      this._right.renderAlerts(out);
    } catch (err) {
      console.warn('[livemap] service alerts poll failed', err);
    }
  }

  async _pollStatus() {
    const opts = { credentials: 'include', cache: 'no-store' };
    const [onDuty, ab] = await Promise.allSettled([
      fetch('/v1/uts/on_duty', opts),
      fetch('/v1/transloc/anti_bunching/status', opts),
    ]);
    const unauth =
      (onDuty.status === 'fulfilled' && onDuty.value.status === 401) ||
      (ab.status === 'fulfilled' && ab.value.status === 401);
    if (unauth) {
      this._statusData = null;
      this._right.setStatusVisible(false);
      return;
    }
    const json = async (r) =>
      r.status === 'fulfilled' && r.value.ok ? r.value.json().catch(() => null) : null;
    this._statusData = { onDuty: await json(onDuty), antiBunching: await json(ab) };
    this._right.setStatusVisible(true);
    this._right.renderStatus(this._statusData, this._staleCount || 0);
  }
}

// --- left control panel -------------------------------------------------------

function buildLeft() {
  const el = document.createElement('div');
  el.className = 'livemap-panel livemap-panel--left';
  el.innerHTML = `
    <div class="lp-head">
      <span class="lp-feed" title="Live vehicle feed">
        <span class="lp-dot" aria-hidden="true"></span><span class="lp-feed-text">Connecting…</span>
      </span>
    </div>`;

  const mapView = section('mapview', 'Map view');
  mapView.body.innerHTML = `
    <div class="lp-pills lp-theme" role="group" aria-label="Map theme">
      ${THEME_MODES.map(([m, label]) => `<button type="button" data-mode="${m}">${label}</button>`).join('')}
    </div>
    <label class="lp-check"><input type="checkbox" data-t="sat" /> <span>Satellite</span></label>
    <button type="button" class="lp-btn" data-act="center">Center map</button>`;

  const layers = section('layers', 'Layers');
  layers.body.innerHTML = `
    <label class="lp-check"><input type="checkbox" data-t="stops" /> <span>Stops</span></label>
    <label class="lp-check"><input type="checkbox" data-t="labels" /> <span>Vehicle labels</span></label>`;

  const cat = section('cat', 'Charlottesville Area Transit');
  cat.body.innerHTML = `
    <label class="lp-check"><input type="checkbox" data-t="cat" /> <span>Show CAT buses, routes &amp; stops</span></label>`;

  // Dispatcher-only: only revealed once an authed /api/ondemand fetch succeeds.
  const micro = section('micro', 'UVA Ride & FlexRide');
  micro.body.innerHTML = `
    <label class="lp-check"><input type="checkbox" data-t="ride" /> <span>UVA Ride</span></label>
    <label class="lp-check"><input type="checkbox" data-t="flex" /> <span>UVA FlexRide</span></label>`;
  micro.wrap.hidden = !isMicroAvailable();

  const safety = section('safety', 'Traffic & Incidents');
  safety.body.innerHTML = `
    <label class="lp-check"><input type="checkbox" data-t="pulsepoint" /> <span>All PulsePoint incidents</span></label>
    <label class="lp-check"><input type="checkbox" data-t="trafficInc" /> <span>Traffic incidents</span></label>
    <label class="lp-check"><input type="checkbox" data-t="trafficFlow" /> <span>Traffic congestion</span></label>`;

  el.append(mapView.wrap, layers.wrap, cat.wrap, micro.wrap, safety.wrap);

  const themeBtns = [...mapView.body.querySelectorAll('[data-mode]')];
  themeBtns.forEach((b) => b.addEventListener('click', () => setThemeMode(b.dataset.mode)));

  const satBox = mapView.body.querySelector('[data-t="sat"]');
  const stopsBox = layers.body.querySelector('[data-t="stops"]');
  const labelsBox = layers.body.querySelector('[data-t="labels"]');
  satBox.checked = isSatelliteVisible();
  stopsBox.checked = areStopsVisible();
  labelsBox.checked = areLabelsVisible();
  satBox.addEventListener('change', () => setSatelliteVisible(satBox.checked));
  stopsBox.addEventListener('change', () => setStopsVisible(stopsBox.checked));
  labelsBox.addEventListener('change', () => setLabelsVisible(labelsBox.checked));

  // "Vehicle labels" toggles the dispatcher-only number/speed pills — hide the
  // row for a public viewer, for whom it does nothing.
  const labelsRow = labelsBox.closest('.lp-check');
  onDispatcher((isDisp) => {
    if (labelsRow) labelsRow.hidden = !isDisp;
  });

  const catBox = cat.body.querySelector('[data-t="cat"]');
  catBox.checked = isCatEnabled();
  catBox.addEventListener('change', () => setCatEnabled(catBox.checked));
  onCatEnabled((on) => {
    catBox.checked = on;
  });

  const rideBox = micro.body.querySelector('[data-t="ride"]');
  const flexBox = micro.body.querySelector('[data-t="flex"]');
  rideBox.checked = isRideOn();
  flexBox.checked = isFlexOn();
  rideBox.addEventListener('change', () => setMicroEnabled('ride', rideBox.checked));
  flexBox.addEventListener('change', () => setMicroEnabled('flex', flexBox.checked));
  onMicroEnabled(({ ride, flex }) => {
    rideBox.checked = ride;
    flexBox.checked = flex;
  });
  onMicroAvailable((ok) => {
    micro.wrap.hidden = !ok;
  });

  for (const key of ['pulsepoint', 'trafficInc', 'trafficFlow']) {
    const box = safety.body.querySelector(`[data-t="${key}"]`);
    box.checked = isSafetyOn(key);
    box.addEventListener('change', () => setSafety(key, box.checked));
    onSafety(key, (on) => {
      box.checked = on;
    });
  }
  // PulsePoint is dispatcher-only (transit-relevant incidents already auto-show;
  // the checkbox just widens it to "every incident"). Hide the row for the public.
  const ppRow = safety.body.querySelector('[data-t="pulsepoint"]').closest('.lp-check');
  onDispatcher((isDisp) => {
    if (ppRow) ppRow.hidden = !isDisp;
  });
  mapView.body.querySelector('[data-act="center"]').addEventListener('click', () => {
    getMap()?.flyTo({ center: DEFAULT_VIEW.center, zoom: DEFAULT_VIEW.zoom, duration: 800 });
  });

  el.appendChild(buildAuth());

  const dot = el.querySelector('.lp-dot');
  const feedText = el.querySelector('.lp-feed-text');

  const api = {
    el,
    tab: null,
    renderFeed(s) {
      const m = STATUS_META[s] || STATUS_META.down;
      dot.className = `lp-dot ${m.cls}`;
      feedText.textContent = m.label;
    },
    syncTheme() {
      const mode = getThemeMode();
      themeBtns.forEach((b) => b.classList.toggle('is-active', b.dataset.mode === mode));
    },
  };
  api.tab = edgeTab('left', () => toggleAway(api, K.left));
  return api;
}

// --- dispatcher sign-in / sign-out ------------------------------------------

/** A small footer control on the left panel: sign in with the dispatcher
 *  password (POST /api/dispatcher/auth) to unlock block pills / OOS buses /
 *  the status panel, or sign back out. Replaces the legacy nav-bar's login for
 *  livemap, which has no nav-bar. */
function buildAuth() {
  const wrap = document.createElement('div');
  wrap.className = 'lp-auth';
  wrap.innerHTML = `
    <button type="button" class="lp-auth-signin lp-btn">Dispatcher sign-in</button>
    <form class="lp-auth-form" hidden>
      <div class="lp-auth-row">
        <input type="password" class="lp-auth-pw" placeholder="Password"
               autocomplete="current-password" aria-label="Dispatcher password" />
        <button type="submit" class="lp-auth-go">Go</button>
        <button type="button" class="lp-auth-cancel" aria-label="Cancel">&times;</button>
      </div>
      <div class="lp-auth-msg" role="alert" hidden></div>
    </form>
    <div class="lp-auth-in" hidden>
      <span class="lp-auth-who"></span>
      <button type="button" class="lp-auth-signout lp-btn">Sign out</button>
    </div>`;

  const signInBtn = wrap.querySelector('.lp-auth-signin');
  const form = wrap.querySelector('.lp-auth-form');
  const pw = wrap.querySelector('.lp-auth-pw');
  const cancel = wrap.querySelector('.lp-auth-cancel');
  const msg = wrap.querySelector('.lp-auth-msg');
  const inBox = wrap.querySelector('.lp-auth-in');
  const who = wrap.querySelector('.lp-auth-who');

  const showMsg = (text) => {
    msg.textContent = text || '';
    msg.hidden = !text;
  };

  const render = () => {
    const authed = isAuthed();
    signInBtn.hidden = authed;
    form.hidden = true;
    inBox.hidden = !authed;
    if (authed) {
      who.textContent = dispatcherOverlaysAllowed()
        ? 'Dispatcher mode'
        : 'Signed in · public view';
    }
    showMsg('');
    pw.value = '';
  };

  signInBtn.addEventListener('click', () => {
    signInBtn.hidden = true;
    form.hidden = false;
    showMsg('');
    pw.focus();
  });

  cancel.addEventListener('click', render);

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const password = pw.value.trim();
    if (!password) {
      showMsg('Enter the password.');
      pw.focus();
      return;
    }
    form.querySelector('.lp-auth-go').disabled = true;
    showMsg('');
    try {
      const res = await fetch('/api/dispatcher/auth', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ password }),
      });
      if (res.ok) {
        await refreshSession();
        render();
        return;
      }
      let detail = 'Incorrect password.';
      try {
        const data = await res.json();
        if (data && typeof data.detail === 'string' && data.detail.trim()) detail = data.detail.trim();
      } catch {
        /* keep the default */
      }
      showMsg(detail);
      pw.value = '';
      pw.focus();
    } catch (err) {
      console.warn('[livemap] dispatcher sign-in failed', err);
      showMsg('Could not reach the server. Try again.');
    } finally {
      form.querySelector('.lp-auth-go').disabled = false;
    }
  });

  wrap.querySelector('.lp-auth-signout').addEventListener('click', async (e) => {
    const btn = e.currentTarget;
    btn.disabled = true;
    try {
      await fetch('/api/dispatcher/logout', { method: 'POST', credentials: 'include' });
    } catch (err) {
      console.warn('[livemap] dispatcher sign-out failed', err);
    } finally {
      btn.disabled = false;
    }
    await refreshSession();
    render();
  });

  // Pick up a login/logout that happened in another tab.
  onDispatcher(render);
  render();
  return wrap;
}

// --- right column: status + route selector ----------------------------------

function buildRight() {
  const el = document.createElement('div');
  el.className = 'livemap-right-column';

  const status = document.createElement('div');
  status.className = 'livemap-panel livemap-panel--status';
  status.hidden = true;

  const alerts = section('alerts', 'Service alerts', {
    headExtra: '<span class="lp-badge" hidden></span>',
  });
  alerts.wrap.classList.add('livemap-panel', 'livemap-panel--alerts');
  alerts.body.classList.add('lp-alerts-body');
  alerts.wrap.hidden = true; // shown once we know the alert state
  const alertBadge = alerts.wrap.querySelector('.lp-badge');
  let lastAlertCount = 0;

  const bulkHead = `
      <span class="lp-bulk">
        <button type="button" data-bulk="all">All</button>
        <button type="button" data-bulk="none">None</button>
      </span>`;

  const routes = section('routes', 'Routes', { headExtra: bulkHead });
  routes.wrap.classList.add('livemap-panel', 'livemap-panel--routes');
  routes.body.classList.add('lp-routes');

  // CAT's own per-route picker — only shown while the CAT overlay is enabled.
  const catRoutes = section('cat-routes', 'CAT routes', { headExtra: bulkHead });
  catRoutes.wrap.classList.add('livemap-panel', 'livemap-panel--routes');
  catRoutes.body.classList.add('lp-routes');
  catRoutes.wrap.hidden = !isCatEnabled();

  el.append(status, alerts.wrap, routes.wrap, catRoutes.wrap);

  routes.wrap.querySelector('[data-bulk="all"]').addEventListener('click', () => setAllHidden(false));
  routes.wrap.querySelector('[data-bulk="none"]').addEventListener('click', () => setAllHidden(true));
  catRoutes.wrap.querySelector('[data-bulk="all"]').addEventListener('click', () => setCatAllHidden(false));
  catRoutes.wrap.querySelector('[data-bulk="none"]').addEventListener('click', () => setCatAllHidden(true));

  const api = {
    el,
    tab: null,
    setStatusVisible(v) {
      status.hidden = !v;
    },
    renderStatus(data, staleCount) {
      if (status.hidden) return;
      status.innerHTML = renderStatusHtml(data, staleCount);
    },
    renderAlerts(list) {
      alerts.wrap.hidden = false;
      const n = list.length;
      alertBadge.hidden = n === 0;
      alertBadge.textContent = String(n);
      alertBadge.classList.toggle('is-active', n > 0);
      // Surface a freshly-raised alert: expand if the count just went up.
      if (n > lastAlertCount) alerts.wrap.classList.remove('is-collapsed');
      lastAlertCount = n;
      alerts.body.innerHTML = n
        ? list.map(alertItemHtml).join('')
        : '<div class="lp-empty">No active alerts</div>';
    },
    renderRoutes(groups) {
      renderRouteRows(routes.body, groups, setGroupHidden, 'No routes running');
    },
    renderCatRoutes(groups) {
      renderRouteRows(catRoutes.body, groups, setCatGroupHidden, 'No CAT routes');
    },
    setCatRoutesVisible(v) {
      catRoutes.wrap.hidden = !v;
    },
  };
  api.tab = edgeTab('right', () => toggleAway(api, K.right));
  return api;
}

/** Fill a section body with one toggle row per route group. Groups arrive
 *  active-first; idle routes (no bus right now) render dimmed with a note. */
function renderRouteRows(bodyEl, groups, onToggle, emptyText) {
  if (!groups.length) {
    bodyEl.innerHTML = `<div class="lp-empty">${esc(emptyText)}</div>`;
    return;
  }
  bodyEl.textContent = '';
  for (const g of groups) {
    const idle = g.active === false;
    const row = document.createElement('button');
    row.type = 'button';
    row.className = 'lp-row' + (g.hidden ? ' is-off' : '') + (idle ? ' is-idle' : '');
    row.setAttribute('aria-pressed', String(!g.hidden));
    row.innerHTML = `
      <span class="lp-sw" style="background:${g.color}"></span>
      <span class="lp-name-wrap">
        <span class="lp-name"></span>
        ${idle ? '<span class="lp-note">no buses</span>' : ''}
      </span>
      <span class="lp-eye" aria-hidden="true"></span>`;
    row.querySelector('.lp-name').textContent = g.name;
    row.addEventListener('click', () => onToggle(g.name, !g.hidden));
    bodyEl.appendChild(row);
  }
}

function renderStatusHtml(data, staleCount) {
  const od = data?.onDuty || {};
  const line = (label, people, next) => {
    let val;
    if (people && people.length) {
      val = people.map((p) => esc(p.name || p)).join(', ');
      if (next && next.length) {
        val += ` <span class="st-next">→ ${esc(next[0].name)} (${esc(next[0].start)})</span>`;
      }
    } else if (next && next.length) {
      val = `<span class="st-next">Next: ${esc(next[0].name)} at ${esc(next[0].start)}</span>`;
    } else {
      val = '<span class="st-empty">— none —</span>';
    }
    return `<div class="st-line"><span class="st-key">${label}</span> ${val}</div>`;
  };

  const ab = String(data?.antiBunching?.status || 'N/A').toUpperCase();
  const abCls = ab === 'ONLINE' ? 'online' : ab === 'OFFLINE' ? 'offline' : 'na';
  const abTxt = ab === 'ONLINE' ? 'Online' : ab === 'OFFLINE' ? 'Offline' : 'N/A';

  return `
    <div class="lp-head lp-head--plain"><span class="lp-title">Status</span></div>
    <div class="lp-section-body">
      <div class="st-group">
        <div class="st-group-label">On Duty</div>
        ${line('Supervisor:', od.supervisors, od.supervisors_next)}
        ${line('Dispatcher:', od.ondemand_dispatchers, od.ondemand_dispatchers_next)}
      </div>
      <div class="st-group">
        <div class="st-group-label">Active Conditions</div>
        <div class="st-cond"><span class="st-ind st-ind--${abCls}"></span> Anti-Bunching: ${abTxt}</div>
        ${
          staleCount > 0
            ? `<div class="st-cond st-cond--warn"><span class="st-ind st-ind--offline"></span> Stale vehicles (in service): ${staleCount}</div>`
            : ''
        }
      </div>
    </div>`;
}

// --- service alerts -----------------------------------------------------------

/** A TransLoc GetMessagesPaged row -> the shape the alerts list renders. */
function normalizeAlert(row) {
  if (!row || typeof row !== 'object') return null;
  const title = String(
    row.MessageTitle || row.Title || row.Subject || '',
  ).trim();
  const message = String(
    row.MessageText || row.MessageBody || row.Text || row.Description || '',
  ).trim();
  if (!title && !message) return null;
  return {
    id: String(row.MessageId ?? row.MessageID ?? row.Id ?? title ?? message ?? ''),
    title: title || 'Service alert',
    message: title && message && title !== message ? message : title ? '' : message,
    start: msDate(row.StartDateUtc),
    end: msDate(row.EndDateUtc),
  };
}

/** A CAT get_service_announcements row -> the alerts-list shape, tagged "CAT". */
function normalizeCatAlert(a) {
  if (!a || typeof a !== 'object') return null;
  const title = String(a.Title || a.Name || '').trim();
  const message = String(a.Message || a.Description || '').trim();
  if (!title && !message) return null;
  return {
    id: `cat:${a.ID ?? a.Id ?? title ?? message ?? ''}`,
    title: `${title || 'Service alert'} · CAT`,
    message: title && message && title !== message ? message : title ? '' : message,
    start: msDate(a.StartDate || a.Effective),
    end: msDate(a.EndDate || a.Expiration),
  };
}

/** `/Date(1731301260000)/` (or ISO, or ms) -> a short ET date string, or ''. */
function msDate(v) {
  if (v == null) return '';
  let ms = null;
  if (typeof v === 'number') ms = v;
  else if (typeof v === 'string') {
    const m = /\/Date\((-?\d+)/.exec(v);
    if (m) ms = Number(m[1]);
    else {
      const t = Date.parse(v);
      if (!Number.isNaN(t)) ms = t;
    }
  }
  if (ms == null || ms < 0) return '';
  try {
    return new Date(ms).toLocaleString('en-US', {
      timeZone: 'America/New_York',
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  } catch {
    return '';
  }
}

function alertItemHtml(a) {
  const range =
    a.start || a.end
      ? `<div class="sa-when">${esc(a.start || '?')}${a.end ? ` – ${esc(a.end)}` : ''}</div>`
      : '';
  const body = a.message ? `<div class="sa-msg">${esc(a.message)}</div>` : '';
  return `<div class="sa-item"><div class="sa-title">${esc(a.title)}</div>${body}${range}</div>`;
}

// --- shared bits ------------------------------------------------------------

/** A collapsible section. Returns { wrap, body }. */
function section(id, title, opts = {}) {
  const wrap = document.createElement('div');
  wrap.className = 'lp-section';
  if (lsGet(K.section(id), '0') === '1') wrap.classList.add('is-collapsed');

  const head = document.createElement('button');
  head.type = 'button';
  head.className = 'lp-section-head';
  head.innerHTML = `
    <span class="lp-caret" aria-hidden="true"></span>
    <span class="lp-section-title">${esc(title)}</span>
    ${opts.headExtra || ''}`;
  head.addEventListener('click', (e) => {
    if (e.target.closest('.lp-bulk')) return; // bulk buttons aren't the collapse toggle
    const collapsed = wrap.classList.toggle('is-collapsed');
    lsSet(K.section(id), collapsed ? '1' : '0');
  });

  const body = document.createElement('div');
  body.className = 'lp-section-body';

  wrap.append(head, body);
  return { wrap, body };
}

/** A screen-edge tab that slides its panel away. */
function edgeTab(side, onToggle) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = `livemap-panel-tab livemap-panel-tab--${side}`;
  btn.setAttribute('aria-label', side === 'left' ? 'Toggle controls' : 'Toggle routes');
  btn.innerHTML = '<span class="lpt-arrow" aria-hidden="true"></span>';
  btn.addEventListener('click', onToggle);
  return btn;
}

function applyAway(panelApi, key, away) {
  panelApi._away = away;
  panelApi.el.classList.toggle('is-away', away);
  panelApi.tab.classList.toggle('is-away', away);
}

function toggleAway(panelApi, key) {
  const away = !panelApi._away;
  applyAway(panelApi, key, away);
  lsSet(key, away ? '1' : '0');
}

function isCompactViewport() {
  return typeof window !== 'undefined' && window.innerWidth < 760;
}

function countStaleInService(list) {
  let n = 0;
  for (const v of Array.isArray(list) ? list : []) {
    if (v.routeId && v.routeId !== '0' && (v.stale || v.ageS > 90)) n++;
  }
  return n;
}

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
