// Trading cards: draws a card per block and per bus from window.CARD_DATA (scripts/cards-data.js, baked by
// scripts/build_block_cards.py). Used by /blockcards, /buscards and /packs through window.Cards.
(function () {
const DATA = window.CARD_DATA;

const TEAM = {Green: 'Green Line', Orange: 'Orange Line', Gold: 'Gold Line', Silver: 'Silver Line', Purple: 'Purple Line', 'Night Pilot': 'Night Pilot'};
const KIND_LABEL = {wkd: 'Wkdy', sat: 'Sat', sun: 'Sun'};
// Which block a block's bus was, or becomes, on the same day (TransLoc interlines them).
const HANDOFFS = {
  '01': 'Thu&ndash;Sat this bus stays out past 22:00 and turns into <b>Night Pilot [04]</b>.',
  '05': 'Never goes home at 22:00: it turns into <b>Night Pilot [03]</b> at the Library, every night.',
  '03': 'Was <b>Orange [05]</b> all day. Same bus, new sign, four more hours.',
  '04': 'Was <b>Green [01]</b> all day. Only comes out Thursday to Saturday nights.',
  '10': 'Called up from the minors: starts the morning as <b>Purple [17]</b>, flips to Gold at 08:25.',
  '06': 'Called up from the minors: starts the morning as <b>Purple [19]</b>, flips to Orange at 08:25.',
  '17': 'The morning bus turns into <b>Gold [10]</b> at 08:25. A fresh [17] comes back out at 14:25.',
  '19': 'The morning bus turns into <b>Orange [06]</b> at 08:25. A fresh [19] comes back out at 14:25.'
};

function inkFor(hex) {
  const n = parseInt(hex.slice(1), 16), r = n >> 16, g = (n >> 8) & 255, b = n & 255;
  return (0.299 * r + 0.587 * g + 0.114 * b) > 160 ? '#2a2620' : '#ffffff';
}
function clock(s) {
  s = ((s % 86400) + 86400) % 86400;
  return String(Math.floor(s / 3600)).padStart(2, '0') + ':' + String(Math.floor(s % 3600 / 60)).padStart(2, '0');
}
function num(v) { return v == null ? '&ndash;' : Number(v).toLocaleString('en-US'); }
function esc(s) { return String(s).replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c])); }

function decodePolyline(str) {
  const pts = []; let i = 0, lat = 0, lng = 0;
  while (i < str.length) {
    for (const which of [0, 1]) {
      let shift = 0, result = 0, b;
      do { b = str.charCodeAt(i++) - 63; result |= (b & 31) << shift; shift += 5; } while (b >= 32);
      const d = (result & 1) ? ~(result >> 1) : (result >> 1);
      if (which === 0) lat += d; else lng += d;
    }
    pts.push([lat / 1e5, lng / 1e5]);
  }
  return pts;
}
// The block's route lines, fitted into the top part of the card's picture box: the variant it runs most is bold,
// any other variant it runs is a faint ghost behind it, all on one scale so they line up.
function routeSvg(c, ink) {
  const lines = (c.routes || []).map(id => decodePolyline(DATA.shapes[id] || '')).filter(p => p.length);
  if (!lines.length) return '';
  const k = Math.cos(lines[0][0][0] * Math.PI / 180), all = lines.flat();
  const X = p => p[1] * k, Y = p => -p[0];
  const x0 = Math.min(...all.map(X)), x1 = Math.max(...all.map(X)), y0 = Math.min(...all.map(Y)), y1 = Math.max(...all.map(Y));
  const W = 270, H = 300, box = {x: 22, y: 34, w: 226, h: 150};
  const sc = Math.min(box.w / (x1 - x0 || 1), box.h / (y1 - y0 || 1));
  const ox = box.x + (box.w - (x1 - x0) * sc) / 2, oy = box.y + (box.h - (y1 - y0) * sc) / 2;
  const path = pts => pts.map((p, i) => (i ? 'L' : 'M') + (ox + (X(p) - x0) * sc).toFixed(1) + ' ' + (oy + (Y(p) - y0) * sc).toFixed(1)).join('');
  const ghosts = lines.slice(1).map(l => `<path d="${path(l)}" fill="none" stroke="${ink}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" opacity=".38"/>`).join('');
  const d = path(lines[0]);
  return `<svg class="route" viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid slice" aria-hidden="true">${ghosts}
    <path d="${d}" fill="none" stroke="rgba(0,0,0,.22)" stroke-width="9" stroke-linejoin="round" stroke-linecap="round" transform="translate(2 3)"/>
    <path d="${d}" fill="none" stroke="${ink}" stroke-width="5" stroke-linejoin="round" stroke-linecap="round" opacity=".92"/></svg>`;
}

function role(c) {
  const w = c.rows.wkd, pieces = w ? w.pieces : [];
  if (c.family === 'Night Pilot') return 'Closer';
  if (c.block === '10' || c.block === '06') return 'Call-up';
  if (c.block === '17' || c.block === '19') return 'Two-way';
  if (pieces.length > 1) return 'Doubleheader';
  if (pieces.length && pieces[0][0] >= 12 * 3600) return 'Reliever';
  return (c.rows.sat || c.rows.sun) ? 'Everyday' : 'Starter';
}
function runs(c) {
  const r = c.rows;
  if (r.wkd && r.wkd.days === 5 && r.sat && r.sun) return 'Every day';
  if (r.wkd && r.wkd.days === 5) return 'Monday to Friday';
  if (r.wkd && r.sat) return 'Thursday to Saturday';
  return Object.keys(r).map(k => KIND_LABEL[k]).join(', ');
}
// A day's lineup: each stretch with the route variant it runs ("07:30-17:45 Day, 17:45-22:00 Loop").
function piecesText(row) {
  return row.lineup.map(p => {
    const label = DATA.variants[p[2]];
    return clock(p[0]) + '&ndash;' + clock(p[1]) + (label ? ' ' + label : '');
  }).join(', ');
}

// Stickers. Each title goes to the block with the highest value (ties share it); a card can collect several.
// small = the line of small print on the sticker, big = the slab-lettered word, tip = the hover text.
const wk = c => c.rows.wkd || {};
const firstOut = c => Math.min(...Object.values(c.rows).map(r => r.pieces[0][0]));
const lastIn = c => Math.max(...Object.values(c.rows).map(r => r.pieces[r.pieces.length - 1][1]));
const TITLES = [
  {small: 'League leader', big: 'Riders', cls: 'gold', get: c => wk(c).riders, tip: v => `${num(v)} riders on a typical weekday`},
  {small: 'League leader', big: 'Riders/hr', cls: 'gold', get: c => wk(c).per_hour, tip: v => `${v.toFixed(1)} riders per hour on a typical weekday`},
  {small: 'League leader', big: 'Miles', cls: 'gold', get: c => wk(c).miles, tip: v => `${num(v)} miles on a typical weekday`},
  {small: 'League leader', big: 'Hours', cls: 'gold', get: c => c.week_hours, tip: v => `${v.toFixed(1)} hours a week`},
  {small: 'Record day', big: v => num(v), cls: 'red', get: c => c.best_day && c.best_day.riders, tip: (v, c) => `${num(v)} riders on ${c.best_day.date}, the most any block has carried in a day`},
  {small: 'Weekend', big: 'Warrior', cls: 'blue', get: c => ((c.rows.sat || {}).riders || 0) + ((c.rows.sun || {}).riders || 0), tip: v => `${num(v)} riders over a typical Saturday and Sunday`},
  {small: 'Busiest hour', big: 'Rush hour', cls: 'red', get: c => Math.max(...c.by_hour), tip: (v, c) => `${num(v)} boardings in the ${String(c.by_hour.indexOf(v)).padStart(2, '0')}:00 hour on an average weekday`},
  {small: 'Most stops', big: 'On time', cls: '', get: c => c.timestops && c.timestops.count, tip: v => `${v} timestops every weekday`},
  {small: 'Fastest', big: 'Mi/hr', cls: 'blue', get: c => wk(c).miles && wk(c).hours ? Math.round(wk(c).miles / wk(c).hours * 10) / 10 : null, tip: v => `${v.toFixed(1)} miles per hour in service, stops and layovers included`},
  {small: 'Hot stop', big: v => Math.round(v * 100) + '%', cls: '', get: c => c.top_stop && c.top_stop.share, tip: (v, c) => `${Math.round(v * 100)}% of its riders board at ${c.top_stop.name}`},
  {small: 'Most loyal', big: 'Same bus', cls: '', get: c => c.usual_bus && c.usual_bus.of >= 20 ? c.usual_bus.days / c.usual_bus.of : null, tip: (v, c) => `Bus ${c.usual_bus.bus} on ${c.usual_bus.days} of ${c.usual_bus.of} days`},
  {small: 'First out', big: 'Early bird', cls: '', get: c => -firstOut(c), tip: v => `On the road at ${clock(-v)}`},
  {small: 'Last in', big: 'Night owl', cls: 'night', get: c => lastIn(c), tip: v => `Still out at ${clock(v)}`},
  {small: 'Shortest day', big: 'Quick shift', cls: '', get: c => wk(c).hours ? -wk(c).hours : null, tip: v => `${(-v).toFixed(1)} hours and done`}
];
function hourBars(c) {
  // Service-day order, 04:00 first, so Night Pilot's after-midnight hours sit at the end.
  const hours = []; for (let i = 0; i < 24; i++) hours.push((i + 4) % 24);
  const vals = hours.map(h => c.by_hour[h] || 0), max = Math.max(...vals, 1);
  if (max <= 1) return '';
  const first = vals.findIndex(v => v > max * 0.02), last = vals.length - 1 - [...vals].reverse().findIndex(v => v > max * 0.02);
  const span = hours.slice(first, last + 1), sv = vals.slice(first, last + 1);
  const bw = 100 / span.length, peak = sv.indexOf(Math.max(...sv));
  const bars = sv.map((v, i) => `<rect x="${(i * bw + bw * .12).toFixed(2)}" y="${(34 - 32 * v / max).toFixed(1)}" width="${(bw * .76).toFixed(2)}" height="${(32 * v / max).toFixed(1)}" fill="${i === peak ? 'var(--ink)' : 'var(--ink-soft)'}" opacity="${i === peak ? 1 : .55}"/>`).join('');
  return `<div class="hours"><div class="lbl"><span>Boardings by hour, ${String(span[0]).padStart(2, '0')}:00&ndash;${String((span[span.length - 1] + 1) % 24).padStart(2, '0')}:00</span><span>peak ${String(span[peak]).padStart(2, '0')}:00</span></div>
    <svg viewBox="0 0 100 34" preserveAspectRatio="none" aria-hidden="true">${bars}</svg></div>`;
}

function fact(c) {
  if (HANDOFFS[c.block]) return HANDOFFS[c.block];
  if (c.best_day) {
    const d = new Date(c.best_day.date + 'T12:00:00');
    return `Career high: <b>${num(c.best_day.riders)} riders</b> on ${d.toLocaleDateString('en-US', {weekday: 'long', month: 'short', day: 'numeric'})}.`;
  }
  return '';
}

function blockCardHtml(c, stuck) {
  const ink = inkFor(c.color), w = c.rows.wkd || c.rows.sat || c.rows.sun;
  const kinds = ['wkd', 'sat', 'sun'].filter(k => c.rows[k]);
  const first = Math.min(...kinds.map(k => c.rows[k].pieces[0][0]));
  const last = Math.max(...kinds.map(k => c.rows[k].pieces[c.rows[k].pieces.length - 1][1]));
  const rows = kinds.map(k => { const r = c.rows[k]; return `<tr><td>${KIND_LABEL[k]}</td><td>${r.days}</td><td>${r.hours.toFixed(1)}</td><td>${num(r.riders)}</td><td>${r.per_hour == null ? '&ndash;' : r.per_hour.toFixed(1)}</td><td>${num(r.miles)}</td></tr>`; }).join('');
  const weekMiles = kinds.reduce((s, k) => s + (c.rows[k].miles || 0) * c.rows[k].days, 0);
  const vit = [];
  vit.push(['Runs', runs(c)]);
  vit.push(['Weekday', c.rows.wkd ? piecesText(c.rows.wkd) : 'Off']);
  if (c.rows.sat || c.rows.sun) vit.push(['Weekend', piecesText(c.rows.sat || c.rows.sun)]);
  if (c.timestops) vit.push(['Timestops', esc(c.timestops.names.join(', ')) + ` &bull; ${c.timestops.count} a day`]);
  if (c.seasons && c.seasons.length > 1) vit.push(['Seasons', c.seasons.map(x => `${x.name.replace('Spring ', "Spr '").replace('Fall ', "Fall '").replace("'20", "'")} ${num(x.riders)}`).join(' &bull; ') + ' riders/day']);
  if (c.top_stop) vit.push(['Hot stop', esc(c.top_stop.name) + ` (${Math.round(c.top_stop.share * 100)}%)`]);
  if (c.usual_bus) vit.push(['Usual bus', esc(c.usual_bus.bus) + ` (${c.usual_bus.days} of ${c.usual_bus.of} days)`]);
  return `<button class="card" type="button" data-family="${esc(c.family)}" style="--c:${c.color};--c-ink:${ink}" aria-label="Block ${c.block}, ${TEAM[c.family]}. Flip card.">
    <div class="inner">
      <div class="face front">
        <div class="photo">
          ${routeSvg(c, ink)}
          <div class="uts">UVA TRANSIT</div>
          <div class="role${role(c).length > 9 ? ' long' : ''}">${role(c)}</div>
          <div class="num">${c.block}</div>
          ${stuck ? `<div class="stickers${stuck.length > 4 ? ' many' : ''}">${stuck.join('')}</div>` : ''}
        </div>
        <div class="plate"><div class="name">BLOCK ${c.block}</div><div class="team">${TEAM[c.family]}</div></div>
        <div class="strip"><span><b>${clock(first)}</b> first out</span><span><b>${num(w.riders)}</b> riders/day</span><span><b>${clock(last)}</b> last in</span></div>
      </div>
      <div class="face back">
        <div class="bhead"><div class="bnum">${c.block}</div><div><div class="t1">BLOCK ${c.block}</div><div class="t2">${TEAM[c.family]} &bull; ${role(c)}</div></div></div>
        <dl class="vitals">${vit.map(v => `<dt>${v[0]}</dt><dd>${v[1]}</dd>`).join('')}</dl>
        <table>
          <tr><th>Day</th><th>Days</th><th>Hrs</th><th>Riders</th><th>R/Hr</th><th>Mi</th></tr>
          ${rows}
          <tr class="total"><td>Week</td><td>${kinds.reduce((s, k) => s + c.rows[k].days, 0)}</td><td>${c.week_hours.toFixed(1)}</td><td>${num(c.week_riders || null)}</td><td></td><td>${num(Math.round(weekMiles) || null)}</td></tr>
        </table>
        ${hourBars(c)}
        <div class="fact">${fact(c)}</div>
      </div>
    </div>
  </button>`;
}

// ---- Bus cards ----
const MIX_COLOR = Object.assign({Training: '#9aa0a8', Charter: '#3b3f47', Event: '#1f9aa8', Other: '#cfc6b0'}, DATA.family_colors || {});
const longDay = s => new Date(s + 'T12:00:00').toLocaleDateString('en-US', {month: 'short', day: 'numeric', year: 'numeric'});
const monthYear = s => new Date(s + 'T12:00:00').toLocaleDateString('en-US', {month: 'short', year: 'numeric'});
const seasonShort = name => name.replace('Spring ', "Spr '").replace('Summer ', "Sum '").replace('Fall ', "Fall '").replace("'20", "'");
// The first two digits of a bus number are its model year (user, 2026-10-06): 18432 is a 2018.
const modelYear = b => 2000 + +b.bus.slice(0, 2);
const NEWEST_YEAR = Math.max(...(DATA.buses || []).map(modelYear)), OLDEST_YEAR = Math.min(...(DATA.buses || []).map(modelYear));
// A rookie is from the newest model year AND first shows up more than three weeks into the log (an old bus that was
// parked in September also shows up late, and is no rookie).
const isRookie = b => modelYear(b) === NEWEST_YEAR && (new Date(b.first_seen) - new Date(DATA.bus_info.from)) / 864e5 > 21;
// The 24xxx buses carry 18 passengers; everything else 60-70 (user, 2026-10-06). Their rider counts are small because
// the bus is, not because a door counter is dead.
const isSmall = b => b.bus.slice(0, 2) === '24';
const topShare = b => b.blocks.length && b.block_days ? b.blocks[0].days / b.block_days : 0;

function busRole(b) {
  if (isRookie(b)) return 'Rookie';
  if (b.mix.length && b.mix[0].name === 'Training') return 'Trainer';
  if (topShare(b) >= 0.3) return 'Regular';
  if (b.mix.length && b.mix[0].share >= 0.5) return 'Specialist';
  if (b.block_count >= 18) return 'Utility';
  return 'Journeyman';
}

// A side-on bus in the card's ink colour: the "photo" on a bus card.
function busSvg(ink) {
  const windows = [0, 1, 2, 3, 4].map(i => `<rect x="${58 + i * 31}" y="52" width="25" height="24" rx="3"/>`).join('');
  return `<svg class="busart" viewBox="0 0 270 150" aria-hidden="true">
    <g transform="translate(3 4)" fill="rgba(0,0,0,.2)"><rect x="24" y="38" width="222" height="78" rx="12"/></g>
    <rect x="24" y="38" width="222" height="78" rx="12" fill="${ink}" opacity=".92"/>
    <g fill="var(--c)">${windows}<rect x="214" y="52" width="24" height="40" rx="3"/><rect x="34" y="52" width="16" height="24" rx="3"/>
      <rect x="24" y="98" width="222" height="4"/></g>
    <g fill="${ink}"><circle cx="76" cy="118" r="15"/><circle cx="196" cy="118" r="15"/></g>
    <g fill="var(--c)"><circle cx="76" cy="118" r="6.5"/><circle cx="196" cy="118" r="6.5"/></g>
  </svg>`;
}

function mixBar(b) {
  if (!b.mix.length) return '';
  const top = b.mix.slice(0, 5), rest = 1 - top.reduce((s, m) => s + m.share, 0);
  const parts = rest > 0.02 ? top.concat([{name: 'Other', share: rest}]) : top;
  return `<div class="mix"><div class="bar">${parts.map(m => `<i style="width:${(m.share * 100).toFixed(1)}%;background:${MIX_COLOR[m.name] || MIX_COLOR.Other}"></i>`).join('')}</div>
    <div class="key">${parts.slice(0, 4).map(m => `<span style="--k:${MIX_COLOR[m.name] || MIX_COLOR.Other}">${esc(m.name)} ${Math.round(m.share * 100)}%</span>`).join('')}</div></div>`;
}

function busFact(b) {
  if (b.riders && b.riders.best) return `Career high: <b>${num(b.riders.best.riders)} riders</b> on ${longDay(b.riders.best.date)}.`;
  return `Longest day: <b>${num(b.best_miles.miles)} miles</b> on ${longDay(b.best_miles.date)}.`;
}

function busCardHtml(b, stuck) {
  const ink = inkFor(b.color), team = b.family ? TEAM[b.family] : 'Free agent', r = busRole(b);
  const vit = [];
  vit.push(['Class of', modelYear(b) + (isRookie(b) ? `, debut ${monthYear(b.first_seen)}` : '')]);
  if (b.blocks.length) vit.push(['Top blocks', b.blocks.slice(0, 3).map(x => `[${x.block}] ${x.days}`).join(', ') + ' days']);
  if (b.block_count) vit.push(['Range', `${b.block_count} different blocks`]);
  if (b.riders) vit.push(['Riders', `${num(b.riders.median)} on a typical day` + (isSmall(b) ? ' (seats 18)' : '')]);
  if (b.riders && b.riders.top_stop) vit.push(['Hot stop', esc(b.riders.top_stop.name) + ` (${Math.round(b.riders.top_stop.share * 100)}%)`]);
  vit.push(['Long haul', `${num(b.best_miles.miles)} mi on ${longDay(b.best_miles.date)}`]);
  const rows = b.seasons.map(s => `<tr><td>${seasonShort(s.name)}</td><td>${s.days}</td><td>${num(s.miles)}</td><td>${s.days ? Math.round(s.miles / s.days) : '&ndash;'}</td></tr>`).join('');
  return `<button class="card" type="button" data-family="${esc(b.family || '')}" style="--c:${b.color};--c-ink:${ink}" aria-label="Bus ${b.bus}, ${team}. Flip card.">
    <div class="inner">
      <div class="face front">
        <div class="photo">
          ${busSvg(ink)}
          <div class="uts">UVA TRANSIT &bull; CLASS OF ${modelYear(b)}</div>
          <div class="role${r.length > 9 ? ' long' : ''}">${r}</div>
          <div class="num bus">${b.bus}</div>
          ${stuck ? `<div class="stickers${stuck.length > 4 ? ' many' : ''}">${stuck.join('')}</div>` : ''}
        </div>
        <div class="plate"><div class="name">BUS ${b.bus}</div><div class="team">${team}</div></div>
        <div class="strip"><span><b>${num(b.miles)}</b> miles</span><span><b>${b.days}</b> days out</span><span><b>${b.block_count}</b> blocks</span></div>
      </div>
      <div class="face back">
        <div class="bhead"><div class="bnum wide">${b.bus}</div><div><div class="t1">BUS ${b.bus}</div><div class="t2">${team} &bull; ${r}</div></div></div>
        <dl class="vitals">${vit.map(v => `<dt>${v[0]}</dt><dd>${v[1]}</dd>`).join('')}</dl>
        ${mixBar(b)}
        <table>
          <tr><th>Season</th><th>Days</th><th>Miles</th><th>Mi/day</th></tr>
          ${rows}
          <tr class="total"><td>Career</td><td>${b.days}</td><td>${num(b.miles)}</td><td>${Math.round(b.miles / b.days)}</td></tr>
        </table>
        <div class="fact">${busFact(b)}</div>
      </div>
    </div>
  </button>`;
}

const BUS_TITLES = [
  {small: 'League leader', big: 'Miles', cls: 'gold', get: b => b.miles, tip: v => `${num(v)} miles since ${monthYear(DATA.bus_info.from)}`},
  {small: 'Most days out', big: 'Iron man', cls: 'gold', get: b => b.days, tip: v => `In service on ${v} days`},
  {small: 'League leader', big: 'Riders', cls: 'gold', get: b => b.riders && b.riders.days >= 30 ? b.riders.median : null, tip: v => `${num(v)} riders on a typical day`},
  {small: 'Record day', big: v => num(v), cls: 'red', get: b => b.riders && b.riders.best ? b.riders.best.riders : null, tip: (v, b) => `${num(v)} riders on ${b.riders.best.date}, the most any bus has carried in a day`},
  {small: 'Longest day', big: 'Road trip', cls: 'blue', get: b => b.best_miles.miles, tip: (v, b) => `${num(v)} miles on ${b.best_miles.date}`},
  {small: 'Most blocks', big: 'Utility', cls: '', get: b => b.block_count, tip: v => `Has run ${v} different blocks`},
  {small: 'One-block bus', big: 'Loyal', cls: '', get: b => b.block_days >= 40 ? Math.round(topShare(b) * 1000) / 1000 : null, tip: (v, b) => `${Math.round(v * 100)}% of its block days on [${b.blocks[0].block}]`},
  {small: 'Weekend', big: 'Warrior', cls: 'blue', get: b => b.weekend_days, tip: v => `Out on ${v} Saturdays and Sundays`},
  {small: 'Night Pilot', big: 'Night owl', cls: 'night', get: b => b.night_days, tip: v => `${v} nights on Night Pilot`},
  {small: 'Top rookie', big: 'R.O.Y.', cls: 'red', get: b => isRookie(b) ? b.miles : null, tip: v => `${num(v)} miles, the most of any bus that joined during the season`},
  {small: 'Top veteran', big: 'Old pro', cls: 'gold', get: b => modelYear(b) === OLDEST_YEAR ? b.miles : null, tip: (v, b) => `${num(v)} miles, the most of the ${modelYear(b)} buses, the oldest in the fleet`},
  {small: 'Most training', big: 'Teacher', cls: '', get: b => (b.mix.find(m => m.name === 'Training') || {}).share, tip: v => `${Math.round(v * 100)}% of its assignments are driver training`},
  {small: 'Most charters', big: 'Tourist', cls: '', get: b => (b.mix.find(m => m.name === 'Charter') || {}).share, tip: v => `${Math.round(v * 100)}% of its assignments are charters`}
];

// ---- Shared by the pages ----
function awardStickers(items, titles, key) {
  const out = {};
  for (const t of titles) {
    const scored = items.map(c => [c, t.get(c)]).filter(x => x[1] != null && x[1] !== 0 && isFinite(x[1]));
    if (!scored.length) continue;
    const best = Math.max(...scored.map(x => x[1]));
    for (const [c, v] of scored) {
      if (v !== best) continue;
      const big = typeof t.big === 'function' ? t.big(v) : t.big;
      (out[c[key]] = out[c[key]] || []).push(`<div class="sticker ${t.cls}" title="${esc(t.tip(v, c))}">${t.small}<b${String(big).length > 6 ? ' class="long"' : ''}>${big}</b></div>`);
    }
  }
  return out;
}
const rarityOf = n => n >= 4 ? 'legendary' : n >= 2 ? 'rare' : n === 1 ? 'uncommon' : 'common';

// Every card in the set: {id, kind, label, family, color, rarity, html}. html is the whole flip card.
let cache = null;
function all() {
  if (cache) return cache;
  const blockStuck = awardStickers(DATA.cards, TITLES, 'block'), busStuck = awardStickers(DATA.buses || [], BUS_TITLES, 'bus');
  const tag = (html, rarity, id) => html.replace('<button class="card"', `<button class="card" data-id="${id}" data-rarity="${rarity}"`);
  cache = DATA.cards.map(c => {
    const stuck = blockStuck[c.block], rarity = rarityOf((stuck || []).length), id = 'block-' + c.block;
    return {id, kind: 'block', label: c.block, family: c.family, color: c.color, rarity, html: tag(blockCardHtml(c, stuck), rarity, id)};
  }).concat((DATA.buses || []).map(b => {
    const stuck = busStuck[b.bus], rarity = rarityOf((stuck || []).length), id = 'bus-' + b.bus;
    return {id, kind: 'bus', label: b.bus, family: b.family, color: b.color, rarity, html: tag(busCardHtml(b, stuck), rarity, id)};
  }));
  return cache;
}

function nav(here) {
  const links = [['/blockcards', 'Blocks', 'block'], ['/buscards', 'Buses', 'bus'], ['/packs', 'Open a pack', 'packs']];
  return `<nav class="cardnav">${links.map(l => `<a href="${l[0]}"${l[2] === here ? ' class="here"' : ''}>${l[1]}</a>`).join('')}</nav>`;
}

// The gallery pages: every card of one kind, a route filter and "flip all".
function gallery(kind) {
  const cards = all().filter(c => c.kind === kind);
  document.getElementById('nav').innerHTML = nav(kind);
  document.getElementById('count').textContent = cards.length;
  const el = document.getElementById('cards');
  el.innerHTML = cards.map(c => c.html).join('');
  el.addEventListener('click', e => { const card = e.target.closest('.card'); if (card) card.classList.toggle('flipped'); });

  const families = [...new Set(cards.map(c => c.family).filter(Boolean))];
  const filters = document.getElementById('filters');
  filters.innerHTML = '<button type="button" class="on" data-f="">All</button>' + families.map(f => {
    const color = DATA.family_colors[f];
    return `<button type="button" data-f="${esc(f)}" style="--chip:${color};--chip-ink:${inkFor(color)}">${TEAM[f]}</button>`;
  }).join('');
  filters.addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    filters.querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b));
    el.querySelectorAll('.card').forEach(card => { card.style.display = (!b.dataset.f || card.dataset.family === b.dataset.f) ? '' : 'none'; });
  });
  document.getElementById('flipall').addEventListener('click', () => {
    const shown = [...el.querySelectorAll('.card')], on = !shown.every(c => c.classList.contains('flipped'));
    shown.forEach(c => c.classList.toggle('flipped', on));
  });

  const day = s => new Date(s + 'T12:00:00').toLocaleDateString('en-US', {month: 'short', day: 'numeric'});
  const foot = document.getElementById('foot');
  if (kind === 'block' && DATA.ridership_ranges && DATA.ridership_ranges.length) {
    const spans = DATA.ridership_ranges.map(r => day(r[0]) + ' to ' + day(r[1])).join(' and ');
    foot.innerHTML =
      `Hours come from TransLoc's block schedule for the week of ${day(DATA.schedule_week)}. Riders and miles are the middle (median) day from
       door-counter and GPS data on the ${DATA.ridership_days} Full Service days from ${spans}, matched to blocks by which bus dispatch
       had on the block; a bus with a broken counter or a mid-day swap makes these rough. Purple blocks have no timestops.`;
  } else if (kind === 'bus' && DATA.bus_info) {
    foot.innerHTML =
      `Miles and days in service are from GPS, ${longDay(DATA.bus_info.from)} to ${longDay(DATA.bus_info.to)}. The colour is the route a bus is
       put on most. Blocks and the route mix count days since ${longDay(DATA.bus_info.blocks_since)}, when the blocks became what they are now.
       Riders come from door counters on the ${DATA.bus_info.rider_days} days that have been pulled, so a bus with a dead counter looks quiet. The 24-series buses seat 18, against 60 to 70 for the rest.`;
  }
}

window.Cards = {all, gallery, nav, esc, num, inkFor};

})();
