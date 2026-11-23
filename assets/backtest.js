/* Intraday strategy backtester — condition builder, engine and UI.
 *
 * Conditions are built from the 44 entries in backtest-series.js. Each row is
 * {left series, operator, right (constant or another series)}, and rows combine
 * with AND or OR. "Crosses above/below" compares the previous bar to the
 * current one, so it fires once per crossing rather than on every bar the
 * inequality happens to hold.
 *
 * Two choices decide whether the output means anything:
 *
 *   A condition is read on a bar's CLOSE and the position opens at the NEXT
 *   bar's OPEN. Entering at the close of the bar that produced the signal is
 *   look-ahead and inflates every result.
 *
 *   When one bar's range spans both target and stop, the STOP is taken.
 *   Candles cannot order two touches inside a bar, and assuming the target
 *   manufactures profit that may never have existed.
 *
 * Results are always split in half and reported out-of-sample beside
 * in-sample. On this project's own data that split is what separates a real
 * effect from a fitted one: daily RSI14 and daily Bollinger both flip sign out
 * of sample, and the 5-minute RSI edge turns out to be a within-day demeaned
 * effect whose raw tradeable sign is the opposite.
 */
'use strict';

const BT = (() => {
  const $ = (id) => document.getElementById(id);
  const cache = new Map();
  const YEARS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026];
  const OPS = [
    ['gt', 'is above'], ['lt', 'is below'],
    ['xa', 'crosses above'], ['xb', 'crosses below'],
  ];
  const MAX_ROWS = 3;

  const fmt = (v, d = 1) =>
    Number.isFinite(v) ? v.toLocaleString('en-IN', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';

  /* ---------- condition rows ---------- */

  function seriesSelect(id, selected) {
    const groups = SERIES.groups.map((g) => {
      const opts = SERIES.defs.filter((d) => d.group === g)
        .map((d) => `<option value="${d.id}"${d.id === selected ? ' selected' : ''}>${d.label}</option>`).join('');
      return `<optgroup label="${g}">${opts}</optgroup>`;
    }).join('');
    return `<select id="${id}">${groups}</select>`;
  }

  function paramInputs(prefix, defId) {
    const def = SERIES.byId.get(defId);
    if (!def || !def.params.length) return '<span class="bt-noparam">no parameters</span>';
    return def.params.map((p) => `
      <label class="bt-param">${p.name}
        <input id="${prefix}_${p.name}" type="number" value="${p.def}"
               min="${p.min}" max="${p.max}" step="${p.step}">
      </label>`).join('');
  }

  function rowHtml(i) {
    const on = i === 0;
    return `
    <div class="bt-row" id="btRow${i}" ${on ? '' : 'data-off="1"'}>
      <div class="bt-rowhead">
        <label class="bt-switch">
          <input type="checkbox" id="btRowOn${i}" ${on ? 'checked' : ''}> Condition ${i + 1}
        </label>
        ${i > 0 ? `<select id="btJoin${i}" class="bt-join">
            <option value="and">AND</option><option value="or">OR</option></select>` : ''}
      </div>
      <div class="bt-rowbody">
        <div class="bt-field">${seriesSelect(`btL${i}`, i === 0 ? 'rsi' : 'close')}
          <div class="bt-params" id="btLP${i}"></div></div>
        <div class="bt-field">
          <select id="btOp${i}">${OPS.map(([v, t]) => `<option value="${v}">${t}</option>`).join('')}</select>
        </div>
        <div class="bt-field">
          <select id="btRk${i}"><option value="num">a number</option><option value="ser">another indicator</option></select>
        </div>
        <div class="bt-field">
          <input id="btRn${i}" type="number" value="${i === 0 ? 70 : 0}" step="0.5">
          <div id="btRsWrap${i}" class="bt-hide">${seriesSelect(`btRs${i}`, 'ema')}
            <div class="bt-params" id="btRP${i}"></div></div>
        </div>
      </div>
    </div>`;
  }

  function readParams(prefix, defId) {
    const def = SERIES.byId.get(defId), out = {};
    if (def) def.params.forEach((p) => {
      const el = $(`${prefix}_${p.name}`);
      out[p.name] = el ? +el.value : p.def;
    });
    return out;
  }

  function readConditions() {
    const rows = [];
    for (let i = 0; i < MAX_ROWS; i++) {
      if (!$(`btRowOn${i}`) || !$(`btRowOn${i}`).checked) continue;
      const leftId = $(`btL${i}`).value;
      const kind = $(`btRk${i}`).value;
      rows.push({
        join: i === 0 ? 'and' : ($(`btJoin${i}`) ? $(`btJoin${i}`).value : 'and'),
        leftId, leftParams: readParams(`btLP${i}`, leftId),
        op: $(`btOp${i}`).value,
        rightKind: kind,
        rightNum: +$(`btRn${i}`).value,
        rightId: $(`btRs${i}`).value,
        rightParams: readParams(`btRP${i}`, $(`btRs${i}`).value),
      });
    }
    return rows;
  }

  function describe(rows) {
    return rows.map((r, i) => {
      const L = SERIES.byId.get(r.leftId).label;
      const lp = Object.values(r.leftParams);
      const op = OPS.find(([v]) => v === r.op)[1];
      const R = r.rightKind === 'num' ? fmt(r.rightNum, 1)
        : SERIES.byId.get(r.rightId).label + (Object.values(r.rightParams).length ? `(${Object.values(r.rightParams).join(',')})` : '');
      return `${i ? r.join.toUpperCase() + ' ' : ''}${L}${lp.length ? `(${lp.join(',')})` : ''} ${op} ${R}`;
    }).join('  ');
  }

  /* ---------- engine ---------- */

  function fires(rows, vals, i) {
    let acc = null;
    for (let k = 0; k < rows.length; k++) {
      const r = rows[k], L = vals[k].left, R = vals[k].right;
      const a = L[i], b = Array.isArray(R) ? R[i] : R;
      const pa = L[i - 1], pb = Array.isArray(R) ? R[i - 1] : R;
      let v;
      if (!Number.isFinite(a) || !Number.isFinite(b)) v = false;
      else if (r.op === 'gt') v = a > b;
      else if (r.op === 'lt') v = a < b;
      else if (!Number.isFinite(pa) || !Number.isFinite(pb)) v = false;
      else if (r.op === 'xa') v = pa <= pb && a > b;
      else v = pa >= pb && a < b;
      acc = acc === null ? v : (r.join === 'or' ? acc || v : acc && v);
    }
    return acc === true;
  }

  function run(data, cfg) {
    const trades = [];
    let coverageWarn = 0, sessions = 0;
    for (const s of data.sessions) {
      const n = s.c.length;
      if (n < 12) continue;
      sessions++;
      const k = data.scale;
      const bars = {
        c: s.c.map((v) => v / k), h: s.h.map((v) => v / k),
        l: s.l.map((v) => v / k), o: s.o.map((v) => v / k),
      };
      const mem = new Map();
      const vals = cfg.rows.map((r) => ({
        left: SERIES.evaluate(SERIES.byId.get(r.leftId), bars, r.leftParams, mem),
        right: r.rightKind === 'num' ? r.rightNum
          : SERIES.evaluate(SERIES.byId.get(r.rightId), bars, r.rightParams, mem),
      }));
      // if a chosen length leaves almost none of the session usable, say so
      const firstUsable = vals[0].left.findIndex(Number.isFinite);
      if (firstUsable < 0 || firstUsable > n * 0.7) coverageWarn++;

      let i = 1, taken = 0;
      while (i < n - 1) {
        if (cfg.oneTrade && taken >= 1) break;
        if (!fires(cfg.rows, vals, i)) { i++; continue; }
        const e = i + 1, entry = bars.o[e], dir = cfg.side;
        const tgt = cfg.tgt > 0 ? entry + dir * cfg.tgt : null;
        const stp = cfg.stp > 0 ? entry - dir * cfg.stp : null;
        let exit = null, why = '', held = 0;
        for (let j = e; j < n; j++) {
          held = j - e + 1;
          const hitStop = stp !== null && (dir > 0 ? bars.l[j] <= stp : bars.h[j] >= stp);
          const hitTgt = tgt !== null && (dir > 0 ? bars.h[j] >= tgt : bars.l[j] <= tgt);
          if (hitStop) { exit = stp; why = 'stop'; break; }
          if (hitTgt) { exit = tgt; why = 'target'; break; }
          if (cfg.maxBars > 0 && held >= cfg.maxBars) { exit = bars.c[j]; why = 'max bars'; break; }
          if (j === n - 1) { exit = bars.c[j]; why = data.res === 'day' ? 'series end' : 'session end'; break; }
        }
        const gross = dir * (exit - entry);
        trades.push({ d: data.dates ? data.dates[s.t[e]] : s.d,
                      t: data.dates ? null : s.t[e], entry, exit, why, held,
                      sig: vals[0].left[i], gross, net: gross - 2 * cfg.slip });
        taken++;
        i = e + held + 1;
      }
    }
    return { trades, coverageWarn, sessions };
  }

  function stats(t) {
    if (!t.length) return null;
    const net = t.reduce((a, x) => a + x.net, 0);
    const wins = t.filter((x) => x.net > 0), losses = t.filter((x) => x.net <= 0);
    let cum = 0, peak = 0, dd = 0;
    for (const x of t) { cum += x.net; peak = Math.max(peak, cum); dd = Math.min(dd, cum - peak); }
    const mean = net / t.length;
    const sd = Math.sqrt(t.reduce((a, x) => a + (x.net - mean) ** 2, 0) / Math.max(t.length - 1, 1));
    return { n: t.length, net, mean, sd, dd,
      winRate: (wins.length / t.length) * 100,
      avgWin: wins.length ? wins.reduce((a, x) => a + x.net, 0) / wins.length : 0,
      avgLoss: losses.length ? losses.reduce((a, x) => a + x.net, 0) / losses.length : 0,
      best: Math.max(...t.map((x) => x.net)), worst: Math.min(...t.map((x) => x.net)),
      t: sd > 0 ? mean / (sd / Math.sqrt(t.length)) : 0 };
  }

  /* ---------- data ---------- */

  function sourceFor(res, year) {
    if (res === 'day') return '../data/backtest/nifty_1day.json';
    if (res !== '1') return `../data/backtest/nifty_${res}min.json`;
    const base = (window.NA_CONFIG && (window.NA_CONFIG.supabaseUrl || window.NA_CONFIG.SUPABASE_URL)) || '';
    return base
      ? `${base.replace(/\/$/, '')}/storage/v1/object/public/public-data/backtest/nifty_1min_${year}.json`
      : `../data/backtest/nifty_1min_${year}.json`;
  }

  async function bars(res, year) {
    const url = sourceFor(res, year);
    if (cache.has(url)) return cache.get(url);
    $('btStatus').textContent = 'Loading candles…';
    const r = await fetch(url);
    if (!r.ok) {
      throw new Error(res === '1'
        ? `1-minute data for ${year} is not published yet (HTTP ${r.status}). 5 and 15 minute work now.`
        : `could not load candles (HTTP ${r.status})`);
    }
    const j = await r.json();
    cache.set(url, j);
    return j;
  }

  /* ---------- rendering ---------- */

  function statBox(s) {
    if (!s) return '<p style="color:var(--muted)">No trades in this half.</p>';
    const cls = (v) => (v > 0 ? 'bt-up' : v < 0 ? 'bt-dn' : '');
    return [
      ['Trades', String(s.n)],
      ['Net points', `<span class="${cls(s.net)}">${fmt(s.net)}</span>`],
      ['Per trade', `<span class="${cls(s.mean)}">${fmt(s.mean, 2)}</span>`],
      ['t-statistic', `<span class="${Math.abs(s.t) > 2 ? cls(s.t) : ''}">${fmt(s.t, 2)}</span>`],
      ['Win rate', `${fmt(s.winRate)}%`],
      ['Average win', fmt(s.avgWin, 1)],
      ['Average loss', fmt(s.avgLoss, 1)],
      ['Best / worst', `${fmt(s.best)} / ${fmt(s.worst)}`],
      ['Max drawdown', `<span class="bt-dn">${fmt(s.dd)}</span>`],
      ['Std dev per trade', fmt(s.sd, 1)],
    ].map(([k, v]) => `<div class="bt-kv"><span>${k}</span><b>${v}</b></div>`).join('');
  }

  function verdict(a, b, slip) {
    if (!b) return 'Too few trades to split in half, so there is no out-of-sample check. Treat this as untested.';
    if (Math.sign(a.mean) !== Math.sign(b.mean)) {
      return `<strong>The sign flips out of sample.</strong> In-sample this earns ${fmt(a.mean, 2)}
        points a trade; out-of-sample ${fmt(b.mean, 2)}. That is the signature of a fitted
        result rather than an edge.`;
    }
    if (b.mean < 0) {
      return Math.abs(b.t) >= 2
        ? `<strong>This loses in both halves</strong> — ${fmt(a.mean, 2)} points a trade in-sample,
           ${fmt(b.mean, 2)} out-of-sample, out-of-sample t of ${fmt(b.t, 2)}. A loss that
           consistent is itself information: the same condition taken the other way round is
           worth testing.`
        : `<strong>This loses in both halves</strong> — ${fmt(a.mean, 2)} then ${fmt(b.mean, 2)}
           points a trade, out-of-sample t ${fmt(b.t, 2)}. A weak loss rather than a reliable
           one, most likely your ${fmt(2 * slip, 1)} points of round-trip cost against a
           signal with no real edge.`;
    }
    if (Math.abs(b.t) < 2) {
      return `<strong>Out-of-sample t is ${fmt(b.t, 2)}</strong>, inside noise. It makes money in
        both halves, but the evidence does not reach the level you would want before
        committing money.`;
    }
    return `Out-of-sample t is <strong>${fmt(b.t, 2)}</strong> at ${fmt(b.mean, 2)} points a trade,
      profitable in both halves. That survives the split. Weigh the ${fmt(b.dd)} point drawdown
      and the per-trade size against your real costs before reading more into it.`;
  }

  function chart(trades) {
    const W = 1000, H = 250, PAD = 36;
    let cum = 0;
    const pts = trades.map((t, i) => { cum += t.net; return [i, cum]; });
    const ys = pts.map((p) => p[1]);
    const mn = Math.min(0, ...ys), mx = Math.max(0, ...ys);
    const sx = (i) => PAD + (i * (W - 2 * PAD)) / Math.max(pts.length - 1, 1);
    const sy = (v) => H - PAD - ((v - mn) * (H - 2 * PAD)) / Math.max(mx - mn, 1);
    const mid = Math.floor(trades.length / 2);
    const path = pts.map((p, i) => `${i ? 'L' : 'M'}${sx(p[0]).toFixed(1)},${sy(p[1]).toFixed(1)}`).join('');
    $('btChart').innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img"
      aria-label="Cumulative points over ${trades.length} trades, out-of-sample half marked">
      <line x1="${PAD}" y1="${sy(0)}" x2="${W - PAD}" y2="${sy(0)}" stroke="var(--card-border)"/>
      <line x1="${sx(mid)}" y1="16" x2="${sx(mid)}" y2="${H - 20}" stroke="var(--muted)" stroke-dasharray="5 4"/>
      <text x="${sx(mid) + 7}" y="26" fill="var(--muted)" font-size="11">out-of-sample &rarr;</text>
      <path d="${path}" fill="none" stroke="var(--accent)" stroke-width="2.5"/>
      <text x="${PAD}" y="${H - 8}" fill="var(--muted)" font-size="11">trade 1</text>
      <text x="${W - PAD}" y="${H - 8}" fill="var(--muted)" font-size="11" text-anchor="end">trade ${trades.length}</text>
      <text x="${PAD}" y="${Math.max(sy(mx) - 8, 14)}" fill="var(--muted)" font-size="11">${fmt(mx, 0)} pts</text>
    </svg>`;
  }

  function tradeTable(t, sigLabel) {
    const rows = t.slice(0, 400).map((x) => {
      const hh = String(Math.floor(x.t / 60)).padStart(2, '0');
      const mm = String(x.t % 60).padStart(2, '0');
      return `<tr><td>${x.d} ${hh}:${mm}</td><td>${fmt(x.entry, 2)}</td><td>${fmt(x.exit, 2)}</td>
        <td>${x.why}</td><td>${x.held}</td><td>${fmt(x.sig, 2)}</td>
        <td class="${x.net > 0 ? 'bt-up' : 'bt-dn'}">${fmt(x.net, 2)}</td></tr>`;
    }).join('');
    $('btTrades').innerHTML = `<thead><tr><th>Entry</th><th>In</th><th>Out</th><th>Exit</th>
      <th>Bars</th><th>${sigLabel}</th><th>Net pts</th></tr></thead><tbody>${rows}</tbody>`;
    $('btTradeCount').textContent = t.length > 400 ? `showing first 400 of ${t.length}` : `${t.length}`;
  }

  /* ---------- wiring ---------- */

  async function execute() {
    const btn = $('btRun');
    btn.disabled = true;
    try {
      const rows = readConditions();
      if (!rows.length) { $('btStatus').textContent = 'Enable at least one condition.'; return; }
      const data = await bars($('btRes').value, $('btYear').value);
      $('btStatus').textContent = 'Running…';
      await new Promise((r) => setTimeout(r, 0));
      const cfg = { rows, side: +$('btSide').value, tgt: +$('btTgt').value, stp: +$('btStp').value,
        maxBars: +$('btMaxBars').value, oneTrade: $('btOnce').value === '1', slip: +$('btSlip').value };
      const t0 = performance.now();
      const { trades, coverageWarn, sessions } = run(data, cfg);
      const ms = performance.now() - t0;
      const scanned = (data.bars || 0).toLocaleString('en-IN');
      $('btRecipe').textContent = describe(rows);

      // A run with no trades must say WHY. The commonest cause is an indicator
      // whose warm-up is longer than a session: ADX(14) needs about 28 bars of
      // Wilder smoothing and a 15-minute session has only 25, so it is NaN for
      // every bar and no condition on it can ever fire. Reporting a bare "no
      // trades matched" would read as "the condition never happened", which is
      // a different and misleading statement.
      const isDay = $('btRes').value === 'day';
      const starved = isDay ? 0 : Math.round((coverageWarn / Math.max(sessions, 1)) * 100);
      if (!trades.length) {
        const why = starved >= 50
          ? ` Your first indicator has no value at all on ${starved}% of sessions: its warm-up
              needs more bars than a ${$('btRes').value}-minute session contains
              (${$('btRes').value === '15' ? '25' : $('btRes').value === '5' ? '75' : '375'} bars).
              Shorten the length, or use a finer candle.`
          : ' The condition was computable but never became true over this period.';
        $('btStatus').innerHTML =
          `No trades. ${scanned} candles scanned in ${ms.toFixed(0)} ms.${why}`;
        $('btOut').classList.add('bt-hide');
        return;
      }
      const mid = Math.floor(trades.length / 2);
      const sIS = stats(trades.slice(0, mid)), sOOS = stats(trades.slice(mid));
      $('btIsBox').innerHTML = statBox(sIS);
      $('btOosBox').innerHTML = statBox(sOOS);
      $('btIsRange').textContent = `${trades[0].d} → ${trades[mid - 1].d}`;
      $('btOosRange').textContent = `${trades[mid].d} → ${trades[trades.length - 1].d}`;
      $('btVerdict').innerHTML = verdict(sIS, sOOS, cfg.slip);
      const warn = starved > 20
        ? `<p class="bt-caution" style="margin-top:12px">Your first indicator has no value on
           <strong>${starved}% of sessions</strong> — its warm-up needs more bars than a
           ${$('btRes').value}-minute session contains. Those sessions produced no trades at all,
           so this result rests on a smaller sample than it appears to. Shorten the length or use
           a finer candle.</p>` : '';
      $('btWarn').innerHTML = warn;
      chart(trades);
      tradeTable(trades, SERIES.byId.get(rows[0].leftId).label);
      $('btOut').classList.remove('bt-hide');
      $('btStatus').textContent =
        `${trades.length.toLocaleString('en-IN')} trades from ${scanned} candles in ${ms.toFixed(0)} ms.`;
    } catch (e) {
      $('btStatus').textContent = `Failed: ${e.message}`;
      $('btOut').classList.add('bt-hide');
    } finally { btn.disabled = false; }
  }

  function init() {
    $('btRows').innerHTML = Array.from({ length: MAX_ROWS }, (_, i) => rowHtml(i)).join('');
    for (let i = 0; i < MAX_ROWS; i++) {
      const refreshL = () => { $(`btLP${i}`).innerHTML = paramInputs(`btLP${i}`, $(`btL${i}`).value); };
      const refreshR = () => { $(`btRP${i}`).innerHTML = paramInputs(`btRP${i}`, $(`btRs${i}`).value); };
      $(`btL${i}`).addEventListener('change', refreshL);
      $(`btRs${i}`).addEventListener('change', refreshR);
      $(`btRk${i}`).addEventListener('change', () => {
        const ser = $(`btRk${i}`).value === 'ser';
        $(`btRsWrap${i}`).classList.toggle('bt-hide', !ser);
        $(`btRn${i}`).classList.toggle('bt-hide', ser);
      });
      $(`btRowOn${i}`).addEventListener('change', () => {
        $(`btRow${i}`).toggleAttribute('data-off', !$(`btRowOn${i}`).checked);
      });
      refreshL(); refreshR();
    }
    const ySel = $('btYear');
    // A DISABLED SELECT STILL SHOWS A VALUE. This used to be hard-coded to
    // 2025 and greyed out on 5- and 15-minute runs, which reads as "only 2025
    // is available" when those runs in fact cover every session from 2017 to
    // 2026. Carry an explicit all-years entry so the box never claims a single
    // year it is not using.
    const allOpt = document.createElement('option');
    allOpt.value = 'all';
    allOpt.textContent = YEARS.length
      ? `All years (${YEARS[0]}\u2013${YEARS[YEARS.length - 1]})` : 'All years';
    ySel.appendChild(allOpt);
    YEARS.forEach((y) => {
      const o = document.createElement('option');
      o.value = String(y); o.textContent = String(y);
      ySel.appendChild(o);
    });
    const syncYear = () => {
      const r = $('btRes').value;
      const one = r === '1';
      if (r === 'day') {
        ySel.disabled = true; ySel.value = 'all';
        $('btYearHint').textContent = 'using every daily bar, 2001\u20132026';
        return;
      }
      ySel.disabled = !one;
      if (!one) ySel.value = 'all';
      else if (ySel.value === 'all') ySel.value = String(YEARS[YEARS.length - 1] || '');
      $('btYearHint').textContent = one
        ? 'required \u2014 1-minute data is stored per year'
        : `using every session, ${YEARS[0]}\u2013${YEARS[YEARS.length - 1]}`;
    };
    $('btRes').addEventListener('change', syncYear);
    syncYear();
    $('btRun').addEventListener('click', execute);
  }

  return { init, run, stats, readConditions, describe };
})();

document.addEventListener('DOMContentLoaded', BT.init);
