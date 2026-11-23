/* UI for the option strategy backtester. Keeps the same reporting discipline as
 * the index tab: always split in half, never read a two-sided loss as approval,
 * always explain a zero-trade run. */
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var INR = function (v) {
    return (v < 0 ? '-' : '') + '₹' + Math.abs(Math.round(v)).toLocaleString('en-IN');
  };

  function cfg() {
    return {
      strategy: $('optStrat').value,
      entryDTE: parseInt($('optDTE').value, 10),
      entryAt: $('optEntryAt').value,
      selectBy: $('optSelBy').value,
      otmPct: parseFloat($('optOtm').value) || 0,
      premium: parseFloat($('optPrem').value) || 0,
      premTol: parseFloat($('optPremTol').value) || 40,
      marginPct: parseFloat($('optMargin').value) || 12,
      capital: parseFloat($('optCapital').value) || 0,
      lots: parseInt($('optLots').value, 10) || 1,
      slip: parseFloat($('optSlip').value) || 0,
      target: parseFloat($('optTgt').value) || 0,
      stop: parseFloat($('optStp').value) || 0,
      exitDTE: parseInt($('optExitDTE').value, 10) || 0
    };
  }

  function years() {
    var a = parseInt($('optFrom').value, 10), b = parseInt($('optTo').value, 10);
    var out = [];
    for (var y = Math.min(a, b); y <= Math.max(a, b); y++) out.push(y);
    return out;
  }

  function box(st, label, range) {
    if (!st) return '<p class="bt-none">no trades in this half</p>';
    var rows = [
      ['Trades', st.n],
      ['Net', INR(st.total)],
      ['Mean / trade', INR(st.mean)],
      ['t-stat', st.t.toFixed(2)],
      ['Win rate', st.winRate.toFixed(0) + '%'],
      ['Avg win', INR(st.avgWin)],
      ['Avg loss', INR(st.avgLoss)],
      ['Max drawdown', INR(st.maxDD)],
      ['Charges', INR(st.charges)],
      [st.capitalExact ? 'Capital (premium paid)' : 'Capital per trade (est.)',
       INR(st.avgCapital)],
      ['Return on that capital', (100 * st.total / Math.max(1, st.avgCapital)).toFixed(1) + '%'],
      ['Drawdown vs capital', (100 * st.maxDD / Math.max(1, st.avgCapital)).toFixed(1) + '%']
    ];
    return '<table class="bt-table">' + rows.map(function (r) {
      return '<tr><th>' + r[0] + '</th><td>' + r[1] + '</td></tr>';
    }).join('') + '</table>';
  }

  /* The verdict must not flatter the result. Both halves losing is a losing
   * strategy, not "a direction that survives the split". */
  function verdict(is, oos, all) {
    if (!is || !oos) return 'Not enough trades to split in half — treat this as a single sample, not a validated result.';
    var bothUp = is.total > 0 && oos.total > 0;
    var bothDown = is.total < 0 && oos.total < 0;
    if (bothDown) {
      return 'Loses in BOTH halves (' + INR(is.total) + ' then ' + INR(oos.total)
        + '). That is a losing strategy on this data, not a tuning problem. '
        + 'If you want to know whether the inverse works, test the inverse — do not read this as encouragement.';
    }
    if (!bothUp) {
      return 'Profitable in one half and not the other (' + INR(is.total) + ' then '
        + INR(oos.total) + '). That is the signature of a result that will not repeat.';
    }
    var weak = Math.abs(all.t) < 2;
    return 'Profitable in both halves (' + INR(is.total) + ' then ' + INR(oos.total) + '), t = '
      + all.t.toFixed(2) + '. ' + (weak
        ? 'But t is under 2, so the edge is not distinguishable from noise on this sample size.'
        : 'The t-stat clears 2. Charges of ' + INR(all.charges) + ' are already deducted.');
  }

  function warn(res, all) {
    var out = [];
    var skips = Object.keys(res.skips || {});
    if (skips.length) {
      out.push('Skipped expiries: ' + skips.map(function (k) {
        return res.skips[k] + ' — ' + k;
      }).join('; ') + '.');
    }
    out.push('Daily option bars only. Entries are a session open or close and exits are a later '
      + 'session close, a stop or target touched on a later day’s high or low, or expiry '
      + 'settlement. There is no intraday option data in this store, so same-day '
      + 'entry-and-exit is not testable here.');
    out.push('When a stop and a target could both have triggered inside one daily bar, the STOP '
      + 'is taken — a daily bar cannot say which came first.');
    if (all && all.n < 40) out.push('Only ' + all.n + ' trades. Anything read off this is anecdote.');
    return out.map(function (t) { return '<p class="bt-warn">' + t + '</p>'; }).join('');
  }

  function run() {
    var c = cfg(), ys = years();
    $('optStatus').textContent = 'Loading ' + ys.length + ' year'
      + (ys.length > 1 ? 's' : '') + ' of option chains…';
    $('optOut').classList.add('bt-hide');
    window.NA_OPT.run(c, ys).then(function (res) {
      var t = res.trades;
      var all = window.NA_OPT.stats(t);
      if (!all) {
        $('optStatus').textContent = '';
        $('optOut').classList.remove('bt-hide');
        $('optVerdict').textContent = 'No trades. Every expiry was skipped.';
        $('optWarn').innerHTML = warn(res, null);
        $('optIsBox').innerHTML = ''; $('optOosBox').innerHTML = '';
        $('optTrades').innerHTML = ''; $('optRecipe').textContent = '';
        return;
      }
      var half = Math.floor(t.length / 2);
      var is = window.NA_OPT.stats(t.slice(0, half));
      var oos = window.NA_OPT.stats(t.slice(half));
      var spec = window.NA_OPT.STRATS[c.strategy];
      $('optRecipe').textContent = spec.label + ', entered at the '
        + c.entryAt + ' of the session at DTE ' + c.entryDTE
        + (c.selectBy === 'premium'
            ? ', strikes chosen by premium nearest \u20b9' + c.premium
            : (c.otmPct > 0 ? ', strikes ' + c.otmPct + '% out' : ', strikes at the money'))
        + ', ' + c.lots + ' lot' + (c.lots > 1 ? 's' : '')
        + ', ' + c.slip + ' point slippage per leg'
        + (c.stop > 0 ? ', stop at ' + c.stop + '% of credit' : '')
        + (c.target > 0 ? ', target at ' + c.target + '% of credit' : '')
        + (c.exitDTE > 0 ? ', else exit at DTE ' + c.exitDTE : ', else held to settlement')
        + '. ' + ys[0] + '–' + ys[ys.length - 1] + '.';
      $('optVerdict').textContent = verdict(is, oos, all);
      if (c.capital > 0) {
        var onAcct = 100 * all.total / c.capital;
        var ddAcct = 100 * all.maxDD / c.capital;
        $('optCapLine').textContent = 'On ' + INR(c.capital) + ' of capital that is '
          + onAcct.toFixed(1) + '% over ' + all.n + ' trades, with a worst drawdown of '
          + INR(all.maxDD) + ' \u2014 ' + ddAcct.toFixed(1) + '% of the account. '
          + (all.avgCapital > c.capital
            ? 'But each trade needs about ' + INR(all.avgCapital)
              + ' of margin, which is MORE than the capital you entered: this size is not fundable.'
            : 'Each trade needs about ' + INR(all.avgCapital) + ', so '
              + Math.floor(c.capital / Math.max(1, all.avgCapital))
              + ' position(s) of this size fit.')
          + (all.capitalExact ? '' : ' Margin is estimated at ' + c.marginPct
             + '% of short notional, not a broker quote.');
      } else { $('optCapLine').textContent = ''; }
      $('optWarn').innerHTML = warn(res, all);
      $('optIsBox').innerHTML = box(is);
      $('optOosBox').innerHTML = box(oos);
      $('optIsRange').textContent = is ? t[0].entryDate + ' → ' + t[half - 1].entryDate : '';
      $('optOosRange').textContent = oos ? t[half].entryDate + ' → ' + t[t.length - 1].entryDate : '';
      $('optTradeCount').textContent = '(' + all.n + ')';
      var head = '<tr><th>entry</th><th>expiry</th><th>DTE</th><th>ATM</th><th>lot</th>'
        + '<th>credit</th><th>settle</th><th>pts</th><th>charges</th><th>net</th><th>exit</th></tr>';
      $('optTrades').innerHTML = head + t.map(function (x) {
        return '<tr><td>' + x.entryDate + '</td><td>' + x.expiry + '</td><td>' + x.dte
          + '</td><td>' + x.atm + '</td><td>' + x.lot + '</td><td>' + x.credit.toFixed(2)
          + '</td><td>' + x.settle.toFixed(2) + '</td><td>' + x.pts.toFixed(2)
          + '</td><td>' + Math.round(x.charges) + '</td><td class="'
          + (x.net >= 0 ? 'bt-up' : 'bt-dn') + '">' + INR(x.net) + '</td><td>' + x.why + '</td></tr>';
      }).join('');
      $('optStatus').textContent = '';
      $('optOut').classList.remove('bt-hide');
    }).catch(function (e) {
      $('optStatus').textContent = 'Failed: ' + e.message;
    });
  }

  function init() {
    if (!$('optStrat')) return;
    var sel = $('optStrat');
    Object.keys(window.NA_OPT.STRATS).forEach(function (k) {
      var o = document.createElement('option');
      o.value = k; o.textContent = window.NA_OPT.STRATS[k].label;
      if (k === 'short_strangle') o.selected = true;
      sel.appendChild(o);
    });
    window.NA_OPT.manifest().then(function (m) {
      var ys = (m.years || []).map(function (y) { return y.year; }).sort();
      [['optFrom', ys[0]], ['optTo', ys[ys.length - 1]]].forEach(function (pair) {
        var s = $(pair[0]);
        ys.forEach(function (y) {
          var o = document.createElement('option');
          o.value = String(y); o.textContent = String(y);
          if (y === pair[1]) o.selected = true;
          s.appendChild(o);
        });
      });
      var rows = (m.years || []).reduce(function (a, y) { return a + y.rows; }, 0);
      $('optDataNote').textContent = 'Real daily option prices: ' + rows.toLocaleString('en-IN')
        + ' traded contract-days across ' + ys[0] + '–' + ys[ys.length - 1]
        + ', strikes within ±' + Math.round(m.band * 100) + '% of the underlying.';
    }).catch(function (e) {
      $('optDataNote').textContent = 'Could not read the option manifest: ' + e.message;
    });
    var sync = function () {
      var byPrem = $('optSelBy').value === 'premium';
      $('optPrem').closest('.bt-field').style.display = byPrem ? '' : 'none';
      $('optOtm').closest('.bt-field').style.display = byPrem ? 'none' : '';
    };
    $('optSelBy').addEventListener('change', sync);
    sync();
    $('optRun').addEventListener('click', run);
  }

  /* tabs */
  function tabs() {
    var btns = document.querySelectorAll('[data-tab]');
    if (!btns.length) return;
    btns.forEach(function (b) {
      b.addEventListener('click', function () {
        var want = b.getAttribute('data-tab');
        btns.forEach(function (x) { x.classList.toggle('is-on', x === b); });
        ['tabIndex', 'tabOptions'].forEach(function (id) {
          var el = document.getElementById(id);
          if (el) el.classList.toggle('bt-hide', id !== want);
        });
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { init(); tabs(); });
  } else { init(); tabs(); }
})();
