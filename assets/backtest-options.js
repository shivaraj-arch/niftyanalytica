/* Option strategy backtester on real NIFTY option prices, 2009-2026.
 *
 * WHAT THIS CAN AND CANNOT TEST
 * The store holds DAILY option bars. So an entry is a session's OPEN or CLOSE,
 * an exit is a later session's close, a stop/target touched on a later day's
 * HIGH/LOW, or expiry settlement. There is no intraday option data, so a
 * "09:20 entry, 15:15 exit" same-day test is NOT available here and the UI
 * does not offer it. The index-indicator tab on this page does work on 5- and
 * 15-minute candles; that is the index, not options.
 *
 * SETTLEMENT is max(settleIndex - K, 0) for a call. settleIndex comes from the
 * expiry-day SttlmPric, which carries the UNDERLYING's settlement price and is
 * identical across every strike and both CE and PE. Reading that column as the
 * option's own value reports a median of +37,661% where the truth is -100%.
 *
 * LOT SIZE is per CONTRACT, not per date. A change applies only to newly
 * introduced expiries, so two sizes trade side by side for weeks: on 2024-12-30
 * everything was 75 except the 30-Jan-2025 expiry, which was still 25. The
 * data ships a (session, expiry) -> lot map for exactly this reason.
 */
(function (global) {
  'use strict';

  var CACHE = {};        // year -> indexed year object
  var MANIFEST = null;

  function base() {
    var rc = global.NA_RUNTIME_CONFIG || {};
    return rc.pagesBase || '';
  }

  function getJSON(url) {
    return fetch(url, { cache: 'force-cache' }).then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status + ' for ' + url);
      return r.json();
    });
  }

  function manifest() {
    if (MANIFEST) return Promise.resolve(MANIFEST);
    return getJSON(base() + 'data/backtest/options/manifest.json').then(function (m) {
      MANIFEST = m; return m;
    });
  }

  /* Index a year file into per-(session,expiry) strike maps. */
  function indexYear(raw) {
    var n = raw.s.length, i;
    var bySX = new Map();                 // "s|x" -> Map(strike -> {ce,pe})
    for (i = 0; i < n; i++) {
      var key = raw.s[i] + '|' + raw.x[i];
      var m = bySX.get(key);
      if (!m) { m = new Map(); bySX.set(key, m); }
      var k = raw.k[i];
      var leg = m.get(k);
      if (!leg) { leg = {}; m.set(k, leg); }
      var bar = { o: raw.o[i] / 100, h: raw.h[i] / 100, l: raw.l[i] / 100,
                  c: raw.c[i] / 100, v: raw.v[i] };
      if (raw.t[i] === 0) leg.ce = bar; else leg.pe = bar;
    }
    var lot = new Map();
    for (i = 0; i < raw.lotV.length; i++) lot.set(raw.lotS[i] + '|' + raw.lotX[i], raw.lotV[i]);
    return {
      year: raw.year,
      sessions: raw.sessions,
      expiries: raw.expiries,
      settle: raw.settle.map(function (v) { return v < 0 ? null : v / 100; }),
      spot: raw.spot.map(function (v) { return v < 0 ? null : v / 100; }),
      spotO: (raw.spotO || []).map(function (v) { return v < 0 ? null : v / 100; }),
      bySX: bySX,
      lot: lot
    };
  }

  function loadYear(year) {
    if (CACHE[year]) return Promise.resolve(CACHE[year]);
    return manifest().then(function (m) {
      var row = (m.years || []).filter(function (y) { return y.year === year; })[0];
      if (!row) throw new Error('year ' + year + ' is not in the options manifest');
      return getJSON(base() + row.file);
    }).then(function (raw) {
      CACHE[year] = indexYear(raw);
      return CACHE[year];
    });
  }

  function dayDiff(a, b) {   // b - a in whole days, both "YYYY-MM-DD"
    return Math.round((Date.parse(b) - Date.parse(a)) / 86400000);
  }

  /* Charges on an F&O options book. Rates verified against Zerodha contract
   * notes, Oct-2026: STT 0.15% of SELL premium (Budget 2026, up from 0.10%),
   * exchange 0.03553% of turnover both sides, SEBI Rs 10/crore, stamp 0.003%
   * of BUY value, GST 18% on (brokerage + exchange + SEBI), Rs 20 per order.
   * On a short-premium book STT is typically half the total bill, so leaving
   * charges out of an option-selling backtest overstates it badly. */
  function charges(buyVal, sellVal, orders) {
    var stt = sellVal * 0.0015;
    var brok = 20 * orders;
    var exch = (buyVal + sellVal) * 0.0003553;
    var sebi = (buyVal + sellVal) * 1e-6;
    var stamp = Math.round(buyVal * 0.00003);
    var gst = (brok + exch + sebi) * 0.18;
    return stt + brok + exch + sebi + stamp + gst;
  }

  var STRATS = {
    short_straddle: { legs: [{ t: 'ce', s: -1, off: 0 }, { t: 'pe', s: -1, off: 0 }], label: 'Short straddle' },
    short_strangle: { legs: [{ t: 'ce', s: -1, off: +1 }, { t: 'pe', s: -1, off: -1 }], label: 'Short strangle' },
    long_straddle:  { legs: [{ t: 'ce', s: +1, off: 0 }, { t: 'pe', s: +1, off: 0 }], label: 'Long straddle' },
    long_strangle:  { legs: [{ t: 'ce', s: +1, off: +1 }, { t: 'pe', s: +1, off: -1 }], label: 'Long strangle' },
    iron_fly:       { legs: [{ t: 'ce', s: -1, off: 0 }, { t: 'pe', s: -1, off: 0 },
                             { t: 'ce', s: +1, off: +2 }, { t: 'pe', s: +1, off: -2 }], label: 'Iron fly' },
    iron_condor:    { legs: [{ t: 'ce', s: -1, off: +1 }, { t: 'pe', s: -1, off: -1 },
                             { t: 'ce', s: +1, off: +2 }, { t: 'pe', s: +1, off: -2 }], label: 'Iron condor' },
    sell_ce: { legs: [{ t: 'ce', s: -1, off: +1 }], label: 'Sell call' },
    sell_pe: { legs: [{ t: 'pe', s: -1, off: -1 }], label: 'Sell put' },
    buy_ce:  { legs: [{ t: 'ce', s: +1, off: +1 }], label: 'Buy call' },
    buy_pe:  { legs: [{ t: 'pe', s: +1, off: -1 }], label: 'Buy put' }
  };

  /* Pick the strike whose own premium is closest to a rupee target, e.g. "sell
   * the call trading nearest Rs 150". This normalises across volatility regimes
   * in a way a fixed %-OTM does not: 1.5% out was worth 40 points in the quiet
   * 2017 tape and 190 points in March 2020, so a %-OTM rule silently changes
   * the trade it is testing. Premium selection keeps the premium constant and
   * lets the distance float. */
  function strikeByPremium(chain, strikes, type, target, at, minStrike, maxStrike) {
    var best = null, bd = Infinity;
    for (var i = 0; i < strikes.length; i++) {
      var k = strikes[i];
      if (minStrike != null && k < minStrike) continue;
      if (maxStrike != null && k > maxStrike) continue;
      var leg = chain.get(k);
      var bar = leg && (type === 'ce' ? leg.ce : leg.pe);
      if (!bar) continue;
      var px = at === 'open' ? bar.o : bar.c;
      if (!(px > 0)) continue;
      var d = Math.abs(px - target);
      if (d < bd) { bd = d; best = k; }
    }
    return best;
  }

  /* The entry reference, derived from the chain itself by put-call parity.
   *
   * C - P = F - K, so F = K + C - P at every strike. Taking the median across
   * the strikes that have BOTH legs priced gives the forward the market is
   * actually quoting at that moment, using nothing but entry-time prices.
   *
   * Why not the futures open? Because the near-month future can open far from
   * the index: on 2026-01-05 it opened at 26,500 and closed 26,331 while the
   * index opened 26,333.70 -- a 166-point basis at the open. Centring strikes
   * on that put the "ATM" 170 points away from the money, and a premium-based
   * search then found no call anywhere near the requested premium and collapsed
   * both legs onto one strike. Why not the futures CLOSE? That is look-ahead on
   * expiry day, which is the bug this replaced. */
  function impliedRef(chain, strikes, at) {
    var fs = [];
    for (var i = 0; i < strikes.length; i++) {
      var leg = chain.get(strikes[i]);
      if (!leg || !leg.ce || !leg.pe) continue;
      var c = at === 'open' ? leg.ce.o : leg.ce.c;
      var pp = at === 'open' ? leg.pe.o : leg.pe.c;
      if (!(c > 0) || !(pp > 0)) continue;
      fs.push(strikes[i] + c - pp);
    }
    if (fs.length < 3) return null;
    fs.sort(function (a, b) { return a - b; });
    return fs[Math.floor(fs.length / 2)];
  }

  function nearestStrike(strikes, target) {
    var best = null, bd = Infinity;
    for (var i = 0; i < strikes.length; i++) {
      var d = Math.abs(strikes[i] - target);
      if (d < bd) { bd = d; best = strikes[i]; }
    }
    return best;
  }

  /* Run one expiry. Returns a trade object or a reason it was skipped. */
  function runExpiry(Y, xIdx, cfg, globalSettle) {
    var xDate = Y.expiries[xIdx];
    var settle = Y.settle[xIdx];
    // AN EXPIRY LISTED IN ONE YEAR SETTLES IN ANOTHER. A January expiry is
    // quoted in December, so its settlement lives in the NEXT year's file.
    // Looking only inside one year dropped 45% of expiries in the monthly era
    // (37 of 83 across 2002-2008), which quietly shrank every early-period
    // sample. Consult the settlement map built across every loaded year.
    if (settle == null && globalSettle) {
      var g = globalSettle[xDate];
      if (g != null) settle = g;
    }
    if (settle == null) {
      return { skip: 'expiry ' + xDate + ' settles outside the years loaded \u2014 '
               + 'load the following year too' };
    }

    // entry session: the last session at or before the requested DTE
    var entry = -1;
    for (var s = 0; s < Y.sessions.length; s++) {
      var dte = dayDiff(Y.sessions[s], xDate);
      if (dte < 0) break;
      if (dte <= cfg.entryDTE) { entry = s; break; }
    }
    if (entry < 0) {
      // NIFTY lists expiries years ahead -- a 2029 contract is quoted in 2024 --
      // so most of these are far-dated listings with no session anywhere near
      // them, not missing data. Say which it is.
      var last = Y.sessions[Y.sessions.length - 1];
      if (dayDiff(last, xDate) > 40) {
        return { skip: 'far-dated listing (' + xDate + ') \u2014 quoted years ahead, '
                 + 'no session near it' };
      }
      return { skip: 'no trading session at DTE ' + cfg.entryDTE + ' for ' + xDate };
    }

    var chain = Y.bySX.get(entry + '|' + xIdx);
    if (!chain) return { skip: 'no traded chain on the entry session' };
    var strikes = Array.from(chain.keys()).sort(function (a, b) { return a - b; });
    if (!strikes.length) return { skip: 'empty chain' };

    // The strike must be chosen from information available AT ENTRY. Using the
    // session CLOSE for an OPEN entry is look-ahead and on expiry day it is
    // devastating: the close sits within the basis of the settlement, so the
    // straddle lands on the answer. Measured before the fix: |settle - ATM|
    // never exceeded 250 points across 355 expiries, win rate 96%.
    var ref = impliedRef(chain, strikes, cfg.entryAt);
    var refSrc = 'parity';
    if (ref == null) {
      ref = cfg.entryAt === 'open' ? Y.spotO[entry] : Y.spot[entry];
      refSrc = cfg.entryAt === 'open' ? 'futures open' : 'futures close';
    }
    if (ref == null) { ref = Y.spot[entry]; refSrc = 'futures close'; }
    if (ref == null) return { skip: 'no entry reference: parity needs 3 two-sided strikes' };
    var atm = nearestStrike(strikes, ref);
    var step = strikes.length > 1 ? Math.min.apply(null, strikes.slice(1).map(function (v, i) {
      return v - strikes[i];
    })) : 50;
    var width = cfg.otmPct > 0 ? Math.max(step, Math.round(ref * cfg.otmPct / 100 / step) * step) : step;

    var spec = STRATS[cfg.strategy];
    var legs = [], i;
    var byPrem = cfg.selectBy === 'premium' && cfg.premium > 0;
    for (i = 0; i < spec.legs.length; i++) {
      var L = spec.legs[i];
      var k;
      if (byPrem && L.off !== 0) {
        // short legs target the requested premium; protective wings target a
        // fraction of it, so an iron structure stays an iron structure
        var tgt = L.s < 0 ? cfg.premium : cfg.premium * (cfg.wingFrac || 0.35);
        k = strikeByPremium(chain, strikes, L.t, tgt, cfg.entryAt,
                            L.t === 'pe' ? null : atm, L.t === 'pe' ? atm : null);
        if (k == null) return { skip: 'no ' + L.t.toUpperCase() + ' priced on entry' };
        // REFUSE RATHER THAN DEGENERATE. At DTE 2 in a quiet week the whole
        // chain can be cheaper than the premium asked for, and the nearest
        // match is then the ATM on both sides -- which silently turns a
        // strangle into a straddle and tests a trade nobody requested.
        var lg0 = chain.get(k);
        var bar0 = L.t === 'ce' ? lg0.ce : lg0.pe;
        var got = cfg.entryAt === 'open' ? bar0.o : bar0.c;
        var tol = (cfg.premTol > 0 ? cfg.premTol : 40) / 100;
        if (Math.abs(got - tgt) > tol * tgt) {
          return { skip: 'no ' + L.t.toUpperCase() + ' near \u20b9' + Math.round(tgt)
                   + ' at DTE ' + cfg.entryDTE + ' (closest was \u20b9' + got.toFixed(0) + ')' };
        }
      } else {
        k = nearestStrike(strikes, atm + L.off * width);
      }
      var c = chain.get(k);
      var bar = c && (L.t === 'ce' ? c.ce : c.pe);
      if (!bar) return { skip: 'leg ' + L.t.toUpperCase() + ' ' + k + ' did not trade on entry' };
      var px = cfg.entryAt === 'open' ? bar.o : bar.c;
      if (!(px > 0)) return { skip: 'leg ' + L.t.toUpperCase() + ' ' + k + ' has no ' + cfg.entryAt + ' price' };
      legs.push({ type: L.t, sign: L.s, strike: k, entry: px });
    }
    // a two-sided structure whose legs collapsed onto one strike is not that
    // structure any more
    if (spec.legs.length > 1) {
      var ceK = legs.filter(function (l) { return l.type === 'ce' && l.sign < 0; });
      var peK = legs.filter(function (l) { return l.type === 'pe' && l.sign < 0; });
      var wantsApart = spec.legs.some(function (l) { return l.off > 0; })
                    && spec.legs.some(function (l) { return l.off < 0; });
      if (wantsApart && ceK.length && peK.length && ceK[0].strike === peK[0].strike) {
        return { skip: 'strangle legs collapsed onto one strike \u2014 the chain had nothing '
                 + 'wide enough at this premium and DTE' };
      }
    }

    // credit received (positive) or debit paid (negative), per unit
    var credit = 0;
    for (i = 0; i < legs.length; i++) credit += -legs[i].sign * legs[i].entry;
    var slip = cfg.slip || 0;
    credit -= legs.length * slip;

    // exit
    var exitIdx = null, exitWhy = 'expiry settlement', exitVal = 0;
    var target = cfg.target > 0 ? credit * cfg.target / 100 : null;
    var stop = cfg.stop > 0 ? credit * cfg.stop / 100 : null;

    for (var s2 = entry; s2 < Y.sessions.length; s2++) {
      var dte2 = dayDiff(Y.sessions[s2], xDate);
      if (dte2 < 0) break;
      var ch2 = Y.bySX.get(s2 + '|' + xIdx);
      if (!ch2) continue;
      // P&L per unit at the worst and best this session, from the daily H/L.
      // A short leg loses when the premium RISES, so its worst case is the
      // session high; a long leg's worst case is the low.
      var plWorst = 0, plBest = 0, ok = true;
      for (i = 0; i < legs.length; i++) {
        var cc = ch2.get(legs[i].strike);
        var b2 = cc && (legs[i].type === 'ce' ? cc.ce : cc.pe);
        if (!b2) { ok = false; break; }
        plWorst += legs[i].sign * ((legs[i].sign < 0 ? b2.h : b2.l) - legs[i].entry);
        plBest  += legs[i].sign * ((legs[i].sign < 0 ? b2.l : b2.h) - legs[i].entry);
      }
      if (!ok) continue;
      if (s2 > entry || cfg.sameDayStops) {
        // the stop wins an ambiguous session: if both could have triggered we
        // assume the loss, because a daily bar cannot say which came first
        if (stop != null && plWorst <= -stop) {
          exitIdx = s2; exitWhy = 'stop ' + cfg.stop + '% of credit'; exitVal = -stop; break;
        }
        if (target != null && plBest >= target) {
          exitIdx = s2; exitWhy = 'target ' + cfg.target + '% of credit'; exitVal = target; break;
        }
      }
      if (cfg.exitDTE > 0 && dte2 <= cfg.exitDTE && s2 > entry) {
        var plClose = 0;
        for (i = 0; i < legs.length; i++) {
          var cc3 = ch2.get(legs[i].strike);
          var b4 = legs[i].type === 'ce' ? cc3.ce : cc3.pe;
          plClose += legs[i].sign * (b4.c - legs[i].entry);
        }
        exitIdx = s2; exitWhy = 'exit at DTE ' + cfg.exitDTE; exitVal = plClose; break;
      }
    }

    if (exitIdx == null) {
      // held to settlement
      exitVal = 0;
      for (i = 0; i < legs.length; i++) {
        var intr = legs[i].type === 'ce' ? Math.max(settle - legs[i].strike, 0)
                                         : Math.max(legs[i].strike - settle, 0);
        exitVal += legs[i].sign * (intr - legs[i].entry);
      }
      exitIdx = null;
    }

    var lot = Y.lot.get(entry + '|' + xIdx) || 0;
    if (!lot) return { skip: 'no lot size for this contract' };
    var units = lot * (cfg.lots || 1);
    var ptsPerUnit = exitVal - legs.length * slip;

    var buyVal = 0, sellVal = 0;
    for (i = 0; i < legs.length; i++) {
      var ev = legs[i].entry * units;
      if (legs[i].sign > 0) buyVal += ev; else sellVal += ev;
    }
    // the closing trade reverses each leg; approximate its value at entry price
    for (i = 0; i < legs.length; i++) {
      var cv = legs[i].entry * units;
      if (legs[i].sign > 0) sellVal += cv; else buyVal += cv;
    }
    var ch = charges(buyVal, sellVal, legs.length * 2);

    /* CAPITAL. For a long-only structure this is exact: the premium paid. For
     * anything with a short leg it is SPAN + exposure margin, which this store
     * does not contain, so it is ESTIMATED as a percentage of notional and
     * labelled as an estimate everywhere it is shown. Do not read it as a
     * broker margin quote. */
    var premiumPaid = 0, shortNotional = 0;
    for (i = 0; i < legs.length; i++) {
      if (legs[i].sign > 0) premiumPaid += legs[i].entry * units;
      else shortNotional += legs[i].strike * units;
    }
    var marginEst = shortNotional * (cfg.marginPct > 0 ? cfg.marginPct / 100 : 0.12);
    var capital = shortNotional > 0 ? marginEst + premiumPaid : premiumPaid;
    var capitalExact = shortNotional === 0;

    return {
      expiry: xDate,
      entryDate: Y.sessions[entry],
      exitDate: exitIdx == null ? xDate : Y.sessions[exitIdx],
      dte: dayDiff(Y.sessions[entry], xDate),
      spot: ref, refSrc: refSrc, atm: atm, width: width, settle: settle, lot: lot, units: units,
      legs: legs, credit: credit, why: exitWhy,
      premiumPaid: premiumPaid, shortNotional: shortNotional,
      capital: capital, capitalExact: capitalExact,
      pts: ptsPerUnit,
      gross: ptsPerUnit * units,
      charges: ch,
      net: ptsPerUnit * units - ch
    };
  }

  function run(cfg, years) {
    return Promise.all(years.map(loadYear)).then(function (Ys) {
      var trades = [], skips = {};
      var globalSettle = {};
      Ys.forEach(function (Y) {
        for (var i = 0; i < Y.expiries.length; i++) {
          if (Y.settle[i] != null) globalSettle[Y.expiries[i]] = Y.settle[i];
        }
      });
      Ys.forEach(function (Y) {
        for (var x = 0; x < Y.expiries.length; x++) {
          var r = runExpiry(Y, x, cfg, globalSettle);
          if (r.skip) { skips[r.skip] = (skips[r.skip] || 0) + 1; continue; }
          trades.push(r);
        }
      });
      // the same expiry can appear in two year files; keep one entry per
      // (expiry, entry date) so a December-quoted January expiry is not traded twice
      var seen = {};
      trades = trades.filter(function (t) {
        var k = t.expiry + '|' + t.entryDate;
        if (seen[k]) return false;
        seen[k] = 1; return true;
      });
      trades.sort(function (a, b) { return a.entryDate < b.entryDate ? -1 : 1; });
      return { trades: trades, skips: skips, cfg: cfg };
    });
  }

  function stats(trades) {
    if (!trades.length) return null;
    var net = trades.map(function (t) { return t.net; });
    var sum = net.reduce(function (a, b) { return a + b; }, 0);
    var wins = net.filter(function (v) { return v > 0; });
    var loss = net.filter(function (v) { return v <= 0; });
    var cum = 0, peak = 0, dd = 0, curve = [];
    net.forEach(function (v) {
      cum += v; curve.push(cum);
      if (cum > peak) peak = cum;
      if (peak - cum > dd) dd = peak - cum;
    });
    var mean = sum / net.length;
    var sd = Math.sqrt(net.reduce(function (a, b) { return a + (b - mean) * (b - mean); }, 0)
                       / Math.max(1, net.length - 1));
    return {
      n: net.length, total: sum, mean: mean, sd: sd,
      t: sd > 0 ? mean / (sd / Math.sqrt(net.length)) : 0,
      winRate: 100 * wins.length / net.length,
      avgWin: wins.length ? wins.reduce(function (a, b) { return a + b; }, 0) / wins.length : 0,
      avgLoss: loss.length ? loss.reduce(function (a, b) { return a + b; }, 0) / loss.length : 0,
      maxDD: dd, curve: curve,
      charges: trades.reduce(function (a, b) { return a + b.charges; }, 0),
      peakCapital: Math.max.apply(null, trades.map(function (t) { return t.capital; })),
      avgCapital: trades.reduce(function (a, b) { return a + b.capital; }, 0) / trades.length,
      capitalExact: trades.every(function (t) { return t.capitalExact; })
    };
  }

  /* ---- used by the INDEX INDICATOR tab to trade an option off a signal ----
   *
   * The index tab generates its signal from index bars and then needs one
   * option contract to express it. Both datasets are daily, so a signal on the
   * close of day N trades the option's OPEN on day N+1 -- the same next-bar
   * entry the index tab already uses, no look-ahead introduced.
   *
   * Intraday resolutions cannot use this: there are no intraday option bars in
   * the store, so pickLeg returns a reason instead of a contract and the UI
   * says so rather than inventing a fill.
   */
  var YEAR_FOR = {};      // 'YYYY' -> indexed year, filled by prepareYears

  function prepareYears(fromY, toY) {
    var want = [];
    for (var y = fromY; y <= toY; y++) want.push(y);
    // one extra year so an expiry quoted in December can settle in January
    want.push(toY + 1);
    return Promise.all(want.map(function (y) {
      return loadYear(y).catch(function () { return null; });
    })).then(function (list) {
      var settle = {};
      list.forEach(function (Y) {
        if (!Y) return;
        YEAR_FOR[String(Y.year)] = Y;
        for (var i = 0; i < Y.expiries.length; i++) {
          if (Y.settle[i] != null) settle[Y.expiries[i]] = Y.settle[i];
        }
      });
      return settle;
    });
  }

  /* Pick one option to trade on `date`. kind 'ce'|'pe', off = strikes from the
   * money, minDTE = refuse a contract with less life than this. */
  function pickLeg(date, kind, off, minDTE, premium, premTol) {
    var Y = YEAR_FOR[date.slice(0, 4)];
    if (!Y) return { skip: 'no option data loaded for ' + date.slice(0, 4) };
    var sIdx = Y.sessions.indexOf(date);
    if (sIdx < 0) return { skip: 'no option session on ' + date };
    // nearest expiry with at least minDTE of life
    var best = -1, bestD = 1e9;
    for (var x = 0; x < Y.expiries.length; x++) {
      var dte = dayDiff(date, Y.expiries[x]);
      if (dte < minDTE) continue;
      if (!Y.bySX.get(sIdx + '|' + x)) continue;
      if (dte < bestD) { bestD = dte; best = x; }
    }
    if (best < 0) return { skip: 'no expiry with >=' + minDTE + ' days trading on ' + date };
    var chain = Y.bySX.get(sIdx + '|' + best);
    var strikes = Array.from(chain.keys()).sort(function (a, b) { return a - b; });
    var ref = impliedRef(chain, strikes, 'open') || impliedRef(chain, strikes, 'close');
    if (ref == null) return { skip: 'cannot price the chain on ' + date };
    var step = strikes.length > 1 ? Math.min.apply(null, strikes.slice(1).map(function (v, i) {
      return v - strikes[i];
    })) : 50;
    var k;
    if (premium > 0) {
      var atm0 = nearestStrike(strikes, ref);
      k = strikeByPremium(chain, strikes, kind, premium, 'open',
                          kind === 'pe' ? null : atm0, kind === 'pe' ? atm0 : null);
      if (k == null) return { skip: 'no ' + kind.toUpperCase() + ' priced on ' + date };
      var lg0 = chain.get(k), b0 = kind === 'ce' ? lg0.ce : lg0.pe;
      var got = b0 ? b0.o : 0;
      var tol = (premTol > 0 ? premTol : 40) / 100;
      if (!(got > 0) || Math.abs(got - premium) > tol * premium) {
        return { skip: 'no ' + kind.toUpperCase() + ' near \u20b9' + premium
                 + ' on ' + date + ' (closest \u20b9' + (got || 0).toFixed(0) + ')' };
      }
    } else {
      var sign = kind === 'ce' ? 1 : -1;
      k = nearestStrike(strikes, nearestStrike(strikes, ref) + sign * off * step);
    }
    var leg = chain.get(k);
    var bar = leg && (kind === 'ce' ? leg.ce : leg.pe);
    if (!bar || !(bar.o > 0)) {
      return { skip: kind.toUpperCase() + ' ' + k + ' did not trade at the open on ' + date };
    }
    var lot = Y.lot.get(sIdx + '|' + best) || 0;
    if (!lot) return { skip: 'no lot size for the ' + Y.expiries[best] + ' contract' };
    return { expiry: Y.expiries[best], dte: bestD, strike: k, kind: kind,
             lot: lot, ref: ref, entry: bar.o, year: Y.year, sIdx: sIdx, xIdx: best };
  }

  /* The option's own daily bar on a later date, for stops and exits. */
  function legBar(leg, date) {
    var Y = YEAR_FOR[date.slice(0, 4)];
    if (!Y) return null;
    var sIdx = Y.sessions.indexOf(date);
    if (sIdx < 0) return null;
    var xIdx = Y.expiries.indexOf(leg.expiry);
    if (xIdx < 0) return null;
    var chain = Y.bySX.get(sIdx + '|' + xIdx);
    if (!chain) return null;
    var l = chain.get(leg.strike);
    return l ? (leg.kind === 'ce' ? l.ce : l.pe) || null : null;
  }

  global.NA_OPT = {
    prepareYears: prepareYears, pickLeg: pickLeg, legBar: legBar,
    manifest: manifest, loadYear: loadYear, run: run, stats: stats,
    STRATS: STRATS, charges: charges
  };
})(window);
