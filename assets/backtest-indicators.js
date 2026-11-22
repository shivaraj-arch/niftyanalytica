/* Indicator library for the intraday backtester.
 *
 * Every function takes plain arrays of a single session's bars and returns an
 * array the same length, NaN-padded where there is not yet enough history.
 * Standard definitions are used throughout so values line up with TradingView
 * and other charting platforms rather than being approximately right.
 *
 * NO VOLUME INDICATORS. Kite returns volume = 0 for NSE indices for every year
 * before 2026, so OBV, MFI, VWAP, VWMA and Volume Profile cannot be computed
 * honestly over this history. Offering them would mean offering a number
 * derived from zeros. They are excluded deliberately, not forgotten.
 */
'use strict';

const IND = (() => {
  const nan = (n) => new Array(n).fill(NaN);

  /* ---------- moving averages ---------- */

  // Averaging a series that itself starts with NaNs (an EMA of an EMA, a
  // signal line, a stochastic of an RSI) must SKIP the leading gap, not
  // substitute zeros for it. Zero-filling seeds the recursion with values that
  // never existed and never fully decays: TEMA(10) on a flat 100 series came
  // out at 99.22 instead of 100. On 5-minute bars a session is 75 candles and
  // TEMA(10) needs 30, so that error covered 40% of every session.
  function firstFinite(x) {
    for (let i = 0; i < x.length; i++) if (Number.isFinite(x[i])) return i;
    return -1;
  }

  function emaFrom(x, n) {
    const o = nan(x.length), f = firstFinite(x);
    if (f < 0 || f + n > x.length) return o;
    const k = 2 / (n + 1);
    let s = 0;
    for (let i = f; i < f + n; i++) s += x[i];
    let prev = s / n;
    o[f + n - 1] = prev;
    for (let i = f + n; i < x.length; i++) {
      if (!Number.isFinite(x[i])) continue;
      prev = x[i] * k + prev * (1 - k);
      o[i] = prev;
    }
    return o;
  }

  function smaFrom(x, n) {
    const o = nan(x.length), f = firstFinite(x);
    if (f < 0) return o;
    let s = 0;
    for (let i = f; i < x.length; i++) {
      s += x[i];
      if (i >= f + n) s -= x[i - n];
      if (i >= f + n - 1) o[i] = s / n;
    }
    return o;
  }


  function sma(x, n) {
    const o = nan(x.length);
    let s = 0;
    for (let i = 0; i < x.length; i++) {
      s += x[i];
      if (i >= n) s -= x[i - n];
      if (i >= n - 1) o[i] = s / n;
    }
    return o;
  }

  function ema(x, n) {
    const o = nan(x.length), k = 2 / (n + 1);
    let prev = NaN;
    for (let i = 0; i < x.length; i++) {
      if (i === n - 1) {                       // seed on the first full SMA
        let s = 0; for (let j = 0; j < n; j++) s += x[j];
        prev = s / n; o[i] = prev;
      } else if (i >= n) {
        prev = x[i] * k + prev * (1 - k); o[i] = prev;
      }
    }
    return o;
  }

  function wma(x, n) {
    const o = nan(x.length), denom = (n * (n + 1)) / 2;
    for (let i = n - 1; i < x.length; i++) {
      let s = 0;
      for (let j = 0; j < n; j++) s += x[i - n + 1 + j] * (j + 1);
      o[i] = s / denom;
    }
    return o;
  }

  // Hull MA: WMA(2*WMA(n/2) - WMA(n), sqrt(n)). Reacts faster with less lag.
  function hma(x, n) {
    const half = Math.max(Math.round(n / 2), 1), sq = Math.max(Math.round(Math.sqrt(n)), 1);
    const a = wma(x, half), b = wma(x, n);
    const diff = x.map((_, i) => (Number.isFinite(a[i]) && Number.isFinite(b[i]) ? 2 * a[i] - b[i] : NaN));
    const f = firstFinite(diff);
    const o = nan(x.length), denom = (sq * (sq + 1)) / 2;
    if (f < 0) return o;
    for (let i = f + sq - 1; i < x.length; i++) {
      let t = 0;
      for (let j = 0; j < sq; j++) t += diff[i - sq + 1 + j] * (j + 1);
      o[i] = t / denom;
    }
    return o;
  }

  function dema(x, n) {
    const e1 = ema(x, n), e2 = emaFrom(e1, n);
    return x.map((_, i) => (Number.isFinite(e1[i]) && Number.isFinite(e2[i]) ? 2 * e1[i] - e2[i] : NaN));
  }

  function tema(x, n) {
    const e1 = ema(x, n), e2 = emaFrom(e1, n), e3 = emaFrom(e2, n);
    return x.map((_, i) => (Number.isFinite(e3[i]) ? 3 * e1[i] - 3 * e2[i] + e3[i] : NaN));
  }

  // Wilder's smoothing — used by RSI, ATR, ADX. Not the same as an EMA of
  // period n: the factor is 1/n, equivalent to an EMA of 2n-1.
  function wilder(x, n, from = n) {
    const o = nan(x.length);
    let s = 0, c = 0;
    for (let i = 0; i < x.length; i++) {
      if (!Number.isFinite(x[i])) continue;
      if (c < n) { s += x[i]; c++; if (c === n) o[i] = s / n; continue; }
      const p = o[i - 1];
      o[i] = Number.isFinite(p) ? (p * (n - 1) + x[i]) / n : NaN;
    }
    return o;
  }

  /* ---------- oscillators ---------- */

  function rsi(c, n) {
    const o = nan(c.length);
    if (c.length <= n) return o;
    let g = 0, l = 0;
    for (let i = 1; i <= n; i++) { const d = c[i] - c[i - 1]; if (d >= 0) g += d; else l -= d; }
    g /= n; l /= n;
    o[n] = l === 0 ? 100 : 100 - 100 / (1 + g / l);
    for (let i = n + 1; i < c.length; i++) {
      const d = c[i] - c[i - 1];
      g = (g * (n - 1) + (d > 0 ? d : 0)) / n;
      l = (l * (n - 1) + (d < 0 ? -d : 0)) / n;
      o[i] = l === 0 ? 100 : 100 - 100 / (1 + g / l);
    }
    return o;
  }

  function stoch(h, l, c, kLen, kSmooth, dLen) {
    const raw = nan(c.length);
    for (let i = kLen - 1; i < c.length; i++) {
      let hi = -Infinity, lo = Infinity;
      for (let j = i - kLen + 1; j <= i; j++) { if (h[j] > hi) hi = h[j]; if (l[j] < lo) lo = l[j]; }
      raw[i] = hi === lo ? 50 : ((c[i] - lo) / (hi - lo)) * 100;
    }
    const k = kSmooth > 1 ? smaFrom(raw, kSmooth) : raw;
    return { k, d: smaFrom(k, dLen) };
  }

  function stochRsi(c, rsiLen, stochLen, kSmooth, dLen) {
    const r = rsi(c, rsiLen);
    const raw = nan(c.length);
    for (let i = rsiLen + stochLen - 1; i < c.length; i++) {
      let hi = -Infinity, lo = Infinity;
      for (let j = i - stochLen + 1; j <= i; j++) {
        if (!Number.isFinite(r[j])) { hi = NaN; break; }
        if (r[j] > hi) hi = r[j]; if (r[j] < lo) lo = r[j];
      }
      if (Number.isFinite(hi)) raw[i] = hi === lo ? 50 : ((r[i] - lo) / (hi - lo)) * 100;
    }
    const k = smaFrom(raw, kSmooth);
    return { k, d: smaFrom(k, dLen) };
  }

  function macd(c, fast, slow, sig) {
    const f = ema(c, fast), s = ema(c, slow);
    const line = c.map((_, i) => (Number.isFinite(f[i]) && Number.isFinite(s[i]) ? f[i] - s[i] : NaN));
    const signal = emaFrom(line, sig);
    const hist = line.map((v, i) => (Number.isFinite(v) && Number.isFinite(signal[i]) ? v - signal[i] : NaN));
    return { line, signal, hist };
  }

  function cci(h, l, c, n) {
    const tp = c.map((_, i) => (h[i] + l[i] + c[i]) / 3);
    const m = sma(tp, n), o = nan(c.length);
    for (let i = n - 1; i < c.length; i++) {
      let dev = 0;
      for (let j = i - n + 1; j <= i; j++) dev += Math.abs(tp[j] - m[i]);
      dev /= n;
      o[i] = dev === 0 ? 0 : (tp[i] - m[i]) / (0.015 * dev);
    }
    return o;
  }

  function williamsR(h, l, c, n) {
    const o = nan(c.length);
    for (let i = n - 1; i < c.length; i++) {
      let hi = -Infinity, lo = Infinity;
      for (let j = i - n + 1; j <= i; j++) { if (h[j] > hi) hi = h[j]; if (l[j] < lo) lo = l[j]; }
      o[i] = hi === lo ? -50 : ((hi - c[i]) / (hi - lo)) * -100;
    }
    return o;
  }

  function roc(c, n) {
    return c.map((v, i) => (i >= n && c[i - n] !== 0 ? ((v - c[i - n]) / c[i - n]) * 100 : NaN));
  }

  function momentum(c, n) {
    return c.map((v, i) => (i >= n ? v - c[i - n] : NaN));
  }

  // Awesome Oscillator: SMA5 - SMA34 of the median price.
  function ao(h, l) {
    const mp = h.map((v, i) => (v + l[i]) / 2);
    const a = sma(mp, 5), b = sma(mp, 34);
    return mp.map((_, i) => (Number.isFinite(a[i]) && Number.isFinite(b[i]) ? a[i] - b[i] : NaN));
  }

  function ultimate(h, l, c, n1 = 7, n2 = 14, n3 = 28) {
    const bp = nan(c.length), tr = nan(c.length);
    for (let i = 1; i < c.length; i++) {
      const tl = Math.min(l[i], c[i - 1]);
      bp[i] = c[i] - tl;
      tr[i] = Math.max(h[i], c[i - 1]) - tl;
    }
    const roll = (a, n, i) => { let s = 0; for (let j = i - n + 1; j <= i; j++) s += a[j]; return s; };
    const o = nan(c.length);
    // A window with zero true range (a dead flat stretch) divides by zero.
    // Treat a rangeless window as neutral 0.5 rather than emitting NaN.
    const avg = (i, n) => { const t = roll(tr, n, i); return t === 0 ? 0.5 : roll(bp, n, i) / t; };
    for (let i = n3; i < c.length; i++) {
      o[i] = (100 * (4 * avg(i, n1) + 2 * avg(i, n2) + avg(i, n3))) / 7;
    }
    return o;
  }

  function trix(c, n) {
    const e3 = emaFrom(emaFrom(ema(c, n), n), n);
    return e3.map((v, i) => (Number.isFinite(v) && Number.isFinite(e3[i - 1])
      ? (e3[i - 1] === 0 ? 0 : ((v - e3[i - 1]) / e3[i - 1]) * 10000) : NaN));
  }

  /* ---------- volatility and bands ---------- */

  // True range is left UNDEFINED at index 0. It needs a previous close, and
  // seeding it with h[0]-l[0] shifts Wilder's window one bar earlier than the
  // directional-movement series, which misaligns ADX: on a pure uptrend that
  // produced +DI of 101.05, above the 100 bound. Leaving [0] as NaN keeps TR,
  // +DM and -DM on the same window.
  function trueRange(h, l, c) {
    const tr = nan(c.length);
    for (let i = 1; i < c.length; i++) {
      tr[i] = Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]));
    }
    return tr;
  }

  function atr(h, l, c, n) { return wilder(trueRange(h, l, c), n); }

  function bollinger(c, n, k) {
    const up = nan(c.length), mid = nan(c.length), lo = nan(c.length),
          pctB = nan(c.length), bw = nan(c.length);
    let s = 0, sq = 0;
    for (let i = 0; i < c.length; i++) {
      s += c[i]; sq += c[i] * c[i];
      if (i >= n) { s -= c[i - n]; sq -= c[i - n] * c[i - n]; }
      if (i >= n - 1) {
        const m = s / n, sd = Math.sqrt(Math.max(sq / n - m * m, 0));
        mid[i] = m; up[i] = m + k * sd; lo[i] = m - k * sd;
        pctB[i] = up[i] === lo[i] ? 0.5 : (c[i] - lo[i]) / (up[i] - lo[i]);
        bw[i] = m === 0 ? 0 : ((up[i] - lo[i]) / m) * 100;
      }
    }
    return { up, mid, lo, pctB, bw };
  }

  function keltner(h, l, c, n, mult) {
    const m = ema(c, n), a = atr(h, l, c, n);
    return {
      mid: m,
      up: m.map((v, i) => (Number.isFinite(v) && Number.isFinite(a[i]) ? v + mult * a[i] : NaN)),
      lo: m.map((v, i) => (Number.isFinite(v) && Number.isFinite(a[i]) ? v - mult * a[i] : NaN)),
    };
  }

  function donchian(h, l, n) {
    const up = nan(h.length), lo = nan(h.length), mid = nan(h.length);
    for (let i = n - 1; i < h.length; i++) {
      let hi = -Infinity, low = Infinity;
      for (let j = i - n + 1; j <= i; j++) { if (h[j] > hi) hi = h[j]; if (l[j] < low) low = l[j]; }
      up[i] = hi; lo[i] = low; mid[i] = (hi + low) / 2;
    }
    return { up, lo, mid };
  }

  // Supertrend. Returns the line and a +1/-1 direction, which is what most
  // people actually trade off.
  function supertrend(h, l, c, n, mult) {
    const a = atr(h, l, c, n), line = nan(c.length), dir = nan(c.length);
    let upper = NaN, lower = NaN, trend = 1;
    for (let i = 0; i < c.length; i++) {
      if (!Number.isFinite(a[i])) continue;
      const mid = (h[i] + l[i]) / 2;
      let u = mid + mult * a[i], d = mid - mult * a[i];
      if (Number.isFinite(upper)) {
        u = c[i - 1] > upper ? Math.max(u, upper) : u;
        d = c[i - 1] < lower ? Math.min(d, lower) : d;
      }
      if (!Number.isFinite(upper)) trend = 1;
      else if (c[i] > upper) trend = 1;
      else if (c[i] < lower) trend = -1;
      upper = u; lower = d;
      dir[i] = trend;
      line[i] = trend === 1 ? d : u;
    }
    return { line, dir };
  }

  /* ---------- trend ---------- */

  function adx(h, l, c, n) {
    const len = c.length, pdm = nan(len), ndm = nan(len);
    for (let i = 1; i < len; i++) {
      const up = h[i] - h[i - 1], dn = l[i - 1] - l[i];
      pdm[i] = up > dn && up > 0 ? up : 0;
      ndm[i] = dn > up && dn > 0 ? dn : 0;
    }
    const tr = wilder(trueRange(h, l, c), n);
    const sp = wilder(pdm, n), sn = wilder(ndm, n);
    const pdi = nan(len), ndi = nan(len), dx = nan(len);
    for (let i = 0; i < len; i++) {
      if (!Number.isFinite(tr[i]) || tr[i] === 0) continue;
      pdi[i] = (sp[i] / tr[i]) * 100;
      ndi[i] = (sn[i] / tr[i]) * 100;
      const sum = pdi[i] + ndi[i];
      dx[i] = sum === 0 ? 0 : (Math.abs(pdi[i] - ndi[i]) / sum) * 100;
    }
    return { adx: wilder(dx, n), pdi, ndi };
  }

  function aroon(h, l, n) {
    const up = nan(h.length), dn = nan(h.length), osc = nan(h.length);
    for (let i = n; i < h.length; i++) {
      let hi = -Infinity, lo = Infinity, hIdx = i, lIdx = i;
      for (let j = i - n; j <= i; j++) {
        if (h[j] >= hi) { hi = h[j]; hIdx = j; }
        if (l[j] <= lo) { lo = l[j]; lIdx = j; }
      }
      up[i] = ((n - (i - hIdx)) / n) * 100;
      dn[i] = ((n - (i - lIdx)) / n) * 100;
      osc[i] = up[i] - dn[i];
    }
    return { up, dn, osc };
  }

  function psar(h, l, step = 0.02, max = 0.2) {
    const o = nan(h.length);
    if (h.length < 2) return o;
    let bull = h[1] >= h[0], af = step;
    let sar = bull ? l[0] : h[0], ep = bull ? h[0] : l[0];
    for (let i = 1; i < h.length; i++) {
      sar += af * (ep - sar);
      if (bull) {
        if (l[i] < sar) { bull = false; sar = ep; ep = l[i]; af = step; }
        else if (h[i] > ep) { ep = h[i]; af = Math.min(af + step, max); }
      } else {
        if (h[i] > sar) { bull = true; sar = ep; ep = h[i]; af = step; }
        else if (l[i] < ep) { ep = l[i]; af = Math.min(af + step, max); }
      }
      o[i] = sar;
    }
    return o;
  }

  return {
    sma, ema, wma, hma, dema, tema, wilder, emaFrom, smaFrom,
    rsi, stoch, stochRsi, macd, cci, williamsR, roc, momentum, ao, ultimate, trix,
    atr, trueRange, bollinger, keltner, donchian, supertrend,
    adx, aroon, psar,
  };
})();

if (typeof module !== 'undefined') module.exports = IND;
