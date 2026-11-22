/* The series registry: everything a condition can be built from.
 *
 * Each entry declares its parameters and how to compute an array aligned to
 * the session's bars. The UI reads this to build its menus, so adding an
 * indicator here is all it takes to make it selectable — nothing in the page
 * or the engine needs editing.
 *
 * Deliberately absent: OBV, MFI, VWAP, VWMA, Volume Profile and anything else
 * requiring volume. Kite returns volume = 0 for NSE indices for every year
 * before 2026, so those would be computed from zeros across almost the whole
 * history. An indicator that cannot be computed honestly is worse than a
 * missing one.
 */
'use strict';

const SERIES = (() => {
  const P = (name, def, min, max, step = 1) => ({ name, def, min, max, step });

  const defs = [
    /* ---- price ---- */
    { id: 'close', label: 'Close', group: 'Price', params: [], fn: (b) => b.c },
    { id: 'open', label: 'Open', group: 'Price', params: [], fn: (b) => b.o },
    { id: 'high', label: 'High', group: 'Price', params: [], fn: (b) => b.h },
    { id: 'low', label: 'Low', group: 'Price', params: [], fn: (b) => b.l },
    { id: 'hl2', label: 'Median price (H+L)/2', group: 'Price', params: [],
      fn: (b) => b.h.map((v, i) => (v + b.l[i]) / 2) },

    /* ---- oscillators ---- */
    { id: 'rsi', label: 'RSI', group: 'Oscillators', params: [P('length', 14, 2, 100)],
      fn: (b, p) => IND.rsi(b.c, p.length), bounds: [0, 100] },
    { id: 'stoch_k', label: 'Stochastic %K', group: 'Oscillators',
      params: [P('length', 14, 2, 100), P('smooth', 3, 1, 20), P('dLength', 3, 1, 20)],
      fn: (b, p) => IND.stoch(b.h, b.l, b.c, p.length, p.smooth, p.dLength).k, bounds: [0, 100] },
    { id: 'stoch_d', label: 'Stochastic %D', group: 'Oscillators',
      params: [P('length', 14, 2, 100), P('smooth', 3, 1, 20), P('dLength', 3, 1, 20)],
      fn: (b, p) => IND.stoch(b.h, b.l, b.c, p.length, p.smooth, p.dLength).d, bounds: [0, 100] },
    { id: 'stochrsi_k', label: 'Stochastic RSI %K', group: 'Oscillators',
      params: [P('rsiLength', 14, 2, 100), P('stochLength', 14, 2, 100), P('smooth', 3, 1, 20), P('dLength', 3, 1, 20)],
      fn: (b, p) => IND.stochRsi(b.c, p.rsiLength, p.stochLength, p.smooth, p.dLength).k, bounds: [0, 100] },
    { id: 'macd', label: 'MACD line', group: 'Oscillators',
      params: [P('fast', 12, 2, 100), P('slow', 26, 3, 200), P('signal', 9, 1, 50)],
      fn: (b, p) => IND.macd(b.c, p.fast, p.slow, p.signal).line },
    { id: 'macd_signal', label: 'MACD signal line', group: 'Oscillators',
      params: [P('fast', 12, 2, 100), P('slow', 26, 3, 200), P('signal', 9, 1, 50)],
      fn: (b, p) => IND.macd(b.c, p.fast, p.slow, p.signal).signal },
    { id: 'macd_hist', label: 'MACD histogram', group: 'Oscillators',
      params: [P('fast', 12, 2, 100), P('slow', 26, 3, 200), P('signal', 9, 1, 50)],
      fn: (b, p) => IND.macd(b.c, p.fast, p.slow, p.signal).hist },
    { id: 'cci', label: 'CCI', group: 'Oscillators', params: [P('length', 20, 2, 200)],
      fn: (b, p) => IND.cci(b.h, b.l, b.c, p.length) },
    { id: 'williams', label: 'Williams %R', group: 'Oscillators', params: [P('length', 14, 2, 200)],
      fn: (b, p) => IND.williamsR(b.h, b.l, b.c, p.length), bounds: [-100, 0] },
    { id: 'roc', label: 'Rate of change %', group: 'Oscillators', params: [P('length', 10, 1, 200)],
      fn: (b, p) => IND.roc(b.c, p.length) },
    { id: 'momentum', label: 'Momentum, points', group: 'Oscillators', params: [P('length', 10, 1, 200)],
      fn: (b, p) => IND.momentum(b.c, p.length) },
    { id: 'ao', label: 'Awesome Oscillator', group: 'Oscillators', params: [],
      fn: (b) => IND.ao(b.h, b.l) },
    { id: 'uo', label: 'Ultimate Oscillator', group: 'Oscillators', params: [],
      fn: (b) => IND.ultimate(b.h, b.l, b.c), bounds: [0, 100] },
    { id: 'trix', label: 'TRIX', group: 'Oscillators', params: [P('length', 15, 2, 100)],
      fn: (b, p) => IND.trix(b.c, p.length) },

    /* ---- moving averages ---- */
    { id: 'sma', label: 'SMA', group: 'Moving averages', params: [P('length', 20, 2, 400)],
      fn: (b, p) => IND.sma(b.c, p.length) },
    { id: 'ema', label: 'EMA', group: 'Moving averages', params: [P('length', 20, 2, 400)],
      fn: (b, p) => IND.ema(b.c, p.length) },
    { id: 'wma', label: 'WMA', group: 'Moving averages', params: [P('length', 20, 2, 400)],
      fn: (b, p) => IND.wma(b.c, p.length) },
    { id: 'hma', label: 'Hull MA', group: 'Moving averages', params: [P('length', 20, 4, 400)],
      fn: (b, p) => IND.hma(b.c, p.length) },
    { id: 'dema', label: 'DEMA', group: 'Moving averages', params: [P('length', 20, 2, 200)],
      fn: (b, p) => IND.dema(b.c, p.length) },
    { id: 'tema', label: 'TEMA', group: 'Moving averages', params: [P('length', 20, 2, 120)],
      fn: (b, p) => IND.tema(b.c, p.length) },

    /* ---- bands and volatility ---- */
    { id: 'bb_upper', label: 'Bollinger upper band', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400), P('stdDev', 2, 0.1, 5, 0.1)],
      fn: (b, p) => IND.bollinger(b.c, p.length, p.stdDev).up },
    { id: 'bb_lower', label: 'Bollinger lower band', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400), P('stdDev', 2, 0.1, 5, 0.1)],
      fn: (b, p) => IND.bollinger(b.c, p.length, p.stdDev).lo },
    { id: 'bb_mid', label: 'Bollinger basis', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400), P('stdDev', 2, 0.1, 5, 0.1)],
      fn: (b, p) => IND.bollinger(b.c, p.length, p.stdDev).mid },
    { id: 'bb_pctb', label: 'Bollinger %B', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400), P('stdDev', 2, 0.1, 5, 0.1)],
      fn: (b, p) => IND.bollinger(b.c, p.length, p.stdDev).pctB },
    { id: 'bb_bw', label: 'Bollinger bandwidth %', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400), P('stdDev', 2, 0.1, 5, 0.1)],
      fn: (b, p) => IND.bollinger(b.c, p.length, p.stdDev).bw },
    { id: 'kc_upper', label: 'Keltner upper', group: 'Bands & volatility',
      params: [P('length', 20, 2, 200), P('multiplier', 2, 0.1, 6, 0.1)],
      fn: (b, p) => IND.keltner(b.h, b.l, b.c, p.length, p.multiplier).up },
    { id: 'kc_lower', label: 'Keltner lower', group: 'Bands & volatility',
      params: [P('length', 20, 2, 200), P('multiplier', 2, 0.1, 6, 0.1)],
      fn: (b, p) => IND.keltner(b.h, b.l, b.c, p.length, p.multiplier).lo },
    { id: 'dc_upper', label: 'Donchian upper', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400)], fn: (b, p) => IND.donchian(b.h, b.l, p.length).up },
    { id: 'dc_lower', label: 'Donchian lower', group: 'Bands & volatility',
      params: [P('length', 20, 2, 400)], fn: (b, p) => IND.donchian(b.h, b.l, p.length).lo },
    { id: 'atr', label: 'ATR, points', group: 'Bands & volatility',
      params: [P('length', 14, 2, 200)], fn: (b, p) => IND.atr(b.h, b.l, b.c, p.length) },
    { id: 'st_line', label: 'Supertrend line', group: 'Bands & volatility',
      params: [P('length', 10, 2, 100), P('multiplier', 3, 0.5, 8, 0.5)],
      fn: (b, p) => IND.supertrend(b.h, b.l, b.c, p.length, p.multiplier).line },
    { id: 'st_dir', label: 'Supertrend direction (+1/-1)', group: 'Bands & volatility',
      params: [P('length', 10, 2, 100), P('multiplier', 3, 0.5, 8, 0.5)],
      fn: (b, p) => IND.supertrend(b.h, b.l, b.c, p.length, p.multiplier).dir, bounds: [-1, 1] },

    /* ---- trend ---- */
    { id: 'adx', label: 'ADX', group: 'Trend', params: [P('length', 14, 2, 100)],
      fn: (b, p) => IND.adx(b.h, b.l, b.c, p.length).adx, bounds: [0, 100] },
    { id: 'di_plus', label: '+DI', group: 'Trend', params: [P('length', 14, 2, 100)],
      fn: (b, p) => IND.adx(b.h, b.l, b.c, p.length).pdi, bounds: [0, 100] },
    { id: 'di_minus', label: '-DI', group: 'Trend', params: [P('length', 14, 2, 100)],
      fn: (b, p) => IND.adx(b.h, b.l, b.c, p.length).ndi, bounds: [0, 100] },
    { id: 'aroon_up', label: 'Aroon up', group: 'Trend', params: [P('length', 14, 2, 200)],
      fn: (b, p) => IND.aroon(b.h, b.l, p.length).up, bounds: [0, 100] },
    { id: 'aroon_dn', label: 'Aroon down', group: 'Trend', params: [P('length', 14, 2, 200)],
      fn: (b, p) => IND.aroon(b.h, b.l, p.length).dn, bounds: [0, 100] },
    { id: 'aroon_osc', label: 'Aroon oscillator', group: 'Trend', params: [P('length', 14, 2, 200)],
      fn: (b, p) => IND.aroon(b.h, b.l, p.length).osc, bounds: [-100, 100] },
    { id: 'psar', label: 'Parabolic SAR', group: 'Trend',
      params: [P('step', 0.02, 0.001, 0.2, 0.001), P('max', 0.2, 0.01, 1, 0.01)],
      fn: (b, p) => IND.psar(b.h, b.l, p.step, p.max) },
  ];

  const byId = new Map(defs.map((d) => [d.id, d]));
  const groups = [...new Set(defs.map((d) => d.group))];

  // Each distinct (id, params) pair is computed once per session and reused,
  // so picking "RSI(14) crosses above RSI(14)" costs one pass, not two.
  function evaluate(def, bars, params, cache) {
    const key = def.id + ':' + JSON.stringify(params);
    if (cache.has(key)) return cache.get(key);
    const v = def.fn(bars, params);
    cache.set(key, v);
    return v;
  }

  return { defs, byId, groups, evaluate };
})();

if (typeof module !== 'undefined') module.exports = SERIES;
