/** node --test scripts/test/ */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  MIN_PREMIUM, STRIKE_WINDOW, aggressorSign, buildFlowMetrics, legacyFlowImbalance,
  normaliseFlowChain, oiShiftRupees, parseNseTimestamp, restingPressure, restingRupees,
  spreadBps, spreadSummary, tradedValue,
} from '../lib/option-flow.mjs';

/* Build an NSE-shaped leg. Defaults are a liquid near-the-money option: a
   realistic premium and a tight touch, so a test that cares about one field
   does not accidentally trip MIN_PREMIUM or the spread guard. */
function leg({
  ltp = 100, bid = 99.5, ask = 100.5, bidQty = 0, askQty = 0,
  bookBuyQty = 0, bookSellQty = 0, oi = 0, oiChange = 0, volume = 0, iv = 12,
  expiryDate = '17-11-2026',
} = {}) {
  return {
    lastPrice: ltp,
    buyPrice1: bid,
    sellPrice1: ask,
    buyQuantity1: bidQty,
    sellQuantity1: askQty,
    totalBuyQuantity: bookBuyQty,
    totalSellQuantity: bookSellQty,
    openInterest: oi,
    changeinOpenInterest: oiChange,
    totalTradedVolume: volume,
    impliedVolatility: iv,
    expiryDate,
  };
}

function chain(entries, { spot = 25000, timestamp = '11-Nov-2026 15:40:00' } = {}) {
  const data = entries.map(({ strike, CE, PE }) => ({
    strikePrice: strike,
    CE: CE ? leg(CE) : undefined,
    PE: PE ? leg(PE) : undefined,
  }));
  return {
    records: { timestamp, underlyingValue: spot, data },
    filtered: { data },
  };
}

test('aggressorSign applies Lee-Ready and refuses to guess inside the spread', () => {
  assert.equal(aggressorSign(leg({ ltp: 100.5, bid: 99.5, ask: 100.5 })), 1, 'at the ask is buyer-initiated');
  assert.equal(aggressorSign(leg({ ltp: 101, bid: 99.5, ask: 100.5 })), 1, 'above the ask is buyer-initiated');
  assert.equal(aggressorSign(leg({ ltp: 99.5, bid: 99.5, ask: 100.5 })), -1, 'at the bid is seller-initiated');
  assert.equal(aggressorSign(leg({ ltp: 99, bid: 99.5, ask: 100.5 })), -1, 'below the bid is seller-initiated');
  // The tick rule needs the previous print, which a snapshot does not carry.
  assert.equal(aggressorSign(leg({ ltp: 100, bid: 99.5, ask: 100.5 })), 0, 'mid-spread is unattributable');
  assert.equal(aggressorSign(leg({ ltp: 0, bid: 0, ask: 0 })), 0, 'an empty leg is unattributable');
  assert.equal(aggressorSign(null), 0);
  // A crossed book is bad data, not a signal.
  assert.equal(aggressorSign(leg({ ltp: 100, bid: 101, ask: 99 })), 0, 'crossed book returns 0');
});

test('spreadBps is relative to the mid and null when uncomputable', () => {
  assert.equal(Math.round(spreadBps(leg({ bid: 99.5, ask: 100.5 }))), 100);
  assert.equal(spreadBps(leg({ bid: 0, ask: 100 })), null);
  assert.equal(spreadBps(leg({ bid: 101, ask: 99 })), null, 'crossed book');
  assert.equal(spreadBps(null), null);
});

test('normaliseFlowChain prefers filtered.data so stacked expiries are not double counted', () => {
  const single = [{ strikePrice: 25000, CE: leg(), PE: leg() }];
  const stacked = [...single, { strikePrice: 25000, CE: leg(), PE: leg() }];
  const c = normaliseFlowChain({
    records: { timestamp: 't', underlyingValue: 25000, data: stacked },
    filtered: { data: single },
  });
  assert.equal(c.rows.length, 1, 'uses filtered, not the stacked records payload');

  // ...and falls back when filtered is empty rather than returning nothing.
  const fallback = normaliseFlowChain({
    records: { timestamp: 't', underlyingValue: 25000, data: single },
    filtered: { data: [] },
  });
  assert.equal(fallback.rows.length, 1);
});

test('the windowed metric is not flipped by far out-of-the-money strikes', () => {
  /* A property of this library, NOT a bug the page had: extractOpenInterest
     already restricted itself to the 13 strikes ATM+/-300. The archive keeps a
     wider window than the page shows, so this pins down that a sub-rupee strike
     outside the window cannot reach the aggregate. legacyFlowImbalance is
     deliberately unwindowed and is what demonstrates the contrast. */
  const c = chain([
    { strike: 25000, CE: { ltp: 200, bidQty: 500, askQty: 0, bookBuyQty: 500, bookSellQty: 0 } },
    { strike: 27300, PE: { ltp: 0.05, bid: 0, ask: 0.1, bidQty: 0, askQty: 900000, bookBuyQty: 0, bookSellQty: 900000 } },
    { strike: 27900, PE: { ltp: 0.05, bid: 0, ask: 0.1, bidQty: 0, askQty: 900000, bookBuyQty: 0, bookSellQty: 900000 } },
  ], { spot: 25000 });

  const normalised = normaliseFlowChain(c, { window: 500 });
  assert.equal(normalised.rows.length, 3);
  assert.equal(normalised.inWindow.length, 1, 'only the 25000 strike is inside the window');

  const legacy = legacyFlowImbalance(normalised);
  assert.ok(legacy < -1_000_000, `legacy figure is dominated by the far strikes, got ${legacy}`);

  const fixed = restingRupees(normalised);
  assert.equal(fixed.callRupees, 500 * 200, 'call side is counted in rupees at the touch');
  assert.equal(fixed.putRupees, 0, 'the sub-rupee far puts contribute nothing');
  assert.ok(fixed.netRupees < 0);
  // The whole point: the two disagree about what the chain is doing.
  assert.notEqual(Math.sign(fixed.netRupees), Math.sign(-legacy));
});

test('premium weighting stops a cheap option outweighing an expensive one', () => {
  // 10,000 lots of a Rs 2 option is Rs 20,000; 500 lots of a Rs 150 option is
  // Rs 75,000. Counted in contracts the cheap strike wins 20:1 and inverts the
  // sign, which is exactly what the unweighted sum did.
  const c = chain([
    { strike: 25000, CE: { ltp: 150, bid: 149, ask: 151, bidQty: 500, askQty: 0 } },
    { strike: 25400, PE: { ltp: 2, bid: 1.9, ask: 2.1, bidQty: 10000, askQty: 0 } },
  ], { spot: 25000 });
  const normalised = normaliseFlowChain(c, { window: 500 });
  const r = restingRupees(normalised);
  assert.equal(r.callRupees, 75000);
  assert.equal(r.putRupees, 20000);
  assert.ok(r.netRupees < 0, 'the expensive call side dominates, as the money does');
});

test('sub-rupee legs are excluded from every rupee aggregate', () => {
  const c = chain([
    { strike: 25000, CE: { ltp: MIN_PREMIUM - 0.01, bidQty: 1_000_000, askQty: 0, oiChange: 500_000 } },
  ], { spot: 25000 });
  const normalised = normaliseFlowChain(c, { window: 500 });
  assert.equal(restingRupees(normalised).callRupees, 0);
  assert.equal(restingRupees(normalised).strikes, 0);
  assert.equal(oiShiftRupees(normalised).callRupees, 0);
});

test('restingPressure ranks on rupees and keeps the OI term separate', () => {
  /* The old scorer was `oiChange + |bidAsk|` -- contracts of open interest
     added to pending-order quantity. Here the high-OI strike has a tiny rupee
     footprint and the low-OI strike a large one; summing the raw terms picks
     the wrong strike. */
  const c = chain([
    { strike: 25000, CE: { ltp: 2, bid: 1.9, ask: 2.1, bidQty: 0, askQty: 50_000, oiChange: 900_000 } },
    { strike: 25100, CE: { ltp: 180, bid: 179, ask: 181, bidQty: 0, askQty: 5_000, oiChange: 1_000 } },
  ], { spot: 25000 });
  const normalised = normaliseFlowChain(c, { window: 500 });
  const ranked = restingPressure(normalised);

  assert.equal(ranked.length, 2);
  assert.equal(ranked[0].strike, 25100, 'Rs 900,000 of pressure outranks Rs 100,000');
  assert.equal(ranked[0].netSellRupees, 5_000 * 180);
  assert.equal(ranked[1].netSellRupees, 50_000 * 2);
  // Under the old additive score the other strike would have won.
  const oldScore = (r) => r.netSellQty + (r.strike === 25000 ? 900_000 : 1_000);
  assert.ok(oldScore(ranked[1]) > oldScore(ranked[0]), 'the additive score inverts the ranking');
  // OI is reported, never folded into the score.
  assert.equal(ranked[0].oiChangeRupees, 1_000 * 180);
  assert.ok(!('score' in ranked[0]));
});

test('restingPressure ignores strikes with net BUY interest', () => {
  const c = chain([
    { strike: 25000, CE: { ltp: 100, bidQty: 900, askQty: 100 } },
  ], { spot: 25000 });
  assert.deepEqual(restingPressure(normaliseFlowChain(c, { window: 500 })), []);
});

test('tradedValue and spreadSummary stay inside the window', () => {
  const c = chain([
    { strike: 25000, CE: { ltp: 100, bid: 99.5, ask: 100.5, volume: 1000 } },
    { strike: 26000, CE: { ltp: 100, bid: 90, ask: 110, volume: 9_000_000 } },
  ], { spot: 25000 });
  const normalised = normaliseFlowChain(c, { window: 500 });
  assert.equal(tradedValue(normalised).callRupees, 1000 * 100, 'the far strike is excluded');
  assert.equal(Math.round(spreadSummary(normalised).callSpreadBps), 100, 'the 2000bps far strike is excluded');
});

test('buildFlowMetrics carries spot, the window census and the legacy figure', () => {
  const c = chain([
    { strike: 25000, CE: { ltp: 200, bidQty: 500 }, PE: { ltp: 190, askQty: 300 } },
    { strike: 27300, PE: { ltp: 0.05, bid: 0, ask: 0.1, bookSellQty: 900000 } },
  ], { spot: 25000 });
  const m = buildFlowMetrics(c, { window: 500 });

  assert.equal(m.spot, 25000);
  assert.equal(m.window, 500);
  assert.equal(m.strikesTotal, 2);
  assert.equal(m.strikesInWindow, 1);
  assert.equal(m.timestamp, '11-Nov-2026 15:40:00');
  assert.equal(m.expiry, '17-11-2026');
  assert.equal(typeof m.legacyFlowImbalance, 'number');
  assert.equal(m.topPressureStrike, 25000);
  assert.equal(m.topPressureSide, 'PE');
  assert.equal(m.topPressureRupees, 300 * 190);
  // No direction label is produced anywhere: the sign of resting put interest
  // is not identifiable from a chain snapshot.
  for (const key of Object.keys(m)) {
    assert.ok(!/^direction/i.test(key), `buildFlowMetrics must not emit a direction label, found ${key}`);
  }
});

test('STRIKE_WINDOW is wider than the page window but still excludes the tail', () => {
  // The page shows ATM+/-300; the archive keeps more so the choice stays
  // testable, but not the sub-rupee strikes 2,000+ points out.
  assert.ok(STRIKE_WINDOW > 300);
  assert.ok(STRIKE_WINDOW < 2045);
});

test('parseNseTimestamp dates a row by the chain, not by the clock', () => {
  assert.equal(parseNseTimestamp('12-Nov-2026 15:40:00'), '2026-11-12');
  assert.equal(parseNseTimestamp('01-Jan-2027 09:15:00'), '2027-01-01');
  // A pre-open run on the 13th reading the 12th's close must archive 11-12,
  // otherwise every forward return joined to the row is off by a session.
  assert.notEqual(parseNseTimestamp('12-Nov-2026 15:40:00'), '2026-11-13');
  assert.equal(parseNseTimestamp('garbage'), null);
  assert.equal(parseNseTimestamp('12-Xxx-2026 15:40:00'), null);
  assert.equal(parseNseTimestamp(undefined), null);
});
