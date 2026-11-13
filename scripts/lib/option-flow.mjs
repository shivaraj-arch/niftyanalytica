/* Option-chain order-book metrics.
 *
 * WHY THIS EXISTS
 * ---------------
 * The page's "Flow Imbalance" summed, over the 13 strikes ATM+/-300 that
 * extractOpenInterest selects:
 *     (putTotalBuyQty - putTotalSellQty) - (callTotalBuyQty - callTotalSellQty)
 * On the 11-Nov-2026 15:40 snapshot (spot 25255.05, ATM 25250) that gave
 * +4,217,265, rendered as a bullish tilt.
 *
 * That window is sound and no single strike dominates it: the largest
 * contributor was 27.6% of the total and 12 of the 13 strikes shared a sign.
 * Premiums inside it ran Rs 108-444, a ~4x spread, so premium weighting shifts
 * the number but does not control it. What IS wrong:
 *
 *   1. It reads the WHOLE BOOK, not the touch. totalBuyQuantity /
 *      totalSellQuantity aggregate every resting limit order, including those
 *      that will never fill, and ran ~200x buyQuantity1 / sellQuantity1 on the
 *      same snapshot. At 25300 CE the two disagreed in SIGN: the full book
 *      showed 6:1 sell (678,275 vs 114,985) while the touch showed 2:1 buy
 *      (260 vs 520). Far-from-touch quantity is also the most trivially
 *      spoofable part of the book.
 *
 *   2. Quantities are summed in contracts, so terms are not commensurable --
 *      lots of a cheap option count the same as lots of an expensive one.
 *      Converting to rupees makes the sum mean something.
 *
 *   3. The direction label was incoherent. 'Bullish positioning build' required
 *      netPositioningChange > 0 AND flowImbalance > 0, but the two terms encode
 *      OPPOSITE economics: put OI building is read as writing (bullish), while
 *      net resting BUY interest in puts is people trying to buy puts (bearish).
 *      Both were positive on 11-Nov, so the page asserted a direction from two
 *      components that disagreed about what was happening.
 *
 *   4. The "trap" detector scored `callOIChange + |callBookBuy - callBookSell|`
 *      -- open-interest contracts ADDED to pending-order quantity. On the same
 *      snapshot the OI term was 7.3% of the winning score (25400: OI 68,802 vs
 *      book -871,130), so the ranking was pure resting quantity with an OI
 *      rounding error on top. Its condition also fired on 4 of the 6 strikes
 *      within 150 points of spot, making "no trap" the rare state.
 *
 * WHAT CHANGED
 * ------------
 * - Resting quantity is read at the TOUCH (buyQuantity1/sellQuantity1).
 * - Quantities are converted to rupees (qty * premium) before summing.
 * - Sub-rupee legs are dropped: a one-tick move there is a 20%+ price change.
 * - Aggregates are windowed, as the page already was. The archive keeps a wider
 *   window (500) than the page shows so the choice stays testable later.
 * - No function here returns a direction label. Whether resting put interest is
 *   bullish or bearish is NOT identifiable from a chain snapshot; that gets
 *   settled by archiving and measuring, not by naming.
 *
 * NOTHING HERE IS VALIDATED AS PREDICTIVE. These are descriptive statistics.
 * NSE's option-chain API is live-only, so none of it could be backtested until
 * archive-option-flow.mjs started storing the inputs.
 */

/** Strikes further than this from spot are excluded from every aggregate. */
export const STRIKE_WINDOW = 500;

/** Sub-rupee options are quote noise: a 1-tick move is a 20%+ price change. */
export const MIN_PREMIUM = 1;

const NSE_MONTHS = {
  Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
  Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12",
};

/** "12-Nov-2026 15:40:00" -> "2026-11-12", or null if unparseable.
 *
 * The archive MUST be dated by the chain's own timestamp, not by when the job
 * ran. A pre-open run on the 13th reads a snapshot still showing the 12th's
 * close; dating that row 2026-11-13 would silently misalign every forward
 * return joined to it later, which is the one thing this archive exists to
 * support.
 */
export function parseNseTimestamp(timestamp) {
  const match = /^(\d{2})-([A-Za-z]{3})-(\d{4})/.exec(String(timestamp ?? "").trim());
  if (!match) return null;
  const month = NSE_MONTHS[match[2]];
  if (!month) return null;
  return `${match[3]}-${month}-${match[1]}`;
}

export function parseNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/** Lee-Ready, degraded to a single snapshot: where did the last print land?
 *
 * At or above the ask -> buyer-initiated (+1). At or below the bid -> seller
 * initiated (-1). Inside the spread it is unattributable (0); the tick rule
 * needs the previous print, which a snapshot does not carry. Returns 0 rather
 * than guessing.
 */
export function aggressorSign(leg) {
  const last = parseNumber(leg?.lastPrice);
  const bid = parseNumber(leg?.buyPrice1);
  const ask = parseNumber(leg?.sellPrice1);
  if (!last || !bid || !ask || ask < bid) return 0;
  if (last >= ask) return 1;
  if (last <= bid) return -1;
  return 0;
}

/** Relative spread in basis points of the mid, or null when uncomputable.
 *
 * This is the one quantity on the page that is always real: it is what
 * execution costs, independent of any view. */
export function spreadBps(leg) {
  const bid = parseNumber(leg?.buyPrice1);
  const ask = parseNumber(leg?.sellPrice1);
  if (!bid || !ask || ask < bid) return null;
  const mid = (bid + ask) / 2;
  if (!mid) return null;
  return ((ask - bid) / mid) * 10000;
}

function normaliseLeg(leg) {
  if (!leg) return null;
  return {
    oi: parseNumber(leg.openInterest),
    oiChange: parseNumber(leg.changeinOpenInterest),
    ltp: parseNumber(leg.lastPrice),
    iv: parseNumber(leg.impliedVolatility),
    volume: parseNumber(leg.totalTradedVolume),
    bid: parseNumber(leg.buyPrice1),
    ask: parseNumber(leg.sellPrice1),
    bidQty: parseNumber(leg.buyQuantity1),
    askQty: parseNumber(leg.sellQuantity1),
    bookBuyQty: parseNumber(leg.totalBuyQuantity),
    bookSellQty: parseNumber(leg.totalSellQuantity),
    aggressor: aggressorSign(leg),
    spreadBps: spreadBps(leg),
  };
}

/** Flatten an NSE option-chain payload into one row per strike.
 *
 * `filtered.data` holds a single expiry and is what the UI shows; `records.data`
 * holds every expiry stacked, so summing it double-counts strikes. Prefer
 * filtered and fall back only when it is empty.
 */
export function normaliseFlowChain(optionChain, { window = STRIKE_WINDOW } = {}) {
  const records = optionChain?.records ?? {};
  const source = (optionChain?.filtered?.data?.length ? optionChain.filtered.data : records.data) ?? [];
  const spot = parseNumber(records.underlyingValue);

  const rows = source
    .map((entry) => {
      const strike = parseNumber(entry?.strikePrice);
      const call = normaliseLeg(entry?.CE);
      const put = normaliseLeg(entry?.PE);
      if (!strike || (!call && !put)) return null;
      return { strike, distance: Math.abs(strike - spot), call, put };
    })
    .filter(Boolean)
    .sort((a, b) => a.strike - b.strike);

  return {
    timestamp: records.timestamp ?? "",
    expiry: source[0]?.CE?.expiryDate ?? source[0]?.PE?.expiryDate ?? "",
    spot,
    window,
    rows,
    inWindow: rows.filter((row) => row.distance <= window),
  };
}

/** Net resting rupees at the touch, puts minus calls, windowed.
 *
 * Deliberately NOT given a bullish/bearish reading -- see the header note. The
 * two sides are returned separately so the page can show them without the
 * subtraction implying a direction.
 */
export function restingRupees(chain) {
  let callRupees = 0;
  let putRupees = 0;
  let strikes = 0;

  for (const row of chain.inWindow) {
    let counted = false;
    for (const [leg, add] of [[row.call, (v) => { callRupees += v; }], [row.put, (v) => { putRupees += v; }]]) {
      if (!leg || leg.ltp < MIN_PREMIUM) continue;
      add((leg.bidQty - leg.askQty) * leg.ltp);
      counted = true;
    }
    if (counted) strikes += 1;
  }

  return {
    callRupees,
    putRupees,
    netRupees: putRupees - callRupees,
    strikes,
  };
}

/** The pre-fix metric, kept so the archive stores both and the change is
 * measurable rather than asserted. Do not display this. */
export function legacyFlowImbalance(chain) {
  return chain.rows.reduce((total, row) => {
    const callBidAsk = (row.call?.bookBuyQty ?? 0) - (row.call?.bookSellQty ?? 0);
    const putBidAsk = (row.put?.bookBuyQty ?? 0) - (row.put?.bookSellQty ?? 0);
    return total + (putBidAsk - callBidAsk);
  }, 0);
}

/** Net OI change, puts minus calls, windowed and weighted by premium.
 *
 * The unwindowed contract-count version is dominated by far strikes for exactly
 * the reason restingRupees was. */
export function oiShiftRupees(chain) {
  let callRupees = 0;
  let putRupees = 0;
  for (const row of chain.inWindow) {
    if (row.call && row.call.ltp >= MIN_PREMIUM) callRupees += row.call.oiChange * row.call.ltp;
    if (row.put && row.put.ltp >= MIN_PREMIUM) putRupees += row.put.oiChange * row.put.ltp;
  }
  return { callRupees, putRupees, netRupees: putRupees - callRupees };
}

/** Traded value on each side, windowed.
 *
 * `totalTradedVolume` is CUMULATIVE for the session, so a single snapshot
 * cannot say what traded in the last interval, and the aggressor sign taken
 * from the latest print must not be applied to the whole day's volume. The
 * archive stores these levels so consecutive snapshots can be differenced
 * later; that is the only honest route to signed flow from this API.
 */
export function tradedValue(chain) {
  let callRupees = 0;
  let putRupees = 0;
  for (const row of chain.inWindow) {
    if (row.call) callRupees += row.call.volume * row.call.ltp;
    if (row.put) putRupees += row.put.volume * row.put.ltp;
  }
  return { callRupees, putRupees, netRupees: putRupees - callRupees };
}

/** Median spread in bps across the window, per side. Execution cost, not a view. */
export function spreadSummary(chain) {
  const pick = (side) => chain.inWindow
    .map((row) => row[side]?.spreadBps)
    .filter((v) => typeof v === "number" && Number.isFinite(v))
    .sort((a, b) => a - b);
  const median = (arr) => (arr.length ? arr[Math.floor(arr.length / 2)] : null);
  return { callSpreadBps: median(pick("call")), putSpreadBps: median(pick("put")) };
}

/** Strikes carrying the largest net resting SELL interest at the touch.
 *
 * Replaces the "Upside trap"/"Downside trap" scorer. Three changes:
 *   - ranks on rupees, not on open-interest-contracts-plus-order-quantity;
 *   - reads the touch rather than the full book;
 *   - returns oiChangeRupees alongside instead of adding it into the score, so
 *     the two can be shown without being conflated.
 * The old condition (strike above spot AND callOIChange > putOIChange AND
 * callBidAsk < 0) fired on 4 of the 6 near-spot strikes on the 11-Nov snapshot,
 * so "no trap" was the rare state rather than the signal.
 */
export function restingPressure(chain, { limit = 3 } = {}) {
  const candidates = [];
  for (const row of chain.inWindow) {
    for (const side of ["call", "put"]) {
      const leg = row[side];
      if (!leg || leg.ltp < MIN_PREMIUM) continue;
      const netQty = leg.bidQty - leg.askQty;
      if (netQty >= 0) continue; // only net sell-side pressure
      candidates.push({
        strike: row.strike,
        side: side === "call" ? "CE" : "PE",
        netSellRupees: Math.abs(netQty) * leg.ltp,
        netSellQty: Math.abs(netQty),
        oiChangeRupees: leg.oiChange * leg.ltp,
        spreadBps: leg.spreadBps,
      });
    }
  }
  candidates.sort((a, b) => b.netSellRupees - a.netSellRupees);
  return candidates.slice(0, limit);
}

/** One archive row per snapshot: everything the page shows, plus the legacy
 * value, plus spot so a forward return can be attached later. */
export function buildFlowMetrics(optionChain, { window = STRIKE_WINDOW } = {}) {
  const chain = normaliseFlowChain(optionChain, { window });
  const resting = restingRupees(chain);
  const oiShift = oiShiftRupees(chain);
  const traded = tradedValue(chain);
  const spreads = spreadSummary(chain);
  const pressure = restingPressure(chain, { limit: 1 });

  return {
    timestamp: chain.timestamp,
    expiry: chain.expiry,
    spot: chain.spot,
    window,
    strikesTotal: chain.rows.length,
    strikesInWindow: chain.inWindow.length,
    restingCallRupees: resting.callRupees,
    restingPutRupees: resting.putRupees,
    restingNetRupees: resting.netRupees,
    oiShiftCallRupees: oiShift.callRupees,
    oiShiftPutRupees: oiShift.putRupees,
    oiShiftNetRupees: oiShift.netRupees,
    tradedCallRupees: traded.callRupees,
    tradedPutRupees: traded.putRupees,
    tradedNetRupees: traded.netRupees,
    callSpreadBps: spreads.callSpreadBps,
    putSpreadBps: spreads.putSpreadBps,
    topPressureStrike: pressure[0]?.strike ?? null,
    topPressureSide: pressure[0]?.side ?? null,
    topPressureRupees: pressure[0]?.netSellRupees ?? null,
    legacyFlowImbalance: legacyFlowImbalance(chain),
    chain,
  };
}
