/* Archive the option chain's order-book fields so the flow metrics become
 * testable.
 *
 * NSE's option-chain API is live-only: there is no historical endpoint, and
 * data/index-cache/ stores only date/fetchedAt/contributorsMetadata/marketStatus
 * while data/live-snapshot.json is overwritten on every refresh. So before this
 * script existed there was no way to ask whether "Flow Imbalance" or an
 * "Upside trap" was ever followed by the move it implied -- not merely untested
 * but untestable. That is the defect worth fixing first; see option-flow.mjs
 * for what was and was not wrong with the metrics themselves. This writes the inputs down.
 *
 * Two files:
 *   data/option-flow/metrics.csv            one row per snapshot (the ledger)
 *   data/option-flow/strikes-YYYY-MM.csv    one row per strike per snapshot
 *
 * metrics.csv is the dedupe authority. NSE's records.timestamp only advances
 * when the chain actually refreshes, so a re-run inside the same refresh window
 * is a no-op and the per-strike file stays safely append-only. That matters
 * because the per-strike file grows to ~25k rows a month and must not be
 * rewritten every 15 minutes.
 *
 * totalTradedVolume is CUMULATIVE for the session. It is stored as a level, not
 * a rate; signed flow comes from differencing consecutive snapshots once there
 * is history, which is the whole point of keeping them.
 */

import fs from "node:fs/promises";
import path from "node:path";
import { buildFlowMetrics, parseNseTimestamp, STRIKE_WINDOW } from "./lib/option-flow.mjs";

const SNAPSHOT_FILE = path.resolve(process.env.OPTION_FLOW_SNAPSHOT || "data/live-snapshot.json");
const OUTPUT_DIR = path.resolve(process.env.OPTION_FLOW_DIR || "data/option-flow");
const METRICS_FILE = path.join(OUTPUT_DIR, "metrics.csv");
const IST_TIME_ZONE = "Asia/Kolkata";
/* Same project the pg_cron migrations post to; overridable via SUPABASE_URL. */
const DEFAULT_SUPABASE_URL = "https://gthnjueqoufdtwtzjcxg.supabase.co";

const METRIC_COLUMNS = [
  "date", "fetchedAt", "nseTimestamp", "expiry", "spot", "window",
  "strikesTotal", "strikesInWindow",
  "restingCallRupees", "restingPutRupees", "restingNetRupees",
  "oiShiftCallRupees", "oiShiftPutRupees", "oiShiftNetRupees",
  "tradedCallRupees", "tradedPutRupees", "tradedNetRupees",
  "callSpreadBps", "putSpreadBps",
  "topPressureStrike", "topPressureSide", "topPressureRupees",
  "legacyFlowImbalance",
];

const STRIKE_COLUMNS = [
  "date", "nseTimestamp", "expiry", "spot", "strike", "distance",
  "ceOi", "ceOiChange", "ceLtp", "ceIv", "ceVolume",
  "ceBid", "ceAsk", "ceBidQty", "ceAskQty", "ceBookBuyQty", "ceBookSellQty", "ceAggressor",
  "peOi", "peOiChange", "peLtp", "peIv", "peVolume",
  "peBid", "peAsk", "peBidQty", "peAskQty", "peBookBuyQty", "peBookSellQty", "peAggressor",
];

function toIstIsoDate(date = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: IST_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function escapeCsv(value) {
  const stringValue = value === null || value === undefined ? "" : String(value);
  if (!/[",\n]/.test(stringValue)) return stringValue;
  return `"${stringValue.replace(/"/g, '""')}"`;
}

function round(value, dp = 2) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "";
  return Number(Number(value).toFixed(dp));
}

async function readLines(file) {
  try {
    const text = await fs.readFile(file, "utf8");
    return text.split(/\r?\n/).filter(Boolean);
  } catch (error) {
    if (error?.code === "ENOENT") return [];
    throw error;
  }
}

function toRow(columns, record) {
  return columns.map((column) => escapeCsv(record[column])).join(",");
}

/* Where the chain comes from.
 *
 * data/live-snapshot.json in the repo is only refreshed by the nightly Sync
 * Closing Snapshot job, so during market hours the committed copy is hours
 * stale -- archiving it would rewrite the same 15:40 row forever.
 *
 * With --from-storage the snapshot is read from Supabase storage instead, which
 * refresh-live-snapshot repopulates EVERY MINUTE during market hours
 * (pg_cron '30-59 3 * * 1-5' and '* 4-9 * * 1-5'). That is the source the
 * intraday cron uses.
 *
 * Scraping NSE directly from CI is deliberately NOT an option here: the
 * equity-stockIndices endpoint returns 404 to unprimed clients behind Akamai,
 * which is why fetchNseSnapshot cannot be called from a runner. The Edge
 * Function already holds working cookies, so the storage object is the only
 * reliable fresh copy.
 */
async function loadSnapshot() {
  const fromStorage = process.argv.includes("--from-storage") || process.env.OPTION_FLOW_FROM_STORAGE === "1";
  if (!fromStorage) {
    return JSON.parse(await fs.readFile(SNAPSHOT_FILE, "utf8"));
  }

  const base = (process.env.SUPABASE_URL || DEFAULT_SUPABASE_URL).replace(/\/$/, "");
  const bucket = process.env.LIVE_SNAPSHOT_BUCKET || "public-data";
  const objectPath = process.env.LIVE_SNAPSHOT_PATH || "live/live-snapshot.json";
  const url = `${base}/storage/v1/object/public/${bucket}/${objectPath}?t=${Date.now()}`;

  const response = await fetch(url, { headers: { "cache-control": "no-cache" } });
  if (!response.ok) {
    throw new Error(`Snapshot storage returned ${response.status} for ${bucket}/${objectPath}`);
  }
  const snapshot = JSON.parse(await response.text());
  console.log(`Read snapshot from storage, fetchedAt ${snapshot.fetchedAt ?? "unknown"}`);
  return snapshot;
}

async function main() {
  const snapshot = await loadSnapshot();
  const optionChain = snapshot.optionChain;
  if (!optionChain?.records) {
    throw new Error("Snapshot has no optionChain.records; nothing to archive.");
  }

  const window = Number(process.env.OPTION_FLOW_WINDOW || STRIKE_WINDOW);
  const metrics = buildFlowMetrics(optionChain, { window });

  if (!metrics.timestamp) {
    throw new Error("optionChain.records.timestamp is missing; refusing to archive an unkeyable snapshot.");
  }
  if (!metrics.spot) {
    throw new Error("optionChain.records.underlyingValue is missing; refusing to archive without spot.");
  }
  if (!metrics.strikesInWindow) {
    throw new Error(`No strikes within ${window} points of spot ${metrics.spot}; refusing to archive an empty window.`);
  }

  const fetchedAt = snapshot.fetchedAt ?? new Date().toISOString();
  // Dated by the CHAIN, not by the clock: a pre-open run reads a snapshot still
  // showing the previous close, and dating it today would misalign every
  // forward return joined to this archive later.
  const chainDate = parseNseTimestamp(metrics.timestamp);
  const clockDate = toIstIsoDate(new Date(fetchedAt));
  const date = process.env.OPTION_FLOW_DATE || chainDate || clockDate;

  /* Refuse a chain that is older than the clock reading it.
   *
   * Outside market hours NSE keeps serving the previous close under its old
   * records.timestamp while underlyingValue goes on ticking -- observed
   * 13-Nov-2026 pre-open, where the same "12-Nov-2026 15:40:00" chain came back
   * with spot 25267.05 and then 25209.85 minutes apart. Archiving that pairs a
   * 12-Nov order book with a 13-Nov spot, and a row whose chain and spot
   * disagree is worse than a missing row: it corrupts the forward-return join
   * silently. The dedupe alone does not catch it, because the first pre-open run
   * of a day may be the first sighting of that timestamp.
   *
   * OPTION_FLOW_ALLOW_STALE=1 overrides, for backfilling from a stored file
   * whose fetchedAt legitimately post-dates the close it captured.
   */
  if (chainDate && chainDate !== clockDate && process.env.OPTION_FLOW_ALLOW_STALE !== "1") {
    console.log(
      `Chain is dated ${chainDate} but was read on ${clockDate}; market is closed `
      + 'and spot no longer matches the book. Nothing archived.',
    );
    return;
  }

  await fs.mkdir(OUTPUT_DIR, { recursive: true });

  // metrics.csv is the ledger: if this NSE timestamp is already recorded for
  // this expiry, the chain has not refreshed and re-running must change nothing.
  const metricHeader = METRIC_COLUMNS.join(",");
  const existing = await readLines(METRICS_FILE);
  const existingData = existing.slice(1);
  const key = `${escapeCsv(metrics.timestamp)},${escapeCsv(metrics.expiry)}`;
  const alreadyLogged = existingData.some((line) => {
    const cells = line.split(",");
    return `${escapeCsv(cells[2])},${escapeCsv(cells[3])}` === key;
  });

  if (alreadyLogged) {
    console.log(`Already archived ${metrics.timestamp} (${metrics.expiry}); no change.`);
    return;
  }

  const metricRecord = {
    date,
    fetchedAt,
    nseTimestamp: metrics.timestamp,
    expiry: metrics.expiry,
    spot: round(metrics.spot),
    window,
    strikesTotal: metrics.strikesTotal,
    strikesInWindow: metrics.strikesInWindow,
    restingCallRupees: round(metrics.restingCallRupees, 0),
    restingPutRupees: round(metrics.restingPutRupees, 0),
    restingNetRupees: round(metrics.restingNetRupees, 0),
    oiShiftCallRupees: round(metrics.oiShiftCallRupees, 0),
    oiShiftPutRupees: round(metrics.oiShiftPutRupees, 0),
    oiShiftNetRupees: round(metrics.oiShiftNetRupees, 0),
    tradedCallRupees: round(metrics.tradedCallRupees, 0),
    tradedPutRupees: round(metrics.tradedPutRupees, 0),
    tradedNetRupees: round(metrics.tradedNetRupees, 0),
    callSpreadBps: round(metrics.callSpreadBps),
    putSpreadBps: round(metrics.putSpreadBps),
    topPressureStrike: metrics.topPressureStrike ?? "",
    topPressureSide: metrics.topPressureSide ?? "",
    topPressureRupees: round(metrics.topPressureRupees, 0),
    legacyFlowImbalance: round(metrics.legacyFlowImbalance, 0),
  };

  await fs.writeFile(
    METRICS_FILE,
    `${[metricHeader, ...existingData, toRow(METRIC_COLUMNS, metricRecord)].join("\n")}\n`,
    "utf8",
  );

  // Per-strike rows: append-only, guarded by the ledger check above.
  const month = date.slice(0, 7);
  const strikesFile = path.join(OUTPUT_DIR, `strikes-${month}.csv`);
  const strikeRows = metrics.chain.inWindow.map((row) => toRow(STRIKE_COLUMNS, {
    date,
    nseTimestamp: metrics.timestamp,
    expiry: metrics.expiry,
    spot: round(metrics.spot),
    strike: row.strike,
    distance: round(row.distance),
    ceOi: row.call?.oi ?? "",
    ceOiChange: row.call?.oiChange ?? "",
    ceLtp: row.call?.ltp ?? "",
    ceIv: row.call?.iv ?? "",
    ceVolume: row.call?.volume ?? "",
    ceBid: row.call?.bid ?? "",
    ceAsk: row.call?.ask ?? "",
    ceBidQty: row.call?.bidQty ?? "",
    ceAskQty: row.call?.askQty ?? "",
    ceBookBuyQty: row.call?.bookBuyQty ?? "",
    ceBookSellQty: row.call?.bookSellQty ?? "",
    ceAggressor: row.call?.aggressor ?? "",
    peOi: row.put?.oi ?? "",
    peOiChange: row.put?.oiChange ?? "",
    peLtp: row.put?.ltp ?? "",
    peIv: row.put?.iv ?? "",
    peVolume: row.put?.volume ?? "",
    peBid: row.put?.bid ?? "",
    peAsk: row.put?.ask ?? "",
    peBidQty: row.put?.bidQty ?? "",
    peAskQty: row.put?.askQty ?? "",
    peBookBuyQty: row.put?.bookBuyQty ?? "",
    peBookSellQty: row.put?.bookSellQty ?? "",
    peAggressor: row.put?.aggressor ?? "",
  }));

  const strikeHeader = STRIKE_COLUMNS.join(",");
  const strikesExist = (await readLines(strikesFile)).length > 0;
  await fs.appendFile(
    strikesFile,
    `${strikesExist ? "" : `${strikeHeader}\n`}${strikeRows.join("\n")}\n`,
    "utf8",
  );

  console.log(
    `Archived ${metrics.timestamp} (${metrics.expiry}) spot ${round(metrics.spot)}: `
    + `${strikeRows.length}/${metrics.strikesTotal} strikes within ${window} pts -> `
    + `${path.relative(process.cwd(), strikesFile)}`,
  );
  console.log(
    `  resting net Rs ${round(metrics.restingNetRupees, 0)} `
    + `(calls ${round(metrics.restingCallRupees, 0)}, puts ${round(metrics.restingPutRupees, 0)})  `
    + `legacy all-strike figure was ${round(metrics.legacyFlowImbalance, 0)}`,
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
