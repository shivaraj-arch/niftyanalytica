import fs from "node:fs/promises";
import path from "node:path";

const SNAPSHOT_FILE = path.resolve("data/live-snapshot.json");
const HISTORY_FILE = path.resolve("data/index-summary-history.csv");
const IST_TIME_ZONE = "Asia/Kolkata";

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

function buildMetadataFallback(snapshot) {
  const indexRecord = Array.isArray(snapshot.contributors?.data) ? snapshot.contributors.data[0] : null;
  if (!indexRecord) {
    return null;
  }

  return {
    indexName: snapshot.contributors?.name ?? indexRecord.symbol ?? null,
    open: indexRecord.open ?? null,
    high: indexRecord.dayHigh ?? null,
    low: indexRecord.dayLow ?? null,
    previousClose: indexRecord.previousClose ?? null,
    last: indexRecord.lastPrice ?? null,
    percChange: indexRecord.pChange ?? null,
    change: indexRecord.change ?? null,
    totalTradedVolume: indexRecord.totalTradedVolume ?? null,
    totalTradedValue: indexRecord.totalTradedValue ?? null,
    ffmc_sum: indexRecord.ffmc ?? null,
    timeVal: snapshot.contributors?.timestamp ?? indexRecord.lastUpdateTime ?? null,
  };
}

function buildMarketStatusFallback(snapshot) {
  const indexRecord = Array.isArray(snapshot.contributors?.data) ? snapshot.contributors.data[0] : null;
  if (!indexRecord) {
    return null;
  }

  return {
    marketStatus: "Closed",
    tradeDate: snapshot.contributors?.timestamp ?? indexRecord.lastUpdateTime ?? null,
  };
}

function escapeCsv(value) {
  const stringValue = String(value ?? "");
  if (!/[",\n]/.test(stringValue)) {
    return stringValue;
  }

  return `"${stringValue.replace(/"/g, '""')}"`;
}

async function loadExistingRows() {
  try {
    const existing = await fs.readFile(HISTORY_FILE, "utf8");
    return existing.split(/\r?\n/).filter(Boolean);
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return [];
    }
    throw error;
  }
}

async function main() {
  const snapshot = JSON.parse(await fs.readFile(SNAPSHOT_FILE, "utf8"));
  const metadata = snapshot.contributors?.metadata ?? buildMetadataFallback(snapshot);
  const marketStatus = snapshot.marketStatus ?? buildMarketStatusFallback(snapshot);

  if (!metadata || !marketStatus) {
    throw new Error("Missing contributors metadata or market status in live snapshot.");
  }

  const fetchedAt = snapshot.fetchedAt ?? new Date().toISOString();
  const date = process.env.NSE_INDEX_HISTORY_DATE || toIstIsoDate(new Date(fetchedAt));
  const header = [
    "date",
    "fetchedAt",
    "tradeDate",
    "marketStatus",
    "indexName",
    "last",
    "open",
    "high",
    "low",
    "previousClose",
    "change",
    "percentChange",
    "totalTradedValue",
    "totalTradedVolume",
    "ffmcSum",
  ].join(",");

  const rowValue = [
    date,
    fetchedAt,
    marketStatus.tradeDate ?? metadata.timeVal ?? "",
    marketStatus.marketStatus ?? "",
    metadata.indexName ?? "NIFTY 50",
    metadata.last ?? "",
    metadata.open ?? "",
    metadata.high ?? "",
    metadata.low ?? "",
    metadata.previousClose ?? "",
    metadata.change ?? marketStatus.variation ?? "",
    metadata.percChange ?? marketStatus.percentChange ?? "",
    metadata.totalTradedValue ?? "",
    metadata.totalTradedVolume ?? "",
    metadata.ffmc_sum ?? "",
  ].map(escapeCsv).join(",");

  const existingRows = await loadExistingRows();
  const dataRows = existingRows.filter((line, index) => index > 0);
  const rowPrefix = `${escapeCsv(date)},`;
  const nextRows = [header, ...dataRows.filter((line) => !line.startsWith(rowPrefix)), rowValue];

  await fs.mkdir(path.dirname(HISTORY_FILE), { recursive: true });
  await fs.writeFile(HISTORY_FILE, `${nextRows.join("\n")}\n`, "utf8");
  console.log(`Updated ${path.relative(process.cwd(), HISTORY_FILE)} for ${date}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
