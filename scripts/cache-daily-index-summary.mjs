import fs from "node:fs/promises";
import path from "node:path";

const SNAPSHOT_FILE = path.resolve("data/live-snapshot.json");
const OUTPUT_DIR = path.resolve("data/index-cache");
const IST_TIME_ZONE = "Asia/Kolkata";

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
    yearHigh: indexRecord.yearHigh ?? null,
    yearLow: indexRecord.yearLow ?? null,
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
    market: "Capital Market",
    marketStatus: "Closed",
    tradeDate: snapshot.contributors?.timestamp ?? indexRecord.lastUpdateTime ?? null,
    index: snapshot.contributors?.name ?? indexRecord.symbol ?? null,
    last: indexRecord.lastPrice ?? null,
    variation: indexRecord.change ?? null,
    percentChange: indexRecord.pChange ?? null,
    marketStatusMessage: null,
  };
}

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

async function main() {
  const snapshot = JSON.parse(await fs.readFile(SNAPSHOT_FILE, "utf8"));
  const date = process.env.NSE_INDEX_CACHE_DATE || toIstIsoDate(new Date(snapshot.fetchedAt || Date.now()));
  const contributorsMetadata = snapshot.contributors?.metadata ?? buildMetadataFallback(snapshot);
  const marketStatus = snapshot.marketStatus ?? buildMarketStatusFallback(snapshot);

  const payload = {
    date,
    fetchedAt: snapshot.fetchedAt ?? null,
    contributorsMetadata,
    marketStatus,
  };

  if (!payload.contributorsMetadata || !payload.marketStatus) {
    throw new Error("Missing contributors metadata or market status in live snapshot.");
  }

  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  const outputFile = path.join(OUTPUT_DIR, `${date}.json`);
  await fs.writeFile(outputFile, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  console.log(`Updated ${path.relative(process.cwd(), outputFile)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});