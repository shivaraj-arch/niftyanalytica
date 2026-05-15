import fs from "node:fs/promises";
import https from "node:https";
import path from "node:path";

const HISTORY_FILE = path.resolve("data/market-activity-history.csv");
const LIVE_SNAPSHOT_FILE = path.resolve("data/live-snapshot.json");
const IST_TIME_ZONE = "Asia/Kolkata";
const METRIC_LABELS = {
  "Traded Value (Rs. In Crores)": "tradedValueCrores",
  "Traded Quantity (in Lakhs)": "tradedQuantityLakhs",
  "Number of Trades": "numberOfTrades",
  "Total Market Capitalisation (Rs. Crores)": "totalMarketCapitalisationCrores",
};

function toIstParts(date = new Date()) {
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

  return {
    isoDate: `${parts.year}-${parts.month}-${parts.day}`,
    compactDate: `${parts.day}${parts.month}${String(parts.year).slice(-2)}`,
  };
}

function resolveArchiveDate(date = new Date()) {
  const candidate = new Date(date);
  while ([0, 6].includes(candidate.getUTCDay())) {
    candidate.setUTCDate(candidate.getUTCDate() - 1);
  }
  return toIstParts(candidate).compactDate;
}

function compactDateFromIsoDate(isoDate) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(isoDate ?? "").trim());
  if (!match) {
    throw new Error(`Invalid NSE_TARGET_DATE: ${isoDate}. Expected YYYY-MM-DD.`);
  }

  const [, year, month, day] = match;
  return `${day}${month}${year.slice(-2)}`;
}

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "*/*",
      },
    }, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Request failed: ${response.statusCode} ${url}`));
        response.resume();
        return;
      }

      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () => {
        resolve({
          body,
          lastModified: response.headers["last-modified"] ?? "",
        });
      });
    }).on("error", reject);
  });
}

function parseArchiveCsv(body, archiveFile, lastModified) {
  const lines = body.split(/\r?\n/).filter(Boolean);
  if (lines.length < 6) {
    throw new Error(`Unexpected archive CSV format for ${archiveFile}`);
  }

  const row = {
    date: lines[0].replace(/^,/, "").trim(),
    archiveFile,
    lastModified,
    summary: lines[1].replace(/^,/, "").trim(),
    tradedValueCrores: "",
    tradedQuantityLakhs: "",
    numberOfTrades: "",
    totalMarketCapitalisationCrores: "",
  };

  for (const line of lines.slice(2, 6)) {
    const cells = line.split(",").map((cell) => cell.trim()).filter(Boolean);
    const label = cells[0];
    const value = cells[1] ?? "";
    const key = METRIC_LABELS[label];
    if (key) {
      row[key] = value;
    }
  }

  return row;
}

function parseNumber(value) {
  const parsed = Number.parseFloat(String(value ?? "").replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

async function loadLiveSnapshotMetrics() {
  try {
    const raw = await fs.readFile(LIVE_SNAPSHOT_FILE, "utf8");
    const payload = JSON.parse(raw);
    const metadata = payload?.contributors?.metadata || {};
    const fiiDiiRows = Array.isArray(payload?.fiiDii) ? payload.fiiDii : [];
    const fii = fiiDiiRows.find((row) => row.category === "FII/FPI") || {};
    const dii = fiiDiiRows.find((row) => row.category === "DII") || {};

    return {
      ffmcCrores: parseNumber(metadata.ffmc_sum) / 100,
      fiiDiiDate: fii.date || dii.date || "",
      fiiNetCrores: parseNumber(fii.netValue),
      diiNetCrores: parseNumber(dii.netValue),
    };
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return {
        ffmcCrores: 0,
        fiiDiiDate: "",
        fiiNetCrores: 0,
        diiNetCrores: 0,
      };
    }

    throw error;
  }
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
  const archiveDate =
    process.env.NSE_ARCHIVE_DATE
    || (process.env.NSE_TARGET_DATE ? compactDateFromIsoDate(process.env.NSE_TARGET_DATE) : null)
    || resolveArchiveDate();
  const archiveFile = `MA${archiveDate}.csv`;
  const archiveUrl = `https://nsearchives.nseindia.com/archives/equities/mkt/${archiveFile}`;

  const { body, lastModified } = await fetchText(archiveUrl);
  const parsed = parseArchiveCsv(body, archiveFile, lastModified);
  const snapshotMetrics = await loadLiveSnapshotMetrics();
  parsed.ffmcCrores = snapshotMetrics.ffmcCrores;
  parsed.fiiDiiDate = snapshotMetrics.fiiDiiDate;
  parsed.fiiNetCrores = snapshotMetrics.fiiNetCrores;
  parsed.diiNetCrores = snapshotMetrics.diiNetCrores;

  const header = [
    "date",
    "archiveFile",
    "lastModified",
    "summary",
    "tradedValueCrores",
    "tradedQuantityLakhs",
    "numberOfTrades",
    "totalMarketCapitalisationCrores",
    "ffmcCrores",
    "fiiDiiDate",
    "fiiNetCrores",
    "diiNetCrores",
  ].join(",");

  const rowValue = [
    parsed.date,
    parsed.archiveFile,
    parsed.lastModified,
    parsed.summary,
    parsed.tradedValueCrores,
    parsed.tradedQuantityLakhs,
    parsed.numberOfTrades,
    parsed.totalMarketCapitalisationCrores,
    parsed.ffmcCrores,
    parsed.fiiDiiDate,
    parsed.fiiNetCrores,
    parsed.diiNetCrores,
  ].map(escapeCsv).join(",");

  const existingRows = await loadExistingRows();
  const dataRows = existingRows.filter((line, index) => index > 0);
  const rowPrefix = `${escapeCsv(parsed.date)},`;
  const nextRows = [header, ...dataRows.filter((line) => !line.startsWith(rowPrefix)), rowValue];

  await fs.mkdir(path.dirname(HISTORY_FILE), { recursive: true });
  await fs.writeFile(HISTORY_FILE, `${nextRows.join("\n")}\n`, "utf8");
  console.log(`Updated ${path.relative(process.cwd(), HISTORY_FILE)} with ${archiveFile}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});