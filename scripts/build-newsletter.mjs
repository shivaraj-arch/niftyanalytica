import fs from "node:fs/promises";
import path from "node:path";

const LIVE_SNAPSHOT_FILE = path.resolve(process.env.NEWSLETTER_SNAPSHOT_FILE || "data/live-snapshot.json");
const MARKET_ACTIVITY_HISTORY_FILE = path.resolve(process.env.NEWSLETTER_MARKET_ACTIVITY_FILE || "data/market-activity-history.csv");
const INDEX_SUMMARY_HISTORY_FILE = path.resolve(process.env.NEWSLETTER_INDEX_HISTORY_FILE || "data/index-summary-history.csv");
const AI_ANALYSIS_FILE = path.resolve(process.env.NEWSLETTER_AI_ANALYSIS_FILE || "data/ai-analysis.json");
const OUTPUT_FILE = path.resolve(process.env.NEWSLETTER_OUTPUT_FILE || "tmp/newsletter/latest.json");
const WEBSITE_URL = String(process.env.NEWSLETTER_WEBSITE_URL || "https://www.niftyanalytica.com").replace(/\/$/, "");

const numberFormatter = new Intl.NumberFormat("en-IN", {
  maximumFractionDigits: 2,
});

function parseNumber(value) {
  const parsed = Number.parseFloat(String(value ?? "").replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function signed(value, digits = 2) {
  const amount = parseNumber(value);
  const formatted = numberFormatter.format(Number(amount.toFixed(digits)));
  if (amount > 0) return `+${formatted}`;
  if (amount < 0) return `-${numberFormatter.format(Number(Math.abs(amount).toFixed(digits)))}`;
  return formatted;
}

function formatNumber(value, digits = 2) {
  return numberFormatter.format(Number(parseNumber(value).toFixed(digits)));
}

function formatPercent(value, digits = 2) {
  return `${signed(value, digits)}%`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateLabel(value) {
  if (!value) return "-";
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [year, month, day] = value.split("-");
    return `${day}-${month}-${year}`;
  }
  return String(value);
}

function monthName(value) {
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return monthNames[Number(value) - 1] || value;
}

function parseCsvLine(line) {
  const cells = [];
  let current = "";
  let quoted = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      if (quoted && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        quoted = !quoted;
      }
      continue;
    }

    if (character === "," && !quoted) {
      cells.push(current);
      current = "";
      continue;
    }

    current += character;
  }

  cells.push(current);
  return cells;
}

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function readCsvRows(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];
  const header = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    return header.reduce((row, key, index) => {
      row[key] = cells[index] ?? "";
      return row;
    }, {});
  });
}

function normalizeFiiDii(payload) {
  const rows = Array.isArray(payload) ? payload : payload?.data || [];
  const fii = rows.find((item) => item.category === "FII/FPI") || {};
  const dii = rows.find((item) => item.category === "DII") || {};
  return {
    date: fii.date || dii.date || "",
    fiiNet: parseNumber(fii.netValue),
    diiNet: parseNumber(dii.netValue),
  };
}

function getFirstExpiry(records) {
  const item = Array.isArray(records) ? records.find((row) => row.expiryDates) : null;
  return item?.expiryDates || "";
}

function formatExpiryLabel(value) {
  if (!value) return "";
  if (/^\d{2}-[A-Za-z]{3}-\d{4}$/.test(value)) return value;
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [year, month, day] = value.split("-");
    return `${day}-${monthName(month)}-${year}`;
  }
  if (/^\d{2}-\d{2}-\d{4}$/.test(value)) {
    const [day, month, year] = value.split("-");
    return `${day}-${monthName(month)}-${year}`;
  }
  return String(value);
}

function extractOpenInterest(payload) {
  const records = payload?.records || {};
  const data = Array.isArray(records.data) ? records.data : [];
  const spot = parseNumber(records.underlyingValue);
  const atm = Math.round(spot / 50) * 50;
  const strikes = Array.from({ length: 13 }, (_, index) => atm - 300 + index * 50);
  const expiry = formatExpiryLabel(getFirstExpiry(data));

  const normalizedStrikes = strikes.map((strikePrice) => {
    const ce = (data.find((item) => item.CE?.strikePrice === strikePrice) || {}).CE || {};
    const pe = (data.find((item) => item.PE?.strikePrice === strikePrice) || {}).PE || {};

    return {
      strikePrice,
      callOI: parseNumber(ce.openInterest),
      callOIChange: parseNumber(ce.changeinOpenInterest),
      callBidAsk: parseNumber(ce.totalBuyQuantity) - parseNumber(ce.totalSellQuantity),
      callIV: parseNumber(ce.impliedVolatility),
      putOI: parseNumber(pe.openInterest),
      putOIChange: parseNumber(pe.changeinOpenInterest),
      putBidAsk: parseNumber(pe.totalBuyQuantity) - parseNumber(pe.totalSellQuantity),
      putIV: parseNumber(pe.impliedVolatility),
    };
  });

  return {
    timestamp: records.timestamp || "",
    spot,
    expiry,
    strikes: normalizedStrikes,
  };
}

function buildPositioningChanges(strikes) {
  const totals = strikes.reduce((accumulator, strikeRow) => {
    const netOIChange = strikeRow.putOIChange - strikeRow.callOIChange;
    const netFlow = strikeRow.putBidAsk - strikeRow.callBidAsk;
    const ivSkew = strikeRow.putIV - strikeRow.callIV;

    return {
      netPositioningChange: accumulator.netPositioningChange + netOIChange,
      flowImbalance: accumulator.flowImbalance + netFlow,
      ivSkewTotal: accumulator.ivSkewTotal + ivSkew,
    };
  }, {
    netPositioningChange: 0,
    flowImbalance: 0,
    ivSkewTotal: 0,
  });

  const strongestShift = strikes.reduce((best, current) => {
    const currentNetChange = current.putOIChange - current.callOIChange;
    if (!best) {
      return { strikePrice: current.strikePrice, netChange: currentNetChange };
    }
    return Math.abs(currentNetChange) > Math.abs(best.netChange)
      ? { strikePrice: current.strikePrice, netChange: currentNetChange }
      : best;
  }, null);

  const averageIvSkew = strikes.length ? totals.ivSkewTotal / strikes.length : 0;
  const directionLabel = totals.netPositioningChange > 0 && totals.flowImbalance > 0
    ? "Bullish positioning build"
    : totals.netPositioningChange < 0 && totals.flowImbalance < 0
      ? "Bearish positioning build"
      : totals.netPositioningChange > 0
        ? "Put-side hedge build"
        : totals.netPositioningChange < 0
          ? "Call-side hedge build"
          : "Balanced positioning";

  return {
    netPositioningChange: totals.netPositioningChange,
    flowImbalance: totals.flowImbalance,
    ivSkew: averageIvSkew,
    directionLabel,
    strongestShiftLabel: strongestShift ? `${strongestShift.strikePrice} (${signed(strongestShift.netChange)})` : "-",
    note: strongestShift
      ? `${directionLabel} led by ${strongestShift.strikePrice}, with net OI shift ${signed(totals.netPositioningChange)}, flow imbalance ${signed(totals.flowImbalance)}, and IV skew ${signed(averageIvSkew)}%.`
      : "Positioning changes are waiting for the next option-chain refresh.",
  };
}

function signalDirection(value) {
  return value > 0 ? 1 : value < 0 ? -1 : 0;
}

function buildOpenInterestSignals({ openInterest, advances, declines, fiiDii, percentChange }) {
  const priceDirection = signalDirection(percentChange);
  const positioningDirection = signalDirection(openInterest.positioning.netPositioningChange);
  const breadthDirection = signalDirection((advances || 0) - (declines || 0));
  const institutionalDirection = signalDirection((fiiDii.fiiNet || 0) + (fiiDii.diiNet || 0));
  const divergences = [];

  if (priceDirection > 0 && positioningDirection < 0) divergences.push("Price up vs defensive options");
  if (priceDirection < 0 && positioningDirection > 0) divergences.push("Price down vs supportive options");
  if (priceDirection > 0 && breadthDirection < 0) divergences.push("Price up vs weak breadth");
  if (priceDirection < 0 && breadthDirection > 0) divergences.push("Price down vs resilient breadth");
  if (priceDirection > 0 && institutionalDirection < 0) divergences.push("Price up vs net negative institutions");
  if (priceDirection < 0 && institutionalDirection > 0) divergences.push("Price down vs supportive institutions");

  const nearSpotRows = (openInterest.strikes || []).filter((row) => Math.abs(row.strikePrice - openInterest.spot) <= 150);
  const trapCandidates = nearSpotRows.flatMap((row) => {
    const upsideScore = row.strikePrice >= openInterest.spot && row.callOIChange > row.putOIChange && row.callBidAsk < 0
      ? row.callOIChange + Math.abs(row.callBidAsk)
      : 0;
    const downsideScore = row.strikePrice <= openInterest.spot && row.putOIChange > row.callOIChange && row.putBidAsk < 0
      ? row.putOIChange + Math.abs(row.putBidAsk)
      : 0;

    return [
      upsideScore > 0 ? { label: `Upside trap @ ${row.strikePrice}`, score: upsideScore } : null,
      downsideScore > 0 ? { label: `Downside trap @ ${row.strikePrice}`, score: downsideScore } : null,
    ].filter(Boolean);
  });

  const topTrap = trapCandidates.reduce((best, current) => {
    if (!best) return current;
    return current.score > best.score ? current : best;
  }, null);

  const divergenceSummary = divergences.length ? divergences[0] : "Aligned sentiment";
  const noteParts = [divergenceSummary];
  if (divergences.length > 1) noteParts.push(`${divergences.length} divergences active`);
  if (topTrap) noteParts.push(`${topTrap.label} near spot`);

  return {
    divergenceScore: divergences.length,
    divergenceSummary,
    trapRiskLabel: topTrap?.label || "No clear trap",
    note: `${noteParts.join(". ")}.`,
  };
}

function trimParagraphs(value, maxParagraphs = 2) {
  const paragraphs = String(value || "")
    .split(/\n\s*\n/)
    .map((paragraph) => paragraph.replace(/\s+/g, " ").trim())
    .filter(Boolean);
  return paragraphs.slice(0, maxParagraphs);
}

function cleanAiParagraph(paragraph) {
  return String(paragraph || "")
    .replace(/^#+\s*/g, "")
    .replace(/^[*-]\s+/g, "")
    .replace(/\*\*/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractAiBriefParagraphs(summary) {
  const source = String(summary || "");
  const preferred = source.includes("Aggregator Agent")
    ? source.split("Aggregator Agent").pop()
    : source;

  return trimParagraphs(preferred, 3)
    .map(cleanAiParagraph)
    .filter((paragraph) => paragraph && !/agent$/i.test(paragraph));
}

function buildPredictions({ bias, indexSummary, positioning, signals, fiiDii }) {
  const predictions = [];
  const institutionalNet = (fiiDii.fiiNet || 0) + (fiiDii.diiNet || 0);

  if (signals.divergenceScore >= 2) {
    predictions.push("Expect a more two-sided opening move because price, breadth, or options positioning are not fully aligned.");
  }

  if (positioning.netPositioningChange > 0 && institutionalNet >= 0) {
    predictions.push("Dips may find support first, provided overnight global cues do not deteriorate sharply.");
  } else if (positioning.netPositioningChange < 0 && institutionalNet < 0) {
    predictions.push("Rallies are more likely to meet supply early, so strength may need confirmation before follow-through.");
  }

  if (/bearish/i.test(String(bias || "")) || indexSummary.percentChange < 0) {
    predictions.push("Risk management should stay tighter than usual because the current setup still favors selective selling on bounce.");
  } else if (/bullish/i.test(String(bias || "")) || indexSummary.percentChange > 0) {
    predictions.push("If the index holds the early range, momentum names can stay in focus for follow-up buying.");
  }

  if (signals.trapRiskLabel !== "No clear trap") {
    predictions.push(`Watch ${signals.trapRiskLabel.toLowerCase()} closely, as trapped positioning can amplify intraday reversals.`);
  }

  if (predictions.length < 3) {
    predictions.push("Stock-specific action should stay more important than chasing a broad one-way index move in the first hour.");
  }

  return predictions.slice(0, 4);
}

function buildMetricCards(summaryMetrics) {
  return summaryMetrics.map((metric) => `
    <div style="background:#f7fafc;border:1px solid #d7e3eb;border-radius:14px;padding:14px 16px;min-width:160px;flex:1 1 180px;">
      <div style="font-size:12px;letter-spacing:0.04em;text-transform:uppercase;color:#4f6b79;">${escapeHtml(metric.label)}</div>
      <div style="margin-top:6px;font-size:22px;font-weight:700;color:#0f2430;">${escapeHtml(metric.value)}</div>
    </div>
  `).join("");
}

async function main() {
  const [snapshot, marketHistoryRows, indexHistoryRows, aiAnalysis] = await Promise.all([
    readJson(LIVE_SNAPSHOT_FILE),
    readCsvRows(MARKET_ACTIVITY_HISTORY_FILE),
    readCsvRows(INDEX_SUMMARY_HISTORY_FILE),
    readJson(AI_ANALYSIS_FILE).catch(() => null),
  ]);

  const latestMarketActivity = marketHistoryRows[marketHistoryRows.length - 1] || {};
  const previousMarketActivity = marketHistoryRows[marketHistoryRows.length - 2] || {};
  const latestIndexHistory = indexHistoryRows[indexHistoryRows.length - 1] || {};

  const metadata = snapshot?.contributors?.metadata || {};
  const firstContributor = Array.isArray(snapshot?.contributors?.data) ? snapshot.contributors.data[0] : null;
  const advances = parseNumber(snapshot?.contributors?.advance?.advances);
  const declines = parseNumber(snapshot?.contributors?.advance?.declines);
  const fiiDii = normalizeFiiDii(snapshot?.fiiDii);
  const openInterest = extractOpenInterest(snapshot?.optionChain || {});
  openInterest.positioning = buildPositioningChanges(openInterest.strikes);

  const indexSummary = {
    last: parseNumber(metadata.last || firstContributor?.lastPrice),
    open: parseNumber(metadata.open || firstContributor?.open),
    high: parseNumber(metadata.high || firstContributor?.dayHigh),
    low: parseNumber(metadata.low || firstContributor?.dayLow),
    previousClose: parseNumber(metadata.previousClose || firstContributor?.previousClose),
    change: parseNumber(metadata.change || firstContributor?.change),
    percentChange: parseNumber(metadata.percChange || firstContributor?.pChange),
    totalTradedValue: parseNumber(metadata.totalTradedValue || firstContributor?.totalTradedValue),
    totalTradedVolume: parseNumber(metadata.totalTradedVolume || firstContributor?.totalTradedVolume),
    marketStatusLabel: String(snapshot?.marketStatus?.marketStatus || "Closed"),
    tradeDate: snapshot?.marketStatus?.tradeDate || metadata.timeVal || snapshot?.contributors?.timestamp || latestIndexHistory.tradeDate || "",
  };

  const signals = buildOpenInterestSignals({
    openInterest,
    advances,
    declines,
    fiiDii,
    percentChange: indexSummary.percentChange,
  });

  const marketCapToday = parseNumber(latestMarketActivity.totalMarketCapitalisationCrores || latestIndexHistory.ffmcSum || 0);
  const marketCapPrev = parseNumber(previousMarketActivity.totalMarketCapitalisationCrores || 0);
  const tradedValueToday = parseNumber(latestMarketActivity.tradedValueCrores || indexSummary.totalTradedValue / 10000000);
  const tradedValuePrev = parseNumber(previousMarketActivity.tradedValueCrores || 0);
  const ffmcToday = parseNumber(latestMarketActivity.ffmcCrores || (metadata.ffmc_sum || 0) / 100);
  const ffmcPrev = parseNumber(previousMarketActivity.ffmcCrores || 0);
  const aiParagraphs = extractAiBriefParagraphs(aiAnalysis?.summary);
  const bias = String(aiAnalysis?.bias || "Neutral");
  const predictions = buildPredictions({ bias, indexSummary, positioning: openInterest.positioning, signals, fiiDii });
  const summaryMetrics = [
    { label: "Nifty Close", value: formatNumber(indexSummary.last) },
    { label: "Day Change", value: formatPercent(indexSummary.percentChange) },
    { label: "Market Cap", value: `${formatNumber(marketCapToday)} Cr` },
    { label: "Traded Value", value: `${formatNumber(tradedValueToday)} Cr` },
    { label: "FFMC", value: `${formatNumber(ffmcToday)} Cr` },
    { label: "FII / DII Net", value: `${signed(fiiDii.fiiNet)} / ${signed(fiiDii.diiNet)} Cr` },
  ];

  const headline = indexSummary.percentChange >= 0
    ? `Nifty closed higher with ${openInterest.positioning.directionLabel.toLowerCase()}.`
    : `Nifty ended under pressure while ${openInterest.positioning.directionLabel.toLowerCase()} stayed in focus.`;

  const hasPreviousMarketActivity = Boolean(previousMarketActivity.date);
  const marketActivityNote = hasPreviousMarketActivity
    ? `Market cap moved ${signed(marketCapToday - marketCapPrev)} Cr day over day, traded value moved ${signed(tradedValueToday - tradedValuePrev)} Cr, and FFMC moved ${signed(ffmcToday - ffmcPrev)} Cr.`
    : `Archived close metrics now include market cap ${formatNumber(marketCapToday)} Cr, traded value ${formatNumber(tradedValueToday)} Cr, and FFMC ${formatNumber(ffmcToday)} Cr.`;
  const note = [
    `Breadth closed at ${advances} advances versus ${declines} declines.`,
    marketActivityNote,
    openInterest.positioning.note,
    signals.note,
  ].join(" ");

  const subjectDate = formatDateLabel(latestMarketActivity.date || latestIndexHistory.date || "");
  const subject = `Nifty Analytica AI Brief | ${subjectDate} | Market cap, flows and option signals`;
  const previewText = `${headline} Divergence score ${signals.divergenceScore}, ${signals.trapRiskLabel.toLowerCase()}, and a client-ready view for the next session.`;
  const unsubscribeUrl = `${WEBSITE_URL}/unsubscribe/?email={{EMAIL_ENCODED}}`;

  const textLines = [
    `Nifty Analytica AI Brief | ${subjectDate}`,
    "",
    headline,
    "",
    `Nifty close: ${formatNumber(indexSummary.last)} (${formatPercent(indexSummary.percentChange)})`,
    `Day range: ${formatNumber(indexSummary.low)} - ${formatNumber(indexSummary.high)}`,
    `Breadth: ${advances} advances / ${declines} declines`,
    `Market cap: ${formatNumber(marketCapToday)} Cr`,
    `Traded value: ${formatNumber(tradedValueToday)} Cr`,
    `FFMC: ${formatNumber(ffmcToday)} Cr`,
    `FII net: ${signed(fiiDii.fiiNet)} Cr`,
    `DII net: ${signed(fiiDii.diiNet)} Cr`,
    "",
    "Option setup",
    `Net positioning change: ${signed(openInterest.positioning.netPositioningChange)}`,
    `Flow imbalance: ${signed(openInterest.positioning.flowImbalance)}`,
    `IV skew: ${signed(openInterest.positioning.ivSkew)}%`,
    `Strongest shift: ${openInterest.positioning.strongestShiftLabel}`,
    `Divergence score: ${signals.divergenceScore}`,
    `Trap risk: ${signals.trapRiskLabel}`,
    "",
    "Client note",
    note,
    "",
    "Tomorrow setup",
    ...predictions.map((item, index) => `${index + 1}. ${item}`),
    "",
    ...(aiParagraphs.length ? ["AI brief", ...aiParagraphs, ""] : []),
    `Dashboard: ${WEBSITE_URL}`,
    `Unsubscribe: ${unsubscribeUrl}`,
  ];

  const html = `<!DOCTYPE html>
<html lang="en">
  <body style="margin:0;padding:0;background:#edf4f7;font-family:Arial,sans-serif;color:#16313d;">
    <div style="max-width:760px;margin:0 auto;padding:24px 16px;">
      <div style="background:#0f4c5c;color:#f4fbfd;border-radius:20px;padding:28px 28px 24px;box-shadow:0 18px 48px rgba(15,76,92,0.18);">
        <div style="font-size:12px;letter-spacing:0.1em;text-transform:uppercase;opacity:0.8;">Nifty Analytica AI Brief</div>
        <h1 style="margin:10px 0 10px;font-size:30px;line-height:1.2;">${escapeHtml(headline)}</h1>
        <p style="margin:0;font-size:15px;line-height:1.7;color:#d6edf4;">${escapeHtml(previewText)}</p>
      </div>

      <div style="display:flex;flex-wrap:wrap;gap:12px;margin-top:18px;">
        ${buildMetricCards(summaryMetrics)}
      </div>

      <div style="background:#ffffff;border:1px solid #d7e3eb;border-radius:20px;padding:24px 24px 10px;margin-top:18px;box-shadow:0 14px 30px rgba(15,76,92,0.08);">
        <h2 style="margin:0 0 14px;font-size:22px;color:#0f2430;">What mattered today</h2>
        <p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#284452;">${escapeHtml(note)}</p>
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:14px;">
          <div style="background:#f7fafc;border-radius:14px;padding:14px 16px;">
            <div style="font-size:12px;text-transform:uppercase;color:#4f6b79;">Positioning</div>
            <div style="margin-top:6px;font-size:18px;font-weight:700;color:#0f2430;">${escapeHtml(openInterest.positioning.directionLabel)}</div>
            <div style="margin-top:6px;font-size:14px;color:#4f6b79;">Strongest shift: ${escapeHtml(openInterest.positioning.strongestShiftLabel)}</div>
          </div>
          <div style="background:#f7fafc;border-radius:14px;padding:14px 16px;">
            <div style="font-size:12px;text-transform:uppercase;color:#4f6b79;">Divergence</div>
            <div style="margin-top:6px;font-size:18px;font-weight:700;color:#0f2430;">Score ${escapeHtml(String(signals.divergenceScore))}</div>
            <div style="margin-top:6px;font-size:14px;color:#4f6b79;">${escapeHtml(signals.divergenceSummary)}</div>
          </div>
          <div style="background:#f7fafc;border-radius:14px;padding:14px 16px;">
            <div style="font-size:12px;text-transform:uppercase;color:#4f6b79;">Trap Risk</div>
            <div style="margin-top:6px;font-size:18px;font-weight:700;color:#0f2430;">${escapeHtml(signals.trapRiskLabel)}</div>
            <div style="margin-top:6px;font-size:14px;color:#4f6b79;">Spot ${escapeHtml(formatNumber(openInterest.spot))} | Expiry ${escapeHtml(openInterest.expiry || "-")}</div>
          </div>
        </div>
      </div>

      <div style="background:#ffffff;border:1px solid #d7e3eb;border-radius:20px;padding:24px;margin-top:18px;box-shadow:0 14px 30px rgba(15,76,92,0.08);">
        <h2 style="margin:0 0 14px;font-size:22px;color:#0f2430;">Tomorrow setup</h2>
        <ol style="margin:0;padding-left:22px;color:#284452;line-height:1.8;">
          ${predictions.map((item) => `<li style="margin-bottom:8px;">${escapeHtml(item)}</li>`).join("")}
        </ol>
      </div>

      ${aiParagraphs.length ? `
      <div style="background:#ffffff;border:1px solid #d7e3eb;border-radius:20px;padding:24px;margin-top:18px;box-shadow:0 14px 30px rgba(15,76,92,0.08);">
        <h2 style="margin:0 0 14px;font-size:22px;color:#0f2430;">AI desk summary</h2>
        ${aiParagraphs.map((paragraph) => `<p style="margin:0 0 12px;font-size:15px;line-height:1.8;color:#284452;">${escapeHtml(paragraph)}</p>`).join("")}
      </div>` : ""}

      <div style="padding:20px 4px 12px;font-size:13px;line-height:1.8;color:#4f6b79;">
        <div>Dashboard: <a href="${escapeHtml(WEBSITE_URL)}" style="color:#0f4c5c;">${escapeHtml(WEBSITE_URL)}</a></div>
        <div>Trade date: ${escapeHtml(latestMarketActivity.date || subjectDate)}</div>
        <div>This brief is for market context and client communication only, not a promise of returns.</div>
        <div>Unsubscribe: <a href="${escapeHtml(unsubscribeUrl)}" style="color:#0f4c5c;">manage email preferences</a></div>
      </div>
    </div>
  </body>
</html>`;

  const output = {
    generatedAt: new Date().toISOString(),
    subject,
    previewText,
    headline,
    bias,
    meta: {
      tradeDate: latestMarketActivity.date || formatDateLabel(latestIndexHistory.date || "") || subjectDate,
      marketActivityDate: latestMarketActivity.date || "",
      snapshotTimestamp: snapshot?.fetchedAt || "",
    },
    metrics: {
      niftyClose: indexSummary.last,
      percentChange: indexSummary.percentChange,
      marketCapCrores: marketCapToday,
      tradedValueCrores: tradedValueToday,
      ffmcCrores: ffmcToday,
      fiiNetCrores: fiiDii.fiiNet,
      diiNetCrores: fiiDii.diiNet,
      divergenceScore: signals.divergenceScore,
      trapRiskLabel: signals.trapRiskLabel,
      strongestShiftLabel: openInterest.positioning.strongestShiftLabel,
    },
    note,
    predictions,
    text: textLines.join("\n"),
    html,
  };

  await fs.mkdir(path.dirname(OUTPUT_FILE), { recursive: true });
  await fs.writeFile(OUTPUT_FILE, `${JSON.stringify(output, null, 2)}\n`, "utf8");
  console.log(`Built newsletter brief at ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});