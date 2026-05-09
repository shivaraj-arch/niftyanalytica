import fs from 'node:fs/promises';
import https from 'node:https';
import path from 'node:path';

const OUTPUT_FILE = path.resolve('data/news-feed.json');
const IST_TIME_ZONE = 'Asia/Kolkata';
const MAX_HEADLINES = 12;
const MAX_HEADLINE_AGE_MS = 48 * 60 * 60 * 1000;

const MARKET_TAPE_SYMBOLS = [
  { label: 'S&P 500', symbol: '^spx' },
  { label: 'Dow Jones', symbol: '^dji' },
  { label: 'Nasdaq', symbol: '^ndq' },
  { label: 'Nikkei 225', symbol: '^nkx' },
  { label: 'Hang Seng', symbol: '^hsi' },
  { label: 'FTSE 100', symbol: '^ukx' },
  { label: 'DAX', symbol: '^dax' },
  { label: 'Crude Oil', symbol: 'cl.f' },
  { label: 'Gold', symbol: 'xauusd' },
];

const RSS_QUERIES = [
  'india stock market OR nifty OR sensex when:1d',
  'site:livemint.com nifty OR stock market when:1d',
  'site:business-standard.com nifty OR stock market when:1d',
  'site:bloomberg.com markets OR economy when:1d',
];

function buildGoogleNewsRssUrl(query) {
  const encodedQuery = encodeURIComponent(query);
  return `https://news.google.com/rss/search?q=${encodedQuery}&hl=en-IN&gl=IN&ceid=IN:en`;
}

function fetchText(url, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: {
        'user-agent': 'Mozilla/5.0',
        accept: 'application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8',
      },
    }, (response) => {
      const { statusCode = 0, headers } = response;

      if (statusCode >= 300 && statusCode < 400 && headers.location) {
        response.resume();
        if (redirectCount >= 5) {
          reject(new Error(`Too many redirects for ${url}`));
          return;
        }
        const nextUrl = headers.location.startsWith('http')
          ? headers.location
          : new URL(headers.location, url).toString();
        fetchText(nextUrl, redirectCount + 1).then(resolve).catch(reject);
        return;
      }

      if (statusCode !== 200) {
        response.resume();
        reject(new Error(`Request failed: ${statusCode} ${url}`));
        return;
      }

      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        resolve(body);
      });
    }).on('error', reject);
  });
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([\da-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16)))
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .trim();
}

function extractTagValue(xml, tagName) {
  const match = String(xml || '').match(new RegExp(`<${tagName}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tagName}>`, 'i'));
  return match ? decodeHtml(match[1]) : '';
}

function normalizeHeadlineKey(value) {
  return decodeHtml(value)
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/\b(livemint|business standard|bloomberg|bloomberg economics)\b/g, '')
    .replace(/\s*[|:-]\s*(live|latest|update|updates)\b.*$/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function formatIstDateLabel(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return '-';
  }

  return `${new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIME_ZONE,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(date)} IST`;
}

function parseRssItems(xml) {
  const itemMatches = String(xml || '').match(/<item>([\s\S]*?)<\/item>/gi) || [];

  return itemMatches.map((itemXml) => {
    const title = extractTagValue(itemXml, 'title');
    const link = extractTagValue(itemXml, 'link');
    const source = extractTagValue(itemXml, 'source') || 'Google News';
    const publishedAtRaw = extractTagValue(itemXml, 'pubDate');
    const publishedAt = new Date(publishedAtRaw);

    if (!title || !link || Number.isNaN(publishedAt.getTime())) {
      return null;
    }

    return {
      source,
      title,
      link,
      publishedAt,
      key: normalizeHeadlineKey(title),
    };
  }).filter(Boolean);
}

async function buildHeadlines() {
  const now = Date.now();
  const sourceErrors = [];
  const seen = new Set();
  const collected = [];

  await Promise.all(RSS_QUERIES.map(async (query) => {
    const feedUrl = buildGoogleNewsRssUrl(query);
    try {
      const xml = await fetchText(feedUrl);
      for (const item of parseRssItems(xml)) {
        if (!item.key || seen.has(item.key)) {
          continue;
        }
        if ((now - item.publishedAt.getTime()) > MAX_HEADLINE_AGE_MS) {
          continue;
        }
        seen.add(item.key);
        collected.push(item);
      }
    } catch (error) {
      sourceErrors.push(error instanceof Error ? error.message : String(error));
    }
  }));

  const items = collected
    .sort((left, right) => right.publishedAt.getTime() - left.publishedAt.getTime())
    .slice(0, MAX_HEADLINES)
    .map((item) => ({
      source: item.source,
      title: item.title,
      link: item.link,
      publishedAt: item.publishedAt.toISOString(),
      publishedAtLabel: formatIstDateLabel(item.publishedAt),
    }));

  return {
    items,
    sourceErrors,
    sourcesAvailable: [...new Set(items.map((item) => item.source))],
  };
}

function parseNumber(value) {
  const numericValue = Number(String(value || '').replace(/,/g, '').trim());
  return Number.isFinite(numericValue) ? numericValue : 0;
}

function parseStooqQuoteLine(rawText, label) {
  const quoteLine = String(rawText || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => /^[^,]+,\d{4}-\d{2}-\d{2},/.test(line));

  if (!quoteLine) {
    throw new Error(`No quote line returned for ${label}.`);
  }

  const parts = quoteLine.split(',');
  if (parts.length < 7 || parts[1] === 'N/D') {
    throw new Error(`Incomplete quote returned for ${label}.`);
  }

  const openPrice = parseNumber(parts[3]);
  const closePrice = parseNumber(parts[6]);
  const changePercent = openPrice ? ((closePrice - openPrice) / openPrice) * 100 : 0;

  return {
    label,
    last: closePrice,
    changePercent,
    source: 'Stooq',
    updatedAt: `${parts[1]} ${parts[2]}`,
  };
}

async function buildMarketTape(existingPayload) {
  const items = [];

  await Promise.all(MARKET_TAPE_SYMBOLS.map(async ({ label, symbol }) => {
    try {
      const csv = await fetchText(`https://stooq.com/q/d/l/?s=${encodeURIComponent(symbol)}&i=d`);
      items.push(parseStooqQuoteLine(csv, label));
    } catch (error) {
      const existingItem = (existingPayload?.items || []).find((item) => item.label === label);
      if (existingItem) {
        items.push(existingItem);
      } else {
        console.warn(error instanceof Error ? error.message : String(error));
      }
    }
  }));

  const sortedItems = MARKET_TAPE_SYMBOLS
    .map(({ label }) => items.find((item) => item.label === label))
    .filter(Boolean);

  return {
    updatedAt: new Date().toISOString(),
    items: sortedItems,
  };
}

async function loadExistingPayload() {
  try {
    const raw = await fs.readFile(OUTPUT_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    return {
      marketTape: { updatedAt: '', items: [] },
      newsFeed: { updatedAt: '', updatedAtLabel: '', items: [], sourcesAvailable: [], sourceErrors: [] },
    };
  }
}

async function main() {
  const existingPayload = await loadExistingPayload();
  const now = new Date();
  const [marketTape, headlinePayload] = await Promise.all([
    buildMarketTape(existingPayload.marketTape),
    buildHeadlines(),
  ]);

  const payload = {
    marketTape,
    newsFeed: {
      updatedAt: now.toISOString(),
      updatedAtLabel: formatIstDateLabel(now),
      items: headlinePayload.items,
      sourcesAvailable: headlinePayload.sourcesAvailable,
      sourceErrors: headlinePayload.sourceErrors,
    },
  };

  await fs.mkdir(path.dirname(OUTPUT_FILE), { recursive: true });
  await fs.writeFile(OUTPUT_FILE, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  console.log(`Updated ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});