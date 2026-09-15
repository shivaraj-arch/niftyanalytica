import https from 'node:https';

const IST_TIME_ZONE = 'Asia/Kolkata';
const MAX_HEADLINES = 12;
const MAX_HEADLINE_AGE_MS = 48 * 60 * 60 * 1000;
const SGX_NIFTY_URL = 'https://sgxnifty.org/';

// sgxnifty.org carries no ADRs and no crypto, so these come from CNBC's public
// quote endpoint. Indian ADRs are the useful half: they price HDFC Bank,
// Infosys and ICICI through the US session, which is a direct overnight read on
// tomorrow's heavyweights rather than a foreign cue. On 14-Sep INFY closed
// +4.79% and WIT +5.33% against a red US tape; NIFTY IT opened +2.94%.
const CNBC_QUOTE_URL = 'https://quote.cnbc.com/quote-html-webservice/restQuote/symbolType/symbol';
const ADR_SYMBOLS = [
  ['INFY', 'Infosys'],
  ['WIT', 'Wipro'],
  ['HDB', 'HDFC Bank'],
  ['IBN', 'ICICI Bank'],
  ['RDY', "Dr Reddy's"],
  ['MMYT', 'MakeMyTrip'],
];
// Bitcoin rides in the world ticker next to Gold/Silver rather than forming a
// category of one — it reads as a risk-appetite gauge, not a separate asset class.
const CRYPTO_SYMBOLS = [
  ['BTC.CM=', 'Bitcoin'],
];

const RSS_QUERIES = [
  'india stock market OR nifty OR sensex when:1d',
  'site:livemint.com nifty OR stock market when:1d',
  'site:business-standard.com nifty OR stock market when:1d',
  'site:bloomberg.com markets OR economy when:1d',
];

function buildGoogleNewsRssUrl(query) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-IN&gl=IN&ceid=IN:en`;
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
    .replace(/&nbsp;/g, ' ')
    .trim();
}

function stripHtml(value) {
  return decodeHtml(String(value || '').replace(/<[^>]+>/g, ' '))
    .replace(/\s+/g, ' ')
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

export function formatIstDateLabel(date) {
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

function parseNumber(value) {
  const numericValue = Number(String(value || '').replace(/,/g, '').trim());
  return Number.isFinite(numericValue) ? numericValue : 0;
}

function extractInternationalSection(html) {
  const start = html.indexOf('<h2 class="widgettitle">International</h2>');
  if (start === -1) {
    throw new Error('International widget not found on SGX Nifty page.');
  }

  const endCandidates = [
    html.indexOf('<div id="recent-posts-widget-with-thumbnails', start),
    html.indexOf('<div class="widget recent-posts-widget-with-thumbnails', start),
    html.indexOf('<h2 class="widgettitle">Latest News</h2>', start),
  ].filter((value) => value > start);
  const end = endCandidates.length ? Math.min(...endCandidates) : html.length;
  return html.slice(start, end);
}

function parseInternationalMarketTape(html) {
  const section = extractInternationalSection(html);
  const tables = [...section.matchAll(/<table class="index-table">([\s\S]*?)<\/table>/g)];
  const items = [];

  for (const tableMatch of tables) {
    const tableHtml = tableMatch[1] || '';
    const category = stripHtml(extractTagValue(tableHtml, 'th')) || 'International';
    const rowMatches = [...tableHtml.matchAll(/<tr class="index-line">([\s\S]*?)<\/tr>/g)];

    for (const rowMatch of rowMatches) {
      const rowHtml = rowMatch[1] || '';
      const cells = [...rowHtml.matchAll(/<td class="([^"]+)">([\s\S]*?)<\/td>/g)];
      const cellMap = new Map(cells.map((cell) => [cell[1], cell[2]]));
      const rawName = stripHtml(cellMap.get('index-name left-align') || cellMap.get('index-name') || '');
      const last = parseNumber(stripHtml(cellMap.get('index-price') || ''));
      const changePercent = parseNumber(stripHtml(cellMap.get('index-percent') || '').replace(/%/g, ''));
      const clockCell = rowHtml.match(/<td class="index-clock\s+([^"]+)"[^>]*>[\s\S]*?title="([^"]+)"/i);
      const status = stripHtml(clockCell?.[2] || clockCell?.[1] || '').replace(/[^A-Za-z]/g, '') || 'Unknown';

      if (!rawName || !Number.isFinite(last) || last <= 0) {
        continue;
      }

      items.push({
        label: `${rawName} (${status})`,
        last,
        changePercent,
        source: `SGX Nifty / ${category}`,
        updatedAt: new Date().toISOString(),
      });
    }
  }

  return items;
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

function parseCnbcQuotes(rawJson, specs, category) {
  const payload = JSON.parse(rawJson);
  const quotes = payload?.FormattedQuoteResult?.FormattedQuote || [];
  const names = new Map(specs);
  const items = [];

  for (const quote of quotes) {
    const symbol = String(quote?.symbol || '');
    const last = parseNumber(String(quote?.last || '').replace(/,/g, ''));
    if (!symbol || !Number.isFinite(last) || last <= 0) {
      continue;
    }
    const changePercent = parseNumber(String(quote?.change_pct || '').replace(/[%+]/g, ''));
    // Extended hours runs to 01:30 IST, so for a 09:00 brief it is the freshest
    // read available — carried separately rather than folded into the close.
    const extendedRaw = quote?.ExtendedMktQuote?.change_pct;
    const extended = extendedRaw ? parseNumber(String(extendedRaw).replace(/[%+]/g, '')) : null;
    const label = names.get(symbol) || quote?.shortName || symbol;
    const status = String(quote?.curmktstatus || '').includes('REG_MKT') ? 'Open' : 'Close';

    items.push({
      label: `${label} ${symbol} (${status})`,
      last,
      changePercent,
      ...(Number.isFinite(extended) ? { extendedChangePercent: extended } : {}),
      source: `CNBC / ${category}`,
      updatedAt: new Date().toISOString(),
    });
  }

  return items;
}

async function fetchCnbcTape(specs, category) {
  // Symbols go in the `symbols` query param, pipe-separated and NOT encoded —
  // putting them in the path returns 404.
  const symbols = specs.map(([symbol]) => symbol).join('|');
  const url = `${CNBC_QUOTE_URL}?symbols=${symbols}`
    + '&requestMethod=itv&noform=1&partnerId=2&fund=1&exthrs=1&output=json';
  try {
    return parseCnbcQuotes(await fetchText(url), specs, category);
  } catch (error) {
    console.warn(`CNBC ${category} tape failed: ${error instanceof Error ? error.message : String(error)}`);
    return [];
  }
}

async function buildMarketTape() {
  let parsedItems = [];

  try {
    const html = await fetchText(SGX_NIFTY_URL);
    parsedItems = parseInternationalMarketTape(html);
  } catch (error) {
    console.warn(error instanceof Error ? error.message : String(error));
  }

  // Appended after the sgxnifty rows so ADRs and crypto read as their own
  // categories downstream rather than as more "International" cues.
  const [adrItems, cryptoItems] = await Promise.all([
    fetchCnbcTape(ADR_SYMBOLS, 'ADR'),
    fetchCnbcTape(CRYPTO_SYMBOLS, 'Commodities'),
  ]);

  return {
    updatedAt: new Date().toISOString(),
    items: [...parsedItems, ...adrItems, ...cryptoItems],
  };
}

export async function buildNewsBundle() {
  const now = new Date();
  const [marketTape, headlinePayload] = await Promise.all([
    buildMarketTape(),
    buildHeadlines(),
  ]);

  return {
    marketTape,
    newsFeed: {
      updatedAt: now.toISOString(),
      updatedAtLabel: formatIstDateLabel(now),
      items: headlinePayload.items,
      sourcesAvailable: headlinePayload.sourcesAvailable,
      sourceErrors: headlinePayload.sourceErrors,
    },
  };
}