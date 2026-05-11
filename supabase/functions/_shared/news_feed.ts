const IST_TIME_ZONE = "Asia/Kolkata";
const MAX_HEADLINES = 12;
const MAX_HEADLINE_AGE_MS = 48 * 60 * 60 * 1000;
const UPSTREAM_FETCH_TIMEOUT_MS = 12000;
const SGX_NIFTY_URL = "https://sgxnifty.org/";

const RSS_QUERIES = [
  "india stock market OR nifty OR sensex when:1d",
  "site:livemint.com nifty OR stock market when:1d",
  "site:business-standard.com nifty OR stock market when:1d",
  "site:bloomberg.com markets OR economy when:1d",
];

type HeadlineItem = {
  source: string;
  title: string;
  link: string;
  publishedAt: string;
  publishedAtLabel: string;
};

type MarketTapeItem = {
  label: string;
  last: number;
  changePercent: number;
  source: string;
  updatedAt: string;
};

type ExistingNewsPayload = {
  marketTape?: {
    updatedAt?: string;
    items?: MarketTapeItem[];
  };
  newsFeed?: {
    updatedAt?: string;
    updatedAtLabel?: string;
    items?: HeadlineItem[];
    sourcesAvailable?: string[];
    sourceErrors?: string[];
  };
};

function buildGoogleNewsRssUrl(query: string) {
  return `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-IN&gl=IN&ceid=IN:en`;
}

async function fetchTextWithTimeout(url: string, redirectCount = 0): Promise<string> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), UPSTREAM_FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      headers: {
        "user-agent": "Mozilla/5.0",
        accept: "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
      },
      redirect: "manual",
      signal: controller.signal,
    });

    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("location");
      if (!location) {
        throw new Error(`Redirect without location for ${url}`);
      }
      if (redirectCount >= 5) {
        throw new Error(`Too many redirects for ${url}`);
      }
      const nextUrl = location.startsWith("http") ? location : new URL(location, url).toString();
      return await fetchTextWithTimeout(nextUrl, redirectCount + 1);
    }

    if (!response.ok) {
      throw new Error(`Request failed: ${response.status} ${url}`);
    }

    return await response.text();
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`Request timed out for ${url}`);
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchText(url: string, redirectCount = 0): Promise<string> {
  return await fetchTextWithTimeout(url, redirectCount);
}

function decodeHtml(value: string) {
  return String(value || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([\da-f]+);/gi, (_, code) => String.fromCharCode(parseInt(code, 16)))
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ")
    .trim();
}

  function stripHtml(value: string) {
    return decodeHtml(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
  }

function extractTagValue(xml: string, tagName: string) {
  const match = String(xml || "").match(new RegExp(`<${tagName}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tagName}>`, "i"));
  return match ? decodeHtml(match[1]) : "";
}

function normalizeHeadlineKey(value: string) {
  return decodeHtml(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/\b(livemint|business standard|bloomberg|bloomberg economics)\b/g, "")
    .replace(/\s*[|:-]\s*(live|latest|update|updates)\b.*$/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function formatIstDateLabel(date: Date) {
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return `${new Intl.DateTimeFormat("en-IN", {
    timeZone: IST_TIME_ZONE,
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  }).format(date)} IST`;
}

function parseRssItems(xml: string) {
  const itemMatches = String(xml || "").match(/<item>([\s\S]*?)<\/item>/gi) || [];

  return itemMatches.map((itemXml) => {
    const title = extractTagValue(itemXml, "title");
    const link = extractTagValue(itemXml, "link");
    const source = extractTagValue(itemXml, "source") || "Google News";
    const publishedAtRaw = extractTagValue(itemXml, "pubDate");
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
  }).filter(Boolean) as Array<{
    source: string;
    title: string;
    link: string;
    publishedAt: Date;
    key: string;
  }>;
}

async function buildHeadlines(existingPayload?: ExistingNewsPayload["newsFeed"]) {
  const now = Date.now();
  const sourceErrors: string[] = [];
  const seen = new Set<string>();
  const collected: Array<{
    source: string;
    title: string;
    link: string;
    publishedAt: Date;
    key: string;
  }> = [];

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
    items: items.length ? items : (existingPayload?.items || []),
    sourceErrors,
    sourcesAvailable: [...new Set((items.length ? items : (existingPayload?.items || [])).map((item) => item.source))],
  };
}

function parseNumber(value: string | number) {
  const numericValue = Number(String(value || "").replace(/,/g, "").trim());
  return Number.isFinite(numericValue) ? numericValue : 0;
}

function extractInternationalSection(html: string) {
  const start = html.indexOf('<h2 class="widgettitle">International</h2>');
  if (start === -1) {
    throw new Error("International widget not found on SGX Nifty page.");
  }

  const endCandidates = [
    html.indexOf('<div id="recent-posts-widget-with-thumbnails', start),
    html.indexOf('<div class="widget recent-posts-widget-with-thumbnails', start),
    html.indexOf('<h2 class="widgettitle">Latest News</h2>', start),
  ].filter((value) => value > start);
  const end = endCandidates.length ? Math.min(...endCandidates) : html.length;
  return html.slice(start, end);
}

function parseInternationalMarketTape(html: string) {
  const section = extractInternationalSection(html);
  const tables = [...section.matchAll(/<table class="index-table">([\s\S]*?)<\/table>/g)];
  const items: MarketTapeItem[] = [];

  for (const tableMatch of tables) {
    const tableHtml = tableMatch[1] || "";
    const category = stripHtml(extractTagValue(tableHtml, "th")) || "International";
    const rowMatches = [...tableHtml.matchAll(/<tr class="index-line">([\s\S]*?)<\/tr>/g)];

    for (const rowMatch of rowMatches) {
      const rowHtml = rowMatch[1] || "";
      const cells = [...rowHtml.matchAll(/<td class="([^"]+)">([\s\S]*?)<\/td>/g)];
      const cellMap = new Map(cells.map((cell) => [cell[1], cell[2]]));
      const rawName = stripHtml(cellMap.get("index-name left-align") || cellMap.get("index-name") || "");
      const last = parseNumber(stripHtml(cellMap.get("index-price") || ""));
      const changePercent = parseNumber(stripHtml(cellMap.get("index-percent") || "").replace(/%/g, ""));
      const clockCell = rowHtml.match(/<td class="index-clock\s+([^"]+)"[^>]*>[\s\S]*?title="([^"]+)"/i);
      const status = stripHtml(clockCell?.[2] || clockCell?.[1] || "").replace(/[^A-Za-z]/g, "") || "Unknown";

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

async function buildMarketTape(existingPayload?: ExistingNewsPayload["marketTape"]) {
  let parsedItems: MarketTapeItem[] = [];

  try {
    const html = await fetchText(SGX_NIFTY_URL);
    parsedItems = parseInternationalMarketTape(html);
  } catch (error) {
    console.warn(error instanceof Error ? error.message : String(error));
  }

  const sortedItems = parsedItems.length ? parsedItems : (existingPayload?.items || []);

  return {
    updatedAt: sortedItems.length
      ? new Date().toISOString()
      : (existingPayload?.updatedAt || new Date().toISOString()),
    items: sortedItems,
  };
}

export async function buildNewsBundle(existingPayload?: ExistingNewsPayload) {
  const now = new Date();
  const [marketTape, headlinePayload] = await Promise.all([
    buildMarketTape(existingPayload?.marketTape),
    buildHeadlines(existingPayload?.newsFeed),
  ]);

  return {
    marketTape,
    newsFeed: {
      updatedAt: headlinePayload.items.length
        ? now.toISOString()
        : (existingPayload?.newsFeed?.updatedAt || now.toISOString()),
      updatedAtLabel: headlinePayload.items.length
        ? formatIstDateLabel(now)
        : (existingPayload?.newsFeed?.updatedAtLabel || formatIstDateLabel(now)),
      items: headlinePayload.items,
      sourcesAvailable: headlinePayload.sourcesAvailable,
      sourceErrors: headlinePayload.sourceErrors,
    },
  };
}