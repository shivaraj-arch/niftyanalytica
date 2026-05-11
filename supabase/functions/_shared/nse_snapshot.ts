const NSE_ORIGIN = "https://www.nseindia.com";
const IST_TIME_ZONE = "Asia/Kolkata";
const MARKET_OPEN_MINUTE_IST = 9 * 60;
const PRE_MARKET_END_MINUTE_IST = (9 * 60) + 15;
const LAST_SNAPSHOT_FETCH_MINUTE_IST = 16 * 60;
const MARKET_CLOSE_MINUTE_IST = (16 * 60) + 1;
const POST_MARKET_START_MINUTE_IST = MARKET_CLOSE_MINUTE_IST;

export type MarketSession = "closed" | "pre-market" | "open" | "post-market";

const IST_PARTS_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: IST_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  weekday: "short",
  hour12: false,
});

const NSE_BASE_HEADERS = {
  Accept: "application/json",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: NSE_ORIGIN,
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
};

type HolidayConfig = {
  year?: number;
  dates?: string[];
};

export class NseRequestError extends Error {
  status: number;
  url: string;

  constructor(status: number, url: string) {
    super(`NSE request failed: ${status} ${url}`);
    this.status = status;
    this.url = url;
  }
}

export function toIstDateParts(date: Date) {
  const parts = Object.fromEntries(
    IST_PARTS_FORMATTER.formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  ) as Record<string, string>;

  const weekdayMap: Record<string, number> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
  };

  return {
    date: `${parts.year}-${parts.month}-${parts.day}`,
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    weekday: weekdayMap[parts.weekday] ?? -1,
  };
}

function loadHolidaySet(): Set<string> {
  const raw = Deno.env.get("NSE_TRADING_HOLIDAYS_JSON") ?? "[]";
  const parsed = JSON.parse(raw) as HolidayConfig[] | string[];

  if (Array.isArray(parsed) && parsed.length > 0 && typeof parsed[0] === "string") {
    return new Set(parsed as string[]);
  }

  return new Set((parsed as HolidayConfig[]).flatMap((entry) => entry.dates ?? []));
}

export function getMarketWindowState(date = new Date()) {
  const at = toIstDateParts(date);
  const currentMinute = at.hour * 60 + at.minute;

  if (at.weekday === 0 || at.weekday === 6) {
    return { open: false, canFetchSnapshot: false, session: "closed" as MarketSession, reason: "Weekend", at };
  }

  if (loadHolidaySet().has(at.date)) {
    return { open: false, canFetchSnapshot: false, session: "closed" as MarketSession, reason: "NSE holiday", at };
  }

  if (currentMinute < MARKET_OPEN_MINUTE_IST || currentMinute >= MARKET_CLOSE_MINUTE_IST) {
    return {
      open: false,
      canFetchSnapshot: false,
      session: "closed" as MarketSession,
      reason: "Market closed",
      at,
    };
  }

  const session: MarketSession = currentMinute < PRE_MARKET_END_MINUTE_IST
    ? "pre-market"
    : currentMinute < POST_MARKET_START_MINUTE_IST
    ? "open"
    : "post-market";

  const reason = session === "pre-market"
    ? "Pre-market session"
    : session === "open"
    ? "Regular market session"
    : "Post-market session";

  return {
    open: true,
    canFetchSnapshot: currentMinute <= LAST_SNAPSHOT_FETCH_MINUTE_IST,
    session,
    reason,
    at,
  };
}

function getCookieHeader(response: Response) {
  const getSetCookie = response.headers.getSetCookie?.bind(response.headers);
  const cookieLines = typeof getSetCookie === "function"
    ? getSetCookie()
    : [response.headers.get("set-cookie")].filter(Boolean) as string[];

  return cookieLines
    .flatMap((line) => line.split(/, (?=[^;]+=)/g))
    .map((line) => line.split(";")[0]?.trim())
    .filter(Boolean)
    .join("; ");
}

async function bootstrapCookie() {
  const response = await fetch(NSE_ORIGIN, {
    headers: NSE_BASE_HEADERS,
  });

  if (!response.ok) {
    throw new Error(`Failed to bootstrap NSE session: ${response.status}`);
  }

  return getCookieHeader(response);
}

async function fetchNseJson(url: string, cookieHeader: string) {
  const response = await fetch(url, {
    headers: {
      ...NSE_BASE_HEADERS,
      ...(cookieHeader ? { Cookie: cookieHeader } : {}),
    },
  });

  if (!response.ok) {
    throw new NseRequestError(response.status, url);
  }

  return await response.json();
}

async function fetchNseJsonWithFallback(url: string) {
  try {
    return await fetchNseJson(url, "");
  } catch (error) {
    if (!(error instanceof NseRequestError) || ![401, 403].includes(error.status)) {
      throw error;
    }

    const cookieHeader = await bootstrapCookie();
    return await fetchNseJson(url, cookieHeader);
  }
}

export async function fetchNseSnapshot() {
  const [contributors, fiiDii, expiryInfo] = await Promise.all([
    fetchNseJsonWithFallback(`${NSE_ORIGIN}/api/equity-stockIndices?index=NIFTY%2050`),
    fetchNseJsonWithFallback(`${NSE_ORIGIN}/api/fiidiiTradeReact`),
    fetchNseJsonWithFallback(`${NSE_ORIGIN}/api/option-chain-contract-info?symbol=NIFTY`),
  ]);

  const expiryDates = Array.isArray(expiryInfo?.expiryDates) ? expiryInfo.expiryDates : [];
  if (expiryDates.length === 0) {
    throw new Error("No option-chain expiry dates returned by NSE.");
  }

  const optionChain = await fetchNseJsonWithFallback(
    `${NSE_ORIGIN}/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry=${encodeURIComponent(expiryDates[0])}`,
  );

  return {
    fetchedAt: new Date().toISOString(),
    contributors,
    fiiDii,
    optionChain,
  };
}