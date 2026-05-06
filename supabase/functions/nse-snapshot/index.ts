import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Content-Type": "application/json",
};

const NSE_ORIGIN = "https://www.nseindia.com";
const NSE_BASE_HEADERS = {
  Accept: "application/json",
  "Accept-Language": "en-US,en;q=0.9",
  Referer: NSE_ORIGIN,
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
};

function jsonResponse(payload: unknown, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: CORS_HEADERS,
  });
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
    throw new Error(`NSE request failed: ${response.status} ${url}`);
  }

  return await response.json();
}

serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: CORS_HEADERS });
  }

  if (request.method !== "GET") {
    return jsonResponse({ ok: false, error: "Method not allowed" }, 405);
  }

  try {
    const cookieHeader = await bootstrapCookie();
    const [contributors, fiiDii, expiryInfo] = await Promise.all([
      fetchNseJson(`${NSE_ORIGIN}/api/equity-stockIndices?index=NIFTY%2050`, cookieHeader),
      fetchNseJson(`${NSE_ORIGIN}/api/fiidiiTradeReact`, cookieHeader),
      fetchNseJson(`${NSE_ORIGIN}/api/option-chain-contract-info?symbol=NIFTY`, cookieHeader),
    ]);

    const expiryDates = Array.isArray(expiryInfo?.expiryDates) ? expiryInfo.expiryDates : [];
    if (expiryDates.length === 0) {
      throw new Error("No option-chain expiry dates returned by NSE.");
    }

    const optionChain = await fetchNseJson(
      `${NSE_ORIGIN}/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry=${encodeURIComponent(expiryDates[0])}`,
      cookieHeader,
    );

    return jsonResponse({
      fetchedAt: new Date().toISOString(),
      contributors,
      fiiDii,
      optionChain,
    });
  } catch (error) {
    return jsonResponse(
      {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      },
      500,
    );
  }
});