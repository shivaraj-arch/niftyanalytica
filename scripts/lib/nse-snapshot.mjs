const NSE_ORIGIN = 'https://www.nseindia.com';

const NSE_BASE_HEADERS = {
  Accept: 'application/json',
  'Accept-Language': 'en-US,en;q=0.9',
  Referer: NSE_ORIGIN,
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
};

class NseRequestError extends Error {
  constructor(status, url) {
    super(`NSE request failed: ${status} ${url}`);
    this.status = status;
    this.url = url;
  }
}

function getCookieHeader(response) {
  const rawSetCookie = response.headers.get('set-cookie');
  return rawSetCookie
    ? rawSetCookie.split(/, (?=[^;]+=)/g).map((line) => line.split(';')[0]?.trim()).filter(Boolean).join('; ')
    : '';
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

async function fetchNseJson(url, cookieHeader) {
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

async function fetchNseJsonWithFallback(url) {
  try {
    return await fetchNseJson(url, '');
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
    throw new Error('No option-chain expiry dates returned by NSE.');
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