const currency = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });
const compactNumber = new Intl.NumberFormat('en-IN', { notation: 'compact', maximumFractionDigits: 1 });

const chartState = {};
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const AI_REFRESH_HOURS = [6, 9, 12, 15, 18, 21];
const REALTIME_REFRESH_MS = 60 * 1000;
const MARKET_TAPE_REFRESH_MS = 3 * 60 * 1000;
const IST_TIMEZONE = 'Asia/Kolkata';
const STALE_SNAPSHOT_MAX_AGE_MS = 36 * 60 * 60 * 1000;
const DEFAULT_FETCH_TIMEOUT_MS = 12000;
const LIVE_SNAPSHOT_FETCH_TIMEOUT_MS = 18000;
const MARKET_OPEN_MINUTE_IST = 9 * 60;
const PRE_MARKET_END_MINUTE_IST = (9 * 60) + 15;
const LAST_LIVE_FETCH_MINUTE_IST = 16 * 60;
const MARKET_CLOSE_MINUTE_IST = (16 * 60) + 1;
const POST_MARKET_START_MINUTE_IST = MARKET_CLOSE_MINUTE_IST;
const runtimeConfig = window.RUNTIME_CONFIG || {};
const LIVE_SNAPSHOT_BUCKET = String(runtimeConfig.LIVE_SNAPSHOT_BUCKET || '').trim() || 'public-data';
const LIVE_SNAPSHOT_PATH = String(runtimeConfig.LIVE_SNAPSHOT_PATH || '').trim() || 'live/live-snapshot.json';
const SUPABASE_URL = String(runtimeConfig.SUPABASE_URL || '').trim().replace(/\/$/, '');
const DATA_BASE_URL = String(runtimeConfig.DATA_BASE_URL || '').trim().replace(/\/$/, '');
const NEWSLETTER_SUBSCRIBE_URL = String(runtimeConfig.NEWSLETTER_SUBSCRIBE_URL || '').trim() || (SUPABASE_URL ? `${SUPABASE_URL}/functions/v1/newsletter-subscribe` : '');
const HEADLINE_REFRESH_URL = String(runtimeConfig.HEADLINE_REFRESH_URL || '').trim() || (SUPABASE_URL ? `${SUPABASE_URL}/functions/v1/refresh-news-feed` : '');
const NSE_SNAPSHOT_URL = String(runtimeConfig.NSE_SNAPSHOT_URL || '').trim() || (SUPABASE_URL ? `${SUPABASE_URL}/functions/v1/nse-snapshot` : '');
const LIVE_SNAPSHOT_URL = String(runtimeConfig.LIVE_SNAPSHOT_URL || '').trim() || (SUPABASE_URL ? `${SUPABASE_URL}/storage/v1/object/public/${LIVE_SNAPSHOT_BUCKET}/${LIVE_SNAPSHOT_PATH}` : '');

const setText = (id, value) => {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value;
  }
};

const setDisplay = (id, visible) => {
  const element = document.getElementById(id);
  if (element) {
    element.style.display = visible ? '' : 'none';
  }
};

const setHidden = (id, hidden) => {
  const element = document.getElementById(id);
  if (element) {
    element.hidden = hidden;
  }
};

let realtimeRefreshTimer = null;
let aiRefreshTimer = null;
let marketActivityRows = [];
let lastNewsRailSnapshot = null;
let lastMarketTapeLoadAt = 0;

const fetchWithTimeout = async (url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) => {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }
};

const loadJson = async (url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) => {
  const response = await fetchWithTimeout(url, options, timeoutMs);
  if (!response.ok) {
    throw new Error(`Failed to load data: ${response.status}`);
  }
  return response.json();
};

const loadText = async (url, options = {}, timeoutMs = DEFAULT_FETCH_TIMEOUT_MS) => {
  const response = await fetchWithTimeout(url, options, timeoutMs);
  if (!response.ok) {
    throw new Error(`Failed to load text: ${response.status}`);
  }
  return response.text();
};

const buildDataUrl = (path, { bust = false, source = 'auto' } = {}) => {
  const normalizedPath = String(path || '').replace(/^\/+/, '');
  const baseUrl = source === 'local'
    ? ''
    : (source === 'remote' ? DATA_BASE_URL : (DATA_BASE_URL || ''));
  const target = baseUrl ? `${baseUrl}/${normalizedPath}` : normalizedPath;
  if (!bust) {
    return target;
  }
  return `${target}${target.includes('?') ? '&' : '?'}t=${Date.now()}`;
};

const loadDataWithFallback = async (path, loader) => {
  const localUrl = buildDataUrl(path, { bust: true, source: 'local' });
  if (!DATA_BASE_URL) {
    return loader(localUrl, { cache: 'no-store' });
  }

  const remoteUrl = buildDataUrl(path, { bust: true, source: 'remote' });
  try {
    return await loader(remoteUrl, { cache: 'no-store' });
  } catch (error) {
    console.warn(`Remote data load failed for ${path}, falling back to local data.`, error);
    return loader(localUrl, { cache: 'no-store' });
  }
};

const loadJsonData = async (path) => loadDataWithFallback(path, loadJson);
const loadTextData = async (path) => loadDataWithFallback(path, loadText);

const parseTimestampValue = (value) => {
  if (!value) {
    return null;
  }

  const date = parseFeedDate(value);
  if (date) {
    return date;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const getSnapshotTimestamp = (payload) => (
  payload?.fetchedAt
  || payload?.updatedAt
  || payload?.newsFeed?.updatedAt
  || payload?.marketTape?.updatedAt
  || payload?.generatedAt
  || payload?.contributors?.timestamp
  || payload?.marketStatus?.tradeDate
  || ''
);

const getSnapshotAgeMs = (payload) => {
  const date = parseTimestampValue(getSnapshotTimestamp(payload));
  return date ? Math.max(0, Date.now() - date.getTime()) : Number.POSITIVE_INFINITY;
};

const pickFresherSnapshot = (left, right) => {
  if (!left) return right;
  if (!right) return left;
  return getSnapshotAgeMs(left) <= getSnapshotAgeMs(right) ? left : right;
};

const getIstClock = (date = new Date()) => {
  const formatter = new Intl.DateTimeFormat('en-GB', {
    timeZone: IST_TIMEZONE,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = Object.fromEntries(
    formatter.formatToParts(date)
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, part.value]),
  );

  return {
    weekday: parts.weekday,
    minutes: Number(parts.hour || 0) * 60 + Number(parts.minute || 0),
  };
};

const getMarketSessionState = (date = new Date()) => {
  const { weekday, minutes } = getIstClock(date);
  if (weekday === 'Sat' || weekday === 'Sun') {
    return { session: 'closed', canFetchLiveSnapshot: false };
  }

  if (minutes < MARKET_OPEN_MINUTE_IST || minutes >= MARKET_CLOSE_MINUTE_IST) {
    return { session: 'closed', canFetchLiveSnapshot: false };
  }

  if (minutes < PRE_MARKET_END_MINUTE_IST) {
    return { session: 'pre-market', canFetchLiveSnapshot: true };
  }

  if (minutes < POST_MARKET_START_MINUTE_IST) {
    return {
      session: 'open',
      canFetchLiveSnapshot: minutes <= LAST_LIVE_FETCH_MINUTE_IST,
    };
  }

  return { session: 'post-market', canFetchLiveSnapshot: false };
};

const getMarketSessionLabel = (state) => {
  if (!state) return '-';
  if (state.session === 'pre-market') return 'Pre-Market';
  if (state.session === 'open') return 'Open';
  if (state.session === 'post-market') return 'Post-Market';
  return 'Closed';
};

const isRealtimeRefreshWindow = (date = new Date()) => getMarketSessionState(date).canFetchLiveSnapshot;

const fetchAiAnalysis = async () => loadJsonData('data/ai-analysis.json');
const fetchLegacyNewsRailSnapshot = async () => loadJsonData('data/news-feed.json');
const fetchMarketActivityHistory = async () => loadTextData('data/market-activity-history.csv');
const fetchRealtimeSnapshot = async () => {
  const liveUrl = NSE_SNAPSHOT_URL
    ? `${NSE_SNAPSHOT_URL}${NSE_SNAPSHOT_URL.includes('?') ? '&' : '?'}t=${Date.now()}`
    : '';

  if (!liveUrl) {
    throw new Error('Realtime snapshot endpoint is not configured.');
  }

  return await loadJson(liveUrl, {
    cache: 'no-store',
  }, LIVE_SNAPSHOT_FETCH_TIMEOUT_MS);
};

const fetchLiveSnapshotCache = async ({ preferLive = false } = {}) => {
  const liveUrl = LIVE_SNAPSHOT_URL
    ? `${LIVE_SNAPSHOT_URL}${LIVE_SNAPSHOT_URL.includes('?') ? '&' : '?'}t=${Date.now()}`
    : '';
  const marketSessionState = getMarketSessionState();
  const staticSnapshot = await loadJsonData('data/live-snapshot.json');

  if (!liveUrl) {
    return staticSnapshot;
  }

  if (!preferLive && !marketSessionState.canFetchLiveSnapshot && getSnapshotAgeMs(staticSnapshot) <= STALE_SNAPSHOT_MAX_AGE_MS) {
    return staticSnapshot;
  }

  try {
    const liveSnapshot = await loadJson(liveUrl, {
      cache: 'no-store',
    }, LIVE_SNAPSHOT_FETCH_TIMEOUT_MS);
    return pickFresherSnapshot(staticSnapshot, liveSnapshot);
  } catch (error) {
    console.warn('Stored live snapshot fetch failed, falling back to bundled cache.', error);
    return staticSnapshot;
  }
};
const fetchHolidaySnapshot = async () => loadJsonData('data/nse-holidays.json');

const parseFeedDate = (value) => {
  if (!value) return null;
  const rawValue = String(value).trim();
  if (!rawValue) return null;

  // RSS feeds sometimes omit timezone info; treat those timestamps as UTC.
  const normalizedValue = /(?:z|[+-]\d{2}:?\d{2}|gmt|utc)$/i.test(rawValue)
    ? rawValue
    : rawValue.replace(' ', 'T') + 'Z';

  const date = new Date(normalizedValue);
  return Number.isNaN(date.getTime()) ? null : date;
};

const toIstLabel = (value) => {
  if (!value) return '-';
  const date = parseFeedDate(value);
  return !date ? value : new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIMEZONE,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(date).replace(',', '') + ' IST';
};

const normalizeCachedHolidayPayload = (payload) => {
  if (payload?.months && Array.isArray(payload.months)) {
    return payload;
  }

  const rawHolidays = payload?.holidays || payload || {};
  const records = Array.isArray(rawHolidays.CM)
    ? rawHolidays.CM
    : Array.isArray(rawHolidays.FO)
      ? rawHolidays.FO
      : Object.values(rawHolidays).find(Array.isArray) || [];

  const normalized = normalizeHolidayPayload(records);
  if (payload?.updatedAt) {
    normalized.updatedAt = payload.updatedAt;
  }
  return normalized;
};

const cleanHeadlineTitle = (title, source) => {
  const suffix = ` - ${source}`;
  return title.endsWith(suffix) ? title.slice(0, -suffix.length).trim() : title.trim();
};

const loadAiData = async () => {
  const aiResult = await fetchAiAnalysis();
  renderAiSection(aiResult);
};

const hasEmbeddedNewsRail = (payload) => (
  Array.isArray(payload?.newsFeed?.items)
  || Array.isArray(payload?.marketTape?.items)
);

const hasNewsFeedItems = (payload) => Array.isArray(payload?.newsFeed?.items) && payload.newsFeed.items.length > 0;

const hasMarketTapeItems = (payload) => Array.isArray(payload?.marketTape?.items) && payload.marketTape.items.length > 0;

const mergeNewsRailSnapshot = (payload) => {
  const previous = lastNewsRailSnapshot || {};
  const nextMarketTape = hasMarketTapeItems(payload)
    ? payload.marketTape
    : (hasMarketTapeItems(previous) ? previous.marketTape : payload?.marketTape || {});
  const nextNewsFeed = hasNewsFeedItems(payload)
    ? payload.newsFeed
    : (hasNewsFeedItems(previous) ? previous.newsFeed : payload?.newsFeed || {});

  return {
    marketTape: nextMarketTape || {},
    newsFeed: nextNewsFeed || {},
  };
};

const cacheNewsRailSnapshot = (payload) => {
  if (!payload) {
    return;
  }

  if (hasMarketTapeItems(payload) || hasNewsFeedItems(payload)) {
    lastNewsRailSnapshot = {
      marketTape: payload.marketTape || {},
      newsFeed: payload.newsFeed || {},
    };
  }
};

const resolveNewsRailSnapshot = async (snapshotPayload) => {
  if (hasEmbeddedNewsRail(snapshotPayload)) {
    const embeddedSnapshot = {
      marketTape: snapshotPayload.marketTape || {},
      newsFeed: snapshotPayload.newsFeed || {},
    };

    if (hasMarketTapeItems(embeddedSnapshot) && hasNewsFeedItems(embeddedSnapshot)) {
      return embeddedSnapshot;
    }

    try {
      const legacySnapshot = await fetchLegacyNewsRailSnapshot();
      return {
        marketTape: hasMarketTapeItems(embeddedSnapshot) ? embeddedSnapshot.marketTape : (legacySnapshot?.marketTape || {}),
        newsFeed: hasNewsFeedItems(embeddedSnapshot) ? embeddedSnapshot.newsFeed : (legacySnapshot?.newsFeed || {}),
      };
    } catch (error) {
      console.warn('Legacy news rail fallback failed.', error);
      return embeddedSnapshot;
    }
  }

  return await fetchLegacyNewsRailSnapshot();
};

const renderNewsRailSnapshot = (payload) => {
  const mergedSnapshot = mergeNewsRailSnapshot(payload);
  const marketTape = mergedSnapshot.marketTape || {};
  const newsFeed = mergedSnapshot.newsFeed || {};

  try {
    if (hasMarketTapeItems(mergedSnapshot)) {
      setText('marketTapeStamp', formatDateTime(marketTape.updatedAt) || '-');
      renderMarketTape(marketTape.items || []);
    } else {
      renderMarketTapeError('World markets are waiting for the next successful refresh.');
    }
  } catch (error) {
    console.error('Market tape render failed', error);
    renderMarketTapeError(error?.message || 'Market tape render failed.');
  }

  try {
    if (hasNewsFeedItems(mergedSnapshot)) {
      renderNewsFeed(newsFeed);
      setText('newsFeedStamp', newsFeed.updatedAtLabel || formatDateTime(newsFeed.updatedAt));
      setText('newsFeedMeta', '');
      setHidden('newsFeedMeta', true);
    } else {
      renderHeadlineError('Headlines are waiting for the next successful refresh.');
    }
  } catch (error) {
    console.error('Headline render failed', error);
    renderHeadlineError(error?.message || 'Headline rail render failed.');
  }

  cacheNewsRailSnapshot(mergedSnapshot);
};

const getPrimarySnapshot = async ({ preferLive = false } = {}) => {
  const marketSessionState = getMarketSessionState();

  if (marketSessionState.canFetchLiveSnapshot) {
    try {
      return await fetchRealtimeSnapshot();
    } catch (error) {
      console.warn('Realtime snapshot fetch failed, falling back to cached snapshot.', error);
    }
  }

  return await fetchLiveSnapshotCache({ preferLive });
};

const loadRealtimeData = async ({ preferLive = false } = {}) => {
  const [liveResult, holidaysResult, marketActivityResult] = await Promise.allSettled([
    getPrimarySnapshot({ preferLive }),
    fetchHolidaySnapshot(),
    fetchMarketActivityHistory(),
  ]);

  if (liveResult.status === 'fulfilled') {
    try {
      const newsRailSnapshot = await resolveNewsRailSnapshot(liveResult.value);
      const shouldRefreshMarketTape = preferLive || !lastMarketTapeLoadAt || (Date.now() - lastMarketTapeLoadAt) >= MARKET_TAPE_REFRESH_MS;
      renderNewsRailSnapshot(
        shouldRefreshMarketTape
          ? newsRailSnapshot
          : {
            marketTape: lastNewsRailSnapshot?.marketTape || newsRailSnapshot?.marketTape || {},
            newsFeed: newsRailSnapshot?.newsFeed || {},
          },
      );
      if (shouldRefreshMarketTape && hasMarketTapeItems(newsRailSnapshot)) {
        lastMarketTapeLoadAt = Date.now();
      }
    } catch (error) {
      console.error('News rail render failed', error);
      renderMarketTapeError(error?.message || 'Market tape refresh failed.');
      renderHeadlineError(error?.message || 'Headline rail refresh failed.');
    }

    try {
      renderLiveSections(normalizeLiveSnapshot(liveResult.value));
    } catch (error) {
      console.error('Live render failed', error);
      renderLiveError(error?.message || 'Live market render failed.');
    }
  } else {
    renderMarketTapeError(liveResult.reason?.message || 'Market tape refresh failed.');
    renderHeadlineError(liveResult.reason?.message || 'Headline rail refresh failed.');
    renderLiveError(liveResult.reason?.message || 'Live market refresh failed.');
  }

  if (marketActivityResult.status === 'fulfilled') {
    try {
      renderMarketActivitySection(normalizeMarketActivityHistory(marketActivityResult.value));
    } catch (error) {
      console.error('Market activity render failed', error);
      renderMarketActivityError(error?.message || 'Market activity render failed.');
    }
  } else {
    renderMarketActivityError(marketActivityResult.reason?.message || 'Market activity refresh failed.');
  }

  if (holidaysResult.status === 'fulfilled') {
    try {
      renderHolidayCalendar(normalizeCachedHolidayPayload(holidaysResult.value));
    } catch (error) {
      console.error('Holiday render failed', error);
      renderHolidayError(error?.message || 'Holiday calendar render failed.');
    }
  } else {
    renderHolidayError(holidaysResult.reason?.message || 'Holiday calendar refresh failed.');
  }

};

const renderAiError = (message) => {
  setText('aiAnalysisStamp', 'AI refresh pending');
  document.getElementById('aiSummaryText').innerHTML = `<p>${escapeHtml(message)}</p>`;
  renderMetricGrid('aiSummary', [
    { label: 'Agent', value: '-' },
    { label: 'Model', value: '-' },
    { label: 'Bias', value: '-' },
    { label: 'Status', value: 'Error', tone: 'negative' },
  ]);
  renderChipList('aiKeyLevels', []);
  renderChipList('aiWatchlist', []);
  setDisplay('aiKeyLevelsBlock', false);
  setDisplay('aiWatchlistBlock', false);
};

const renderMarketTapeError = (message) => {
  if (hasMarketTapeItems(lastNewsRailSnapshot)) {
    setText('marketTapeStamp', formatDateTime(lastNewsRailSnapshot.marketTape?.updatedAt) || 'Using previous world markets');
    setText('marketTapeMeta', `${message} Showing previous world markets.`);
    return;
  }

  setText('marketTapeStamp', 'Market tape refresh pending');
  setText('marketTapeMeta', message);
  setText('marketTapeHighlights', 'Top Movements: Waiting for world markets.');
  document.getElementById('marketTape').innerHTML = `<p>${message}</p>`;
};

const renderHeadlineError = (message) => {
  if (hasNewsFeedItems(lastNewsRailSnapshot)) {
    setText('newsFeedStamp', lastNewsRailSnapshot.newsFeed?.updatedAtLabel || formatDateTime(lastNewsRailSnapshot.newsFeed?.updatedAt));
    setText('newsFeedMeta', `${message} Showing previous headlines.`);
    setHidden('newsFeedMeta', false);
    return;
  }

  setText('newsFeedStamp', 'Headline refresh pending');
  setText('newsFeedMeta', message);
  setHidden('newsFeedMeta', false);
  document.getElementById('newsFeedList').innerHTML = `<li class="news-feed-item">${message}</li>`;
};

const emphasizeAgentHeadings = (value) => String(value || '')
  .replace(/^Nifty Analysis Agent$/gm, '**Nifty Analysis Agent**')
  .replace(/^Nifty Flow Agent$/gm, '**Nifty Flow Agent**')
  .replace(/^Aggregator Agent$/gm, '**Aggregator Agent**');

const renderHolidayError = (message) => {
  setText('holidayStamp', 'Holiday refresh pending');
  setText('holidayMeta', message);
  document.getElementById('holidayCalendarGrid').innerHTML = `<p>${message}</p>`;
};

const renderMarketActivityError = (message) => {
  setText('heroMarketCap', '-');
  document.getElementById('optionChainMarketCapChart').innerHTML = `<p>${message}</p>`;
  document.getElementById('optionChainMarketCapLegend').innerHTML = '';
  document.getElementById('optionChainMarketFlowChart').innerHTML = `<p>${message}</p>`;
  document.getElementById('optionChainMarketFlowLegend').innerHTML = '';
};

const renderLiveError = (message) => {
  setText('generatedAt', 'Live refresh pending');
  setText('heroSpot', '-');
  setText('heroExpiry', '-');
  setText('heroTopContributor', '-');
  setText('heroLowestIv', '-');
  setText('heroTradedValue', '-');
  setText('heroMarketCap', '-');
  setText('heroBreadth', '-');
  setText('contributorsStamp', 'Live refresh pending');
  setText('openInterestStamp', 'Live refresh pending');
  setText('blackScholesStamp', 'Live refresh pending');
  setText('lowIvStamp', 'Live refresh pending');
  setText('openInterestPositioningNote', message);
  setText('openInterestSignalNote', message);
  document.getElementById('contributorsRows').innerHTML = `<tr><td colspan="5">${message}</td></tr>`;
  document.getElementById('blackScholesRows').innerHTML = `<tr><td colspan="7">${message}</td></tr>`;
  document.getElementById('lowIvRows').innerHTML = `<tr><td colspan="6">${message}</td></tr>`;
  document.getElementById('oiChart').innerHTML = `<p>${message}</p>`;
  document.getElementById('bidAskChart').innerHTML = `<p>${message}</p>`;
};

const renderAiSection = (aiAnalysis) => {
  setText('aiAnalysisStamp', formatDateTime(aiAnalysis.generatedAt) || '-');
  renderMetricGrid('aiSummary', [
    { label: 'Agent', value: aiAnalysis.agent || 'ADK Multi-Agent' },
    { label: 'Model', value: aiAnalysis.model || '-' },
    { label: 'Bias', value: aiAnalysis.bias || '-' },
    { label: 'Status', value: aiAnalysis.status || '-', tone: aiAnalysis.status === 'ok' ? 'positive' : '' },
  ]);

  document.getElementById('aiSummaryText').innerHTML = renderMarkdown(emphasizeAgentHeadings(aiAnalysis.summary || 'AI analysis is pending for the next refresh.'));
  renderChipList('aiKeyLevels', aiAnalysis.keyLevels || []);
  renderChipList('aiWatchlist', aiAnalysis.watchlist || []);
  setDisplay('aiKeyLevelsBlock', (aiAnalysis.keyLevels || []).length > 0);
  setDisplay('aiWatchlistBlock', (aiAnalysis.watchlist || []).length > 0);
};

const renderLiveSections = (snapshot) => {
  const contributors = snapshot.contributors;
  const openInterest = snapshot.openInterest;
  const blackScholes = snapshot.blackScholes;
  const lowIv = snapshot.lowIV;
  const indexSummary = snapshot.indexSummary;
  const signedChangeValue = signed(indexSummary.percentChange);
  const dayRangeLabel = indexSummary.low > 0 || indexSummary.high > 0
    ? `${formatNumber(indexSummary.low)} - ${formatNumber(indexSummary.high)}`
    : '-';

  setText('generatedAt', formatDateTime(snapshot.fetchedAt));
  setText('heroSpot', formatNumber(indexSummary.last));
  setText('heroExpiry', indexSummary.marketStatusLabel);
  setText('heroTopContributor', `${signedChangeValue}%`);
  setText('heroLowestIv', dayRangeLabel);
  setText('heroTradedValue', `${formatNumber(indexSummary.tradedValueCrores)} Cr`);
  setText('heroBreadth', `${contributors.advances} / ${contributors.declines}`);
  setText('contributorsStamp', contributors.timestamp || '-');
  setText('openInterestStamp', openInterest.timestamp || '-');
  setText('blackScholesStamp', blackScholes.timestamp || '-');
  setText('lowIvStamp', lowIv.timestamp || '-');

  renderMetricGrid('contributorsSummary', [
    { label: 'Nifty Last', value: formatNumber(contributors.lastPrice) },
    { label: 'Total CP', value: signed(contributors.totalPoints), tone: tone(contributors.totalPoints) },
    { label: 'Adv / Dec', value: `${contributors.advances} / ${contributors.declines}` },
    { label: 'Tracked Stocks', value: String(contributors.rows.length) },
  ]);

  renderMetricGrid('openInterestSummary', [
    { label: 'Spot', value: formatNumber(openInterest.spot) },
    { label: 'Expiry', value: openInterest.expiry || '-' },
    { label: 'Net Positioning Δ', value: signed(openInterest.positioning.netPositioningChange), tone: tone(openInterest.positioning.netPositioningChange) },
    { label: 'Flow Imbalance', value: signed(openInterest.positioning.flowImbalance), tone: tone(openInterest.positioning.flowImbalance) },
    { label: 'IV Skew', value: `${signed(openInterest.positioning.ivSkew)}%`, tone: tone(openInterest.positioning.ivSkew) },
    { label: 'Strongest Shift', value: openInterest.positioning.strongestShiftLabel },
    { label: 'Divergence Score', value: String(openInterest.signals.divergenceScore), tone: openInterest.signals.divergenceScore > 0 ? 'negative' : '' },
    { label: 'Trap Risk', value: openInterest.signals.trapRiskLabel, tone: openInterest.signals.trapRiskTone },
  ]);

  setText('openInterestPositioningNote', openInterest.positioning.note);
  setText('openInterestSignalNote', openInterest.signals.note);

  renderMetricGrid('blackScholesSummary', [
    { label: 'Spot', value: formatNumber(blackScholes.spot) },
    { label: 'Expiry', value: blackScholes.expiry || '-' },
    { label: 'Avg Call Gap', value: signed(averageGap(blackScholes.rows, 'call')), tone: tone(averageGap(blackScholes.rows, 'call')) },
    { label: 'Avg Put Gap', value: signed(averageGap(blackScholes.rows, 'put')), tone: tone(averageGap(blackScholes.rows, 'put')) },
  ]);

  document.getElementById('contributorsRows').innerHTML = contributors.rows.map((row) => `
    <tr style="${rowColors(row.pChange)}">
      <td>${row.symbol}</td>
      <td>${formatNumber(row.last)}</td>
      <td class="${tone(row.pChange)}">${signed(row.pChange)}%</td>
      <td class="${tone(row.contributingPoints)}">${signed(row.contributingPoints)}</td>
      <td>${formatNumber(row.tradedValueCr)}</td>
    </tr>
  `).join('');

  document.getElementById('blackScholesRows').innerHTML = blackScholes.rows.map((row) => `
    <tr>
      <td>${row.strikePrice}</td>
      <td>${formatNumber(row.call.marketPrice)}</td>
      <td class="${tone(row.call.marketPrice - row.call.bsValue)}">${formatNumber(row.call.bsValue)}</td>
      <td>${formatNumber(row.put.marketPrice)}</td>
      <td class="${tone(row.put.marketPrice - row.put.bsValue)}">${formatNumber(row.put.bsValue)}</td>
      <td>${formatNumber(row.call.iv)}%</td>
      <td>${formatNumber(row.put.iv)}%</td>
    </tr>
  `).join('');

  renderLowIv(lowIv, lowIv.defaultExpiry);

  renderBarChart('oiChart', openInterest.strikes, ({ strikePrice, callOIChange, putOIChange }) => ({
    label: String(strikePrice),
    valueA: callOIChange,
    valueB: putOIChange,
    colorA: '#1f8f6b',
    colorB: '#b94a48',
    labelA: 'Call ΔOI',
    labelB: 'Put ΔOI',
  }), 'oiLegend', 'Net OI Change by Strike');

  renderBarChart('bidAskChart', openInterest.strikes, ({ strikePrice, callBidAsk, putBidAsk }) => ({
    label: String(strikePrice),
    valueA: callBidAsk,
    valueB: putBidAsk,
    colorA: '#0f4c5c',
    colorB: '#b47b19',
    labelA: 'Call Buy-Sell',
    labelB: 'Put Buy-Sell',
  }), 'bidAskLegend', 'Buy Minus Sell by Strike');

  bindChartExpansion();
};

const OPTION_CHAIN_MARKET_CAP_SERIES = {
  key: 'marketCapCrores',
  label: 'Market Cap (Cr)',
  color: '#b94a48',
};

const OPTION_CHAIN_MARKET_FLOW_SERIES = [
  { key: 'tradedValueCrores', label: 'Traded Value (Cr)', color: '#0f4c5c' },
  { key: 'ffmcCrores', label: 'FFMC (Cr)', color: '#1f8f6b' },
  { key: 'fiiNetCrores', label: 'FII Net (Cr)', color: '#b47b19' },
  { key: 'diiNetCrores', label: 'DII Net (Cr)', color: '#7a4cc2' },
];

const renderMarketActivitySection = (rows) => {
  marketActivityRows = rows;
  const latest = rows[rows.length - 1];
  setText('heroMarketCap', formatNumber(latest.totalMarketCapitalisationCrores));
  renderOptionChainMarketActivity();
};

const renderOptionChainMarketActivity = () => {
  if (!marketActivityRows.length) {
    return;
  }

  const rows = marketActivityRows
    .map((row) => ({
      date: row.date,
      marketCapCrores: parseNumber(row.totalMarketCapitalisationCrores),
      tradedValueCrores: parseNumber(row.tradedValueCrores),
      ffmcCrores: parseNumber(row.ffmcCrores),
      fiiNetCrores: parseNumber(row.fiiNetCrores),
      diiNetCrores: parseNumber(row.diiNetCrores),
    }))
    .slice(-22);

  if (!rows.length) {
    renderMarketActivityError('No option-chain market activity history available.');
    return;
  }

  renderSingleSeriesChart(
    'optionChainMarketCapChart',
    rows,
    (row) => ({
      label: formatArchiveDateLabel(row.date),
      value: parseNumber(row.marketCapCrores),
      color: OPTION_CHAIN_MARKET_CAP_SERIES.color,
      labelA: OPTION_CHAIN_MARKET_CAP_SERIES.label,
    }),
    'optionChainMarketCapLegend',
    'Daily Market Cap',
  );

  renderGroupedSeriesChart(
    'optionChainMarketFlowChart',
    rows,
    (row) => ({
      label: formatArchiveDateLabel(row.date),
      series: OPTION_CHAIN_MARKET_FLOW_SERIES.map((series) => ({
        value: parseNumber(row[series.key]),
        color: series.color,
        label: series.label,
      })),
    }),
    'optionChainMarketFlowLegend',
    'Daily Market Activity and Flows',
  );
};

const normalizeLiveSnapshot = (payload) => {
  const contributors = normalizeContributors(payload.contributors || {});
  const fiiDii = normalizeFiiDii(payload.fiiDii || {});
  const openInterest = extractOpenInterest(payload.optionChain || {});
  const blackScholes = buildBlackScholes(openInterest);
  const lowIV = buildLowIv(payload.optionChain || {});
  const indexSummary = normalizeIndexSummary(payload);
  openInterest.signals = buildOpenInterestSignals({ openInterest, contributors, fiiDii, indexSummary });

  return {
    fetchedAt: payload.fetchedAt || new Date().toISOString(),
    contributors,
    fiiDii,
    openInterest,
    blackScholes,
    lowIV,
    indexSummary,
  };
};

const normalizeIndexSummary = (payload) => {
  const contributorsPayload = payload.contributors || {};
  const metadata = contributorsPayload.metadata || buildIndexMetadataFallback(contributorsPayload);
  const marketStatus = payload.marketStatus || buildMarketStatusFallback(contributorsPayload);

  return {
    last: parseNumber(metadata.last),
    open: parseNumber(metadata.open),
    high: parseNumber(metadata.high),
    low: parseNumber(metadata.low),
    previousClose: parseNumber(metadata.previousClose),
    change: parseNumber(metadata.change || marketStatus.variation),
    percentChange: parseNumber(metadata.percChange || marketStatus.percentChange),
    tradedValueCrores: parseNumber(metadata.totalTradedValue) / 10000000,
    totalTradedVolume: parseNumber(metadata.totalTradedVolume),
    ffmc: parseNumber(metadata.ffmc_sum),
    marketStatusLabel: getMarketSessionLabel(getMarketSessionState()),
    marketStatus,
  };
};

const buildIndexMetadataFallback = (contributorsPayload) => {
  const rows = Array.isArray(contributorsPayload.data) ? contributorsPayload.data : [];
  const niftyRow = rows.find((item) => item.symbol === 'NIFTY 50') || rows[0] || {};
  return {
    indexName: contributorsPayload.name || niftyRow.symbol || 'NIFTY 50',
    open: niftyRow.open,
    high: niftyRow.dayHigh,
    low: niftyRow.dayLow,
    previousClose: niftyRow.previousClose,
    last: niftyRow.lastPrice,
    percChange: niftyRow.pChange,
    change: niftyRow.change,
    totalTradedVolume: niftyRow.totalTradedVolume,
    totalTradedValue: niftyRow.totalTradedValue,
    ffmc_sum: niftyRow.ffmc,
    timeVal: contributorsPayload.timestamp || niftyRow.lastUpdateTime || '',
  };
};

const buildMarketStatusFallback = (contributorsPayload) => {
  const rows = Array.isArray(contributorsPayload.data) ? contributorsPayload.data : [];
  const niftyRow = rows.find((item) => item.symbol === 'NIFTY 50') || rows[0] || {};
  return {
    market: 'Capital Market',
    marketStatus: getMarketSessionLabel(getMarketSessionState()),
    tradeDate: contributorsPayload.timestamp || niftyRow.lastUpdateTime || '',
    index: contributorsPayload.name || niftyRow.symbol || 'NIFTY 50',
    last: parseNumber(niftyRow.lastPrice),
    variation: parseNumber(niftyRow.change),
    percentChange: parseNumber(niftyRow.pChange),
  };
};

const normalizeMarketActivityHistory = (csvText) => {
  const lines = String(csvText || '').split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    throw new Error('No market activity history available.');
  }

  const [headerLine, ...dataLines] = lines;
  const headers = parseCsvLine(headerLine);
  const rows = dataLines.map((line) => {
    const values = parseCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] || '']));
  }).map((row) => ({
    ...row,
    tradedValueCrores: parseNumber(row.tradedValueCrores),
    tradedQuantityLakhs: parseNumber(row.tradedQuantityLakhs),
    numberOfTrades: parseNumber(row.numberOfTrades),
    totalMarketCapitalisationCrores: parseNumber(row.totalMarketCapitalisationCrores),
    ffmcCrores: parseNumber(row.ffmcCrores),
    fiiNetCrores: parseNumber(row.fiiNetCrores),
    diiNetCrores: parseNumber(row.diiNetCrores),
  })).filter((row) => row.date);

  if (!rows.length) {
    throw new Error('No market activity rows available.');
  }

  return rows;
};

const normalizeIndexSummaryHistory = (csvText) => {
  const lines = String(csvText || '').split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    throw new Error('No index summary history available.');
  }

  const [headerLine, ...dataLines] = lines;
  const headers = parseCsvLine(headerLine);
  const rows = dataLines.map((line) => {
    const values = parseCsvLine(line);
    return Object.fromEntries(headers.map((header, index) => [header, values[index] || '']));
  }).map((row) => ({
    ...row,
    last: parseNumber(row.last),
    open: parseNumber(row.open),
    high: parseNumber(row.high),
    low: parseNumber(row.low),
    previousClose: parseNumber(row.previousClose),
    change: parseNumber(row.change),
    percentChange: parseNumber(row.percentChange),
    totalTradedValue: parseNumber(row.totalTradedValue),
    totalTradedValueCrores: parseNumber(row.totalTradedValue) / 10000000,
    totalTradedVolume: parseNumber(row.totalTradedVolume),
    ffmcSum: parseNumber(row.ffmcSum),
  })).filter((row) => row.date);

  if (!rows.length) {
    throw new Error('No index summary history rows available.');
  }

  return rows;
};

const parseCsvLine = (line) => {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      if (inQuotes && line[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (character === ',' && !inQuotes) {
      values.push(current);
      current = '';
      continue;
    }

    current += character;
  }

  values.push(current);
  return values;
};

const normalizeHistoryDateKey = (value) => {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const match = raw.match(/^(\d{2})-([A-Za-z]{3})-(\d{4})$/);
  if (!match) {
    return raw;
  }

  const [, day, monthLabel, year] = match;
  const monthIndex = MONTHS.findIndex((label) => label.toLowerCase() === monthLabel.toLowerCase());
  if (monthIndex < 0) {
    return raw;
  }

  return `${year}-${String(monthIndex + 1).padStart(2, '0')}-${day}`;
};

const normalizeContributors = (payload) => {
  const rows = payload.data || [];
  const advance = payload.advance || {};
  const niftyRow = rows.find((item) => item.symbol === 'NIFTY 50');
  if (!niftyRow) {
    throw new Error('NIFTY 50 row missing in contributor payload.');
  }

  const niftyPreviousClose = parseNumber(niftyRow.previousClose);
  const niftyFfmc = parseNumber(niftyRow.ffmc) || 1;
  const transformed = rows
    .filter((row) => row.symbol !== 'NIFTY 50')
    .map((row) => {
      const ffmc = parseNumber(row.ffmc);
      const pctChange = parseNumber(row.pChange);
      return {
        symbol: row.symbol || '',
        last: parseNumber(row.lastPrice),
        pChange: pctChange,
        tradedValueCr: parseNumber(row.totalTradedValue) / 10000000,
        contributingPoints: (niftyPreviousClose * (ffmc / niftyFfmc) * pctChange) / 10000000,
      };
    })
    .sort((left, right) => Math.abs(right.contributingPoints) - Math.abs(left.contributingPoints));

  return {
    timestamp: payload.timestamp || '',
    lastPrice: parseNumber(niftyRow.lastPrice),
    indexChange: parseNumber(niftyRow.pChange),
    totalPoints: transformed.reduce((sum, row) => sum + row.contributingPoints, 0),
    positiveSum: transformed.filter((row) => row.contributingPoints > 0).reduce((sum, row) => sum + row.contributingPoints, 0),
    negativeSum: transformed.filter((row) => row.contributingPoints < 0).reduce((sum, row) => sum + row.contributingPoints, 0),
    advances: Number(advance.advances || 0),
    declines: Number(advance.declines || 0),
    rows: transformed,
  };
};

const normalizeFiiDii = (payload) => {
  const rows = Array.isArray(payload) ? payload : payload.data || [];
  const fii = rows.find((item) => item.category === 'FII/FPI') || {};
  const dii = rows.find((item) => item.category === 'DII') || {};
  return {
    date: fii.date || dii.date || '',
    fiiNet: parseNumber(fii.netValue),
    diiNet: parseNumber(dii.netValue),
  };
};

const extractOpenInterest = (payload) => {
  const records = payload.records || {};
  const data = records.data || [];
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

  const positioning = buildPositioningChanges(normalizedStrikes);

  return {
    timestamp: records.timestamp || '',
    spot,
    expiry,
    strikes: normalizedStrikes,
    positioning,
    rawRecords: data,
  };
};

const buildPositioningChanges = (strikes) => {
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
      return {
        strikePrice: current.strikePrice,
        netChange: currentNetChange,
      };
    }

    return Math.abs(currentNetChange) > Math.abs(best.netChange)
      ? {
        strikePrice: current.strikePrice,
        netChange: currentNetChange,
      }
      : best;
  }, null);

  const averageIvSkew = strikes.length ? totals.ivSkewTotal / strikes.length : 0;
  const directionLabel = totals.netPositioningChange > 0 && totals.flowImbalance > 0
    ? 'Bullish positioning build'
    : totals.netPositioningChange < 0 && totals.flowImbalance < 0
      ? 'Bearish positioning build'
      : totals.netPositioningChange > 0
        ? 'Put-side hedge build'
        : totals.netPositioningChange < 0
          ? 'Call-side hedge build'
          : 'Balanced positioning';
  const strongestShiftLabel = strongestShift
    ? `${strongestShift.strikePrice} (${signed(strongestShift.netChange)})`
    : '-';
  const note = strongestShift
    ? `${directionLabel} led by ${strongestShift.strikePrice}, with net OI shift ${signed(totals.netPositioningChange)}, flow imbalance ${signed(totals.flowImbalance)}, and IV skew ${signed(averageIvSkew)}%.`
    : 'Positioning changes are waiting for the next option-chain refresh.';

  return {
    netPositioningChange: totals.netPositioningChange,
    flowImbalance: totals.flowImbalance,
    ivSkew: averageIvSkew,
    directionLabel,
    strongestShiftLabel,
    note,
  };
};

const signalDirection = (value) => (value > 0 ? 1 : value < 0 ? -1 : 0);

const buildOpenInterestSignals = ({ openInterest, contributors, fiiDii, indexSummary }) => {
  const priceDirection = signalDirection(indexSummary.percentChange);
  const positioningDirection = signalDirection(openInterest.positioning.netPositioningChange);
  const breadthDirection = signalDirection(contributors.advances - contributors.declines);
  const institutionalDirection = signalDirection((fiiDii.fiiNet || 0) + (fiiDii.diiNet || 0));
  const divergences = [];

  if (priceDirection > 0 && positioningDirection < 0) divergences.push('Price up vs defensive options');
  if (priceDirection < 0 && positioningDirection > 0) divergences.push('Price down vs supportive options');
  if (priceDirection > 0 && breadthDirection < 0) divergences.push('Price up vs weak breadth');
  if (priceDirection < 0 && breadthDirection > 0) divergences.push('Price down vs resilient breadth');
  if (priceDirection > 0 && institutionalDirection < 0) divergences.push('Price up vs net negative institutions');
  if (priceDirection < 0 && institutionalDirection > 0) divergences.push('Price down vs supportive institutions');

  const nearSpotRows = (openInterest.strikes || []).filter((row) => Math.abs(row.strikePrice - openInterest.spot) <= 150);
  const trapCandidates = nearSpotRows.flatMap((row) => {
    const upsideScore = row.strikePrice >= openInterest.spot && row.callOIChange > row.putOIChange && row.callBidAsk < 0
      ? row.callOIChange + Math.abs(row.callBidAsk)
      : 0;
    const downsideScore = row.strikePrice <= openInterest.spot && row.putOIChange > row.callOIChange && row.putBidAsk < 0
      ? row.putOIChange + Math.abs(row.putBidAsk)
      : 0;

    return [
      upsideScore > 0 ? { label: `Upside trap @ ${row.strikePrice}`, score: upsideScore, tone: 'negative' } : null,
      downsideScore > 0 ? { label: `Downside trap @ ${row.strikePrice}`, score: downsideScore, tone: 'positive' } : null,
    ].filter(Boolean);
  });

  const topTrap = trapCandidates.reduce((best, current) => {
    if (!best) return current;
    return current.score > best.score ? current : best;
  }, null);

  const divergenceSummary = divergences.length
    ? divergences[0]
    : 'Aligned sentiment';
  const trapRiskLabel = topTrap?.label || 'No clear trap';
  const trapRiskTone = topTrap?.tone || '';
  const noteParts = [];

  noteParts.push(divergenceSummary);
  if (divergences.length > 1) {
    noteParts.push(`${divergences.length} divergences active`);
  }
  if (topTrap) {
    noteParts.push(`${topTrap.label} near spot`);
  }

  return {
    divergenceScore: divergences.length,
    divergenceSummary,
    trapRiskLabel,
    trapRiskTone,
    note: noteParts.join('. ') + '.',
  };
};

const buildBlackScholes = (openInterestPayload) => {
  const rawRecords = openInterestPayload.rawRecords || [];
  const expiryDate = parseDisplayExpiry(openInterestPayload.expiry);
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const daysToExpiry = Math.max(Math.round((expiryDate.getTime() - today.getTime()) / 86400000), 1);
  const years = daysToExpiry / 365;
  const rate = 0.1;

  return {
    timestamp: openInterestPayload.timestamp,
    spot: openInterestPayload.spot,
    expiry: openInterestPayload.expiry,
    rows: openInterestPayload.strikes.map((strikeRow) => {
      const ce = (rawRecords.find((item) => item.CE?.strikePrice === strikeRow.strikePrice) || {}).CE || {};
      const pe = (rawRecords.find((item) => item.PE?.strikePrice === strikeRow.strikePrice) || {}).PE || {};
      const callIv = parseNumber(ce.impliedVolatility) / 100 || 0.2;
      const putIv = parseNumber(pe.impliedVolatility) / 100 || 0.2;
      return {
        strikePrice: strikeRow.strikePrice,
        call: {
          marketPrice: parseNumber(ce.lastPrice),
          bsValue: blackScholes(openInterestPayload.spot, strikeRow.strikePrice, years, rate, callIv, 'call'),
          iv: parseNumber(ce.impliedVolatility),
        },
        put: {
          marketPrice: parseNumber(pe.lastPrice),
          bsValue: blackScholes(openInterestPayload.spot, strikeRow.strikePrice, years, rate, putIv, 'put'),
          iv: parseNumber(pe.impliedVolatility),
        },
      };
    }),
  };
};

const buildLowIv = (payload) => {
  const records = payload.records || {};
  const data = records.data || [];
  const spot = parseNumber(records.underlyingValue);
  const expiries = [...new Set(data.map((item) => formatExpiryLabel(item.CE?.expiryDate || item.PE?.expiryDate)).filter(Boolean))].slice(0, 4);
  const rowsByExpiry = {};

  expiries.forEach((expiry) => {
    const rows = [];
    data.forEach((item) => {
      const contractExpiry = formatExpiryLabel(item.CE?.expiryDate || item.PE?.expiryDate);
      const strike = item.CE?.strikePrice || item.PE?.strikePrice;
      if (contractExpiry !== expiry || !strike || Math.abs(strike - spot) > 1000) {
        return;
      }

      [['CE', item.CE], ['PE', item.PE]].forEach(([type, contract]) => {
        if (!contract) {
          return;
        }

        const iv = parseNumber(contract.impliedVolatility);
        const volume = parseNumber(contract.totalTradedVolume);
        if (iv <= 0 || volume <= 0) {
          return;
        }

        rows.push({
          type,
          strike,
          expiry,
          iv,
          oiChangePercent: parseNumber(contract.pchangeinOpenInterest),
          lastPrice: parseNumber(contract.lastPrice),
          oi: parseNumber(contract.openInterest),
        });
      });
    });

    rows.sort((left, right) => left.iv - right.iv);
    rowsByExpiry[expiry] = rows.slice(0, 10);
  });

  return {
    timestamp: records.timestamp || '',
    expiries,
    defaultExpiry: expiries[0] || '',
    rowsByExpiry,
  };
};

const renderHolidayCalendar = (holidayPayload) => {
  setText('holidayStamp', formatDateTime(holidayPayload.updatedAt) || '-');
  setText('holidayMeta', 'Exchange holidays are marked in red. Saturdays and Sundays are treated as market holidays.');
  document.getElementById('holidayCalendarGrid').innerHTML = holidayPayload.months.map((month) => `
    <article class="holiday-month">
      <h3>${month.label}</h3>
      <div class="holiday-weekdays">${WEEKDAYS.map((day) => `<span>${day}</span>`).join('')}</div>
      <div class="holiday-days">${month.cells.map((cell) => {
        if (!cell) {
          return '<div class="holiday-day empty"></div>';
        }
        const classes = ['holiday-day'];
        if (cell.isWeekend) classes.push('weekend');
        if (cell.isHoliday) classes.push('holiday');
        return `<div class="${classes.join(' ')}" title="${escapeHtml(cell.descriptions.join(' | '))}"><span>${cell.day}</span>${cell.isHoliday ? '<small>Closed</small>' : ''}</div>`;
      }).join('')}</div>
      ${month.holidays.length ? `<ul class="holiday-list">${month.holidays.map((holiday) => `
        <li class="holiday-item">
          <span class="holiday-item-date">${holiday.tradingDate} • ${holiday.weekDay}</span>
          <span class="holiday-item-text">${escapeHtml(holiday.description)}</span>
        </li>
      `).join('')}</ul>` : '<p class="holiday-month-note">Weekends are highlighted as non-trading days.</p>'}
    </article>
  `).join('');
};

const renderMetricGrid = (id, metrics) => {
  document.getElementById(id).innerHTML = metrics.map((metric) => `
    <article class="metric">
      <div class="metric-label">${metric.label}</div>
      <div class="metric-value ${metric.tone || ''}">${metric.value}</div>
    </article>
  `).join('');
};

const renderChipList = (id, items) => {
  document.getElementById(id).innerHTML = items.map((item) => `<li>${item}</li>`).join('');
};

const getMarketTapeCategory = (item) => String(item?.source || '').split('/').pop()?.trim() || 'International';

const buildMarketTapeHighlights = (items) => {
  const orderedCategories = ['Indices', 'Futures', 'Commodities', 'Currencies'];
  const parts = orderedCategories.map((category) => {
    const categoryItems = items.filter((item) => getMarketTapeCategory(item) === category);
    if (!categoryItems.length) {
      return null;
    }

    const topMover = categoryItems.reduce((best, current) => {
      if (!best) return current;
      return Math.abs(current.changePercent) > Math.abs(best.changePercent) ? current : best;
    }, null);

    if (!topMover) {
      return null;
    }

    const shortLabel = String(topMover.label || '').replace(/\s*\((Open|Close)\)\s*$/i, '').trim();
    return `${category} - ${shortLabel} ${signed(topMover.changePercent)}%`;
  }).filter(Boolean);

  return parts.length ? `Top Movements: ${parts.join(' , ')}` : 'Top Movements: Waiting for world markets.';
};

const renderMarketTape = (items) => {
  const host = document.getElementById('marketTape');
  if (!items.length) {
    setText('marketTapeHighlights', 'Top Movements: Waiting for world markets.');
    host.innerHTML = '<p>No market tape available.</p>';
    return;
  }

  const chips = items.map((item) => `
    <article class="ticker-chip ${tone(item.changePercent)}" tabindex="0">
      <span class="ticker-label">${item.label}</span>
      <span class="ticker-value">${formatTickerValue(item.last)}</span>
      <span class="ticker-change ${tone(item.changePercent)}">${signed(item.changePercent)}%</span>
    </article>
  `).join('');

  setText('marketTapeMeta', 'Global market pulse for the current session. Hover to pause the tape and read each move.');
  setText('marketTapeHighlights', buildMarketTapeHighlights(items));

  host.innerHTML = `
    <div class="ticker-marquee">
      <div class="ticker-track">${chips}${chips}</div>
    </div>
  `;
};

const renderNewsFeed = (newsFeed) => {
  const items = newsFeed.items || [];
  const list = document.getElementById('newsFeedList');
  if (!items.length) {
    list.innerHTML = '<li class="news-feed-item">No headlines available.</li>';
    return;
  }

  list.innerHTML = items.map((item) => `
    <li class="news-feed-item">
      <a href="${item.link}" target="_blank" rel="noreferrer">
        <span class="news-source">${item.source}</span>
        <span class="news-title">${item.title}</span>
        <span class="news-time">${escapeHtml(item.publishedAtLabel || formatDateTime(item.publishedAt))}</span>
      </a>
    </li>
  `).join('');
};

const renderBarChart = (id, rows, mapper, legendId, title, expanded = false) => {
  if (!rows.length) {
    document.getElementById(id).innerHTML = '<p>No data available.</p>';
    return;
  }

  const mapped = rows.map(mapper);
  const maxValue = Math.max(...mapped.flatMap((item) => [Math.abs(item.valueA), Math.abs(item.valueB), 1]));
  const width = expanded ? 1400 : 960;
  const height = expanded ? 520 : 360;
  const baseline = Math.round(height / 2);
  const left = 60;
  const groupWidth = (width - left - 20) / mapped.length;
  const barWidth = Math.max(expanded ? 12 : 8, groupWidth / 3);

  const bars = mapped.map((item, index) => {
    const x = left + index * groupWidth + groupWidth * 0.15;
    return [
      buildSvgRect(x, baseline, barWidth, item.valueA, maxValue, item.colorA, expanded),
      buildSvgRect(x + barWidth + 6, baseline, barWidth, item.valueB, maxValue, item.colorB, expanded),
      `<text x="${x + barWidth}" y="${height - 12}" text-anchor="middle" font-size="${expanded ? 15 : 11}" fill="#4d6971">${item.label}</text>`,
    ].join('');
  }).join('');

  if (legendId) {
    document.getElementById(legendId).innerHTML = renderLegend(mapped[0]);
  }

  chartState[id] = {
    title,
    legendHtml: renderLegend(mapped[0]),
    render: (targetId, expandedMode) => renderBarChart(targetId, rows, mapper, null, title, expandedMode),
  };

  document.getElementById(id).innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${title}">
      <line x1="${left}" x2="${width - 10}" y1="${baseline}" y2="${baseline}" stroke="rgba(15,76,92,0.18)" stroke-width="1.2"></line>
      <text x="18" y="24" font-size="${expanded ? 14 : 11}" fill="#4d6971">${formatCompact(maxValue)}</text>
      <text x="18" y="${baseline - 6}" font-size="${expanded ? 14 : 11}" fill="#4d6971">0</text>
      <text x="18" y="${height - 42}" font-size="${expanded ? 14 : 11}" fill="#4d6971">-${formatCompact(maxValue)}</text>
      ${bars}
    </svg>
  `;
};

const renderSingleSeriesChart = (id, rows, mapper, legendId, title, expanded = false) => {
  if (!rows.length) {
    document.getElementById(id).innerHTML = '<p>No data available.</p>';
    return;
  }

  const mapped = rows.map(mapper);
  const maxValue = Math.max(...mapped.map((item) => Math.abs(item.value) || 0), 1);
  const width = expanded ? 1400 : 960;
  const height = expanded ? 520 : 360;
  const top = 24;
  const baseline = height - 48;
  const left = 60;
  const groupWidth = (width - left - 20) / mapped.length;
  const barWidth = Math.max(expanded ? 8 : 6, Math.min(expanded ? 18 : 12, groupWidth * 0.34));

  const bars = mapped.map((item, index) => {
    const scaledHeight = (Math.abs(item.value) / maxValue) * (expanded ? 360 : 240);
    const x = left + index * groupWidth + (groupWidth - barWidth) / 2;
    const y = baseline - scaledHeight;
    return [
      `<rect x="${x}" y="${y}" width="${barWidth}" height="${scaledHeight}" rx="6" fill="${item.color}" opacity="0.9"></rect>`,
      `<text x="${x + barWidth / 2}" y="${height - 12}" text-anchor="middle" font-size="${expanded ? 15 : 11}" font-weight="700" fill="#26444d">${item.label}</text>`,
    ].join('');
  }).join('');

  const legendHtml = `<div class="legend-item"><span class="legend-swatch" style="background:${mapped[0].color}"></span>${mapped[0].labelA}</div>`;
  if (legendId) {
    document.getElementById(legendId).innerHTML = legendHtml;
  }

  chartState[id] = {
    title,
    legendHtml,
    render: (targetId, expandedMode) => renderSingleSeriesChart(targetId, rows, mapper, null, title, expandedMode),
  };

  document.getElementById(id).innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${title}">
      <line x1="${left}" x2="${width - 10}" y1="${baseline}" y2="${baseline}" stroke="rgba(15,76,92,0.18)" stroke-width="1.2"></line>
      <text x="18" y="24" font-size="${expanded ? 14 : 11}" font-weight="700" fill="#26444d">${formatLargeCompact(maxValue)}</text>
      <text x="18" y="${baseline - 6}" font-size="${expanded ? 14 : 11}" font-weight="700" fill="#26444d">0</text>
      ${bars}
    </svg>
  `;
};

const renderGroupedSeriesChart = (id, rows, mapper, legendId, title, expanded = false) => {
  if (!rows.length) {
    document.getElementById(id).innerHTML = '<p>No data available.</p>';
    return;
  }

  const mapped = rows.map(mapper);
  const seriesCount = Math.max(...mapped.map((item) => item.series.length), 1);
  const maxValue = Math.max(
    ...mapped.flatMap((item) => item.series.map((series) => Math.abs(series.value || 0))),
    1,
  );
  const hasNegative = mapped.some((item) => item.series.some((series) => (series.value || 0) < 0));
  const width = expanded ? 1440 : 980;
  const height = expanded ? 520 : 360;
  const top = 28;
  const bottom = 56;
  const left = 72;
  const chartHeight = height - top - bottom;
  const baseline = hasNegative ? top + chartHeight / 2 : height - bottom;
  const groupWidth = (width - left - 24) / mapped.length;
  const gap = expanded ? 4 : 3;
  const barWidth = Math.max(
    expanded ? 5 : 4,
    Math.min(expanded ? 14 : 9, (groupWidth - gap * (seriesCount - 1)) / Math.max(seriesCount, 1)),
  );
  const usedWidth = seriesCount * barWidth + (seriesCount - 1) * gap;

  const bars = mapped.map((item, index) => {
    const startX = left + index * groupWidth + Math.max((groupWidth - usedWidth) / 2, 0);
    const seriesRects = item.series.map((series, seriesIndex) => {
      if (!series.value) {
        return '';
      }

      let scaledHeight = (Math.abs(series.value) / maxValue) * (hasNegative ? chartHeight / 2 : chartHeight);
      if (scaledHeight > 0 && scaledHeight < 3) {
        scaledHeight = 3;
      }

      const x = startX + seriesIndex * (barWidth + gap);
      const y = series.value >= 0 ? baseline - scaledHeight : baseline;
      return `<rect x="${x}" y="${y}" width="${barWidth}" height="${scaledHeight}" rx="4" fill="${series.color}" opacity="0.92"></rect>`;
    }).join('');

    return `${seriesRects}<text x="${left + index * groupWidth + groupWidth / 2}" y="${height - 16}" text-anchor="middle" font-size="${expanded ? 15 : 11}" font-weight="700" fill="#26444d">${item.label}</text>`;
  }).join('');

  const legendHtml = mapped[0].series
    .map((series) => `<div class="legend-item"><span class="legend-swatch" style="background:${series.color}"></span>${series.label}</div>`)
    .join('');

  if (legendId) {
    document.getElementById(legendId).innerHTML = legendHtml;
  }

  chartState[id] = {
    title,
    legendHtml,
    render: (targetId, expandedMode) => renderGroupedSeriesChart(targetId, rows, mapper, null, title, expandedMode),
  };

  document.getElementById(id).innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${title}">
      <line x1="${left}" x2="${width - 10}" y1="${baseline}" y2="${baseline}" stroke="rgba(15,76,92,0.22)" stroke-width="1.4"></line>
      <text x="18" y="26" font-size="${expanded ? 15 : 11}" font-weight="700" fill="#26444d">${formatCompact(maxValue)}</text>
      <text x="18" y="${baseline - 8}" font-size="${expanded ? 15 : 11}" font-weight="700" fill="#26444d">0</text>
      ${hasNegative ? `<text x="18" y="${height - 18}" font-size="${expanded ? 15 : 11}" font-weight="700" fill="#26444d">-${formatCompact(maxValue)}</text>` : ''}
      ${bars}
    </svg>
  `;
};

const renderStackedSeriesChart = (id, rows, mapper, legendId, title, expanded = false) => {
  if (!rows.length) {
    document.getElementById(id).innerHTML = '<p>No data available.</p>';
    return;
  }

  const mapped = rows.map(mapper);
  const maxValue = Math.max(
    ...mapped.map((item) => item.segments.reduce((sum, segment) => sum + Math.abs(segment.value || 0), 0)),
    1,
  );
  const width = expanded ? 1400 : 960;
  const height = expanded ? 520 : 360;
  const top = 24;
  const baseline = height - 48;
  const left = 60;
  const groupWidth = (width - left - 20) / mapped.length;
  const barWidth = Math.max(expanded ? 30 : 22, groupWidth * 0.58);
  const chartHeight = baseline - top;

  const bars = mapped.map((item, index) => {
    const total = item.segments.reduce((sum, segment) => sum + Math.abs(segment.value || 0), 0);
    const x = left + index * groupWidth + (groupWidth - barWidth) / 2;
    let currentY = baseline;

    const segments = item.segments.map((segment) => {
      if (!segment.value) {
        return '';
      }

      let scaledHeight = (Math.abs(segment.value) / maxValue) * chartHeight;
      if (scaledHeight > 0 && scaledHeight < 3) {
        scaledHeight = 3;
      }

      currentY -= scaledHeight;
      return `<rect x="${x}" y="${currentY}" width="${barWidth}" height="${scaledHeight}" rx="6" fill="${segment.color}" opacity="0.92"></rect>`;
    }).join('');

    return [
      segments,
      `<text x="${x + barWidth / 2}" y="${height - 12}" text-anchor="middle" font-size="${expanded ? 15 : 11}" fill="#4d6971">${item.label}</text>`,
      `<text x="${x + barWidth / 2}" y="${Math.max(currentY - 8, top + 12)}" text-anchor="middle" font-size="${expanded ? 13 : 10}" fill="#14313b">${formatCompact(total)}</text>`,
    ].join('');
  }).join('');

  const legendHtml = OPTION_CHAIN_MARKET_ACTIVITY_SERIES
    .map((series) => `<div class="legend-item"><span class="legend-swatch" style="background:${series.color}"></span>${series.label}</div>`)
    .join('');

  if (legendId) {
    document.getElementById(legendId).innerHTML = legendHtml;
  }

  chartState[id] = {
    title,
    legendHtml,
    render: (targetId, expandedMode) => renderStackedSeriesChart(targetId, rows, mapper, null, title, expandedMode),
  };

  document.getElementById(id).innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="${title}">
      <line x1="${left}" x2="${width - 10}" y1="${baseline}" y2="${baseline}" stroke="rgba(15,76,92,0.18)" stroke-width="1.2"></line>
      <text x="18" y="24" font-size="${expanded ? 14 : 11}" fill="#4d6971">${formatCompact(maxValue)}</text>
      <text x="18" y="${baseline - 6}" font-size="${expanded ? 14 : 11}" fill="#4d6971">0</text>
      ${bars}
    </svg>
  `;
};

const buildSvgRect = (x, baseline, width, value, maxValue, color, expanded) => {
  const scaledHeight = (Math.abs(value) / maxValue) * (expanded ? 210 : 145);
  const y = value >= 0 ? baseline - scaledHeight : baseline;
  return `<rect x="${x}" y="${y}" width="${width}" height="${scaledHeight}" rx="5" fill="${color}" opacity="0.88"></rect>`;
};

const renderLegend = (sample) => `
  <div class="legend-item"><span class="legend-swatch" style="background:${sample.colorA}"></span>${sample.labelA}</div>
  <div class="legend-item"><span class="legend-swatch" style="background:${sample.colorB}"></span>${sample.labelB}</div>
`;

const bindChartExpansion = () => {
  document.querySelectorAll('.chart-panel').forEach((panel) => {
    panel.onclick = (event) => {
      if (event.target.closest('button')) {
        return;
      }
      openChartModal(panel.dataset.chartTarget, panel.dataset.chartTitle);
    };
  });
  document.querySelectorAll('.chart-expand').forEach((button) => {
    button.onclick = () => openChartModal(button.dataset.chartTarget, button.dataset.chartTitle);
  });
  document.getElementById('chartModalClose').onclick = closeChartModal;
  document.querySelectorAll('[data-close-modal="true"]').forEach((element) => {
    element.onclick = closeChartModal;
  });
};

const openChartModal = (chartId, title) => {
  const modal = document.getElementById('chartModal');
  const modalLegend = document.getElementById('chartModalLegend');
  const state = chartState[chartId];
  if (!state) {
    return;
  }
  document.getElementById('chartModalTitle').textContent = title;
  modal.hidden = false;
  document.body.classList.add('modal-open');
  modalLegend.innerHTML = state.legendHtml || '';
  state.render('chartModalBody', true);
};

const closeChartModal = () => {
  document.getElementById('chartModal').hidden = true;
  document.body.classList.remove('modal-open');
};

const renderLowIv = (lowIv, selectedExpiry) => {
  const select = document.getElementById('lowIvExpirySelect');
  const expiries = lowIv.expiries || [];
  select.innerHTML = expiries.map((expiry) => `<option value="${expiry}">${expiry}</option>`).join('');
  if (!expiries.length) {
    document.getElementById('lowIvRows').innerHTML = '<tr><td colspan="6">No low IV data available.</td></tr>';
    renderMetricGrid('lowIvSummary', [
      { label: 'Tracked Expiries', value: '0' },
      { label: 'Selected Expiry', value: '-' },
      { label: 'Lowest IV', value: '-' },
      { label: 'Cheapest Contract', value: '-' },
    ]);
    return;
  }
  select.value = selectedExpiry;
  select.onchange = () => updateLowIvRows(lowIv, select.value);
  updateLowIvRows(lowIv, selectedExpiry);
};

const updateLowIvRows = (lowIv, expiry) => {
  const rows = lowIv.rowsByExpiry?.[expiry] || [];
  document.getElementById('lowIvRows').innerHTML = rows.map((row) => `
    <tr>
      <td>${row.type}</td>
      <td>${row.strike}</td>
      <td>${row.expiry}</td>
      <td>${formatNumber(row.iv)}%</td>
      <td class="${tone(row.oiChangePercent)}">${signed(row.oiChangePercent)}%</td>
      <td>${formatNumber(row.lastPrice)}</td>
    </tr>
  `).join('');
  renderMetricGrid('lowIvSummary', [
    { label: 'Tracked Expiries', value: String(lowIv.expiries.length) },
    { label: 'Selected Expiry', value: expiry || '-' },
    { label: 'Lowest IV', value: rows[0] ? `${rows[0].iv.toFixed(2)}%` : '-' },
    { label: 'Cheapest Contract', value: lowestLabel(rows) },
  ]);
};

const parseNumber = (value) => {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number') return value;
  const cleaned = String(value).replace(/,/g, '').replace(/%/g, '').trim();
  return cleaned ? Number(cleaned) || 0 : 0;
};

const formatExpiryLabel = (value) => {
  if (!value) return '';
  if (value.includes('-') && value.split('-')[1]?.length === 3) {
    return value;
  }
  const parts = value.split('-');
  if (parts.length !== 3) return value;
  const [day, month, year] = parts;
  return `${day}-${MONTHS[Number(month) - 1] || month}-${year}`;
};

const parseDisplayExpiry = (value) => {
  const parts = (value || '').split('-');
  if (parts.length !== 3) {
    return new Date(Date.now() + 86400000);
  }
  const monthIndex = MONTHS.indexOf(parts[1]);
  return new Date(Number(parts[2]), monthIndex >= 0 ? monthIndex : 0, Number(parts[0]));
};

const getFirstExpiry = (rows) => {
  const sample = rows.find((item) => item.CE?.expiryDate || item.PE?.expiryDate) || {};
  return sample.CE?.expiryDate || sample.PE?.expiryDate || '';
};

const getLowestIvLabel = (lowIv) => {
  const rows = lowIv.rowsByExpiry?.[lowIv.defaultExpiry] || [];
  return rows[0] ? `${rows[0].type} ${rows[0].strike} @ ${rows[0].iv.toFixed(2)}%` : '-';
};

const blackScholes = (spot, strike, years, rate, sigma, optionType) => {
  const normalizedSigma = sigma > 0 ? sigma : 0.2;
  const normalizedYears = years > 0 ? years : 1 / 365;
  const d1 = (Math.log(spot / strike) + (rate + 0.5 * normalizedSigma * normalizedSigma) * normalizedYears) / (normalizedSigma * Math.sqrt(normalizedYears));
  const d2 = d1 - normalizedSigma * Math.sqrt(normalizedYears);
  if (optionType === 'call') {
    return spot * normCdf(d1) - strike * Math.exp(-rate * normalizedYears) * normCdf(d2);
  }
  return strike * Math.exp(-rate * normalizedYears) * normCdf(-d2) - spot * normCdf(-d1);
};

const normCdf = (value) => 0.5 * (1 + erf(value / Math.sqrt(2)));

const erf = (value) => {
  const sign = value >= 0 ? 1 : -1;
  const absolute = Math.abs(value);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + p * absolute);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absolute * absolute);
  return sign * y;
};

const formatDateTime = (isoString) => {
  if (!isoString) return '-';
  const date = parseFeedDate(isoString);
  return !date ? isoString : new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIMEZONE,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }).format(date).replace(',', '') + ' IST';
};

const formatNumber = (value) => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
  return currency.format(Number(value));
};

const formatCompact = (value) => {
  const abs = Math.abs(Number(value));
  if (abs >= 10000000) return `${(value / 1000000).toFixed(1)}M`;
  if (abs >= 100000) return `${(value / 100000).toFixed(1)}L`;
  if (abs >= 1000) return `${(value / 1000).toFixed(1)}K`;
  return Number(value).toFixed(0);
};

const formatLargeCompact = (value) => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
  return compactNumber.format(Number(value));
};

const formatArchiveDateLabel = (value) => {
  if (!value) return '-';
  const [day, month] = String(value).split('-');
  return `${day} ${month || ''}`.trim();
};

const formatTickerValue = (value) => {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'string') return value;
  if (Number.isNaN(Number(value))) return '-';
  const number = Number(value);
  return Math.abs(number) >= 1000 ? currency.format(number) : number.toFixed(2);
};

const signed = (value) => {
  const number = Number(value);
  if (Number.isNaN(number)) return '-';
  return `${number > 0 ? '+' : ''}${number.toFixed(2)}`;
};

const tone = (value) => {
  const number = Number(value);
  if (Number.isNaN(number) || number === 0) return '';
  return number > 0 ? 'positive' : 'negative';
};

const rowColors = (pctChange) => {
  const number = Number(pctChange) || 0;
  const absChange = Math.min(Math.abs(number), 10) / 10;
  const intensity = Math.sqrt(absChange);
  if (number > 0) {
    return `background: rgba(200,255,${Math.floor(200 - 50 * (1 - intensity))},0.38);`;
  }
  if (number < 0) {
    return `background: rgba(255,${Math.floor(200 - 40 * intensity)},${Math.floor(200 - 40 * intensity)},0.38);`;
  }
  return '';
};

const averageGap = (rows, side) => {
  if (!rows.length) return 0;
  return rows.reduce((sum, row) => sum + (row[side].marketPrice - row[side].bsValue), 0) / rows.length;
};

const peakLabel = (rows, key) => {
  if (!rows.length) return '-';
  const best = [...rows].sort((left, right) => Math.abs(right[key]) - Math.abs(left[key]))[0];
  return `${best.strikePrice} (${signed(best[key])})`;
};

const lowestLabel = (rows) => {
  if (!rows.length) return '-';
  const best = [...rows].sort((left, right) => left.lastPrice - right.lastPrice)[0];
  return `${best.type} ${best.strike}`;
};

const parseEmbeddedJson = (text) => {
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('Holiday payload could not be parsed.');
  }
  return JSON.parse(text.slice(start, end + 1));
};

const normalizeHolidayPayload = (records) => {
  const deduped = [];
  const seen = new Set();
  records.forEach((record) => {
    const key = `${record.tradingDate}-${record.description}`;
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(record);
    }
  });

  const year = deduped.length ? Number(deduped[0].tradingDate.split('-')[2]) : new Date().getFullYear();
  const holidayMap = new Map();
  deduped.forEach((record) => {
    const date = parseTradingDate(record.tradingDate);
    const key = toDateKey(date);
    if (!holidayMap.has(key)) {
      holidayMap.set(key, []);
    }
    holidayMap.get(key).push(record.description);
  });

  return {
    updatedAt: new Date().toISOString(),
    year,
    months: MONTHS.map((label, monthIndex) => buildHolidayMonth(label, monthIndex, year, deduped, holidayMap)),
  };
};

const buildHolidayMonth = (label, monthIndex, year, records, holidayMap) => {
  const firstDay = new Date(year, monthIndex, 1);
  const lastDay = new Date(year, monthIndex + 1, 0);
  const monthRecords = records.filter((record) => parseTradingDate(record.tradingDate).getMonth() === monthIndex);
  const cells = [];

  for (let filler = 0; filler < firstDay.getDay(); filler += 1) {
    cells.push(null);
  }

  for (let day = 1; day <= lastDay.getDate(); day += 1) {
    const date = new Date(year, monthIndex, day);
    const key = toDateKey(date);
    const isWeekend = date.getDay() === 0 || date.getDay() === 6;
    const descriptions = holidayMap.get(key) || (isWeekend ? ['Weekend'] : []);
    cells.push({
      day,
      isWeekend,
      isHoliday: descriptions.length > 0,
      descriptions,
    });
  }

  return {
    label,
    cells,
    holidays: monthRecords,
  };
};

const parseTradingDate = (value) => {
  const [day, month, year] = String(value || '').split('-');
  return new Date(Number(year), MONTHS.indexOf(month), Number(day));
};

const toDateKey = (date) => `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;

const escapeHtml = (value) => String(value)
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;');

const normalizeHeadlineKey = (value) => cleanHeadlineTitle(value || '', '')
  .toLowerCase()
  .replace(/&amp;/g, 'and')
  .replace(/\b(business standard|livemint|bloomberg economics|bloomberg)\b/g, '')
  .replace(/\s*[|:-]\s*(live|latest|update|updates)\b.*$/g, '')
  .replace(/[^a-z0-9]+/g, ' ')
  .trim();

const parseStooqQuoteLine = (rawText, label) => {
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
};

const formatMarkdownInline = (value) => escapeHtml(value)
  .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
  .replace(/__(.+?)__/g, '<strong>$1</strong>')
  .replace(/\*(.+?)\*/g, '<em>$1</em>')
  .replace(/_(.+?)_/g, '<em>$1</em>')
  .replace(/`(.+?)`/g, '<code>$1</code>');

const renderMarkdown = (value) => {
  const lines = String(value || '').replace(/\r\n/g, '\n').split('\n');
  const blocks = [];
  let paragraph = [];
  let listType = null;
  let listItems = [];

  const flushParagraph = () => {
    if (!paragraph.length) {
      return;
    }
    blocks.push(`<p>${formatMarkdownInline(paragraph.join(' '))}</p>`);
    paragraph = [];
  };

  const flushList = () => {
    if (!listItems.length || !listType) {
      return;
    }
    blocks.push(`<${listType}>${listItems.map((item) => `<li>${formatMarkdownInline(item)}</li>`).join('')}</${listType}>`);
    listItems = [];
    listType = null;
  };

  lines.forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      flushParagraph();
      flushList();
      return;
    }

    const headingMatch = trimmed.match(/^(#{1,4})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph();
      flushList();
      const level = Math.min(4, headingMatch[1].length + 1);
      blocks.push(`<h${level}>${formatMarkdownInline(headingMatch[2])}</h${level}>`);
      return;
    }

    const bulletMatch = trimmed.match(/^[-*]\s+(.+)$/);
    if (bulletMatch) {
      flushParagraph();
      if (listType && listType !== 'ul') {
        flushList();
      }
      listType = 'ul';
      listItems.push(bulletMatch[1]);
      return;
    }

    const orderedMatch = trimmed.match(/^\d+[.)]\s+(.+)$/);
    if (orderedMatch) {
      flushParagraph();
      if (listType && listType !== 'ol') {
        flushList();
      }
      listType = 'ol';
      listItems.push(orderedMatch[1]);
      return;
    }

    flushList();
    paragraph.push(trimmed);
  });

  flushParagraph();
  flushList();

  return blocks.join('') || `<p>${formatMarkdownInline(String(value || ''))}</p>`;
};

const getNextAiRefreshDelay = () => {
  const now = new Date();
  for (let offset = 0; offset < 8; offset += 1) {
    const candidateDate = new Date(now);
    candidateDate.setHours(0, 0, 0, 0);
    candidateDate.setDate(candidateDate.getDate() + offset);
    if (candidateDate.getDay() === 0 || candidateDate.getDay() === 6) {
      continue;
    }

    for (const hour of AI_REFRESH_HOURS) {
      const candidate = new Date(candidateDate);
      candidate.setHours(hour, 0, 0, 0);
      if (candidate > now) {
        return candidate.getTime() - now.getTime();
      }
    }
  }
  return 3 * 60 * 60 * 1000;
};

const scheduleAiRefresh = () => {
  if (aiRefreshTimer) {
    window.clearTimeout(aiRefreshTimer);
    aiRefreshTimer = null;
  }

  aiRefreshTimer = window.setTimeout(() => {
    loadAiData().catch((error) => {
      console.error('Scheduled AI load failed', error);
    }).finally(() => {
      scheduleAiRefresh();
    });
  }, getNextAiRefreshDelay());
};

const bindDonateModal = () => {
  const modal = document.getElementById('donateModal');
  const openButton = document.getElementById('donateButton');
  const closeButton = document.getElementById('donateClose');

  if (!modal || !openButton || !closeButton) {
    return;
  }

  modal.hidden = true;
  modal.setAttribute('aria-hidden', 'true');

  const closeModal = () => {
    modal.hidden = true;
    modal.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
  };

  openButton.onclick = () => {
    modal.hidden = false;
    modal.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
  };
  closeButton.onclick = closeModal;
  modal.onclick = (event) => {
    if (event.target === modal) {
      closeModal();
    }
  };
  window.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !modal.hidden) {
      closeModal();
    }
  });
};

const bindNewsletterForm = () => {
  const form = document.getElementById('newsletterForm');
  const emailInput = document.getElementById('newsletterEmail');
  const submitButton = document.getElementById('newsletterSubmit');
  const status = document.getElementById('newsletterStatus');
  const toggleButton = document.getElementById('newsletterToggle');
  const tray = document.getElementById('newsletterTray');

  if (!form || !emailInput || !submitButton || !status) {
    return;
  }

  const setTrayState = (open) => {
    if (!tray || !toggleButton) {
      return;
    }
    tray.hidden = !open;
    toggleButton.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (open) {
      emailInput.focus();
    }
  };

  if (toggleButton && tray) {
    toggleButton.onclick = () => setTrayState(tray.hidden);
  }

  const setStatus = (message, tone = '') => {
    status.textContent = message;
    status.className = `subscribe-status${tone ? ` ${tone}` : ''}`;
  };

  if (!NEWSLETTER_SUBSCRIBE_URL) {
    setStatus('Email signup is waiting for the Supabase subscription endpoint to be configured.', 'is-error');
    submitButton.disabled = true;
    return;
  }

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const email = String(emailInput.value || '').trim();
    if (!email) {
      setStatus('Enter an email address to subscribe.', 'is-error');
      return;
    }

    submitButton.disabled = true;
    setStatus('Submitting your request...');

    try {
      const response = await fetch(NEWSLETTER_SUBSCRIBE_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/plain;charset=UTF-8',
        },
        body: JSON.stringify({
          email,
          publishableKey: SUPABASE_PUBLISHABLE_KEY || undefined,
        }),
      });

      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error || `Subscription failed: ${response.status}`);
      }

      emailInput.value = '';
      setStatus(payload?.message || 'Subscription saved. You will receive the 8 PM AI Brief newsletter on trading days.', 'is-success');
      setTrayState(false);
    } catch (error) {
      const message = error?.message === 'Failed to fetch'
        ? 'Subscription request could not reach the newsletter endpoint. Check the Edge Function deployment and CORS settings.'
        : (error?.message || 'Subscription failed. Please try again later.');
      setStatus(message, 'is-error');
    } finally {
      submitButton.disabled = false;
    }
  });
};

const syncRefreshToggle = () => {
  const toggle = document.getElementById('autoRefreshToggle');
  if (!toggle) {
    return;
  }
  toggle.checked = true;
  toggle.onchange = () => {
    if (toggle.checked) {
      startRealtimeRefresh();
      loadRealtimeData().catch((error) => {
        console.error('Realtime load failed', error);
      });
      return;
    }
    stopRealtimeRefresh();
  };
};

const startRealtimeRefresh = () => {
  stopRealtimeRefresh();
  realtimeRefreshTimer = window.setInterval(() => {
    loadRealtimeData({ preferLive: (Date.now() - lastMarketTapeLoadAt) >= MARKET_TAPE_REFRESH_MS }).catch((error) => {
      console.error('Scheduled realtime load failed', error);
    });
  }, REALTIME_REFRESH_MS);
};

const stopRealtimeRefresh = () => {
  if (realtimeRefreshTimer) {
    window.clearInterval(realtimeRefreshTimer);
    realtimeRefreshTimer = null;
  }
};

loadAiData().catch((error) => {
  console.error('AI load failed', error);
  renderAiError(error?.message || 'AI analysis refresh failed.');
});

loadRealtimeData().catch((error) => {
  console.error('Realtime load failed', error);
  const message = error?.message || 'Realtime refresh failed.';
  renderHeadlineError(message);
  renderMarketTapeError(message);
  renderLiveError(message);
  renderHolidayError(message);
});

syncRefreshToggle();
bindDonateModal();
bindNewsletterForm();

scheduleAiRefresh();
startRealtimeRefresh();
