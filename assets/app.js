const currency = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });

const chartState = {};
const runtimeConfig = window.RUNTIME_CONFIG || {};
const SUPABASE_URL = (runtimeConfig.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_PUBLISHABLE_KEY = runtimeConfig.SUPABASE_PUBLISHABLE_KEY || '';
const LIVE_SNAPSHOT_URL = `${SUPABASE_URL}/functions/v1/nse-snapshot`;
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const AI_REFRESH_MS = 60 * 60 * 1000;
const REALTIME_REFRESH_MS = 60 * 1000;
const NSE_HOLIDAYS_URL = 'https://r.jina.ai/http://www.nseindia.com/api/holiday-master?type=trading';
const LIVE_NEWS_SOURCES = [
  {
    source: 'Business Standard',
    url: 'https://news.google.com/rss/search?q=site%3Abusiness-standard.com%20%28market%20OR%20stocks%20OR%20nifty%20OR%20economy%20OR%20inflation%29&hl=en-IN&gl=IN&ceid=IN%3Aen',
  },
  {
    source: 'Livemint',
    url: 'https://www.livemint.com/rss/news',
  },
  {
    source: 'Bloomberg Economics',
    url: 'https://feeds.bloomberg.com/economics/news.rss',
  },
];
const MARKET_NEWS_KEYWORDS = [
  'market', 'stock', 'stocks', 'economy', 'economic', 'inflation', 'fed', 'ecb', 'bank', 'oil', 'trade',
  'finance', 'nifty', 'sensex', 'wall street', 'bond', 'currency', 'rupee', 'dollar', 'ipo', 'earnings',
  'shares', 'fii', 'dii', 'rates', 'gdp', 'tariff', 'commodity', 'crude', 'buyback', 'profit', 'results',
];

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

let realtimeRefreshTimer = null;

const loadJson = async (url, options = {}) => {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Failed to load data: ${response.status}`);
  }
  return response.json();
};

const loadText = async (url, options = {}) => {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Failed to load text: ${response.status}`);
  }
  return response.text();
};

const fetchAiAnalysis = async () => loadJson('data/ai-analysis.json', { cache: 'no-store' });
const fetchNewsRailSnapshot = async () => loadJson(`data/news-feed.json?t=${Date.now()}`, { cache: 'no-store' });

const fetchLiveHeadlines = async () => {
  const results = await Promise.allSettled(
    LIVE_NEWS_SOURCES.map(async ({ source, url }) => {
      const payload = await loadJson(`https://api.rss2json.com/v1/api.json?rss_url=${encodeURIComponent(url)}&_=${Date.now()}`, { cache: 'no-store' });
      return { source, payload };
    })
  );

  const items = [];
  const sourcesAvailable = [];
  const sourceErrors = [];
  const seenTitles = new Set();

  results.forEach((result, index) => {
    const source = LIVE_NEWS_SOURCES[index].source;
    if (result.status !== 'fulfilled') {
      sourceErrors.push({ source, error: result.reason?.message || 'Feed fetch failed' });
      return;
    }

    const feedItems = result.value.payload?.items || [];
    sourcesAvailable.push(source);
    feedItems.forEach((item) => {
      const title = cleanHeadlineTitle(item.title || '', source);
      const description = item.description || '';
      if (!title || seenTitles.has(title.toLowerCase()) || !isMarketStory(title, description)) {
        return;
      }
      seenTitles.add(title.toLowerCase());
      items.push({
        source,
        title,
        link: item.link,
        publishedAt: item.pubDate || item.publishedAt || '',
      });
    });
  });

  items.sort((left, right) => new Date(right.publishedAt || 0) - new Date(left.publishedAt || 0));
  return {
    updatedAt: new Date().toISOString(),
    items: items.slice(0, 10),
    sourcesAvailable,
    sourceErrors,
  };
};

const fetchNseHolidays = async () => {
  const rawText = await loadText(`${NSE_HOLIDAYS_URL}&_=${Date.now()}`, { cache: 'no-store' });
  const payload = parseEmbeddedJson(rawText);
  const records = Array.isArray(payload.CM)
    ? payload.CM
    : Array.isArray(payload.FO)
      ? payload.FO
      : Object.values(payload).find(Array.isArray) || [];
  return normalizeHolidayPayload(records);
};

const fetchLiveSnapshot = async () => {
  if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
    throw new Error('Supabase runtime configuration is missing.');
  }

  return loadJson(LIVE_SNAPSHOT_URL, {
    cache: 'no-store',
    headers: {
      apikey: SUPABASE_PUBLISHABLE_KEY,
    },
  });
};

const isMarketStory = (title, description) => {
  const haystack = `${title} ${description}`.toLowerCase();
  return MARKET_NEWS_KEYWORDS.some((keyword) => haystack.includes(keyword));
};

const cleanHeadlineTitle = (title, source) => {
  const suffix = ` - ${source}`;
  return title.endsWith(suffix) ? title.slice(0, -suffix.length).trim() : title.trim();
};

const loadAiData = async () => {
  const aiResult = await fetchAiAnalysis();
  renderAiSection(aiResult);
};

const loadRealtimeData = async () => {
  const [liveResult, marketTapeResult, headlinesResult, holidaysResult] = await Promise.allSettled([
    fetchLiveSnapshot(),
    fetchNewsRailSnapshot(),
    fetchLiveHeadlines(),
    fetchNseHolidays(),
  ]);

  if (marketTapeResult.status === 'fulfilled') {
    try {
      const marketTape = marketTapeResult.value.marketTape || {};
      setText('marketTapeStamp', formatDateTime(marketTape.updatedAt) || '-');
      renderMarketTape(marketTape.items || []);
    } catch (error) {
      console.error('Market tape render failed', error);
      renderMarketTapeError(error?.message || 'Market tape render failed.');
    }
  } else {
    renderMarketTapeError(marketTapeResult.reason?.message || 'Market tape refresh failed.');
  }

  if (headlinesResult.status === 'fulfilled') {
    try {
      renderNewsFeed(headlinesResult.value);
      setText('newsFeedStamp', formatDateTime(headlinesResult.value.updatedAt) || '-');
    } catch (error) {
      console.error('Headline render failed', error);
      renderHeadlineError(error?.message || 'Headline rail render failed.');
    }
  } else {
    renderHeadlineError(headlinesResult.reason?.message || 'Headline rail refresh failed.');
  }

  if (liveResult.status === 'fulfilled') {
    try {
      renderLiveSections(normalizeLiveSnapshot(liveResult.value));
    } catch (error) {
      console.error('Live render failed', error);
      renderLiveError(error?.message || 'Live market render failed.');
    }
  } else {
    renderLiveError(liveResult.reason?.message || 'Live market refresh failed.');
  }

  if (holidaysResult.status === 'fulfilled') {
    try {
      renderHolidayCalendar(holidaysResult.value);
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
  setText('aiSummaryText', message);
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
  setText('marketTapeStamp', 'Market tape refresh pending');
  document.getElementById('marketTape').innerHTML = `<p>${message}</p>`;
};

const renderHeadlineError = (message) => {
  setText('newsFeedStamp', 'Headline refresh pending');
  setText('newsFeedMeta', message);
  document.getElementById('newsFeedList').innerHTML = `<li class="news-feed-item">${message}</li>`;
};

const renderHolidayError = (message) => {
  setText('holidayStamp', 'Holiday refresh pending');
  setText('holidayMeta', message);
  document.getElementById('holidayCalendarGrid').innerHTML = `<p>${message}</p>`;
};

const renderLiveError = (message) => {
  setText('generatedAt', 'Live refresh pending');
  setText('heroSpot', '-');
  setText('heroExpiry', '-');
  setText('heroTopContributor', '-');
  setText('heroLowestIv', '-');
  setText('contributorsStamp', 'Live refresh pending');
  setText('openInterestStamp', 'Live refresh pending');
  setText('blackScholesStamp', 'Live refresh pending');
  setText('lowIvStamp', 'Live refresh pending');
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

  setText('aiSummaryText', aiAnalysis.summary || 'AI analysis is pending for the next refresh.');
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

  setText('generatedAt', formatDateTime(snapshot.fetchedAt));
  setText('heroSpot', formatNumber(openInterest.spot));
  setText('heroExpiry', openInterest.expiry || '-');
  setText('heroTopContributor', contributors.rows[0] ? `${contributors.rows[0].symbol} ${signed(contributors.rows[0].contributingPoints)}` : '-');
  setText('heroLowestIv', getLowestIvLabel(lowIv));
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
    { label: 'Strongest Call ΔOI', value: peakLabel(openInterest.strikes, 'callOIChange') },
    { label: 'Strongest Put ΔOI', value: peakLabel(openInterest.strikes, 'putOIChange') },
  ]);

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

const normalizeLiveSnapshot = (payload) => {
  const contributors = normalizeContributors(payload.contributors || {});
  const fiiDii = normalizeFiiDii(payload.fiiDii || {});
  const openInterest = extractOpenInterest(payload.optionChain || {});
  const blackScholes = buildBlackScholes(openInterest);
  const lowIV = buildLowIv(payload.optionChain || {});

  return {
    fetchedAt: payload.fetchedAt || new Date().toISOString(),
    contributors,
    fiiDii,
    openInterest,
    blackScholes,
    lowIV,
  };
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

  return {
    timestamp: records.timestamp || '',
    spot,
    expiry,
    strikes: strikes.map((strikePrice) => {
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
    }),
    rawRecords: data,
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

const renderMarketTape = (items) => {
  const host = document.getElementById('marketTape');
  if (!items.length) {
    host.innerHTML = '<p>No market tape available.</p>';
    return;
  }

  const chips = items.map((item) => `
    <article class="ticker-chip">
      <span class="ticker-label">${item.label}</span>
      <span class="ticker-value">${formatTickerValue(item.last)}</span>
      <span class="ticker-change ${tone(item.changePercent)}">${signed(item.changePercent)}%</span>
    </article>
  `).join('');

  host.innerHTML = `
    <div class="ticker-marquee">
      <div class="ticker-track">${chips}${chips}</div>
    </div>
  `;
};

const renderNewsFeed = (newsFeed) => {
  const items = newsFeed.items || [];
  const meta = document.getElementById('newsFeedMeta');
  const list = document.getElementById('newsFeedList');
  const sources = (newsFeed.sourcesAvailable || []).join(', ');
  const blocked = (newsFeed.sourceErrors || []).map((item) => item.source).join(', ');
  if (!items.length) {
    meta.textContent = blocked ? `Unavailable right now: ${blocked}.` : 'No RSS headlines available.';
    list.innerHTML = '<li class="news-feed-item">No headlines available.</li>';
    return;
  }

  meta.textContent = blocked ? `Sources: ${sources}. Unavailable right now: ${blocked}.` : `Sources: ${sources}.`;

  list.innerHTML = items.map((item) => `
    <li class="news-feed-item">
      <a href="${item.link}" target="_blank" rel="noreferrer">
        <span class="news-source">${item.source}</span>
        <span class="news-title">${item.title}</span>
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

  chartState[id] = { rows, mapper, title };

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
  modalLegend.innerHTML = renderLegend(state.mapper(state.rows[0]));
  renderBarChart('chartModalBody', state.rows, state.mapper, null, title, true);
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
  const date = new Date(isoString);
  return Number.isNaN(date.getTime()) ? isoString : date.toLocaleString('en-IN');
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
    loadRealtimeData().catch((error) => {
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

window.setInterval(() => {
  loadAiData().catch((error) => {
    console.error('Scheduled AI load failed', error);
  });
}, AI_REFRESH_MS);

startRealtimeRefresh();
