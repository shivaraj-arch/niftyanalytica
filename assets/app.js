const currency = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });

const chartState = {};
const runtimeConfig = window.RUNTIME_CONFIG || {};
const SUPABASE_URL = (runtimeConfig.SUPABASE_URL || '').replace(/\/$/, '');
const SUPABASE_PUBLISHABLE_KEY = runtimeConfig.SUPABASE_PUBLISHABLE_KEY || '';
const LIVE_SNAPSHOT_URL = `${SUPABASE_URL}/functions/v1/nse-snapshot`;
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

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

const loadJson = async (url, options = {}) => {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`Failed to load data: ${response.status}`);
  }
  return response.json();
};

const fetchAiAnalysis = async () => loadJson('data/ai-analysis.json', { cache: 'no-store' });

const fetchLiveSnapshot = async () => {
  if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
    throw new Error('Supabase runtime configuration is missing.');
  }

  return loadJson(LIVE_SNAPSHOT_URL, {
    cache: 'no-store',
    headers: {
      apikey: SUPABASE_PUBLISHABLE_KEY,
      'Cache-Control': 'no-store',
    },
  });
};

const loadData = async () => {
  const [aiResult, liveResult] = await Promise.allSettled([fetchAiAnalysis(), fetchLiveSnapshot()]);

  if (aiResult.status === 'fulfilled') {
    renderAiSection(aiResult.value);
  } else {
    renderAiError(aiResult.reason?.message || 'AI analysis refresh failed.');
  }

  if (liveResult.status === 'fulfilled') {
    renderLiveSections(normalizeLiveSnapshot(liveResult.value));
  } else {
    renderLiveError(liveResult.reason?.message || 'Live market refresh failed.');
  }
};

const renderAiError = (message) => {
  setText('aiAnalysisStamp', 'AI refresh pending');
  setText('aiSummaryText', message);
  document.getElementById('aiHeadlines').innerHTML = `<li>${message}</li>`;
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

const renderLiveError = (message) => {
  setText('generatedAt', 'Live refresh pending');
  setText('marketTapeStamp', 'Live refresh pending');
  setText('heroSpot', '-');
  setText('heroExpiry', '-');
  setText('heroTopContributor', '-');
  setText('heroLowestIv', '-');
  setText('contributorsStamp', 'Live refresh pending');
  setText('openInterestStamp', 'Live refresh pending');
  setText('blackScholesStamp', 'Live refresh pending');
  setText('lowIvStamp', 'Live refresh pending');
  setText('newsFeedMeta', message);
  document.getElementById('marketTape').innerHTML = `<p>${message}</p>`;
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

  const headlines = aiAnalysis.headlines || [];
  document.getElementById('aiHeadlines').innerHTML = headlines.length
    ? headlines.map((headline) => `<li>${headline}</li>`).join('')
    : '<li>Headlines are embedded in the cached AI brief.</li>';
};

const renderLiveSections = (snapshot) => {
  const contributors = snapshot.contributors;
  const openInterest = snapshot.openInterest;
  const blackScholes = snapshot.blackScholes;
  const lowIv = snapshot.lowIV;

  setText('generatedAt', formatDateTime(snapshot.fetchedAt));
  setText('marketTapeStamp', formatDateTime(snapshot.fetchedAt));
  setText('heroSpot', formatNumber(openInterest.spot));
  setText('heroExpiry', openInterest.expiry || '-');
  setText('heroTopContributor', contributors.rows[0] ? `${contributors.rows[0].symbol} ${signed(contributors.rows[0].contributingPoints)}` : '-');
  setText('heroLowestIv', getLowestIvLabel(lowIv));
  setText('contributorsStamp', contributors.timestamp || '-');
  setText('openInterestStamp', openInterest.timestamp || '-');
  setText('blackScholesStamp', blackScholes.timestamp || '-');
  setText('lowIvStamp', lowIv.timestamp || '-');
  setText('newsFeedMeta', `Live via Supabase Edge Function • ${formatDateTime(snapshot.fetchedAt)}`);

  renderMarketTape(snapshot.marketTape);

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
    marketTape: buildMarketTape(contributors, fiiDii, openInterest),
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
  const expiry = formatExpiryLabel(getFirstExpiry(data));
  const rows = [];

  data.forEach((item) => {
    const strike = item.CE?.strikePrice || item.PE?.strikePrice;
    if (!strike || Math.abs(strike - spot) > 1000) {
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
  const limitedRows = rows.slice(0, 10);

  return {
    timestamp: records.timestamp || '',
    expiries: expiry ? [expiry] : [],
    defaultExpiry: expiry || '',
    rowsByExpiry: expiry ? { [expiry]: limitedRows } : {},
  };
};

const buildMarketTape = (contributors, fiiDii, openInterest) => ([
  {
    label: 'Nifty 50',
    valueText: formatNumber(contributors.lastPrice),
    detailText: `${signed(contributors.indexChange)}%`,
    toneClass: tone(contributors.indexChange),
  },
  {
    label: 'Advances',
    valueText: String(contributors.advances),
    detailText: `${contributors.rows.length} tracked`,
    toneClass: 'positive',
  },
  {
    label: 'Declines',
    valueText: String(contributors.declines),
    detailText: `${contributors.rows.length} tracked`,
    toneClass: 'negative',
  },
  {
    label: 'FII Net',
    valueText: `${signed(fiiDii.fiiNet)} Cr`,
    detailText: fiiDii.date || 'Latest session',
    toneClass: tone(fiiDii.fiiNet),
  },
  {
    label: 'DII Net',
    valueText: `${signed(fiiDii.diiNet)} Cr`,
    detailText: fiiDii.date || 'Latest session',
    toneClass: tone(fiiDii.diiNet),
  },
  {
    label: 'Nearest Expiry',
    valueText: openInterest.expiry || '-',
    detailText: openInterest.timestamp || 'Live',
    toneClass: '',
  },
]);

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

  host.innerHTML = `<div class="ticker-grid">${items.map((item) => `
    <article class="ticker-chip">
      <span class="ticker-label">${item.label}</span>
      <span class="ticker-value">${item.valueText || '-'}</span>
      <span class="ticker-change ${item.toneClass || ''}">${item.detailText || ''}</span>
    </article>
  `).join('')}</div>`;
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

loadData();
