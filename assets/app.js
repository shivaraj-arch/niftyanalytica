const currency = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });

const chartState = {};

const loadData = async () => {
  try {
    const response = await fetch('data/market-data.json', { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Failed to load data: ${response.status}`);
    }

    const payload = await response.json();
    renderDashboard(payload);
  } catch (error) {
    renderError(error.message);
  }
};

const renderError = (message) => {
  document.getElementById('generatedAt').textContent = 'Data refresh pending';
  document.getElementById('aiSummaryText').textContent = message;
  document.getElementById('marketTape').innerHTML = `<p>${message}</p>`;
  document.getElementById('newsFeedList').innerHTML = `<li class="news-feed-item">${message}</li>`;
  document.getElementById('newsFeedMeta').textContent = 'Headline refresh pending';
  document.getElementById('contributorsRows').innerHTML = `<tr><td colspan="5">${message}</td></tr>`;
  document.getElementById('blackScholesRows').innerHTML = `<tr><td colspan="7">${message}</td></tr>`;
  document.getElementById('lowIvRows').innerHTML = `<tr><td colspan="6">${message}</td></tr>`;
  document.getElementById('oiChart').innerHTML = `<p>${message}</p>`;
  document.getElementById('bidAskChart').innerHTML = `<p>${message}</p>`;
};

const renderDashboard = (payload) => {
  const aiAnalysis = payload.aiAnalysis || {};
  const contributors = payload.indexContributors;
  const openInterest = payload.openInterest;
  const blackScholes = payload.blackScholes;
  const lowIv = payload.lowIV;
  const marketTape = payload.marketTape || {};
  const newsFeed = payload.newsFeed || {};
  const defaultLowIvRows = lowIv.rowsByExpiry?.[lowIv.defaultExpiry] || [];

  document.getElementById('generatedAt').textContent = formatDateTime(payload.generatedAt);
  document.getElementById('marketTapeStamp').textContent = formatDateTime(marketTape.updatedAt) || '-';
  document.getElementById('newsFeedStamp').textContent = formatDateTime(newsFeed.updatedAt) || '-';
  document.getElementById('heroSpot').textContent = formatNumber(openInterest.spot);
  document.getElementById('heroExpiry').textContent = openInterest.expiry || '-';
  document.getElementById('heroTopContributor').textContent = contributors.rows[0] ? `${contributors.rows[0].symbol} ${signed(contributors.rows[0].contributingPoints)}` : '-';
  document.getElementById('heroLowestIv').textContent = defaultLowIvRows[0] ? `${defaultLowIvRows[0].type} ${defaultLowIvRows[0].strike} @ ${defaultLowIvRows[0].iv.toFixed(2)}%` : '-';

  document.getElementById('aiAnalysisStamp').textContent = formatDateTime(aiAnalysis.generatedAt) || '-';
  document.getElementById('contributorsStamp').textContent = contributors.timestamp || '-';
  document.getElementById('openInterestStamp').textContent = openInterest.timestamp || '-';
  document.getElementById('blackScholesStamp').textContent = blackScholes.timestamp || '-';
  document.getElementById('lowIvStamp').textContent = lowIv.timestamp || '-';

  renderMetricGrid('aiSummary', [
    { label: 'Bias', value: aiAnalysis.bias || '-', tone: biasTone(aiAnalysis.bias) },
    { label: 'FII Net', value: aiAnalysis.fiiDii ? `${signed(aiAnalysis.fiiDii.fiiNet)} Cr` : '-', tone: tone(aiAnalysis.fiiDii?.fiiNet || 0) },
    { label: 'DII Net', value: aiAnalysis.fiiDii ? `${signed(aiAnalysis.fiiDii.diiNet)} Cr` : '-', tone: tone(aiAnalysis.fiiDii?.diiNet || 0) },
    { label: 'Model', value: aiAnalysis.model || 'Gemini pending' },
  ]);

  document.getElementById('aiSummaryText').textContent = aiAnalysis.summary || 'AI analysis is pending for the next refresh.';
  renderChipList('aiKeyLevels', aiAnalysis.keyLevels || []);
  renderChipList('aiWatchlist', aiAnalysis.watchlist || []);
  document.getElementById('aiHeadlines').innerHTML = (aiAnalysis.headlines || []).map((headline) => `<li>${headline}</li>`).join('');
  renderMarketTape(marketTape.items || []);
  renderNewsFeed(newsFeed);

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
  select.innerHTML = lowIv.expiries.map((expiry) => `<option value="${expiry}">${expiry}</option>`).join('');
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
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
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

const biasTone = (value) => {
  if (!value) return '';
  const normalized = value.toLowerCase();
  if (normalized === 'bullish') return 'positive';
  if (normalized === 'bearish') return 'negative';
  return '';
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
  const total = rows.reduce((sum, row) => sum + (row[side].marketPrice - row[side].bsValue), 0);
  return total / rows.length;
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