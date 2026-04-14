const currency = new Intl.NumberFormat('en-IN', { maximumFractionDigits: 2 });

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
  document.getElementById('contributorsRows').innerHTML = `<tr><td colspan="5">${message}</td></tr>`;
  document.getElementById('blackScholesRows').innerHTML = `<tr><td colspan="7">${message}</td></tr>`;
  document.getElementById('lowIvRows').innerHTML = `<tr><td colspan="7">${message}</td></tr>`;
  document.getElementById('oiChart').innerHTML = `<p>${message}</p>`;
  document.getElementById('bidAskChart').innerHTML = `<p>${message}</p>`;
};

const renderDashboard = (payload) => {
  const contributors = payload.indexContributors;
  const openInterest = payload.openInterest;
  const blackScholes = payload.blackScholes;
  const lowIv = payload.lowIV;

  document.getElementById('generatedAt').textContent = formatDateTime(payload.generatedAt);
  document.getElementById('heroSpot').textContent = formatNumber(openInterest.spot);
  document.getElementById('heroExpiry').textContent = openInterest.expiry || '-';
  document.getElementById('heroTopContributor').textContent = contributors.rows[0] ? `${contributors.rows[0].symbol} ${signed(contributors.rows[0].contributingPoints)}` : '-';
  document.getElementById('heroLowestIv').textContent = lowIv.rows[0] ? `${lowIv.rows[0].type} ${lowIv.rows[0].strike} @ ${lowIv.rows[0].iv.toFixed(2)}%` : '-';

  document.getElementById('contributorsStamp').textContent = contributors.timestamp || '-';
  document.getElementById('openInterestStamp').textContent = openInterest.timestamp || '-';
  document.getElementById('blackScholesStamp').textContent = blackScholes.timestamp || '-';
  document.getElementById('lowIvStamp').textContent = lowIv.timestamp || '-';

  renderMetricGrid('contributorsSummary', [
    { label: 'Nifty Last', value: formatNumber(contributors.lastPrice) },
    { label: 'Total CP', value: signed(contributors.totalPoints), tone: tone(contributors.totalPoints) },
    { label: 'Adv / Dec', value: `${contributors.advances} / ${contributors.declines}` },
    { label: 'Top Weight', value: contributors.rows[0] ? contributors.rows[0].symbol : '-' },
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

  renderMetricGrid('lowIvSummary', [
    { label: 'Tracked Expiries', value: String(lowIv.expiries.length) },
    { label: 'Lowest IV', value: lowIv.rows[0] ? `${lowIv.rows[0].iv.toFixed(2)}%` : '-' },
    { label: 'Cheapest Contract', value: lowestLabel(lowIv.rows) },
    { label: 'Most Negative IV Δ', value: ivChangeLabel(lowIv.rows) },
  ]);

  document.getElementById('contributorsRows').innerHTML = contributors.rows.map((row) => `
    <tr>
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

  document.getElementById('lowIvRows').innerHTML = lowIv.rows.map((row) => `
    <tr>
      <td>${row.type}</td>
      <td>${row.strike}</td>
      <td>${row.expiry}</td>
      <td>${formatNumber(row.iv)}%</td>
      <td class="${tone(row.ivChange)}">${signed(row.ivChange)}</td>
      <td class="${tone(row.oiChangePercent)}">${signed(row.oiChangePercent)}%</td>
      <td>${formatNumber(row.lastPrice)}</td>
    </tr>
  `).join('');

  renderBarChart('oiChart', openInterest.strikes, ({ strikePrice, callOIChange, putOIChange }) => ({
    label: String(strikePrice),
    valueA: callOIChange,
    valueB: putOIChange,
    colorA: '#1f8f6b',
    colorB: '#b94a48',
    labelA: 'Call ΔOI',
    labelB: 'Put ΔOI',
  }));

  renderBarChart('bidAskChart', openInterest.strikes, ({ strikePrice, callBidAsk, putBidAsk }) => ({
    label: String(strikePrice),
    valueA: callBidAsk,
    valueB: putBidAsk,
    colorA: '#0f4c5c',
    colorB: '#b47b19',
    labelA: 'Call Buy-Sell',
    labelB: 'Put Buy-Sell',
  }));
};

const renderMetricGrid = (id, metrics) => {
  document.getElementById(id).innerHTML = metrics.map((metric) => `
    <article class="metric">
      <div class="metric-label">${metric.label}</div>
      <div class="metric-value ${metric.tone || ''}">${metric.value}</div>
    </article>
  `).join('');
};

const renderBarChart = (id, rows, mapper) => {
  if (!rows.length) {
    document.getElementById(id).innerHTML = '<p>No data available.</p>';
    return;
  }

  const mapped = rows.map(mapper);
  const maxValue = Math.max(...mapped.flatMap((item) => [Math.abs(item.valueA), Math.abs(item.valueB), 1]));
  const width = 860;
  const height = 300;
  const baseline = 150;
  const left = 60;
  const groupWidth = (width - left - 20) / mapped.length;
  const barWidth = Math.max(8, groupWidth / 3);

  const bars = mapped.map((item, index) => {
    const x = left + index * groupWidth + groupWidth * 0.15;
    return [
      buildSvgRect(x, baseline, barWidth, item.valueA, maxValue, item.colorA),
      buildSvgRect(x + barWidth + 6, baseline, barWidth, item.valueB, maxValue, item.colorB),
      `<text x="${x + barWidth}" y="${height - 12}" text-anchor="middle" font-size="11" fill="#4d6971">${item.label}</text>`,
    ].join('');
  }).join('');

  document.getElementById(id).innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Market chart">
      <line x1="${left}" x2="${width - 10}" y1="${baseline}" y2="${baseline}" stroke="rgba(15,76,92,0.18)" stroke-width="1.2"></line>
      <text x="18" y="24" font-size="11" fill="#4d6971">${formatCompact(maxValue)}</text>
      <text x="18" y="${baseline - 6}" font-size="11" fill="#4d6971">0</text>
      <text x="18" y="${height - 42}" font-size="11" fill="#4d6971">-${formatCompact(maxValue)}</text>
      ${bars}
    </svg>
  `;
};

const buildSvgRect = (x, baseline, width, value, maxValue, color) => {
  const scaledHeight = (Math.abs(value) / maxValue) * 120;
  const y = value >= 0 ? baseline - scaledHeight : baseline;
  return `<rect x="${x}" y="${y}" width="${width}" height="${scaledHeight}" rx="5" fill="${color}" opacity="0.88"></rect>`;
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

const ivChangeLabel = (rows) => {
  if (!rows.length) return '-';
  const best = [...rows].sort((left, right) => left.ivChange - right.ivChange)[0];
  return `${best.type} ${best.strike} (${signed(best.ivChange)})`;
};

loadData();