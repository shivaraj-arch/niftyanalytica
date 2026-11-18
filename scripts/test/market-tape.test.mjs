/** node --test scripts/test/ */
import test from 'node:test';
import assert from 'node:assert/strict';
import { parseSgxMarketTape } from '../lib/news-feed.mjs';

/* sgxnifty.org serves the tape as window.FD_SIDE, a JSON object keyed by
   category whose rows are [name, last, change, changePercent, status, id].
   The fixture is trimmed to two rows per category plus the neighbouring
   window.FD_DATA assignment, which is ~170 KB on the real page and is what a
   greedy brace match would swallow. */
const fixture = `<!doctype html><html><body>
<script data-cfasync="false">window.FD_FEED={"id":"242","name":"SGX Nifty","last":25254.5};
window.FD_SIDE={"Indices":[["Dow 30",50759.4,-848.75,-1.64,"Close","100"],["S&P 500",7340.53,-76.06,-1.03,"Close","102"]],"Futures":[["Nasdaq 100 Futures",27224.2,148.75,0.55,"Open","204"],["US Dollar Index",103.645,0.16,0.15,"Open","110"]],"Commodities":[["Gold",3802.9,-61.3,-1.59,"Open","700"],["Crude Oil",85.15,-0.86,-1,"Open","712"]],"Currencies":[["EUR/USD",1.0987,-0.001,-0.09,"Open","300"],["USD/INR",93.47,0,0,"Close","330"]]};
window.FD_DATA={"name":"Sgx Nifty","intra":[[1,2],[3,4]],"nested":{"deep":{"deeper":1}}};</script>
</body></html>`;

test('parses every category into the tape contract', () => {
  const items = parseSgxMarketTape(fixture);
  assert.equal(items.length, 8);

  const [dow] = items;
  assert.equal(dow.label, 'Dow 30 (Close)');
  assert.equal(dow.last, 50759.4);
  assert.equal(dow.changePercent, -1.64);
  assert.equal(dow.source, 'SGX Nifty / Indices');
  assert.ok(Date.parse(dow.updatedAt) > 0);

  assert.deepEqual(
    [...new Set(items.map((item) => item.source))],
    ['SGX Nifty / Indices', 'SGX Nifty / Futures', 'SGX Nifty / Commodities', 'SGX Nifty / Currencies'],
  );
});

test('reads the change percent column, not the absolute change', () => {
  const gold = parseSgxMarketTape(fixture).find((item) => item.label.startsWith('Gold'));
  assert.equal(gold.changePercent, -1.59, 'must not pick up the -61.3 absolute move');
  assert.equal(gold.last, 3802.9);
});

test('keeps a zero percent move rather than dropping the row', () => {
  const inr = parseSgxMarketTape(fixture).find((item) => item.label.startsWith('USD/INR'));
  assert.equal(inr.changePercent, 0);
  assert.equal(inr.label, 'USD/INR (Close)');
});

test('stops at the end of FD_SIDE and ignores the neighbouring payload', () => {
  const items = parseSgxMarketTape(fixture);
  assert.ok(!items.some((item) => item.source.includes('intra')));
  assert.ok(!items.some((item) => item.label.startsWith('Sgx Nifty')));
});

test('throws rather than reporting a half-empty tape', () => {
  assert.throws(() => parseSgxMarketTape('<html>redesigned again</html>'), /FD_SIDE not found/);
  assert.throws(() => parseSgxMarketTape('<script>window.FD_SIDE=null</script>'), /not an object literal/);
  assert.throws(() => parseSgxMarketTape('<script>window.FD_SIDE={}</script>'), /no usable rows/);
  assert.throws(
    () => parseSgxMarketTape('<script>window.FD_SIDE={"Indices":[["Dow 30",1,2,3,"Open","1"]]'),
    /unterminated/,
  );
  assert.throws(
    () => parseSgxMarketTape('<script>window.FD_SIDE={"Indices":[["Dow 30",0,0,0,"Open","1"]]}</script>'),
    /no usable rows/,
  );
});
