import fs from 'node:fs/promises';
import path from 'node:path';
import { fetchNseSnapshot } from './lib/nse-snapshot.mjs';
import { buildNewsBundle } from './lib/news-feed.mjs';

const LIVE_SNAPSHOT_FILE = path.resolve('data/live-snapshot.json');
const NEWS_FEED_FILE = path.resolve('data/news-feed.json');

async function loadJson(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function main() {
  const refreshNewsBundle = process.env.REFRESH_NEWS_BUNDLE !== 'false';
  const existingSnapshot = await loadJson(LIVE_SNAPSHOT_FILE, {});
  const existingNewsFeed = await loadJson(NEWS_FEED_FILE, {
    marketTape: { updatedAt: '', items: [] },
    newsFeed: { updatedAt: '', updatedAtLabel: '', items: [], sourcesAvailable: [], sourceErrors: [] },
  });

  const snapshot = await fetchNseSnapshot();
  const newsBundle = refreshNewsBundle
    ? await buildNewsBundle()
    : {
      marketTape: existingSnapshot.marketTape || existingNewsFeed.marketTape,
      newsFeed: existingSnapshot.newsFeed || existingNewsFeed.newsFeed,
    };

  const payload = {
    ...existingSnapshot,
    ...snapshot,
    ...newsBundle,
  };

  await fs.mkdir(path.dirname(LIVE_SNAPSHOT_FILE), { recursive: true });
  await fs.writeFile(LIVE_SNAPSHOT_FILE, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');

  if (refreshNewsBundle) {
    const newsPayload = {
      marketTape: payload.marketTape,
      newsFeed: payload.newsFeed,
    };
    await fs.writeFile(NEWS_FEED_FILE, `${JSON.stringify(newsPayload, null, 2)}\n`, 'utf8');
  }

  console.log(`Updated ${path.relative(process.cwd(), LIVE_SNAPSHOT_FILE)}${refreshNewsBundle ? ' and data/news-feed.json' : ''}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});