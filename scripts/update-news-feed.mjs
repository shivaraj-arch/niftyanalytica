import fs from 'node:fs/promises';
import path from 'node:path';
import { buildNewsBundle } from './lib/news-feed.mjs';

const OUTPUT_FILE = path.resolve('data/news-feed.json');
async function loadExistingPayload() {
  try {
    const raw = await fs.readFile(OUTPUT_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    return {
      marketTape: { updatedAt: '', items: [] },
      newsFeed: { updatedAt: '', updatedAtLabel: '', items: [], sourcesAvailable: [], sourceErrors: [] },
    };
  }
}

async function main() {
  await loadExistingPayload();
  const payload = await buildNewsBundle();

  await fs.mkdir(path.dirname(OUTPUT_FILE), { recursive: true });
  await fs.writeFile(OUTPUT_FILE, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  console.log(`Updated ${path.relative(process.cwd(), OUTPUT_FILE)}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});