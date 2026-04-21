#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import https from 'https';
import http from 'http';

const BASE_URL = 'https://cook-feast-api.mantle.xyz';
const DATA_DIR = './data';
const ADDRESS_CSV = './cook-feast-address.csv';
const MAX_CONCURRENT = 5;
const MAX_RETRIES = 10;
const INITIAL_DELAY = 2000;
const BATCH_DELAY = 500;

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, { timeout: 30000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        httpGet(res.headers.location).then(resolve).catch(reject);
        return;
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode === 429) {
          reject(new Error(`HTTP 429 Too Many Requests`));
          return;
        }
        if (res.statusCode >= 500) {
          reject(new Error(`HTTP ${res.statusCode} Server Error`));
          return;
        }
        if (res.statusCode >= 400) {
          reject(new Error(`HTTP ${res.statusCode} Client Error`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve(data);
        }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

async function fetchWithRetry(url, retries = MAX_RETRIES, delay = INITIAL_DELAY) {
  for (let i = 0; i < retries; i++) {
    try {
      return await httpGet(url);
    } catch (err) {
      const is429 = err.message.includes('429');
      const backoff = is429
        ? delay * Math.pow(2, i) + Math.random() * 1000
        : delay * (i + 1);
      if (i < retries - 1) {
        await new Promise(r => setTimeout(r, backoff));
      } else {
        throw err;
      }
    }
  }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function concurrent(tasks, concurrency) {
  const results = [];
  const executing = new Set();

  for (const task of tasks) {
    const p = task().then(result => {
      executing.delete(p);
      return result;
    });
    executing.add(p);
    results.push(p);

    if (executing.size >= concurrency) {
      await Promise.race(executing);
    }
  }

  return Promise.all(results);
}

function parseCSV(filepath) {
  const content = fs.readFileSync(filepath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim() && !l.startsWith('userAddress'));
  return lines.map(l => l.trim()).filter(l => l.length > 0);
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function isFileValid(filepath) {
  if (!fs.existsSync(filepath)) return false;
  try {
    const data = JSON.parse(fs.readFileSync(filepath, 'utf-8'));
    if (data && data.statusCode === 429) return false;
    if (data && data.statusCode >= 500) return false;
    return true;
  } catch {
    return false;
  }
}

async function main() {
  console.log('Starting static data fetcher (rate-limit safe)...\n');

  ensureDir(DATA_DIR);

  const addresses = parseCSV(ADDRESS_CSV);
  console.log(`Loaded ${addresses.length} addresses from CSV\n`);

  const staticEndpoints = [
    { path: '/activity/cook-feast-season-1', name: 'activity-cook-feast-season-1' },
    { path: '/multiplier-config', name: 'multiplier-config' },
    { path: '/power/total', name: 'power-total' },
  ];

  console.log('=== Fetching static endpoints ===');
  for (const endpoint of staticEndpoints) {
    const url = `${BASE_URL}${endpoint.path}`;
    const filename = `${endpoint.name}.json`;
    const filepath = path.join(DATA_DIR, filename);

    if (isFileValid(filepath)) {
      console.log(`  SKIP (already valid): ${filename}`);
      continue;
    }

    console.log(`  Fetching: ${endpoint.name}`);
    try {
      const data = await fetchWithRetry(url);
      fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
      console.log(`  -> Saved to ${filename}`);
    } catch (err) {
      console.error(`  -> ERROR: ${err.message}`);
    }
  }

  const perAddressEndpoints = [
    { path: (addr) => `/power/${addr}`, dir: 'power', name: (addr) => `power-${addr}` },
    { path: (addr) => `/release-records/total/activity/cook-feast-season-1/user/${addr}`, dir: 'release-records', name: (addr) => `release-records-${addr}` },
    { path: (addr) => `/rewards/activities/cook-feast-season-1/users/${addr}/proof`, dir: 'rewards-proof', name: (addr) => `rewards-proof-${addr}` },
  ];

  console.log('\n=== Fetching per-address endpoints ===');
  console.log(`Total addresses: ${addresses.length}`);
  console.log(`Endpoints per address: ${perAddressEndpoints.length}`);
  console.log(`Concurrency: ${MAX_CONCURRENT}, Max retries: ${MAX_RETRIES}\n`);

  let completed = 0;
  let skipped = 0;
  let errors = 0;

  const tasks = [];
  for (const addr of addresses) {
    for (const endpoint of perAddressEndpoints) {
      const filename = `${endpoint.name(addr)}.json`;
      const filepath = path.join(DATA_DIR, endpoint.dir, filename);

      if (isFileValid(filepath)) {
        skipped++;
        continue;
      }

      const url = `${BASE_URL}${endpoint.path(addr)}`;
      ensureDir(path.join(DATA_DIR, endpoint.dir));
      tasks.push(async () => {
        try {
          const data = await fetchWithRetry(url);
          fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
          return { success: true, addr, endpoint: endpoint.name(addr) };
        } catch (err) {
          return { success: false, addr, endpoint: endpoint.name(addr), error: err.message };
        }
      });
    }
  }

  console.log(`Tasks to fetch: ${tasks.length} (skipped ${skipped} already-valid)\n`);

  for (let i = 0; i < tasks.length; i += MAX_CONCURRENT) {
    const batch = tasks.slice(i, i + MAX_CONCURRENT);
    const results = await concurrent(batch, MAX_CONCURRENT);

    for (const result of results) {
      if (result.success) {
        completed++;
      } else {
        errors++;
        console.error(`  ERROR [${result.addr}]: ${result.error.substring(0, 60)}`);
      }
    }

    const progress = Math.min(i + MAX_CONCURRENT, tasks.length);
    if (progress % 50 < MAX_CONCURRENT || progress === tasks.length) {
      console.log(`Progress: ${progress}/${tasks.length} (${completed} saved, ${errors} errors, ${skipped} skipped)`);
    }

    if (i + MAX_CONCURRENT < tasks.length) {
      await sleep(BATCH_DELAY);
    }
  }

  console.log(`\n=== Complete ===`);
  console.log(`Files saved: ${completed}, Skipped: ${skipped}, Errors: ${errors}`);

  const index = {
    generated: new Date().toISOString(),
    totalAddresses: addresses.length,
    staticEndpoints: staticEndpoints.map(e => e.name),
    filesGenerated: completed + skipped,
    errors: errors
  };
  fs.writeFileSync(path.join(DATA_DIR, 'index.json'), JSON.stringify(index, null, 2));
  console.log(`Index saved to data/index.json`);
}

main().catch(console.error);
