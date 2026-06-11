// ─── holoVault Scraper ────────────────────────────────────────────────────────
// Triggered externally (cron-job.org → repository_dispatch webhook).
//
// Per-run logic:
//   • Fetches all products from shop.hololivepro.com (JP + EN in parallel)
//   • Compares against existing merch.json (keyed by product_id)
//   • NEW products only:
//       - Looks up the Langify EN image URL for each JP image URL
//       - If EN URL found  → convert to WebP q80, upload to R2
//       - If no EN URL     → skip that image (EN only, no JP fallback)
//       - product_id in merch.json is the deduplication gate — no HeadObject calls
//   • EXISTING products: only syncs is_available / delisted_at
//   • Skips commit if nothing changed

import fs   from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import sharp from 'sharp';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const BASE_JP    = 'https://shop.hololivepro.com';
const BASE_EN    = 'https://shop.hololivepro.com/en';
const MERCH_FILE = 'merch.json';
const IMAGES_DIR = 'Products';

// ─── R2 client ────────────────────────────────────────────────────────────────
// GitHub Actions secrets required:
//   R2_ACCOUNT_ID  – Cloudflare account ID
//   R2_ACCESS_KEY  – R2 Access Key ID
//   R2_SECRET_KEY  – R2 Secret Access Key
//   R2_BUCKET      – bucket name
//   R2_PUBLIC_URL  – e.g. https://images.yourdomain.com  (no trailing slash)

const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

const R2_BUCKET     = process.env.R2_BUCKET;
const R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchWithRetry(url, retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return res;
    } catch (err) {
      if (i === retries - 1) throw err;
      log(`Retry ${i + 1}/${retries} for ${url}`);
      await sleep(delayMs * (i + 1));
    }
  }
}

// ─── R2 upload ────────────────────────────────────────────────────────────────
// No HeadObject check — product_id gate in merch.json handles deduplication.
// PutObject is idempotent so re-uploading on a partial-run recovery is safe.

async function uploadToR2(imgUrl, key) {
  const res  = await fetchWithRetry(imgUrl);
  const raw  = Buffer.from(await res.arrayBuffer());
  const webp = await sharp(raw).webp({ quality: 80 }).toBuffer();

  await r2.send(new PutObjectCommand({
    Bucket:      R2_BUCKET,
    Key:         key,
    Body:        webp,
    ContentType: 'image/webp',
  }));

  return `${R2_PUBLIC_URL}/${key}`;
}

// Strips query string + original extension, appends .webp.
// e.g. https://cdn/.../image_main.jpg?v=123 → Products/handle/image_main.webp
function r2KeyFor(handle, imgUrl) {
  const base = imgUrl.split('?')[0].split('/').pop() || 'image';
  const stem = base.replace(/\.[^.]+$/, '');
  return `${IMAGES_DIR}/${handle}/${stem}.webp`;
}

// Processes items in chunks of `size` concurrently, preserving order.
// Returns an array of the same length as items (fn may return null for skips).
async function inChunks(items, size, fn) {
  const results = [];
  for (let i = 0; i < items.length; i += size) {
    results.push(...await Promise.all(items.slice(i, i + size).map(fn)));
  }
  return results;
}

// ─── Fetch Langify EN image map ───────────────────────────────────────────────
// EN images are injected client-side by Langify via a JS block on the EN page.
// Key: JP image filename (lowercase, no query string) → EN CDN URL.
// The `s` (dotAll) flag ensures .*? matches across newlines.

async function fetchEnImageMap() {
  log('Fetching Langify EN image map…');
  try {
    const res  = await fetchWithRetry(BASE_EN);
    const html = await res.text();

    const map = new Map();
    const re  = /customContents_image\[extractImageObject\("([^"]+)".*?\)\.file\]\s*=\s*extractImageObject\("([^"]+)"\)/gs;
    let m;
    while ((m = re.exec(html)) !== null) {
      const jpUrl = m[1].replace(/\\\//g, '/');
      const enUrl = m[2].replace(/\\\//g, '/');
      const key   = jpUrl.split('/').pop().split('?')[0].toLowerCase();
      map.set(key, enUrl);
    }

    log(`  → ${map.size} EN image mappings found`);
    return map;
  } catch (err) {
    log(`⚠ Failed to fetch EN image map: ${err.message}`);
    return new Map();
  }
}

// Returns the Langify EN CDN URL for a JP image URL, or null if not mapped.
// Always pass a JP image URL — the key is derived from the JP filename.
function getEnUrl(jpImgUrl, enMap) {
  const key = jpImgUrl.split('/').pop().split('?')[0].toLowerCase();
  return enMap.get(key) ?? null;
}

// ─── Merch taxonomies (type + category) ──────────────────────────────────────
// The shop's "Merch type" and "Merch category" filters are metafields that
// products.json does NOT expose. The filtered collection pages are public, so
// we walk /collections/all?<filter>=<value> per value and map product handles
// → values (a product can carry several, e.g. acrylic stand + badge set).

const TAXONOMIES = [
  {
    field: 'categories',                       // merch.json field name
    param: 'filter.v.m.custom.product_type',   // "Merch type" (variant metafield)
    values: [
      'アクリルスタンド・アクリルパネル',
      'キーホルダー・ストラップ',
      'バッジ・ピンバッジ',
      'ぬいぐるみ',
      'ステーショナリー',
      'ボイス',
      'タペストリー',
      'フィギュア',
      'カードゲーム・サプライ',
      'アパレル・アパレル小物',
      '生活雑貨',
      'CD・LP・BD',
      'OTHERS',
    ],
  },
  {
    field: 'merch_categories',                 // merch.json field name
    param: 'filter.p.m.custom.merch-category', // "Merch category" (product metafield)
    values: [
      'タレントプレミアムグッズ',
      'デジタルコンテンツ',
      'ユニット記念グッズ',
      'シーズナル企画グッズ',
      'イベント・ライブグッズ',
      'スターターグッズ',
      'hololive friends with u',
      'hololive friends to Go',
      'hololive friends poutique',
      'hololive friends Meets',
      'hololive closet',
      'HOLOSTARS CLOSET',
      '事務所グッズ',
      'ホロ活',
      'ホロmini',
      'Nendoroid',
      'OTHERS',
    ],
  },
];

// Storefront pages 429 non-browser user agents almost immediately — send
// browser-like headers, pace generously, and honor Retry-After on the SAME
// page so nothing is silently skipped.
const PAGE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml',
  'Accept-Language': 'en-US,en;q=0.9',
};
const TAXONOMY_PAGE_DELAY_MS = 4000;

async function politeFetchText(url, attempts = 5) {
  for (let i = 0; i < attempts; i++) {
    const res = await fetch(url, { headers: PAGE_HEADERS, signal: AbortSignal.timeout(30_000) }).catch(() => null);
    if (res && res.ok) return res.text();
    const status = res ? res.status : 'network error';
    const retryAfter = res ? parseInt(res.headers.get('retry-after') || '0', 10) : 0;
    const waitMs = Math.max(retryAfter * 1000, 15_000 * (i + 1));
    if (i === attempts - 1) throw new Error(`HTTP ${status} for ${url}`);
    await sleep(waitMs);
  }
}

// maxPages = Infinity → full crawl (backfill). A small maxPages with the
// newest-first sort covers recently added products cheaply on scheduled runs.
async function fetchTaxonomyMap(tax, maxPages) {
  log(`Fetching ${tax.field} map (up to ${maxPages === Infinity ? 'all' : maxPages} page(s)/value)…`);
  const map = new Map(); // handle → [values, taxonomy order]

  for (const value of tax.values) {
    const seen = new Set();
    for (let page = 1; page <= Math.min(maxPages, 200); page++) {
      const url = `${BASE_JP}/collections/all?${tax.param}=${encodeURIComponent(value)}&sort_by=created-descending&page=${page}`;
      let html;
      try {
        html = await politeFetchText(url);
      } catch (err) {
        // Persistent failure: abort this value so partial data is obvious in
        // the log rather than silently truncated.
        log(`  ⚠ ${value} aborted at page ${page}: ${err.message}`);
        break;
      }
      const before = seen.size;
      const re = /href="\/products\/([a-zA-Z0-9_-]+)/g;
      let m;
      while ((m = re.exec(html)) !== null) seen.add(m[1]);
      if (seen.size === before) break; // no new handles → past the last page
      await sleep(TAXONOMY_PAGE_DELAY_MS);
    }
    for (const h of seen) {
      if (!map.has(h)) map.set(h, []);
      map.get(h).push(value);
    }
    log(`  → ${value}: ${seen.size} products`);
  }

  log(`${tax.field} map covers ${map.size} products`);
  return map;
}

function sameCategories(a, b) {
  a = a || []; b = b || [];
  return a.length === b.length && a.every((v, i) => v === b[i]);
}

// ─── merch.json I/O ──────────────────────────────────────────────────────────

function loadExistingMerch() {
  const filePath = path.join(process.cwd(), MERCH_FILE);
  if (!fs.existsSync(filePath)) {
    log('No existing merch.json — starting fresh');
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    log('Could not parse merch.json — starting fresh');
    return [];
  }
}

// ─── Fetch all Shopify products (paginated) ───────────────────────────────────
// JP endpoint: JPY prices + JP titles/tags + JP image URLs (used as Langify keys)
// EN endpoint: EN titles/tags only (EN images come from Langify, not Shopify)

async function fetchAllShopifyProducts() {
  const products = [];
  let page = 1;

  while (true) {
    log(`Fetching page ${page}…`);

    const [resJp, resEn] = await Promise.all([
      fetchWithRetry(`${BASE_JP}/products.json?limit=250&page=${page}&currency=JPY`),
      fetchWithRetry(`${BASE_EN}/products.json?limit=250&page=${page}`),
    ]);

    const dataJp = await resJp.json();
    if (!dataJp.products?.length) break;

    const enByHandle = {};
    for (const p of ((await resEn.json()).products || [])) {
      enByHandle[p.handle] = {
        title: p.title,
        tags:  p.tags || [],
      };
    }

    for (const p of dataJp.products) {
      const en     = enByHandle[p.handle] || {};
      const prices = (p.variants || []).map(v => parseFloat(v.price)).filter(n => n > 0);

      products.push({
        product_id:   String(p.id),
        handle:       p.handle,
        title_jp:     p.title,
        title_en:     en.title || p.title,
        tags_jp:      p.tags   || [],
        tags_en:      en.tags  || [],
        // jp_images: used as Langify lookup keys only — not persisted to merch.json
        jp_images:    (p.images || []).map(i => i.src),
        min_price:    prices.length ? Math.min(...prices) : 0,
        max_price:    prices.length ? Math.max(...prices) : 0,
        published_at: p.published_at || '',
        available:    (p.variants || []).some(v => v.available),
      });
    }

    log(`  → ${products.length} products fetched so far`);
    if (dataJp.products.length < 250) break;
    page++;
    await sleep(300);
  }

  return products;
}

// ─── Git commit + R2 upload for merch.json ───────────────────────────────────
// merch.json is committed to the private repo (source of truth) AND uploaded
// to R2 so the site can fetch it publicly without the repo needing to be public.
// R2 key: merch.json (publicly accessible at R2_PUBLIC_URL/merch.json)

async function uploadMerchToR2() {
  const content = fs.readFileSync(path.join(process.cwd(), MERCH_FILE));
  await r2.send(new PutObjectCommand({
    Bucket:      R2_BUCKET,
    Key:         MERCH_FILE,
    Body:        content,
    ContentType: 'application/json',
  }));
  log(`Uploaded merch.json to R2 → ${R2_PUBLIC_URL}/${MERCH_FILE}`);
}

async function commitChanges(newCount, updatedCount) {
  try {
    execSync('git config user.name "holoVault Scraper"');
    execSync('git config user.email "scraper@holovault"');
    execSync('git add merch.json');

    const msg = [
      newCount     > 0 ? `Add ${newCount} new product${newCount !== 1 ? 's' : ''}` : '',
      updatedCount > 0 ? `Update ${updatedCount} product status${updatedCount !== 1 ? 'es' : ''}` : '',
    ].filter(Boolean).join(', ');

    execSync(`git commit -m ${JSON.stringify(msg)}`);
    execSync('git push');
    log(`Committed: ${msg}`);
  } catch (err) {
    log(`⚠ Git commit/push failed: ${err.message}`);
    throw err;
  }

  // Upload merch.json to R2 so the site can fetch it publicly
  await uploadMerchToR2();
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  log('── holoVault Scraper starting ──');

  // 1. Load existing archive — product_id presence here is the only gate
  //    for skipping image work. If it's here, images are already in R2.
  const existing    = loadExistingMerch();
  const existingMap = new Map(existing.map(p => [p.product_id, p]));
  log(`Loaded ${existing.length} existing products`);

  // 2. Fetch live Shopify catalog
  const shopifyProducts = await fetchAllShopifyProducts();
  log(`Fetched ${shopifyProducts.length} products from Shopify`);

  // 3. Fetch Langify EN image map once — covers all products
  const enImageMap = await fetchEnImageMap();

  // Guard: if the EN map is empty, Langify scrape failed or the page changed.
  // Skip new product processing to avoid committing ghost entries (products
  // with empty images[] that will never be retried due to the product_id gate).
  if (enImageMap.size === 0) {
    log('⚠ EN image map is empty — skipping new product processing this run');
    log('  Availability sync will still run for existing products.');
  }

  const currentIds = new Set(shopifyProducts.map(p => p.product_id));
  const shopifyMap = new Map(shopifyProducts.map(p => [p.product_id, p]));

  let newCount     = 0;
  let updatedCount = 0;

  // 4. Process NEW products only (skipped entirely if EN map is empty)
  if (enImageMap.size > 0) {
    for (const p of shopifyProducts) {
      // Already archived — skip, zero R2 calls
      if (existingMap.has(p.product_id)) continue;

      log(`New product: ${p.title_en} (${p.product_id})`);

      // Upload images 5 at a time to avoid hammering R2/CDN on large drops.
      // inChunks returns results in the same order as jp_images (order preserved).
      // fn returns null for skipped/failed images; filter(Boolean) removes them.
      const archivedImages = (await inChunks(
        p.jp_images.map((jpUrl, i) => ({ jpUrl, i })),
        5,
        async ({ jpUrl, i }) => {
          // EN-only: skip if Langify has no EN version for this JP image
          const enUrl = getEnUrl(jpUrl, enImageMap);
          if (!enUrl) {
            log(`  Image ${i}: no EN mapping — skipping`);
            return null;
          }

          const key = r2KeyFor(p.handle, enUrl);
          try {
            const publicUrl = await uploadToR2(enUrl, key);
            log(`  ↑ ${key}`);
            return publicUrl;
          } catch (err) {
            log(`  ⚠ Failed image ${i} (${enUrl}): ${err.message}`);
            return null; // not stored — only confirmed R2 uploads go into images[]
          }
        }
      )).filter(Boolean);

      // jp_images intentionally omitted from the stored entry
      existing.push({
        product_id:   p.product_id,
        handle:       p.handle,
        title_jp:     p.title_jp,
        title_en:     p.title_en,
        tags_jp:      p.tags_jp,
        tags_en:      p.tags_en,
        categories:       [],   // filled by the taxonomy sync below
        merch_categories: [],   // filled by the taxonomy sync below
        images:       archivedImages,
        min_price:    p.min_price,
        max_price:    p.max_price,
        published_at: p.published_at,
        is_available: p.available,
        delisted_at:  null,
      });

      existingMap.set(p.product_id, existing[existing.length - 1]);
      newCount++;
    }
  }

  // 5. Sync availability for existing products (always runs)
  for (const archived of existing) {
    if (!currentIds.has(archived.product_id)) {
      if (!archived.delisted_at) {
        archived.delisted_at  = new Date().toISOString();
        archived.is_available = false;
        log(`Delisted: ${archived.title_en} (${archived.product_id})`);
        updatedCount++;
      }
    } else {
      const live = shopifyMap.get(archived.product_id);
      if (!live) continue;

      if (archived.delisted_at) {
        archived.delisted_at  = null;
        archived.is_available = live.available;
        log(`Re-listed: ${archived.title_en}`);
        updatedCount++;
        continue;
      }

      if (archived.is_available !== live.available) {
        archived.is_available = live.available;
        log(`Availability: ${archived.title_en} → ${live.available ? 'Available' : 'Sold Out'}`);
        updatedCount++;
      }
    }
  }

  // 5b. Light taxonomy sync: crawl only the first pages (newest-first) of each
  // filter value — enough to categorize newly added products. Only fills
  // products with EMPTY fields; partial maps must never shrink existing data.
  // Full coverage comes from `node scraper.js --backfill-categories`.
  // Skipped entirely when no new products were found, so routine runs stay
  // fast (zero storefront page fetches).
  if (newCount === 0) {
    log('No new products — taxonomy sync skipped');
  } else {
    try {
      for (const tax of TAXONOMIES) {
        const map = await fetchTaxonomyMap(tax, 2);
        if (map.size === 0) continue;
        for (const archived of existing) {
          const cats = map.get(archived.handle);
          if (cats && cats.length && (archived[tax.field] || []).length === 0) {
            archived[tax.field] = cats;
            log(`${tax.field}: ${archived.title_en} → ${cats.join(', ')}`);
            updatedCount++;
          }
        }
      }
    } catch (err) {
      log(`⚠ Taxonomy sync failed (non-fatal): ${err.message}`);
    }
  }

  // 6. Skip commit if nothing changed, but always upload merch.json to R2
  // so the site stays in sync even if the repo is private.
  if (newCount === 0 && updatedCount === 0) {
    log('Nothing changed — skipping commit, uploading merch.json to R2 anyway');
    await uploadMerchToR2();
    return;
  }

  // 7. Write merch.json sorted newest first
  existing.sort((a, b) => b.published_at.localeCompare(a.published_at));
  fs.writeFileSync(
    path.join(process.cwd(), MERCH_FILE),
    JSON.stringify(existing, null, 2)
  );
  log(`Wrote merch.json (${existing.length} total products)`);

  // 8. Commit merch.json only — images live in R2
  commitChanges(newCount, updatedCount);
  log(`── Done. ${newCount} new, ${updatedCount} updated ──`);
}

// ─── EN Image Backfill (one-time) ────────────────────────────────────────────
// node scraper.js --backfill-en
//
// For products in merch.json whose images[] don't yet point to R2:
// re-fetches the live JP catalog to get original JP image URLs (needed as
// Langify lookup keys — old GitHub raw / Shopify CDN URLs won't match),
// finds EN versions, converts to WebP q80, uploads to R2, commits in batches.
//
// Known limitation: products fully delisted from Shopify won't appear in the
// live catalog, so their JP image URLs can't be retrieved. Those products will
// retain their old (non-R2) image URLs permanently unless handled manually.
//
// Safe to re-run — PutObject is idempotent on R2.

async function backfillEnImages() {
  log('── EN Image Backfill starting ──');

  const existing = loadExistingMerch();
  log(`Loaded ${existing.length} existing products`);

  // Re-fetch the JP catalog to get original JP image URLs for Langify key lookup
  log('Re-fetching JP catalog for image URL lookup…');
  const shopifyProducts  = await fetchAllShopifyProducts();
  const jpImagesByHandle = new Map(shopifyProducts.map(p => [p.handle, p.jp_images]));

  const enImageMap = await fetchEnImageMap();
  if (enImageMap.size === 0) {
    log('⚠ EN image map is empty — aborting backfill');
    return;
  }

  execSync('git config user.name "holoVault Scraper"');
  execSync('git config user.email "scraper@holovault"');

  const BATCH_SIZE = 50;
  let backfilledCount = 0;
  let batchCount      = 0;

  for (const product of existing) {
    // Skip if already pointing to R2
    if ((product.images || []).every(u => u.startsWith(R2_PUBLIC_URL))) continue;

    // Get JP image URLs from the live catalog for Langify key lookup.
    // Products fully delisted from Shopify won't be in jpImagesByHandle —
    // they're skipped here and will retain stale image URLs permanently.
    const jpImages = jpImagesByHandle.get(product.handle);
    if (!jpImages?.length) {
      log(`[${product.handle}] Not in live catalog (delisted?) — skipping`);
      continue;
    }

    // inChunks preserves order; null = no EN mapping or upload failed
    const updatedImages = (await inChunks(
      jpImages.map((jpUrl, i) => ({ jpUrl, i })),
      5,
      async ({ jpUrl, i }) => {
        const enUrl = getEnUrl(jpUrl, enImageMap);
        if (!enUrl) {
          log(`  [${product.handle}] Image ${i}: no EN mapping — skipping`);
          return null;
        }

        const key = r2KeyFor(product.handle, enUrl);
        try {
          const publicUrl = await uploadToR2(enUrl, key);
          log(`  [${product.handle}] ↑ ${key}`);
          return publicUrl;
        } catch (err) {
          log(`  [${product.handle}] ⚠ Failed image ${i}: ${err.message}`);
          return null;
        }
      }
    )).filter(Boolean);

    product.images = updatedImages;
    delete product.images_en; // remove legacy field if present

    backfilledCount++;
    batchCount++;
    log(`Backfilled ${updatedImages.length} images for ${product.title_en}`);

    if (batchCount >= BATCH_SIZE) {
      existing.sort((a, b) => b.published_at.localeCompare(a.published_at));
      fs.writeFileSync(path.join(process.cwd(), MERCH_FILE), JSON.stringify(existing, null, 2));
      execSync('git add merch.json');
      execSync(`git commit -m ${JSON.stringify(`Backfill R2 images (batch, ${backfilledCount} so far)`)}`);
      execSync('git push');
      log(`Pushed batch — ${backfilledCount} products done so far`);
      batchCount = 0;
    }

    await sleep(100);
  }

  if (batchCount > 0) {
    existing.sort((a, b) => b.published_at.localeCompare(a.published_at));
    fs.writeFileSync(path.join(process.cwd(), MERCH_FILE), JSON.stringify(existing, null, 2));
    execSync('git add merch.json');
    execSync(`git commit -m ${JSON.stringify(`Backfill R2 images (final, ${backfilledCount} total)`)}`);
    execSync('git push');
  }

  log(backfilledCount === 0
    ? 'Nothing needed backfill'
    : `── Backfill done. ${backfilledCount} products updated ──`
  );
}

// ─── Taxonomy Backfill (one-time) ────────────────────────────────────────────
// node scraper.js --backfill-categories
//
// Walks ALL of the shop's merch-type and merch-category filter pages and
// writes categories[] / merch_categories[] for every product still in the
// live catalog. ~450 paced page fetches → expect ~45–60 minutes. Needs no
// R2/git credentials — commit the updated merch.json and the next scheduled
// scrape uploads it to R2.
//
// Known limitation: delisted products don't appear in filtered pages, so they
// keep empty fields — the site falls back to title/tag inference for those.

async function backfillCategories() {
  log('── Taxonomy Backfill starting ──');
  const existing = loadExistingMerch();
  log(`Loaded ${existing.length} existing products`);

  for (const tax of TAXONOMIES) {
    const map = await fetchTaxonomyMap(tax, Infinity);
    if (map.size === 0) {
      log(`⚠ ${tax.field} map is empty — skipping this taxonomy`);
      continue;
    }
    let updated = 0;
    for (const p of existing) {
      const cats = map.get(p.handle);
      if (cats && !sameCategories(p[tax.field], cats)) {
        p[tax.field] = cats;
        updated++;
      } else if (!p[tax.field]) {
        p[tax.field] = [];
      }
    }
    // Write after each taxonomy so an interrupted run keeps its progress
    fs.writeFileSync(path.join(process.cwd(), MERCH_FILE), JSON.stringify(existing, null, 2));
    log(`${tax.field}: ${updated} of ${existing.length} products updated`);
  }

  // Persist: commit + upload when running in CI with credentials; when run
  // locally without them, the file is written — commit it manually and the
  // next scheduled scrape uploads it to R2.
  try {
    execSync('git config user.name "holoVault Scraper"');
    execSync('git config user.email "scraper@holovault"');
    execSync('git add merch.json');
    execSync('git diff --cached --quiet || git commit -m "Backfill merch type/category taxonomies"');
    execSync('git push');
    log('Committed and pushed merch.json');
  } catch (err) {
    log(`⚠ Git commit/push skipped — commit merch.json manually (${String(err.message).split('\n')[0]})`);
  }
  if (R2_BUCKET && process.env.R2_ACCOUNT_ID) {
    try { await uploadMerchToR2(); }
    catch (err) { log(`⚠ R2 upload failed: ${err.message}`); }
  } else {
    log('R2 credentials not set — the next scheduled scrape uploads merch.json to R2');
  }

  log('── Backfill done ──');
}

// ─── Entry point ─────────────────────────────────────────────────────────────

const entry = process.argv.includes('--backfill-en')         ? backfillEnImages
            : process.argv.includes('--backfill-categories') ? backfillCategories
            : main;
entry().catch(err => {
  console.error('Scraper failed:', err);
  process.exit(1);
});
