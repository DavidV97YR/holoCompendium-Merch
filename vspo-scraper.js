// ─── VSPO! merch scraper ─────────────────────────────────────────────────────
// The VSPO! half of the archive: store.vspo.jp, the official VSPO! shop. Runs
// as its own step after scraper.js (the hololive shop) in the same workflow,
// with the same R2 bucket and secrets, and writes its own files:
//
//   vspo-merch.json          committed here, the source of truth
//   vspo/merch.json          the same file in R2, which the site reads
//   VSPO/<handle>/<n>.webp   each product's images, in R2
//
// Records have merch.json's shape exactly, so the site reads both archives
// with one piece of code. What differs is where the fields come from:
//
//   • talents     the shop tags each product with its members' Japanese names
//                 (橘ひなの), which the site matches as it does hololive's
//   • categories  hololive's product types (the JP values scraper.js stores),
//                 worked out from the shop's TYPE_ tags and the names of the
//                 items in the product — a VSPO! product is usually a set, with
//                 one variant per item (A3ペナント, デザート皿, …)
//   • merch_categories   anniversary, birthday, event, digital, starter
//   • title_en    only the product's own /en-us page has it; the listing is
//                 Japanese in both locales. Fetched once, when first archived.
//
// Per run: new products are archived with their images; products whose image
// list grew get the rest; every product's availability is synced, and one
// missing from the shop is marked delisted (and unmarked if it comes back).
//
// The first run archives the whole shop (~430 products, ~4,000 images). It
// stops cleanly at BUDGET_MIN and commits what it finished; the next run picks
// up the rest, since a product is only skipped once its images are all in.
//
// DRY_RUN=1: fetch and classify, print a summary, write vspo-merch.dry.json,
// and upload or commit nothing. Images keep their shop URLs.

import fs   from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import sharp from 'sharp';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const BASE        = 'https://store.vspo.jp';
const MERCH_FILE  = 'vspo-merch.json';
const R2_JSON_KEY = 'vspo/merch.json';
const IMAGES_DIR  = 'VSPO';
const DRY_RUN     = process.env.DRY_RUN === '1';
const BUDGET_MIN  = Number(process.env.BUDGET_MIN || 50);
const IMAGE_CONCURRENCY = 4;

const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY || 'dry-run',
    secretAccessKey: process.env.R2_SECRET_KEY || 'dry-run',
  },
});
const R2_BUCKET     = process.env.R2_BUCKET;
const R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL || '').replace(/\/$/, '');

// ─── Helpers ─────────────────────────────────────────────────────────────────

function log(msg) { console.log(`[${new Date().toISOString()}] ${msg}`); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Same policy as scraper.js: a 4xx other than 408/429 is an answer, not a
// hiccup, so it is not retried.
async function fetchWithRetry(url, retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    let res;
    try {
      res = await fetch(url, { signal: AbortSignal.timeout(30_000), headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return res;
    } catch (err) {
      const status = res ? res.status : 0;
      const definitive = status >= 400 && status < 500 && status !== 408 && status !== 429;
      if (definitive || i === retries - 1) throw err;
      const retryAfter = res ? parseInt(res.headers.get('retry-after') || '0', 10) : 0;
      await sleep(retryAfter > 0 ? retryAfter * 1000 : delayMs * Math.pow(2, i) + Math.random() * 1000);
    }
  }
}

async function inChunks(items, size, fn) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(...await Promise.all(items.slice(i, i + size).map(fn)));
  return out;
}

// ─── Classification ──────────────────────────────────────────────────────────
// Product types, in the vocabulary scraper.js stores (the hololive shop's own
// JP values), so the site's labels and filter apply unchanged. Tested against
// the shop's TYPE_ tags and every item name in the product; first match per
// type, listed in this order.
const TYPE_RULES = [
  ['アクリルスタンド・アクリルパネル', /アクリル(スタンド|パネル|ジオラマ|ブロック)|アクスタ/],
  ['キーホルダー・ストラップ',         /キーホルダー|ストラップ|チャーム|キーリング/],
  ['バッジ・ピンバッジ',               /缶バッジ|バッジ|ピンズ/],
  ['ぬいぐるみ',                       /ぬいぐるみ|マスコット/],
  ['ステーショナリー',                 /ステッカー|クリアファイル|ノート|ペン|ポストカード|色紙|ブロマイド|カード(?!ゲーム)/],
  ['ボイス',                           /^TYPE_ボイス$|ボイス/],
  ['タペストリー',                     /^TYPE_タペストリー・ポスター$|タペストリー|ポスター|ペナント/],
  ['フィギュア',                       /フィギュア/],
  ['カードゲーム・サプライ',           /Xross Stars|スリーブ|ブースター|プレイマット|デッキ/i],
  ['アパレル・アパレル小物',           /^TYPE_アパレル$|Tシャツ|パーカー|キャップ|ジャケット|ソックス|パジャマ|ユニフォーム|アパレル/],
  ['生活雑貨',                         /^TYPE_雑貨・小物$|タオル|マグ|タンブラー|クッション|ブランケット|皿|カトラリー|トレイ|マット|時計|ポーチ|バッグ/],
  ['CD・LP・BD',                       /\bCD\b|アルバム|Blu-ray/],
];

// Merch categories. The first three are hololive's own values (the site
// already labels them); the last two are VSPO!'s, and the site labels those too.
function merchCategoriesFor(title, tags) {
  const out = [];
  if (/周年記念/.test(title))                                   out.push('周年記念グッズ');
  if (/誕生日記念/.test(title))                                 out.push('誕生日記念グッズ');
  if (tags.includes('TYPE_イベントグッズ'))                     out.push('イベント・ライブグッズ');
  if (tags.includes('TYPE_デジタルグッズ') || tags.includes('TYPE_ボイス')) out.push('デジタルコンテンツ');
  if (tags.some(t => /スタートセット/.test(t)) || /スタートセット/.test(title)) out.push('スターターグッズ');
  return out;
}

// The first type is the one a card shows. A product the shop files under a
// single specific TYPE_ (apparel, voice, a tapestry) leads with that; a set,
// which the shop only calls グッズ / 雑貨・小物, leads with its first item type.
const LEAD_TAGS = { 'TYPE_アパレル': 'アパレル・アパレル小物', 'TYPE_ボイス': 'ボイス', 'TYPE_タペストリー・ポスター': 'タペストリー' };

function typesFor(title, tags, itemNames) {
  const haystack = [title, ...tags, ...itemNames];
  const found = TYPE_RULES.filter(([, re]) => haystack.some(s => re.test(s))).map(([type]) => type);
  const lead  = tags.map(t => LEAD_TAGS[t]).filter(Boolean);
  return [...new Set([...lead, ...found])];
}

// The shop's titles mix in full-width spaces ("Aizawa Ema　5th Anniversary").
const tidy = s => String(s || '').replace(/[\s　]+/g, ' ').trim();

// ─── Shop ────────────────────────────────────────────────────────────────────

async function fetchAllProducts() {
  const out = [];
  for (let page = 1; ; page++) {
    const res  = await fetchWithRetry(`${BASE}/products.json?limit=250&page=${page}`);
    const list = (await res.json()).products || [];
    if (!list.length) break;
    out.push(...list);
    log(`  page ${page}: ${list.length} products (${out.length} so far)`);
    if (list.length < 250) break;
    await sleep(500);
  }
  return out;
}

// The English title lives only on the product's own /en-us page.
async function englishTitle(handle) {
  try {
    const res = await fetchWithRetry(`${BASE}/en-us/products/${encodeURIComponent(handle)}.json`);
    return tidy(((await res.json()).product || {}).title);
  } catch (e) {
    log(`  ⚠ no English title for ${handle}: ${e.message}`);
    return '';
  }
}

// ─── R2 ──────────────────────────────────────────────────────────────────────

function r2KeyFor(handle, imgUrl) {
  const base = imgUrl.split('?')[0].split('/').pop() || 'image';
  return `${IMAGES_DIR}/${handle}/${base.replace(/\.[^.]+$/, '')}.webp`;
}

// WebP q80, as scraper.js stores hololive's.
async function archiveImage(handle, src) {
  if (DRY_RUN) return src;
  const key  = r2KeyFor(handle, src);
  const raw  = Buffer.from(await (await fetchWithRetry(src)).arrayBuffer());
  const webp = await sharp(raw).webp({ quality: 80 }).toBuffer();
  await r2.send(new PutObjectCommand({ Bucket: R2_BUCKET, Key: key, Body: webp, ContentType: 'image/webp' }));
  return `${R2_PUBLIC_URL}/${key}`;
}

async function uploadJson() {
  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: R2_JSON_KEY,
    Body: fs.readFileSync(path.join(process.cwd(), MERCH_FILE)),
    ContentType: 'application/json',
  }));
  log(`Uploaded ${MERCH_FILE} → ${R2_PUBLIC_URL}/${R2_JSON_KEY}`);
}

// ─── Archive I/O ─────────────────────────────────────────────────────────────

const BY_NEWEST = (a, b) => (b.published_at || '').localeCompare(a.published_at || '');

function loadExisting() {
  const fp = path.join(process.cwd(), MERCH_FILE);
  if (!fs.existsSync(fp)) { log(`No ${MERCH_FILE} yet — starting fresh`); return []; }
  try { return JSON.parse(fs.readFileSync(fp, 'utf8')); }
  catch { log(`Could not parse ${MERCH_FILE} — starting fresh`); return []; }
}

function write(products) {
  fs.writeFileSync(path.join(process.cwd(), DRY_RUN ? 'vspo-merch.dry.json' : MERCH_FILE),
                   JSON.stringify([...products].sort(BY_NEWEST), null, 2));
}

function commit(msg) {
  execSync('git config user.name "holoVault Scraper"');
  execSync('git config user.email "scraper@holovault"');
  execSync(`git add ${MERCH_FILE}`);
  try { execSync('git diff --cached --quiet'); log('Nothing to commit'); return; } catch { /* staged changes */ }
  // Non-fatal, as in scraper.js: the R2 upload after this still runs, so the
  // site gets this run's archive even when the push fails.
  try {
    execSync(`git commit -m ${JSON.stringify(msg)}`);
    execSync('git push');
    log(`Committed: ${msg}`);
  } catch (e) { log(`⚠ Git commit/push failed: ${e.message}`); }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  log(`── VSPO! merch scraper starting${DRY_RUN ? ' (DRY RUN)' : ''} ──`);
  const started = Date.now();
  const outOfTime = () => Date.now() - started > BUDGET_MIN * 60_000;

  const existing = loadExisting();
  const byId     = new Map(existing.map(p => [p.product_id, p]));
  log(`Loaded ${existing.length} archived products`);

  // Everything the shop lists, including the signed-postcard lottery entry
  // pages (tagged "Excluded Product", ¥0, so the site shows no price).
  const live = await fetchAllProducts();
  log(`Shop lists ${live.length} products`);
  const liveIds = new Set(live.map(p => String(p.id)));

  let added = 0, toppedUp = 0, synced = 0, delisted = 0, stopped = false;

  for (const p of live) {
    const id       = String(p.id);
    const tags     = p.tags || [];
    const items    = (p.variants || []).map(v => v.title).filter(t => t && t !== 'Default Title');
    const prices   = (p.variants || []).map(v => parseFloat(v.price)).filter(n => n > 0);
    const images   = (p.images || []).map(i => i.src);
    const record   = byId.get(id);

    // Availability and tags follow the shop on every run (prices, as in
    // scraper.js, are the ones from when the product was archived).
    if (record) {
      const available = (p.variants || []).some(v => v.available);
      if (record.is_available !== available || record.delisted_at) {
        record.is_available = available; record.delisted_at = null; synced++;
      }
      record.tags_jp = tags; record.tags_en = tags;
      // A new product's /en-us page can take days to be translated; until it is
      // the title falls back to the Japanese one, and is asked again for a month.
      if (record.title_en === record.title_jp && Date.now() - Date.parse(record.published_at) < 30 * 864e5) {
        const en = await englishTitle(p.handle);
        if (en && en !== record.title_en) { record.title_en = en; synced++; }
      }
      if (record.images.length >= images.length) continue;     // complete
    }

    if (outOfTime()) { stopped = true; break; }

    const entry = record || {
      product_id:       id,
      handle:           p.handle,
      title_jp:         tidy(p.title),
      title_en:         '',
      tags_jp:          tags,
      tags_en:          tags,
      categories:       typesFor(p.title || '', tags, items),
      merch_categories: merchCategoriesFor(p.title || '', tags),
      images:           [],
      min_price:        prices.length ? Math.min(...prices) : 0,
      max_price:        prices.length ? Math.max(...prices) : 0,
      published_at:     p.published_at || '',
      is_available:     (p.variants || []).some(v => v.available),
      delisted_at:      null,
    };
    if (!entry.title_en) entry.title_en = (await englishTitle(p.handle)) || entry.title_jp;

    // Only the images not archived yet: an entry keeps the ones it has.
    const have = new Set(entry.images.map(u => u.split('/').pop()));
    const todo = images.filter(src => !have.has(r2KeyFor(p.handle, src).split('/').pop()));
    const done = await inChunks(todo, IMAGE_CONCURRENCY, src =>
      archiveImage(p.handle, src).catch(e => { log(`  ⚠ image ${src}: ${e.message}`); return null; }));
    entry.images.push(...done.filter(Boolean));

    if (record) toppedUp++; else { byId.set(id, entry); added++; log(`New: ${entry.title_en}`); }
    if ((added + toppedUp) % 25 === 0) write([...byId.values()]);   // progress survives a kill
  }

  // Gone from the shop: delisted, kept in the archive.
  for (const rec of byId.values()) {
    if (!liveIds.has(rec.product_id) && !rec.delisted_at) {
      rec.delisted_at = new Date().toISOString(); rec.is_available = false; delisted++;
    }
  }

  const all = [...byId.values()];
  write(all);
  log(`${all.length} archived — ${added} new, ${toppedUp} topped up, ${synced} status changes, ${delisted} delisted`
      + (stopped ? `; stopped at the ${BUDGET_MIN}-minute budget, the next run continues` : ''));

  if (DRY_RUN) { summarize(all); return; }

  const msg = [added && `VSPO!: add ${added} product${added !== 1 ? 's' : ''}`,
               toppedUp && `${toppedUp} topped up`,
               (synced || delisted) && `${synced + delisted} status update${synced + delisted !== 1 ? 's' : ''}`]
              .filter(Boolean).join(', ') || 'VSPO!: refresh';
  commit(msg);
  try { await uploadJson(); }
  catch (e) { log(`⚠ upload of ${R2_JSON_KEY} failed (committed; the next run re-uploads): ${e.message}`); }
}

function summarize(all) {
  const count = key => { const m = {}; all.forEach(p => (p[key] || []).forEach(v => m[v] = (m[v] || 0) + 1)); return m; };
  console.log('types:', count('categories'));
  console.log('merch categories:', count('merch_categories'));
  console.log('no type:', all.filter(p => !p.categories.length).length, '| sample:',
              all.filter(p => !p.categories.length).slice(0, 5).map(p => p.title_jp).join(' / '));
  console.log('sample:', JSON.stringify(all[0], null, 1).slice(0, 900));
}

main().catch(e => { console.error(e); process.exit(1); });
