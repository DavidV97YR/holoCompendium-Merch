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
//   • categories  product types. The shop's own where it has specific ones
//                 (its TYPE_ tags and its card game page), then the finer
//                 hololive types worked out from the names of the items in the
//                 product — a VSPO! product is usually a set, with one variant
//                 per item (A3ペナント, デザート皿, …). A TYPE_ tag the shop adds
//                 later is kept under its own name.
//   • merch_categories   the shop's commemorative / event / digital /
//                 starter set, and lottery entries
//   • series      the shop's collections (VSPO!LOOK, SHOWDOWN2026, …): the
//                 site's Series filter, as hololive's units fill its Group one.
//                 A collection the shop adds later appears by itself.
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
import { sendNewProductEmail, sendBroadcast } from './email.js';

const BASE        = 'https://store.vspo.jp';
const MERCH_FILE  = 'vspo-merch.json';
const R2_JSON_KEY = 'vspo/merch.json';
const R2_LATEST_KEY = 'vspo/latest.json';   // Home's newest 40 (see latestJson)
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
async function fetchWithRetry(url, retries = 3, delayMs = 1000, headers = {}) {
  for (let i = 0; i < retries; i++) {
    let res;
    try {
      res = await fetch(url, { signal: AbortSignal.timeout(30_000), headers: { 'User-Agent': 'Mozilla/5.0', ...headers } });
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
// Types are in the vocabulary scraper.js stores (the hololive shop's own JP
// values), so the site's labels and filter apply to both archives.

// The shop's own types, where they are specific. Its other TYPE_ tags are too
// broad to filter by (グッズ is on 9 products in 10) or are categories rather
// than types (event, digital).
const OFFICIAL_TYPES = {
  'TYPE_アパレル':               'アパレル・アパレル小物',
  'TYPE_タペストリー・ポスター': 'タペストリー',
  'TYPE_ボイス':                 'ボイス',
};
const BROAD_TYPE_TAGS = new Set(['TYPE_グッズ', 'TYPE_雑貨・小物', 'TYPE_その他', 'TYPE_イベントグッズ', 'TYPE_デジタルグッズ']);

// The finer types, from the product's title and item names; first match per
// type, in this order.
const TYPE_RULES = [
  ['アクリルスタンド・アクリルパネル', /アクリル(スタンド|パネル|ジオラマ|ブロック)|アクスタ/],
  ['キーホルダー・ストラップ',         /キーホルダー|ストラップ|チャーム|キーリング/],
  ['バッジ・ピンバッジ',               /缶バッジ|バッジ|ピンズ/],
  ['ぬいぐるみ',                       /ぬいぐるみ|マスコット/],
  ['ステーショナリー',                 /ステッカー|クリアファイル|ノート|ペン|ポストカード|色紙|ブロマイド|カード(?!ゲーム)/],
  ['ボイス',                           /ボイス/],
  ['タペストリー',                     /タペストリー|ポスター|ペナント/],
  ['フィギュア',                       /フィギュア/],
  ['カードゲーム・サプライ',           /Xross Stars|スリーブ|ブースター|プレイマット|デッキ/i],
  ['アパレル・アパレル小物',           /Tシャツ|パーカー|キャップ|ジャケット|ソックス|パジャマ|ユニフォーム|アパレル/],
  ['生活雑貨',                         /タオル|マグ|タンブラー|クッション|ブランケット|皿|カトラリー|トレイ|マット|時計|ポーチ|バッグ/],
  ['CD・LP・BD',                       /\bCD\b|アルバム|Blu-ray|Songs Collection|限定盤|通常盤/],
];

// The signed-postcard lottery entry pages: ¥0, nothing to buy. The shop marks
// them "Excluded Product"; the title says what they are.
const isLottery = (title, tags) => /抽選/.test(title) || tags.includes('Excluded Product');

// `inShop` is the set of the product's shop collections, by handle.
function typesFor(title, tags, itemNames, inShop) {
  if (isLottery(title, tags)) return ['OTHERS'];   // an entry page, not an item
  const official = [];
  for (const t of tags) {
    if (!t.startsWith('TYPE_') || BROAD_TYPE_TAGS.has(t)) continue;
    official.push(OFFICIAL_TYPES[t] || t.slice(5));   // a new one keeps its own name
  }
  if (inShop.has('cardgame')) official.push('カードゲーム・サプライ');
  const haystack = [title, ...itemNames];
  const found = TYPE_RULES.filter(([, re]) => haystack.some(s => re.test(s))).map(([type]) => type);
  const types = [...new Set([...official, ...found])];
  // Anything else (VSPO! GEAR keyboards and mice, art books) is hololive's
  // "Others", so every product can be found by type.
  return types.length ? types : ['OTHERS'];
}

// Categories. イベント・ライブグッズ, デジタルコンテンツ and スターターグッズ are
// hololive's own values; the rest are VSPO!'s, and the site labels them too.
// `commemorative`: the shop files the product under its own 記念グッズ
// (Commemorative goods) filter; see fetchOfficialFilter.
function merchCategoriesFor(title, tags, inShop, commemorative) {
  const out = [];
  // Commemorative merch: the shop's own tag, plus what the title says it is
  // (anniversary, birthday, a talent's own yearly Day: 胡桃の日記念), because
  // the shop left the tag off a few older ones (胡桃のあ 誕生日記念2023). A
  // release's signed-postcard lottery page is in it too, beside the merch, so
  // the lottery is seen where the release is (the shop's filter leaves it out).
  if (commemorative || /周年記念|誕生日記念|の日記念/.test(title))
                                                                out.push('記念グッズ');
  if (tags.includes('TYPE_イベントグッズ'))                     out.push('イベント・ライブグッズ');
  if (tags.includes('TYPE_デジタルグッズ') || tags.includes('TYPE_ボイス')) out.push('デジタルコンテンツ');
  if (inShop.has('startset') || /スタートセット/.test(title))   out.push('スターターグッズ');
  if (isLottery(title, tags))                                   out.push('抽選応募');
  return out;
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

// The product's sales period, which the shop shows only on the product page
// ("Sales period:2026-09-30 19:00 (JST) 〜 2026-10-14 0:00 (JST)"); its JSON
// just calls a product not on sale yet unavailable, the same as a sold-out one.
// { start, end } as ISO times, null where the page gives none; null for the
// whole when the page could not be read, so the next run asks again.
async function salesPeriod(handle) {
  try {
    const res  = await fetchWithRetry(`${BASE}/en-us/products/${encodeURIComponent(handle)}`);
    const html = await res.text();
    const box  = /class="Product_SalesPeriod_Wrapper"[^>]*>([\s\S]*?)<\/div>/.exec(html);
    // Either side can be blank ("Sales period: (JST) 〜 2026-10-11 0:00 (JST)"
    // is on sale now, until the 11th), so each side is read on its own.
    const [from, to] = ((box && box[1]) || '').split(/[〜～~]/);
    const at = side => {
      const m = /(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})/.exec(side || '');
      return m ? `${m[1]}T${m[2].padStart(2, '0')}:${m[3]}:00+09:00` : null;
    };
    return { start: at(from), end: at(to) };
  } catch (e) {
    log(`  ⚠ no sales period for ${handle}: ${e.message}`);
    return null;
  }
}

// When a product's sale or application period really opened, for one whose
// sales period bar leaves the start blank ("販売期間： 〜 2026年05月20日
// 0時00分"). The Japanese description gives it:
//   【受注期間】 5月5日（火）～5月19日（火）23:59まで
//   日本標準時 (JST)：8月25日（火）21:30～…          (VSPO! EN's, after the label)
//   ▼抽選応募期間 2026年2月26日（木）00:00～23:59   (postcard lotteries)
// It differs from published_at in two ways. A product the shop re-listed after
// its sale (to show it as ended) has the re-listing: Kisaragi Ren's birthday
// set, on sale May 5–19, reads as listed on May 21. And a lottery page can go
// up hours before applications open: Amayui Moka's at 20:00 on Feb 25, for
// 00:00 on the 26th.
//
// Used only when it falls on another day than the listing. On the same day
// the listing is the better time: the description's is approximate ("20:00頃
// ～", announced before the stream), and a product went up at 20:31 for one
// said to open around 20:00. Of the 186 products without a start on the bar,
// that picked out exactly the 8 above that were wrong (Ren's, Sumire's,
// Nazuna's, Mimi's 4th anniversary, and 4 lotteries). With no year given, the
// year nearest the listing that does not put the start after the end. An ISO
// time, or null (no such line, the same day, or the page could not be read).
async function orderStart(handle, endIso, listedIso) {
  try {
    // The Japanese page: the English one's description is machine translated.
    const res  = await fetchWithRetry(`${BASE}/products/${encodeURIComponent(handle)}`, 3, 1000, { 'Accept-Language': 'ja' });
    const text = (await res.text()).replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
    // Sale and application periods only (受注・販売・予約・応募・受付), not
    // another period a description may give first, such as 【配信期間】.
    const m = /(?:【[^】]{0,10}(?:受注|販売|予約|応募|受付)期間】|▼[^\s▼]{0,10}(?:受注|販売|予約|応募|受付)期間)\s*(?:日本標準時\s*\(JST\)[：:]\s*)?(?:(\d{4})年)?(\d{1,2})月(\d{1,2})日(?:[（(][^）)]*[）)])?\s*(?:(\d{1,2}):(\d{2}))?/.exec(text);
    if (!m) return null;
    const listed = Date.parse(listedIso), end = Date.parse(endIso) || Infinity;
    const at = y => `${y}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}T${(m[4] || '0').padStart(2, '0')}:${m[5] || '00'}:00+09:00`;
    const year = new Date(listed + 9 * 36e5).getUTCFullYear();
    const iso = m[1] ? at(m[1]) : [year - 1, year, year + 1].map(at)
      .filter(s => Date.parse(s) <= end)
      .sort((a, b) => Math.abs(Date.parse(a) - listed) - Math.abs(Date.parse(b) - listed))[0];
    if (!iso) return null;
    return iso.slice(0, 10) === new Date(listed + 9 * 36e5).toISOString().slice(0, 10) ? null : iso;
  } catch (e) {
    log(`  ⚠ no order period for ${handle}: ${e.message}`);
    return null;
  }
}

// The shop's collections, as product id → { handles, series }. `handles` is
// every collection the product is in (the card game and starter set pages are
// read from it); `series` names the ones that are a series.
//
// Not a series: the shop's own navigation and type pages, listed here, and
// its talent pages. A talent page's title is the talent's tag (花芽 すみれ →
// 花芽すみれ) and reads as a name: short, with no digits, brackets or
// punctuation and no ぶいすぽ. That test matters because the shop also tags
// products with some series' names (ぶいすぽっ！サマービーチ2024). So a new
// talent's page is skipped without an edit, and anything else the shop adds
// is a new series and appears by itself.
const NOT_SERIES = new Set(['all', 'frontpage', 'new', 'ending-soon', 'members', 'en-members', 'vspo-en',
  'goods', 'goods-accessories', 'others', 'apparel', 'event-goods', 'tapestry-poster', 'digitalgoods',
  'voice', 'cardgame', 'startset']);
// Read for types and categories, though not series.
const WANTED = new Set(['cardgame', 'startset']);

// The shop's Japanese titles, tidied: its English ones are machine translated
// ("V-Spot!"). Brackets around a whole title go, and the brand reads VSPO!.
function seriesName(title) {
  return tidy(String(title)
    .replace(/^【(.*)】/, '$1 ')
    .replace(/ぶいすぽっ[！!]?|ぶいすぽ(?!っ)/g, 'VSPO! '))
    .replace(/VSPO! (?=[」』])/g, 'VSPO!');
}

async function fetchCollections(talentTags) {
  const list = [];
  for (let page = 1; ; page++) {
    const res  = await fetchWithRetry(`${BASE}/collections.json?limit=250&page=${page}`);
    const cols = (await res.json()).collections || [];
    list.push(...cols);
    if (cols.length < 250) break;
  }
  const byProduct = new Map();
  const seenSets  = new Set();   // two collections with the same products are one series
  // The talent pages' names: the new-merch email shows these as chips.
  byProduct.talents = new Set();
  for (const c of list) {
    if (!c.products_count) continue;
    const name       = String(c.title).replace(/[\s　]+/g, '');
    const talentPage = talentTags.has(name) && name.length <= 7
                       && !/[0-9０-９!！?？「」【】『』()（）・&＆]|ぶいすぽ/.test(name);
    const isSeries   = !NOT_SERIES.has(c.handle) && !talentPage;
    if (talentPage) byProduct.talents.add(name);
    if (!isSeries && !WANTED.has(c.handle)) continue;
    const ids = [];
    for (let page = 1; ; page++) {
      const res   = await fetchWithRetry(`${BASE}/collections/${encodeURIComponent(c.handle)}/products.json?limit=250&page=${page}`);
      const prods = (await res.json()).products || [];
      ids.push(...prods.map(p => String(p.id)));
      if (prods.length < 250) break;
    }
    await sleep(300);
    const key    = ids.slice().sort().join(',');
    const series = isSeries && !seenSets.has(key) ? seriesName(c.title) : null;
    if (series) seenSets.add(key);
    for (const id of ids) {
      if (!byProduct.has(id)) byProduct.set(id, { handles: new Set(), series: [] });
      const e = byProduct.get(id);
      e.handles.add(c.handle);
      if (series && !e.series.includes(series)) e.series.push(series);
    }
  }
  return byProduct;
}

// Every product the shop lists under one of its own filters. These come from
// product fields that /products.json leaves out, so they are read from the
// filtered listing pages, as scraper.js reads hololive's taxonomy. The pages
// are asked for in Japanese: the English site names the filter values in
// English, and a Japanese value there matches (almost) nothing.
async function fetchOfficialFilter(field, value) {
  const handles = new Set();
  for (let page = 1; page <= 60; page++) {
    const res  = await fetch(`${BASE}/collections/all?filter.p.m.custom.${field}=${encodeURIComponent(value)}&page=${page}`,
      { signal: AbortSignal.timeout(30_000), headers: { 'User-Agent': 'Mozilla/5.0', 'Accept-Language': 'ja' } });
    if (!res.ok) throw new Error(`HTTP ${res.status} for the ${value} filter`);
    // Each product card is a <product-item>; its first product link is it.
    const found = (await res.text()).split('<product-item ').slice(1)
      .map(card => (card.match(/\/products\/([^"?#\/<\s]+)/) || [])[1]).filter(Boolean).map(decodeURIComponent);
    const before = handles.size;
    found.forEach(h => handles.add(h));
    if (!found.length || handles.size === before) return handles;   // past the last page
    await sleep(400);
  }
  return handles;
}

// ─── R2 ──────────────────────────────────────────────────────────────────────

// Where the shop keeps each archived picture, for the site's full-screen
// viewer: "files/png" for <shop CDN>/files/<name>.png, null for a picture the
// shop no longer lists. The viewer asks the shop's CDN for a copy sized to the
// screen (the originals run to 4500×4500, which a phone struggled to slide
// between); the archive keeps the originals for when the product is gone.
// Stored this short, not as addresses, because it goes to every visitor in
// the archive file. Matched by name, as r2KeyFor names the archived copy.
function shopFiles(archived, shopSrcs) {
  const byName = new Map();
  for (const src of shopSrcs || []) {
    const m = /\/(files|products)\/([^/?]+)\.([a-z0-9]+)(?:\?|$)/i.exec(src);
    if (m) byName.set(m[2], m[1] + '/' + m[3]);
  }
  return (archived || []).map(u => byName.get(String(u).split('/').pop().replace(/\.webp$/, '')) || null);
}

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

// The site's Home page shows the 40 newest products and how many are still
// listed, and downloaded the whole archive for it. latest.json is just that,
// uploaded beside the archive each run: { total, products: [the 40 newest
// still listed, as stored] }. Newest by the site's own rule (listedTs in its
// js/merch-core.js): the sale's opening where the shop gives one, else the
// listing. A failed upload only warns: Home falls back to the full archive.
function latestJson(archive) {
  const ts = p => Date.parse(p.sale_start) || Date.parse(p.published_at) || 0;
  const listed = archive.filter(p => !p.delisted_at);
  return JSON.stringify({ total: listed.length, products: [...listed].sort((a, b) => ts(b) - ts(a)).slice(0, 40) });
}

async function uploadLatest(content) {
  try {
    await r2.send(new PutObjectCommand({
      Bucket:      R2_BUCKET,
      Key:         R2_LATEST_KEY,
      Body:        latestJson(JSON.parse(content)),
      ContentType: 'application/json',
    }));
    log(`Uploaded ${R2_LATEST_KEY} → ${R2_PUBLIC_URL}/${R2_LATEST_KEY}`);
  } catch (e) {
    log(`⚠ upload of ${R2_LATEST_KEY} failed (Home falls back to the full archive): ${e.message}`);
  }
}

async function uploadJson() {
  const content = fs.readFileSync(path.join(process.cwd(), MERCH_FILE));
  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: R2_JSON_KEY,
    Body: content,
    ContentType: 'application/json',
  }));
  log(`Uploaded ${MERCH_FILE} → ${R2_PUBLIC_URL}/${R2_JSON_KEY}`);
  await uploadLatest(content);
}

// ─── Archive I/O ─────────────────────────────────────────────────────────────

const BY_NEWEST = (a, b) => (b.published_at || '').localeCompare(a.published_at || '');

function loadExisting() {
  const fp = path.join(process.cwd(), MERCH_FILE);
  if (!fs.existsSync(fp)) { log(`No ${MERCH_FILE} yet — starting fresh`); return []; }
  try { return JSON.parse(fs.readFileSync(fp, 'utf8')); }
  // Unreadable stops the run, as in scraper.js: starting fresh would save
  // over it and lose every delisted product for good.
  catch (e) { throw new Error(`${MERCH_FILE} could not be read (${e.message}); fix or restore it before the next run`); }
}

function write(products) {
  fs.writeFileSync(path.join(process.cwd(), DRY_RUN ? 'vspo-merch.dry.json' : MERCH_FILE),
                   JSON.stringify([...products].sort(BY_NEWEST), null, 2));
}

// True once the archive is in the repo (or already was).
function commit(msg) {
  execSync('git config user.name "holoVault Scraper"');
  execSync('git config user.email "scraper@holovault"');
  execSync(`git add ${MERCH_FILE}`);
  try { execSync('git diff --cached --quiet'); log('Nothing to commit'); return true; } catch { /* staged changes */ }
  // Non-fatal, as in scraper.js: the R2 upload after this still runs, so the
  // site gets this run's archive even when the push fails.
  try {
    execSync(`git commit -m ${JSON.stringify(msg)}`);
    execSync('git push');
    log(`Committed: ${msg}`);
    return true;
  } catch (e) { log(`⚠ Git commit/push failed: ${e.message}`); return false; }
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
  // A shop in maintenance, or a cut-short answer, would otherwise mark most of
  // the archive delisted. Stop and fail the run instead; the next one retries.
  const listedBefore = existing.filter(p => !p.delisted_at).length;
  if (listedBefore > 20 && live.length < listedBefore / 2) {
    throw new Error(`Shop listed only ${live.length} of ${listedBefore} products; not syncing this run`);
  }
  const liveIds = new Set(live.map(p => String(p.id)));

  // Talents' Japanese names, as the shop tags products with them: every tag
  // with no TYPE_ prefix and no Latin letters (the shop's own flags are
  // English). Only used to tell a talent's collection from a series.
  const talentTags = new Set(live.flatMap(p => p.tags || []).filter(t => !/^TYPE_|[A-Za-z]/.test(t)));
  // If the collections can't be read, products keep the types, categories and
  // series they already have rather than losing them for a run.
  let collections = null;
  try {
    collections = await fetchCollections(talentTags);
    log(`Read the shop's collections: ${new Set([...collections.values()].flatMap(e => e.series)).size} series`);
  } catch (e) {
    log(`⚠ Could not read the shop's collections (${e.message}); keeping what each product has`);
  }
  let commemorative = null;
  try {
    commemorative = await fetchOfficialFilter('tag', '記念グッズ');
    log(`Read the shop's Commemorative goods filter: ${commemorative.size} products`);
  } catch (e) {
    log(`⚠ Could not read the shop's Commemorative goods filter (${e.message}); keeping what each product has`);
  }
  const shopRead = !!(collections && commemorative);
  const shopInfo = id => (collections && collections.get(id)) || { handles: new Set(), series: [] };
  const isCommemorative = handle => !!(commemorative && commemorative.has(handle));

  let added = 0, toppedUp = 0, synced = 0, delisted = 0, stopped = false;
  const fresh = [];   // products new to the archive this run, for the email

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
      record.shop_files = shopFiles(record.images, images);
      // Read once (the archive's older products on the first run after this was
      // added), and again while a sale is still to start, in case it moves.
      const pending = record.sale_start && !available && Date.parse(record.sale_start) > Date.now();
      // Once more for a period with a start and no end that the first parser
      // read: it took an end-only period ("〜 2026-10-11 0:00") as the start,
      // and the two look the same once stored (Runa's, Beni's and Ema's
      // anniversary sets on sale, dated as opening when they close).
      // sale_v marks a period this parser has read.
      const reread = record.sale_start && !record.sale_end && !record.sale_v;
      if ((record.sale_start === undefined || pending || reread) && !outOfTime()) {
        const sp = await salesPeriod(p.handle);
        // A start read from the description (sale_from, see orderStart) stays
        // while the bar still gives none; one the bar gives replaces it.
        if (sp && !sp.start && record.sale_from === 'text') sp.start = record.sale_start;
        else if (sp && sp.start) delete record.sale_from;
        if (sp && (sp.start !== record.sale_start || sp.end !== record.sale_end)) {
          record.sale_start = sp.start; record.sale_end = sp.end; synced++;
        }
        if (sp && !record.sale_v) { record.sale_v = 2; synced++; }
      }
      // No start on the bar: read once what the description gives
      // (orderStart). sale_v 3 marks that as done. The bar has been read when
      // sale_start is set at all (null: read, no start). Not only sale_v 2:
      // a product with no bar whatsoever, read before sale_v existed, has no
      // sale_v (61 of them, the postcard lotteries among them).
      if (record.sale_start === null && (record.sale_v || 0) < 3 && !outOfTime()) {
        const start = await orderStart(p.handle, record.sale_end, record.published_at);
        if (start) {
          record.sale_start = start; record.sale_from = 'text';
          log(`  ${p.handle}: listed ${record.published_at}, opens ${start} by its description`);
        }
        record.sale_v = 3; synced++;
        await sleep(500);
      }
      // Worked out again every run, so a rule added here, or a collection the
      // shop adds, reaches the whole archive, not only products archived after.
      if (shopRead) {
        const info = shopInfo(id);
        record.categories       = typesFor(p.title || '', tags, items, info.handles);
        record.merch_categories = merchCategoriesFor(p.title || '', tags, info.handles, isCommemorative(p.handle));
        record.series           = info.series;
      }
      // A new product's /en-us page can take days to be translated; until it is
      // the title falls back to the Japanese one, and is asked again for a month.
      if (record.title_en === record.title_jp && Date.now() - Date.parse(record.published_at) < 30 * 864e5) {
        const en = await englishTitle(p.handle);
        if (en && en !== record.title_en) { record.title_en = en; synced++; }
      }
      if (record.images.length >= images.length) continue;     // complete
    }

    // Past the budget, only the archiving stops: the products after this one
    // still get their availability synced above.
    if (outOfTime()) { stopped = true; continue; }

    const entry = record || {
      product_id:       id,
      handle:           p.handle,
      title_jp:         tidy(p.title),
      title_en:         '',
      tags_jp:          tags,
      tags_en:          tags,
      categories:       typesFor(p.title || '', tags, items, shopInfo(id).handles),
      merch_categories: merchCategoriesFor(p.title || '', tags, shopInfo(id).handles, isCommemorative(p.handle)),
      series:           shopInfo(id).series,
      images:           [],
      min_price:        prices.length ? Math.min(...prices) : 0,
      max_price:        prices.length ? Math.max(...prices) : 0,
      published_at:     p.published_at || '',
      is_available:     (p.variants || []).some(v => v.available),
      delisted_at:      null,
    };
    if (!entry.title_en) entry.title_en = (await englishTitle(p.handle)) || entry.title_jp;
    if (entry.sale_start === undefined) {
      const sp = await salesPeriod(p.handle);
      if (sp) { entry.sale_start = sp.start; entry.sale_end = sp.end; entry.sale_v = 2; }
      if (sp && !sp.start) {
        const start = await orderStart(p.handle, sp.end, entry.published_at);
        if (start) { entry.sale_start = start; entry.sale_from = 'text'; }
        entry.sale_v = 3;
      }
    }

    // Only the images not archived yet: an entry keeps the ones it has.
    const have = new Set(entry.images.map(u => u.split('/').pop()));
    const todo = images.filter(src => !have.has(r2KeyFor(p.handle, src).split('/').pop()));
    const done = await inChunks(todo, IMAGE_CONCURRENCY, src =>
      archiveImage(p.handle, src).catch(e => { log(`  ⚠ image ${src}: ${e.message}`); return null; }));
    entry.images.push(...done.filter(Boolean));

    entry.shop_files = shopFiles(entry.images, images);
    if (record) toppedUp++; else { byId.set(id, entry); fresh.push(entry); added++; log(`New: ${entry.title_en}`); }
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
  const committed = commit(msg);
  try { await uploadJson(); }
  catch (e) { log(`⚠ upload of ${R2_JSON_KEY} failed (committed; the next run re-uploads): ${e.message}`); }

  // New products go to the same signup list as hololive's (one list for both
  // shops), and to the owner's own address. Never from an empty archive: a
  // first run finds the whole shop "new". Nor when the commit failed: the next
  // run starts without these products, finds them again and sends then, so
  // sending now as well would mail them twice.
  if (!fresh.length) return;
  if (!existing.length) { log(`Archive started from empty — no email for ${fresh.length} products`); return; }
  if (!committed) { log('Not committed — the email waits for the run that commits these products'); return; }
  const opts = { shop: 'vspo', talents: (collections && collections.talents) || new Set() };
  try { await sendNewProductEmail(fresh, log, opts); }
  catch (e) { log(`⚠ New-product email failed (data is safe): ${e.message}`); }
  try { await sendBroadcast(fresh, log, opts); }
  catch (e) { log(`⚠ Subscriber broadcast failed (data is safe): ${e.message}`); }
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
