// ─── holoVault Scraper ───────────────────────────────────────────────────────
// Triggered externally (cron-job.org → repository_dispatch webhook).
//
// Per-run logic:
//   • Fetches all products from shop.hololivepro.com (JP + EN in parallel)
//   • Compares against existing merch.json (keyed by product_id)
//   • NEW products, and any product published within IMAGE_RETRY_DAYS whose
//     images[] is shorter than the shop's image list:
//       - Resolves each image best-source-first: Langify EN render, then
//         Cover's EN twin on the CDN, then the product's own artwork
//       - Bilingual products therefore archive their EN images; single-language
//         products archive the one version they have, JP or EN
//       - Whatever wins is converted to WebP q80 and uploaded to R2
//       - Images already in R2 are reused, not re-uploaded
//       - A product still on its own artwork is re-checked while it's inside
//         IMAGE_RETRY_DAYS, so a late Langify translation still gets picked up
//   • EXISTING products: syncs is_available / delisted_at
//   • Skips commit if nothing changed

import fs   from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import sharp from 'sharp';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { sendNewProductEmail, sendBroadcast } from './email.js';

const BASE_JP    = 'https://shop.hololivepro.com';
const BASE_EN    = 'https://shop.hololivepro.com/en';
const MERCH_FILE = 'merch.json';
const LATEST_FILE = 'latest.json';   // Home's newest 40 (see latestJson)
const IMAGES_DIR = 'Products';

// Langify publishes a product's EN artwork hours — sometimes days — after the
// product itself goes live, so the first scrape to see a product can honestly
// find no EN images at all. Scheduled runs keep retrying anything still short
// of a full image set for this long after publication. Older gaps are swept on
// demand by `node scraper.js --backfill-en`, which ignores this window.
const IMAGE_RETRY_DAYS = 30;

// Langify's global dump on the EN homepage used to carry ~13,570 image
// mappings, and a short one meant the page had rendered wrong. That stopped
// being true on 2026-09-18: Cover moved EN artwork to their own …_en uploads,
// the homepage shrank from 5.4MB to 610KB, and the dump now holds ~35 entries.
// Product pages carry no ly_* references at all any more.
//
// So the dump is now a bonus source rather than a prerequisite, and this number
// only decides whether to log that it looks unusual. The authoritative per
// product signal is productImgEn on the product page (see enStemFor), and any
// product that still lands on its own artwork is rechecked for the next
// IMAGE_RETRY_DAYS — so a wrong fallback during an upstream wobble self-heals
// rather than freezing, which is what the old hard gate existed to prevent.
const TYPICAL_EN_MAPPINGS = 35;

// A guessed EN filename is only accepted if the file it finds is the same size
// as the Japanese original. Shopify serves every file in one flat namespace for
// the whole store, so "the name I guessed exists" is not the same as "it is this
// product's artwork" — generic names like EN.jpg and EN.png are real files.
// Matching width and height is cheap evidence that it is the same picture.
const VERIFY_DIMENSIONS = true;

// A product gets this long to turn up an English version before own artwork is
// accepted. Inside the window an unanswered probe leaves the image out so the
// next run retries; past it, taking the Japanese image beats leaving a hole.
const STRICT_DAYS = 7;

// Probes run per product. Scheduled runs touch a handful of products, backfills
// touch thousands — the same burst rate throttles the CDN, and a throttled probe
// is what produced Japanese artwork for two products on 18 Sept.
const PROBE_CONCURRENCY_RUN      = 5;
const PROBE_CONCURRENCY_BACKFILL = 2;

// ─── R2 client ─────────────────────────────────────────────────────────
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

// ─── Helpers ──────────────────────────────────────────────────────────

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchWithRetry(url, retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    let res;
    try {
      res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      return res;
    } catch (err) {
      // A 4xx means the server understood and refused — repeating the request
      // cannot change the answer, so fail fast. 408 (timeout) and 429 (rate
      // limited) are the two that DO change on a retry. This matters at volume:
      // Langify's dump still lists ~600 images under deleted /t/<id>/assets/
      // theme folders, and retrying each dead one three times with backoff cost
      // roughly five seconds apiece for nothing.
      const status = res ? res.status : 0;
      const definitive = status >= 400 && status < 500 && status !== 408 && status !== 429;
      if (definitive || i === retries - 1) throw err;

      // Extract Retry-After header (in seconds), if present
      const retryAfter = res ? parseInt(res.headers.get('retry-after') || '0', 10) : 0;
      const waitMs = retryAfter > 0 
        ? retryAfter * 1000 
        : delayMs * Math.pow(2, i) + Math.random() * 1000; // exponential backoff + jitter
      
      log(`Retry ${i + 1}/${retries} for ${url} (waiting ${Math.ceil(waitMs / 1000)}s)`);
      await sleep(waitMs);
    }
  }
}

// ─── Storefront session ───────────────────────────────────────────────────────
// The /en/ URLs are a trap without one. Requested cold they return 200 and a
// perfectly ordinary-looking page — which is the Japanese one, byte for byte
// identical to the /products/ version. Nothing signals the substitution, so
// every "no EN version exists" conclusion drawn from a cookieless fetch was
// really "we never asked in English".
//
// The store hands out _shopify_essential on any request; carry it and the same
// URL returns the real English page (566 KB vs 427 KB for the photo frame).
// One warm-up request per run buys it.
const storeCookies = new Map();

function absorbCookies(res) {
  const raw = typeof res.headers.getSetCookie === 'function'
    ? res.headers.getSetCookie()
    : (res.headers.get('set-cookie') || '').split(/,(?=[^;]+=)/);
  for (const line of raw) {
    const [pair] = String(line).trim().split(';');
    const i = pair.indexOf('=');
    if (i > 0) storeCookies.set(pair.slice(0, i).trim(), pair.slice(i + 1));
  }
}

async function storeFetch(url, timeoutMs = 20_000) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
  };
  if (storeCookies.size) {
    headers.Cookie = [...storeCookies].map(([k, v]) => `${k}=${v}`).join('; ');
  }
  const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) }).catch(() => null);
  if (res) absorbCookies(res);
  return res;
}

// A product page, in English. A product sold only in Japan ("Restricted
// products", e.g. hololivepanic_merch) answers 404 to a visitor the store
// places outside Japan, and it places anyone who sends the session's
// localization cookie or any Accept-Language (fetch sends "*" by default).
// The same address asked with neither is served as the English page, gallery
// and all, so a 404 is asked once more that way.
async function productPage(handle, timeoutMs = 20_000) {
  const url = `${BASE_EN}/products/${handle}`;
  const res = await storeFetch(url, timeoutMs);
  if (!res || res.status !== 404) return res;
  return fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
      'Accept-Language': '',   // empty, so fetch sends none of its own
    },
    redirect: 'manual',        // a redirect means the store still said no
    signal: AbortSignal.timeout(timeoutMs),
  }).catch(() => null);
}

async function initStoreSession() {
  const res = await storeFetch(BASE_EN, 30_000);
  const ok  = storeCookies.has('_shopify_essential');
  log(ok
    ? `Storefront session established (${[...storeCookies.keys()].join(', ')})`
    : `⚠ No storefront session cookie — EN pages will read as Japanese (HTTP ${res ? res.status : 'no response'})`);
  return ok;
}

// ─── R2 upload ─────────────────────────────────────────────────────────
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
    const res  = await storeFetch(BASE_EN, 30_000);
    if (!res || !res.ok) throw new Error(`HTTP ${res ? res.status : 'no response'} for ${BASE_EN}`);
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

// ─── EN image resolution ──────────────────────────────────────────────────────
// Cover publishes EN artwork two different ways, and both have to be checked:
//
//   1. Langify translation — a re-hosted copy named ly_<slug>_<timestamp>.png,
//      registered in the customContents_image dump on the EN homepage.
//   2. Cover's own upload — the SAME Shopify CDN folder, same filename with the
//      language token swapped (…_JP_… → …_EN_…, …_jp.png → …_en.png). The
//      storefront theme picks these up through its productImgEn field; they are
//      never registered with Langify.
//
// Path 2 used to be invisible here, which is why a product whose EN art was
// uploaded directly archived with an empty images[] while the EN files sat on
// the CDN the whole time — regloss_anniversary_3rd had all 12 of them.

// The JP→EN filename twin, or null if the name carries no language token.
function enTwinUrl(jpImgUrl) {
  const base = jpImgUrl.split('?')[0];
  const cut  = base.lastIndexOf('/');
  const file = base.slice(cut + 1);
  // A separator is required on BOTH sides so bilingual names survive intact:
  // 0314_Kuronii_Banner_SQ_JPEN_shusei1.png must not become …_ENEN_….
  const swapped = file
    .replace(/([_-])JP([_.-])/g, '$1EN$2')
    .replace(/([_-])jp([_.-])/g, '$1en$2');
  return swapped === file ? null : base.slice(0, cut + 1) + swapped;
}

// HEAD probes are cached for the run — the same twin is re-checked on every
// retry pass, and most products share a naming batch.
const twinCache = new Map();

// true = present, false = definitively absent, null = we could not find out.
//
// The null case is the important one. Only 404/410 mean "this file isn't
// there"; a 429 or 5xx means the CDN declined to answer. Recording that as
// absent would send the caller to the product's own artwork, the product would
// then look complete, and the real EN image would be lost for good. So an
// unknown result is never cached and never treated as a no.
async function cdnExists(url) {
  if (twinCache.has(url)) return twinCache.get(url);

  let result = null;
  for (let i = 0; i < 3; i++) {
    const res = await fetch(url, { method: 'HEAD', signal: AbortSignal.timeout(15_000) }).catch(() => null);
    if (res && res.ok) { result = true; break; }
    if (res && (res.status === 404 || res.status === 410)) { result = false; break; }
    await sleep(1000 * (i + 1));  // 429, 5xx or network error — worth another go
  }

  if (result !== null) twinCache.set(url, result);
  return result;
}

// Every source for one image, best first. Pass the product's own image URL;
// both EN lookups key off its filename.
//
//   1. Langify translation     — the EN render of a bilingual product
//   2. Cover's EN twin on CDN  — same thing, uploaded directly by Cover
//   3. the product's own image — for a SINGLE-language product this is the only
//      artwork that exists, and it is the correct one either way: Japanese for
//      momosuzunene_an3rd or the holoplus Sora wallpaper, already-English for
//      the card game English Editions.
//
// A list rather than one URL because a Langify entry can be stale — ~600 of
// them still point into a deleted /t/<id>/assets/ theme folder and 404 on
// download. archiveImages walks the list until something actually fetches, so
// a dead translation falls through to artwork that exists instead of nothing.
// ─── EN filenames from the storefront theme ──────────────────────────────────
// Cover names EN artwork inconsistently, and no single rule covers it:
//
//   …_2_jp.jpg          → …_2_en.jpg              language tag before the extension
//   …I_27mNot…_2_jp.jpg → …I_mNot…_2_en.jpg       and the encoded apostrophe drops
//   wachawacha_0909_…   → wachawacha_en_0909_…    tag inserted after a prefix,
//                                                 with no _jp in the JP name at all
//
// Rather than guess, read the pair the EN product page publishes for the cover
// image — productImg and productImgEn — and learn the transformation from it.
// Two derivations are applied to the rest of the set, because the learned diff
// alone can't cope when the image index sits inside the part that differs:
//
//   byIndex  — keep the EN stem, substitute this image's own index
//   byPrefix — keep the common prefix/suffix, swap the differing middle
//
// Both are HEAD-verified before use, so a wrong guess simply doesn't apply.
// The page is fetched at most once per product, and only after the cheap twin
// has already failed.
const enPairCache = new Map();

async function enNamePair(handle) {
  if (enPairCache.has(handle)) return enPairCache.get(handle);

  let out = null;
  const r = await productPage(handle);
  if (r && r.ok) {
    const html = await r.text().catch(() => '');
    const en = (html.match(/productImgEn:\s*"([^"]+)"/) || [])[1];
    const jp = (html.match(/productImg:\s*"([^"]+)"/)   || [])[1];
    const name = u => u && u.split('?')[0].split('/').pop();
    if (en && jp && name(en) !== name(jp)) out = { jp: name(jp), en: name(en) };
  }
  enPairCache.set(handle, out);
  return out;
}

const dirOf  = url => { const b = url.split('?')[0]; return b.slice(0, b.lastIndexOf('/') + 1); };
const nameOf = url => url.split('?')[0].split('/').pop();

// Keeps the EN stem and substitutes this image's index: …_3_jp.jpg → <enstem>_3_en.jpg
function enByIndex(imgUrl, pair) {
  if (!pair) return null;
  const parts = pair.en.match(/^(.*)_(\d+)_en\.([a-z0-9]+)$/i);
  const idx   = nameOf(imgUrl).match(/_(\d+)_jp\.[a-z0-9]+$/i);
  if (!parts || !idx) return null;
  return `${dirOf(imgUrl)}${parts[1]}_${idx[1]}_en.${parts[3]}`;
}

// Learns the differing middle between the two cover filenames and applies the
// same substitution to another image in the set.
function enByPrefix(imgUrl, pair) {
  if (!pair) return null;
  const { jp, en } = pair;

  let head = 0;
  while (head < jp.length && head < en.length && jp[head] === en[head]) head++;

  let tail = 0;
  while (tail < jp.length - head && tail < en.length - head &&
         jp[jp.length - 1 - tail] === en[en.length - 1 - tail]) tail++;

  const prefix = jp.slice(0, head);
  const midJp  = jp.slice(head, jp.length - tail);
  const midEn  = en.slice(head, en.length - tail);

  const file = nameOf(imgUrl);
  if (!file.startsWith(prefix)) return null;
  const rest = file.slice(prefix.length);
  if (midJp && !rest.startsWith(midJp)) return null;   // the diff sits elsewhere in this name

  return dirOf(imgUrl) + prefix + midEn + rest.slice(midJp.length);
}

// Returns null when the twin probe couldn't answer and nothing better is
// already in hand — the caller then leaves the image alone rather than guessing.
// Token-level derivation, learned from the cover pair and applied in BOTH
// directions.
//
// Cover's naming is inconsistent per image, not just per product. On the
// wacha-wacha festival the JP cover is …_enent_2.png while its English twin is
// …_event_2.png — but image 11 is the reverse, JP …_event_11.png against EN
// …_enent_11.png. A one-directional rule learned from the cover therefore fixes
// ten images and misses that one.
//
// Splitting both filenames on "_" gives the language token (present in EN,
// absent in JP) and a substitution for any token that differs. Applying that
// substitution in either direction covers both spellings. Still HEAD-verified,
// so a wrong direction simply doesn't resolve.
function enByTokens(imgUrl, pair) {
  if (!pair) return null;

  const split = f => f.split('_');
  const jpT = split(pair.jp);
  const enT = split(pair.en);
  if (enT.length !== jpT.length + 1) return null;      // expect exactly one inserted token

  // The inserted token is the first position where the two sequences diverge.
  let at = 0;
  while (at < jpT.length && jpT[at] === enT[at]) at++;
  const langToken = enT[at];
  if (!/^en$/i.test(langToken)) return null;

  const enRest = enT.slice(0, at).concat(enT.slice(at + 1));
  if (enRest.length !== jpT.length) return null;

  // Any token that still differs is a spelling variant; record it both ways.
  const subs = new Map();
  for (let i = 0; i < jpT.length; i++) {
    if (jpT[i] !== enRest[i]) { subs.set(jpT[i], enRest[i]); subs.set(enRest[i], jpT[i]); }
  }

  const base = imgUrl.split('?')[0];
  const dir  = base.slice(0, base.lastIndexOf('/') + 1);
  const tgt  = split(base.split('/').pop()).map(t => subs.get(t) ?? t);
  tgt.splice(at, 0, langToken);
  return dir + tgt.join('_');
}

// Reads an image's pixel dimensions from the first few KB of the file, so a
// candidate can be compared against the original without downloading it.
// Returns null when the format isn't recognised, and callers treat null as
// "can't tell" rather than "mismatch".
const sizeCache = new Map();

async function imageSize(url) {
  if (sizeCache.has(url)) return sizeCache.get(url);
  let out = null;
  const r = await fetch(url, { headers: { Range: 'bytes=0-8191' }, signal: AbortSignal.timeout(15_000) })
    .catch(() => null);
  if (r && (r.ok || r.status === 206)) {
    const b = Buffer.from(await r.arrayBuffer().catch(() => new ArrayBuffer(0)));
    if (b.length >= 24 && b[0] === 0x89 && b[1] === 0x50) {
      out = [b.readUInt32BE(16), b.readUInt32BE(20)];                 // PNG IHDR
    } else if (b.length > 4 && b[0] === 0xFF && b[1] === 0xD8) {      // JPEG: find SOF
      let i = 2;
      while (i + 9 < b.length) {
        if (b[i] !== 0xFF) { i++; continue; }
        const m = b[i + 1];
        if (m >= 0xC0 && m <= 0xCF && m !== 0xC4 && m !== 0xC8 && m !== 0xCC) {
          out = [b.readUInt16BE(i + 7), b.readUInt16BE(i + 5)];
          break;
        }
        i += 2 + b.readUInt16BE(i + 2);
      }
    }
  }
  sizeCache.set(url, out);
  return out;
}

// Generic EN filename guesses that depend on nothing but the JP name itself.
//
// These matter because the learned-from-the-page rule can be poisoned by a
// single bad sample: the wacha-wacha festival cover is named
// wachawacha_0909_enent_2.png in Japanese but wachawacha_en_0909_event_2.png in
// English — Cover typo'd "event" in the JP one. The transform learned from that
// pair then matched none of the other ten images, all of which do have English
// versions. Position-independent guesses are immune to that.
//
// Every one is HEAD-verified before use, so extra guesses cost a cached probe
// and never a wrong image.
function enGenericGuesses(imgUrl) {
  const base = imgUrl.split('?')[0];
  const dir  = base.slice(0, base.lastIndexOf('/') + 1);
  const file = base.split('/').pop();
  const dot  = file.lastIndexOf('.');
  const stem = dot > 0 ? file.slice(0, dot) : file;
  const ext  = dot > 0 ? file.slice(dot) : '';

  const out = new Set();

  // wachawacha_0909_… → wachawacha_en_0909_…   (tag after the first token)
  const firstUnderscore = stem.indexOf('_');
  if (firstUnderscore > 0) {
    out.add(dir + stem.slice(0, firstUnderscore) + '_en' + stem.slice(firstUnderscore) + ext);
  }

  // …_thing → …_thing_en                        (tag appended)
  out.add(dir + stem + '_en' + ext);

  return [...out];
}

// ─── EN filenames straight from the storefront ───────────────────────────────
// Everything below this point in the chain guesses a filename and then checks
// whether it exists. That works until Cover uploads English artwork under a
// name carrying no trace of the original — EN_f843f9df-a285-4e9b-9a0d-…png is
// not derivable from フォトフレーム_ヨコ_JP.png by any rule, and no amount of
// probing finds it.
//
// The English product page already lists them. Each gallery slide carries
// data-media-id, and that id is the same `id` products.json gives for the
// Japanese image — so the page yields an exact slot-for-slot mapping with no
// inference at all, and no dimension check needed: the store is naming the file
// itself rather than us guessing a name that happens to exist.
//
// One fetch per product, cached, and the promise is cached rather than the
// result so concurrent images of the same product share a single request.
const mediaMapCache = new Map();

function enMediaMap(handle) {
  if (mediaMapCache.has(handle)) return mediaMapCache.get(handle);

  const p = (async () => {
    // null means "couldn't ask" and feeds the unknown-probe logic below; an
    // empty map means the page answered and this product simply has no
    // translated gallery. Conflating the two would let a network blip archive
    // Japanese artwork and mark the product finished.
    const res = await productPage(handle);
    if (!res || !res.ok) return null;
    const html = await res.text().catch(() => '');
    if (!html) return null;

    // Each slide repeats at several widths; the first hit per id is enough.
    // A slide's picture is looked for only up to the next slide, and in both
    // folders: older products keep some pictures under products/, and a
    // search for files/ alone ran on into the next slide and gave this slot
    // that one's English file (the Kikimanzai members picture became the
    // first combo's, which then found nothing of its own).
    const out = new Map();
    const re = /data-media-id="(\d+)"(?:(?!data-media-id=)[\s\S]){0,400}?cdn\/shop\/(files|products)\/([A-Za-z0-9_%.\-]+\.(?:png|jpe?g|webp))/gi;
    let m;
    while ((m = re.exec(html)) !== null) {
      if (!out.has(m[1])) out.set(m[1], `${BASE_JP}/cdn/shop/${m[2].toLowerCase()}/${m[3]}`);
    }
    return out;
  })();

  mediaMapCache.set(handle, p);
  return p;
}

async function imageCandidates(imgUrl, enMap, handle, expect, strict = true, mediaId = null) {
  const out = [];

  // Tracks "the CDN wouldn't tell us" across every probe below. A throttled
  // answer must never be read as "no EN version exists" — that would archive
  // the Japanese artwork and call the product done.
  let probeUnknown = false;

  const tryGuess = async url => {
    if (!url || out.length) return;
    const exists = await cdnExists(url);
    if (exists === null) { probeUnknown = true; return; }
    if (!exists) return;

    // The name exists — but is it this product's artwork? Compare dimensions
    // against the original before trusting it.
    if (VERIFY_DIMENSIONS && expect && expect[0] && expect[1]) {
      const got = await imageSize(url);
      if (got && (got[0] !== expect[0] || got[1] !== expect[1])) {
        log(`  guess ${url.split('/').pop().split('?')[0]} is ${got[0]}x${got[1]}, expected ${expect[0]}x${expect[1]} — rejected`);
        return;
      }
    }
    out.push(url);
  };

  // What the English page itself shows in this slot. Not a guess, so it skips
  // the existence probe and the dimension check — both exist to weed out names
  // we invented, and this name came from the store.
  if (handle && mediaId) {
    const byMedia = await enMediaMap(handle);
    if (byMedia === null) probeUnknown = true;
    else {
      const en = byMedia.get(String(mediaId));
      // Equal means this slot simply isn't translated — fall through to the
      // guesses rather than treating the JP file as an English find.
      if (en && nameOf(en).toLowerCase() !== nameOf(imgUrl).toLowerCase()) out.push(en);
    }
  }

  // Langify's own mapping first where one still exists — but through the same
  // verification as everything else. It used to be pushed straight in, which
  // meant a stale mapping skipped the dimension check that guards every other
  // candidate.
  await tryGuess(enMap.get(imgUrl.split('/').pop().split('?')[0].toLowerCase()));

  // Then everything derivable from the filename alone, no page fetch.
  await tryGuess(enTwinUrl(imgUrl));
  for (const g of enGenericGuesses(imgUrl)) await tryGuess(g);

  // Only if those all missed, ask the storefront what it calls the EN file.
  // One page fetch per product, cached.
  if (!out.length && handle) {
    const pair = await enNamePair(handle);
    await tryGuess(enByIndex(imgUrl, pair));
    await tryGuess(enByPrefix(imgUrl, pair));
    await tryGuess(enByTokens(imgUrl, pair));
  }

  // An unanswered probe only matters when nothing better was found: then we
  // genuinely don't know whether an EN version exists, and archiving the JP
  // artwork would freeze that guess in place. Leaving the image out keeps the
  // product incomplete, so the next run tries again — but only while the
  // product is still new. Past STRICT_DAYS a hole is worse than Japanese
  // artwork, so the fallback below is taken instead.
  if (!out.length && probeUnknown && strict) return null;

  out.push(imgUrl);
  return out;
}

// Resolves and uploads a product's EN images, reusing anything already in R2.
// Returns images in source order; it never returns fewer URLs than were already
// archived, so a transient failure mid-retry cannot shrink a product's gallery.
async function archiveImages(handle, jpImages, enMap, alreadyArchived, opts = {}) {
  const have = alreadyArchived || [];
  const hset = new Set(have);

  const { sizes = null, mediaIds = null, strict = true, concurrency = PROBE_CONCURRENCY_RUN } = opts;

  // Puts one source picture in R2; its public URL, or null if it won't fetch.
  const archiveOne = async (src, i) => {
    const key       = r2KeyFor(handle, src);
    const publicUrl = `${R2_PUBLIC_URL}/${key}`;
    if (hset.has(publicUrl)) return publicUrl;  // already in R2 — no re-upload
    try {
      await uploadToR2(src, key);
      log(`  [${handle}] ↑ ${key}`);
      return publicUrl;
    } catch (err) {
      log(`  [${handle}] image ${i}: ${nameOf(src)} unavailable, trying next source`);
      return null;
    }
  };

  const results = await inChunks(
    jpImages.map((jpUrl, i) => ({ jpUrl, i })),
    concurrency,
    async ({ jpUrl, i }) => {
      const expect  = sizes    ? sizes[jpUrl]    : null;
      const mediaId = mediaIds ? mediaIds[jpUrl] : null;
      const cands   = await imageCandidates(jpUrl, enMap, handle, expect, strict, mediaId);
      if (!cands) {
        // Couldn't establish whether an EN twin exists. Leaving the image out
        // keeps the product incomplete, so a later run retries it — far better
        // than archiving the JP original and calling the product done.
        log(`  [${handle}] image ${i}: EN twin unverifiable — leaving for a later run`);
        return null;
      }

      // Best source first, falling through on anything that won't fetch.
      for (const cand of cands) {
        const publicUrl = await archiveOne(cand, i);
        if (publicUrl) return publicUrl;
      }

      log(`  [${handle}] ⚠ image ${i}: no usable source — skipping`);
      return null;
    }
  );

  // One English file for two pictures means a guess landed on the wrong one.
  // A JP name with no language token gets "_en" appended as a guess, and
  // holodori_diorama_Takanashi_Kiara.png (a photo of the stand) found
  // …Kiara_en.png, the English twin of the same product's …Kiara_jp.png card:
  // both 1500×1500, so the size check passed, and the gallery showed the card
  // twice and never the photo. The slot whose JP name shares the most words
  // with the English file keeps it (ManzaiVoices_member_1500_jp and …combi01…
  // both found ly_manzaivoices_combi01_1500_en_…); on a tie, the one whose
  // name carries the language token. The others keep their own picture.
  const LANG_TOKEN = /(^|[_-])(jp|ja)([_.-]|$)/i;
  // Words of a file name, a language tag dropped wherever it sits: b2en → b2.
  const words = u => new Set(nameOf(u).replace(/\.[^.]+$/, '').toLowerCase().split(/[_\-\s]+/)
    .map(w => w.replace(/(en|jp|ja)$/, '')).filter(Boolean));
  const shared = (a, b) => { const B = words(b); return [...words(a)].filter(w => B.has(w)).length; };
  const slotsByUrl = new Map();
  results.forEach((u, i) => { if (u) slotsByUrl.set(u, [...(slotsByUrl.get(u) || []), i]); });
  for (const [url, slots] of slotsByUrl) {
    if (slots.length < 2) continue;
    const score  = i => shared(jpImages[i], url) * 2 + (LANG_TOKEN.test(nameOf(jpImages[i])) ? 1 : 0);
    const keeper = slots.reduce((best, i) => score(i) > score(best) ? i : best);
    for (const i of slots) {
      if (i === keeper) continue;
      // The shop listing the very same file twice is left as the shop has it.
      if (`${R2_PUBLIC_URL}/${r2KeyFor(handle, jpImages[i])}` === url) continue;
      log(`  [${handle}] image ${i}: ${nameOf(url)} belongs to image ${keeper} — keeping its own picture`);
      results[i] = await archiveOne(jpImages[i], i);
    }
  }

  let images = results.filter(Boolean);

  // Never regress: if this pass resolved less than we already had, keep the old
  // URLs too rather than writing a shorter gallery than the one we started with.
  if (images.length < have.length) {
    log(`  [${handle}] ⚠ resolved ${images.length} but ${have.length} archived — keeping existing`);
    images = images.concat(have.filter(u => !images.includes(u)));
  }

  return images;
}

// True when any archived image is the product's own artwork rather than an
// EN-specific render — i.e. it fell through to candidate 3. Worth re-resolving
// while the product is still in the retry window, because Langify may publish a
// translation days after the product goes live. Without this the fallback taken
// during that gap would be permanent, silently costing the EN image forever.
function usesOwnArtwork(archived, product) {
  const set = new Set(archived.images || []);
  return product.jp_images.some(u => set.has(`${R2_PUBLIC_URL}/${r2KeyFor(product.handle, u)}`));
}

// Scheduled runs only chase recent gaps; --backfill-en passes ignoreAge.
function shouldRetryImages(publishedAt) {
  const t = Date.parse(publishedAt || '');
  return Number.isFinite(t) && (Date.now() - t) < IMAGE_RETRY_DAYS * 86_400_000;
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
      'ねんどろいど',   // the shop's own spelling; 'Nendoroid' matched nothing
      'OTHERS',
    ],
  },
];

// Storefront pages 429 non-browser user agents almost immediately — send
// browser-like headers, pace generously, and honor Retry-After on the SAME
// page so nothing is silently skipped.
//
// No Accept-Language (empty, so fetch sends none of its own): any value places
// the visitor outside Japan, and the filtered pages then leave out products
// sold only in Japan, as productPage explains — hololivepanic_merch was
// missing from both of its types for that reason alone.
const PAGE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml',
  'Accept-Language': '',
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

// ─── merch.json I/O ───────────────────────────────────────────────────────

const BY_NEWEST = (a, b) => (b.published_at || '').localeCompare(a.published_at || '');

// Writes merch.json newest-first WITHOUT reordering the caller's array. The
// backfill writes mid-iteration, and sorting the array a for…of is walking
// reorders it under the cursor — products get skipped and others visited twice.
function writeMerch(products) {
  fs.writeFileSync(
    path.join(process.cwd(), MERCH_FILE),
    JSON.stringify([...products].sort(BY_NEWEST), null, 2)
  );
}

function loadExistingMerch() {
  const filePath = path.join(process.cwd(), MERCH_FILE);
  if (!fs.existsSync(filePath)) {
    log('No existing merch.json — starting fresh');
    return [];
  }
  // A file that is there but unreadable stops the run. Starting fresh would
  // save over it, and every delisted product (no longer in the shop to be
  // found again) would be gone from the archive for good.
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (err) {
    throw new Error(`merch.json could not be read (${err.message}); fix or restore it before the next run`);
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

    // Each read as it arrives. Reading both after both had come back failed
    // the run: the English page was rate-limited (Retry-After 80s), and by
    // then the Japanese one's 30s timeout, which covers reading its body too,
    // had aborted it (run #5872, "DOMException [AbortError]").
    const json = url => fetchWithRetry(url).then(r => r.json());
    const [dataJp, dataEn] = await Promise.all([
      json(`${BASE_JP}/products.json?limit=250&page=${page}&currency=JPY`),
      json(`${BASE_EN}/products.json?limit=250&page=${page}`),
    ]);

    if (!dataJp.products?.length) break;

    const enByHandle = {};
    for (const p of (dataEn.products || [])) {
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
        // src → [width, height], used to confirm a guessed EN file is the same
        // artwork and not just a name that happens to exist.
        jp_sizes:     Object.fromEntries((p.images || []).map(i => [i.src, [i.width, i.height]])),
        // src → Shopify media id. The EN product page tags each gallery slide
        // with the same id, which is what makes the mapping exact.
        jp_media_ids: Object.fromEntries((p.images || []).map(i => [i.src, String(i.id)])),
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
      Key:         LATEST_FILE,
      Body:        latestJson(JSON.parse(content)),
      ContentType: 'application/json',
    }));
    log(`Uploaded latest.json → ${R2_PUBLIC_URL}/${LATEST_FILE}`);
  } catch (e) {
    log(`⚠ upload of latest.json failed (Home falls back to the full archive): ${e.message}`);
  }
}

async function uploadMerchToR2() {
  const content = fs.readFileSync(path.join(process.cwd(), MERCH_FILE));
  await r2.send(new PutObjectCommand({
    Bucket:      R2_BUCKET,
    Key:         MERCH_FILE,
    Body:        content,
    ContentType: 'application/json',
  }));
  log(`Uploaded merch.json to R2 → ${R2_PUBLIC_URL}/${MERCH_FILE}`);
  await uploadLatest(content);
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

  // Upload merch.json to R2 so the site can fetch it publicly.
  //
  // Deliberately non-fatal. By this point the commit has already landed, so the
  // products are permanently in merch.json and no later run will see them as
  // new. Letting a transient R2 error throw here would abort main() before the
  // notification email, and that email would then never be sent for those
  // products — the data is saved, so nothing would ever retry it.
  //
  // Skipping the upload costs nothing lasting: every run uploads merch.json,
  // including the "nothing changed" path, so the next one puts it right.
  try {
    await uploadMerchToR2();
  } catch (err) {
    log(`⚠ R2 upload of merch.json failed (committed to git; next run re-uploads): ${err.message}`);
  }
}

// ─── Main ───────────────────────────────────────────────────────────

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
  // A shop in maintenance, or a cut-short answer, would otherwise mark most of
  // the archive delisted. Stop and fail the run instead; the next one retries.
  const listedBefore = existing.filter(p => !p.delisted_at).length;
  if (listedBefore > 20 && shopifyProducts.length < listedBefore / 2) {
    throw new Error(`Shop listed only ${shopifyProducts.length} of ${listedBefore} products; not syncing this run`);
  }

  // 3. Fetch Langify EN image map once — covers all products
  await initStoreSession();
  const enImageMap = await fetchEnImageMap();

  // Advisory only. A big jump either way is worth seeing in the log — if the
  // dump ever comes back, that is useful to know — but it no longer gates
  // anything: EN artwork is discovered per product now, not from this map.
  if (enImageMap.size > TYPICAL_EN_MAPPINGS * 10) {
    log(`  note: EN image map is unusually large (${enImageMap.size}) — Langify may be back`);
  }

  const currentIds = new Set(shopifyProducts.map(p => p.product_id));
  const shopifyMap = new Map(shopifyProducts.map(p => [p.product_id, p]));

  let newCount     = 0;
  let updatedCount = 0;

  // Entries created this run, for the notification email.
  const freshProducts = [];

  // 4. Archive images for new products, AND top up any recent product whose
  //    images[] is still short of what the shop lists.
  //
  //    Presence of the product_id used to be the only gate, on the assumption
  //    that "if it's here, its images are in R2". That assumption is false:
  //    Langify often hasn't produced a new product's EN artwork yet when the
  //    first scrape runs, so the product was archived with a partial or empty
  //    images[] and the gate then guaranteed it was never looked at again.
  //    Comparing against the live image count is what makes a momentary miss
  //    recoverable instead of permanent.
  {
    for (const p of shopifyProducts) {
      const archived = existingMap.get(p.product_id);
      const have     = archived ? (archived.images || []).length : 0;

      if (archived) {
        const short    = have < p.jp_images.length;
        const fallback = usesOwnArtwork(archived, p);
        if (!short && !fallback) continue;                  // complete, all EN renders
        if (!shouldRetryImages(p.published_at)) continue;   // too old for scheduled runs
        log(short
          ? `Incomplete: ${p.title_en} — ${have}/${p.jp_images.length} images, retrying`
          : `Recheck: ${p.title_en} — on own artwork, looking for an EN render`);
      } else {
        log(`New product: ${p.title_en} (${p.product_id})`);
      }

      const ageDays = (Date.now() - Date.parse(p.published_at || '')) / 86_400_000;
      const images = await archiveImages(p.handle, p.jp_images, enImageMap, archived?.images, {
        sizes: p.jp_sizes,
        mediaIds: p.jp_media_ids,
        // Inside STRICT_DAYS an unanswered probe leaves the image out so the
        // next run retries; after that, own artwork beats a missing image.
        strict: Number.isFinite(ageDays) && ageDays <= STRICT_DAYS,
        concurrency: PROBE_CONCURRENCY_RUN,
      });

      if (archived) {
        const old = archived.images || [];

        // Only write when the result is genuinely better: more images, or fewer
        // of them still on the product's own artwork. Comparing the arrays for
        // any difference instead made flaky probes churn — a product would swap
        // one image for another every run, commit, and log an "upgrade" that
        // wasn't one. Counting own-artwork images is what actually distinguishes
        // a real EN upgrade from a reshuffle.
        const ownCount = list => {
          const set = new Set(list);
          return p.jp_images.filter(j => set.has(`${R2_PUBLIC_URL}/${r2KeyFor(p.handle, j)}`)).length;
        };
        const gainedImages = images.length > old.length;
        const gainedEnglish = ownCount(images) < ownCount(old);

        if (gainedImages || gainedEnglish) {
          const before = ownCount(old), after = ownCount(images);
          archived.images = images;
          log(gainedImages
            ? `  → ${old.length} → ${images.length} images` +
              (after < before ? ` (${before - after} swapped to EN)` : '')
            : `  → ${before - after} image(s) upgraded to EN`);
          updatedCount++;
        }
        continue;
      }

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
        images,
        min_price:    p.min_price,
        max_price:    p.max_price,
        published_at: p.published_at,
        is_available: p.available,
        delisted_at:  null,
      });

      existingMap.set(p.product_id, existing[existing.length - 1]);
      freshProducts.push(existing[existing.length - 1]);
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
  writeMerch(existing);
  log(`Wrote merch.json (${existing.length} total products)`);

  // 8. Commit merch.json only — images live in R2.
  // Awaited: commitChanges also uploads merch.json to R2, and without the await
  // a failure there surfaced as an unhandled rejection instead of a caught error.
  await commitChanges(newCount, updatedCount);

  // Never from an empty archive, as in vspo-scraper.js: a first run finds the
  // whole shop "new", and would mail every product to the subscriber list.
  if (existing.length === newCount) {
    log(`Archive started from empty — no email for ${newCount} products`);
    return;
  }

  // After the commit, so an email never announces something that failed to
  // persist. Wrapped because a mail outage must not fail an otherwise good run —
  // the data is already saved by this point.
  try {
    await sendNewProductEmail(freshProducts, log);
  } catch (err) {
    log(`⚠ New-product email failed (data is safe): ${err.message}`);
  }

  // The public signup list, if one is configured. Caught separately so a
  // broadcast failure never costs you the direct email above.
  try {
    await sendBroadcast(freshProducts, log);
  } catch (err) {
    log(`⚠ Subscriber broadcast failed (data is safe): ${err.message}`);
  }
  log(`── Done. ${newCount} new, ${updatedCount} updated ──`);
}

// ─── EN Image Backfill (re-runnable repair pass) ─────────────────────────────
// node scraper.js --backfill-en
//
// Started life as a one-time migration to R2, and is now also the repair pass
// for image gaps: any product whose images[] is shorter than the shop's list,
// or that still doesn't point at R2. Re-fetches the live JP catalog for the
// original JP image URLs (needed as lookup keys — old GitHub raw / Shopify CDN
// URLs won't match), resolves each to an EN one via Langify then CDN twin,
// converts to WebP q80, uploads to R2, commits in batches.
//
// Unlike scheduled runs this ignores IMAGE_RETRY_DAYS, so it reaches the whole
// back catalogue rather than only recently published products.
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
  const jpSizesByHandle  = new Map(shopifyProducts.map(p => [p.handle, p.jp_sizes]));
  const jpMediaIdsByHandle = new Map(shopifyProducts.map(p => [p.handle, p.jp_media_ids]));

  await initStoreSession();
  const enImageMap = await fetchEnImageMap();
  // No longer gated on the Langify dump — see TYPICAL_EN_MAPPINGS. EN artwork
  // is resolved per product from the storefront, so an empty map is normal.

  execSync('git config user.name "holoVault Scraper"');
  execSync('git config user.email "scraper@holovault"');

  const BATCH_SIZE = 50;
  let backfilledCount = 0;
  let batchCount      = 0;

  for (const product of existing) {
    // Get JP image URLs from the live catalog for EN lookup.
    // Products fully delisted from Shopify won't be in jpImagesByHandle —
    // they're skipped here and will retain stale image URLs permanently.
    const jpImages = jpImagesByHandle.get(product.handle);
    if (!jpImages?.length) {
      log(`[${product.handle}] Not in live catalog (delisted?) — skipping`);
      continue;
    }

    // The old check was `images.every(u => u.startsWith(R2_PUBLIC_URL))`, which
    // is vacuously TRUE for an empty array — so every product that archived
    // with zero images was skipped by the one pass meant to repair it, and the
    // count check below is what actually makes partial galleries reachable.
    // A full count is not the same as a correct one. The holoplus photo frame
    // archived 3 of 3 images with two of them Japanese, so a count check alone
    // skipped the one pass meant to repair it — and the scheduled run only
    // rechecks inside IMAGE_RETRY_DAYS, leaving nothing able to reach it.
    // Anything still sitting on its own artwork gets another look.
    const imgs = product.images || [];
    // A picture listed twice is a wrong guess (see archiveImages) unless the
    // shop itself lists one file twice, which this then rechecks harmlessly.
    const complete = imgs.length >= jpImages.length && imgs.every(u => u.startsWith(R2_PUBLIC_URL))
                  && new Set(imgs).size === imgs.length;
    if (complete && !usesOwnArtwork(product, { handle: product.handle, jp_images: jpImages })) continue;

    const before        = imgs.length;
    const updatedImages = await archiveImages(product.handle, jpImages, enImageMap, imgs, {
      sizes: jpSizesByHandle.get(product.handle),
      mediaIds: jpMediaIdsByHandle.get(product.handle),
      strict: false,                              // old products: never leave a hole
      concurrency: PROBE_CONCURRENCY_BACKFILL,    // a sweep must not throttle itself
    });

    if (updatedImages.length === before && updatedImages.every((u, i) => u === imgs[i])) {
      log(`[${product.handle}] still ${before}/${jpImages.length} — no EN version available yet`);
      continue;
    }

    product.images = updatedImages;
    delete product.images_en; // remove legacy field if present

    backfilledCount++;
    batchCount++;
    log(`Backfilled ${product.title_en}: ${before} → ${updatedImages.length}/${jpImages.length} images`);

    if (batchCount >= BATCH_SIZE) {
      writeMerch(existing);
      execSync('git add merch.json');
      execSync(`git commit -m ${JSON.stringify(`Backfill R2 images (batch, ${backfilledCount} so far)`)}`);
      execSync('git push');
      log(`Pushed batch — ${backfilledCount} products done so far`);
      batchCount = 0;
    }

    await sleep(100);
  }

  if (batchCount > 0) {
    writeMerch(existing);
    execSync('git add merch.json');
    execSync(`git commit -m ${JSON.stringify(`Backfill R2 images (final, ${backfilledCount} total)`)}`);
    execSync('git push');
  }

  // Publish to R2. The git commit above only updates the private repo, but the
  // site fetches merch.json from R2 — so without this a backfill uploads every
  // image and the site still shows the old, image-less entries until some other
  // run happens to push the manifest.
  if (backfilledCount > 0) {
    try { await uploadMerchToR2(); }
    catch (err) { log(`⚠ R2 upload of merch.json failed: ${err.message}`); }
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

// ─── Email test (on demand) ──────────────────────────────────────────────────
// node scraper.js --test-email      (needs TEST_EMAIL_TO, or falls back to MAIL_TO)
//
// A real new-product email only fires when Cover actually drops something, which
// can be days away — long enough that a broken template or a wrong secret would
// go unnoticed. This sends the same email, built from the three newest products
// already in merch.json, to one address on demand.
//
// It scrapes nothing and writes nothing: merch.json, R2 and git are untouched.

async function testEmail() {
  log('── Email test ──');

  const existing = loadExistingMerch();
  if (!existing.length) { log('merch.json is empty — nothing to build a preview from'); return; }

  const sample = [...existing].sort(BY_NEWEST).slice(0, 3);
  log('Using the 3 newest products:');
  sample.forEach(p => log(`   • ${p.title_en} (${(p.images || []).length} images)`));

  const to = process.env.TEST_EMAIL_TO || process.env.MAIL_TO;
  if (!to) { log('⚠ Neither TEST_EMAIL_TO nor MAIL_TO is set — nowhere to send'); return; }

  // Point the direct email at the test address only, then put MAIL_TO back so
  // nothing later in the process sees a mutated environment.
  const prev = process.env.MAIL_TO;
  process.env.MAIL_TO = to;
  try {
    await sendNewProductEmail(sample, log);
  } catch (err) {
    log(`⚠ Direct email failed: ${err.message}`);
  } finally {
    if (prev === undefined) delete process.env.MAIL_TO; else process.env.MAIL_TO = prev;
  }

  // Draft only — see sendBroadcast. Subscribers get nothing.
  try {
    await sendBroadcast(sample, log, { draftOnly: true });
  } catch (err) {
    log(`⚠ Draft broadcast failed: ${err.message}`);
  }

  log('── Email test done (no data was changed) ──');
}

// ─── Entry point ────────────────────────────────────────────────────────

// --test-email is checked first: it's read-only and quick, so if it's somehow
// combined with a backfill the harmless one wins.
const entry = process.argv.includes('--test-email')          ? testEmail
            : process.argv.includes('--backfill-en')         ? backfillEnImages
            : process.argv.includes('--backfill-categories') ? backfillCategories
            : main;
entry().catch(err => {
  console.error('Scraper failed:', err);
  process.exit(1);
});
