// ─── New-product email notification ──────────────────────────────────────────
// Sends one digest per run listing every product that entered the archive for
// the first time. Called from scraper.js after the commit succeeds.
//
// Uses Resend's REST API directly rather than the npm SDK — it's a single POST
// and Node 20 has fetch, so there's nothing to install.
//
// GitHub Actions secrets required:
//   RESEND_API_KEY – from resend.com/api-keys
//   MAIL_TO        – where to send (comma-separated for several recipients)
//   MAIL_FROM      – e.g. "holoCompendium <merch@holocompendium.cc>"
//                    The domain must be verified in Resend. Before you've done
//                    that, "onboarding@resend.dev" works but only delivers to
//                    the address that owns the Resend account.

const RESEND_ENDPOINT = 'https://api.resend.com/emails';

// A fresh archive would otherwise produce a 2,000-product email. Anything past
// this is summarised as a count.
const MAX_CARDS = 12;

// One signup list covers both shops (the form is on both Merch pages), so each
// scraper sends its own shop's new products to it. `shop` in the options below
// picks these; hololive is the default. The archive link names the agency, as
// the site otherwise opens whichever one the reader last looked at.
const SHOPS = {
  hololive: { name: 'hololive', product: 'https://shop.hololivepro.com/en/products',
              site: 'https://holocompendium.cc/merch?org=hololive' },
  vspo:     { name: 'VSPO!',    product: 'https://store.vspo.jp/en-us/products',
              site: 'https://holocompendium.cc/merch?org=vspo' },
};
const shopOf = opts => SHOPS[opts && opts.shop] || SHOPS.hololive;
// VSPO!'s archive can hold a product whose English title is not out yet.
const titleOf = p => p.title_en || p.title_jp || '';

// Mail clients default to Times when no family is set, so this has to be
// declared on every text-bearing element — several of them (Outlook especially)
// don't inherit it reliably from a parent.
const FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Helvetica,Arial,sans-serif";

// Palette lifted from the site so the mail reads as the same product.
const C = {
  bg:      '#0f0d17',
  card:    '#1a1626',
  border:  '#2d2640',
  text:    '#e9e5f5',
  muted:   '#9d94b8',
  accent:  '#a78bfa',
  pill:    '#241d35',
  okBg:    '#10281f',
  okText:  '#34d399',
  outBg:   '#2a1618',
  outText: '#f87171',
  soonBg:  '#2a2410',
  soonText:'#ffe082',
};

const esc = s => String(s ?? '')
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

const yen = n => '¥' + Number(n || 0).toLocaleString('en-US');

function priceLabel(p) {
  if (!p.min_price) return 'Price TBA';
  return p.min_price === p.max_price ? yen(p.min_price) : `${yen(p.min_price)} – ${yen(p.max_price)}`;
}

// "Talent_一条莉々華" → "一条莉々華", "Generation_ReGLOSS" → "ReGLOSS".
// Both tag arrays carry Japanese talent names; the EN display names live in the
// site repo, not here, so the Japanese ones are what we can show accurately.
// VSPO!'s shop tags a talent by their plain name, among other plain tags
// (抽選販売, フレグランス), so its chips come from the talent names the scraper
// read off the shop's talent pages (opts.talents), and its series.
function chips(p, opts) {
  const tags    = p.tags_en?.length ? p.tags_en : (p.tags_jp || []);
  const vspo    = opts && opts.shop === 'vspo';
  const talents = vspo ? tags.filter(t => opts.talents && opts.talents.has(t))
                       : tags.filter(t => t.startsWith('Talent_')).map(t => t.slice(7));
  const gen     = vspo ? (p.series || [])[0]
               : tags.find(t => t.startsWith('Generation_'))?.slice(11)
               || tags.find(t => t.startsWith('Group_'))?.slice(6);

  const out = [];
  if (gen) out.push({ text: gen, accent: true });
  talents.slice(0, 4).forEach(t => out.push({ text: t }));
  if (talents.length > 4) out.push({ text: `+${talents.length - 4}` });
  return out;
}

function chipHtml(c) {
  const color = c.accent ? C.accent : C.muted;
  return `<span style="display:inline-block;background:${C.pill};color:${color};`
       + `font-family:${FONT};font-size:12px;line-height:1;padding:6px 10px;border-radius:999px;`
       + `margin:0 6px 6px 0;white-space:nowrap;">${esc(c.text)}</span>`;
}

// Layout switch. 'gallery' (default) shows the cover plus a strip of up to four
// more of the product's images, which for a merch set is the difference between
// seeing a banner and seeing what's actually in the box. 'hero' is the original
// cover-only card, kept as a fallback: set EMAIL_LAYOUT=hero to go back.
const LAYOUT = (process.env.EMAIL_LAYOUT || 'gallery').toLowerCase();

// Up to four of the images after the cover, as one evenly-divided row.
// Percentage widths rather than fixed pixels so it survives a phone's narrower
// viewport; the html width attribute is the fallback for Outlook, which ignores
// percentage widths on images.
const THUMB_COLS = 4;

function thumbStrip(p, shop) {
  const rest = (p.images || []).slice(1, 1 + THUMB_COLS);
  if (!rest.length) return '';

  // Always THUMB_COLS columns, padding the row with empty cells. Sizing the
  // cells by rest.length instead would stretch a lone second image to the full
  // card width — two full-bleed images stacked, rather than a cover and a
  // thumbnail. Empty cells keep every thumbnail the same size whatever the
  // product has.
  const cells = [];
  for (let i = 0; i < THUMB_COLS; i++) {
    const last = i === THUMB_COLS - 1;
    const pad  = `padding:0 ${last ? 0 : 6}px 0 0;`;
    cells.push(rest[i]
      ? `<td width="25%" style="${pad}">
            <a href="${shop.product}/${esc(p.handle)}" style="display:block;">
              <img src="${esc(rest[i])}" alt="" width="142"
                   style="display:block;width:100%;height:auto;border:0;border-radius:8px;">
            </a>
          </td>`
      : `<td width="25%" style="${pad}">&nbsp;</td>`);
  }

  return `
    <tr><td style="padding:6px 6px 0 6px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
        <tr>${cells.join('')}</tr>
      </table>
    </td></tr>`;
}

// VSPO!'s shop lists a product before its sales period opens, unavailable
// until then; vspo-scraper.js records the period, and such a product is
// Coming Soon rather than Sold Out (the site says the same).
function statusOf(p) {
  if (p.is_available) return 'Available';
  return Date.parse(p.sale_start) > Date.now() ? 'Coming Soon' : 'Sold Out';
}

function cardHtml(p, opts) {
  const shop  = shopOf(opts);
  // R2 WebP. Gmail, Apple Mail and Outlook.com all render it; Outlook desktop
  // for Windows does not, and falls back to the alt text.
  const img   = (p.images || [])[0];
  const status = statusOf(p);
  const [bg, fg] = status === 'Available' ? [C.okBg, C.okText] : status === 'Coming Soon' ? [C.soonBg, C.soonText] : [C.outBg, C.outText];
  const badge = `<span style="background:${bg};color:${fg};font-family:${FONT};font-size:12px;font-weight:600;padding:5px 10px;border-radius:6px;">${status}</span>`;

  return `
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background:${C.card};border:1px solid ${C.border};border-radius:14px;margin:0 0 16px 0;">
    ${img ? `<tr><td style="padding:0;">
      <a href="${shop.product}/${esc(p.handle)}" style="display:block;">
        <img src="${esc(img)}" width="598" alt="${esc(titleOf(p))}"
             style="display:block;width:100%;height:auto;border:0;border-radius:13px 13px 0 0;">
      </a></td></tr>` : ''}
    ${LAYOUT === 'gallery' ? thumbStrip(p, shop) : ''}
    <tr><td style="padding:18px 20px 20px 20px;">
      <a href="${shop.product}/${esc(p.handle)}"
         style="color:${C.text};font-family:${FONT};font-size:17px;font-weight:700;line-height:1.35;text-decoration:none;">
        ${esc(titleOf(p))}</a>
      <div style="margin:12px 0 0 0;">${chips(p, opts).map(chipHtml).join('')}</div>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:14px;">
        <tr>
          <td align="left" style="color:${C.accent};font-family:${FONT};font-size:19px;font-weight:700;">${esc(priceLabel(p))}</td>
          <td align="right">${badge}</td>
        </tr>
      </table>
      <a href="${shop.product}/${esc(p.handle)}"
         style="display:inline-block;margin-top:16px;background:${C.accent};color:#1a1626;
                font-family:${FONT};font-size:14px;font-weight:700;text-decoration:none;padding:11px 20px;border-radius:8px;">
        View on Shop</a>
    </td></tr>
  </table>`;
}

// Resend swaps this token for a per-recipient unsubscribe URL when the mail is
// sent as a broadcast. A broadcast without one is rejected, and a public list
// legally needs it anyway.
const UNSUB = '{{{RESEND_UNSUBSCRIBE_URL}}}';

// Resend rejects a broadcast whose `name` exceeds 70 characters, so this always
// returns something within that budget.
const BROADCAST_NAME_MAX = 70;

function broadcastName(subject) {
  const stamp  = new Date().toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  const suffix = ` · ${stamp}`;
  const room   = BROADCAST_NAME_MAX - suffix.length;
  const head   = subject.length > room ? subject.slice(0, room - 1).trimEnd() + '…' : subject;
  return (head + suffix).slice(0, BROADCAST_NAME_MAX);   // belt and braces
}

function buildHtml(products, opts) {
  const list   = opts && opts.list;
  const shop   = shopOf(opts);
  const shown  = products.slice(0, MAX_CARDS);
  const extra  = products.length - shown.length;
  const heading = products.length === 1
    ? `1 New Item in the ${shop.name} Shop`
    : `${products.length} New Items in the ${shop.name} Shop`;

  return `<!doctype html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark">
<title>${esc(heading)}</title>
</head>
<body style="margin:0;padding:0;background:${C.bg};font-family:${FONT};">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${C.bg};padding:28px 12px;">
<tr><td align="center">
  <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:100%;max-width:600px;">

    <tr><td style="padding:0 0 22px 0;">
      <div style="color:${C.accent};font-family:${FONT};font-size:13px;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;">
        holoCompendium</div>
      <div style="color:${C.text};font-family:${FONT};font-size:25px;font-weight:700;margin-top:8px;line-height:1.25;">
        ${esc(heading)}</div>
    </td></tr>

    <tr><td>${shown.map(p => cardHtml(p, opts)).join('')}</td></tr>

    ${extra > 0 ? `<tr><td align="center" style="color:${C.muted};font-family:${FONT};font-size:14px;padding:4px 0 18px 0;">
      + ${extra} more not shown</td></tr>` : ''}

    <tr><td align="center" style="padding:14px 0 0 0;border-top:1px solid ${C.border};">
      <a href="${shop.site}" style="color:${C.accent};font-family:${FONT};font-size:14px;font-weight:600;text-decoration:none;">
        Browse the Full Archive →</a>
      ${list ? `<div style="margin-top:14px;">
        <a href="${UNSUB}"
           style="display:inline-block;color:${C.muted};font-family:${FONT};font-size:12px;font-weight:600;
                  text-decoration:none;padding:8px 18px;border:1px solid ${C.border};border-radius:7px;">
          Unsubscribe</a>
      </div>` : ''}
    </td></tr>

  </table>
</td></tr></table>
</body></html>`;
}

// Plain-text alternative — improves deliverability and is what a text-only
// client shows instead of a blank message.
function buildText(products, opts) {
  const shop  = shopOf(opts);
  const lines = products.slice(0, MAX_CARDS).map(p =>
    `• ${titleOf(p)}\n  ${priceLabel(p)} — ${statusOf(p)}\n  ${shop.product}/${p.handle}`);
  if (products.length > MAX_CARDS) lines.push(`+ ${products.length - MAX_CARDS} more not shown`);
  return `${products.length} New Item${products.length === 1 ? '' : 's'} in the ${shop.name} Shop\n\n${lines.join('\n\n')}\n\n${shop.site}\n`;
}

// "New: <title>" for one product, "<n> New <shop> Products" for several.
const subjectFor = (products, opts) => products.length === 1
  ? `New: ${titleOf(products[0])}`
  : `${products.length} New ${shopOf(opts).name} Products`;

export async function sendNewProductEmail(products, log = console.log, opts = {}) {
  if (!products?.length) return;

  const key  = process.env.RESEND_API_KEY;
  const to   = process.env.MAIL_TO;
  const from = process.env.MAIL_FROM;
  if (!key || !to || !from) {
    log('Email not configured (RESEND_API_KEY / MAIL_TO / MAIL_FROM) — skipping notification');
    return;
  }

  const subject = subjectFor(products, opts);

  const res = await fetch(RESEND_ENDPOINT, {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from,
      to: to.split(',').map(s => s.trim()).filter(Boolean),
      subject,
      html: buildHtml(products, opts),
      text: buildText(products, opts),
    }),
    signal: AbortSignal.timeout(20_000),
  });

  if (!res.ok) throw new Error(`Resend HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
  log(`Emailed ${products.length} new product${products.length === 1 ? '' : 's'} to ${to}`);
}

// Sends the same digest to everyone on the public signup list, as a Resend
// broadcast. Separate from sendNewProductEmail so the direct mail to the owner
// still goes out even if the list send fails — and so the list is optional:
// with RESEND_SEGMENT_ID unset this is a no-op and nothing else changes.
// (Resend renamed Audiences to Segments; broadcasts take segment_id.)
export async function sendBroadcast(products, log = console.log, opts = {}) {
  if (!products?.length) return;

  const key     = process.env.RESEND_API_KEY;
  const segment = process.env.RESEND_SEGMENT_ID;
  const from    = process.env.MAIL_FROM;
  if (!key || !segment || !from) return;   // list not set up — stay quiet

  const subject = subjectFor(products, opts);

  const create = await fetch('https://api.resend.com/broadcasts', {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      segment_id: segment,
      // `name` is the label in Resend's Broadcasts list, separate from the
      // subject line recipients see. Without it every entry reads "Untitled",
      // which makes the list useless once there's more than one.
      //
      // Resend caps it at 70 characters and rejects the whole broadcast with a
      // 422 if it's longer — which silently cost one product its announcement,
      // because a single-product subject is "New: <full title>" and hololive
      // titles run long. The date is kept and the title is what gets trimmed.
      name: broadcastName(subject),
      from, subject,
      html: buildHtml(products, { ...opts, list: true }),
      text: buildText(products, opts) + '\nUnsubscribe: ' + UNSUB + '\n',
    }),
    signal: AbortSignal.timeout(20_000),
  });
  if (!create.ok) throw new Error(`Broadcast create HTTP ${create.status}: ${(await create.text()).slice(0, 200)}`);

  const { id } = await create.json();

  // Draft mode proves the whole broadcast path — segment, sender, rendered HTML
  // — without mailing real subscribers about a product drop that didn't happen.
  // The draft sits in Resend → Broadcasts where it can be previewed or deleted.
  if (opts.draftOnly) {
    log(`Draft broadcast ${id} created and NOT sent — preview it in Resend → Broadcasts`);
    return;
  }

  const send = await fetch(`https://api.resend.com/broadcasts/${id}/send`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${key}` },
    signal: AbortSignal.timeout(20_000),
  });
  if (!send.ok) throw new Error(`Broadcast send HTTP ${send.status}: ${(await send.text()).slice(0, 200)}`);

  log(`Broadcast ${id} sent to the subscriber list`);
}

// Exported for the local preview script.
export const __test = { buildHtml, buildText };
