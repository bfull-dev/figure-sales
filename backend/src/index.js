'use strict';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const { S3Client, GetObjectCommand, PutObjectCommand,
        CopyObjectCommand, DeleteObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const sharp = require('sharp');

const app = express();
const PORT = process.env.PORT || 3000;

const KINTONE_DOMAIN = process.env.KINTONE_DOMAIN;
const API_TOKEN_167  = process.env.KINTONE_API_TOKEN_167;
const API_TOKEN_629  = process.env.KINTONE_API_TOKEN_629;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'http://localhost:5500';
const SITE_PASSWORD   = process.env.SITE_PASSWORD;

// ── R2 / Cloudflare config ────────────────────────────────────────────────────
const R2_ACCOUNT_ID        = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID     = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET_NAME       = process.env.R2_BUCKET_NAME;
const R2_PUBLIC_BASE_URL   = (process.env.R2_PUBLIC_BASE_URL || '').replace(/\/$/, '');
const CF_ZONE_ID           = process.env.CLOUDFLARE_ZONE_ID;
const CF_API_TOKEN         = process.env.CLOUDFLARE_API_TOKEN;
const ADMIN_SECRET         = process.env.ADMIN_SECRET;
const CARD_IMAGE_WIDTH       = Number(process.env.CARD_IMAGE_WIDTH)  || 700;
const CARD_IMAGE_QUALITY     = Number(process.env.CARD_IMAGE_QUALITY) || 75;
const WEBHOOK_167_SECRET     = process.env.WEBHOOK_167_SECRET;

const s3 = new S3Client({
  region: 'auto',
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
});

function brandToSlug(brand) {
  if (brand === 'FOTS') return 'fots';
  if (brand === 'インサイト') return 'insight';
  return brand.toLowerCase().replace(/[^\w-]/g, '') || 'unknown';
}
function cardKey(kikakuId, brand) {
  return `${kikakuId}-${brandToSlug(brand)}`;
}

// Valid tokens (server-side Set, reset on restart)
const validTokens = new Set();

// ── Products cache ──────────────────────────────────────────────────────────
const CACHE_TTL_MS = 5 * 60 * 1000; // 5分
let productsCache = { data: null, fetchedAt: null };

// ── R2 versions cache ─────────────────────────────────────────────────────────
// Shape: Map<string, string>  e.g. "KK-001-fots" → "20260407-1530"
let versionsCache = new Map();

async function loadVersionsFromR2() {
  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: 'versions.json' }));
    const text = await res.Body.transformToString();
    versionsCache = new Map(Object.entries(JSON.parse(text)));
    console.log(`[r2] versions loaded: ${versionsCache.size} entries`);
  } catch (err) {
    if (err.name === 'NoSuchKey') {
      console.log('[r2] versions.json not found — starting empty');
      versionsCache = new Map();
    } else {
      console.error('[r2] failed to load versions.json:', err.message);
    }
  }
}

async function saveVersionsToR2() {
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: 'versions.json',
    Body: JSON.stringify(Object.fromEntries(versionsCache)),
    ContentType: 'application/json',
  }));
}

function makeImageVersion() {
  const now = new Date();
  const p = n => String(n).padStart(2, '0');
  return `${now.getFullYear()}${p(now.getMonth() + 1)}${p(now.getDate())}-${p(now.getHours())}${p(now.getMinutes())}`;
}

// ── R2 image lifecycle ────────────────────────────────────────────────────────

async function downloadKintoneFile(fileKey) {
  const url = `https://${KINTONE_DOMAIN}/k/v1/file.json?fileKey=${encodeURIComponent(fileKey)}`;
  const res = await fetch(url, { headers: { 'X-Cybozu-API-Token': API_TOKEN_629 } });
  if (!res.ok) throw new Error(`Kintone file download failed: ${res.status}`);
  return Buffer.from(await res.arrayBuffer());
}

async function processAndUploadImage(fileKey, kikakuId, brand) {
  const rawBuffer = await downloadKintoneFile(fileKey);
  const jpegBuffer = await sharp(rawBuffer)
    .resize({ width: CARD_IMAGE_WIDTH, withoutEnlargement: true })
    .jpeg({ quality: CARD_IMAGE_QUALITY })
    .toBuffer();
  const key = cardKey(kikakuId, brand);
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET_NAME,
    Key: `products/${key}/card.jpg`,
    Body: jpegBuffer,
    ContentType: 'image/jpeg',
  }));
  const version = makeImageVersion();
  versionsCache.set(key, version);
  await saveVersionsToR2();
  return version;
}

async function archiveImage(kikakuId, brand) {
  const key = cardKey(kikakuId, brand);
  const src = `products/${key}/card.jpg`;
  const dst = `archive/${key}/card.jpg`;
  await s3.send(new CopyObjectCommand({
    Bucket: R2_BUCKET_NAME,
    CopySource: `${R2_BUCKET_NAME}/${src}`,
    Key: dst,
  }));
  await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET_NAME, Key: src }));
  versionsCache.delete(key);
  await saveVersionsToR2();
  await purgeCdnUrl(`${R2_PUBLIC_BASE_URL}/${src}`);
}

async function restoreImage(kikakuId, brand) {
  const key = cardKey(kikakuId, brand);
  const src = `archive/${key}/card.jpg`;
  const dst = `products/${key}/card.jpg`;
  await s3.send(new CopyObjectCommand({
    Bucket: R2_BUCKET_NAME,
    CopySource: `${R2_BUCKET_NAME}/${src}`,
    Key: dst,
  }));
  await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET_NAME, Key: src }));
  const version = makeImageVersion();
  versionsCache.set(key, version);
  await saveVersionsToR2();
  await purgeCdnUrl(`${R2_PUBLIC_BASE_URL}/${dst}`);
}

async function archiveExists(kikakuId, brand) {
  try {
    await s3.send(new HeadObjectCommand({
      Bucket: R2_BUCKET_NAME,
      Key: `archive/${cardKey(kikakuId, brand)}/card.jpg`,
    }));
    return true;
  } catch {
    return false;
  }
}

async function purgeCdnUrl(url) {
  if (!CF_ZONE_ID || !CF_API_TOKEN) return;
  try {
    await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/purge_cache`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${CF_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ files: [url] }),
    });
  } catch (err) {
    console.error('[cdn] purge failed:', err.message);
  }
}

// ── Middleware ──────────────────────────────────────────────────────────────

app.use(cors({
  origin: FRONTEND_ORIGIN,
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

app.use(express.json());

// ── Auth helpers ─────────────────────────────────────────────────────────────

function requireAuth(req, res, next) {
  const authHeader = req.headers['authorization'];
  let token = null;
  if (authHeader && authHeader.startsWith('Bearer ')) {
    token = authHeader.slice(7);
  } else if (req.query.token) {
    token = req.query.token;
  }
  if (!token || !validTokens.has(token)) {
    return res.status(401).json({ error: 'Unauthorized', code: 'INVALID_TOKEN' });
  }
  next();
}

function requireAdmin(req, res, next) {
  if (req.headers['x-admin-secret'] !== ADMIN_SECRET) {
    return res.status(403).json({ error: 'Forbidden', code: 'INVALID_ADMIN_SECRET' });
  }
  next();
}

// ── Kintone fetch helpers ────────────────────────────────────────────────────

async function fetchAllRecords(appId, apiToken, query, fields) {
  const records = [];
  const limit = 500;
  let offset = 0;

  while (true) {
    const params = new URLSearchParams({
      app: appId,
      query: `${query} limit ${limit} offset ${offset}`,
      totalCount: 'true',
    });
    for (const f of fields) params.append('fields[]', f);

    const url = `https://${KINTONE_DOMAIN}/k/v1/records.json?${params}`;
    const res = await fetch(url, {
      headers: { 'X-Cybozu-API-Token': apiToken },
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Kintone API error app=${appId} status=${res.status}: ${body}`);
    }

    const data = await res.json();
    records.push(...data.records);

    if (records.length >= Number(data.totalCount)) break;
    offset += limit;
  }

  return records;
}

// ── Card builder ─────────────────────────────────────────────────────────────

const BRAND_IMAGE_KEY = {
  'インサイト': '貼り付け用画像_インサイト',
  'FOTS': '貼り付け用画像_FOTS',
};
const BRAND_DESC_KEY = {
  'インサイト': '商品説明_インサイト',
  'FOTS': '商品説明_FOTS',
};

function buildCards(records167, map629) {
  const cards = [];

  for (const rec of records167) {
    const kikakuId = rec['企画ID'].value;
    const rec629 = map629.get(kikakuId) || null;

    // Group subtable rows by brand
    const subtable = rec['売上見込_0'].value || [];
    const brandMap = new Map();
    for (const row of subtable) {
      const v = row.value;
      const brand = v['ブランド_'].value || '(未設定)';
      if (!brandMap.has(brand)) brandMap.set(brand, []);
      brandMap.get(brand).push(v);
    }

    // Release date: App167 発売月 only
    const releaseDate = rec['発売月']?.value || null;

    // Reservation end: App167 予約締切日 only
    const reservationEndDate = rec['予約締切日']?.value || null;

    for (const [brand, rows] of brandMap) {
      // Pick image URL from R2 versions cache
      let imageUrl = null;
      let imageVersion = null;
      const imgField = BRAND_IMAGE_KEY[brand];
      if (imgField && rec629 && rec629[imgField]?.value?.length > 0) {
        const key = cardKey(kikakuId, brand);
        const version = versionsCache.get(key);
        if (version) {
          imageUrl = `${R2_PUBLIC_BASE_URL}/products/${key}/card.jpg`;
          imageVersion = version;
        }
      }

      // Pick BOX URL
      let boxOrderUrl = null;
      if (brand === 'インサイト') boxOrderUrl = rec['box_order_url_IN']?.value || null;
      if (brand === 'FOTS') boxOrderUrl = rec['box_order_url_FOTS']?.value || null;

      // Pick description
      let description = null;
      const descField = BRAND_DESC_KEY[brand];
      if (descField && rec629 && rec629[descField]?.value) {
        description = rec629[descField].value || null;
      }

      // Build product list
      const products = rows.map(v => ({
        productName: v['商品名_スケール']?.value || '',
        scale: v['スケール']?.value || '',
        planType: v['企画タイプ']?.value || '',
        priceTax: Number(v['税込価格']?.value) || 0,
        priceNoTax: Number(v['税抜価格']?.value) || 0,
        height: v['全高サイズ']?.value || '',
        janCode: v['JANコード']?.value || '',
        distribution: v['流通']?.value || '',
        note: v['補足']?.value || '',
      }));

      cards.push({
        kikakuId,
        brand,
        title: rec['タイトル']?.value || '',
        characterName: rec['キャラクター名']?.value || '',
        copyright: rec['コピーライト_0']?.value || '',
        material: rec['材料選択']?.value || '',
        productionCountry: rec['生産国']?.value || '',
        reservationStartDate: rec['予約開始日']?.value || null,
        reservationEndDate,
        releaseDate,
        boxOrderUrl,
        imageUrl,
        imageVersion,
        description,
        products,
        noteEnabled: Array.isArray(rec['補足文']?.value) && rec['補足文'].value.includes('ON'),
        noteText: rec['カード用補足文']?.value || null,
      });
    }
  }

  return cards;
}

// ── Routes ───────────────────────────────────────────────────────────────────

// Health check
app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

// Login
app.post('/api/login', (req, res) => {
  const { password } = req.body || {};
  if (!password || password !== SITE_PASSWORD) {
    return res.status(401).json({ error: 'Unauthorized', code: 'WRONG_PASSWORD' });
  }
  const token = crypto.randomBytes(16).toString('hex');
  validTokens.add(token);
  res.json({ token });
});

// Products
app.get('/api/products', requireAuth, async (_req, res) => {
  try {
    const fields167 = [
      '企画ID', '非表示', '予約開始日', '予約締切日', '発売月',
      'タイトル', 'キャラクター名', 'コピーライト_0', '材料選択', '生産国',
      'box_order_url_IN', 'box_order_url_FOTS', '売上見込_0',
      '補足文', 'カード用補足文',
    ];
    const fields629 = [
      '企画ID', '貼り付け用画像_インサイト', '貼り付け用画像_FOTS',
      '商品説明_インサイト', '商品説明_FOTS', '発売予定日', 'ご予約締め切り日',
      'スケール_1', 'スケール_2', '商品名_1', '商品名_2',
      '税込価格_1', '税込価格_2', 'JANコード_1', 'JANコード_2',
      '材料選択', '生産国', 'カートン入数',
    ];

    // キャッシュが有効な場合はそのまま返す
    const now = Date.now();
    if (productsCache.data && (now - productsCache.fetchedAt) < CACHE_TTL_MS) {
      console.log(`[cache] HIT (age: ${Math.round((now - productsCache.fetchedAt) / 1000)}s)`);
      return res.json(productsCache.data);
    }

    console.log('[cache] MISS — fetching from Kintone');
    await loadVersionsFromR2();
    const [records167, records629] = await Promise.all([
      fetchAllRecords(167, API_TOKEN_167,
        'サイト表示 in ("ON") order by 予約開始日 desc', fields167),
      fetchAllRecords(629, API_TOKEN_629,
        'order by $id asc', fields629),
    ]);

    // Build Map for App 629
    const map629 = new Map();
    for (const rec of records629) {
      const id = rec['企画ID']?.value;
      if (id) map629.set(id, Object.fromEntries(
        Object.entries(rec).map(([k, v]) => [k, v])
      ));
    }

    const cards = buildCards(records167, map629);
    const responseData = {
      cards,
      fetchedAt: new Date().toISOString(),
      total: cards.length,
    };

    // キャッシュを更新
    productsCache = { data: responseData, fetchedAt: Date.now() };
    console.log(`[cache] STORED ${cards.length} cards`);

    res.json(responseData);
  } catch (err) {
    console.error('Error fetching products:', err);
    res.status(503).json({ error: 'データを取得できませんでした', code: 'KINTONE_ERROR' });
  }
});

// ── Admin: Full sync ──────────────────────────────────────────────────────────
app.post('/api/admin/sync', requireAdmin, async (_req, res) => {
  try {
    const [records167, records629] = await Promise.all([
      fetchAllRecords(167, API_TOKEN_167, 'サイト表示 in ("ON") order by $id asc',
        ['企画ID', '非表示', 'サイト表示']),
      fetchAllRecords(629, API_TOKEN_629, 'order by $id asc',
        ['企画ID', '貼り付け用画像_インサイト', '貼り付け用画像_FOTS']),
    ]);
    const map629 = new Map(records629
      .filter(r => r['企画ID']?.value)
      .map(r => [r['企画ID'].value, r]));

    const results = [];
    for (const rec of records167) {
      const kikakuId = rec['企画ID'].value;
      const rec629 = map629.get(kikakuId);
      if (!rec629) continue;
      for (const [brand, imgField] of Object.entries(BRAND_IMAGE_KEY)) {
        const files = rec629[imgField]?.value;
        if (!files?.length) continue;
        const key = cardKey(kikakuId, brand);
        if (versionsCache.has(key)) {
          results.push({ key, status: 'skipped' });
          continue;
        }
        try {
          await processAndUploadImage(files[0].fileKey, kikakuId, brand);
          results.push({ key, status: 'uploaded' });
        } catch (err) {
          console.error(`[sync] error for ${key}:`, err.message);
          results.push({ key, status: 'error', message: err.message });
        }
      }
    }
    productsCache = { data: null, fetchedAt: null };
    res.json({ synced: results.length, results });
  } catch (err) {
    console.error('[sync] error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Archive ────────────────────────────────────────────────────────────
app.post('/api/admin/archive/:kikakuId/:brand', requireAdmin, async (req, res) => {
  try {
    await archiveImage(req.params.kikakuId, req.params.brand);
    productsCache = { data: null, fetchedAt: null };
    res.json({ status: 'archived', key: cardKey(req.params.kikakuId, req.params.brand) });
  } catch (err) {
    console.error('[archive] error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Restore ────────────────────────────────────────────────────────────
app.post('/api/admin/restore/:kikakuId/:brand', requireAdmin, async (req, res) => {
  try {
    await restoreImage(req.params.kikakuId, req.params.brand);
    productsCache = { data: null, fetchedAt: null };
    res.json({ status: 'restored', key: cardKey(req.params.kikakuId, req.params.brand) });
  } catch (err) {
    console.error('[restore] error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── Webhook: App 167 サイト表示 変更 ─────────────────────────────────────────
app.post('/webhook/app167', async (req, res) => {
  // Secret 検証（クエリパラメータ ?k=）
  if (WEBHOOK_167_SECRET && req.query.k !== WEBHOOK_167_SECRET) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }

  try {
    const { type, record } = req.body;

    // ADD_RECORD / EDIT_RECORD のみ処理
    if (type !== 'ADD_RECORD' && type !== 'EDIT_RECORD') {
      return res.json({ ok: true, skipped: `type=${type}` });
    }
    if (!record) return res.json({ ok: true, skipped: 'no record' });

    const kikakuId = record['企画ID']?.value;
    if (!kikakuId) return res.json({ ok: true, skipped: 'no kikakuId' });

    const siteDisplay = record['サイト表示']?.value;
    const isPublished = Array.isArray(siteDisplay) && siteDisplay.includes('ON');

    if (isPublished) {
      // ── 公開: App 629 から画像を取得して R2 に同期 ──
      const records629 = await fetchAllRecords(629, API_TOKEN_629,
        `企画ID = "${kikakuId}"`,
        ['企画ID', '貼り付け用画像_インサイト', '貼り付け用画像_FOTS']);
      const rec629 = records629[0] || null;

      const uploadResults = [];
      for (const [brand, imgField] of Object.entries(BRAND_IMAGE_KEY)) {
        const files = rec629?.[imgField]?.value;
        if (!files?.length) continue;

        const key = cardKey(kikakuId, brand);
        if (versionsCache.has(key)) {
          uploadResults.push({ key, status: 'skipped' });
        } else if (await archiveExists(kikakuId, brand)) {
          await restoreImage(kikakuId, brand);
          uploadResults.push({ key, status: 'restored' });
        } else {
          await processAndUploadImage(files[0].fileKey, kikakuId, brand);
          uploadResults.push({ key, status: 'uploaded' });
        }
      }

      productsCache = { data: null, fetchedAt: null };
      console.log(`[webhook/167] published kikakuId=${kikakuId}`, uploadResults);
      return res.json({ ok: true, kikakuId, action: 'published', results: uploadResults });

    } else {
      // ── 非公開: R2 の画像を archive へ移動 ──
      const archiveResults = [];
      for (const brand of Object.keys(BRAND_IMAGE_KEY)) {
        const key = cardKey(kikakuId, brand);
        if (versionsCache.has(key)) {
          await archiveImage(kikakuId, brand);
          archiveResults.push({ key, status: 'archived' });
        } else {
          archiveResults.push({ key, status: 'skipped' });
        }
      }

      productsCache = { data: null, fetchedAt: null };
      console.log(`[webhook/167] unpublished kikakuId=${kikakuId}`, archiveResults);
      return res.json({ ok: true, kikakuId, action: 'unpublished', results: archiveResults });
    }

  } catch (err) {
    console.error('[webhook/167] error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── Start ────────────────────────────────────────────────────────────────────

app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await loadVersionsFromR2();
});
