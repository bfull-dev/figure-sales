'use strict';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();
const PORT = process.env.PORT || 3000;

const KINTONE_DOMAIN = process.env.KINTONE_DOMAIN;
const API_TOKEN_167 = process.env.KINTONE_API_TOKEN_167;
const API_TOKEN_629 = process.env.KINTONE_API_TOKEN_629;
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'http://localhost:5500';
const SITE_PASSWORD = process.env.SITE_PASSWORD;

// Valid tokens (server-side Set, reset on restart)
const validTokens = new Set();

// ── Products cache ──────────────────────────────────────────────────────────
const CACHE_TTL_MS = 5 * 60 * 1000; // 5分
let productsCache = { data: null, fetchedAt: null };

// ── Middleware ──────────────────────────────────────────────────────────────

app.use(cors({
  origin: FRONTEND_ORIGIN,
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

app.use(express.json());

// ── Auth helper ─────────────────────────────────────────────────────────────

function requireAuth(req, res, next) {
  // Support both Bearer header and ?token= query param (for <img> src usage)
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
      // Pick image fileKey
      let imageFileKey = null;
      const imgField = BRAND_IMAGE_KEY[brand];
      if (imgField && rec629 && rec629[imgField]?.value?.length > 0) {
        imageFileKey = rec629[imgField].value[0].fileKey;
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
        imageFileKey,
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
  const token = crypto.randomBytes(16).toString('hex'); // 32 chars
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

// File proxy
const FILE_KEY_RE = /^[a-zA-Z0-9\-_.]+$/;

app.get('/api/file/:fileKey', requireAuth, async (req, res) => {
  const { fileKey } = req.params;
  if (!FILE_KEY_RE.test(fileKey)) {
    return res.status(404).json({ error: 'Not found', code: 'INVALID_FILE_KEY' });
  }

  try {
    const url = `https://${KINTONE_DOMAIN}/k/v1/file.json?fileKey=${encodeURIComponent(fileKey)}`;
    const kRes = await fetch(url, {
      headers: { 'X-Cybozu-API-Token': API_TOKEN_629 },
    });

    if (!kRes.ok) {
      return res.status(404).json({ error: 'File not found', code: 'FILE_NOT_FOUND' });
    }

    const contentType = kRes.headers.get('content-type') || 'application/octet-stream';
    res.set('Content-Type', contentType);
    res.set('Cache-Control', 'public, max-age=3600');
    kRes.body.pipe(res);
  } catch (err) {
    console.error('Error fetching file:', err);
    res.status(503).json({ error: 'ファイルを取得できませんでした', code: 'FILE_ERROR' });
  }
});

// ── Start ────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
