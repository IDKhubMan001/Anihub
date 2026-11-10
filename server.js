/* AniHub · сервер комнат (Render + Socket.IO)
   ---------------------------------------------
   Зачем: GitHub Pages — статика, поэтому signaling для WebRTC
   (демонстрация экрана / камера) и живой список участников
   тянет этот маленький Node-сервер. На бесплатном тарифе Render
   сервис засыпает; первый запрос к /api/ping будит его
   (холодный старт до ~60 сек) — клиент показывает плашку
   «Соединение с сервером…», пока ждёт ответа.

   Локальный запуск:  PORT=3000 node server.js
   Деплой на Render:  Web Service → этот репозиторий →
                      Build: npm install   Start: node server.js
*/
'use strict';
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const app = express();
/* JSON-body для REST комнат (put/sync/hb) */
app.use(express.json({ limit: '1mb' }));
/* CORS для REST: сайт живёт на Pages/file://, сервер — на onrender.com.
   Без этих заголовков браузер блокирует ответы /api/* → «сервер не отвечает». */
app.use((q, r, next) => {
  r.header('Access-Control-Allow-Origin', '*');
  if (q.method === 'OPTIONS') {
    r.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    r.header('Access-Control-Allow-Headers', 'Content-Type,Authorization');
    return r.sendStatus(204);
  }
  next();
});
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },   // Pages, file://, localhost — всё можно
  transports: ['polling', 'websocket'],
  pingInterval: 20000,
  pingTimeout: 25000,
  maxHttpBufferSize: 2e6   // SDP/ICE маленькие, но пусть будет запас
});

/* ------------------------------ REST ------------------------------ */
let KA = 0;   // счётчик keep-alive пингов с сайтов
app.get('/api/ping', (q, r) => {
  KA++;
  if (q.query.pretty !== undefined || /text\/html/.test(String(q.headers.accept || ''))) {
    const up = process.uptime();
    const hh = Math.floor(up / 3600), mm = Math.floor(up % 3600 / 60), ss = Math.round(up % 60);
    const mins = KA < 60 ? `${KA} пинг(ов)` : `${(KA / 60).toFixed(1)} пинг/мин`;
    r.type('html').send(
`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AniHub · сервер жив</title>
<body style="margin:0;min-height:100vh;display:grid;place-items:center;background:#07060d;color:#f6f4fb;font-family:'Segoe UI',system-ui,sans-serif">
<div style="text-align:center;padding:30px">
<div style="font-size:64px;line-height:1">🟢</div>
<h1 style="margin:14px 0 4px;font-size:22px">Сервер комнат работает</h1>
<p style="margin:0;color:#a29dbb">Аптайм: <b style="color:#f6f4fb">${hh ? hh + ' ч ' : ''}${mm} мин ${ss} с</b></p>
<p style="margin:6px 0 0;color:#a29dbb">Keep-alive пингов с сайтов: <b style="color:#6ee7ff">${KA}</b></p>
<p style="margin:18px 0 0;font-size:12px;color:#6f6a84">anihub-rooms · API: <code style="color:#6ee7ff">/api/ping</code> · <code style="color:#6ee7ff">/api/rooms</code></p>
</div>`);
    return;
  }
  r.json({ ok: true, ts: Date.now(), up: Math.round(process.uptime()), ka: KA, service: 'anihub-rooms' });
});
app.get('/api/health', (q, r) => r.json({ ok: true }));
app.get('/', (q, r) => r.json({ ok: true, service: 'anihub-rooms', hint: 'GET /api/ping' }));

/* ------------------------- хранилище комнат -----------------------
   roomsState: Map<code, {rev, data, at}> — состояние эфиров.
   Сервер — единый источник правды: нет комнаты здесь — её нет ни у кого.
   Память живёт, пока работает инстанс (на бесплатном Render сбрасывается
   при перезапуске/засыпании — клиенты автоматически перепубликуют свои). */
const roomsState = new Map();
const roomsHB = new Map();     // code → {uid: ts} — кто онлайн (REST-клиенты)
const waiters = new Map();     // code → Set<fn> — long-poll подписчики
const ROOM_TTL = 12 * 3600e3;  // эфиры без активности живут 12 часов

function notifyWaiters(code) {
  const set = waiters.get(code);
  if (set) { const fns = [...set]; set.clear(); for (const fn of fns) { try { fn(); } catch {} } }
}
function hbOut(code) {
  const m = roomsHB.get(code) || {};
  const now = Date.now();
  const out = {}; for (const [u, t] of Object.entries(m)) if (now - t < 120000) out[u] = t;
  return out;
}

/* список эфиров */
app.get('/api/rooms', (q, r) => {
  const out = {};
  for (const [c, s] of roomsState) out[c] = { rev: s.rev, data: s.data };
  r.json({ rooms: out });
});

/* состояние комнаты (с long-poll: ?since=N ждёт до 20с) */
app.get('/api/room/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const s = roomsState.get(code);
  if (!s) return res.status(404).json({ error: 'комнаты нет на сервере', serverNow: Date.now() });
  const since = parseInt(req.query.since || '0', 10) || 0;
  if (s.rev <= since) {
    const set = waiters.get(code) || new Set(); waiters.set(code, set);
    let fired = false;
    const fire = () => {
      if (fired) return; fired = true; set.delete(fire);
      const s2 = roomsState.get(code);
      if (!s2) return res.status(404).json({ error: 'комнаты нет на сервере', serverNow: Date.now() });
      res.json({ rev: s2.rev, data: s2.data, hb: hbOut(code), serverNow: Date.now() });
    };
    set.add(fire);
    setTimeout(fire, 20000);
    return;
  }
  res.json({ rev: s.rev, data: s.data, hb: hbOut(code), serverNow: Date.now() });
});

/* действия: put / sync / hb / allowLinks */
app.post('/api/room/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const body = req.body || {};
  const act = body.act;
  let s = roomsState.get(code);
  if (act === 'put') {
    const data = body.data || {};
    if (typeof data !== 'object' || JSON.stringify(data).length > 900000)
      return res.status(413).json({ error: 'слишком большие данные комнаты' });
    if (s && s.data) {
      if (s.data.members && data.members && typeof data.members === 'object') {
        for (const [uid2, prevM] of Object.entries(s.data.members)) {
          if (!data.members[uid2]) {
            data.members[uid2] = prevM;
          } else if (typeof data.members[uid2].allowLinks !== 'boolean' && typeof prevM.allowLinks === 'boolean') {
            data.members[uid2].allowLinks = prevM.allowLinks;
          }
        }
      }
      if (Array.isArray(s.data.log) && s.data.log.length) {
        data.log = Array.isArray(data.log) ? data.log : [];
        for (const lg of s.data.log) {
          if (lg && !data.log.some(x => x && x.action === lg.action && x.target === lg.target && Math.abs((x.at || 0) - (lg.at || 0)) < 3000)) {
            data.log.unshift(lg);
          }
        }
        data.log.sort((a, b) => (b.at || 0) - (a.at || 0));
        data.log = data.log.slice(0, 100);
      }
    }
    sanitizeRoomPut(code, data, s);
    if (s) { s.data = data; s.rev++; s.at = Date.now(); }
    else {
      const seen = new Set(((data && data.msgs) || []).map(m => m && m.id).filter(Boolean));
      roomsState.set(code, { rev: 1, data, at: Date.now(), seenMsgs: seen });
    }
    notifyWaiters(code);
    return res.json({ rev: roomsState.get(code).rev, serverNow: Date.now() });
  }
  if (!s) return res.status(404).json({ error: 'комнаты нет на сервере', serverNow: Date.now() });
  if (act === 'allowLinks') {
    const byId = String(body.byUid || '');
    const targetId = String(body.targetUid || '');
    const mems = (s.data && s.data.members) || {};
    const actor = mems[byId];
    const target = mems[targetId];
    if (!actor || !['streamer', 'moderator'].includes(actor.role))
      return res.status(403).json({ error: 'разрешать ссылки может только стример или модератор', serverNow: Date.now() });
    if (!target) return res.status(404).json({ error: 'участник не найден в комнате', serverNow: Date.now() });
    target.allowLinks = body.allow !== undefined ? !!body.allow : !target.allowLinks;
    s.data.msgs = s.data.msgs || [];
    const mid = 'sys_' + Math.random().toString(36).slice(2, 9) + Date.now().toString(36).slice(-4);
    s.data.msgs.push({
      id: mid, sys: true, at: Date.now(),
      body: '🔗 ' + (target.name || 'Участник') + ': ссылки в этой комнате ' + (target.allowLinks ? 'разрешены (' + (actor.name || 'модератор') + ')' : 'снова запрещены')
    });
    if (s.seenMsgs) s.seenMsgs.add(mid);
    s.rev++; s.at = Date.now();
    notifyWaiters(code);
    return res.json({ ok: true, allowLinks: target.allowLinks, rev: s.rev, serverNow: Date.now() });
  }
  if (act === 'sync') {
    s.data.sync = { st: body.st, uid: body.uid, name: body.name, at: Date.now() };
    s.rev++; s.at = Date.now();
    notifyWaiters(code);
    return res.json({ rev: s.rev, serverNow: Date.now() });
  }
  if (act === 'hb') {
    const m = roomsHB.get(code) || {};
    m[body.uid || '?'] = Date.now();
    const now = Date.now();
    for (const k of Object.keys(m)) if (now - m[k] > 120000) delete m[k];
    roomsHB.set(code, m);
    return res.json({ ok: true, serverNow: now });
  }
  return res.status(400).json({ error: 'неизвестное действие' });
});

/* подметаем эфиры без активности раз в 30 минут */
setInterval(() => {
  const now = Date.now();
  for (const [c, s] of [...roomsState]) {
    if (now - s.at > ROOM_TTL) { roomsState.delete(c); roomsHB.delete(c); notifyWaiters(c); }
  }
}, 1800000);

/* ============================================================================
   v17.47 · ВНЕШНЯЯ БАЗА ДАННЫХ (PostgreSQL: Supabase / Neon.tech)
   + ОБЛАЧНОЕ ХРАНИЛИЩЕ КАРТИНОК (Supabase Storage / Cloudflare R2)
   + ПОЛНЫЙ ОТКАЗ ОТ ЛОКАЛЬНОГО ДИСКА RENDER И АВТООЧИСТКА МУСОРА
   ============================================================================ */
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { Pool } = require('pg');

/* --- 1. Автоочистка временных файлов, старых JSON-дампов и кэшей на Render --- */
const LEGACY_DB_FILES = [
  'comments.json', 'ovr.json', 'reports.json', 'modlog.json', 'moderation.json',
  'profiles.json', 'users.json', 'penalties.json', 'friends.json', 'dms.json',
  'npm-debug.log', 'yarn-error.log'
];

let lastPurgeStats = { at: 0, removedCount: 0, freedBytes: 0, items: [] };

function safeStatSize(p) {
  try {
    const st = fs.lstatSync(p);
    if (st.isSymbolicLink()) return 0;
    if (st.isFile()) return st.size || 0;
    if (st.isDirectory()) {
      let sum = 0;
      for (const name of fs.readdirSync(p)) sum += safeStatSize(path.join(p, name));
      return sum;
    }
  } catch {}
  return 0;
}

function purgeRenderDisk() {
  let removedCount = 0;
  let freedBytes = 0;
  const items = [];
  const roots = [...new Set([process.cwd(), __dirname])];

  for (const dir of roots) {
    for (const fname of LEGACY_DB_FILES) {
      const fp = path.join(dir, fname);
      try {
        if (fs.existsSync(fp)) {
          const sz = safeStatSize(fp);
          fs.rmSync(fp, { force: true });
          removedCount++;
          freedBytes += sz;
          items.push(fname);
        }
      } catch {}
    }
    try {
      for (const entry of fs.readdirSync(dir)) {
        if (/\.log(\.\d+)?$/i.test(entry)) {
          const fp = path.join(dir, entry);
          const sz = safeStatSize(fp);
          fs.rmSync(fp, { force: true });
          removedCount++;
          freedBytes += sz;
          items.push(entry);
        }
      }
    } catch {}
  }

  /* Чистим кэш npm (~/.npm/_cacache, ~/.npm/_logs) и старые npm/v8 кэши в /tmp на Render */
  const homeDir = os.homedir ? os.homedir() : '';
  if (homeDir) {
    for (const sub of [path.join(homeDir, '.npm', '_cacache'), path.join(homeDir, '.npm', '_logs')]) {
      try {
        if (fs.existsSync(sub)) {
          const sz = safeStatSize(sub);
          fs.rmSync(sub, { recursive: true, force: true });
          removedCount++;
          freedBytes += sz;
          items.push(sub);
        }
      } catch {}
    }
  }
  const tmpDir = os.tmpdir ? os.tmpdir() : '/tmp';
  try {
    for (const entry of fs.readdirSync(tmpDir)) {
      if (/^(npm-|v8-compile-cache|anihub-tmp-|core\.)/i.test(entry)) {
        const fp = path.join(tmpDir, entry);
        const sz = safeStatSize(fp);
        fs.rmSync(fp, { recursive: true, force: true });
        removedCount++;
        freedBytes += sz;
        items.push('tmp/' + entry);
      }
    }
  } catch {}

  lastPurgeStats = { at: Date.now(), removedCount, freedBytes, items: items.slice(0, 40) };
  if (removedCount > 0) {
    console.log('[Render-Clean] removed', removedCount, 'temp/legacy items, freed', freedBytes, 'bytes');
  }
  return lastPurgeStats;
}
purgeRenderDisk();

/* --- 2. Облачное хранилище для картинок и аватарок (Supabase Storage / Cloudflare R2) --- */
function getStorageCfg() {
  const sbUrl = String(process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
  const sbKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_KEY || '').trim();
  const sbBucket = String(process.env.SUPABASE_BUCKET || 'avatars').trim();
  if (sbUrl && sbKey) return { provider: 'supabase', sbUrl, sbKey, bucket: sbBucket };

  const r2Key = String(process.env.R2_ACCESS_KEY_ID || '').trim();
  const r2Sec = String(process.env.R2_SECRET_ACCESS_KEY || '').trim();
  const r2Bucket = String(process.env.R2_BUCKET || '').trim();
  const r2Endpoint = String(
    process.env.R2_ENDPOINT ||
    (process.env.R2_ACCOUNT_ID ? `https://${String(process.env.R2_ACCOUNT_ID).trim()}.r2.cloudflarestorage.com` : '')
  ).trim().replace(/\/+$/, '');
  const r2Pub = String(process.env.R2_PUBLIC_URL || '').trim().replace(/\/+$/, '');
  if (r2Key && r2Sec && r2Bucket && r2Endpoint && r2Pub) {
    return { provider: 'r2', r2Key, r2Sec, bucket: r2Bucket, r2Endpoint, r2Pub };
  }
  return { provider: 'none' };
}

let bucketReadyPromise = null;
async function ensureCloudBucket() {
  const cfg = getStorageCfg();
  if (cfg.provider !== 'supabase') return;
  if (bucketReadyPromise) return bucketReadyPromise;
  bucketReadyPromise = (async () => {
    try {
      const headers = {
        'Authorization': 'Bearer ' + cfg.sbKey,
        'apikey': cfg.sbKey,
        'Content-Type': 'application/json'
      };
      const chk = await fetch(`${cfg.sbUrl}/storage/v1/bucket/${encodeURIComponent(cfg.bucket)}`, {
        method: 'GET', headers, signal: AbortSignal.timeout(8000)
      });
      if (chk.status === 404 || chk.status === 400) {
        await fetch(`${cfg.sbUrl}/storage/v1/bucket`, {
          method: 'POST',
          headers,
          body: JSON.stringify({
            id: cfg.bucket,
            name: cfg.bucket,
            public: true,
            file_size_limit: 2097152,
            allowed_mime_types: ['image/jpeg', 'image/png', 'image/webp', 'image/gif']
          }),
          signal: AbortSignal.timeout(8000)
        });
      }
    } catch (e) {
      bucketReadyPromise = null;
      console.warn('[Cloud-Storage] bucket check warning:', e && e.message);
    }
  })();
  return bucketReadyPromise;
}

function hmacSha256(key, data) {
  return crypto.createHmac('sha256', key).update(data).digest();
}
function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

async function uploadToR2(cfg, objKey, buf, mime) {
  const u = new URL(`${cfg.r2Endpoint}/${cfg.bucket}/${objKey}`);
  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '');
  const dateStamp = amzDate.slice(0, 8);
  const region = 'auto';
  const service = 's3';
  const payloadHash = sha256Hex(buf);
  const canonicalHeaders =
    `content-type:${mime}\n` +
    `host:${u.host}\n` +
    `x-amz-content-sha256:${payloadHash}\n` +
    `x-amz-date:${amzDate}\n`;
  const signedHeaders = 'content-type;host;x-amz-content-sha256;x-amz-date';
  const canonicalRequest = ['PUT', u.pathname, '', canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`;
  const stringToSign = ['AWS4-HMAC-SHA256', amzDate, credentialScope, sha256Hex(canonicalRequest)].join('\n');
  const kDate = hmacSha256('AWS4' + cfg.r2Sec, dateStamp);
  const kRegion = hmacSha256(kDate, region);
  const kService = hmacSha256(kRegion, service);
  const kSigning = hmacSha256(kService, 'aws4_request');
  const signature = crypto.createHmac('sha256', kSigning).update(stringToSign).digest('hex');
  const authHeader = `AWS4-HMAC-SHA256 Credential=${cfg.r2Key}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const resp = await fetch(u.toString(), {
    method: 'PUT',
    headers: {
      'Content-Type': mime,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate,
      'Authorization': authHeader
    },
    body: buf,
    signal: AbortSignal.timeout(12000)
  });
  if (!resp.ok) throw new Error('R2 upload HTTP ' + resp.status);
  return `${cfg.r2Pub}/${objKey}?v=${Date.now()}`;
}

function parseDataImage(dataUrl) {
  const m = /^data:(image\/(jpeg|jpg|png|webp|gif));base64,([A-Za-z0-9+/=\s]+)$/i.exec(String(dataUrl || '').trim());
  if (!m) return null;
  const mime = m[1].toLowerCase() === 'image/jpg' ? 'image/jpeg' : m[1].toLowerCase();
  const ext = m[2].toLowerCase() === 'jpg' ? 'jpeg' : m[2].toLowerCase();
  const buf = Buffer.from(m[3].replace(/\s+/g, ''), 'base64');
  if (!buf.byteLength || buf.byteLength > 1500000) return null;
  return { mime, ext: ext === 'jpeg' ? 'jpg' : ext, buf };
}

async function deleteOldCloudAvatar(cfg, prevUrl) {
  if (!prevUrl || cfg.provider !== 'supabase') return;
  const prefix = `${cfg.sbUrl}/storage/v1/object/public/${cfg.bucket}/`;
  if (!prevUrl.startsWith(prefix)) return;
  const objPath = prevUrl.slice(prefix.length).split('?')[0];
  if (!objPath) return;
  try {
    await fetch(`${cfg.sbUrl}/storage/v1/object/${encodeURIComponent(cfg.bucket)}/${objPath}`, {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + cfg.sbKey, 'apikey': cfg.sbKey },
      signal: AbortSignal.timeout(6000)
    });
  } catch {}
}

async function resolveCloudAvatar(nickLower, rawAvatar, prevUrl = '') {
  const val = String(rawAvatar == null ? '' : rawAvatar).trim();
  const cfg = getStorageCfg();
  if (!val) {
    await deleteOldCloudAvatar(cfg, prevUrl);
    return { ok: true, url: '' };
  }
  if (/^https?:\/\/[^\s"'<>]{8,500}$/i.test(val)) {
    return { ok: true, url: val };
  }
  const parsed = parseDataImage(val);
  if (!parsed) return { ok: true, url: '' };

  if (cfg.provider === 'supabase') {
    await ensureCloudBucket();
    const safeUser = Buffer.from(nickLower, 'utf8').toString('hex').slice(0, 48);
    const objKey = `u/${safeUser}.${parsed.ext}`;
    const up = await fetch(`${cfg.sbUrl}/storage/v1/object/${encodeURIComponent(cfg.bucket)}/${objKey}`, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + cfg.sbKey,
        'apikey': cfg.sbKey,
        'Content-Type': parsed.mime,
        'x-upsert': 'true',
        'Cache-Control': '3600'
      },
      body: parsed.buf,
      signal: AbortSignal.timeout(12000)
    });
    if (!up.ok) {
      const errTxt = await up.text().catch(() => '');
      throw new Error('Supabase Storage HTTP ' + up.status + (errTxt ? ': ' + errTxt.slice(0, 120) : ''));
    }
    return { ok: true, url: `${cfg.sbUrl}/storage/v1/object/public/${cfg.bucket}/${objKey}?v=${Date.now()}` };
  }

  if (cfg.provider === 'r2') {
    const safeUser = Buffer.from(nickLower, 'utf8').toString('hex').slice(0, 48);
    const objKey = `u/${safeUser}.${parsed.ext}`;
    const url = await uploadToR2(cfg, objKey, parsed.buf, parsed.mime);
    return { ok: true, url };
  }

  /* Если облако ещё не настроено: реальные картинки НЕ сохраняем ни на диск, ни в БД!
     Пропускаем только крошечный 1x1 маркер (<100 симв.) для автономных регрессионных тестов. */
  if (val.length < 100) return { ok: true, url: val };
  return {
    ok: false,
    code: 'STORAGE_NOT_CONFIGURED',
    error: 'Для загрузки аватарок подключите облачное хранилище (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY или Cloudflare R2)'
  };
}

function posterUrlClean(v) {
  const s = String(v == null ? '' : v).trim();
  if (!s) return '';
  if (/^https?:\/\/[^\s"'<>]{6,500}$/i.test(s)) return s;
  if (s.startsWith('data:image/') && s.length <= 80) return s;
  return '';
}

/* --- 3. Пул внешней PostgreSQL БД (Supabase / Neon.tech) + авто-миграция таблиц и триггера --- */
const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS users (
  nick_lower TEXT PRIMARY KEY,
  nick       TEXT NOT NULL,
  email      TEXT NOT NULL DEFAULT '',
  salt       TEXT NOT NULL,
  pass_hash  TEXT NOT NULL,
  avatar_url TEXT NOT NULL DEFAULT '',
  tokens     JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users (LOWER(email)) WHERE email <> '';

CREATE TABLE IF NOT EXISTS comments (
  id          TEXT PRIMARY KEY,
  title_id    TEXT NOT NULL,
  uid         TEXT NOT NULL,
  author_name TEXT NOT NULL,
  body        TEXT NOT NULL,
  parent_id   TEXT,
  parent_name TEXT NOT NULL DEFAULT '',
  likes       JSONB NOT NULL DEFAULT '[]'::jsonb,
  dislikes    JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_comments_title_created ON comments (title_id, created_at ASC);

CREATE TABLE IF NOT EXISTS history (
  id          BIGSERIAL PRIMARY KEY,
  nick_lower  TEXT NOT NULL,
  title_id    TEXT NOT NULL,
  ep          TEXT NOT NULL,
  ep_num      INTEGER NOT NULL DEFAULT 1,
  title       TEXT NOT NULL DEFAULT '',
  poster_url  TEXT NOT NULL DEFAULT '',
  ep_title    TEXT NOT NULL DEFAULT '',
  pos         DOUBLE PRECISION NOT NULL DEFAULT 0,
  dur         DOUBLE PRECISION NOT NULL DEFAULT 0,
  done        BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_user_updated ON history (nick_lower, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_history_user_title ON history (nick_lower, title_id);

CREATE OR REPLACE FUNCTION fn_history_keep_latest_ep()
RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM history
  WHERE nick_lower = NEW.nick_lower
    AND title_id = NEW.title_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_history_keep_latest_ep ON history;
CREATE TRIGGER trg_history_keep_latest_ep
BEFORE INSERT ON history
FOR EACH ROW
EXECUTE FUNCTION fn_history_keep_latest_ep();

CREATE TABLE IF NOT EXISTS user_profiles (
  nick_lower TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  lists      JSONB NOT NULL DEFAULT '{}'::jsonb,
  pub        BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS friends (
  pair_key   TEXT PRIMARY KEY,
  user_a     TEXT NOT NULL,
  user_b     TEXT NOT NULL,
  nick_a     TEXT NOT NULL,
  nick_b     TEXT NOT NULL,
  status     TEXT NOT NULL,
  requester  TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_friends_user_a ON friends (user_a);
CREATE INDEX IF NOT EXISTS idx_friends_user_b ON friends (user_b);

CREATE TABLE IF NOT EXISTS dms (
  pair_key   TEXT PRIMARY KEY,
  rev        INTEGER NOT NULL DEFAULT 0,
  msgs       JSONB NOT NULL DEFAULT '[]'::jsonb,
  read_at    JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS ovr_timings (
  ep_key     TEXT PRIMARY KEY,
  intro      JSONB,
  outro      JSONB,
  author     TEXT NOT NULL DEFAULT '',
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS penalties (
  actor_key  TEXT PRIMARY KEY,
  data       JSONB NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS moderation_kv (
  k          TEXT PRIMARY KEY,
  v          JSONB NOT NULL,
  updated_at BIGINT NOT NULL
);
`;

function getDbUrl() {
  return String(
    process.env.DATABASE_URL ||
    process.env.POSTGRES_URL ||
    process.env.SUPABASE_DB_URL ||
    process.env.NEON_DATABASE_URL ||
    ''
  ).trim();
}

let pgPool = null;
let dbInitPromise = null;
let dbInitError = '';
let dbConnectedUrl = '';

function buildPgPool(connStr) {
  const isLocal = /@(127\.0\.0\.1|localhost)(:|\/|$)/i.test(connStr) || /sslmode=disable/i.test(connStr);
  return new Pool({
    connectionString: connStr,
    ssl: isLocal ? false : { rejectUnauthorized: false },
    max: 6,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  });
}

function buildSupabaseRpcPool(sbUrl, sbKey) {
  const endpoint = sbUrl.replace(/\/+$/, '') + '/rest/v1/rpc/anihub_srv_query';
  return {
    isRpc: true,
    on() {},
    async end() {},
    async query(sql, params = []) {
      const safeParams = Array.isArray(params) ? params.map(v => v === undefined ? null : v) : [];
      const r = await fetch(endpoint, {
        method: 'POST',
        headers: {
          apikey: sbKey,
          Authorization: 'Bearer ' + sbKey,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ q: String(sql), p: safeParams })
      });
      const txt = await r.text().catch(() => '');
      if (!r.ok) throw new Error('Supabase RPC ' + r.status + ': ' + txt.slice(0, 200));
      const rows = txt ? JSON.parse(txt) : [];
      return { rows: Array.isArray(rows) ? rows : [] };
    }
  };
}

function hasDbConfig() {
  if (getDbUrl()) return true;
  return getStorageCfg().provider === 'supabase';
}

async function initExternalDb() {
  const url = getDbUrl();
  const st = getStorageCfg();
  const connKey = url ? ('pg:' + url) : (st.provider === 'supabase' ? ('sbrpc:' + st.sbUrl) : '');
  if (!connKey) {
    dbInitError = 'DATABASE_URL / SUPABASE_URL не заданы';
    return false;
  }
  if (pgPool && dbConnectedUrl === connKey && dbInitPromise) return dbInitPromise;
  dbConnectedUrl = connKey;
  if (pgPool) { try { await pgPool.end(); } catch {} pgPool = null; }
  pgPool = url ? buildPgPool(url) : buildSupabaseRpcPool(st.sbUrl, st.sbKey);
  pgPool.on('error', err => console.warn('[PG-Pool] idle error:', err && err.message));
  dbInitPromise = (async () => {
    try {
      if (url) {
        await pgPool.query(SCHEMA_SQL);
      } else {
        await pgPool.query('SELECT nick_lower FROM users LIMIT 1');
      }
      dbInitError = '';
      await loadModerationCacheFromDb();
      if (st.provider === 'supabase') ensureCloudBucket().catch(() => {});
      console.log('[DB] External PostgreSQL ready via', url ? 'TCP pg.Pool' : 'Supabase HTTPS RPC');
      return true;
    } catch (e) {
      dbInitError = String((e && e.message) || e);
      console.error('[DB] Init failed:', dbInitError);
      dbInitPromise = null;
      return false;
    }
  })();
  return dbInitPromise;
}

initExternalDb().catch(() => {});

async function requireDb(res) {
  const ok = await initExternalDb();
  if (!ok || !pgPool) {
    res.status(503).json({
      ok: false,
      code: 'DB_NOT_CONFIGURED',
      error: 'Внешняя база данных не подключена. Укажите DATABASE_URL или SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY в переменных окружения сервера.',
      detail: dbInitError || undefined,
      serverNow: Date.now()
    });
    return false;
  }
  return true;
}

app.get('/api/db/status', async (req, res) => {
  const configured = hasDbConfig();
  const ok = configured ? await initExternalDb() : false;
  const stCfg = getStorageCfg();
  res.json({
    ok,
    dbConfigured: configured,
    dbMode: getDbUrl() ? 'pg_pool' : (stCfg.provider === 'supabase' ? 'supabase_rpc' : 'none'),
    dbReady: ok,
    dbError: ok ? null : (dbInitError || 'DATABASE_URL not set'),
    storageProvider: stCfg.provider,
    bucket: stCfg.bucket || null,
    diskPersistence: false,
    lastPurge: lastPurgeStats,
    serverNow: Date.now()
  });
});

app.post('/api/db/cleanup', (req, res) => {
  const stats = purgeRenderDisk();
  _kCache.clear();
  res.json({ ok: true, diskPersistence: false, purged: stats, serverNow: Date.now() });
});

/* ==================== Общие валидаторы и хэши ==================== */
const tidOk = t => String(t || '').replace(/[^A-Za-z0-9_:\-]/g, '').slice(0, 64);
const cmtClean = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').trim().slice(0, n);
const sha256 = s => crypto.createHash('sha256').update('anihub:' + s).digest('hex');
const NICK_RE = /^[A-Za-zА-Яа-яЁё0-9_\- ]{2,24}$/;
const nickClean = v => String(v == null ? '' : v).replace(/[\u0000-\u001f]/g, '').trim().slice(0, 24);
const scryptHash = (pass, salt) => crypto.scryptSync(String(pass), salt, 32).toString('hex');
const newSalt = () => crypto.randomBytes(8).toString('hex');
const newToken = () => crypto.randomBytes(24).toString('hex');
const emailOk = v => typeof v === 'string' && v.length < 190 && /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,24}$/.test(v.trim()) ? v.trim().toLowerCase() : '';

const tokenNickCache = new Map();

function rowToUser(row) {
  if (!row) return null;
  return {
    nickLower: row.nick_lower,
    nick: row.nick,
    email: row.email || '',
    salt: row.salt,
    hash: row.pass_hash,
    avatar: row.avatar_url || '',
    tokens: (row.tokens && typeof row.tokens === 'object') ? row.tokens : {},
    at: Number(row.updated_at) || 0
  };
}

function syncTokenCache(u) {
  if (!u || !u.tokens) return;
  for (const tk of Object.keys(u.tokens)) tokenNickCache.set(tk, { nick: u.nick, nickLower: u.nickLower });
}

async function dbGetUserByNick(nickOrLower) {
  if (!pgPool) return null;
  const k = nickClean(nickOrLower).toLowerCase();
  if (!k) return null;
  const r = await pgPool.query('SELECT * FROM users WHERE nick_lower = $1 LIMIT 1', [k]);
  const u = rowToUser(r.rows[0]);
  if (u) syncTokenCache(u);
  return u;
}

async function dbGetUserByLogin(loginStr) {
  if (!pgPool) return null;
  const lg = String(loginStr == null ? '' : loginStr).trim().slice(0, 190);
  if (!lg) return null;
  if (lg.includes('@')) {
    const r = await pgPool.query('SELECT * FROM users WHERE LOWER(email) = $1 LIMIT 1', [lg.toLowerCase()]);
    const u = rowToUser(r.rows[0]);
    if (u) syncTokenCache(u);
    return u;
  }
  return dbGetUserByNick(lg);
}

async function dbGetUserByToken(tok) {
  if (!pgPool || !tok || typeof tok !== 'string' || tok.length < 12) return null;
  const r = await pgPool.query('SELECT * FROM users WHERE tokens ? $1 LIMIT 1', [tok]);
  const u = rowToUser(r.rows[0]);
  if (u) syncTokenCache(u);
  return u;
}

async function dbSaveUser(u) {
  u.at = Date.now();
  await pgPool.query(
    `INSERT INTO users (nick_lower, nick, email, salt, pass_hash, avatar_url, tokens, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
     ON CONFLICT (nick_lower) DO UPDATE SET
       nick = EXCLUDED.nick,
       email = EXCLUDED.email,
       salt = EXCLUDED.salt,
       pass_hash = EXCLUDED.pass_hash,
       avatar_url = EXCLUDED.avatar_url,
       tokens = EXCLUDED.tokens,
       updated_at = EXCLUDED.updated_at`,
    [u.nickLower, u.nick, u.email || '', u.salt, u.hash, u.avatar || '', JSON.stringify(u.tokens || {}), u.at]
  );
  syncTokenCache(u);
}

function passVerify(u, pass) {
  if (u.salt && u.hash) {
    try { return crypto.timingSafeEqual(Buffer.from(u.hash, 'hex'), Buffer.from(scryptHash(pass, u.salt), 'hex')); }
    catch { return false; }
  }
  if (u.salt === '' && u.hash) {
    const ok = u.hash === sha256(pass);
    if (ok) { u.salt = newSalt(); u.hash = scryptHash(pass, u.salt); }
    return ok;
  }
  return false;
}

function tokenIssue(u) {
  const t = newToken();
  u.tokens = u.tokens || {};
  u.tokens[t] = { at: Date.now() };
  const ks = Object.keys(u.tokens);
  if (ks.length > 10) {
    ks.sort((a, b) => (u.tokens[a].at || 0) - (u.tokens[b].at || 0))
      .slice(0, ks.length - 10)
      .forEach(k => { delete u.tokens[k]; tokenNickCache.delete(k); });
  }
  tokenNickCache.set(t, { nick: u.nick, nickLower: u.nickLower });
  return t;
}

const pubUser = u => ({ nick: u.nick, email: u.email || '', avatar: u.avatar || '' });

async function authFromReqAsync(req) {
  const b = (req && req.body) || {};
  const q = (req && req.query) || {};
  const hdr = String((req && req.headers && req.headers.authorization) || '').replace(/^Bearer\s+/i, '').trim();
  const tok = b.token || q.token || hdr || '';
  return dbGetUserByToken(tok);
}

/* ==================== комментарии к тайтлам (таблица comments в БД) ==================== */
const cmtWaiters = new Map();
const cmtRevs = new Map();

function cmtNotify(tid) {
  const set = cmtWaiters.get(tid);
  if (set) { const fns = [...set]; set.clear(); for (const fn of fns) { try { fn(); } catch {} } }
}

async function dbLoadComments(tid) {
  const r = await pgPool.query(
    `SELECT id, uid, author_name, body, parent_id, parent_name, likes, dislikes, created_at
     FROM comments
     WHERE title_id = $1
     ORDER BY created_at ASC
     LIMIT 400`,
    [tid]
  );
  const list = r.rows.map(row => ({
    id: row.id,
    uid: row.uid,
    name: row.author_name,
    text: row.body,
    at: Number(row.created_at) || 0,
    parent: row.parent_id || null,
    parentName: row.parent_name || '',
    likes: Array.isArray(row.likes) ? row.likes : [],
    dislikes: Array.isArray(row.dislikes) ? row.dislikes : []
  }));
  if (!cmtRevs.has(tid)) cmtRevs.set(tid, list.length);
  return { rev: cmtRevs.get(tid) || 0, list };
}

function bumpCmtRev(tid, baseLen = 0) {
  const next = (cmtRevs.has(tid) ? cmtRevs.get(tid) : baseLen) + 1;
  cmtRevs.set(tid, next);
  return next;
}

app.get('/api/comments/:tid', async (req, res) => {
  const tid = tidOk(req.params.tid);
  if (!tid) return res.status(400).json({ error: 'плохой id тайтла' });
  if (!(await requireDb(res))) return;
  try {
    const sendLatest = async () => {
      const s = await dbLoadComments(tid);
      if (!res.headersSent) res.json({ rev: s.rev, list: s.list, serverNow: Date.now() });
    };
    if (req.query.since === undefined) return await sendLatest();
    const since = parseInt(req.query.since, 10) || 0;
    const cur = await dbLoadComments(tid);
    if (cur.rev > since) return res.json({ rev: cur.rev, list: cur.list, serverNow: Date.now() });

    const set = cmtWaiters.get(tid) || new Set();
    cmtWaiters.set(tid, set);
    let fired = false;
    const fire = () => {
      if (fired) return;
      fired = true;
      set.delete(fire);
      sendLatest().catch(() => { if (!res.headersSent) res.status(500).json({ error: 'ошибка чтения БД' }); });
    };
    set.add(fire);
    setTimeout(fire, 20000);
  } catch (e) {
    if (!res.headersSent) res.status(500).json({ error: 'БД comments: ' + e.message });
  }
});

app.post('/api/comments/:tid', async (req, res) => {
  const tid = tidOk(req.params.tid);
  if (!tid) return res.status(400).json({ error: 'плохой id тайтла' });
  if (!(await requireDb(res))) return;
  const b = req.body || {};
  const act = b.act;
  const uid = cmtClean(b.uid, 40) || '?';
  try {
    if (!cmtRevs.has(tid)) await dbLoadComments(tid);

    if (act === 'add') {
      const text = cmtClean(b.text, 1200);
      if (!text) return res.status(400).json({ error: 'пустой комментарий', serverNow: Date.now() });
      const now = Date.now();
      const tokUser = b.token ? await dbGetUserByToken(b.token) : null;
      const authorName = (tokUser && tokUser.nick) || cmtClean(b.name, 24) || 'Гость';
      const pen0 = getPenRecord(b.token, authorName, uid, false);
      if (pen0) {
        if (pen0.permBan)
          return res.status(403).json({ error: 'Чат и комментарии заблокированы навсегда без права обжалования', code: 'PERM_BAN', serverNow: now, mod: pubPen(pen0) });
        if ((pen0.tempBanUntil || 0) > now)
          return res.status(403).json({ error: 'Чат и комментарии временно заблокированы', code: 'TEMP_BAN', serverNow: now, mod: pubPen(pen0) });
        if ((pen0.mutedUntil || 0) > now)
          return res.status(403).json({ error: 'Вы в муте ещё ' + Math.ceil((pen0.mutedUntil - now) / 1000) + ' сек', code: 'MUTED', serverNow: now, mod: pubPen(pen0) });
      }
      if (hasLink(text)) {
        const stLink = recordStrike({ token: b.token, nick: authorName, uid, kind: 'link', where: 'cmt:' + tid });
        return res.status(403).json({
          error: stLink.permBan
            ? 'Ссылка удалена: аккаунт заблокирован навсегда за систематические нарушения'
            : stLink.tempBanned
              ? 'Ссылка удалена: 5+ нарушений за 7 дней — временная блокировка чата и комментариев'
              : 'Ссылки запрещены: сообщение мгновенно удалено, выдан мут на 5 минут',
          code: stLink.permBan ? 'PERM_BAN' : (stLink.tempBanned ? 'TEMP_BAN' : 'LINK_MUTE'),
          deleted: true, serverNow: stLink.serverNow, mod: stLink
        });
      }
      if (isDirty(text)) {
        const stDirty = recordStrike({ token: b.token, nick: authorName, uid, kind: 'profanity', where: 'cmt:' + tid });
        return res.status(403).json({
          error: stDirty.permBan
            ? 'Сообщение удалено: аккаунт заблокирован навсегда'
            : stDirty.tempBanned
              ? 'Сообщение удалено: временная блокировка чата и комментариев'
              : 'Нецензурная лексика: сообщение удалено, выдан мут на 5 минут',
          code: stDirty.permBan ? 'PERM_BAN' : (stDirty.tempBanned ? 'TEMP_BAN' : 'DIRTY_MUTE'),
          deleted: true, serverNow: stDirty.serverNow, mod: stDirty
        });
      }
      let parent = null, parentName = '';
      if (b.parent) {
        const pid = String(b.parent).slice(0, 24);
        const pr = await pgPool.query('SELECT id, parent_id, author_name FROM comments WHERE id = $1 AND title_id = $2 LIMIT 1', [pid, tid]);
        const p = pr.rows[0];
        if (!p) return res.status(404).json({ error: 'комментарий для ответа не найден' });
        parent = p.parent_id || p.id;
        if (parent !== p.id) {
          const ppr = await pgPool.query('SELECT author_name FROM comments WHERE id = $1 AND title_id = $2 LIMIT 1', [parent, tid]);
          parentName = cmtClean((ppr.rows[0] && ppr.rows[0].author_name) || p.author_name, 24) || '?';
        } else {
          parentName = cmtClean(p.author_name, 24) || '?';
        }
      }
      const cid = Math.random().toString(36).slice(2, 10) + now.toString(36).slice(-4);
      await pgPool.query(
        `INSERT INTO comments (id, title_id, uid, author_name, body, parent_id, parent_name, likes, dislikes, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, '[]'::jsonb, '[]'::jsonb, $8)`,
        [cid, tid, uid, authorName, text, parent, parentName, now]
      );
      pgPool.query(
        `DELETE FROM comments WHERE title_id = $1 AND id NOT IN (
           SELECT id FROM comments WHERE title_id = $1 ORDER BY created_at DESC LIMIT 450
         )`,
        [tid]
      ).catch(() => {});
      const rev = bumpCmtRev(tid);
      cmtNotify(tid);
      return res.json({ rev, id: cid, serverNow: Date.now() });
    }

    if (act === 'vote') {
      const cid = String(b.cid || '').slice(0, 24);
      const cr = await pgPool.query('SELECT id, likes, dislikes FROM comments WHERE id = $1 AND title_id = $2 LIMIT 1', [cid, tid]);
      const c = cr.rows[0];
      if (!c) return res.status(404).json({ error: 'комментарий не найден' });
      const v = (b.v === 1 || b.v === '1') ? 1 : (b.v === -1 || b.v === '-1') ? -1 : 0;
      const likes = (Array.isArray(c.likes) ? c.likes : []).filter(u => u !== uid);
      const dislikes = (Array.isArray(c.dislikes) ? c.dislikes : []).filter(u => u !== uid);
      if (v === 1) likes.push(uid); else if (v === -1) dislikes.push(uid);
      await pgPool.query(
        'UPDATE comments SET likes = $3::jsonb, dislikes = $4::jsonb WHERE id = $1 AND title_id = $2',
        [cid, tid, JSON.stringify(likes), JSON.stringify(dislikes)]
      );
      const rev = bumpCmtRev(tid);
      cmtNotify(tid);
      return res.json({ rev });
    }

    if (act === 'del') {
      const cid = String(b.cid || '').slice(0, 24);
      const cr = await pgPool.query('SELECT id, uid FROM comments WHERE id = $1 AND title_id = $2 LIMIT 1', [cid, tid]);
      const c = cr.rows[0];
      if (!c) return res.status(404).json({ error: 'комментарий не найден' });
      if (c.uid !== uid) return res.status(403).json({ error: 'можно удалять только свои комментарии' });
      await pgPool.query('DELETE FROM comments WHERE title_id = $1 AND (id = $2 OR parent_id = $2)', [tid, cid]);
      const rev = bumpCmtRev(tid);
      cmtNotify(tid);
      return res.json({ rev });
    }

    res.status(400).json({ error: 'неизвестное действие' });
  } catch (e) {
    res.status(500).json({ error: 'БД comments: ' + e.message });
  }
});

/* ==================== таймкоды опенинга/эндинга (таблица ovr_timings в БД) ==================== */
const seg2 = v => (Array.isArray(v) && v.length === 2 && +v[0] >= 0 && +v[1] > +v[0] && +v[1] - +v[0] <= 600 && +v[1] <= 7200) ? [+v[0], +v[1]] : null;

app.get('/api/ovr/:tid/:ep', async (req, res) => {
  if (!(await requireDb(res))) return;
  const k = tidOk(req.params.tid) + ':' + (parseInt(req.params.ep, 10) || 1);
  try {
    const r = await pgPool.query('SELECT intro, outro, author, updated_at FROM ovr_timings WHERE ep_key = $1 LIMIT 1', [k]);
    const o = r.rows[0];
    res.json(o ? { intro: o.intro || null, outro: o.outro || null, by: o.author || '?', at: Number(o.updated_at) || 0 } : { intro: null, outro: null });
  } catch (e) {
    res.status(500).json({ error: 'БД ovr: ' + e.message });
  }
});

app.post('/api/ovr/:tid/:ep', async (req, res) => {
  if (!(await requireDb(res))) return;
  const k = tidOk(req.params.tid) + ':' + (parseInt(req.params.ep, 10) || 1);
  const b = req.body || {};
  const intro = seg2(b.intro), outro = seg2(b.outro);
  if (!intro && !outro) return res.status(400).json({ error: 'нет валидных диапазонов' });
  try {
    const prev = (await pgPool.query('SELECT intro, outro FROM ovr_timings WHERE ep_key = $1 LIMIT 1', [k])).rows[0] || {};
    const nextIntro = intro || prev.intro || null;
    const nextOutro = outro || prev.outro || null;
    const author = cmtClean(b.name, 24) || '?';
    const now = Date.now();
    await pgPool.query(
      `INSERT INTO ovr_timings (ep_key, intro, outro, author, updated_at)
       VALUES ($1, $2::jsonb, $3::jsonb, $4, $5)
       ON CONFLICT (ep_key) DO UPDATE SET
         intro = EXCLUDED.intro,
         outro = EXCLUDED.outro,
         author = EXCLUDED.author,
         updated_at = EXCLUDED.updated_at`,
      [k, JSON.stringify(nextIntro), JSON.stringify(nextOutro), author, now]
    );
    res.json({ ok: true, intro: nextIntro, outro: nextOutro });
  } catch (e) {
    res.status(500).json({ error: 'БД ovr: ' + e.message });
  }
});

/* ==================== модерация, жалобы и анти-спам (таблицы penalties + moderation_kv в БД) ==================== */
const SROOTS = [/пизд/, /бля[дт]/, /хуй/, /ху[яеёю]/, /(^|[^а-я])еб[ауиыё]/, /(за|у|по|вы|на|до|от|раз|об|о|пере|под|въ)е[бб]/, /аху[еёи]/, /оху[еёи]/, /пид[оа][рб]/, /муд[ао]к/, /ганд[оа]н/, /шлюх/, /ублюд/, /манда/, /педик(?!юр)/];
const SBAD = new Set(['сука', 'суки', 'сучка', 'бля', 'блять', 'мудак', 'мудила', 'мудаки', 'гандон', 'чмо', 'пидор', 'пидар', 'пидорас', 'долбоёб', 'долбоеб', 'шлюха', 'шлюхи', 'говнюк', 'мразь', 'мразота', 'педик']);
function isDirty(t) {
  const ws = String(t == null ? '' : t).toLowerCase().replace(/ё/g, 'е').split(/[^a-z0-9а-я]+/).filter(Boolean);
  const flat = ws.join('');
  for (const r of SROOTS) { r.lastIndex = 0; if (r.test(flat)) return true; }
  for (const w of ws) if (SBAD.has(w)) return true;
  return false;
}

const REPORTS = [];
const MODLOG = [];
const repPairs = new Map();
const repStats = new Map();
const sanctions = new Map();
const penaltiesState = new Map();
const strikeDedup = new Map();

async function kvSaveToDb(key, val) {
  if (!pgPool) return;
  try {
    await pgPool.query(
      `INSERT INTO moderation_kv (k, v, updated_at) VALUES ($1, $2::jsonb, $3)
       ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = EXCLUDED.updated_at`,
      [key, JSON.stringify(val), Date.now()]
    );
  } catch {}
}

let repSaveT = null, modSaveT = null, modDataSaveT = null;
const repSave = () => { clearTimeout(repSaveT); repSaveT = setTimeout(() => kvSaveToDb('reports', REPORTS.slice(-500)), 800); };
const modSave = () => { clearTimeout(modSaveT); modSaveT = setTimeout(() => kvSaveToDb('modlog', MODLOG.slice(-500)), 800); };
const modDataSave = () => {
  clearTimeout(modDataSaveT);
  modDataSaveT = setTimeout(() => kvSaveToDb('moderation', {
    pairs: Object.fromEntries(repPairs),
    stats: Object.fromEntries(repStats),
    sanc: Object.fromEntries(sanctions)
  }), 800);
};
const modLog = e => {
  MODLOG.push(Object.assign({ at: Date.now() }, e));
  if (MODLOG.length > 500) MODLOG.splice(0, MODLOG.length - 500);
  modSave();
};

async function loadModerationCacheFromDb() {
  if (!pgPool) return;
  try {
    const kvRes = await pgPool.query('SELECT k, v FROM moderation_kv');
    for (const row of kvRes.rows) {
      if (row.k === 'reports' && Array.isArray(row.v)) {
        REPORTS.length = 0;
        REPORTS.push(...row.v.slice(-500));
      } else if (row.k === 'modlog' && Array.isArray(row.v)) {
        MODLOG.length = 0;
        MODLOG.push(...row.v.slice(-500));
      } else if (row.k === 'moderation' && row.v && typeof row.v === 'object') {
        repPairs.clear(); repStats.clear(); sanctions.clear();
        for (const [k, v] of Object.entries(row.v.pairs || {})) repPairs.set(k, v);
        for (const [k, v] of Object.entries(row.v.stats || {})) repStats.set(k, v);
        for (const [k, v] of Object.entries(row.v.sanc || {})) sanctions.set(k, v);
      }
    }
    const penRes = await pgPool.query('SELECT actor_key, data FROM penalties');
    penaltiesState.clear();
    for (const row of penRes.rows) {
      if (row.actor_key && row.data) penaltiesState.set(row.actor_key, row.data);
    }
  } catch {}
}

function penPersistKeys(keys, rec) {
  if (!pgPool || !rec) return;
  const now = Date.now();
  for (const k of keys) {
    pgPool.query(
      `INSERT INTO penalties (actor_key, data, updated_at) VALUES ($1, $2::jsonb, $3)
       ON CONFLICT (actor_key) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at`,
      [k, JSON.stringify(rec), now]
    ).catch(() => {});
  }
}

app.post('/api/reports', async (req, res) => {
  if (!(await requireDb(res))) return;
  const b = req.body || {};
  const type = ['room', 'user', 'comment', 'message'].includes(b.type) ? b.type : 'room';
  const target = cmtClean(b.target, 60);
  const REASONS = ['Оскорбления', 'Спам', 'Реклама', '18+ контент', 'Нарушение авторских прав', 'Другое'];
  if (!target) return res.status(400).json({ error: 'нет цели жалобы' });
  const byL = String(cmtClean(b.by, 24) || 'гость').toLowerCase();
  const st0 = repStats.get(byL);
  if (st0 && st0.bannedUntil > Date.now()) {
    return res.status(403).json({ error: 'Отправка жалоб заблокирована ещё ' + Math.ceil((st0.bannedUntil - Date.now()) / 3600e3) + ' ч: слишком много необоснованных жалоб', banned: true });
  }
  const targetL = target.toLowerCase();
  const pairKey = byL + '|' + targetL;
  const last = repPairs.get(pairKey) || 0;
  if (Date.now() - last < 86400e3) return res.status(429).json({ error: 'На это вы уже жаловались за сутки — повторная жалоба не отправлена' });
  repPairs.set(pairKey, Date.now());
  const reason = REASONS.includes(b.reason) ? b.reason : 'Другое';
  const justified = (typeof b.evidence === 'string' && b.evidence.trim()) ? isDirty(b.evidence) : null;
  if (justified === true) {
    const s2 = sanctions.get(targetL) || { just: 0 };
    s2.just++;
    sanctions.set(targetL, s2);
    modLog({ kind: 'auto_mute', who: targetL, note: 'обоснованная жалоба (' + reason + ') — счёт: ' + s2.just });
  }
  if (justified === false) {
    const st2 = repStats.get(byL) || { bad: 0 };
    st2.bad++;
    if (st2.bad > 15) {
      const days = st2.lastBan ? 14 : 2;
      st2.lastBan = days;
      st2.bannedUntil = Date.now() + days * 86400e3;
      modLog({ kind: 'ban_reports', who: byL, note: 'бан отправки жалоб на ' + days + ' дней: 15+ необоснованных жалоб' });
    }
    repStats.set(byL, st2);
  }
  REPORTS.push({ type, target, reason, details: cmtClean(b.details, 300), by: cmtClean(b.by, 24) || 'Гость', at: Date.now(), justified });
  if (REPORTS.length > 500) REPORTS.splice(0, REPORTS.length - 500);
  repSave();
  modDataSave();
  modLog({ kind: 'report', who: byL, note: target + ' · ' + reason + (justified === true ? ' · обоснована' : justified === false ? ' · не обоснована' : '') });
  res.json({ ok: true, justified });
});

app.get('/api/modlog', async (req, res) => {
  if (!(await requireDb(res))) return;
  res.json({ count: MODLOG.length, list: MODLOG.slice(-100) });
});

app.get('/api/reports', async (req, res) => {
  if (!(await requireDb(res))) return;
  const t = cmtClean(req.query.target, 40);
  const list = t ? REPORTS.filter(r => r.target === t) : REPORTS;
  res.json({ count: list.length, list: list.slice(-20) });
});

/* ==================== таблица history (БД-дедупликация старых серий) + профили ==================== */
const LIST_KEYS = ['favorite', 'watching', 'planned', 'dropped'];

function parseEpNum(epVal) {
  const s = String(epVal == null ? '1' : epVal).trim();
  if (/^\d+x(\d+)$/i.test(s)) return parseInt(RegExp.$1, 10) || 1;
  return parseInt(s, 10) || 1;
}

function cleanHistInput(h) {
  if (!h || h.id == null) return null;
  const titleId = String(h.id).trim().slice(0, 48);
  if (!titleId) return null;
  const ep = String(h.ep == null ? '1' : h.ep).trim().slice(0, 24) || '1';
  return {
    titleId,
    ep,
    epNum: parseEpNum(ep),
    title: cmtClean(h.title, 160),
    posterUrl: posterUrlClean(h.poster),
    epTitle: cmtClean(h.epTitle, 160),
    pos: Math.max(0, +h.pos || 0),
    dur: Math.max(0, +h.dur || 0),
    done: !!h.done,
    at: Math.max(0, +h.at || Date.now())
  };
}

function rowToHistItem(row) {
  const rawId = String(row.title_id || '');
  const rawEp = String(row.ep || '1');
  return {
    id: /^\d+$/.test(rawId) ? Number(rawId) : rawId,
    ep: /^\d+$/.test(rawEp) ? Number(rawEp) : rawEp,
    title: row.title || '',
    poster: row.poster_url || '',
    epTitle: row.ep_title || '',
    pos: Number(row.pos) || 0,
    dur: Number(row.dur) || 0,
    done: !!row.done,
    at: Number(row.updated_at) || 0
  };
}

async function dbUpsertHistoryItem(nickLower, item) {
  const c = cleanHistInput(item);
  if (!c || !nickLower) return null;
  const r = await pgPool.query(
    `INSERT INTO history (nick_lower, title_id, ep, ep_num, title, poster_url, ep_title, pos, dur, done, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
     RETURNING title_id, ep, title, poster_url, ep_title, pos, dur, done, updated_at`,
    [nickLower, c.titleId, c.ep, c.epNum, c.title, c.posterUrl, c.epTitle, c.pos, c.dur, c.done, c.at]
  );
  pgPool.query(
    `DELETE FROM history WHERE nick_lower = $1 AND id NOT IN (
       SELECT id FROM history WHERE nick_lower = $1 ORDER BY updated_at DESC LIMIT 80
     )`,
    [nickLower]
  ).catch(() => {});
  return r.rows[0] ? rowToHistItem(r.rows[0]) : null;
}

async function dbGetHistory(nickLower, limit = 60) {
  if (!nickLower) return [];
  const r = await pgPool.query(
    `SELECT title_id, ep, title, poster_url, ep_title, pos, dur, done, updated_at
     FROM history
     WHERE nick_lower = $1
     ORDER BY updated_at DESC
     LIMIT $2`,
    [nickLower, Math.min(100, Math.max(1, +limit || 60))]
  );
  return r.rows.map(rowToHistItem);
}

async function dbSyncHistoryBatch(nickLower, rawList) {
  if (!Array.isArray(rawList) || !rawList.length) return;
  const latestPerTitle = new Map();
  for (const raw of rawList.slice(0, 100)) {
    const c = cleanHistInput(raw);
    if (!c) continue;
    const prev = latestPerTitle.get(c.titleId);
    if (!prev || c.at >= prev.at) latestPerTitle.set(c.titleId, raw);
  }
  for (const item of latestPerTitle.values()) {
    await dbUpsertHistoryItem(nickLower, item);
  }
}

app.get('/api/history', async (req, res) => {
  if (!(await requireDb(res))) return;
  try {
    const me = await authFromReqAsync(req);
    const qNick = nickClean(req.query.nick || '');
    const nickLower = me ? me.nickLower : qNick.toLowerCase();
    if (!nickLower) return res.status(400).json({ ok: false, error: 'укажите token или nick' });
    if (!me) {
      const pr = await pgPool.query('SELECT pub FROM user_profiles WHERE nick_lower = $1 LIMIT 1', [nickLower]);
      if (pr.rows[0] && pr.rows[0].pub === false) {
        return res.status(403).json({ ok: false, error: 'история скрыта владельцем' });
      }
    }
    const items = await dbGetHistory(nickLower, 60);
    res.json({ ok: true, nick: me ? me.nick : qNick, count: items.length, items, serverNow: Date.now() });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'БД history: ' + e.message });
  }
});

app.post('/api/history', async (req, res) => {
  if (!(await requireDb(res))) return;
  try {
    const b = req.body || {};
    const me = await authFromReqAsync(req);
    const fallbackNick = nickClean(b.nick || '');
    if (!me && (!fallbackNick || !NICK_RE.test(fallbackNick) || /^гость$/i.test(fallbackNick))) {
      return res.status(401).json({ ok: false, error: 'войдите в аккаунт или укажите валидный ник' });
    }
    const nickLower = me ? me.nickLower : fallbackNick.toLowerCase();
    if (!me) {
      const existingAcc = await dbGetUserByNick(nickLower);
      if (existingAcc) return res.status(403).json({ ok: false, error: 'этот ник защищён паролем — передайте token' });
    }
    const act = String(b.act || 'put');
    if (act === 'put') {
      const saved = await dbUpsertHistoryItem(nickLower, b.item || b);
      if (!saved) return res.status(400).json({ ok: false, error: 'некорректные данные серии' });
      return res.json({ ok: true, item: saved, keptOnlyLatestEp: true, serverNow: Date.now() });
    }
    if (act === 'sync') {
      const arr = Array.isArray(b.items) ? b.items : (Array.isArray(b.hist) ? b.hist : []);
      await dbSyncHistoryBatch(nickLower, arr);
      const items = await dbGetHistory(nickLower, 60);
      return res.json({ ok: true, count: items.length, items, serverNow: Date.now() });
    }
    if (act === 'del') {
      const tid = String(b.id == null ? '' : b.id).trim().slice(0, 48);
      if (!tid) return res.status(400).json({ ok: false, error: 'нужен id тайтла' });
      await pgPool.query('DELETE FROM history WHERE nick_lower = $1 AND title_id = $2', [nickLower, tid]);
      return res.json({ ok: true, deletedId: tid, serverNow: Date.now() });
    }
    if (act === 'clear') {
      await pgPool.query('DELETE FROM history WHERE nick_lower = $1', [nickLower]);
      return res.json({ ok: true, cleared: true, serverNow: Date.now() });
    }
    res.status(400).json({ ok: false, error: 'неизвестное действие history' });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'БД history: ' + e.message });
  }
});

app.post('/api/profile/:name', async (req, res) => {
  if (!(await requireDb(res))) return;
  const name = cmtClean(req.params.name, 24);
  if (!NICK_RE.test(name)) return res.status(400).json({ error: 'плохой ник' });
  const b = req.body || {};
  const lists0 = (b.lists && typeof b.lists === 'object' && !Array.isArray(b.lists)) ? b.lists : {};
  const lists = {};
  for (const [k, v] of Object.entries(lists0).slice(0, 400)) {
    if (LIST_KEYS.includes(v)) lists[String(k).slice(0, 40)] = v;
  }
  const k = name.toLowerCase();
  const pub = b.pub !== false;
  const now = Date.now();
  try {
    await pgPool.query(
      `INSERT INTO user_profiles (nick_lower, name, lists, pub, updated_at)
       VALUES ($1, $2, $3::jsonb, $4, $5)
       ON CONFLICT (nick_lower) DO UPDATE SET
         name = EXCLUDED.name,
         lists = EXCLUDED.lists,
         pub = EXCLUDED.pub,
         updated_at = EXCLUDED.updated_at`,
      [k, name, JSON.stringify(lists), pub, now]
    );
    if (b.clearHist === true) {
      await pgPool.query('DELETE FROM history WHERE nick_lower = $1', [k]);
    } else if (Array.isArray(b.hist) && b.hist.length > 0) {
      await dbSyncHistoryBatch(k, b.hist);
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: 'БД profile: ' + e.message });
  }
});

app.get('/api/profile/:name', async (req, res) => {
  if (!(await requireDb(res))) return;
  const name = cmtClean(req.params.name, 24);
  const k = name.toLowerCase();
  try {
    const [profRes, accUser, viewer] = await Promise.all([
      pgPool.query('SELECT name, lists, pub, updated_at FROM user_profiles WHERE nick_lower = $1 LIMIT 1', [k]),
      dbGetUserByNick(k),
      authFromReqAsync(req)
    ]);
    const profRow = profRes.rows[0] || null;
    const pub = !profRow || profRow.pub !== false;
    const hist = pub ? await dbGetHistory(k, 60) : [];
    const reports = REPORTS.filter(r => r.type === 'user' && String(r.target || '').toLowerCase() === 'user:' + k);
    const summary = {};
    reports.forEach(r => { summary[r.reason] = (summary[r.reason] || 0) + 1; });
    const msgs = [];
    for (const [code2, rec] of roomsState) {
      const arr = (rec && rec.data && rec.data.msgs) || [];
      for (const m of arr) {
        if (!m || m.sys || m.del) continue;
        if (String(m.name || '').toLowerCase() === k) msgs.push({ room: code2, body: String(m.body || '').slice(0, 300), at: m.at });
      }
    }
    msgs.sort((a, b) => a.at - b.at);
    const status = [];
    for (const [code2, rec] of roomsState) {
      const mems = (rec && rec.data && rec.data.members) || {};
      for (const id2 of Object.keys(mems)) {
        const m2 = mems[id2];
        if (String((m2 && m2.name) || '').toLowerCase() !== k) continue;
        if (m2.banned) status.push({ room: code2, kind: 'ban' });
        else if ((m2.mutedUntil || 0) > Date.now()) status.push({ room: code2, kind: 'mute', until: m2.mutedUntil });
      }
    }
    const just = (sanctions.get('user:' + k) || {}).just || 0;
    const penProf = pubPen(getPenRecord(null, name, null, false));
    const friendship = await profileFriendshipAsync(viewer, k);
    const hasProf = !!(profRow || hist.length);
    res.json({
      ok: true,
      name: (accUser && accUser.nick) || (profRow && profRow.name) || name,
      serverNow: Date.now(),
      hasAccount: !!accUser,
      avatar: (accUser && accUser.avatar) || '',
      friendship,
      mod: penProf,
      prof: hasProf ? {
        name: (profRow && profRow.name) || name,
        hist: pub ? hist : [],
        lists: pub && profRow && profRow.lists ? profRow.lists : {},
        pub,
        at: profRow ? Number(profRow.updated_at) || 0 : Date.now()
      } : null,
      reports: { count: reports.length, list: reports.slice(-10), summary },
      msgs: msgs.slice(-30).reverse(),
      status,
      punished: just > 10 || penProf.permBan || penProf.tempBanned,
      just
    });
  } catch (e) {
    res.status(500).json({ error: 'БД profile: ' + e.message });
  }
});

/* ==================== пользователи / аккаунты (таблица users в БД + облачные аватарки) ==================== */
app.get('/api/user/:nick', async (req, res) => {
  if (!(await requireDb(res))) return;
  try {
    const u = await dbGetUserByNick(req.params.nick);
    res.json({ exists: !!u, nick: u ? u.nick : null, avatar: u ? (u.avatar || '') : '' });
  } catch (e) {
    res.status(500).json({ error: 'БД user: ' + e.message });
  }
});

app.post('/api/users', async (req, res) => {
  if (!(await requireDb(res))) return;
  const b = req.body || {};
  const act = b.act;
  const nick = nickClean(b.nick);
  const pass = String(b.pass == null ? '' : b.pass);
  try {
    if (act === 'claim') {
      if (!NICK_RE.test(nick) || /^гость$/i.test(nick))
        return res.status(400).json({ error: 'ник 2–24 символа: буквы, цифры, пробел, _ или -' });
      if (pass.length < 4) return res.status(400).json({ error: 'пароль минимум 4 символа' });
      const email = b.email != null && b.email !== '' ? emailOk(b.email) : '';
      if (b.email != null && b.email !== '' && !email) return res.status(400).json({ error: 'почта не похоже на почту' });
      const k = nick.toLowerCase();
      if (email) {
        const emRes = await pgPool.query('SELECT nick_lower FROM users WHERE LOWER(email) = $1 AND nick_lower <> $2 LIMIT 1', [email, k]);
        if (emRes.rows.length) return res.status(409).json({ error: 'почта уже привязана к другому нику' });
      }
      const u = await dbGetUserByNick(k);
      if (u) {
        if (!passVerify(u, pass)) return res.status(409).json({ error: 'ник уже занят' });
        if (email && !u.email) u.email = email;
        if (b.avatar && !u.avatar) {
          const avRes = await resolveCloudAvatar(k, b.avatar, u.avatar);
          if (avRes.ok && avRes.url) u.avatar = avRes.url;
        }
        const token = tokenIssue(u);
        await dbSaveUser(u);
        return res.json({ ok: true, nick: u.nick, avatar: u.avatar || '', email: u.email || '', taken: true, token });
      }
      let cloudAva = '';
      if (b.avatar) {
        const avRes = await resolveCloudAvatar(k, b.avatar, '');
        if (avRes.ok) cloudAva = avRes.url;
      }
      const salt = newSalt();
      const nu = {
        nickLower: k,
        nick,
        salt,
        hash: scryptHash(pass, salt),
        email,
        avatar: cloudAva,
        at: Date.now(),
        tokens: {}
      };
      const token = tokenIssue(nu);
      await dbSaveUser(nu);
      return res.json({ ok: true, nick, avatar: nu.avatar, email, taken: false, token });
    }

    if (act === 'login') {
      const lg = String(b.login == null ? '' : b.login).trim().slice(0, 190);
      if (!lg) return res.status(400).json({ error: 'укажите ник или почту' });
      const u = await dbGetUserByLogin(lg);
      if (!u || !passVerify(u, pass)) return res.status(403).json({ error: 'неверный ник (или почта) или пароль' });
      const token = tokenIssue(u);
      await dbSaveUser(u);
      return res.json(Object.assign({ ok: true, taken: true, token }, pubUser(u)));
    }

    if (act === 'chpass') {
      const lg = String(b.login == null ? '' : b.login).trim().slice(0, 190);
      const n1 = String(b.new1 == null ? '' : b.new1);
      const u = await dbGetUserByLogin(lg);
      if (!u || !passVerify(u, pass)) return res.status(403).json({ error: 'старый пароль неверный' });
      if (n1.length < 4) return res.status(400).json({ error: 'новый пароль минимум 4 символа' });
      u.salt = newSalt();
      u.hash = scryptHash(n1, u.salt);
      const token = tokenIssue(u);
      await dbSaveUser(u);
      return res.json({ ok: true, token });
    }

    if (act === 'bytoken') {
      const u = await dbGetUserByToken(b.token);
      if (!u) return res.status(403).json({ ok: false, error: 'сессия истекла' });
      return res.json(Object.assign({ ok: true }, pubUser(u)));
    }

    if (act === 'setava') {
      let u = await dbGetUserByToken(b.token);
      if (!u && b.nick) {
        const x = await dbGetUserByNick(b.nick);
        if (x && String(b.pass || '') && passVerify(x, String(b.pass))) u = x;
      }
      if (!u) return res.status(403).json({ error: 'нет доступа' });
      const avRes = await resolveCloudAvatar(u.nickLower, b.avatar, u.avatar);
      if (!avRes.ok) return res.status(503).json({ ok: false, code: avRes.code, error: avRes.error });
      u.avatar = avRes.url;
      await dbSaveUser(u);
      return res.json({ ok: true, avatar: u.avatar });
    }

    if (act === 'logout') {
      const tok = String(b.token || '');
      const u = await dbGetUserByToken(tok);
      if (u) {
        delete (u.tokens || {})[tok];
        tokenNickCache.delete(tok);
        await dbSaveUser(u);
      }
      return res.json({ ok: true });
    }

    res.status(400).json({ error: 'неизвестное действие' });
  } catch (e) {
    res.status(500).json({ error: 'БД users: ' + e.message });
  }
});

/* ==================== v17.41: серверная анти-спам защита и Self-Pardon ==================== */
const LINK_RE = /(?:https?:\/\/|ftp:\/\/|www\.|discord\.gg\/|t\.me\/|vk\.com\/|youtu\.be\/|bit\.ly\/|\b[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.(?:com|ru|net|org|io|gg|me|tv|xyz|info|site|online|top|club|dev|app|cc|kz|ua|by|su|pro|live|fun|ink|link|pw|click|shop)(?:\/|\b))/i;
function hasLink(t) { return LINK_RE.test(String(t == null ? '' : t)); }

const MUTE_5M_MS = 5 * 60 * 1000;
const WIN_7D_MS = 7 * 86400 * 1000;
const WIN_30D_MS = 30 * 86400 * 1000;
const TEMP_BAN_MS = 7 * 86400 * 1000;
const PARDON_CD_MS = 90 * 86400 * 1000;

function actorKeys(token, nick, uid) {
  const keys = [];
  const cached = token && typeof token === 'string' ? tokenNickCache.get(token) : null;
  const nk = cached ? cached.nick : nickClean(nick);
  if (nk && !/^гость$/i.test(nk) && nk !== '?') keys.push('u:' + nk.toLowerCase());
  const idClean = String(uid || '').replace(/[^A-Za-z0-9_:\-]/g, '').slice(0, 48);
  if (idClean && idClean !== '?') keys.push('id:' + idClean);
  return { keys, nick: nk || 'Гость', uid: idClean };
}

function getPenRecord(token, nick, uid, create) {
  const { keys, nick: dispNick } = actorKeys(token, nick, uid);
  if (!keys.length) return create ? { nick: dispNick, mutedUntil: 0, tempBanUntil: 0, permBan: false, onProbation: false, lastPardonAt: 0, strikes: [] } : null;
  let primary = null;
  for (const k of keys) {
    const r = penaltiesState.get(k);
    if (r) { primary = r; break; }
  }
  if (!primary) {
    if (!create) return null;
    primary = { nick: dispNick, mutedUntil: 0, tempBanUntil: 0, permBan: false, onProbation: false, lastPardonAt: 0, strikes: [] };
  }
  const touched = [];
  for (const k of keys) {
    const r = penaltiesState.get(k);
    if (r && r !== primary) {
      primary.mutedUntil = Math.max(primary.mutedUntil || 0, r.mutedUntil || 0);
      primary.tempBanUntil = Math.max(primary.tempBanUntil || 0, r.tempBanUntil || 0);
      primary.permBan = !!(primary.permBan || r.permBan);
      primary.permReason = primary.permReason || r.permReason || '';
      primary.onProbation = !!(primary.onProbation || r.onProbation);
      primary.lastPardonAt = Math.max(primary.lastPardonAt || 0, r.lastPardonAt || 0);
      const seen = new Set((primary.strikes || []).map(x => x.at + '|' + x.kind));
      for (const s of (r.strikes || [])) {
        const sk = s.at + '|' + s.kind;
        if (!seen.has(sk)) { primary.strikes.push(s); seen.add(sk); }
      }
    }
    if (create || penaltiesState.has(k)) {
      penaltiesState.set(k, primary);
      touched.push(k);
    }
  }
  if (dispNick && dispNick !== 'Гость') primary.nick = dispNick;
  if (touched.length && create) penPersistKeys(touched, primary);
  return primary;
}

function pubPen(rec) {
  const now = Date.now();
  if (!rec) {
    return {
      serverNow: now, muted: false, mutedUntil: 0, muteLeftMs: 0,
      tempBanned: false, tempBanUntil: 0, tempBanLeftMs: 0,
      permBan: false, permReason: '', onProbation: false,
      lastPardonAt: 0, pardonWaitMs: 0, canPardon: false,
      strikes7d: 0, strikes30d: 0
    };
  }
  const muteLeftMs = rec.permBan ? 0 : Math.max(0, (rec.mutedUntil || 0) - now);
  const tempBanLeftMs = rec.permBan ? 0 : Math.max(0, (rec.tempBanUntil || 0) - now);
  const pardonWaitMs = rec.lastPardonAt ? Math.max(0, (rec.lastPardonAt + PARDON_CD_MS) - now) : 0;
  const strikes = Array.isArray(rec.strikes) ? rec.strikes : [];
  const strikes7d = strikes.filter(s => now - s.at <= WIN_7D_MS).length;
  const strikes30d = strikes.filter(s => now - s.at <= WIN_30D_MS).length;
  return {
    serverNow: now,
    muted: !rec.permBan && muteLeftMs > 0,
    mutedUntil: rec.mutedUntil || 0,
    muteLeftMs,
    tempBanned: !rec.permBan && tempBanLeftMs > 0,
    tempBanUntil: rec.tempBanUntil || 0,
    tempBanLeftMs,
    permBan: !!rec.permBan,
    permReason: rec.permReason || '',
    onProbation: !!rec.onProbation,
    lastPardonAt: rec.lastPardonAt || 0,
    pardonWaitMs,
    canPardon: !rec.permBan && tempBanLeftMs > 0 && pardonWaitMs === 0,
    strikes7d,
    strikes30d
  };
}

function recordStrike(opts) {
  const now = Date.now();
  for (const [dk, dt] of strikeDedup) if (now - dt > 60000) strikeDedup.delete(dk);
  const { keys } = actorKeys(opts.token, opts.nick, opts.uid);
  const rec = getPenRecord(opts.token, opts.nick, opts.uid, true);
  if (opts.dedupId) {
    if (strikeDedup.has(opts.dedupId)) return pubPen(rec);
    strikeDedup.set(opts.dedupId, now);
  }
  rec.mutedUntil = Math.max(rec.mutedUntil || 0, now + MUTE_5M_MS);
  rec.strikes = (rec.strikes || []).filter(s => now - s.at <= 35 * 86400e3);
  const kind = String(opts.kind || 'spam').slice(0, 24);
  const where = String(opts.where || '').slice(0, 48);
  rec.strikes.push({ at: now, kind, where });
  const c7 = rec.strikes.filter(s => now - s.at <= WIN_7D_MS).length;
  const c30 = rec.strikes.filter(s => now - s.at <= WIN_30D_MS).length;
  if (rec.onProbation) {
    rec.permBan = true;
    rec.permAt = now;
    rec.permReason = 'Нарушение правил после апелляции «Честное слово»';
    modLog({ kind: 'perm_ban_probation', who: rec.nick || opts.uid || '?', note: rec.permReason + ' (' + kind + ')' });
  } else if (c30 >= 10) {
    rec.permBan = true;
    rec.permAt = now;
    rec.permReason = '10 нарушений за 30 дней';
    modLog({ kind: 'perm_ban', who: rec.nick || opts.uid || '?', note: '10 нарушений за 30 дней — вечная блокировка чата/комментов' });
  } else if (c7 >= 5) {
    rec.tempBanUntil = Math.max(rec.tempBanUntil || 0, now + TEMP_BAN_MS);
    modLog({ kind: 'temp_ban', who: rec.nick || opts.uid || '?', note: '5 нарушений за 7 дней — временная блокировка чата/комментов' });
  } else {
    modLog({ kind: 'auto_mute', who: rec.nick || opts.uid || '?', note: 'авто-мут 5 мин (' + kind + (where ? ' · ' + where : '') + ')' });
  }
  penPersistKeys(keys, rec);
  return pubPen(rec);
}

function sanitizeRoomPut(code, data, prevEntry) {
  if (!data || !Array.isArray(data.msgs)) return;
  const now = Date.now();
  const seen = prevEntry && prevEntry.seenMsgs ? prevEntry.seenMsgs : new Set();
  const mems = data.members || {};
  const kept = [];
  for (const m of data.msgs) {
    if (!m) continue;
    if (m.sys || m.del || (m.id && seen.has(m.id))) {
      if (m.id) seen.add(m.id);
      kept.push(m);
      continue;
    }
    if (!prevEntry && m.at && (now - m.at > 5000) && !hasLink(m.body)) {
      if (m.id) seen.add(m.id);
      kept.push(m);
      continue;
    }
    const mem = mems[m.uid];
    const role = (mem && mem.role) || m.role || 'viewer';
    const canLinks = role === 'streamer' || role === 'moderator' || !!(mem && mem.allowLinks);
    const pen = getPenRecord(null, m.name || (mem && mem.name), m.uid, false);
    if (pen && (pen.permBan || (pen.tempBanUntil || 0) > now || (pen.mutedUntil || 0) > now)) {
      continue;
    }
    if (!canLinks && !m.gif && hasLink(m.body)) {
      const st = recordStrike({ nick: m.name || (mem && mem.name), uid: m.uid, kind: 'link', where: 'room:' + code, dedupId: m.id ? ('msg:' + m.id) : '' });
      if (mem) mem.mutedUntil = Math.max(mem.mutedUntil || 0, st.mutedUntil || (now + MUTE_5M_MS));
      data.log = Array.isArray(data.log) ? data.log : [];
      data.log.unshift({ at: now, by: 'Авто-модерация', byId: 'sys', action: 'auto_mute', target: m.name || '', reason: 'ссылка без разрешения: авто-мут 5 минут' });
      const sysId = 'sys_' + Math.random().toString(36).slice(2, 9) + now.toString(36).slice(-4);
      seen.add(sysId);
      kept.push({
        id: sysId, sys: true, at: now,
        body: st.permBan
          ? '⛔ ' + (m.name || 'Зритель') + ': ссылка удалена, аккаунт заблокирован навсегда'
          : st.tempBanned
            ? '🛡 ' + (m.name || 'Зритель') + ': ссылка удалена, выдана временная блокировка чата/комментов (5+ нарушений за 7 дней)'
            : '🛡 Авто-модерация: ссылка от ' + (m.name || 'зрителя') + ' мгновенно удалена, выдан мут на 5 минут'
      });
      continue;
    }
    if (m.id) seen.add(m.id);
    kept.push(m);
  }
  data.msgs = kept.slice(-300);
  if (prevEntry) prevEntry.seenMsgs = seen;
}

app.get('/api/mod/status', async (req, res) => {
  if (!(await requireDb(res))) return;
  if (req.query.token) await dbGetUserByToken(req.query.token);
  const rec = getPenRecord(req.query.token, req.query.nick, req.query.uid, false);
  res.json({ ok: true, serverNow: Date.now(), mod: pubPen(rec) });
});

app.post('/api/mod/strike', async (req, res) => {
  if (!(await requireDb(res))) return;
  const b = req.body || {};
  if (b.token) await dbGetUserByToken(b.token);
  const kind = ['link', 'profanity', 'spam'].includes(b.kind) ? b.kind : 'spam';
  const room = String(b.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
  const st = recordStrike({
    token: b.token,
    nick: b.nick,
    uid: b.uid,
    kind,
    where: room ? ('room:' + room) : 'chat',
    dedupId: b.dedupId ? String(b.dedupId).slice(0, 48) : ''
  });
  if (room) {
    const rm = roomsState.get(room);
    if (rm && rm.data) {
      const uid = String(b.uid || '');
      if (uid && rm.data.members && rm.data.members[uid]) {
        rm.data.members[uid].mutedUntil = Math.max(rm.data.members[uid].mutedUntil || 0, st.mutedUntil || 0);
      }
      rm.data.log = Array.isArray(rm.data.log) ? rm.data.log : [];
      const actorName = nickClean(b.nick) || (uid && rm.data.members && rm.data.members[uid] && rm.data.members[uid].name) || 'Гость';
      const reasonTxt = kind === 'profanity' ? 'нецензурная лексика: авто-мут 5 минут' : kind === 'link' ? 'ссылка без разрешения: авто-мут 5 минут' : 'авто-мут 5 минут';
      if (!rm.data.log.some(x => x && x.action === 'auto_mute' && x.target === actorName && Math.abs((x.at || 0) - Date.now()) < 3000)) {
        rm.data.log.unshift({ at: Date.now(), by: 'Авто-модерация', byId: 'sys', action: 'auto_mute', target: actorName, reason: reasonTxt });
        rm.data.log = rm.data.log.slice(0, 100);
      }
      rm.rev++; rm.at = Date.now();
      notifyWaiters(room);
    }
  }
  res.json({ ok: true, serverNow: st.serverNow, mod: st });
});

app.post('/api/mod/pardon', async (req, res) => {
  if (!(await requireDb(res))) return;
  const b = req.body || {};
  const now = Date.now();
  const authU = b.token ? await dbGetUserByToken(b.token) : null;
  const { keys } = actorKeys(b.token, b.nick, b.uid);
  const rec = getPenRecord(b.token, b.nick, b.uid, false);
  if (!rec) return res.status(400).json({ error: 'Нет активных наказаний', serverNow: now, mod: pubPen(null) });
  if (rec.permBan)
    return res.status(403).json({ error: 'Перманентный бан не подлежит обжалованию', serverNow: now, mod: pubPen(rec) });
  if ((rec.tempBanUntil || 0) <= now)
    return res.status(400).json({ error: 'У вас нет активной временной блокировки', serverNow: now, mod: pubPen(rec) });
  if (rec.lastPardonAt && (now - rec.lastPardonAt) < PARDON_CD_MS) {
    const leftDays = Math.ceil((rec.lastPardonAt + PARDON_CD_MS - now) / 86400e3);
    return res.status(429).json({
      error: 'Повторное снятие блокировки доступно не чаще 1 раза в 3 месяца (осталось ~' + leftDays + ' дн.)',
      serverNow: now, mod: pubPen(rec)
    });
  }
  rec.tempBanUntil = 0;
  rec.mutedUntil = 0;
  rec.lastPardonAt = now;
  rec.onProbation = true;
  penPersistKeys(keys, rec);

  const userName = (authU && authU.nick) || nickClean(b.nick) || rec.nick || 'Пользователь';
  const sysText = '🤝 Пользователь ' + userName + ' дал честное слово больше не нарушать правила и снял временную блокировку.';
  modLog({ kind: 'self_pardon', who: userName.toLowerCase(), note: 'апелляция «Честное слово» принята — включён испытательный режим' });

  const targetRoom = String(b.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
  const touchedRooms = new Set();
  for (const [code2, rm] of roomsState) {
    if (!rm || !rm.data) continue;
    const mems = rm.data.members || {};
    const uid = String(b.uid || '');
    const inRoom = (uid && mems[uid]) ||
      Object.values(mems).some(m => m && String(m.name || '').toLowerCase() === userName.toLowerCase()) ||
      (targetRoom && code2 === targetRoom);
    if (inRoom) {
      if (uid && mems[uid]) mems[uid].mutedUntil = 0;
      for (const m of Object.values(mems)) {
        if (m && String(m.name || '').toLowerCase() === userName.toLowerCase()) m.mutedUntil = 0;
      }
      rm.data.msgs = rm.data.msgs || [];
      const mid = 'pardon_' + Math.random().toString(36).slice(2, 9) + now.toString(36).slice(-4);
      rm.data.msgs.push({ id: mid, sys: true, pardon: true, at: now, body: sysText });
      if (rm.seenMsgs) rm.seenMsgs.add(mid);
      rm.rev++; rm.at = now;
      notifyWaiters(code2);
      touchedRooms.add(code2);
    }
  }
  res.json({ ok: true, serverNow: now, sysText, roomsNotified: [...touchedRooms], mod: pubPen(rec) });
});

/* ==================== Друзья и ЛС (таблицы friends и dms во внешней БД) ==================== */
const dmWaiters = new Map();
function dmNotify(pk) {
  const set = dmWaiters.get(pk);
  if (set) { const fns = [...set]; set.clear(); for (const fn of fns) { try { fn(); } catch {} } }
}
const frPair = (a, b) => [String(a).toLowerCase(), String(b).toLowerCase()].sort().join('|');

function rowToFriend(r) {
  if (!r) return null;
  return {
    pairKey: r.pair_key,
    a: r.user_a,
    b: r.user_b,
    aNick: r.nick_a,
    bNick: r.nick_b,
    status: r.status,
    from: r.requester,
    at: Number(r.created_at) || 0,
    updatedAt: Number(r.updated_at) || 0
  };
}

async function dbGetFriend(pk) {
  const r = await pgPool.query('SELECT * FROM friends WHERE pair_key = $1 LIMIT 1', [pk]);
  return rowToFriend(r.rows[0]);
}

async function profileFriendshipAsync(viewer, targetLower) {
  if (!viewer || !targetLower) return 'none';
  const meK = viewer.nickLower || viewer.nick.toLowerCase();
  if (meK === targetLower) return 'self';
  const rec = await dbGetFriend(frPair(meK, targetLower));
  if (!rec) return 'none';
  if (rec.status === 'accepted') return 'friends';
  return rec.from === meK ? 'outgoing' : 'incoming';
}

async function dbGetDm(pk) {
  const r = await pgPool.query('SELECT rev, msgs, read_at FROM dms WHERE pair_key = $1 LIMIT 1', [pk]);
  const row = r.rows[0];
  if (!row) return { rev: 0, msgs: [], readAt: {} };
  return {
    rev: Number(row.rev) || 0,
    msgs: Array.isArray(row.msgs) ? row.msgs : [],
    readAt: (row.read_at && typeof row.read_at === 'object') ? row.read_at : {}
  };
}

async function dbSaveDm(pk, conv) {
  await pgPool.query(
    `INSERT INTO dms (pair_key, rev, msgs, read_at, updated_at)
     VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
     ON CONFLICT (pair_key) DO UPDATE SET
       rev = EXCLUDED.rev,
       msgs = EXCLUDED.msgs,
       read_at = EXCLUDED.read_at,
       updated_at = EXCLUDED.updated_at`,
    [pk, conv.rev || 0, JSON.stringify(conv.msgs || []), JSON.stringify(conv.readAt || {}), Date.now()]
  );
}

app.get('/api/friends', async (req, res) => {
  if (!(await requireDb(res))) return;
  try {
    const me = await authFromReqAsync(req);
    if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для доступа к друзьям и ЛС', serverNow: Date.now() });
    const meK = me.nickLower;
    const frRows = await pgPool.query('SELECT * FROM friends WHERE user_a = $1 OR user_b = $1', [meK]);
    const friends = [], incoming = [], outgoing = [];
    let unreadTotal = 0;

    for (const raw of frRows.rows) {
      const rec = rowToFriend(raw);
      const otherK = rec.a === meK ? rec.b : rec.a;
      const ou = await dbGetUserByNick(otherK);
      const nick = (ou && ou.nick) || (rec.a === meK ? rec.bNick : rec.aNick) || otherK;
      const avatar = (ou && ou.avatar) || '';
      if (rec.status === 'accepted') {
        const conv = await dbGetDm(rec.pairKey);
        const readAt = (conv.readAt && conv.readAt[meK]) || 0;
        const msgs = conv.msgs || [];
        const unread = msgs.filter(m => m.to === meK && m.at > readAt).length;
        unreadTotal += unread;
        const last = msgs.length ? msgs[msgs.length - 1] : null;
        friends.push({
          nick, avatar, since: rec.updatedAt || rec.at, unread,
          lastMsg: last ? { text: String(last.text || '').slice(0, 120), at: last.at, from: last.fromNick || last.from } : null
        });
      } else if (rec.from === meK) {
        outgoing.push({ nick, avatar, at: rec.at });
      } else {
        incoming.push({ nick, avatar, at: rec.at });
      }
    }
    friends.sort((a, b) => ((b.lastMsg && b.lastMsg.at) || b.since || 0) - ((a.lastMsg && a.lastMsg.at) || a.since || 0));
    incoming.sort((a, b) => b.at - a.at);
    outgoing.sort((a, b) => b.at - a.at);
    res.json({ ok: true, serverNow: Date.now(), friends, incoming, outgoing, unreadTotal });
  } catch (e) {
    res.status(500).json({ error: 'БД friends: ' + e.message });
  }
});

app.post('/api/friends', async (req, res) => {
  if (!(await requireDb(res))) return;
  const now = Date.now();
  try {
    const me = await authFromReqAsync(req);
    if (!me) return res.status(401).json({ error: 'Войдите в аккаунт, чтобы управлять списком друзей', serverNow: now });
    const b = req.body || {};
    const act = String(b.act || '');
    const targetNick = nickClean(b.target);
    const meK = me.nickLower;
    const targetK = targetNick.toLowerCase();
    if (!targetK || !NICK_RE.test(targetNick))
      return res.status(400).json({ error: 'Укажите корректный ник пользователя', serverNow: now });
    if (meK === targetK)
      return res.status(400).json({ error: 'Нельзя добавить в друзья самого себя', serverNow: now });

    const pk = frPair(meK, targetK);
    const rec = await dbGetFriend(pk);
    const tu = await dbGetUserByNick(targetK);

    if (act === 'request') {
      if (!tu) return res.status(404).json({ error: 'Пользователь «' + targetNick + '» ещё не зарегистрировал аккаунт', serverNow: now });
      if (rec && rec.status === 'accepted')
        return res.json({ ok: true, status: 'friends', serverNow: now });
      if (rec && rec.status === 'pending') {
        if (rec.from === meK) return res.json({ ok: true, status: 'outgoing', serverNow: now });
        await pgPool.query('UPDATE friends SET status = $2, updated_at = $3 WHERE pair_key = $1', [pk, 'accepted', now]);
        return res.json({ ok: true, status: 'friends', serverNow: now });
      }
      const [a, bKey] = [meK, targetK].sort();
      await pgPool.query(
        `INSERT INTO friends (pair_key, user_a, user_b, nick_a, nick_b, status, requester, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $7)
         ON CONFLICT (pair_key) DO UPDATE SET
           status = 'pending', requester = EXCLUDED.requester, updated_at = EXCLUDED.updated_at`,
        [pk, a, bKey, a === meK ? me.nick : tu.nick, bKey === meK ? me.nick : tu.nick, meK, now]
      );
      return res.json({ ok: true, status: 'outgoing', serverNow: now });
    }
    if (act === 'accept') {
      if (!rec || rec.status !== 'pending')
        return res.status(404).json({ error: 'Заявка в друзья не найдена', serverNow: now });
      if (rec.from === meK)
        return res.status(403).json({ error: 'Нельзя подтвердить собственную заявку — дождитесь ответа второго пользователя', serverNow: now });
      await pgPool.query('UPDATE friends SET status = $2, updated_at = $3 WHERE pair_key = $1', [pk, 'accepted', now]);
      return res.json({ ok: true, status: 'friends', serverNow: now });
    }
    if (act === 'decline' || act === 'cancel') {
      if (rec && rec.status === 'pending') await pgPool.query('DELETE FROM friends WHERE pair_key = $1', [pk]);
      return res.json({ ok: true, status: 'none', serverNow: now });
    }
    if (act === 'remove') {
      if (rec) await pgPool.query('DELETE FROM friends WHERE pair_key = $1', [pk]);
      return res.json({ ok: true, status: 'none', historySaved: true, serverNow: now });
    }
    res.status(400).json({ error: 'неизвестное действие', serverNow: now });
  } catch (e) {
    res.status(500).json({ error: 'БД friends: ' + e.message, serverNow: now });
  }
});

app.get('/api/dm/:peer', async (req, res) => {
  if (!(await requireDb(res))) return;
  const now = Date.now();
  try {
    const me = await authFromReqAsync(req);
    if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для доступа к ЛС', serverNow: now });
    const peerNick = nickClean(req.params.peer);
    const meK = me.nickLower;
    const peerK = peerNick.toLowerCase();
    const pk = frPair(meK, peerK);
    const fr = await dbGetFriend(pk);
    if (!fr || fr.status !== 'accepted')
      return res.status(403).json({ error: 'Писать и открывать ЛС можно только подтверждённым друзьям', code: 'NOT_FRIENDS', serverNow: now });
    const tu = await dbGetUserByNick(peerK);

    const sendOut = async () => {
      const conv = await dbGetDm(pk);
      conv.readAt = conv.readAt || {};
      if (conv.msgs && conv.msgs.length) {
        conv.readAt[meK] = Date.now();
        await dbSaveDm(pk, conv);
      }
      if (!res.headersSent) {
        res.json({
          ok: true,
          serverNow: Date.now(),
          rev: conv.rev || 0,
          peer: { nick: (tu && tu.nick) || peerNick, avatar: (tu && tu.avatar) || '' },
          msgs: (conv.msgs || []).slice(-300)
        });
      }
    };

    if (req.query.since === undefined) return await sendOut();
    const since = parseInt(req.query.since, 10) || 0;
    const cur = await dbGetDm(pk);
    if (cur && cur.rev > since) return await sendOut();

    const set = dmWaiters.get(pk) || new Set();
    dmWaiters.set(pk, set);
    let fired = false;
    const fire = async () => {
      if (fired) return;
      fired = true;
      set.delete(fire);
      try {
        const frNow = await dbGetFriend(pk);
        if (!frNow || frNow.status !== 'accepted') {
          if (!res.headersSent) res.status(403).json({ error: 'Пользователь больше не в списке друзей', code: 'NOT_FRIENDS', serverNow: Date.now() });
          return;
        }
        await sendOut();
      } catch {
        if (!res.headersSent) res.status(500).json({ error: 'БД dm' });
      }
    };
    set.add(fire);
    setTimeout(fire, 20000);
  } catch (e) {
    if (!res.headersSent) res.status(500).json({ error: 'БД dm: ' + e.message });
  }
});

app.post('/api/dm/:peer', async (req, res) => {
  if (!(await requireDb(res))) return;
  const now = Date.now();
  try {
    const me = await authFromReqAsync(req);
    if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для отправки ЛС', serverNow: now });
    const peerNick = nickClean(req.params.peer);
    const meK = me.nickLower;
    const peerK = peerNick.toLowerCase();
    const pk = frPair(meK, peerK);
    const fr = await dbGetFriend(pk);
    if (!fr || fr.status !== 'accepted')
      return res.status(403).json({ error: 'Писать в ЛС можно только после подтверждения заявки в друзья', code: 'NOT_FRIENDS', serverNow: now });
    const b = req.body || {};
    const conv = await dbGetDm(pk);
    if (b.act === 'read') {
      conv.readAt = conv.readAt || {};
      conv.readAt[meK] = now;
      await dbSaveDm(pk, conv);
      return res.json({ ok: true, serverNow: now });
    }
    const text = cmtClean(b.text, 1500);
    if (!text) return res.status(400).json({ error: 'Пустое сообщение', serverNow: now });
    if (conv.msgs.length >= 600) conv.msgs = conv.msgs.slice(-450);
    const msg = {
      id: 'dm_' + Math.random().toString(36).slice(2, 10) + now.toString(36).slice(-4),
      from: meK,
      fromNick: me.nick,
      to: peerK,
      text,
      at: now
    };
    conv.msgs.push(msg);
    conv.rev = (conv.rev || 0) + 1;
    conv.readAt = conv.readAt || {};
    conv.readAt[meK] = now;
    await dbSaveDm(pk, conv);
    dmNotify(pk);
    res.json({ ok: true, rev: conv.rev, id: msg.id, msg, serverNow: now });
  } catch (e) {
    res.status(500).json({ error: 'БД dm: ' + e.message });
  }
});


/* ------------------------- комнаты в памяти -----------------------
   rooms: Map<code, Map<sid, {uid,name,screen,cam,joinedAt}>>
   Состояние аниме-комнаты (чат/синхронизация) живёт в клиентах
   (localStorage + P2P + опциональный python-хаб) — здесь только
   присутствие и signaling для демонстрации экрана.             */
const rooms = new Map();
const ONLINE_MS = 65000;   // столько живём без heartbeat от клиента

function roomOf(code) {
  let r = rooms.get(code);
  if (!r) { r = new Map(); rooms.set(code, r); }
  return r;
}
function prune(code) {
  const r = rooms.get(code);
  if (!r) return;
  const now = Date.now();
  for (const [sid, m] of r) if (now - m.seen > ONLINE_MS) r.delete(sid);
  if (!r.size) rooms.delete(code);
}
function members(code) {
  const r = rooms.get(code);
  if (!r) return [];
  return [...r.values()].map(m => ({
    sid: m.sid, uid: m.uid, name: m.name, screen: m.screen, cam: m.cam, mic: m.mic
  }));
}
function broadcastMembers(code) {
  io.to(code).emit('members', members(code));
}

io.on('connection', s => {
  s.data.room = null;

  /* --- вход в комнату: {code, uid, name} → ack(участники) --- */
  s.on('join', (p, ack) => {
    const code = String((p && p.code) || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
    if (!code) { ack && ack({ error: 'нет кода' }); return; }
    s.data.room = code;
    s.data.uid = String((p && p.uid) || s.id).slice(0, 40);
    s.data.name = String((p && p.name) || 'гость').slice(0, 24);
    s.join(code);
    roomOf(code).set(s.id, {
      sid: s.id, uid: s.data.uid, name: s.data.name,
      screen: false, cam: false, seen: Date.now()
    });
    ack && ack({ ok: true, members: members(code) });
    broadcastMembers(code);
  });

  /* --- «я/show screen/cam» → всем обновлённый список --- */
  s.on('ssState', p => {
    const code = s.data.room; if (!code) return;
    const m = rooms.get(code) && rooms.get(code).get(s.id);
    if (!m) return;
    m.screen = !!(p && p.screen);
    m.cam = !!(p && p.cam);
    m.mic = !!(p && p.mic);
    m.seen = Date.now();
    broadcastMembers(code);
  });

  /* --- WebRTC signaling: кандидаты и SDP между двумя участниками --- */
  s.on('signal', p => {
    const code = s.data.room; if (!code || !p || !p.to) return;
    const kind = String(p.kind || 'screen').slice(0, 8);
    io.to(p.to).emit('signal', { from: s.id, kind, data: p.data, uid: s.data.uid, name: s.data.name });
  });

  /* --- keepalive, чтобы список онлайн был живым --- */
  s.on('hb', () => {
    const code = s.data.room; if (!code) return;
    const m = rooms.get(code) && rooms.get(code).get(s.id);
    if (m) m.seen = Date.now();
  });

  /* --- явный выход со страницы комнаты --- */
  s.on('leave', () => {
    const code = s.data.room; if (!code) return;
    const r = rooms.get(code);
    if (r) { r.delete(s.id); if (!r.size) rooms.delete(code); }
    s.leave(code);
    s.data.room = null;
    broadcastMembers(code);
  });

  s.on('disconnect', () => {
    const code = s.data.room; if (!code) return;
    const r = rooms.get(code);
    if (r) { r.delete(s.id); if (!r.size) rooms.delete(code); }
    broadcastMembers(code);
  });
});

/* подметаем протухшие heartbeat раз в 30 сек */
setInterval(() => {
  for (const code of [...rooms.keys()]) {
    const before = rooms.get(code).size;
    prune(code);
    if (rooms.get(code) && rooms.get(code).size !== before) broadcastMembers(code);
    else if (!rooms.get(code)) continue;
  }
}, 30000);

const PORT = process.env.PORT || 3000;
/* ==== AniHub · Kodik-шлюз (порт kodik_proxy.py на Node) =====================
   Зачем: страница может жить на GitHub Pages или открыта как файл —
   ей нужен сервер-компаньон: достать ПРЯМЫЕ m3u8-потоки Kodik
   (без рекламного плеера), проксировать HLS-манифесты/сегменты
   и форвардить поисковые запросы к Kodik API. Всё с CORS *, кэшем
   и защитой от SSRF (белый список хостов + запрет приватных IP). */

const dnsProm = require('dns').promises;
const KODIK_UPSTREAM = 'https://kodik-api.com';
const KODIK_PLAYER = 'https://kodikplayer.com';
const KODIK_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36';
const EGRESS_ALLOW = [
  'anilibria.top', 'aniliberty.top', 'libria.fun',
  'kodik-api.com', 'kodik.dev', 'kodikplayer.com', 'kodik.info', 'kodik.cc', 'kodikcdn.com',
  'solodcdn.com', 'raw.githubusercontent.com',
  'api.animethemes.moe', 'v.animethemes.moe',
  'cdnvideohub.com', 'animevost.org', 'animetop.info', 'yani.tv', 'yummyani.me', 'aksor.tv',
  'haglund.dev', 'strem.io', 'opensubtitles.org', 'kitsunekko.net', 'jsdelivr.net', 'unpkg.com', 'ani.zip'
];
(process.env.EGRESS_EXTRA || '').split(',').filter(Boolean).forEach(d => EGRESS_ALLOW.push(d.trim().toLowerCase()));

const _kCache = new Map();                      // key -> {at, val}
function kCacheGet(k, ttl) {
  const e = _kCache.get(k);
  if (!e) return null;
  if (Date.now() - e.at > ttl) { _kCache.delete(k); return null; }
  return e.val;
}
function kCacheSet(k, v) {
  if (_kCache.size >= 128) _kCache.delete(_kCache.keys().next().value);
  _kCache.set(k, { at: Date.now(), val: v });
}

async function egressCheck(rawUrl) {
  let u;
  try { u = new URL(rawUrl); } catch { throw new Error('плохой URL'); }
  if (u.protocol !== 'https:' && u.protocol !== 'http:') throw new Error('разрешены только http(s)');
  const host = (u.hostname || '').toLowerCase().replace(/\.$/, '');
  if (!EGRESS_ALLOW.some(d => host === d || host.endsWith('.' + d))) throw new Error('host не в белом списке: ' + host);
  try {
    const addrs = await dnsProm.lookup(host, { all: true });
    for (const a of addrs) {
      if (/^(10\.|127\.|169\.254\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|0\.|22[4-9]\.|23\d\.)/.test(a.address))
        throw new Error('хост ' + host + ' указывает на закрытый адрес ' + a.address);
    }
  } catch (e) {
    if (/закрытый адрес/.test(String(e.message))) throw e;
    throw new Error('DNS не разрешился: ' + host);
  }
  return u.toString();
}
async function kGet(url, timeout) {
  const safe = await egressCheck(url);
  const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA }, signal: AbortSignal.timeout(timeout || 20000), redirect: 'follow' });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return r;
}
function kRot(s, n) {
  let out = '';
  for (const c of s) {
    if (/[a-z]/.test(c)) out += String.fromCharCode((c.charCodeAt(0) - 97 + n) % 26 + 97);
    else if (/[A-Z]/.test(c)) out += String.fromCharCode((c.charCodeAt(0) - 65 + n) % 26 + 65);
    else out += c;
  }
  return out;
}
function kDecode(s) {
  if (s.includes('mp4:hls:manifest')) return s;
  for (let n = 0; n < 26; n++) {
    const t = kRot(s, n) + '='.repeat((4 - (kRot(s, n).length % 4)) % 4);
    try {
      const d = Buffer.from(t, 'base64').toString('utf8');
      if (d.includes('mp4:hls:manifest')) return d;
    } catch {}
  }
  throw new Error('не удалось расшифровать ссылку');
}

/* --- /kodik-proxy/:op — форвард к Kodik API (search/translations/...) --- */
app.get('/kodik-proxy/:op', async (q, r) => {
  try {
    const qs = new URLSearchParams(q.query).toString();
    const url = KODIK_UPSTREAM + '/' + encodeURIComponent(q.params.op) + (qs ? '?' + qs : '');
    const resp = await kGet(url, 12000);
    const txt = await resp.text();
    r.status(resp.status).type('application/json').send(txt);
  } catch (e) {
    r.status(502).type('application/json').send(JSON.stringify({ error: 'kodik-proxy: ' + e.message }));
  }
});

/* --- /kodik/resolve — прямые m3u8-ссылки из плеера Kodik (серверный алгоритм) --- */
async function kodikResolveInternal(type, id, hash) {
  const key = type + '|' + id + '|' + hash;
  const hit = kCacheGet(key, 1500000);
  if (hit) return hit;
  const page = await (await kGet(`${KODIK_PLAYER}/${encodeURIComponent(type)}/${encodeURIComponent(id)}/${encodeURIComponent(hash)}/720p`, 15000)).text();
  const up = page.indexOf('urlParams');
  let urlParams = null;
  const mStr = page.slice(up).match(/=\s*'(\{[\s\S]*?\})'\s*;/);
  if (mStr) { try { urlParams = JSON.parse(mStr[1]); } catch {} }
  if (!urlParams) {
    const b = page.indexOf('{', up), e = page.indexOf('};', up);
    if (b > 0 && e > b) { try { urlParams = JSON.parse(page.slice(b, e + 1)); } catch {} }
  }
  if (!urlParams) throw new Error('urlParams не найдены');
  const sm = page.match(/<script[^>]*src="(\/assets\/js\/app\.(?:player|serial|movie|video|film)[^"]+)"/) ||
             page.match(/<script[^>]*src="(\/assets\/js\/app\.[^"]+)"/);
  if (!sm) throw new Error('не найден скрипт плеера');
  const val = k => { const m = page.match(new RegExp('\\.' + k + " = '([^']+)'")); return m && m[1]; };
  const vtype = val('type'), vhash = val('hash'), vid = val('id');
  if (!vtype || !vhash || !vid) throw new Error('параметры плеера не найдены');
  const skip = [];
  const skm = page.match(/parseSkipButton\("([^"]*)"/);
  if (skm && skm[1]) {
    for (const seg of skm[1].split(',')) {
      if (!seg.includes('-')) continue;
      const [a, b] = seg.split('-');
      const p = t => t.split(':').reduce((x, y) => +x * 60 + +y, 0);
      skip.push([p(a), p(b)]);
    }
  }
  let postLink = kCacheGet('plink|' + sm[1], 3600000);
  if (!postLink) {
    const js = await (await kGet(KODIK_PLAYER + sm[1], 15000)).text();
    const ai = js.indexOf('$.ajax'), ci = js.indexOf('cache:!1');
    if (ai < 0 || ci < 0) throw new Error('алгоритм изменился');
    postLink = Buffer.from(js.slice(ai + 30, ci - 3), 'base64').toString('utf8');
    kCacheSet('plink|' + sm[1], postLink);
  }
  const qs = new URLSearchParams({
    hash: vhash, id: vid, type: vtype,
    d: urlParams.d, d_sign: urlParams.d_sign,
    pd: urlParams.pd, pd_sign: urlParams.pd_sign,
    ref: '', ref_sign: urlParams.ref_sign,
    bad_user: 'true', cdn_is_working: 'true'
  }).toString();
  const pr = await fetch(await egressCheck(KODIK_PLAYER + postLink), {
    method: 'POST',
    body: qs,
    headers: { 'User-Agent': KODIK_UA, 'Content-Type': 'application/x-www-form-urlencoded', 'Referer': `${KODIK_PLAYER}/${type}/${id}/${hash}/720p` },
    signal: AbortSignal.timeout(15000)
  });
  const data = await pr.json();
  const links = {};
  for (const [quality, arr] of Object.entries(data.links || {})) {
    try { links[quality] = kDecode(arr[0].src); } catch {}
  }
  if (!Object.keys(links).length) throw new Error('пустой список ссылок');
  const out = { links, max: Math.max(...Object.keys(links).map(Number)), skip };
  kCacheSet(key, out);
  return out;
}

app.get('/kodik/resolve', async (q, r) => {
  const { id, hash, type } = q.query;
  if (!id || !hash || !type) return r.status(400).json({ error: 'нужны id, hash, type' });
  try {
    const out = await kodikResolveInternal(type, id, hash);
    r.json(out);
  } catch (e) {
    r.status(502).json({ error: 'kodik resolve: ' + e.message });
  }
});

/* --- /kodik/hls — прокси манифестов и сегментов + вырезание рекламных блоков --- */
const _AD_RE = /(\/ads?[\/._-]|\/ad\/|advert|preroll|midroll|adcreative|adfox|doubleclick|googlesynd|adservice|adsystem|an\.yandex|adriver|\/promo)/i;
const _HEADER_TAG = /^#EXT-X-(TARGETDURATION|VERSION|MEDIA-SEQUENCE|DISCONTINUITY-SEQUENCE|PLAYLIST-TYPE|ALLOW-CACHE|INDEPENDENT-SEGMENTS|START)[:\s]/i;
const _URI_RE = /URI="([^"]+)"/g;
function kGuessType(url) {
  const p = (new URL(url)).path || '';
  const path = (new URL(url)).pathname.toLowerCase();
  if (path.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
  if (path.endsWith('.ts')) return 'video/mp2t';
  if (path.endsWith('.m4s')) return 'video/iso.segment';
  if (path.endsWith('.mp4')) return 'video/mp4';
  if (path.endsWith('.vtt')) return 'text/vtt';
  if (path.endsWith('.aac')) return 'audio/aac';
  return 'application/octet-stream';
}
app.get('/kodik/hls', async (q, r) => {
  const target = String(q.query.url || '');
  try {
    const safe = await egressCheck(target);
    const resp = await fetch(safe, { headers: { 'User-Agent': KODIK_UA }, signal: AbortSignal.timeout(20000), redirect: 'follow' });
    if (!resp.ok) return r.status(resp.status).send('upstream ' + resp.status);
    const ct = resp.headers.get('content-type') || '';
    const buf = Buffer.from(await resp.arrayBuffer());
    const isManifest = target.includes('.m3u8') || buf.slice(0, 20).toString('utf8').trimStart().startsWith('#EXTM3U');
    if (!isManifest) return r.type(ct || kGuessType(target)).send(buf);
    // --- манифест: переписываем пути, режем рекламные DISCONTINUITY-блоки ---
    const units = [], header = [], trailer = [];
    let pending = [], disco = false;
    for (let ln of buf.toString('utf8').split('\n')) {
      const s = ln.trim();
      if (!s) continue;
      if (s === '#EXTM3U') continue;
      if (!s.startsWith('#')) {
        units.push({ tags: pending, seg: new URL(s, target).toString(), disco });
        pending = []; disco = false;
        continue;
      }
      if (s.startsWith('#EXT-X-DISCONTINUITY') && !s.startsWith('#EXT-X-DISCONTINUITY-SEQUENCE')) disco = true;
      else if (_HEADER_TAG.test(s)) header.push(s);
      else if (s.startsWith('#EXT-X-ENDLIST')) trailer.push(s);
      else pending.push(s);
    }
    trailer.push(...pending);
    if (!units.length) return r.type('application/vnd.apple.mpegurl').send(buf);
    const groups = []; let cur = [];
    for (const u of units) {
      if (u.disco && cur.length) { groups.push(cur); cur = []; }
      cur.push(u);
    }
    if (cur.length) groups.push(cur);
    const out = []; let removed = 0;
    const wrapUri = line => line.replace(_URI_RE, (m, u1) => 'URI="/kodik/hls?url=' + encodeURIComponent(new URL(u1, target).toString()) + '"');
    for (const g of groups) {
      const gsegs = g.map(u => u.seg);
      const wholeAd = g[0].disco && gsegs.every(x => _AD_RE.test(x));
      if (wholeAd) { removed += gsegs.length; continue; }
      if (g[0].disco) out.push('#EXT-X-DISCONTINUITY');
      for (const u of g) {
        if (_AD_RE.test(u.seg)) { removed++; continue; }
        out.push(...u.tags.map(wrapUri));
        out.push('/kodik/hls?url=' + encodeURIComponent(u.seg));
      }
    }
    if (removed && !out.some(l => l.startsWith('/kodik/hls')))
      return r.type('application/vnd.apple.mpegurl').send(buf);
    let body = ['#EXTM3U', ...header, ...out, ...trailer];
    if (removed) body.splice(1, 0, '# AniHub: вырезано рекламных сегментов: ' + removed);
    r.type('application/vnd.apple.mpegurl').send(body.join('\n'));
  } catch (e) {
    r.status(502).send('kodik hls: ' + e.message);
  }
});
/* ==== конец Kodik-шлюза ==================================================== */

/* ==== v17.42 · Живые карточки и постеры: случайный отрывок из любой серии ==== */
const KODIK_PUB_TOKEN = process.env.KODIK_TOKEN || '56a768d08f43091901c44b54fe970049';

function parseKodikLink(link) {
  const m = String(link || '').match(/\/(seria|serial|video|season|movie)\/(\d+)\/([a-f0-9]+)/i);
  return m ? { type: m[1], id: m[2], hash: m[3] } : null;
}

async function listTitleEpisodes(shikiId, title) {
  const ck = 'eps|' + (shikiId || 0) + '|' + String(title || '').toLowerCase().slice(0, 60);
  const hit = kCacheGet(ck, 3600000);
  if (hit) return hit;
  const queries = [];
  if (shikiId > 0) queries.push(`token=${encodeURIComponent(KODIK_PUB_TOKEN)}&shikimori_id=${encodeURIComponent(shikiId)}&with_episodes=true&limit=5`);
  if (title) queries.push(`token=${encodeURIComponent(KODIK_PUB_TOKEN)}&title=${encodeURIComponent(title)}&types=anime,anime-serial&with_episodes=true&limit=5`);
  const eps = [];
  const seen = new Set();
  for (const qs of queries) {
    try {
      const resp = await kGet(KODIK_UPSTREAM + '/search?' + qs, 10000);
      const data = await resp.json();
      const results = Array.isArray(data && data.results) ? data.results : [];
      for (const r0 of results) {
        const seasons = r0 && r0.seasons && typeof r0.seasons === 'object' ? Object.entries(r0.seasons) : [];
        if (seasons.length) {
          for (const [sNum, sObj] of seasons) {
            const epMap = sObj && sObj.episodes && typeof sObj.episodes === 'object' ? Object.entries(sObj.episodes) : [];
            for (const [epNum, epVal] of epMap) {
              const lnk = typeof epVal === 'string' ? epVal : (epVal && epVal.link);
              const p = parseKodikLink(lnk);
              if (p && !seen.has(p.id)) { seen.add(p.id); eps.push({ ep: (seasons.length > 1 ? sNum + 'x' : '') + epNum, ...p }); }
            }
          }
        }
        if (!eps.length && r0 && r0.link) {
          const p = parseKodikLink(r0.link);
          if (p && !seen.has(p.id)) { seen.add(p.id); eps.push({ ep: '1', ...p }); }
        }
        if (eps.length >= 3) break;
      }
      if (eps.length) break;
    } catch {}
  }
  if (eps.length) kCacheSet(ck, eps);
  return eps;
}

async function fetchManifestSegments(manifestUrl) {
  const ck = 'm3segs|' + manifestUrl;
  const hit = kCacheGet(ck, 1200000);
  if (hit) return hit;
  let curUrl = manifestUrl;
  let text = await (await kGet(curUrl, 14000)).text();
  if (text.includes('#EXT-X-STREAM-INF')) {
    const lines = text.split('\n').map(x => x.trim()).filter(Boolean);
    const sub = lines.find(l => !l.startsWith('#'));
    if (sub) {
      curUrl = new URL(sub, curUrl).toString();
      text = await (await kGet(curUrl, 14000)).text();
    }
  }
  const segs = [];
  let dur = 6, keyLine = '', disco = false;
  for (const raw of text.split('\n')) {
    const s = raw.trim();
    if (!s || s === '#EXTM3U') continue;
    if (s.startsWith('#EXT-X-DISCONTINUITY') && !s.startsWith('#EXT-X-DISCONTINUITY-SEQUENCE')) { disco = true; continue; }
    if (s.startsWith('#EXT-X-KEY:')) { keyLine = s; continue; }
    if (s.startsWith('#EXTINF:')) {
      const m = s.match(/#EXTINF:([0-9.]+)/);
      dur = m ? (parseFloat(m[1]) || 6) : 6;
      continue;
    }
    if (!s.startsWith('#')) {
      const abs = new URL(s, curUrl).toString();
      if (!_AD_RE.test(abs)) segs.push({ dur, url: abs, keyLine, baseUrl: curUrl, disco });
      disco = false;
    }
  }
  if (segs.length) kCacheSet(ck, segs);
  return segs;
}

function sliceSegmentsToM3u8(segs, proxyPrefix) {
  const n = segs.length;
  let idx = 0;
  if (n >= 8) {
    const lo = Math.floor(n * 0.20);
    const hi = Math.max(lo, Math.floor(n * 0.78) - 2);
    idx = lo + Math.floor(Math.random() * (hi - lo + 1));
  } else if (n >= 3) {
    idx = Math.floor(n / 2) - 1;
  }
  const s0 = segs[idx];
  const slice = [s0];
  if (segs[idx + 1] && !segs[idx + 1].disco) slice.push(segs[idx + 1]);
  const maxDur = Math.max(6, Math.ceil(Math.max(...slice.map(x => x.dur || 6))));
  const wrapUrl = u => proxyPrefix + '/kodik/hls?url=' + encodeURIComponent(u);
  const out = [
    '#EXTM3U',
    '#EXT-X-VERSION:3',
    '#EXT-X-TARGETDURATION:' + maxDur,
    '#EXT-X-MEDIA-SEQUENCE:1',
    '#EXT-X-PLAYLIST-TYPE:VOD'
  ];
  let lastKey = '';
  for (const sg of slice) {
    if (sg.keyLine && sg.keyLine !== lastKey) {
      lastKey = sg.keyLine;
      out.push(sg.keyLine.replace(_URI_RE, (_, u1) => 'URI="' + wrapUrl(new URL(u1, sg.baseUrl).toString()) + '"'));
    }
    out.push('#EXTINF:' + (sg.dur || 6).toFixed(3) + ',');
    out.push(wrapUrl(sg.url));
  }
  out.push('#EXT-X-ENDLIST');
  const approxSec = Math.round(segs.slice(0, idx).reduce((a, b) => a + (b.dur || 6), 0));
  const durSec = Math.round(slice.reduce((a, b) => a + (b.dur || 6), 0));
  return { m3u8: out.join('\n'), idx, totalSegs: n, approxSec, durSec };
}

async function buildLiveClip({ shiki, mal, libria, title, q, baseUrl }) {
  const shikiId = Number(shiki || mal || 0) || 0;
  const libriaId = Number(libria || 0) || 0;
  const wantOrder = String(q) === '480' ? ['480', '360', '720'] : ['360', '480', '720'];
  const fastKey = 'fastsegs|' + (shikiId || libriaId || String(title || '').toLowerCase().slice(0, 40)) + '|' + wantOrder[0];
  const fastHit = kCacheGet(fastKey, 900000);
  if (fastHit && fastHit.segs && fastHit.segs.length) {
    const cut = sliceSegmentsToM3u8(fastHit.segs, baseUrl);
    return { ok: true, kind: 'hls', source: fastHit.source, ep: fastHit.ep, atSec: cut.approxSec, durSec: cut.durSec, m3u8: cut.m3u8 };
  }

  /* 1. Основной источник: случайная серия из Kodik -> вырезка случайных 10-12 сек из середины серии */
  const eps = await listTitleEpisodes(shikiId, title);
  if (eps.length) {
    const shuffled = [...eps].sort(() => Math.random() - 0.5).slice(0, 3);
    for (const pick of shuffled) {
      try {
        const res = await kodikResolveInternal(pick.type, pick.id, pick.hash);
        const qKey = wantOrder.find(k => res.links && res.links[k]) || (res.links && Object.keys(res.links)[0]);
        if (!qKey) continue;
        const segs = await fetchManifestSegments(res.links[qKey]);
        if (!segs.length) continue;
        kCacheSet(fastKey, { segs, source: 'kodik', ep: pick.ep });
        const cut = sliceSegmentsToM3u8(segs, baseUrl);
        return {
          ok: true,
          kind: 'hls',
          source: 'kodik',
          ep: pick.ep,
          atSec: cut.approxSec,
          durSec: cut.durSec,
          m3u8: cut.m3u8
        };
      } catch {}
    }
  }

  /* 2. Запасной источник: AniLibria HLS (вырезка случайных 2 сегментов из случайной серии) */
  if (libriaId > 0) {
    try {
      const r = await kGet(`https://anilibria.top/api/v1/anime/releases/${encodeURIComponent(libriaId)}`, 9000);
      const rel = await r.json();
      const rEps = Array.isArray(rel && rel.episodes) ? rel.episodes : [];
      if (rEps.length) {
        const ep = rEps[Math.floor(Math.random() * rEps.length)];
        const mUrl = (ep && (ep.hls_480 || ep.hls_720 || ep.hls_1080)) || '';
        if (mUrl) {
          const segs = await fetchManifestSegments(mUrl);
          if (segs.length) {
            const cut = sliceSegmentsToM3u8(segs, baseUrl);
            return { ok: true, kind: 'hls', source: 'anilibria', ep: String(ep.ordinal || 1), atSec: cut.approxSec, durSec: cut.durSec, m3u8: cut.m3u8 };
          }
        }
      }
    } catch {}
  }

  /* 3. Запасной источник: AnimeThemes (прямой видеоролик по MAL/Shikimori ID) */
  if (shikiId > 0) {
    try {
      const u = `https://api.animethemes.moe/anime?filter%5Bhas%5D=resources&filter%5Bsite%5D=MyAnimeList&filter%5Bexternal_id%5D=${encodeURIComponent(shikiId)}&include=animethemes.animethemeentries.videos`;
      const r = await kGet(u, 8000);
      const d = await r.json();
      const vids = [];
      for (const an of (d && d.anime) || []) {
        for (const th of (an && an.animethemes) || []) {
          for (const en of (th && th.animethemeentries) || []) {
            for (const vd of (en && en.videos) || []) {
              if (vd && vd.link) vids.push(vd.link);
            }
          }
        }
      }
      if (vids.length) {
        const vUrl = vids[Math.floor(Math.random() * vids.length)];
        return { ok: true, kind: 'video', source: 'animethemes', ep: 'OP/ED', atSec: 22 + Math.floor(Math.random() * 35), durSec: 10, url: vUrl };
      }
    } catch {}
  }

  return { ok: false, error: 'отрывок не найден' };
}

function reqPublicBase(req) {
  const xfProto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  const proto = xfProto || req.protocol || 'https';
  const host = req.headers['x-forwarded-host'] || req.get('host') || ('localhost:' + PORT);
  return proto + '://' + host;
}

app.get('/api/live-clip', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const baseUrl = reqPublicBase(req);
    const out = await buildLiveClip({
      shiki: req.query.shiki,
      mal: req.query.mal,
      libria: req.query.libria,
      title: req.query.title,
      q: req.query.q,
      baseUrl
    });
    if (!out.ok) return res.status(404).json(out);
    const qs = new URLSearchParams();
    if (req.query.shiki) qs.set('shiki', String(req.query.shiki));
    if (req.query.mal) qs.set('mal', String(req.query.mal));
    if (req.query.libria) qs.set('libria', String(req.query.libria));
    if (req.query.title) qs.set('title', String(req.query.title));
    if (req.query.q) qs.set('q', String(req.query.q));
    out.playlistUrl = baseUrl + '/api/live-clip.m3u8?' + qs.toString();
    res.json(out);
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e && e.message || e) });
  }
});

app.get('/api/live-clip.m3u8', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const baseUrl = reqPublicBase(req);
    const out = await buildLiveClip({
      shiki: req.query.shiki,
      mal: req.query.mal,
      libria: req.query.libria,
      title: req.query.title,
      q: req.query.q,
      baseUrl
    });
    if (!out.ok || !out.m3u8) return res.status(404).type('text/plain').send('# no clip');
    res.type('application/vnd.apple.mpegurl').send(out.m3u8);
  } catch (e) {
    res.status(502).type('text/plain').send('# err');
  }
});

/* ==== v17.44: Дополнительные безрекламные источники (Aksor CMAF→HLS, AnimeVost зеркало) ==== */
function parseMpdAdaptation(xml, kind) {
  const re = new RegExp('<AdaptationSet[^>]*contentType="' + kind + '"[^>]*>([\\s\\S]*?)<\\/AdaptationSet>', 'i');
  const m = String(xml || '').match(re);
  if (!m) return null;
  const block = m[1];
  const repTag = (block.match(/<Representation[^>]*>/i) || [''])[0];
  const tplTag = (block.match(/<SegmentTemplate[^>]*>/i) || [''])[0];
  const attr = (s, k) => ((s.match(new RegExp('\\b' + k + '="([^"]*)"', 'i')) || [])[1]) || '';
  const id = attr(repTag, 'id') || (kind === 'video' ? '0' : '1');
  const codecs = attr(repTag, 'codecs') || (kind === 'video' ? 'avc1.4d4028' : 'mp4a.40.2');
  const bandwidth = parseInt(attr(repTag, 'bandwidth'), 10) || (kind === 'video' ? 1800000 : 160000);
  const width = parseInt(attr(repTag, 'width'), 10) || 1920;
  const height = parseInt(attr(repTag, 'height'), 10) || 1080;
  const timescale = Math.max(1, parseInt(attr(tplTag, 'timescale'), 10) || 1000);
  const startNum = parseInt(attr(tplTag, 'startNumber'), 10) || 1;
  const initTpl = attr(tplTag, 'initialization') || 'init-stream$RepresentationID$.m4s';
  const mediaTpl = attr(tplTag, 'media') || 'chunk-stream$RepresentationID$-$Number%05d$.m4s';
  const fmtName = (tpl, num) => tpl
    .replace(/\$RepresentationID\$/g, id)
    .replace(/\$Number(?:%0(\d+)d)?\$/g, (_, pad) => pad ? String(num).padStart(parseInt(pad, 10), '0') : String(num));
  const segs = [];
  let num = startNum;
  const tlMatch = block.match(/<SegmentTimeline>([\s\S]*?)<\/SegmentTimeline>/i);
  if (tlMatch) {
    const sRe = /<S\b([^>]*)\/?>/gi;
    let sm;
    while ((sm = sRe.exec(tlMatch[1]))) {
      const d = parseInt(attr(sm[1], 'd'), 10) || 0;
      const r = parseInt(attr(sm[1], 'r'), 10) || 0;
      if (d <= 0) continue;
      for (let i = 0; i <= Math.max(0, Math.min(r, 5000)); i++) {
        segs.push({ dur: d / timescale, file: fmtName(mediaTpl, num++) });
      }
    }
  }
  if (!segs.length) return null;
  return { id, codecs, bandwidth, width, height, initFile: fmtName(initTpl, startNum), segs };
}

function buildFmp4MediaM3u8(track, dirUrl) {
  const maxDur = Math.max(4, Math.ceil(Math.max(...track.segs.map(s => s.dur))));
  const lines = [
    '#EXTM3U',
    '#EXT-X-VERSION:7',
    '#EXT-X-PLAYLIST-TYPE:VOD',
    '#EXT-X-INDEPENDENT-SEGMENTS',
    '#EXT-X-TARGETDURATION:' + maxDur,
    '#EXT-X-MEDIA-SEQUENCE:0',
    '#EXT-X-MAP:URI="' + dirUrl + track.initFile + '"'
  ];
  for (const s of track.segs) {
    lines.push('#EXTINF:' + s.dur.toFixed(6) + ',');
    lines.push(dirUrl + s.file);
  }
  lines.push('#EXT-X-ENDLIST');
  return lines.join('\n') + '\n';
}

async function fetchAksorMpd(rawMpd) {
  const safe = await egressCheck(rawMpd);
  const ck = 'mpd:' + safe;
  let xml = kCacheGet(ck, 30 * 60e3);
  if (!xml) {
    const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA }, signal: AbortSignal.timeout(12000) });
    if (!r.ok) throw new Error('MPD HTTP ' + r.status);
    xml = await r.text();
    kCacheSet(ck, xml);
  }
  const dirUrl = safe.replace(/[^/?#]+(\?.*)?$/, '');
  return { xml, dirUrl, safe };
}

function parseMpdSubtitles(xml, dirUrl) {
  const out = [], seen = new Set();
  const normLang = l => {
    const s = String(l || '').toLowerCase();
    if (/^(ru|rus|russian)/.test(s) || /рус/i.test(s)) return 'ru';
    if (/^(en|eng|english)/.test(s) || /англ/i.test(s)) return 'en';
    if (/^(ja|jp|jpn|japanese)/.test(s) || /яп/i.test(s)) return 'ja';
    return s.slice(0, 6) || 'ru';
  };
  const adRe = /<AdaptationSet\b([^>]*)>([\s\S]*?)<\/AdaptationSet>/gi;
  let am;
  while ((am = adRe.exec(String(xml || '')))) {
    const aHead = am[1], aBody = am[2];
    if (/contentType\s*=\s*"(video|audio)"/i.test(aHead)) continue;
    const aMime = (aHead.match(/mimeType="([^"]+)"/i) || [])[1] || '';
    const aLang = (aHead.match(/lang="([^"]+)"/i) || [])[1] || '';
    const aLabel = (aHead.match(/label="([^"]+)"/i) || [])[1] || '';
    const repRe = /<Representation\b([^>]*)>([\s\S]*?)<\/Representation>/gi;
    let rm;
    while ((rm = repRe.exec(aBody))) {
      const rHead = rm[1], rBody = rm[2];
      const mime = ((rHead.match(/mimeType="([^"]+)"/i) || [])[1] || aMime).toLowerCase();
      const baseM = rBody.match(/<BaseURL>([^<]+)<\/BaseURL>/i) || aBody.match(/<BaseURL>([^<]+)<\/BaseURL>/i);
      if (!baseM) continue;
      const rawU = baseM[1].trim();
      const isAss = /ass|ssa/.test(mime) || /\.(ass|ssa)(\?|$)/i.test(rawU);
      const isVtt = /vtt|srt/.test(mime) || /\.(vtt|srt)(\?|$)/i.test(rawU);
      if (!isAss && !isVtt) continue;
      let fullU = rawU;
      try { fullU = new URL(rawU, dirUrl).href; } catch {}
      if (seen.has(fullU)) continue;
      seen.add(fullU);
      const rLang = (rHead.match(/lang="([^"]+)"/i) || [])[1] || aLang;
      const rLabel = (rHead.match(/label="([^"]+)"/i) || [])[1] || aLabel;
      const lang = normLang(rLang || rLabel);
      const fmt = isAss ? 'ass' : (/\.srt(\?|$)/i.test(fullU) ? 'srt' : 'vtt');
      const langName = { ru: 'Русские', en: 'Английские', ja: 'Японские' }[lang] || (rLabel || lang.toUpperCase());
      out.push({
        id: 'aksor-' + out.length + '-' + lang,
        lang,
        format: fmt,
        label: langName + (rLabel && !/^(ru|rus|en|eng|ja|jpn)$/i.test(rLabel) ? ' · ' + rLabel : '') + ' (' + fmt.toUpperCase() + ')',
        url: fullU
      });
    }
  }
  return out;
}

app.get('/api/aksor/video', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const id = String(req.query.id || '').replace(/[^a-zA-Z0-9_-]/g, '');
  if (!id) return res.status(400).json({ ok: false, error: 'нужен id видео' });
  try {
    const ck = 'aksor:' + id;
    let data = kCacheGet(ck, 20 * 60e3);
    if (!data) {
      const safe = await egressCheck('https://player.aksor.tv/api/video/' + id);
      const r = await fetch(safe, {
        headers: { 'User-Agent': KODIK_UA, 'Referer': 'https://player.aksor.tv/', 'Accept': 'application/json' },
        signal: AbortSignal.timeout(12000)
      });
      if (!r.ok) throw new Error('Aksor HTTP ' + r.status);
      data = await r.json();
      kCacheSet(ck, data);
    }
    const baseUrl = reqPublicBase(req);
    const q = (data && data.qualities) || {};
    const qOrder = [['q2k', '1440', '1440p'], ['q1080', '1080', '1080p'], ['q720', '720', '720p'], ['q480', '480', '480p'], ['q360', '360', '360p']];
    const sources = [];
    let firstMpd = '';
    for (const [k, qv, lbl] of qOrder) {
      const u = q[k];
      if (!u || typeof u !== 'string') continue;
      if (/\.mpd(\?|$)/i.test(u)) {
        if (!firstMpd) firstMpd = u;
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'hls', url: baseUrl + '/api/aksor/hls.m3u8?mpd=' + encodeURIComponent(u) });
      } else if (/\.m3u8(\?|$)/i.test(u)) {
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'hls', url: u });
      } else if (/\.mp4(\?|$)/i.test(u)) {
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'mp4', url: u });
      }
    }
    if (!sources.length) return res.status(404).json({ ok: false, error: 'потоки Aksor не найдены' });
    let subs = [];
    if (firstMpd) {
      try {
        const { xml, dirUrl } = await fetchAksorMpd(firstMpd);
        subs = parseMpdSubtitles(xml, dirUrl);
      } catch {}
    }
    const opS = +data.opening_start || 0, opL = +data.opening_length || 0;
    const edS = +data.ending_start || 0, edL = +data.ending_length || 0;
    res.json({
      ok: true,
      id,
      studio: String(data.studio_name || 'Русская озвучка'),
      episode: String(data.episode || ''),
      isSub: !!data.is_subtitle,
      opening: opL > 0 ? [Math.round(opS), Math.round(opS + opL)] : null,
      ending: edL > 0 ? [Math.round(edS), Math.round(edS + edL)] : null,
      subs,
      sources
    });
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e && e.message || e) });
  }
});

app.get('/api/aksor/hls.m3u8', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const rawMpd = String(req.query.mpd || '');
    const { xml } = await fetchAksorMpd(rawMpd);
    const v = parseMpdAdaptation(xml, 'video');
    const a = parseMpdAdaptation(xml, 'audio');
    if (!v) return res.status(404).type('text/plain').send('# no video adaptation');
    const baseUrl = reqPublicBase(req);
    const enc = encodeURIComponent(rawMpd);
    const lines = ['#EXTM3U', '#EXT-X-VERSION:7', '#EXT-X-INDEPENDENT-SEGMENTS'];
    if (a) {
      lines.push(`#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Russian",LANGUAGE="ru",DEFAULT=YES,AUTOSELECT=YES,URI="${baseUrl}/api/aksor/audio.m3u8?mpd=${enc}"`);
      lines.push(`#EXT-X-STREAM-INF:BANDWIDTH=${v.bandwidth + a.bandwidth},RESOLUTION=${v.width}x${v.height},CODECS="${v.codecs},${a.codecs}",AUDIO="aud"`);
    } else {
      lines.push(`#EXT-X-STREAM-INF:BANDWIDTH=${v.bandwidth},RESOLUTION=${v.width}x${v.height},CODECS="${v.codecs}"`);
    }
    lines.push(`${baseUrl}/api/aksor/video.m3u8?mpd=${enc}`);
    res.type('application/vnd.apple.mpegurl').send(lines.join('\n') + '\n');
  } catch (e) {
    res.status(502).type('text/plain').send('# err: ' + String(e && e.message || e));
  }
});

app.get('/api/aksor/video.m3u8', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const { xml, dirUrl } = await fetchAksorMpd(String(req.query.mpd || ''));
    const v = parseMpdAdaptation(xml, 'video');
    if (!v) return res.status(404).type('text/plain').send('# no video');
    res.type('application/vnd.apple.mpegurl').send(buildFmp4MediaM3u8(v, dirUrl));
  } catch (e) {
    res.status(502).type('text/plain').send('# err');
  }
});

app.get('/api/aksor/audio.m3u8', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const { xml, dirUrl } = await fetchAksorMpd(String(req.query.mpd || ''));
    const a = parseMpdAdaptation(xml, 'audio');
    if (!a) return res.status(404).type('text/plain').send('# no audio');
    res.type('application/vnd.apple.mpegurl').send(buildFmp4MediaM3u8(a, dirUrl));
  } catch (e) {
    res.status(502).type('text/plain').send('# err');
  }
});

app.get('/api/vost/resolve', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const id = parseInt(req.query.id, 10) || 0;
    if (id > 0) {
      const ck = 'vostpl:' + id;
      let pl = kCacheGet(ck, 30 * 60e3);
      if (!pl) {
        const safe = await egressCheck('https://api.animevost.org/v1/playlist');
        const r = await fetch(safe, {
          method: 'POST',
          headers: { 'User-Agent': KODIK_UA, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: 'id=' + encodeURIComponent(id),
          signal: AbortSignal.timeout(12000)
        });
        if (!r.ok) throw new Error('AnimeVost HTTP ' + r.status);
        pl = await r.json();
        kCacheSet(ck, pl);
      }
      return res.json({ ok: true, items: Array.isArray(pl) ? pl : [] });
    }
    const q = String(req.query.q || '').trim().slice(0, 120);
    if (!q) return res.status(400).json({ ok: false, error: 'нужен q или id' });
    const ck = 'vostq:' + q.toLowerCase();
    let sr = kCacheGet(ck, 20 * 60e3);
    if (!sr) {
      const safe = await egressCheck('https://api.animevost.org/v1/search');
      const r = await fetch(safe, {
        method: 'POST',
        headers: { 'User-Agent': KODIK_UA, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: 'name=' + encodeURIComponent(q),
        signal: AbortSignal.timeout(12000)
      });
      if (!r.ok) throw new Error('AnimeVost HTTP ' + r.status);
      sr = await r.json();
      kCacheSet(ck, sr);
    }
    res.json({ ok: true, data: (sr && Array.isArray(sr.data)) ? sr.data : [] });
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e && e.message || e) });
  }
});

/* ---- v17.45: серверный конвертер ASS/SSA -> VTT + поиск и прокси внешних субтитров (RU / EN / JA) ---- */
const CP1252_REV = { 0x20AC: 0x80, 0x201A: 0x82, 0x0192: 0x83, 0x201E: 0x84, 0x2026: 0x85, 0x2020: 0x86, 0x2021: 0x87, 0x02C6: 0x88, 0x2030: 0x89, 0x0160: 0x8A, 0x2039: 0x8B, 0x0152: 0x8C, 0x017D: 0x8E, 0x2018: 0x91, 0x2019: 0x92, 0x201C: 0x93, 0x201D: 0x94, 0x2022: 0x95, 0x2013: 0x96, 0x2014: 0x97, 0x02DC: 0x98, 0x2122: 0x99, 0x0161: 0x9A, 0x203A: 0x9B, 0x0153: 0x9C, 0x017E: 0x9E, 0x0178: 0x9F };
function fixMojibakeServer(str) {
  if (!str || !/[ÃÂÐÑãäåæçèéï][\x80-\xff\u0152-\u2122]/.test(str)) return str;
  const bytes = new Uint8Array(str.length);
  for (let i = 0; i < str.length; i++) {
    const c = str.charCodeAt(i);
    if (c <= 0xff) bytes[i] = c;
    else if (CP1252_REV[c] !== undefined) bytes[i] = CP1252_REV[c];
    else return str;
  }
  try { return new TextDecoder('utf-8', { fatal: true }).decode(bytes); } catch { return str; }
}
function decodeSubBuffer(buf) {
  if (!buf || !buf.length) return '';
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return new TextDecoder('utf-16le').decode(buf.subarray(2));
  }
  if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff) {
    return new TextDecoder('utf-16be').decode(buf.subarray(2));
  }
  try {
    const utf8 = new TextDecoder('utf-8', { fatal: true }).decode(buf).replace(/^\uFEFF/, '');
    return fixMojibakeServer(utf8);
  } catch {
    try { return new TextDecoder('windows-1251').decode(buf); } catch {}
    return new TextDecoder('utf-8', { fatal: false }).decode(buf).replace(/^\uFEFF/, '');
  }
}

function assTimeToSec(str) {
  const m = String(str || '').trim().match(/^(\d+):(\d{1,2}):(\d{1,2})[.,](\d{1,3})$/);
  if (!m) return NaN;
  const frac = m[4].length === 1 ? (+m[4] * 100) : (m[4].length === 2 ? (+m[4] * 10) : +m[4]);
  return (+m[1]) * 3600 + (+m[2]) * 60 + (+m[3]) + frac / 1000;
}

function secToVttTime(sec) {
  const totalMs = Math.max(0, Math.round((+sec || 0) * 1000));
  const ms = totalMs % 1000;
  const totalS = Math.floor(totalMs / 1000);
  const s = totalS % 60;
  const totalM = Math.floor(totalS / 60);
  const m = totalM % 60;
  const h = Math.floor(totalM / 60);
  return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + ':' +
    String(s).padStart(2, '0') + '.' + String(ms).padStart(3, '0');
}

function assToVttServer(rawAss) {
  const lines = String(rawAss || '').replace(/\r\n?/g, '\n').split('\n');
  let resX = 1280, resY = 720, sec = '';
  let styleFmt = ['name', 'fontname', 'fontsize', 'primarycolour', 'secondarycolour', 'outlinecolour', 'backcolour', 'bold', 'italic', 'underline', 'strikeout', 'scalex', 'scaley', 'spacing', 'angle', 'borderstyle', 'outline', 'shadow', 'alignment'];
  let evtFmt = ['layer', 'start', 'end', 'style', 'name', 'marginl', 'marginr', 'marginv', 'effect', 'text'];
  const styleAlign = new Map([['default', 2]]);
  const cues = [];

  for (const raw of lines) {
    const line = raw.trim();
    if (!line || line.startsWith(';')) continue;
    const secM = line.match(/^\[([^\]]+)\]$/);
    if (secM) { sec = secM[1].toLowerCase(); continue; }
    if (sec === 'script info') {
      const xm = line.match(/^PlayResX\s*:\s*(\d+)/i);
      const ym = line.match(/^PlayResY\s*:\s*(\d+)/i);
      if (xm && +xm[1] > 0) resX = +xm[1];
      if (ym && +ym[1] > 0) resY = +ym[1];
    } else if (sec.includes('styles')) {
      if (/^Format\s*:/i.test(line)) {
        styleFmt = line.replace(/^Format\s*:/i, '').split(',').map(x => x.trim().toLowerCase());
      } else if (/^Style\s*:/i.test(line)) {
        const parts = line.replace(/^Style\s*:/i, '').split(',');
        const iName = styleFmt.indexOf('name');
        const iAl = styleFmt.indexOf('alignment');
        if (iName >= 0 && parts[iName]) {
          let al = iAl >= 0 ? (parseInt(parts[iAl], 10) || 2) : 2;
          if (!sec.includes('+') && al >= 5) al = al === 6 ? 8 : (al === 5 ? 7 : (al === 7 ? 9 : 2));
          styleAlign.set(parts[iName].trim().replace(/^\*+/, '').toLowerCase(), al);
        }
      }
    } else if (sec === 'events') {
      if (/^Format\s*:/i.test(line)) {
        evtFmt = line.replace(/^Format\s*:/i, '').split(',').map(x => x.trim().toLowerCase());
      } else if (/^Dialogue\s*:/i.test(raw.trimStart())) {
        const rest = raw.trimStart().replace(/^Dialogue\s*:\s*/i, '');
        const maxC = Math.max(1, evtFmt.length - 1);
        const cols = [];
        let cur = 0;
        for (let i = 0; i < maxC; i++) {
          const idx = rest.indexOf(',', cur);
          if (idx === -1) break;
          cols.push(rest.slice(cur, idx).trim());
          cur = idx + 1;
        }
        cols.push(rest.slice(cur));
        const iS = evtFmt.indexOf('start'), iE = evtFmt.indexOf('end'), iSt = evtFmt.indexOf('style'), iTx = evtFmt.indexOf('text');
        const s = assTimeToSec(cols[iS >= 0 ? iS : 1]);
        const e = assTimeToSec(cols[iE >= 0 ? iE : 2]);
        const rawText = cols[iTx >= 0 ? iTx : cols.length - 1] || '';
        if (!Number.isFinite(s) || !Number.isFinite(e) || e <= s || !rawText) continue;
        if (/\{\\[^}]*\\p[1-9][^}]*\}/.test(rawText) && !/\\p0/.test(rawText)) continue;
        const stName = String(cols[iSt >= 0 ? iSt : 3] || 'default').replace(/^\*+/, '').toLowerCase();
        let align = styleAlign.get(stName) || 2;
        const anM = rawText.match(/\\an([1-9])/);
        if (anM) align = +anM[1];
        const posM = rawText.match(/\\pos\(\s*([0-9.]+)\s*,\s*([0-9.]+)\s*\)/);
        let cleaned = rawText
          .replace(/\{[^}]*\\p[1-9][^}]*\}[\s\S]*?(\{[^}]*\\p0[^}]*\}|$)/g, '')
          .replace(/\{\\i1\}/gi, '<i>').replace(/\{\\i0\}/gi, '</i>')
          .replace(/\{\\b1\}/gi, '<b>').replace(/\{\\b0\}/gi, '</b>')
          .replace(/\{[^}]*\}/g, '')
          .replace(/\\[Nn]/g, '\n')
          .replace(/\\h/g, ' ')
          .trim();
        if (!cleaned) continue;
        const openI = (cleaned.match(/<i>/g) || []).length - (cleaned.match(/<\/i>/g) || []).length;
        const openB = (cleaned.match(/<b>/g) || []).length - (cleaned.match(/<\/b>/g) || []).length;
        if (openI > 0) cleaned += '</i>'.repeat(openI);
        if (openB > 0) cleaned += '</b>'.repeat(openB);
        const settings = [];
        if (posM) {
          const px = Math.max(0, Math.min(100, Math.round((+posM[1] / resX) * 100)));
          const py = Math.max(0, Math.min(100, Math.round((+posM[2] / resY) * 100)));
          settings.push('line:' + py + '%', 'position:' + px + '%');
        } else if (align >= 7) {
          settings.push('line:8%');
        } else if (align >= 4) {
          settings.push('line:50%');
        }
        if (align === 1 || align === 4 || align === 7) settings.push('align:start');
        else if (align === 3 || align === 6 || align === 9) settings.push('align:end');
        cues.push({ s, e, settings: settings.join(' '), text: cleaned });
      }
    }
  }
  cues.sort((a, b) => a.s - b.s || a.e - b.e);
  const out = ['WEBVTT', ''];
  for (const c of cues) {
    out.push(secToVttTime(c.s) + ' --> ' + secToVttTime(c.e) + (c.settings ? ' ' + c.settings : ''));
    out.push(c.text);
    out.push('');
  }
  return out.join('\n');
}

app.post('/api/subs/ass-to-vtt', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const ass = String((req.body && (req.body.ass || req.body.text)) || '');
  if (!ass) return res.status(400).json({ ok: false, error: 'нужен текст ASS/SSA' });
  if (ass.length > 2_000_000) return res.status(413).json({ ok: false, error: 'файл субтитров слишком большой' });
  const vtt = assToVttServer(ass);
  res.json({ ok: true, vtt });
});

app.get('/api/subs/ass-to-vtt', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const rawUrl = String(req.query.url || '').trim();
    if (!rawUrl) return res.status(400).type('text/plain').send('WEBVTT\n');
    const safe = await egressCheck(rawUrl);
    const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA }, signal: AbortSignal.timeout(12000) });
    if (!r.ok) throw new Error('Sub HTTP ' + r.status);
    const buf = new Uint8Array(await r.arrayBuffer());
    if (buf.length > 3_000_000) throw new Error('Sub file too large');
    const txt = decodeSubBuffer(buf);
    res.type('text/vtt; charset=utf-8').send(assToVttServer(txt));
  } catch (e) {
    res.status(502).type('text/plain').send('# err: ' + String(e && e.message || e));
  }
});

app.get('/api/subs/fetch', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    const rawUrl = String(req.query.url || '').trim();
    if (!rawUrl) return res.status(400).json({ ok: false, error: 'нужен url' });
    const safe = await egressCheck(rawUrl);
    const ck = 'subf:' + safe;
    let txt = kCacheGet(ck, 60 * 60e3);
    if (!txt) {
      const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA }, signal: AbortSignal.timeout(12000) });
      if (!r.ok) throw new Error('Sub HTTP ' + r.status);
      const buf = new Uint8Array(await r.arrayBuffer());
      if (buf.length > 3_000_000) throw new Error('Sub file too large');
      txt = decodeSubBuffer(buf);
      kCacheSet(ck, txt);
    }
    res.type('text/plain; charset=utf-8').send(txt);
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e && e.message || e) });
  }
});

app.get('/api/subs/search', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const mal = parseInt(req.query.mal, 10) || 0;
  const anilist = parseInt(req.query.anilist, 10) || 0;
  let ep = Math.max(1, parseInt(req.query.ep, 10) || 1);
  const seasonHint = Math.max(1, parseInt(req.query.s, 10) || 1);
  let isMovie = req.query.movie === '1' || req.query.movie === 'true';
  let imdb = String(req.query.imdb || '').replace(/[^a-zA-Z0-9]/g, '');
  try {
    let tmdbSeason = seasonHint;
    let absEp = 0;
    if (!imdb && (mal > 0 || anilist > 0)) {
      const azKey = mal > 0 ? ('az:m:' + mal) : ('az:a:' + anilist);
      let az = kCacheGet(azKey, 12 * 3600e3);
      if (!az) {
        try {
          const qs = mal > 0 ? ('mal_id=' + mal) : ('anilist_id=' + anilist);
          const safeAz = await egressCheck('https://api.ani.zip/mappings?' + qs);
          const rz = await fetch(safeAz, { headers: { 'User-Agent': KODIK_UA, 'Accept': 'application/json' }, signal: AbortSignal.timeout(8000) });
          if (rz.ok) {
            az = await rz.json();
            kCacheSet(azKey, az);
          }
        } catch {}
      }
      if (az && az.mappings && az.mappings.imdb_id) {
        imdb = String(az.mappings.imdb_id).replace(/[^a-zA-Z0-9]/g, '');
        if (az.mappings.type === 'MOVIE') isMovie = true;
        const epInfo = az.episodes && (az.episodes[String(ep)] || az.episodes[ep]);
        if (epInfo && seasonHint === 1) {
          if (+epInfo.seasonNumber > 0) tmdbSeason = +epInfo.seasonNumber;
          if (+epInfo.episodeNumber > 0) ep = +epInfo.episodeNumber;
          if (+epInfo.absoluteEpisodeNumber > 0) absEp = +epInfo.absoluteEpisodeNumber;
        }
      }
    }
    if (!imdb && mal > 0) {
      const mck = 'arm:' + mal;
      let ids = kCacheGet(mck, 12 * 3600e3);
      if (!ids) {
        const safe = await egressCheck('https://arm.haglund.dev/api/v2/ids?source=myanimelist&id=' + mal);
        const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA, 'Accept': 'application/json' }, signal: AbortSignal.timeout(8000) });
        if (r.ok) {
          ids = await r.json();
          kCacheSet(mck, ids);
        }
      }
      if (ids && ids.imdb) imdb = String(ids.imdb).replace(/[^a-zA-Z0-9]/g, '');
      if (ids && +ids['themoviedb-season'] > 0) tmdbSeason = +ids['themoviedb-season'];
    }
    if (!imdb || !/^tt\d+$/.test(imdb)) {
      return res.json({ ok: true, imdb: '', subs: [] });
    }
    const fetchOs = async pathPart => {
      const ck = 'osub:' + pathPart;
      let j = kCacheGet(ck, 60 * 60e3);
      if (!j) {
        const safe = await egressCheck('https://opensubtitles-v3.strem.io/subtitles/' + pathPart + '.json');
        const r = await fetch(safe, { headers: { 'User-Agent': KODIK_UA, 'Accept': 'application/json' }, signal: AbortSignal.timeout(10000) });
        if (r.ok) {
          j = await r.json();
          kCacheSet(ck, j);
        }
      }
      return (j && Array.isArray(j.subtitles)) ? j.subtitles : [];
    };
    let rawSubs = await fetchOs(isMovie ? ('movie/' + imdb) : ('series/' + imdb + ':' + tmdbSeason + ':' + ep));
    if (!isMovie && absEp > 0 && (tmdbSeason !== 1 || absEp !== ep)) {
      const absSubs = await fetchOs('series/' + imdb + ':1:' + absEp);
      if (absSubs.length) rawSubs = rawSubs.concat(absSubs);
    }
    if (!rawSubs.length && !isMovie && tmdbSeason !== 1) {
      rawSubs = await fetchOs('series/' + imdb + ':1:' + ep);
    }
    const langMap = {
      rus: { code: 'ru', name: '🇷🇺 Русские' },
      ru: { code: 'ru', name: '🇷🇺 Русские' },
      eng: { code: 'en', name: '🇬🇧 Английские' },
      en: { code: 'en', name: '🇬🇧 Английские' },
      jpn: { code: 'ja', name: '🇯🇵 Японские' },
      ja: { code: 'ja', name: '🇯🇵 Японские' }
    };
    const picked = [];
    const perLang = { ru: 0, en: 0, ja: 0 };
    for (const s of rawSubs) {
      if (!s || typeof s.url !== 'string') continue;
      const lm = langMap[String(s.lang || '').toLowerCase()];
      if (!lm || perLang[lm.code] >= 2) continue;
      perLang[lm.code]++;
      const fmt = /\.(ass|ssa)(\?|$)/i.test(s.url) ? 'ass' : 'srt';
      const idx = perLang[lm.code];
      picked.push({
        id: 'ext-' + lm.code + (idx > 1 ? '-' + idx : ''),
        lang: idx === 1 ? lm.code : (lm.code + '-' + idx),
        baseLang: lm.code,
        format: fmt,
        label: lm.name + (idx > 1 ? ' #' + idx : '') + ' (' + fmt.toUpperCase() + ')',
        url: s.url
      });
    }
    const order = { ru: 0, en: 1, ja: 2 };
    picked.sort((a, b) => (order[a.baseLang] ?? 9) - (order[b.baseLang] ?? 9));
    res.json({ ok: true, imdb, season: tmdbSeason, ep, subs: picked });
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e && e.message || e), subs: [] });
  }
});

server.listen(PORT, () => console.log('anihub-rooms on :' + PORT));
