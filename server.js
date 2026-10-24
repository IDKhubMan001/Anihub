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
    r.header('Access-Control-Allow-Headers', 'Content-Type');
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
  if (!s) return res.status(404).json({ error: 'комнаты нет на сервере' });
  const since = parseInt(req.query.since || '0', 10) || 0;
  if (s.rev <= since) {
    const set = waiters.get(code) || new Set(); waiters.set(code, set);
    let fired = false;
    const fire = () => {
      if (fired) return; fired = true; set.delete(fire);
      const s2 = roomsState.get(code);
      if (!s2) return res.status(404).json({ error: 'комнаты нет на сервере' });
      res.json({ rev: s2.rev, data: s2.data, hb: hbOut(code) });
    };
    set.add(fire);
    setTimeout(fire, 20000);
    return;
  }
  res.json({ rev: s.rev, data: s.data, hb: hbOut(code) });
});

/* действия: put / sync / hb */
app.post('/api/room/:code', (req, res) => {
  const code = String(req.params.code || '').toUpperCase();
  const body = req.body || {};
  const act = body.act;
  let s = roomsState.get(code);
  if (act === 'put') {
    const data = body.data || {};
    if (typeof data !== 'object' || JSON.stringify(data).length > 900000)
      return res.status(413).json({ error: 'слишком большие данные комнаты' });
    if (s) { s.data = data; s.rev++; s.at = Date.now(); }
    else { roomsState.set(code, { rev: 1, data, at: Date.now() }); }
    notifyWaiters(code);
    return res.json({ rev: roomsState.get(code).rev });
  }
  if (!s) return res.status(404).json({ error: 'комнаты нет на сервере' });
  if (act === 'sync') {
    s.data.sync = { st: body.st, uid: body.uid, name: body.name, at: Date.now() };
    s.rev++; s.at = Date.now();
    notifyWaiters(code);
    return res.json({ rev: s.rev });
  }
  if (act === 'hb') {
    const m = roomsHB.get(code) || {};
    m[body.uid || '?'] = Date.now();
    const now = Date.now();
    for (const k of Object.keys(m)) if (now - m[k] > 120000) delete m[k];
    roomsHB.set(code, m);
    return res.json({ ok: true });
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

/* ==================== комментарии к тайтлам (v17.5) ====================
   tid → {rev, list:[{id,uid,name,text,at,parent,parentName,likes,dislikes}]}
   Живут в памяти инстанса; дополнительно сохраняем в comments.json,
   если диск доступен (на Render-диске переживут рестарт сервиса). */
const fs = require('fs');
const commentsState = new Map();
const cmtWaiters = new Map();
try {
  const j = JSON.parse(fs.readFileSync('comments.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) commentsState.set(k, v);
  console.log('comments loaded:', commentsState.size, 'тайтлов');
} catch {}
let cmtSaveT = null;
function cmtSave() {
  clearTimeout(cmtSaveT);
  cmtSaveT = setTimeout(() => {
    try { fs.writeFileSync('comments.json', JSON.stringify(Object.fromEntries(commentsState))); } catch {}
  }, 3000);
}
function cmtNotify(tid) {
  const set = cmtWaiters.get(tid);
  if (set) { const fns = [...set]; set.clear(); for (const fn of fns) { try { fn(); } catch {} } }
}
const tidOk = t => String(t || '').replace(/[^A-Za-z0-9_:\-]/g, '').slice(0, 64);
const cmtClean = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').trim().slice(0, n);

/* GET: без ?since — текущий список сразу; с ?since=N — long-poll до 20с */
app.get('/api/comments/:tid', (req, res) => {
  const tid = tidOk(req.params.tid);
  if (!tid) return res.status(400).json({ error: 'плохой id тайтла' });
  const s = commentsState.get(tid);
  const out = () => res.json({ rev: s ? s.rev : 0, list: s ? s.list.slice(-400) : [] });
  if (req.query.since === undefined) return out();
  const since = parseInt(req.query.since, 10) || 0;
  if (s && s.rev > since) return out();
  const set = cmtWaiters.get(tid) || new Set(); cmtWaiters.set(tid, set);
  let fired = false;
  const fire = () => { if (fired) return; fired = true; set.delete(fire); out(); };
  set.add(fire);
  setTimeout(fire, 20000);
});

/* POST: add / vote / del */
app.post('/api/comments/:tid', (req, res) => {
  const tid = tidOk(req.params.tid);
  if (!tid) return res.status(400).json({ error: 'плохой id тайтла' });
  const b = req.body || {};
  const act = b.act;
  const uid = cmtClean(b.uid, 40) || '?';
  let s = commentsState.get(tid);
  if (!s) { s = { rev: 0, list: [], at: Date.now() }; commentsState.set(tid, s); }
  if (act === 'add') {
    const text = cmtClean(b.text, 1200);
    if (!text) return res.status(400).json({ error: 'пустой комментарий' });
    if (s.list.length >= 500) s.list = s.list.slice(-400);
    let parent = null, parentName = '';
    if (b.parent) {
      const p = s.list.find(c => c.id === String(b.parent).slice(0, 24));
      if (!p) return res.status(404).json({ error: 'комментарий для ответа не найден' });
      parent = p.parent || p.id;
      const pp = parent !== p.id ? s.list.find(c => c.id === parent) : null;
      parentName = cmtClean(pp ? pp.name : p.name, 24) || '?';
    }
    const c = {
      id: Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4),
      uid, name: cmtClean(b.name, 24) || 'Гость', text,
      at: Date.now(), parent, parentName, likes: [], dislikes: []
    };
    s.list.push(c); s.rev++; s.at = Date.now();
    cmtNotify(tid); cmtSave();
    return res.json({ rev: s.rev, id: c.id });
  }
  if (act === 'vote') {
    const c = s.list.find(x => x.id === String(b.cid || '').slice(0, 24));
    if (!c) return res.status(404).json({ error: 'комментарий не найден' });
    const v = (b.v === 1 || b.v === '1') ? 1 : (b.v === -1 || b.v === '-1') ? -1 : 0;
    c.likes = c.likes.filter(u => u !== uid);
    c.dislikes = c.dislikes.filter(u => u !== uid);
    if (v === 1) c.likes.push(uid); else if (v === -1) c.dislikes.push(uid);
    s.rev++; s.at = Date.now();
    cmtNotify(tid); cmtSave();
    return res.json({ rev: s.rev });
  }
  if (act === 'del') {
    const c = s.list.find(x => x.id === String(b.cid || '').slice(0, 24));
    if (!c) return res.status(404).json({ error: 'комментарий не найден' });
    if (c.uid !== uid) return res.status(403).json({ error: 'можно удалять только свои комментарии' });
    s.list = s.list.filter(x => x.id !== c.id && x.parent !== c.id);
    s.rev++; s.at = Date.now();
    cmtNotify(tid); cmtSave();
    return res.json({ rev: s.rev });
  }
  res.status(400).json({ error: 'неизвестное действие' });
});

/* ==================== таймкоды опенинга/эндинга (v17.11) ====================
   OVR: Map<`${tid}:${ep}`, {intro:[a,b], outro:[a,b], by, at}> — находки сообщества. */
const ovrState = new Map();
try {
  const j = JSON.parse(fs.readFileSync('ovr.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) ovrState.set(k, v);
  console.log('ovr loaded:', ovrState.size);
} catch {}
let ovrSaveT = null;
function ovrSave() {
  clearTimeout(ovrSaveT);
  ovrSaveT = setTimeout(() => {
    try { fs.writeFileSync('ovr.json', JSON.stringify(Object.fromEntries(ovrState))); } catch {}
  }, 3000);
}
const seg2 = v => (Array.isArray(v) && v.length === 2 && +v[0] >= 0 && +v[1] > +v[0] && +v[1] - +v[0] <= 600 && +v[1] <= 7200) ? [+v[0], +v[1]] : null;

app.get('/api/ovr/:tid/:ep', (req, res) => {
  const k = tidOk(req.params.tid) + ':' + (parseInt(req.params.ep, 10) || 1);
  const o = ovrState.get(k);
  res.json(o ? { intro: o.intro, outro: o.outro, by: o.by, at: o.at } : { intro: null, outro: null });
});
app.post('/api/ovr/:tid/:ep', (req, res) => {
  const k = tidOk(req.params.tid) + ':' + (parseInt(req.params.ep, 10) || 1);
  const b = req.body || {};
  const cur = ovrState.get(k) || { intro: null, outro: null };
  const intro = seg2(b.intro), outro = seg2(b.outro);
  if (!intro && !outro) return res.status(400).json({ error: 'нет валидных диапазонов' });
  if (intro) cur.intro = intro;
  if (outro) cur.outro = outro;
  cur.by = cmtClean(b.name, 24) || '?';
  cur.at = Date.now();
  ovrState.set(k, cur); ovrSave();
  res.json({ ok: true, intro: cur.intro, outro: cur.outro });
});

/* ==================== жалобы (v17.11) ==================== */
const REPORTS = [];
try {
  const j = JSON.parse(fs.readFileSync('reports.json', 'utf8'));
  if (Array.isArray(j)) REPORTS.push(...j.slice(-500));
} catch {}
let repSaveT = null;
function repSave() {
  clearTimeout(repSaveT);
  repSaveT = setTimeout(() => {
    try { fs.writeFileSync('reports.json', JSON.stringify(REPORTS.slice(-500))); } catch {}
  }, 3000);
}

/* ==================== v17.13: авто-модерация, анти-абьюз жалоб, журнал ==================== */
const SROOTS = [/пизд/, /бля[дт]/, /хуй/, /ху[яеёю]/, /(^|[^а-я])еб[ауиыё]/, /(за|у|по|вы|на|до|от|раз|об|о|пере|под|въ)е[бб]/, /аху[еёи]/, /оху[еёи]/, /пид[оа][рб]/, /муд[ао]к/, /ганд[оа]н/, /шлюх/, /ублюд/, /манда/];
const SBAD = new Set(['сука', 'суки', 'сучка', 'бля', 'блять', 'мудак', 'мудила', 'мудаки', 'гандон', 'чмо', 'пидор', 'пидар', 'пидорас', 'долбоёб', 'долбоеб', 'шлюха', 'шлюхи', 'говнюк', 'мразь', 'мразота']);
function isDirty(t) {
  const ws = String(t == null ? '' : t).toLowerCase().replace(/ё/g, 'е').split(/[^a-z0-9а-я]+/).filter(Boolean);
  const flat = ws.join('');
  for (const r of SROOTS) { r.lastIndex = 0; if (r.test(flat)) return true; }
  for (const w of ws) if (SBAD.has(w)) return true;
  return false;
}
const MODLOG = [];
try {
  const j = JSON.parse(fs.readFileSync('modlog.json', 'utf8'));
  if (Array.isArray(j)) MODLOG.push(...j.slice(-500));
  console.log('modlog loaded:', MODLOG.length);
} catch {}
let modSaveT = null;
const modSave = () => { clearTimeout(modSaveT); modSaveT = setTimeout(() => { try { fs.writeFileSync('modlog.json', JSON.stringify(MODLOG.slice(-500))); } catch {} }, 3000); };
const modLog = e => { MODLOG.push(Object.assign({ at: Date.now() }, e)); if (MODLOG.length > 500) MODLOG.splice(0, MODLOG.length - 500); modSave(); };

const repPairs = new Map();   /* byL|targetL → ts: не чаще 1 жалобы в сутки на пару */
const repStats = new Map();   /* byL → {bad, bannedUntil, lastBan} */
const sanctions = new Map();  /* targetL → {just} — обоснованные жалобы на юзера */
let modDataSaveT = null;
const modDataSave = () => { clearTimeout(modDataSaveT); modDataSaveT = setTimeout(() => { try { fs.writeFileSync('moderation.json', JSON.stringify({ pairs: Object.fromEntries(repPairs), stats: Object.fromEntries(repStats), sanc: Object.fromEntries(sanctions) })); } catch {} }, 3000); };
try {
  const j = JSON.parse(fs.readFileSync('moderation.json', 'utf8'));
  for (const [k, v] of Object.entries(j.pairs || {})) repPairs.set(k, v);
  for (const [k, v] of Object.entries(j.stats || {})) repStats.set(k, v);
  for (const [k, v] of Object.entries(j.sanc || {})) sanctions.set(k, v);
  console.log('moderation loaded:', repPairs.size, repStats.size, sanctions.size);
} catch {}
app.post('/api/reports', (req, res) => {
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
app.get('/api/modlog', (req, res) => res.json({ count: MODLOG.length, list: MODLOG.slice(-100) }));
app.get('/api/reports', (req, res) => {
  const t = cmtClean(req.query.target, 40);
  const list = t ? REPORTS.filter(r => r.target === t) : REPORTS;
  res.json({ count: list.length, list: list.slice(-20) });
});

/* ==================== публичные профили (v17.12) ====================
   PROFILES: Map<nick.toLowerCase(), {name, hist, lists, pub, at}> — история/списки публикует клиент; жалобы и сообщения добираем на сервере. */
const PROFILES = new Map();
try {
  const j = JSON.parse(fs.readFileSync('profiles.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) PROFILES.set(k, v);
  console.log('profiles loaded:', PROFILES.size);
} catch {}
let profSaveT = null;
function profSave() {
  clearTimeout(profSaveT);
  profSaveT = setTimeout(() => {
    try { fs.writeFileSync('profiles.json', JSON.stringify(Object.fromEntries(PROFILES))); } catch {}
  }, 3000);
}
const HIST_KEYS = ['id', 'ep', 'title', 'poster', 'epTitle', 'pos', 'dur', 'done', 'at'];
const LIST_KEYS = ['favorite', 'watching', 'planned', 'dropped'];
app.post('/api/profile/:name', (req, res) => {
  const name = cmtClean(req.params.name, 24);
  if (!NICK_RE.test(name)) return res.status(400).json({ error: 'плохой ник' });
  const b = req.body || {};
  const hist = Array.isArray(b.hist) ? b.hist.slice(0, 60).map(h => {
    const o = {};
    HIST_KEYS.forEach(k => { if (h && h[k] != null) o[k] = (typeof h[k] === 'string') ? String(h[k]).slice(0, 160) : h[k]; });
    return o;
  }) : [];
  const lists0 = (b.lists && typeof b.lists === 'object' && !Array.isArray(b.lists)) ? b.lists : {};
  const lists = {};
  for (const [k, v] of Object.entries(lists0).slice(0, 400)) if (LIST_KEYS.includes(v)) lists[String(k).slice(0, 40)] = v;
  PROFILES.set(name.toLowerCase(), { name, hist, lists, pub: b.pub !== false, at: Date.now() });
  if (PROFILES.size > 400) PROFILES.delete(PROFILES.keys().next().value);
  profSave();
  res.json({ ok: true });
});
app.get('/api/profile/:name', (req, res) => {
  const name = cmtClean(req.params.name, 24);
  const k = name.toLowerCase();
  const prof = PROFILES.get(k) || null;
  const pub = !prof || prof.pub !== false;
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
  res.json({
    ok: true, name,
    prof: prof ? { name: prof.name, hist: pub ? prof.hist : [], lists: pub ? prof.lists : {}, pub: prof.pub !== false, at: prof.at } : null,
    reports: { count: reports.length, list: reports.slice(-10), summary },
    msgs: msgs.slice(-30).reverse(),
    status, punished: just > 10, just
  });
});

/* ==================== пользователи: занятые ники (v17.6) ====================
   users: Map<nickLower, {nick, pass(sha256), avatar(dataURL), at}>
   Пароль хранится хешем; аватар — dataURL до ~150КБ. Персист в users.json. */
const usersState = new Map();
try {
  const j = JSON.parse(fs.readFileSync('users.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) usersState.set(k, v);
  console.log('users loaded:', usersState.size);
} catch {}
let usrSaveT = null;
function usrSave() {
  clearTimeout(usrSaveT);
  usrSaveT = setTimeout(() => {
    try { fs.writeFileSync('users.json', JSON.stringify(Object.fromEntries(usersState))); } catch {}
  }, 3000);
}
const sha256 = s => require('crypto').createHash('sha256').update('anihub:' + s).digest('hex');
const NICK_RE = /^[A-Za-zА-Яа-яЁё0-9_\- ]{2,24}$/;
const nickClean = v => String(v == null ? '' : v).replace(/[\u0000-\u001f]/g, '').trim().slice(0, 24);
const avaOk = v => typeof v === 'string' && v.startsWith('data:image/') && v.length < 150000 ? v : '';

app.get('/api/user/:nick', (req, res) => {
  const k = nickClean(req.params.nick).toLowerCase();
  const u = usersState.get(k);
  res.json({ exists: !!u, nick: u ? u.nick : null, avatar: u ? (u.avatar || '') : '' });
});

app.post('/api/users', (req, res) => {
  const b = req.body || {};
  const act = b.act;
  const nick = nickClean(b.nick);
  const pass = String(b.pass == null ? '' : b.pass);
  if (act === 'claim') {
    if (!NICK_RE.test(nick) || /^гость$/i.test(nick))
      return res.status(400).json({ error: 'ник 2–24 символа: буквы, цифры, пробел, _ или -' });
    if (pass.length < 4) return res.status(400).json({ error: 'пароль минимум 4 символа' });
    const k = nick.toLowerCase();
    const u = usersState.get(k);
    if (u && u.pass !== sha256(pass)) return res.status(409).json({ error: 'ник уже занят' });
    const avatar = avaOk(b.avatar) || (u ? u.avatar : '');
    usersState.set(k, { nick, pass: sha256(pass), avatar, at: Date.now() });
    usrSave();
    return res.json({ ok: true, nick, avatar, taken: !!u });
  }
  if (act === 'login') {
    const u = usersState.get(nick.toLowerCase());
    if (!u || u.pass !== sha256(pass)) return res.status(403).json({ error: 'неверный ник или пароль' });
    return res.json({ ok: true, nick: u.nick, avatar: u.avatar || '' });
  }
  if (act === 'setava') {
    const u = usersState.get(nick.toLowerCase());
    if (!u || u.pass !== sha256(pass)) return res.status(403).json({ error: 'нет доступа' });
    u.avatar = avaOk(b.avatar);
    u.at = Date.now();
    usrSave();
    return res.json({ ok: true });
  }
  res.status(400).json({ error: 'неизвестное действие' });
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
server.listen(PORT, () => console.log('anihub-rooms on :' + PORT));
