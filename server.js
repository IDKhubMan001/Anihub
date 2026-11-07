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
  const out = () => res.json({ rev: s ? s.rev : 0, list: s ? s.list.slice(-400) : [], serverNow: Date.now() });
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
    if (!text) return res.status(400).json({ error: 'пустой комментарий', serverNow: Date.now() });
    const now = Date.now();
    const authorName = (tokenUser(b.token) && tokenUser(b.token).nick) || cmtClean(b.name, 24) || 'Гость';
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
      uid, name: authorName, text,
      at: Date.now(), parent, parentName, likes: [], dislikes: []
    };
    s.list.push(c); s.rev++; s.at = Date.now();
    cmtNotify(tid); cmtSave();
    return res.json({ rev: s.rev, id: c.id, serverNow: Date.now() });
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
const SROOTS = [/пизд/, /бля[дт]/, /хуй/, /ху[яеёю]/, /(^|[^а-я])еб[ауиыё]/, /(за|у|по|вы|на|до|от|раз|об|о|пере|под|въ)е[бб]/, /аху[еёи]/, /оху[еёи]/, /пид[оа][рб]/, /муд[ао]к/, /ганд[оа]н/, /шлюх/, /ублюд/, /манда/, /педик(?!юр)/];
const SBAD = new Set(['сука', 'суки', 'сучка', 'бля', 'блять', 'мудак', 'мудила', 'мудаки', 'гандон', 'чмо', 'пидор', 'пидар', 'пидорас', 'долбоёб', 'долбоеб', 'шлюха', 'шлюхи', 'говнюк', 'мразь', 'мразота', 'педик']);
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
  const penProf = pubPen(getPenRecord(null, name, null, false));
  const viewer = authFromReq(req);
  const friendship = profileFriendship(viewer, k);
  res.json({
    ok: true, name, serverNow: Date.now(),
    hasAccount: usersState.has(k),
    friendship,
    mod: penProf,
    prof: prof ? { name: prof.name, hist: pub ? prof.hist : [], lists: pub ? prof.lists : {}, pub: prof.pub !== false, at: prof.at } : null,
    reports: { count: reports.length, list: reports.slice(-10), summary },
    msgs: msgs.slice(-30).reverse(),
    status, punished: just > 10 || penProf.permBan || penProf.tempBanned, just
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

/* ---- аккаунты v17.34: scrypt-пароли с солью, токены сессий, почта ---- */
const crypto = require('crypto');
const scryptHash = (pass, salt) => crypto.scryptSync(String(pass), salt, 32).toString('hex');
const newSalt = () => crypto.randomBytes(8).toString('hex');
const newToken = () => crypto.randomBytes(24).toString('hex');
const emailOk = v => typeof v === 'string' && v.length < 190 && /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,24}$/.test(v.trim()) ? v.trim().toLowerCase() : '';
const passVerify = (u, pass) => {
  if (u.salt && u.hash) { try { return crypto.timingSafeEqual(Buffer.from(u.hash, 'hex'), Buffer.from(scryptHash(pass, u.salt), 'hex')); } catch { return false; } }
  if (u.pass) { const ok = u.pass === sha256(pass); if (ok) { u.salt = newSalt(); u.hash = scryptHash(pass, u.salt); delete u.pass; } return ok; }
  return false;
};
const tokenIssue = u => { const t = newToken(); u.tokens = u.tokens || {}; u.tokens[t] = { at: Date.now() };
  const ks = Object.keys(u.tokens); if (ks.length > 10) ks.sort((a, b) => (u.tokens[a].at||0) - (u.tokens[b].at||0)).slice(0, ks.length - 10).forEach(k => delete u.tokens[k]); return t; };
const tokenUser = t => { if (!t || typeof t !== 'string') return null; for (const u of usersState.values()) if (u.tokens && u.tokens[t]) return u; return null; };
const pubUser = u => ({ nick: u.nick, email: u.email || '', avatar: u.avatar || '' });

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
    const email = b.email != null && b.email !== '' ? emailOk(b.email) : '';
    if (b.email != null && b.email !== '' && !email) return res.status(400).json({ error: 'почта не похоже на почту' });
    if (email) for (const u of usersState.values()) if (u.email && u.email === email && u.nick.toLowerCase() !== nick.toLowerCase())
      return res.status(409).json({ error: 'почта уже привязана к другому нику' });
    const k = nick.toLowerCase();
    const u = usersState.get(k);
    if (u) {
      if (!passVerify(u, pass)) return res.status(409).json({ error: 'ник уже занят' });
      if (email && !u.email) u.email = email;
      u.at = Date.now(); usrSave();
      return res.json({ ok: true, nick: u.nick, avatar: u.avatar || '', email: u.email || '', taken: true, token: tokenIssue(u) });
    }
    const nu = { nick, salt: newSalt(), hash: scryptHash(pass, newSalt()), email, avatar: avaOk(b.avatar), at: Date.now(), tokens: {} };
    nu.hash = scryptHash(pass, nu.salt);
    usersState.set(k, nu); usrSave();
    return res.json({ ok: true, nick, avatar: nu.avatar, email, taken: false, token: tokenIssue(nu) });
  }
  if (act === 'login') {
    const lg = String(b.login == null ? '' : b.login).trim().slice(0, 190);
    if (!lg) return res.status(400).json({ error: 'укажите ник или почту' });
    let u = null;
    if (lg.includes('@')) { for (const x of usersState.values()) if (x.email && x.email.toLowerCase() === lg.toLowerCase()) { u = x; break; } }
    else u = usersState.get(nickClean(lg).toLowerCase());
    if (!u || !passVerify(u, pass)) return res.status(403).json({ error: 'неверный ник (или почта) или пароль' });
    u.at = Date.now(); usrSave();
    return res.json(Object.assign({ ok: true, taken: true, token: tokenIssue(u) }, pubUser(u)));
  }
  if (act === 'chpass') {
    const lg = String(b.login == null ? '' : b.login).trim().slice(0, 190);
    const n1 = String(b.new1 == null ? '' : b.new1);
    let u = null;
    if (lg.includes('@')) { for (const x of usersState.values()) if (x.email && x.email.toLowerCase() === lg.toLowerCase()) { u = x; break; } }
    else u = usersState.get(nickClean(lg).toLowerCase());
    if (!u || !passVerify(u, pass)) return res.status(403).json({ error: 'старый пароль неверный' });
    if (n1.length < 4) return res.status(400).json({ error: 'новый пароль минимум 4 символа' });
    u.salt = newSalt(); u.hash = scryptHash(n1, u.salt); u.at = Date.now(); usrSave();
    return res.json({ ok: true, token: tokenIssue(u) });
  }
  if (act === 'bytoken') {
    const u = tokenUser(b.token);
    if (!u) return res.status(403).json({ ok: false, error: 'сессия истекла' });
    return res.json(Object.assign({ ok: true }, pubUser(u)));
  }
  if (act === 'setava') {
    let u = tokenUser(b.token);
    if (!u && b.nick) { const x = usersState.get(nickClean(b.nick).toLowerCase()); if (x && String(b.pass || '') && passVerify(x, String(b.pass))) u = x; }
    if (!u) return res.status(403).json({ error: 'нет доступа' });
    u.avatar = avaOk(b.avatar); u.at = Date.now(); usrSave();
    return res.json({ ok: true });
  }
  if (act === 'logout') {
    const u = tokenUser(b.token);
    if (u) { delete (u.tokens || {})[String(b.token)]; usrSave(); }
    return res.json({ ok: true });
  }
  res.status(400).json({ error: 'неизвестное действие' });
});

/* ==================== v17.41: серверная анти-спам защита, Self-Pardon, друзья и ЛС ====================
   Все таймеры наказаний, эскалаций и апелляций считаются СТРОГО по серверному Date.now()
   (перевод часов на телефоне/ПК не влияет ни на проверки, ни на возвращаемые остатки времени). */
const LINK_RE = /(?:https?:\/\/|ftp:\/\/|www\.|discord\.gg\/|t\.me\/|vk\.com\/|youtu\.be\/|bit\.ly\/|\b[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.(?:com|ru|net|org|io|gg|me|tv|xyz|info|site|online|top|club|dev|app|cc|kz|ua|by|su|pro|live|fun|ink|link|pw|click|shop)(?:\/|\b))/i;
function hasLink(t) { return LINK_RE.test(String(t == null ? '' : t)); }

const MUTE_5M_MS = 5 * 60 * 1000;
const WIN_7D_MS = 7 * 86400 * 1000;
const WIN_30D_MS = 30 * 86400 * 1000;
const TEMP_BAN_MS = 7 * 86400 * 1000;
const PARDON_CD_MS = 90 * 86400 * 1000; /* 3 месяца */

const penaltiesState = new Map();
const strikeDedup = new Map();
try {
  const j = JSON.parse(fs.readFileSync('penalties.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) penaltiesState.set(k, v);
  console.log('penalties loaded:', penaltiesState.size);
} catch {}
let penSaveT = null;
function penSave() {
  clearTimeout(penSaveT);
  penSaveT = setTimeout(() => {
    try { fs.writeFileSync('penalties.json', JSON.stringify(Object.fromEntries(penaltiesState))); } catch {}
  }, 2500);
}

function authFromReq(req) {
  const b = (req && req.body) || {};
  const q = (req && req.query) || {};
  const hdr = String((req && req.headers && req.headers.authorization) || '').replace(/^Bearer\s+/i, '').trim();
  const tok = b.token || q.token || hdr || '';
  return tokenUser(tok);
}

function actorKeys(token, nick, uid) {
  const keys = [];
  const u = tokenUser(token);
  const nk = u ? u.nick : nickClean(nick);
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
  /* мёрджим, если ранее были записи по второму ключу (например до входа в аккаунт) */
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
    if (create || penaltiesState.has(k)) penaltiesState.set(k, primary);
  }
  if (dispNick && dispNick !== 'Гость') primary.nick = dispNick;
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
  penSave();
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
    /* исторический сид при первом создании комнаты (>5 сек назад и без ссылок) пропускаем как есть */
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
      continue; /* заблокированные/замученные по серверному времени не могут пушить сообщения */
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

app.get('/api/mod/status', (req, res) => {
  const rec = getPenRecord(req.query.token, req.query.nick, req.query.uid, false);
  res.json({ ok: true, serverNow: Date.now(), mod: pubPen(rec) });
});

app.post('/api/mod/strike', (req, res) => {
  const b = req.body || {};
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
  if (room && roomsState.has(room)) {
    const rs = roomsState.get(room);
    const mems = (rs.data && rs.data.members) || {};
    const mem = mems[String(b.uid || '')];
    if (mem) mem.mutedUntil = Math.max(mem.mutedUntil || 0, st.mutedUntil);
    if (rs.data) {
      rs.data.log = Array.isArray(rs.data.log) ? rs.data.log : [];
      const whoTgt = String(b.nick || (mem && mem.name) || 'Гость').slice(0, 24);
      const rTxt = kind === 'link' ? 'ссылка без разрешения: авто-мут 5 минут' : (kind === 'profanity' ? 'нецензурная лексика: авто-мут 5 минут' : 'спам: авто-мут 5 минут');
      if (!rs.data.log.some(l => l && l.action === 'auto_mute' && l.target === whoTgt && Date.now() - (l.at || 0) < 3000)) {
        rs.data.log.unshift({ at: Date.now(), by: whoTgt, byId: String(b.uid || 'sys'), action: 'auto_mute', target: whoTgt, reason: rTxt });
        rs.data.log = rs.data.log.slice(0, 100);
      }
    }
    rs.rev++; rs.at = Date.now();
    notifyWaiters(room);
  }
  res.json({ ok: true, serverNow: st.serverNow, mod: st });
});

/* Self-Pardon («Честное слово»): мгновенное снятие временного бана без модератора, не чаще 1 раза в 3 мес */
app.post('/api/mod/pardon', (req, res) => {
  const b = req.body || {};
  const now = Date.now();
  const rec = getPenRecord(b.token, b.nick, b.uid, false);
  if (!rec) return res.status(400).json({ error: 'Нет активных наказаний', serverNow: now, mod: pubPen(null) });
  if (rec.permBan)
    return res.status(403).json({ error: 'Перманентный бан не подлежит обжалованию', serverNow: now, mod: pubPen(rec) });
  if (!((rec.tempBanUntil || 0) > now))
    return res.status(400).json({ error: 'У вас нет активной временной блокировки', serverNow: now, mod: pubPen(rec) });
  if (rec.lastPardonAt && (now - rec.lastPardonAt < PARDON_CD_MS)) {
    return res.status(429).json({
      error: 'Обжаловать наказание можно не чаще 1 раза в 3 месяца',
      serverNow: now, mod: pubPen(rec)
    });
  }
  rec.tempBanUntil = 0;
  rec.mutedUntil = 0;
  rec.lastPardonAt = now;
  rec.onProbation = true;
  penSave();

  const uAuth = tokenUser(b.token);
  const whoName = (uAuth && uAuth.nick) || nickClean(b.nick) || rec.nick || 'Участник';
  const sysText = '📜 ' + whoName + ' дал(а) честное слово больше не нарушать правила — временная блокировка снята. При повторном нарушении аккаунт будет заблокирован навсегда.';
  modLog({ kind: 'self_pardon', who: whoName, note: 'Честное слово: временный бан снят, включён испытательный режим' });

  const targetRoom = String(b.room || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12);
  const touchedRooms = new Set();
  for (const [c, rs] of roomsState) {
    const mems = (rs.data && rs.data.members) || {};
    let hasUser = c === targetRoom;
    for (const id2 of Object.keys(mems)) {
      const m2 = mems[id2];
      if (id2 === b.uid || (m2 && String(m2.name || '').toLowerCase() === whoName.toLowerCase())) {
        m2.mutedUntil = 0;
        hasUser = true;
      }
    }
    if (hasUser && rs.data) {
      rs.data.msgs = Array.isArray(rs.data.msgs) ? rs.data.msgs : [];
      const mid = 'pardon_' + Math.random().toString(36).slice(2, 9) + now.toString(36).slice(-4);
      rs.data.msgs.push({ id: mid, sys: true, body: sysText, at: now });
      rs.data.msgs = rs.data.msgs.slice(-300);
      if (rs.seenMsgs) rs.seenMsgs.add(mid);
      rs.rev++; rs.at = now;
      touchedRooms.add(c);
      notifyWaiters(c);
    }
  }
  res.json({ ok: true, serverNow: now, sysText, roomsNotified: [...touchedRooms], mod: pubPen(rec) });
});

/* ---- Друзья (двустороннее подтверждение) и Личные сообщения (ЛС) ---- */
const friendsState = new Map();
const dmsState = new Map();
const dmWaiters = new Map();
try {
  const j = JSON.parse(fs.readFileSync('friends.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) friendsState.set(k, v);
  console.log('friends loaded:', friendsState.size);
} catch {}
try {
  const j = JSON.parse(fs.readFileSync('dms.json', 'utf8'));
  for (const [k, v] of Object.entries(j || {})) dmsState.set(k, v);
  console.log('dms loaded:', dmsState.size);
} catch {}
let frSaveT = null, dmSaveT = null;
function frSave() {
  clearTimeout(frSaveT);
  frSaveT = setTimeout(() => {
    try { fs.writeFileSync('friends.json', JSON.stringify(Object.fromEntries(friendsState))); } catch {}
  }, 2500);
}
function dmSave() {
  clearTimeout(dmSaveT);
  dmSaveT = setTimeout(() => {
    try { fs.writeFileSync('dms.json', JSON.stringify(Object.fromEntries(dmsState))); } catch {}
  }, 2500);
}
function dmNotify(pk) {
  const set = dmWaiters.get(pk);
  if (set) { const fns = [...set]; set.clear(); for (const fn of fns) { try { fn(); } catch {} } }
}
const frPair = (a, b) => [String(a).toLowerCase(), String(b).toLowerCase()].sort().join('|');

function profileFriendship(viewer, targetLower) {
  if (!viewer || !targetLower) return 'none';
  const meK = viewer.nick.toLowerCase();
  if (meK === targetLower) return 'self';
  const rec = friendsState.get(frPair(meK, targetLower));
  if (!rec) return 'none';
  if (rec.status === 'accepted') return 'friends';
  return rec.from === meK ? 'outgoing' : 'incoming';
}

app.get('/api/friends', (req, res) => {
  const me = authFromReq(req);
  if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для доступа к друзьям и ЛС', serverNow: Date.now() });
  const meK = me.nick.toLowerCase();
  const friends = [], incoming = [], outgoing = [];
  let unreadTotal = 0;
  for (const [pk, rec] of friendsState) {
    if (rec.a !== meK && rec.b !== meK) continue;
    const otherK = rec.a === meK ? rec.b : rec.a;
    const ou = usersState.get(otherK);
    const nick = (ou && ou.nick) || (rec.a === meK ? rec.bNick : rec.aNick) || otherK;
    const avatar = (ou && ou.avatar) || '';
    if (rec.status === 'accepted') {
      const conv = dmsState.get(pk);
      const readAt = (conv && conv.readAt && conv.readAt[meK]) || 0;
      const msgs = (conv && conv.msgs) || [];
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
});

app.post('/api/friends', (req, res) => {
  const me = authFromReq(req);
  const now = Date.now();
  if (!me) return res.status(401).json({ error: 'Войдите в аккаунт, чтобы управлять списком друзей', serverNow: now });
  const b = req.body || {};
  const act = String(b.act || '');
  const targetNick = nickClean(b.target);
  const meK = me.nick.toLowerCase();
  const targetK = targetNick.toLowerCase();
  if (!targetK || !NICK_RE.test(targetNick))
    return res.status(400).json({ error: 'Укажите корректный ник пользователя', serverNow: now });
  if (meK === targetK)
    return res.status(400).json({ error: 'Нельзя добавить в друзья самого себя', serverNow: now });
  const pk = frPair(meK, targetK);
  const rec = friendsState.get(pk);
  const tu = usersState.get(targetK);

  if (act === 'request') {
    if (!tu) return res.status(404).json({ error: 'Пользователь «' + targetNick + '» ещё не зарегистрировал аккаунт', serverNow: now });
    if (rec && rec.status === 'accepted')
      return res.json({ ok: true, status: 'friends', serverNow: now });
    if (rec && rec.status === 'pending') {
      if (rec.from === meK) return res.json({ ok: true, status: 'outgoing', serverNow: now });
      /* встречная заявка уже висела от второго пользователя -> взаимное подтверждение! */
      rec.status = 'accepted';
      rec.updatedAt = now;
      frSave();
      return res.json({ ok: true, status: 'friends', serverNow: now });
    }
    const [a, bKey] = [meK, targetK].sort();
    friendsState.set(pk, {
      a, b: bKey,
      aNick: a === meK ? me.nick : tu.nick,
      bNick: bKey === meK ? me.nick : tu.nick,
      status: 'pending',
      from: meK,
      at: now,
      updatedAt: now
    });
    frSave();
    return res.json({ ok: true, status: 'outgoing', serverNow: now });
  }
  if (act === 'accept') {
    if (!rec || rec.status !== 'pending')
      return res.status(404).json({ error: 'Заявка в друзья не найдена', serverNow: now });
    if (rec.from === meK)
      return res.status(403).json({ error: 'Нельзя подтвердить собственную заявку — дождитесь ответа второго пользователя', serverNow: now });
    rec.status = 'accepted';
    rec.updatedAt = now;
    frSave();
    return res.json({ ok: true, status: 'friends', serverNow: now });
  }
  if (act === 'decline' || act === 'cancel') {
    if (rec && rec.status === 'pending') { friendsState.delete(pk); frSave(); }
    return res.json({ ok: true, status: 'none', serverNow: now });
  }
  if (act === 'remove') {
    /* Удаляем дружбу, но dmsState.get(pk) сохраняем нетронутым в базе! */
    if (rec) { friendsState.delete(pk); frSave(); }
    return res.json({ ok: true, status: 'none', historySaved: true, serverNow: now });
  }
  res.status(400).json({ error: 'неизвестное действие', serverNow: now });
});

/* ЛС доступны ТОЛЬКО подтверждённым друзьям (даже при временном/вечном бане публичного чата и комментов!) */
app.get('/api/dm/:peer', (req, res) => {
  const me = authFromReq(req);
  const now = Date.now();
  if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для доступа к ЛС', serverNow: now });
  const peerNick = nickClean(req.params.peer);
  const meK = me.nick.toLowerCase();
  const peerK = peerNick.toLowerCase();
  const pk = frPair(meK, peerK);
  const fr = friendsState.get(pk);
  if (!fr || fr.status !== 'accepted')
    return res.status(403).json({ error: 'Писать и открывать ЛС можно только подтверждённым друзьям', code: 'NOT_FRIENDS', serverNow: now });
  const tu = usersState.get(peerK);
  const sendOut = () => {
    const conv = dmsState.get(pk) || { rev: 0, msgs: [], readAt: {} };
    conv.readAt = conv.readAt || {};
    if (conv.msgs && conv.msgs.length) { conv.readAt[meK] = Date.now(); dmsState.set(pk, conv); dmSave(); }
    res.json({
      ok: true,
      serverNow: Date.now(),
      rev: conv.rev || 0,
      peer: { nick: (tu && tu.nick) || peerNick, avatar: (tu && tu.avatar) || '' },
      msgs: (conv.msgs || []).slice(-300)
    });
  };
  if (req.query.since === undefined) return sendOut();
  const since = parseInt(req.query.since, 10) || 0;
  const cur = dmsState.get(pk);
  if (cur && cur.rev > since) return sendOut();
  const set = dmWaiters.get(pk) || new Set();
  dmWaiters.set(pk, set);
  let fired = false;
  const fire = () => {
    if (fired) return;
    fired = true;
    set.delete(fire);
    const frNow = friendsState.get(pk);
    if (!frNow || frNow.status !== 'accepted')
      return res.status(403).json({ error: 'Пользователь больше не в списке друзей', code: 'NOT_FRIENDS', serverNow: Date.now() });
    sendOut();
  };
  set.add(fire);
  setTimeout(fire, 20000);
});

app.post('/api/dm/:peer', (req, res) => {
  const me = authFromReq(req);
  const now = Date.now();
  if (!me) return res.status(401).json({ error: 'Войдите в аккаунт для отправки ЛС', serverNow: now });
  const peerNick = nickClean(req.params.peer);
  const meK = me.nick.toLowerCase();
  const peerK = peerNick.toLowerCase();
  const pk = frPair(meK, peerK);
  const fr = friendsState.get(pk);
  if (!fr || fr.status !== 'accepted')
    return res.status(403).json({ error: 'Писать в ЛС можно только после подтверждения заявки в друзья', code: 'NOT_FRIENDS', serverNow: now });
  const b = req.body || {};
  if (b.act === 'read') {
    const conv = dmsState.get(pk);
    if (conv) { conv.readAt = conv.readAt || {}; conv.readAt[meK] = now; dmSave(); }
    return res.json({ ok: true, serverNow: now });
  }
  const text = cmtClean(b.text, 1500);
  if (!text) return res.status(400).json({ error: 'Пустое сообщение', serverNow: now });
  let conv = dmsState.get(pk);
  if (!conv) { conv = { rev: 0, msgs: [], readAt: {} }; dmsState.set(pk, conv); }
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
  dmSave();
  dmNotify(pk);
  res.json({ ok: true, rev: conv.rev, id: msg.id, msg, serverNow: now });
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
  'cdnvideohub.com', 'animevost.org', 'animetop.info', 'yani.tv', 'yummyani.me', 'aksor.tv'
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
  if (_kCache.size >= 256) _kCache.delete(_kCache.keys().next().value);
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
    for (const [k, qv, lbl] of qOrder) {
      const u = q[k];
      if (!u || typeof u !== 'string') continue;
      if (/\.mpd(\?|$)/i.test(u)) {
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'hls', url: baseUrl + '/api/aksor/hls.m3u8?mpd=' + encodeURIComponent(u) });
      } else if (/\.m3u8(\?|$)/i.test(u)) {
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'hls', url: u });
      } else if (/\.mp4(\?|$)/i.test(u)) {
        sources.push({ quality: qv, label: lbl + ' · Aksor', type: 'mp4', url: u });
      }
    }
    if (!sources.length) return res.status(404).json({ ok: false, error: 'потоки Aksor не найдены' });
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

server.listen(PORT, () => console.log('anihub-rooms on :' + PORT));
