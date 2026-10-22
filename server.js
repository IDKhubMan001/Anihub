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
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },   // Pages, file://, localhost — всё можно
  transports: ['polling', 'websocket'],
  pingInterval: 20000,
  pingTimeout: 25000,
  maxHttpBufferSize: 2e6   // SDP/ICE маленькие, но пусть будет запас
});

/* ------------------------------ REST ------------------------------ */
app.get('/api/ping', (q, r) => r.json({ ok: true, ts: Date.now(), service: 'anihub-rooms' }));
app.get('/api/health', (q, r) => r.json({ ok: true }));
app.get('/', (q, r) => r.json({ ok: true, service: 'anihub-rooms', hint: 'GET /api/ping' }));

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
    sid: m.sid, uid: m.uid, name: m.name, screen: m.screen, cam: m.cam
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
