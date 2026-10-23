/* AniHub Service Worker: офлайн-оболочка и быстрый повторный заход (PWA).
   Стратегия: сеть-first, при отсутствии сети — кэш. /api/ никогда не кэшируется. */
const CACHE = 'anihub-v1';
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const u = new URL(e.request.url);
  if (e.request.method !== 'GET' || u.pathname.startsWith('/api/')) return;
  e.respondWith(
    caches.open(CACHE).then(async c => {
      const hit = await c.match(e.request);
      const net = fetch(e.request).then(r => { try { c.put(e.request, r.clone()); } catch {} return r; }).catch(() => hit);
      return hit || net;
    })
  );
});
