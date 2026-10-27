/* AniHub Service Worker: сеть-first (обновления прилетают сразу), кэш — только для офлайна. /api/ никогда не кэшируется. */
const CACHE = 'anihub-v2';
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', e => {
  const u = new URL(e.request.url);
  if (e.request.method !== 'GET' || u.pathname.startsWith('/api/')) return;
  e.respondWith(
    fetch(e.request).then(r => {
      try { const cl = r.clone(); caches.open(CACHE).then(c => c.put(e.request, cl)); } catch {}
      return r;
    }).catch(() => caches.match(e.request).then(hit => hit || caches.match('/')))
  );
});
