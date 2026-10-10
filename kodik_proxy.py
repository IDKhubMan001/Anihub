#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AniHub — локальный прокси-сервер для просмотра без CORS-ограничений.

Запуск:      python3 kodik_proxy.py
Потом:       открой в браузере  http://localhost:8090

Что делает:
  - раздаёт anihub.html (и всё рядом лежащее) как обычный сайт;
  - проксирует запросы к Kodik API (http://localhost:8090/kodik-proxy/...),
    добавляя CORS-заголовки, чтобы браузер ничего не блокировал;
  - /kodik/resolve  — достаёт ПРЯМУЮ m3u8-ссылку на серию из плеера Kodik;
  - /kodik/hls      — проксирует m3u8-манифест и .ts-сегменты видео,
    чтобы плеер на странице мог их воспроизвести без CORS-проблем.

Ничего не устанавливает, использует только стандартную библиотеку Python.
"""
import http.server
import socketserver
import urllib.request
import urllib.parse
import base64
import json
import re
import os

PORT = int(os.environ.get("PORT", "8090"))
ROOT = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = "https://kodik-api.com"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36"}

# простой кэш с временем жизни (подписи ссылок живут ~1 час)
_CACHE = {}


def _cache_get(key, ttl):
    e = _CACHE.get(key)
    if not e:
        return None
    import time
    if time.time() - e[0] > ttl:
        _CACHE.pop(key, None)
        return None
    return e[1]


def _http_get(url, headers=None, timeout=25):
    url = urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%-._~")
    h = dict(UA)
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _rot(s, n):
    out = []
    for c in s:
        if c.isalpha():
            b = 'a' if c.islower() else 'A'
            out.append(chr((ord(c) - ord(b) + n) % 26 + ord(b)))
        else:
            out.append(c)
    return ''.join(out)


def _kodik_decode(s):
    if 'mp4:hls:manifest' in s:
        return s
    for n in range(26):
        t = _rot(s, n)
        t += '=' * ((4 - len(t) % 4) % 4)
        try:
            d = base64.b64decode(t).decode('utf-8')
            if 'mp4:hls:manifest' in d:
                return d
        except Exception:
            continue
    raise Exception('Kodik: не удалось расшифровать ссылку')


def kodik_resolve(media_id, media_hash, media_type):
    """Достаёт прямые m3u8-ссылки на серию/фильм из плеера Kodik."""
    key = (media_type, media_id, media_hash)
    hit = _cache_get(key, 1500)
    if hit:
        return hit

    page_url = "https://kodikplayer.com/%s/%s/%s/720p" % (media_type, media_id, media_hash)
    page = _http_get(page_url).decode('utf-8', 'replace')

    up = page[page.find('urlParams') + 13:]
    url_params = json.loads(up[:up.find(';') - 1])
    m = re.search(r'<script[^>]*src="(/assets/js/app\.(?:player|serial|movie|video|film)[^"]+)"', page)
    if not m:
        raise Exception('Kodik: не найден скрипт плеера')
    script_url = m.group(1)
    vtype = re.search(r"\.type = '([^']+)'", page).group(1)
    vhash = re.search(r"\.hash = '([^']+)'", page).group(1)
    vid = re.search(r"\.id = '([^']+)'", page).group(1)

    # таймкоды опенинга/эндинга, если есть: "0:30-1:30,20:0-21:30"
    skip = []
    sm = re.search(r'parseSkipButton\("([^"]*)"', page)
    if sm and sm.group(1):
        for seg in sm.group(1).split(','):
            if '-' not in seg:
                continue
            try:
                a, b = seg.split('-')
                a1, a2 = a.split(':')
                b1, b2 = b.split(':')
                skip.append([int(a1) * 60 + int(a2), int(b1) * 60 + int(b2)])
            except Exception:
                continue

    js = _http_get("https://kodikplayer.com" + script_url).decode('utf-8', 'replace')
    seg = js[js.find('$.ajax') + 30: js.find('cache:!1') - 3]
    post_link = base64.b64decode(seg).decode()

    params = {
        'hash': vhash, 'id': vid, 'type': vtype,
        'd': url_params['d'], 'd_sign': url_params['d_sign'],
        'pd': url_params['pd'], 'pd_sign': url_params['pd_sign'],
        'ref': '', 'ref_sign': url_params['ref_sign'],
        'bad_user': 'true', 'cdn_is_working': 'true',
    }
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        "https://kodikplayer.com" + post_link, data=body,
        headers=dict(UA, **{"Content-Type": "application/x-www-form-urlencoded"}))
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())

    links = {}
    for q, arr in (data.get('links') or {}).items():
        try:
            src = arr[0]['src']
            links[q] = _kodik_decode(src)
        except Exception:
            continue
    if not links:
        raise Exception('Kodik: пустой список ссылок')

    out = {"links": links, "max": max(int(q) for q in links), "skip": skip}
    _CACHE[key] = (None, out)
    import time
    _CACHE[key] = (time.time(), out)
    return out


def kodik_hls_proxy(target_url):
    """Проксирует m3u8/ts; для манифестов переписывает пути сегментов на себя."""
    raw = _http_get(target_url)
    is_manifest = ('.m3u8' in target_url) or raw[:20].lstrip().startswith(b'#EXTM3U')
    if is_manifest:
        base = target_url.rsplit('/', 1)[0]
        lines = []
        for ln in raw.decode('utf-8', 'replace').splitlines():
            s = ln.strip()
            if not s or s.startswith('#'):
                lines.append(ln)
                continue
            seg = s
            if seg.startswith('http'):
                pass
            elif seg.startswith('/'):
                seg = urllib.parse.urljoin(target_url, seg)
            else:
                seg = base + '/' + seg.lstrip('./')
            # оборачиваем сегмент в этот же прокси, чтобы не было CORS
            lines.append('/kodik/hls?url=' + urllib.parse.quote(seg, safe=''))
        return '\n'.join(lines).encode('utf-8'), 'application/vnd.apple.mpegurl'
    return raw, 'video/mp2t'


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    # --- прокси для Kodik API: /kodik-proxy/<path>?<query> ---
    def do_PROXY(self):
        parsed = urllib.parse.urlparse(self.path)
        sub = parsed.path[len("/kodik-proxy"):] or "/"
        target = UPSTREAM + sub
        if parsed.query:
            target += "?" + parsed.query
        try:
            req = urllib.request.Request(target, headers={
                "User-Agent": "Mozilla/5.0 (AniHub local proxy)",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read()
                ctype = r.headers.get("Content-Type", "application/json")
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            msg = ('{"error": "%s"}' % str(e).replace('"', "'")).encode()
            self.send_response(502)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(msg)

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/kodik-proxy"):
            return self.do_PROXY()

        # --- resolve прямой ссылки ---
        if self.path.startswith("/kodik/resolve"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:
                out = kodik_resolve(q.get('id', [''])[0], q.get('hash', [''])[0], q.get('type', ['seria'])[0])
                return self._json(200, out)
            except Exception as e:
                return self._json(502, {"error": str(e)})

        # --- прокси HLS ---
        if self.path.startswith("/kodik/hls"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            url = q.get('url', [''])[0]
            if not url:
                return self._json(400, {"error": "нет url"})
            try:
                body, ctype = kodik_hls_proxy(url)
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                return self._json(502, {"error": str(e)})
            return

        # --- универсальный прокси для API (AniLibria и др.) ---
        if self.path.startswith("/proxy"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            url = q.get('url', [''])[0]
            host = urllib.parse.urlparse(url).hostname or ''
            if not url or not any(host.endswith(h) for h in ('anilibria.top', 'aniliberty.top', 'libria.fun', 'kodik-api.com', 'raw.githubusercontent.com')):
                return self._json(403, {"error": "host не в белом списке"})
            try:
                body = _http_get(url)
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except Exception as e:
                return self._json(502, {"error": str(e)})
            return

        if self.path in ("/", ""):
            self.path = "/anihub.html"
        return super().do_GET()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_POST(self):
        if self.path.startswith("/kodik-proxy"):
            return self.do_PROXY()
        self.send_error(404)

    def log_message(self, fmt, *args):
        pass


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    os.chdir(ROOT)
    with ReusableTCPServer(("0.0.0.0", PORT), Handler) as httpd:
        print("=" * 54)
        print("  AniHub запущен!")
        print("  Открой в браузере:  http://localhost:%d" % PORT)
        print("  (Kodik API + прямые HLS-ссылки проксируются)")
        print("  Остановка: Ctrl+C")
        print("=" * 54)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nОстановлено.")
