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
import time
import threading
import ipaddress
import socket

PORT = int(os.environ.get("PORT", "8090"))
ROOT = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = "https://kodik-api.com"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36"}

# кэш с временем жизни: потокобезопасный, ограниченный по размеру
_CACHE = {}
_CACHE_LOCK = threading.Lock()
_CACHE_MAX = 256


def _cache_get(key, ttl):
    with _CACHE_LOCK:
        e = _CACHE.get(key)
        if not e:
            return None
        if time.time() - e[0] > ttl:
            _CACHE.pop(key, None)
            return None
        return e[1]


def _cache_put(key, value):
    with _CACHE_LOCK:
        if key not in _CACHE and len(_CACHE) >= _CACHE_MAX:
            _CACHE.pop(next(iter(_CACHE)), None)  # вытесняем самый старый
        _CACHE[key] = (time.time(), value)


# --- P0: защита от SSRF: исходящие запросы только к разрешённым хостам ---
EGRESS_ALLOW = (
    'anilibria.top', 'aniliberty.top', 'libria.fun',
    'kodik-api.com', 'kodik.dev', 'kodikplayer.com', 'kodik.info', 'kodik.cc', 'kodikcdn.com',
    'solodcdn.com', 'raw.githubusercontent.com',
)
for _extra in filter(None, os.environ.get('EGRESS_EXTRA', '').split(',')):
    EGRESS_ALLOW += (_extra.strip().lower(),)


class Blocked(Exception):
    pass


def _host_allowed(host):
    host = (host or '').lower().rstrip('.')
    return any(host == d or host.endswith('.' + d) for d in EGRESS_ALLOW)


_DNS_OK = {}
_DNS_LOCK = threading.Lock()


def _check_dns(host):
    host = (host or '').lower().rstrip('.')
    with _DNS_LOCK:
        hit = _DNS_OK.get(host)
    if hit and time.time() - hit[0] < 300:
        return
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        raise Blocked('DNS не разрешился: %s' % host)
    for inf in infos:
        try:
            ip = ipaddress.ip_address(inf[4][0])
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified:
            raise Blocked('хост %s указывает на закрытый адрес %s' % (host, ip))
    with _DNS_LOCK:
        _DNS_OK[host] = (time.time(), True)


def safe_target(url):
    # проверяет исходящий URL: схема, белый список доменов, приватные IP (в т.ч. через DNS)
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ('https', 'http'):
        raise Blocked('разрешены только http(s)-схемы')
    if not _host_allowed(u.hostname):
        raise Blocked('host не в белом списке: %s' % (u.hostname or '?'))
    _check_dns(u.hostname)
    return url


class RedirectGuard(urllib.request.HTTPRedirectHandler):
    # проверяет КАЖДЫЙ переход по редиректу, а не только первый адрес
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        safe_target(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(RedirectGuard)


def _http_get(url, headers=None, timeout=25):
    url = safe_target(urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%-._~"))
    h = dict(UA)
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with _OPENER.open(req, timeout=timeout) as r:
        return r.read()


def _guess_type(url):
    p = urllib.parse.urlsplit(url).path.lower()
    if p.endswith('.m3u8'):
        return 'application/vnd.apple.mpegurl'
    if p.endswith('.ts'):
        return 'video/mp2t'
    if p.endswith('.m4s'):
        return 'video/iso.segment'
    if p.endswith('.mp4'):
        return 'video/mp4'
    if p.endswith('.vtt'):
        return 'text/vtt'
    if p.endswith('.aac'):
        return 'audio/aac'
    return 'application/octet-stream'  # ключи, init-сегменты и прочее


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
    _cache_put(key, out)
    return out


_AD_RE = re.compile(r'(/ads?[/._-]|/ad/|advert|preroll|midroll|adcreative|adfox|doubleclick|googlesynd|adservice|adsystem|an\.yandex|adriver|/promo)', re.I)
_HEADER_TAG = re.compile(r'^#EXT-X-(TARGETDURATION|VERSION|MEDIA-SEQUENCE|DISCONTINUITY-SEQUENCE|PLAYLIST-TYPE|ALLOW-CACHE|INDEPENDENT-SEGMENTS|START)[:\s]', re.I)
_URI_RE = re.compile(r'URI="([^"]+)"')


def _wrap_uri_tag(line, base):
    # переписываем URI внутри #EXT-X-KEY / #EXT-X-MAP / #EXT-X-MEDIA / #EXT-X-STREAM-INF
    def f(m):
        u = urllib.parse.urljoin(base, m.group(1))
        return 'URI="/kodik/hls?url=' + urllib.parse.quote(u, safe='') + '"'
    return _URI_RE.sub(f, line)


def kodik_hls_proxy(target_url):
    # проксирует m3u8/ts; манифест чистим от рекламы, переписываем пути и URI в тегах
    safe_target(target_url)
    raw = _http_get(target_url)
    is_manifest = ('.m3u8' in target_url) or raw[:20].lstrip().startswith(b'#EXTM3U')
    if not is_manifest:
        return raw, _guess_type(target_url)

    units, header, trailer = [], [], []
    pending, disco = [], False
    for ln in raw.decode('utf-8', 'replace').splitlines():
        s = ln.strip()
        if not s:
            continue
        if not s.startswith('#'):
            units.append({'tags': pending, 'seg': urllib.parse.urljoin(target_url, s), 'disco': disco})
            pending, disco = [], False
            continue
        if s.startswith('#EXT-X-DISCONTINUITY') and not s.startswith('#EXT-X-DISCONTINUITY-SEQUENCE'):
            disco = True
        elif _HEADER_TAG.match(s):
            header.append(s)
        elif s.startswith('#EXT-X-ENDLIST'):
            trailer.append(s)
        else:
            pending.append(s)
    trailer += pending  # теги после последнего сегмента (обычно ENDLIST-соседи)

    if not units:
        return raw, 'application/vnd.apple.mpegurl'

    # группы: начинаются с unit с флагом disco (граница DISCONTINUITY)
    groups, cur = [], []
    for u in units:
        if u['disco'] and cur:
            groups.append(cur)
            cur = []
        cur.append(u)
    if cur:
        groups.append(cur)

    out, removed = [], 0
    for g in groups:
        gsegs = [u['seg'] for u in g]
        whole_ad = bool(g[0]['disco']) and all(_AD_RE.search(x) for x in gsegs)
        if whole_ad:
            removed += len(gsegs)  # весь блок ограничен DISCONTINUITY и целиком совпадает с ad-маркерами
            continue
        if g[0]['disco']:
            out.append('#EXT-X-DISCONTINUITY')
        for u in g:
            if _AD_RE.search(u['seg']):
                removed += 1  # одиночный рекламный сегмент — режем вместе с его тегами
                continue
            out.extend(u['tags'])
            for t in u['tags']:
                if 'URI="' in t:
                    out[-len(u['tags']) + u['tags'].index(t)] = _wrap_uri_tag(t, target_url)
            out.append('/kodik/hls?url=' + urllib.parse.quote(u['seg'], safe=''))
    if removed and not any(l.startswith('/kodik/hls') for l in out):
        return raw, 'application/vnd.apple.mpegurl'  # перестарались — отдаём как было
    body = ['#EXTM3U'] + header + out + trailer
    if removed:
        body.insert(1, '# AniHub: вырезано рекламных сегментов: %d' % removed)
    return '\n'.join(body).encode('utf-8'), 'application/vnd.apple.mpegurl'

# ---------------- хаб комнат: синхронизация между устройствами ----------------
ROOMS = {}
ROOMS_HB = {}
ROOMS_LOCK = threading.Lock()
ROOMS_COND = threading.Condition(ROOMS_LOCK)
ROOMS_FILE = os.path.join(ROOT, 'rooms.json')


def rooms_load():
    try:
        with open(ROOMS_FILE, 'r', encoding='utf-8') as f:
            j = json.load(f)
        for code, r in (j.get('rooms') or {}).items():
            ROOMS[code] = {'rev': int(r.get('rev') or 1), 'data': r.get('data') or {}}
    except Exception:
        pass


def rooms_save():
    try:
        tmp = ROOMS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'rooms': {c: {'rev': r['rev'], 'data': r['data']} for c, r in ROOMS.items()}}, f, ensure_ascii=False)
        os.replace(tmp, ROOMS_FILE)
    except Exception:
        pass


def room_bump(code):
    r = ROOMS.get(code)
    if r:
        r['rev'] += 1
        ROOMS_COND.notify_all()
    return r


rooms_load()


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
            with _OPENER.open(req, timeout=20) as r:
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

        # --- ping (клиент комнат будит им Render; локально отвечает мгновенно) ---
        if self.path.startswith("/api/ping"):
            return self._json(200, {"ok": True})

        # --- хаб комнат ---
        if self.path.startswith("/api/rooms"):
            with ROOMS_LOCK:
                out = {c: {'rev': r['rev'], 'data': r['data']} for c, r in ROOMS.items()}
            return self._json(200, {'rooms': out})

        if self.path.startswith("/api/room/"):
            code = urllib.parse.urlparse(self.path).path.split('/')[-1].upper()
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            since = int((q.get('since') or ['0'])[0] or 0)
            with ROOMS_COND:
                r = ROOMS.get(code)
                if not r:
                    return self._json(404, {"error": "комната не найдена на сервере"})
                deadline = time.time() + 20
                while r['rev'] <= since:
                    left = deadline - time.time()
                    if left <= 0:
                        break
                    ROOMS_COND.wait(left)
                hb = {u: t for u, t in ROOMS_HB.get(code, {}).items() if time.time() - t < 60}
                out = {'rev': r['rev'], 'data': r['data'], 'hb': hb}
            return self._json(200, out)

        # --- GIF-поиск Gifs.ru (для чата; у gifs.ru нет CORS — ходим через прокси) ---
        if self.path.startswith("/gifs"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            query = (q.get('q') or ['аниме'])[0]
            try:
                req = urllib.request.Request(
                    "https://gifs.ru/api/v1/Gif/GetGifs",
                    data=json.dumps({"query": query, "skip": 0, "take": 14}).encode('utf-8'),
                    headers={"Content-Type": "application/json",
                             "User-Agent": "Mozilla/5.0 (AniHub)"},
                    method="POST")
                with urllib.request.urlopen(req, timeout=15) as resp:
                    body = json.loads(resp.read().decode('utf-8', 'replace'))
                return self._json(200, body)
            except Exception as e:
                return self._json(502, {"error": str(e)})

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
                safe_target(url)
            except Blocked as b:
                return self._json(403, {"error": str(b)})
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
            try:
                if not url or not _host_allowed(urllib.parse.urlsplit(url).hostname):
                    return self._json(403, {"error": "host не в белом списке"})
                safe_target(url)
            except Blocked as b:
                return self._json(403, {"error": str(b)})
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

        if self.path.startswith("/api/room/"):
            code = urllib.parse.urlparse(self.path).path.split('/')[-1].upper()
            try:
                ln = int(self.headers.get('Content-Length') or 0)
                body = json.loads(self.rfile.read(ln).decode('utf-8') or '{}')
            except Exception:
                return self._json(400, {"error": "неверный JSON"})
            act = body.get('act')
            with ROOMS_LOCK:
                r = ROOMS.get(code)
                if act == 'put':
                    data = body.get('data') or {}
                    if not isinstance(data, dict) or len(json.dumps(data)) > 900000:
                        return self._json(413, {"error": "слишком большие данные комнаты"})
                    if r:
                        r['data'] = data
                        room_bump(code)
                    else:
                        ROOMS[code] = {'rev': 1, 'data': data}
                        ROOMS_COND.notify_all()
                    rooms_save()
                    return self._json(200, {'rev': ROOMS[code]['rev']})
                if not r:
                    return self._json(404, {"error": "комната не найдена на сервере"})
                if act == 'sync':
                    r['data']['sync'] = {'st': body.get('st'), 'uid': body.get('uid'), 'name': body.get('name'), 'at': time.time()}
                    room_bump(code)
                    return self._json(200, {'rev': r['rev']})
                if act == 'hb':
                    ROOMS_HB.setdefault(code, {})[body.get('uid') or '?'] = time.time()
                    ROOMS_HB[code] = {u: t for u, t in ROOMS_HB[code].items() if time.time() - t < 120}
                    return self._json(200, {'ok': True})
                return self._json(400, {"error": "неизвестное действие"})
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
