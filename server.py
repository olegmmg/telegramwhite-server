#!/usr/bin/env python3
"""
Combined Server for Render.com:
- TelegramWhite (WebSocket) on /ws
- WorldWar Game (WebSocket) on /game
- VPN Bot + Admin Panel (HTTP) on /, /admin/orders, /api/*
All on ONE PORT
"""

import os, sys, json, asyncio, hashlib, time, logging, math, urllib.parse, urllib.request, urllib.error, base64, random, string
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

try:
    import websockets
except ImportError:
    print("ERROR: pip install websockets"); sys.exit(1)
try:
    import psycopg2, psycopg2.extras
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
log = logging.getLogger("combined")

# ========== КОНФИГУРАЦИЯ ==========
PORT = int(os.environ.get("PORT", 10000))
HOST = "0.0.0.0"
SECRET_KEY = os.environ.get("TW_SECRET", "telegramwhite-2025")
SESSION_TTL = 30 * 24 * 3600
MAX_MSG_LEN = 4096
MAX_HISTORY = 100
PING_INTERVAL = 20
PING_TIMEOUT = 10

# GitHub для VPN
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_NAME = "olegmmg/olegmmg.github.io"
BRANCH = "main"

# Firebase
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL",
    "https://database-52a2b-default-rtdb.europe-west1.firebasedatabase.app")
FIREBASE_SECRET = os.environ.get("FIREBASE_SECRET", "")

# DATABASE_URL
def get_database_url():
    raw = os.environ.get("DATABASE_URL", "").strip()
    if not raw:
        log.error("❌ DATABASE_URL не задан!"); sys.exit(1)
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://"):]
    return raw

DATABASE_URL = get_database_url()

# ========== ИГРОВЫЕ КОНСТАНТЫ ==========
STARTING_COINS = 2000
HP_PER_HIT = 0.4
UNPLAYED_SHIELD = 2.5
RADAR_ROTATE_DEG = 1.5
GAME_TICK_MS = 100
CROP_TICK_SEC = 20
CROP_SELL_PRICE = 12

UNITS = {
    'shahed': {'name': 'Шахед-136', 'cost': 30, 'speed': 0.06, 'damage_mult': 1.0,
               'hp': 1, 'splash': 0.0, 'range': 999, 'stealth': False,
               'icon': '🛸', 'color': '#ff8800'},
    'fpv': {'name': 'ФПВ-дрон', 'cost': 15, 'speed': 0.15, 'damage_mult': 0.5,
            'hp': 1, 'splash': 0.0, 'range': 8, 'stealth': True,
            'icon': '🔴', 'color': '#ff4444'},
    'recon': {'name': 'Дрон-разведчик', 'cost': 50, 'speed': 0.1, 'damage_mult': 0.0,
              'hp': 1, 'splash': 0.0, 'range': 30, 'stealth': True,
              'icon': '👁', 'color': '#00aaff'},
    'missile': {'name': 'Ракета (Искандер)', 'cost': 80, 'speed': 0.5, 'damage_mult': 1.5,
                'hp': 2, 'splash': 0.3, 'range': 999, 'stealth': False,
                'icon': '🚀', 'color': '#ff3333'},
    'cruise': {'name': 'Крылатая ракета', 'cost': 120, 'speed': 0.25, 'damage_mult': 2.0,
               'hp': 2, 'splash': 0.5, 'range': 999, 'stealth': True,
               'icon': '✈', 'color': '#ff6600'},
    'oresnik': {'name': 'Орешник', 'cost': 500, 'speed': 1.5, 'damage_mult': 25.0,
                'hp': 10, 'splash': 2.0, 'range': 999, 'stealth': False,
                'icon': '☢', 'color': '#ff00ff'},
    'bomber': {'name': 'Бомбардировщик Ту-95', 'cost': 800, 'speed': 0.12, 'damage_mult': 5.0,
               'hp': 8, 'splash': 1.5, 'range': 999, 'stealth': False,
               'icon': '✈', 'color': '#cc4400'},
}

PVO_SYSTEMS = {
    's300': {'name': 'С-300', 'cost': 400, 'range': 12.0, 'ammo': 24,
             'reload_sec': 8, 'intercept_chance': 0.75, 'icon': '🟢', 'auto': True},
    'patriot': {'name': 'Patriot PAC-3', 'cost': 600, 'range': 15.0, 'ammo': 16,
                'reload_sec': 12, 'intercept_chance': 0.85, 'icon': '🔵', 'auto': True},
    'iris_t': {'name': 'IRIS-T SLM', 'cost': 350, 'range': 8.0, 'ammo': 30,
               'reload_sec': 5, 'intercept_chance': 0.70, 'icon': '🟡', 'auto': True},
}

# ========== FIREBASE ==========
class Firebase:
    def __init__(self, db_url, secret):
        self.base = db_url.rstrip('/')
        self.auth = f"?auth={secret}" if secret else ""
    def _req(self, method, path, body=None):
        url = f"{self.base}/{path}.json{self.auth}"
        data = json.dumps(body, ensure_ascii=False, default=str).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=6) as r:
                resp = r.read().decode()
                return json.loads(resp) if resp.strip() not in ('null', '') else None
        except: return None
    def get(self, p): return self._req("GET", p)
    def set(self, p, v): return self._req("PUT", p, v)
    def push(self, p, v): return self._req("POST", p, v)

fb = Firebase(FIREBASE_DB_URL, FIREBASE_SECRET)

# ========== GAME STATE (сокращённая версия) ==========
class GameState:
    def __init__(self):
        self.countries: Dict[str, dict] = {}
        self.projectiles: Dict[str, dict] = {}
        self._pid = 0
    def _save_country(self, iso): pass
    def get_country(self, iso) -> dict:
        if iso not in self.countries:
            self.countries[iso] = {'iso': iso, 'players': [], 'cities': {}, 'radars': [], 'launchers': [], 'pvo': [], 'crops': {}, 'territory_hp': 100.0}
        return self.countries[iso]
    def join(self, iso, pid, username):
        c = self.get_country(iso)
        if not any(p['id'] == pid for p in c['players']):
            c['players'].append({'id': pid, 'name': username, 'coins': STARTING_COINS})
        return c
    def get_player(self, iso, pid):
        c = self.countries.get(iso)
        return next((p for p in c['players'] if p['id'] == pid), None) if c else None
    def spend(self, iso, pid, amount) -> bool:
        p = self.get_player(iso, pid)
        if not p or p.get('coins', 0) < amount: return False
        p['coins'] -= amount; return True

# ========== GAME SERVER ==========
class GameServer:
    def __init__(self, state: GameState):
        self.state = state
        self.players: Dict[str, dict] = {}
    async def _send(self, ws, data):
        try: await ws.send(json.dumps(data, ensure_ascii=False, default=str))
        except: pass
    async def handle(self, ws, data):
        t = data.get('type')
        if t == 'ping':
            await self._send(ws, {'type': 'pong'})
        elif t == 'join':
            pid = data.get('player_id', '')
            username = data.get('username', 'Аноним')
            iso = data.get('iso', '').upper()
            if pid and iso:
                self.players[pid] = {'ws': ws, 'iso': iso, 'username': username}
                self.state.join(iso, pid, username)
                await self._send(ws, {'type': 'joined', 'iso': iso, 'units': UNITS})
    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try: await self.handle(ws, json.loads(raw))
                except: pass
        except: pass

# ========== MESSENGER ==========
class Database:
    def __init__(self):
        self.pool = None
        try:
            self.pool = ThreadedConnectionPool(1, 10, DATABASE_URL, sslmode='require', connect_timeout=10)
            log.info("✅ DB pool ok")
        except Exception as e:
            log.error(f"DB: {e}")
    def execute(self, sql, params=(), one=False, all=False):
        if not self.pool: return None
        conn = self.pool.getconn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            if one: return dict(cur.fetchone()) if cur.rowcount else None
            if all: return [dict(r) for r in cur.fetchall()]
            conn.commit(); return None
        finally: self.pool.putconn(conn)
    def get_user_chats(self, uid):
        return self.execute("SELECT id,name,type FROM chats c JOIN chat_members cm ON cm.chat_id=c.id WHERE cm.user_id=%s", (uid,), all=True) or []
    def get_messages(self, cid, limit=50):
        return self.execute("SELECT id,user_id,username,text,created_at FROM messages WHERE chat_id=%s ORDER BY id DESC LIMIT %s", (cid, limit), all=True) or []
    def save_message(self, cid, uid, uname, text):
        return self.execute("INSERT INTO messages(chat_id,user_id,username,text,created_at) VALUES(%s,%s,%s,%s,%s) RETURNING id,user_id,username,text,created_at", (cid, uid, uname, text, int(time.time())), one=True)

class MessengerServer:
    def __init__(self, db):
        self.db = db
        self.conns: Dict[int, dict] = {}
    async def _send(self, ws, data):
        try: await ws.send(json.dumps(data, ensure_ascii=False, default=str))
        except: pass
    async def handle(self, ws, data):
        t = data.get('type')
        if t == 'ping':
            await self._send(ws, {'type': 'pong'})
        elif t == 'get_messages':
            cid = data.get('chat_id', 1)
            msgs = self.db.get_messages(cid)
            await self._send(ws, {'type': 'messages', 'messages': msgs})
    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try: await self.handle(ws, json.loads(raw))
                except: pass
        except: pass

# ========== HTTP HANDLER (админка и API) ==========
class HTTPHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    
    def do_GET(self):
        path = self.path.split('?')[0]
        if path == '/':
            self._send_html('''
            <h1>🚀 Сервер работает</h1>
            <ul>
                <li>📱 TelegramWhite: <code>ws://your-server/ws</code></li>
                <li>🎮 WorldWar: <code>ws://your-server/game</code></li>
                <li>🔐 VPN: <a href="/admin/orders">/admin/orders</a></li>
            </ul>
            ''')
        elif path == '/admin/orders':
            self._send_html('''
            <h1>Заявки на оплату</h1>
            <p>Заглушка — для полноценной работы настройте GitHub токен</p>
            <form method="POST" action="/admin/confirm">
                <input name="code" placeholder="Код из заявки">
                <button type="submit">Подтвердить</button>
            </form>
            ''')
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_POST(self):
        if self.path == '/admin/confirm':
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode()
            import urllib.parse
            params = urllib.parse.parse_qs(body)
            code = params.get('code', [''])[0]
            self._send_html(f'<h1>Заявка {code} обработана</h1><a href="/">Назад</a>')
        else:
            self.send_response(404)
            self.end_headers()
    
    def _send_html(self, html):
        self.send_response(200)
        self.send_header('Content-type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(html.encode('utf-8'))

def run_http():
    server = HTTPServer((HOST, PORT), HTTPHandler)
    server.serve_forever()

# ========== MAIN ==========
async def main():
    log.info("=" * 56)
    log.info("🚀 Combined Server: TelegramWhite + WorldWar + VPN")
    log.info("=" * 56)
    
    db = Database()
    msg_server = MessengerServer(db)
    game_state = GameState()
    game_server = GameServer(game_state)
    
    # Запускаем HTTP сервер в потоке
    http_thread = threading.Thread(target=run_http, daemon=True)
    http_thread.start()
    log.info(f"✅ HTTP Admin: http://{HOST}:{PORT}")
    
    # Функция роутинга WebSocket соединений
    async def route(ws, path):
        if path == "/ws":
            await msg_server.ws_handler(ws)
        elif path == "/game":
            await game_server.ws_handler(ws)
        else:
            await ws.close(1008, "Not found")
    
    async with websockets.serve(route, HOST, PORT, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT):
        log.info(f"✅ TelegramWhite: ws://{HOST}:{PORT}/ws")
        log.info(f"✅ WorldWar: ws://{HOST}:{PORT}/game")
        log.info(f"✅ Сервер запущен на порту {PORT}")
        await asyncio.Future()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("👋 Стоп")
    except Exception as e:
        log.error(f"FATAL: {e}")
        sys.exit(1)
