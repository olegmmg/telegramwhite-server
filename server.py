#!/usr/bin/env python3
"""
Combined Server for Render.com:
- TelegramWhite (WebSocket) on /ws
- WorldWar Game (WebSocket) on /game
- VPN Bot (HTTP) on / and /api/*
"""

import os, sys, json, asyncio, hashlib, time, logging, math, urllib.parse, urllib.request, urllib.error, base64, random, string
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from flask import Flask, request, jsonify, render_template_string
from github import Github, Auth
from github.GithubException import GithubException

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

# GitHub для VPN (токен через переменную окружения!)
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
    def patch(self, p, v): return self._req("PATCH", p, v)
    def push(self, p, v): return self._req("POST", p, v)

fb = Firebase(FIREBASE_DB_URL, FIREBASE_SECRET)

# ========== GAME STATE ==========
class GameState:
    def __init__(self):
        self.countries: Dict[str, dict] = {}
        self.projectiles: Dict[str, dict] = {}
        self._pid = 0
        self._load()
    def _load(self):
        try:
            data = fb.get("game/countries")
            if data: self.countries = data
            proj = fb.get("game/projectiles")
            if proj: self.projectiles = {k: v for k, v in proj.items() if v.get('active')}
        except: pass
    def _save_country(self, iso):
        if iso in self.countries:
            try: fb.set(f"game/countries/{iso}", self.countries[iso])
            except: pass
    def _save_proj(self, pid):
        if pid in self.projectiles:
            try: fb.set(f"game/projectiles/{pid}", self.projectiles[pid])
            except: pass
    def _next_pid(self):
        self._pid += 1
        return f"p{int(time.time())}_{self._pid}"
    def get_country(self, iso) -> dict:
        if iso not in self.countries:
            self.countries[iso] = {
                'iso': iso, 'players': [], 'cities': {},
                'radars': [], 'launchers': [], 'pvo': [],
                'crops': {}, 'territory_hp': 100.0, 'occupied_by': None,
                'recon_boost': {},
            }
            self._save_country(iso)
        return self.countries[iso]
    def join(self, iso, pid, username):
        c = self.get_country(iso)
        if not any(p['id'] == pid for p in c['players']):
            c['players'].append({'id': pid, 'name': username, 'coins': STARTING_COINS})
            c['crops'][pid] = {'amount': 0, 'last_tick': int(time.time())}
            self._save_country(iso)
        return c
    def get_player(self, iso, pid):
        c = self.countries.get(iso)
        if not c: return None
        return next((p for p in c['players'] if p['id'] == pid), None)
    def add_coins(self, iso, pid, amount):
        p = self.get_player(iso, pid)
        if p: p['coins'] = max(0, p.get('coins', 0) + amount); self._save_country(iso)
    def spend(self, iso, pid, amount) -> bool:
        p = self.get_player(iso, pid)
        if not p or p.get('coins', 0) < amount: return False
        p['coins'] -= amount; self._save_country(iso); return True
    def place_radar(self, iso, pid, lat, lon, radius=9.0) -> bool:
        if not self.spend(iso, pid, 200): return False
        c = self.get_country(iso)
        c['radars'].append({'lat': lat, 'lon': lon, 'radius': radius, 'angle': 0.0, 'sweep': 60.0, 'owner': pid})
        self._save_country(iso); return True
    def place_launcher(self, iso, pid, lat, lon) -> bool:
        if not self.spend(iso, pid, 300): return False
        c = self.get_country(iso)
        c['launchers'].append({'lat': lat, 'lon': lon, 'owner': pid, 'ammo': 50})
        self._save_country(iso); return True
    def place_pvo(self, iso, pid, pvo_type, lat, lon) -> bool:
        pdef = PVO_SYSTEMS.get(pvo_type)
        if not pdef: return False
        if not self.spend(iso, pid, pdef['cost']): return False
        c = self.get_country(iso)
        c['pvo'].append({'type': pvo_type, 'lat': lat, 'lon': lon, 'owner': pid, 'ammo': pdef['ammo'], 'last_shot': 0, 'auto': True, 'range': pdef['range']})
        self._save_country(iso); return True
    def launch(self, owner_iso, pid, utype, src_lat, src_lon, dst_lat, dst_lon, target_iso='', target_city='capital') -> Optional[str]:
        udef = UNITS.get(utype)
        if not udef: return None
        if not self.spend(owner_iso, pid, udef['cost']): return None
        pid_str = self._next_pid()
        self.projectiles[pid_str] = {
            'id': pid_str, 'type': utype, 'owner_iso': owner_iso, 'player_id': pid,
            'src_lat': src_lat, 'src_lon': src_lon, 'dst_lat': dst_lat, 'dst_lon': dst_lon,
            'lat': src_lat, 'lon': src_lon, 'active': True, 'launched_at': int(time.time()),
            'target_iso': target_iso, 'target_city': target_city, 'hp': udef['hp'],
            'speed': udef['speed'], 'stealth': udef.get('stealth', False),
        }
        self._save_proj(pid_str)
        return pid_str
    def tick_projectiles(self) -> List[dict]:
        impacts = []
        for pid, proj in list(self.projectiles.items()):
            if not proj['active']: continue
            speed = proj.get('speed', 0.3)
            dlat = proj['dst_lat'] - proj['lat']
            dlon = proj['dst_lon'] - proj['lon']
            dist = math.hypot(dlat, dlon)
            if dist <= speed:
                proj['lat'] = proj['dst_lat']; proj['lon'] = proj['dst_lon']
                proj['active'] = False
                impacts.append(dict(proj))
            else:
                r = speed / dist
                proj['lat'] += dlat * r; proj['lon'] += dlon * r
        return impacts
    def process_impact(self, proj) -> Optional[dict]:
        target_iso = proj.get('target_iso')
        if not target_iso: return None
        udef = UNITS.get(proj['type'], {})
        damage_mult = udef.get('damage_mult', 1.0)
        c = self.get_country(target_iso)
        is_played = bool(c['players'])
        shield = 1.0 if is_played else UNPLAYED_SHIELD
        base_damage = HP_PER_HIT * damage_mult / shield
        city_name = proj.get('target_city', 'capital')
        if city_name not in c['cities']:
            c['cities'][city_name] = {'hp': 100.0, 'hits': 0, 'destroyed': False}
        city = c['cities'][city_name]
        if city['destroyed']: return None
        city['hits'] += 1
        city['hp'] = max(0.0, city['hp'] - base_damage)
        if city['hp'] <= 0:
            city['hp'] = 0.0; city['destroyed'] = True
        self._save_country(target_iso)
        return {'hit_iso': target_iso, 'city': city_name, 'hp': city['hp'], 'destroyed': city['destroyed'], 'damage': round(base_damage, 2)}
    def get_detected(self, iso) -> List[dict]:
        c = self.countries.get(iso, {})
        radars = c.get('radars', [])
        visible = []
        for pid, proj in self.projectiles.items():
            if not proj['active']: continue
            if proj['owner_iso'] == iso: continue
            is_stealth = proj.get('stealth', False)
            for r in radars:
                dist = math.hypot(proj['lat'] - r['lat'], proj['lon'] - r['lon'])
                eff_radius = r['radius'] * (0.6 if is_stealth else 1.0)
                if dist > eff_radius: continue
                dlon = proj['lon'] - r['lon']; dlat = proj['lat'] - r['lat']
                bearing = (math.degrees(math.atan2(dlon, dlat)) + 360) % 360
                sweep_half = r['sweep'] / 2
                angle_diff = abs((bearing - r['angle'] + 180) % 360 - 180)
                if angle_diff <= sweep_half:
                    visible.append({**proj, 'detected': True}); break
        return visible
    def full_state(self, iso) -> dict:
        c = self.get_country(iso)
        detected = self.get_detected(iso)
        return {'country': c, 'projectiles': detected}

# ========== GAME SERVER ==========
class GameServer:
    def __init__(self, state: GameState):
        self.state = state
        self.players: Dict[str, dict] = {}
    async def _send(self, ws, data):
        try: await ws.send(json.dumps(data, ensure_ascii=False, default=str))
        except: pass
    async def _bcast(self, data):
        msg = json.dumps(data, ensure_ascii=False, default=str)
        tasks = [p['ws'].send(msg) for p in self.players.values()]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
    async def handle(self, ws, data):
        t = data.get('type')
        if t == 'ping':
            await self._send(ws, {'type': 'pong'}); return
        if t == 'join':
            pid = data.get('player_id', '')
            username = data.get('username', 'Аноним')
            iso = data.get('iso', '').upper()
            if not pid or not iso:
                await self._send(ws, {'type': 'error', 'msg': 'Нужен player_id и iso'}); return
            self.players[pid] = {'ws': ws, 'iso': iso, 'username': username}
            self.state.join(iso, pid, username)
            await self._send(ws, {'type': 'joined', 'iso': iso, 'state': self.state.full_state(iso),
                                  'units': UNITS, 'pvo_systems': PVO_SYSTEMS})
            return
        pid = data.get('player_id', '')
        info = self.players.get(pid)
        if not info:
            await self._send(ws, {'type': 'error', 'msg': 'Сначала join'}); return
        iso = info['iso']
        if t == 'place_radar':
            ok = self.state.place_radar(iso, pid, data['lat'], data['lon'], data.get('radius', 9.0))
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'radar'})
            return
        if t == 'place_launcher':
            ok = self.state.place_launcher(iso, pid, data['lat'], data['lon'])
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'launcher'})
            return
        if t == 'place_pvo':
            ok = self.state.place_pvo(iso, pid, data.get('pvo_type', 's300'), data['lat'], data['lon'])
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'pvo'})
            return
        if t == 'launch':
            pid_s = self.state.launch(iso, pid, data['utype'], data['src_lat'], data['src_lon'],
                                      data['dst_lat'], data['dst_lon'], data.get('target_iso', ''))
            if pid_s:
                await self._bcast({'type': 'proj_launched', 'proj': self.state.projectiles[pid_s]})
            else:
                await self._send(ws, {'type': 'error', 'msg': 'Нет монет'})
            return
        if t == 'get_state':
            await self._send(ws, {'type': 'state', 'state': self.state.full_state(iso)})
            return
    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try: await self.handle(ws, json.loads(raw))
                except: pass
        except websockets.exceptions.ConnectionClosed: pass
        finally:
            for pid, p in list(self.players.items()):
                if p['ws'] is ws:
                    self.players.pop(pid, None); break
    async def tick_loop(self):
        while True:
            await asyncio.sleep(GAME_TICK_MS / 1000)
            impacts = self.state.tick_projectiles()
            for proj in impacts:
                ev = self.state.process_impact(proj)
                if ev:
                    await self._bcast({'type': 'impact', **ev})
            for pid, p in self.players.items():
                det = self.state.get_detected(p['iso'])
                try: await self._send(p['ws'], {'type': 'projs_update', 'projs': det})
                except: pass

# ========== TELEGRAMWHITE MESSENGER ==========
class Database:
    def __init__(self):
        self.pool = None; self._init_pool(); self._init_tables()
    def _init_pool(self):
        for i in range(5):
            try:
                self.pool = ThreadedConnectionPool(1, 20, DATABASE_URL, sslmode='require', connect_timeout=10)
                log.info("✅ DB pool ok"); return
            except Exception as e:
                log.error(f"DB pool {i+1}/5: {e}")
                if i < 4: time.sleep(3*(i+1))
        sys.exit(1)
    def _get(self): return self.pool.getconn()
    def _put(self, c):
        if self.pool and c: self.pool.putconn(c)
    def execute(self, sql, params=(), *, one=False, all=False):
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            if one: row = cur.fetchone(); conn.commit(); return dict(row) if row else None
            elif all: rows = cur.fetchall(); conn.commit(); return [dict(r) for r in rows]
            else: conn.commit(); return None
        except Exception as e:
            if conn: conn.rollback()
            raise
        finally: self._put(conn)
    def _hash(self, p): return hashlib.pbkdf2_hmac('sha256', p.encode(), SECRET_KEY.encode(), 100_000).hex()
    def _init_tables(self):
        ddl = [
            """CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY,username TEXT UNIQUE NOT NULL, email TEXT DEFAULT'', password TEXT NOT NULL, bio TEXT DEFAULT'', status TEXT DEFAULT'offline', last_seen BIGINT NOT NULL DEFAULT 0, created_at BIGINT NOT NULL DEFAULT 0, messages_count INTEGER DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS chats(id SERIAL PRIMARY KEY,name TEXT NOT NULL, type TEXT NOT NULL DEFAULT'group', description TEXT DEFAULT'', created_by INTEGER, created_at BIGINT NOT NULL DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS chat_members(chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, role TEXT NOT NULL DEFAULT'member', joined_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY(chat_id,user_id))""",
            """CREATE TABLE IF NOT EXISTS messages(id SERIAL PRIMARY KEY,chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE, user_id INTEGER REFERENCES users(id), username TEXT NOT NULL, text TEXT NOT NULL, created_at BIGINT NOT NULL DEFAULT 0, deleted BOOLEAN DEFAULT FALSE)""",
            """CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, created_at BIGINT NOT NULL DEFAULT 0, expires_at BIGINT NOT NULL DEFAULT 0)""",
            """CREATE INDEX IF NOT EXISTS idx_msgs_chat ON messages(chat_id,id)""",
            "CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at)",
        ]
        conn = None
        try:
            conn = self._get(); cur = conn.cursor()
            for q in ddl: cur.execute(q)
            cur.execute("SELECT id FROM chats WHERE id=1")
            if not cur.fetchone():
                cur.execute("INSERT INTO chats(id,name,type,description,created_at) VALUES(1,'Общий чат','group','Добро пожаловать!',%s)", (int(time.time()),))
            conn.commit(); log.info("✅ DB schema ok")
        except: pass
        finally: self._put(conn)
    def create_user(self, u, p, e=""):
        now = int(time.time())
        try:
            user = self.execute("INSERT INTO users(username,email,password,last_seen,created_at) VALUES(%s,%s,%s,%s,%s) RETURNING id,username,email,bio,created_at", (u, e, self._hash(p), now, now), one=True)
            if user: self.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(1,%s,'member',%s) ON CONFLICT DO NOTHING", (user['id'], now))
            return user
        except: return None
    def verify_user(self, u, p): return self.execute("SELECT id,username,email,bio,status,created_at,messages_count FROM users WHERE username=%s AND password=%s", (u, self._hash(p)), one=True)
    def get_user(self, uid): return self.execute("SELECT id,username,email,bio,status,last_seen,created_at,messages_count FROM users WHERE id=%s", (uid,), one=True)
    def set_status(self, uid, s): self.execute("UPDATE users SET status=%s,last_seen=%s WHERE id=%s", (s, int(time.time()), uid))
    def create_session(self, uid): tok = hashlib.sha256(f"{uid}{time.time()}{SECRET_KEY}".encode()).hexdigest(); self.execute("INSERT INTO sessions(token,user_id,created_at,expires_at) VALUES(%s,%s,%s,%s)", (tok, uid, int(time.time()), int(time.time())+SESSION_TTL)); return tok
    def check_session(self, tok): r = self.execute("SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s", (tok, int(time.time())), one=True); return r['user_id'] if r else None
    def get_user_chats(self, uid): return self.execute("SELECT c.id,c.name,c.type,c.description FROM chats c JOIN chat_members cm ON cm.chat_id=c.id WHERE cm.user_id=%s", (uid,), all=True) or []
    def get_messages(self, cid, limit=MAX_HISTORY): return self.execute("SELECT id,chat_id,user_id,username,text,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE ORDER BY id ASC LIMIT %s", (cid, limit), all=True) or []
    def save_message(self, cid, uid, uname, text, system=False):
        try: return self.execute("INSERT INTO messages(chat_id,user_id,username,text,created_at,deleted) VALUES(%s,%s,%s,%s,%s,FALSE) RETURNING id,chat_id,user_id,username,text,created_at", (cid, uid, uname, text, int(time.time())), one=True)
        except: return None

class MessengerServer:
    def __init__(self, db):
        self.db = db; self.conns: Dict[int, dict] = {}
    async def _s(self, ws, d):
        try: await ws.send(json.dumps(d, ensure_ascii=False, default=str))
        except: pass
    async def _bc(self, d, cid, skip=None):
        msg = json.dumps(d, ensure_ascii=False, default=str)
        members = {m['user_id'] for m in self.db.execute("SELECT user_id FROM chat_members WHERE chat_id=%s", (cid,), all=True)}
        tasks = [c['ws'].send(msg) for uid, c in self.conns.items() if uid in members and uid != skip]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
    def _uid(self, ws):
        for uid, c in self.conns.items():
            if c['ws'] is ws: return uid
        return None
    async def handle(self, ws, data):
        t = data.get('type')
        if t == 'ping':
            await self._s(ws, {'type': 'pong'}); return
        if t == 'register':
            u = data.get('username', '').strip(); p = data.get('password', '')
            if len(u) < 3 or len(p) < 6:
                await self._s(ws, {'type': 'error', 'message': 'Имя >=3, пароль >=6'}); return
            user = self.db.create_user(u, p)
            if not user:
                await self._s(ws, {'type': 'error', 'message': 'Имя занято'}); return
            tok = self.db.create_session(user['id'])
            await self._s(ws, {'type': 'registered', 'user': user, 'token': tok}); return
        if t == 'login':
            u = data.get('username', '').strip(); p = data.get('password', '')
            user = self.db.verify_user(u, p)
            if not user:
                await self._s(ws, {'type': 'error', 'message': 'Неверный логин/пароль'}); return
            uid = user['id']; tok = self.db.create_session(uid); self.db.set_status(uid, 'online')
            self.conns[uid] = {'ws': ws, 'username': u}
            chats = self.db.get_user_chats(uid)
            await self._s(ws, {'type': 'logged_in', 'user': user, 'token': tok, 'chats': chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._s(ws, {'type': 'history', 'chat_id': ch['id'], 'messages': msgs})
            return
        if t == 'session':
            tok = data.get('token', ''); uid = self.db.check_session(tok)
            if not uid:
                await self._s(ws, {'type': 'error', 'message': 'Сессия истекла'}); return
            user = self.db.get_user(uid)
            if not user: return
            self.db.set_status(uid, 'online'); self.conns[uid] = {'ws': ws, 'username': user['username']}
            chats = self.db.get_user_chats(uid)
            await self._s(ws, {'type': 'session_ok', 'user': user, 'chats': chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._s(ws, {'type': 'history', 'chat_id': ch['id'], 'messages': msgs})
            return
        uid = self._uid(ws)
        if uid is None: return
        conn = self.conns[uid]
        if t == 'send_message':
            cid = data.get('chat_id', 1); text = data.get('text', '').strip()
            if not text: return
            msg = self.db.save_message(cid, uid, conn['username'], text)
            if msg: await self._bc({'type': 'message', 'message': msg}, cid)
            return
        if t == 'get_history':
            cid = data.get('chat_id', 1); lim = min(int(data.get('limit', MAX_HISTORY)), MAX_HISTORY)
            await self._s(ws, {'type': 'history', 'chat_id': cid, 'messages': self.db.get_messages(cid, lim)})
            return
    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try: await self.handle(ws, json.loads(raw))
                except: pass
        except websockets.exceptions.ConnectionClosed: pass
        finally:
            uid = self._uid(ws)
            if uid:
                self.conns.pop(uid, None); self.db.set_status(uid, 'offline')

# ========== VPN БОТ (Flask) ==========
flask_app = Flask(__name__)

vpn = type('VpnHelper', (), {
    'repo': None,
    'get_file_content': lambda self, path: None,
    'save_file': lambda self, path, content, msg: False,
    'generate_code': lambda self: str(random.randint(100000, 999999))
})()

if GITHUB_TOKEN:
    try:
        auth = Auth.Token(GITHUB_TOKEN)
        g = Github(auth=auth)
        vpn.repo = g.get_repo(REPO_NAME)
        vpn.get_file_content = lambda self, path: base64.b64decode(self.repo.get_contents(path, ref=BRANCH).content).decode() if self.repo else None
        def save_file(self, path, content, msg):
            if not self.repo: return False
            try:
                existing = self.repo.get_contents(path, ref=BRANCH)
                self.repo.update_file(path, msg, content, existing.sha, branch=BRANCH)
            except: self.repo.create_file(path, msg, content, branch=BRANCH)
            return True
        vpn.save_file = save_file.__get__(vpn)
        log.info("✅ GitHub подключён")
    except Exception as e:
        log.error(f"GitHub: {e}")

def create_subscription(sub_type, days):
    template_path = "vpn/sub" if sub_type == "main" else "vpn/test"
    output_dir = "vpn/subs" if sub_type == "main" else "vpn/tests"
    template = vpn.get_file_content(template_path)
    if not template: return None, "Шаблон не найден"
    expire_ts = int((datetime.now() + timedelta(days=days)).timestamp())
    expire_date = (datetime.now() + timedelta(days=days)).strftime("%d.%m.%Y")
    userinfo = f"#subscription-userinfo: upload=0; download=0; total=999999999999999999999999999999999; expire={expire_ts}\n"
    final = userinfo + template
    code = vpn.generate_code()
    filename = f"{code}_{random.randint(1000,9999)}"
    path = f"{output_dir}/{filename}"
    if vpn.save_file(path, final, f"Подписка {sub_type} до {expire_date}"):
        return f"https://olegmmg.github.io/{path}", f"✅ Ссылка: https://olegmmg.github.io/{path}"
    return None, "Ошибка"

ORDERS_DIR = "orders"

@flask_app.route('/')
def index():
    return render_template_string('''
    <h1>🚀 Сервер работает</h1>
    <ul>
        <li>📱 TelegramWhite: <code>ws://' + request.host + '/ws</code></li>
        <li>🎮 WorldWar: <code>ws://' + request.host + '/game</code></li>
        <li>🔐 VPN: <a href="/admin/orders">/admin/orders</a></li>
    </ul>
    ''')

@flask_app.route('/api/create-order', methods=['POST'])
def create_order():
    data = request.json
    code = vpn.generate_code()
    order = {
        'code': code, 'type': data.get('type'), 'duration': data.get('duration'),
        'days': data.get('days'), 'price': data.get('price'),
        'timestamp': datetime.now().isoformat(), 'status': 'pending'
    }
    if vpn.save_file(f"{ORDERS_DIR}/{code}.json", json.dumps(order, ensure_ascii=False), f"Заявка {code}"):
        return jsonify({'success': True, 'code': code})
    return jsonify({'success': False, 'error': 'Ошибка'})

@flask_app.route('/admin/orders')
def admin_orders():
    if not vpn.repo: return "<h1>GitHub не настроен</h1>"
    try:
        contents = vpn.repo.get_contents(ORDERS_DIR, ref=BRANCH)
        orders = []
        for c in contents:
            if c.name.endswith('.json'):
                data = json.loads(base64.b64decode(c.content).decode())
                orders.append(data)
        orders.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        html = '<html><head><title>Заявки</title><style>table{border-collapse:collapse}td,th{padding:8px;border:1px solid #ccc}</style></head><body><h1>Заявки на оплату</h1><tr><th>Код</th><th>Тип</th><th>Срок</th><th>Сумма</th><th>Дата</th><th>Статус</th></tr>'
        for o in orders:
            html += f'<tr><td>{o.get("code")}</td><td>{o.get("type")}</td><td>{o.get("duration")}</td><td>{o.get("price")}₽</td><td>{o.get("timestamp")[:16]}</td><td>{o.get("status")}</td></tr>'
        html += '</table><h2>Подтвердить оплату</h2><form method="POST"><input name="code" placeholder="Код"><button type="submit">Подтвердить</button></form></body></html>'
        return html
    except: return "<h1>Нет заявок</h1>"

@flask_app.route('/admin/confirm', methods=['POST'])
def confirm_order():
    code = request.form.get('code')
    path = f"{ORDERS_DIR}/{code}.json"
    content = vpn.get_file_content(path)
    if not content: return "Не найдено"
    order = json.loads(content)
    url, _ = create_subscription(order.get('type', 'main'), int(order.get('days', 30)))
    if url:
        order['status'] = 'completed'
        order['subscription_url'] = url
        vpn.save_file(path, json.dumps(order, ensure_ascii=False), f"Подтверждена {code}")
        return f"✅ Подписка создана: {url}"
    return "Ошибка"

# ========== ЗАПУСК ВСЕГО НА ОДНОМ ПОРТУ ==========
async def main():
    log.info("=" * 56)
    log.info("🚀 Combined Server: TelegramWhite + WorldWar + VPN")
    log.info("=" * 56)
    
    db = Database()
    msg_server = MessengerServer(db)
    game_state = GameState()
    game_server = GameServer(game_state)
    
    # Запускаем HTTP (Flask) в отдельном потоке
    from threading import Thread
    def run_flask():
        flask_app.run(host=HOST, port=PORT, debug=False, use_reloader=False)
    Thread(target=run_flask, daemon=True).start()
    
    # Запускаем WebSocket серверы на том же порту, но разных путях
    # Используем websockets.serve с process_request для роутинга
    async def route_request(path, request_headers):
        if path == "/ws":
            return msg_server.ws_handler
        elif path == "/game":
            return game_server.ws_handler
        else:
            return None  # отдать Flask'у
    
    from websockets.legacy.server import serve as ws_serve
    from websockets.legacy.http import read_request
    
    async def combined_handler(ws, path):
        if path == "/ws":
            await msg_server.ws_handler(ws)
        elif path == "/game":
            await game_server.ws_handler(ws)
        else:
            await ws.close(1008, "Not found")
    
    async def custom_serve(ws, path):
        if path == "/ws":
            await msg_server.ws_handler(ws)
        elif path == "/game":
            await game_server.ws_handler(ws)
        else:
            await ws.close(1008, "Not found")
    
    server = await websockets.serve(custom_serve, HOST, PORT,
                                     ping_interval=PING_INTERVAL,
                                     ping_timeout=PING_TIMEOUT,
                                     max_size=10*1024*1024)
    
    log.info(f"✅ TelegramWhite: ws://{HOST}:{PORT}/ws")
    log.info(f"✅ WorldWar: ws://{HOST}:{PORT}/game")
    log.info(f"✅ VPN Bot: http://{HOST}:{PORT}")
    log.info(f"✅ Admin: http://{HOST}:{PORT}/admin/orders")
    
    asyncio.create_task(game_server.tick_loop())
    
    await asyncio.Future()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("👋 Стоп")
    except Exception as e:
        log.error(f"FATAL: {e}", exc_info=True)
        sys.exit(1)
