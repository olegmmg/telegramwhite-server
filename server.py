#!/usr/bin/env python3
"""
Combined Server: TelegramWhite (PORT) + WorldWar Game (GAME_PORT)
Firebase: database-52a2b-default-rtdb.europe-west1.firebasedatabase.app
"""

import os, sys, json, asyncio, hashlib, time, logging, math, urllib.parse, urllib.request, urllib.error
from typing import Dict, Optional, List

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

# ── Config ──────────────────────────────────────────────────────────────
MESSENGER_PORT = int(os.environ.get("PORT", 10000))
GAME_PORT      = int(os.environ.get("GAME_PORT", 10001))
HOST           = "0.0.0.0"
SECRET_KEY     = os.environ.get("TW_SECRET", "telegramwhite-2025")
SESSION_TTL    = 30 * 24 * 3600
MAX_MSG_LEN    = 4096
MAX_HISTORY    = 100
PING_INTERVAL  = 20
PING_TIMEOUT   = 10

FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL",
    "https://database-52a2b-default-rtdb.europe-west1.firebasedatabase.app")
FIREBASE_SECRET = os.environ.get("FIREBASE_SECRET", "")

# ── Game constants ───────────────────────────────────────────────────────
STARTING_COINS    = 2000
HP_PER_HIT        = 0.4        # each hit = 0.4% city HP
UNPLAYED_SHIELD   = 2.5
RADAR_ROTATE_DEG  = 1.5        # degrees per 100ms tick
GAME_TICK_MS      = 100
CROP_TICK_SEC     = 20
CROP_SELL_PRICE   = 12

# ── Unit definitions ─────────────────────────────────────────────────────
UNITS = {
    # key: {name, cost, speed(deg/tick), damage_mult, hp, splash_radius(deg),
    #       range(deg), stealth(bool), description}
    'shahed': {
        'name': 'Шахед-136', 'cost': 30, 'speed': 0.06, 'damage_mult': 1.0,
        'hp': 1, 'splash': 0.0, 'range': 999, 'stealth': False,
        'icon': '🛸', 'color': '#ff8800',
        'desc': 'Дешёвый дрон-камикадзе. Медленный, но дешёвый.'
    },
    'fpv': {
        'name': 'ФПВ-дрон', 'cost': 15, 'speed': 0.15, 'damage_mult': 0.5,
        'hp': 1, 'splash': 0.0, 'range': 8, 'stealth': True,
        'icon': '🔴', 'color': '#ff4444',
        'desc': 'Быстрый ФПВ. Малый радиус, плохо виден радарам.'
    },
    'recon': {
        'name': 'Дрон-разведчик', 'cost': 50, 'speed': 0.1, 'damage_mult': 0.0,
        'hp': 1, 'splash': 0.0, 'range': 30, 'stealth': True,
        'icon': '👁', 'color': '#00aaff',
        'desc': 'Не атакует. Расширяет зону видимости радара на 30°.'
    },
    'missile': {
        'name': 'Ракета (Искандер)', 'cost': 80, 'speed': 0.5, 'damage_mult': 1.5,
        'hp': 2, 'splash': 0.3, 'range': 999, 'stealth': False,
        'icon': '🚀', 'color': '#ff3333',
        'desc': 'Баллистическая ракета. Быстрая, сложнее сбить.'
    },
    'cruise': {
        'name': 'Крылатая ракета', 'cost': 120, 'speed': 0.25, 'damage_mult': 2.0,
        'hp': 2, 'splash': 0.5, 'range': 999, 'stealth': True,
        'icon': '✈', 'color': '#ff6600',
        'desc': 'Летит низко, труднее обнаружить. Высокий урон.'
    },
    'oresnik': {
        'name': 'Орешник', 'cost': 500, 'speed': 1.5, 'damage_mult': 25.0,
        'hp': 10, 'splash': 2.0, 'range': 999, 'stealth': False,
        'icon': '☢', 'color': '#ff00ff',
        'desc': 'Гиперзвуковая ракета. 25% урона городу. Сбивает всё в радиусе 2°.'
    },
    'bomber': {
        'name': 'Бомбардировщик Ту-95', 'cost': 800, 'speed': 0.12, 'damage_mult': 5.0,
        'hp': 8, 'splash': 1.5, 'range': 999, 'stealth': False,
        'icon': '✈', 'color': '#cc4400',
        'desc': 'Медленный, но живучий. Огромный урон по площади.'
    },
}

# ── PVO definitions ─────────────────────────────────────────────────────
PVO_SYSTEMS = {
    's300': {
        'name': 'С-300', 'cost': 400, 'range': 12.0, 'ammo': 24,
        'reload_sec': 8, 'intercept_chance': 0.75, 'icon': '🟢',
        'auto': True,
        'desc': 'Советский/российский ЗРК. Средний радиус, высокая ёмкость.'
    },
    'patriot': {
        'name': 'Patriot PAC-3', 'cost': 600, 'range': 15.0, 'ammo': 16,
        'reload_sec': 12, 'intercept_chance': 0.85, 'icon': '🔵',
        'auto': True,
        'desc': 'Американский ЗРК. Больший радиус, выше точность.'
    },
    'iris_t': {
        'name': 'IRIS-T SLM', 'cost': 350, 'range': 8.0, 'ammo': 30,
        'reload_sec': 5, 'intercept_chance': 0.70, 'icon': '🟡',
        'auto': True,
        'desc': 'Немецкий ЗРК. Малый радиус, быстрая перезарядка.'
    },
}

# ═══════════════════════════════════════════════════════════════════════
#   DATABASE_URL normalization
# ═══════════════════════════════════════════════════════════════════════
def get_database_url():
    raw = os.environ.get("DATABASE_URL", "").strip()
    if not raw:
        log.error("❌ DATABASE_URL не задан!"); sys.exit(1)
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://"):]
    try:
        r = urllib.parse.urlparse(raw)
        if not r.scheme or not r.hostname: raise ValueError("bad url")
        log.info(f"✅ DB: {r.scheme}://{r.hostname}/{r.path.lstrip('/')}")
    except Exception as e:
        log.error(f"❌ DATABASE_URL invalid: {e}"); sys.exit(1)
    return raw

DATABASE_URL = get_database_url()

# ═══════════════════════════════════════════════════════════════════════
#   FIREBASE REST
# ═══════════════════════════════════════════════════════════════════════
class Firebase:
    def __init__(self, db_url, secret):
        self.base = db_url.rstrip('/')
        self.auth = f"?auth={secret}" if secret else ""

    def _req(self, method, path, body=None):
        url  = f"{self.base}/{path}.json{self.auth}"
        data = json.dumps(body, ensure_ascii=False, default=str).encode() if body is not None else None
        req  = urllib.request.Request(url, data=data, method=method,
                                      headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=6) as r:
                resp = r.read().decode()
                return json.loads(resp) if resp.strip() not in ('null', '') else None
        except urllib.error.HTTPError as e:
            log.warning(f"Firebase {method} {path}: HTTP {e.code}")
            return None
        except Exception as e:
            log.warning(f"Firebase {method} {path}: {e}")
            return None

    def get(self, p):        return self._req("GET",    p)
    def set(self, p, v):     return self._req("PUT",    p, v)
    def patch(self, p, v):   return self._req("PATCH",  p, v)
    def push(self, p, v):    return self._req("POST",   p, v)
    def delete(self, p):     return self._req("DELETE", p)

fb = Firebase(FIREBASE_DB_URL, FIREBASE_SECRET)
log.info(f"✅ Firebase: {FIREBASE_DB_URL}")

# ═══════════════════════════════════════════════════════════════════════
#   GAME STATE
# ═══════════════════════════════════════════════════════════════════════
class GameState:
    def __init__(self):
        self.countries:    Dict[str, dict] = {}
        self.projectiles:  Dict[str, dict] = {}
        self._pid = 0
        self._load()

    def _load(self):
        try:
            data = fb.get("game/countries")
            if data: self.countries = data; log.info(f"✅ Загружено стран: {len(data)}")
            proj = fb.get("game/projectiles")
            if proj:
                self.projectiles = {k: v for k, v in proj.items() if v.get('active')}
                log.info(f"✅ Загружено снарядов: {len(self.projectiles)}")
        except Exception as e:
            log.error(f"❌ Firebase load: {e}")

    def _save_country(self, iso):
        if iso in self.countries:
            try: fb.set(f"game/countries/{iso}", self.countries[iso])
            except: pass

    def _save_proj(self, pid):
        if pid in self.projectiles:
            try: fb.set(f"game/projectiles/{pid}", self.projectiles[pid])
            except: pass

    def _log(self, ev):
        try: fb.push("game/events", {**ev, 't': int(time.time())})
        except: pass

    def _next_pid(self):
        self._pid += 1
        return f"p{int(time.time())}_{self._pid}"

    # ── Country ────────────────────────────────────────────────────────
    def get_country(self, iso) -> dict:
        if iso not in self.countries:
            self.countries[iso] = {
                'iso': iso, 'players': [], 'cities': {},
                'radars': [], 'launchers': [], 'pvo': [],
                'crops': {}, 'territory_hp': 100.0, 'occupied_by': None,
                'recon_boost': {},   # player_id → expires_ts
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

    def leave(self, iso, pid):
        if iso not in self.countries: return
        c = self.countries[iso]
        c['players'] = [p for p in c['players'] if p['id'] != pid]
        self._save_country(iso)

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

    # ── Buildings ──────────────────────────────────────────────────────
    def place_radar(self, iso, pid, lat, lon, radius=9.0) -> bool:
        if not self.spend(iso, pid, 200): return False
        c = self.get_country(iso)
        c['radars'].append({'lat': lat, 'lon': lon, 'radius': radius,
                            'angle': 0.0, 'sweep': 60.0, 'owner': pid,
                            'recon_bonus': 0.0})
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
        c['pvo'].append({
            'type': pvo_type, 'lat': lat, 'lon': lon, 'owner': pid,
            'ammo': pdef['ammo'], 'last_shot': 0,
            'auto': True, 'manual_target': None,
            'range': pdef['range'],
        })
        self._save_country(iso); return True

    # ── Launch ────────────────────────────────────────────────────────
    def launch(self, owner_iso, pid, utype, src_lat, src_lon,
               dst_lat, dst_lon, target_iso='', target_city='capital') -> Optional[str]:
        udef = UNITS.get(utype)
        if not udef: return None
        if not self.spend(owner_iso, pid, udef['cost']): return None
        pid_str = self._next_pid()
        self.projectiles[pid_str] = {
            'id': pid_str, 'type': utype, 'owner_iso': owner_iso, 'player_id': pid,
            'src_lat': src_lat, 'src_lon': src_lon,
            'dst_lat': dst_lat, 'dst_lon': dst_lon,
            'lat': src_lat, 'lon': src_lon, 'active': True,
            'launched_at': int(time.time()),
            'target_iso': target_iso, 'target_city': target_city,
            'hp': udef['hp'], 'speed': udef['speed'],
            'stealth': udef.get('stealth', False),
        }
        self._save_proj(pid_str)
        self._log({'type': 'launch', 'owner': owner_iso, 'utype': utype,
                   'target': target_iso})
        return pid_str

    # ── PVO intercept ──────────────────────────────────────────────────
    def try_intercept_pvo(self, iso, pvo_idx, proj_id) -> dict:
        """Manual PVO shot at specific projectile."""
        c    = self.get_country(iso)
        pvos = c.get('pvo', [])
        if pvo_idx >= len(pvos): return {'ok': False, 'reason': 'Нет такой ПВО'}
        pvo  = pvos[pvo_idx]
        pdef = PVO_SYSTEMS.get(pvo['type'], {})
        proj = self.projectiles.get(proj_id)
        if not proj or not proj['active']: return {'ok': False, 'reason': 'Цель уже уничтожена'}
        if pvo.get('ammo', 0) <= 0: return {'ok': False, 'reason': 'Нет боеприпасов'}
        now = time.time()
        if now - pvo.get('last_shot', 0) < pdef.get('reload_sec', 8):
            return {'ok': False, 'reason': 'Перезарядка...'}
        dist = math.hypot(proj['lat'] - pvo['lat'], proj['lon'] - pvo['lon'])
        if dist > pvo['range']: return {'ok': False, 'reason': 'Вне зоны досягаемости'}
        pvo['ammo'] -= 1; pvo['last_shot'] = now
        chance = pdef.get('intercept_chance', 0.75)
        # Reduce chance for fast/stealthy targets
        udef = UNITS.get(proj['type'], {})
        if udef.get('stealth'): chance *= 0.7
        if udef.get('speed', 0) > 0.8: chance *= 0.6
        hit = (hash(f"{proj_id}{now}") % 100) / 100.0 < chance
        if hit:
            proj['hp'] = max(0, proj['hp'] - 2)
            if proj['hp'] <= 0:
                proj['active'] = False
                self._save_proj(proj_id)
                self._save_country(iso)
                self._log({'type': 'intercept', 'by': iso, 'pid': proj_id, 'manual': True})
                return {'ok': True, 'destroyed': True}
            self._save_proj(proj_id)
        self._save_country(iso)
        return {'ok': True, 'destroyed': False, 'hit': hit}

    def auto_pvo_tick(self, iso) -> List[str]:
        """Auto-PVO: shoot at nearest projectile in range. Returns destroyed proj IDs."""
        c       = self.get_country(iso)
        pvos    = c.get('pvo', [])
        now     = time.time()
        destroyed = []
        for pvo in pvos:
            if not pvo.get('auto'): continue
            if pvo.get('ammo', 0) <= 0: continue
            pdef = PVO_SYSTEMS.get(pvo['type'], {})
            if now - pvo.get('last_shot', 0) < pdef.get('reload_sec', 8): continue
            # Find nearest enemy proj in range
            best_pid, best_dist = None, 999
            for pid, proj in self.projectiles.items():
                if not proj['active']: continue
                if proj['owner_iso'] == iso: continue
                dist = math.hypot(proj['lat'] - pvo['lat'], proj['lon'] - pvo['lon'])
                if dist < pvo['range'] and dist < best_dist:
                    best_dist = dist; best_pid = pid
            if not best_pid: continue
            proj = self.projectiles[best_pid]
            pvo['ammo'] -= 1; pvo['last_shot'] = now
            udef   = UNITS.get(proj['type'], {})
            chance = pdef.get('intercept_chance', 0.75)
            if udef.get('stealth'): chance *= 0.7
            if udef.get('speed', 0) > 0.8: chance *= 0.5
            hit = (hash(f"{best_pid}{now}{iso}") % 100) / 100.0 < chance
            if hit:
                proj['hp'] -= 2
                if proj['hp'] <= 0:
                    proj['active'] = False
                    destroyed.append(best_pid)
                    self._log({'type': 'intercept', 'by': iso, 'pid': best_pid, 'auto': True})
                self._save_proj(best_pid)
        if pvos: self._save_country(iso)
        return destroyed

    # ── Projectile tick ────────────────────────────────────────────────
    def tick_projectiles(self) -> List[dict]:
        impacts = []
        for pid, proj in list(self.projectiles.items()):
            if not proj['active']: continue
            speed = proj.get('speed', 0.3)
            dlat  = proj['dst_lat'] - proj['lat']
            dlon  = proj['dst_lon'] - proj['lon']
            dist  = math.hypot(dlat, dlon)
            if dist <= speed:
                proj['lat'] = proj['dst_lat']; proj['lon'] = proj['dst_lon']
                proj['active'] = False
                impacts.append(dict(proj))
            else:
                r = speed / dist
                proj['lat'] += dlat * r; proj['lon'] += dlon * r
        return impacts

    def tick_radars(self):
        for iso, c in self.countries.items():
            for r in c.get('radars', []):
                r['angle'] = (r['angle'] + RADAR_ROTATE_DEG) % 360

    def process_impact(self, proj) -> Optional[dict]:
        target_iso  = proj.get('target_iso')
        if not target_iso: return None
        udef        = UNITS.get(proj['type'], {})
        damage_mult = udef.get('damage_mult', 1.0)
        splash      = udef.get('splash', 0.0)
        c           = self.get_country(target_iso)
        is_played   = bool(c['players'])
        shield      = 1.0 if is_played else UNPLAYED_SHIELD
        base_damage = HP_PER_HIT * damage_mult / shield

        # Орешник: сбивает всё в радиусе splash
        splash_killed = []
        if splash > 0:
            for pid2, p2 in list(self.projectiles.items()):
                if not p2['active']: continue
                if math.hypot(p2['lat'] - proj['dst_lat'],
                              p2['lon'] - proj['dst_lon']) <= splash:
                    p2['active'] = False
                    splash_killed.append(pid2)

        city_name = proj.get('target_city', 'capital')
        if city_name not in c['cities']:
            c['cities'][city_name] = {'hp': 100.0, 'hits': 0, 'destroyed': False}
        city = c['cities'][city_name]
        if city['destroyed']:
            self._save_country(target_iso)
            return None

        city['hits'] += 1
        city['hp']    = max(0.0, city['hp'] - base_damage)
        if city['hp'] <= 0:
            city['hp'] = 0.0; city['destroyed'] = True

        self._save_country(target_iso)
        self._log({'type': 'impact', 'iso': target_iso, 'city': city_name,
                   'hp': city['hp'], 'destroyed': city['destroyed'],
                   'proj_type': proj['type']})
        return {
            'hit_iso': target_iso, 'city': city_name,
            'hp': city['hp'], 'destroyed': city['destroyed'],
            'proj_id': proj['id'], 'proj_type': proj['type'],
            'damage': round(base_damage, 2),
            'splash_killed': splash_killed,
        }

    # ── Radar detection ────────────────────────────────────────────────
    def get_detected(self, iso) -> List[dict]:
        c = self.countries.get(iso, {})
        radars = c.get('radars', [])
        visible = {}
        for pid, proj in self.projectiles.items():
            if not proj['active']: continue
            if proj['owner_iso'] == iso:
                visible[pid] = {**proj, 'own': True}; continue
            is_stealth = proj.get('stealth', False)
            for r in radars:
                dist = math.hypot(proj['lat'] - r['lat'], proj['lon'] - r['lon'])
                eff_radius = r['radius'] * (0.6 if is_stealth else 1.0)
                eff_radius += r.get('recon_bonus', 0)
                if dist > eff_radius: continue
                dlon = proj['lon'] - r['lon']; dlat = proj['lat'] - r['lat']
                bearing    = (math.degrees(math.atan2(dlon, dlat)) + 360) % 360
                sweep_half = r['sweep'] / 2
                angle_diff = abs((bearing - r['angle'] + 180) % 360 - 180)
                if angle_diff <= sweep_half:
                    visible[pid] = {**proj, 'detected': True}; break
        return list(visible.values())

    # ── Territory ──────────────────────────────────────────────────────
    def attack_territory(self, attacker_iso, target_iso, pid, force=10.0) -> dict:
        target    = self.get_country(target_iso)
        is_played = bool(target['players'])
        target['territory_hp'] = max(0.0, target.get('territory_hp', 100.0) - force)
        captured = False
        if target['territory_hp'] <= 0:
            target['territory_hp'] = 100.0
            target['occupied_by']  = attacker_iso; captured = True
            self._log({'type': 'capture', 'by': attacker_iso, 'target': target_iso})
        self._save_country(target_iso)
        return {'territory_hp': target['territory_hp'], 'captured': captured,
                'target_iso': target_iso, 'shield': not is_played}

    # ── Economy ────────────────────────────────────────────────────────
    def tick_crops(self) -> List[tuple]:
        now = int(time.time()); updates = []
        for iso, c in self.countries.items():
            changed = False
            for pid, crop in c.get('crops', {}).items():
                grown = int((now - crop.get('last_tick', now)) / CROP_TICK_SEC)
                if grown > 0:
                    crop['amount'] += grown; crop['last_tick'] = now
                    updates.append((iso, pid, crop['amount'])); changed = True
            if changed: self._save_country(iso)
        return updates

    def sell_crops(self, iso, pid, amount, buyer_iso) -> dict:
        c    = self.get_country(iso)
        crop = c.get('crops', {}).get(pid, {})
        if crop.get('amount', 0) < amount: return {'ok': False, 'reason': 'Мало урожая'}
        crop['amount'] -= amount
        earned = amount * CROP_SELL_PRICE
        self.add_coins(iso, pid, earned)
        self._save_country(iso)
        return {'ok': True, 'earned': earned, 'left': crop['amount']}

    def full_state(self, iso) -> dict:
        c        = self.get_country(iso)
        detected = self.get_detected(iso)
        world    = {
            k: {
                'iso': k,
                'players_count': len(v.get('players', [])),
                'territory_hp':  v.get('territory_hp', 100),
                'occupied_by':   v.get('occupied_by'),
                'cities':        v.get('cities', {}),
                'pvo_count':     len(v.get('pvo', [])),
                'radar_count':   len(v.get('radars', [])),
            }
            for k, v in self.countries.items()
        }
        return {'country': c, 'projectiles': detected, 'world': world}


# ═══════════════════════════════════════════════════════════════════════
#   GAME SERVER
# ═══════════════════════════════════════════════════════════════════════
class GameServer:
    def __init__(self, state: GameState):
        self.state   = state
        self.players: Dict[str, dict] = {}   # pid → {ws,iso,username}

    async def _send(self, ws, data):
        try: await ws.send(json.dumps(data, ensure_ascii=False, default=str))
        except: pass

    async def _bcast(self, data):
        msg   = json.dumps(data, ensure_ascii=False, default=str)
        tasks = [p['ws'].send(msg) for p in self.players.values()]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def _bcast_country(self, iso, data, skip=None):
        msg   = json.dumps(data, ensure_ascii=False, default=str)
        tasks = [p['ws'].send(msg) for pid, p in self.players.items()
                 if p['iso'] == iso and pid != skip]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def handle(self, ws, data):
        t = data.get('type')
        if t == 'ping': await self._send(ws, {'type': 'pong'}); return

        # ── Join ──────────────────────────────────────────────────────
        if t == 'join':
            pid      = data.get('player_id', '')
            username = data.get('username', 'Аноним')
            iso      = data.get('iso', '').upper()
            if not pid or not iso:
                await self._send(ws, {'type': 'error', 'msg': 'Нужен player_id и iso'}); return
            old = self.players.get(pid)
            if old and old['iso'] != iso:
                self.state.leave(old['iso'], pid)
            self.players[pid] = {'ws': ws, 'iso': iso, 'username': username}
            self.state.join(iso, pid, username)
            await self._send(ws, {'type': 'joined', 'iso': iso,
                                  'state': self.state.full_state(iso),
                                  'units': UNITS, 'pvo_systems': PVO_SYSTEMS})
            return

        pid  = data.get('player_id', '')
        info = self.players.get(pid)
        if not info:
            await self._send(ws, {'type': 'error', 'msg': 'Сначала join'}); return
        iso = info['iso']

        # ── Place radar ───────────────────────────────────────────────
        if t == 'place_radar':
            ok = self.state.place_radar(iso, pid, data['lat'], data['lon'],
                                        data.get('radius', 9.0))
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'radar',
                                  'reason': '' if ok else 'Нужно 200₽'})
            if ok: await self._bcast_country(iso, {'type':'country_update',
                                                    'country': self.state.get_country(iso)})
            return

        # ── Place launcher ────────────────────────────────────────────
        if t == 'place_launcher':
            ok = self.state.place_launcher(iso, pid, data['lat'], data['lon'])
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'launcher',
                                  'reason': '' if ok else 'Нужно 300₽'})
            if ok: await self._bcast_country(iso, {'type':'country_update',
                                                    'country': self.state.get_country(iso)})
            return

        # ── Place PVO ─────────────────────────────────────────────────
        if t == 'place_pvo':
            pvo_type = data.get('pvo_type', 's300')
            ok = self.state.place_pvo(iso, pid, pvo_type, data['lat'], data['lon'])
            pdef = PVO_SYSTEMS.get(pvo_type, {})
            await self._send(ws, {'type': 'build_result', 'ok': ok, 'what': 'pvo',
                                  'reason': '' if ok else f"Нужно {pdef.get('cost',0)}₽"})
            if ok: await self._bcast_country(iso, {'type':'country_update',
                                                    'country': self.state.get_country(iso)})
            return

        # ── Toggle PVO auto ───────────────────────────────────────────
        if t == 'pvo_toggle_auto':
            c   = self.state.get_country(iso)
            idx = data.get('pvo_idx', 0)
            pvos = c.get('pvo', [])
            if idx < len(pvos):
                pvos[idx]['auto'] = not pvos[idx].get('auto', True)
                self.state._save_country(iso)
                await self._send(ws, {'type': 'pvo_auto', 'idx': idx,
                                      'auto': pvos[idx]['auto']})
            return

        # ── PVO manual intercept ──────────────────────────────────────
        if t == 'pvo_intercept':
            result = self.state.try_intercept_pvo(iso, data.get('pvo_idx', 0),
                                                   data.get('proj_id', ''))
            await self._send(ws, {'type': 'intercept_result', **result})
            if result.get('ok') and result.get('destroyed'):
                await self._bcast({'type': 'proj_destroyed',
                                   'proj_id': data['proj_id'], 'by': iso, 'manual': True})
            return

        # ── Launch ────────────────────────────────────────────────────
        if t == 'launch':
            utype = data.get('utype', 'missile')
            pid_s = self.state.launch(
                iso, pid, utype,
                data['src_lat'], data['src_lon'],
                data['dst_lat'], data['dst_lon'],
                data.get('target_iso', ''), data.get('target_city', 'capital'))
            if pid_s:
                await self._bcast({'type': 'proj_launched',
                                   'proj': self.state.projectiles[pid_s],
                                   'owner_iso': iso})
            else:
                udef = UNITS.get(utype, {})
                await self._send(ws, {'type': 'error',
                                      'msg': f"Нет монет (нужно {udef.get('cost','?')}₽)"})
            return

        # ── Attack territory ──────────────────────────────────────────
        if t == 'attack_territory':
            if not self.state.spend(iso, pid, 100):
                await self._send(ws, {'type': 'error', 'msg': 'Нужно 100₽'}); return
            result = self.state.attack_territory(iso, data['target_iso'], pid,
                                                  data.get('force', 10.0))
            await self._send(ws, {'type': 'territory_result', **result})
            if result['captured']:
                await self._bcast({'type': 'territory_captured',
                                   'by': iso, 'target': data['target_iso']})
            return

        # ── Sell crops ────────────────────────────────────────────────
        if t == 'sell_crops':
            r = self.state.sell_crops(iso, pid, data.get('amount', 1),
                                       data.get('buyer_iso', iso))
            await self._send(ws, {'type': 'sell_result', **r})
            if r.get('ok'):
                await self._bcast_country(iso, {'type': 'country_update',
                                                'country': self.state.get_country(iso)})
            return

        # ── Get state ─────────────────────────────────────────────────
        if t == 'get_state':
            await self._send(ws, {'type': 'state', 'state': self.state.full_state(iso)})
            return

    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try:   await self.handle(ws, json.loads(raw))
                except json.JSONDecodeError: await self._send(ws, {'type':'error','msg':'JSON'})
                except Exception as e: log.error(f"game handle: {e}", exc_info=True)
        except websockets.exceptions.ConnectionClosed: pass
        finally:
            for pid, p in list(self.players.items()):
                if p['ws'] is ws:
                    self.state.leave(p['iso'], pid); del self.players[pid]
                    log.info(f"🔌 {p['username']} ({p['iso']}) offline"); break

    # ── Tick loop ──────────────────────────────────────────────────────
    async def tick_loop(self):
        crop_i = 0
        while True:
            await asyncio.sleep(GAME_TICK_MS / 1000)

            # Move projectiles
            impacts = self.state.tick_projectiles()

            # Auto PVO for all countries with players
            all_destroyed = []
            for iso_act in {p['iso'] for p in self.players.values()}:
                killed = self.state.auto_pvo_tick(iso_act)
                for kid in killed:
                    all_destroyed.append(kid)
                    await self._bcast({'type': 'proj_destroyed',
                                       'proj_id': kid, 'by': iso_act, 'auto': True})

            # Process impacts
            for proj in impacts:
                if proj['id'] in all_destroyed: continue
                ev = self.state.process_impact(proj)
                if ev:
                    await self._bcast({'type': 'impact', **ev})
                    # Orëshnik splash
                    for sk in ev.get('splash_killed', []):
                        await self._bcast({'type': 'proj_destroyed',
                                           'proj_id': sk, 'by': '', 'splash': True})

            # Rotate radars
            self.state.tick_radars()

            # Send radar angles
            radar_map = {}
            for iso_a in {p['iso'] for p in self.players.values()}:
                c = self.state.countries.get(iso_a, {})
                radar_map[iso_a] = [
                    {'angle': r['angle'], 'lat': r['lat'], 'lon': r['lon'],
                     'radius': r['radius'], 'sweep': r['sweep']}
                    for r in c.get('radars', [])
                ]
            if radar_map:
                await self._bcast({'type': 'radar_tick', 'radars': radar_map})

            # Per-player: detected projectiles
            for pid, p in self.players.items():
                det = self.state.get_detected(p['iso'])
                try:
                    await self._send(p['ws'], {'type': 'projs_update', 'projs': det})
                except: pass

            # Crops every 10s
            crop_i += 1
            if crop_i >= int(10000 / GAME_TICK_MS):
                crop_i = 0
                for iso_c, pid_c, amt in self.state.tick_crops():
                    pp = self.players.get(pid_c)
                    if pp: await self._send(pp['ws'], {'type': 'crops_update', 'amount': amt})


# ═══════════════════════════════════════════════════════════════════════
#   MESSENGER (unchanged logic)
# ═══════════════════════════════════════════════════════════════════════
class Database:
    def __init__(self):
        self.pool = None; self._init_pool(); self._init_tables()

    def _init_pool(self):
        for i in range(5):
            try:
                self.pool = ThreadedConnectionPool(1, 20, DATABASE_URL,
                                                   sslmode='require', connect_timeout=10)
                log.info("✅ DB pool ok"); return
            except Exception as e:
                log.error(f"DB pool {i+1}/5: {e}")
                if i < 4: time.sleep(3*(i+1))
        sys.exit(1)

    def _get(self):
        if not self.pool: self._init_pool()
        return self.pool.getconn()
    def _put(self, c):
        if self.pool and c:
            try: self.pool.putconn(c)
            except: pass

    def execute(self, sql, params=(), *, one=False, all=False):
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            if one:   row=cur.fetchone(); conn.commit(); return dict(row) if row else None
            elif all: rows=cur.fetchall(); conn.commit(); return [dict(r) for r in rows]
            else:     conn.commit(); return None
        except Exception as e:
            if conn:
                try: conn.rollback()
                except: pass
            log.error(f"SQL: {e}"); raise
        finally: self._put(conn)

    def _init_tables(self):
        ddl = [
            """CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY,username TEXT UNIQUE NOT NULL,
               email TEXT DEFAULT'',password TEXT NOT NULL,bio TEXT DEFAULT'',status TEXT DEFAULT'offline',
               last_seen BIGINT NOT NULL DEFAULT 0,created_at BIGINT NOT NULL DEFAULT 0,messages_count INTEGER DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS chats(id SERIAL PRIMARY KEY,name TEXT NOT NULL,
               type TEXT NOT NULL DEFAULT'group',description TEXT DEFAULT'',created_by INTEGER,
               created_at BIGINT NOT NULL DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS chat_members(chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
               user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,role TEXT NOT NULL DEFAULT'member',
               joined_at BIGINT NOT NULL DEFAULT 0,PRIMARY KEY(chat_id,user_id))""",
            """CREATE TABLE IF NOT EXISTS messages(id SERIAL PRIMARY KEY,chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
               user_id INTEGER REFERENCES users(id),username TEXT NOT NULL,text TEXT NOT NULL,
               created_at BIGINT NOT NULL DEFAULT 0,deleted BOOLEAN DEFAULT FALSE)""",
            """CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
               created_at BIGINT NOT NULL DEFAULT 0,expires_at BIGINT NOT NULL DEFAULT 0)""",
            """CREATE TABLE IF NOT EXISTS calls(id SERIAL PRIMARY KEY,chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
               initiator INTEGER REFERENCES users(id),status TEXT NOT NULL DEFAULT'ringing',
               started_at BIGINT NOT NULL DEFAULT 0,ended_at BIGINT)""",
            "CREATE INDEX IF NOT EXISTS idx_msgs_chat ON messages(chat_id,id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at)",
        ]
        conn = None
        try:
            conn = self._get(); cur = conn.cursor()
            for q in ddl: cur.execute(q)
            cur.execute("SELECT id FROM chats WHERE id=1")
            if not cur.fetchone():
                cur.execute("INSERT INTO chats(id,name,type,description,created_at) VALUES(1,'Общий чат','group','Добро пожаловать!',%s)",(int(time.time()),))
            conn.commit(); log.info("✅ DB schema ok")
        except Exception as e:
            log.error(f"Schema: {e}")
            if conn:
                try: conn.rollback()
                except: pass
            raise
        finally: self._put(conn)

    def _hash(self,p): return hashlib.pbkdf2_hmac('sha256',p.encode(),SECRET_KEY.encode(),100_000).hex()
    def create_user(self,u,p,e=""):
        now=int(time.time()); conn=None
        try:
            conn=self._get(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM users WHERE username=%s",(u,))
            if cur.fetchone(): return None
            cur.execute("INSERT INTO users(username,email,password,last_seen,created_at) VALUES(%s,%s,%s,%s,%s) RETURNING id,username,email,bio,created_at",(u,e,self._hash(p),now,now))
            user=dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(1,%s,'member',%s) ON CONFLICT DO NOTHING",(user['id'],now))
            conn.commit(); return user
        except:
            if conn:
                try: conn.rollback()
                except: pass
            return None
        finally: self._put(conn)
    def verify_user(self,u,p):
        return self.execute("SELECT id,username,email,bio,status,created_at,messages_count FROM users WHERE username=%s AND password=%s",(u,self._hash(p)),one=True)
    def get_user(self,uid):
        return self.execute("SELECT id,username,email,bio,status,last_seen,created_at,messages_count FROM users WHERE id=%s",(uid,),one=True)
    def get_user_by_name(self,u):
        return self.execute("SELECT id,username,bio,status,last_seen FROM users WHERE username=%s",(u,),one=True)
    def set_status(self,uid,s):
        self.execute("UPDATE users SET status=%s,last_seen=%s WHERE id=%s",(s,int(time.time()),uid))
    def update_profile(self,uid,**kw):
        a={k:v for k,v in kw.items() if k in('bio','email') and v is not None}
        if not a: return
        self.execute(f"UPDATE users SET {', '.join(f'{k}=%s' for k in a)} WHERE id=%s",tuple(a.values())+(uid,))
    def change_password(self,uid,op,np):
        r=self.execute("SELECT password FROM users WHERE id=%s",(uid,),one=True)
        if not r or r['password']!=self._hash(op): return False
        self.execute("UPDATE users SET password=%s WHERE id=%s",(self._hash(np),uid)); return True
    def create_session(self,uid):
        tok=hashlib.sha256(f"{uid}{time.time()}{SECRET_KEY}".encode()).hexdigest(); now=int(time.time())
        self.execute("INSERT INTO sessions(token,user_id,created_at,expires_at) VALUES(%s,%s,%s,%s)",(tok,uid,now,now+SESSION_TTL)); return tok
    def check_session(self,tok):
        r=self.execute("SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s",(tok,int(time.time())),one=True)
        return r['user_id'] if r else None
    def delete_session(self,tok): self.execute("DELETE FROM sessions WHERE token=%s",(tok,))
    def cleanup_sessions(self): self.execute("DELETE FROM sessions WHERE expires_at<%s",(int(time.time()),))
    def get_user_chats(self,uid):
        return self.execute("""SELECT c.id,c.name,c.type,c.description,(SELECT row_to_json(m) FROM(SELECT username,text,created_at FROM messages WHERE chat_id=c.id AND deleted=FALSE ORDER BY id DESC LIMIT 1)m) AS last_message FROM chats c JOIN chat_members cm ON cm.chat_id=c.id WHERE cm.user_id=%s ORDER BY(SELECT COALESCE(MAX(id),0) FROM messages WHERE chat_id=c.id) DESC""",(uid,),all=True) or []
    def get_chat_info(self,cid): return self.execute("SELECT id,name,type,description,created_by,created_at FROM chats WHERE id=%s",(cid,),one=True)
    def get_chat_members(self,cid): return self.execute("""SELECT u.id,u.username,u.status,cm.role FROM users u JOIN chat_members cm ON cm.user_id=u.id WHERE cm.chat_id=%s ORDER BY CASE cm.role WHEN'owner'THEN 1 WHEN'admin'THEN 2 ELSE 3 END,u.username""",(cid,),all=True) or []
    def get_or_create_private(self,u1,u2,n2):
        ex=self.execute("""SELECT c.id,c.name,c.type FROM chats c JOIN chat_members m1 ON m1.chat_id=c.id JOIN chat_members m2 ON m2.chat_id=c.id WHERE c.type='private' AND m1.user_id=%s AND m2.user_id=%s LIMIT 1""",(u1,u2),one=True)
        if ex: return ex
        now=int(time.time()); conn=None
        try:
            conn=self._get(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("INSERT INTO chats(name,type,created_by,created_at) VALUES(%s,'private',%s,%s) RETURNING id,name,type",(n2,u1,now))
            chat=dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(%s,%s,'member',%s),(%s,%s,'member',%s)",(chat['id'],u1,now,chat['id'],u2,now))
            conn.commit(); return chat
        except:
            if conn:
                try: conn.rollback()
                except: pass
            return None
        finally: self._put(conn)
    def create_group(self,name,desc,creator,members):
        now=int(time.time()); conn=None
        try:
            conn=self._get(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("INSERT INTO chats(name,type,description,created_by,created_at) VALUES(%s,'group',%s,%s,%s) RETURNING id,name,type,description,created_at",(name,desc,creator,now))
            chat=dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(%s,%s,'owner',%s)",(chat['id'],creator,now))
            for mid in members:
                if mid!=creator: cur.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(%s,%s,'member',%s) ON CONFLICT DO NOTHING",(chat['id'],mid,now))
            conn.commit(); return chat
        except:
            if conn:
                try: conn.rollback()
                except: pass
            return None
        finally: self._put(conn)
    def add_member(self,cid,uid): self.execute("INSERT INTO chat_members(chat_id,user_id,role,joined_at) VALUES(%s,%s,'member',%s) ON CONFLICT DO NOTHING",(cid,uid,int(time.time())))
    def remove_member(self,cid,uid): self.execute("DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",(cid,uid))
    def get_messages(self,cid,limit=MAX_HISTORY):
        try: return self.execute("""SELECT id,chat_id,user_id,username,text,created_at FROM(SELECT id,chat_id,user_id,username,text,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE ORDER BY id DESC LIMIT %s)sub ORDER BY id ASC""",(cid,limit),all=True) or []
        except: return []
    def save_message(self,cid,uid,uname,text,system=False):
        conn=None
        try:
            conn=self._get(); cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("INSERT INTO messages(chat_id,user_id,username,text,created_at,deleted) VALUES(%s,%s,%s,%s,%s,FALSE) RETURNING id,chat_id,user_id,username,text,created_at",(cid,uid,uname,text,int(time.time())))
            msg=dict(cur.fetchone())
            if not system: cur.execute("UPDATE users SET messages_count=messages_count+1 WHERE id=%s",(uid,))
            conn.commit(); return msg
        except:
            if conn:
                try: conn.rollback()
                except: pass
            return None
        finally: self._put(conn)
    def get_online(self): return self.execute("SELECT id,username,status FROM users WHERE status='online' ORDER BY username",all=True) or []
    def create_call(self,cid,ini): return self.execute("INSERT INTO calls(chat_id,initiator,status,started_at) VALUES(%s,%s,'ringing',%s) RETURNING id,chat_id,status,started_at",(cid,ini,int(time.time())),one=True)
    def end_call(self,cid): self.execute("UPDATE calls SET status='ended',ended_at=%s WHERE id=%s",(int(time.time()),cid))


class MConn:
    def __init__(self,ws,uid,uname): self.ws=ws; self.user_id=uid; self.username=uname; self.current_chat=1

class MessengerServer:
    def __init__(self,db):
        self.db=db; self.conns:Dict[int,MConn]={}; self.calls:Dict[int,dict]={}

    async def _s(self,ws,d):
        try: await ws.send(json.dumps(d,ensure_ascii=False,default=str))
        except: pass
    async def _bc(self,d,cid,skip=None):
        msg=json.dumps(d,ensure_ascii=False,default=str)
        members={m['id'] for m in self.db.get_chat_members(cid)}
        tasks=[c.ws.send(msg) for uid,c in self.conns.items() if uid in members and uid!=skip]
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)
    async def _bco(self):
        payload=json.dumps({'type':'online_users','users':self.db.get_online()},ensure_ascii=False,default=str)
        tasks=[c.ws.send(payload) for c in self.conns.values()]
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)
    def _uid(self,ws):
        for uid,c in self.conns.items():
            if c.ws is ws: return uid
        return None

    async def handle(self,ws,data):
        t=data.get('type')
        if not t: return
        if t=='ping': await self._s(ws,{'type':'pong'}); return
        if t=='register':
            u=data.get('username','').strip(); p=data.get('password',''); e=data.get('email','').strip()
            if len(u)<3: await self._s(ws,{'type':'error','message':'Имя минимум 3 символа'}); return
            if len(p)<6: await self._s(ws,{'type':'error','message':'Пароль минимум 6 символов'}); return
            user=self.db.create_user(u,p,e)
            if not user: await self._s(ws,{'type':'error','message':'Имя уже занято'}); return
            tok=self.db.create_session(user['id'])
            await self._s(ws,{'type':'registered','user':user,'token':tok}); return
        if t=='login':
            u=data.get('username','').strip(); p=data.get('password','')
            user=self.db.verify_user(u,p)
            if not user: await self._s(ws,{'type':'error','message':'Неверный логин или пароль'}); return
            uid=user['id']; tok=self.db.create_session(uid); self.db.set_status(uid,'online')
            self.conns[uid]=MConn(ws,uid,u); chats=self.db.get_user_chats(uid)
            await self._s(ws,{'type':'logged_in','user':user,'token':tok,'chats':chats})
            for ch in chats:
                msgs=self.db.get_messages(ch['id'])
                await self._s(ws,{'type':'history','chat_id':ch['id'],'messages':msgs})
            await self._bco()
            sm=self.db.save_message(1,uid,'Система',f'👋 {u} присоединился',system=True)
            if sm: await self._bc({'type':'message','message':sm},1,skip=uid)
            return
        if t=='session':
            tok=data.get('token',''); uid=self.db.check_session(tok)
            if not uid: await self._s(ws,{'type':'error','message':'Сессия истекла'}); return
            user=self.db.get_user(uid)
            if not user: await self._s(ws,{'type':'error','message':'Не найден'}); return
            self.db.set_status(uid,'online'); self.conns[uid]=MConn(ws,uid,user['username'])
            chats=self.db.get_user_chats(uid)
            await self._s(ws,{'type':'session_ok','user':user,'chats':chats})
            for ch in chats:
                msgs=self.db.get_messages(ch['id'])
                await self._s(ws,{'type':'history','chat_id':ch['id'],'messages':msgs})
            await self._bco(); return
        uid=self._uid(ws)
        if uid is None: await self._s(ws,{'type':'error','message':'Требуется авторизация'}); return
        conn=self.conns[uid]
        if t=='send_message':
            cid=data.get('chat_id',1); text=data.get('text','').strip()
            if not text or len(text)>MAX_MSG_LEN: return
            msg=self.db.save_message(cid,uid,conn.username,text)
            if msg: await self._bc({'type':'message','message':msg},cid)
            return
        if t=='get_history':
            cid=data.get('chat_id',1); lim=min(int(data.get('limit',MAX_HISTORY)),MAX_HISTORY)
            await self._s(ws,{'type':'history','chat_id':cid,'messages':self.db.get_messages(cid,lim)}); return
        if t=='switch_chat': conn.current_chat=data.get('chat_id',1); return
        if t=='typing':
            cid=data.get('chat_id',1)
            await self._bc({'type':'typing','username':conn.username,'chat_id':cid},cid,skip=uid); return
        if t=='get_profile':
            tgt=data.get('username'); p=self.db.get_user_by_name(tgt) if tgt else self.db.get_user(uid)
            if not p: await self._s(ws,{'type':'error','message':'Не найден'}); return
            await self._s(ws,{'type':'profile','user':p}); return
        if t=='update_profile':
            self.db.update_profile(uid,bio=data.get('bio'),email=data.get('email'))
            await self._s(ws,{'type':'profile_updated','user':self.db.get_user(uid)}); return
        if t=='change_password':
            ok=self.db.change_password(uid,data.get('old_password',''),data.get('new_password',''))
            await self._s(ws,{'type':'password_changed'} if ok else {'type':'error','message':'Неверный пароль'}); return
        if t=='get_online': await self._s(ws,{'type':'online_users','users':self.db.get_online()}); return
        if t=='start_private':
            tn=data.get('username','').strip(); tgt=self.db.get_user_by_name(tn)
            if not tgt: await self._s(ws,{'type':'error','message':'Не найден'}); return
            chat=self.db.get_or_create_private(uid,tgt['id'],tn)
            if not chat: await self._s(ws,{'type':'error','message':'Ошибка'}); return
            msgs=self.db.get_messages(chat['id'])
            await self._s(ws,{'type':'private_chat_created','chat':{'id':chat['id'],'name':tn,'type':'private'},'messages':msgs})
            if tgt['id'] in self.conns:
                await self._s(self.conns[tgt['id']].ws,{'type':'new_private_chat','chat':{'id':chat['id'],'name':conn.username,'type':'private'},'messages':msgs})
            return
        if t=='create_group':
            name=data.get('name','').strip(); desc=data.get('description','').strip()
            names=data.get('members',[])
            if not name: await self._s(ws,{'type':'error','message':'Введите название'}); return
            ids=[uid]
            for n in names:
                u2=self.db.get_user_by_name(n)
                if u2 and u2['id'] not in ids: ids.append(u2['id'])
            chat=self.db.create_group(name,desc,uid,ids)
            if not chat: await self._s(ws,{'type':'error','message':'Ошибка'}); return
            mi=self.db.get_chat_members(chat['id'])
            for m in mi:
                if m['id']!=uid and m['id'] in self.conns:
                    await self._s(self.conns[m['id']].ws,{'type':'new_group_chat','chat':{**chat,'type':'group'}})
            await self._s(ws,{'type':'group_created','chat':{**chat,'type':'group'},'members':mi}); return
        if t=='get_chat_info':
            cid=data.get('chat_id')
            await self._s(ws,{'type':'chat_info','chat':self.db.get_chat_info(cid),'members':self.db.get_chat_members(cid)}); return
        if t=='add_member':
            cid=data.get('chat_id'); un=data.get('username','').strip(); tgt=self.db.get_user_by_name(un)
            if not tgt: await self._s(ws,{'type':'error','message':'Не найден'}); return
            self.db.add_member(cid,tgt['id']); chat=self.db.get_chat_info(cid)
            if tgt['id'] in self.conns:
                await self._s(self.conns[tgt['id']].ws,{'type':'added_to_chat','chat':{**chat,'type':'group'},'messages':self.db.get_messages(cid)})
            sm=self.db.save_message(cid,uid,'Система',f'➕ {un} добавлен',system=True)
            if sm: await self._bc({'type':'message','message':sm},cid)
            return
        if t=='leave_chat':
            cid=data.get('chat_id'); self.db.remove_member(cid,uid)
            sm=self.db.save_message(cid,uid,'Система',f'👋 {conn.username} покинул',system=True)
            if sm: await self._bc({'type':'message','message':sm},cid)
            await self._s(ws,{'type':'left_chat','chat_id':cid}); return
        if t=='call_start':
            cid=data.get('chat_id',conn.current_chat); call=self.db.create_call(cid,uid)
            if not call: await self._s(ws,{'type':'error','message':'Ошибка звонка'}); return
            self.calls[call['id']]={'chat_id':cid,'initiator':uid,'participants':{uid}}
            cn=(self.db.get_chat_info(cid) or {}).get('name','Чат')
            await self._bc({'type':'incoming_call','call_id':call['id'],'chat_id':cid,'from':conn.username,'chat_name':cn},cid,skip=uid)
            await self._s(ws,{'type':'call_started','call_id':call['id']}); return
        if t=='call_accept':
            cid=data.get('call_id')
            if cid not in self.calls: return
            call=self.calls[cid]; call['participants'].add(uid)
            ini=call['initiator']
            if ini in self.conns: await self._s(self.conns[ini].ws,{'type':'call_accepted','call_id':cid,'by':conn.username})
            await self._s(ws,{'type':'call_joined','call_id':cid}); return
        if t=='call_decline':
            cid=data.get('call_id')
            if cid not in self.calls: return
            call=self.calls[cid]
            if call['initiator'] in self.conns: await self._s(self.conns[call['initiator']].ws,{'type':'call_declined','call_id':cid,'by':conn.username})
            self.db.end_call(cid); del self.calls[cid]; return
        if t=='call_end':
            cid=data.get('call_id')
            if cid not in self.calls: return
            call=self.calls[cid]; self.db.end_call(cid)
            await self._bc({'type':'call_ended','call_id':cid},call['chat_id'])
            del self.calls[cid]; return
        if t=='logout':
            tok=data.get('token','')
            if tok: self.db.delete_session(tok)
            await self._s(ws,{'type':'logged_out'}); return

    async def ws_handler(self,ws):
        try:
            async for raw in ws:
                try:   await self.handle(ws,json.loads(raw))
                except json.JSONDecodeError: await self._s(ws,{'type':'error','message':'Неверный JSON'})
                except Exception as e: log.error(f"msg handle: {e}",exc_info=True); await self._s(ws,{'type':'error','message':'Ошибка'})
        except websockets.exceptions.ConnectionClosed: pass
        except Exception as e: log.error(f"msg ws: {e}")
        finally:
            uid=self._uid(ws)
            if uid and uid in self.conns:
                uname=self.conns[uid].username; del self.conns[uid]; self.db.set_status(uid,'offline')
                for cid,call in list(self.calls.items()):
                    if uid in call['participants']:
                        self.db.end_call(cid); await self._bc({'type':'call_ended','call_id':cid},call['chat_id']); del self.calls[cid]
                await self._bco()

    async def cleanup_loop(self):
        while True:
            await asyncio.sleep(3600)
            try: self.db.cleanup_sessions()
            except: pass


# ═══════════════════════════════════════════════════════════════════════
#   MAIN
# ═══════════════════════════════════════════════════════════════════════
async def main():
    log.info("="*56)
    log.info("🚀  WorldWar + TelegramWhite Combined Server")
    log.info("="*56)
    db  = Database()
    msg = MessengerServer(db)
    gs  = GameState()
    gsv = GameServer(gs)

    mws = await websockets.serve(msg.ws_handler, HOST, MESSENGER_PORT,
                                  ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
                                  max_size=10*1024*1024)
    gws = await websockets.serve(gsv.ws_handler, HOST, GAME_PORT,
                                  ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT,
                                  max_size=4*1024*1024)
    log.info(f"✅ Messenger  ws://{HOST}:{MESSENGER_PORT}")
    log.info(f"✅ WorldWar   ws://{HOST}:{GAME_PORT}")

    asyncio.create_task(msg.cleanup_loop())
    asyncio.create_task(gsv.tick_loop())
    log.info("✅ Все сервисы запущены")
    await asyncio.Future()

if __name__ == '__main__':
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("👋 Стоп")
    except Exception as e: log.error(f"FATAL: {e}",exc_info=True); sys.exit(1)
