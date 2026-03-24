#!/usr/bin/env python3
"""
TelegramWhite WebSocket Server
Render.com compatible — читает DATABASE_URL из переменных окружения
"""

import os
import sys
import json
import asyncio
import hashlib
import time
import logging
import urllib.parse
from typing import Dict, Optional

# ── Зависимости ──────────────────────────────────────────────────────────
try:
    import websockets
except ImportError:
    print("ERROR: pip install websockets")
    sys.exit(1)

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ── Логирование ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
log = logging.getLogger("tgwhite")

# ── Конфиг ────────────────────────────────────────────────────────────────
PORT        = int(os.environ.get("PORT", 10000))
HOST        = "0.0.0.0"
SECRET_KEY  = os.environ.get("TW_SECRET", "telegramwhite-2025")
SESSION_TTL = 30 * 24 * 3600   # 30 дней
MAX_MSG_LEN = 4096
MAX_HISTORY = 100
PING_INTERVAL = 20
PING_TIMEOUT  = 10

# ── DATABASE_URL: нормализация ────────────────────────────────────────────
def get_database_url() -> str:
    raw = os.environ.get("DATABASE_URL", "").strip()
    if not raw:
        log.error("❌ DATABASE_URL не задан!")
        log.error("   Добавьте его в Render → Environment Variables")
        sys.exit(1)

    # Render отдаёт postgres:// — psycopg2 хочет postgresql://
    if raw.startswith("postgres://"):
        raw = "postgresql://" + raw[len("postgres://"):]

    # Проверяем что строка валидна
    try:
        r = urllib.parse.urlparse(raw)
        if not r.scheme or not r.hostname:
            raise ValueError("Неверный формат URL")
        log.info(f"✅ DATABASE_URL: {r.scheme}://{r.hostname}:{r.port}/{r.path.lstrip('/')}")
    except Exception as e:
        log.error(f"❌ Неверный DATABASE_URL: {e}")
        log.error(f"   Значение: {raw[:30]}...")
        sys.exit(1)

    return raw


DATABASE_URL = get_database_url()


# ═══════════════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════════════
class Database:
    def __init__(self):
        self.url  = DATABASE_URL
        self.pool = None
        self._init_pool()
        self._init_tables()

    # ── Connection pool ────────────────────────────────────────────────
    def _init_pool(self):
        for attempt in range(5):
            try:
                self.pool = ThreadedConnectionPool(
                    1, 20, self.url,
                    sslmode='require',
                    connect_timeout=10
                )
                log.info("✅ Пул БД создан")
                return
            except Exception as e:
                log.error(f"❌ Пул БД (попытка {attempt+1}/5): {e}")
                if attempt < 4:
                    time.sleep(3 * (attempt + 1))
        log.error("❌ Не удалось создать пул БД")
        sys.exit(1)

    def _get(self):
        if not self.pool:
            self._init_pool()
        return self.pool.getconn()

    def _put(self, conn):
        if self.pool and conn:
            try:
                self.pool.putconn(conn)
            except Exception:
                pass

    # ── Generic execute ────────────────────────────────────────────────
    def execute(self, sql: str, params=(), *, one=False, all=False):
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params)
            if one:
                row = cur.fetchone()
                conn.commit()
                return dict(row) if row else None
            elif all:
                rows = cur.fetchall()
                conn.commit()
                return [dict(r) for r in rows]
            else:
                conn.commit()
                return None
        except Exception as e:
            if conn:
                try: conn.rollback()
                except: pass
            log.error(f"❌ SQL: {e} | {sql[:60]}")
            raise
        finally:
            self._put(conn)

    # ── Schema ─────────────────────────────────────────────────────────
    def _init_tables(self):
        ddl = [
            """CREATE TABLE IF NOT EXISTS users (
                id             SERIAL PRIMARY KEY,
                username       TEXT UNIQUE NOT NULL,
                email          TEXT DEFAULT '',
                password       TEXT NOT NULL,
                bio            TEXT DEFAULT '',
                status         TEXT DEFAULT 'offline',
                last_seen      BIGINT NOT NULL DEFAULT 0,
                created_at     BIGINT NOT NULL DEFAULT 0,
                messages_count INTEGER DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS chats (
                id           SERIAL PRIMARY KEY,
                name         TEXT NOT NULL,
                type         TEXT NOT NULL DEFAULT 'group',
                description  TEXT DEFAULT '',
                created_by   INTEGER,
                created_at   BIGINT NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS chat_members (
                chat_id   INTEGER REFERENCES chats(id)  ON DELETE CASCADE,
                user_id   INTEGER REFERENCES users(id)  ON DELETE CASCADE,
                role      TEXT NOT NULL DEFAULT 'member',
                joined_at BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            )""",
            """CREATE TABLE IF NOT EXISTS messages (
                id         SERIAL PRIMARY KEY,
                chat_id    INTEGER REFERENCES chats(id)  ON DELETE CASCADE,
                user_id    INTEGER REFERENCES users(id),
                username   TEXT NOT NULL,
                text       TEXT NOT NULL,
                is_system  BOOLEAN DEFAULT FALSE,
                created_at BIGINT NOT NULL DEFAULT 0,
                deleted    BOOLEAN DEFAULT FALSE
            )""",
            """CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                created_at BIGINT NOT NULL DEFAULT 0,
                expires_at BIGINT NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS calls (
                id         SERIAL PRIMARY KEY,
                chat_id    INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                initiator  INTEGER REFERENCES users(id),
                status     TEXT NOT NULL DEFAULT 'ringing',
                started_at BIGINT NOT NULL DEFAULT 0,
                ended_at   BIGINT
            )""",
            "CREATE INDEX IF NOT EXISTS idx_msgs_chat ON messages(chat_id, id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at)",
        ]
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor()
            for q in ddl:
                cur.execute(q)
            # Создаём общий чат id=1 если нет
            cur.execute("SELECT id FROM chats WHERE id = 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO chats (id, name, type, description, created_at) "
                    "VALUES (1, 'Общий чат', 'group', 'Добро пожаловать!', %s)",
                    (int(time.time()),)
                )
                log.info("✅ Создан общий чат (id=1)")
            conn.commit()
            log.info("✅ Схема БД готова")
        except Exception as e:
            log.error(f"❌ Инициализация схемы: {e}")
            if conn:
                try: conn.rollback()
                except: pass
            raise
        finally:
            self._put(conn)

    # ── Auth ───────────────────────────────────────────────────────────
    def _hash(self, pwd: str) -> str:
        return hashlib.pbkdf2_hmac('sha256', pwd.encode(), SECRET_KEY.encode(), 100_000).hex()

    def create_user(self, username: str, password: str, email: str = "") -> Optional[dict]:
        now = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM users WHERE username = %s", (username,))
            if cur.fetchone():
                return None
            cur.execute(
                "INSERT INTO users (username, email, password, last_seen, created_at) "
                "VALUES (%s,%s,%s,%s,%s) RETURNING id, username, email, bio, created_at",
                (username, email, self._hash(password), now, now)
            )
            user = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members (chat_id, user_id, role, joined_at) "
                "VALUES (1, %s, 'member', %s) ON CONFLICT DO NOTHING",
                (user['id'], now)
            )
            conn.commit()
            log.info(f"✅ Новый пользователь: {username}")
            return user
        except Exception as e:
            if conn:
                try: conn.rollback()
                except: pass
            log.error(f"❌ create_user: {e}")
            return None
        finally:
            self._put(conn)

    def verify_user(self, username: str, password: str) -> Optional[dict]:
        return self.execute(
            "SELECT id, username, email, bio, status, created_at, messages_count "
            "FROM users WHERE username=%s AND password=%s",
            (username, self._hash(password)), one=True
        )

    def get_user(self, user_id: int) -> Optional[dict]:
        return self.execute(
            "SELECT id, username, email, bio, status, last_seen, created_at, messages_count "
            "FROM users WHERE id=%s", (user_id,), one=True
        )

    def get_user_by_name(self, username: str) -> Optional[dict]:
        return self.execute(
            "SELECT id, username, bio, status, last_seen FROM users WHERE username=%s",
            (username,), one=True
        )

    def set_status(self, user_id: int, status: str):
        self.execute(
            "UPDATE users SET status=%s, last_seen=%s WHERE id=%s",
            (status, int(time.time()), user_id)
        )

    def update_profile(self, user_id: int, **kw):
        allowed = {k: v for k, v in kw.items() if k in ('bio','email') and v is not None}
        if not allowed: return
        sets = ', '.join(f"{k}=%s" for k in allowed)
        self.execute(f"UPDATE users SET {sets} WHERE id=%s",
                     tuple(allowed.values()) + (user_id,))

    def change_password(self, user_id: int, old_pwd: str, new_pwd: str) -> bool:
        row = self.execute("SELECT password FROM users WHERE id=%s", (user_id,), one=True)
        if not row or row['password'] != self._hash(old_pwd):
            return False
        self.execute("UPDATE users SET password=%s WHERE id=%s", (self._hash(new_pwd), user_id))
        return True

    # ── Sessions ────────────────────────────────────────────────────────
    def create_session(self, user_id: int) -> str:
        token = hashlib.sha256(f"{user_id}{time.time()}{SECRET_KEY}".encode()).hexdigest()
        now   = int(time.time())
        self.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (%s,%s,%s,%s)",
            (token, user_id, now, now + SESSION_TTL)
        )
        return token

    def check_session(self, token: str) -> Optional[int]:
        row = self.execute(
            "SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s",
            (token, int(time.time())), one=True
        )
        return row['user_id'] if row else None

    def delete_session(self, token: str):
        self.execute("DELETE FROM sessions WHERE token=%s", (token,))

    def cleanup_sessions(self):
        self.execute("DELETE FROM sessions WHERE expires_at<%s", (int(time.time()),))

    # ── Chats ───────────────────────────────────────────────────────────
    def get_user_chats(self, user_id: int) -> list:
        return self.execute("""
            SELECT c.id, c.name, c.type, c.description,
                   (SELECT row_to_json(m) FROM
                       (SELECT username, text, created_at FROM messages
                        WHERE chat_id=c.id AND deleted=FALSE ORDER BY id DESC LIMIT 1) m
                   ) AS last_message
            FROM chats c
            JOIN chat_members cm ON cm.chat_id=c.id
            WHERE cm.user_id=%s
            ORDER BY (SELECT COALESCE(MAX(id),0) FROM messages WHERE chat_id=c.id) DESC
        """, (user_id,), all=True) or []

    def get_chat_info(self, chat_id: int) -> Optional[dict]:
        return self.execute(
            "SELECT id, name, type, description, created_by, created_at FROM chats WHERE id=%s",
            (chat_id,), one=True
        )

    def get_chat_members(self, chat_id: int) -> list:
        return self.execute("""
            SELECT u.id, u.username, u.status, cm.role
            FROM users u JOIN chat_members cm ON cm.user_id=u.id
            WHERE cm.chat_id=%s
            ORDER BY CASE cm.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 ELSE 3 END, u.username
        """, (chat_id,), all=True) or []

    def get_or_create_private(self, uid1: int, uid2: int, name2: str) -> Optional[dict]:
        existing = self.execute("""
            SELECT c.id, c.name, c.type FROM chats c
            JOIN chat_members m1 ON m1.chat_id=c.id
            JOIN chat_members m2 ON m2.chat_id=c.id
            WHERE c.type='private' AND m1.user_id=%s AND m2.user_id=%s LIMIT 1
        """, (uid1, uid2), one=True)
        if existing:
            return existing

        now  = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO chats (name,type,created_by,created_at) VALUES (%s,'private',%s,%s) "
                "RETURNING id, name, type",
                (name2, uid1, now)
            )
            chat = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'member',%s),(%s,%s,'member',%s)",
                (chat['id'], uid1, now, chat['id'], uid2, now)
            )
            conn.commit()
            return chat
        except Exception as e:
            if conn:
                try: conn.rollback()
                except: pass
            log.error(f"❌ get_or_create_private: {e}")
            return None
        finally:
            self._put(conn)

    def create_group(self, name: str, desc: str, creator_id: int, member_ids: list) -> Optional[dict]:
        now  = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO chats (name,type,description,created_by,created_at) "
                "VALUES (%s,'group',%s,%s,%s) RETURNING id, name, type, description, created_at",
                (name, desc, creator_id, now)
            )
            chat = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'owner',%s)",
                (chat['id'], creator_id, now)
            )
            for mid in member_ids:
                if mid != creator_id:
                    cur.execute(
                        "INSERT INTO chat_members (chat_id,user_id,role,joined_at) "
                        "VALUES (%s,%s,'member',%s) ON CONFLICT DO NOTHING",
                        (chat['id'], mid, now)
                    )
            conn.commit()
            return chat
        except Exception as e:
            if conn:
                try: conn.rollback()
                except: pass
            log.error(f"❌ create_group: {e}")
            return None
        finally:
            self._put(conn)

    def add_member(self, chat_id: int, user_id: int):
        self.execute(
            "INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'member',%s) "
            "ON CONFLICT DO NOTHING",
            (chat_id, user_id, int(time.time()))
        )

    def remove_member(self, chat_id: int, user_id: int):
        self.execute("DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))

    # ── Messages ────────────────────────────────────────────────────────
  def get_messages(self, chat_id: int, limit: int = MAX_HISTORY) -> list:
    try:
        rows = self.execute("""
            SELECT id, chat_id, user_id, username, text, created_at
            FROM (
                SELECT id, chat_id, user_id, username, text, created_at
                FROM messages WHERE chat_id=%s AND deleted=FALSE
                ORDER BY id DESC LIMIT %s
            ) sub ORDER BY id ASC
        """, (chat_id, limit), all=True)
        return rows or []
    except Exception as e:
        log.error(f"❌ get_messages: {e}")
        return []

   def save_message(self, chat_id: int, user_id: int, username: str, text: str,
                 is_system: bool = False) -> Optional[dict]:
    conn = None
    try:
        conn = self._get()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "INSERT INTO messages (chat_id,user_id,username,text,created_at,deleted) "
            "VALUES (%s,%s,%s,%s,%s,FALSE) "
            "RETURNING id, chat_id, user_id, username, text, created_at",
            (chat_id, user_id, username, text, int(time.time()))
        )
        msg = dict(cur.fetchone())
        if not is_system:
            cur.execute(
                "UPDATE users SET messages_count=messages_count+1 WHERE id=%s", (user_id,)
            )
        conn.commit()
        return msg
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        log.error(f"❌ save_message: {e}")
        return None
    finally:
        self._put(conn)

    # ── Online ──────────────────────────────────────────────────────────
    def get_online(self) -> list:
        return self.execute(
            "SELECT id, username, status FROM users WHERE status='online' ORDER BY username",
            all=True
        ) or []

    # ── Calls ────────────────────────────────────────────────────────────
    def create_call(self, chat_id: int, initiator: int) -> Optional[dict]:
        return self.execute(
            "INSERT INTO calls (chat_id,initiator,status,started_at) VALUES (%s,%s,'ringing',%s) "
            "RETURNING id, chat_id, status, started_at",
            (chat_id, initiator, int(time.time())), one=True
        )

    def end_call(self, call_id: int):
        self.execute(
            "UPDATE calls SET status='ended', ended_at=%s WHERE id=%s",
            (int(time.time()), call_id)
        )


# ═══════════════════════════════════════════════════════════════════════════
#  CONNECTION
# ═══════════════════════════════════════════════════════════════════════════
class Conn:
    def __init__(self, ws, user_id: int, username: str):
        self.ws          = ws
        self.user_id     = user_id
        self.username    = username
        self.current_chat = 1


# ═══════════════════════════════════════════════════════════════════════════
#  SERVER
# ═══════════════════════════════════════════════════════════════════════════
class Server:
    def __init__(self):
        log.info("=" * 52)
        log.info("🚀  TelegramWhite WebSocket Server")
        log.info("=" * 52)
        self.db          = Database()
        self.conns       : Dict[int, Conn] = {}   # user_id → Conn
        self.active_calls: Dict[int, dict]  = {}   # call_id → info
        log.info(f"✅  Сервер готов, слушаю {HOST}:{PORT}")

    # ── Helpers ───────────────────────────────────────────────────────
    async def _send(self, ws, data: dict):
        try:
            await ws.send(json.dumps(data, ensure_ascii=False, default=str))
        except Exception:
            pass

    async def _broadcast(self, data: dict, chat_id: int, skip: int = None):
        msg = json.dumps(data, ensure_ascii=False, default=str)
        members = {m['id'] for m in self.db.get_chat_members(chat_id)}
        tasks   = [
            c.ws.send(msg)
            for uid, c in self.conns.items()
            if uid in members and uid != skip
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_online(self):
        payload = json.dumps({'type': 'online_users', 'users': self.db.get_online()},
                              ensure_ascii=False, default=str)
        tasks = [c.ws.send(payload) for c in self.conns.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _get_uid(self, ws) -> Optional[int]:
        for uid, c in self.conns.items():
            if c.ws is ws:
                return uid
        return None

    # ── Handler ───────────────────────────────────────────────────────
    async def handle(self, ws, data: dict):
        t = data.get('type')
        if not t:
            return

        # ── Ping ──────────────────────────────────────────────────────
        if t == 'ping':
            await self._send(ws, {'type': 'pong'})
            return

        # ── Register ───────────────────────────────────────────────────
        if t == 'register':
            uname = data.get('username', '').strip()
            pwd   = data.get('password', '')
            email = data.get('email', '').strip()
            if len(uname) < 3:
                await self._send(ws, {'type':'error','message':'Имя минимум 3 символа'}); return
            if len(pwd) < 6:
                await self._send(ws, {'type':'error','message':'Пароль минимум 6 символов'}); return
            user = self.db.create_user(uname, pwd, email)
            if not user:
                await self._send(ws, {'type':'error','message':'Имя уже занято'}); return
            token = self.db.create_session(user['id'])
            await self._send(ws, {'type':'registered','user':user,'token':token})
            return

        # ── Login ──────────────────────────────────────────────────────
        if t == 'login':
            uname = data.get('username','').strip()
            pwd   = data.get('password','')
            user  = self.db.verify_user(uname, pwd)
            if not user:
                await self._send(ws, {'type':'error','message':'Неверный логин или пароль'}); return
            uid   = user['id']
            token = self.db.create_session(uid)
            self.db.set_status(uid, 'online')
            self.conns[uid] = Conn(ws, uid, uname)
            chats = self.db.get_user_chats(uid)
            await self._send(ws, {'type':'logged_in','user':user,'token':token,'chats':chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._send(ws, {'type':'history','chat_id':ch['id'],'messages':msgs})
            await self._broadcast_online()
            sys_msg = self.db.save_message(1, uid, 'Система',
                                           f'👋 {uname} присоединился', is_system=True)
            if sys_msg:
                await self._broadcast({'type':'message','message':sys_msg}, 1, skip=uid)
            log.info(f"✅ Вход: {uname}")
            return

        # ── Session ────────────────────────────────────────────────────
        if t == 'session':
            token  = data.get('token','')
            uid    = self.db.check_session(token)
            if not uid:
                await self._send(ws, {'type':'error','message':'Сессия истекла'}); return
            user = self.db.get_user(uid)
            if not user:
                await self._send(ws, {'type':'error','message':'Пользователь не найден'}); return
            self.db.set_status(uid, 'online')
            self.conns[uid] = Conn(ws, uid, user['username'])
            chats = self.db.get_user_chats(uid)
            await self._send(ws, {'type':'session_ok','user':user,'chats':chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._send(ws, {'type':'history','chat_id':ch['id'],'messages':msgs})
            await self._broadcast_online()
            log.info(f"✅ Сессия: {user['username']}")
            return

        # ── Требуется авторизация ──────────────────────────────────────
        uid = self._get_uid(ws)
        if uid is None:
            await self._send(ws, {'type':'error','message':'Требуется авторизация'}); return
        conn = self.conns[uid]

        # ── Send message ───────────────────────────────────────────────
        if t == 'send_message':
            chat_id = data.get('chat_id', 1)
            text    = data.get('text','').strip()
            if not text or len(text) > MAX_MSG_LEN: return
            msg = self.db.save_message(chat_id, uid, conn.username, text)
            if msg:
                await self._broadcast({'type':'message','message':msg}, chat_id)
            return

        # ── History ────────────────────────────────────────────────────
        if t == 'get_history':
            chat_id = data.get('chat_id', 1)
            limit   = min(int(data.get('limit', MAX_HISTORY)), MAX_HISTORY)
            msgs    = self.db.get_messages(chat_id, limit)
            await self._send(ws, {'type':'history','chat_id':chat_id,'messages':msgs})
            return

        # ── Switch chat ────────────────────────────────────────────────
        if t == 'switch_chat':
            conn.current_chat = data.get('chat_id', 1); return

        # ── Typing ─────────────────────────────────────────────────────
        if t == 'typing':
            chat_id = data.get('chat_id', 1)
            await self._broadcast({'type':'typing','username':conn.username,'chat_id':chat_id},
                                  chat_id, skip=uid)
            return

        # ── Profile ────────────────────────────────────────────────────
        if t == 'get_profile':
            target = data.get('username')
            p = self.db.get_user_by_name(target) if target else self.db.get_user(uid)
            if not p:
                await self._send(ws, {'type':'error','message':'Не найден'}); return
            await self._send(ws, {'type':'profile','user':p})
            return

        if t == 'update_profile':
            self.db.update_profile(uid, bio=data.get('bio'), email=data.get('email'))
            await self._send(ws, {'type':'profile_updated','user':self.db.get_user(uid)})
            return

        if t == 'change_password':
            ok = self.db.change_password(uid, data.get('old_password',''), data.get('new_password',''))
            if ok:
                await self._send(ws, {'type':'password_changed'})
            else:
                await self._send(ws, {'type':'error','message':'Неверный текущий пароль'})
            return

        # ── Online ─────────────────────────────────────────────────────
        if t == 'get_online':
            await self._send(ws, {'type':'online_users','users':self.db.get_online()})
            return

        # ── Private chat ────────────────────────────────────────────────
        if t == 'start_private':
            target_name = data.get('username','').strip()
            target = self.db.get_user_by_name(target_name)
            if not target:
                await self._send(ws, {'type':'error','message':'Пользователь не найден'}); return
            chat = self.db.get_or_create_private(uid, target['id'], target_name)
            if not chat:
                await self._send(ws, {'type':'error','message':'Ошибка создания чата'}); return
            msgs = self.db.get_messages(chat['id'])
            await self._send(ws, {
                'type':'private_chat_created',
                'chat':{'id':chat['id'],'name':target_name,'type':'private'},
                'messages':msgs
            })
            if target['id'] in self.conns:
                await self._send(self.conns[target['id']].ws, {
                    'type':'new_private_chat',
                    'chat':{'id':chat['id'],'name':conn.username,'type':'private'},
                    'messages':msgs
                })
            return

        # ── Create group ────────────────────────────────────────────────
        if t == 'create_group':
            name  = data.get('name','').strip()
            desc  = data.get('description','').strip()
            names = data.get('members', [])
            if not name:
                await self._send(ws, {'type':'error','message':'Введите название'}); return
            ids = [uid]
            for n in names:
                u = self.db.get_user_by_name(n)
                if u and u['id'] not in ids:
                    ids.append(u['id'])
            chat = self.db.create_group(name, desc, uid, ids)
            if not chat:
                await self._send(ws, {'type':'error','message':'Ошибка'}); return
            members_info = self.db.get_chat_members(chat['id'])
            for m in members_info:
                if m['id'] != uid and m['id'] in self.conns:
                    await self._send(self.conns[m['id']].ws,
                                     {'type':'new_group_chat','chat':{**chat,'type':'group'}})
            await self._send(ws, {'type':'group_created','chat':{**chat,'type':'group'},'members':members_info})
            return

        # ── Chat info ───────────────────────────────────────────────────
        if t == 'get_chat_info':
            cid = data.get('chat_id')
            await self._send(ws, {
                'type':'chat_info',
                'chat': self.db.get_chat_info(cid),
                'members': self.db.get_chat_members(cid)
            })
            return

        # ── Add/remove member ───────────────────────────────────────────
        if t == 'add_member':
            cid    = data.get('chat_id')
            uname  = data.get('username','').strip()
            target = self.db.get_user_by_name(uname)
            if not target:
                await self._send(ws, {'type':'error','message':'Не найден'}); return
            self.db.add_member(cid, target['id'])
            chat = self.db.get_chat_info(cid)
            if target['id'] in self.conns:
                await self._send(self.conns[target['id']].ws, {
                    'type':'added_to_chat',
                    'chat':{**chat,'type':'group'},
                    'messages': self.db.get_messages(cid)
                })
            sys_msg = self.db.save_message(cid, uid, 'Система', f'➕ {uname} добавлен', is_system=True)
            if sys_msg:
                await self._broadcast({'type':'message','message':sys_msg}, cid)
            return

        if t == 'leave_chat':
            cid = data.get('chat_id')
            self.db.remove_member(cid, uid)
            sys_msg = self.db.save_message(cid, uid, 'Система',
                                           f'👋 {conn.username} покинул чат', is_system=True)
            if sys_msg:
                await self._broadcast({'type':'message','message':sys_msg}, cid)
            await self._send(ws, {'type':'left_chat','chat_id':cid})
            return

        # ── Calls ────────────────────────────────────────────────────────
        if t == 'call_start':
            chat_id   = data.get('chat_id', conn.current_chat)
            call      = self.db.create_call(chat_id, uid)
            if not call:
                await self._send(ws, {'type':'error','message':'Ошибка звонка'}); return
            self.active_calls[call['id']] = {
                'chat_id': chat_id,
                'initiator': uid,
                'participants': {uid}
            }
            chat_name = (self.db.get_chat_info(chat_id) or {}).get('name','Чат')
            await self._broadcast({
                'type':'incoming_call',
                'call_id': call['id'],
                'chat_id': chat_id,
                'from': conn.username,
                'chat_name': chat_name
            }, chat_id, skip=uid)
            await self._send(ws, {'type':'call_started','call_id':call['id']})
            log.info(f"📞 Звонок {call['id']} в чате {chat_id} от {conn.username}")
            return

        if t == 'call_accept':
            call_id = data.get('call_id')
            if call_id not in self.active_calls:
                return
            call = self.active_calls[call_id]
            call['participants'].add(uid)
            init_id = call['initiator']
            if init_id in self.conns:
                await self._send(self.conns[init_id].ws,
                                 {'type':'call_accepted','call_id':call_id,'by':conn.username})
            await self._send(ws, {'type':'call_joined','call_id':call_id})
            return

        if t == 'call_decline':
            call_id = data.get('call_id')
            if call_id not in self.active_calls:
                return
            call = self.active_calls[call_id]
            if call['initiator'] in self.conns:
                await self._send(self.conns[call['initiator']].ws,
                                 {'type':'call_declined','call_id':call_id,'by':conn.username})
            self.db.end_call(call_id)
            del self.active_calls[call_id]
            return

        if t == 'call_end':
            call_id = data.get('call_id')
            if call_id not in self.active_calls:
                return
            call = self.active_calls[call_id]
            self.db.end_call(call_id)
            await self._broadcast({'type':'call_ended','call_id':call_id}, call['chat_id'])
            del self.active_calls[call_id]
            return

        # ── Logout ──────────────────────────────────────────────────────
        if t == 'logout':
            token = data.get('token','')
            if token:
                self.db.delete_session(token)
            await self._send(ws, {'type':'logged_out'})
            return

    # ── WS handler ────────────────────────────────────────────────────
    async def ws_handler(self, ws):
        addr = f"{ws.remote_address[0]}:{ws.remote_address[1]}" if ws.remote_address else "?"
        log.info(f"➕ {addr}")
        try:
            async for raw in ws:
                try:
                    await self.handle(ws, json.loads(raw))
                except json.JSONDecodeError:
                    await self._send(ws, {'type':'error','message':'Неверный JSON'})
                except Exception as e:
                    log.error(f"❌ handle: {e}", exc_info=True)
                    await self._send(ws, {'type':'error','message':'Внутренняя ошибка'})
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            log.error(f"❌ ws_handler: {e}")
        finally:
            uid = self._get_uid(ws)
            if uid and uid in self.conns:
                uname = self.conns[uid].username
                del self.conns[uid]
                self.db.set_status(uid, 'offline')
                log.info(f"➖ {uname}")
                # Завершаем активные звонки этого пользователя
                for cid, call in list(self.active_calls.items()):
                    if uid in call['participants']:
                        self.db.end_call(cid)
                        await self._broadcast({'type':'call_ended','call_id':cid}, call['chat_id'])
                        del self.active_calls[cid]
                await self._broadcast_online()
            else:
                log.info(f"➖ {addr} (без авторизации)")

    # ── Background tasks ──────────────────────────────────────────────
    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(3600)
            try:
                self.db.cleanup_sessions()
                log.info("🧹 Старые сессии удалены")
            except Exception as e:
                log.error(f"❌ cleanup: {e}")

    # ── Run ───────────────────────────────────────────────────────────
    async def run(self):
        log.info(f"🚀 Запуск на {HOST}:{PORT}")
        async with websockets.serve(
            self.ws_handler, HOST, PORT,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            max_size=10 * 1024 * 1024,
        ):
            log.info(f"✅ Сервер слушает порт {PORT}")
            asyncio.create_task(self._cleanup_loop())
            await asyncio.Future()   # run forever


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    server = Server()
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        log.info("👋 Остановлен")
    except Exception as e:
        log.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
