#!/usr/bin/env python3
"""
TelegramWhite Messenger Server
"""

import os, sys, json, asyncio, hashlib, time, logging
from typing import Dict, Optional

try:
    import psycopg2, psycopg2.extras
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

try:
    import websockets
except ImportError:
    print("ERROR: pip install websockets"); sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
log = logging.getLogger("messenger")

# ── Config ──────────────────────────────────────────────────────────────
PORT           = int(os.environ.get("PORT", 10000))
HOST           = "0.0.0.0"
SECRET_KEY     = os.environ.get("TW_SECRET", "telegramwhite-2025")
SESSION_TTL    = 30 * 24 * 3600
MAX_MSG_LEN    = 4096
MAX_HISTORY    = 100
PING_INTERVAL  = 20
PING_TIMEOUT   = 10

DATABASE_URL = os.environ.get("DATABASE_URL",
    "postgresql://tgwhite_mkup_user:zRKzHPaQfYoQZ2SSZiA1lv571MxDS6Bw@dpg-d7p02je7r5hc73dnv020-a/tgwhite_mkup")

# ═══════════════════════════════════════════════════════════════════════
#   DATABASE
# ═══════════════════════════════════════════════════════════════════════
class Database:
    def __init__(self):
        self.pool = None
        self._init_pool()
        self._init_tables()

    def _init_pool(self):
        for i in range(5):
            try:
                self.pool = ThreadedConnectionPool(1, 20, DATABASE_URL,
                                                   sslmode='require', connect_timeout=10)
                log.info("✅ DB pool ok")
                return
            except Exception as e:
                log.error(f"DB pool {i+1}/5: {e}")
                if i < 4:
                    time.sleep(3 * (i + 1))
        sys.exit(1)

    def _get(self):
        if not self.pool:
            self._init_pool()
        return self.pool.getconn()

    def _put(self, c):
        if self.pool and c:
            try:
                self.pool.putconn(c)
            except:
                pass

    def execute(self, sql, params=(), *, one=False, all=False):
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
                try:
                    conn.rollback()
                except:
                    pass
            log.error(f"SQL: {e}")
            raise
        finally:
            self._put(conn)

    def _init_tables(self):
        ddl = [
            """CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT DEFAULT '',
                password TEXT NOT NULL,
                bio TEXT DEFAULT '',
                status TEXT DEFAULT 'offline',
                last_seen BIGINT NOT NULL DEFAULT 0,
                created_at BIGINT NOT NULL DEFAULT 0,
                messages_count INTEGER DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS chats (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'group',
                description TEXT DEFAULT '',
                created_by INTEGER,
                created_at BIGINT NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS chat_members (
                chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                role TEXT NOT NULL DEFAULT 'member',
                joined_at BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, user_id)
            )""",
            """CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                user_id INTEGER REFERENCES users(id),
                username TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at BIGINT NOT NULL DEFAULT 0,
                deleted BOOLEAN DEFAULT FALSE
            )""",
            """CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                created_at BIGINT NOT NULL DEFAULT 0,
                expires_at BIGINT NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS calls (
                id SERIAL PRIMARY KEY,
                chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                initiator INTEGER REFERENCES users(id),
                status TEXT NOT NULL DEFAULT 'ringing',
                started_at BIGINT NOT NULL DEFAULT 0,
                ended_at BIGINT
            )""",
            "CREATE INDEX IF NOT EXISTS idx_msgs_chat ON messages(chat_id, id)",
            "CREATE INDEX IF NOT EXISTS idx_sessions_exp ON sessions(expires_at)",
        ]
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor()
            for q in ddl:
                cur.execute(q)
            cur.execute("SELECT id FROM chats WHERE id=1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO chats(id, name, type, description, created_at) VALUES(1, 'Общий чат', 'group', 'Добро пожаловать!', %s)",
                    (int(time.time()),))
            conn.commit()
            log.info("✅ DB schema ok")
        except Exception as e:
            log.error(f"Schema: {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            self._put(conn)

    def _hash(self, p):
        return hashlib.pbkdf2_hmac('sha256', p.encode(), SECRET_KEY.encode(), 100_000).hex()

    def create_user(self, u, p, e=""):
        now = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id FROM users WHERE username=%s", (u,))
            if cur.fetchone():
                return None
            cur.execute(
                "INSERT INTO users(username, email, password, last_seen, created_at) VALUES(%s, %s, %s, %s, %s) RETURNING id, username, email, bio, created_at",
                (u, e, self._hash(p), now, now))
            user = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES(1, %s, 'member', %s) ON CONFLICT DO NOTHING",
                (user['id'], now))
            conn.commit()
            return user
        except:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            self._put(conn)

    def verify_user(self, u, p):
        return self.execute(
            "SELECT id, username, email, bio, status, created_at, messages_count FROM users WHERE username=%s AND password=%s",
            (u, self._hash(p)), one=True)

    def get_user(self, uid):
        return self.execute(
            "SELECT id, username, email, bio, status, last_seen, created_at, messages_count FROM users WHERE id=%s",
            (uid,), one=True)

    def get_user_by_name(self, u):
        return self.execute(
            "SELECT id, username, bio, status, last_seen FROM users WHERE username=%s",
            (u,), one=True)

    def set_status(self, uid, s):
        self.execute("UPDATE users SET status=%s, last_seen=%s WHERE id=%s", (s, int(time.time()), uid))

    def update_profile(self, uid, **kw):
        a = {k: v for k, v in kw.items() if k in ('bio', 'email') and v is not None}
        if not a:
            return
        self.execute(f"UPDATE users SET {', '.join(f'{k}=%s' for k in a)} WHERE id=%s",
                     tuple(a.values()) + (uid,))

    def change_password(self, uid, op, np):
        r = self.execute("SELECT password FROM users WHERE id=%s", (uid,), one=True)
        if not r or r['password'] != self._hash(op):
            return False
        self.execute("UPDATE users SET password=%s WHERE id=%s", (self._hash(np), uid))
        return True

    def create_session(self, uid):
        tok = hashlib.sha256(f"{uid}{time.time()}{SECRET_KEY}".encode()).hexdigest()
        now = int(time.time())
        self.execute("INSERT INTO sessions(token, user_id, created_at, expires_at) VALUES(%s, %s, %s, %s)",
                     (tok, uid, now, now + SESSION_TTL))
        return tok

    def check_session(self, tok):
        r = self.execute("SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s",
                         (tok, int(time.time())), one=True)
        return r['user_id'] if r else None

    def delete_session(self, tok):
        self.execute("DELETE FROM sessions WHERE token=%s", (tok,))

    def cleanup_sessions(self):
        self.execute("DELETE FROM sessions WHERE expires_at<%s", (int(time.time()),))

    def get_user_chats(self, uid):
        return self.execute("""SELECT c.id, c.name, c.type, c.description,
            (SELECT row_to_json(m) FROM (
                SELECT username, text, created_at FROM messages
                WHERE chat_id=c.id AND deleted=FALSE ORDER BY id DESC LIMIT 1
            ) m) AS last_message
            FROM chats c
            JOIN chat_members cm ON cm.chat_id=c.id
            WHERE cm.user_id=%s
            ORDER BY (SELECT COALESCE(MAX(id), 0) FROM messages WHERE chat_id=c.id) DESC""",
                            (uid,), all=True) or []

    def get_chat_info(self, cid):
        return self.execute("SELECT id, name, type, description, created_by, created_at FROM chats WHERE id=%s",
                            (cid,), one=True)

    def get_chat_members(self, cid):
        return self.execute("""SELECT u.id, u.username, u.status, cm.role
            FROM users u
            JOIN chat_members cm ON cm.user_id=u.id
            WHERE cm.chat_id=%s
            ORDER BY CASE cm.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 ELSE 3 END, u.username""",
                            (cid,), all=True) or []

    def get_or_create_private(self, u1, u2, n2):
        ex = self.execute("""SELECT c.id, c.name, c.type FROM chats c
            JOIN chat_members m1 ON m1.chat_id=c.id
            JOIN chat_members m2 ON m2.chat_id=c.id
            WHERE c.type='private' AND m1.user_id=%s AND m2.user_id=%s LIMIT 1""",
                          (u1, u2), one=True)
        if ex:
            return ex
        now = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO chats(name, type, created_by, created_at) VALUES(%s, 'private', %s, %s) RETURNING id, name, type",
                (n2, u1, now))
            chat = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES(%s, %s, 'member', %s), (%s, %s, 'member', %s)",
                (chat['id'], u1, now, chat['id'], u2, now))
            conn.commit()
            return chat
        except:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            self._put(conn)

    def create_group(self, name, desc, creator, members):
        now = int(time.time())
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO chats(name, type, description, created_by, created_at) VALUES(%s, 'group', %s, %s, %s) RETURNING id, name, type, description, created_at",
                (name, desc, creator, now))
            chat = dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES(%s, %s, 'owner', %s)",
                        (chat['id'], creator, now))
            for mid in members:
                if mid != creator:
                    cur.execute(
                        "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES(%s, %s, 'member', %s) ON CONFLICT DO NOTHING",
                        (chat['id'], mid, now))
            conn.commit()
            return chat
        except:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            self._put(conn)

    def add_member(self, cid, uid):
        self.execute(
            "INSERT INTO chat_members(chat_id, user_id, role, joined_at) VALUES(%s, %s, 'member', %s) ON CONFLICT DO NOTHING",
            (cid, uid, int(time.time())))

    def remove_member(self, cid, uid):
        self.execute("DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s", (cid, uid))

    def get_messages(self, cid, limit=MAX_HISTORY):
        try:
            return self.execute("""SELECT id, chat_id, user_id, username, text, created_at
                FROM (SELECT id, chat_id, user_id, username, text, created_at
                      FROM messages WHERE chat_id=%s AND deleted=FALSE
                      ORDER BY id DESC LIMIT %s) sub
                ORDER BY id ASC""", (cid, limit), all=True) or []
        except:
            return []

    def save_message(self, cid, uid, uname, text, system=False):
        conn = None
        try:
            conn = self._get()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO messages(chat_id, user_id, username, text, created_at, deleted) VALUES(%s, %s, %s, %s, %s, FALSE) RETURNING id, chat_id, user_id, username, text, created_at",
                (cid, uid, uname, text, int(time.time())))
            msg = dict(cur.fetchone())
            if not system:
                cur.execute("UPDATE users SET messages_count=messages_count+1 WHERE id=%s", (uid,))
            conn.commit()
            return msg
        except:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            self._put(conn)

    def get_online(self):
        return self.execute("SELECT id, username, status FROM users WHERE status='online' ORDER BY username",
                            all=True) or []

    def create_call(self, cid, ini):
        return self.execute(
            "INSERT INTO calls(chat_id, initiator, status, started_at) VALUES(%s, %s, 'ringing', %s) RETURNING id, chat_id, status, started_at",
            (cid, ini, int(time.time())), one=True)

    def end_call(self, cid):
        self.execute("UPDATE calls SET status='ended', ended_at=%s WHERE id=%s", (int(time.time()), cid))


# ═══════════════════════════════════════════════════════════════════════
#   CONNECTION
# ═══════════════════════════════════════════════════════════════════════
class MConn:
    def __init__(self, ws, uid, uname):
        self.ws = ws
        self.user_id = uid
        self.username = uname
        self.current_chat = 1


# ═══════════════════════════════════════════════════════════════════════
#   SERVER
# ═══════════════════════════════════════════════════════════════════════
class MessengerServer:
    def __init__(self, db):
        self.db = db
        self.conns: Dict[int, MConn] = {}
        self.calls: Dict[int, dict] = {}

    async def _s(self, ws, d):
        try:
            await ws.send(json.dumps(d, ensure_ascii=False, default=str))
        except:
            pass

    async def _bc(self, d, cid, skip=None):
        msg = json.dumps(d, ensure_ascii=False, default=str)
        members = {m['id'] for m in self.db.get_chat_members(cid)}
        tasks = [c.ws.send(msg) for uid, c in self.conns.items() if uid in members and uid != skip]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _bco(self):
        payload = json.dumps({'type': 'online_users', 'users': self.db.get_online()},
                             ensure_ascii=False, default=str)
        tasks = [c.ws.send(payload) for c in self.conns.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _uid(self, ws):
        for uid, c in self.conns.items():
            if c.ws is ws:
                return uid
        return None

    async def handle(self, ws, data):
        t = data.get('type')
        if not t:
            return

        if t == 'ping':
            await self._s(ws, {'type': 'pong'})
            return

        if t == 'register':
            u = data.get('username', '').strip()
            p = data.get('password', '')
            e = data.get('email', '').strip()
            if len(u) < 3:
                await self._s(ws, {'type': 'error', 'message': 'Имя минимум 3 символа'})
                return
            if len(p) < 6:
                await self._s(ws, {'type': 'error', 'message': 'Пароль минимум 6 символов'})
                return
            user = self.db.create_user(u, p, e)
            if not user:
                await self._s(ws, {'type': 'error', 'message': 'Имя уже занято'})
                return
            tok = self.db.create_session(user['id'])
            await self._s(ws, {'type': 'registered', 'user': user, 'token': tok})
            return

        if t == 'login':
            u = data.get('username', '').strip()
            p = data.get('password', '')
            user = self.db.verify_user(u, p)
            if not user:
                await self._s(ws, {'type': 'error', 'message': 'Неверный логин или пароль'})
                return
            uid = user['id']
            tok = self.db.create_session(uid)
            self.db.set_status(uid, 'online')
            self.conns[uid] = MConn(ws, uid, u)
            chats = self.db.get_user_chats(uid)
            await self._s(ws, {'type': 'logged_in', 'user': user, 'token': tok, 'chats': chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._s(ws, {'type': 'history', 'chat_id': ch['id'], 'messages': msgs})
            await self._bco()
            sm = self.db.save_message(1, uid, 'Система', f'👋 {u} присоединился', system=True)
            if sm:
                await self._bc({'type': 'message', 'message': sm}, 1, skip=uid)
            return

        if t == 'session':
            tok = data.get('token', '')
            uid = self.db.check_session(tok)
            if not uid:
                await self._s(ws, {'type': 'error', 'message': 'Сессия истекла'})
                return
            user = self.db.get_user(uid)
            if not user:
                await self._s(ws, {'type': 'error', 'message': 'Не найден'})
                return
            self.db.set_status(uid, 'online')
            self.conns[uid] = MConn(ws, uid, user['username'])
            chats = self.db.get_user_chats(uid)
            await self._s(ws, {'type': 'session_ok', 'user': user, 'chats': chats})
            for ch in chats:
                msgs = self.db.get_messages(ch['id'])
                await self._s(ws, {'type': 'history', 'chat_id': ch['id'], 'messages': msgs})
            await self._bco()
            return

        uid = self._uid(ws)
        if uid is None:
            await self._s(ws, {'type': 'error', 'message': 'Требуется авторизация'})
            return

        conn = self.conns[uid]

        if t == 'send_message':
            cid = data.get('chat_id', 1)
            text = data.get('text', '').strip()
            if not text or len(text) > MAX_MSG_LEN:
                return
            msg = self.db.save_message(cid, uid, conn.username, text)
            if msg:
                await self._bc({'type': 'message', 'message': msg}, cid)
            return

        if t == 'get_history':
            cid = data.get('chat_id', 1)
            lim = min(int(data.get('limit', MAX_HISTORY)), MAX_HISTORY)
            await self._s(ws, {'type': 'history', 'chat_id': cid, 'messages': self.db.get_messages(cid, lim)})
            return

        if t == 'switch_chat':
            conn.current_chat = data.get('chat_id', 1)
            return

        if t == 'typing':
            cid = data.get('chat_id', 1)
            await self._bc({'type': 'typing', 'username': conn.username, 'chat_id': cid}, cid, skip=uid)
            return

        if t == 'get_profile':
            tgt = data.get('username')
            p = self.db.get_user_by_name(tgt) if tgt else self.db.get_user(uid)
            if not p:
                await self._s(ws, {'type': 'error', 'message': 'Не найден'})
                return
            await self._s(ws, {'type': 'profile', 'user': p})
            return

        if t == 'update_profile':
            self.db.update_profile(uid, bio=data.get('bio'), email=data.get('email'))
            await self._s(ws, {'type': 'profile_updated', 'user': self.db.get_user(uid)})
            return

        if t == 'change_password':
            ok = self.db.change_password(uid, data.get('old_password', ''), data.get('new_password', ''))
            await self._s(ws, {'type': 'password_changed'} if ok else {'type': 'error', 'message': 'Неверный пароль'})
            return

        if t == 'get_online':
            await self._s(ws, {'type': 'online_users', 'users': self.db.get_online()})
            return

        if t == 'start_private':
            tn = data.get('username', '').strip()
            tgt = self.db.get_user_by_name(tn)
            if not tgt:
                await self._s(ws, {'type': 'error', 'message': 'Не найден'})
                return
            chat = self.db.get_or_create_private(uid, tgt['id'], tn)
            if not chat:
                await self._s(ws, {'type': 'error', 'message': 'Ошибка'})
                return
            msgs = self.db.get_messages(chat['id'])
            await self._s(ws, {'type': 'private_chat_created',
                               'chat': {'id': chat['id'], 'name': tn, 'type': 'private'},
                               'messages': msgs})
            if tgt['id'] in self.conns:
                await self._s(self.conns[tgt['id']].ws,
                              {'type': 'new_private_chat',
                               'chat': {'id': chat['id'], 'name': conn.username, 'type': 'private'},
                               'messages': msgs})
            return

        if t == 'create_group':
            name = data.get('name', '').strip()
            desc = data.get('description', '').strip()
            names = data.get('members', [])
            if not name:
                await self._s(ws, {'type': 'error', 'message': 'Введите название'})
                return
            ids = [uid]
            for n in names:
                u2 = self.db.get_user_by_name(n)
                if u2 and u2['id'] not in ids:
                    ids.append(u2['id'])
            chat = self.db.create_group(name, desc, uid, ids)
            if not chat:
                await self._s(ws, {'type': 'error', 'message': 'Ошибка'})
                return
            mi = self.db.get_chat_members(chat['id'])
            for m in mi:
                if m['id'] != uid and m['id'] in self.conns:
                    await self._s(self.conns[m['id']].ws,
                                  {'type': 'new_group_chat', 'chat': {**chat, 'type': 'group'}})
            await self._s(ws, {'type': 'group_created', 'chat': {**chat, 'type': 'group'}, 'members': mi})
            return

        if t == 'get_chat_info':
            cid = data.get('chat_id')
            await self._s(ws, {'type': 'chat_info', 'chat': self.db.get_chat_info(cid),
                               'members': self.db.get_chat_members(cid)})
            return

        if t == 'add_member':
            cid = data.get('chat_id')
            un = data.get('username', '').strip()
            tgt = self.db.get_user_by_name(un)
            if not tgt:
                await self._s(ws, {'type': 'error', 'message': 'Не найден'})
                return
            self.db.add_member(cid, tgt['id'])
            chat = self.db.get_chat_info(cid)
            if tgt['id'] in self.conns:
                await self._s(self.conns[tgt['id']].ws,
                              {'type': 'added_to_chat', 'chat': {**chat, 'type': 'group'},
                               'messages': self.db.get_messages(cid)})
            sm = self.db.save_message(cid, uid, 'Система', f'➕ {un} добавлен', system=True)
            if sm:
                await self._bc({'type': 'message', 'message': sm}, cid)
            return

        if t == 'leave_chat':
            cid = data.get('chat_id')
            self.db.remove_member(cid, uid)
            sm = self.db.save_message(cid, uid, 'Система', f'👋 {conn.username} покинул', system=True)
            if sm:
                await self._bc({'type': 'message', 'message': sm}, cid)
            await self._s(ws, {'type': 'left_chat', 'chat_id': cid})
            return

        if t == 'call_start':
            cid = data.get('chat_id', conn.current_chat)
            call = self.db.create_call(cid, uid)
            if not call:
                await self._s(ws, {'type': 'error', 'message': 'Ошибка звонка'})
                return
            self.calls[call['id']] = {'chat_id': cid, 'initiator': uid, 'participants': {uid}}
            cn = (self.db.get_chat_info(cid) or {}).get('name', 'Чат')
            await self._bc({'type': 'incoming_call', 'call_id': call['id'], 'chat_id': cid,
                            'from': conn.username, 'chat_name': cn}, cid, skip=uid)
            await self._s(ws, {'type': 'call_started', 'call_id': call['id']})
            return

        if t == 'call_accept':
            cid = data.get('call_id')
            if cid not in self.calls:
                return
            call = self.calls[cid]
            call['participants'].add(uid)
            ini = call['initiator']
            if ini in self.conns:
                await self._s(self.conns[ini].ws, {'type': 'call_accepted', 'call_id': cid, 'by': conn.username})
            await self._s(ws, {'type': 'call_joined', 'call_id': cid})
            return

        if t == 'call_decline':
            cid = data.get('call_id')
            if cid not in self.calls:
                return
            call = self.calls[cid]
            if call['initiator'] in self.conns:
                await self._s(self.conns[call['initiator']].ws,
                              {'type': 'call_declined', 'call_id': cid, 'by': conn.username})
            self.db.end_call(cid)
            del self.calls[cid]
            return

        if t == 'call_end':
            cid = data.get('call_id')
            if cid not in self.calls:
                return
            call = self.calls[cid]
            self.db.end_call(cid)
            await self._bc({'type': 'call_ended', 'call_id': cid}, call['chat_id'])
            del self.calls[cid]
            return

        if t == 'logout':
            tok = data.get('token', '')
            if tok:
                self.db.delete_session(tok)
            await self._s(ws, {'type': 'logged_out'})
            return

    async def ws_handler(self, ws):
        try:
            async for raw in ws:
                try:
                    await self.handle(ws, json.loads(raw))
                except json.JSONDecodeError:
                    await self._s(ws, {'type': 'error', 'message': 'Неверный JSON'})
                except Exception as e:
                    log.error(f"msg handle: {e}", exc_info=True)
                    await self._s(ws, {'type': 'error', 'message': 'Ошибка'})
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            log.error(f"msg ws: {e}")
        finally:
            uid = self._uid(ws)
            if uid and uid in self.conns:
                uname = self.conns[uid].username
                del self.conns[uid]
                self.db.set_status(uid, 'offline')
                for cid, call in list(self.calls.items()):
                    if uid in call['participants']:
                        self.db.end_call(cid)
                        await self._bc({'type': 'call_ended', 'call_id': cid}, call['chat_id'])
                        del self.calls[cid]
                await self._bco()

    async def cleanup_loop(self):
        while True:
            await asyncio.sleep(3600)
            try:
                self.db.cleanup_sessions()
            except:
                pass


# ═══════════════════════════════════════════════════════════════════════
#   MAIN
# ═══════════════════════════════════════════════════════════════════════
async def main():
    log.info("=" * 56)
    log.info("🚀  TelegramWhite Messenger Server")
    log.info("=" * 56)

    db = Database()
    server = MessengerServer(db)

    ws_server = await websockets.serve(server.ws_handler, HOST, PORT,
                                       ping_interval=PING_INTERVAL,
                                       ping_timeout=PING_TIMEOUT,
                                       max_size=10 * 1024 * 1024)
    log.info(f"✅ Messenger  ws://{HOST}:{PORT}")

    asyncio.create_task(server.cleanup_loop())
    log.info("✅ Все сервисы запущены")

    await asyncio.Future()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("👋 Стоп")
    except Exception as e:
        log.error(f"FATAL: {e}", exc_info=True)
        sys.exit(1)
