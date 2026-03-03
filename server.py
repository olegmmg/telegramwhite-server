#!/usr/bin/env python3
"""
TelegramWhite WebSocket Server v2
Профили, групповые чаты, WebRTC звонки
pip install websockets psycopg2-binary
"""

import asyncio, json, hashlib, os, time, logging
from typing import Dict, Optional
import websockets, psycopg2, psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("TW")

HOST         = "0.0.0.0"
PORT         = int(os.environ.get("PORT", 8080))
SECRET_KEY   = os.environ.get("TW_SECRET", "change-me-please")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
MAX_AVATAR   = 2 * 1024 * 1024  # 2MB


class DB:
    def __init__(self, dsn):
        if dsn.startswith("postgres://"):
            dsn = dsn.replace("postgres://", "postgresql://", 1)
        self.pool = ThreadedConnectionPool(1, 20, dsn)
        self._init()

    def _conn(self):
        c = self.pool.getconn()
        c.autocommit = False
        return c

    def _release(self, c):
        self.pool.putconn(c)

    def _init(self):
        c = self._conn()
        try:
            cur = c.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id         SERIAL PRIMARY KEY,
                    username   TEXT   UNIQUE NOT NULL,
                    email      TEXT   DEFAULT '',
                    password   TEXT   NOT NULL,
                    bio        TEXT   DEFAULT '',
                    avatar_url TEXT   DEFAULT '',
                    phone      TEXT   DEFAULT '',
                    status     TEXT   DEFAULT 'offline',
                    created_at BIGINT NOT NULL,
                    last_seen  BIGINT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chats (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT   NOT NULL,
                    type        TEXT   NOT NULL DEFAULT 'group',
                    description TEXT   DEFAULT '',
                    avatar_url  TEXT   DEFAULT '',
                    created_by  INTEGER REFERENCES users(id),
                    owner_id    INTEGER REFERENCES users(id),
                    created_at  BIGINT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chat_members (
                    chat_id   INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                    user_id   INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    role      TEXT   NOT NULL DEFAULT 'member',
                    joined_at BIGINT NOT NULL,
                    PRIMARY KEY (chat_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id         SERIAL PRIMARY KEY,
                    chat_id    INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                    user_id    INTEGER REFERENCES users(id),
                    username   TEXT    NOT NULL,
                    text       TEXT    NOT NULL,
                    type       TEXT    DEFAULT 'text',
                    created_at BIGINT  NOT NULL,
                    edited_at  BIGINT,
                    deleted    BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    token      TEXT PRIMARY KEY,
                    user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calls (
                    id         SERIAL PRIMARY KEY,
                    chat_id    INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                    initiator  INTEGER REFERENCES users(id),
                    type       TEXT NOT NULL DEFAULT 'audio',
                    status     TEXT NOT NULL DEFAULT 'ringing',
                    started_at BIGINT NOT NULL,
                    ended_at   BIGINT,
                    duration   INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS reactions (
                    message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                    user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    emoji      TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    PRIMARY KEY (message_id, user_id)
                );
                CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_calls    ON calls(chat_id);
            """)
            cur.execute("SELECT id FROM chats WHERE id=1 LIMIT 1")
            if not cur.fetchone():
                cur.execute("INSERT INTO chats (name,type,created_at) VALUES ('Общий чат','group',%s)", (int(time.time()),))
            c.commit()
            log.info("PostgreSQL: БД готова")
        except Exception as e:
            c.rollback(); log.error(f"Ошибка БД: {e}"); raise
        finally:
            self._release(c)

    def _one(self, q, p=()):
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, p)
            r = cur.fetchone()
            return dict(r) if r else None
        finally:
            self._release(c)

    def _all(self, q, p=()):
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, p)
            return [dict(r) for r in cur.fetchall()]
        finally:
            self._release(c)

    def _run(self, q, p=(), ret=False):
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, p)
            r = dict(cur.fetchone()) if ret else None
            c.commit()
            return r
        except Exception as e:
            c.rollback(); raise e
        finally:
            self._release(c)

    def hash(self, pw):
        return hashlib.pbkdf2_hmac('sha256', pw.encode(), SECRET_KEY.encode(), 100_000).hex()

    # ── Пользователи ─────────────────────────────
    def create_user(self, username, password, email=""):
        now = int(time.time())
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO users (username,email,password,created_at,last_seen) VALUES (%s,%s,%s,%s,%s) RETURNING id,username,email",
                (username, email, self.hash(password), now, now)
            )
            user = dict(cur.fetchone())
            cur.execute(
                "INSERT INTO chat_members (chat_id,user_id,role,joined_at) SELECT 1,%s,'member',%s WHERE EXISTS(SELECT 1 FROM chats WHERE id=1) ON CONFLICT DO NOTHING",
                (user['id'], now)
            )
            c.commit()
            return user
        except psycopg2.errors.UniqueViolation:
            c.rollback(); return None
        except Exception as e:
            c.rollback(); raise e
        finally:
            self._release(c)

    def verify_user(self, username, password):
        return self._one(
            "SELECT id,username,email,bio,avatar_url,phone,created_at FROM users WHERE username=%s AND password=%s",
            (username, self.hash(password))
        )

    def get_user(self, uid):
        return self._one(
            "SELECT id,username,email,bio,avatar_url,phone,status,last_seen,created_at FROM users WHERE id=%s",
            (uid,)
        )

    def get_user_by_name(self, username):
        return self._one(
            "SELECT id,username,bio,avatar_url,status,last_seen,created_at FROM users WHERE username=%s",
            (username,)
        )

    def update_profile(self, uid, bio=None, avatar_url=None, phone=None, email=None):
        fields, vals = [], []
        if bio is not None:        fields.append("bio=%s");        vals.append(bio)
        if avatar_url is not None: fields.append("avatar_url=%s"); vals.append(avatar_url)
        if phone is not None:      fields.append("phone=%s");      vals.append(phone)
        if email is not None:      fields.append("email=%s");      vals.append(email)
        if not fields: return
        vals.append(uid)
        self._run(f"UPDATE users SET {','.join(fields)} WHERE id=%s", vals)

    def change_password(self, uid, old_pw, new_pw):
        user = self._one("SELECT password FROM users WHERE id=%s", (uid,))
        if not user or user['password'] != self.hash(old_pw): return False
        self._run("UPDATE users SET password=%s WHERE id=%s", (self.hash(new_pw), uid))
        return True

    def set_status(self, uid, status):
        self._run("UPDATE users SET status=%s,last_seen=%s WHERE id=%s", (status, int(time.time()), uid))

    # ── Сессии ───────────────────────────────────
    def new_session(self, uid):
        token = hashlib.sha256(f"{uid}{time.time()}{SECRET_KEY}".encode()).hexdigest()
        now = int(time.time())
        self._run("INSERT INTO sessions (token,user_id,created_at,expires_at) VALUES (%s,%s,%s,%s)",
                  (token, uid, now, now + 86400*30))
        return token

    def check_session(self, token):
        r = self._one("SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s", (token, int(time.time())))
        return r['user_id'] if r else None

    def del_session(self, token):
        self._run("DELETE FROM sessions WHERE token=%s", (token,))

    # ── Чаты ─────────────────────────────────────
    def user_chats(self, uid):
        return self._all("""
            SELECT c.id, c.name, c.type, c.description, c.avatar_url, cm.role,
                (SELECT text FROM messages WHERE chat_id=c.id AND deleted=FALSE ORDER BY created_at DESC LIMIT 1) AS last_msg,
                (SELECT created_at FROM messages WHERE chat_id=c.id AND deleted=FALSE ORDER BY created_at DESC LIMIT 1) AS last_ts,
                (SELECT username FROM messages WHERE chat_id=c.id AND deleted=FALSE ORDER BY created_at DESC LIMIT 1) AS last_user
            FROM chats c JOIN chat_members cm ON cm.chat_id=c.id
            WHERE cm.user_id=%s
            ORDER BY (SELECT COALESCE(MAX(created_at),0) FROM messages WHERE chat_id=c.id) DESC
        """, (uid,))

    def create_group(self, name, description, owner_id, member_ids):
        now = int(time.time())
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "INSERT INTO chats (name,type,description,created_by,owner_id,created_at) VALUES (%s,'group',%s,%s,%s,%s) RETURNING id,name,type,description,avatar_url",
                (name, description, owner_id, owner_id, now)
            )
            chat = dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'owner',%s)",
                        (chat['id'], owner_id, now))
            for mid in member_ids:
                if mid != owner_id:
                    cur.execute("INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'member',%s) ON CONFLICT DO NOTHING",
                                (chat['id'], mid, now))
            c.commit()
            return chat
        except Exception as e:
            c.rollback(); raise e
        finally:
            self._release(c)

    def get_chat_members(self, chat_id):
        return self._all("""
            SELECT u.id, u.username, u.avatar_url, u.status, cm.role
            FROM users u JOIN chat_members cm ON cm.user_id=u.id
            WHERE cm.chat_id=%s ORDER BY cm.role DESC, u.username
        """, (chat_id,))

    def add_member(self, chat_id, user_id):
        now = int(time.time())
        self._run("INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'member',%s) ON CONFLICT DO NOTHING",
                  (chat_id, user_id, now))

    def remove_member(self, chat_id, user_id):
        self._run("DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))

    def get_or_create_private(self, uid1, uid2, name2):
        now = int(time.time())
        c = self._conn()
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT c.id,c.name FROM chats c
                JOIN chat_members a ON a.chat_id=c.id AND a.user_id=%s
                JOIN chat_members b ON b.chat_id=c.id AND b.user_id=%s
                WHERE c.type='private' LIMIT 1
            """, (uid1, uid2))
            ex = cur.fetchone()
            if ex: return dict(ex)
            cur.execute("INSERT INTO chats (name,type,created_by,created_at) VALUES (%s,'private',%s,%s) RETURNING id,name",
                        (name2, uid1, now))
            chat = dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members (chat_id,user_id,role,joined_at) VALUES (%s,%s,'member',%s),(%s,%s,'member',%s)",
                        (chat['id'],uid1,now, chat['id'],uid2,now))
            c.commit()
            return chat
        except Exception as e:
            c.rollback(); raise e
        finally:
            self._release(c)

    # ── Сообщения ─────────────────────────────────
    def save_msg(self, chat_id, uid, username, text, msg_type='text'):
        return self._run(
            "INSERT INTO messages (chat_id,user_id,username,text,type,created_at) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id,chat_id,user_id,username,text,type,created_at",
            (chat_id, uid, username, text, msg_type, int(time.time())), ret=True
        )

    def get_msgs(self, chat_id, limit=50, before_id=None):
        if before_id:
            rows = self._all(
                "SELECT id,user_id,username,text,type,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE AND id<%s ORDER BY created_at DESC LIMIT %s",
                (chat_id, before_id, limit))
        else:
            rows = self._all(
                "SELECT id,user_id,username,text,type,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE ORDER BY created_at DESC LIMIT %s",
                (chat_id, limit))
        return list(reversed(rows))

    def add_reaction(self, message_id, user_id, emoji):
        try:
            self._run(
                "INSERT INTO reactions (message_id,user_id,emoji,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT (message_id,user_id) DO UPDATE SET emoji=%s",
                (message_id, user_id, emoji, int(time.time()), emoji))
            return True
        except:
            return False

    def get_reactions(self, message_id):
        return self._all("SELECT emoji, COUNT(*) as count FROM reactions WHERE message_id=%s GROUP BY emoji", (message_id,))

    def online_users(self):
        return self._all("SELECT id,username,avatar_url FROM users WHERE status='online' ORDER BY username")

    # ── Звонки ────────────────────────────────────
    def create_call(self, chat_id, initiator, call_type='audio'):
        return self._run(
            "INSERT INTO calls (chat_id,initiator,type,status,started_at) VALUES (%s,%s,%s,'ringing',%s) RETURNING id",
            (chat_id, initiator, call_type, int(time.time())), ret=True)

    def end_call(self, call_id):
        now = int(time.time())
        c = self._conn()
        try:
            cur = c.cursor()
            cur.execute("SELECT started_at FROM calls WHERE id=%s", (call_id,))
            row = cur.fetchone()
            duration = (now - row[0]) if row else 0
            cur.execute("UPDATE calls SET status='ended',ended_at=%s,duration=%s WHERE id=%s", (now, duration, call_id))
            c.commit()
        except Exception as e:
            c.rollback()
        finally:
            self._release(c)


# ══════════════════════════════════════════════════
# СЕРВЕР
# ══════════════════════════════════════════════════
if not DATABASE_URL:
    log.error("DATABASE_URL не задан!"); exit(1)

db = DB(DATABASE_URL)
conns: Dict[int, dict] = {}
active_calls: Dict[int, dict] = {}


async def send(ws, data):
    try:
        await ws.send(json.dumps(data, ensure_ascii=False, default=str))
    except:
        pass


async def broadcast(data, chat_id=None, skip=None):
    msg = json.dumps(data, ensure_ascii=False, default=str)
    if chat_id is None:
        targets = [info["ws"] for uid, info in conns.items() if info["ws"] is not skip]
    else:
        members = db._all("SELECT user_id FROM chat_members WHERE chat_id=%s", (chat_id,))
        member_ids = {m["user_id"] for m in members}
        targets = [info["ws"] for uid, info in conns.items()
                   if info["ws"] is not skip and uid in member_ids]
    if targets:
        await asyncio.gather(*[t.send(msg) for t in targets], return_exceptions=True)


async def push_online():
    await broadcast({"type": "online_users", "users": db.online_users()})


async def handler(ws):
    uid = None
    log.info(f"+ {ws.remote_address}")
    try:
        async for raw in ws:
            try:
                d = json.loads(raw)
            except:
                await send(ws, {"type": "error", "message": "Неверный JSON"}); continue

            t = d.get("type", "")

            if t == "ping":
                await send(ws, {"type": "pong"}); continue

            if t == "register":
                u = d.get("username","").strip(); p = d.get("password",""); e = d.get("email","").strip()
                if len(u) < 3: await send(ws, {"type":"error","message":"Имя: минимум 3 символа"}); continue
                if len(p) < 6: await send(ws, {"type":"error","message":"Пароль: минимум 6 символов"}); continue
                user = db.create_user(u, p, e)
                if not user: await send(ws, {"type":"error","message":"Имя уже занято"}); continue
                token = db.new_session(user["id"])
                log.info(f"Регистрация: {u}")
                await send(ws, {"type":"registered","user":user,"token":token})
                continue

            if t == "login":
                u = d.get("username","").strip(); p = d.get("password","")
                user = db.verify_user(u, p)
                if not user: await send(ws, {"type":"error","message":"Неверный логин или пароль"}); continue
                token = db.new_session(user["id"])
                db.set_status(user["id"], "online")
                uid = user["id"]
                conns[uid] = {"ws":ws,"username":user["username"],"current_chat":1,"avatar_url":user.get("avatar_url","")}
                await send(ws, {"type":"logged_in","user":user,"token":token,"chats":db.user_chats(uid)})
                await send(ws, {"type":"history","chat_id":1,"messages":db.get_msgs(1)})
                await push_online()
                await broadcast({"type":"system","chat_id":1,"text":f"👋 {u} вошёл"}, chat_id=1, skip=ws)
                log.info(f"Вход: {u}")
                continue

            if t == "session":
                token = d.get("token","")
                user_id = db.check_session(token)
                if not user_id: await send(ws, {"type":"error","message":"Сессия истекла"}); continue
                user = db.get_user(user_id)
                if not user: await send(ws, {"type":"error","message":"Пользователь не найден"}); continue
                db.set_status(user_id, "online")
                uid = user_id
                conns[uid] = {"ws":ws,"username":user["username"],"current_chat":1,"avatar_url":user.get("avatar_url","")}
                await send(ws, {"type":"session_ok","user":user,"chats":db.user_chats(uid)})
                await send(ws, {"type":"history","chat_id":1,"messages":db.get_msgs(1)})
                await push_online()
                continue

            if not uid or uid not in conns:
                await send(ws, {"type":"error","message":"Необходима авторизация"}); continue

            # ── Сообщения ────────────────────────
            if t == "send_message":
                chat_id = d.get("chat_id",1); text = d.get("text","").strip()
                if not text or len(text) > 4096: await send(ws, {"type":"error","message":"Пустое сообщение"}); continue
                msg = db.save_msg(chat_id, uid, conns[uid]["username"], text)
                await broadcast({"type":"message","message":msg}, chat_id=chat_id)

            elif t == "get_history":
                chat_id = d.get("chat_id",1)
                await send(ws, {"type":"history","chat_id":chat_id,"messages":db.get_msgs(chat_id, before_id=d.get("before_id"))})

            elif t == "switch_chat":
                chat_id = d.get("chat_id",1)
                conns[uid]["current_chat"] = chat_id
                await send(ws, {"type":"history","chat_id":chat_id,"messages":db.get_msgs(chat_id)})

            elif t == "typing":
                chat_id = d.get("chat_id",1)
                await broadcast({"type":"typing","username":conns[uid]["username"],"chat_id":chat_id}, chat_id=chat_id, skip=ws)

            # ── Профиль ──────────────────────────
            elif t == "get_profile":
                target = d.get("username")
                u = db.get_user_by_name(target) if target else db.get_user(uid)
                if not u: await send(ws, {"type":"error","message":"Пользователь не найден"}); continue
                await send(ws, {"type":"profile","user":u})

            elif t == "update_profile":
                bio = d.get("bio"); phone = d.get("phone"); email = d.get("email")
                avatar_b64 = d.get("avatar"); avatar_url = None
                if avatar_b64:
                    if len(avatar_b64) > MAX_AVATAR * 1.4:
                        await send(ws, {"type":"error","message":"Аватар слишком большой (макс 2MB)"}); continue
                    avatar_url = avatar_b64
                db.update_profile(uid, bio=bio, avatar_url=avatar_url, phone=phone, email=email)
                if avatar_url: conns[uid]["avatar_url"] = avatar_url
                user = db.get_user(uid)
                await send(ws, {"type":"profile_updated","user":user})
                await push_online()

            elif t == "change_password":
                old_pw = d.get("old_password",""); new_pw = d.get("new_password","")
                if len(new_pw) < 6: await send(ws, {"type":"error","message":"Минимум 6 символов"}); continue
                if db.change_password(uid, old_pw, new_pw):
                    await send(ws, {"type":"password_changed"})
                else:
                    await send(ws, {"type":"error","message":"Неверный текущий пароль"})

            # ── Группы ───────────────────────────
            elif t == "create_group":
                name = d.get("name","").strip(); description = d.get("description","").strip()
                member_names = d.get("members",[])
                if not name: await send(ws, {"type":"error","message":"Введите название группы"}); continue
                member_ids = [uid]
                for uname in member_names:
                    u = db._one("SELECT id FROM users WHERE username=%s", (uname,))
                    if u: member_ids.append(u["id"])
                chat = db.create_group(name, description, uid, member_ids)
                members = db.get_chat_members(chat["id"])
                for m in members:
                    if m["id"] in conns and m["id"] != uid:
                        await send(conns[m["id"]]["ws"], {"type":"new_group_chat","chat":{**chat,"type":"group"}})
                await send(ws, {"type":"group_created","chat":{**chat,"type":"group"},"members":members})
                log.info(f"Группа: {name}")

            elif t == "get_chat_info":
                chat_id = d.get("chat_id")
                chat = db._one("SELECT id,name,type,description,avatar_url,owner_id FROM chats WHERE id=%s", (chat_id,))
                members = db.get_chat_members(chat_id)
                await send(ws, {"type":"chat_info","chat":chat,"members":members})

            elif t == "add_member":
                chat_id = d.get("chat_id"); username = d.get("username","").strip()
                target = db._one("SELECT id FROM users WHERE username=%s", (username,))
                if not target: await send(ws, {"type":"error","message":"Пользователь не найден"}); continue
                db.add_member(chat_id, target["id"])
                chat_name = db._one("SELECT name FROM chats WHERE id=%s", (chat_id,))["name"]
                if target["id"] in conns:
                    await send(conns[target["id"]]["ws"], {"type":"added_to_chat","chat":{"id":chat_id,"name":chat_name,"type":"group"}})
                await broadcast({"type":"system","chat_id":chat_id,"text":f"➕ {username} добавлен"}, chat_id=chat_id)

            elif t == "leave_chat":
                chat_id = d.get("chat_id"); uname = conns[uid]["username"]
                db.remove_member(chat_id, uid)
                await broadcast({"type":"system","chat_id":chat_id,"text":f"👋 {uname} покинул чат"}, chat_id=chat_id)
                await send(ws, {"type":"left_chat","chat_id":chat_id})

            # ── Личные чаты ──────────────────────
            elif t == "start_private":
                target_name = d.get("username","").strip()
                target = db._one("SELECT id FROM users WHERE username=%s", (target_name,))
                if not target: await send(ws, {"type":"error","message":"Пользователь не найден"}); continue
                chat = db.get_or_create_private(uid, target['id'], target_name)
                msgs = db.get_msgs(chat['id'])
                await send(ws, {"type":"private_chat_created","chat":{"id":chat["id"],"name":target_name,"type":"private"},"messages":msgs})
                if target['id'] in conns:
                    await send(conns[target['id']]["ws"], {
                        "type":"new_private_chat",
                        "chat":{"id":chat["id"],"name":conns[uid]["username"],"type":"private"},
                        "messages":db.get_msgs(chat["id"])
                    })

            # ── Реакции ──────────────────────────
            elif t == "react":
                msg_id = d.get("message_id"); emoji = d.get("emoji",""); chat_id = d.get("chat_id",1)
                if msg_id and emoji:
                    db.add_reaction(msg_id, uid, emoji)
                    await broadcast({"type":"reactions_updated","message_id":msg_id,"reactions":db.get_reactions(msg_id)}, chat_id=chat_id)

            # ── Звонки ───────────────────────────
            elif t == "call_start":
                chat_id = d.get("chat_id"); call_type = d.get("call_type","audio")
                call = db.create_call(chat_id, uid, call_type)
                call_id = call["id"]
                active_calls[call_id] = {"chat_id":chat_id,"type":call_type,"initiator":uid,"participants":{uid}}
                await broadcast({"type":"incoming_call","call_id":call_id,"chat_id":chat_id,"call_type":call_type,
                                 "from":conns[uid]["username"],"from_id":uid,"avatar":conns[uid].get("avatar_url","")},
                                chat_id=chat_id, skip=ws)
                await send(ws, {"type":"call_started","call_id":call_id,"call_type":call_type})

            elif t == "call_accept":
                call_id = d.get("call_id")
                if call_id not in active_calls: continue
                active_calls[call_id]["participants"].add(uid)
                iid = active_calls[call_id]["initiator"]
                if iid in conns:
                    await send(conns[iid]["ws"], {"type":"call_accepted","call_id":call_id,"by":conns[uid]["username"],"by_id":uid})
                await send(ws, {"type":"call_joined","call_id":call_id})

            elif t == "call_decline":
                call_id = d.get("call_id")
                if call_id not in active_calls: continue
                iid = active_calls[call_id]["initiator"]
                if iid in conns:
                    await send(conns[iid]["ws"], {"type":"call_declined","call_id":call_id,"by":conns[uid]["username"]})

            elif t == "call_end":
                call_id = d.get("call_id")
                if call_id in active_calls:
                    chat_id = active_calls[call_id]["chat_id"]
                    db.end_call(call_id)
                    await broadcast({"type":"call_ended","call_id":call_id}, chat_id=chat_id)
                    del active_calls[call_id]

            # ── WebRTC сигналы ───────────────────
            elif t == "webrtc_offer":
                tid = d.get("target_id")
                if tid in conns:
                    await send(conns[tid]["ws"], {"type":"webrtc_offer","call_id":d.get("call_id"),"offer":d.get("offer"),"from_id":uid})

            elif t == "webrtc_answer":
                tid = d.get("target_id")
                if tid in conns:
                    await send(conns[tid]["ws"], {"type":"webrtc_answer","call_id":d.get("call_id"),"answer":d.get("answer"),"from_id":uid})

            elif t == "webrtc_ice":
                tid = d.get("target_id")
                if tid in conns:
                    await send(conns[tid]["ws"], {"type":"webrtc_ice","call_id":d.get("call_id"),"candidate":d.get("candidate"),"from_id":uid})

            # ── Выход ────────────────────────────
            elif t == "logout":
                db.del_session(d.get("token",""))
                db.set_status(uid, "offline")
                uname = conns.pop(uid, {}).get("username","")
                uid = None
                await broadcast({"type":"system","chat_id":1,"text":f"🔴 {uname} вышел"}, chat_id=1)
                await push_online()
                await send(ws, {"type":"logged_out"})

            elif t == "get_online":
                await send(ws, {"type":"online_users","users":db.online_users()})

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Ошибка: {e}", exc_info=True)
    finally:
        if uid and uid in conns:
            uname = conns.pop(uid, {}).get("username","")
            db.set_status(uid, "offline")
            for cid, call in list(active_calls.items()):
                if call["initiator"] == uid:
                    db.end_call(cid)
                    await broadcast({"type":"call_ended","call_id":cid}, chat_id=call["chat_id"])
                    del active_calls[cid]
            await broadcast({"type":"system","chat_id":1,"text":f"🔴 {uname} отключился"}, chat_id=1)
            await push_online()
        log.info(f"- {ws.remote_address}")


async def main():
    log.info(f"🚀 Сервер на {HOST}:{PORT}")
    async with websockets.serve(handler, HOST, PORT, ping_interval=30, ping_timeout=10):
        log.info("✅ Готов!")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
