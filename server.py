#!/usr/bin/env python3
"""
TelegramWhite WebSocket Server — PostgreSQL edition
pip install websockets psycopg2-binary
"""

import asyncio
import json
import hashlib
import os
import time
import logging
from typing import Dict, Optional

import websockets
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("TW")

HOST         = "0.0.0.0"
PORT         = int(os.environ.get("PORT", 8080))
SECRET_KEY   = os.environ.get("TW_SECRET", "change-me-please")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ════════════════════════════════════════════════════
class DB:
    def __init__(self, dsn: str):
        if dsn.startswith("postgres://"):
            dsn = dsn.replace("postgres://", "postgresql://", 1)
        self.pool = ThreadedConnectionPool(1, 10, dsn)
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
                    status     TEXT   DEFAULT 'offline',
                    created_at BIGINT NOT NULL,
                    last_seen  BIGINT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chats (
                    id         SERIAL PRIMARY KEY,
                    name       TEXT   NOT NULL,
                    type       TEXT   NOT NULL DEFAULT 'group',
                    created_by INTEGER REFERENCES users(id),
                    created_at BIGINT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chat_members (
                    chat_id   INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                    user_id   INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    joined_at BIGINT NOT NULL,
                    PRIMARY KEY (chat_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id         SERIAL PRIMARY KEY,
                    chat_id    INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                    user_id    INTEGER REFERENCES users(id),
                    username   TEXT    NOT NULL,
                    text       TEXT    NOT NULL,
                    created_at BIGINT  NOT NULL,
                    deleted    BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    token      TEXT PRIMARY KEY,
                    user_id    INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id, created_at);
            """)
            # Общий чат
            cur.execute("SELECT id FROM chats LIMIT 1")
            if not cur.fetchone():
                cur.execute("INSERT INTO chats (name, type, created_at) VALUES ('Общий чат','group',%s)", (int(time.time()),))
            c.commit()
            log.info("PostgreSQL: БД готова")
        except Exception as e:
            c.rollback()
            log.error(f"Ошибка инициализации: {e}")
            raise
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
            c.rollback()
            raise e
        finally:
            self._release(c)

    def hash(self, pw: str) -> str:
        return hashlib.pbkdf2_hmac('sha256', pw.encode(), SECRET_KEY.encode(), 100_000).hex()

    # ── Пользователи ────────────────────────────────
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
                "INSERT INTO chat_members (chat_id,user_id,joined_at) SELECT 1,%s,%s WHERE EXISTS(SELECT 1 FROM chats WHERE id=1) ON CONFLICT DO NOTHING",
                (user['id'], now)
            )
            c.commit()
            return user
        except psycopg2.errors.UniqueViolation:
            c.rollback()
            return None
        except Exception as e:
            c.rollback()
            raise e
        finally:
            self._release(c)

    def verify_user(self, username, password):
        r = self._one("SELECT id,username,email FROM users WHERE username=%s AND password=%s", (username, self.hash(password)))
        return r

    def get_user(self, uid):
        return self._one("SELECT id,username,email,status,last_seen FROM users WHERE id=%s", (uid,))

    def set_status(self, uid, status):
        self._run("UPDATE users SET status=%s,last_seen=%s WHERE id=%s", (status, int(time.time()), uid))

    # ── Сессии ──────────────────────────────────────
    def new_session(self, uid) -> str:
        token = hashlib.sha256(f"{uid}{time.time()}{SECRET_KEY}".encode()).hexdigest()
        now = int(time.time())
        self._run("INSERT INTO sessions (token,user_id,created_at,expires_at) VALUES (%s,%s,%s,%s)",
                  (token, uid, now, now + 86400 * 30))
        return token

    def check_session(self, token) -> Optional[int]:
        r = self._one("SELECT user_id FROM sessions WHERE token=%s AND expires_at>%s", (token, int(time.time())))
        return r['user_id'] if r else None

    def del_session(self, token):
        self._run("DELETE FROM sessions WHERE token=%s", (token,))

    # ── Чаты ────────────────────────────────────────
def user_chats(self, uid):
        return self._all("""
            SELECT
                c.id,
                c.name,
                c.type,
                (
                    SELECT text FROM messages
                    WHERE chat_id = c.id AND deleted = FALSE
                    ORDER BY created_at DESC LIMIT 1
                ) AS last_msg,
                (
                    SELECT created_at FROM messages
                    WHERE chat_id = c.id AND deleted = FALSE
                    ORDER BY created_at DESC LIMIT 1
                ) AS last_ts
            FROM chats c
            JOIN chat_members cm ON cm.chat_id = c.id
            WHERE cm.user_id = %s
            ORDER BY (
                SELECT COALESCE(MAX(created_at), 0) FROM messages WHERE chat_id = c.id
            ) DESC
        """, (uid,))
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
            if ex:
                return dict(ex)
            cur.execute("INSERT INTO chats (name,type,created_by,created_at) VALUES (%s,'private',%s,%s) RETURNING id,name",
                        (name2, uid1, now))
            chat = dict(cur.fetchone())
            cur.execute("INSERT INTO chat_members (chat_id,user_id,joined_at) VALUES (%s,%s,%s),(%s,%s,%s)",
                        (chat['id'], uid1, now, chat['id'], uid2, now))
            c.commit()
            return chat
        except Exception as e:
            c.rollback()
            raise e
        finally:
            self._release(c)

    # ── Сообщения ───────────────────────────────────
    def save_msg(self, chat_id, uid, username, text):
        return self._run(
            "INSERT INTO messages (chat_id,user_id,username,text,created_at) VALUES (%s,%s,%s,%s,%s) RETURNING id,chat_id,user_id,username,text,created_at",
            (chat_id, uid, username, text, int(time.time())), ret=True
        )

    def get_msgs(self, chat_id, limit=50, before_id=None):
        if before_id:
            rows = self._all(
                "SELECT id,user_id,username,text,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE AND id<%s ORDER BY created_at DESC LIMIT %s",
                (chat_id, before_id, limit)
            )
        else:
            rows = self._all(
                "SELECT id,user_id,username,text,created_at FROM messages WHERE chat_id=%s AND deleted=FALSE ORDER BY created_at DESC LIMIT %s",
                (chat_id, limit)
            )
        return list(reversed(rows))

    def online_users(self):
        return self._all("SELECT id,username FROM users WHERE status='online' ORDER BY username")


# ════════════════════════════════════════════════════
# WEBSOCKET СЕРВЕР
# ════════════════════════════════════════════════════
if not DATABASE_URL:
    log.error("DATABASE_URL не задан!")
    exit(1)

db = DB(DATABASE_URL)
conns: Dict[int, dict] = {}   # uid -> {ws, username, current_chat}


async def send(ws, data):
    try:
        await ws.send(json.dumps(data, ensure_ascii=False, default=str))
    except Exception:
        pass


async def broadcast(data, chat_id=None, skip=None):
    msg = json.dumps(data, ensure_ascii=False, default=str)
    targets = [
        info["ws"] for uid, info in conns.items()
        if info["ws"] is not skip
        and (chat_id is None or info.get("current_chat") == chat_id)
    ]
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
            except Exception:
                await send(ws, {"type": "error", "message": "Неверный JSON"})
                continue

            t = d.get("type", "")

            # — Без авторизации —
            if t == "ping":
                await send(ws, {"type": "pong"})
                continue

            if t == "register":
                username = d.get("username", "").strip()
                password = d.get("password", "")
                email    = d.get("email", "").strip()
                if len(username) < 3:
                    await send(ws, {"type": "error", "message": "Имя: минимум 3 символа"}); continue
                if len(password) < 6:
                    await send(ws, {"type": "error", "message": "Пароль: минимум 6 символов"}); continue
                user = db.create_user(username, password, email)
                if not user:
                    await send(ws, {"type": "error", "message": "Имя уже занято"}); continue
                token = db.new_session(user["id"])
                log.info(f"Регистрация: {username}")
                await send(ws, {"type": "registered", "user": user, "token": token})
                continue

            if t == "login":
                username = d.get("username", "").strip()
                password = d.get("password", "")
                user = db.verify_user(username, password)
                if not user:
                    await send(ws, {"type": "error", "message": "Неверный логин или пароль"}); continue
                token = db.new_session(user["id"])
                db.set_status(user["id"], "online")
                uid = user["id"]
                conns[uid] = {"ws": ws, "username": user["username"], "current_chat": 1}
                chats = db.user_chats(uid)
                await send(ws, {"type": "logged_in", "user": user, "token": token, "chats": chats})
                await send(ws, {"type": "history", "chat_id": 1, "messages": db.get_msgs(1)})
                await push_online()
                await broadcast({"type": "system", "chat_id": 1, "text": f"👋 {username} вошёл"}, chat_id=1, skip=ws)
                log.info(f"Вход: {username}")
                continue

            if t == "session":
                token = d.get("token", "")
                user_id = db.check_session(token)
                if not user_id:
                    await send(ws, {"type": "error", "message": "Сессия истекла"}); continue
                user = db.get_user(user_id)
                if not user:
                    await send(ws, {"type": "error", "message": "Пользователь не найден"}); continue
                db.set_status(user_id, "online")
                uid = user_id
                conns[uid] = {"ws": ws, "username": user["username"], "current_chat": 1}
                chats = db.user_chats(uid)
                await send(ws, {"type": "session_ok", "user": user, "chats": chats})
                await send(ws, {"type": "history", "chat_id": 1, "messages": db.get_msgs(1)})
                await push_online()
                continue

            # — Только для авторизованных —
            if not uid or uid not in conns:
                await send(ws, {"type": "error", "message": "Необходима авторизация"})
                continue

            if t == "send_message":
                chat_id = d.get("chat_id", 1)
                text = d.get("text", "").strip()
                if not text or len(text) > 4096:
                    await send(ws, {"type": "error", "message": "Пустое или слишком длинное сообщение"}); continue
                msg = db.save_msg(chat_id, uid, conns[uid]["username"], text)
                await broadcast({"type": "message", "message": msg}, chat_id=chat_id)

            elif t == "get_history":
                chat_id  = d.get("chat_id", 1)
                before   = d.get("before_id")
                msgs     = db.get_msgs(chat_id, before_id=before)
                await send(ws, {"type": "history", "chat_id": chat_id, "messages": msgs})

            elif t == "switch_chat":
                chat_id = d.get("chat_id", 1)
                conns[uid]["current_chat"] = chat_id
                await send(ws, {"type": "history", "chat_id": chat_id, "messages": db.get_msgs(chat_id)})

            elif t == "typing":
                chat_id = d.get("chat_id", 1)
                await broadcast({"type": "typing", "username": conns[uid]["username"], "chat_id": chat_id},
                                chat_id=chat_id, skip=ws)

            elif t == "start_private":
                target_name = d.get("username", "").strip()
                target = db._one("SELECT id FROM users WHERE username=%s", (target_name,))
                if not target:
                    await send(ws, {"type": "error", "message": "Пользователь не найден"}); continue
                chat = db.get_or_create_private(uid, target['id'], target_name)
                msgs = db.get_msgs(chat['id'])
                await send(ws, {"type": "private_chat_created",
                                "chat": {"id": chat["id"], "name": target_name, "type": "private"},
                                "messages": msgs})
                if target['id'] in conns:
                    my_name = conns[uid]["username"]
                    await send(conns[target['id']]["ws"], {
                        "type": "new_private_chat",
                        "chat": {"id": chat["id"], "name": my_name, "type": "private"}
                    })

            elif t == "logout":
                token = d.get("token", "")
                db.del_session(token)
                db.set_status(uid, "offline")
                uname = conns.pop(uid, {}).get("username", "")
                uid = None
                await broadcast({"type": "system", "chat_id": 1, "text": f"🔴 {uname} вышел"}, chat_id=1)
                await push_online()
                await send(ws, {"type": "logged_out"})

            elif t == "get_online":
                await send(ws, {"type": "online_users", "users": db.online_users()})

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Ошибка: {e}", exc_info=True)
    finally:
        if uid and uid in conns:
            uname = conns.pop(uid, {}).get("username", "")
            db.set_status(uid, "offline")
            await broadcast({"type": "system", "chat_id": 1, "text": f"🔴 {uname} отключился"}, chat_id=1)
            await push_online()
        log.info(f"- {ws.remote_address}")


async def main():
    log.info(f"🚀 Сервер запускается на {HOST}:{PORT}")
    async with websockets.serve(handler, HOST, PORT, ping_interval=30, ping_timeout=10):
        log.info("✅ Готов!")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
