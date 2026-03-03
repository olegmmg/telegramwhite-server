#!/usr/bin/env python3
"""
TelegramWhite WebSocket Server - Исправленная версия
"""

import os
import sys
import json
import asyncio
import hashlib
import time
import logging
from typing import Dict, Optional

import websockets
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("tgwhite")

PORT = int(os.environ.get("PORT", 10000))
HOST = "0.0.0.0"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SECRET_KEY = os.environ.get("TW_SECRET", "telegramwhite-fixed-key-2024")

MAX_MESSAGE_LENGTH = 4096
PING_INTERVAL = 25
PING_TIMEOUT = 10
SESSION_TTL = 30 * 24 * 60 * 60  # 30 дней
MAX_HISTORY = 100


class Database:
    def __init__(self, database_url):
        if not database_url:
            log.error("❌ DATABASE_URL не задан!")
            sys.exit(1)
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        self.database_url = database_url
        self.pool = None
        self.init_pool()
        self.init_tables()

    def init_pool(self):
        try:
            self.pool = SimpleConnectionPool(1, 20, self.database_url, sslmode='require')
            log.info("✅ Пул соединений с БД создан")
        except Exception as e:
            log.error(f"❌ Ошибка создания пула: {e}")
            sys.exit(1)

    def get_conn(self):
        if not self.pool:
            self.init_pool()
        return self.pool.getconn()

    def put_conn(self, conn):
        if self.pool and conn:
            self.pool.putconn(conn)

    def execute(self, query: str, params: tuple = (), fetch_one: bool = False, fetch_all: bool = False):
        conn = None
        try:
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, params)
            if fetch_one:
                result = cur.fetchone()
                conn.commit()
                return dict(result) if result else None
            elif fetch_all:
                results = cur.fetchall()
                conn.commit()
                return [dict(r) for r in results]
            else:
                conn.commit()
                return None
        except Exception as e:
            if conn:
                conn.rollback()
            log.error(f"❌ Ошибка БД: {e}")
            raise
        finally:
            if conn:
                self.put_conn(conn)

    def init_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL PRIMARY KEY,
                username        TEXT UNIQUE NOT NULL,
                email           TEXT DEFAULT '',
                password        TEXT NOT NULL,
                bio             TEXT DEFAULT '',
                avatar_url      TEXT DEFAULT '',
                phone           TEXT DEFAULT '',
                status          TEXT DEFAULT 'offline',
                last_seen       BIGINT NOT NULL,
                created_at      BIGINT NOT NULL,
                messages_count  INTEGER DEFAULT 0,
                friends_count   INTEGER DEFAULT 0
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS chats (
                id              SERIAL PRIMARY KEY,
                name            TEXT NOT NULL,
                type            TEXT NOT NULL DEFAULT 'group',
                description     TEXT DEFAULT '',
                avatar_url      TEXT DEFAULT '',
                created_by      INTEGER REFERENCES users(id),
                created_at      BIGINT NOT NULL,
                messages_count  INTEGER DEFAULT 0
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS chat_members (
                chat_id     INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                role        TEXT NOT NULL DEFAULT 'member',
                joined_at   BIGINT NOT NULL,
                last_read   BIGINT DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS messages (
                id          SERIAL PRIMARY KEY,
                chat_id     INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                user_id     INTEGER REFERENCES users(id),
                username    TEXT NOT NULL,
                text        TEXT NOT NULL,
                type        TEXT DEFAULT 'text',
                created_at  BIGINT NOT NULL,
                edited_at   BIGINT,
                reply_to    INTEGER,
                deleted     BOOLEAN DEFAULT FALSE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token       TEXT PRIMARY KEY,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                created_at  BIGINT NOT NULL,
                expires_at  BIGINT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS calls (
                id          SERIAL PRIMARY KEY,
                chat_id     INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                initiator   INTEGER REFERENCES users(id),
                type        TEXT NOT NULL DEFAULT 'audio',
                status      TEXT NOT NULL DEFAULT 'ringing',
                started_at  BIGINT NOT NULL,
                ended_at    BIGINT,
                duration    INTEGER DEFAULT 0
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS reactions (
                message_id  INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
                emoji       TEXT NOT NULL,
                created_at  BIGINT NOT NULL,
                PRIMARY KEY (message_id, user_id)
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id);",
            "CREATE INDEX IF NOT EXISTS idx_messages_id ON messages(id);",
            "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);",
            "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);",
        ]
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            for query in queries:
                cur.execute(query)
            cur.execute("SELECT id FROM chats WHERE id = 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO chats (id, name, type, description, created_at) VALUES (1, 'Общий чат', 'group', 'Добро пожаловать!', %s)",
                    (int(time.time()),)
                )
                log.info("✅ Создан общий чат")
            conn.commit()
            log.info("✅ Таблицы созданы/проверены")
        except Exception as e:
            log.error(f"❌ Ошибка инициализации БД: {e}")
            raise
        finally:
            if conn:
                self.put_conn(conn)

    def hash_password(self, password: str) -> str:
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            SECRET_KEY.encode('utf-8'),
            100000
        ).hex()

    def create_user(self, username: str, password: str, email: str = "") -> Optional[dict]:
        now = int(time.time())
        hashed = self.hash_password(password)
        try:
            existing = self.execute("SELECT id FROM users WHERE username = %s", (username,), fetch_one=True)
            if existing:
                return None
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("""
                    INSERT INTO users (username, email, password, last_seen, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id, username, email, bio, avatar_url, phone, created_at
                """, (username, email, hashed, now, now))
                user = dict(cur.fetchone())
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                    VALUES (1, %s, 'member', %s)
                    ON CONFLICT (chat_id, user_id) DO NOTHING
                """, (user['id'], now))
                conn.commit()
                log.info(f"✅ Новый пользователь: {username} (ID: {user['id']})")
                return user
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                self.put_conn(conn)
        except Exception as e:
            log.error(f"❌ Ошибка создания пользователя: {e}")
            return None

    def verify_user(self, username: str, password: str) -> Optional[dict]:
        hashed = self.hash_password(password)
        user = self.execute("""
            SELECT id, username, email, bio, avatar_url, phone, created_at,
                   messages_count, friends_count
            FROM users WHERE username = %s AND password = %s
        """, (username, hashed), fetch_one=True)
        if user:
            log.info(f"✅ Успешная верификация: {username}")
        else:
            log.warning(f"❌ Неудачная верификация: {username}")
        return user

    def get_user(self, user_id: int) -> Optional[dict]:
        return self.execute("""
            SELECT id, username, email, bio, avatar_url, phone, status,
                   last_seen, created_at, messages_count, friends_count
            FROM users WHERE id = %s
        """, (user_id,), fetch_one=True)

    def get_user_by_username(self, username: str) -> Optional[dict]:
        return self.execute("""
            SELECT id, username, bio, avatar_url, status, last_seen
            FROM users WHERE username = %s
        """, (username,), fetch_one=True)

    def update_profile(self, user_id: int, **kwargs) -> bool:
        allowed = ['bio', 'email', 'phone', 'avatar_url']
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if not updates:
            return False
        set_clause = ', '.join([f"{k} = %s" for k in updates.keys()])
        query = f"UPDATE users SET {set_clause} WHERE id = %s"
        try:
            self.execute(query, tuple(updates.values()) + (user_id,))
            return True
        except Exception as e:
            log.error(f"❌ Ошибка обновления профиля: {e}")
            return False

    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        user = self.execute("SELECT password FROM users WHERE id = %s", (user_id,), fetch_one=True)
        if not user or user['password'] != self.hash_password(old_password):
            return False
        self.execute("UPDATE users SET password = %s WHERE id = %s", (self.hash_password(new_password), user_id))
        return True

    def set_status(self, user_id: int, status: str):
        self.execute(
            "UPDATE users SET status = %s, last_seen = %s WHERE id = %s",
            (status, int(time.time()), user_id)
        )

    def create_session(self, user_id: int) -> str:
        token = hashlib.sha256(f"{user_id}{time.time()}{SECRET_KEY}".encode()).hexdigest()
        now = int(time.time())
        self.execute("""
            INSERT INTO sessions (token, user_id, created_at, expires_at)
            VALUES (%s, %s, %s, %s)
        """, (token, user_id, now, now + SESSION_TTL))
        return token

    def check_session(self, token: str) -> Optional[int]:
        result = self.execute("""
            SELECT user_id FROM sessions
            WHERE token = %s AND expires_at > %s
        """, (token, int(time.time())), fetch_one=True)
        return result['user_id'] if result else None

    def delete_session(self, token: str):
        self.execute("DELETE FROM sessions WHERE token = %s", (token,))

    def get_user_chats(self, user_id: int) -> list:
        return self.execute("""
            SELECT
                c.id, c.name, c.type, c.description, c.avatar_url,
                (
                    SELECT row_to_json(msg)
                    FROM (
                        SELECT m.text, m.username, m.created_at
                        FROM messages m
                        WHERE m.chat_id = c.id AND m.deleted = FALSE
                        ORDER BY m.id DESC
                        LIMIT 1
                    ) msg
                ) as last_message
            FROM chats c
            JOIN chat_members cm ON cm.chat_id = c.id
            WHERE cm.user_id = %s
            ORDER BY (
                SELECT COALESCE(MAX(id), 0) FROM messages WHERE chat_id = c.id
            ) DESC
        """, (user_id,), fetch_all=True) or []

    def get_messages(self, chat_id: int, limit: int = MAX_HISTORY, before_id: int = None) -> list:
        """Получение сообщений чата — исправленная версия"""
        try:
            if before_id:
                msgs = self.execute("""
                    SELECT id, chat_id, user_id, username, text, created_at
                    FROM messages
                    WHERE chat_id = %s AND deleted = FALSE AND id < %s
                    ORDER BY id ASC
                    LIMIT %s
                """, (chat_id, before_id, limit), fetch_all=True)
            else:
                # Берём последние N по id DESC, потом переворачиваем для правильного порядка
                msgs = self.execute("""
                    SELECT id, chat_id, user_id, username, text, created_at
                    FROM (
                        SELECT id, chat_id, user_id, username, text, created_at
                        FROM messages
                        WHERE chat_id = %s AND deleted = FALSE
                        ORDER BY id DESC
                        LIMIT %s
                    ) sub
                    ORDER BY id ASC
                """, (chat_id, limit), fetch_all=True)

            count = len(msgs) if msgs else 0
            log.info(f"📥 Загружено {count} сообщений из чата {chat_id}")

            # Диагностика если пусто
            if count == 0:
                try:
                    total = self.execute(
                        "SELECT COUNT(*) as cnt FROM messages WHERE chat_id = %s",
                        (chat_id,), fetch_one=True
                    )
                    deleted_count = self.execute(
                        "SELECT COUNT(*) as cnt FROM messages WHERE chat_id = %s AND deleted = TRUE",
                        (chat_id,), fetch_one=True
                    )
                    log.warning(
                        f"⚠️ Чат {chat_id}: всего={total['cnt'] if total else '?'}, "
                        f"удалённых={deleted_count['cnt'] if deleted_count else '?'}"
                    )
                except Exception as diag_e:
                    log.error(f"❌ Ошибка диагностики: {diag_e}")

            return msgs if msgs else []

        except Exception as e:
            log.error(f"❌ Ошибка получения сообщений: {e}")
            return []

    def save_message(self, chat_id: int, user_id: int, username: str, text: str) -> Optional[dict]:
        """Сохранение сообщения — явная транзакция"""
        try:
            now = int(time.time())
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("""
                    INSERT INTO messages (chat_id, user_id, username, text, created_at, deleted)
                    VALUES (%s, %s, %s, %s, %s, FALSE)
                    RETURNING id, chat_id, user_id, username, text, created_at
                """, (chat_id, user_id, username, text, now))
                msg = dict(cur.fetchone())
                cur.execute(
                    "UPDATE users SET messages_count = messages_count + 1 WHERE id = %s",
                    (user_id,)
                )
                cur.execute(
                    "UPDATE chats SET messages_count = messages_count + 1 WHERE id = %s",
                    (chat_id,)
                )
                conn.commit()
                log.info(f"💾 Сообщение сохранено в чат {chat_id} (ID: {msg['id']})")
                return msg
            except Exception as e:
                conn.rollback()
                log.error(f"❌ Ошибка транзакции save_message: {e}")
                raise
            finally:
                self.put_conn(conn)
        except Exception as e:
            log.error(f"❌ Ошибка сохранения сообщения: {e}")
            return None

    def add_reaction(self, message_id: int, user_id: int, emoji: str):
        try:
            self.execute("""
                INSERT INTO reactions (message_id, user_id, emoji, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id, user_id) DO UPDATE SET emoji = %s
            """, (message_id, user_id, emoji, int(time.time()), emoji))
        except Exception as e:
            log.error(f"❌ Ошибка добавления реакции: {e}")

    def get_reactions(self, message_id: int) -> list:
        try:
            return self.execute("""
                SELECT emoji, COUNT(*) as count FROM reactions
                WHERE message_id = %s GROUP BY emoji
            """, (message_id,), fetch_all=True) or []
        except Exception as e:
            log.error(f"❌ Ошибка получения реакций: {e}")
            return []

    def get_online_users(self) -> list:
        try:
            return self.execute("""
                SELECT id, username, avatar_url, status FROM users
                WHERE status = 'online' ORDER BY username
            """, fetch_all=True) or []
        except Exception as e:
            log.error(f"❌ Ошибка получения онлайн пользователей: {e}")
            return []

    def get_or_create_private(self, user1_id: int, user2_id: int, user2_name: str) -> Optional[dict]:
        now = int(time.time())
        try:
            existing = self.execute("""
                SELECT c.id, c.name, c.type
                FROM chats c
                JOIN chat_members m1 ON m1.chat_id = c.id
                JOIN chat_members m2 ON m2.chat_id = c.id
                WHERE c.type = 'private' AND m1.user_id = %s AND m2.user_id = %s
                LIMIT 1
            """, (user1_id, user2_id), fetch_one=True)
            if existing:
                log.info(f"✅ Найден существующий личный чат {existing['id']}")
                return existing
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("""
                    INSERT INTO chats (name, type, created_by, created_at)
                    VALUES (%s, 'private', %s, %s)
                    RETURNING id, name, type
                """, (user2_name, user1_id, now))
                chat = dict(cur.fetchone())
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                    VALUES (%s, %s, 'member', %s), (%s, %s, 'member', %s)
                """, (chat['id'], user1_id, now, chat['id'], user2_id, now))
                conn.commit()
                log.info(f"✅ Создан личный чат {chat['id']} между {user1_id} и {user2_id}")
                return chat
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                self.put_conn(conn)
        except Exception as e:
            log.error(f"❌ Ошибка создания личного чата: {e}")
            return None

    def create_group(self, name: str, description: str, creator_id: int, members: list) -> Optional[dict]:
        now = int(time.time())
        try:
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("""
                    INSERT INTO chats (name, type, description, created_by, created_at)
                    VALUES (%s, 'group', %s, %s, %s)
                    RETURNING id, name, type, description, avatar_url, created_at
                """, (name, description, creator_id, now))
                chat = dict(cur.fetchone())
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                    VALUES (%s, %s, 'owner', %s)
                """, (chat['id'], creator_id, now))
                for member_id in members:
                    if member_id != creator_id:
                        cur.execute("""
                            INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                            VALUES (%s, %s, 'member', %s)
                            ON CONFLICT DO NOTHING
                        """, (chat['id'], member_id, now))
                conn.commit()
                log.info(f"✅ Группа создана: {name} (ID: {chat['id']})")
                return chat
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                self.put_conn(conn)
        except Exception as e:
            log.error(f"❌ Ошибка создания группы: {e}")
            return None

    def get_chat_info(self, chat_id: int) -> Optional[dict]:
        return self.execute("""
            SELECT id, name, type, description, avatar_url, created_by, created_at
            FROM chats WHERE id = %s
        """, (chat_id,), fetch_one=True)

    def get_chat_members(self, chat_id: int) -> list:
        return self.execute("""
            SELECT u.id, u.username, u.avatar_url, u.status, cm.role, cm.joined_at
            FROM users u
            JOIN chat_members cm ON cm.user_id = u.id
            WHERE cm.chat_id = %s
            ORDER BY CASE cm.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 ELSE 3 END, u.username
        """, (chat_id,), fetch_all=True) or []

    def add_member(self, chat_id: int, user_id: int):
        try:
            self.execute("""
                INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                VALUES (%s, %s, 'member', %s)
                ON CONFLICT DO NOTHING
            """, (chat_id, user_id, int(time.time())))
        except Exception as e:
            log.error(f"❌ Ошибка добавления участника: {e}")

    def remove_member(self, chat_id: int, user_id: int):
        try:
            self.execute("DELETE FROM chat_members WHERE chat_id = %s AND user_id = %s", (chat_id, user_id))
        except Exception as e:
            log.error(f"❌ Ошибка удаления участника: {e}")

    def create_call(self, chat_id: int, initiator_id: int, call_type: str = 'audio') -> Optional[dict]:
        try:
            return self.execute("""
                INSERT INTO calls (chat_id, initiator, type, status, started_at)
                VALUES (%s, %s, %s, 'ringing', %s)
                RETURNING id, chat_id, type, status, started_at
            """, (chat_id, initiator_id, call_type, int(time.time())), fetch_one=True)
        except Exception as e:
            log.error(f"❌ Ошибка создания звонка: {e}")
            return None

    def end_call(self, call_id: int):
        try:
            now = int(time.time())
            call = self.execute("SELECT started_at FROM calls WHERE id = %s", (call_id,), fetch_one=True)
            if call:
                duration = now - call['started_at']
                self.execute("""
                    UPDATE calls SET status = 'ended', ended_at = %s, duration = %s WHERE id = %s
                """, (now, duration, call_id))
        except Exception as e:
            log.error(f"❌ Ошибка завершения звонка: {e}")


class Connection:
    def __init__(self, ws, user_id: int, username: str):
        self.ws = ws
        self.user_id = user_id
        self.username = username
        self.current_chat = 1
        self.last_ping = time.time()


class TelegramWhiteServer:
    def __init__(self):
        log.info("=" * 50)
        log.info("🚀 TelegramWhite Server")
        log.info("=" * 50)
        log.info(f"Порт: {PORT}")
        self.db = Database(DATABASE_URL)
        self.connections: Dict[int, Connection] = {}
        self.active_calls: Dict[int, dict] = {}
        log.info("✅ Сервер инициализирован")

    async def send(self, ws, data: dict):
        try:
            await ws.send(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            log.debug(f"Ошибка отправки: {e}")

    async def broadcast(self, data: dict, chat_id: int = None, skip_user: int = None):
        message = json.dumps(data, ensure_ascii=False)
        targets = []
        if chat_id:
            members = self.db.get_chat_members(chat_id)
            member_ids = {m['id'] for m in members} if members else set()
            targets = [
                conn.ws for uid, conn in self.connections.items()
                if uid in member_ids and uid != skip_user
            ]
        else:
            targets = [
                conn.ws for uid, conn in self.connections.items()
                if uid != skip_user
            ]
        if targets:
            await asyncio.gather(*[t.send(message) for t in targets], return_exceptions=True)

    async def broadcast_online(self):
        await self.broadcast({'type': 'online_users', 'users': self.db.get_online_users()})

    async def handle_message(self, ws, data: dict):
        msg_type = data.get('type')
        if not msg_type:
            return

        log.info(f"📨 Получено сообщение типа: {msg_type}")

        # Ping
        if msg_type == 'ping':
            for conn in self.connections.values():
                if conn.ws == ws:
                    conn.last_ping = time.time()
                    break
            await self.send(ws, {'type': 'pong'})
            return

        # Регистрация
        if msg_type == 'register':
            username = data.get('username', '').strip()
            password = data.get('password', '')
            email = data.get('email', '').strip()
            if len(username) < 3:
                await self.send(ws, {'type': 'error', 'message': 'Имя должно содержать минимум 3 символа'})
                return
            if len(password) < 6:
                await self.send(ws, {'type': 'error', 'message': 'Пароль должен содержать минимум 6 символов'})
                return
            user = self.db.create_user(username, password, email)
            if not user:
                await self.send(ws, {'type': 'error', 'message': 'Имя пользователя уже занято'})
                return
            token = self.db.create_session(user['id'])
            await self.send(ws, {'type': 'registered', 'user': user, 'token': token})
            log.info(f"✅ Регистрация успешна: {username}")
            return

        # Вход
        if msg_type == 'login':
            username = data.get('username', '').strip()
            password = data.get('password', '')
            log.info(f"🔑 Вход: {username}")
            user = self.db.verify_user(username, password)
            if not user:
                await self.send(ws, {'type': 'error', 'message': 'Неверный логин или пароль'})
                return
            self.db.set_status(user['id'], 'online')
            token = self.db.create_session(user['id'])
            self.connections[user['id']] = Connection(ws, user['id'], username)
            chats = self.db.get_user_chats(user['id'])
            log.info(f"📋 Найдено чатов: {len(chats)}")
            await self.send(ws, {'type': 'logged_in', 'user': user, 'token': token, 'chats': chats})
            for chat in chats:
                messages = self.db.get_messages(chat['id'])
                await self.send(ws, {'type': 'history', 'chat_id': chat['id'], 'messages': messages})
            await self.broadcast_online()
            await self.broadcast({'type': 'system', 'chat_id': 1, 'text': f'👋 {username} присоединился'}, chat_id=1, skip_user=user['id'])
            log.info(f"✅ Успешный вход: {username}")
            return

        # Восстановление сессии
        if msg_type == 'session':
            token = data.get('token', '')
            user_id = self.db.check_session(token)
            if not user_id:
                await self.send(ws, {'type': 'error', 'message': 'Сессия истекла'})
                return
            user = self.db.get_user(user_id)
            if not user:
                await self.send(ws, {'type': 'error', 'message': 'Пользователь не найден'})
                return
            self.db.set_status(user_id, 'online')
            self.connections[user_id] = Connection(ws, user_id, user['username'])
            chats = self.db.get_user_chats(user_id)
            log.info(f"📋 Найдено чатов: {len(chats)}")
            await self.send(ws, {'type': 'session_ok', 'user': user, 'chats': chats})
            for chat in chats:
                messages = self.db.get_messages(chat['id'])
                await self.send(ws, {'type': 'history', 'chat_id': chat['id'], 'messages': messages})
            await self.broadcast_online()
            log.info(f"✅ Восстановлена сессия: {user['username']}")
            return

        # Проверка авторизации
        user_id = None
        for uid, conn in self.connections.items():
            if conn.ws == ws:
                user_id = uid
                break
        if not user_id:
            await self.send(ws, {'type': 'error', 'message': 'Требуется авторизация'})
            return

        conn = self.connections[user_id]

        # Отправка сообщения
        if msg_type == 'send_message':
            chat_id = data.get('chat_id', 1)
            text = data.get('text', '').strip()
            if not text:
                return
            if len(text) > MAX_MESSAGE_LENGTH:
                await self.send(ws, {'type': 'error', 'message': 'Сообщение слишком длинное'})
                return
            msg = self.db.save_message(chat_id, user_id, conn.username, text)
            if msg:
                await self.broadcast({'type': 'message', 'message': msg}, chat_id=chat_id)
                log.info(f"💬 Сообщение от {conn.username} в чат {chat_id}")
            return

        # История
        if msg_type == 'get_history':
            chat_id = data.get('chat_id', 1)
            before_id = data.get('before_id')
            messages = self.db.get_messages(chat_id, before_id=before_id)
            await self.send(ws, {'type': 'history', 'chat_id': chat_id, 'messages': messages})
            log.info(f"📜 Отправлена история чата {chat_id} ({len(messages)} сообщений)")
            return

        # Смена чата
        if msg_type == 'switch_chat':
            conn.current_chat = data.get('chat_id', 1)
            return

        # Печатает
        if msg_type == 'typing':
            chat_id = data.get('chat_id', 1)
            await self.broadcast({'type': 'typing', 'username': conn.username, 'chat_id': chat_id}, chat_id=chat_id, skip_user=user_id)
            return

        # Реакции
        if msg_type == 'react':
            message_id = data.get('message_id')
            emoji = data.get('emoji')
            chat_id = data.get('chat_id', 1)
            if message_id and emoji:
                self.db.add_reaction(message_id, user_id, emoji)
                reactions = self.db.get_reactions(message_id)
                await self.broadcast({'type': 'reactions_updated', 'message_id': message_id, 'reactions': reactions}, chat_id=chat_id)
            return

        # Профиль
        if msg_type == 'get_profile':
            target = data.get('username')
            profile = self.db.get_user_by_username(target) if target else self.db.get_user(user_id)
            if not profile:
                await self.send(ws, {'type': 'error', 'message': 'Пользователь не найден'})
                return
            await self.send(ws, {'type': 'profile', 'user': profile})
            return

        # Обновление профиля
        if msg_type == 'update_profile':
            self.db.update_profile(user_id, bio=data.get('bio'), email=data.get('email'), phone=data.get('phone'), avatar_url=data.get('avatar'))
            user = self.db.get_user(user_id)
            await self.send(ws, {'type': 'profile_updated', 'user': user})
            log.info(f"👤 Профиль обновлен: {conn.username}")
            return

        # Смена пароля
        if msg_type == 'change_password':
            old_pass = data.get('old_password', '')
            new_pass = data.get('new_password', '')
            if len(new_pass) < 6:
                await self.send(ws, {'type': 'error', 'message': 'Минимальная длина пароля 6 символов'})
                return
            if self.db.change_password(user_id, old_pass, new_pass):
                await self.send(ws, {'type': 'password_changed'})
            else:
                await self.send(ws, {'type': 'error', 'message': 'Неверный текущий пароль'})
            return

        # Создание группы
        if msg_type == 'create_group':
            name = data.get('name', '').strip()
            description = data.get('description', '').strip()
            members = data.get('members', [])
            if not name:
                await self.send(ws, {'type': 'error', 'message': 'Введите название группы'})
                return
            member_ids = [user_id]
            for uname in members:
                u = self.db.get_user_by_username(uname)
                if u:
                    member_ids.append(u['id'])
            chat = self.db.create_group(name, description, user_id, member_ids)
            if not chat:
                await self.send(ws, {'type': 'error', 'message': 'Ошибка создания группы'})
                return
            members_info = self.db.get_chat_members(chat['id'])
            for member in members_info:
                if member['id'] in self.connections and member['id'] != user_id:
                    await self.send(self.connections[member['id']].ws, {
                        'type': 'new_group_chat',
                        'chat': {**chat, 'type': 'group'},
                        'messages': []
                    })
            await self.send(ws, {'type': 'group_created', 'chat': {**chat, 'type': 'group'}, 'members': members_info})
            log.info(f"👥 Группа создана: {name}")
            return

        # Информация о чате
        if msg_type == 'get_chat_info':
            chat_id = data.get('chat_id')
            chat = self.db.get_chat_info(chat_id)
            members = self.db.get_chat_members(chat_id)
            await self.send(ws, {'type': 'chat_info', 'chat': chat, 'members': members})
            return

        # Добавление участника
        if msg_type == 'add_member':
            chat_id = data.get('chat_id')
            username = data.get('username', '').strip()
            target = self.db.get_user_by_username(username)
            if not target:
                await self.send(ws, {'type': 'error', 'message': 'Пользователь не найден'})
                return
            self.db.add_member(chat_id, target['id'])
            if target['id'] in self.connections:
                chat = self.db.get_chat_info(chat_id)
                messages = self.db.get_messages(chat_id)
                await self.send(self.connections[target['id']].ws, {
                    'type': 'added_to_chat',
                    'chat': {**chat, 'type': 'group'},
                    'messages': messages
                })
            await self.broadcast({'type': 'system', 'chat_id': chat_id, 'text': f'➕ {username} присоединился к группе'}, chat_id=chat_id)
            log.info(f"➕ {username} добавлен в чат {chat_id}")
            return

        # Выход из группы
        if msg_type == 'leave_chat':
            chat_id = data.get('chat_id')
            self.db.remove_member(chat_id, user_id)
            await self.broadcast({'type': 'system', 'chat_id': chat_id, 'text': f'👋 {conn.username} покинул группу'}, chat_id=chat_id)
            await self.send(ws, {'type': 'left_chat', 'chat_id': chat_id})
            log.info(f"👋 {conn.username} покинул чат {chat_id}")
            return

        # Личный чат
        if msg_type == 'start_private':
            target_name = data.get('username', '').strip()
            target = self.db.get_user_by_username(target_name)
            if not target:
                await self.send(ws, {'type': 'error', 'message': 'Пользователь не найден'})
                return
            try:
                chat = self.db.get_or_create_private(user_id, target['id'], target_name)
                if chat:
                    messages = self.db.get_messages(chat['id'])
                    await self.send(ws, {
                        'type': 'private_chat_created',
                        'chat': {'id': chat['id'], 'name': target_name, 'type': 'private'},
                        'messages': messages
                    })
                    if target['id'] in self.connections:
                        await self.send(self.connections[target['id']].ws, {
                            'type': 'new_private_chat',
                            'chat': {'id': chat['id'], 'name': conn.username, 'type': 'private'},
                            'messages': messages
                        })
                    log.info(f"💬 Личный чат: {conn.username} — {target_name}")
                else:
                    await self.send(ws, {'type': 'error', 'message': 'Ошибка создания чата'})
            except Exception as e:
                log.error(f"❌ Ошибка создания личного чата: {e}")
                await self.send(ws, {'type': 'error', 'message': 'Ошибка создания чата'})
            return

        # Звонки
        if msg_type == 'call_start':
            chat_id = data.get('chat_id')
            call_type = data.get('call_type', 'audio')
            call = self.db.create_call(chat_id, user_id, call_type)
            if not call:
                await self.send(ws, {'type': 'error', 'message': 'Не удалось начать звонок'})
                return
            self.active_calls[call['id']] = {'chat_id': chat_id, 'type': call_type, 'initiator': user_id, 'participants': {user_id}}
            await self.broadcast({'type': 'incoming_call', 'call_id': call['id'], 'chat_id': chat_id, 'call_type': call_type, 'from': conn.username, 'from_id': user_id}, chat_id=chat_id, skip_user=user_id)
            await self.send(ws, {'type': 'call_started', 'call_id': call['id'], 'call_type': call_type})
            return

        if msg_type == 'call_accept':
            call_id = data.get('call_id')
            if call_id not in self.active_calls:
                return
            call = self.active_calls[call_id]
            call['participants'].add(user_id)
            initiator_id = call['initiator']
            if initiator_id in self.connections:
                await self.send(self.connections[initiator_id].ws, {'type': 'call_accepted', 'call_id': call_id, 'by': conn.username, 'by_id': user_id})
            await self.send(ws, {'type': 'call_joined', 'call_id': call_id})
            return

        if msg_type == 'call_decline':
            call_id = data.get('call_id')
            if call_id not in self.active_calls:
                return
            call = self.active_calls[call_id]
            if call['initiator'] in self.connections:
                await self.send(self.connections[call['initiator']].ws, {'type': 'call_declined', 'call_id': call_id, 'by': conn.username})
            del self.active_calls[call_id]
            self.db.end_call(call_id)
            return

        if msg_type == 'call_end':
            call_id = data.get('call_id')
            if call_id in self.active_calls:
                call = self.active_calls[call_id]
                self.db.end_call(call_id)
                await self.broadcast({'type': 'call_ended', 'call_id': call_id}, chat_id=call['chat_id'])
                del self.active_calls[call_id]
            return

        # WebRTC
        if msg_type == 'webrtc_offer':
            target_id = data.get('target_id')
            if target_id in self.connections:
                await self.send(self.connections[target_id].ws, {'type': 'webrtc_offer', 'call_id': data.get('call_id'), 'offer': data.get('offer'), 'from_id': user_id, 'from_name': conn.username})
            return

        if msg_type == 'webrtc_answer':
            target_id = data.get('target_id')
            if target_id in self.connections:
                await self.send(self.connections[target_id].ws, {'type': 'webrtc_answer', 'call_id': data.get('call_id'), 'answer': data.get('answer'), 'from_id': user_id})
            return

        if msg_type == 'webrtc_ice':
            target_id = data.get('target_id')
            if target_id in self.connections:
                await self.send(self.connections[target_id].ws, {'type': 'webrtc_ice', 'call_id': data.get('call_id'), 'candidate': data.get('candidate'), 'from_id': user_id})
            return

        # Выход
        if msg_type == 'logout':
            token = data.get('token', '')
            if token:
                self.db.delete_session(token)
            if user_id in self.connections:
                username = self.connections[user_id].username
                for call_id, call in list(self.active_calls.items()):
                    if user_id in call['participants']:
                        self.db.end_call(call_id)
                        await self.broadcast({'type': 'call_ended', 'call_id': call_id}, chat_id=call['chat_id'])
                        del self.active_calls[call_id]
                del self.connections[user_id]
                self.db.set_status(user_id, 'offline')
                await self.broadcast_online()
                log.info(f"👋 Выход: {username}")
            await self.send(ws, {'type': 'logged_out'})
            return

        # Онлайн пользователи
        if msg_type == 'get_online':
            await self.send(ws, {'type': 'online_users', 'users': self.db.get_online_users()})
            return

    async def handler(self, ws, path):
        client_info = f"{ws.remote_address[0]}:{ws.remote_address[1]}"
        log.info(f"➕ Новое подключение: {client_info}")
        try:
            async for message in ws:
                try:
                    data = json.loads(message)
                    await self.handle_message(ws, data)
                except json.JSONDecodeError:
                    await self.send(ws, {'type': 'error', 'message': 'Неверный JSON'})
                except Exception as e:
                    log.error(f"❌ Ошибка обработки: {e}", exc_info=True)
                    await self.send(ws, {'type': 'error', 'message': 'Внутренняя ошибка'})
        except websockets.exceptions.ConnectionClosed:
            log.info(f"📴 Соединение закрыто: {client_info}")
        except Exception as e:
            log.error(f"❌ Ошибка: {e}")
        finally:
            user_id = None
            for uid, conn in list(self.connections.items()):
                if conn.ws == ws:
                    user_id = uid
                    for call_id, call in list(self.active_calls.items()):
                        if user_id in call['participants']:
                            self.db.end_call(call_id)
                            await self.broadcast({'type': 'call_ended', 'call_id': call_id}, chat_id=call['chat_id'])
                            del self.active_calls[call_id]
                    del self.connections[uid]
                    self.db.set_status(uid, 'offline')
                    break
            if user_id:
                log.info(f"➖ Отключение: {self.connections.get(user_id, type('', (), {'username': str(user_id)})()).username if user_id in self.connections else user_id}")
                await self.broadcast_online()
            else:
                log.info(f"➖ Отключение (без авторизации): {client_info}")

    async def ping_checker(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            for user_id, conn in list(self.connections.items()):
                if now - conn.last_ping > PING_TIMEOUT * 3:
                    log.warning(f"⚠️ Таймаут ping для user_id={user_id}")
                    try:
                        await conn.ws.close()
                    except:
                        pass

    async def cleanup_old_sessions(self):
        while True:
            await asyncio.sleep(3600)
            try:
                self.db.execute("DELETE FROM sessions WHERE expires_at < %s", (int(time.time()),))
                log.info("🧹 Очистка старых сессий выполнена")
            except Exception as e:
                log.error(f"❌ Ошибка очистки сессий: {e}")

    async def run(self):
        log.info(f"🚀 Запуск сервера на {HOST}:{PORT}")
        async with websockets.serve(
            self.handler, HOST, PORT,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            max_size=10 * 1024 * 1024
        ):
            log.info(f"✅ Сервер запущен и слушает порт {PORT}")
            asyncio.create_task(self.ping_checker())
            asyncio.create_task(self.cleanup_old_sessions())
            await asyncio.Future()


if __name__ == "__main__":
    if not DATABASE_URL:
        log.error("❌ DATABASE_URL не задан!")
        sys.exit(1)
    server = TelegramWhiteServer()
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        log.info("👋 Сервер остановлен")
    except Exception as e:
        log.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
