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
from typing import Dict, Optional, Set, Any

import websockets
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("tgwhite")

# Конфигурация
PORT = int(os.environ.get("PORT", 10000))
HOST = "0.0.0.0"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SECRET_KEY = os.environ.get("TW_SECRET", "telegramwhite-fixed-key-2024")

# Константы
MAX_MESSAGE_LENGTH = 4096
PING_INTERVAL = 25
PING_TIMEOUT = 10
SESSION_TTL = 30 * 24 * 60 * 60  # 30 дней


class Database:
    """Управление базой данных"""
    
    def __init__(self, database_url):
        if not database_url:
            log.error("❌ DATABASE_URL не задан!")
            sys.exit(1)
        
        # Исправляем URL для PostgreSQL
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        
        self.database_url = database_url
        self.pool = None
        self.init_pool()
        self.init_tables()

    def init_pool(self):
        """Инициализация пула соединений"""
        try:
            self.pool = SimpleConnectionPool(
                1, 20,
                self.database_url,
                sslmode='require'
            )
            log.info("✅ Пул соединений с БД создан")
        except Exception as e:
            log.error(f"❌ Ошибка создания пула: {e}")
            sys.exit(1)

    def get_conn(self):
        """Получение соединения из пула"""
        if not self.pool:
            self.init_pool()
        return self.pool.getconn()

    def put_conn(self, conn):
        """Возврат соединения в пул"""
        if self.pool and conn:
            self.pool.putconn(conn)

    def execute(self, query: str, params: tuple = (), fetch_one: bool = False, fetch_all: bool = False):
        """Универсальный метод выполнения запросов"""
        conn = None
        try:
            conn = self.get_conn()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(query, params)
            
            if fetch_one:
                result = cur.fetchone()
                return dict(result) if result else None
            elif fetch_all:
                results = cur.fetchall()
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
        """Создание всех необходимых таблиц"""
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
            """
        ]

        try:
            conn = self.get_conn()
            cur = conn.cursor()
            
            for query in queries:
                cur.execute(query)
            
            # Создаем общий чат, если его нет
            cur.execute("SELECT id FROM chats WHERE id = 1")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO chats (id, name, type, description, created_at) VALUES (1, 'Общий чат', 'group', 'Добро пожаловать!', %s)",
                    (int(time.time()),)
                )
            
            conn.commit()
            log.info("✅ Таблицы созданы/проверены")
            
        except Exception as e:
            log.error(f"❌ Ошибка инициализации БД: {e}")
            raise
        finally:
            if conn:
                self.put_conn(conn)

    def hash_password(self, password: str) -> str:
        """Хеширование пароля"""
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            SECRET_KEY.encode('utf-8'),
            100000
        ).hex()

    def create_user(self, username: str, password: str, email: str = "") -> Optional[dict]:
        """Создание нового пользователя"""
        now = int(time.time())
        hashed = self.hash_password(password)
        
        try:
            # Проверяем существование
            existing = self.execute(
                "SELECT id FROM users WHERE username = %s",
                (username,),
                fetch_one=True
            )
            if existing:
                return None

            # Создаем пользователя
            user = self.execute("""
                INSERT INTO users (username, email, password, last_seen, created_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, username, email, bio, avatar_url, phone, created_at
            """, (username, email, hashed, now, now), fetch_one=True)

            # Добавляем в общий чат
            self.execute("""
                INSERT INTO chat_members (chat_id, user_id, role, joined_at)
                VALUES (1, %s, 'member', %s)
                ON CONFLICT DO NOTHING
            """, (user['id'], now))

            log.info(f"✅ Новый пользователь: {username}")
            return user

        except Exception as e:
            log.error(f"❌ Ошибка создания пользователя: {e}")
            return None

    def verify_user(self, username: str, password: str) -> Optional[dict]:
        """Проверка логина и пароля"""
        hashed = self.hash_password(password)
        
        return self.execute("""
            SELECT id, username, email, bio, avatar_url, phone, created_at,
                   messages_count, friends_count
            FROM users
            WHERE username = %s AND password = %s
        """, (username, hashed), fetch_one=True)

    def get_user(self, user_id: int) -> Optional[dict]:
        """Получение пользователя по ID"""
        return self.execute("""
            SELECT id, username, email, bio, avatar_url, phone, status,
                   last_seen, created_at, messages_count, friends_count
            FROM users
            WHERE id = %s
        """, (user_id,), fetch_one=True)

    def get_user_by_username(self, username: str) -> Optional[dict]:
        """Получение пользователя по имени"""
        return self.execute("""
            SELECT id, username, bio, avatar_url, status, last_seen
            FROM users
            WHERE username = %s
        """, (username,), fetch_one=True)

    def set_status(self, user_id: int, status: str):
        """Установка статуса пользователя"""
        self.execute(
            "UPDATE users SET status = %s, last_seen = %s WHERE id = %s",
            (status, int(time.time()), user_id)
        )

    def create_session(self, user_id: int) -> str:
        """Создание сессии"""
        token = hashlib.sha256(
            f"{user_id}{time.time()}{SECRET_KEY}".encode()
        ).hexdigest()
        
        now = int(time.time())
        self.execute("""
            INSERT INTO sessions (token, user_id, created_at, expires_at)
            VALUES (%s, %s, %s, %s)
        """, (token, user_id, now, now + SESSION_TTL))
        
        return token

    def check_session(self, token: str) -> Optional[int]:
        """Проверка сессии"""
        result = self.execute("""
            SELECT user_id FROM sessions
            WHERE token = %s AND expires_at > %s
        """, (token, int(time.time())), fetch_one=True)
        
        return result['user_id'] if result else None

    def delete_session(self, token: str):
        """Удаление сессии"""
        self.execute("DELETE FROM sessions WHERE token = %s", (token,))

    def get_user_chats(self, user_id: int) -> list:
        """Получение всех чатов пользователя"""
        return self.execute("""
            SELECT c.id, c.name, c.type, c.description, c.avatar_url
            FROM chats c
            JOIN chat_members cm ON cm.chat_id = c.id
            WHERE cm.user_id = %s
            ORDER BY c.id
        """, (user_id,), fetch_all=True)

    def get_messages(self, chat_id: int, limit: int = 50) -> list:
        """Получение сообщений чата"""
        msgs = self.execute("""
            SELECT * FROM messages
            WHERE chat_id = %s AND deleted = FALSE
            ORDER BY created_at ASC
            LIMIT %s
        """, (chat_id, limit), fetch_all=True)
        
        return msgs if msgs else []

    def save_message(self, chat_id: int, user_id: int, username: str, text: str) -> dict:
        """Сохранение сообщения"""
        msg = self.execute("""
            INSERT INTO messages (chat_id, user_id, username, text, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, chat_id, user_id, username, text, created_at
        """, (chat_id, user_id, username, text, int(time.time())), fetch_one=True)

        return msg

    def get_online_users(self) -> list:
        """Получение списка онлайн пользователей"""
        return self.execute("""
            SELECT id, username, avatar_url, status
            FROM users
            WHERE status = 'online'
            ORDER BY username
        """, fetch_all=True)

    def get_or_create_private(self, user1_id: int, user2_id: int, user2_name: str) -> dict:
        """Получение или создание личного чата"""
        now = int(time.time())
        
        # Ищем существующий чат
        existing = self.execute("""
            SELECT c.id, c.name, c.type
            FROM chats c
            JOIN chat_members m1 ON m1.chat_id = c.id
            JOIN chat_members m2 ON m2.chat_id = c.id
            WHERE c.type = 'private'
                AND m1.user_id = %s
                AND m2.user_id = %s
            LIMIT 1
        """, (user1_id, user2_id), fetch_one=True)
        
        if existing:
            return existing
        
        # Создаем новый
        chat = self.execute("""
            INSERT INTO chats (name, type, created_by, created_at)
            VALUES (%s, 'private', %s, %s)
            RETURNING id, name, type
        """, (user2_name, user1_id, now), fetch_one=True)

        # Добавляем участников
        self.execute("""
            INSERT INTO chat_members (chat_id, user_id, role, joined_at)
            VALUES (%s, %s, 'member', %s), (%s, %s, 'member', %s)
        """, (chat['id'], user1_id, now, chat['id'], user2_id, now))

        return chat


class Connection:
    """Информация о подключении клиента"""
    def __init__(self, ws, user_id: int, username: str):
        self.ws = ws
        self.user_id = user_id
        self.username = username
        self.current_chat = 1
        self.last_ping = time.time()


class TelegramWhiteServer:
    """Основной класс сервера"""
    
    def __init__(self):
        log.info("=" * 50)
        log.info("🚀 TelegramWhite Server")
        log.info("=" * 50)
        log.info(f"Порт: {PORT}")
        
        self.db = Database(DATABASE_URL)
        self.connections: Dict[int, Connection] = {}
        
        log.info("✅ Сервер инициализирован")

    async def send(self, ws, data: dict):
        """Отправка сообщения клиенту"""
        try:
            await ws.send(json.dumps(data, ensure_ascii=False))
        except Exception as e:
            log.debug(f"Ошибка отправки: {e}")

    async def broadcast(self, data: dict, chat_id: int = None, skip_user: int = None):
        """Рассылка сообщения участникам чата"""
        message = json.dumps(data, ensure_ascii=False)
        
        targets = []
        for uid, conn in self.connections.items():
            if uid != skip_user:
                targets.append(conn.ws)
        
        if targets:
            await asyncio.gather(
                *[t.send(message) for t in targets],
                return_exceptions=True
            )

    async def broadcast_online(self):
        """Обновление списка онлайн пользователей"""
        await self.broadcast({
            'type': 'online_users',
            'users': self.db.get_online_users()
        })

    async def handle_message(self, ws, data: dict):
        """Обработка входящих сообщений"""
        msg_type = data.get('type')
        
        if not msg_type:
            return

        log.info(f"📨 Получено сообщение типа: {msg_type}")

        # Ping/Pong
        if msg_type == 'ping':
            await self.send(ws, {'type': 'pong'})
            return

        # Регистрация
        if msg_type == 'register':
            username = data.get('username', '').strip()
            password = data.get('password', '')
            email = data.get('email', '').strip()
            
            log.info(f"📝 Регистрация: {username}")
            
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
            await self.send(ws, {
                'type': 'registered',
                'user': user,
                'token': token
            })
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
            
            # Обновляем статус
            self.db.set_status(user['id'], 'online')
            
            # Создаем сессию
            token = self.db.create_session(user['id'])
            
            # Сохраняем соединение
            self.connections[user['id']] = Connection(ws, user['id'], username)
            
            # Отправляем данные
            chats = self.db.get_user_chats(user['id'])
            await self.send(ws, {
                'type': 'logged_in',
                'user': user,
                'token': token,
                'chats': chats
            })
            
            # Отправляем историю общего чата
            await self.send(ws, {
                'type': 'history',
                'chat_id': 1,
                'messages': self.db.get_messages(1)
            })
            
            # Оповещаем всех
            await self.broadcast_online()
            await self.broadcast({
                'type': 'system',
                'chat_id': 1,
                'text': f'👋 {username} присоединился'
            }, skip_user=user['id'])
            
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
            
            # Обновляем статус
            self.db.set_status(user_id, 'online')
            self.connections[user_id] = Connection(ws, user_id, user['username'])
            
            chats = self.db.get_user_chats(user_id)
            await self.send(ws, {
                'type': 'session_ok',
                'user': user,
                'chats': chats
            })
            
            await self.send(ws, {
                'type': 'history',
                'chat_id': 1,
                'messages': self.db.get_messages(1)
            })
            
            await self.broadcast_online()
            log.info(f"✅ Восстановлена сессия: {user['username']}")
            return

        # Проверка авторизации для остальных методов
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
            
            msg = self.db.save_message(chat_id, user_id, conn.username, text)
            await self.broadcast({'type': 'message', 'message': msg}, chat_id=chat_id)
            return

        # Смена чата
        if msg_type == 'switch_chat':
            chat_id = data.get('chat_id', 1)
            conn.current_chat = chat_id
            messages = self.db.get_messages(chat_id)
            await self.send(ws, {
                'type': 'history',
                'chat_id': chat_id,
                'messages': messages
            })
            return

        # Печатает...
        if msg_type == 'typing':
            chat_id = data.get('chat_id', 1)
            await self.broadcast({
                'type': 'typing',
                'username': conn.username,
                'chat_id': chat_id
            }, skip_user=user_id)
            return

        # Личный чат
        if msg_type == 'start_private':
            target_name = data.get('username', '').strip()
            target = self.db.get_user_by_username(target_name)
            
            if not target:
                await self.send(ws, {'type': 'error', 'message': 'Пользователь не найден'})
                return
            
            chat = self.db.get_or_create_private(user_id, target['id'], target_name)
            messages = self.db.get_messages(chat['id'])
            
            await self.send(ws, {
                'type': 'private_chat_created',
                'chat': {
                    'id': chat['id'],
                    'name': target_name,
                    'type': 'private'
                },
                'messages': messages
            })
            
            # Оповещаем собеседника
            if target['id'] in self.connections:
                await self.send(self.connections[target['id']].ws, {
                    'type': 'new_private_chat',
                    'chat': {
                        'id': chat['id'],
                        'name': conn.username,
                        'type': 'private'
                    },
                    'messages': messages
                })
            
            log.info(f"💬 Личный чат: {conn.username} - {target_name}")
            return

        # Выход
        if msg_type == 'logout':
            if user_id in self.connections:
                username = self.connections[user_id].username
                del self.connections[user_id]
                self.db.set_status(user_id, 'offline')
                await self.broadcast_online()
            
            await self.send(ws, {'type': 'logged_out'})
            return

    async def handler(self, ws, path):
        """Основной обработчик WebSocket соединений"""
        client_info = f"{ws.remote_address[0]}:{ws.remote_address[1]}"
        log.info(f"➕ Новое подключение: {client_info}")
        
        try:
            async for message in ws:
                try:
                    data = json.loads(message)
                    await self.handle_message(ws, data)
                except json.JSONDecodeError:
                    log.error(f"❌ Неверный JSON: {message[:100]}")
                    await self.send(ws, {'type': 'error', 'message': 'Неверный JSON'})
                except Exception as e:
                    log.error(f"❌ Ошибка обработки: {e}", exc_info=True)
                    await self.send(ws, {'type': 'error', 'message': 'Внутренняя ошибка'})
        
        except websockets.exceptions.ConnectionClosed:
            log.info(f"📴 Соединение закрыто: {client_info}")
        except Exception as e:
            log.error(f"❌ Ошибка: {e}")
        finally:
            # Очистка при отключении
            user_id = None
            for uid, conn in list(self.connections.items()):
                if conn.ws == ws:
                    user_id = uid
                    username = conn.username
                    del self.connections[uid]
                    self.db.set_status(uid, 'offline')
                    log.info(f"➖ Отключение: {username}")
                    break
            
            if user_id:
                await self.broadcast_online()

    async def run(self):
        """Запуск сервера"""
        log.info(f"🚀 Запуск сервера на {HOST}:{PORT}")
        
        async with websockets.serve(
            self.handler,
            HOST,
            PORT,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT
        ):
            log.info(f"✅ Сервер запущен и слушает порт {PORT}")
            
            # Бесконечное ожидание
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
