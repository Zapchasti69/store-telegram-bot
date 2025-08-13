import asyncpg
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from typing import Dict, Any, Optional
from datetime import datetime # Імпортуємо datetime для created_at

# 🛠️ Налаштування логування для db.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = "aws-0-eu-north-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.frxhghhoqlfuatrfldvb"
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = 6543

# Глобальна змінна для пулу з'єднань
_pool = None

async def get_db_pool():
    """Повертає існуючий пул з'єднань або ініціалізує його."""
    global _pool
    if _pool is None:
        await init_db_pool()
    return _pool

async def init_db_pool():
    """Ініціалізує пул з'єднань asyncpg."""
    global _pool
    if _pool is None:
        try:
            _pool = await asyncpg.create_pool(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                min_size=1,  # Мінімальна кількість з'єднань у пулі
                max_size=10, # Максимальна кількість з'єднань у пулі
            )
            logger.info("Пул з'єднань БД успішно ініціалізовано.")
            await init_tables() # Викликаємо функцію для створення/оновлення таблиць після ініціалізації пулу
        except Exception as e:
            logger.error(f"Помилка ініціалізації пулу з'єднань БД: {e}")
            _pool = None # Забезпечити, що пул не буде встановлений, якщо сталася помилка

async def init_tables():
    """Створює/оновлює таблиці, якщо вони не існують, використовуючи asyncpg."""
    if _pool is None:
        logger.error("Пул з'єднань БД не ініціалізовано. Неможливо створити/оновлювати таблиці.")
        return

    async with _pool.acquire() as conn:
        try:
            # Використовуємо транзакцію для створення декількох таблиць атомарно
            async with conn.transaction():
                # Таблиця orders
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id TEXT PRIMARY KEY,
                        client_id BIGINT,
                        status TEXT,
                        price NUMERIC(10, 2) NULL,
                        description TEXT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """)
                
                # Додаємо колонки price, description, created_at, якщо їх ще немає
                await conn.execute("""
                    DO $$ BEGIN
                        ALTER TABLE orders ADD COLUMN IF NOT EXISTS price NUMERIC(10, 2) NULL;
                        ALTER TABLE orders ADD COLUMN IF NOT EXISTS description TEXT NULL;
                        ALTER TABLE orders ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
                    END $$;
                """)

                # Таблиця client_states
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS client_states (
                        client_id BIGINT PRIMARY KEY,
                        is_active BOOLEAN DEFAULT FALSE,
                        is_notified BOOLEAN DEFAULT FALSE,
                        current_manager_id BIGINT NULL,
                        last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """)
                # Таблиця client_messages
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS client_messages (
                        message_id SERIAL PRIMARY KEY,
                        client_id BIGINT NOT NULL,
                        sender_type TEXT NOT NULL,
                        message_text TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (client_id) REFERENCES client_states(client_id) ON DELETE CASCADE
                    );
                """)
                # Таблиця manager_active_dialogs
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS manager_active_dialogs (
                        manager_id BIGINT PRIMARY KEY,
                        active_client_id BIGINT NULL,
                        FOREIGN KEY (active_client_id) REFERENCES client_states(client_id) ON DELETE SET NULL
                    );
                """)

                # 🔥 НОВА ТАБЛИЦЯ: bonus_accounts (для балансу бонусів клієнтів)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bonus_accounts (
                        telegram_user_id BIGINT PRIMARY KEY,
                        instagram_user_id TEXT UNIQUE,
                        bonus_balance NUMERIC(10, 2) DEFAULT 0.00 NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    );
                """)

                # 🔥 ОНОВЛЕННЯ ІСНУЮЧОЇ ТАБЛИЦІ: bonus_codes
                # Додаємо лише колонку activated_by_tg_user_id,
                # оскільки інші вже існують під іншими назвами (bonus_amount, user_id, redeemed_at).
                await conn.execute("""
                    DO $$ BEGIN
                        ALTER TABLE bonus_codes ADD COLUMN IF NOT EXISTS activated_by_tg_user_id BIGINT;
                    END $$;
                """)
                logger.info("Таблиці успішно ініціалізовані/перевірені.")
        except Exception as e:
            logger.error(f"Помилка при створенні/перевірці таблиць БД: {e}")

async def add_order(order_id: str, client_id: int, status: str, price: Optional[float] = None, description: Optional[str] = None):
    """Додає нове замовлення до таблиці 'orders' за допомогою asyncpg."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо додати замовлення.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO orders (order_id, client_id, status, price, description, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (order_id) DO NOTHING
            """, order_id, client_id, status, price, description)
            logger.info(f"Замовлення {order_id} додано зі статусом '{status}'.")
            # Перевірка, чи замовлення дійсно додалося
            check_record = await conn.fetchrow("SELECT order_id, status FROM orders WHERE order_id = $1", order_id)
            if check_record:
                logger.info(f"Перевірка: Замовлення {check_record['order_id']} знайдено в БД зі статусом '{check_record['status']}'.")
            else:
                logger.warning(f"Перевірка: Замовлення {order_id} НЕ знайдено в БД після спроби додавання.")

        except Exception as e:
            logger.error(f"Помилка при додаванні замовлення {order_id}: {e}")

async def get_order_details(order_id: str) -> Optional[Dict[str, Any]]:
    """Повертає деталі замовлення (статус, ціну, опис) за його order_id, або None, якщо не знайдено."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати деталі замовлення.")
        return None
    async with pool.acquire() as conn:
        try:
            record = await conn.fetchrow("SELECT order_id, status, price, description, created_at FROM orders WHERE order_id = $1", order_id)
            return dict(record) if record else None
        except Exception as e:
            logger.error(f"Помилка при отриманні деталей замовлення {order_id}: {e}")
            return None

async def update_order_status(order_id: str, new_status: str):
    """Оновлює статус замовлення за його order_id."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити статус замовлення.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("UPDATE orders SET status = $1 WHERE order_id = $2", new_status, order_id)
            logger.info(f"Статус замовлення {order_id} оновлено на {new_status}.")
        except Exception as e:
            logger.error(f"Помилка при оновленні статусу замовлення {order_id}: {e}")

async def get_client_id_by_order_id(order_id: str) -> Optional[int]: # ЗМІНА НАЗВИ ФУНКЦІЇ
    """
    Отримує client_id з таблиці 'orders' за 'order_id'.
    Повертає client_id або None, якщо замовлення не знайдено.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати client_id за order_id.")
        return None
    async with pool.acquire() as conn:
        try:
            client_id = await conn.fetchval(
                "SELECT client_id FROM orders WHERE order_id = $1",
                order_id
            )
            return client_id
        except Exception as e:
            logger.error(f"Помилка при отриманні client_id за order_id {order_id}: {e}")
            return None

async def get_all_orders():
    """Повертає всі замовлення."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати всі замовлення.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT order_id, client_id, status, price, description, created_at FROM orders")
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні всіх замовлень: {e}")
            return []

async def get_orders_by_status(status: str):
    """Повертає замовлення за певним статусом."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати замовлення за статусом.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT order_id, client_id, status, price, description, created_at FROM orders WHERE status = $1", status)
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні замовлень за статусом '{status}': {e}")
            return []

async def delete_order(order_id: str): # ЗМІНА ПАРАМЕТРА
    """Видаляє замовлення за його order_id."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо видалити замовлення.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("DELETE FROM orders WHERE order_id = $1", order_id)
            logger.info(f"Замовлення {order_id} видалено.")
        except Exception as e:
            logger.error(f"Помилка при видаленні замовлення {order_id}: {e}")

async def add_client_state(client_id: int, is_active: bool = False, is_notified: bool = False, current_manager_id: Optional[int] = None):
    """Додає новий стан клієнта або оновлює існуючий."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо додати/оновити стан клієнта.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO client_states (client_id, is_active, is_notified, current_manager_id, last_activity)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (client_id) DO UPDATE SET
                    is_active = EXCLUDED.is_active,
                    is_notified = EXCLUDED.is_notified,
                    current_manager_id = EXCLUDED.current_manager_id,
                    last_activity = NOW()
            """, client_id, is_active, is_notified, current_manager_id)
            logger.info(f"Стан клієнта {client_id} додано/оновлено.")
        except Exception as e:
            logger.error(f"Помилка при додаванні/оновленні стану клієнта {client_id}: {e}")

async def get_client_state(client_id: int):
    """Повертає стан клієнта за його client_id, або None, якщо не знайдено."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати стан клієнта.")
        return None
    async with pool.acquire() as conn:
        try:
            record = await conn.fetchrow("SELECT is_active, is_notified, current_manager_id, last_activity FROM client_states WHERE client_id = $1", client_id)
            return dict(record) if record else None
        except Exception as e:
            logger.error(f"Помилка при отриманні стану клієнта {client_id}: {e}")
            return None

async def update_client_active_status(client_id: int, is_active: bool):
    """Оновлює статус активності клієнта."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити статус активності клієнта.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("UPDATE client_states SET is_active = $1, last_activity = NOW() WHERE client_id = $2", is_active, client_id)
            logger.info(f"Статус активності клієнта {client_id} оновлено на {is_active}.")
        except Exception as e:
            logger.error(f"Помилка при оновленні статусу активності клієнта {client_id}: {e}")

async def update_client_notified_status(client_id: int, is_notified: bool):
    """Оновлює статус сповіщення клієнта."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити статус сповіщення клієнта.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("UPDATE client_states SET is_notified = $1 WHERE client_id = $2", is_notified, client_id)
            logger.info(f"Статус сповіщення клієнта {client_id} оновлено на {is_notified}.")
        except Exception as e:
            logger.error(f"Помилка при оновленні статусу сповіщення клієнта {client_id}: {e}")

async def update_client_manager(client_id: int, manager_id: Optional[int]):
    """Оновлює ID менеджера, який зараз працює з клієнтом."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити менеджера для клієнта.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("UPDATE client_states SET current_manager_id = $1 WHERE client_id = $2", manager_id, client_id)
            logger.info(f"Менеджер для клієнта {client_id} оновлено на {manager_id}.")
        except Exception as e:
            logger.error(f"Помилка при оновленні менеджера для клієнта {client_id}: {e}")

async def get_manager_active_dialogs(manager_id: int) -> Optional[int]:
    """
    Повертає client_id, з яким менеджер manager_id зараз веде активний діалог.
    Якщо немає активного діалогу, повертає None.
    Ця функція тепер використовує нову таблицю `manager_active_dialogs`.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати активний діалог менеджера.")
        return None
    async with pool.acquire() as conn:
        try:
            active_client_id = await conn.fetchval(
                "SELECT active_client_id FROM manager_active_dialogs WHERE manager_id = $1",
                manager_id
            )
            return active_client_id
        except Exception as e:
            logger.error(f"Помилка при отриманні активного діалогу для менеджера {manager_id}: {e}")
            return None

async def update_manager_active_dialog(manager_id: int, client_id: Optional[int]):
    """
    Встановлює або очищає активний діалог для менеджера.
    Якщо client_id є None, це означає, що менеджер більше не веде активний діалог.
    Ця функція оновлює запис у таблиці `manager_active_dialogs`.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити активний діалог менеджера.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO manager_active_dialogs (manager_id, active_client_id)
                VALUES ($1, $2)
                ON CONFLICT (manager_id) DO UPDATE SET
                    active_client_id = EXCLUDED.active_client_id
            """, manager_id, client_id)
            if client_id:
                logger.info(f"Менеджер {manager_id} тепер веде активний діалог з клієнтом {client_id}.")
            else:
                logger.info(f"Активний діалог для менеджера {manager_id} очищено.")
        except Exception as e:
            logger.error(f"Помилка при оновленні активного діалогу для менеджера {manager_id}: {e}")

async def get_active_clients():
    """Повертає список ID активних клієнтів."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати активних клієнтів.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT client_id FROM client_states WHERE is_active = TRUE")
            return [r['client_id'] for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні активних клієнтів: {e}")
            return []

async def get_pending_clients() -> list[Dict[str, Any]]:
    """
    Повертає список клієнтів, які активні, але ще не взяті в роботу менеджером.
    Включає client_id та last_activity.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати очікуючих клієнтів.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("""
                SELECT client_id, last_activity
                FROM client_states
                WHERE is_active = TRUE AND current_manager_id IS NULL
                ORDER BY last_activity ASC;
            """)
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні очікуючих клієнтів: {e}")
            return []

async def get_not_notified_clients():
    """Повертає список ID клієнтів, які не були сповіщені."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати несповіщених клієнтів.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT client_id FROM client_states WHERE is_notified = FALSE")
            return [r['client_id'] for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні несповіщених клієнтів: {e}")
            return []

async def add_client_message(client_id: int, sender_type: str, message_text: str):
    """Додає повідомлення від клієнта або менеджера до історії."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо додати повідомлення.")
        return
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                INSERT INTO client_messages (client_id, sender_type, message_text)
                VALUES ($1, $2, $3)
            """, client_id, sender_type, message_text)
            logger.info(f"Повідомлення для клієнта {client_id} додано.")
        except Exception as e:
            logger.error(f"Помилка при додаванні повідомлення для клієнта {client_id}: {e}")

async def get_client_messages(client_id: int):
    """Повертає всі повідомлення для певного клієнта."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати повідомлення клієнта.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT sender_type, message_text, timestamp FROM client_messages WHERE client_id = $1 ORDER BY timestamp ASC", client_id)
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні повідомлень для клієнта {client_id}: {e}")
            return []

async def export_orders_to_excel():
    """Експортує всі замовлення в Excel файл."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо експортувати.")
        return None
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("SELECT order_id, client_id, status, price, description, created_at FROM orders")
            
            data = [dict(r) for r in records]
            
            if not data:
                logger.info("Немає даних для експорту.")
                return None

            df = pd.DataFrame(data)
            file_path = "orders_export.xlsx"
            df.to_excel(file_path, index=False)
            logger.info(f"Дані замовлень експортовано до {file_path}.")
            return file_path
        except Exception as e:
            logger.error(f"Помилка при експорті замовлень в Excel: {e}")
            return None

async def close_db_pool():
    """Закриває пул з'єднань asyncpg."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Пул з'єднань БД закрито.")

# 🔥 НОВІ ФУНКЦІЇ ДЛЯ ОБРОБКИ БОНУСІВ 🔥

async def create_or_get_bonus_account(telegram_user_id: int, instagram_user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Створює або повертає запис про бонусний акаунт клієнта."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо створити/отримати бонусний акаунт.")
        return None
    async with pool.acquire() as conn:
        try:
            # Спроба отримати існуючий акаунт
            record = await conn.fetchrow(
                "SELECT telegram_user_id, instagram_user_id, bonus_balance FROM bonus_accounts WHERE telegram_user_id = $1;",
                telegram_user_id
            )
            if record:
                # Якщо акаунт знайдено, можливо, потрібно оновити instagram_user_id, якщо він змінився
                if instagram_user_id and record['instagram_user_id'] is None:
                    await conn.execute(
                        "UPDATE bonus_accounts SET instagram_user_id = $2, last_updated = NOW() WHERE telegram_user_id = $1;",
                        telegram_user_id, instagram_user_id
                    )
                    record = await conn.fetchrow( # Оновлюємо record після оновлення
                        "SELECT telegram_user_id, instagram_user_id, bonus_balance FROM bonus_accounts WHERE telegram_user_id = $1;",
                        telegram_user_id
                    )
                return dict(record)
            else:
                # Якщо акаунт не знайдено, створюємо новий
                await conn.execute(
                    """
                    INSERT INTO bonus_accounts (telegram_user_id, instagram_user_id, bonus_balance, last_updated)
                    VALUES ($1, $2, $3, NOW());
                    """,
                    telegram_user_id, instagram_user_id, 0.00 # Початковий баланс 0
                )
                logger.info(f"Створено новий бонусний акаунт для TG ID: {telegram_user_id}")
                # Повертаємо щойно створений запис
                return {"telegram_user_id": telegram_user_id, "instagram_user_id": instagram_user_id, "bonus_balance": 0.00}
        except Exception as e:
            logger.error(f"Помилка при створенні/отриманні бонусного акаунту для TG ID {telegram_user_id}: {e}")
            return None

async def update_bonus_balance(telegram_user_id: int, amount: float) -> bool:
    """Оновлює баланс бонусів для клієнта."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо оновити баланс бонусів.")
        return False
    async with pool.acquire() as conn:
        try:
            # Перевіряємо, чи існує бонусний акаунт, інакше створюємо його з 0
            await create_or_get_bonus_account(telegram_user_id)
            
            await conn.execute(
                "UPDATE bonus_accounts SET bonus_balance = bonus_balance + $2, last_updated = NOW() WHERE telegram_user_id = $1;",
                telegram_user_id, amount
            )
            logger.info(f"Оновлено баланс бонусів для TG ID: {telegram_user_id} на {amount}.")
            return True
        except Exception as e:
            logger.error(f"Помилка при оновленні балансу бонусів для TG ID {telegram_user_id}: {e}")
            return False

async def get_bonus_code_details(code: str) -> Optional[Dict[str, Any]]:
    """Отримує деталі бонусного коду за його значенням."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати деталі бонусного коду.")
        return None
    async with pool.acquire() as conn:
        try:
            # Вибираємо колонки згідно з вашою фактичною схемою
            record = await conn.fetchrow(
                "SELECT id, code, is_active, bonus_amount, user_id, redeemed_at, activated_by_tg_user_id FROM bonus_codes WHERE code = $1 LIMIT 1;",
                code
            )
            # Перейменовуємо ключі словника, щоб вони відповідали очікуваним назвам у main.py
            if record:
                details = dict(record)
                details['value'] = details.pop('bonus_amount') # Перейменовуємо bonus_amount на value
                details['assigned_to_ig_user_id'] = details.pop('user_id') # Перейменовуємо user_id на assigned_to_ig_user_id
                details['activation_date'] = details.pop('redeemed_at') # Перейменовуємо redeemed_at на activation_date
                return details
            return None
        except Exception as e:
            logger.error(f"Помилка при отриманні деталей бонусного коду '{code}': {e}")
            return None

async def activate_bonus_code(code_id: str, telegram_user_id: int) -> bool:
    """
    Активує бонусний код, позначаючи його як використаний
    та записуючи, хто його активував.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо активувати бонусний код.")
        return False
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                UPDATE bonus_codes
                SET is_active = FALSE, activated_by_tg_user_id = $2, redeemed_at = NOW()
                WHERE id = $1 AND is_active = TRUE;
                """,
                code_id, telegram_user_id
            )
            logger.info(f"Бонусний код ID {code_id} активовано користувачем TG ID {telegram_user_id}.")
            return True
        except Exception as e:
            logger.error(f"Помилка при активації бонусного коду ID {code_id} для TG ID {telegram_user_id}: {e}")
            return False

async def get_telegram_id_by_instagram_id(instagram_user_id: str) -> Optional[int]:
    """Повертає Telegram ID, пов'язаний з даним Instagram ID."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати TG ID за IG ID.")
        return None
    async with pool.acquire() as conn:
        try:
            record = await conn.fetchrow(
                "SELECT telegram_user_id FROM bonus_accounts WHERE instagram_user_id = $1 LIMIT 1;",
                instagram_user_id
            )
            return record["telegram_user_id"] if record else None
        except Exception as e:
            logger.error(f"Помилка при отриманні TG ID для IG ID {instagram_user_id}: {e}")
            return None

async def link_instagram_to_telegram_account(telegram_user_id: int, instagram_user_id: str) -> bool:
    """Зв'язує Instagram ID з існуючим Telegram-акаунтом."""
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо зв'язати акаунти.")
        return False
    async with pool.acquire() as conn:
        try:
            # Переконаємось, що бонусний акаунт для TG ID існує
            await create_or_get_bonus_account(telegram_user_id) 

            await conn.execute(
                """
                UPDATE bonus_accounts
                SET instagram_user_id = $2, last_updated = NOW()
                WHERE telegram_user_id = $1;
                """,
                telegram_user_id, instagram_user_id
            )
            logger.info(f"Зв'язано IG ID {instagram_user_id} з TG ID {telegram_user_id}.")
            return True
        except Exception as e:
            logger.error(f"Помилка при зв'язуванні IG ID {instagram_user_id} з TG ID {telegram_user_id}: {e}")
            return False

# 🔥 НОВІ ФУНКЦІЇ ДЛЯ ОТРИМАННЯ СПИСКІВ ЗАМОВЛЕНЬ 🔥

async def get_client_orders(client_id: int) -> list[Dict[str, Any]]:
    """
    Повертає всі замовлення для певного клієнта, включаючи ціну та опис.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати замовлення клієнта.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("""
                SELECT order_id, status, created_at, price, description
                FROM orders
                WHERE client_id = $1
                ORDER BY created_at DESC;
            """, client_id)
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні замовлень для клієнта {client_id}: {e}")
            return []

async def get_all_active_orders() -> list[Dict[str, Any]]:
    """
    Повертає всі замовлення, статус яких НЕ "✅ Замовлення виконано",
    включаючи order_id, client_id, status, price, description.
    """
    pool = await get_db_pool()
    if pool is None:
        logger.warning("Пул з'єднань БД не ініціалізовано. Неможливо отримати всі активні замовлення.")
        return []
    async with pool.acquire() as conn:
        try:
            records = await conn.fetch("""
                SELECT order_id, client_id, status, price, description, created_at
                FROM orders
                WHERE status != '✅ Замовлення виконано'
                ORDER BY created_at DESC;
            """)
            logger.info(f"get_all_active_orders: Знайдено {len(records)} активних замовлень.")
            for r in records:
                logger.info(f"Активне замовлення: ID={r['order_id']}, Status='{r['status']}'") # Оновлено логування
            return [dict(r) for r in records]
        except Exception as e:
            logger.error(f"Помилка при отриманні всіх активних замовлень: {e}")
            return []



