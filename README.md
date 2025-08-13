# Telegram-бот для магазину автозапчастин

## 📌 Опис
Цей бот автоматизує роботу магазину автозапчастин у Telegram: обробку замовлень, взаємодію з клієнтами, управління бонусами та комунікацію менеджерів із покупцями.  
Реалізовано на **python-telegram-bot** (асинхронна версія) з використанням **FastAPI** для обробки webhook і **PostgreSQL/Supabase** як бази даних.

---

## ⚙️ Функціонал

### Для клієнтів
- Оформлення замовлень.
- Перегляд історії замовлень.
- Перевірка бонусного балансу.
- Активація бонусних кодів.
- Отримання інформації про доставку, оплату, магазин.

### Для менеджерів
- Отримання нових замовлень у групу.
- Взяття клієнта в обробку.
- Відповіді клієнту в особистих повідомленнях.
- Додавання замовлень із ціною та описом.
- Перегляд і зміна статусу замовлень.
- Експорт замовлень у форматі Excel.
- Зміна бонусного балансу клієнтів.

---

## 🗄 Структура проєкту
main.py # Основна логіка бота та webhook
db.py # Робота з базою даних (PostgreSQL/Supabase)
requirements.txt # Список залежностей
.env.example # Приклад конфігурації середовища
README.md # Опис проєкту


---

## 🛢 Структура бази даних
У Supabase або PostgreSQL потрібно створити такі таблиці:

### Таблиця `orders`
| Поле        | Тип       | Опис |
|-------------|-----------|------|
| id          | serial PK | ID замовлення |
| user_id     | bigint    | ID користувача Telegram |
| description | text      | Опис замовлення |
| price       | numeric   | Ціна замовлення |
| status      | text      | Статус замовлення |
| created_at  | timestamp | Дата створення |

### Таблиця `bonuses`
| Поле    | Тип       | Опис |
|---------|-----------|------|
| id      | serial PK | ID запису |
| user_id | bigint    | ID користувача Telegram |
| balance | numeric   | Бонусний баланс |

### Таблиця `messages`
| Поле        | Тип       | Опис |
|-------------|-----------|------|
| id          | serial PK | ID повідомлення |
| user_id     | bigint    | ID користувача Telegram |
| message_text| text      | Текст повідомлення |
| created_at  | timestamp | Дата повідомлення |

---

## 📂 Конфігурація
У корені проєкту створіть файл `.env` за прикладом:

```env
BOT_TOKEN=ваш_токен_бота
MANAGER_ID=ID_менеджера
MANAGER_GROUP_ID=ID_групи_менеджерів
WEBHOOK_URL=https://ваш-домен.com
WEBHOOK_SECRET_TOKEN=секретний_токен
PORT=8000
DATABASE_URL=postgresql://user:password@host:port/dbname

🚀 Встановлення
1. Клонувати репозиторій:
git clone https://github.com/Zapchasti69/store-telegram-bot.git
cd store-telegram-bot

2. Встановити залежності:
pip install -r requirements.txt

3.Запуск
Через webhook (production):
uvicorn main:fastapi_app --host 0.0.0.0 --port 8000

Локально (polling, для тестування):
У main.py замініть запуск FastAPI на:
telegram_app.run_polling()

📦 Використані технології
Python 3.10+
python-telegram-bot — робота з Telegram API
FastAPI — веб-сервер для webhook
PostgreSQL / Supabase — база даних
asyncpg — асинхронний драйвер PostgreSQL
pandas та openpyxl — експорт замовлень у Excel
python-dotenv — завантаження конфігурації

