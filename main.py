import os
import logging
import random
from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters
)
import asyncio
from datetime import datetime
from telegram.error import BadRequest
from decimal import Decimal # <<< ДОДАНО: Імпорт Decimal для точних розрахунків

from fastapi import FastAPI, Request, Response, HTTPException
from typing import Dict, Any, Optional

import uvicorn

from db import (
    init_db_pool, close_db_pool, get_db_pool,
    add_order, update_order_status, get_order_details, export_orders_to_excel,
    add_client_state, get_client_state, update_client_active_status,
    update_client_notified_status, update_client_manager,
    add_client_message, get_client_messages,
    get_client_id_by_order_id,
    get_manager_active_dialogs,
    update_manager_active_dialog,
    get_pending_clients,
    create_or_get_bonus_account,
    update_bonus_balance,
    get_bonus_code_details,
    activate_bonus_code,
    get_telegram_id_by_instagram_id,
    link_instagram_to_telegram_account,
    get_client_orders,
    get_all_active_orders
)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
MANAGER_ID = int(os.getenv("MANAGER_ID")) # Це може бути ID одного менеджера, якщо у вас їх декілька, то потрібно буде змінити логіку для MANAGER_GROUP_ID
MANAGER_GROUP_ID = int(os.getenv("MANAGER_GROUP_ID")) # Ця група буде отримувати нові запити
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN")
WEB_SERVER_PORT = int(os.getenv("PORT", 8000))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- КЛАВІАТУРИ ---
main_menu = ReplyKeyboardMarkup([
    ["📦 Зробити запит/замовлення"],
    ["🎯 Акція", "🔍 Перевірити замовлення"],
    ["🎁 Мої бонуси", "ℹ️ Інформація"]
], resize_keyboard=True)

info_menu = ReplyKeyboardMarkup([
    ["👥 Про нас"],
    ["📞 Контакти"],
    ["📦 Доставка"],
    ["🔙 Назад"]
], resize_keyboard=True)

bonus_main_menu = ReplyKeyboardMarkup([
    ["💰 Перевірити баланс"],
    ["⬆️ Ввести бонус-код"],
    ["🔙 Назад"]
], resize_keyboard=True)

back_button = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
end_dialog_client_button = ReplyKeyboardMarkup([["❌ Завершити діалог"]], resize_keyboard=True)

manager_main_menu = ReplyKeyboardMarkup([
    ["📊 Запити клієнтів", "📝 Змінити баланс"],
    ["📤 Експорт замовлень", "🔍 Інфо по клієнту"]
], resize_keyboard=True)

manager_requests_menu = ReplyKeyboardMarkup([
    ["💬 Активний діалог"],
    ["📨 Нові запити"],
    ["✅ Оформлені замовлення"],
    ["🔙 Назад"]
], resize_keyboard=True)

manager_processed_orders_menu = ReplyKeyboardMarkup([
    ["✏️ Змінити статус замовлення"],
    ["🔙 Назад до запитів"]
], resize_keyboard=True)

active_dialog_client_buttons = ReplyKeyboardMarkup([
    ["📦 Оформити замовлення"],
    ["📂 Архів повідомлень"],
    ["📜 Замовлення клієнта"],
    ["❌ Завершити діалог"],
    ["🔙 Назад"]
], resize_keyboard=True)

order_status_change_menu = ReplyKeyboardMarkup([
    ["🔄 Комплектування", "🚚 З ЄС"],
    ["📮 По Україні", "✅ Виконано"],
    ["🔙 Назад"]
], resize_keyboard=True)

telegram_app: Application = None

# --- ДОПОМІЖНІ ФУНКЦІЇ ---
async def send_dialog_archive(client_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Надсилає архів повідомлень діалогу в групу менеджера."""
    history_records = await get_client_messages(client_id)
    history_formatted = "\n".join([f"{rec['sender_type'].capitalize()}: {rec['message_text']}" for rec in history_records])
    history = history_formatted or "📭 Історія порожня"

    await context.bot.send_message(
        MANAGER_GROUP_ID,
        f"📂 **АРХІВ ДІАЛОГУ** з клієнтом (ID: `{client_id}`):\n\n{history}",
        parse_mode="Markdown"
    )
    logger.info(f"Архів діалогу з клієнтом {client_id} надіслано.")

async def close_client_dialog(client_id: int, context: ContextTypes.DEFAULT_TYPE, initiator: str):
    """
    Централізована функція для завершення діалогу.
    Оновлює статус клієнта, очищає менеджера, сповіщає обидві сторони.
    """
    client_state = await get_client_state(client_id)
    if not client_state or not client_state.get("is_active"):
        logger.info(f"Діалог з клієнтом {client_id} вже не активний або не існує. Завершення не потрібне.")
        return

    await update_client_active_status(client_id, is_active=False)
    await update_client_notified_status(client_id, is_notified=False)

    manager_id_for_client = client_state.get("current_manager_id")
    if manager_id_for_client:
        await update_client_manager(client_id, None)
        await update_manager_active_dialog(manager_id_for_client, None)
        logger.info(f"Менеджер {manager_id_for_client} відкріплений від клієнта {client_id}.")

        client_info = None
        try:
            client_info = await context.bot.get_chat(client_id)
        except Exception as e:
            logger.warning(f"Не вдалося отримати інформацію про Telegram чат для {client_id}: {e}")

        client_full_name = client_info.full_name if client_info else f"Клієнт (ID: {client_id})"
        # Надсилаємо сповіщення менеджеру, який керував діалогом
        try:
            await context.bot.send_message(
                manager_id_for_client, # Надсилаємо конкретному менеджеру, а не в групу
                f"❌ Діалог з клієнтом **{client_full_name}** (ID: `{client_id}`) завершено {initiator}.",
                parse_mode="Markdown",
                reply_markup=manager_main_menu
            )
        except Exception as e:
            logger.warning(f"Не вдалося надіслати сповіщення менеджеру {manager_id_for_client} про завершення діалогу: {e}")

    await context.bot.send_message(
        client_id,
        "✅ Діалог завершено. Дякуємо!",
        reply_markup=main_menu
    )
    logger.info(f"Діалог з клієнтом {client_id} завершено {initiator}.")

# --- КОМАНДИ І ОБРОБНИКИ ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    client_db_state = await get_client_state(uid)

    bonus_acc = await create_or_get_bonus_account(uid)
    if bonus_acc and bonus_acc.get("bonus_balance") == Decimal('0.00'): # Змінено 0.00 на Decimal('0.00')
        await update_bonus_balance(uid, Decimal('50.00')) # Змінено 50.00 на Decimal('50.00')
        logger.info(f"Клієнту {uid} нараховано стартовий бонус 50 грн.")
        await update.message.reply_text(
            "🎉 Вітаємо! Як новому користувачу, вам нараховано **50 грн бонусів** на перший запит!",
            parse_mode="Markdown"
        )

    if not client_db_state:
        await add_client_state(uid, is_active=False, is_notified=False)
    else:
        # Якщо клієнт вже був в активному діалозі, завершуємо його при новому /start
        if client_db_state.get("is_active"):
            await close_client_dialog(uid, context, "автоматично при /start")

    if uid == MANAGER_ID: # Тут потрібно буде додати логіку для багатьох менеджерів
        context.user_data.pop("manager_awaiting_balance_client_id", None)
        context.user_data.pop("manager_awaiting_balance_amount", None)
        context.user_data.pop("temp_client_id_for_balance", None)
        context.user_data.pop("manager_awaiting_client_info_id", None)
        context.user_data.pop("manager_menu_state", None)
        context.user_data.pop("manager_awaiting_order_price", None)
        context.user_data.pop("manager_awaiting_order_description", None)
        context.user_data.pop("temp_order_client_id", None)
        context.user_data.pop("temp_order_price", None)
        context.user_data.pop("manager_awaiting_order_id_for_status_change", None)
        context.user_data.pop("temp_order_id_for_status_change", None)

        await update.message.reply_text(
            "👋 Вітаємо, Менеджер! Оберіть дію:",
            reply_markup=manager_main_menu
        )
        logger.info(f"Менеджер {uid} запустив команду /start.")
    else:
        await update.message.reply_text(
            "👋 Вітаємо в Zapchasti Market 69!\nНатисніть кнопку нижче, щоб продовжити:",
            reply_markup=main_menu
        )
        context.user_data["client_menu_state"] = "main"
        logger.info(f"Клієнт {uid} запустив команду /start.")

async def manager_menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    # Очищення всіх станів менеджера при переході в головне меню менеджера
    context.user_data.pop("manager_awaiting_balance_client_id", None)
    context.user_data.pop("manager_awaiting_balance_amount", None)
    context.user_data.pop("temp_client_id_for_balance", None)
    context.user_data.pop("manager_awaiting_client_info_id", None)
    context.user_data.pop("manager_awaiting_order_price", None)
    context.user_data.pop("manager_awaiting_order_description", None)
    context.user_data.pop("temp_order_client_id", None)
    context.user_data.pop("temp_order_price", None)
    context.user_data.pop("manager_awaiting_order_id_for_status_change", None)
    context.user_data.pop("temp_order_id_for_status_change", None)
    context.user_data["manager_menu_state"] = "main"

    await update.message.reply_text(
        "📊 Меню менеджера:",
        reply_markup=manager_main_menu
    )

async def client_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id_from_handler: Optional[int] = None):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    target_tg_id = None
    if target_id_from_handler:
        target_tg_id = target_id_from_handler
    elif context.args and len(context.args) == 1:
        try:
            target_tg_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Невірний формат Telegram ID. Використання: `/client_info <Telegram_ID_клієнта>`", parse_mode="Markdown")
            return
    else:
        await update.message.reply_text("Використання: `/client_info <Telegram_ID_клієнта>`", parse_mode="Markdown")
        return

    try:
        client_tg_info = None
        try:
            client_tg_info = await context.bot.get_chat(target_tg_id)
        except Exception as e:
            logger.warning(f"Не вдалося отримати інформацію про Telegram чат для {target_tg_id}: {e}")

        client_state = await get_client_state(target_tg_id)
        bonus_acc = await create_or_get_bonus_account(target_tg_id)

        info_text = f"**ℹ️ Інформація про клієнта (ID: `{target_tg_id}`)**\n\n"

        if client_tg_info:
            info_text += f"👤 Ім'я: {client_tg_info.full_name}\n"
            if client_tg_info.username:
                info_text += f"🔗 Username: @{client_tg_info.username}\n"
        else:
            info_text += "👤 Інформація про Telegram-акаунт недоступна.\n"

        if client_state:
            info_text += f"💬 Активний діалог: {'✅ Так' if client_state.get('is_active') else '❌ Ні'}\n"
            if client_state.get('current_manager_id'):
                info_text += f"👨‍💻 Менеджер: `{client_state.get('current_manager_id')}`\n"
            if client_state.get('last_activity'):
                info_text += f"⏱️ Остання активність: {client_state['last_activity'].strftime('%d.%m.%Y %H:%M:%S')}\n"
        else:
            info_text += "💬 Стан діалогу: Немає запису (новий клієнт).\n"

        if bonus_acc:
            info_text += f"💰 Баланс бонусів: **{bonus_acc.get('bonus_balance', Decimal('0.00')):.2f} грн**\n" # Змінено 0.00 на Decimal('0.00')
            if bonus_acc.get('instagram_user_id'):
                info_text += f"📸 Instagram ID: `{bonus_acc.get('instagram_user_id')}`\n"
        else:
            info_text += "💰 Бонусний акаунт: Немає запису.\n"

        await update.message.reply_text(info_text, parse_mode="Markdown")
        logger.info(f"Менеджер {uid} запросив інфо про клієнта {target_tg_id}.")

    except ValueError:
        await update.message.reply_text("❌ Невірний формат Telegram ID. Використання: `/client_info <Telegram_ID_клієнта>`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Помилка в client_info_command: {e}")
        await update.message.reply_text(f"Виникла непередбачена помилка: {e}")

async def manager_requests_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    context.user_data["manager_menu_state"] = "requests_menu"
    await update.message.reply_text(
        "📊 Оберіть категорію запитів:",
        reply_markup=manager_requests_menu
    )
    logger.info(f"Менеджер {uid} перейшов до підменю 'Запити клієнтів'.")

async def active_dialog_details_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    manager_current_dialog_id = await get_manager_active_dialogs(uid)
    if manager_current_dialog_id:
        context.user_data["manager_menu_state"] = "active_dialog"
        client_tg_info = None
        try:
            client_tg_info = await context.bot.get_chat(manager_current_dialog_id)
        except Exception:
            pass
        client_name = client_tg_info.full_name if client_tg_info else f"Клієнт (ID: {manager_current_dialog_id})"

        await update.message.reply_text(
            f"💬 **Ваш активний діалог з {client_name}** (ID: `{manager_current_dialog_id}`)\n"
            f"Оберіть дію:",
            parse_mode="Markdown",
            reply_markup=active_dialog_client_buttons
        )
        logger.info(f"Менеджер {uid} переглянув активний діалог з клієнтом {manager_current_dialog_id}.")
    else:
        await update.message.reply_text(
            "❌ У вас немає активного діалогу.",
            reply_markup=manager_main_menu
        )
        context.user_data["manager_menu_state"] = "main"
        logger.info(f"Менеджер {uid} спробував переглянути активний діалог, але його немає.")

async def new_requests_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    context.user_data["manager_menu_state"] = "new_requests_list"
    response_text = "📨 **Нові запити від клієнтів:**\n\n"
    keyboard_buttons = []

    pending_clients = await get_pending_clients()

    if pending_clients:
        for client in pending_clients:
            client_id = client['client_id']
            last_activity = client['last_activity']

            client_tg_info = None
            try:
                client_tg_info = await context.bot.get_chat(client_id)
            except Exception:
                pass

            client_name = client_tg_info.full_name if client_tg_info else f"Клієнт (ID: {client_id})"

            response_text += f"👤 **{client_name}** (ID: `{client_id}`)\n"
            response_text += f"⏱️ Звернувся: {last_activity.strftime('%d.%m.%Y %H:%M:%S')}\n"

            bonus_acc = await create_or_get_bonus_account(client_id)
            if bonus_acc and bonus_acc.get('instagram_user_id'):
                response_text += "🔗 **Постійний клієнт (зв'язаний IG)**\n"

            response_text += "\n"
            client_state = await get_client_state(client_id)
            if not client_state or not client_state.get("current_manager_id"):
                keyboard_buttons.append([InlineKeyboardButton(f"🛠 Взяти {client_name}", callback_data=f"take_{client_id}")])
            else:
                manager_info = None
                try:
                    manager_info = await context.bot.get_chat(client_state.get("current_manager_id"))
                except Exception:
                    pass
                manager_name = manager_info.full_name if manager_info else f"Менеджер (ID: {client_state.get('current_manager_id')})"
                # Можливо, замість url=f"tg://user?id=..." краще просто показати ім'я менеджера без посилання
                keyboard_buttons.append([InlineKeyboardButton(f"👨‍💻 В роботі у {manager_name}", callback_data=f"taken_{client_id}")])

        response_text += "\n"
    else:
        response_text += "--- **Наразі немає очікуючих запитів.** ---\n\n"

    keyboard_buttons.append([InlineKeyboardButton("🔄 Оновити список", callback_data="refresh_new_requests")])
    keyboard_buttons.append([InlineKeyboardButton("🔙 Назад до запитів", callback_data="back_to_manager_requests_menu_inline")])

    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    await update.message.reply_text(response_text, parse_mode="Markdown", reply_markup=reply_markup)
    logger.info(f"Менеджер {uid} запросив нові запити.")

async def processed_orders_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != MANAGER_ID:
        await update.message.reply_text("❌ Ця команда доступна лише для менеджера.")
        return

    context.user_data["manager_menu_state"] = "processed_orders_list"
    orders = await get_all_active_orders()

    response_text = "✅ **Оформлені замовлення (не виконані):**\n\n"

    if orders:
        for order in orders:
            order_id = order.get('order_id', 'N/A')
            response_text += f"📦 Номер: `{order_id}`\n"
            response_text += f"👤 Клієнт ID: `{order.get('client_id', 'N/A')}`\n"
            response_text += f"📊 Статус: **{order.get('status', 'N/A')}**\n"
            if order.get('price') is not None:
                response_text += f"💰 Ціна: **{order['price']:.2f} грн**\n"
            if order.get('description'):
                response_text += f"📝 Опис: {order['description']}\n"
            response_text += "\n"

        response_text += "Щоб змінити статус замовлення, натисніть кнопку '✏️ Змінити статус замовлення' та введіть номер замовлення."
    else:
        response_text += "--- **Наразі немає активних замовлень.** ---\n"

    reply_markup = manager_processed_orders_menu

    await update.message.reply_text(response_text, parse_mode="Markdown", reply_markup=reply_markup)
    logger.info(f"Менеджер {uid} переглянув оформлені замовлення.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text

    client_db_state = await get_client_state(uid)
    if not client_db_state:
        await add_client_state(uid, is_active=False, is_notified=False)
        client_db_state = await get_client_state(uid) # Re-fetch after creation
        if not client_db_state:
            logger.error(f"Не вдалося ініціалізувати або отримати стан клієнта {uid}")
            await update.message.reply_text("Вибачте, сталася помилка. Спробуйте ще раз або зверніться до підтримки.")
            return

    # --- ЛОГІКА ДЛЯ МЕНЕДЖЕРА ---
    if uid == MANAGER_ID:
        # Перевірка на режими очікування введення
        if context.user_data.get("manager_awaiting_order_id_for_status_change"):
            order_id_to_change = text.strip()
            order_details = await get_order_details(order_id_to_change)
            if order_details:
                context.user_data["temp_order_id_for_status_change"] = order_id_to_change
                context.user_data["manager_awaiting_order_id_for_status_change"] = False
                context.user_data["manager_menu_state"] = "awaiting_status_selection"
                await update.message.reply_text(
                    f"📦 Замовлення `{order_id_to_change}`. Поточний статус: **{order_details.get('status', 'N/A')}**\n"
                    "Оберіть новий статус:",
                    parse_mode="Markdown",
                    reply_markup=order_status_change_menu
                )
                logger.info(f"Менеджер {uid} перейшов до зміни статусу замовлення {order_id_to_change}.")
            else:
                await update.message.reply_text("❌ Замовлення з таким номером не знайдено. Спробуйте ще раз або натисніть 'Назад'.", reply_markup=back_button)
                logger.warning(f"Менеджер {uid} ввів неіснуючий номер замовлення '{order_id_to_change}' для зміни статусу.")
            return

        elif context.user_data.get("manager_awaiting_balance_client_id"):
            try:
                target_tg_id = int(text.strip())
                bonus_acc = await create_or_get_bonus_account(target_tg_id)
                if not bonus_acc:
                    await update.message.reply_text(f"❌ Не вдалося знайти/створити бонусний акаунт для клієнта (ID: `{target_tg_id}`). Спробуйте ще раз або натисніть 'Назад'.", parse_mode="Markdown", reply_markup=back_button)
                    logger.warning(f"Менеджер {uid} ввів неіснуючий TG ID {target_tg_id} для зміни балансу.")
                    return

                context.user_data["temp_client_id_for_balance"] = target_tg_id
                context.user_data["manager_awaiting_balance_client_id"] = False
                context.user_data["manager_awaiting_balance_amount"] = True
                await update.message.reply_text(
                    f"✅ ID клієнта `{target_tg_id}` прийнято.\n"
                    "🔢 Тепер введіть суму або новий баланс.\n"
                    "Наприклад: `100` (додати 100), `-50` (списати 50), `=200` (встановити 200).",
                    parse_mode="Markdown",
                    reply_markup=back_button
                )
                logger.info(f"Менеджер {uid} перейшов до введення суми для клієнта {target_tg_id}.")
                return
            except ValueError:
                await update.message.reply_text("❌ Невірний формат ID клієнта. Введіть лише числове ID.", reply_markup=back_button)
                logger.warning(f"Менеджер {uid} ввів невірний формат ID для зміни балансу: '{text}'.")
                return

        elif context.user_data.get("manager_awaiting_balance_amount"):
            target_tg_id = context.user_data.get("temp_client_id_for_balance")
            if not target_tg_id:
                await update.message.reply_text("❌ Виникла внутрішня помилка. Спробуйте почати зміну балансу знову.", reply_markup=manager_main_menu)
                context.user_data.pop("manager_awaiting_balance_amount", None)
                context.user_data.pop("temp_client_id_for_balance", None)
                return

            try:
                amount_str = text.strip()
                if amount_str == "🔙 Назад": # Handle back button here again, as it's a specific mode
                    context.user_data.pop("manager_awaiting_balance_amount", None)
                    context.user_data.pop("temp_client_id_for_balance", None)
                    await update.message.reply_text("🔙 Повертаємось до меню менеджера.", reply_markup=manager_main_menu)
                    logger.info(f"Менеджер {uid} повернувся до меню менеджера з режиму введення суми.")
                    return

                # Save original context.args if needed later
                original_context_args = context.args[:] if context.args else []

                if amount_str.startswith("="):
                    processed_amount_str = amount_str[1:]
                    context.args = [str(target_tg_id), processed_amount_str]
                    await set_bonus_command_manager(update, context)
                else:
                    context.args = [str(target_tg_id), amount_str]
                    await add_bonus_command_manager(update, context)

                context.args = original_context_args # Restore original args
                context.user_data.pop("manager_awaiting_balance_amount", None)
                context.user_data.pop("temp_client_id_for_balance", None)
                await update.message.reply_text("✅ Операцію з балансом виконано.", reply_markup=manager_main_menu)
                logger.info(f"Менеджер {uid} змінив баланс клієнта {target_tg_id} через текстовий ввід.")
                return
            except ValueError:
                await update.message.reply_text("❌ Невірний формат суми. Введіть число (наприклад, `100`, `-50`, `=200`).", parse_mode="Markdown", reply_markup=back_button)
                logger.warning(f"Менеджер {uid} ввів невірний формат суми для зміни балансу: '{text}'.")
                return
            except Exception as e:
                logger.error(f"Помилка при обробці суми балансу від менеджера: {e}")
                await update.message.reply_text(f"Виникла непередбачена помилка: {e}", reply_markup=manager_main_menu)
                context.user_data.pop("manager_awaiting_balance_amount", None)
                context.user_data.pop("temp_client_id_for_balance", None)
                return

        elif context.user_data.get("manager_awaiting_client_info_id"):
            try:
                target_tg_id = int(text.strip())
                await client_info_command(update, context, target_id_from_handler=target_tg_id)

                context.user_data.pop("manager_awaiting_client_info_id", None)
                await update.message.reply_text("✅ Інформація про клієнта надана.", reply_markup=manager_main_menu)
                logger.info(f"Менеджер {uid} отримав інфо про клієнта {target_tg_id}.")
                return
            except ValueError:
                await update.message.reply_text("❌ Невірний формат ID. Введіть лише числове Telegram ID.", reply_markup=manager_main_menu)
                logger.warning(f"Менеджер {uid} ввів невірний формат ID для інфо про клієнта: '{text}'.")
                return

        elif context.user_data.get("manager_awaiting_order_price"):
            client_id_for_order = context.user_data.get("temp_order_client_id")
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID) # Re-fetch to be safe

            if not client_id_for_order or not manager_current_dialog == client_id_for_order:
                await update.message.reply_text("❌ Виникла внутрішня помилка або діалог з клієнтом змінився. Спробуйте оформити замовлення знову.", reply_markup=active_dialog_client_buttons)
                context.user_data.pop("manager_awaiting_order_price", None)
                context.user_data.pop("temp_order_client_id", None)
                return
            try:
                price = Decimal(text.strip()) # Змінено float на Decimal
                if price <= Decimal('0'): # Змінено 0 на Decimal('0')
                    await update.message.reply_text("❌ Ціна має бути позитивним числом. Введіть коректну ціну:", reply_markup=back_button)
                    return
                context.user_data["temp_order_price"] = price
                context.user_data["manager_awaiting_order_price"] = False
                context.user_data["manager_awaiting_order_description"] = True
                await update.message.reply_text("📝 Тепер введіть опис замовлення (наприклад, код запчастини, тип, бренд):", reply_markup=back_button)
                logger.info(f"Менеджер {uid} ввів ціну {price} для замовлення клієнта {client_id_for_order}.")
                return
            except ValueError:
                await update.message.reply_text("❌ Невірний формат ціни. Введіть число (наприклад, `1500.50`):", reply_markup=back_button)
                logger.warning(f"Менеджер {uid} ввів невірний формат ціни: '{text}'.")
                return

        elif context.user_data.get("manager_awaiting_order_description"):
            client_id_for_order = context.user_data.get("temp_order_client_id")
            price_for_order = context.user_data.get("temp_order_price")
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID) # Re-fetch to be safe

            if not client_id_for_order or price_for_order is None or not manager_current_dialog == client_id_for_order:
                await update.message.reply_text("❌ Виникла внутрішня помилка або діалог з клієнтом змінився. Спробуйте оформити замовлення знову.", reply_markup=active_dialog_client_buttons)
                context.user_data.pop("manager_awaiting_order_description", None)
                context.user_data.pop("temp_order_client_id", None)
                context.user_data.pop("temp_order_price", None)
                return

            description = text.strip()
            # Генерація унікального ID замовлення
            order_id_val = f"{random.randint(100000, 999999)}{str(client_id_for_order)[-4:]}"
            await add_order(order_id_val, client_id_for_order, "🔄 Комплектування замовлення", price_for_order, description)

            try:
                await context.bot.send_message(client_id_for_order, f"📦 Ваше замовлення сформоване!\nНомер: `{order_id_val}`\n💰 Ціна: **{price_for_order:.2f} грн**\n📝 Опис: {description}", parse_mode="Markdown", reply_markup=end_dialog_client_button)
            except Exception as e:
                logger.warning(f"Не вдалося надіслати повідомлення клієнту {client_id_for_order} про оформлення замовлення: {e}")

            try:
                await context.bot.send_message(uid, # Надсилаємо саме менеджеру, який оформив
                                               f"📦 Замовлення оформлено!\nНомер: `{order_id_val}`\n💰 Ціна: **{price_for_order:.2f} грн**\n📝 Опис: {description}",
                                               parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Не вдалося надіслати повідомлення менеджеру {uid} про оформлення замовлення: {e}")

            await update.message.reply_text(f"✅ Замовлення `{order_id_val}` оформлено для клієнта `{client_id_for_order}`.", parse_mode="Markdown", reply_markup=active_dialog_client_buttons)
            logger.info(f"Менеджер {uid} оформив замовлення {order_id_val} для клієнта {client_id_for_order} з ціною {price_for_order} та описом '{description}'.")

            context.user_data.pop("manager_awaiting_order_description", None)
            context.user_data.pop("temp_order_client_id", None)
            context.user_data.pop("temp_order_price", None)
            return

        # Обробка кнопок "Змінити статус замовлення" (вибір нового статусу)
        elif text in ["🔄 Комплектування", "🚚 З ЄС", "📮 По Україні", "✅ Виконано"] and context.user_data.get("manager_menu_state") == "awaiting_status_selection":
            order_id_to_change = context.user_data.get("temp_order_id_for_status_change")
            if not order_id_to_change:
                await update.message.reply_text("❌ Виникла помилка: не знайдено ID замовлення для зміни статусу. Спробуйте ще раз.", reply_markup=manager_main_menu)
                context.user_data.pop("manager_awaiting_order_id_for_status_change", None)
                context.user_data.pop("temp_order_id_for_status_change", None)
                context.user_data["manager_menu_state"] = "main"
                return

            status_map = {
                "🔄 Комплектування": "🔄 Комплектування замовлення",
                "🚚 З ЄС": "🚚 Очікуємо доставку з ЄС",
                "📮 По Україні": "📮 Доставка по Україні",
                "✅ Виконано": "✅ Замовлення виконано"
            }
            new_status = status_map.get(text)
            if new_status:
                await update_order_status(order_id_to_change, new_status)
                client_id_from_order = await get_client_id_by_order_id(order_id_to_change)
                if client_id_from_order:
                    try:
                        await context.bot.send_message(client_id_from_order, f"📦 Новий статус вашого замовлення:\n**{new_status}**", parse_mode="Markdown")
                        logger.info(f"Клієнту {client_id_from_order} надіслано оновлення статусу замовлення {order_id_to_change}.")
                    except Exception as e:
                        logger.warning(f"Не вдалося надіслати сповіщення клієнту {client_id_from_order} про оновлення статусу: {e}")

                await update.message.reply_text(f"✅ Статус замовлення `{order_id_to_change}` оновлено на: **{new_status}**", parse_mode="Markdown", reply_markup=manager_main_menu)
                context.user_data.pop("manager_awaiting_order_id_for_status_change", None)
                context.user_data.pop("temp_order_id_for_status_change", None)
                context.user_data["manager_menu_state"] = "main"
                logger.info(f"Менеджер {uid} оновив статус замовлення {order_id_to_change} на {new_status}.")
            else:
                await update.message.reply_text("Невідомий статус. Будь ласка, оберіть зі списку.", reply_markup=order_status_change_menu)
                logger.warning(f"Менеджер {uid} спробував встановити невідомий статус для замовлення {order_id_to_change}.")
            return

        # Обробка кнопки "Назад" у режимі очікування номера замовлення для зміни статусу АБО вибору статусу
        elif (context.user_data.get("manager_awaiting_order_id_for_status_change") or \
              context.user_data.get("manager_menu_state") == "awaiting_status_selection") and text == "🔙 Назад":
            context.user_data.pop("manager_awaiting_order_id_for_status_change", None)
            context.user_data.pop("temp_order_id_for_status_change", None)
            context.user_data["manager_menu_state"] = "requests_menu" # Повертаємо до меню запитів
            await processed_orders_command(update, context) # Показуємо знову список замовлень, з якого йшли
            logger.info(f"Менеджер {uid} повернувся до списку оформлених замовлень.")
            return

        # --- ОБРОБКА ПОВІДОМЛЕНЬ В АКТИВНОМУ ДІАЛОЗІ МЕНЕДЖЕРА ---
        manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID)
        if manager_current_dialog and \
           context.user_data.get("manager_menu_state") == "active_dialog":
            client_id_to_reply = manager_current_dialog
            target_client_state = await get_client_state(client_id_to_reply)

            # Проверяем, является ли сообщение одной из кнопок активного диалога менеджера
            if text not in ["📦 Оформити замовлення", "📂 Архів повідомлень", "📜 Замовлення клієнта", "❌ Завершити діалог", "🔙 Назад"]:
                if target_client_state and target_client_state.get("is_active"):
                    await add_client_message(client_id_to_reply, "manager", text)
                    try:
                        await context.bot.send_message(client_id_to_reply, text)
                        await update.message.reply_text(f"✅ Відповідь надіслано клієнту (ID: `{client_id_to_reply}`).", parse_mode="Markdown", reply_markup=active_dialog_client_buttons)
                        logger.info(f"Менеджер {uid} відповів клієнту {client_id_to_reply}.")
                    except Exception as e:
                        logger.warning(f"Не вдалося надіслати повідомлення клієнту {client_id_to_reply}: {e}")
                        await update.message.reply_text(f"❌ Не вдалося надіслати повідомлення клієнту (можливо, він заблокував бота).", reply_markup=active_dialog_client_buttons)
                else:
                    await update.message.reply_text(f"❌ Діалог з клієнтом (ID: `{client_id_to_reply}`) вже завершено.", parse_mode="Markdown", reply_markup=manager_main_menu)
                    await update_manager_active_dialog(MANAGER_ID, None)
                    logger.warning(f"Менеджер {uid} намагався відповісти неактивному клієнту {client_id_to_reply}.")
                return

        # --- ОБРОБКА КНОПОК МЕНЕДЖЕРА ---
        if text == "📊 Запити клієнтів":
            await manager_requests_menu_handler(update, context)
        elif text == "📝 Змінити баланс":
            context.user_data["manager_awaiting_balance_client_id"] = True
            await update.message.reply_text(
                "🔢 Будь ласка, введіть Telegram ID клієнта, баланс якого ви хочете змінити:",
                reply_markup=back_button
            )
            logger.info(f"Менеджер {uid} увійшов в режим зміни балансу (очікує ID клієнта).")
        elif text == "📤 Експорт замовлень":
            path = await export_orders_to_excel()
            if path:
                try:
                    await context.bot.send_document(uid, document=open(path, "rb"))
                except Exception as e:
                    logger.error(f"Не вдалося надіслати файл експорту менеджеру {uid}: {e}")
                    await update.message.reply_text("❌ Виникла помилка при відправці файлу.", reply_markup=manager_main_menu)
                    return
                os.remove(path)
                logger.info(f"Менеджер {uid} експортував замовлення в Excel.")
                await update.message.reply_text("✅ Замовлення експортовано.", reply_markup=manager_main_menu)
            else:
                await update.message.reply_text("📭 Немає замовлень для експорту.", reply_markup=manager_main_menu)
        elif text == "🔍 Інфо по клієнту":
            context.user_data["manager_awaiting_client_info_id"] = True
            await update.message.reply_text("🔢 Введіть Telegram ID клієнта для отримання інформації:", reply_markup=back_button)
            logger.info(f"Менеджер {uid} увійшов в режим запиту інфо про клієнта.")

        # --- НОВІ КНОПКИ ПІДМЕНЮ "ЗАПИТИ КЛІЄНТІВ" ---
        elif text == "💬 Активний діалог" and context.user_data.get("manager_menu_state") == "requests_menu":
            await active_dialog_details_handler(update, context)
        elif text == "📨 Нові запити" and context.user_data.get("manager_menu_state") == "requests_menu":
            await new_requests_command(update, context)
        elif text == "✅ Оформлені замовлення" and context.user_data.get("manager_menu_state") == "requests_menu":
            await processed_orders_command(update, context)

        # --- КНОПКИ В МЕНЮ АКТИВНОГО ДІАЛОГУ ---
        elif text == "📦 Оформити замовлення" and context.user_data.get("manager_menu_state") == "active_dialog":
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID)
            if manager_current_dialog:
                context.user_data["manager_awaiting_order_price"] = True
                context.user_data["temp_order_client_id"] = manager_current_dialog
                await update.message.reply_text("🔢 Будь ласка, введіть ціну замовлення (число, наприклад, `1250.75`):", parse_mode="Markdown", reply_markup=back_button)
                logger.info(f"Менеджер {uid} ініціював оформлення замовлення для клієнта {manager_current_dialog}.")
            else:
                await update.message.reply_text("❌ Для оформлення замовлення спочатку візьміть клієнта в роботу.", reply_markup=active_dialog_client_buttons)
        elif text == "📂 Архів повідомлень" and context.user_data.get("manager_menu_state") == "active_dialog":
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID)
            if manager_current_dialog:
                await send_dialog_archive(manager_current_dialog, context)
                await update.message.reply_text("✅ Архів повідомлень надіслано.", reply_markup=active_dialog_client_buttons)
            else:
                await update.message.reply_text("❌ Немає активного діалогу для перегляду архіву.", reply_markup=active_dialog_client_buttons)
        elif text == "📜 Замовлення клієнта" and context.user_data.get("manager_menu_state") == "active_dialog":
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID)
            if manager_current_dialog:
                client_orders = await get_client_orders(manager_current_dialog)
                if client_orders:
                    orders_text = f"📜 **Історія замовлень клієнта (ID: `{manager_current_dialog}`):**\n\n"
                    for order in client_orders:
                        order_id = order.get('order_id', 'N/A')
                        orders_text += f"📦 Номер: `{order_id}`\n"
                        orders_text += f"📊 Статус: **{order.get('status', 'N/A')}**\n"
                        if order.get('price') is not None:
                            orders_text += f"💰 Ціна: **{order['price']:.2f} грн**\n"
                        if order.get('description'):
                            orders_text += f"📝 Опис: {order['description']}\n"
                        orders_text += f"📅 Дата: {order.get('created_at', datetime.now()).strftime('%d.%m.%Y %H:%M:%S')}\n"
                        orders_text += "\n"
                    await update.message.reply_text(orders_text, parse_mode="Markdown", reply_markup=active_dialog_client_buttons)
                else:
                    await update.message.reply_text(f"📭 У клієнта (ID: `{manager_current_dialog}`) немає оформлених замовлень.", parse_mode="Markdown", reply_markup=active_dialog_client_buttons)
                logger.info(f"Менеджер {uid} переглянув замовлення клієнта {manager_current_dialog}.")
            else:
                await update.message.reply_text("❌ Немає активного діалогу для перегляду замовлень.", reply_markup=active_dialog_client_buttons)
        elif text == "❌ Завершити діалог" and context.user_data.get("manager_menu_state") == "active_dialog":
            manager_current_dialog = await get_manager_active_dialogs(MANAGER_ID)
            if manager_current_dialog:
                await close_client_dialog(manager_current_dialog, context, "менеджером (з меню активного діалогу)")
                await update.message.reply_text(f"✅ Активний діалог з клієнтом (ID: `{manager_current_dialog}`) завершено.", parse_mode="Markdown", reply_markup=manager_main_menu)
                context.user_data["manager_menu_state"] = "main"
            else:
                await update.message.reply_text("❌ Наразі немає активного діалогу для завершення.", reply_markup=active_dialog_client_buttons)
            logger.info(f"Менеджер {uid} завершив діалог через кнопку в активному діалозі.")

        # --- КНОПКИ В МЕНЮ ОФОРМЛЕНИХ ЗАМОВЛЕНЬ ---
        elif text == "✏️ Змінити статус замовлення" and context.user_data.get("manager_menu_state") == "processed_orders_list":
            context.user_data["manager_awaiting_order_id_for_status_change"] = True
            await update.message.reply_text(
                "🔢 Будь ласка, введіть номер замовлення, статус якого ви хочете змінити:",
                reply_markup=back_button
            )
            logger.info(f"Менеджер {uid} ініціював зміну статусу замовлення.")

        # --- ОБРОБКА КНОПКИ "НАЗАД" В МЕНЮ МЕНЕДЖЕРА ТА ЇЇ ПІДМЕНЮ ---
        elif text == "🔙 Назад":
            current_manager_state = context.user_data.get("manager_menu_state")

            if context.user_data.get("manager_awaiting_balance_client_id") or \
               context.user_data.get("manager_awaiting_balance_amount") or \
               context.user_data.get("manager_awaiting_client_info_id") or \
               context.user_data.get("manager_awaiting_order_price") or \
               context.user_data.get("manager_awaiting_order_description"):
                # Якщо ми були в режимі очікування вводу, повертаємось в головне меню менеджера
                context.user_data.pop("manager_awaiting_balance_client_id", None)
                context.user_data.pop("manager_awaiting_balance_amount", None)
                context.user_data.pop("temp_client_id_for_balance", None)
                context.user_data.pop("manager_awaiting_client_info_id", None)
                context.user_data.pop("manager_awaiting_order_price", None)
                context.user_data.pop("manager_awaiting_order_description", None)
                context.user_data.pop("temp_order_client_id", None)
                context.user_data.pop("temp_order_price", None)
                context.user_data["manager_menu_state"] = "main"
                await update.message.reply_text("🔙 Повертаємось до головного меню менеджера.", reply_markup=manager_main_menu)
                logger.info(f"Менеджер {uid} повернувся до головного меню менеджера з режиму очікування вводу.")
            elif current_manager_state == "requests_menu":
                context.user_data["manager_menu_state"] = "main"
                await update.message.reply_text("🔙 Повертаємось до головного меню менеджера.", reply_markup=manager_main_menu)
                logger.info(f"Менеджер {uid} повернувся до головного меню менеджера з меню запитів.")
            elif current_manager_state == "active_dialog":
                context.user_data["manager_menu_state"] = "requests_menu"
                await update.message.reply_text("🔙 Повертаємось до меню запитів.", reply_markup=manager_requests_menu)
                logger.info(f"Менеджер {uid} повернувся до меню запитів з активного діалогу.")
            elif current_manager_state == "new_requests_list":
                context.user_data["manager_menu_state"] = "requests_menu"
                await update.message.reply_text("🔙 Повертаємось до меню запитів.", reply_markup=manager_requests_menu)
                logger.info(f"Менеджер {uid} повернувся до меню запитів з нових запитів.")
            elif current_manager_state == "processed_orders_list":
                context.user_data["manager_menu_state"] = "requests_menu"
                await update.message.reply_text("🔙 Повертаємось до меню запитів.", reply_markup=manager_requests_menu)
                logger.info(f"Менеджер {uid} повернувся до меню запитів з оформлених замовлень.")
            else:
                await update.message.reply_text("🤖 Я вас не розумію. Скористайтесь меню менеджера.", reply_markup=manager_main_menu)
                logger.info(f"Менеджер {uid} надіслав нерозпізнане повідомлення '{text}' у неактивному діалозі.")
            return

        # Якщо менеджер ввів щось, що не є кнопкою і не є частиною режиму очікування вводу
        else:
            await update.message.reply_text("🤖 Я вас не розумію. Скористайтесь меню менеджера.", reply_markup=manager_main_menu)
            logger.info(f"Менеджер {uid} надіслав нерозпізнане повідомлення '{text}' у неактивному діалозі.")
        return # Важливо вийти з функції, якщо повідомлення оброблене менеджером

    # --- ЛОГІКА ДЛЯ КЛІЄНТА ---
    if text == "📦 Зробити запит/замовлення":
        # Якщо клієнт вже був в активному діалозі, завершуємо його перед створенням нового
        if client_db_state.get("is_active"):
            await close_client_dialog(uid, context, "автоматично (новий запит)")

        await update_client_active_status(uid, is_active=True)
        await update_client_notified_status(uid, is_notified=False) # Скидаємо прапорець сповіщення
        await update.message.reply_text(
            "✍️ Напишіть повідомлення. Менеджер відповість найближчим часом.",
            reply_markup=end_dialog_client_button
        )
        context.user_data["client_menu_state"] = "active_dialog"
        logger.info(f"Клієнт {uid} ініціював запит/замовлення.")

    elif text == "❌ Завершити діалог":
        await close_client_dialog(uid, context, "клієнтом")
        context.user_data["client_menu_state"] = "main"
        logger.info(f"Клієнт {uid} натиснув 'Завершити діалог'.")

    elif text == "ℹ️ Інформація":
        await update.message.reply_text("ℹ️ Виберіть:", reply_markup=info_menu)
        context.user_data["client_menu_state"] = "info"
        logger.info(f"Клієнт {uid} перейшов в меню 'Інформація'.")

    elif text == "🎁 Мої бонуси":
        await update.message.reply_text(
            "🎁 Виберіть дію з бонусами:",
            reply_markup=bonus_main_menu
        )
        context.user_data["awaiting_bonus_code"] = False # Скидаємо цей стан
        context.user_data["client_menu_state"] = "bonus"
        logger.info(f"Клієнт {uid} перейшов в меню 'Мої бонуси'.")

    elif text == "💰 Перевірити баланс":
        bonus_acc = await create_or_get_bonus_account(uid)
        balance = bonus_acc.get("bonus_balance", Decimal('0.00')) if bonus_acc else Decimal('0.00') # Змінено 0.00 на Decimal('0.00')
        await update.message.reply_text(
            f"💰 Ваш поточний баланс бонусів: **{balance:.2f} грн**",
            parse_mode="Markdown",
            reply_markup=bonus_main_menu
        )
        logger.info(f"Клієнт {uid} перевірив баланс бонусів.")

    elif text == "⬆️ Ввести бонус-код":
        context.user_data["awaiting_bonus_code"] = True
        context.user_data["client_menu_state"] = "awaiting_bonus_code_input"
        await update.message.reply_text(
            "🔢 Будь ласка, введіть ваш бонусний код:",
            reply_markup=back_button
        )
        logger.info(f"Клієнт {uid} ініціював введення бонус-коду.")

    elif context.user_data.get("awaiting_bonus_code"):
        bonus_code_input = text.strip().upper()
        code_details = await get_bonus_code_details(bonus_code_input)

        if code_details and code_details["is_active"]:
            value = Decimal(code_details["value"]) # Переконайтеся, що значення з бази даних також обробляється як Decimal
            activated = await activate_bonus_code(code_details["id"], uid)
            if activated:
                await update_bonus_balance(uid, value)

                if code_details.get("assigned_to_ig_user_id"):
                    await link_instagram_to_telegram_account(uid, code_details["assigned_to_ig_user_id"])
                    logger.info(f"Зв'язано IG ID {code_details['assigned_to_ig_user_id']} з TG ID {uid} через активацію бонусу.")

                await update.message.reply_text(
                    f"🎉 Вітаємо! Код `{bonus_code_input}` успішно активовано! Вам нараховано **{value:.2f} грн** бонусів.",
                    parse_mode="Markdown",
                    reply_markup=main_menu
                )
                logger.info(f"Клієнт {uid} активував бонусний код '{bonus_code_input}' на {value} грн.")
            else:
                await update.message.reply_text(
                    "❌ Виникла помилка під час активації коду. Спробуйте пізніше або зверніться до підтримки.",
                    reply_markup=main_menu
                )
                logger.error(f"Не вдалося активувати код {bonus_code_input} для {uid}.")
        else:
            await update.message.reply_text(
                "❌ Невірний або вже використаний бонусний код. Перевірте та спробуйте ще раз.",
                reply_markup=main_menu
            )
            logger.warning(f"Клієнт {uid} ввів невірний/використаний бонусний код: '{bonus_code_input}'.")
        context.user_data.pop("awaiting_bonus_code", None)
        context.user_data["client_menu_state"] = "main"
        logger.info(f"Клієнт {uid} завершив введення бонус-коду.")

    elif text == "📦 Доставка":
        await update.message.reply_text(
            "📦 Замовлення їдуть з Європи\n⏱️ 3–5 робочих днів\n"
            "📮 Далі відправка Новою Поштою або іншим перевізником\n"
            "📬 Доставка згідно тарифів перевізника\n"
            "🚚 Безкоштовна доставка при замовленні від 3000 грн", reply_markup=back_button
        )
        context.user_data["client_menu_state"] = "info_delivery"
        logger.info(f"Клієнт {uid} переглянув 'Доставку'.")

    elif text == "📞 Контакти":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📲 Telegram", url="https://t.me/zapchastimarket69")],
            [InlineKeyboardButton("📸 Instagram", url="https://www.instagram.com/zapchastimarket69")],
        ])
        await update.message.reply_text("📲 Наші контакти:", reply_markup=keyboard)
        context.user_data["client_menu_state"] = "info_contacts"
        logger.info(f"Клієнт {uid} переглянув 'Контакти'.")

    elif text == "👥 Про нас":
        await update.message.reply_text(
            """🧰 Ми – Zapchasti Market 69.
🔩 Продаємо оригінальні автозапчастини з Європи
💸 Найкращі ціни
📍 Доставка по Україні
🔍 Підбір по VIN та коду запчастини
💬 Пиши — підберу як для себе!""",
            reply_markup=back_button
        )
        context.user_data["client_menu_state"] = "info_about_us"
        logger.info(f"Клієнт {uid} переглянув 'Про нас'.")

    elif text == "🔍 Перевірити замовлення":
        client_orders = await get_client_orders(uid)
        if client_orders:
            orders_text = "📜 **Ваша історія замовлень:**\n\n"
            for order in client_orders:
                order_id = order.get('order_id', 'N/A')
                orders_text += f"📦 Номер: `{order_id}`\n"
                orders_text += f"📊 Статус: **{order.get('status', 'N/A')}**\n"
                if order.get('price') is not None:
                    orders_text += f"💰 Ціна: **{order['price']:.2f} грн**\n"
                if order.get('description'):
                    orders_text += f"📝 Опис: {order['description']}\n"
                orders_text += f"📅 Дата: {order.get('created_at', datetime.now()).strftime('%d.%m.%Y %H:%M:%S')}\n"
                orders_text += "\n"
            await update.message.reply_text(orders_text, parse_mode="Markdown", reply_markup=main_menu)
            logger.info(f"Клієнт {uid} переглянув свою історію замовлень.")
        else:
            await update.message.reply_text("📭 У вас немає оформлених замовлень.", reply_markup=main_menu)
            logger.info(f"Клієнт {uid} не має оформлених замовлень.")
        context.user_data["client_menu_state"] = "main"
        return

    elif text == "🎯 Акція":
        await update.message.reply_text(
            "🎯 Акція: Безкоштовна доставка при замовленні пари гальмівних дисків\n"
            "📅 Термін дії: 01.07.2025 – 31.08.2025", reply_markup=back_button
        )
        context.user_data["client_menu_state"] = "promo"
        logger.info(f"Клієнт {uid} переглянув 'Акцію'.")

    elif text == "🔙 Назад":
        client_current_state = context.user_data.get("client_menu_state")

        if client_current_state == "awaiting_bonus_code_input":
            context.user_data.pop("awaiting_bonus_code", None)
            context.user_data["client_menu_state"] = "bonus"
            await update.message.reply_text("🔙 Повертаємось до меню бонусів.", reply_markup=bonus_main_menu)
            logger.info(f"Клієнт {uid} вийшов з режиму введення коду.")
        elif client_current_state in ["info_delivery", "info_contacts", "info_about_us"]:
            context.user_data["client_menu_state"] = "info"
            await update.message.reply_text("🔙 Повертаємось до меню інформації.", reply_markup=info_menu)
            logger.info(f"Клієнт {uid} повернувся до меню інформації.")
        elif client_current_state == "bonus":
            context.user_data["client_menu_state"] = "main"
            await update.message.reply_text("🔙 Повертаємось до головного меню.", reply_markup=main_menu)
            logger.info(f"Клієнт {uid} повернувся до головного меню з меню бонусів.")
        elif client_current_state == "info":
            context.user_data["client_menu_state"] = "main"
            await update.message.reply_text("🔙 Повертаємось до головного меню.", reply_markup=main_menu)
            logger.info(f"Клієнт {uid} повернувся до головного меню з меню інформації.")
        elif client_current_state == "promo":
            context.user_data["client_menu_state"] = "main"
            await update.message.reply_text("🔙 Повертаємось до головного меню.", reply_markup=main_menu)
            logger.info(f"Клієнт {uid} повернувся до головного меню з меню акції.")
        elif client_current_state == "active_dialog":
            await update.message.reply_text("Ви вже в активному діалозі. Щоб завершити, натисніть '❌ Завершити діалог'.", reply_markup=end_dialog_client_button)
            logger.info(f"Клієнт {uid} спробував натиснути 'Назад' в активному діалозі.")
        else:
            await update.message.reply_text("🤖 Я вас не розумію. Скористайтесь меню.", reply_markup=main_menu)
            context.user_data["client_menu_state"] = "main"
            logger.info(f"Клієнт {uid} надіслав нерозпізнане повідомлення '{text}' у невідомому стані.")

    # Обробка повідомлень клієнта, якщо діалог активний
    elif client_db_state.get("is_active"):
        await add_client_message(uid, "client", text)

        if client_db_state.get("current_manager_id"):
            manager_id = client_db_state.get("current_manager_id")
            try:
                await context.bot.send_message(
                    chat_id=manager_id,
                    text=f"✉️ **Від клієнта** {update.effective_user.full_name} (ID: `{uid}`):\n{text}",
                    parse_mode="Markdown"
                )
                logger.info(f"Повідомлення від активного клієнта {uid} переслано менеджеру {manager_id}.")
            except Exception as e:
                logger.warning(f"Не вдалося переслати повідомлення від клієнта {uid} до менеджера {manager_id}: {e}")
        else:
            # Надсилаємо сповіщення в групу, тільки якщо менеджер ще не був сповіщений
            if not client_db_state.get("is_notified"):
                await context.bot.send_message(
                    chat_id=MANAGER_GROUP_ID,
                    text=f"🔔 **Новий запит** від {update.effective_user.full_name} (ID: `{uid}`)\n"
                         f"Натисніть кнопку нижче, щоб взяти запит в роботу та переглянути історію діалогу.",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"🛠 Взяти {update.effective_user.full_name}", callback_data=f"take_{uid}")]])
                )
                await update_client_notified_status(uid, True)
                await update.message.reply_text("🔧 Дякуємо! Ваш запит отримано. Менеджер скоро зв'яжеться з вами.", reply_markup=end_dialog_client_button)
                logger.info(f"Нове повідомлення від клієнта {uid}. Менеджер сповіщений про запит.")
            else:
                # Якщо вже сповіщений, просто підтверджуємо отримання
                await update.message.reply_text("Ваше повідомлення отримано. Будь ласка, зачекайте відповіді менеджера.", reply_markup=end_dialog_client_button)
                logger.info(f"Клієнт {uid} надіслав повідомлення, менеджер вже був сповіщений.")


    else:
        # Якщо повідомлення не є командою і діалог не активний
        await update.message.reply_text("🤖 Я вас не розумію. Скористайтесь меню.", reply_markup=main_menu)
        context.user_data["client_menu_state"] = "main"
        logger.info(f"Клієнт {uid} надіслав нерозпізнане повідомлення '{text}' у невідомому стані.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    manager_id = query.from_user.id

    if manager_id != MANAGER_ID:
        await query.edit_message_text("❌ Ви не є менеджером.")
        return

    if data.startswith("take_"):
        client_id_to_take = int(data.split("_")[1])

        manager_current_dialog = await get_manager_active_dialogs(manager_id)
        if manager_current_dialog and manager_current_dialog != client_id_to_take:
            await query.message.reply_text(f"❌ У вас вже є активний діалог з клієнтом (ID: `{manager_current_dialog}`). Будь ласка, завершіть його перед тим, як брати нового клієнта.", parse_mode="Markdown", reply_markup=manager_main_menu)
            context.user_data["manager_menu_state"] = "main"
            logger.warning(f"Менеджер {manager_id} намагався взяти нового клієнта {client_id_to_take}, маючи активний діалог з {manager_current_dialog}.")
            return

        client_state = await get_client_state(client_id_to_take)
        if not client_state or not client_state.get("is_active"):
            await query.edit_message_text(f"❌ Діалог з клієнтом (ID: `{client_id_to_take}`) вже завершено або неактивний.", parse_mode="Markdown")
            logger.warning(f"Менеджер {manager_id} намагався взяти неактивний діалог {client_id_to_take}.")
            return

        if client_state.get("current_manager_id") and client_state.get("current_manager_id") != manager_id:
            manager_info = None
            try:
                manager_info = await context.bot.get_chat(client_state.get("current_manager_id"))
            except Exception:
                pass
            manager_name = manager_info.full_name if manager_info else f"Менеджер (ID: {client_state.get('current_manager_id')})"
            await query.answer(f"Цей клієнт вже в роботі у {manager_name}.", show_alert=True)
            return

        await update_client_manager(client_id_to_take, manager_id)
        await update_manager_active_dialog(manager_id, client_id_to_take)

        history_records = await get_client_messages(client_id_to_take)
        history_formatted = "\n".join([f"{rec['sender_type'].capitalize()}: {rec['message_text']}" for rec in history_records])
        history = history_formatted or "📭 Історія порожня"

        # Після того, як менеджер взяв діалог, видаляємо інлайн-кнопку "Взяти" з оригінального повідомлення
        try:
            # Оновлюємо текст повідомлення, щоб показати, що запит взято
            await query.edit_message_text(
                f"✅ **Новий запит** від {query.message.text.split('від ')[1].split(' (ID:')[0]} (ID: `{client_id_to_take}`)\n"
                f"***Запит взято в роботу менеджером {query.from_user.full_name}***",
                parse_mode="Markdown",
                reply_markup=None # Видаляємо всі кнопки з повідомлення
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                logger.info(f"Менеджер {manager_id}: Повідомлення про новий запит для клієнта {client_id_to_take} вже змінено.")
            else:
                logger.error(f"Помилка при оновленні повідомлення (прибирання кнопки 'Взяти') для менеджера {manager_id}: {e}")

        await context.bot.send_message(
            manager_id,
            f"✅ **Діалог з клієнтом (ID: `{client_id_to_take}`) взято в роботу.**\n"
            f"✉️ **Історія діалогу:**\n{history}\n\n"
            f"Тепер ви можете відповідати клієнту, просто надсилаючи повідомлення.", # Це повідомлення буде надсилатися менеджеру
            reply_markup=active_dialog_client_buttons,
            parse_mode="Markdown"
        )
        try:
            await context.bot.send_message(client_id_to_take, "🎉 Менеджер приєднався до діалогу!")
        except Exception as e:
            logger.warning(f"Не вдалося надіслати повідомлення клієнту {client_id_to_take} про приєднання менеджера: {e}")

        context.user_data["manager_menu_state"] = "active_dialog"
        logger.info(f"Менеджер {manager_id} взяв в роботу діалог з клієнтом {client_id_to_take}.")

    elif data.startswith("taken_"):
        client_id = int(data.split("_")[1])
        client_state = await get_client_state(client_id)
        if client_state and client_state.get("current_manager_id"):
            manager_info = None
            try:
                manager_info = await context.bot.get_chat(client_state.get("current_manager_id"))
            except Exception:
                pass
            manager_name = manager_info.full_name if manager_info else f"Менеджер (ID: {client_state.get('current_manager_id')})"
            await query.answer(f"Цей клієнт вже в роботі у {manager_name}.", show_alert=True)
        else:
            await query.answer("Цей запит більше не активний.", show_alert=True)

    elif data == "back_to_manager_requests_menu_inline":
        context.user_data["manager_menu_state"] = "requests_menu"
        await query.edit_message_text(
            "📊 Оберіть категорію запитів:",
            reply_markup=manager_requests_menu,
            parse_mode="Markdown"
        )
        logger.info(f"Менеджер {manager_id} повернувся до меню запитів з інлайн-списку нових запитів.")

    elif data == "refresh_new_requests":
        await new_requests_command(update, context)
        logger.info(f"Менеджер {manager_id} оновив список нових запитів.")

    # Ця callback_data більше не використовується, оскільки зміна статусу тепер через текстовий ввід
    # elif data == "change_order_status_prompt":
    #     context.user_data["manager_awaiting_order_id_for_status_change"] = True
    #     await query.edit_message_text(
    #         "🔢 Будь ласка, введіть номер замовлення, статус якого ви хочете змінити:",
    #         reply_markup=back_button
    #     )
    #     logger.info(f"Менеджер {manager_id} ініціював зміну статусу замовлення.")

    elif data == "export_excel": # Ця callback_data може бути викликана, якщо її використовують в інлайн-клавіатурі
        path = await export_orders_to_excel()
        if path:
            try:
                await context.bot.send_document(manager_id, document=open(path, "rb"))
            except Exception as e:
                logger.error(f"Не вдалося надіслати файл експорту менеджеру {manager_id}: {e}")
                await query.edit_message_text("❌ Виникла помилка при відправці файлу.", reply_markup=manager_main_menu)
                return

            os.remove(path)
            logger.info(f"Менеджер {manager_id} експортував замовлення в Excel.")
            await query.edit_message_text("✅ Замовлення експортовано.", reply_markup=manager_main_menu)
            context.user_data["manager_menu_state"] = "main"
        else:
            await context.bot.send_message(MANAGER_GROUP_ID, "📭 Немає замовлень для експорту") # Це повідомлення буде в групу
            await query.edit_message_text("📭 Немає замовлень для експорту.", reply_markup=manager_main_menu) # А це менеджерові
            context.user_data["manager_menu_state"] = "main"
            logger.info(f"Менеджер {manager_id} намагався експортувати замовлення, але їх немає.")

# --- API Fast_API та Uvicorn ---
fastapi_app = FastAPI(docs_url=None, redoc_url=None)

WEBHOOK_PATH = "/webhook"

@fastapi_app.on_event("startup")
async def startup_event():
    global telegram_app
    logger.info("FastAPI startup: Ініціалізація пулу БД...")
    await init_db_pool()

    logger.info("FastAPI startup: Ініціалізація Telegram Application...")
    telegram_app = Application.builder().token(TOKEN).build()
    await telegram_app.initialize()

    # Додавання обробників команд та повідомлень
    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CommandHandler("manager_menu", manager_menu_command, filters.User(MANAGER_ID)))
    telegram_app.add_handler(CommandHandler("client_info", client_info_command, filters.User(MANAGER_ID)))

    # Обробники кнопок меню менеджера
    telegram_app.add_handler(MessageHandler(filters.Regex("^📊 Запити клієнтів$") & filters.User(MANAGER_ID), manager_requests_menu_handler))
    telegram_app.add_handler(MessageHandler(filters.Regex("^📝 Змінити баланс$") & filters.User(MANAGER_ID), handle_message)) # handle_message буде обробляти вхід в режим очікування ID
    telegram_app.add_handler(MessageHandler(filters.Regex("^📤 Експорт замовлень$") & filters.User(MANAGER_ID), handle_message)) # handle_message буде викликати export_orders_to_excel
    telegram_app.add_handler(MessageHandler(filters.Regex("^🔍 Інфо по клієнту$") & filters.User(MANAGER_ID), handle_message)) # handle_message буде обробляти вхід в режим очікування ID

    # Обробники кнопок підменю "Запити клієнтів"
    telegram_app.add_handler(MessageHandler(filters.Regex("^💬 Активний діалог$") & filters.User(MANAGER_ID), active_dialog_details_handler))
    telegram_app.add_handler(MessageHandler(filters.Regex("^📨 Нові запити$") & filters.User(MANAGER_ID), new_requests_command))
    telegram_app.add_handler(MessageHandler(filters.Regex("^✅ Оформлені замовлення$") & filters.User(MANAGER_ID), processed_orders_command))

    # Обробники кнопок в активному діалозі менеджера
    telegram_app.add_handler(MessageHandler(filters.Regex("^📦 Оформити замовлення$") & filters.User(MANAGER_ID), handle_message))
    telegram_app.add_handler(MessageHandler(filters.Regex("^📂 Архів повідомлень$") & filters.User(MANAGER_ID), handle_message))
    telegram_app.add_handler(MessageHandler(filters.Regex("^📜 Замовлення клієнта$") & filters.User(MANAGER_ID), handle_message))
    telegram_app.add_handler(MessageHandler(filters.Regex("^❌ Завершити діалог$") & filters.User(MANAGER_ID), handle_message))

    # Обробник кнопки "Змінити статус замовлення" в меню оформлених замовлень
    telegram_app.add_handler(MessageHandler(filters.Regex("^✏️ Змінити статус замовлення$") & filters.User(MANAGER_ID), handle_message))
    # Обробники кнопок вибору нового статусу замовлення
    telegram_app.add_handler(MessageHandler(filters.Regex("^(🔄 Комплектування|🚚 З ЄС|📮 По Україні|✅ Виконано)$") & filters.User(MANAGER_ID), handle_message))

    # Обробники кнопок "Назад" для менеджера
    telegram_app.add_handler(MessageHandler(filters.Regex("^🔙 Назад$") & filters.User(MANAGER_ID), handle_message))

    # Обробники команд для зміни бонусів (менеджерські команди)
    telegram_app.add_handler(CommandHandler("add_bonus", add_bonus_command_manager, filters.User(MANAGER_ID)))
    telegram_app.add_handler(CommandHandler("set_bonus", set_bonus_command_manager, filters.User(MANAGER_ID)))
    telegram_app.add_handler(CommandHandler("get_balance", get_balance_command_manager, filters.User(MANAGER_ID)))

    # Загальний обробник текстових повідомлень (після всіх команд і специфічних кнопок)
    telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    # Обробник callback-запитів від інлайн-клавіатур
    telegram_app.add_handler(CallbackQueryHandler(handle_callback))

    full_webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

    logger.info(f"FastAPI startup: Встановлення вебхука на: {full_webhook_url}")
    await telegram_app.bot.set_webhook(url=full_webhook_url, secret_token=WEBHOOK_SECRET_TOKEN)
    logger.info(f"FastAPI startup: Вебхук встановлено.")

@fastapi_app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI shutdown: Закриття пулу БД...")
    await close_db_pool()
    logger.info("FastAPI shutdown: Закриття Telegram Application...")
    if telegram_app:
        await telegram_app.shutdown()

@fastapi_app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    x_telegram_bot_api_secret_token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if not WEBHOOK_SECRET_TOKEN or x_telegram_bot_api_secret_token != WEBHOOK_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid secret token")

    try:
        req_json = await request.json()
        update = Update.de_json(req_json, telegram_app.bot)

        await telegram_app.process_update(update)
        return Response(status_code=200)
    except Exception as e:
        logger.exception(f"Помилка при обробці вебхука: {e}")
        return Response(status_code=500)

@fastapi_app.get("/")
async def read_root():
    return {"status": "ok", "message": "Bot is running with webhook setup and secure secret token"}

# --- МЕНЕДЖЕРСЬКІ КОМАНДИ ДЛЯ БОНУСІВ (ОКРЕМІ ФУНКЦІЇ) ---
async def add_bonus_command_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Ця функція тепер може викликатися як з команди, так і з handle_message
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("Використання: `/add_bonus <Telegram_ID_клієнта> <сума_бонусів>`", parse_mode="Markdown")
        return

    try:
        target_tg_id = int(context.args[0])
        # ЗМІНА ТУТ: перетворюємо вхідну суму в Decimal
        amount = Decimal(context.args[1]) # Перетворення рядка на Decimal

        bonus_acc = await create_or_get_bonus_account(target_tg_id)
        if not bonus_acc:
            await update.message.reply_text(f"❌ Не вдалося знайти/створити бонусний акаунт для клієнта (ID: `{target_tg_id}`).", parse_mode="Markdown")
            return

        # current_balance має бути Decimal, якщо db повертає його так.
        current_balance = bonus_acc.get("bonus_balance", Decimal('0.00')) # Забезпечуємо, що значення за замовчуванням є Decimal
        new_balance = current_balance + amount # Тепер обидва операнди Decimal!

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE bonus_accounts SET bonus_balance = $2, last_updated = NOW() WHERE telegram_user_id = $1;",
                target_tg_id, new_balance
            )

        # Сповіщення менеджера про успішне оновлення
        if not context.user_data.get("manager_awaiting_balance_amount"): # Щоб уникнути дублювання, якщо викликано з handle_message
            await update.message.reply_text(f"✅ Баланс клієнта (ID: `{target_tg_id}`) оновлено на **{amount:.2f} грн**.\nНовий баланс: **{new_balance:.2f} грн**.", parse_mode="Markdown")

        # Сповіщення клієнта
        try:
            await context.bot.send_message(target_tg_id, f"🎉 Ваш бонусний баланс оновлено менеджером на **{amount:.2f} грн**.\nВаш новий баланс: **{new_balance:.2f} грн**.", parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Не вдалося надіслати сповіщення клієнту {target_tg_id} про оновлення балансу: {e}")
    except ValueError:
        await update.message.reply_text("Невірний формат ID клієнта або суми. Сума має бути числом (наприклад, `100.50` або `-20`).", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Помилка в add_bonus_command_manager: {e}")
        await update.message.reply_text(f"Виникла непередбачена помилка: {e}")

async def set_bonus_command_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Ця функція тепер може викликатися як з команди, так і з handle_message
    if not context.args or len(context.args) != 2:
        await update.message.reply_text("Використання: `/set_bonus <Telegram_ID_клієнта> <новий_баланс>`", parse_mode="Markdown")
        return

    try:
        target_tg_id = int(context.args[0])
        # ЗМІНА ТУТ: перетворюємо вхідний новий_баланс в Decimal
        new_balance = Decimal(context.args[1]) # Перетворення рядка на Decimal

        bonus_acc = await create_or_get_bonus_account(target_tg_id)
        if not bonus_acc:
            await update.message.reply_text(f"❌ Не вдалося знайти/створити бонусний акаунт для клієнта (ID: `{target_tg_id}`).", parse_mode="Markdown")
            return

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE bonus_accounts SET bonus_balance = $2, last_updated = NOW() WHERE telegram_user_id = $1;",
                target_tg_id, new_balance
            )

        # Сповіщення менеджера про успішне оновлення
        if not context.user_data.get("manager_awaiting_balance_amount"): # Щоб уникнути дублювання
            await update.message.reply_text(f"✅ Баланс клієнта (ID: `{target_tg_id}`) встановлено на: **{new_balance:.2f} грн**.", parse_mode="Markdown")

        # Сповіщення клієнта
        try:
            await context.bot.send_message(target_tg_id, f"🎉 Ваш бонусний баланс встановлено менеджером на **{new_balance:.2f} грн**.", parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Не вдалося надіслати сповіщення клієнту {target_tg_id} про встановлення балансу: {e}")
    except ValueError:
        await update.message.reply_text("Невірний формат ID клієнта або суми. Сума має бути числом (наприклад, `100.50`).", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Помилка в set_bonus_command_manager: {e}")
        await update.message.reply_text(f"Виникла непередбачена помилка: {e}")

async def get_balance_command_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("Використання: `/get_balance <Telegram_ID_клієнта>`", parse_mode="Markdown")
        return

    try:
        target_tg_id = int(context.args[0])

        bonus_acc = await create_or_get_bonus_account(target_tg_id)
        balance = bonus_acc.get("bonus_balance", Decimal('0.00')) if bonus_acc else Decimal('0.00') # Змінено 0.00 на Decimal('0.00')

        await update.message.reply_text(f"💰 Баланс бонусів клієнта (ID: `{target_tg_id}`): **{balance:.2f} грн**.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("Невірний формат ID клієнта. Використання: `/get_balance <Telegram_ID_клієнта>`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Помилка в get_balance_command_manager: {e}")
        await update.message.reply_text(f"Виникла непередбачена помилка: {e}")

if __name__ == "__main__":
    logger.info("Запуск Uvicorn сервера...")
    uvicorn.run(fastapi_app, host="0.0.0.0", port=WEB_SERVER_PORT)
