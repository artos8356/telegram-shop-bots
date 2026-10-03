import os
import json
import glob
import logging
import asyncio
import sqlite3
from datetime import datetime

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeChat,
)
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

DEFAULT_CONFIG = {
    "bot_token": "ВСТАВЬ_ТОКЕН_СЮДА",
    "admin_chat_id": 0,
    "shop_name": "Магазин",
    "support_contact": "@your_support_username",
    "welcome_footer": "Бот создан на заказ",
    "reviews_url": "https://t.me/your_reviews_channel",
    "payment_requisites": "Реквизиты не заполнены — впишите в config.json поле payment_requisites",
    "reminder_delay_seconds": 3600,
    "keep_backups": 14,
    "backup_interval_seconds": 86400,
}


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        raise SystemExit(
            f"\nСоздан файл config.json рядом с ботом ({CONFIG_FILE}).\n"
            "Заполните в нём bot_token, admin_chat_id, payment_requisites и остальные поля, "
            "затем запустите бота снова."
        )

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    for key, value in DEFAULT_CONFIG.items():
        cfg.setdefault(key, value)

    cfg["bot_token"] = os.environ.get("BOT_TOKEN", cfg["bot_token"])
    cfg["admin_chat_id"] = int(os.environ.get("ADMIN_CHAT_ID", cfg["admin_chat_id"]))

    if cfg["bot_token"] == DEFAULT_CONFIG["bot_token"] or not cfg["bot_token"]:
        raise SystemExit(f"\nВ config.json не заполнен bot_token. Откройте {CONFIG_FILE}.")
    if not cfg["admin_chat_id"]:
        raise SystemExit(f"\nВ config.json не заполнен admin_chat_id. Откройте {CONFIG_FILE}.")

    return cfg


CONFIG = load_config()

TOKEN = CONFIG["bot_token"]
ADMIN_CHAT_ID = CONFIG["admin_chat_id"]
SHOP_NAME = CONFIG["shop_name"]
SUPPORT_CONTACT = CONFIG["support_contact"]
WELCOME_FOOTER = CONFIG["welcome_footer"]
PAYMENT_REQUISITES = CONFIG["payment_requisites"]
REMINDER_DELAY_SECONDS = CONFIG["reminder_delay_seconds"]
KEEP_BACKUPS = CONFIG["keep_backups"]
BACKUP_INTERVAL_SECONDS = CONFIG["backup_interval_seconds"]

DB_FILE = os.path.join(BASE_DIR, "clients.db")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

DB_LOCK = asyncio.Lock()

# Состояния для админ-диалога добавления товара
ITEM_PHOTO, ITEM_NAME, ITEM_PRICE, ITEM_MEASUREMENTS, ITEM_CONDITION, ITEM_MATERIAL, ITEM_CONFIRM = range(7)
BROADCAST_TEXT, BROADCAST_CONFIRM = range(10, 12)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [["⭐ Отзывы", "🗂 Каталог"], ["🛠 Поддержка"]],
    resize_keyboard=True,
)
BROADCAST_CONFIRM_KEYBOARD = ReplyKeyboardMarkup(
    [["✅ Разослать", "❌ Отмена"]],
    resize_keyboard=True,
)
ITEM_CONFIRM_KEYBOARD = ReplyKeyboardMarkup(
    [["✅ Сохранить", "✏️ Начать заново"]],
    resize_keyboard=True,
)


# ---------- SQLITE БАЗА ----------

def _init_db_if_missing():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            orders_count INTEGER DEFAULT 0,
            last_order TEXT,
            blocked INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            price TEXT,
            photo_file_id TEXT,
            measurements TEXT,
            condition_info TEXT,
            material TEXT,
            sold INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            username TEXT,
            product_id INTEGER,
            delivery_info TEXT,
            receipt_file_id TEXT,
            status TEXT DEFAULT 'awaiting_confirmation',
            created_at TEXT,
            confirmed_at TEXT
        )
    """)
    conn.commit()
    conn.close()


# ---------- ТОВАРЫ ----------

def _add_product_sync(name, price, photo_file_id, measurements, condition_info, material):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO products (name, price, photo_file_id, measurements, condition_info, material, sold, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
        (name, price, photo_file_id, measurements, condition_info, material,
         datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()
    conn.close()


async def add_product(name, price, photo_file_id, measurements, condition_info, material):
    async with DB_LOCK:
        await asyncio.to_thread(
            _add_product_sync, name, price, photo_file_id, measurements, condition_info, material
        )


def _get_available_products_sync():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, name, price FROM products WHERE sold = 0 ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    return rows


async def get_available_products():
    async with DB_LOCK:
        return await asyncio.to_thread(_get_available_products_sync)


def _get_product_sync(product_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, price, photo_file_id, measurements, condition_info, material, sold "
        "FROM products WHERE id = ?",
        (product_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


async def get_product(product_id):
    async with DB_LOCK:
        return await asyncio.to_thread(_get_product_sync, product_id)


def _mark_product_sold_sync(product_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("UPDATE products SET sold = 1 WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()


async def mark_product_sold(product_id):
    async with DB_LOCK:
        await asyncio.to_thread(_mark_product_sold_sync, product_id)


def _list_all_products_sync():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, name, price, sold FROM products ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()
    return rows


async def list_all_products():
    async with DB_LOCK:
        return await asyncio.to_thread(_list_all_products_sync)


def _remove_product_sync(product_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM products WHERE id = ?", (product_id,))
    changed = cur.rowcount
    conn.commit()
    conn.close()
    return changed


async def remove_product(product_id):
    async with DB_LOCK:
        return await asyncio.to_thread(_remove_product_sync, product_id)


# ---------- ЗАКАЗЫ (SALES) ----------

def _create_sale_sync(chat_id, username, product_id, delivery_info, receipt_file_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO sales (chat_id, username, product_id, delivery_info, receipt_file_id, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'awaiting_confirmation', ?)""",
        (chat_id, username, product_id, delivery_info, receipt_file_id,
         datetime.now().isoformat(timespec="seconds")),
    )
    sale_id = cur.lastrowid
    conn.commit()
    conn.close()
    return sale_id


async def create_sale(chat_id, username, product_id, delivery_info, receipt_file_id):
    async with DB_LOCK:
        return await asyncio.to_thread(
            _create_sale_sync, chat_id, username, product_id, delivery_info, receipt_file_id
        )


def _get_sale_sync(sale_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, username, product_id, delivery_info, receipt_file_id, status "
        "FROM sales WHERE id = ?",
        (sale_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


async def get_sale(sale_id):
    async with DB_LOCK:
        return await asyncio.to_thread(_get_sale_sync, sale_id)


def _set_sale_status_sync(sale_id, status):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now_iso = datetime.now().isoformat(timespec="seconds")
    if status == "confirmed":
        cur.execute("UPDATE sales SET status = ?, confirmed_at = ? WHERE id = ?", (status, now_iso, sale_id))
    else:
        cur.execute("UPDATE sales SET status = ? WHERE id = ?", (status, sale_id))
    conn.commit()
    conn.close()


async def set_sale_status(sale_id, status):
    async with DB_LOCK:
        await asyncio.to_thread(_set_sale_status_sync, sale_id, status)


def _save_client_after_sale_sync(chat_id, username, full_name):
    now_iso = datetime.now().isoformat(timespec="seconds")
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT orders_count FROM clients WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE clients SET username=?, full_name=?, orders_count=orders_count+1, last_order=?, blocked=0 "
            "WHERE chat_id=?",
            (username, full_name, now_iso, chat_id),
        )
    else:
        cur.execute(
            "INSERT INTO clients (chat_id, username, full_name, orders_count, last_order, blocked) "
            "VALUES (?, ?, ?, 1, ?, 0)",
            (chat_id, username, full_name, now_iso),
        )
    conn.commit()
    conn.close()


async def save_client_after_sale(chat_id, username, full_name):
    async with DB_LOCK:
        await asyncio.to_thread(_save_client_after_sale_sync, chat_id, username, full_name)


def _get_active_chat_ids_sync():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM clients WHERE blocked = 0")
    ids = [r[0] for r in cur.fetchall()]
    conn.close()
    return ids


async def get_active_chat_ids():
    async with DB_LOCK:
        return await asyncio.to_thread(_get_active_chat_ids_sync)


def _mark_blocked_sync(chat_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("UPDATE clients SET blocked = 1 WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


async def mark_blocked(chat_id):
    async with DB_LOCK:
        await asyncio.to_thread(_mark_blocked_sync, chat_id)


def _get_stats_sync():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM clients")
    total_clients = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM clients WHERE blocked = 1")
    blocked_clients = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM sales WHERE status = 'confirmed'")
    total_sales = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM sales WHERE status = 'confirmed' AND date(confirmed_at) = date('now')"
    )
    sales_today = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(*) FROM sales WHERE status = 'confirmed' AND confirmed_at >= datetime('now', '-7 days')"
    )
    sales_week = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products WHERE sold = 0")
    products_available = cur.fetchone()[0]

    cur.execute(
        "SELECT username, full_name, orders_count FROM clients ORDER BY orders_count DESC LIMIT 3"
    )
    top_clients = cur.fetchall()

    conn.close()
    return {
        "total_clients": total_clients,
        "blocked_clients": blocked_clients,
        "total_sales": total_sales,
        "sales_today": sales_today,
        "sales_week": sales_week,
        "products_available": products_available,
        "top_clients": top_clients,
    }


async def get_stats():
    async with DB_LOCK:
        return await asyncio.to_thread(_get_stats_sync)


# ---------- АВТОБЭКАП ----------

def _make_backup_sync():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    backup_path = os.path.join(BACKUP_DIR, f"clients_{timestamp}.db")
    conn = sqlite3.connect(DB_FILE)
    backup_conn = sqlite3.connect(backup_path)
    conn.backup(backup_conn)
    backup_conn.close()
    conn.close()
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, "clients_*.db")))
    for old_backup in backups[:-KEEP_BACKUPS]:
        try:
            os.remove(old_backup)
        except OSError:
            logger.exception("Не удалось удалить старый бэкап %s", old_backup)
    return backup_path


async def scheduled_backup(context: ContextTypes.DEFAULT_TYPE):
    async with DB_LOCK:
        try:
            path = await asyncio.to_thread(_make_backup_sync)
            logger.info("Бэкап базы создан: %s", path)
        except Exception:
            logger.exception("Не удалось создать бэкап базы")


# ---------- ОБЩИЕ ХЕНДЛЕРЫ ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        f"Добро пожаловать в {SHOP_NAME}!\nВыберите действие:\n\n{WELCOME_FOOTER}",
        reply_markup=MAIN_KEYBOARD,
    )


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        f"{SUPPORT_CONTACT} напишите для прямой связи",
        reply_markup=MAIN_KEYBOARD,
    )


REVIEWS_CHANNEL_URL = CONFIG["reviews_url"]


async def show_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Перейти к отзывам", url=REVIEWS_CHANNEL_URL)],
    ])
    await update.message.reply_text(
        "Здесь собраны реальные отзывы наших покупателей — переходите и убедитесь сами:",
        reply_markup=keyboard,
    )


# ---------- КАТАЛОГ (ПОКУПАТЕЛЬ) ----------

async def show_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    products = await get_available_products()

    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
        await update.callback_query.answer()
    else:
        chat_id = update.effective_chat.id

    if not products:
        await context.bot.send_message(chat_id=chat_id, text="Каталог пока пуст, загляните позже!")
        return

    keyboard = [
        [InlineKeyboardButton(f"{name} — {price}", callback_data=f"item_{pid}")]
        for pid, name, price in products
    ]
    await context.bot.send_message(
        chat_id=chat_id,
        text="🗂 Каталог доступных товаров:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def show_item_card(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer()
    product = await get_product(product_id)

    if not product or product[7] == 1:  # sold
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="Эта вещь уже продана. Загляните в каталог за другими товарами.",
        )
        return

    pid, name, price, photo_file_id, *_ = product
    caption = f"👕 {name}\n💰 Цена: {price}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("ℹ️ Информация о вещи", callback_data=f"info_{pid}")],
        [InlineKeyboardButton("🛒 Заказать", callback_data=f"order_{pid}")],
        [InlineKeyboardButton("◀️ Назад к каталогу", callback_data="backcat")],
    ])

    if photo_file_id:
        await context.bot.send_photo(
            chat_id=query.message.chat_id, photo=photo_file_id, caption=caption, reply_markup=keyboard
        )
    else:
        await context.bot.send_message(chat_id=query.message.chat_id, text=caption, reply_markup=keyboard)


async def show_info_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📏 Замеры", callback_data=f"meas_{product_id}")],
        [InlineKeyboardButton("🔧 Состояние", callback_data=f"cond_{product_id}")],
        [InlineKeyboardButton("🧵 Материал", callback_data=f"mat_{product_id}")],
        [InlineKeyboardButton("◀️ Назад", callback_data=f"backitem_{product_id}")],
    ])
    await context.bot.send_message(
        chat_id=query.message.chat_id, text="Выберите, что хотите узнать:", reply_markup=keyboard
    )


async def show_info_field(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int, field: str):
    query = update.callback_query
    await query.answer()
    product = await get_product(product_id)
    if not product:
        await context.bot.send_message(chat_id=query.message.chat_id, text="Товар не найден.")
        return

    _, name, price, photo_file_id, measurements, condition_info, material, sold = product
    field_map = {
        "meas": ("📏 Замеры", measurements),
        "cond": ("🔧 Состояние", condition_info),
        "mat": ("🧵 Материал", material),
    }
    title, value = field_map[field]
    text = f"{title}:\n{value or 'информация не указана'}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀️ Назад", callback_data=f"backinfo_{product_id}")],
    ])
    await context.bot.send_message(chat_id=query.message.chat_id, text=text, reply_markup=keyboard)


# ---------- ЗАКАЗ И ОПЛАТА (ПОКУПАТЕЛЬ) ----------

async def start_order(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer()

    product = await get_product(product_id)
    if not product or product[7] == 1:
        await context.bot.send_message(
            chat_id=query.message.chat_id, text="К сожалению, эта вещь уже продана."
        )
        return

    context.user_data["order_product_id"] = product_id
    context.user_data["delivery_info"] = None
    context.user_data["receipt_file_id"] = None

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Перейти к оплате", callback_data=f"pay_{product_id}")],
    ])
    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text=(
            "Чтобы оформить заказ, отправьте одним сообщением:\n"
                "Напишите ФИО, номер телефона, город, адрес ближайшего ПВЗ Озон.\n\n"
                f"Если у вас в городе нет пункта выдачи Озон, напишите менеджеру {SUPPORT_CONTACT}.\n\n"
            "Когда будете готовы — нажмите «Перейти к оплате»."
        ),
        reply_markup=keyboard,
    )


async def handle_buyer_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "order_product_id" not in context.user_data:
        return  # нет активного заказа — игнорируем текст
    context.user_data["delivery_info"] = update.message.text
    await update.message.reply_text(
        "Данные приняты, спасибо! Когда оплатите — нажмите «Перейти к оплате» "
        "в сообщении выше (если ещё не нажимали) и следуйте инструкции."
    )


async def handle_buyer_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "order_product_id" not in context.user_data:
        return  # нет активного заказа — игнорируем фото
    photo = update.message.photo[-1]
    context.user_data["receipt_file_id"] = photo.file_id
    await update.message.reply_text(
        "Чек получен, спасибо! Нажмите «✅ Я оплатил и скинул чек», если ещё не нажимали."
    )


async def show_payment_info(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer()

    product = await get_product(product_id)
    if not product or product[7] == 1:
        await context.bot.send_message(chat_id=query.message.chat_id, text="Эта вещь уже продана.")
        return

    _, name, price, *_ = product
    text = (
                f"💰 Цена за вещь: {price}\n+ 400р (Доставка)\n\n"
        f"Реквизиты для оплаты:\n{PAYMENT_REQUISITES}\n\n"
        "После оплаты пришлите, пожалуйста, фото чека прямо в этот чат, "
        "затем нажмите кнопку ниже."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Я оплатил и скинул чек", callback_data=f"confirmpaid_{product_id}")],
    ])
    await context.bot.send_message(chat_id=query.message.chat_id, text=text, reply_markup=keyboard)


async def confirm_paid_by_buyer(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer()

    if context.user_data.get("order_product_id") != product_id:
        await context.bot.send_message(chat_id=query.message.chat_id, text="Заказ не найден, начните заново из каталога.")
        return

    delivery_info = context.user_data.get("delivery_info")
    receipt_file_id = context.user_data.get("receipt_file_id")

    if not delivery_info:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="Вы ещё не прислали данные для доставки (имя, телефон, адрес). Пришлите их одним сообщением.",
        )
        return
    if not receipt_file_id:
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="Не вижу фото чека. Пришлите, пожалуйста, фото чека, затем нажмите кнопку снова.",
        )
        return

    user = query.from_user
    username = f"@{user.username}" if user.username else "(username не указан)"

    sale_id = await create_sale(user.id, username, product_id, delivery_info, receipt_file_id)
    product = await get_product(product_id)
    _, name, price, *_ = product

    await context.bot.send_message(
        chat_id=query.message.chat_id,
        text="Проверяем оплату, это может занять до 10 минут. Мы напишем, как только всё подтвердим!",
    )

    admin_text = (
        f"🆕 Новый заказ (ожидает подтверждения оплаты)\n\n"
        f"👤 Покупатель: {username}\n"
        f"🆔 ID: {user.id}\n\n"
        f"👕 Товар: {name} (ID {product_id})\n"
        f"💰 Цена: {price}\n\n"
        f"📦 Данные доставки:\n{delivery_info}\n\n"
        f"Проверьте чек ниже и подтвердите оплату."
    )
    admin_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Подтвердить оплату", callback_data=f"admconfirm_{sale_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"admreject_{sale_id}"),
        ]
    ])
    try:
        await context.bot.send_photo(
            chat_id=ADMIN_CHAT_ID, photo=receipt_file_id, caption=admin_text, reply_markup=admin_keyboard
        )
    except Exception:
        logger.exception("Не удалось отправить заявку с чеком админу")

    context.user_data.pop("order_product_id", None)
    context.user_data.pop("delivery_info", None)
    context.user_data.pop("receipt_file_id", None)


# ---------- ПОДТВЕРЖДЕНИЕ ОПЛАТЫ (АДМИН) ----------

async def admin_confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE, sale_id: int):
    query = update.callback_query
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Недоступно", show_alert=True)
        return
    await query.answer()

    sale = await get_sale(sale_id)
    if not sale:
        await query.edit_message_caption(caption=(query.message.caption or "") + "\n\n⚠️ Заказ не найден.")
        return

    _, chat_id, username, product_id, delivery_info, receipt_file_id, status = sale
    if status == "confirmed":
        await query.edit_message_caption(caption=(query.message.caption or "") + "\n\n✅ Уже подтверждено.")
        return

    await set_sale_status(sale_id, "confirmed")
    await mark_product_sold(product_id)

    try:
        chat = await context.bot.get_chat(chat_id)
        full_name = chat.full_name or ""
    except Exception:
        full_name = ""
    await save_client_after_sale(chat_id, username, full_name)

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🎉 Вещь успешно заказана!\n\n"
                "Спасибо за покупку! Мы уже готовим отправку.\n\n"
                f"По всем вопросам пишите: {SUPPORT_CONTACT}"
            ),
        )
    except Exception:
        logger.exception("Не удалось уведомить покупателя о подтверждении, chat_id=%s", chat_id)

    await query.edit_message_caption(
        caption=(query.message.caption or "") + "\n\n✅ Оплата подтверждена, покупатель уведомлён."
    )


async def admin_reject_payment(update: Update, context: ContextTypes.DEFAULT_TYPE, sale_id: int):
    query = update.callback_query
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Недоступно", show_alert=True)
        return
    await query.answer()

    sale = await get_sale(sale_id)
    if not sale:
        return
    _, chat_id, username, product_id, delivery_info, receipt_file_id, status = sale

    await set_sale_status(sale_id, "rejected")

    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "К сожалению, мы не смогли подтвердить оплату по вашему заказу.\n"
                f"Пожалуйста, свяжитесь с поддержкой: {SUPPORT_CONTACT}"
            ),
        )
    except Exception:
        logger.exception("Не удалось уведомить покупателя об отклонении, chat_id=%s", chat_id)

    await query.edit_message_caption(caption=(query.message.caption or "") + "\n\n❌ Отклонено.")


# ---------- ГЛАВНЫЙ ОБРАБОТЧИК CALLBACK-КНОПОК ----------

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data

    if data == "catalog" or data == "backcat":
        await show_catalog(update, context)
        return

    prefix, _, rest = data.partition("_")

    if prefix == "item":
        await show_item_card(update, context, int(rest))
    elif prefix == "info":
        await show_info_menu(update, context, int(rest))
    elif prefix == "backitem":
        await show_item_card(update, context, int(rest))
    elif prefix == "backinfo":
        await show_info_menu(update, context, int(rest))
    elif prefix in ("meas", "cond", "mat"):
        await show_info_field(update, context, int(rest), prefix)
    elif prefix == "order":
        await start_order(update, context, int(rest))
    elif prefix == "pay":
        await show_payment_info(update, context, int(rest))
    elif prefix == "confirmpaid":
        await confirm_paid_by_buyer(update, context, int(rest))
    elif prefix == "admconfirm":
        await admin_confirm_payment(update, context, int(rest))
    elif prefix == "admreject":
        await admin_reject_payment(update, context, int(rest))
    elif prefix == "delask":
        await ask_delete_confirm(update, context, int(rest))
    elif prefix == "delyes":
        await do_delete_item(update, context, int(rest))
    elif prefix == "delno":
        await cancel_delete_item(update, context, int(rest))
    else:
        await update.callback_query.answer()


# ---------- ДОБАВЛЕНИЕ ТОВАРА (ТОЛЬКО АДМИН) ----------

async def additem_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return ConversationHandler.END
    context.user_data.clear()
    await update.message.reply_text("Пришлите фото товара:")
    return ITEM_PHOTO


async def additem_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("Нужно именно фото. Пришлите фото товара:")
        return ITEM_PHOTO
    context.user_data["new_item_photo"] = update.message.photo[-1].file_id
    await update.message.reply_text("Название товара:")
    return ITEM_NAME


async def additem_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_item_name"] = update.message.text
    await update.message.reply_text("Цена:")
    return ITEM_PRICE


async def additem_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_item_price"] = update.message.text
    await update.message.reply_text("Замеры:")
    return ITEM_MEASUREMENTS


async def additem_measurements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_item_measurements"] = update.message.text
    await update.message.reply_text("Состояние (дефекты и т.д., если есть):")
    return ITEM_CONDITION


async def additem_condition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_item_condition"] = update.message.text
    await update.message.reply_text("Материал:")
    return ITEM_MATERIAL


async def additem_material(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["new_item_material"] = update.message.text

    d = context.user_data
    summary = (
        "Проверьте товар перед сохранением:\n\n"
        f"Название: {d['new_item_name']}\n"
        f"Цена: {d['new_item_price']}\n"
        f"Замеры: {d['new_item_measurements']}\n"
        f"Состояние: {d['new_item_condition']}\n"
        f"Материал: {d['new_item_material']}"
    )
    await update.message.reply_photo(
        photo=d["new_item_photo"], caption=summary, reply_markup=ITEM_CONFIRM_KEYBOARD
    )
    return ITEM_CONFIRM


async def additem_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = context.user_data
    await add_product(
        d["new_item_name"], d["new_item_price"], d["new_item_photo"],
        d["new_item_measurements"], d["new_item_condition"], d["new_item_material"],
    )
    await update.message.reply_text("Товар добавлен в каталог!", reply_markup=MAIN_KEYBOARD)
    context.user_data.clear()
    return ConversationHandler.END


async def additem_restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Хорошо, начнём заново. Пришлите фото товара:")
    return ITEM_PHOTO


async def additem_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Добавление товара отменено.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


# ---------- УПРАВЛЕНИЕ ТОВАРАМИ (ТОЛЬКО АДМИН) ----------

async def list_items(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return
    products = await list_all_products()
    if not products:
        await update.message.reply_text("Товаров пока нет.")
        return
    for pid, name, price, sold in products:
        status = "продано" if sold else "в наличии"
        text = f"#{pid} — {name} ({price}) — {status}"
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🗑 Удалить", callback_data=f"delask_{pid}")]]
        )
        await update.message.reply_text(text, reply_markup=keyboard)


async def ask_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delyes_{product_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"delno_{product_id}"),
    ]])
    await query.edit_message_text(
        text=f"{query.message.text}\n\nТочно удалить?",
        reply_markup=keyboard,
    )


async def do_delete_item(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    if query.from_user.id != ADMIN_CHAT_ID:
        await query.answer("Нет доступа", show_alert=True)
        return
    changed = await remove_product(product_id)
    await query.answer("Удалено" if changed else "Не найдено")
    if changed:
        await query.edit_message_text(f"❌ Товар #{product_id} удалён.")
    else:
        await query.edit_message_text(f"Товар #{product_id} не найден (возможно, уже удалён).")


async def cancel_delete_item(update: Update, context: ContextTypes.DEFAULT_TYPE, product_id: int):
    query = update.callback_query
    await query.answer("Отменено")
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🗑 Удалить", callback_data=f"delask_{product_id}")]]
    )
    original_text = query.message.text.replace("\n\nТочно удалить?", "")
    await query.edit_message_text(text=original_text, reply_markup=keyboard)


async def remove_item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /removeitem ID (посмотреть ID — /listitems)")
        return
    try:
        product_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID должен быть числом.")
        return
    changed = await remove_product(product_id)
    if changed:
        await update.message.reply_text(f"Товар #{product_id} удалён.")
    else:
        await update.message.reply_text(f"Товар #{product_id} не найден.")


# ---------- ЭКСПОРТ БАЗЫ (ТОЛЬКО АДМИН) ----------

async def export_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return
    if not os.path.exists(DB_FILE):
        await update.message.reply_text("База пока пуста.")
        return
    async with DB_LOCK:
        try:
            with open(DB_FILE, "rb") as f:
                await update.message.reply_document(
                    document=f, filename="clients.db",
                    caption=f"База на {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                )
        except Exception:
            logger.exception("Не удалось отправить файл базы админу")
            await update.message.reply_text("Не получилось отправить файл базы.")


# ---------- СТАТИСТИКА (ТОЛЬКО АДМИН) ----------

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return
    data = await get_stats()
    top_lines = ""
    for username, full_name, count in data["top_clients"]:
        name = username or full_name or "—"
        top_lines += f"  • {name}: {count} заказ(ов)\n"
    if not top_lines:
        top_lines = "  пока нет данных\n"

    text = (
        "📊 Статистика\n\n"
        f"Всего клиентов: {data['total_clients']}\n"
        f"Заблокировали бота: {data['blocked_clients']}\n"
        f"Товаров в наличии: {data['products_available']}\n"
        f"Всего продаж: {data['total_sales']}\n"
        f"Продаж сегодня: {data['sales_today']}\n"
        f"Продаж за 7 дней: {data['sales_week']}\n\n"
        f"Топ клиентов:\n{top_lines}"
    )
    await update.message.reply_text(text)


# ---------- РУЧНАЯ РАССЫЛКА (ТОЛЬКО АДМИН) ----------

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_CHAT_ID:
        return ConversationHandler.END
    await update.message.reply_text("Отправьте текст сообщения для рассылки всем клиентам.")
    return BROADCAST_TEXT


async def broadcast_get_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["broadcast_text"] = update.message.text
    chat_ids = await get_active_chat_ids()
    context.user_data["broadcast_recipients"] = chat_ids
    await update.message.reply_text(
        f"Предпросмотр (уйдёт {len(chat_ids)} получателям):\n\n"
        f"—————————————\n{update.message.text}\n—————————————\n\nРазослать?",
        reply_markup=BROADCAST_CONFIRM_KEYBOARD,
    )
    return BROADCAST_CONFIRM


async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = context.user_data.get("broadcast_text", "")
    chat_ids = context.user_data.get("broadcast_recipients", [])
    await update.message.reply_text(f"Начинаю рассылку {len(chat_ids)} получателям...")
    sent, blocked, failed = 0, 0, 0
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
            sent += 1
        except Forbidden:
            blocked += 1
            await mark_blocked(chat_id)
        except TelegramError:
            failed += 1
            logger.exception("Ошибка рассылки chat_id=%s", chat_id)
        await asyncio.sleep(0.05)
    await update.message.reply_text(
        f"Готово.\nДоставлено: {sent}\nЗаблокировали бота: {blocked}\nОшибки: {failed}",
        reply_markup=MAIN_KEYBOARD,
    )
    context.user_data.pop("broadcast_text", None)
    context.user_data.pop("broadcast_recipients", None)
    return ConversationHandler.END


async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("broadcast_text", None)
    context.user_data.pop("broadcast_recipients", None)
    await update.message.reply_text("Рассылка отменена.", reply_markup=MAIN_KEYBOARD)
    return ConversationHandler.END


# ---------- ЗАПУСК ----------

async def restart_cmd(update, context):
    if update.effective_user.id != ADMIN_CHAT_ID:
        return
    await update.message.reply_text("Перезапускаю бота...")
    os._exit(0)

async def setup_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Запустить бота / главное меню"),
    ])
    await app.bot.set_my_commands(
        [
            BotCommand("start", "Запустить бота / главное меню"),
            BotCommand("additem", "Добавить товар в каталог"),
            BotCommand("listitems", "Список всех товаров"),
            BotCommand("removeitem", "Удалить товар по ID"),
            BotCommand("stats", "Статистика по заказам и клиентам"),
            BotCommand("export", "Скачать базу (clients.db)"),
            BotCommand("broadcast", "Разослать сообщение всем клиентам"),
            BotCommand("restart", "Перезапустить бота"),
        ],
        scope=BotCommandScopeChat(chat_id=ADMIN_CHAT_ID),
    )


def main():
    _init_db_if_missing()
    app = ApplicationBuilder().token(TOKEN).post_init(setup_commands).build()

    additem_handler = ConversationHandler(
        entry_points=[CommandHandler("additem", additem_start)],
        states={
            ITEM_PHOTO: [MessageHandler(filters.PHOTO, additem_photo)],
            ITEM_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, additem_name)],
            ITEM_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, additem_price)],
            ITEM_MEASUREMENTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, additem_measurements)],
            ITEM_CONDITION: [MessageHandler(filters.TEXT & ~filters.COMMAND, additem_condition)],
            ITEM_MATERIAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, additem_material)],
            ITEM_CONFIRM: [
                MessageHandler(filters.Regex("^✅ Сохранить$"), additem_confirm),
                MessageHandler(filters.Regex("^✏️ Начать заново$"), additem_restart),
            ],
        },
        fallbacks=[CommandHandler("cancel", additem_cancel)],
    )

    broadcast_handler = ConversationHandler(
        entry_points=[CommandHandler("broadcast", broadcast_start)],
        states={
            BROADCAST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_get_text)],
            BROADCAST_CONFIRM: [
                MessageHandler(filters.Regex("^✅ Разослать$"), broadcast_send),
                MessageHandler(filters.Regex("^❌ Отмена$"), broadcast_cancel),
            ],
        },
        fallbacks=[],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("export", export_clients))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("listitems", list_items))
    app.add_handler(CommandHandler("removeitem", remove_item))
    app.add_handler(broadcast_handler)
    app.add_handler(additem_handler)
    app.add_handler(CommandHandler("restart", restart_cmd))

    # Кнопки главного меню — важно зарегистрировать раньше общих текстовых хендлеров
    app.add_handler(MessageHandler(filters.Regex("^🛠 Поддержка$"), support))
    app.add_handler(MessageHandler(filters.Regex("^⭐ Отзывы$"), show_reviews))
    app.add_handler(MessageHandler(filters.Regex("^🗂 Каталог$"), show_catalog))

    # Callback-кнопки каталога/заказа/оплаты
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Приём данных доставки и чека от покупателя (только если у него активен заказ)
    app.add_handler(MessageHandler(filters.PHOTO, handle_buyer_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buyer_text))

    if app.job_queue is not None:
        app.job_queue.run_repeating(
            scheduled_backup, interval=BACKUP_INTERVAL_SECONDS, first=60, name="daily_backup",
        )

    logger.info("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Бот упал с ошибкой при запуске")
        input("\nБот остановлен из-за ошибки (текст выше). Нажмите Enter, чтобы закрыть окно...")
