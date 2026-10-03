# -*- coding: utf-8 -*-
import os
import sys
import json
import sqlite3
import asyncio
import logging
import datetime
import calendar

from telegram import Update, ReplyKeyboardMarkup, BotCommand, BotCommandScopeChat
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------- КОНФИГ ----------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
DB_PATH = os.path.join(BASE_DIR, "clients.db")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

BOT_TOKEN = CONFIG["bot_token"]
ADMIN_CHAT_ID = int(CONFIG["admin_chat_id"])
CONTACT_USERNAME = CONFIG.get("contact_username", "@your_username")
PAYMENT_REQUISITES = CONFIG.get(
    "payment_requisites",
    "Реквизиты не заполнены, впишите в config.json поле payment_requisites",
)
DEFAULT_PRICE = int(CONFIG.get("default_price", 1000))
REMINDER_DAYS_BEFORE = int(CONFIG.get("reminder_days_before", 3))
REMINDER_HOUR = int(CONFIG.get("reminder_hour", 10))  # час дня по времени сервера

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["Наши клиенты"],
        ["Сделать бота"],
        ["У меня уже есть бот"],
    ],
    resize_keyboard=True,
)

# ---------- ДАТЫ ----------

def today() -> datetime.date:
    return datetime.date.today()


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%d.%m.%Y").date()


def fmt_date(d: datetime.date) -> str:
    return d.strftime("%d.%m.%Y")


def next_billing_date(base: datetime.date, billing_day: int) -> datetime.date:
    """Ближайшая дата платежа в месяце, следующем за base.
    Если в месяце нет такого числа (31 февраля), берём последний день месяца."""
    year, month = base.year, base.month
    month += 1
    if month > 12:
        month = 1
        year += 1
    last_day = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, min(billing_day, last_day))


def compute_next_due(pay_date: datetime.date, billing_day: int, current_next_due: str | None) -> datetime.date:
    """Оплата вовремя продлевает от текущей даты окончания,
    оплата с опозданием - от даты платежа."""
    base = pay_date
    if current_next_due:
        cur = parse_date(current_next_due)
        if cur > pay_date:
            base = cur
    return next_billing_date(base, billing_day)


# ---------- БАЗА ----------

def _table_columns(cur: sqlite3.Cursor, table: str) -> list[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _migrate_old_schema(con: sqlite3.Connection) -> None:
    """Старая схема: одна таблица clients с колонкой code (один клиент = один бот).
    Новая: clients (владельцы) + bots (боты с подпиской)."""
    cur = con.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='clients'"
    )
    if not cur.fetchone():
        return
    cols = _table_columns(cur, "clients")
    if "code" not in cols:
        return  # уже новая схема

    logger.info("Обнаружена старая схема базы, запускаю миграцию")
    cur.execute("ALTER TABLE clients RENAME TO clients_old")
    _create_tables(cur)

    cur.execute(
        "SELECT code, shop_name, client_username, bot_username, price, "
        "billing_day, last_payment_date, next_due_date, reminder_sent_for "
        "FROM clients_old ORDER BY code"
    )
    old_rows = cur.fetchall()

    owner_to_id: dict[str, int] = {}
    for (code, shop, client_u, bot_u, price, b_day, last_pay, next_due, rem) in old_rows:
        key = (client_u or "").strip().lower()
        if key not in owner_to_id:
            cur.execute(
                "INSERT INTO clients (channel_name, owner_username) VALUES (?, ?)",
                (shop, client_u),
            )
            owner_to_id[key] = cur.lastrowid
        cur.execute(
            "INSERT INTO bots (id, client_id, shop_name, bot_username, price, "
            "billing_day, last_payment_date, next_due_date, reminder_sent_for) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (code, owner_to_id[key], shop, bot_u, price, b_day, last_pay, next_due, rem),
        )

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='watchers'"
    )
    if cur.fetchone():
        cur.execute("ALTER TABLE watchers RENAME TO watchers_old")

    con.commit()
    logger.info("Миграция завершена: %d ботов перенесено", len(old_rows))


def _create_tables(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT NOT NULL,
            owner_username TEXT NOT NULL,
            owner_user_id INTEGER,
            created_at TEXT DEFAULT (datetime('now'))
        )"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS bots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL REFERENCES clients(id),
            shop_name TEXT NOT NULL,
            bot_username TEXT NOT NULL,
            price INTEGER DEFAULT 1000,
            billing_day INTEGER,
            last_payment_date TEXT,
            next_due_date TEXT,
            reminder_sent_for TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )"""
    )


def _init_db_sync() -> None:
    con = sqlite3.connect(DB_PATH)
    try:
        _migrate_old_schema(con)
        cur = con.cursor()
        _create_tables(cur)
        con.commit()
    finally:
        con.close()


async def init_db() -> None:
    await asyncio.to_thread(_init_db_sync)


def _db(query: str, params: tuple = (), fetch: bool = False):
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        cur.execute(query, params)
        if fetch:
            return cur.fetchall()
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


async def db(query: str, params: tuple = (), fetch: bool = False):
    return await asyncio.to_thread(_db, query, params, fetch)


# ---------- СТАТУС ПОДПИСКИ ----------

def bot_status_line(price, next_due) -> str:
    if not next_due:
        return f"{price} руб в месяц, оплат ещё не было"
    due = parse_date(next_due)
    if due < today():
        return f"{price} руб в месяц, просрочено с {fmt_date(due)}"
    return f"{price} руб в месяц, оплачено до {fmt_date(due)}"


# ---------- ПУБЛИЧНЫЕ КНОПКИ ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Здравствуйте! Мы делаем Telegram-ботов для магазинов.\n"
        "Выберите действие на клавиатуре ниже.",
        reply_markup=MAIN_KEYBOARD,
    )


async def show_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await db(
        "SELECT c.channel_name, c.owner_username, b.shop_name, b.bot_username "
        "FROM bots b JOIN clients c ON c.id = b.client_id "
        "ORDER BY c.id, b.id",
        fetch=True,
    )
    if not rows:
        await update.message.reply_text(
            "Пока нет ни одного магазина, скоро здесь появится первый."
        )
        return
    grouped: dict[str, dict] = {}
    for channel, owner_u, shop, bot_u in rows:
        g = grouped.setdefault(owner_u, {"channel": channel, "bots": []})
        g["bots"].append((shop, bot_u))
    blocks = []
    for owner_u, g in grouped.items():
        lines = [g["channel"], f"Канал: {owner_u}"]
        for shop, bot_u in g["bots"]:
            if len(g["bots"]) == 1:
                lines.append(f"Бот: {bot_u}")
            else:
                lines.append(f"Бот ({shop}): {bot_u}")
        blocks.append("\n".join(lines))
    text = "Клиенты, для которых мы сделали ботов:\n\n" + "\n\n".join(blocks)
    await update.message.reply_text(text)


async def make_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Чтобы заказать бота, напишите: {CONTACT_USERNAME}"
    )


async def my_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    uname = ("@" + user.username.lower()) if user.username else None

    rows = await db(
        "SELECT id, owner_username, owner_user_id FROM clients", fetch=True
    )
    client_id = None
    for cid, owner_u, owner_uid in rows:
        if owner_uid == uid:
            client_id = cid
            break
    if client_id is None and uname:
        for cid, owner_u, owner_uid in rows:
            if (owner_u or "").strip().lower() == uname:
                if owner_uid is None:
                    await db(
                        "UPDATE clients SET owner_user_id = ? WHERE id = ?",
                        (uid, cid),
                    )
                    client_id = cid
                break

    if client_id is None:
        await update.message.reply_text(
            "Не нашёл ваших ботов.\n"
            f"Если вы наш клиент, напишите {CONTACT_USERNAME}, мы всё поправим."
        )
        return

    bots = await db(
        "SELECT shop_name, bot_username, price, next_due_date "
        "FROM bots WHERE client_id = ? ORDER BY id",
        (client_id,),
        fetch=True,
    )
    if not bots:
        await update.message.reply_text(
            f"За вами пока не числится ботов. Напишите {CONTACT_USERNAME}."
        )
        return

    blocks = []
    for shop, bot_u, price, next_due in bots:
        blocks.append(f"{shop} ({bot_u})\n{bot_status_line(price, next_due)}")
    text = (
        "Ваши боты:\n\n"
        + "\n\n".join(blocks)
        + "\n\nРеквизиты для оплаты:\n"
        + PAYMENT_REQUISITES
    )
    await update.message.reply_text(text)


# ---------- АДМИН-КОМАНДЫ ----------

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_CHAT_ID:
            return
        await func(update, context)
    return wrapper


@admin_only
async def addclient_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.partition(" ")[2].strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) != 2 or not all(parts):
        await update.message.reply_text(
            "Формат:\n/addclient Название канала | @юзер_владельца\n\n"
            "Пример:\n/addclient Мой магазин | @my_channel"
        )
        return
    channel, owner_u = parts
    cid = await db(
        "INSERT INTO clients (channel_name, owner_username) VALUES (?, ?)",
        (channel, owner_u),
    )
    await update.message.reply_text(
        f"Клиент #{cid}: {channel} ({owner_u}).\n"
        f"Теперь добавь бота:\n/addbot {cid} | Название магазина | @юзер_бота | [цена]"
    )


@admin_only
async def addbot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.partition(" ")[2].strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) not in (3, 4) or not all(parts[:3]):
        await update.message.reply_text(
            "Формат:\n/addbot id_клиента | Название магазина | @юзер_бота | [цена]\n\n"
            "Пример:\n/addbot 1 | Мой магазин | @my_shop_bot | 1000"
        )
        return
    try:
        client_id = int(parts[0])
    except ValueError:
        await update.message.reply_text("id клиента должен быть числом.")
        return
    exists = await db("SELECT id FROM clients WHERE id = ?", (client_id,), fetch=True)
    if not exists:
        await update.message.reply_text(f"Клиента #{client_id} нет. Смотри /status.")
        return
    shop, bot_u = parts[1], parts[2]
    price = DEFAULT_PRICE
    if len(parts) == 4:
        try:
            price = int(parts[3])
        except ValueError:
            await update.message.reply_text("Цена должна быть числом.")
            return
    bid = await db(
        "INSERT INTO bots (client_id, shop_name, bot_username, price) "
        "VALUES (?, ?, ?, ?)",
        (client_id, shop, bot_u, price),
    )
    await update.message.reply_text(
        f"Бот #{bid}: {shop} ({bot_u}), {price} руб в месяц, клиент #{client_id}.\n"
        f"После первой оплаты: /pay {bid}"
    )


@admin_only
async def pay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args:
        await update.message.reply_text(
            "Формат:\n/pay id_бота [ДД.ММ.ГГГГ]\n\nПример:\n/pay 2 15.08.2026"
        )
        return
    try:
        bot_id = int(args[0])
    except ValueError:
        await update.message.reply_text("id бота должен быть числом.")
        return
    pay_date = today()
    if len(args) > 1:
        try:
            pay_date = parse_date(args[1])
        except ValueError:
            await update.message.reply_text("Дата в формате ДД.ММ.ГГГГ, например 15.08.2026")
            return
    rows = await db(
        "SELECT shop_name, billing_day, next_due_date FROM bots WHERE id = ?",
        (bot_id,),
        fetch=True,
    )
    if not rows:
        await update.message.reply_text(f"Бота #{bot_id} нет. Смотри /status.")
        return
    shop, billing_day, next_due = rows[0]
    if billing_day is None:
        billing_day = pay_date.day
    new_due = compute_next_due(pay_date, billing_day, next_due)
    await db(
        "UPDATE bots SET billing_day = ?, last_payment_date = ?, next_due_date = ? "
        "WHERE id = ?",
        (billing_day, fmt_date(pay_date), fmt_date(new_due), bot_id),
    )
    await update.message.reply_text(
        f"Оплата по боту #{bot_id} ({shop}) записана: {fmt_date(pay_date)}.\n"
        f"Оплачено до {fmt_date(new_due)}, платёжный день: {billing_day} число."
    )


@admin_only
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clients = await db(
        "SELECT id, channel_name, owner_username, owner_user_id FROM clients ORDER BY id",
        fetch=True,
    )
    if not clients:
        await update.message.reply_text("База пуста. Добавь клиента: /addclient")
        return
    blocks = []
    for cid, channel, owner_u, owner_uid in clients:
        bound = "привязан" if owner_uid else "не привязан"
        lines = [f"Клиент #{cid}: {channel} ({owner_u}), {bound}"]
        bots = await db(
            "SELECT id, shop_name, bot_username, price, next_due_date "
            "FROM bots WHERE client_id = ? ORDER BY id",
            (cid,),
            fetch=True,
        )
        if not bots:
            lines.append("  ботов нет")
        for bid, shop, bot_u, price, next_due in bots:
            lines.append(f"  Бот #{bid}: {shop} ({bot_u}), {bot_status_line(price, next_due)}")
        blocks.append("\n".join(lines))
    await update.message.reply_text("\n\n".join(blocks))


@admin_only
async def removebot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args or not args[0].isdigit():
        await update.message.reply_text("Формат: /removebot id_бота")
        return
    bot_id = int(args[0])
    rows = await db("SELECT shop_name FROM bots WHERE id = ?", (bot_id,), fetch=True)
    if not rows:
        await update.message.reply_text(f"Бота #{bot_id} нет.")
        return
    await db("DELETE FROM bots WHERE id = ?", (bot_id,))
    await update.message.reply_text(f"Бот #{bot_id} ({rows[0][0]}) удалён.")


@admin_only
async def removeclient_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if not args or not args[0].isdigit():
        await update.message.reply_text("Формат: /removeclient id_клиента")
        return
    client_id = int(args[0])
    rows = await db("SELECT channel_name FROM clients WHERE id = ?", (client_id,), fetch=True)
    if not rows:
        await update.message.reply_text(f"Клиента #{client_id} нет.")
        return
    bots = await db("SELECT id FROM bots WHERE client_id = ?", (client_id,), fetch=True)
    if bots:
        ids = ", ".join(str(b[0]) for b in bots)
        await update.message.reply_text(
            f"У клиента #{client_id} есть боты: {ids}.\n"
            "Сначала удали их через /removebot."
        )
        return
    await db("DELETE FROM clients WHERE id = ?", (client_id,))
    await update.message.reply_text(f"Клиент #{client_id} ({rows[0][0]}) удалён.")


@admin_only
async def bind_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()[1:]
    if len(args) != 2 or not all(a.lstrip("-").isdigit() for a in args):
        await update.message.reply_text(
            "Формат: /bind id_клиента user_id\n"
            "Нужно, если клиент сменил юзернейм до первой привязки."
        )
        return
    client_id, user_id = int(args[0]), int(args[1])
    rows = await db("SELECT channel_name FROM clients WHERE id = ?", (client_id,), fetch=True)
    if not rows:
        await update.message.reply_text(f"Клиента #{client_id} нет.")
        return
    await db("UPDATE clients SET owner_user_id = ? WHERE id = ?", (user_id, client_id))
    await update.message.reply_text(f"Клиент #{client_id} привязан к user_id {user_id}.")


@admin_only
async def restart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Перезапускаюсь...")
    os.execl(sys.executable, sys.executable, *sys.argv)


# ---------- НАПОМИНАНИЯ ----------

async def daily_reminders(context: ContextTypes.DEFAULT_TYPE):
    rows = await db(
        "SELECT b.id, b.shop_name, b.bot_username, b.price, b.next_due_date, "
        "b.reminder_sent_for, c.owner_user_id, c.owner_username "
        "FROM bots b JOIN clients c ON c.id = b.client_id "
        "WHERE b.next_due_date IS NOT NULL",
        fetch=True,
    )
    for bid, shop, bot_u, price, next_due, sent_for, owner_uid, owner_u in rows:
        due = parse_date(next_due)
        if (due - today()).days != REMINDER_DAYS_BEFORE:
            continue
        if sent_for == next_due:
            continue
        client_text = (
            f"Напоминание: подписка на бота {shop} ({bot_u}) "
            f"действует до {fmt_date(due)}.\n"
            f"Сумма: {price} руб.\n\nРеквизиты:\n{PAYMENT_REQUISITES}"
        )
        delivered = False
        if owner_uid:
            try:
                await context.bot.send_message(chat_id=owner_uid, text=client_text)
                delivered = True
            except (Forbidden, TelegramError) as e:
                logger.warning("Не доставлено владельцу %s: %s", owner_uid, e)
        admin_note = "отправлено владельцу" if delivered else (
            "владельцу НЕ отправлено (нет привязки)" if not owner_uid
            else "владельцу НЕ отправлено (ошибка доставки)"
        )
        try:
            await context.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=(
                    f"Бот #{bid} ({shop}, {owner_u}): оплата до {fmt_date(due)}, "
                    f"{price} руб. Напоминание {admin_note}."
                ),
            )
        except TelegramError as e:
            logger.warning("Не доставлено админу: %s", e)
        await db(
            "UPDATE bots SET reminder_sent_for = ? WHERE id = ?", (next_due, bid)
        )


# ---------- ЗАПУСК ----------

async def post_init(app: Application):
    await init_db()
    admin_commands = [
        BotCommand("status", "Сводка по клиентам и оплатам"),
        BotCommand("addclient", "Добавить клиента"),
        BotCommand("addbot", "Добавить бота клиенту"),
        BotCommand("pay", "Записать оплату"),
        BotCommand("bind", "Привязать user_id вручную"),
        BotCommand("removebot", "Удалить бота"),
        BotCommand("removeclient", "Удалить клиента"),
        BotCommand("restart", "Перезапустить бота"),
    ]
    try:
        await app.bot.set_my_commands(
            admin_commands, scope=BotCommandScopeChat(chat_id=ADMIN_CHAT_ID)
        )
    except TelegramError as e:
        logger.warning("Не удалось выставить команды: %s", e)


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addclient", addclient_cmd))
    app.add_handler(CommandHandler("addbot", addbot_cmd))
    app.add_handler(CommandHandler("pay", pay_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("removebot", removebot_cmd))
    app.add_handler(CommandHandler("removeclient", removeclient_cmd))
    app.add_handler(CommandHandler("bind", bind_cmd))
    app.add_handler(CommandHandler("restart", restart_cmd))

    app.add_handler(MessageHandler(filters.Regex("^Наши клиенты$"), show_clients))
    app.add_handler(MessageHandler(filters.Regex("^Сделать бота$"), make_bot))
    app.add_handler(MessageHandler(filters.Regex("^У меня уже есть бот$"), my_bots))

    if app.job_queue:
        app.job_queue.run_daily(
            daily_reminders, time=datetime.time(hour=REMINDER_HOUR, minute=0)
        )

    logger.info("Админ-бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
