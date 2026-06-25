import os
import asyncio
import logging
import aiohttp
import aiosqlite
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

# ─── Конфигурация ────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
KIWI_API_KEY = os.getenv("KIWI_API_KEY", "")
AVIASALES_TOKEN = os.getenv("AVIASALES_TOKEN", "")
DB_PATH = "subscriptions.db"
CHECK_INTERVAL_HOURS = 2

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── Состояния диалога ───────────────────────────────────────────────────────
FROM_CITY, TO_CITY, DATES, PASSENGERS, LUGGAGE, STOPS = range(6)

# ═══════════════════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                from_code   TEXT NOT NULL,
                from_name   TEXT NOT NULL,
                to_code     TEXT NOT NULL,
                to_name     TEXT NOT NULL,
                date_from   TEXT NOT NULL,
                date_to     TEXT NOT NULL,
                passengers  INTEGER DEFAULT 1,
                max_stops   INTEGER DEFAULT 2,
                last_price  REAL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active   INTEGER DEFAULT 1
            )
        """)
        await db.commit()


async def db_add_subscription(user_id, from_code, from_name, to_code, to_name,
                               date_from, date_to, passengers, max_stops, last_price):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO subscriptions
                (user_id, from_code, from_name, to_code, to_name,
                 date_from, date_to, passengers, max_stops, last_price)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (user_id, from_code, from_name, to_code, to_name,
              date_from, date_to, passengers, max_stops, last_price))
        await db.commit()
        return cur.lastrowid


async def db_get_user_subs(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM subscriptions WHERE user_id=? AND is_active=1 ORDER BY id DESC",
            (user_id,)
        )
        return await cur.fetchall()


async def db_get_all_subs():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM subscriptions WHERE is_active=1")
        return await cur.fetchall()


async def db_update_price(sub_id, price):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscriptions SET last_price=? WHERE id=?", (price, sub_id))
        await db.commit()


async def db_deactivate(sub_id, user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE subscriptions SET is_active=0 WHERE id=? AND user_id=?",
            (sub_id, user_id)
        )
        await db.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# ПОИСК АВИАБИЛЕТОВ
# ═══════════════════════════════════════════════════════════════════════════════

async def find_location(query: str) -> list:
    """Ищет аэропорт/город через Travelpayouts autocomplete (бесплатно, без ключа)."""
    url = "https://autocomplete.travelpayouts.com/places2"
    params = {
        "term": query,
        "locale": "ru",
        "types[]": ["city", "airport"],
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params,
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    results = []
                    for loc in data[:5]:
                        code = loc.get("code", "")
                        name = loc.get("name", code)
                        country = loc.get("country_name", "")
                        if not code:
                            continue
                        results.append({
                            "code": code,
                            "name": name,
                            "country": country,
                            "label": f"{name} ({code}) — {country}",
                        })
                    return results
    except Exception as e:
        logger.error(f"Location search error: {e}")
    return []


async def search_kiwi(from_code, to_code, date_from, date_to, passengers, max_stops) -> list:
    """Поиск билетов через Kiwi.com Tequila API."""
    url = "https://tequila.kiwi.com/v2/search"
    # Kiwi ожидает DD/MM/YYYY
    def fmt(d):
        return d.replace(".", "/")

    params = {
        "fly_from": from_code,
        "fly_to": to_code,
        "date_from": fmt(date_from),
        "date_to": fmt(date_to),
        "adults": passengers,
        "selected_cabins": "M",
        "curr": "RUB",
        "limit": 8,
        "sort": "price",
        "asc": 1,
        "max_stopovers": max_stops,
        "partner": "picky",
        "one_for_city": 0,
    }
    headers = {"apikey": KIWI_API_KEY}

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params, headers=headers,
                             timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    results = []
                    for f in data.get("data", [])[:6]:
                        route = f.get("route", [])
                        stops = max(len(route) - 1, 0)
                        total_sec = f.get("duration", {}).get("total", 0)
                        h, m = divmod(total_sec // 60, 60)
                        airlines = list(dict.fromkeys(r.get("airline", "") for r in route))
                        try:
                            dep = datetime.utcfromtimestamp(f["dTime"]).strftime("%d.%m %H:%M")
                            arr = datetime.utcfromtimestamp(f["aTime"]).strftime("%d.%m %H:%M")
                        except Exception:
                            dep = arr = "—"

                        results.append({
                            "source": "Kiwi.com",
                            "price": int(f.get("price", 0)),
                            "stops": stops,
                            "duration": f"{h}ч {m:02d}мин",
                            "airlines": ", ".join(airlines) or "—",
                            "dep": dep,
                            "arr": arr,
                            "link": f.get("deep_link", "https://www.kiwi.com"),
                        })
                    return results
    except Exception as e:
        logger.error(f"Kiwi search error: {e}")
    return []


async def search_aviasales(from_code, to_code, date_from, date_to, passengers, max_stops) -> list:
    """Поиск через Aviasales/Travelpayouts (кэшированные дешёвые цены)."""
    url = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"

    try:
        dt_from = datetime.strptime(date_from, "%d.%m.%Y")
        dt_to   = datetime.strptime(date_to,   "%d.%m.%Y")
    except Exception:
        dt_from = dt_to = None

    params = {
        "origin": from_code,
        "destination": to_code,
        "departure_at": dt_from.strftime("%Y-%m") if dt_from else date_from[:7],
        "unique": "false",
        "sorting": "price",
        "direct": "true" if max_stops == 0 else "false",
        "currency": "rub",
        "limit": 30,
        "one_way": "true",
        "token": AVIASALES_TOKEN,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params,
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    data = await r.json()
                    results = []
                    for f in data.get("data", []):
                        dep_raw = f.get("departure_at", "")
                        stops   = f.get("transfers", 0)
                        price   = int(f.get("price", 0))

                        if stops > max_stops:
                            continue

                        if dt_from and dt_to:
                            try:
                                dep_dt = datetime.fromisoformat(dep_raw[:10])
                                if not (dt_from <= dep_dt <= dt_to):
                                    continue
                            except Exception:
                                pass

                        try:
                            dep_dt_obj = datetime.fromisoformat(dep_raw.rstrip("Z"))
                            dep = dep_dt_obj.strftime("%d.%m %H:%M")
                            # Правильный формат ссылки: DDMM
                            dep_date_url = dep_dt_obj.strftime("%d%m")
                        except Exception:
                            dep = dep_raw[:10]
                            dep_date_url = dep_raw[8:10] + dep_raw[5:7]  # fallback

                        airline  = f.get("airline", "")
                        fnum     = f.get("flight_number", "")
                        has_bag  = f.get("has_baggage")
                        baggage  = ("🧳 с багажом" if has_bag else "🎒 ручная кладь") if has_bag is not None else ""

                        # Правильная ссылка Aviasales: CODE + DDMM + CODE + passengers
                        link = f"https://www.aviasales.ru/search/{from_code}{dep_date_url}{to_code}{passengers}"

                        results.append({
                            "source": "Aviasales",
                            "price": price,
                            "stops": stops,
                            "duration": "—",
                            "airlines": f"{airline} {fnum}".strip() or "—",
                            "dep": dep,
                            "arr": "—",
                            "baggage": baggage,
                            "link": link,
                            "multi_leg": False,
                        })
                        if len(results) >= 10:
                            break
                    return results
    except Exception as e:
        logger.error(f"Aviasales search error: {e}")
    return []


def trip_link(from_code, to_code, date_from, passengers) -> str:
    try:
        ds = datetime.strptime(date_from, "%d.%m.%Y").strftime("%Y%m%d")
    except Exception:
        ds = ""
    return (
        f"https://www.trip.com/flights/{from_code.lower()}-to-{to_code.lower()}/"
        f"?depdate={ds}&adult={passengers}&curr=RUB"
    )


# ─── Хабы для поиска составных маршрутов ────────────────────────────────────
SMART_HUBS = [
    ("ALA", "Алматы"),
    ("TAS", "Ташкент"),
    ("IST", "Стамбул"),
    ("DXB", "Дубай"),
    ("BKK", "Бангкок"),
    ("SVO", "Москва"),
    ("LED", "Питер"),
    ("SVX", "Екатеринбург"),
    ("NSK", "Новосибирск"),
    ("HKG", "Гонконг"),
    ("KUL", "Куала-Лумпур"),
]


async def search_via_hub(from_code, hub_code, hub_name, to_code,
                         date_from, date_to, passengers) -> dict | None:
    """Ищет маршрут из A через хаб H в B и суммирует цены."""
    leg1, leg2 = await asyncio.gather(
        search_aviasales(from_code, hub_code, date_from, date_to, passengers, 1),
        search_aviasales(hub_code,  to_code,  date_from, date_to, passengers, 1),
        return_exceptions=True,
    )
    if not (isinstance(leg1, list) and leg1 and isinstance(leg2, list) and leg2):
        return None

    total = leg1[0]["price"] + leg2[0]["price"]
    return {
        "source": f"через {hub_name}",
        "price": total,
        "stops": leg1[0]["stops"] + leg2[0]["stops"] + 1,
        "duration": "—",
        "airlines": f"{leg1[0]['airlines']} → {leg2[0]['airlines']}",
        "dep": leg1[0]["dep"],
        "arr": leg2[0]["dep"],
        "baggage": "",
        "link": leg1[0]["link"],
        "link2": leg2[0]["link"],
        "hub": hub_name,
        "multi_leg": True,
        "leg1_price": leg1[0]["price"],
        "leg2_price": leg2[0]["price"],
    }


async def search_all(from_code, to_code, date_from, date_to, passengers, max_stops) -> list:
    """Ищет прямые/с пересадками + умный поиск через хабы."""
    kiwi_task = search_kiwi(from_code, to_code, date_from, date_to, passengers, max_stops)
    avia_task = search_aviasales(from_code, to_code, date_from, date_to, passengers, max_stops)

    kiwi_res, avia_res = await asyncio.gather(kiwi_task, avia_task, return_exceptions=True)

    combined = []
    if isinstance(kiwi_res, list):
        combined.extend(kiwi_res)
    if isinstance(avia_res, list):
        combined.extend(avia_res)

    # Умный поиск через хабы параллельно
    hub_tasks = [
        search_via_hub(from_code, hub_code, hub_name, to_code,
                       date_from, date_to, passengers)
        for hub_code, hub_name in SMART_HUBS
        if hub_code not in (from_code, to_code)
    ]
    hub_results = await asyncio.gather(*hub_tasks, return_exceptions=True)
    for r in hub_results:
        if isinstance(r, dict) and r:
            combined.append(r)

    # Дедупликация и сортировка по цене
    seen = set()
    unique = []
    for f in sorted(combined, key=lambda x: x["price"]):
        key = (f["price"], f["airlines"])
        if key not in seen:
            seen.add(key)
            unique.append(f)

    return unique[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

def fmt_price(p: int) -> str:
    return f"{p:,}".replace(",", " ") + " ₽"


def fmt_stops(n: int) -> str:
    if n == 0:
        return "✅ Прямой"
    if n == 1:
        return "🔄 1 пересадка"
    return f"🔄 {n} пересадки"


def build_results_text(results, from_name, to_name, tlink) -> str:
    if not results:
        return (
            "😔 *Ничего не найдено*\n\n"
            "Попробуйте:\n"
            "• другие даты\n"
            "• разрешить пересадки\n"
            "• более широкий диапазон дат"
        )

    text = f"✈️ *{from_name} → {to_name}*\n"
    text += f"Найдено {len(results)} вариантов — от дешёвого к дорогому:\n\n"

    for i, f in enumerate(results[:10], 1):
        medal = " 🏆" if i == 1 else ""
        text += f"*{i}. {fmt_price(f['price'])}*{medal}\n"

        if f.get("multi_leg"):
            # Составной маршрут через хаб
            text += f"   🗺 Маршрут через {f['hub']}\n"
            text += f"   ✈️ {f['airlines']}\n"
            text += f"   🕐 Вылет: {f['dep']}\n"
            text += f"   {fmt_stops(f['stops'])}\n"
            l1_price = fmt_price(f.get('leg1_price', 0))
            l2_price = fmt_price(f.get('leg2_price', 0))
            text += f"   💰 {l1_price} + {l2_price}\n"
            text += f"   🔗 [Плечо 1]({f['link']})  •  [Плечо 2]({f.get('link2', f['link'])})\n\n"
        else:
            text += f"   ✈️ {f['airlines']}\n"
            text += f"   🕐 {f['dep']}"
            if f.get("arr") and f["arr"] != "—":
                text += f" → {f['arr']}"
            if f.get("duration") and f["duration"] != "—":
                text += f" ({f['duration']})"
            text += "\n"
            text += f"   {fmt_stops(f['stops'])}\n"
            if f.get("baggage"):
                text += f"   {f['baggage']}\n"
            text += f"   🔗 [{f['source']}]({f['link']})\n\n"

    text += f"🌏 [Trip.com — поискать ещё]({tlink})\n"
    text += "💡 _Кликай на ссылки — откроется страница бронирования._"
    return text


# ═══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✈️ *Привет! Я ищу дешёвые авиабилеты по всему миру.*\n\n"
        "Проверяю сразу три источника:\n"
        "• 🌍 Kiwi.com — весь мир, бюджетные авиакомпании\n"
        "• 🇷🇺 Aviasales — СНГ и международные рейсы\n"
        "• 🌏 Trip.com — особенно хорош для Азии\n\n"
        "Что умею:\n"
        "🔍 /search — найти билеты\n"
        "🔔 /myroutes — мои подписки\n"
        "❓ /help — помощь\n\n"
        "Начнём? Жми /search 👇",
        parse_mode="Markdown",
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🆘 *Как пользоваться ботом*\n\n"
        "1. Нажми /search\n"
        "2. Я спрошу: *откуда → куда → даты → пассажиры → пересадки*\n"
        "3. Ищу во всех источниках и показываю лучшие цены\n"
        "4. Нажми «Подписаться» — я буду мониторить цену каждые 2 часа\n"
        "   и пришлю уведомление если цена упадёт 📉\n\n"
        "📌 Города можно писать *на русском или английском*\n"
        "📌 Даты в формате: `01.07.2026` или период `01.07.2026-10.07.2026`\n\n"
        "Управление подписками: /myroutes",
        parse_mode="Markdown",
    )


# ── Начало поиска ─────────────────────────────────────────────────────────────

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "🔍 *Новый поиск*\n\n"
        "Откуда летим? Напишите город или аэропорт\n"
        "_(например: Барнаул, Алматы, Москва, Bangkok, Dubai)_",
        parse_mode="Markdown",
    )
    return FROM_CITY


async def handle_from_city(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    msg = await update.message.reply_text("🔎 Ищу аэропорт...")
    locs = await find_location(query)

    if not locs:
        await msg.edit_text(
            "❌ Не нашёл такой город. Попробуйте написать иначе или на английском:"
        )
        return FROM_CITY

    if len(locs) == 1:
        ctx.user_data["from_code"] = locs[0]["code"]
        ctx.user_data["from_name"] = locs[0]["name"]
        await msg.edit_text(
            f"✅ *{locs[0]['name']} ({locs[0]['code']})*\n\n"
            "Куда летим? Напишите город назначения:",
            parse_mode="Markdown",
        )
        return TO_CITY

    ctx.user_data["_locs_from"] = locs
    kb = [[InlineKeyboardButton(l["label"], callback_data=f"F{i}")] for i, l in enumerate(locs[:4])]
    await msg.edit_text("Уточните аэропорт:", reply_markup=InlineKeyboardMarkup(kb))
    return FROM_CITY


async def cb_select_from(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data[1:])
    loc = ctx.user_data["_locs_from"][idx]
    ctx.user_data["from_code"] = loc["code"]
    ctx.user_data["from_name"] = loc["name"]
    await q.edit_message_text(
        f"✅ *{loc['name']} ({loc['code']})*\n\nКуда летим?",
        parse_mode="Markdown",
    )
    return TO_CITY


async def handle_to_city(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    msg = await update.message.reply_text("🔎 Ищу аэропорт...")
    locs = await find_location(query)

    if not locs:
        await msg.edit_text("❌ Не нашёл. Попробуйте иначе или на английском:")
        return TO_CITY

    if len(locs) == 1:
        ctx.user_data["to_code"] = locs[0]["code"]
        ctx.user_data["to_name"] = locs[0]["name"]
        await msg.edit_text(
            f"✅ *{locs[0]['name']} ({locs[0]['code']})*\n\n"
            "📅 Напишите дату вылета или период:\n"
            "• Один день: `01.07.2026`\n"
            "• Период: `01.07.2026-10.07.2026`",
            parse_mode="Markdown",
        )
        return DATES

    ctx.user_data["_locs_to"] = locs
    kb = [[InlineKeyboardButton(l["label"], callback_data=f"T{i}")] for i, l in enumerate(locs[:4])]
    await msg.edit_text("Уточните аэропорт:", reply_markup=InlineKeyboardMarkup(kb))
    return TO_CITY


async def cb_select_to(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    idx = int(q.data[1:])
    loc = ctx.user_data["_locs_to"][idx]
    ctx.user_data["to_code"] = loc["code"]
    ctx.user_data["to_name"] = loc["name"]
    await q.edit_message_text(
        f"✅ *{loc['name']} ({loc['code']})*\n\n"
        "📅 Дата вылета или период:\n"
        "• `01.07.2026`\n"
        "• `01.07.2026-10.07.2026`",
        parse_mode="Markdown",
    )
    return DATES


async def handle_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # Попытка разобрать период DD.MM.YYYY-DD.MM.YYYY
    if "-" in text:
        parts = text.split("-", 1)
        d_from, d_to = parts[0].strip(), parts[1].strip()
    else:
        d_from = d_to = text.strip()

    try:
        datetime.strptime(d_from, "%d.%m.%Y")
        datetime.strptime(d_to, "%d.%m.%Y")
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат. Пример:\n"
            "• `01.07.2026`\n"
            "• `01.07.2026-10.07.2026`",
            parse_mode="Markdown",
        )
        return DATES

    ctx.user_data["date_from"] = d_from
    ctx.user_data["date_to"] = d_to

    kb = [
        [
            InlineKeyboardButton("1 чел.", callback_data="P1"),
            InlineKeyboardButton("2 чел.", callback_data="P2"),
            InlineKeyboardButton("3 чел.", callback_data="P3"),
        ],
        [
            InlineKeyboardButton("4 чел.", callback_data="P4"),
            InlineKeyboardButton("5 чел.", callback_data="P5"),
            InlineKeyboardButton("6 чел.", callback_data="P6"),
        ],
    ]
    await update.message.reply_text(
        f"✅ {d_from} → {d_to}\n\n👥 Сколько пассажиров?",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return PASSENGERS


async def cb_passengers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    n = int(q.data[1:])
    ctx.user_data["passengers"] = n

    kb = [
        [
            InlineKeyboardButton("✅ Только прямые", callback_data="S0"),
        ],
        [
            InlineKeyboardButton("🔄 До 1 пересадки", callback_data="S1"),
        ],
        [
            InlineKeyboardButton("🔄 Любые пересадки", callback_data="S2"),
        ],
    ]
    await q.edit_message_text(
        f"✅ {n} пассажир(а)\n\n✈️ Пересадки?",
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return STOPS


async def cb_stops_and_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    max_stops = int(q.data[1:])
    ctx.user_data["max_stops"] = max_stops

    from_code = ctx.user_data["from_code"]
    from_name = ctx.user_data["from_name"]
    to_code   = ctx.user_data["to_code"]
    to_name   = ctx.user_data["to_name"]
    date_from = ctx.user_data["date_from"]
    date_to   = ctx.user_data["date_to"]
    passengers = ctx.user_data["passengers"]

    stops_label = ["только прямые", "до 1 пересадки", "любые пересадки"][max_stops]

    await q.edit_message_text(
        f"🔍 *Ищу билеты...*\n\n"
        f"📍 {from_name} → {to_name}\n"
        f"📅 {date_from} — {date_to}\n"
        f"👥 {passengers} пас. | {stops_label}\n\n"
        "_Проверяю Kiwi.com, Aviasales и Trip.com..._",
        parse_mode="Markdown",
    )

    results = await search_all(from_code, to_code, date_from, date_to, passengers, max_stops)
    tlink = trip_link(from_code, to_code, date_from, passengers)
    text = build_results_text(results, from_name, to_name, tlink)

    kb = []
    if results:
        best = results[0]["price"]
        # Упаковываем параметры в callback (макс 64 байта → храним в user_data)
        ctx.user_data["_sub_params"] = {
            "from_code": from_code, "from_name": from_name,
            "to_code": to_code, "to_name": to_name,
            "date_from": date_from, "date_to": date_to,
            "passengers": passengers, "max_stops": max_stops,
            "last_price": best,
        }
        kb.append([InlineKeyboardButton(
            f"🔔 Следить за ценой (сейчас {fmt_price(best)})",
            callback_data="DO_SUBSCRIBE",
        )])
    kb.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="NEW_SEARCH")])

    await q.edit_message_text(
        text,
        parse_mode="Markdown",
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(kb),
    )
    return ConversationHandler.END


# ── Подписка ──────────────────────────────────────────────────────────────────

async def cb_subscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("✅ Подписка оформлена!")

    p = ctx.user_data.get("_sub_params")
    if not p:
        await q.message.reply_text("❌ Не удалось оформить подписку. Начните поиск заново: /search")
        return

    await db_add_subscription(
        user_id=q.from_user.id,
        from_code=p["from_code"], from_name=p["from_name"],
        to_code=p["to_code"],   to_name=p["to_name"],
        date_from=p["date_from"], date_to=p["date_to"],
        passengers=p["passengers"], max_stops=p["max_stops"],
        last_price=p["last_price"],
    )

    await q.message.reply_text(
        f"🔔 *Подписка активирована!*\n\n"
        f"✈️ {p['from_name']} → {p['to_name']}\n"
        f"📅 {p['date_from']} — {p['date_to']}\n"
        f"💰 Цена сейчас: {fmt_price(p['last_price'])}\n\n"
        f"Проверяю каждые {CHECK_INTERVAL_HOURS} часа и пришлю уведомление,\n"
        f"если найду цену ниже 📉\n\n"
        f"Управление подписками: /myroutes",
        parse_mode="Markdown",
    )


# ── Мои маршруты ─────────────────────────────────────────────────────────────

async def cmd_myroutes(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    subs = await db_get_user_subs(update.effective_user.id)

    if not subs:
        await update.message.reply_text(
            "📭 Активных подписок нет.\n\n"
            "Найдите билеты через /search и нажмите «Следить за ценой»."
        )
        return

    text = f"🔔 *Ваши подписки ({len(subs)}):*\n\n"
    kb = []
    for s in subs:
        price_str = fmt_price(s["last_price"]) if s["last_price"] else "—"
        text += (
            f"*{s['from_name']} → {s['to_name']}*\n"
            f"📅 {s['date_from']} — {s['date_to']}\n"
            f"💰 Последняя цена: {price_str}\n\n"
        )
        kb.append([InlineKeyboardButton(
            f"❌ {s['from_name'][:12]} → {s['to_name'][:12]}",
            callback_data=f"UNSUB_{s['id']}",
        )])

    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb),
    )


async def cb_unsubscribe(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    sub_id = int(q.data.split("_", 1)[1])
    await db_deactivate(sub_id, q.from_user.id)
    await q.answer("✅ Отписались")
    await q.edit_message_text("✅ Подписка отменена.\n\nОставшиеся подписки: /myroutes")


# ── Новый поиск через кнопку ──────────────────────────────────────────────────

async def cb_new_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx.user_data.clear()
    await q.message.reply_text(
        "🔍 Откуда летим? Напишите город:",
        parse_mode="Markdown",
    )
    return FROM_CITY


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("❌ Поиск отменён.\n\n/search — начать заново.")
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# МОНИТОРИНГ ЦЕН (запускается по расписанию)
# ═══════════════════════════════════════════════════════════════════════════════

async def monitor_prices(app: Application):
    logger.info("Запуск проверки цен по подпискам...")
    subs = await db_get_all_subs()

    for sub in subs:
        try:
            results = await search_all(
                sub["from_code"], sub["to_code"],
                sub["date_from"], sub["date_to"],
                sub["passengers"], sub["max_stops"],
            )
            if not results:
                continue

            new_price = results[0]["price"]
            old_price = sub["last_price"] or 9_999_999
            drop = old_price - new_price

            # Уведомляем если цена упала на 500₽ или на 5%
            if drop >= 500 or (drop / old_price) >= 0.05:
                best = results[0]
                tlink = trip_link(sub["from_code"], sub["to_code"], sub["date_from"], sub["passengers"])

                msg = (
                    f"📉 *Цена упала!*\n\n"
                    f"✈️ {sub['from_name']} → {sub['to_name']}\n"
                    f"📅 {sub['date_from']} — {sub['date_to']}\n\n"
                    f"💸 Было: {fmt_price(int(old_price))}\n"
                    f"💚 Стало: *{fmt_price(new_price)}*\n"
                    f"📉 Экономия: {fmt_price(int(drop))} "
                    f"({drop / old_price * 100:.1f}%)\n\n"
                    f"✈️ {best['airlines']}\n"
                    f"🕐 {best['dep']} → {best['arr']}"
                )
                if best["duration"] != "—":
                    msg += f" ({best['duration']})"
                msg += (
                    f"\n{fmt_stops(best['stops'])}\n\n"
                    f"🔗 [Купить на {best['source']}]({best['link']})\n"
                    f"🌏 [Trip.com]({tlink})"
                )

                await app.bot.send_message(
                    chat_id=sub["user_id"],
                    text=msg,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )

            if new_price < old_price:
                await db_update_price(sub["id"], new_price)

        except Exception as e:
            logger.error(f"Ошибка мониторинга подписки {sub['id']}: {e}")

        await asyncio.sleep(1)  # пауза между запросами


# ═══════════════════════════════════════════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════════════════════════════════════════

async def on_startup(app):
    """Запускается при старте PTB — инициализируем БД и планировщик."""
    await db_init()
    logger.info("✈️ БД инициализирована")

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        monitor_prices,
        trigger="interval",
        hours=CHECK_INTERVAL_HOURS,
        args=[app],
        id="price_monitor",
        next_run_time=None,
    )
    scheduler.start()
    logger.info("✈️ Планировщик запущен")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в .env")
    if not KIWI_API_KEY:
        logger.warning("KIWI_API_KEY не задан — поиск Kiwi.com не будет работать")
    if not AVIASALES_TOKEN:
        logger.warning("AVIASALES_TOKEN не задан — поиск Aviasales не будет работать")

    app = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()

    # ── ConversationHandler для пошагового поиска ────────────────────────────
    conv = ConversationHandler(
        entry_points=[
            CommandHandler("search", cmd_search),
            CallbackQueryHandler(cb_new_search, pattern="^NEW_SEARCH$"),
        ],
        states={
            FROM_CITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_from_city),
                CallbackQueryHandler(cb_select_from, pattern=r"^F\d+$"),
            ],
            TO_CITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_to_city),
                CallbackQueryHandler(cb_select_to, pattern=r"^T\d+$"),
            ],
            DATES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_dates),
            ],
            PASSENGERS: [
                CallbackQueryHandler(cb_passengers, pattern=r"^P\d+$"),
            ],
            STOPS: [
                CallbackQueryHandler(cb_stops_and_search, pattern=r"^S\d+$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("myroutes", cmd_myroutes))
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(cb_subscribe,   pattern="^DO_SUBSCRIBE$"))
    app.add_handler(CallbackQueryHandler(cb_unsubscribe, pattern=r"^UNSUB_\d+$"))

    logger.info("✈️ Бот запущен!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
