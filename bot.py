#!/usr/bin/env python3
"""
Dexscreener Telegram Lead Bot – DM Only
Hosting-ready version for Railway with persistent SQLite storage.
"""

import asyncio
import logging
import os
import sys

import aiohttp
import aiosqlite
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ----------------------------------------------------------------------
# Configuration (all from environment)
# ----------------------------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    sys.exit("BOT_TOKEN environment variable is required")

DB_PATH = os.getenv("DB_PATH", "leads.db")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "45"))
DEFAULT_MIN_LIQUIDITY = float(os.getenv("DEFAULT_MIN_LIQUIDITY", "5000"))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------
async def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent_links (
                telegram_url TEXT PRIMARY KEY,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                active INTEGER DEFAULT 1,
                min_liquidity REAL DEFAULT 5000.0
            )
        """)
        await db.commit()

async def get_active_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_id, chat_id, min_liquidity FROM users WHERE active = 1"
        )
        return await cursor.fetchall()

async def is_duplicate(telegram_url: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM sent_links WHERE telegram_url = ?", (telegram_url,)
        )
        return await cursor.fetchone() is not None

async def mark_as_sent(telegram_url: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO sent_links (telegram_url) VALUES (?)",
            (telegram_url,),
        )
        await db.commit()

async def upsert_user(user_id: int, chat_id: int, active: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, chat_id, active, min_liquidity)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                chat_id = excluded.chat_id,
                active = excluded.active
            """,
            (user_id, chat_id, int(active), DEFAULT_MIN_LIQUIDITY),
        )
        await db.commit()

async def set_active(user_id: int, active: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET active = ? WHERE user_id = ?",
            (int(active), user_id),
        )
        await db.commit()

async def set_min_liquidity(user_id: int, value: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET min_liquidity = ? WHERE user_id = ?",
            (value, user_id),
        )
        await db.commit()

# ----------------------------------------------------------------------
# Dexscreener API
# ----------------------------------------------------------------------
async def fetch_latest_profiles(session: aiohttp.ClientSession) -> list[dict]:
    url = "https://api.dexscreener.com/token-profiles/latest/v1"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    return data
                logger.warning("Unexpected API response format: %s", type(data))
            else:
                logger.error("Profiles API returned status %s", resp.status)
    except Exception as e:
        logger.error("Error fetching profiles: %s", e)
    return []

async def fetch_liquidity_batch(session: aiohttp.ClientSession, addresses: list[str]) -> dict:
    result = {}
    base_url = "https://api.dexscreener.com/latest/dex/tokens/"
    chunk_size = 30
    for i in range(0, len(addresses), chunk_size):
        chunk = addresses[i:i + chunk_size]
        url = base_url + ",".join(chunk)
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for pair in data.get("pairs", []):
                        base = pair.get("baseToken", {})
                        addr = base.get("address")
                        if not addr:
                            continue
                        liq = pair.get("liquidity", {}).get("usd", 0)
                        result[addr] = result.get(addr, 0) + liq
        except Exception as e:
            logger.error("Error fetching liquidity chunk: %s", e)
    return result

# ----------------------------------------------------------------------
# Main fetch & send loop
# ----------------------------------------------------------------------
async def job_fetch_and_send(context: ContextTypes.DEFAULT_TYPE):
    active_users = await get_active_users()
    if not active_users:
        return

    async with aiohttp.ClientSession() as session:
        profiles = await fetch_latest_profiles(session)
        if not profiles:
            return

        candidates = []
        seen_now = set()

        for token in profiles:
            if not all(k in token for k in ("name", "chainId", "tokenAddress", "links")):
                continue

            tg_link = None
            twitter = None
            website = None
            for link in token.get("links", []):
                if isinstance(link, dict):
                    lt = link.get("type", "").lower()
                    url = link.get("url", "")
                    if not url:
                        continue
                    if lt == "telegram" and url.startswith("http"):
                        tg_link = url
                    elif lt == "twitter":
                        twitter = url
                    elif lt == "website":
                        website = url

            if not tg_link:
                continue
            if tg_link in seen_now or await is_duplicate(tg_link):
                continue

            full_addr = f"{token['chainId']}:{token['tokenAddress']}"
            candidates.append({
                "name": token["name"],
                "chain": token["chainId"],
                "tg": tg_link,
                "tw": twitter,
                "web": website,
                "addr": token["tokenAddress"],
                "full_addr": full_addr,
            })

        if not candidates:
            return

        liq_map = await fetch_liquidity_batch(
            session, [c["full_addr"] for c in candidates]
        )

        for user_id, chat_id, min_liq in active_users:
            for cand in candidates:
                total_liq = liq_map.get(cand["addr"], 0.0)
                if total_liq < min_liq:
                    continue

                msg = (
                    f"🚀 Project: {cand['name']}\n"
                    f"🔗 Telegram: {cand['tg']}\n"
                    f"🐦 Twitter: {cand['tw'] or 'NIL'}\n"
                    f"🌐 Website: {cand['web'] or 'NIL'}\n"
                    f"💧 Liquidity: ${total_liq:,.2f}\n"
                    f"⛓ Chain: {cand['chain']}"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg)
                    logger.info("Sent %s to user %s", cand['name'], user_id)
                    await mark_as_sent(cand["tg"])
                    seen_now.add(cand["tg"])
                except Exception as e:
                    logger.error("Send failed to user %s: %s", user_id, e)
                    if "Forbidden" in str(e):
                        await set_active(user_id, False)
                        logger.info("User %s deactivated", user_id)

# ----------------------------------------------------------------------
# Command handlers
# ----------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    await upsert_user(user.id, chat.id, active=True)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT min_liquidity FROM users WHERE user_id = ?", (user.id,)
        )
        row = await cursor.fetchone()
        min_liq = row[0] if row else DEFAULT_MIN_LIQUIDITY
    await update.message.reply_text(
        f"✅ Bot activated!\nMinimum liquidity: ${min_liq:,.2f}\n"
        "Use /minliq <value> to change it.\n/stop to pause."
    )

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await set_active(update.effective_user.id, False)
    await update.message.reply_text("⏸ Bot paused. /start to resume.")

async def cmd_minliq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /minliq <amount>")
        return
    try:
        value = float(context.args[0])
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a positive number.")
        return
    await set_min_liquidity(update.effective_user.id, value)
    await update.message.reply_text(f"💰 Minimum liquidity set to ${value:,.2f}")

# ----------------------------------------------------------------------
# Setup & run
# ----------------------------------------------------------------------
async def build_bot():
    """Async initialisation (database, handlers, job queue).
    Returns the fully configured Application."""
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("minliq", cmd_minliq))
    app.job_queue.run_repeating(job_fetch_and_send, interval=FETCH_INTERVAL, first=10)
    logger.info("Bot is ready.")
    return app

def main():
    # Run async setup to get the app
    app = asyncio.run(build_bot())
    # Then start polling – this call is blocking and manages the event loop itself
    logger.info("Bot polling started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
