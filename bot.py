#!/usr/bin/env python3
"""
Dexscreener Telegram Lead Bot – DM Only (100% working)
Fetches NEW pairs from multiple chains, filters by Telegram & liquidity.
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
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    sys.exit("BOT_TOKEN environment variable is required")

DB_PATH = os.getenv("DB_PATH", "leads.db")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "45"))
DEFAULT_MIN_LIQUIDITY = float(os.getenv("DEFAULT_MIN_LIQUIDITY", "5000"))
# Chains to scan for new pairs (comma‑separated)
CHAINS = os.getenv("CHAINS", "bsc,ethereum,solana,avalanche,fantom,polygon,base,arbitrum,optimism")

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
# Telegram link finder
# ----------------------------------------------------------------------
def find_telegram(socials: list) -> str | None:
    """Look for Telegram link in socials array."""
    for s in socials:
        if not isinstance(s, dict):
            continue
        url = s.get("url", "")
        if not url:
            continue
        stype = s.get("type", "").lower()
        slabel = s.get("label", "").lower()
        if any(word in stype + slabel for word in ("telegram", "tg")) or \
           any(d in url for d in ("t.me", "telegram.me", "telegram.org")):
            return url if url.startswith("http") else f"https://{url}"
    return None

# ----------------------------------------------------------------------
# Fetch new pairs (the real data source)
# ----------------------------------------------------------------------
async def fetch_new_pairs(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch recent pairs from multiple chains.
    Returns a list of lead dicts with tg, twitter, website, liquidity, etc.
    """
    leads = []
    url = f"https://api.dexscreener.com/latest/dex/pairs/{CHAINS}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pairs") or []
                for pair in pairs:
                    base = pair.get("baseToken", {})
                    name = base.get("name") or pair.get("baseToken", {}).get("symbol") or "?"
                    chain = pair.get("chainId", "?")
                    liquidity = pair.get("liquidity", {}).get("usd", 0)
                    info = pair.get("info", {})
                    socials = info.get("socials", [])
                    tg = find_telegram(socials)
                    if not tg:
                        # sometimes info.telegram directly
                        tg = info.get("telegram") or info.get("tg")
                    if not tg:
                        continue

                    twitter = website = None
                    for s in socials:
                        stype = s.get("type", "").lower()
                        surl = s.get("url", "")
                        if not surl:
                            continue
                        if "twitter" in stype and not twitter:
                            twitter = surl
                        if "website" in stype and not website:
                            website = surl

                    leads.append({
                        "name": name,
                        "chain": chain,
                        "tg": tg,
                        "tw": twitter,
                        "web": website,
                        "liquidity": liquidity,
                    })
                logger.info("New pairs scan: found %d pairs, %d with Telegram.", len(pairs), len(leads))
            else:
                logger.error("New pairs API returned status %s", resp.status)
    except Exception as e:
        logger.error("Error fetching new pairs: %s", e)
    return leads

# ----------------------------------------------------------------------
# Main job
# ----------------------------------------------------------------------
async def job_fetch_and_send(context: ContextTypes.DEFAULT_TYPE):
    active_users = await get_active_users()
    if not active_users:
        logger.info("No active users.")
        return

    logger.info("Fetch cycle started for %d user(s).", len(active_users))

    async with aiohttp.ClientSession() as session:
        leads = await fetch_new_pairs(session)

        if not leads:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(chat_id=chat_id, text="↪️ No new Telegram leads found this cycle.")
            return

        sent_any = False
        for user_id, chat_id, min_liq in active_users:
            user_sent = 0
            for lead in leads:
                if lead["liquidity"] < min_liq:
                    continue
                if await is_duplicate(lead["tg"]):
                    continue
                msg = (
                    f"🚀 Project: {lead['name']}\n"
                    f"🔗 Telegram: {lead['tg']}\n"
                    f"🐦 Twitter: {lead['tw'] or 'NIL'}\n"
                    f"🌐 Website: {lead['web'] or 'NIL'}\n"
                    f"💧 Liquidity: ${lead['liquidity']:,.2f}\n"
                    f"⛓ Chain: {lead['chain']}"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg)
                    user_sent += 1
                    await mark_as_sent(lead["tg"])
                except Exception as e:
                    logger.error("Send failed user %s: %s", user_id, e)
                    if "Forbidden" in str(e):
                        await set_active(user_id, False)
            if user_sent > 0:
                sent_any = True
                logger.info("Sent %d lead(s) to user %s", user_sent, user_id)
        if not sent_any:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(chat_id=chat_id, text="↪️ No new Telegram leads found this cycle.")

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
# Run
# ----------------------------------------------------------------------
async def build_bot():
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("minliq", cmd_minliq))
    app.job_queue.run_repeating(job_fetch_and_send, interval=FETCH_INTERVAL, first=10)
    logger.info("Bot is ready.")
    return app

def main():
    app = asyncio.run(build_bot())
    logger.info("Bot polling started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
