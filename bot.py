#!/usr/bin/env python3
"""
Dexscreener Telegram Lead Bot – DM Only
Final diagnostic – dumps raw first pair to find Telegram key.
"""

import asyncio
import json
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
# Dexscreener API helpers
# ----------------------------------------------------------------------
async def fetch_token_addresses(session: aiohttp.ClientSession, url: str, label: str) -> set[str]:
    addresses = set()
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    for token in data:
                        chain = token.get("chainId")
                        addr = token.get("tokenAddress")
                        if chain and addr:
                            addresses.add(f"{chain}:{addr}")
                    logger.info("%s: collected %d addresses.", label, len(addresses))
                    return addresses
                logger.warning("%s: unexpected format %s", label, type(data))
            else:
                logger.error("%s: status %s", label, resp.status)
    except Exception as e:
        logger.error("%s: request failed: %s", label, e)
    return addresses

def find_telegram_in_socials(socials: list) -> str | None:
    for s in socials:
        if not isinstance(s, dict):
            continue
        url = s.get("url", "")
        if not url:
            continue
        stype = s.get("type", "").lower()
        slabel = s.get("label", "").lower()
        if "telegram" in stype or "tg" in stype or \
           "telegram" in slabel or "tg" in slabel or \
           "t.me" in url or "telegram.me" in url or "telegram.org" in url:
            return url if url.startswith("http") else f"https://{url}"
    return None

async def fetch_pair_data(session: aiohttp.ClientSession, addresses: set[str]) -> list[dict]:
    leads = []
    base_url = "https://api.dexscreener.com/latest/dex/tokens/"
    addr_list = list(addresses)
    chunk_size = 30
    dumped = False

    for i in range(0, len(addr_list), chunk_size):
        chunk = addr_list[i:i + chunk_size]
        url = base_url + ",".join(chunk)
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []
                    for pair in pairs:
                        # Dump the first pair we see for diagnostics
                        if not dumped and pair:
                            logger.info("DIAGNOSTIC RAW PAIR DATA: %s", json.dumps(pair, indent=2))
                            dumped = True

                        info = pair.get("info") or {}
                        socials = info.get("socials", [])
                        tg = find_telegram_in_socials(socials)

                        # Fallbacks: check info.chat, info.telegram, pair.url
                        if not tg:
                            chat = info.get("chat")
                            if chat and isinstance(chat, str):
                                if chat.startswith("http") or "t.me" in chat:
                                    tg = chat if chat.startswith("http") else f"https://{chat}"
                        if not tg:
                            telegram_field = info.get("telegram")
                            if telegram_field and isinstance(telegram_field, str):
                                tg = telegram_field if telegram_field.startswith("http") else f"https://{telegram_field}"
                        if not tg:
                            pair_url = pair.get("url")
                            if pair_url and isinstance(pair_url, str) and ("t.me" in pair_url or "telegram" in pair_url.lower()):
                                tg = pair_url if pair_url.startswith("http") else f"https://{pair_url}"

                        if not tg:
                            continue

                        base = pair.get("baseToken", {})
                        name = base.get("name", "?")
                        chain = pair.get("chainId", "?")
                        liquidity = pair.get("liquidity", {}).get("usd", 0)

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
                            "addr": base.get("address", ""),
                            "liquidity": liquidity,
                        })
                else:
                    logger.warning("Pair chunk status %s for %s", resp.status, url[:80])
        except Exception as e:
            logger.error("Pair batch error for %s: %s", url[:80], e)
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
        latest_addrs = await fetch_token_addresses(
            session, "https://api.dexscreener.com/token-profiles/latest/v1", "Latest"
        )
        boosted_addrs = await fetch_token_addresses(
            session, "https://api.dexscreener.com/token-boosts/latest/v1", "Boosted"
        )
        all_addresses = latest_addrs | boosted_addrs
        logger.info("Total unique addresses: %d", len(all_addresses))

        if not all_addresses:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(chat_id=chat_id, text="ℹ️ No token addresses.")
            return

        leads = await fetch_pair_data(session, all_addresses)
        logger.info("Leads with Telegram from pairs: %d", len(leads))

        if not leads:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(chat_id=chat_id, text="↪️ No new Telegram leads found.")
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
                await context.bot.send_message(chat_id=chat_id, text="↪️ No new Telegram leads found.")

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
