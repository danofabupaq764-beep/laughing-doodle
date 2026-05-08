#!/usr/bin/env python3
"""
Dexscreener Telegram Lead Bot – DM Only
Finds tokens launched in the last 24 hours (via Token Profiles + Pair age filter).
"""

import asyncio
import logging
import os
import sys
import time

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
DEFAULT_MIN_LIQUIDITY = float(os.getenv("DEFAULT_MIN_LIQUIDITY", "50"))
MAX_PAIR_AGE_HOURS = int(os.getenv("MAX_PAIR_AGE_HOURS", "24"))

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
                min_liquidity REAL DEFAULT 50.0
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
                active = excluded.active,
                min_liquidity = COALESCE(users.min_liquidity, excluded.min_liquidity)
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
async def fetch_addresses(session: aiohttp.ClientSession, url: str, label: str) -> set[tuple]:
    """Return set of (chainId, tokenAddress) from a token-profiles endpoint."""
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
                            addresses.add((chain, addr))
                    logger.info("%s: %d addresses", label, len(addresses))
                    return addresses
                logger.warning("%s: unexpected format %s", label, type(data))
            else:
                logger.error("%s: status %s", label, resp.status)
    except Exception as e:
        logger.error("%s: request failed: %s", label, e)
    return addresses

def find_telegram(socials: list, info: dict) -> str | None:
    """Extract Telegram link from socials array or info dict."""
    # 1. socials
    for s in (socials or []):
        if not isinstance(s, dict):
            continue
        url = s.get("url", "")
        if not url:
            continue
        t = (s.get("type", "") + s.get("label", "")).lower()
        if any(w in t for w in ("telegram", "tg")) or \
           any(d in url for d in ("t.me", "telegram.me", "telegram.org")):
            return url if url.startswith("http") else f"https://{url}"
    # 2. info dict
    for key in ("telegram", "tg"):
        val = (info or {}).get(key)
        if val and isinstance(val, str) and "t.me" in val:
            return val if val.startswith("http") else f"https://{val}"
    return None

async def fetch_pairs_batch(session: aiohttp.ClientSession, addr_set: set[tuple]) -> list[dict]:
    """Batch-fetch pair data for given (chain, tokenAddress) tuple set.
    Returns a list of lead dicts that pass the 24h age filter and have a Telegram link."""
    leads = []
    addr_list = [f"{chain}:{addr}" for chain, addr in addr_set]
    base_url = "https://api.dexscreener.com/latest/dex/tokens/"
    chunk_size = 30

    for i in range(0, len(addr_list), chunk_size):
        chunk = addr_list[i:i + chunk_size]
        url = base_url + ",".join(chunk)
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pairs = data.get("pairs") or []   # handle null
                    for pair in pairs:
                        # Age filter
                        created_at = pair.get("pairCreatedAt")
                        if not created_at:
                            continue
                        age_hours = (time.time() * 1000 - created_at) / (1000 * 3600)
                        if age_hours > MAX_PAIR_AGE_HOURS:
                            continue

                        info = pair.get("info") or {}
                        socials = info.get("socials", [])
                        tg = find_telegram(socials, info)
                        if not tg:
                            continue

                        base = pair.get("baseToken", {})
                        name = base.get("name") or base.get("symbol") or "?"
                        chain = pair.get("chainId", "?")
                        liquidity = pair.get("liquidity", {}).get("usd", 0)

                        twitter = website = None
                        for s in socials:
                            st = s.get("type", "").lower()
                            su = s.get("url", "")
                            if not su:
                                continue
                            if "twitter" in st and not twitter:
                                twitter = su
                            if "website" in st and not website:
                                website = su

                        leads.append({
                            "name": name,
                            "chain": chain,
                            "tg": tg,
                            "tw": twitter,
                            "web": website,
                            "liquidity": liquidity,
                        })
                else:
                    logger.warning("Pairs batch %s status %s", url[:80], resp.status)
        except Exception as e:
            logger.error("Pairs batch error %s: %s", url[:80], e)
    return leads

# ----------------------------------------------------------------------
# Main job
# ----------------------------------------------------------------------
async def job_fetch_and_send(context: ContextTypes.DEFAULT_TYPE):
    active_users = await get_active_users()
    if not active_users:
        return

    logger.info("Fetch started")

    async with aiohttp.ClientSession() as session:
        # 1. Gather ALL token addresses from profile endpoints
        latest = await fetch_addresses(session, "https://api.dexscreener.com/token-profiles/latest/v1", "Latest")
        boosted = await fetch_addresses(session, "https://api.dexscreener.com/token-boosts/latest/v1", "Boosted")
        all_addrs = latest | boosted
        logger.info("Total unique token addresses: %d", len(all_addrs))

        if not all_addrs:
            return

        # 2. Fetch pair data for all addresses, keep only fresh ones with Telegram
        leads = await fetch_pairs_batch(session, all_addrs)
        logger.info("Fresh leads with Telegram (≤%dh): %d", MAX_PAIR_AGE_HOURS, len(leads))
        if leads:
            sample = leads[0]
            logger.info("Sample: %s | tg=%s | liq=$%.2f", sample["name"], sample["tg"], sample["liquidity"])

        if not leads:
            return  # silent

        # 3. Send to active users (respecting min liquidity and dedup)
        sent_any = False
        for uid, cid, minliq in active_users:
            sent = 0
            for ld in leads:
                if ld["liquidity"] < minliq:
                    continue
                if await is_duplicate(ld["tg"]):
                    continue
                msg = (
                    f"🚀 Project: {ld['name']}\n"
                    f"🔗 Telegram: {ld['tg']}\n"
                    f"🐦 Twitter: {ld['tw'] or 'NIL'}\n"
                    f"🌐 Website: {ld['web'] or 'NIL'}\n"
                    f"💧 Liquidity: ${ld['liquidity']:,.2f}\n"
                    f"⛓ Chain: {ld['chain']}"
                )
                try:
                    await context.bot.send_message(chat_id=cid, text=msg)
                    sent += 1
                    await mark_as_sent(ld["tg"])
                except Exception as e:
                    logger.error("Send fail %s: %s", uid, e)
                    if "Forbidden" in str(e):
                        await set_active(uid, False)
            if sent:
                sent_any = True
                logger.info("Sent %d lead(s) to user %s", sent, uid)

        if not sent_any:
            logger.info("All leads filtered out (duplicates/liquidity).")

# ----------------------------------------------------------------------
# Command handlers
# ----------------------------------------------------------------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    c = update.effective_chat
    await upsert_user(u.id, c.id, True)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT min_liquidity FROM users WHERE user_id=?", (u.id,))
        row = await cur.fetchone()
        ml = row[0] if row else DEFAULT_MIN_LIQUIDITY
    await update.message.reply_text(
        f"✅ Bot activated!\nMinimum liquidity: ${ml:,.2f}\n"
        "Use /minliq <value> to change.\n/stop to pause."
    )

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await set_active(update.effective_user.id, False)
    await update.message.reply_text("⏸ Bot paused. /start to resume.")

async def cmd_minliq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /minliq <amount>")
        return
    try:
        v = float(context.args[0])
        if v <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Provide a positive number.")
        return
    await set_min_liquidity(update.effective_user.id, v)
    await update.message.reply_text(f"💰 Min liquidity set to ${v:,.2f}")

# ----------------------------------------------------------------------
# Application
# ----------------------------------------------------------------------
async def build_bot():
    await init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("minliq", cmd_minliq))
    app.job_queue.run_repeating(job_fetch_and_send, interval=FETCH_INTERVAL, first=10)
    logger.info("Bot ready.")
    return app

def main():
    app = asyncio.run(build_bot())
    logger.info("Bot polling started.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
