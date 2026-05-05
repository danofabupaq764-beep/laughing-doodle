#!/usr/bin/env python3
"""
Dexscreener Telegram Lead Bot – DM Only
Final version – finds Telegram links in profiles AND pair socials.
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

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Database helpers (unchanged)
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
# Dexscreener API – generalised Telegram detection
# ----------------------------------------------------------------------
def find_telegram(links: list) -> str | None:
    """
    Search a list of link objects for a Telegram URL.
    Checks 'type', 'label', and the URL itself.
    Returns a clean https://t.me/... URL, or None.
    """
    for link in links:
        if not isinstance(link, dict):
            continue
        url = link.get("url", "")
        if not url:
            continue

        # Check type & label
        link_type = link.get("type", "").lower()
        link_label = link.get("label", "").lower()
        if "telegram" in link_type or "tg" in link_type or \
           "telegram" in link_label or "tg" in link_label:
            return url if url.startswith("http") else f"https://{url}"

        # Check the URL itself for t.me
        if "t.me" in url:
            # Could be an invite or channel
            if not url.startswith("http"):
                url = f"https://{url}"
            return url
    return None

async def fetch_pair_details(session: aiohttp.ClientSession, chain_id: str, pair_address: str) -> dict | None:
    """Get a single pair's data, including socials."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_id}/{pair_address}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = data.get("pair")
                return pairs  # could be None if not found
    except Exception as e:
        logger.error("Pair fetch error: %s", e)
    return None

async def fetch_liquidity_and_socials(
    session: aiohttp.ClientSession, addresses: list[dict]
) -> list[dict]:
    """
    For each candidate (with chain:address), fetch pairs and extract
    total liquidity AND a Telegram link if not already found.
    Returns a list of enriched candidate dicts.
    """
    # First, batch-fetch liquidity for all tokens
    base_url = "https://api.dexscreener.com/latest/dex/tokens/"
    liq_map = {}
    chunk_size = 30
    chain_addr_list = [f"{c['chainId']}:{c['tokenAddress']}" for c in addresses]
    for i in range(0, len(chain_addr_list), chunk_size):
        chunk = chain_addr_list[i:i + chunk_size]
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
                        liq_map[addr] = liq_map.get(addr, 0) + liq
        except Exception as e:
            logger.error("Liquidity batch error: %s", e)

    # Now enrich each candidate with liquidity and possibly Telegram from pairs
    enriched = []
    for cand in addresses:
        total_liq = liq_map.get(cand["tokenAddress"], 0.0)
        tg = cand.get("tg")
        if not tg:
            # Try to get Telegram from the first pair's socials
            # Use the token's pair address if available, else fetch from chain+address pairs list
            pair_addr = cand.get("pairAddress")
            if pair_addr:
                pair_data = await fetch_pair_details(session, cand["chainId"], pair_addr)
                if pair_data:
                    socials = pair_data.get("info", {}).get("socials", [])
                    tg = find_telegram(socials)
            if not tg:
                # fallback: fetch pairs list for the token and check each
                token_url = f"https://api.dexscreener.com/latest/dex/tokens/{cand['chainId']}:{cand['tokenAddress']}"
                try:
                    async with session.get(token_url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for pair in data.get("pairs", []):
                                socials = pair.get("info", {}).get("socials", [])
                                tg = find_telegram(socials)
                                if tg:
                                    break
                except Exception:
                    pass

        enriched.append({
            "name": cand.get("name", "?"),
            "chain": cand["chainId"],
            "tg": tg,
            "tw": cand.get("tw"),
            "web": cand.get("web"),
            "addr": cand["tokenAddress"],
            "liquidity": total_liq,
        })
    return enriched

async def fetch_profiles(session: aiohttp.ClientSession, url: str, label: str) -> list[dict]:
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list):
                    logger.info("%s: fetched %d profiles.", label, len(data))
                    return data
                logger.warning("%s: unexpected format %s", label, type(data))
            else:
                logger.error("%s: status %s", label, resp.status)
    except Exception as e:
        logger.error("%s: request failed: %s", label, e)
    return []

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
        profiles_latest = await fetch_profiles(
            session,
            "https://api.dexscreener.com/token-profiles/latest/v1",
            "Latest profiles",
        )
        profiles_boosted = await fetch_profiles(
            session,
            "https://api.dexscreener.com/token-boosts/latest/v1",
            "Boosted tokens",
        )
        all_profiles = profiles_latest + profiles_boosted
        logger.info("Total profiles collected: %d", len(all_profiles))

        if not all_profiles:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(
                    chat_id=chat_id, text="ℹ️ No profiles from Dexscreener."
                )
            return

        # Extract basic info and Telegram if already in profile links
        raw_candidates = []
        seen_tg = set()
        for token in all_profiles:
            if not all(k in token for k in ("name", "chainId", "tokenAddress", "links")):
                continue
            tg = find_telegram(token.get("links", []))
            if tg and (tg in seen_tg or await is_duplicate(tg)):
                continue
            if tg:
                seen_tg.add(tg)
            raw_candidates.append({
                "name": token["name"],
                "chainId": token["chainId"],
                "tokenAddress": token["tokenAddress"],
                "tg": tg,
                "tw": next((l["url"] for l in token["links"] if isinstance(l, dict) and l.get("type") == "twitter"), None),
                "web": next((l["url"] for l in token["links"] if isinstance(l, dict) and ("website" in l.get("type", "").lower() or "website" in l.get("label", "").lower())), None),
                "pairAddress": token.get("pairAddress"),  # may exist in profiles
            })

        # Deduplicate by tokenAddress
        unique = {}
        for c in raw_candidates:
            addr = c["tokenAddress"]
            if addr not in unique:
                unique[addr] = c
            else:
                # keep the one with Telegram if possible
                if c["tg"] and not unique[addr]["tg"]:
                    unique[addr] = c

        candidates_list = list(unique.values())
        logger.info("Unique tokens before liquidity/social enrichment: %d", len(candidates_list))

        # Enrich with liquidity and possibly Telegram from pair socials
        enriched = await fetch_liquidity_and_socials(session, candidates_list)

        # Filter by user min_liquidity and send
        sent_any = False
        for user_id, chat_id, min_liq in active_users:
            user_sent = 0
            for cand in enriched:
                if not cand["tg"]:
                    continue
                if cand["liquidity"] < min_liq:
                    continue
                msg = (
                    f"🚀 Project: {cand['name']}\n"
                    f"🔗 Telegram: {cand['tg']}\n"
                    f"🐦 Twitter: {cand['tw'] or 'NIL'}\n"
                    f"🌐 Website: {cand['web'] or 'NIL'}\n"
                    f"💧 Liquidity: ${cand['liquidity']:,.2f}\n"
                    f"⛓ Chain: {cand['chain']}"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg)
                    user_sent += 1
                    await mark_as_sent(cand["tg"])
                except Exception as e:
                    logger.error("Send failed user %s: %s", user_id, e)
                    if "Forbidden" in str(e):
                        await set_active(user_id, False)
            if user_sent > 0:
                sent_any = True
                logger.info("Sent %d lead(s) to user %s", user_sent, user_id)
            else:
                logger.info("No leads above liquidity for user %s", user_id)

        if not sent_any:
            for _, chat_id, _ in active_users:
                await context.bot.send_message(
                    chat_id=chat_id, text="↪️ No new Telegram leads found this cycle."
                )

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
