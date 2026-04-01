"""
Spawner Price Logger — Phase 2
Watches configured channels, logs every message + embed to Postgres,
and extracts skeleton spawner prices into spawner_prices table.
"""

import os
import re
import json
import asyncio
import datetime
import logging
import discord
import asyncpg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("spawner_logger")

TOKEN        = os.environ["USER_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]

with open("config.json") as f:
    _cfg = json.load(f)

CHANNEL_IDS:     set[int]        = {int(ch["id"]) for ch in _cfg["channels"]}
CHANNEL_LABELS:  dict[int, str]  = {int(ch["id"]): ch["label"] for ch in _cfg["channels"]}
HISTORY_CHANNELS: set[int]       = {int(ch["id"]) for ch in _cfg["channels"] if ch.get("check_history")}

# ── Skeleton spawner price extraction ────────────────────────────────────────
# Matches patterns like:
#   "Skeleton Spawner - $12,500"
#   "skeleton spawner x1 | 8500"
#   "1x Skeleton Spawner 12500"
#   "[Skeleton Spawner] Price: 15,000"
#   "Selling skeleton spawner for 9k"

_SKELETON_RE = re.compile(
    r"""
    (?:^|\b)                          # word boundary or line start
    (?:\d+\s*[xX×]\s*)?               # optional quantity: "3x", "1 x"
    skeleton\s+spawner                # the thing we care about
    (?:\s*[xX×]\s*\d+)?               # or quantity after
    [^\d$]*?                          # any filler (type, dash, pipe, colon...)
    [\$]?                             # optional dollar sign
    ([\d,]+(?:\.\d+)?)                # PRICE — digits with optional commas/decimal
    \s*(?:k\b)?                       # optional "k" multiplier
    """,
    re.IGNORECASE | re.VERBOSE,
)

_K_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*k\b", re.IGNORECASE)


def _extract_skeleton_price(text: str) -> int | None:
    """Return the first skeleton spawner price found in text, or None."""
    if not text:
        return None
    m = _SKELETON_RE.search(text)
    if not m:
        return None
    raw = m.group(1).replace(",", "")
    # check if the original match had a trailing "k"
    full_match = m.group(0)
    try:
        value = float(raw)
        if re.search(r"\d\s*k\b", full_match, re.IGNORECASE):
            value *= 1000
        return int(value)
    except ValueError:
        return None


def _extract_skeleton_price_from_entry(entry: dict) -> int | None:
    """Check content + all embed fields for a skeleton spawner price."""
    price = _extract_skeleton_price(entry.get("content") or "")
    if price:
        return price
    for embed in entry.get("embeds") or []:
        for field in ["title", "description", "footer", "author"]:
            price = _extract_skeleton_price(embed.get(field) or "")
            if price:
                return price
        for ef in embed.get("fields") or []:
            price = _extract_skeleton_price(ef.get("value") or "")
            if price:
                return price
            price = _extract_skeleton_price(ef.get("name") or "")
            if price:
                return price
    return None


# ── DB ────────────────────────────────────────────────────────────────────────

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        await _init_tables(_pool)
    return _pool


async def _init_tables(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_price_log (
                id          BIGSERIAL PRIMARY KEY,
                source      TEXT NOT NULL,
                logged_at   TIMESTAMPTZ DEFAULT NOW(),
                guild       TEXT,
                guild_id    BIGINT,
                channel     TEXT,
                channel_id  BIGINT NOT NULL,
                author      TEXT,
                author_id   BIGINT,
                is_bot      BOOLEAN,
                content     TEXT,
                embeds      JSONB
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS spawner_prices (
                id          BIGSERIAL PRIMARY KEY,
                logged_at   TIMESTAMPTZ DEFAULT NOW(),
                channel_id  BIGINT NOT NULL,
                server_label TEXT,
                price       BIGINT NOT NULL,
                raw_log_id  BIGINT REFERENCES raw_price_log(id)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spawner_prices_logged_at
            ON spawner_prices (logged_at DESC)
        """)
    log.info("DB tables ready.")


async def _insert_entry(entry: dict) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row_id = await conn.fetchval("""
            INSERT INTO raw_price_log
                (source, guild, guild_id, channel, channel_id,
                 author, author_id, is_bot, content, embeds)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING id
        """,
            entry["source"],
            entry["guild"],
            entry["guild_id"],
            entry["channel"],
            entry["channel_id"],
            entry["author"],
            entry["author_id"],
            entry["is_bot"],
            entry["content"],
            json.dumps(entry["embeds"]),
        )
        return row_id


async def _insert_price(channel_id: int, label: str, price: int, log_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO spawner_prices (channel_id, server_label, price, raw_log_id)
            VALUES ($1, $2, $3, $4)
        """, channel_id, label, price, log_id)


# ── Discord client ────────────────────────────────────────────────────────────

def _serialize_embed(embed: discord.Embed) -> dict:
    return {
        "title":       embed.title,
        "description": embed.description,
        "color":       embed.color.value if embed.color else None,
        "fields":      [{"name": f.name, "value": f.value, "inline": f.inline} for f in embed.fields],
        "footer":      embed.footer.text if embed.footer else None,
        "author":      embed.author.name if embed.author else None,
        "image":       embed.image.url if embed.image else None,
        "thumbnail":   embed.thumbnail.url if embed.thumbnail else None,
    }


def _build_entry(message: discord.Message, source: str = "live") -> dict:
    return {
        "source":     source,
        "guild":      str(message.guild) if message.guild else "DM",
        "guild_id":   message.guild.id if message.guild else None,
        "channel":    str(message.channel),
        "channel_id": message.channel.id,
        "author":     str(message.author),
        "author_id":  message.author.id,
        "is_bot":     message.author.bot,
        "content":    message.content,
        "embeds":     [_serialize_embed(e) for e in message.embeds],
    }


async def _process(entry: dict) -> None:
    label  = CHANNEL_LABELS.get(entry["channel_id"], str(entry["channel_id"]))
    kind   = "EMBED" if entry["embeds"] else "MSG"
    preview = (entry["content"] or "(no content)")[:80]

    log_id = await _insert_entry(entry)

    price = _extract_skeleton_price_from_entry(entry)
    if price:
        await _insert_price(entry["channel_id"], label, price, log_id)
        log.info("[%s] %s — SKELETON PRICE FOUND: $%s | %s", label, kind, f"{price:,}", preview)
    else:
        log.info("[%s] %s: %s", label, kind, preview)


client = discord.Client()


@client.event
async def on_ready():
    log.info("Logged in as %s", client.user)
    log.info("Watching %d channel(s):", len(CHANNEL_IDS))
    for cid, label in CHANNEL_LABELS.items():
        flag = " [history]" if cid in HISTORY_CHANNELS else ""
        log.info("  %s — %s%s", cid, label, flag)

    # ensure DB is ready before processing history
    await get_pool()

    for cid in HISTORY_CHANNELS:
        channel = client.get_channel(cid)
        if channel is None:
            log.warning("Could not find history channel %s", cid)
            continue
        log.info("Fetching last 2 messages from %s...", CHANNEL_LABELS.get(cid, cid))
        async for message in channel.history(limit=2):
            await _process(_build_entry(message, source="history"))


@client.event
async def on_message(message: discord.Message):
    if message.channel.id not in CHANNEL_IDS:
        return
    await _process(_build_entry(message, source="live"))


client.run(TOKEN)
