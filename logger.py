"""
Spawner Price Logger — Phase 2
Watches configured channels, logs every message + embed to Postgres,
extracts skeleton spawner prices, and updates on message edits.
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

CHANNEL_IDS:      set[int]       = {int(ch["id"]) for ch in _cfg["channels"]}
CHANNEL_LABELS:   dict[int, str] = {int(ch["id"]): ch["label"] for ch in _cfg["channels"]}
HISTORY_CHANNELS: set[int]       = {int(ch["id"]) for ch in _cfg["channels"] if ch.get("check_history")}

# ── Skeleton spawner price extraction ────────────────────────────────────────
#
# Handles both directions:
#   AFTER:  "Skeleton Spawner - $12,500"
#           "skeleton spawner x1 | 8.5k"
#   BEFORE: "WE PAY 4.4M PER SKELETON SPAWNER"
#           "## WE PAY **4.6M** PER SKELETON SPAWNER"
#           "selling 9k skeleton spawner"
#
# Multipliers: k (×1000), m (×1,000,000)
# Markdown bold stripped before matching.

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")

# Price-AFTER pattern: skeleton spawner [filler] PRICE[k/m]
_AFTER_RE = re.compile(
    r"""
    \bskeleton\s+spawner\b
    [^\d$]*?                          # filler (dash, pipe, colon, spaces…)
    \$?([\d,]+(?:\.\d+)?)             # price digits
    \s*([km])\b                       # multiplier
    |
    \bskeleton\s+spawner\b
    [^\d$]*?
    \$?([\d,]+(?:\.\d+)?)             # price with no multiplier
    (?!\s*[km]\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Price-BEFORE pattern: PRICE[k/m] [per/for/each/of] skeleton spawner
_BEFORE_RE = re.compile(
    r"""
    \$?([\d,]+(?:\.\d+)?)             # price digits
    \s*([km])\b                       # multiplier required (4.4M, 9k, etc.)
    \s*(?:per|for|each|of|/)?         # optional connector
    \s*\bskeleton\s+spawner\b
    |
    \$?([\d,]+(?:\.\d+)?)             # price without multiplier
    (?!\s*[km]\b)
    \s*(?:per|for|each|of|/)
    \s*\bskeleton\s+spawner\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _parse_price(digits: str, multiplier: str | None) -> int:
    value = float(digits.replace(",", ""))
    if multiplier:
        mul = multiplier.lower()
        if mul == "k":
            value *= 1_000
        elif mul == "m":
            value *= 1_000_000
    return int(value)


def _extract_skeleton_price(text: str) -> int | None:
    if not text:
        return None
    # strip markdown bold so **4.6M** becomes 4.6M
    text = _BOLD_RE.sub(r"\1", text)

    # try before pattern first (more specific)
    m = _BEFORE_RE.search(text)
    if m:
        if m.group(1):
            return _parse_price(m.group(1), m.group(2))
        if m.group(3):
            return _parse_price(m.group(3), None)

    m = _AFTER_RE.search(text)
    if m:
        if m.group(1):
            return _parse_price(m.group(1), m.group(2))
        if m.group(3):
            return _parse_price(m.group(3), None)

    return None


def _extract_from_entry(entry: dict) -> int | None:
    price = _extract_skeleton_price(entry.get("content") or "")
    if price:
        return price
    for embed in entry.get("embeds") or []:
        for field in ["title", "description", "footer", "author"]:
            price = _extract_skeleton_price(embed.get(field) or "")
            if price:
                return price
        for ef in embed.get("fields") or []:
            for key in ("value", "name"):
                price = _extract_skeleton_price(ef.get(key) or "")
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
                message_id  BIGINT UNIQUE,
                source      TEXT NOT NULL,
                logged_at   TIMESTAMPTZ DEFAULT NOW(),
                updated_at  TIMESTAMPTZ DEFAULT NOW(),
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
        # add message_id column if upgrading from old schema
        await conn.execute("""
            ALTER TABLE raw_price_log
            ADD COLUMN IF NOT EXISTS message_id BIGINT UNIQUE
        """)
        await conn.execute("""
            ALTER TABLE raw_price_log
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS spawner_prices (
                id           BIGSERIAL PRIMARY KEY,
                logged_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_at   TIMESTAMPTZ DEFAULT NOW(),
                channel_id   BIGINT NOT NULL,
                server_label TEXT,
                price        BIGINT NOT NULL,
                raw_log_id   BIGINT REFERENCES raw_price_log(id)
            )
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spawner_prices_logged_at
            ON spawner_prices (logged_at DESC)
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS raw_log_id BIGINT REFERENCES raw_price_log(id)
        """)
        # unique constraint so ON CONFLICT (raw_log_id) works
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_spawner_prices_raw_log_id
            ON spawner_prices (raw_log_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_raw_price_log_message_id
            ON raw_price_log (message_id)
            WHERE message_id IS NOT NULL
        """)
    log.info("DB tables ready.")


async def _upsert_entry(entry: dict) -> int:
    """Insert or update raw_price_log by message_id. Returns the row id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row_id = await conn.fetchval("""
            INSERT INTO raw_price_log
                (message_id, source, guild, guild_id, channel, channel_id,
                 author, author_id, is_bot, content, embeds)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (message_id) DO UPDATE SET
                content    = EXCLUDED.content,
                embeds     = EXCLUDED.embeds,
                updated_at = NOW()
            RETURNING id
        """,
            entry.get("message_id"),
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


async def _upsert_price(channel_id: int, label: str, price: int, log_id: int) -> None:
    """Insert or update spawner_prices keyed by raw_log_id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO spawner_prices (channel_id, server_label, price, raw_log_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (raw_log_id) DO UPDATE SET
                price      = EXCLUDED.price,
                updated_at = NOW()
        """, channel_id, label, price, log_id)


async def _delete_price(log_id: int) -> None:
    """Remove a price entry if an edited message no longer has a skeleton price."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM spawner_prices WHERE raw_log_id = $1", log_id)


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
        "message_id": message.id,
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


async def _process(entry: dict, event: str = "new") -> None:
    label   = CHANNEL_LABELS.get(entry["channel_id"], str(entry["channel_id"]))
    kind    = "EMBED" if entry["embeds"] else "MSG"
    preview = (entry["content"] or "(no content)")[:80]

    log_id = await _upsert_entry(entry)
    price  = _extract_from_entry(entry)

    if price:
        await _upsert_price(entry["channel_id"], label, price, log_id)
        log.info("[%s] %s %s — SKELETON PRICE: $%s | %s", label, event.upper(), kind, f"{price:,}", preview)
    else:
        if event == "edit":
            await _delete_price(log_id)
        log.info("[%s] %s %s: %s", label, event.upper(), kind, preview)


client = discord.Client()


@client.event
async def on_ready():
    log.info("Logged in as %s", client.user)
    log.info("Watching %d channel(s):", len(CHANNEL_IDS))
    for cid, label in CHANNEL_LABELS.items():
        flag = " [history]" if cid in HISTORY_CHANNELS else ""
        ch = client.get_channel(cid)
        guild_name = ch.guild.name if ch and ch.guild else "NOT FOUND"
        log.info("  %s — %s | guild: %s%s", cid, label, guild_name, flag)

    await get_pool()

    for cid in HISTORY_CHANNELS:
        channel = client.get_channel(cid)
        if channel is None:
            log.warning("Could not find history channel %s", cid)
            continue
        log.info("Fetching last 2 messages from %s...", CHANNEL_LABELS.get(cid, cid))
        async for message in channel.history(limit=2):
            await _process(_build_entry(message, source="history"), event="history")


@client.event
async def on_message(message: discord.Message):
    if message.channel.id not in CHANNEL_IDS:
        return
    await _process(_build_entry(message, source="live"), event="new")


@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.channel.id not in CHANNEL_IDS:
        return
    await _process(_build_entry(after, source="edit"), event="edit")


client.run(TOKEN)
