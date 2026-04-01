"""
Spawner Price Logger — Phase 3
Watches configured channels, logs every message + embed to Postgres,
extracts skeleton spawner buy/sell prices, and updates on message edits.

Prices are stored from a MEMBER'S perspective:
  buy_price  — what a member pays to BUY FROM the server  (e.g. "WE SELL FOR 5.3M")
  sell_price — what a member gets when SELLING TO the server (e.g. "WE PAY 4.4M")
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
# Dual-direction extraction (member perspective):
#
#   sell_price (member sells TO server, server pays member):
#     "WE PAY 4.4M PER SKELETON SPAWNER"
#     "WE PAY **4.6M** PER SKELETON SPAWNER"
#
#   buy_price (member buys FROM server, server sells to member):
#     "WE SELL SKELETON SPAWNERS TO YOU FOR 5.3M PER SPAWNER"
#     "WE SELL SKELETON SPAWNERS FOR 5.3M"
#
#   Generic (direction unknown — stored as sell_price):
#     "Skeleton Spawner - $12,500"
#     "skeleton spawner x1 | 8.5k"
#     "selling 9k skeleton spawner"
#
# Multipliers: k (×1000), m (×1,000,000)
# Markdown bold stripped before matching.

_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")

# "WE PAY X per skeleton spawner" → sell_price
_SELL_RE = re.compile(
    r"""
    \bwe\s+pay\b
    \s+
    \$?([\d,]+(?:\.\d+)?)          # digits
    \s*([km])\b                     # multiplier
    \s*(?:per|for|each|of|/)?
    \s*\bskeleton\s+spawners?\b
    |
    \bwe\s+pay\b
    \s+
    \$?([\d,]+(?:\.\d+)?)          # digits (no multiplier)
    (?!\s*[km]\b)
    \s*(?:per|for|each|of|/)
    \s*\bskeleton\s+spawners?\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# "WE SELL SKELETON SPAWNERS (TO YOU)? FOR X" → buy_price
_BUY_RE = re.compile(
    r"""
    \bwe\s+sell\b
    .*?
    \bskeleton\s+spawners?\b
    .*?
    \bfor\b
    \s+
    \$?([\d,]+(?:\.\d+)?)          # digits
    \s*([km])\b                     # multiplier
    |
    \bwe\s+sell\b
    .*?
    \bskeleton\s+spawners?\b
    .*?
    \bfor\b
    \s+
    \$?([\d,]+(?:\.\d+)?)          # digits (no multiplier)
    (?!\s*[km]\b)
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# Generic price-AFTER: skeleton spawner [filler] PRICE[k/m]
_AFTER_RE = re.compile(
    r"""
    \bskeleton\s+spawners?\b
    [^\d$]*?
    \$?([\d,]+(?:\.\d+)?)
    \s*([km])\b
    |
    \bskeleton\s+spawners?\b
    [^\d$]*?
    \$?([\d,]+(?:\.\d+)?)
    (?!\s*[km]\b)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Generic price-BEFORE: PRICE[k/m] [connector] skeleton spawner
_BEFORE_RE = re.compile(
    r"""
    \$?([\d,]+(?:\.\d+)?)
    \s*([km])\b
    \s*(?:per|for|each|of|/)?
    \s*\bskeleton\s+spawners?\b
    |
    \$?([\d,]+(?:\.\d+)?)
    (?!\s*[km]\b)
    \s*(?:per|for|each|of|/)
    \s*\bskeleton\s+spawners?\b
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


def _extract_prices(text: str) -> dict[str, int | None]:
    """
    Returns {"buy_price": int|None, "sell_price": int|None}.
    buy_price  = member buys FROM server
    sell_price = member sells TO server
    """
    if not text:
        return {"buy_price": None, "sell_price": None}

    text = _BOLD_RE.sub(r"\1", text)

    buy_price  = None
    sell_price = None

    # Try directional patterns first
    m = _SELL_RE.search(text)
    if m:
        if m.group(1):
            sell_price = _parse_price(m.group(1), m.group(2))
        elif m.group(3):
            sell_price = _parse_price(m.group(3), None)

    m = _BUY_RE.search(text)
    if m:
        if m.group(1):
            buy_price = _parse_price(m.group(1), m.group(2))
        elif m.group(3):
            buy_price = _parse_price(m.group(3), None)

    # If neither directional pattern fired, fall back to generic → sell_price
    if buy_price is None and sell_price is None:
        m = _BEFORE_RE.search(text)
        if m:
            if m.group(1):
                sell_price = _parse_price(m.group(1), m.group(2))
            elif m.group(3):
                sell_price = _parse_price(m.group(3), None)

        if sell_price is None:
            m = _AFTER_RE.search(text)
            if m:
                if m.group(1):
                    sell_price = _parse_price(m.group(1), m.group(2))
                elif m.group(3):
                    sell_price = _parse_price(m.group(3), None)

    return {"buy_price": buy_price, "sell_price": sell_price}


def _extract_from_entry(entry: dict) -> dict[str, int | None]:
    """Scan content + all embed fields, merge buy/sell across all sources."""
    result: dict[str, int | None] = {"buy_price": None, "sell_price": None}

    def _merge(prices: dict) -> None:
        if prices["buy_price"]  and result["buy_price"]  is None:
            result["buy_price"]  = prices["buy_price"]
        if prices["sell_price"] and result["sell_price"] is None:
            result["sell_price"] = prices["sell_price"]

    _merge(_extract_prices(entry.get("content") or ""))

    for embed in entry.get("embeds") or []:
        for field in ["title", "description", "footer", "author"]:
            _merge(_extract_prices(embed.get(field) or ""))
        for ef in embed.get("fields") or []:
            for key in ("value", "name"):
                _merge(_extract_prices(ef.get(key) or ""))

    return result


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
                buy_price    BIGINT,
                sell_price   BIGINT,
                raw_log_id   BIGINT REFERENCES raw_price_log(id)
            )
        """)
        # migrate old schema: drop NOT NULL on legacy price col, add buy/sell cols
        await conn.execute("""
            ALTER TABLE spawner_prices
            ALTER COLUMN price DROP NOT NULL
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS buy_price BIGINT
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS sell_price BIGINT
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
        """)
        await conn.execute("""
            ALTER TABLE spawner_prices
            ADD COLUMN IF NOT EXISTS raw_log_id BIGINT REFERENCES raw_price_log(id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spawner_prices_logged_at
            ON spawner_prices (logged_at DESC)
        """)
        await conn.execute("DROP INDEX IF EXISTS idx_spawner_prices_raw_log_id")
        await conn.execute("""
            CREATE UNIQUE INDEX idx_spawner_prices_raw_log_id
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


async def _upsert_price(channel_id: int, label: str, buy_price: int | None,
                        sell_price: int | None, log_id: int) -> None:
    """Insert or update spawner_prices keyed by raw_log_id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO spawner_prices (channel_id, server_label, buy_price, sell_price, raw_log_id)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (raw_log_id) DO UPDATE SET
                buy_price  = EXCLUDED.buy_price,
                sell_price = EXCLUDED.sell_price,
                updated_at = NOW()
        """, channel_id, label, buy_price, sell_price, log_id)


async def _delete_price(log_id: int) -> None:
    """Remove a price entry if an edited message no longer has any skeleton price."""
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
    prices = _extract_from_entry(entry)
    buy    = prices["buy_price"]
    sell   = prices["sell_price"]

    if buy or sell:
        await _upsert_price(entry["channel_id"], label, buy, sell, log_id)
        parts = []
        if buy:
            parts.append(f"BUY ${buy:,}")
        if sell:
            parts.append(f"SELL ${sell:,}")
        log.info("[%s] %s %s — SKELETON PRICE: %s | %s",
                 label, event.upper(), kind, " | ".join(parts), preview)
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
