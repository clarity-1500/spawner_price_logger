"""
Spawner Price Logger — Phase 1
Watches configured channels and logs every message + embed to price_log.jsonl.
Add/remove channels in config.json — no code changes needed.
"""

import os
import json
import datetime
import discord
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ["USER_TOKEN"]

with open("config.json") as f:
    _cfg = json.load(f)

CHANNEL_IDS: set[int] = {int(ch["id"]) for ch in _cfg["channels"]}
CHANNEL_LABELS: dict[int, str] = {int(ch["id"]): ch["label"] for ch in _cfg["channels"]}
HISTORY_CHANNELS: set[int] = {int(ch["id"]) for ch in _cfg["channels"] if ch.get("check_history")}

LOG_FILE = "price_log.jsonl"


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


def _log(entry: dict) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    label = CHANNEL_LABELS.get(entry["channel_id"], str(entry["channel_id"]))
    kind  = "EMBED" if entry["embeds"] else "MSG"
    preview = (entry["content"] or "(no content)")[:100]
    print(f"[{entry['timestamp']}] {label} — {kind}: {preview}")


def _build_entry(message: discord.Message, source: str = "live") -> dict:
    return {
        "source":     source,
        "timestamp":  datetime.datetime.utcnow().isoformat(),
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


client = discord.Client()


@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    print(f"Watching {len(CHANNEL_IDS)} channel(s):")
    for cid, label in CHANNEL_LABELS.items():
        flag = " [history]" if cid in HISTORY_CHANNELS else ""
        print(f"  {cid} — {label}{flag}")
    print(f"Logging to {LOG_FILE}\n")

    # For channels that don't delete messages, fetch the last 2 on startup
    for cid in HISTORY_CHANNELS:
        channel = client.get_channel(cid)
        if channel is None:
            print(f"WARNING: could not find history channel {cid} — not in guild?")
            continue
        print(f"Fetching last 2 messages from {CHANNEL_LABELS.get(cid, cid)}...")
        async for message in channel.history(limit=2):
            entry = _build_entry(message, source="history")
            _log(entry)


@client.event
async def on_message(message: discord.Message):
    if message.channel.id not in CHANNEL_IDS:
        return
    _log(_build_entry(message, source="live"))


client.run(TOKEN)
