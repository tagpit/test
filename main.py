import asyncio
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import os

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.raw import functions

# ========== ENV ==========
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
bot_token = os.getenv("BOT_TOKEN")

target_chat_id = -1002918606681

channels_file = "canals.csv"
list1_file = "list1.csv"
list2_file = "list2.csv"

history_dir = Path("history")
history_dir.mkdir(exist_ok=True)

processed_file = Path("processed_ids.txt")
processed_file.touch(exist_ok=True)

POLL_INTERVAL = 30
POLL_LIMIT = 30
MAX_MSG_AGE = timedelta(hours=24)

# ========== LOG ==========
def gui_print(s: str):
    print(s, flush=True)

# ========== UTILS ==========
def load_lines_csv(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        return []
    return [line.strip() for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

def normalize_text(s: str) -> str:
    return unicodedata.normalize("NFKC", s).lower()

def clean_nonprintable(s: str) -> str:
    return re.sub(r"[\x00-\x1F\x7F-\x9F]", "", s)

def extract_text(msg: Message) -> Optional[str]:
    return msg.text or msg.caption

def msg_key(chat_id: int, msg_id: int) -> str:
    return f"{chat_id}:{msg_id}"

def post_link(username: Optional[str], chat_id: int, msg_id: int) -> str:
    if username:
        return f"https://t.me/{username}/{msg_id}"
    cid = str(chat_id)
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{msg_id}"
    return f"ID:{chat_id}/{msg_id}"

def is_recent(msg: Message) -> bool:
    msg_dt = msg.date.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - msg_dt) <= MAX_MSG_AGE

# ========== CONFIG ==========
channels = load_lines_csv(channels_file)
list1 = [s.lower() for s in load_lines_csv(list1_file)]
list2 = [s.lower() for s in load_lines_csv(list2_file)]

processed = set(processed_file.read_text(encoding="utf-8").splitlines())

# ========== CLIENTS ==========
user = Client("user_session", api_id=api_id, api_hash=api_hash)
bot = Client("bot_session", api_id=api_id, api_hash=api_hash, bot_token=bot_token)

# ========== HISTORY ==========
def log_history(message: Message, text: str):
    msg_date = message.date.astimezone()
    fn = history_dir / f"{msg_date.strftime('%Y-%m-%d')}.txt"

    with fn.open("a", encoding="utf-8") as f:
        f.write(
            f"{msg_date}\n"
            f"{message.chat.title or 'Без названия'}\n"
            f"{post_link(message.chat.username, message.chat.id, message.id)}\n"
            f"{clean_nonprintable(text)}\n\n"
        )

# ========== FILTER ==========
def check_keywords(text: str) -> bool:
    t = normalize_text(text)
    return any(w in t for w in list1) and any(w in t for w in list2)

def mark_processed(key: str):
    processed.add(key)
    with processed_file.open("a", encoding="utf-8") as f:
        f.write(key + "\n")

# ========== HANDLER ==========
async def handle_message(message: Message):
    key = msg_key(message.chat.id, message.id)

    if key in processed or not is_recent(message):
        mark_processed(key)
        return

    text = extract_text(message)
    if not text:
        mark_processed(key)
        return

    if check_keywords(text):
        log_history(message, text)

        msg_date = message.date.astimezone().strftime("%Y-%m-%d %H:%M:%S")

        out_text = (
            f"{message.chat.title or 'Без названия'}\n"
            f"{post_link(message.chat.username, message.chat.id, message.id)}\n\n"
            f"{text}"
        )

        await bot.send_message(target_chat_id, out_text, disable_web_page_preview=True)
        gui_print(f"[SEND] {msg_date} | {message.chat.title}")

    mark_processed(key)

# ========== LIVE ==========
@user.on_message(filters.chat(chats=channels))
async def live_handler(_, message: Message):
    try:
        await handle_message(message)
        await user.read_history(message.chat.id)
    except Exception as e:
        print(f"[LIVE ERROR] {e}")

# ========== POLL ==========
async def poll_loop():
    while True:
        for ch in channels:
            try:
                async for msg in user.get_chat_history(ch, limit=POLL_LIMIT):
                    await handle_message(msg)

                peer = await user.resolve_peer(ch)
                await user.invoke(functions.messages.ReadHistory(peer=peer))

                await asyncio.sleep(0.4)

            except FloodWait as fw:
                await asyncio.sleep(fw.seconds + 1)
            except Exception as e:
                print(f"[POLL ERROR] {ch}: {e}")

        await asyncio.sleep(POLL_INTERVAL)

# ========== RUN ==========
async def main_async():
    async with bot, user:
        asyncio.create_task(poll_loop())
        print("Bot started")

        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main_async())
