import asyncio
import json
import os

import structlog
import aiogram
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message,
    MessageReactionUpdated,
    MessageReactionCountUpdated,
    ReactionTypeEmoji,
    ReactionTypeCustomEmoji,
)
from redis.asyncio import Redis

BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LIKE_EMOJI = os.getenv("LIKE_EMOJI", "👍")

SRC1_CHAT_ID = int(os.getenv("SRC1_CHAT_ID"))
SRC1_THREAD_ID = int(os.getenv("SRC1_THREAD_ID"))
SRC2_CHAT_ID = int(os.getenv("SRC2_CHAT_ID"))
SRC2_THREAD_ID = int(os.getenv("SRC2_THREAD_ID"))
DST_CHAT_ID = int(os.getenv("DST_CHAT_ID"))
DST_THREAD_ID = int(os.getenv("DST_THREAD_ID"))

ALLOWED_UPDATES = ["message", "message_reaction", "message_reaction_count"]

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
)
log = structlog.get_logger()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

redis: Redis | None = None  

def key_map(src_chat_id: int, src_msg_id: int) -> str:
    return f"task:map:{src_chat_id}:{src_msg_id}"

def key_rev(dst_chat_id: int, dst_msg_id: int) -> str:
    return f"task:rev:{dst_chat_id}:{dst_msg_id}"

async def save_mapping(*, src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, src_thread_id, dst_thread_id):
    assert redis is not None
    data = {
        "src_chat_id": src_chat_id,
        "src_msg_id": src_msg_id,
        "src_thread_id": src_thread_id,
        "dst_chat_id": dst_chat_id,
        "dst_msg_id": dst_msg_id,
        "dst_thread_id": dst_thread_id,
    }
    await redis.set(key_map(src_chat_id, src_msg_id), json.dumps(data))
    await redis.set(key_rev(dst_chat_id, dst_msg_id), json.dumps(data))
    log.info("map_saved", **data)

async def get_by_dst(*, dst_chat_id, dst_msg_id):
    assert redis is not None
    raw = await redis.get(key_rev(dst_chat_id, dst_msg_id))
    return json.loads(raw) if raw else None

def is_source_topic(m: Message) -> bool:
    return (
        m.chat is not None
        and m.message_thread_id is not None
        and (
            (m.chat.id == SRC1_CHAT_ID and m.message_thread_id == SRC1_THREAD_ID) or
            (m.chat.id == SRC2_CHAT_ID and m.message_thread_id == SRC2_THREAD_ID)
        )
    )

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def on_message(msg: Message):
    if not msg.message_thread_id or not is_source_topic(msg):
        return

    try:
        cp = await bot.copy_message(
            chat_id=DST_CHAT_ID,
            from_chat_id=msg.chat.id,
            message_id=msg.message_id,
            message_thread_id=DST_THREAD_ID,  
        )
        log.info("copied", returned_message_id=getattr(cp, "message_id", None))

        await save_mapping(
            src_chat_id=msg.chat.id,
            src_msg_id=msg.message_id,
            dst_chat_id=DST_CHAT_ID,          
            dst_msg_id=cp.message_id,         
            src_thread_id=msg.message_thread_id,
            dst_thread_id=DST_THREAD_ID,
        )

        log.info(
            "task_mirrored",
            src_chat_id=msg.chat.id, src_msg_id=msg.message_id,
            dst_chat_id=DST_CHAT_ID, dst_msg_id=cp.message_id
        )
    except Exception as e:
        log.error("mirror_failed", error=str(e))

def list_has_like(reaction_list: list | None, like_emoji: str) -> bool:
    if not reaction_list:
        return False
    for r in reaction_list:
        if isinstance(r, ReactionTypeEmoji) and r.emoji == like_emoji:
            return True
        if isinstance(r, ReactionTypeCustomEmoji):
            continue
    return False

@router.message_reaction()
async def handle_message_reaction(ev: MessageReactionUpdated):
    if ev.chat is None or ev.message_id is None or ev.chat.id != DST_CHAT_ID:
        return

    log.info(
        "reaction_update",
        msg_id=ev.message_id,
        new=[getattr(getattr(x, "emoji", None), "encode", lambda: None)() if False else str(x) for x in (ev.new_reaction or [])],
        old=[str(x) for x in (ev.old_reaction or [])],
    )

    new_has_like = list_has_like(ev.new_reaction, LIKE_EMOJI)
    old_has_like = list_has_like(ev.old_reaction, LIKE_EMOJI)

    if new_has_like and not old_has_like:
        mapping = await get_by_dst(dst_chat_id=ev.chat.id, dst_msg_id=ev.message_id)
        if not mapping:
            log.info("reaction_no_mapping", dst_chat_id=ev.chat.id, dst_msg_id=ev.message_id)
            return

        try:
            ok = await bot.set_message_reaction(
                chat_id=mapping["src_chat_id"],
                message_id=mapping["src_msg_id"],
                reaction=[ReactionTypeEmoji(emoji=LIKE_EMOJI)],
                is_big=False,
            )
            log.info("parent_liked",
                     src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"], ok=ok)
        except Exception as e:
            log.error("parent_like_failed",
                      error=str(e), src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"])

    if old_has_like and not new_has_like:
        mapping = await get_by_dst(dst_chat_id=ev.chat.id, dst_msg_id=ev.message_id)
        if not mapping:
            return
        try:
            ok = await bot.set_message_reaction(
                chat_id=mapping["src_chat_id"],
                message_id=mapping["src_msg_id"],
                reaction=[], 
            )
            log.info("parent_like_cleared",
                     src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"], ok=ok)
        except Exception as e:
            log.error("parent_like_clear_failed",
                      error=str(e), src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"])

@router.message_reaction_count()
async def handle_message_reaction_count(ev: MessageReactionCountUpdated):
    if ev.chat is None or ev.message_id is None or ev.chat.id != DST_CHAT_ID:
        return

    def counts_have_like(counts: list | None, like_emoji: str) -> bool:
        if not counts:
            return False
        for c in counts:
            rtype = getattr(c, "type", None)
            if isinstance(rtype, ReactionTypeEmoji) and rtype.emoji == like_emoji:
                return c.total_count > 0
        return False

    want_like = counts_have_like(ev.reactions, LIKE_EMOJI)
    mapping = await get_by_dst(dst_chat_id=ev.chat.id, dst_msg_id=ev.message_id)
    if not mapping:
        return

    try:
        ok = await bot.set_message_reaction(
            chat_id=mapping["src_chat_id"],
            message_id=mapping["src_msg_id"],
            reaction=[ReactionTypeEmoji(emoji=LIKE_EMOJI)] if want_like else [],
            is_big=False,
        )
        log.info("parent_sync_count",
                 src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"], want_like=want_like, ok=ok)
    except Exception as e:
        log.error("parent_sync_count_failed",
                  error=str(e), src_chat_id=mapping["src_chat_id"], src_msg_id=mapping["src_msg_id"])

async def main():
    global redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    log.info(
        "bot_starting",
        aiogram=aiogram.__version__,
        allowed_updates=ALLOWED_UPDATES,
        dst_chat_id=DST_CHAT_ID,
        dst_thread_id=DST_THREAD_ID,
        like_emoji=LIKE_EMOJI,
    )
    try:
        await dp.start_polling(bot, allowed_updates=ALLOWED_UPDATES)
    finally:
        if redis:
            await redis.close()

if __name__ == "__main__":
    asyncio.run(main())
