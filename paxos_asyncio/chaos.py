
import asyncio
import random

from . import settings

async def fuzz() -> None:
    t = random.random() * settings.FUZZ_MAX_TIME
    if random.random() < settings.PROB_FREEZE:
        t *= settings.FREEZE_SCALE
    await asyncio.sleep(t)

def chaos_monkey_should_die():
    return random.random() < settings.PROB_KILL

def chaos_monkey_should_lose_message():
    return random.random() < settings.PROB_MESSAGE_LOSS
