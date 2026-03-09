import asyncio
import logging
import traceback

from .rewrite_queue import process_rewrite_queue

logger = logging.getLogger("research-custom-polling")


async def start_rewrite_loop(interval: int = 2) -> None:
    logger.info("deep-research-custom rewrite polling 시작")
    while True:
        try:
            await process_rewrite_queue()
        except Exception as e:
            logger.error("rewrite polling 실패: %s", e)
            logger.error(traceback.format_exc())
        await asyncio.sleep(interval)
