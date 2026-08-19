from __future__ import annotations

import asyncio
import logging
import signal
import time
import uuid
from datetime import datetime

from redis.asyncio import Redis

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.constants import REDIS_SCHEDULE_KEY, REDIS_SCHEDULE_META_PREFIX
from app.queue.producer import RedisProducer, priority_score, queue_key
from app.queue.redis import get_redis
from app.scheduler.reconciler import QueueReconciler
from app.workers.reaper import StaleWorkerReaper

logger = logging.getLogger(__name__)

_MOVE_DUE_SCRIPT = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 then
  redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
  redis.call('DEL', KEYS[3])
  return 1
end
return 0
"""


def schedule_meta_key(job_id: uuid.UUID | str) -> str:
    return f"{REDIS_SCHEDULE_META_PREFIX}:{job_id}"


class RedisScheduler:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def schedule(
        self,
        job_id: uuid.UUID | str,
        *,
        due_at: datetime,
        queue_name: str,
        priority: int,
    ) -> None:
        member = str(job_id)
        due_score = due_at.timestamp()
        pipe = self.redis.pipeline(transaction=True)
        pipe.hset(
            schedule_meta_key(member),
            mapping={"queue_name": queue_name, "priority": str(priority)},
        )
        pipe.zadd(REDIS_SCHEDULE_KEY, {member: due_score})
        await pipe.execute()

    async def move_due(self, *, limit: int = 100) -> int:
        due_ids = await self.redis.zrangebyscore(
            REDIS_SCHEDULE_KEY,
            min="-inf",
            max=time.time(),
            start=0,
            num=limit,
        )
        moved = 0
        for raw_job_id in due_ids:
            meta_key = schedule_meta_key(raw_job_id)
            meta = await self.redis.hgetall(meta_key)
            if not meta:
                await self.redis.zrem(REDIS_SCHEDULE_KEY, raw_job_id)
                continue
            target_queue = meta["queue_name"]
            score = priority_score(int(meta["priority"]))
            result = await self.redis.eval(
                _MOVE_DUE_SCRIPT,
                3,
                REDIS_SCHEDULE_KEY,
                queue_key(target_queue),
                meta_key,
                raw_job_id,
                score,
            )
            moved += int(result or 0)
        return moved


async def run_scheduler() -> None:
    settings = get_settings()
    redis = get_redis()
    scheduler = RedisScheduler(redis)
    reconciler = QueueReconciler(RedisProducer(redis))
    reaper = StaleWorkerReaper(redis)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows event loop
            pass

    logger.info("scheduler_started")
    last_reconciliation = 0.0
    last_reap = 0.0
    try:
        while not stop.is_set():
            moved = await scheduler.move_due(limit=settings.scheduler_batch_size)
            if moved:
                logger.info("scheduled_jobs_released", extra={"count": moved})
            now = time.monotonic()
            if now - last_reconciliation >= settings.reconciliation_interval_seconds:
                reconciled = await reconciler.reconcile(limit=settings.reconciliation_batch_size)
                if reconciled:
                    logger.info("queue_reconciled", extra={"count": reconciled})
                last_reconciliation = now
            if now - last_reap >= settings.reaper_interval_seconds:
                recovered = await reaper.recover_stale(limit=settings.reconciliation_batch_size)
                if recovered:
                    logger.warning("stale_jobs_recovered", extra={"count": recovered})
                last_reap = now
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.scheduler_poll_interval_seconds
                )
            except TimeoutError:
                pass
    finally:
        await redis.aclose()
        logger.info("scheduler_stopped")


def main() -> None:
    configure_logging()
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
