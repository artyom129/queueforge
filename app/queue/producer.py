from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

from app.core.constants import MAX_PRIORITY, MIN_PRIORITY, REDIS_QUEUE_PREFIX

# Keep priority as the dominant ordering dimension, FIFO inside a priority band.
# Current epoch milliseconds are ~1e12; 1e14 leaves ample room while remaining
# safely below IEEE-754 exact-integer limits for the configured priority range.
_PRIORITY_BAND = 100_000_000_000_000


def queue_key(queue_name: str) -> str:
    return f"{REDIS_QUEUE_PREFIX}:{queue_name}"


def priority_score(priority: int, timestamp_ms: int | None = None) -> int:
    clamped = max(MIN_PRIORITY, min(MAX_PRIORITY, priority))
    timestamp_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
    return (MAX_PRIORITY - clamped) * _PRIORITY_BAND + timestamp_ms


class RedisProducer:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def enqueue(
        self,
        job_id: uuid.UUID | str,
        *,
        queue_name: str,
        priority: int,
    ) -> None:
        # ZADD is idempotent because job_id is the sorted-set member. This is
        # important when an outbox publisher crashes after Redis accepted the
        # write but before PostgreSQL records published_at.
        await self.redis.zadd(
            queue_key(queue_name),
            {str(job_id): priority_score(priority)},
            nx=True,
        )
