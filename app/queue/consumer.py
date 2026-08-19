from __future__ import annotations

import uuid

from redis.asyncio import Redis

from app.queue.producer import queue_key


class RedisConsumer:
    def __init__(self, redis: Redis, queues: list[str], *, timeout_seconds: int = 5) -> None:
        self.redis = redis
        self.queues = queues
        self.timeout_seconds = timeout_seconds

    async def get(self) -> tuple[uuid.UUID, str] | None:
        # BZPOPMIN blocks in Redis, so idle workers do not spin the CPU.
        result = await self.redis.bzpopmin(
            [queue_key(queue) for queue in self.queues],
            timeout=self.timeout_seconds,
        )
        if result is None:
            return None
        key, member, _score = result
        queue_name = str(key).rsplit(":", 1)[-1]
        return uuid.UUID(str(member)), queue_name
