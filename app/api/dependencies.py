from __future__ import annotations

from collections.abc import AsyncIterator

from redis.asyncio import Redis

from app.queue.redis import get_redis


async def get_redis_client() -> AsyncIterator[Redis]:
    # Shared process-level pool. It is closed by the FastAPI lifespan.
    yield get_redis()
