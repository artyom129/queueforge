from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from redis.asyncio import Redis

from app.core.constants import REDIS_DLQ_KEY


class DeadLetterQueue:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def record(self, job_id: uuid.UUID, *, error: str) -> None:
        payload = json.dumps(
            {
                "job_id": str(job_id),
                "error": error,
                "dead_lettered_at": datetime.now(UTC).isoformat(),
            }
        )
        await self.redis.lpush(REDIS_DLQ_KEY, payload)
        await self.redis.ltrim(REDIS_DLQ_KEY, 0, 9_999)
