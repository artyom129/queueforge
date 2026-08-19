from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime

from redis.asyncio import Redis

from app.core.config import get_settings
from app.core.constants import REDIS_WORKER_PREFIX
from app.core.metrics import WORKER_HEARTBEATS
from app.database.repositories.workers import worker_repository
from app.database.session import SessionFactory

logger = logging.getLogger(__name__)


class HeartbeatLoop:
    def __init__(self, redis: Redis, worker_id: str, queues: list[str]) -> None:
        self.redis = redis
        self.worker_id = worker_id
        self.queues = queues
        self.settings = get_settings()

    async def beat(self) -> None:
        now = datetime.now(UTC)
        await self.redis.set(
            f"{REDIS_WORKER_PREFIX}:{self.worker_id}",
            json.dumps(
                {
                    "worker_id": self.worker_id,
                    "queues": self.queues,
                    "last_heartbeat": now.isoformat(),
                }
            ),
            ex=self.settings.worker_heartbeat_ttl_seconds,
        )
        async with SessionFactory() as session:
            async with session.begin():
                await worker_repository.heartbeat(session, self.worker_id)
        WORKER_HEARTBEATS.inc()

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.beat()
            except Exception:
                logger.exception("worker_heartbeat_failed", extra={"worker_id": self.worker_id})
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.settings.worker_heartbeat_interval_seconds,
                )
            except TimeoutError:
                pass
