from __future__ import annotations

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.constants import REDIS_WORKER_PREFIX
from app.database.repositories.workers import worker_repository
from app.schemas.workers import WorkerRead


class WorkerService:
    def __init__(self, session: AsyncSession, redis: Redis) -> None:
        self.session = session
        self.redis = redis
        self.settings = get_settings()

    async def list(self) -> list[WorkerRead]:
        workers = await worker_repository.list(self.session)
        if not workers:
            return []
        pipe = self.redis.pipeline(transaction=False)
        for worker in workers:
            pipe.exists(f"{REDIS_WORKER_PREFIX}:{worker.worker_id}")
        alive_flags = await pipe.execute()
        return [
            WorkerRead.model_validate(worker).model_copy(update={"alive": bool(alive)})
            for worker, alive in zip(workers, alive_flags, strict=True)
        ]
