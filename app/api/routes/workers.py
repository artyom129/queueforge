from __future__ import annotations

from fastapi import APIRouter, Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_redis_client
from app.database.session import get_db_session
from app.schemas.workers import WorkerRead
from app.services.worker_service import WorkerService

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=list[WorkerRead])
async def list_workers(
    session: AsyncSession = Depends(get_db_session),
    redis: Redis = Depends(get_redis_client),
) -> list[WorkerRead]:
    return await WorkerService(session, redis).list()
