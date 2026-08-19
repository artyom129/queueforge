from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.repositories.jobs import job_repository
from app.database.session import get_db_session
from app.schemas.jobs import JobRead

router = APIRouter(prefix="/dlq", tags=["dlq"])


@router.get("", response_model=list[JobRead])
async def list_dead_letter_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_db_session),
) -> list[JobRead]:
    jobs = await job_repository.dlq(session, limit=limit)
    return [JobRead.model_validate(job) for job in jobs]
