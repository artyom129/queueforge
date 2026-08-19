from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.constants import JobStatus, TERMINAL_JOB_STATUSES
from app.core.exceptions import InvalidStateTransitionError, JobNotFoundError
from app.core.security import require_api_key
from app.database.repositories.jobs import job_repository
from app.database.session import get_db_session
from app.schemas.jobs import (
    JobAccepted,
    JobCancelResponse,
    JobCreate,
    JobDetail,
    JobRead,
    JobRequeueResponse,
)
from app.services.jobs import JobService

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _not_found(exc: JobNotFoundError) -> HTTPException:
    return HTTPException(status_code=404, detail=str(exc))


@router.post(
    "",
    response_model=JobAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_key)],
)
async def create_job(
    payload: JobCreate,
    session: AsyncSession = Depends(get_db_session),
) -> JobAccepted:
    service = JobService(session)
    job, created = await service.create(payload)
    return JobAccepted(
        id=job.id,
        status=job.status,
        created_at=job.created_at,
        queue_name=job.queue_name,
        deduplicated=not created,
    )


@router.get("", response_model=list[JobRead])
async def list_jobs(
    job_status: JobStatus | None = Query(default=None, alias="status"),
    queue_name: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_db_session),
) -> list[JobRead]:
    jobs = await job_repository.list(
        session,
        status=job_status,
        queue_name=queue_name,
        limit=limit,
        offset=offset,
    )
    return [JobRead.model_validate(job) for job in jobs]


@router.get("/{job_id}", response_model=JobDetail)
async def get_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobDetail:
    service = JobService(session)
    try:
        job, attempts = await service.get_detail(job_id)
    except JobNotFoundError as exc:
        raise _not_found(exc) from exc
    return JobDetail(
        **JobRead.model_validate(job).model_dump(),
        attempts=attempts,
    )


@router.post(
    "/{job_id}/cancel",
    response_model=JobCancelResponse,
    dependencies=[Depends(require_api_key)],
)
async def cancel_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobCancelResponse:
    service = JobService(session)
    try:
        job = await service.cancel(job_id)
    except JobNotFoundError as exc:
        raise _not_found(exc) from exc
    return JobCancelResponse(
        id=job.id,
        status=job.status,
        cancellation_requested=job.cancellation_requested,
    )


@router.post(
    "/{job_id}/requeue",
    response_model=JobRequeueResponse,
    dependencies=[Depends(require_api_key)],
)
async def requeue_dead_letter(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
) -> JobRequeueResponse:
    service = JobService(session)
    try:
        job = await service.requeue_dead_letter(job_id)
    except JobNotFoundError as exc:
        raise _not_found(exc) from exc
    except InvalidStateTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return JobRequeueResponse(id=job.id, status=job.status, queue_name=job.queue_name)


@router.get("/{job_id}/events")
async def job_events(
    job_id: uuid.UUID,
    request: Request,
) -> StreamingResponse:
    settings = get_settings()

    async def stream():
        last_version: tuple[str, str] | None = None
        from app.database.session import SessionFactory

        while not await request.is_disconnected():
            async with SessionFactory() as session:
                job = await job_repository.get(session, job_id)
                if job is None:
                    yield "event: error\ndata: {\"detail\": \"job not found\"}\n\n"
                    return
                version = (job.status.value, job.updated_at.isoformat())
                if version != last_version:
                    data = json.dumps(
                        {
                            "id": str(job.id),
                            "status": job.status.value,
                            "retry_count": job.retry_count,
                            "worker_id": job.worker_id,
                            "result": job.result,
                            "error": job.error,
                            "updated_at": job.updated_at.isoformat(),
                        },
                        default=str,
                    )
                    yield f"event: job\ndata: {data}\n\n"
                    last_version = version
                if job.status in TERMINAL_JOB_STATUSES:
                    return
            await asyncio.sleep(settings.sse_poll_interval_seconds)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
