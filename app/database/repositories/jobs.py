from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.core.state_machine import validate_job_transition
from app.database.models.job import Job
from app.database.models.job_attempt import JobAttempt
from app.database.models.outbox import OutboxEvent


class JobRepository:
    async def get(self, session: AsyncSession, job_id: uuid.UUID) -> Job | None:
        return await session.get(Job, job_id)

    async def get_for_update(
        self, session: AsyncSession, job_id: uuid.UUID
    ) -> Job | None:
        statement = select(Job).where(Job.id == job_id).with_for_update()
        return await session.scalar(statement)

    async def get_by_idempotency_key(
        self, session: AsyncSession, idempotency_key: str
    ) -> Job | None:
        statement = select(Job).where(Job.idempotency_key == idempotency_key)
        return await session.scalar(statement)

    async def list(
        self,
        session: AsyncSession,
        *,
        status: JobStatus | None = None,
        queue_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Job]:
        statement: Select[tuple[Job]] = select(Job).order_by(Job.created_at.desc())
        if status is not None:
            statement = statement.where(Job.status == status)
        if queue_name is not None:
            statement = statement.where(Job.queue_name == queue_name)
        statement = statement.limit(limit).offset(offset)
        return list((await session.scalars(statement)).all())

    async def transition(
        self,
        session: AsyncSession,
        job: Job,
        target: JobStatus,
        **changes: Any,
    ) -> Job:
        validate_job_transition(job.status, target)
        job.status = target
        for key, value in changes.items():
            setattr(job, key, value)
        job.updated_at = datetime.now(UTC)
        await session.flush()
        return job

    async def claim(
        self,
        session: AsyncSession,
        job_id: uuid.UUID,
        worker_id: str,
    ) -> Job | None:
        job = await self.get_for_update(session, job_id)
        if job is None:
            return None
        if job.cancellation_requested:
            if job.status in {
                JobStatus.QUEUED,
                JobStatus.SCHEDULED,
                JobStatus.RETRYING,
            }:
                await self.transition(
                    session,
                    job,
                    JobStatus.CANCELLED,
                    completed_at=datetime.now(UTC),
                    error="Cancellation requested before execution",
                )
            return None
        if job.status not in {
            JobStatus.QUEUED,
            JobStatus.SCHEDULED,
            JobStatus.RETRYING,
        }:
            return None
        now = datetime.now(UTC)
        if job.scheduled_at is not None and job.scheduled_at > now:
            return None
        return await self.transition(
            session,
            job,
            JobStatus.RUNNING,
            worker_id=worker_id,
            execution_token=uuid.uuid4(),
            started_at=job.started_at or now,
            error=None,
        )

    async def create_attempt(
        self,
        session: AsyncSession,
        *,
        job_id: uuid.UUID,
        attempt_number: int,
        worker_id: str,
    ) -> JobAttempt:
        attempt = JobAttempt(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=worker_id,
        )
        session.add(attempt)
        await session.flush()
        return attempt

    async def finish_attempt(
        self,
        session: AsyncSession,
        attempt: JobAttempt,
        *,
        outcome: str,
        duration_ms: int,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        attempt.finished_at = datetime.now(UTC)
        attempt.outcome = outcome
        attempt.duration_ms = duration_ms
        attempt.result = result
        attempt.error = error
        await session.flush()

    async def attempts(
        self, session: AsyncSession, job_id: uuid.UUID
    ) -> list[JobAttempt]:
        statement = (
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number)
        )
        return list((await session.scalars(statement)).all())

    async def dlq(self, session: AsyncSession, limit: int = 100) -> list[Job]:
        statement = (
            select(Job)
            .where(Job.status == JobStatus.DEAD_LETTER)
            .order_by(Job.updated_at.desc())
            .limit(limit)
        )
        return list((await session.scalars(statement)).all())

    async def pending_outbox_count(self, session: AsyncSession) -> int:
        statement = select(func.count()).select_from(OutboxEvent).where(
            OutboxEvent.published_at.is_(None)
        )
        return int((await session.scalar(statement)) or 0)


job_repository = JobRepository()
