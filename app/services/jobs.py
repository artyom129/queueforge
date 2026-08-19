from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.core.exceptions import InvalidStateTransitionError, JobNotFoundError
from app.core.metrics import JOBS_CREATED
from app.database.models.job import Job
from app.database.models.outbox import OutboxEvent
from app.database.repositories.jobs import job_repository
from app.queue.redis import get_redis
from app.core.constants import REDIS_CANCEL_PREFIX
from app.schemas.jobs import JobCreate

logger = logging.getLogger(__name__)


class JobService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, data: JobCreate) -> tuple[Job, bool]:
        now = datetime.now(UTC)
        scheduled = data.scheduled_at is not None and data.scheduled_at > now
        job = Job(
            type=data.type,
            payload=data.payload,
            status=JobStatus.SCHEDULED if scheduled else JobStatus.QUEUED,
            priority=data.priority,
            scheduled_at=data.scheduled_at,
            max_retries=data.max_retries,
            timeout_seconds=data.timeout_seconds,
            callback_url=str(data.callback_url) if data.callback_url else None,
            idempotency_key=data.idempotency_key,
            queue_name=data.queue_name,
        )

        try:
            async with self.session.begin():
                self.session.add(job)
                await self.session.flush()  # unique idempotency constraint arbitrates races
                event_type = "job.schedule" if scheduled else "job.enqueue"
                payload = {
                    "job_id": str(job.id),
                    "queue_name": job.queue_name,
                    "priority": job.priority,
                }
                if scheduled:
                    payload["scheduled_at"] = data.scheduled_at.isoformat()
                self.session.add(
                    OutboxEvent(
                        event_type=event_type,
                        aggregate_id=str(job.id),
                        dedupe_key=f"create:{job.id}",
                        payload=payload,
                    )
                )
        except IntegrityError:
            # The INSERT may race with another request. PostgreSQL's unique
            # constraint, not a racy preflight `if exists`, decides the winner.
            await self.session.rollback()
            if data.idempotency_key is None:
                raise
            existing = await job_repository.get_by_idempotency_key(
                self.session, data.idempotency_key
            )
            if existing is None:
                raise
            return existing, False

        JOBS_CREATED.labels(queue=job.queue_name, type=job.type).inc()
        return job, True

    async def get(self, job_id: uuid.UUID) -> Job:
        job = await job_repository.get(self.session, job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        return job

    async def get_detail(self, job_id: uuid.UUID) -> tuple[Job, list]:
        job = await self.get(job_id)
        attempts = await job_repository.attempts(self.session, job_id)
        return job, attempts

    async def cancel(self, job_id: uuid.UUID) -> Job:
        redis = get_redis()
        async with self.session.begin():
            job = await job_repository.get_for_update(self.session, job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status in {
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
                JobStatus.DEAD_LETTER,
            }:
                return job

            job.cancellation_requested = True
            if job.status in {
                JobStatus.QUEUED,
                JobStatus.SCHEDULED,
                JobStatus.RETRYING,
            }:
                await job_repository.transition(
                    self.session,
                    job,
                    JobStatus.CANCELLED,
                    completed_at=datetime.now(UTC),
                    error="Cancelled by API request",
                )
            await self.session.flush()

        # Redis is only a fast cancellation signal for running workers. The DB
        # flag remains authoritative if this write fails.
        try:
            await redis.set(
                f"{REDIS_CANCEL_PREFIX}:{job_id}",
                "1",
                ex=max(job.timeout_seconds + 60, 120),
            )
        except Exception:
            # Cancellation is already durable in PostgreSQL. Redis only reduces
            # latency for a currently running worker, so a transient Redis
            # outage must not turn a successful cancellation into an API 500.
            logger.exception("cancellation_signal_failed", extra={"job_id": str(job_id)})
        return job

    async def requeue_dead_letter(self, job_id: uuid.UUID) -> Job:
        async with self.session.begin():
            job = await job_repository.get_for_update(self.session, job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status != JobStatus.DEAD_LETTER:
                raise InvalidStateTransitionError(job.status, JobStatus.QUEUED)

            # Explicit operator action starts a fresh retry budget while keeping
            # historical JobAttempt rows for auditability.
            job.status = JobStatus.QUEUED
            job.retry_count = 0
            job.error = None
            job.completed_at = None
            job.worker_id = None
            job.execution_token = None
            job.cancellation_requested = False
            job.updated_at = datetime.now(UTC)
            self.session.add(
                OutboxEvent(
                    event_type="job.enqueue",
                    aggregate_id=str(job.id),
                    dedupe_key=f"dlq-requeue:{job.id}:{job.updated_at.timestamp()}",
                    payload={
                        "job_id": str(job.id),
                        "queue_name": job.queue_name,
                        "priority": job.priority,
                    },
                )
            )
            await self.session.flush()
        return job
