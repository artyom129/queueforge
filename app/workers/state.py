from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from app.core.config import Settings
from app.core.constants import JobStatus
from app.core.exceptions import LostJobLease
from app.database.models.job import Job
from app.database.models.job_attempt import JobAttempt
from app.database.models.outbox import OutboxEvent
from app.database.repositories.jobs import job_repository
from app.database.session import SessionFactory
from app.services.retry import exponential_backoff


class ExecutionStateManager:
    """Durable job-attempt state transitions for one worker identity."""

    def __init__(self, worker_id: str, settings: Settings) -> None:
        self.worker_id = worker_id
        self.settings = settings

    async def claim(self, job_id) -> tuple[Job, JobAttempt] | None:
        async with SessionFactory() as session:
            async with session.begin():
                job = await job_repository.claim(session, job_id, self.worker_id)
                if job is None:
                    return None
                attempt = await job_repository.create_attempt(
                    session,
                    job_id=job.id,
                    attempt_number=job.retry_count + 1,
                    worker_id=self.worker_id,
                )
                await session.flush()
                return job, attempt

    async def _load_attempt_for_update(
        self, session, job_id, attempt_number: int
    ) -> JobAttempt:
        statement = (
            select(JobAttempt)
            .where(
                JobAttempt.job_id == job_id,
                JobAttempt.attempt_number == attempt_number,
            )
            .with_for_update()
        )
        attempt = await session.scalar(statement)
        if attempt is None:
            raise RuntimeError("JobAttempt disappeared during execution")
        return attempt

    def _assert_lease(self, job: Job, execution_token) -> None:
        if (
            job.status != JobStatus.RUNNING
            or job.worker_id != self.worker_id
            or job.execution_token != execution_token
        ):
            raise LostJobLease(
                f"Worker {self.worker_id} no longer owns execution lease for job {job.id}"
            )

    async def finish_success(
        self,
        job_id,
        attempt_number: int,
        execution_token,
        result: dict,
        duration_ms: int,
    ) -> Job:
        async with SessionFactory() as session:
            async with session.begin():
                job = await job_repository.get_for_update(session, job_id)
                if job is None:
                    raise RuntimeError("Job disappeared during execution")
                self._assert_lease(job, execution_token)
                attempt = await self._load_attempt_for_update(session, job_id, attempt_number)
                await job_repository.finish_attempt(
                    session,
                    attempt,
                    outcome="completed",
                    duration_ms=duration_ms,
                    result=result,
                )
                if job.cancellation_requested:
                    await job_repository.transition(
                        session,
                        job,
                        JobStatus.CANCELLED,
                        completed_at=datetime.now(UTC),
                        result=None,
                        error="Cancellation won the completion race",
                        execution_token=None,
                    )
                else:
                    await job_repository.transition(
                        session,
                        job,
                        JobStatus.COMPLETED,
                        completed_at=datetime.now(UTC),
                        result=result,
                        error=None,
                        execution_token=None,
                    )
                return job

    async def finish_cancelled(
        self,
        job_id,
        attempt_number: int,
        execution_token,
        error: str,
        duration_ms: int,
    ) -> Job:
        async with SessionFactory() as session:
            async with session.begin():
                job = await job_repository.get_for_update(session, job_id)
                if job is None:
                    raise RuntimeError("Job disappeared during execution")
                self._assert_lease(job, execution_token)
                attempt = await self._load_attempt_for_update(session, job_id, attempt_number)
                await job_repository.finish_attempt(
                    session,
                    attempt,
                    outcome="cancelled",
                    duration_ms=duration_ms,
                    error=error,
                )
                await job_repository.transition(
                    session,
                    job,
                    JobStatus.CANCELLED,
                    completed_at=datetime.now(UTC),
                    error=error,
                    execution_token=None,
                )
                return job

    async def finish_failure(
        self,
        job_id,
        attempt_number: int,
        execution_token,
        error: str,
        duration_ms: int,
        *,
        retryable: bool,
    ) -> tuple[Job, str]:
        async with SessionFactory() as session:
            async with session.begin():
                job = await job_repository.get_for_update(session, job_id)
                if job is None:
                    raise RuntimeError("Job disappeared during execution")
                self._assert_lease(job, execution_token)
                attempt = await self._load_attempt_for_update(session, job_id, attempt_number)
                await job_repository.finish_attempt(
                    session,
                    attempt,
                    outcome="failed",
                    duration_ms=duration_ms,
                    error=error,
                )

                if not retryable:
                    await job_repository.transition(
                        session,
                        job,
                        JobStatus.FAILED,
                        completed_at=datetime.now(UTC),
                        error=error,
                        execution_token=None,
                    )
                    return job, "failed"

                if job.retry_count >= job.max_retries:
                    await job_repository.transition(
                        session,
                        job,
                        JobStatus.DEAD_LETTER,
                        completed_at=datetime.now(UTC),
                        error=error,
                        execution_token=None,
                    )
                    return job, "dead_letter"

                next_retry_count = job.retry_count + 1
                delay = exponential_backoff(
                    job.retry_count,
                    base_delay=self.settings.retry_base_delay_seconds,
                    max_delay=self.settings.retry_max_delay_seconds,
                    jitter_ratio=self.settings.retry_jitter_ratio,
                )
                retry_at = datetime.now(UTC) + timedelta(seconds=delay)
                await job_repository.transition(
                    session,
                    job,
                    JobStatus.RETRYING,
                    retry_count=next_retry_count,
                    scheduled_at=retry_at,
                    error=error,
                    worker_id=None,
                    execution_token=None,
                )
                session.add(
                    OutboxEvent(
                        event_type="job.schedule",
                        aggregate_id=str(job.id),
                        dedupe_key=f"retry:{job.id}:{next_retry_count}",
                        payload={
                            "job_id": str(job.id),
                            "queue_name": job.queue_name,
                            "priority": job.priority,
                            "scheduled_at": retry_at.isoformat(),
                        },
                    )
                )
                return job, "retrying"
