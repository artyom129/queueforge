from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy import select

from app.callbacks.dispatcher import CallbackDispatcher
from app.core.config import get_settings
from app.core.constants import JobStatus, REDIS_WORKER_PREFIX
from app.database.models.job import Job
from app.database.models.job_attempt import JobAttempt
from app.database.models.outbox import OutboxEvent
from app.database.models.worker import Worker
from app.database.repositories.jobs import job_repository
from app.database.session import SessionFactory
from app.queue.dlq import DeadLetterQueue
from app.services.retry import exponential_backoff

logger = logging.getLogger(__name__)


class StaleWorkerReaper:
    """Recover RUNNING jobs whose worker lease is no longer alive.

    The recovery deliberately provides at-least-once rather than exactly-once
    semantics. A task may have produced an external side effect just before its
    process died, so task handlers should use job_id as a downstream idempotency
    key where possible. execution_token fences a late/stale worker from
    overwriting the state produced by a newer attempt.
    """

    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.settings = get_settings()
        self.dlq = DeadLetterQueue(redis)
        self.callbacks = CallbackDispatcher()

    async def _candidate_ids(self, *, limit: int) -> list:
        cutoff = datetime.now(UTC) - timedelta(
            seconds=self.settings.worker_lost_after_seconds
        )
        async with SessionFactory() as session:
            statement = (
                select(Job.id)
                .where(
                    Job.status == JobStatus.RUNNING,
                    Job.started_at.is_not(None),
                    Job.started_at <= cutoff,
                )
                .order_by(Job.started_at)
                .limit(limit)
            )
            return list((await session.scalars(statement)).all())

    async def _worker_is_stale(self, worker_id: str | None) -> bool:
        if worker_id is None:
            return True
        if await self.redis.exists(f"{REDIS_WORKER_PREFIX}:{worker_id}"):
            return False

        cutoff = datetime.now(UTC) - timedelta(
            seconds=self.settings.worker_lost_after_seconds
        )
        async with SessionFactory() as session:
            statement = select(Worker).where(Worker.worker_id == worker_id)
            worker = await session.scalar(statement)
            return worker is None or worker.last_heartbeat <= cutoff

    async def recover_stale(self, *, limit: int = 100) -> int:
        recovered = 0
        for job_id in await self._candidate_ids(limit=limit):
            async with SessionFactory() as probe:
                current = await job_repository.get(probe, job_id)
            if current is None or current.status != JobStatus.RUNNING:
                continue
            if not await self._worker_is_stale(current.worker_id):
                continue

            terminal_job: Job | None = None
            async with SessionFactory() as session:
                async with session.begin():
                    job = await job_repository.get_for_update(session, job_id)
                    if job is None or job.status != JobStatus.RUNNING:
                        continue
                    if not await self._worker_is_stale(job.worker_id):
                        continue

                    attempt_number = job.retry_count + 1
                    attempt_statement = (
                        select(JobAttempt)
                        .where(
                            JobAttempt.job_id == job.id,
                            JobAttempt.attempt_number == attempt_number,
                        )
                        .with_for_update()
                    )
                    attempt = await session.scalar(attempt_statement)
                    error = "Worker heartbeat expired before attempt completion"
                    if attempt is not None and attempt.finished_at is None:
                        elapsed = datetime.now(UTC) - attempt.started_at
                        await job_repository.finish_attempt(
                            session,
                            attempt,
                            outcome="worker_lost",
                            duration_ms=max(0, int(elapsed.total_seconds() * 1000)),
                            error=error,
                        )

                    if job.retry_count >= job.max_retries:
                        terminal_job = await job_repository.transition(
                            session,
                            job,
                            JobStatus.DEAD_LETTER,
                            completed_at=datetime.now(UTC),
                            error=error,
                            worker_id=None,
                            execution_token=None,
                        )
                    else:
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
                                dedupe_key=f"worker-loss:{job.id}:{next_retry_count}",
                                payload={
                                    "job_id": str(job.id),
                                    "queue_name": job.queue_name,
                                    "priority": job.priority,
                                    "scheduled_at": retry_at.isoformat(),
                                },
                            )
                        )
                    recovered += 1

            if terminal_job is not None:
                await self.dlq.record(terminal_job.id, error=terminal_job.error or "worker lost")
                await self.callbacks.dispatch(terminal_job)
            logger.warning("stale_job_recovered", extra={"job_id": str(job_id)})
        return recovered
