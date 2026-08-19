from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import or_, select

from app.core.constants import JobStatus
from app.database.models.job import Job
from app.database.session import SessionFactory
from app.queue.producer import RedisProducer


class QueueReconciler:
    """Safety net for the destructive Redis pop -> DB claim crash window.

    Redis remains the transport, but PostgreSQL is authoritative. If a worker
    dies after popping a ready member and before claiming its DB row, the job
    remains QUEUED (or due SCHEDULED/RETRYING). Reconciliation re-publishes the
    same UUID with ZADD NX, so the job cannot remain lost indefinitely.
    """

    def __init__(self, producer: RedisProducer) -> None:
        self.producer = producer

    async def reconcile(self, *, limit: int = 500) -> int:
        now = datetime.now(UTC)
        async with SessionFactory() as session:
            statement = (
                select(Job)
                .where(
                    Job.cancellation_requested.is_(False),
                    or_(
                        Job.status == JobStatus.QUEUED,
                        (
                            Job.status.in_({JobStatus.SCHEDULED, JobStatus.RETRYING})
                            & (Job.scheduled_at.is_not(None))
                            & (Job.scheduled_at <= now)
                        ),
                    ),
                )
                .order_by(Job.priority.desc(), Job.created_at)
                .limit(limit)
            )
            jobs = list((await session.scalars(statement)).all())

        for job in jobs:
            await self.producer.enqueue(
                job.id,
                queue_name=job.queue_name,
                priority=job.priority,
            )
        return len(jobs)
