from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import WorkerStatus
from app.database.models.worker import Worker


class WorkerRepository:
    async def upsert(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        hostname: str,
        pid: int,
        queues: list[str],
    ) -> Worker:
        statement = select(Worker).where(Worker.worker_id == worker_id).with_for_update()
        worker = await session.scalar(statement)
        now = datetime.now(UTC)
        if worker is None:
            worker = Worker(
                worker_id=worker_id,
                hostname=hostname,
                pid=pid,
                queues=queues,
                status=WorkerStatus.ONLINE,
                started_at=now,
                last_heartbeat=now,
            )
            session.add(worker)
        else:
            worker.hostname = hostname
            worker.pid = pid
            worker.queues = queues
            worker.status = WorkerStatus.ONLINE
            worker.last_heartbeat = now
            worker.stopped_at = None
        await session.flush()
        return worker

    async def heartbeat(self, session: AsyncSession, worker_id: str) -> None:
        statement = select(Worker).where(Worker.worker_id == worker_id).with_for_update()
        worker = await session.scalar(statement)
        if worker is not None:
            worker.last_heartbeat = datetime.now(UTC)
            worker.status = WorkerStatus.ONLINE
            await session.flush()

    async def set_status(
        self, session: AsyncSession, worker_id: str, status: WorkerStatus
    ) -> None:
        statement = select(Worker).where(Worker.worker_id == worker_id).with_for_update()
        worker = await session.scalar(statement)
        if worker is not None:
            worker.status = status
            if status == WorkerStatus.OFFLINE:
                worker.stopped_at = datetime.now(UTC)
            await session.flush()

    async def list(self, session: AsyncSession, limit: int = 100) -> list[Worker]:
        statement = select(Worker).order_by(Worker.last_heartbeat.desc()).limit(limit)
        return list((await session.scalars(statement)).all())


worker_repository = WorkerRepository()
