from __future__ import annotations

import os
import socket
import uuid

from app.core.constants import WorkerStatus
from app.database.repositories.workers import worker_repository
from app.database.session import SessionFactory


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


async def register_worker(worker_id: str, queues: list[str]) -> None:
    async with SessionFactory() as session:
        async with session.begin():
            await worker_repository.upsert(
                session,
                worker_id=worker_id,
                hostname=socket.gethostname(),
                pid=os.getpid(),
                queues=queues,
            )


async def mark_worker_status(worker_id: str, status: WorkerStatus) -> None:
    async with SessionFactory() as session:
        async with session.begin():
            await worker_repository.set_status(session, worker_id, status)
