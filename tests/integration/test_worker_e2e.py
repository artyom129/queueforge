from __future__ import annotations

import asyncio

import pytest

from app.core.constants import JobStatus
from app.database.repositories.jobs import job_repository
from app.queue.outbox import OutboxPublisher
from app.scheduler.scheduler import RedisScheduler
from app.schemas.jobs import JobCreate
from app.services.jobs import JobService
from app.workers.worker import WorkerRuntime

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_echo_job_end_to_end(integration_session_factory) -> None:
    async with integration_session_factory() as session:
        job, created = await JobService(session).create(
            JobCreate(type="echo", payload={"message": "hello"})
        )
    assert created is True

    publisher = OutboxPublisher()
    assert await publisher.publish_batch(limit=10) == 1

    worker = WorkerRuntime()
    item = await worker.consumer.get()
    assert item is not None
    job_id, queue_name = item
    assert job_id == job.id
    await worker.process_one(job_id, queue_name)

    async with integration_session_factory() as session:
        stored = await job_repository.get(session, job.id)
        attempts = await job_repository.attempts(session, job.id)
    assert stored is not None
    assert stored.status == JobStatus.COMPLETED
    assert stored.result == {"message": "hello"}
    assert len(attempts) == 1
    assert attempts[0].outcome == "completed"


@pytest.mark.asyncio
async def test_retry_is_scheduled_then_succeeds(
    integration_session_factory, monkeypatch
) -> None:
    async with integration_session_factory() as session:
        job, _created = await JobService(session).create(
            JobCreate(
                type="unstable_task",
                payload={"fail_until_attempt": 1},
                max_retries=2,
            )
        )

    publisher = OutboxPublisher()
    await publisher.publish_batch(limit=10)
    worker = WorkerRuntime()
    monkeypatch.setattr(worker.settings, "retry_base_delay_seconds", 0.01)
    monkeypatch.setattr(worker.settings, "retry_max_delay_seconds", 0.01)
    monkeypatch.setattr(worker.settings, "retry_jitter_ratio", 0.0)

    first = await worker.consumer.get()
    assert first is not None
    await worker.process_one(*first)

    async with integration_session_factory() as session:
        retrying = await job_repository.get(session, job.id)
    assert retrying is not None
    assert retrying.status == JobStatus.RETRYING
    assert retrying.retry_count == 1

    assert await publisher.publish_batch(limit=10) == 1
    await asyncio.sleep(0.02)
    moved = await RedisScheduler(worker.redis).move_due(limit=10)
    assert moved == 1

    second = await worker.consumer.get()
    assert second is not None
    await worker.process_one(*second)

    async with integration_session_factory() as session:
        completed = await job_repository.get(session, job.id)
        attempts = await job_repository.attempts(session, job.id)
    assert completed is not None
    assert completed.status == JobStatus.COMPLETED
    assert completed.retry_count == 1
    assert completed.result == {"succeeded_on_attempt": 2}
    assert [attempt.outcome for attempt in attempts] == ["failed", "completed"]
