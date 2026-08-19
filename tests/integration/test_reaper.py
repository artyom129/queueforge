from __future__ import annotations

import pytest

from app.core.constants import JobStatus
from app.core.exceptions import LostJobLease
from app.database.repositories.jobs import job_repository
from app.queue.outbox import OutboxPublisher
from app.schemas.jobs import JobCreate
from app.services.jobs import JobService
from app.workers.reaper import StaleWorkerReaper
from app.workers.worker import WorkerRuntime

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_stale_worker_is_retried_and_old_token_is_fenced(
    integration_session_factory, monkeypatch
) -> None:
    async with integration_session_factory() as session:
        job, _ = await JobService(session).create(
            JobCreate(type="echo", payload={"message": "lease"}, max_retries=1)
        )

    publisher = OutboxPublisher()
    await publisher.publish_batch(limit=10)
    worker = WorkerRuntime()
    item = await worker.consumer.get()
    assert item is not None
    claimed = await worker.claim(item[0])
    assert claimed is not None
    claimed_job, _attempt = claimed
    old_token = claimed_job.execution_token
    assert old_token is not None

    reaper = StaleWorkerReaper(worker.redis)
    monkeypatch.setattr(reaper.settings, "worker_lost_after_seconds", 0.0)
    monkeypatch.setattr(reaper.settings, "retry_base_delay_seconds", 0.0)
    monkeypatch.setattr(reaper.settings, "retry_max_delay_seconds", 0.0)
    monkeypatch.setattr(reaper.settings, "retry_jitter_ratio", 0.0)

    assert await reaper.recover_stale(limit=10) == 1

    async with integration_session_factory() as session:
        recovered = await job_repository.get(session, job.id)
        attempts = await job_repository.attempts(session, job.id)
    assert recovered is not None
    assert recovered.status == JobStatus.RETRYING
    assert recovered.retry_count == 1
    assert recovered.execution_token is None
    assert attempts[0].outcome == "worker_lost"

    with pytest.raises(LostJobLease):
        await worker.state.finish_success(
            job.id,
            1,
            old_token,
            {"message": "too late"},
            1,
        )
