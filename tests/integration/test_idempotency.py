from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select

from app.database.models.job import Job
from app.database.models.outbox import OutboxEvent
from app.schemas.jobs import JobCreate
from app.services.jobs import JobService

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_concurrent_duplicate_submission_creates_one_job_and_one_outbox(
    integration_session_factory,
) -> None:
    payload = JobCreate(
        type="generate_report",
        payload={"customer_id": 421},
        idempotency_key="report-customer-421",
    )

    async def submit():
        async with integration_session_factory() as session:
            job, created = await JobService(session).create(payload)
            return job.id, created

    results = await asyncio.gather(*[submit() for _ in range(8)])
    ids = {job_id for job_id, _created in results}
    created_count = sum(1 for _job_id, created in results if created)

    assert len(ids) == 1
    assert created_count == 1

    async with integration_session_factory() as session:
        jobs = await session.scalar(select(func.count()).select_from(Job))
        outbox = await session.scalar(select(func.count()).select_from(OutboxEvent))
    assert jobs == 1
    assert outbox == 1
