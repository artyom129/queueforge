from __future__ import annotations

import pytest
from sqlalchemy import select

from app.database.models.outbox import OutboxEvent
from app.queue.outbox import OutboxPublisher
from app.queue.producer import queue_key
from app.schemas.jobs import JobCreate
from app.services.jobs import JobService

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_outbox_publication_is_idempotent(
    integration_session_factory, integration_redis, monkeypatch
) -> None:
    import app.queue.outbox as outbox_module

    monkeypatch.setattr(outbox_module, "SessionFactory", integration_session_factory)
    async with integration_session_factory() as session:
        job, created = await JobService(session).create(
            JobCreate(type="echo", payload={"message": "hi"})
        )
    assert created is True

    publisher = OutboxPublisher()
    publisher.redis = integration_redis
    from app.queue.producer import RedisProducer
    from app.scheduler.scheduler import RedisScheduler

    publisher.producer = RedisProducer(integration_redis)
    publisher.scheduler = RedisScheduler(integration_redis)

    assert await publisher.publish_batch(limit=10) == 1
    assert await publisher.publish_batch(limit=10) == 0
    assert await integration_redis.zcard(queue_key("default")) == 1

    async with integration_session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
        assert event is not None
        assert event.published_at is not None
