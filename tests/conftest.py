from __future__ import annotations

import os

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.getenv(
    "QUEUEFORGE_DATABASE_URL",
    "postgresql+asyncpg://queueforge:queueforge@localhost:5432/queueforge_test",
)
TEST_REDIS_URL = os.getenv("QUEUEFORGE_REDIS_URL", "redis://localhost:6379/15")


@pytest_asyncio.fixture
async def integration_session_factory():
    engine = create_async_engine(TEST_DATABASE_URL, pool_pre_ping=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE job_attempts, outbox_events, workers, jobs "
                "RESTART IDENTITY CASCADE"
            )
        )
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def integration_redis():
    from redis.asyncio import Redis

    redis = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    await redis.flushdb()
    try:
        yield redis
    finally:
        await redis.flushdb()
        await redis.aclose()


@pytest_asyncio.fixture(autouse=True)
async def reset_process_redis_for_integration(request):
    if request.node.get_closest_marker("integration") is None:
        yield
        return

    from app.queue.redis import get_redis

    get_redis.cache_clear()
    yield
    if get_redis.cache_info().currsize:
        client = get_redis()
        await client.aclose()
    get_redis.cache_clear()
