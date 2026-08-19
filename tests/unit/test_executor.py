import asyncio
import uuid

import pytest

from app.core.exceptions import JobCancelled, JobExecutionTimeout
from app.tasks.base import TaskContext
from app.workers.executor import execute_task


@pytest.mark.asyncio
async def test_executor_timeout() -> None:
    async def handler(payload: dict, context: TaskContext) -> dict:
        await asyncio.sleep(1)
        return {}

    async def not_cancelled() -> bool:
        return False

    with pytest.raises(JobExecutionTimeout):
        await execute_task(
            handler,
            {},
            context=TaskContext(uuid.uuid4(), 1, "worker"),
            timeout_seconds=0.01,
            is_cancelled=not_cancelled,
            cancellation_poll_seconds=0.001,
        )


@pytest.mark.asyncio
async def test_executor_cancellation() -> None:
    async def handler(payload: dict, context: TaskContext) -> dict:
        await asyncio.sleep(1)
        return {}

    async def cancelled() -> bool:
        return True

    with pytest.raises(JobCancelled):
        await execute_task(
            handler,
            {},
            context=TaskContext(uuid.uuid4(), 1, "worker"),
            timeout_seconds=1,
            is_cancelled=cancelled,
            cancellation_poll_seconds=0.001,
        )


@pytest.mark.asyncio
async def test_cancellation_probe_error_is_not_treated_as_user_cancellation() -> None:
    async def handler(payload: dict, context: TaskContext) -> dict:
        await asyncio.sleep(1)
        return {}

    async def broken_probe() -> bool:
        raise RuntimeError("redis/db unavailable")

    with pytest.raises(RuntimeError, match="redis/db unavailable"):
        await execute_task(
            handler,
            {},
            context=TaskContext(uuid.uuid4(), 1, "worker"),
            timeout_seconds=1,
            is_cancelled=broken_probe,
            cancellation_poll_seconds=0.001,
        )
