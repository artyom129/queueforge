import uuid

import pytest

from app.core.exceptions import RetryableTaskError, UnknownTaskError
from app.tasks import get_task
from app.tasks.base import TaskContext


@pytest.mark.asyncio
async def test_echo_task() -> None:
    context = TaskContext(job_id=uuid.uuid4(), attempt=1, worker_id="test")
    result = await get_task("echo")({"message": "hello"}, context)
    assert result == {"message": "hello"}


@pytest.mark.asyncio
async def test_unstable_task_is_deterministic() -> None:
    job_id = uuid.uuid4()
    handler = get_task("unstable_task")
    with pytest.raises(RetryableTaskError):
        await handler(
            {"fail_until_attempt": 2},
            TaskContext(job_id=job_id, attempt=1, worker_id="test"),
        )
    with pytest.raises(RetryableTaskError):
        await handler(
            {"fail_until_attempt": 2},
            TaskContext(job_id=job_id, attempt=2, worker_id="test"),
        )
    result = await handler(
        {"fail_until_attempt": 2},
        TaskContext(job_id=job_id, attempt=3, worker_id="test"),
    )
    assert result == {"succeeded_on_attempt": 3}


def test_unknown_task() -> None:
    with pytest.raises(UnknownTaskError):
        get_task("does-not-exist")
