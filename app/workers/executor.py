from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from app.core.exceptions import JobCancelled, JobExecutionTimeout
from app.tasks.base import TaskContext, TaskHandler


async def execute_task(
    handler: TaskHandler,
    payload: dict,
    *,
    context: TaskContext,
    timeout_seconds: int,
    is_cancelled: Callable[[], Awaitable[bool]],
    cancellation_poll_seconds: float = 0.25,
) -> dict:
    async def watch_cancellation() -> None:
        while True:
            if await is_cancelled():
                return
            await asyncio.sleep(cancellation_poll_seconds)

    task = asyncio.create_task(handler(payload, context), name=f"job:{context.job_id}")
    cancel_watch = asyncio.create_task(
        watch_cancellation(), name=f"job-cancel-watch:{context.job_id}"
    )
    try:
        done, _pending = await asyncio.wait(
            {task, cancel_watch},
            timeout=timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            return await task
        if cancel_watch in done:
            watch_error = cancel_watch.exception()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if watch_error is not None:
                raise watch_error
            raise JobCancelled(f"Job {context.job_id} was cancelled")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise JobExecutionTimeout(
            f"Job {context.job_id} exceeded timeout of {timeout_seconds}s"
        )
    finally:
        cancel_watch.cancel()
        await asyncio.gather(cancel_watch, return_exceptions=True)
