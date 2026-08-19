from __future__ import annotations

import asyncio

from app.core.exceptions import NonRetryableTaskError, RetryableTaskError
from app.tasks.base import TaskContext
from app.tasks.registry import task


@task("echo")
async def echo(payload: dict, context: TaskContext) -> dict:
    return {"message": payload.get("message")}


@task("generate_report")
async def generate_report(payload: dict, context: TaskContext) -> dict:
    customer_id = payload.get("customer_id")
    if customer_id is None:
        raise NonRetryableTaskError("customer_id is required")
    await asyncio.sleep(0.05)
    return {
        "customer_id": customer_id,
        "report": f"demo-report-{customer_id}",
        "attempt": context.attempt,
    }


@task("slow_task")
async def slow_task(payload: dict, context: TaskContext) -> dict:
    seconds = float(payload.get("seconds", 1))
    if seconds < 0 or seconds > 300:
        raise NonRetryableTaskError("seconds must be between 0 and 300")
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}


@task("unstable_task")
async def unstable_task(payload: dict, context: TaskContext) -> dict:
    fail_until_attempt = int(payload.get("fail_until_attempt", 0))
    if context.attempt <= fail_until_attempt:
        raise RetryableTaskError(
            f"Deterministic demo failure on attempt {context.attempt}; "
            f"configured to fail through attempt {fail_until_attempt}"
        )
    return {"succeeded_on_attempt": context.attempt}
