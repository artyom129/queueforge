from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.core.config import get_settings
from app.core.metrics import CALLBACK_DELIVERIES
from app.database.models.job import Job

logger = logging.getLogger(__name__)


class CallbackDispatcher:
    def __init__(self) -> None:
        self.settings = get_settings()

    async def dispatch(self, job: Job) -> bool:
        if not job.callback_url:
            return True

        payload: dict[str, Any] = {
            "event": "job.completed" if job.status.value == "completed" else "job.terminal",
            "job": {
                "id": str(job.id),
                "type": job.type,
                "status": job.status.value,
                "result": job.result,
                "error": job.error,
                "retry_count": job.retry_count,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            },
        }
        timeout = httpx.Timeout(
            connect=self.settings.callback_connect_timeout_seconds,
            read=self.settings.callback_read_timeout_seconds,
            write=self.settings.callback_read_timeout_seconds,
            pool=self.settings.callback_connect_timeout_seconds,
        )
        async with httpx.AsyncClient(timeout=timeout) as client:
            for attempt in range(1, self.settings.callback_max_attempts + 1):
                try:
                    response = await client.post(
                        job.callback_url,
                        json=payload,
                        headers={
                            "User-Agent": "QueueForge/1.0",
                            "X-QueueForge-Job-ID": str(job.id),
                            "X-QueueForge-Delivery-Attempt": str(attempt),
                        },
                    )
                    response.raise_for_status()
                except (httpx.HTTPError, httpx.TimeoutException) as exc:
                    logger.warning(
                        "callback_delivery_failed",
                        extra={
                            "job_id": str(job.id),
                            "attempt": attempt,
                            "error": str(exc),
                        },
                    )
                    if attempt < self.settings.callback_max_attempts:
                        await asyncio.sleep(
                            self.settings.callback_base_delay_seconds * (2 ** (attempt - 1))
                        )
                    continue
                CALLBACK_DELIVERIES.labels(outcome="success").inc()
                return True
        CALLBACK_DELIVERIES.labels(outcome="failed").inc()
        return False
