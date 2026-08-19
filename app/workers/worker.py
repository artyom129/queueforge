from __future__ import annotations

import asyncio
import logging
import signal
import time

from app.callbacks.dispatcher import CallbackDispatcher
from app.core.config import get_settings
from app.core.constants import REDIS_CANCEL_PREFIX, WorkerStatus
from app.core.exceptions import (
    JobCancelled,
    JobExecutionTimeout,
    LostJobLease,
    NonRetryableTaskError,
    RetryableTaskError,
    UnknownTaskError,
)
from app.core.logging import configure_logging
from app.core.metrics import JOB_EXECUTION_SECONDS, JOB_RETRIES, JOBS_EXECUTED
from app.database.repositories.jobs import job_repository
from app.database.session import SessionFactory
from app.queue.consumer import RedisConsumer
from app.queue.dlq import DeadLetterQueue
from app.queue.producer import RedisProducer
from app.queue.redis import get_redis
from app.tasks import get_task
from app.tasks.base import TaskContext
from app.workers.executor import execute_task
from app.workers.heartbeat import HeartbeatLoop
from app.workers.registry import make_worker_id, mark_worker_status, register_worker
from app.workers.state import ExecutionStateManager

logger = logging.getLogger(__name__)


class WorkerRuntime:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.redis = get_redis()
        self.worker_id = make_worker_id()
        self.queues = self.settings.worker_queue_names
        self.consumer = RedisConsumer(
            self.redis,
            self.queues,
            timeout_seconds=self.settings.worker_poll_timeout_seconds,
        )
        self.producer = RedisProducer(self.redis)
        self.dlq = DeadLetterQueue(self.redis)
        self.callbacks = CallbackDispatcher()
        self.state = ExecutionStateManager(self.worker_id, self.settings)
        self.stop = asyncio.Event()

    def request_shutdown(self) -> None:
        if not self.stop.is_set():
            logger.info("worker_shutdown_requested", extra={"worker_id": self.worker_id})
            self.stop.set()

    async def is_cancelled(self, job_id) -> bool:
        try:
            if await self.redis.exists(f"{REDIS_CANCEL_PREFIX}:{job_id}"):
                return True
        except Exception:
            # Redis is the low-latency path; PostgreSQL remains authoritative.
            pass
        try:
            async with SessionFactory() as session:
                job = await job_repository.get(session, job_id)
                return bool(job and job.cancellation_requested)
        except Exception:
            # Do not manufacture a cancellation from an infrastructure outage.
            # The later durable state write will surface DB failures normally.
            return False

    async def claim(self, job_id):
        return await self.state.claim(job_id)

    async def _handle_failure(
        self,
        job,
        attempt_number: int,
        execution_token,
        error: str,
        duration_ms: int,
        *,
        retryable: bool,
    ) -> None:
        final_job, outcome = await self.state.finish_failure(
            job.id,
            attempt_number,
            execution_token,
            error,
            duration_ms,
            retryable=retryable,
        )
        JOBS_EXECUTED.labels(queue=job.queue_name, type=job.type, outcome=outcome).inc()
        if outcome == "retrying":
            JOB_RETRIES.labels(queue=job.queue_name, type=job.type).inc()
        elif outcome == "dead_letter":
            await self.dlq.record(job.id, error=error)
            await self.callbacks.dispatch(final_job)
        elif outcome == "failed":
            await self.callbacks.dispatch(final_job)

    async def process_one(self, job_id, popped_queue: str) -> None:
        claimed = await self.state.claim(job_id)
        if claimed is None:
            logger.info(
                "job_claim_skipped",
                extra={"job_id": str(job_id), "queue": popped_queue},
            )
            return

        job, _attempt = claimed
        attempt_number = job.retry_count + 1
        execution_token = job.execution_token
        if execution_token is None:
            raise RuntimeError("Claimed job has no execution token")

        started = time.perf_counter()
        context = TaskContext(
            job_id=job.id,
            attempt=attempt_number,
            worker_id=self.worker_id,
        )
        logger.info(
            "job_started",
            extra={
                "job_id": str(job.id),
                "task_type": job.type,
                "attempt": attempt_number,
                "worker_id": self.worker_id,
            },
        )

        try:
            handler = get_task(job.type)
            result = await execute_task(
                handler,
                job.payload,
                context=context,
                timeout_seconds=job.timeout_seconds,
                is_cancelled=lambda: self.is_cancelled(job.id),
            )
        except JobCancelled as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            final_job = await self.state.finish_cancelled(
                job.id,
                attempt_number,
                execution_token,
                str(exc),
                duration_ms,
            )
            JOBS_EXECUTED.labels(
                queue=job.queue_name, type=job.type, outcome="cancelled"
            ).inc()
            await self.callbacks.dispatch(final_job)
            return
        except (UnknownTaskError, NonRetryableTaskError) as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            await self._handle_failure(
                job,
                attempt_number,
                execution_token,
                str(exc),
                duration_ms,
                retryable=False,
            )
            return
        except (RetryableTaskError, JobExecutionTimeout) as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            await self._handle_failure(
                job,
                attempt_number,
                execution_token,
                str(exc),
                duration_ms,
                retryable=True,
            )
            return
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.exception("job_unhandled_exception", extra={"job_id": str(job.id)})
            await self._handle_failure(
                job,
                attempt_number,
                execution_token,
                f"{type(exc).__name__}: {exc}",
                duration_ms,
                retryable=True,
            )
            return

        duration = time.perf_counter() - started
        JOB_EXECUTION_SECONDS.labels(queue=job.queue_name, type=job.type).observe(duration)
        final_job = await self.state.finish_success(
            job.id,
            attempt_number,
            execution_token,
            result,
            int(duration * 1000),
        )
        JOBS_EXECUTED.labels(
            queue=job.queue_name, type=job.type, outcome=final_job.status.value
        ).inc()
        await self.callbacks.dispatch(final_job)
        logger.info(
            "job_finished",
            extra={
                "job_id": str(job.id),
                "status": final_job.status.value,
                "duration_ms": int(duration * 1000),
            },
        )

    async def _return_unstarted_item(self, job_id, queue_name: str) -> None:
        async with SessionFactory() as session:
            queued_job = await job_repository.get(session, job_id)
        await self.producer.enqueue(
            job_id,
            queue_name=queue_name,
            priority=queued_job.priority if queued_job is not None else 5,
        )

    async def run(self) -> None:
        await register_worker(self.worker_id, self.queues)
        heartbeat = HeartbeatLoop(self.redis, self.worker_id, self.queues)
        heartbeat_task = asyncio.create_task(
            heartbeat.run(self.stop), name=f"heartbeat:{self.worker_id}"
        )
        logger.info(
            "worker_started",
            extra={"worker_id": self.worker_id, "queues": self.queues},
        )
        try:
            while not self.stop.is_set():
                item = await self.consumer.get()
                if item is None:
                    continue
                job_id, queue_name = item
                if self.stop.is_set():
                    await self._return_unstarted_item(job_id, queue_name)
                    break
                try:
                    await self.process_one(job_id, queue_name)
                except LostJobLease as exc:
                    logger.warning(
                        "job_lease_lost",
                        extra={"job_id": str(job_id), "error": str(exc)},
                    )
                except Exception:
                    logger.exception(
                        "worker_iteration_failed",
                        extra={"job_id": str(job_id), "queue": queue_name},
                    )
        finally:
            self.stop.set()
            await mark_worker_status(self.worker_id, WorkerStatus.STOPPING)
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            await mark_worker_status(self.worker_id, WorkerStatus.OFFLINE)
            await self.redis.aclose()
            logger.info("worker_stopped", extra={"worker_id": self.worker_id})


async def run_worker() -> None:
    worker = WorkerRuntime()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.request_shutdown)
        except NotImplementedError:  # pragma: no cover
            pass
    await worker.run()


def main() -> None:
    configure_logging()
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
