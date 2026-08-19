from __future__ import annotations

import asyncio
import logging
import signal
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.core.metrics import OUTBOX_PENDING
from app.database.models.outbox import OutboxEvent
from app.database.session import SessionFactory
from app.queue.producer import RedisProducer
from app.queue.redis import get_redis
from app.scheduler.scheduler import RedisScheduler

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self) -> None:
        self.redis = get_redis()
        self.producer = RedisProducer(self.redis)
        self.scheduler = RedisScheduler(self.redis)

    async def publish_batch(self, *, limit: int) -> int:
        published = 0
        async with SessionFactory() as session:
            async with session.begin():
                pending_count = await session.scalar(
                    select(func.count()).select_from(OutboxEvent).where(
                        OutboxEvent.published_at.is_(None)
                    )
                )
                OUTBOX_PENDING.set(int(pending_count or 0))
                statement = (
                    select(OutboxEvent)
                    .where(
                        OutboxEvent.published_at.is_(None),
                        OutboxEvent.available_at <= datetime.now(UTC),
                    )
                    .order_by(OutboxEvent.created_at)
                    .with_for_update(skip_locked=True)
                    .limit(limit)
                )
                events = list((await session.scalars(statement)).all())
                for event in events:
                    try:
                        payload = event.payload
                        if event.event_type == "job.enqueue":
                            await self.producer.enqueue(
                                payload["job_id"],
                                queue_name=payload["queue_name"],
                                priority=int(payload["priority"]),
                            )
                        elif event.event_type == "job.schedule":
                            await self.scheduler.schedule(
                                payload["job_id"],
                                due_at=datetime.fromisoformat(payload["scheduled_at"]),
                                queue_name=payload["queue_name"],
                                priority=int(payload["priority"]),
                            )
                        else:
                            raise ValueError(f"Unsupported outbox event {event.event_type}")
                    except Exception as exc:
                        event.attempts += 1
                        event.last_error = str(exc)
                        logger.exception(
                            "outbox_publish_failed",
                            extra={"event_id": str(event.id), "event_type": event.event_type},
                        )
                        continue

                    event.published_at = datetime.now(UTC)
                    event.attempts += 1
                    event.last_error = None
                    published += 1
        return published


async def run_outbox() -> None:
    settings = get_settings()
    publisher = OutboxPublisher()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover
            pass

    logger.info("outbox_publisher_started")
    try:
        while not stop.is_set():
            count = await publisher.publish_batch(limit=settings.outbox_batch_size)
            if count:
                logger.info("outbox_published", extra={"count": count})
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.outbox_poll_interval_seconds
                )
            except TimeoutError:
                pass
    finally:
        await publisher.redis.aclose()
        logger.info("outbox_publisher_stopped")


def main() -> None:
    configure_logging()
    asyncio.run(run_outbox())


if __name__ == "__main__":
    main()
