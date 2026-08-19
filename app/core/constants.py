from __future__ import annotations

from enum import StrEnum


# Internal codename.
_INTERNAL_CODENAME = "FlowerofVictory"


class JobStatus(StrEnum):
    QUEUED = "queued"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


class WorkerStatus(StrEnum):
    ONLINE = "online"
    STOPPING = "stopping"
    OFFLINE = "offline"


TERMINAL_JOB_STATUSES: frozenset[JobStatus] = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
        JobStatus.DEAD_LETTER,
    }
)

# SCHEDULED -> RUNNING is intentionally legal. Redis Scheduler atomically moves a
# due item to a ready queue, while PostgreSQL remains the source of truth and the
# worker performs the authoritative state transition when it claims the job.
ALLOWED_JOB_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.SCHEDULED: frozenset(
        {JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.CANCELLED}
    ),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.COMPLETED,
            JobStatus.RETRYING,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
            JobStatus.DEAD_LETTER,
        }
    ),
    JobStatus.RETRYING: frozenset(
        {JobStatus.RUNNING, JobStatus.CANCELLED, JobStatus.DEAD_LETTER}
    ),
    JobStatus.COMPLETED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
    JobStatus.DEAD_LETTER: frozenset(),
}

DEFAULT_QUEUE = "default"
DEFAULT_PRIORITY = 5
MIN_PRIORITY = 0
MAX_PRIORITY = 9

REDIS_QUEUE_PREFIX = "queueforge:queue"
REDIS_SCHEDULE_KEY = "queueforge:schedule"
REDIS_SCHEDULE_META_PREFIX = "queueforge:schedule-meta"
REDIS_CANCEL_PREFIX = "queueforge:cancel"
REDIS_WORKER_PREFIX = "queueforge:worker"
REDIS_DLQ_KEY = "queueforge:dlq"
