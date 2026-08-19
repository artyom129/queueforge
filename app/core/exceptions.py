from __future__ import annotations

from uuid import UUID

from app.core.constants import JobStatus


class QueueForgeError(Exception):
    """Base error for application-level failures."""


class JobNotFoundError(QueueForgeError):
    def __init__(self, job_id: UUID) -> None:
        super().__init__(f"Job {job_id} was not found")
        self.job_id = job_id


class InvalidStateTransitionError(QueueForgeError):
    def __init__(self, current: JobStatus, target: JobStatus) -> None:
        super().__init__(f"Invalid job state transition: {current.value} -> {target.value}")
        self.current = current
        self.target = target


class UnknownTaskError(QueueForgeError):
    def __init__(self, task_type: str) -> None:
        super().__init__(f"Unknown task type: {task_type}")
        self.task_type = task_type


class RetryableTaskError(QueueForgeError):
    """A task failure that may be retried according to the job retry policy."""


class NonRetryableTaskError(QueueForgeError):
    """A deterministic/permanent task failure that should fail immediately."""


class JobExecutionTimeout(QueueForgeError):
    pass


class JobCancelled(QueueForgeError):
    pass


class LostJobLease(QueueForgeError):
    """A stale worker attempted to commit after losing its execution lease."""

