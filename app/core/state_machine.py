from __future__ import annotations

from app.core.constants import ALLOWED_JOB_TRANSITIONS, JobStatus
from app.core.exceptions import InvalidStateTransitionError


def validate_job_transition(current: JobStatus, target: JobStatus) -> None:
    if target not in ALLOWED_JOB_TRANSITIONS[current]:
        raise InvalidStateTransitionError(current, target)
