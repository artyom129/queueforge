import pytest

from app.core.constants import JobStatus
from app.core.exceptions import InvalidStateTransitionError
from app.core.state_machine import validate_job_transition


def test_queued_can_run() -> None:
    validate_job_transition(JobStatus.QUEUED, JobStatus.RUNNING)


def test_completed_cannot_run_again() -> None:
    with pytest.raises(InvalidStateTransitionError):
        validate_job_transition(JobStatus.COMPLETED, JobStatus.RUNNING)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (JobStatus.RUNNING, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.RETRYING),
        (JobStatus.RUNNING, JobStatus.DEAD_LETTER),
        (JobStatus.RETRYING, JobStatus.RUNNING),
        (JobStatus.SCHEDULED, JobStatus.RUNNING),
    ],
)
def test_expected_transitions_are_valid(current: JobStatus, target: JobStatus) -> None:
    validate_job_transition(current, target)
