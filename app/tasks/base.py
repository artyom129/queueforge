from __future__ import annotations

import uuid
from dataclasses import dataclass
from collections.abc import Awaitable
from typing import Protocol


@dataclass(slots=True, frozen=True)
class TaskContext:
    job_id: uuid.UUID
    attempt: int
    worker_id: str


class TaskHandler(Protocol):
    def __call__(
        self, payload: dict, context: TaskContext
    ) -> Awaitable[dict]: ...
