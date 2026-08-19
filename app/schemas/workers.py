from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict

from app.core.constants import WorkerStatus


class WorkerRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    worker_id: str
    hostname: str
    pid: int
    queues: list[str]
    status: WorkerStatus
    started_at: datetime
    last_heartbeat: datetime
    stopped_at: datetime | None
    alive: bool = False
