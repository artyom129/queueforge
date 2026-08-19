from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from app.core.constants import DEFAULT_PRIORITY, DEFAULT_QUEUE, JobStatus


class JobCreate(BaseModel):
    type: str = Field(min_length=1, max_length=120, pattern=r"^[a-zA-Z0-9_.:-]+$")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=DEFAULT_PRIORITY, ge=0, le=9)
    scheduled_at: datetime | None = None
    max_retries: int = Field(default=3, ge=0, le=20)
    timeout_seconds: int = Field(default=60, ge=1, le=86_400)
    callback_url: AnyHttpUrl | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)
    queue_name: str = Field(
        default=DEFAULT_QUEUE,
        min_length=1,
        max_length=80,
        pattern=r"^[a-zA-Z0-9_.:-]+$",
    )

    @field_validator("scheduled_at")
    @classmethod
    def normalize_scheduled_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("scheduled_at must include a timezone")
        return value.astimezone(UTC)


class JobAccepted(BaseModel):
    id: uuid.UUID
    status: JobStatus
    created_at: datetime
    queue_name: str
    deduplicated: bool = False


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    created_at: datetime
    updated_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    retry_count: int
    max_retries: int
    timeout_seconds: int
    result: dict[str, Any] | None
    error: str | None
    callback_url: str | None
    idempotency_key: str | None
    queue_name: str
    worker_id: str | None
    cancellation_requested: bool


class JobAttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str | None
    result: dict[str, Any] | None
    error: str | None
    duration_ms: int | None


class JobDetail(JobRead):
    attempts: list[JobAttemptRead] = Field(default_factory=list)


class JobCancelResponse(BaseModel):
    id: uuid.UUID
    status: JobStatus
    cancellation_requested: bool


class JobRequeueResponse(BaseModel):
    id: uuid.UUID
    status: JobStatus
    queue_name: str
