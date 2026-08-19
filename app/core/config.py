from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="QUEUEFORGE_",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "QueueForge"
    environment: str = "development"
    log_level: str = "INFO"
    api_key: str | None = None

    database_url: str = "postgresql+asyncpg://queueforge:queueforge@postgres:5432/queueforge"
    redis_url: str = "redis://redis:6379/0"

    worker_queues: str = "critical,default,low"
    worker_poll_timeout_seconds: int = 5
    worker_heartbeat_interval_seconds: float = 5.0
    worker_heartbeat_ttl_seconds: int = 20
    worker_lost_after_seconds: float = 30.0
    reaper_interval_seconds: float = 10.0

    scheduler_poll_interval_seconds: float = 0.5
    scheduler_batch_size: int = 100
    reconciliation_interval_seconds: float = 10.0
    reconciliation_batch_size: int = 500
    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = 100

    retry_base_delay_seconds: float = 1.0
    retry_max_delay_seconds: float = 60.0
    retry_jitter_ratio: float = Field(default=0.10, ge=0.0, le=1.0)

    callback_connect_timeout_seconds: float = 5.0
    callback_read_timeout_seconds: float = 10.0
    callback_max_attempts: int = 3
    callback_base_delay_seconds: float = 0.5

    sse_poll_interval_seconds: float = 0.75

    @field_validator("worker_queues")
    @classmethod
    def validate_worker_queues(cls, value: str) -> str:
        queues = [item.strip() for item in value.split(",") if item.strip()]
        if not queues:
            raise ValueError("worker_queues must contain at least one queue")
        return ",".join(queues)

    @property
    def worker_queue_names(self) -> list[str]:
        return [item.strip() for item in self.worker_queues.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
