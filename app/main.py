from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.queue.redis import get_redis

configure_logging()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await get_redis().aclose()


app = FastAPI(
    title="QueueForge",
    version="1.0.0",
    description=(
        "Reliable distributed background job processing platform built directly "
        "on PostgreSQL and Redis."
    ),
    lifespan=lifespan,
)
app.include_router(api_router)


@app.get("/", tags=["meta"])
async def root() -> dict[str, str]:
    return {
        "name": settings.app_name,
        "docs": "/docs",
        "health": "/api/v1/health/ready",
        "metrics": "/api/v1/metrics",
    }
