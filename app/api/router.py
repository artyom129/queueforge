from fastapi import APIRouter

from app.api.routes import dlq, health, jobs, metrics, workers

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(jobs.router)
api_router.include_router(workers.router)
api_router.include_router(dlq.router)
api_router.include_router(health.router)
api_router.include_router(metrics.router)
