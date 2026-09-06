# QueueForge

[English](README.md) | **Русский**

**Надёжная распределённая платформа фоновых задач для Python-приложений.**

FastAPI • PostgreSQL • Redis • Workers • Retries • DLQ • Idempotency

QueueForge — production-oriented система обработки фоновых задач, построенная напрямую поверх PostgreSQL + Redis без Celery, RQ или другого готового job framework.

Главная ценность проекта — не CRUD API, а надёжность: delivery semantics, конкуренция, state transitions, retries, idempotency, scheduling, worker recovery и observability.

## Ключевые возможности

- transactional outbox между PostgreSQL и Redis;
- at-least-once delivery;
- database-enforced idempotency;
- race-safe обработка одновременных duplicate submissions;
- row-level locking для worker claims;
- явная job state machine;
- retries с exponential backoff и bounded jitter;
- delayed и scheduled jobs;
- Dead Letter Queue и operator requeue;
- graceful worker shutdown;
- heartbeat и stale-worker recovery;
- execution-token fencing;
- cancellation запущенных задач;
- execution timeouts;
- HTTP callbacks с retry;
- SSE status events;
- Prometheus metrics;
- JSON logs;
- liveness/readiness probes;
- Alembic migrations;
- unit и integration tests;
- Docker Compose и GitHub Actions CI.

## Архитектура

```text
Client
  │ HTTP
  ▼
FastAPI API
  │
  ├──────────────► PostgreSQL (source of truth)
  │                  │
  │                  └── Job + transactional outbox
  │
  ▼
Outbox Publisher ──► Redis ready queue / scheduled set
                          │
                          ▼
                    Worker process
                          │
              ┌───────────┼────────────┐
              ▼           ▼            ▼
          completed    retrying      failed
                          │            │
                          │            └──► DLQ
                          ▼
                      Scheduler
```

PostgreSQL хранит authoritative job state. Redis используется как transport для ready/scheduled queues. Даже если Redis содержит повторный или устаревший элемент, только корректный PostgreSQL state позволяет worker'у перевести job в `RUNNING`.

## Idempotency

Наивная проверка `if not exists(key): create()` содержит race condition. QueueForge закрывает её через:

1. unique constraint на `jobs.idempotency_key`;
2. `INSERT` внутри transaction;
3. обработку `IntegrityError`;
4. возврат уже существующей job проигравшему concurrent request;
5. создание outbox row только победившей transaction.

## Delivery semantics

Система использует **at-least-once delivery с idempotent transport operations**. Transactional outbox закрывает crash-window между сохранением job в БД и публикацией в Redis. Reconciliation восстанавливает authoritative queued jobs, а stale-worker reaper возвращает потерянные executions в retry/DLQ.

## Быстрый запуск

Требуются Docker и Docker Compose v2.

```bash
docker compose up --build
```

Будут запущены:

- PostgreSQL;
- Redis;
- migrations;
- FastAPI API;
- outbox publisher;
- scheduler;
- worker.

Открыть:

- Swagger: `http://localhost:8000/docs`
- Readiness: `http://localhost:8000/api/v1/health/ready`
- Metrics: `http://localhost:8000/api/v1/metrics`

## Тесты

```bash
pytest tests/unit
pytest -m integration tests/integration
pytest --cov=app --cov-report=term-missing
```

Integration suite включает concurrent idempotency test: восемь одновременных запросов с одним idempotency key должны получить один job ID, а в PostgreSQL должна появиться ровно одна job и одна outbox row.

## Масштабирование workers

```bash
docker compose up --build --scale worker=4
```

Workers координируются через Redis transport и PostgreSQL state transitions.

## Что демонстрирует проект

QueueForge — сильный backend/system-design проект про конкурентность, распределённую обработку задач, delivery guarantees, retries, idempotency, failure recovery, observability и production-oriented testing.

## License

MIT.
