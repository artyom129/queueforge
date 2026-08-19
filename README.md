# QueueForge

**Reliable distributed background job processing platform for Python applications.**

QueueForge is a production-oriented portfolio project that implements a background-job system directly on top of **PostgreSQL + Redis**, without Celery, RQ, Dramatiq, or another full job framework.

The interesting part is not the REST CRUD. The project focuses on delivery semantics, concurrency, state transitions, retry behavior, graceful workers, scheduling, idempotency, observability, and failure recovery.

## Why this project exists

A client submits work over HTTP. QueueForge validates it, persists the authoritative job record in PostgreSQL, writes a transactional outbox event, publishes the job to Redis, and lets independent workers execute registered task handlers.

Failures can be retried with exponential backoff and jitter. Exhausted retryable jobs enter a Dead Letter Queue. Scheduled jobs are released when due. Running jobs have timeouts and cancellation signals. Terminal jobs can invoke an HTTP webhook callback. Worker heartbeats, health checks, JSON logs, SSE status events, and Prometheus metrics make the system observable.

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
                          │            └──► DLQ when retries exhaust
                          ▼
                      Scheduler

Worker ──► webhook callback
Worker ──► Redis + PostgreSQL heartbeat
API    ──► SSE job status stream
```

## Engineering highlights

- FastAPI REST API with Pydantic v2 validation
- PostgreSQL as the durable source of truth
- SQLAlchemy 2.x async + asyncpg
- Redis transport implemented directly with sorted sets
- blocking worker consumption with `BZPOPMIN` (no CPU busy-loop)
- named queues and priority ordering
- transactional outbox between PostgreSQL and Redis
- database-enforced idempotency with race-safe conflict handling
- row-level locking for authoritative worker claims
- explicit job state machine
- deterministic retries with exponential backoff + bounded jitter
- delayed/scheduled jobs
- Dead Letter Queue and operator requeue
- graceful SIGINT/SIGTERM worker shutdown
- stale-worker recovery with heartbeat lease fencing
- running-job cancellation
- execution timeouts
- registered task handlers only; no arbitrary code execution
- HTTP callback delivery with retry
- worker heartbeat and liveness reporting
- Prometheus metrics
- JSON application logs
- liveness/readiness endpoints
- SSE status updates
- Alembic migrations
- unit and integration tests
- concurrent duplicate-submission test
- Docker / Docker Compose
- GitHub Actions CI

## Stack

Python 3.12+, FastAPI, Uvicorn, Pydantic v2, SQLAlchemy 2.x async, asyncpg, PostgreSQL, Redis, Alembic, httpx, prometheus-client, pytest, pytest-asyncio, pytest-cov, Ruff, Docker, Docker Compose, GitHub Actions.

## Quick start

Requirements: Docker Engine + Docker Compose v2.

```bash
docker compose up --build
```

That command works with built-in Compose defaults. Copy `.env.example` to `.env` only when you want to override retry, heartbeat, API-key, or scheduler settings.

The compose stack starts:

- PostgreSQL
- Redis
- migration job
- FastAPI API
- transactional outbox publisher
- scheduler
- worker

Open:

- Swagger UI: `http://localhost:8000/docs`
- readiness: `http://localhost:8000/api/v1/health/ready`
- metrics: `http://localhost:8000/api/v1/metrics`

## Create a job

```bash
curl -i -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "generate_report",
    "payload": {"customer_id": 421},
    "priority": 5,
    "max_retries": 3,
    "timeout_seconds": 60,
    "idempotency_key": "report-customer-421"
  }'
```

The API returns **202 Accepted**:

```json
{
  "id": "9e6d61e4-528b-4d76-84b0-8063c6101c78",
  "status": "queued",
  "created_at": "2026-08-19T10:00:00Z",
  "queue_name": "default",
  "deduplicated": false
}
```

Submit the same request again with the same `idempotency_key`. QueueForge returns the existing job and sets `deduplicated: true` instead of creating another job.

## Demo task types

### `echo`

```json
{
  "type": "echo",
  "payload": {"message": "hello"}
}
```

Result:

```json
{"message": "hello"}
```

### `slow_task`

```json
{
  "type": "slow_task",
  "payload": {"seconds": 3},
  "timeout_seconds": 10
}
```

### `unstable_task`

This task is intentionally deterministic for retry demonstrations and tests.

```json
{
  "type": "unstable_task",
  "payload": {"fail_until_attempt": 2},
  "max_retries": 3
}
```

Attempt 1 fails, attempt 2 fails, attempt 3 succeeds. There is no random test behavior.

## Job states

```text
QUEUED ─────► RUNNING ─────► COMPLETED
   │             │
   │             ├─────────► RETRYING ─────► RUNNING
   │             │                               │
   │             ├─────────► FAILED              └─ ...
   │             ├─────────► DEAD_LETTER
   │             └─────────► CANCELLED
   └───────────────────────► CANCELLED

SCHEDULED ──► RUNNING / CANCELLED
RETRYING  ──► RUNNING / CANCELLED / DEAD_LETTER
```

Terminal states cannot transition back into execution through the normal state machine. For example `COMPLETED -> RUNNING` is rejected. DLQ requeue is an explicit operator action and intentionally resets the retry budget.

## Idempotency and the race condition

A naive implementation does this:

```python
if not await exists(key):
    await create_job(key)
```

Two concurrent requests can both pass `exists()` before either inserts.

QueueForge instead uses:

1. a unique PostgreSQL constraint on `jobs.idempotency_key`;
2. `INSERT` + `flush()` inside a transaction;
3. `IntegrityError` handling after PostgreSQL decides the winner;
4. lookup of the already-created job for the losing request;
5. transactional outbox creation in the same transaction as the winning job.

Because only the winning transaction creates the outbox row, duplicate submissions do not create duplicate enqueue intentions. The Redis queue also uses the `job_id` as its sorted-set member, so re-publishing the same outbox event is idempotent at the transport layer.

See `tests/integration/test_idempotency.py`.

## Redis queue design

Ready queues are Redis sorted sets:

```text
queueforge:queue:critical
queueforge:queue:default
queueforge:queue:low
```

A score combines priority and FIFO time. Higher application priority receives a lower sorted-set score, and jobs with equal priority are FIFO.

Workers use Redis `BZPOPMIN`, which blocks efficiently when no work exists.

Scheduled and retry jobs live in:

```text
queueforge:schedule
```

The scheduler atomically moves a due member from the schedule sorted set into its target ready queue with a Lua script.

## Delivery semantics

QueueForge is designed around **at-least-once delivery with idempotent transport operations**.

The database claim is authoritative. A worker may pop a stale or duplicated Redis member, but only a job in a claimable PostgreSQL state can enter `RUNNING`. Claiming uses a row lock, preventing two workers from successfully running the same state transition concurrently. Each claim also receives a fresh `execution_token`; completion is accepted only from the worker that still owns that token.

The transactional outbox prevents the classic API crash window between “job committed” and “message published”. If the outbox publisher crashes after Redis accepted a `ZADD` but before `published_at` is saved, the event can be published again safely because the sorted-set member is the same `job_id`.

A reconciliation pass repairs the destructive Redis-pop-before-DB-claim window by safely re-publishing authoritative QUEUED/due jobs with `ZADD NX`. A stale-worker reaper detects expired worker heartbeats for long-running `RUNNING` jobs, records the lost attempt, and moves the job into retry/DLQ. The old worker is fenced by its invalidated execution token and cannot overwrite a newer attempt.

## Retry policy

Retryable failures use:

```text
delay = min(base_delay * 2 ** retry_count, max_delay)
```

A configurable bounded jitter is added to reduce retry synchronization across workers.

Defaults:

```text
base delay: 1s
max delay: 60s
jitter: ±10%
```

When the retry budget is exhausted, the job transitions to `dead_letter` and a compact DLQ audit record is also written to Redis.

## Cancellation

`POST /api/v1/jobs/{job_id}/cancel`

Queued/scheduled/retrying jobs are cancelled immediately in PostgreSQL. Running jobs receive a Redis cancellation signal, while a durable `cancellation_requested` flag remains in PostgreSQL. The worker executor polls this signal and cancels the handler task cooperatively.

## Execution timeouts

Every job has `timeout_seconds`. The worker executes handlers inside an asyncio task and races handler completion against cancellation and the configured deadline. Timeout failures are retryable by policy.

## Webhook callbacks

Set `callback_url` when creating a job. Terminal jobs POST a compact event payload to the callback endpoint. Callback delivery has bounded retries and exponential delay. Callback failure never rewrites the already-durable job result.

For a larger production deployment, callback delivery would normally be promoted into its own durable outbox/queue. QueueForge keeps it isolated in `app/callbacks/dispatcher.py` so that extension is straightforward.

## SSE status events

```bash
curl -N http://localhost:8000/api/v1/jobs/<JOB_ID>/events
```

The endpoint emits state changes until the job reaches a terminal state.

## Health and observability

```text
GET /api/v1/health/live
GET /api/v1/health/ready
GET /api/v1/metrics
GET /api/v1/workers
```

Readiness checks PostgreSQL and Redis. Worker liveness uses short-lived Redis heartbeat keys plus durable PostgreSQL worker records.

Prometheus metrics include accepted jobs, execution outcomes, retry counts, execution latency, callback outcomes, heartbeat writes, and outbox observations.

## API-key protection

For local development, API-key enforcement is disabled when `QUEUEFORGE_API_KEY` is empty.

Set it in production-like environments:

```bash
QUEUEFORGE_API_KEY=replace-me
```

Mutation endpoints then require:

```text
X-API-Key: replace-me
```

## Run locally without Docker

Start PostgreSQL and Redis, then:

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
python -m pip install -e ".[dev]"
cp .env.example .env
alembic upgrade head
uvicorn app.main:app --reload
```

In separate terminals:

```bash
python -m app.queue.outbox
python -m app.scheduler.scheduler
python -m app.workers.worker
```

Or via the CLI:

```bash
queueforge outbox
queueforge scheduler
queueforge worker
```

## Tests

```bash
pytest tests/unit
pytest -m integration tests/integration
pytest --cov=app --cov-report=term-missing
```

Integration tests require PostgreSQL + Redis and expect the schema to be migrated. GitHub Actions provisions both services automatically.

The concurrency test fires eight simultaneous requests at the service layer using the same idempotency key and asserts:

- all calls return the same job ID;
- exactly one caller wins creation;
- exactly one job row exists;
- exactly one outbox row exists.

## Lint / format

```bash
ruff check .
ruff format --check .
```

Auto-fix:

```bash
ruff check --fix .
ruff format .
```

## Scale workers

With Docker Compose:

```bash
docker compose up --build --scale worker=4
```

All worker processes coordinate through Redis transport and PostgreSQL state transitions.

## Repository layout

```text
queueforge/
├── .github/workflows/ci.yml
├── app/
│   ├── api/
│   ├── callbacks/
│   ├── core/
│   ├── database/
│   ├── queue/
│   ├── scheduler/
│   ├── schemas/
│   ├── services/
│   ├── tasks/
│   └── workers/
├── alembic/
├── docs/
├── scripts/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── README.md
```

## Design decisions

More detail:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/RELIABILITY.md`](docs/RELIABILITY.md)
- [`docs/API_EXAMPLES.md`](docs/API_EXAMPLES.md)

## Non-goals

QueueForge deliberately does **not** execute arbitrary user-provided Python code. API clients can only submit task types that the application registered ahead of time.

QueueForge is also not trying to reproduce every feature of Celery. Its purpose is to expose the backend engineering decisions that a full framework normally hides.

## License

MIT.
