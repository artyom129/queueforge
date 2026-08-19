# QueueForge Architecture

## Components

### API

FastAPI accepts, validates, queries, and cancels jobs. PostgreSQL is the source of truth. Creating a job never writes Redis directly; the API transaction writes a job row and one outbox row atomically.

### Transactional outbox publisher

`app.queue.outbox` reads unpublished outbox rows using `FOR UPDATE SKIP LOCKED`. It publishes either to a ready queue or the delayed schedule set. Only after the Redis operation succeeds does it set `published_at`.

This allows several outbox publishers to run concurrently without publishing the same locked batch at the same time.

### Redis transport

Ready queues are sorted sets. Member = job UUID. Score = priority band + enqueue timestamp. This gives priority ordering and FIFO inside one priority.

Workers block with `BZPOPMIN`. Redis therefore does the waiting; no tight polling loop consumes a CPU core.

### PostgreSQL claim

A Redis pop is not enough to prove ownership. Before execution the worker locks the job row and verifies a claimable state. It then transitions the job to `RUNNING` and inserts the corresponding `job_attempts` row in one transaction.

This makes PostgreSQL authoritative even if Redis delivers a stale or repeated member. A fresh execution token is written during claim and acts as a fencing token for later completion.

### Worker

Each worker has a unique process ID, durable worker row, Redis heartbeat with TTL, and a graceful shutdown event.

Execution lifecycle:

1. block for a Redis member;
2. lock/claim the PostgreSQL row;
3. record attempt N;
4. resolve the task handler from the registry;
5. race task execution against timeout and cancellation;
6. persist result or failure;
7. on retryable failure, write a delayed retry outbox event;
8. on retry exhaustion, transition to DLQ;
9. dispatch callback for terminal outcomes.

### Scheduler, reconciliation, and stale-worker reaper

The scheduler scans Redis's delayed sorted set for due timestamps. A Lua script atomically removes the member from the schedule set and inserts it into the destination ready sorted set.

A periodic PostgreSQL reconciliation pass re-publishes authoritative QUEUED and due SCHEDULED/RETRYING rows with `ZADD NX`. This repairs the small destructive-pop-before-database-claim crash window without creating duplicate sorted-set members.

The stale-worker reaper looks for RUNNING jobs whose worker heartbeat expired. It marks the unfinished attempt as `worker_lost`, invalidates the execution token, and moves the job into retry or DLQ. If the old process later returns, its stale token prevents it from committing over the new attempt.

### Callback dispatcher

The callback dispatcher performs bounded HTTP retries with httpx. Job state is already durable before callback delivery starts, so a callback outage cannot roll back completed work.

### SSE

The API offers a simple server-sent event stream for a job. It emits only when status/version changes and closes on terminal states.

## Data model

### jobs

The durable state machine and task metadata.

### job_attempts

Immutable-ish execution history per attempt: worker, start/end, duration, outcome, result/error.

### workers

Durable registration and heartbeat timestamps.

### outbox_events

Durable publication intent from PostgreSQL into Redis.

## State machine

The normal transition graph is explicit in `app/core/constants.py` and validated by `app/core/state_machine.py`.

Terminal states have no outgoing normal transitions. DLQ requeue is a separate operator command rather than a hidden state-machine edge.

## Concurrency boundaries

- job creation: PostgreSQL unique idempotency constraint
- outbox publisher: `FOR UPDATE SKIP LOCKED`
- worker claim: job row `FOR UPDATE` + execution fencing token
- ready queue pop: Redis atomic blocking pop
- scheduled release: Redis Lua atomic move
- queue publication: idempotent `ZADD` member keyed by job UUID
