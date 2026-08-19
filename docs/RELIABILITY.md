# Reliability Model

## Source of truth

PostgreSQL is authoritative for job state. Redis is a fast transport and coordination layer.

If Redis contains a member for a completed/cancelled job, the worker pop is harmless because the PostgreSQL claim rejects the state.

## Idempotent submission

`idempotency_key` has a unique database constraint. QueueForge intentionally does not rely on a separate preflight existence check. Concurrent requests compete on the INSERT; PostgreSQL picks one winner and the losing transaction fetches the committed existing row.

The job and its first outbox event are committed atomically.

## API-to-queue crash safety

Without an outbox:

1. commit job;
2. crash;
3. never publish Redis message.

With QueueForge:

1. commit job + outbox event;
2. publisher eventually sees the event;
3. Redis receives the message;
4. publisher marks it published.

If the publisher crashes between steps 3 and 4, it may re-run `ZADD`. Since the member is the same job UUID, that operation is idempotent.

## At-least-once semantics

QueueForge uses at-least-once delivery principles rather than pretending exactly-once execution can be guaranteed across PostgreSQL, Redis, worker processes, and arbitrary task side effects.

Task handlers that call external systems should therefore use an application idempotency key derived from `TaskContext.job_id` when the downstream system supports it.

## Worker crash during RUNNING

Workers publish a short-lived Redis heartbeat and a durable PostgreSQL heartbeat. The scheduler process also runs a stale-worker reaper. A RUNNING job becomes a recovery candidate only after its start time and worker heartbeat are older than the configured lost-worker threshold and the Redis heartbeat key is absent.

Recovery closes the previous `job_attempt`, invalidates the job's `execution_token`, and either schedules a retry or moves the job to DLQ when the retry budget is exhausted. A late worker must present the execution token it received during claim; after recovery that token no longer matches, so stale completion is rejected.

This is still intentionally **at-least-once** execution. If a worker performed an external side effect immediately before dying, a later retry can repeat that side effect. Handlers that call external systems should use `job_id` as an idempotency key whenever the downstream API supports it.

## Retry safety

Retryable errors use exponential backoff with bounded jitter. Retry count is persisted before a delayed retry outbox event is committed. The outbox dedupe key includes the retry number.

## DLQ

After the retry budget is exhausted, the job enters `dead_letter`. The authoritative payload/error remains in PostgreSQL. Redis also receives a capped diagnostic DLQ list for quick operational inspection.

Requeue is an explicit API operation and resets the retry budget while preserving historical attempt rows.

## Cancellation

PostgreSQL stores the durable cancellation request. Redis provides a low-latency signal to a running worker. This dual mechanism avoids making cancellation correctness depend only on an expiring Redis key.

## Timeouts

Timeout is enforced by the worker executor. The underlying asyncio task is cancelled when the deadline expires. Task authors should still write cancellation-friendly async handlers and use timeouts on their own external I/O.

## Callback failure

Callback delivery occurs after job state is committed. A webhook failure cannot make successful task execution disappear. Callback delivery is retried a bounded number of times and logged/metricized.

For stronger callback durability, promote callback intents into a dedicated outbox and callback worker using the same pattern as job publication.
