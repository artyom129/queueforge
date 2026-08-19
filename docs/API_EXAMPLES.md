# API Examples

Base URL: `http://localhost:8000/api/v1`

## Create an echo job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "echo",
    "payload": {"message": "hello"}
  }'
```

## Priority + named queue

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "generate_report",
    "payload": {"customer_id": 421},
    "priority": 9,
    "queue_name": "critical"
  }'
```

## Schedule a job

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "echo",
    "payload": {"message": "later"},
    "scheduled_at": "2026-08-20T12:00:00+00:00"
  }'
```

## Retry demo

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "unstable_task",
    "payload": {"fail_until_attempt": 2},
    "max_retries": 3
  }'
```

## Idempotent report

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "generate_report",
    "payload": {"customer_id": 421},
    "idempotency_key": "report-customer-421"
  }'
```

Repeat the exact request. The second response references the same job ID and contains `"deduplicated": true`.

## Get job and attempt history

```bash
curl http://localhost:8000/api/v1/jobs/<JOB_ID>
```

## Watch state changes with SSE

```bash
curl -N http://localhost:8000/api/v1/jobs/<JOB_ID>/events
```

## Cancel

```bash
curl -X POST http://localhost:8000/api/v1/jobs/<JOB_ID>/cancel
```

## DLQ

```bash
curl http://localhost:8000/api/v1/dlq
```

## Requeue a DLQ job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/<JOB_ID>/requeue
```

## Workers

```bash
curl http://localhost:8000/api/v1/workers
```

## Health

```bash
curl http://localhost:8000/api/v1/health/live
curl http://localhost:8000/api/v1/health/ready
```

## Metrics

```bash
curl http://localhost:8000/api/v1/metrics
```
