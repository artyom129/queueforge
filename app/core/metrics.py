from prometheus_client import Counter, Gauge, Histogram

JOBS_CREATED = Counter(
    "queueforge_jobs_created_total",
    "Jobs accepted by QueueForge",
    ["queue", "type"],
)
JOBS_EXECUTED = Counter(
    "queueforge_jobs_executed_total",
    "Worker execution outcomes",
    ["queue", "type", "outcome"],
)
JOB_RETRIES = Counter(
    "queueforge_job_retries_total",
    "Retry attempts scheduled",
    ["queue", "type"],
)
JOB_EXECUTION_SECONDS = Histogram(
    "queueforge_job_execution_seconds",
    "Task handler execution duration",
    ["queue", "type"],
)
CALLBACK_DELIVERIES = Counter(
    "queueforge_callback_deliveries_total",
    "Webhook delivery outcomes",
    ["outcome"],
)
WORKER_HEARTBEATS = Counter(
    "queueforge_worker_heartbeats_total",
    "Worker heartbeat writes",
)
OUTBOX_PENDING = Gauge(
    "queueforge_outbox_pending",
    "Number of unpublished outbox rows observed by the publisher",
)
