# Contributing

## Development setup

1. Use Python 3.12+.
2. Install development dependencies: `python -m pip install -e ".[dev]"`.
3. Start PostgreSQL and Redis or use `docker compose up`.
4. Run `alembic upgrade head`.
5. Run unit tests and Ruff before opening a pull request.

## Quality gates

```bash
ruff check .
ruff format --check .
pytest --cov=app --cov-report=term-missing
```

## Architecture rules

- PostgreSQL remains authoritative for durable job state.
- Redis must not become the only copy of a state transition.
- Do not add Celery/RQ/Dramatiq or another full job framework.
- New job states require explicit state-machine edges and tests.
- Task handlers must be pre-registered. Never execute arbitrary submitted Python.
- New concurrency behavior should include a race/failure-mode test where practical.
- Avoid god modules; keep API, storage, transport, execution, and policy separate.

## Commit style

Prefer small commits with clear intent, for example:

```text
feat(worker): add cooperative cancellation
fix(idempotency): handle concurrent unique-key conflicts
chore(ci): run postgres-backed integration tests
```
