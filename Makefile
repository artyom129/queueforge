.PHONY: install dev up down logs test test-unit test-integration lint format coverage migrate revision clean

install:
	python -m pip install -e .

dev:
	python -m pip install -e ".[dev]"

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f api worker scheduler outbox

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest -m integration tests/integration

lint:
	ruff check .

format:
	ruff check --fix .
	ruff format .

coverage:
	pytest --cov=app --cov-report=term-missing --cov-report=html

migrate:
	alembic upgrade head

revision:
	alembic revision --autogenerate -m "$(m)"

clean:
	rm -rf .pytest_cache .ruff_cache .coverage htmlcov dist build *.egg-info
