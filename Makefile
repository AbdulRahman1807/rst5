.PHONY: help up down test eval lint format clean

VENV ?= .venv
PYTHON = $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)
PYTEST = $(if $(wildcard $(VENV)/bin/pytest),$(VENV)/bin/pytest,pytest)
RUFF = $(if $(wildcard $(VENV)/bin/ruff),$(VENV)/bin/ruff,ruff)
MYPY = $(if $(wildcard $(VENV)/bin/mypy),$(VENV)/bin/mypy,mypy)

help:
	@echo "Available targets:"
	@echo "  make up      - Start the 5-service docker-compose stack"
	@echo "  make down    - Stop docker-compose stack (non-destructive, preserves volumes)"
	@echo "  make clean   - Tear down docker-compose stack with volumes (-v) and remove caches"
	@echo "  make test    - Run pytest test suite with coverage"
	@echo "  make eval    - Run evaluation harness"
	@echo "  make lint    - Run ruff linter and mypy typechecker"
	@echo "  make format  - Auto-format and auto-fix code with ruff"

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -f .coverage

test:
	$(PYTEST) tests/ -v --cov=api --cov=loader --cov-report=term-missing

eval:
	$(PYTHON) -m eval.evaluate

lint:
	$(RUFF) check api loader tests eval pyproject.toml
	$(MYPY) api loader tests eval --explicit-package-bases

format:
	$(RUFF) check --fix api loader tests eval pyproject.toml
	$(RUFF) format api loader tests eval pyproject.toml
