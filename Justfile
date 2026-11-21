# Arthala ID — by Robit Al Hazmi
# Standardized local dev & quality commands. Run `just` to list recipes.

set dotenv-load
set shell := ["bash", "-euo", "pipefail", "-c"]

compose_local := "docker compose -f infra/docker/docker-compose.local.yml"
compose_prod := "infra/coolify/docker-compose.prod.yml"
pg_user := env_var_or_default("POSTGRES_USER", "arthala")
pg_db := env_var_or_default("POSTGRES_DB", "arthala")

# List available recipes
default:
    @just --list --unsorted

# --- Setup -------------------------------------------------------------------

# Install Python deps (frozen lockfile) and git hooks
[group('setup')]
bootstrap:
    uv sync --frozen
    uv run pre-commit install
    @test -f .env || (cp .env.example .env && echo "Created .env from .env.example (local defaults)")

# --- Local sandbox (Postgres 16 + MinIO + Redpanda) --------------------------

# Start the offline local sandbox
[group('sandbox')]
up:
    {{ compose_local }} up -d --wait

# Stop the local sandbox (keeps volumes)
[group('sandbox')]
down:
    {{ compose_local }} down

# Stop the local sandbox AND delete its volumes (local data only)
[group('sandbox')]
nuke:
    {{ compose_local }} down -v

# Show sandbox container status
[group('sandbox')]
ps:
    {{ compose_local }} ps

# Tail sandbox logs (optionally for one service)
[group('sandbox')]
logs service="":
    {{ compose_local }} logs -f --tail=100 {{ service }}

# Re-apply the idempotent DDL to LOCAL Postgres (no host psql required)
[group('sandbox')]
db-init:
    {{ compose_local }} exec -T postgres psql -v ON_ERROR_STOP=1 -U {{ pg_user }} -d {{ pg_db }} < infra/sql/ddl.sql

# Open a psql shell on LOCAL Postgres
[group('sandbox')]
psql:
    {{ compose_local }} exec postgres psql -U {{ pg_user }} -d {{ pg_db }}

# --- Quality gates -----------------------------------------------------------

# Ruff lint
[group('quality')]
lint:
    uv run ruff check .

# Auto-fix lint issues and format code
[group('quality')]
fmt:
    uv run ruff check --fix .
    uv run ruff format .

# Strict mypy type check
[group('quality')]
typecheck:
    uv run mypy apps/api apps/worker pipelines

# Pytest with coverage
[group('quality')]
test *args:
    uv run pytest --cov {{ args }}

# Validate docker compose files
[group('quality')]
compose-check:
    {{ compose_local }} config -q
    @if [ -f {{ compose_prod }} ]; then docker compose -f {{ compose_prod }} config -q; fi

# Run every Python + compose gate enforced by CI
[group('quality')]
check:
    uv run ruff check .
    uv run ruff format --check .
    uv run mypy apps/api apps/worker pipelines
    uv run pytest --cov
    @just compose-check

# Run all pre-commit hooks against the whole repo
[group('quality')]
pre-commit:
    uv run pre-commit run --all-files

# --- Pipelines & services (implemented in later phases) -----------------------

# Bronze -> Silver -> Gold -> local Postgres (Phase 1)
[group('pipelines')]
etl-local:
    uv run python -m pipelines.ingestion.runner --env local
    cd pipelines/dbt && uv run dbt run --target local && uv run dbt test --target local
    uv run python -m pipelines.ingestion.sync_neon --env local

# Train LightGBM distress model locally (Phase 2)
[group('pipelines')]
ml-train:
    uv run python -m pipelines.ml.train --env local

# Evidently drift check locally (Phase 2)
[group('pipelines')]
ml-monitor:
    uv run python -m pipelines.ml.monitor --env local

# Replay historical IDX ticks into local Redpanda (Phase 3)
[group('pipelines')]
replay speed="10" tickers="BBCA,BBRI,ASII,TLKM":
    uv run python -m pipelines.replay.producer --speed {{ speed }} --tickers {{ tickers }}

# FastAPI dev server (Phase 3)
[group('services')]
api:
    uv run uvicorn apps.api.app.main:app --reload --port 8000

# Kafka consumer + @ArthalaIDBot (Phase 3)
[group('services')]
worker:
    uv run python -m apps.worker.worker.main

# Next.js dev server (Phase 4)
[group('services')]
web:
    cd apps/web && npm run dev
