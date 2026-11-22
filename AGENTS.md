# AGENTS.md — Arthala ID (by Robit Al Hazmi)

This file is the canonical instruction manual for **Google Antigravity IDE** and all AI coding agents operating in the `arthala-id` repository. Read this file first before proposing plans, creating files, or executing terminal commands.

## 1. Project Identity & Source of Truth

* **Product:** **Arthala ID** — *by Robit Al Hazmi* (`arthala-id` / `@ArthalaIDBot`)

* **Mission:** An open-source, zero-cost-cloud **IDX Fundamental, Dividend & MLOps Intelligence Platform** built for:

  1. Personal Indonesian stock investing & real-time risk/dividend alerting.

  2. Public monetization (**Freemium Next.js Web Dashboard + Paid `@ArthalaIDBot` Telegram Alerts via Mayar.id QRIS**).

  3. Production-grade engineering alignment with **Bank OCBC NISP Data Engineer** standards (Medallion Lakehouse, Kafka streaming, versioned/monitored MLOps, data quality contracts, CI/CD, and IaC).

* **Canonical Reference Docs (Do Not Duplicate — Consult Directly):**

  * `docs/Implementation_Plan.md` — Architecture, Cloudflare R2 Medallion layout, Neon Postgres DDL, ML feature formulas, and OCBC traceability matrix.

  * `docs/Task.md` — Phased engineering backlog and active checklist.

  * `docs/Walkthrough.md` — Local Antigravity sandbox setup, free cloud provisioning, Coolify deploy, and verification runbook.

## 2. Hard Architectural & Resource Guardrails (Non-Negotiable)

Every design and code change **must** respect these production constraints:

1. **1 vCPU / 2 GB Coolify VPS Memory Budget:**

   * Keep the VPS dedicated strictly to lightweight runtime services with enforced Docker limits:

     * `arthala-api`: `mem_limit: 450m`, `--workers 1` (load ML artifacts once at app startup).

     * `arthala-worker`: `mem_limit: 250m` (async `aiokafka` consumer + `@ArthalaIDBot`).

     * `grafana-alloy`: `mem_limit: 100m` (ships metrics/logs to Grafana Cloud Free).

   * **Never** install heavy JVM stacks (Spark, Hadoop, Kafka brokers, Airflow schedulers, local Grafana/Prometheus TSDB) onto the live Coolify VPS.

   * **Never** move the Next.js frontend onto the VPS unless explicitly instructed—`apps/web` deploys to **Cloudflare Pages / Vercel Free Tier** (0 MiB VPS RAM).

2. **100% Python + SQL + DuckDB Data/ML Core (Zero Paid Services):**

   * Use only free open-source leaders and forever-free cloud tiers: **Cloudflare R2** (Lakehouse Parquet & MLflow artifacts), **Neon Serverless Postgres** (OLTP serving), **Aiven Kafka Free** (streaming), **GitHub Actions** (scheduled batch ETL & ML retraining), and **Mayar.id** (QRIS).

   * Do not introduce paid SaaS dependencies or non-Python backend microservices without explicit approval.

3. **Never Touch Production Destructively:**

   * All local development and tests run against `infra/docker/docker-compose.local.yml` (local Postgres 16, MinIO, Redpanda).

   * Never execute destructive SQL (`DROP TABLE`, `TRUNCATE`) or write test fixtures to production Neon, R2, or Aiven endpoints.

## 3. Monorepo Map

```text
arthala-id/
├── .agents/
│   └── workflows/   # Antigravity slash commands (/next-task, /verify-all, /run-etl-local, /pr-ready)
├── apps/
│   ├── api/         # FastAPI REST API, LightGBM+SHAP online inference, Mayar QRIS webhooks, /metrics
│   ├── worker/      # Async Aiven Kafka consumer, anomaly/disclosure rule engine, @ArthalaIDBot
│   └── web/         # Next.js (App Router, TypeScript, Tailwind, TradingView Lightweight Charts, EN/ID)
├── pipelines/
│   ├── ingestion/   # Multi-source IDX/yfinance scrapers, Pandera contracts, R2 Quarantine/DLQ, Neon sync
│   ├── dbt/         # dbt-duckdb Bronze -> Silver -> Gold marts & ML feature stores
│   ├── ml/          # LightGBM training, MLflow registry, bilingual SHAP explainability, Evidently drift
│   ├── replay/      # 24/7 Historical IDX tick & disclosure Kafka Replay Producer
│   └── airflow/     # Portable Airflow DAG mirrors (OCBC orchestration showcase)
├── infra/
│   ├── docker/      # docker-compose.local.yml (Postgres, MinIO, Redpanda)
│   ├── coolify/     # docker-compose.prod.yml (strict VPS memory caps)
│   ├── k8s/         # k3s / Helm manifests (OCBC Kubernetes showcase)
│   ├── terraform/   # IaC definitions for R2, Neon, Aiven
│   └── sql/         # ddl.sql (idempotent Neon Postgres schema)
└── docs/            # Implementation_Plan.md, Task.md, Walkthrough.md
```

## 4. Standard Commands (Use These — Do Not Guess)

Use `uv` for all Python dependencies and execution (never global `pip install`), `npm` inside `apps/web`, and `Justfile` targets when available:

```bash
# Local infrastructure (Postgres + MinIO + Redpanda)
docker compose -f infra/docker/docker-compose.local.yml up -d

# Python quality gates (run before finishing ANY Python task)
uv run ruff check .
uv run ruff format --check .
uv run mypy apps/api apps/worker pipelines
uv run pytest --cov

# Data pipeline (local MinIO + DuckDB + dbt + local Postgres)
uv run python -m pipelines.ingestion.runner --env local
cd pipelines/dbt && uv run dbt run --target local && uv run dbt test --target local
uv run python -m pipelines.ingestion.sync_neon --env local

# ML training & drift check
uv run python -m pipelines.ml.train --env local
uv run python -m pipelines.ml.monitor --env local

# Services (local dev)
uv run uvicorn apps.api.app.main:app --reload --port 8000
uv run python -m apps.worker.worker.main
uv run python -m pipelines.replay.producer --speed 10 --tickers BBCA,BBRI,ASII,TLKM

# Frontend quality gates (run before finishing ANY Next.js task)
cd apps/web && npm run lint && npm run typecheck && npm run build
```

## 5. Domain Engineering Standards

### 5.1 Python Backend & Worker (`apps/api`, `apps/worker`, `pipelines`)

* **Runtime & Typing:** Python 3.12+. Require full type annotations (`mypy` clean) and **Pydantic v2** schemas for API I/O and settings (`pydantic-settings`).

* **Async-First I/O:** Use async drivers (`asyncpg` / `psycopg` async, `httpx.AsyncClient`, `aiokafka`) in `arthala-api` and `arthala-worker`. Never block the event loop with synchronous network calls or heavy CPU loops without offloading.

* **Observability:** Emit structured JSON logs via `structlog` with correlation/run IDs. Never use `print()` in application code. Expose Prometheus metrics on `GET /metrics` (latency, error rates, quarantine counts, active model version).

* **Secrets & Config:** Read all credentials strictly from environment variables. Whenever adding a new env var, immediately document a placeholder in `.env.example`.

### 5.2 Data Engineering & Lakehouse (`pipelines/ingestion`, `pipelines/dbt`)

* **Schema Contracts & Quarantine/DLQ:** Every ingestion source must validate payloads using **Pandera** before writing to Bronze. Malformed rows must be written to `quarantine/{source}/dt=YYYY-MM-DD/` with failure metadata—**never silently drop bad rows**.

* **Idempotency & Watermarks:** All Bronze writes, Silver/Gold transforms, and Neon Postgres syncs must be safe to re-run multiple times without creating duplicate rows. Record every run in `pipeline_runs`.

* **Scraper Hygiene:** Respect IDX endpoints with jittered backoff, timeouts, `curl_cffi` primary / Playwright fallback, and deterministic offline fixtures for unit tests (CI tests must **never** depend on live IDX scraping).

* **SQL & DuckDB Optimization:** Maintain Hive partitioning on R2 (`dt=YYYY-MM-DD`, `fiscal_year=YYYY`), include dbt generic/singular tests (`unique`, `not_null`, value ranges), and keep Neon queries backed by indexes in `infra/sql/ddl.sql`.

### 5.3 Machine Learning & MLOps (`pipelines/ml`, `apps/api/app/ml`)

* **Zero Data Leakage:** Always use time-based splits (strictly respecting `filing_date` / `as_of_date`) when training or evaluating financial distress (`v1`) and dividend sustainability (`v1.5`) models.

* **Reproducibility & Registry:** Log hyperparameters, dataset watermarks, PR-AUC, ROC-AUC, Brier score, and artifacts to **MLflow** (artifacts in R2). Promote `Staging -> Production` only if evaluation metrics beat or match the active baseline.

* **Bilingual Explainability:** Every distress prediction must return Top-3 SHAP drivers in both English (`shap_top3_en`) and Bahasa Indonesia (`shap_top3_id`).

* **Sector Awareness:** Account for Indonesian banking issuers (e.g., `BBCA`, `BBRI`, `BMRI`, `BBNI`) where structural leverage differs from non-bank corporates when computing Altman Z-Score and leverage features.

### 5.4 Payments, Auth & Security (`apps/api`, `apps/worker`)

* **Mayar.id QRIS Webhooks:** Always verify HMAC/webhook signature headers and enforce strict idempotency on `payment_transactions.external_tx_id` so retried webhooks never double-extend `pro_expires_at`.

* **Telegram Bot (`@ArthalaIDBot`):** Enforce single-use, short-TTL (`15 min`) account linking tokens and strictly separate `FREE` (EOD summary) vs. `PRO` (real-time Kafka alerts) dispatch logic.

### 5.5 Frontend & UX (`apps/web`)

* **Stack:** Next.js App Router, strict TypeScript, Tailwind CSS, and **TradingView Lightweight Charts**.

* **Bilingual First:** Keep all copy in `src/messages/en.json` and `src/messages/id.json` (no hardcoded user-facing strings). Default language is English (`en`) with an instant `EN | ID` switcher persisted in `localStorage`.

* **Brand Consistency:** Display **Arthala ID** and subtle founder attribution (**by Robit Al Hazmi**) in the header/footer across all v1 screens (`/`, `/ticker/[symbol]`, `/watchlist`, `/status`).

## 6. Antigravity Agent Workflow & Verification Protocol

Follow this loop for every feature or bugfix:

1. **Plan First:** Read the target checkbox in `docs/Task.md` and relevant sections of `docs/Implementation_Plan.md`. Outline affected files and acceptance criteria before writing code.

2. **Branch Hygiene:** Work on a short-lived branch named `<type>/<short-scope>` (e.g., `feat/phase-1-pandera-contracts`, `fix/mayar-webhook-idempotency`). Never commit directly to `main`.

3. **Implement Cleanly & Minimally:** Keep diffs scoped to the active task. Write unit/integration tests alongside new modules (mocking external HTTP, Kafka, R2, and Mayar calls).

4. **Verify Before Reporting Done:**

   * Python changes: `ruff check`, `ruff format --check`, `mypy`, and `pytest` must all pass.

   * dbt changes: `dbt parse` and `dbt test --target local` must pass.

   * Web changes: `npm run lint`, `npm run typecheck`, `npm run build` must pass, and verify rendered UI flows (desktop + mobile, `EN` + `ID` toggle) using **Antigravity's built-in Chrome browser automation** when the dev server is running.

5. **Update Tracker & Commit:** Check off completed items in `docs/Task.md` and use **Conventional Commits** (`feat(ml): ...`, `fix(api): ...`, `ci(etl): ...`, `docs: ...`).

## 7. Escalation Rules (Stop & Ask the Human)

Pause and ask Robit before proceeding if a task requires:

* Adding a new cloud service, paid dependency, or heavy runtime container that risks exceeding the **2 GB VPS RAM budget**.

* Breaking changes to `infra/sql/ddl.sql` primary keys or Cloudflare R2 partition paths.

* Altering Mayar.id pricing/webhook contract assumptions or exposing new public unauthenticated write endpoints.

* Deleting existing migration files, historical training datasets, or production configuration.

## 8. Available Antigravity Slash Commands (`.agents/workflows/`)

Trigger these reusable workflows directly from the Antigravity IDE chat input:

* `/next-task` — Picks the next unchecked item in `docs/Task.md`, creates a feature branch, implements with tests, runs quality gates, and updates `docs/Task.md`.
* `/verify-all` — Runs the complete CI quality suite (`ruff`, `mypy`, `pytest`, `dbt test`, Next.js build, and Docker Compose validation).
* `/run-etl-local` — Boots local Postgres, MinIO, and Redpanda and verifies the full Bronze -> Silver -> Gold -> Postgres pipeline safely offline.
* `/pr-ready` — Audits your branch diff against the 2 GB VPS RAM budget, security, i18n, and MLOps guardrails, then drafts a Conventional Commit PR description.
