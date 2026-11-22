# Arthala ID — Developer, Deployment & Interview Walkthrough

> **Brand:** Arthala ID — *by Robit Al Hazmi* (`arthala-id` / `@ArthalaIDBot`)

This guide walks you step by step from an empty folder in **Antigravity IDE** to a live, monetizable IDX intelligence platform deployed on your **1 vCPU / 2 GB Coolify VPS** and free cloud tiers, followed by a **Bank OCBC NISP 3-Minute Interview Demo Script**.

---

## Step 1: Local Machine & Antigravity IDE Setup

### 1.1 Prerequisites
Install the following free open-source tools on your laptop before opening Antigravity IDE:
- **Git** + **GitHub CLI (`gh`)**
- **uv** (fast Python package & workspace manager): `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Node.js 22 LTS** (for Next.js `apps/web`)
- **Docker Desktop** (or Rancher Desktop / OrbStack) for local Postgres, MinIO, and Redpanda
- **Just** (task runner for clean commands): `brew install just` or via your OS package manager

### 1.2 Initialize Repository with GitHub Best Practices
1. Create your repository and clone it into Antigravity IDE:
   ```bash
   gh repo create arthala-id --public --description "Arthala ID by Robit Al Hazmi — IDX Fundamental, Dividend & MLOps Intelligence Platform"
   git clone https://github.com/<your-username>/arthala-id.git
   cd arthala-id
   ```
2. Place the three planning documents into `/docs` (`Implementation_Plan.md`, `Task.md`, `Walkthrough.md`) so Antigravity IDE's agent has complete architectural context on every prompt.
3. Configure GitHub Branch Protection on `main` (**Settings -> Branches -> Add rule**):
   - Require a pull request before merging.
   - Require status checks to pass (`python-ci`, `dbt-ci`, `web-ci`).
   - Allow squash merging only; enable automatic deletion of head branches.
4. Initialize Python tooling and pre-commit hooks:
   ```bash
   uv init --python 3.12
   uv add --dev ruff mypy pytest pytest-cov pre-commit
   uv run pre-commit install
   ```

### 1.3 Start the Offline Local Sandbox (`infra/docker/docker-compose.local.yml`)
Run all cloud dependencies locally so you can iterate rapidly without touching production quotas:
- **Postgres 16** on `localhost:5432` (emulates Neon)
- **MinIO** on `localhost:9000` / console `:9001` (emulates Cloudflare R2 S3 API)
- **Redpanda** on `localhost:9092` / console `:8080` (single-binary Kafka-compatible broker, emulates Aiven Kafka)

Use standardized commands in your `Justfile`:
```makefile
# Justfile - run from Antigravity IDE terminal
up:
    docker compose -f infra/docker/docker-compose.local.yml up -d

db-init:
    psql "$DATABASE_URL" -f infra/sql/ddl.sql

etl-local:
    uv run python -m pipelines.ingestion.runner --env local
    cd pipelines/dbt && uv run dbt run --target local && uv run dbt test --target local
    uv run python -m pipelines.ingestion.sync_neon --env local

ml-train:
    uv run python -m pipelines.ml.train --env local

api:
    uv run uvicorn apps.api.app.main:app --reload --port 8000

worker:
    uv run python -m apps.worker.worker.main

web:
    cd apps/web && npm run dev

replay:
    uv run python -m pipelines.replay.producer --speed 10 --tickers BBCA,BBRI,ASII,TLKM

check:
    uv run ruff check .
    uv run mypy apps/api apps/worker pipelines
    uv run pytest
```

> **Antigravity IDE Productivity Tip:** Work one checkbox at a time from `docs/Task.md`. Start each session by telling Antigravity: *"Read `docs/Implementation_Plan.md` and `docs/Task.md`. Let's implement Task 1.1 on a new branch `feat/pandera-contracts` and write unit tests."*

---

## Step 2: Provision Zero-Cost Cloud Services

Configure each free tier and store all secrets in **GitHub Repository Secrets** (for Actions ETL/ML) and **Coolify Environment Variables** (for `arthala-api` and `arthala-worker`):

1. **Cloudflare R2 (Lakehouse & ML Artifacts):**
   - Create bucket `arthala-lakehouse`.
   - Create an R2 API Token with Object Read & Write permissions.
   - Save `R2_ENDPOINT_URL`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET=arthala-lakehouse`.
2. **Neon Postgres (OLTP & Serving):**
   - Create project `arthala-prod` (Singapore `ap-southeast-1` region for lowest latency to Indonesia).
   - Run `infra/sql/ddl.sql` against the pooled connection string.
   - Save `DATABASE_URL` (with `sslmode=require`).
3. **Aiven for Apache Kafka (Free Tier Streaming):**
   - Create Free Kafka service in `ap-southeast-1` (Singapore).
   - Create topics: `idx.ticks.raw`, `idx.disclosures.raw`, `alerts.dispatch`, `user.subscription.updated`.
   - Download/store SSL credentials (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SSL_CA`, `KAFKA_SSL_CERT`, `KAFKA_SSL_KEY`).
4. **Telegram Bot (`@BotFather`):**
   - Message `@BotFather` -> `/newbot` -> set name `Arthala ID` and handle `@ArthalaIDBot`.
   - Set bot description to *"Arthala ID by Robit Al Hazmi — Real-time IDX fundamental, dividend & anomaly alerts."*
   - Save `TELEGRAM_BOT_TOKEN`.
5. **Mayar.id (QRIS Monetization):**
   - Sign up as an individual Indonesian merchant (KTP verification).
   - Create a recurring/monthly product: **Arthala ID Pro (Rp39.000 / bulan)**.
   - Set Webhook URL to `https://api.<your-domain>/v1/webhooks/mayar` and save `MAYAR_WEBHOOK_SECRET` and `MAYAR_PRO_PAYMENT_URL`.
6. **Grafana Cloud (Free Observability):**
   - Create a free Grafana Cloud stack (Singapore/APAC region).
   - Generate a Prometheus Remote Write token and Loki Logs token for Grafana Alloy.

---

## Step 3: Prepare Your 1 vCPU / 2 GB Coolify VPS

Before deploying any container, protect the VPS from OOM spikes by adding a 2 GB swap file:

```bash
# SSH into your Coolify VPS
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
echo 'vm.swappiness=20' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
free -h
```

### Deploy Backend Services via Coolify
1. In Coolify, create a new Project **Arthala ID** -> **Production** -> Add Resource -> **Docker Compose** linked to your `arthala-id` GitHub repo pointing to `infra/coolify/docker-compose.prod.yml`.
2. Ensure memory caps are enforced in `docker-compose.prod.yml`:
   - `arthala-api`: `mem_limit: 450m`, `--workers 1` on Uvicorn.
   - `arthala-worker`: `mem_limit: 250m`.
   - `grafana-alloy`: `mem_limit: 100m`.
3. Assign your API domain (e.g., `api.arthala.id` or Coolify `sslip.io` HTTPS URL for MVP) to the `arthala-api` service on port `8000` with health check `/health`.
4. Add all production environment variables inside Coolify's Secrets tab and enable **Auto Deploy on push to `main`**.

### Deploy Next.js Frontend (0 MiB VPS RAM)
1. Connect your `arthala-id` GitHub repo to **Cloudflare Pages** (or **Vercel Free Tier**).
2. Set Root Directory to `apps/web`.
3. Set environment variables:
   - `NEXT_PUBLIC_API_URL=https://api.<your-domain>`
   - `NEXT_PUBLIC_MAYAR_PRO_URL=<your-mayar-payment-link>`
   - `NEXT_PUBLIC_TELEGRAM_BOT_USERNAME=ArthalaIDBot`
4. Deploy and confirm CORS on FastAPI allows your exact frontend origin.

---

## Step 4: End-to-End Verification Runbook

Before sharing **Arthala ID** publicly or attaching it to your Bank OCBC NISP application, run these 6 checks:

1. **ETL & Data Quality Proof:** Manually trigger `.github/workflows/etl_daily.yml`. Confirm Bronze/Silver/Gold Parquet files appear in `arthala-lakehouse` on R2, `dbt test` passes, intentional malformed test fixtures route to `quarantine/`, and `mart_screener_latest` updates in Neon.
2. **MLOps Proof:** Trigger `.github/workflows/ml_retrain.yml`. Confirm MLflow logs PR-AUC/ROC-AUC metrics, registers the model to R2, updates SHAP EN/ID reasons in Neon, and generates an Evidently drift report.
3. **API & Live Inference Proof:**
   ```bash
   curl -s https://api.<your-domain>/health
   curl -s "https://api.<your-domain>/v1/screener?limit=5&sort=dividend_yield_pct"
   curl -s https://api.<your-domain>/v1/tickers/BBCA
   ```
4. **24/7 Kafka Streaming & Telegram Alert Proof:** Trigger `.github/workflows/replay_demo.yml` from GitHub Actions. Watch `arthala-worker` consume replayed IDX ticks from Aiven Kafka and deliver a formatted alert via `@ArthalaIDBot` within 2 seconds.
5. **Mayar.id QRIS Webhook Proof:** Send a signed test webhook payload (or complete a live Rp1.000 test payment link) to `POST /v1/webhooks/mayar`. Verify idempotency (re-sending the same `external_tx_id` does not double-extend expiry) and confirm `@ArthalaIDBot` sends the Pro upgrade confirmation.
6. **VPS Memory Audit:** Run `docker stats --no-stream` on your Coolify VPS and verify total host memory stays comfortably under `1.3 GB / 2.0 GB`.

---

## Step 5: Bank OCBC NISP 3-Minute Interview Demo Script

When an OCBC hiring manager or Senior Data Engineer asks you to walk through a production system you built, follow this exact sequence:

1. **The Systems Problem (30 sec):** *"I built **Arthala ID**, a live Indonesian stock intelligence and anomaly alerting platform engineered under a strict zero-cost cloud-tier and 2 GB VPS constraint to prove real cost efficiency, data reliability, and end-to-end ownership."*
2. **Medallion Lakehouse & Data Quality (45 sec):** Open `/status` on the live site and your GitHub Actions ETL run. Show how multi-source IDX ingestion enforces **Pandera schema contracts** with a **Quarantine/DLQ on Cloudflare R2**, transforms Bronze -> Silver -> Gold via **DuckDB + dbt-duckdb**, and syncs serving marts to **Neon Postgres**.
3. **Production MLOps & Explainability (45 sec):** Open `/ticker/BBCA` (and a high-risk Notasi Khusus stock). Show the **LightGBM Financial Distress & Accounting Anomaly Score** (Altman Z, Beneish M, Accruals Ratio) with bilingual **SHAP Top-3 explanations**, backed by **MLflow** versioning and **Evidently AI** drift monitoring—explicitly connecting it to OCBC's credit scoring and fraud detection workloads.
4. **Real-Time Streaming & Production Engineering (45 sec):** Click **Run Workflow** on `replay_demo.yml` to push real historical IDX events into **Aiven Kafka**, show the live alert arriving from `@ArthalaIDBot` via the async Coolify worker, and open **Grafana Cloud** to show container RAM, API p95 latency, and Kafka consumer metrics.
5. **Enterprise Scalability Bridge (15 sec):** Show `/infra/k8s` and `/infra/terraform` and explain: *"Live production runs lean on Coolify and free cloud tiers to keep burn at zero, while the exact same containers and DAGs are packaged with Helm/k3s and Airflow/Terraform for horizontal enterprise scale like OCBC."*
