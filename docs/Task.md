# Arthala ID — Engineering Task Tracker

> **Brand:** Arthala ID — *by Robit Al Hazmi* (`arthala-id` / `@ArthalaIDBot`)  
> **Workflow Convention:** Create one short-lived feature branch per milestone or sub-task (`feat/phase-1-ingestion`, `feat/phase-2-mlops`), open a PR into `main`, verify green GitHub Actions CI, and squash-merge.

---

## Phase 0: Repository Governance, Cloud Free Tiers & Antigravity Local Dev
- [x] **0.1 GitHub Repo Bootstrap:** Create public repository `arthala-id`, configure `main` branch protection rules, squash-merge only, `CODEOWNERS`, `PULL_REQUEST_TEMPLATE.md`, `ISSUE_TEMPLATE/`, and `dependabot.yml`.
- [x] **0.2 Monorepo Skeleton & Tooling:** Initialize `uv` Python 3.12 workspace (`pyproject.toml`), `ruff`, `mypy`, `pytest`, `.pre-commit-config.yaml`, `Justfile`, and `.env.example`.
- [x] **0.3 Local Sandbox Compose:** Create `infra/docker/docker-compose.local.yml` with local Postgres 16, MinIO (S3/R2 emulator), and Redpanda (single-node Kafka emulator) so all development works offline in Antigravity IDE.
- [ ] **0.4 Provision Free Cloud Services (100% No-Credit-Card Free Tiers):**
  - *Progress:* automated provisioning & verification runner (`pipelines/ingestion/cloud_bootstrap.py` + `just cloud-check`) implemented to create/verify `arthala-lakehouse` S3 bucket (Backblaze B2 / Supabase S3 / R2), apply `infra/sql/ddl.sql` to Neon Postgres via `psycopg` (no host `psql` needed), verify Aiven Kafka SSL/topics, Grafana Cloud remote-write endpoints, Telegram `@ArthalaIDBot` `getMe`, and Mayar.id QRIS webhook settings; pending live cloud account tokens in `.env` / GitHub Secrets.
  - [x] S3-Compatible Lakehouse Bucket (`arthala-lakehouse`, private, `s3.us-east-005.backblazeb2.com`) + S3 Application Keys via **Backblaze B2 Free (10 GB, no credit card)** — verified live & stored in GitHub Actions Secrets (`R2_*`).
  - [x] **Neon Postgres Free** project (`falling-brook-19498331`, branch `production`, no credit card) + executed `infra/sql/ddl.sql` via `DATABASE_URL_UNPOOLED` and verified all 8 serving tables over pooled `DATABASE_URL` (`just cloud-check prod`, stored in GitHub Actions Secrets).
  - [x] **Aiven for Apache Kafka Free** service (`arthala-kafka-arthala-id.h.aivencloud.com:26713`, mTLS `SSL`, no credit card) + verified all 4 topics x 2 partitions (`idx.ticks.raw`, `idx.disclosures.raw`, `alerts.dispatch`, `user.subscription.updated`) and stored credentials in GitHub Actions Secrets (`KAFKA_*`).
  - [x] **Grafana Cloud Free** stack (`prometheus-prod-52-prod-ap-southeast-2.grafana.net` & `logs-prod-032.grafana.net/loki/api/v1/push`, no credit card) + verified live Loki `204` log push & Prometheus remote-write auth and stored credentials in GitHub Actions Secrets (`GRAFANA_*`).
  - [x] **Telegram Bot** via `@BotFather` (`@ArthalaIDBot`, no credit card) + verified live via Telegram `getMe`, configured bot description/commands, and stored `TELEGRAM_BOT_TOKEN` in GitHub Actions Secrets.
  - [ ] **Mayar.id** individual account (no credit card; Indonesian KTP/local bank) + create **Arthala ID Pro** monthly Payment Link (Rp39.000) + webhook secret.
- [ ] **0.5 Harden Coolify VPS (1 vCPU / 2 GB):** Enable 2 GB Linux swap file, verify Coolify dashboard health, and configure GitHub App deployment link.
- [x] **0.6 Baseline CI Pipeline:** Add `.github/workflows/ci.yml` (Python lint/type/test + Next.js lint/build + gitleaks) and verify first green PR.

---

## Phase 1: Resilient Ingestion & Medallion Lakehouse (R2 + DuckDB + dbt)
- [x] **1.1 Pandera Data Contracts & DLQ:** Implement `pipelines/ingestion/contracts.py` (`PriceSchema`, `FinancialReportSchema`, `CorporateActionSchema`) with automatic Quarantine/DLQ writer to `r2://arthala-lakehouse/quarantine/`.
- [ ] **1.2 Multi-Source IDX & Market Scrapers:**
  - [ ] EOD OHLCV + dividends collector (`yfinance` for `.JK` tickers with batching and retry backoff).
  - [ ] IDX quarterly financials, disclosures & Notasi Khusus (FCA) collector (`curl_cffi` primary + Playwright fallback).
  - [ ] Watermark tracking & idempotent writes to Bronze Parquet on R2 (`pipelines/ingestion/runner.py`).
- [ ] **1.3 dbt-duckdb Silver & Gold Models:**
  - [ ] Configure `pipelines/dbt/profiles.yml` for local MinIO and production Cloudflare R2 via DuckDB `httpfs`.
  - [ ] Silver models: `dim_tickers`, `fct_daily_prices`, `fct_quarterly_financials`, `fct_corporate_actions`.
  - [ ] Gold feature store (`feat_distress_anomaly`): annualized EPS, PER, PBV, ROE, DER, YoY growth, Accruals Ratio, Altman Z-Score, Beneish M-Score, Short-Term Debt Pressure, and sector percentiles.
  - [ ] Add dbt schema tests (`unique`, `not_null`, `accepted_values`, range checks).
- [ ] **1.4 Gold-to-Neon Sync Script:** Write idempotent Postgres bulk upsert job (`pipelines/ingestion/sync_neon.py`) populating `dim_tickers`, `fct_quarterly_financials`, `fct_dividend_calendar`, `mart_screener_latest`, and `pipeline_runs`.
- [ ] **1.5 Automate Daily ETL:** Create `.github/workflows/etl_daily.yml` running Mon-Fri at 18:30 WIB (post-IDX close) and manual `workflow_dispatch`.

---

## Phase 2: v1 Flagship ML & Full MLOps Pipeline (Distress & Anomaly Score)
- [ ] **2.1 Ground-Truth Label Builder:** Construct historical training dataset labeling IDX Notasi Khusus / FCA, delistings, and severe earnings/cash-flow collapses from Bronze/Silver history.
- [ ] **2.2 LightGBM Training & MLflow Tracking:** Implement `pipelines/ml/train.py` using time-based cross-validation (no lookahead leakage), class-imbalance handling, PR-AUC / ROC-AUC / Brier metrics, logging parameters and artifacts to MLflow (backed by R2).
- [ ] **2.3 Bilingual SHAP Explainability Engine:** Build `pipelines/ml/explain.py` computing `TreeExplainer` values and mapping Top-3 drivers to structured English (`shap_top3_en`) and Bahasa Indonesia (`shap_top3_id`) explanations.
- [ ] **2.4 Model Promotion Gate & Batch Scorer:** Automate `Staging` -> `Production` promotion only if candidate PR-AUC beats current production baseline; run daily batch scoring to update `mart_screener_latest`.
- [ ] **2.5 Evidently AI Drift Monitoring:** Implement `pipelines/ml/monitor.py` comparing latest quarterly feature distributions against training baseline, storing HTML/JSON reports in R2, updating `pipeline_runs.drift_detected`, and opening a GitHub Issue on drift breach.
- [ ] **2.6 Weekly Retraining Workflow:** Create `.github/workflows/ml_retrain.yml` running weekly + triggered on drift alerts.

---

## Phase 3: Production Microservices (FastAPI, Kafka Worker, Telegram & Mayar QRIS)
- [ ] **3.1 FastAPI Core & Endpoints (`apps/api`):**
  - [ ] `GET /health` & `GET /metrics` (Prometheus metrics via `prometheus-fastapi-instrumentator`).
  - [ ] `GET /v1/screener` (filtering, sorting, pagination backed by Neon indexes).
  - [ ] `GET /v1/tickers/{symbol}` (price series, quarterly financials, ratios, SHAP Top-3 EN/ID).
  - [ ] `POST /v1/predict/distress` (live on-demand LightGBM + SHAP inference endpoint for production API serving proof).
  - [ ] `GET /v1/watchlist`, `POST /v1/watchlist`, `DELETE /v1/watchlist/{symbol}` + `GET /v1/dividends/calendar`.
  - [ ] `GET /v1/status` (pipeline runs, quarantine counts, active MLflow model version, drift status).
- [ ] **3.2 Mayar.id QRIS Webhook & Subscription State Machine:**
  - [ ] Implement `POST /v1/webhooks/mayar` with signature verification and `external_tx_id` idempotency guard.
  - [ ] Upgrade user `plan_tier` to `PRO` (+30 days), write audit record to `payment_transactions`, and publish `user.subscription.updated` event to Aiven Kafka.
- [ ] **3.3 24/7 Historical IDX Kafka Replay Producer (`pipelines/replay`):**
  - [ ] Build CLI producer that streams real historical IDX OHLCV ticks and disclosure payloads into Aiven Kafka (`idx.ticks.raw`, `idx.disclosures.raw`) at configurable speed.
  - [ ] Add `.github/workflows/replay_demo.yml` for one-click recruiter demo execution.
- [ ] **3.4 Async Kafka Consumer & `@ArthalaIDBot` (`apps/worker`):**
  - [ ] Implement `/start <link_token>`, `/status`, `/watchlist`, `/score <TICKER>` Telegram commands.
  - [ ] Build `aiokafka` consumer evaluating streaming volume/price spikes and disclosure events against user watchlists, enforcing Free vs. Pro dispatch rules, and sending formatted Telegram alerts.
- [ ] **3.5 Containerization & Tests:** Write multi-stage slim Python 3.12 Dockerfiles for `apps/api` and `apps/worker` plus `pytest` unit & integration tests (mocked Kafka, mocked Mayar webhook, FastAPI `TestClient`).

---

## Phase 4: Bilingual Next.js Web App (Cloudflare Pages / Vercel)
- [ ] **4.1 Next.js App Bootstrap (`apps/web`):** Set up Next.js App Router, TypeScript, Tailwind CSS, bilingual `EN | ID` toggle (defaulting to `EN`), **Arthala ID by Robit Al Hazmi** branding/footer, and API client.
- [ ] **4.2 Public Stock Screener (`/`):** Build filter sidebar (Sector, PER, PBV, ROE, DER, Yield, Risk Tier), sortable data table, and responsive mobile cards.
- [ ] **4.3 Ticker Deep-Dive (`/ticker/[symbol]`):** Integrate **TradingView Lightweight Charts** (candlestick + volume), quarterly fundamental charts, Altman Z / Beneish M cards, and the Top-3 SHAP Explainability panel (EN/ID).
- [ ] **4.4 Personal Watchlist & Dividend Calendar (`/watchlist`):** Add user sign-in, watchlist toggle, upcoming cum-date calendar, `@ArthalaIDBot` one-click linking flow, and Mayar.id QRIS Pro upgrade CTA.
- [ ] **4.5 Public System Health Dashboard (`/status`):** Display live ETL freshness, quarantine counts, Kafka replay heartbeat, MLflow model version, and Evidently drift badge.
- [ ] **4.6 Deploy Frontend:** Connect `apps/web` to Cloudflare Pages or Vercel Free Tier with custom/free subdomain and production `NEXT_PUBLIC_API_URL`.

---

## Phase 5: Coolify Production Deploy, Observability & Enterprise IaC Showcase
- [ ] **5.1 Production Coolify Deployment:** Configure `infra/coolify/docker-compose.prod.yml` with strict memory limits (`arthala-api: 450m`, `arthala-worker: 250m`, `grafana-alloy: 100m`), deploy via Coolify with Traefik HTTPS, and verify `< 1.1 GB` total VPS RAM usage under load.
- [ ] **5.2 Grafana Cloud Observability:** Configure Grafana Alloy to ship FastAPI latency/error rates, Kafka consumer lag, and container memory metrics to Grafana Cloud; create a 1-page SRE dashboard.
- [ ] **5.3 Enterprise Infrastructure Showcase (`/infra` & `/pipelines/airflow`):**
  - [ ] Write Terraform configs (`infra/terraform/`) documenting Cloudflare R2, Neon, and Aiven resources.
  - [ ] Write Helm / k3s manifests (`infra/k8s/`) with HPA and resource quotas for `arthala-api` and `arthala-worker`.
  - [ ] Add portable Airflow DAGs (`pipelines/airflow/`) mirroring the GitHub Actions ETL/ML orchestration.
- [ ] **5.4 Portfolio Polish:** Write a crisp `README.md` for **Arthala ID by Robit Al Hazmi** featuring Mermaid architecture diagram, live links, Engineering Traceability Matrix, screenshots, and a 3-minute demo video/GIF.

---

## Phase 6: Post-MVP Expansion (v1.5 & v2)
- [ ] **6.1 v1.5 Dividend Sustainability Predictor:** Train LightGBM classifier predicting $> 25\%$ dividend cut probability using Gold features + payout history; surface on `/watchlist` calendar and `@ArthalaIDBot` alerts.
- [ ] **6.2 v1.5 Streaming Anomaly & Disclosure Classifier:** Upgrade `apps/worker` with online Z-score / Isolation Forest tick anomaly detection and Indonesian IDX disclosure keyword/NLP urgency classification.
- [ ] **6.3 v2 Personalized Stock Screener & Portfolio Risk Recommender:** Analyze user watchlist sector concentration & risk profile to recommend diversifying IDX dividend/value candidates.
- [ ] **6.4 v2 Pro Developer API Tier:** Add scoped API keys, rate limiting, and usage metering on top of existing FastAPI endpoints.
