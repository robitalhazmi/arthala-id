# Arthala ID — Implementation Plan & Architecture Blueprint

> **Product Brand:** **Arthala ID** — *by Robit Al Hazmi* (`arthala.id` / repo: `arthala-id` / bot: `@ArthalaIDBot`)  
> **Brand Positioning:** Combines an institutional, Indonesian-native fintech identity (*Artha* = wealth/capital, echoing the clean cadence of *Kanala ID*) with transparent, founder-led authority (*by Robit Al Hazmi*, echoing Indonesian retail-investing leaders such as *Rivan Kurniawan*, *Leon Hartono*, *Jonathan Thamrin*, *Johan Jascha*, and *Sudarsan Glory*). Zero collision with existing IDX platforms, trustworthy for retail subscribers, and sharp on a **Bank OCBC NISP** engineering portfolio.

---

## 1. Executive Summary & Product Strategy

**Arthala ID by Robit Al Hazmi** is an open-source, cloud-native **All-in-One IDX Intelligence Platform** designed to satisfy three goals simultaneously:
1. **Personal Investing Workflow:** Automated IDX quarterly financial report ingestion, annualized ratios, dividend sustainability tracking, and real-time red-flag / anomaly alerts.
2. **Bank OCBC NISP Data Engineer Portfolio:** Production-grade Medallion Lakehouse, real-time Kafka streaming, versioned and monitored MLOps (LightGBM + SHAP + MLflow + Evidently), data quality contracts, CI/CD, and Infrastructure-as-Code.
3. **Public Monetization (Indonesia-First):** A **Freemium Web Dashboard** (SEO & investor-community funnel) paired with a **Paid Real-Time Telegram Alert Bot (Rp29.000 - Rp49.000/month via Mayar.id QRIS)**.

### Tiered Feature & Monetization Model

| Feature | Free Tier (Public Web + Basic Bot) | Pro Tier (Rp39.000/mo via Mayar QRIS) |
|---|---|---|
| Public IDX Screener (PER, PBV, ROE, DER, Yield) | Full access (EOD delayed) | Full access + advanced filters |
| Ticker Deep-Dive (Lightweight Charts + Fundamentals) | Full access | Full access + 5-year quarterly history |
| v1 ML Financial Distress & Anomaly Score | Traffic-light badge (Safe / Watch / Risk) | Exact probability score + Top-3 SHAP reasons (EN/ID) |
| Personal Watchlist & Dividend Cum-Date Calendar | Up to 5 tickers | Unlimited tickers |
| Real-Time Telegram Alerts (Disclosures, Anomalies, Payout Risk) | Daily EOD summary only | Instant real-time Kafka-driven alerts |
| Developer API Key (Planned v2) | - | Rate-limited REST API access |

---

## 2. Finalized Zero-Cost Market-Leader Tech Stack

Every component uses a free open-source market leader or a generous forever-free cloud tier, engineered around a **100% Python + SQL + DuckDB** data/ML core and a **Next.js** web UI.

| Layer | Technology | Hosting / Tier | Why This Choice |
|---|---|---|---|
| **Frontend Web App** | Next.js (App Router, React, TypeScript, Tailwind CSS, TradingView Lightweight Charts) | **Cloudflare Pages** or **Vercel** (Free Tier) | Polished SaaS UX with **0 MiB RAM impact** on your Coolify VPS. |
| **Backend API & ML Serving** | Python 3.12 (`uv`), **FastAPI**, Uvicorn, Pydantic v2, LightGBM, SHAP | **Coolify VPS** (`arthala-api` container) | Fast async REST API with native ML inference and OpenAPI docs. |
| **Streaming & Alert Worker** | **Aiven for Apache Kafka** (Free Tier) + Python `aiokafka` + `python-telegram-bot` | **Aiven Free** + **Coolify VPS** (`arthala-worker` container) | Real managed Kafka broker off-VPS; lightweight async consumer on VPS. |
| **Lakehouse Storage** | **Cloudflare R2** (S3-compatible Parquet: Bronze / Silver / Gold / Quarantine / ML Artifacts) | **Cloudflare R2** (10 GB free, zero egress) | Zero egress fees make R2 ideal for DuckDB remote reads and CI/CD ETL. |
| **Batch Compute & Transform** | **DuckDB** + **dbt-duckdb** + **Pandera** (Data Quality) | **GitHub Actions** (Scheduled Cron Runners) | Offloads heavy batch ETL & ML training completely from the 2 GB VPS. |
| **Serving Database (OLTP)** | **Neon Serverless Postgres** (Free Tier, 100 CU-hrs/mo) | **Neon Free** | Serves low-latency web queries, auth, watchlists, alert rules, and Mayar billing state. |
| **MLOps & Drift Monitoring** | **MLflow** (artifacts on R2, metadata on Neon) + **Evidently AI** | **GitHub Actions** + R2/Neon | Full experiment tracking, model registry, automated retraining, and data drift reports. |
| **Payments (QRIS)** | **Mayar.id** Payment Links + HMAC-verified Webhooks | **Mayar.id** (Individual KTP friendly) | Fastest Indonesian solo-founder path to live QRIS subscriptions without a PT/CV. |
| **Observability** | Prometheus metrics (`/metrics`), structured JSON logs (`structlog`), **Grafana Cloud** (Free Tier) | **Grafana Cloud Free** + lightweight VPS exporter | Enterprise-grade dashboards & alerts without running Grafana/Prometheus storage on VPS. |
| **Enterprise IaC Showcase** | Docker Compose, Coolify manifests, **Kubernetes (k3s/Helm)** charts, **Terraform**, Airflow DAG blueprints | `/infra` & `/pipelines/airflow` in GitHub | Proves OCBC K8s/Airflow/IaC mastery cleanly alongside lean live Coolify deployment. |

---

## 3. VPS Resource Budget (1 vCPU / 2 GB RAM + Coolify)

To prevent Out-Of-Memory (OOM) kills on a 2 GB VPS, **2 GB of Linux swap** must be enabled on the host, Next.js runs off-VPS, and every Coolify container enforces strict Docker memory limits.

| Component | Deployment Host | Docker Mem Limit | Expected RAM | Notes |
|---|---|---|---|---|
| Host OS + Docker + Coolify + Traefik | Coolify VPS | Host-managed | ~500 MiB | Baseline Coolify footprint |
| `arthala-api` (FastAPI + LightGBM/SHAP) | Coolify VPS | `450m` | ~300-350 MiB | 1 Uvicorn worker; models loaded once at startup into memory |
| `arthala-worker` (Kafka Consumer + Telegram Bot) | Coolify VPS | `250m` | ~180-200 MiB | Async `aiokafka` + Telegram webhook/long-polling + alert rules |
| `grafana-alloy` (Metrics/Log Shipper) | Coolify VPS | `100m` | ~70-80 MiB | Scrapes `/metrics` and pushes to Grafana Cloud Free |
| **Rolling Deploy & Spike Headroom** | **Coolify VPS** | - | **~900 MiB free** | Safe zero-downtime restarts in Coolify |
| Next.js Web, Neon DB, R2, Aiven Kafka, GH Actions | Free Cloud Tiers | - | **0 MiB on VPS** | Zero contention with live inference |

---

## 4. OCBC NISP Job-Requirement Traceability Matrix

Use this matrix in your GitHub `README.md` and during technical interviews to map every job requirement directly to verifiable code in the `arthala-id` repository.

| Bank OCBC NISP Job Requirement | Arthala ID Implementation | Repository Location |
|---|---|---|
| **Scalable, low-latency ETL/ELT pipelines (Kafka, Spark/Airflow/Flink equivalents)** | Medallion Lakehouse (R2 Bronze/Silver/Gold via DuckDB + dbt-duckdb) orchestrated via GitHub Actions + portable Airflow DAGs; real-time Kafka streaming via Aiven + `aiokafka` + 24/7 Historical Market Replay Producer. | `/pipelines/ingestion`, `/pipelines/dbt`, `/pipelines/replay`, `/pipelines/airflow` |
| **Integrate ML models into production via APIs, microservices & containers** | Versioned LightGBM + SHAP models loaded from MLflow/R2 into containerized FastAPI inference microservice (`POST /v1/predict/distress`, `GET /v1/tickers/{symbol}/score`). | `/apps/api/app/ml`, `/infra/docker` |
| **Real-time & batch inference for credit scoring, fraud detection & personalization** | **v1:** Financial Distress & Accounting Anomaly Score (maps 1:1 to credit/fraud scoring). **v1.5:** Dividend Sustainability + Kafka streaming price/volume anomaly detection. **v2:** Personalized portfolio risk screener. | `/pipelines/ml`, `/apps/worker` |
| **Convert DS prototypes into monitored production services with versioning & auto-retraining** | MLflow Model Registry (`Staging` -> `Production` promotion gate), scheduled weekly GitHub Actions retraining, Evidently AI drift detection triggering automated retrain issues/jobs. | `/pipelines/ml/train.py`, `/pipelines/ml/monitor.py`, `.github/workflows/ml_retrain.yml` |
| **Architect big data infrastructure on cloud/hybrid (Cloud, Lakehouse, IaC, Docker, K8s)** | Cloud-native hybrid architecture (Cloudflare R2 + Neon + Aiven Kafka + Coolify VPS) with production Dockerfiles, Terraform IaC for cloud resources, and Helm/k3s manifests. | `/infra/terraform`, `/infra/k8s`, `/infra/coolify` |
| **Optimize query performance, resource allocation & storage at scale** | Hive-partitioned Parquet (`year/quarter/ticker`), DuckDB predicate pushdown, Postgres covering indexes, materialized views for screener queries, and strict container memory budgets. | `/pipelines/dbt/models`, `/infra/sql/ddl.sql` |
| **Embed data-quality checks, observability & MLOps practices** | Pandera schema contracts with R2 Quarantine/DLQ routing, dbt singular/generic tests, Prometheus latency/error/drift metrics, Grafana Cloud dashboards, and public `/status` page. | `/pipelines/ingestion/contracts.py`, `/apps/api/app/telemetry.py` |
| **Fluent Python, SQL, RESTful design, CI/CD, GitHub best practices** | Python 3.12 (`uv`), strict `ruff` + `mypy` + `pytest`, SQL/dbt, OpenAPI v3 REST design, trunk-based PR workflow, GitHub Actions CI/CD, Dependabot, and security scanning. | `.github/workflows/`, `pyproject.toml` |

---

## 5. Data Architecture: Medallion Lakehouse & Ingestion Resilience

### 5.1 Four Ingestion Resilience Patterns
1. **Multi-Source Ingestion with Fallback:**
   - **Primary Fundamentals & Disclosures:** IDX official disclosure/financial endpoints (JSON/XBRL parsed via `curl_cffi` with polite jittered rate-limiting and rotating headers).
   - **Secondary Market & Fallback Fundamentals:** `yfinance` (`.JK` tickers) for daily OHLCV, splits, and dividend history, plus Playwright headless fallback if IDX blocks direct HTTP calls.
2. **Schema Contracts + Quarantine/DLQ:**
   - Every raw payload is validated against strict **Pandera** schemas (`PriceSchema`, `FinancialReportSchema`, `CorporateActionSchema`).
   - Invalid or drifting rows are **never silently dropped**—they are routed to `r2://arthala-lakehouse/quarantine/{source}/dt={YYYY-MM-DD}/` along with failure metadata and increment the `ingestion_quarantine_rows_total` metric.
3. **Idempotent Upserts + Watermarks:**
   - High-watermarks (`last_ingested_at`, `filing_hash`) stored in Neon's `pipeline_runs` / watermark table ensure scheduled GitHub Actions runs can be safely retried without duplicate records.
4. **24/7 Market Replay Producer (`pipelines/replay`):**
   - Because IDX trades Mon-Fri 09:00-16:00 WIB, a lightweight replay CLI/job publishes real historical IDX tick & disclosure events into Aiven Kafka (`idx.ticks.raw`, `idx.disclosures.raw`) on demand or during demo windows so recruiters can verify live Kafka streaming, anomaly detection, and Telegram dispatch 24/7.

### 5.2 Cloudflare R2 Medallion Parquet Partition Layout

```text
r2://arthala-lakehouse/
├── bronze/
│   ├── idx_ohlcv/dt=YYYY-MM-DD/part-000.parquet
│   ├── idx_financials_raw/fiscal_year=YYYY/fiscal_quarter=Q{1-4}/part-000.parquet
│   ├── idx_dividends_raw/year=YYYY/part-000.parquet
│   ├── idx_disclosures_raw/dt=YYYY-MM-DD/part-000.parquet
│   └── idx_special_monitoring/dt=YYYY-MM-DD/part-000.parquet   # Notasi Khusus / FCA labels
├── silver/
│   ├── dim_tickers/latest.parquet
│   ├── fct_daily_prices/year=YYYY/month=MM/data.parquet
│   ├── fct_quarterly_financials/fiscal_year=YYYY/data.parquet
│   └── fct_corporate_actions/year=YYYY/data.parquet
├── gold/
│   ├── mart_screener_snapshot/dt=YYYY-MM-DD/data.parquet
│   ├── feat_distress_anomaly/fiscal_year=YYYY/data.parquet    # ML Feature Store (v1)
│   ├── feat_dividend_safety/fiscal_year=YYYY/data.parquet     # ML Feature Store (v1.5)
│   └── pred_distress_scores/dt=YYYY-MM-DD/data.parquet
├── quarantine/
│   └── {source_name}/dt=YYYY-MM-DD/run_{run_id}_errors.parquet
└── mlflow-artifacts/
    └── {experiment_id}/{run_id}/artifacts/model/
```

### 5.3 Neon Postgres Serving DDL (`infra/sql/ddl.sql`)

```sql
-- 1. Core Stock Master
CREATE TABLE IF NOT EXISTS dim_tickers (
    ticker              VARCHAR(10) PRIMARY KEY, -- e.g., 'BBCA', 'BBRI', 'ASII'
    company_name        TEXT NOT NULL,
    sector              VARCHAR(100),
    sub_sector          VARCHAR(100),
    listing_board       VARCHAR(50),             -- Main, Development, Acceleration, Watchlist (FCA)
    special_notations   TEXT[] DEFAULT '{}',     -- IDX Notasi Khusus e.g. {'X', 'E', 'L'}
    is_active           BOOLEAN DEFAULT TRUE,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Screener Snapshot (Synced from Gold Parquet after dbt run)
CREATE TABLE IF NOT EXISTS mart_screener_latest (
    ticker                  VARCHAR(10) PRIMARY KEY REFERENCES dim_tickers(ticker),
    as_of_date              DATE NOT NULL,
    close_price             NUMERIC(14, 2),
    daily_change_pct        NUMERIC(8, 4),
    volume                  BIGINT,
    market_cap_idr          NUMERIC(20, 2),
    ttm_eps                 NUMERIC(14, 4),
    per_ttm                 NUMERIC(12, 4),
    pbv                     NUMERIC(12, 4),
    roe_pct                 NUMERIC(10, 4),
    der                     NUMERIC(12, 4),
    rev_growth_yoy_pct      NUMERIC(10, 4),
    net_inc_growth_yoy_pct  NUMERIC(10, 4),
    dividend_yield_pct      NUMERIC(10, 4),
    altman_z_score          NUMERIC(10, 4),
    beneish_m_score         NUMERIC(10, 4),
    -- v1 ML Output
    distress_prob           NUMERIC(6, 5),       -- 0.00000 to 1.00000
    risk_tier               VARCHAR(20),         -- 'SAFE', 'WATCH', 'HIGH_RISK'
    shap_top3_en            JSONB NOT NULL DEFAULT '[]'::jsonb,
    shap_top3_id            JSONB NOT NULL DEFAULT '[]'::jsonb,
    model_version           VARCHAR(50) NOT NULL,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_screener_risk_yield
    ON mart_screener_latest (risk_tier, dividend_yield_pct DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_screener_valuation
    ON mart_screener_latest (per_ttm, pbv, roe_pct DESC NULLS LAST);

-- 3. Quarterly Fundamentals History (For Ticker Deep-Dive Charts)
CREATE TABLE IF NOT EXISTS fct_quarterly_financials (
    ticker                VARCHAR(10) REFERENCES dim_tickers(ticker),
    fiscal_year           SMALLINT NOT NULL,
    fiscal_quarter        SMALLINT NOT NULL CHECK (fiscal_quarter BETWEEN 1 AND 4),
    revenue_idr           NUMERIC(20, 2),
    net_income_idr        NUMERIC(20, 2),
    operating_cf_idr      NUMERIC(20, 2),
    total_assets_idr      NUMERIC(20, 2),
    total_liabilities_idr NUMERIC(20, 2),
    total_equity_idr      NUMERIC(20, 2),
    short_term_debt_idr   NUMERIC(20, 2),
    eps                   NUMERIC(14, 4),
    filing_date           DATE,
    PRIMARY KEY (ticker, fiscal_year, fiscal_quarter)
);

-- 4. Dividend & Corporate Action Calendar
CREATE TABLE IF NOT EXISTS fct_dividend_calendar (
    id                   BIGSERIAL PRIMARY KEY,
    ticker               VARCHAR(10) NOT NULL REFERENCES dim_tickers(ticker),
    fiscal_year          SMALLINT,
    dividend_per_share   NUMERIC(14, 4) NOT NULL,
    currency             VARCHAR(5) DEFAULT 'IDR',
    cum_date             DATE NOT NULL,
    ex_date              DATE,
    recording_date       DATE,
    payment_date         DATE,
    payout_ratio_pct     NUMERIC(10, 4),
    dividend_safety_tier VARCHAR(20),            -- Populated in v1.5
    UNIQUE (ticker, cum_date, dividend_per_share)
);
CREATE INDEX IF NOT EXISTS idx_div_cum_date ON fct_dividend_calendar (cum_date DESC);

-- 5. Users, Auth, Telegram Linking & Mayar QRIS Subscriptions
CREATE TABLE IF NOT EXISTS users (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email               TEXT UNIQUE NOT NULL,
    full_name           TEXT,
    preferred_lang      VARCHAR(5) DEFAULT 'en' CHECK (preferred_lang IN ('en', 'id')),
    telegram_chat_id    BIGINT UNIQUE,
    telegram_link_token VARCHAR(64) UNIQUE,
    plan_tier           VARCHAR(20) DEFAULT 'FREE' CHECK (plan_tier IN ('FREE', 'PRO')),
    pro_expires_at      TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS watchlists (
    user_id             UUID REFERENCES users(id) ON DELETE CASCADE,
    ticker              VARCHAR(10) REFERENCES dim_tickers(ticker),
    alert_on_disclosure BOOLEAN DEFAULT TRUE,
    alert_on_anomaly    BOOLEAN DEFAULT TRUE,
    alert_on_dividend   BOOLEAN DEFAULT TRUE,
    alert_on_risk_shift BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS payment_transactions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID REFERENCES users(id),
    provider            VARCHAR(20) DEFAULT 'MAYAR',
    external_tx_id      TEXT UNIQUE NOT NULL,    -- Idempotency key from Mayar webhook
    amount_idr          INTEGER NOT NULL,
    status              VARCHAR(30) NOT NULL,    -- 'PENDING', 'PAID', 'EXPIRED', 'REFUNDED'
    raw_payload         JSONB NOT NULL,
    paid_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- 6. Pipeline Watermarks & Public System Health Status
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name       VARCHAR(80) NOT NULL,
    status              VARCHAR(20) NOT NULL,    -- 'RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED'
    rows_processed      INTEGER DEFAULT 0,
    rows_quarantined    INTEGER DEFAULT 0,
    watermark_value     TEXT,
    drift_detected      BOOLEAN DEFAULT FALSE,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ
);
```

---

## 6. ML Feature Engineering, Formulations & Phased Roadmap

### 6.1 Core Forensic & Fundamental Feature Formulations
All features are computed deterministically in `dbt-duckdb` (`gold/feat_distress_anomaly`) and versioned alongside model artifacts:

1. **Cash-Flow Quality (Accruals Ratio):**
   $$\text{Accruals Ratio} = \frac{\text{Net Income} - \text{Operating Cash Flow}}{\text{Total Assets}}$$
   High positive accruals indicate earnings unsupported by actual cash inflows.

2. **Altman Z-Score (Emerging Market / Non-Bank Adaptation):**
   $$Z = 6.56 X_1 + 3.26 X_2 + 6.72 X_3 + 1.05 X_4 + 3.25$$
   where $X_1 = \frac{\text{Working Capital}}{\text{Total Assets}}$, $X_2 = \frac{\text{Retained Earnings}}{\text{Total Assets}}$, $X_3 = \frac{\text{EBIT}}{\text{Total Assets}}$, and $X_4 = \frac{\text{Book Value of Equity}}{\text{Total Liabilities}}$. (For banking stocks like `BBCA`/`BBRI`, sector-relative flags and capital/leverage proxies are supplied so the tree model avoids penalizing structural bank leverage.)

3. **Beneish M-Score (Earnings Manipulation Risk Proxy):**
   $$M = -4.84 + 0.92\,\text{DSRI} + 0.528\,\text{GMI} + 0.404\,\text{AQI} + 0.892\,\text{SGI} + 0.115\,\text{DEPI} - 0.172\,\text{SGAI} + 4.679\,\text{TATA} - 0.327\,\text{LVGI}$$
   Values where $M > -1.78$ signal elevated earnings manipulation risk.

4. **Debt Maturity Pressure:**
   $$\text{Short-Term Debt Pressure} = \frac{\text{Short-Term Debt}}{\max(\text{Cash \& Equivalents} + \text{Operating Cash Flow}, 1)}$$

5. **Additional Fundamental & Market Features:** Annualized EPS, PER (TTM), PBV, ROE, DER, YoY Revenue Growth, YoY Net Income Growth, 3-year Dividend Streak, and Sector-Relative Z-Score Percentiles.

### 6.2 Ground-Truth Labels & Explainability
- **Positive Distress/Anomaly Label ($y = 1$):** Any IDX issuer receiving a negative **IDX Notasi Khusus / Special Monitoring Board (FCA)** designation (e.g., negative equity `E`, delayed financial report `L`, PKPU/bankruptcy petition `B`/`M`, or adverse audit opinion `D`), delisting notice, or experiencing a $> 50\%$ YoY earnings collapse combined with negative operating cash flow.
- **SHAP Bilingual Explainability:** At batch scoring and online inference time, `TreeExplainer` extracts the Top-3 contributing features and maps them to human-readable English & Bahasa Indonesia templates (e.g., *"High accruals ratio: reported profit significantly exceeds operating cash flow"* / *"Rasio akrual tinggi: laba bersih jauh lebih besar dari arus kas operasi"*).

### 6.3 Phased ML Roadmap

| Release | Model | Type & Serving Pattern | MLOps & Business Purpose |
|---|---|---|---|
| **v1 (Flagship)** | **Financial Distress & Accounting Anomaly Score** | LightGBM + SHAP; batch daily scoring + on-demand `POST /v1/predict/distress` API | Establishes full end-to-end MLOps (MLflow, Evidently drift checks, CI retraining gate). Direct match to OCBC credit/fraud scoring. |
| **v1.5 (Paid Catalyst)** | **Dividend Cut & Sustainability Predictor** + **Real-Time Price/Volume & Disclosure Anomaly Classifier** | LightGBM (reuses v1 Feature Store) + Isolation Forest / Z-Score & keyword/NLP classifier on Aiven Kafka stream | Powers instant paid Telegram alerts when unusual volume spikes, earnings drops, or payout cuts occur. |
| **v2 (Retention)** | **Personalized Stock Screener & Portfolio Risk Recommender** | Cosine similarity / ranking over user watchlist sector exposure & risk tolerance | Eliminates cold-start problem by leveraging real v1/v1.5 user watchlists; maps to OCBC customer personalization. |

---

## 7. Telegram Bot & Mayar.id QRIS Subscription Flow

```text
[User on Arthala ID Web]
   │ 1. Clicks "Connect Telegram" -> arthala-api generates one-time token (TTL 15 min)
   ▼
[Telegram App: @ArthalaIDBot -> /start <token>]
   │ 2. arthala-worker verifies token in Neon -> binds telegram_chat_id to user_id
   ▼
[User clicks "Upgrade Pro (Rp39.000/mo QRIS)"]
   │ 3. Next.js redirects to Mayar.id Payment Link (passing user_id/email metadata)
   ▼
[User scans QRIS via GoPay/OVO/BCA/OCBC mobile]
   │ 4. Mayar.id sends POST /v1/webhooks/mayar to Coolify arthala-api
   │ 5. FastAPI verifies signature, checks external_tx_id idempotency in payment_transactions,
   │    sets plan_tier='PRO' and pro_expires_at = NOW() + INTERVAL '30 days'
   ▼
[Kafka Event: user.subscription.updated]
   │ 6. arthala-worker consumes event -> sends instant Telegram welcome confirmation + unlocks Pro alerts
```

---

## 8. Core v1 Web UI Screens (Next.js Bilingual EN/ID)

Every page carries a clean footer and brand mark: **Arthala ID — by Robit Al Hazmi**, reinforcing transparency and accountability for both retail investors and hiring managers.

1. **`/` — Public IDX Stock Screener:** Fast filterable table (Sector, PER, PBV, ROE, DER, Dividend Yield, ML Risk Tier badge) with search and bilingual `EN | ID` toggle (defaults to `EN` for recruiters; remembers preference in `localStorage`).
2. **`/ticker/[symbol]` — Ticker Deep-Dive:** Interactive **TradingView Lightweight Charts** candlestick + volume chart, quarterly Revenue/Net Income/OCF trend cards, Altman Z / Beneish M indicators, and the **Top-3 SHAP Risk Reasons** card.
3. **`/watchlist` — Personal Watchlist & Dividend Calendar:** Authenticated view (Auth.js / Google OAuth or Magic Link) managing watched tickers, upcoming IDX cum-date / payment-date calendar, `@ArthalaIDBot` one-click linking, and Mayar.id QRIS Pro upgrade button.
4. **`/status` — Public System Health & Live Data Observability:** Live status page querying `pipeline_runs`, Kafka lag/replay heartbeats, latest MLflow production model version, quarantine row counts, and Evidently drift badge—acting as an instant live showcase for OCBC hiring managers.

---

## 9. Monorepo Folder Structure & GitHub Best Practices

### 9.1 Clean Monorepo Tree

```text
arthala-id/
├── .github/
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   ├── PULL_REQUEST_TEMPLATE.md
│   ├── CODEOWNERS
│   ├── dependabot.yml
│   └── workflows/
│       ├── ci.yml                  # Ruff, Mypy, Pytest, Next.js build, Docker build check
│       ├── etl_daily.yml           # Scheduled EOD IDX ingestion -> dbt-duckdb -> Neon sync
│       ├── ml_retrain.yml          # Weekly MLflow training + Evidently drift check + model gate
│       └── replay_demo.yml         # Manual workflow_dispatch to replay historical ticks into Kafka
├── apps/
│   ├── api/                        # FastAPI service (REST + ML inference + Mayar webhooks)
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── core/               # config, db, security, telemetry
│   │   │   ├── routers/            # tickers, screener, predict, watchlist, webhooks, status
│   │   │   ├── ml/                 # model loader (R2/MLflow), SHAP bilingual mapper
│   │   │   └── schemas/            # Pydantic v2 models
│   │   ├── tests/
│   │   └── Dockerfile
│   ├── worker/                     # Async Aiven Kafka consumer + @ArthalaIDBot + alert dispatcher
│   │   ├── worker/
│   │   │   ├── main.py
│   │   │   ├── kafka_consumer.py
│   │   │   ├── anomaly_rules.py
│   │   │   └── telegram_bot.py
│   │   ├── tests/
│   │   └── Dockerfile
│   └── web/                        # Next.js (TypeScript, Tailwind, Lightweight Charts, i18n)
│       ├── src/app/
│       ├── src/components/
│       ├── src/messages/           # en.json, id.json
│       └── package.json
├── pipelines/
│   ├── ingestion/                  # Multi-source IDX/yfinance scrapers + Pandera contracts + DLQ
│   ├── dbt/                        # dbt-duckdb project (staging -> silver -> gold marts/features)
│   ├── ml/                         # LightGBM training, SHAP, MLflow registry, Evidently drift
│   ├── replay/                     # 24/7 Kafka Historical IDX Tick & Disclosure Replay Producer
│   └── airflow/                    # Portable Airflow DAG mirrors (OCBC orchestration showcase)
├── infra/
│   ├── docker/
│   │   └── docker-compose.local.yml # Local Postgres, Redpanda (Kafka-compatible), MinIO (R2-compatible)
│   ├── coolify/
│   │   └── docker-compose.prod.yml  # Production Coolify stack with strict memory limits
│   ├── k8s/                        # k3s / Helm chart templates for api & worker (OCBC K8s showcase)
│   ├── terraform/                  # IaC definitions for Cloudflare R2, Neon, and Aiven Kafka
│   └── sql/
│       └── ddl.sql                 # Idempotent Neon Postgres schema
├── docs/
│   ├── Implementation_Plan.md
│   ├── Task.md
│   ├── Walkthrough.md
│   └── architecture-diagram.md     # Mermaid system diagrams
├── pyproject.toml                  # uv workspace / Python dependencies, ruff, mypy, pytest config
├── Justfile                        # Standardized local dev & test commands for Antigravity IDE
├── .pre-commit-config.yaml
├── .env.example
└── README.md                       # Recruiter-ready overview + OCBC traceability table + live links
```

### 9.2 GitHub Best Practices & CI/CD Blueprint
- **Trunk-Based Development:** Protect `main` branch (require PR review/self-checklist, require passing `ci.yml` status checks, disallow force pushes, linear squash-merge history).
- **Conventional Commits:** `feat(ml): add beneish m-score feature`, `fix(ingestion): route malformed idx xbrl rows to r2 quarantine`, `ci(etl): schedule daily dbt-duckdb run`.
- **Automated Quality Gates (`.github/workflows/ci.yml`):**
  - Python job: `uv sync --frozen`, `ruff check .`, `ruff format --check .`, `mypy apps/api apps/worker pipelines`, `pytest --cov`.
  - dbt job: `dbt parse` and `dbt test` against local DuckDB fixture data.
  - Web job: `npm ci`, `npm run lint`, `npm run typecheck`, `npm run build`.
  - Security: `gitleaks` secret scanning + `dependabot.yml` weekly dependency updates.
