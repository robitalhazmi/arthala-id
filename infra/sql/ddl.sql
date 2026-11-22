-- =============================================================================
-- Arthala ID — by Robit Al Hazmi
-- Neon Serverless Postgres serving schema (idempotent; safe to re-run).
-- Source of truth: docs/Implementation_Plan.md §5.3
--
-- Requires Postgres >= 13 (gen_random_uuid() is built in). Neon & local PG16 OK.
-- Additive only: never add DROP / TRUNCATE here. Primary-key or partition
-- changes require explicit approval (AGENTS.md §7).
-- =============================================================================

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
