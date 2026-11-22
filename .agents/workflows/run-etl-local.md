---
description: Boot local Postgres, MinIO, and Redpanda, then execute and verify the full Bronze -> Silver -> Gold -> Postgres pipeline locally.
---

# Workflow: `/run-etl-local`

Verify the end-to-end **Arthala ID** Medallion Lakehouse pipeline locally without touching production Neon, Cloudflare R2, or Aiven Kafka.

1. **Verify Local Sandbox Containers:**
   - Ensure `.env` points strictly to `localhost` (local Postgres, MinIO, Redpanda). **Abort immediately** if `DATABASE_URL` or `R2_ENDPOINT_URL` points to production hosts.
   - Start local services:
   ```bash
   docker compose -f infra/docker/docker-compose.local.yml up -d
   ```

2. **Ensure Local Database Schema is Initialized:**
   ```bash
   psql "$DATABASE_URL" -f infra/sql/ddl.sql
   ```

3. **Run Multi-Source Ingestion (Bronze + Pandera Quarantine/DLQ):**
   ```bash
   uv run python -m pipelines.ingestion.runner --env local
   ```

4. **Run dbt-duckdb Transformations & Data Quality Tests (Silver & Gold):**
   ```bash
   cd pipelines/dbt && uv run dbt run --target local && uv run dbt test --target local
   ```

5. **Sync Gold Marts to Local Postgres:**
   ```bash
   uv run python -m pipelines.ingestion.sync_neon --env local
   ```

6. **Audit Pipeline Health & Watermarks:**
   - Query the latest rows in `pipeline_runs` and row counts in `mart_screener_latest` and `fct_dividend_calendar`:
   ```bash
   psql "$DATABASE_URL" -c "SELECT pipeline_name, status, rows_processed, rows_quarantined, finished_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 5;"
   psql "$DATABASE_URL" -c "SELECT COUNT(*) AS screener_rows FROM mart_screener_latest;"
   ```
   - Report row counts, any quarantined records, and execution status.
