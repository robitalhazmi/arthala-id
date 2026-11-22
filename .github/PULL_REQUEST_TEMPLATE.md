## Summary

<!-- What does this PR change and why? Keep it scoped to one Task.md item. -->

## Task / OCBC Pillar Reference

- Task: `docs/Task.md` → **X.Y** <!-- e.g. 1.1 Pandera Data Contracts & DLQ -->
- OCBC pillar: <!-- e.g. Data quality & observability / MLOps / Streaming / IaC -->

## Changes

-

## Verification Evidence

<!-- Paste command output or mark N/A for untouched areas. -->

- [ ] `uv run ruff check .` and `uv run ruff format --check .`
- [ ] `uv run mypy apps/api apps/worker pipelines`
- [ ] `uv run pytest --cov`
- [ ] `cd pipelines/dbt && uv run dbt parse && uv run dbt test --target local` (dbt changes)
- [ ] `cd apps/web && npm run lint && npm run typecheck && npm run build` (web changes)
- [ ] UI verified on desktop + mobile, `EN` + `ID` (web changes)
- [ ] `docker compose -f infra/docker/docker-compose.local.yml config -q` (infra changes)

## VPS Resource Impact (1 vCPU / 2 GB)

- [ ] No new containers on the Coolify VPS
- [ ] `arthala-api` ≤ `450m` with `--workers 1` · `arthala-worker` ≤ `250m` · `grafana-alloy` ≤ `100m`
- [ ] No JVM / heavy services (Spark, Airflow, Kafka brokers, local TSDB) added to `infra/coolify/`

Expected RAM delta: <!-- e.g. +0 MiB (batch runs on GitHub Actions) -->

## Guardrail Checklist

- [ ] No secrets committed; new env vars documented in `.env.example`
- [ ] Ingestion: Pandera validation + quarantine routing (no silent drops), idempotent writes
- [ ] ML: time-based splits (no leakage), bilingual SHAP (`shap_top3_en` / `shap_top3_id`)
- [ ] Payments: HMAC signature verified, `external_tx_id` idempotency enforced
- [ ] Web: all user-facing strings in both `en.json` and `id.json`
- [ ] `docs/Task.md` checkboxes updated
- [ ] No breaking changes to `infra/sql/ddl.sql` primary keys or R2 partition paths (or explicitly approved)
