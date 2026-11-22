---
name: Bug report
about: Report a defect in Arthala ID (API, worker, pipelines, ML, or web)
title: "fix(<scope>): <short description>"
labels: ["bug"]
assignees: ["robitalhazmi"]
---

## Describe the bug

<!-- A clear, concise description of what is wrong. -->

## Affected component

- [ ] `apps/api` (FastAPI / inference / Mayar webhook)
- [ ] `apps/worker` (Kafka consumer / @ArthalaIDBot)
- [ ] `apps/web` (Next.js dashboard)
- [ ] `pipelines/ingestion` / `pipelines/dbt` (ETL / data quality)
- [ ] `pipelines/ml` (training / SHAP / drift)
- [ ] `infra` (compose / Coolify / k8s / terraform / SQL)

## Steps to reproduce

1.
2.
3.

## Expected behavior

## Actual behavior / logs

<!-- Paste structured log lines (structlog JSON) or stack traces. NEVER paste secrets or tokens. -->

```text
```

## Environment

- Environment: `local` / `ci` / `prod`
- Commit / branch:
- Ticker(s) involved (if data-related):

## Impact

- [ ] Data correctness (wrong ratios / scores)
- [ ] Alert delivery (missed / duplicate Telegram alerts)
- [ ] Payments / subscription state
- [ ] VPS memory / stability
