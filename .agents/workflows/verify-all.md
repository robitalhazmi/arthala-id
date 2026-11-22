---
description: Run the complete Arthala ID local quality suite across Python, dbt, Next.js, and Docker configs.
---

# Workflow: `/verify-all`

Run every automated check that `.github/workflows/ci.yml` enforces before opening a Pull Request.

1. **Check Git Status & Secrets Hygiene:**
   - Run `git status -s` and verify no `.env` files, private keys, or raw data dumps are staged.
   - Confirm any newly referenced environment variables exist in `.env.example`.

2. **Python Lint, Format, Typecheck & Tests:**
   - Skip only if no Python files exist yet in the repo.
   ```bash
   uv run ruff check .
   uv run ruff format --check .
   uv run mypy apps/api apps/worker pipelines
   uv run pytest --cov
   ```

3. **dbt Parse & Local Tests:**
   - Run if `pipelines/dbt/dbt_project.yml` exists.
   ```bash
   cd pipelines/dbt && uv run dbt parse && uv run dbt test --target local
   ```

4. **Next.js Lint, Typecheck & Production Build:**
   - Run if `apps/web/package.json` exists.
   - Also confirm `src/messages/en.json` and `src/messages/id.json` contain matching keys.
   ```bash
   cd apps/web && npm run lint && npm run typecheck && npm run build
   ```

5. **Docker Compose Config Validation:**
   - Validate local and Coolify production compose files if present:
   ```bash
   docker compose -f infra/docker/docker-compose.local.yml config -q
   docker compose -f infra/coolify/docker-compose.prod.yml config -q
   ```

6. **Report Summary Table:**
   - Output a concise Markdown status table (`Check | Status | Notes`) and list exact fixes needed for any failing check.
