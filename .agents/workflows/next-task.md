---
description: Pick the next unchecked task from docs/Task.md, create a feature branch, implement it with tests, run quality gates, and update the tracker.
---

# Workflow: `/next-task`

Execute the next unchecked milestone task in **Arthala ID** following strict branch, quality, and resource guardrails.

1. **Identify the Next Task:**
   - Read `AGENTS.md` and `docs/Task.md`.
   - Find the first unchecked item (`- [ ]`) in `docs/Task.md` (unless the user explicitly specified a task ID such as `1.2` or `3.1`).
   - Read the corresponding architectural spec, schema, or contract in `docs/Implementation_Plan.md`.

2. **Present a Concise Plan & Pause Only if Escalation Applies:**
   - Summarize the target task, files to create/modify, and test plan in 4–6 bullet points.
   - Check Section 7 (*Escalation Rules*) of `AGENTS.md`. If the task does not trigger an escalation rule, proceed immediately.

3. **Create a Short-Lived Feature Branch:**
   - Ensure the working tree is clean on `main` (or ask before stashing uncommitted work).
   - Create and switch to a branch named `<type>/<task-id>-<short-scope>` (e.g., `feat/1-1-pandera-contracts`, `chore/0-2-monorepo-tooling`).

4. **Implement Cleanly:**
   - Write minimal, well-typed code adhering strictly to `AGENTS.md` standards (Python 3.12 + `uv`, Pydantic v2, `structlog`, async I/O, EN/ID i18n keys for `apps/web`, and strict VPS memory limits).
   - Add unit/integration tests alongside the implementation using offline fixtures and mocks (never rely on live IDX scraping or production cloud services in tests).
   - If any new environment variable is introduced, add a placeholder to `.env.example`.

5. **Run Relevant Verification Gates:**
   - For Python changes (`apps/api`, `apps/worker`, `pipelines`):
     ```bash
     uv run ruff check .
     uv run ruff format --check .
     uv run mypy apps/api apps/worker pipelines
     uv run pytest --cov
     ```
   - For dbt changes (`pipelines/dbt`):
     ```bash
     cd pipelines/dbt && uv run dbt parse && uv run dbt test --target local
     ```
   - For Next.js changes (`apps/web`):
     ```bash
     cd apps/web && npm run lint && npm run typecheck && npm run build
     ```
   - Fix any errors until all applicable checks pass cleanly.

6. **Update Tracker & Summarize:**
   - Mark the completed item(s) as `- [x]` in `docs/Task.md`.
   - Stage changes and suggest a Conventional Commit message (e.g., `feat(ingestion): add pandera contracts and r2 quarantine writer`).
   - Report the branch name, files touched, test results, and the next upcoming task in `docs/Task.md`.
