---
description: Audit current branch changes against AGENTS.md guardrails (2 GB VPS budget, security, i18n, tests) and generate a clean PR description.
---

# Workflow: `/pr-ready`

Perform a final senior-engineer self-review before opening a Pull Request into `main`.

1. **Inspect Branch Diff:**
   - Confirm current branch is not `main`.
   - Run `git diff --stat main...HEAD` and review changed files.

2. **Audit Against `AGENTS.md` Guardrails:**
   - **2 GB Coolify VPS RAM Guardrail:** Verify no heavy services were added to `infra/coolify/docker-compose.prod.yml`, `arthala-api` stays `--workers 1` (`mem_limit: 450m`), `arthala-worker` (`mem_limit: 250m`), and `grafana-alloy` (`mem_limit: 100m`).
   - **Data & ML Guardrails:** Verify Pandera quarantine routing (no silent drops), idempotent writes, zero lookahead leakage in ML splits, and bilingual SHAP outputs if ML code changed.
   - **Security & Payments:** Verify no hardcoded secrets, HMAC signature verification + `external_tx_id` idempotency if Mayar webhook code changed.
   - **Bilingual UI:** Verify all user-facing strings in `apps/web` exist in both `en.json` and `id.json`.
   - **Docs Sync:** Verify `docs/Task.md` checkboxes and `.env.example` are updated.

3. **Run `/verify-all` Checks:**
   - Execute all applicable lint, typecheck, test, and build checks for touched directories.

4. **Draft PR Title & Body:**
   - Propose a **Conventional Commit** PR title (e.g., `feat(api): add screener filtering and live distress inference endpoint`).
   - Output a ready-to-paste PR description matching `.github/PULL_REQUEST_TEMPLATE.md` (Summary, Engineering Pillar/Task Reference, Verification Evidence, and VPS Resource Impact).
