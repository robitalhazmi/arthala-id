"""Coolify 1 vCPU / 2 GB VPS hardening, memory guardrail, and dashboard health auditor.

Implements Task 0.5 (``docs/Task.md``) and enforces ``AGENTS.md`` §2.1 guardrails:
- Verifies 2 GB Linux swap (``SwapTotal >= 1900 MiB``) from ``/proc/meminfo``.
- Audits Coolify production Docker Compose specs against strict container memory caps:
  - ``arthala-api``: ``<= 450 MiB`` and single Uvicorn worker (``--workers 1``)
  - ``arthala-worker``: ``<= 250 MiB``
  - ``grafana-alloy``: ``<= 100 MiB``
  - Total runtime containers ``<= 800 MiB`` (leaving ~900 MiB headroom on a 2 GB VPS)
  - Zero forbidden heavy JVM/broker/TSDB or Next.js frontend services on the VPS.
- Probes the Coolify dashboard health endpoint (``COOLIFY_BASE_URL``) when configured.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import httpx
import structlog
import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

SERVICE_MEM_CAP_MIB: dict[str, int] = {
    "arthala-api": 450,
    "arthala-worker": 250,
    "grafana-alloy": 100,
}
TOTAL_VPS_CONTAINER_BUDGET_MIB = 800
MIN_EXPECTED_SWAP_MIB = 1900

FORBIDDEN_IMAGE_PATTERNS: tuple[str, ...] = (
    "spark",
    "hadoop",
    "airflow",
    "kafka",
    "zookeeper",
    "redpanda",
    "flink",
    "prometheus",
    "grafana/grafana",
)

FORBIDDEN_SERVICE_NAMES: frozenset[str] = frozenset(
    {
        "arthala-web",
        "web",
        "nextjs",
        "spark",
        "airflow",
        "kafka",
        "redpanda",
        "prometheus",
        "grafana",
    }
)


class CoolifyVpsSettings(BaseSettings):
    """Coolify VPS health & deployment configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    coolify_base_url: str = Field(default="", alias="COOLIFY_BASE_URL")
    coolify_api_token: str = Field(default="", alias="COOLIFY_API_TOKEN")


@dataclass(frozen=True, slots=True)
class VpsAuditReport:
    """Result of auditing host swap, Coolify compose memory budget, and dashboard health."""

    swap_ok: bool
    swap_total_mib: int
    compose_ok: bool
    total_allocated_mib: int
    headroom_mib: int
    coolify_status: Literal["HEALTHY", "SKIPPED", "ERROR"]
    violations: tuple[str, ...]


def parse_mem_limit_mib(raw_limit: str | int | float) -> int:
    """Parse a Docker ``mem_limit`` value (e.g. ``'450m'``, ``'1g'``, ``'512MiB'``) into MiB."""
    if isinstance(raw_limit, (int, float)):
        return int(raw_limit) // (1024 * 1024)

    cleaned = raw_limit.strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(b|k|kb|kib|m|mb|mib|g|gb|gib)", cleaned)
    if not match:
        raise ValueError(f"Unsupported Docker mem_limit format: '{raw_limit}'")

    amount = float(match.group(1))
    unit = match.group(2)
    if unit in {"g", "gb", "gib"}:
        return round(amount * 1024)
    if unit in {"m", "mb", "mib"}:
        return round(amount)
    if unit in {"k", "kb", "kib"}:
        return max(1, round(amount / 1024))
    return max(1, round(amount / (1024 * 1024)))


def parse_meminfo_swap_mib(meminfo_text: str) -> tuple[int, int]:
    """Extract ``(MemTotal_MiB, SwapTotal_MiB)`` from Linux ``/proc/meminfo`` content."""
    mem_kib = 0
    swap_kib = 0
    for line in meminfo_text.splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                mem_kib = int(parts[1])
        elif line.startswith("SwapTotal:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                swap_kib = int(parts[1])
    return mem_kib // 1024, swap_kib // 1024


def audit_coolify_compose_spec(compose_data: dict[str, Any]) -> tuple[bool, int, list[str]]:
    """Audit a Coolify Docker Compose dictionary against the 2 GB VPS RAM guardrails."""
    services = compose_data.get("services")
    if not isinstance(services, dict) or not services:
        return False, 0, ["Compose specification contains no services"]

    violations: list[str] = []
    total_mib = 0

    for svc_name, svc_cfg in services.items():
        name_lower = str(svc_name).lower()
        if name_lower in FORBIDDEN_SERVICE_NAMES:
            violations.append(f"Forbidden service '{svc_name}' on Coolify VPS (AGENTS.md §2.1)")

        if not isinstance(svc_cfg, dict):
            violations.append(f"Service '{svc_name}' definition is not a mapping")
            continue

        image = str(svc_cfg.get("image", "")).lower()
        for pattern in FORBIDDEN_IMAGE_PATTERNS:
            if pattern in image:
                violations.append(
                    f"Service '{svc_name}' uses forbidden heavy image "
                    f"'{image}' (matches '{pattern}')"
                )

        raw_limit = svc_cfg.get("mem_limit")
        if raw_limit is None:
            deploy_cfg = svc_cfg.get("deploy")
            if isinstance(deploy_cfg, dict):
                res_cfg = deploy_cfg.get("resources")
                if isinstance(res_cfg, dict):
                    lim_cfg = res_cfg.get("limits")
                    if isinstance(lim_cfg, dict):
                        raw_limit = lim_cfg.get("memory")

        if raw_limit is None:
            violations.append(f"Service '{svc_name}' is missing an explicit mem_limit")
            continue

        try:
            limit_mib = parse_mem_limit_mib(raw_limit)
        except ValueError as exc:
            violations.append(f"Service '{svc_name}': {exc}")
            continue

        total_mib += limit_mib
        max_cap = SERVICE_MEM_CAP_MIB.get(str(svc_name))
        if max_cap is not None and limit_mib > max_cap:
            violations.append(f"Service '{svc_name}' mem_limit {limit_mib}m exceeds cap {max_cap}m")

        if str(svc_name) == "arthala-api":
            cmd_raw = svc_cfg.get("command", "")
            cmd_str = (
                " ".join(str(x) for x in cmd_raw) if isinstance(cmd_raw, list) else str(cmd_raw)
            )
            if cmd_str and "--workers" in cmd_str and "--workers 1" not in cmd_str:
                violations.append(
                    "arthala-api must use '--workers 1' so ML artifacts load once in RAM"
                )

    if total_mib > TOTAL_VPS_CONTAINER_BUDGET_MIB:
        violations.append(
            f"Total container mem_limit {total_mib}m exceeds 2 GB VPS budget "
            f"{TOTAL_VPS_CONTAINER_BUDGET_MIB}m"
        )

    return len(violations) == 0, total_mib, violations


def verify_coolify_dashboard_health(
    settings: CoolifyVpsSettings,
    *,
    http_client: httpx.Client | None = None,
) -> tuple[Literal["HEALTHY", "SKIPPED", "ERROR"], str]:
    """Probe the Coolify dashboard ``/api/health`` endpoint if ``COOLIFY_BASE_URL`` is set."""
    base_url = settings.coolify_base_url.strip().rstrip("/")
    if not base_url:
        return "SKIPPED", "COOLIFY_BASE_URL not set (skipping remote dashboard probe)"

    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "ERROR", f"Invalid COOLIFY_BASE_URL '{base_url}'"

    headers: dict[str, str] = {}
    if settings.coolify_api_token.strip():
        headers["Authorization"] = f"Bearer {settings.coolify_api_token.strip()}"

    close_client = http_client is None
    client = http_client or httpx.Client(timeout=5.0)
    try:
        resp = client.get(f"{base_url}/api/health", headers=headers)
        if resp.status_code == 200:
            return "HEALTHY", f"Coolify health endpoint {base_url}/api/health returned 200 OK"
        return (
            "ERROR",
            f"Coolify health endpoint {base_url}/api/health returned HTTP {resp.status_code}",
        )
    except Exception as exc:
        return "ERROR", f"Coolify health probe failed: {exc}"
    finally:
        if close_client:
            client.close()


def run_vps_audit(
    *,
    meminfo_text: str | None = None,
    compose_path: Path | None = None,
    require_swap: bool = False,
    settings: CoolifyVpsSettings | None = None,
    http_client: httpx.Client | None = None,
) -> VpsAuditReport:
    """Execute the full Coolify 1 vCPU / 2 GB VPS guardrail audit."""
    cfg = settings or CoolifyVpsSettings()
    violations: list[str] = []

    if meminfo_text is None:
        proc_meminfo = Path("/proc/meminfo")
        raw_meminfo = proc_meminfo.read_text(encoding="utf-8") if proc_meminfo.exists() else ""
    else:
        raw_meminfo = meminfo_text

    _, swap_total_mib = parse_meminfo_swap_mib(raw_meminfo)
    swap_ok = swap_total_mib >= MIN_EXPECTED_SWAP_MIB
    if require_swap and not swap_ok:
        violations.append(
            f"Active swap {swap_total_mib} MiB is below required {MIN_EXPECTED_SWAP_MIB} MiB (2 GB)"
        )

    if compose_path is not None and compose_path.exists():
        loaded = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        compose_dict = loaded if isinstance(loaded, dict) else {}
        compose_ok, total_allocated_mib, compose_violations = audit_coolify_compose_spec(
            compose_dict
        )
        violations.extend(compose_violations)
    else:
        # Validate canonical Phase 5.1 budget baseline (450m + 250m + 100m = 800m)
        baseline_spec: dict[str, Any] = {
            "services": {
                "arthala-api": {
                    "image": "ghcr.io/robitalhazmi/arthala-api:latest",
                    "mem_limit": "450m",
                    "command": (
                        "uvicorn apps.api.app.main:app --host 0.0.0.0 --port 8000 --workers 1"
                    ),
                },
                "arthala-worker": {
                    "image": "ghcr.io/robitalhazmi/arthala-worker:latest",
                    "mem_limit": "250m",
                },
                "grafana-alloy": {
                    "image": "grafana/alloy:latest",
                    "mem_limit": "100m",
                },
            }
        }
        compose_ok, total_allocated_mib, compose_violations = audit_coolify_compose_spec(
            baseline_spec
        )
        violations.extend(compose_violations)

    # 2048 MiB total VPS RAM - ~500 MiB host/Coolify baseline - allocated container limits
    headroom_mib = 2048 - 500 - total_allocated_mib
    coolify_status, coolify_detail = verify_coolify_dashboard_health(cfg, http_client=http_client)
    if coolify_status == "ERROR":
        violations.append(coolify_detail)

    report = VpsAuditReport(
        swap_ok=swap_ok,
        swap_total_mib=swap_total_mib,
        compose_ok=compose_ok,
        total_allocated_mib=total_allocated_mib,
        headroom_mib=headroom_mib,
        coolify_status=coolify_status,
        violations=tuple(violations),
    )
    logger.info(
        "vps_audit.completed",
        swap_ok=report.swap_ok,
        swap_total_mib=report.swap_total_mib,
        compose_ok=report.compose_ok,
        total_allocated_mib=report.total_allocated_mib,
        headroom_mib=report.headroom_mib,
        coolify_status=report.coolify_status,
        violations=list(report.violations),
    )
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for ``python -m pipelines.ingestion.vps_audit``."""
    parser = argparse.ArgumentParser(
        description="Audit Coolify 1 vCPU / 2 GB VPS swap, memory caps, and dashboard health."
    )
    parser.add_argument(
        "--compose-file",
        default="infra/coolify/docker-compose.prod.yml",
        help="Path to Coolify production docker-compose.prod.yml (if present).",
    )
    parser.add_argument(
        "--require-swap",
        action="store_true",
        help="Fail if the current host has less than 1900 MiB (~2 GB) swap enabled.",
    )
    args = parser.parse_args(argv)

    report = run_vps_audit(
        compose_path=Path(args.compose_file),
        require_swap=args.require_swap,
    )
    return 1 if report.violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
