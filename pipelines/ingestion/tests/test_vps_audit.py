"""Unit tests for Task 0.5 Coolify 1 vCPU / 2 GB VPS hardening and memory auditor."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest

from pipelines.ingestion.vps_audit import (
    CoolifyVpsSettings,
    audit_coolify_compose_spec,
    main,
    parse_mem_limit_mib,
    parse_meminfo_swap_mib,
    run_vps_audit,
    verify_coolify_dashboard_health,
)


def test_parse_mem_limit_mib_variants() -> None:
    assert parse_mem_limit_mib("450m") == 450
    assert parse_mem_limit_mib("250MiB") == 250
    assert parse_mem_limit_mib("1g") == 1024
    assert parse_mem_limit_mib("512000k") == 500
    assert parse_mem_limit_mib(100 * 1024 * 1024) == 100
    assert parse_mem_limit_mib("1048576b") == 1

    with pytest.raises(ValueError, match="Unsupported Docker mem_limit format"):
        parse_mem_limit_mib("unlimited")


def test_parse_meminfo_swap_mib() -> None:
    meminfo = """
MemTotal:        2048000 kB
MemFree:          920000 kB
SwapTotal:       2097148 kB
SwapFree:        2097148 kB
"""
    mem_mib, swap_mib = parse_meminfo_swap_mib(meminfo)
    assert mem_mib == 2000
    assert swap_mib == 2047


def test_audit_coolify_compose_spec_valid_and_violations() -> None:
    valid_spec: dict[str, Any] = {
        "services": {
            "arthala-api": {
                "image": "ghcr.io/robitalhazmi/arthala-api:latest",
                "mem_limit": "450m",
                "command": ["uvicorn", "apps.api.app.main:app", "--workers", "1"],
            },
            "arthala-worker": {
                "image": "ghcr.io/robitalhazmi/arthala-worker:latest",
                "deploy": {"resources": {"limits": {"memory": "250m"}}},
            },
            "grafana-alloy": {
                "image": "grafana/alloy:latest",
                "mem_limit": "100m",
            },
        }
    }
    ok, total_mib, violations = audit_coolify_compose_spec(valid_spec)
    assert ok is True
    assert total_mib == 800
    assert violations == []

    # Empty services
    ok_empty, _, v_empty = audit_coolify_compose_spec({})
    assert ok_empty is False
    assert "no services" in v_empty[0]

    # Forbidden services, images, missing mem_limit, bad workers, and budget overrun
    bad_spec: dict[str, Any] = {
        "services": {
            "arthala-api": {
                "image": "ghcr.io/robitalhazmi/arthala-api:latest",
                "mem_limit": "600m",
                "command": "uvicorn apps.api.app.main:app --workers 4",
            },
            "arthala-worker": {
                "image": "ghcr.io/robitalhazmi/arthala-worker:latest",
                "mem_limit": "invalid_limit",
            },
            "spark": {
                "image": "bitnami/spark:latest",
            },
            "bad-mapping": "not-a-dict",
        }
    }
    ok_bad, _, v_bad = audit_coolify_compose_spec(bad_spec)
    assert ok_bad is False
    joined = " | ".join(v_bad)
    assert "exceeds cap 450m" in joined
    assert "--workers 1" in joined
    assert "Unsupported Docker mem_limit format" in joined
    assert "Forbidden service 'spark'" in joined
    assert "forbidden heavy image" in joined
    assert "missing an explicit mem_limit" in joined
    assert "not a mapping" in joined


def test_verify_coolify_dashboard_health() -> None:
    status_skip, _ = verify_coolify_dashboard_health(CoolifyVpsSettings())
    assert status_skip == "SKIPPED"

    status_bad_url, _ = verify_coolify_dashboard_health(
        CoolifyVpsSettings(COOLIFY_BASE_URL="not-a-url")
    )
    assert status_bad_url == "ERROR"

    ok_client = httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, text="OK"))
    )
    status_ok, _ = verify_coolify_dashboard_health(
        CoolifyVpsSettings(
            COOLIFY_BASE_URL="https://coolify.arthala.id",
            COOLIFY_API_TOKEN="tok_" + ("a" * 20),
        ),
        http_client=ok_client,
    )
    assert status_ok == "HEALTHY"

    err_client = httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(503, text="Unavailable"))
    )
    status_503, _ = verify_coolify_dashboard_health(
        CoolifyVpsSettings(COOLIFY_BASE_URL="https://coolify.arthala.id"),
        http_client=err_client,
    )
    assert status_503 == "ERROR"


def test_run_vps_audit_and_cli(tmp_path: Path) -> None:
    compose_file = tmp_path / "docker-compose.prod.yml"
    compose_file.write_text(
        """
services:
  arthala-api:
    image: ghcr.io/robitalhazmi/arthala-api:latest
    mem_limit: 450m
    command: uvicorn apps.api.app.main:app --workers 1
  arthala-worker:
    image: ghcr.io/robitalhazmi/arthala-worker:latest
    mem_limit: 250m
  grafana-alloy:
    image: grafana/alloy:latest
    mem_limit: 100m
""",
        encoding="utf-8",
    )

    meminfo_2g_swap = "MemTotal: 2097152 kB\nSwapTotal: 2097152 kB\n"
    report = run_vps_audit(
        meminfo_text=meminfo_2g_swap,
        compose_path=compose_file,
        require_swap=True,
    )
    assert report.swap_ok is True
    assert report.compose_ok is True
    assert report.total_allocated_mib == 800
    assert report.headroom_mib == 748
    assert report.violations == ()

    # Fail when swap is required and missing
    report_no_swap = run_vps_audit(
        meminfo_text="MemTotal: 2097152 kB\nSwapTotal: 0 kB\n",
        compose_path=compose_file,
        require_swap=True,
    )
    assert report_no_swap.swap_ok is False
    assert len(report_no_swap.violations) == 1

    # CLI entrypoint with default baseline (no compose file on disk yet)
    assert main(["--compose-file", str(tmp_path / "nonexistent.yml")]) == 0
