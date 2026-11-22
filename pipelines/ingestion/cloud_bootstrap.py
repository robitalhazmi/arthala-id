"""Cloud & local sandbox provisioning and verification runner (Task 0.4).

Automates and verifies the 6 free-tier services in ``docs/Task.md`` §0.4:
1. Cloudflare R2 bucket (``arthala-lakehouse``) + API token S3 probe.
2. Neon Postgres project (``arthala-prod``) + idempotent ``infra/sql/ddl.sql`` execution
   and verification of all 8 serving tables (no host ``psql`` binary required).
3. Aiven for Apache Kafka free tier service + topic contract & TLS/TCP check
   (``idx.ticks.raw``, ``idx.disclosures.raw``, ``alerts.dispatch``,
   ``user.subscription.updated``).
4. Grafana Cloud free stack remote-write metrics/logs credentials check.
5. Telegram Bot (``@ArthalaIDBot``) token & ``getMe`` verification.
6. Mayar.id QRIS (``Arthala ID Pro`` Rp39.000/mo Payment Link + HMAC webhook secret).
"""

from __future__ import annotations

import argparse
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlparse

import boto3
import httpx
import psycopg
import structlog
from dotenv import dotenv_values
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

DEFAULT_DDL_PATH = Path(__file__).resolve().parents[2] / "infra" / "sql" / "ddl.sql"

EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "dim_tickers",
        "mart_screener_latest",
        "fct_quarterly_financials",
        "fct_dividend_calendar",
        "users",
        "watchlists",
        "payment_transactions",
        "pipeline_runs",
    }
)

EXPECTED_KAFKA_TOPICS: tuple[str, ...] = (
    "idx.ticks.raw",
    "idx.disclosures.raw",
    "alerts.dispatch",
    "user.subscription.updated",
)


class CloudProvisionSettings(BaseSettings):
    """Settings across all 6 Task 0.4 free cloud services and local sandbox defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="local", alias="APP_ENV")

    # 1. Cloudflare R2 / MinIO
    r2_endpoint_url: str = Field(default="http://localhost:9000", alias="R2_ENDPOINT_URL")
    r2_access_key_id: str = Field(default="arthala", alias="R2_ACCESS_KEY_ID")
    r2_secret_access_key: str = Field(default="password", alias="R2_SECRET_ACCESS_KEY")
    r2_bucket: str = Field(default="arthala-lakehouse", alias="R2_BUCKET")
    r2_region: str = Field(default="auto", alias="R2_REGION")

    # 2. Neon Lakebase Postgres / Local PG16+
    # Per neon-postgres skill: use pooled DATABASE_URL for app queries and
    # direct DATABASE_URL_UNPOOLED for schema migrations (infra/sql/ddl.sql).
    database_url: str = Field(
        default="postgresql://arthala:password@localhost:5432/arthala",
        alias="DATABASE_URL",
    )
    database_url_unpooled: str = Field(
        default="",
        alias="DATABASE_URL_UNPOOLED",
    )
    neon_branch: str = Field(default="", alias="NEON_BRANCH")
    neon_branch_id: str = Field(default="", alias="NEON_BRANCH_ID")
    neon_function_host_suffix: str = Field(default="", alias="NEON_FUNCTION_HOST_SUFFIX")

    # 3. Aiven Kafka / Local Redpanda
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_ssl_ca: str = Field(default="", alias="KAFKA_SSL_CA")
    kafka_ssl_cert: str = Field(default="", alias="KAFKA_SSL_CERT")
    kafka_ssl_key: str = Field(default="", alias="KAFKA_SSL_KEY")
    kafka_topic_ticks: str = Field(default="idx.ticks.raw", alias="KAFKA_TOPIC_TICKS")
    kafka_topic_disclosures: str = Field(
        default="idx.disclosures.raw", alias="KAFKA_TOPIC_DISCLOSURES"
    )
    kafka_topic_alerts: str = Field(default="alerts.dispatch", alias="KAFKA_TOPIC_ALERTS")
    kafka_topic_subscriptions: str = Field(
        default="user.subscription.updated", alias="KAFKA_TOPIC_SUBSCRIPTIONS"
    )

    # 4. Grafana Cloud Free
    grafana_prom_remote_write_url: str = Field(default="", alias="GRAFANA_PROM_REMOTE_WRITE_URL")
    grafana_prom_username: str = Field(default="", alias="GRAFANA_PROM_USERNAME")
    grafana_loki_url: str = Field(default="", alias="GRAFANA_LOKI_URL")
    grafana_loki_username: str = Field(default="", alias="GRAFANA_LOKI_USERNAME")
    grafana_cloud_api_token: str = Field(default="", alias="GRAFANA_CLOUD_API_TOKEN")

    # 5. Telegram Bot (@ArthalaIDBot)
    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")

    # 6. Mayar.id QRIS
    mayar_webhook_secret: str = Field(default="", alias="MAYAR_WEBHOOK_SECRET")
    mayar_pro_payment_url: str = Field(default="", alias="MAYAR_PRO_PAYMENT_URL")


class R2BucketClientProtocol(Protocol):
    """Minimal boto3 S3 client protocol for bucket verification & creation."""

    def head_bucket(self, *, Bucket: str) -> Any: ...  # noqa: N803

    def create_bucket(self, *, Bucket: str) -> Any: ...  # noqa: N803

    def list_objects_v2(self, *, Bucket: str, MaxKeys: int) -> Any: ...  # noqa: N803


@dataclass(frozen=True, slots=True)
class ServiceCheckResult:
    """Verification outcome for a single cloud/sandbox dependency."""

    service: str
    status: Literal["READY", "LOCAL_ONLY", "MISSING_CREDENTIALS", "ERROR"]
    is_cloud_endpoint: bool
    detail: str


def _is_localhost_url(url_or_host: str) -> bool:
    lowered = url_or_host.lower()
    return "localhost" in lowered or "127.0.0.1" in lowered


def verify_r2_bucket(
    settings: CloudProvisionSettings,
    *,
    s3_client: R2BucketClientProtocol | None = None,
    create_if_missing: bool = False,
) -> ServiceCheckResult:
    """Verify Cloudflare R2 / MinIO bucket accessibility and object read permissions."""
    is_cloud = not _is_localhost_url(settings.r2_endpoint_url)
    client: R2BucketClientProtocol = s3_client or boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint_url,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        region_name=settings.r2_region,
    )
    try:
        try:
            client.head_bucket(Bucket=settings.r2_bucket)
        except Exception:
            if not create_if_missing:
                raise
            client.create_bucket(Bucket=settings.r2_bucket)
        client.list_objects_v2(Bucket=settings.r2_bucket, MaxKeys=1)
    except Exception as exc:
        return ServiceCheckResult(
            service="cloudflare_r2",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Bucket '{settings.r2_bucket}' check failed: {exc}",
        )

    status: Literal["READY", "LOCAL_ONLY"] = "READY" if is_cloud else "LOCAL_ONLY"
    return ServiceCheckResult(
        service="cloudflare_r2",
        status=status,
        is_cloud_endpoint=is_cloud,
        detail=f"Bucket '{settings.r2_bucket}' reachable at {settings.r2_endpoint_url}",
    )


def apply_and_verify_postgres_ddl(
    settings: CloudProvisionSettings,
    *,
    ddl_path: Path = DEFAULT_DDL_PATH,
    apply_ddl: bool = True,
) -> ServiceCheckResult:
    """Apply ``infra/sql/ddl.sql`` idempotently and verify all 8 serving tables exist.

    Follows the ``neon-postgres`` skill best practice:
    - Schema migrations (``ddl.sql``) run over ``DATABASE_URL_UNPOOLED`` (direct connection)
      when available.
    - Serving table verification runs over ``DATABASE_URL`` (pooled ``-pooler`` connection).
    """
    is_cloud = not _is_localhost_url(settings.database_url)
    if not ddl_path.exists():
        return ServiceCheckResult(
            service="neon_postgres",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"DDL file not found at {ddl_path}",
        )

    ddl_sql = ddl_path.read_text(encoding="utf-8")
    migration_url = settings.database_url_unpooled.strip() or settings.database_url
    try:
        if apply_ddl:
            with (
                psycopg.connect(migration_url, autocommit=True) as mig_conn,
                mig_conn.cursor() as cur,
            ):
                cur.execute(ddl_sql)

        with (
            psycopg.connect(settings.database_url, autocommit=True) as app_conn,
            app_conn.cursor() as cur,
        ):
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
            existing = {str(row[0]) for row in cur.fetchall()}
    except Exception as exc:
        return ServiceCheckResult(
            service="neon_postgres",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Postgres connection/DDL check failed: {exc}",
        )

    missing = sorted(EXPECTED_TABLES - existing)
    if missing:
        return ServiceCheckResult(
            service="neon_postgres",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Missing tables in Postgres: {', '.join(missing)}",
        )

    status: Literal["READY", "LOCAL_ONLY"] = "READY" if is_cloud else "LOCAL_ONLY"
    branch_info = f" (branch={settings.neon_branch})" if settings.neon_branch else ""
    return ServiceCheckResult(
        service="neon_postgres",
        status=status,
        is_cloud_endpoint=is_cloud,
        detail=f"All {len(EXPECTED_TABLES)} serving tables verified{branch_info}",
    )


def verify_kafka_broker(
    settings: CloudProvisionSettings,
    *,
    probe_tcp: bool = True,
    timeout_seconds: float = 3.0,
) -> ServiceCheckResult:
    """Verify Aiven Kafka / local Redpanda bootstrap servers, SSL config, and 4 topics."""
    first_broker = settings.kafka_bootstrap_servers.split(",")[0].strip()
    is_cloud = not _is_localhost_url(first_broker)

    configured_topics = (
        settings.kafka_topic_ticks,
        settings.kafka_topic_disclosures,
        settings.kafka_topic_alerts,
        settings.kafka_topic_subscriptions,
    )
    if set(configured_topics) != set(EXPECTED_KAFKA_TOPICS):
        return ServiceCheckResult(
            service="aiven_kafka",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Configured topics {configured_topics} do not match {EXPECTED_KAFKA_TOPICS}",
        )

    if settings.kafka_security_protocol.upper() == "SSL" and not (
        settings.kafka_ssl_ca and settings.kafka_ssl_cert and settings.kafka_ssl_key
    ):
        return ServiceCheckResult(
            service="aiven_kafka",
            status="MISSING_CREDENTIALS",
            is_cloud_endpoint=is_cloud,
            detail=(
                "KAFKA_SECURITY_PROTOCOL=SSL requires KAFKA_SSL_CA, KAFKA_SSL_CERT, KAFKA_SSL_KEY"
            ),
        )

    if ":" not in first_broker:
        return ServiceCheckResult(
            service="aiven_kafka",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Invalid KAFKA_BOOTSTRAP_SERVERS entry '{first_broker}' (expected host:port)",
        )

    host, port_str = first_broker.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError:
        return ServiceCheckResult(
            service="aiven_kafka",
            status="ERROR",
            is_cloud_endpoint=is_cloud,
            detail=f"Invalid port in KAFKA_BOOTSTRAP_SERVERS '{first_broker}'",
        )

    if probe_tcp:
        try:
            with socket.create_connection((host, port), timeout=timeout_seconds):
                pass
        except OSError as exc:
            return ServiceCheckResult(
                service="aiven_kafka",
                status="ERROR",
                is_cloud_endpoint=is_cloud,
                detail=f"Broker {host}:{port} unreachable: {exc}",
            )

    status: Literal["READY", "LOCAL_ONLY"] = "READY" if is_cloud else "LOCAL_ONLY"
    proto = settings.kafka_security_protocol
    return ServiceCheckResult(
        service="aiven_kafka",
        status=status,
        is_cloud_endpoint=is_cloud,
        detail=f"Broker {host}:{port} ({proto}) & 4 topic names verified",
    )


def verify_grafana_cloud(settings: CloudProvisionSettings) -> ServiceCheckResult:
    """Verify Grafana Cloud Prometheus Remote Write & Loki credentials."""
    required = {
        "GRAFANA_PROM_REMOTE_WRITE_URL": settings.grafana_prom_remote_write_url,
        "GRAFANA_PROM_USERNAME": settings.grafana_prom_username,
        "GRAFANA_LOKI_URL": settings.grafana_loki_url,
        "GRAFANA_LOKI_USERNAME": settings.grafana_loki_username,
        "GRAFANA_CLOUD_API_TOKEN": settings.grafana_cloud_api_token,
    }
    missing = [name for name, val in required.items() if not val.strip()]
    if missing:
        return ServiceCheckResult(
            service="grafana_cloud",
            status="MISSING_CREDENTIALS",
            is_cloud_endpoint=False,
            detail=f"Missing env vars: {', '.join(missing)}",
        )

    for label, url in (
        ("GRAFANA_PROM_REMOTE_WRITE_URL", settings.grafana_prom_remote_write_url),
        ("GRAFANA_LOKI_URL", settings.grafana_loki_url),
    ):
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc:
            return ServiceCheckResult(
                service="grafana_cloud",
                status="ERROR",
                is_cloud_endpoint=True,
                detail=f"{label} must be a valid https:// URL",
            )

    return ServiceCheckResult(
        service="grafana_cloud",
        status="READY",
        is_cloud_endpoint=True,
        detail="Grafana Cloud Prometheus & Loki remote-write endpoints configured",
    )


def verify_telegram_bot(
    settings: CloudProvisionSettings,
    *,
    http_client: httpx.Client | None = None,
    probe_api: bool = False,
) -> ServiceCheckResult:
    """Verify ``@ArthalaIDBot`` token format and optional Telegram ``getMe`` probe."""
    token = settings.telegram_bot_token.strip()
    if not token:
        return ServiceCheckResult(
            service="telegram_bot",
            status="MISSING_CREDENTIALS",
            is_cloud_endpoint=False,
            detail="TELEGRAM_BOT_TOKEN is not set",
        )

    parts = token.split(":", 1)
    if len(parts) != 2 or not parts[0].isdigit() or len(parts[1]) < 20:
        return ServiceCheckResult(
            service="telegram_bot",
            status="ERROR",
            is_cloud_endpoint=True,
            detail="TELEGRAM_BOT_TOKEN does not match expected '<bot_id>:<secret>' format",
        )

    if probe_api:
        close_client = http_client is None
        client = http_client or httpx.Client(timeout=5.0)
        try:
            resp = client.get(f"https://api.telegram.org/bot{token}/getMe")
            data = resp.json()
            if resp.status_code != 200 or not data.get("ok"):
                return ServiceCheckResult(
                    service="telegram_bot",
                    status="ERROR",
                    is_cloud_endpoint=True,
                    detail=f"Telegram getMe failed with status {resp.status_code}",
                )
            username = data.get("result", {}).get("username", "unknown")
            return ServiceCheckResult(
                service="telegram_bot",
                status="READY",
                is_cloud_endpoint=True,
                detail=f"Verified Telegram bot @{username}",
            )
        except Exception as exc:
            return ServiceCheckResult(
                service="telegram_bot",
                status="ERROR",
                is_cloud_endpoint=True,
                detail=f"Telegram getMe probe error: {exc}",
            )
        finally:
            if close_client:
                client.close()

    return ServiceCheckResult(
        service="telegram_bot",
        status="READY",
        is_cloud_endpoint=True,
        detail="TELEGRAM_BOT_TOKEN format verified",
    )


def verify_mayar_qris(settings: CloudProvisionSettings) -> ServiceCheckResult:
    """Verify Mayar.id QRIS Pro Payment Link and webhook HMAC secret."""
    secret = settings.mayar_webhook_secret.strip()
    url = settings.mayar_pro_payment_url.strip()
    missing: list[str] = []
    if not secret:
        missing.append("MAYAR_WEBHOOK_SECRET")
    if not url:
        missing.append("MAYAR_PRO_PAYMENT_URL")
    if missing:
        return ServiceCheckResult(
            service="mayar_qris",
            status="MISSING_CREDENTIALS",
            is_cloud_endpoint=False,
            detail=f"Missing env vars: {', '.join(missing)}",
        )

    if len(secret) < 16:
        return ServiceCheckResult(
            service="mayar_qris",
            status="ERROR",
            is_cloud_endpoint=True,
            detail="MAYAR_WEBHOOK_SECRET must be at least 16 characters",
        )

    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc:
        return ServiceCheckResult(
            service="mayar_qris",
            status="ERROR",
            is_cloud_endpoint=True,
            detail="MAYAR_PRO_PAYMENT_URL must be a valid https:// Mayar payment link",
        )

    return ServiceCheckResult(
        service="mayar_qris",
        status="READY",
        is_cloud_endpoint=True,
        detail=f"Mayar.id payment link ({parsed.netloc}) & webhook secret configured",
    )


def run_all_checks(
    settings: CloudProvisionSettings | None = None,
    *,
    apply_ddl: bool = True,
    ensure_r2_bucket: bool = False,
    probe_kafka_tcp: bool = True,
    probe_telegram_api: bool = False,
    s3_client: R2BucketClientProtocol | None = None,
    http_client: httpx.Client | None = None,
) -> list[ServiceCheckResult]:
    """Run all 6 Task 0.4 service checks and log structured results."""
    cfg = settings or CloudProvisionSettings()
    results = [
        verify_r2_bucket(cfg, s3_client=s3_client, create_if_missing=ensure_r2_bucket),
        apply_and_verify_postgres_ddl(cfg, apply_ddl=apply_ddl),
        verify_kafka_broker(cfg, probe_tcp=probe_kafka_tcp),
        verify_grafana_cloud(cfg),
        verify_telegram_bot(cfg, http_client=http_client, probe_api=probe_telegram_api),
        verify_mayar_qris(cfg),
    ]
    for item in results:
        logger.info(
            "cloud_bootstrap.service_checked",
            service=item.service,
            status=item.status,
            is_cloud_endpoint=item.is_cloud_endpoint,
            detail=item.detail,
        )
    return results


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for ``python -m pipelines.ingestion.cloud_bootstrap``."""
    parser = argparse.ArgumentParser(
        description=(
            "Verify and bootstrap Arthala ID local sandbox or free cloud services (Task 0.4)."
        ),
    )
    parser.add_argument(
        "--env",
        choices=("local", "prod"),
        default="local",
        help=(
            "Target validation mode: 'local' requires R2/Postgres/Kafka; "
            "'prod' requires all 6 cloud services."
        ),
    )
    parser.add_argument(
        "--no-apply-ddl",
        action="store_true",
        help="Skip executing infra/sql/ddl.sql before verifying Postgres tables.",
    )
    parser.add_argument(
        "--ensure-bucket",
        action="store_true",
        help="Create the R2/MinIO bucket if it does not exist yet.",
    )
    parser.add_argument(
        "--probe-telegram",
        action="store_true",
        help="Call Telegram getMe API to verify TELEGRAM_BOT_TOKEN live.",
    )
    args = parser.parse_args(argv)

    if args.env == "prod":
        overrides: dict[str, Any] = {}
        for candidate in (Path(".env.local"), Path(".env.prod")):
            if candidate.exists():
                for key, val in dotenv_values(candidate).items():
                    if val is not None and val != "":
                        overrides[key] = val
        settings = (
            CloudProvisionSettings.model_validate(overrides)
            if overrides
            else CloudProvisionSettings()
        )
    else:
        settings = CloudProvisionSettings()

    results = run_all_checks(
        settings=settings,
        apply_ddl=not args.no_apply_ddl,
        ensure_r2_bucket=args.ensure_bucket,
        probe_telegram_api=args.probe_telegram,
    )

    if args.env == "prod":
        failed = [r for r in results if r.status != "READY"]
    else:
        core_services = {"cloudflare_r2", "neon_postgres", "aiven_kafka"}
        failed = [
            r
            for r in results
            if r.service in core_services and r.status not in {"READY", "LOCAL_ONLY"}
        ]

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
