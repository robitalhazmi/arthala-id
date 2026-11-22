"""Offline unit tests for Task 0.4 cloud & local sandbox provisioning verifier."""

from __future__ import annotations

import socket
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest

from pipelines.ingestion.cloud_bootstrap import (
    EXPECTED_TABLES,
    CloudProvisionSettings,
    apply_and_verify_postgres_ddl,
    main,
    run_all_checks,
    verify_grafana_cloud,
    verify_kafka_broker,
    verify_mayar_qris,
    verify_r2_bucket,
    verify_telegram_bot,
)


class FakeBucketS3Client:
    """In-memory S3 bucket verifier stub."""

    def __init__(self, *, bucket_exists: bool = True, fail_list: bool = False) -> None:
        self.bucket_exists = bucket_exists
        self.fail_list = fail_list
        self.created_buckets: list[str] = []

    def head_bucket(self, *, Bucket: str) -> dict[str, Any]:  # noqa: N803
        if not self.bucket_exists:
            raise RuntimeError(f"Bucket {Bucket} does not exist")
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def create_bucket(self, *, Bucket: str) -> dict[str, Any]:  # noqa: N803
        self.bucket_exists = True
        self.created_buckets.append(Bucket)
        return {"Location": f"/{Bucket}"}

    def list_objects_v2(self, *, Bucket: str, MaxKeys: int) -> dict[str, Any]:  # noqa: N803
        if self.fail_list:
            raise RuntimeError("AccessDenied")
        return {"Name": Bucket, "MaxKeys": MaxKeys, "Contents": []}


def test_verify_r2_bucket_local_and_cloud_and_create_if_missing() -> None:
    local_cfg = CloudProvisionSettings(
        R2_ENDPOINT_URL="http://localhost:9000",
        R2_BUCKET="arthala-lakehouse",
    )
    local_res = verify_r2_bucket(local_cfg, s3_client=FakeBucketS3Client())
    assert local_res.status == "LOCAL_ONLY"
    assert not local_res.is_cloud_endpoint

    cloud_cfg = CloudProvisionSettings(
        R2_ENDPOINT_URL="https://acct123.r2.cloudflarestorage.com",
        R2_BUCKET="arthala-lakehouse",
    )
    fake_s3 = FakeBucketS3Client(bucket_exists=False)
    cloud_res = verify_r2_bucket(cloud_cfg, s3_client=fake_s3, create_if_missing=True)
    assert cloud_res.status == "READY"
    assert cloud_res.is_cloud_endpoint
    assert fake_s3.created_buckets == ["arthala-lakehouse"]

    err_res = verify_r2_bucket(
        cloud_cfg,
        s3_client=FakeBucketS3Client(bucket_exists=False),
        create_if_missing=False,
    )
    assert err_res.status == "ERROR"


def test_apply_and_verify_postgres_ddl_success_and_missing_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executed_queries: list[str] = []
    connected_urls: list[str] = []
    returned_tables: set[str] = set(EXPECTED_TABLES)

    class _FakeCursor:
        def __enter__(self) -> _FakeCursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> None:
            executed_queries.append(query)

        def fetchall(self) -> list[tuple[str]]:
            return [(t,) for t in sorted(returned_tables)]

    class _FakeConn:
        def __enter__(self) -> _FakeConn:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

    def _fake_connect(conninfo: str, *, autocommit: bool = False) -> _FakeConn:
        assert autocommit is True
        connected_urls.append(conninfo)
        return _FakeConn()

    monkeypatch.setattr(psycopg, "connect", _fake_connect)

    cfg = CloudProvisionSettings(
        DATABASE_URL="postgresql://user@ep-arthala-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require",
        DATABASE_URL_UNPOOLED="postgresql://user@ep-arthala.ap-southeast-1.aws.neon.tech/neondb?sslmode=require",
        NEON_BRANCH="production",
    )
    res = apply_and_verify_postgres_ddl(cfg, apply_ddl=True)
    assert res.status == "READY"
    assert res.is_cloud_endpoint
    assert "branch=production" in res.detail
    assert len(executed_queries) == 2
    # First connection uses unpooled URL for DDL migration; second uses pooled URL for verification
    assert "-pooler" not in connected_urls[0]
    assert "-pooler" in connected_urls[1]

    # Missing table case
    returned_tables.remove("pipeline_runs")
    res_missing = apply_and_verify_postgres_ddl(cfg, apply_ddl=False)
    assert res_missing.status == "ERROR"
    assert "pipeline_runs" in res_missing.detail

    # Missing DDL file case
    res_no_file = apply_and_verify_postgres_ddl(cfg, ddl_path=tmp_path / "missing.sql")
    assert res_no_file.status == "ERROR"


def test_apply_and_verify_postgres_ddl_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fail_connect(*args: Any, **kwargs: Any) -> Any:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", _fail_connect)
    cfg = CloudProvisionSettings()
    res = apply_and_verify_postgres_ddl(cfg)
    assert res.status == "ERROR"
    assert "connection refused" in res.detail


def test_verify_kafka_broker_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DummySocket:
        def __enter__(self) -> _DummySocket:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda addr, timeout=3.0: _DummySocket(),
    )

    local_cfg = CloudProvisionSettings(
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        KAFKA_SECURITY_PROTOCOL="PLAINTEXT",
    )
    res_local = verify_kafka_broker(local_cfg)
    assert res_local.status == "LOCAL_ONLY"

    ssl_missing_cfg = CloudProvisionSettings(
        KAFKA_BOOTSTRAP_SERVERS="kafka-arthala.aivencloud.com:12345",
        KAFKA_SECURITY_PROTOCOL="SSL",
    )
    res_ssl_missing = verify_kafka_broker(ssl_missing_cfg, probe_tcp=False)
    assert res_ssl_missing.status == "MISSING_CREDENTIALS"

    ssl_ok_cfg = CloudProvisionSettings(
        KAFKA_BOOTSTRAP_SERVERS="kafka-arthala.aivencloud.com:12345",
        KAFKA_SECURITY_PROTOCOL="SSL",
        KAFKA_SSL_CA="ca-cert",
        KAFKA_SSL_CERT="client-cert",
        KAFKA_SSL_KEY="client-key",
    )
    res_ssl_ok = verify_kafka_broker(ssl_ok_cfg, probe_tcp=True)
    assert res_ssl_ok.status == "READY"

    bad_topic_cfg = CloudProvisionSettings(KAFKA_TOPIC_TICKS="wrong.topic")
    assert verify_kafka_broker(bad_topic_cfg, probe_tcp=False).status == "ERROR"

    no_port_cfg = CloudProvisionSettings(KAFKA_BOOTSTRAP_SERVERS="localhost")
    assert verify_kafka_broker(no_port_cfg, probe_tcp=False).status == "ERROR"

    bad_port_cfg = CloudProvisionSettings(KAFKA_BOOTSTRAP_SERVERS="localhost:notaport")
    assert verify_kafka_broker(bad_port_cfg, probe_tcp=False).status == "ERROR"

    def _fail_socket(*args: Any, **kwargs: Any) -> Any:
        raise OSError("timeout")

    monkeypatch.setattr(socket, "create_connection", _fail_socket)
    assert verify_kafka_broker(local_cfg, probe_tcp=True).status == "ERROR"


def test_verify_grafana_cloud_validation() -> None:
    empty_cfg = CloudProvisionSettings()
    assert verify_grafana_cloud(empty_cfg).status == "MISSING_CREDENTIALS"

    bad_url_cfg = CloudProvisionSettings(
        GRAFANA_PROM_REMOTE_WRITE_URL="http://insecure.example.com/api/prom/push",
        GRAFANA_PROM_USERNAME="12345",
        GRAFANA_LOKI_URL="https://logs-prod.grafana.net/loki/api/v1/push",
        GRAFANA_LOKI_USERNAME="67890",
        GRAFANA_CLOUD_API_TOKEN="glc_" + ("x" * 24),
    )
    assert verify_grafana_cloud(bad_url_cfg).status == "ERROR"

    ok_cfg = CloudProvisionSettings(
        GRAFANA_PROM_REMOTE_WRITE_URL="https://prometheus-prod.grafana.net/api/prom/push",
        GRAFANA_PROM_USERNAME="12345",
        GRAFANA_LOKI_URL="https://logs-prod.grafana.net/loki/api/v1/push",
        GRAFANA_LOKI_USERNAME="67890",
        GRAFANA_CLOUD_API_TOKEN="glc_" + ("x" * 24),
    )
    assert verify_grafana_cloud(ok_cfg).status == "READY"


def test_verify_telegram_bot_format_and_live_probe() -> None:
    assert verify_telegram_bot(CloudProvisionSettings()).status == "MISSING_CREDENTIALS"

    bad_cfg = CloudProvisionSettings(TELEGRAM_BOT_TOKEN="invalid-format")
    assert verify_telegram_bot(bad_cfg).status == "ERROR"

    synthetic_token = "123456789:" + ("a" * 35)
    ok_cfg = CloudProvisionSettings(TELEGRAM_BOT_TOKEN=synthetic_token)
    assert verify_telegram_bot(ok_cfg, probe_api=False).status == "READY"

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getMe"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"username": "ArthalaIDBot"}},
            )
        return httpx.Response(404, json={"ok": False})

    client = httpx.Client(transport=httpx.MockTransport(_handler))
    probed = verify_telegram_bot(ok_cfg, http_client=client, probe_api=True)
    assert probed.status == "READY"
    assert "@ArthalaIDBot" in probed.detail

    fail_client = httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(401, json={"ok": False}))
    )
    assert verify_telegram_bot(ok_cfg, http_client=fail_client, probe_api=True).status == "ERROR"


def test_verify_mayar_qris_validation() -> None:
    assert verify_mayar_qris(CloudProvisionSettings()).status == "MISSING_CREDENTIALS"

    short_secret_cfg = CloudProvisionSettings(
        MAYAR_WEBHOOK_SECRET="short",
        MAYAR_PRO_PAYMENT_URL="https://mayar.id/arthala-id-pro",
    )
    assert verify_mayar_qris(short_secret_cfg).status == "ERROR"

    bad_url_cfg = CloudProvisionSettings(
        MAYAR_WEBHOOK_SECRET="w" * 24,
        MAYAR_PRO_PAYMENT_URL="http://mayar.id/arthala-id-pro",
    )
    assert verify_mayar_qris(bad_url_cfg).status == "ERROR"

    ok_cfg = CloudProvisionSettings(
        MAYAR_WEBHOOK_SECRET="w" * 24,
        MAYAR_PRO_PAYMENT_URL="https://mayar.id/arthala-id-pro",
    )
    assert verify_mayar_qris(ok_cfg).status == "READY"


def test_run_all_checks_and_cli_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "pipelines.ingestion.cloud_bootstrap.verify_r2_bucket",
        lambda cfg, **kw: verify_r2_bucket(cfg, s3_client=FakeBucketS3Client()),
    )
    monkeypatch.setattr(
        "pipelines.ingestion.cloud_bootstrap.apply_and_verify_postgres_ddl",
        lambda cfg, **kw: verify_mayar_qris(
            CloudProvisionSettings(
                MAYAR_WEBHOOK_SECRET="w" * 24,
                MAYAR_PRO_PAYMENT_URL="https://mayar.id/arthala-id-pro",
            )
        ),
    )
    monkeypatch.setattr(
        "pipelines.ingestion.cloud_bootstrap.verify_kafka_broker",
        lambda cfg, **kw: verify_kafka_broker(cfg, probe_tcp=False),
    )

    results = run_all_checks(probe_kafka_tcp=False)
    assert len(results) == 6
    assert main(["--env", "local", "--no-apply-ddl"]) == 0
    # In prod mode, missing optional cloud tokens (Grafana/Telegram/Mayar) returns exit code 1
    assert main(["--env", "prod", "--no-apply-ddl"]) == 1
