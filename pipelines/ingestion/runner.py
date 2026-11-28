"""Resilient Bronze ingestion runner, watermark tracker, and idempotent Parquet writer.

Implements Task 1.2c (``docs/Task.md``) & Section 5.1-5.2 of ``docs/Implementation_Plan.md``:
- Enforces local-only safety guardrails when ``--env local`` is passed.
- Tracks high-watermarks and records every execution in ``pipeline_runs``.
- Validates all 5 sources with Pandera contracts (routing bad rows to ``quarantine/``).
- Performs idempotent read-merge-deduplicate writes to Hive-partitioned Bronze Parquet:
  - ``bronze/idx_ohlcv/dt=YYYY-MM-DD/part-000.parquet``
  - ``bronze/idx_financials_raw/fiscal_year=YYYY/fiscal_quarter=Q{1-4}/part-000.parquet``
  - ``bronze/idx_dividends_raw/year=YYYY/part-000.parquet``
  - ``bronze/idx_disclosures_raw/dt=YYYY-MM-DD/part-000.parquet``
  - ``bronze/idx_special_monitoring/dt=YYYY-MM-DD/part-000.parquet``
"""

from __future__ import annotations

import argparse
import io
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Protocol

import boto3
import pandas as pd
import pandera.pandas as pa
import psycopg
import structlog
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import dotenv_values
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipelines.ingestion.contracts import (
    CorporateActionSchema,
    DisclosureSchema,
    FinancialReportSchema,
    PriceSchema,
    QuarantineWriter,
    R2StorageSettings,
    SpecialMonitoringSchema,
    validate_and_quarantine,
)
from pipelines.ingestion.fixtures import (
    build_fixture_disclosures,
    build_fixture_dividends,
    build_fixture_financials,
    build_fixture_ohlcv,
    build_fixture_special_monitoring,
)
from pipelines.ingestion.idx_collector import IdxCollector
from pipelines.ingestion.market_collector import (
    DEFAULT_IDX_TICKERS,
    MarketCollector,
    normalize_idx_ticker,
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class IngestionSettings(BaseSettings):
    """Configuration for the Bronze ingestion runner across local and cloud targets."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="local", alias="APP_ENV")
    database_url: str = Field(
        default="postgresql://arthala:password@localhost:5432/arthala",
        alias="DATABASE_URL",
    )
    r2_endpoint_url: str = Field(default="http://localhost:9000", alias="R2_ENDPOINT_URL")
    r2_access_key_id: str = Field(default="arthala", alias="R2_ACCESS_KEY_ID")
    r2_secret_access_key: str = Field(default="password", alias="R2_SECRET_ACCESS_KEY")
    r2_bucket: str = Field(default="arthala-lakehouse", alias="R2_BUCKET")
    r2_region: str = Field(default="auto", alias="R2_REGION")

    def to_r2_storage_settings(self) -> R2StorageSettings:
        """Convert to ``R2StorageSettings`` for ``QuarantineWriter``."""
        return R2StorageSettings(
            R2_ENDPOINT_URL=self.r2_endpoint_url,
            R2_ACCESS_KEY_ID=self.r2_access_key_id,
            R2_SECRET_ACCESS_KEY=self.r2_secret_access_key,
            R2_BUCKET=self.r2_bucket,
            R2_REGION=self.r2_region,
        )


class S3ReadWriteProtocol(Protocol):
    """S3 client protocol supporting both ``get_object`` and ``put_object``."""

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]: ...  # noqa: N803

    def put_object(
        self,
        *,
        Bucket: str,  # noqa: N803
        Key: str,  # noqa: N803
        Body: bytes,  # noqa: N803
        ContentType: str = ...,  # noqa: N803
    ) -> Any: ...


class PipelineRunStoreProtocol(Protocol):
    """Persistence interface for ``pipeline_runs`` watermarks and run status."""

    def get_latest_watermark(self, pipeline_name: str) -> str | None: ...

    def start_run(self, pipeline_name: str) -> str: ...

    def finish_run(
        self,
        run_id: str,
        *,
        status: Literal["SUCCESS", "PARTIAL", "FAILED"],
        rows_processed: int,
        rows_quarantined: int,
        watermark_value: str | None,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class SourceIngestionSummary:
    """Summary of ingesting a single Bronze dataset."""

    pipeline_name: str
    run_id: str
    status: Literal["SUCCESS", "PARTIAL", "FAILED"]
    rows_processed: int
    rows_valid: int
    rows_quarantined: int
    bronze_keys: tuple[str, ...]
    quarantine_key: str | None
    watermark_value: str | None


def assert_local_endpoints(settings: IngestionSettings) -> None:
    """Abort immediately if ``--env local`` points at non-localhost endpoints."""
    for label, url in (
        ("DATABASE_URL", settings.database_url),
        ("R2_ENDPOINT_URL", settings.r2_endpoint_url),
    ):
        lowered = url.lower()
        if "localhost" not in lowered and "127.0.0.1" not in lowered:
            raise RuntimeError(
                f"Refusing to run '--env local' because {label} points to a non-local host: {url}"
            )


class PostgresPipelineRunStore:
    """Stores watermarks and execution telemetry in Neon / local Postgres ``pipeline_runs``."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def get_latest_watermark(self, pipeline_name: str) -> str | None:
        query = """
            SELECT watermark_value
            FROM pipeline_runs
            WHERE pipeline_name = %s
              AND status IN ('SUCCESS', 'PARTIAL')
              AND watermark_value IS NOT NULL
            ORDER BY finished_at DESC NULLS LAST, started_at DESC
            LIMIT 1;
        """
        with psycopg.connect(self._database_url, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(query, (pipeline_name,))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] is not None else None

    def start_run(self, pipeline_name: str) -> str:
        query = """
            INSERT INTO pipeline_runs (pipeline_name, status)
            VALUES (%s, 'RUNNING')
            RETURNING run_id;
        """
        with psycopg.connect(self._database_url, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(query, (pipeline_name,))
            row = cur.fetchone()
            if row is None:
                return uuid.uuid4().hex
            return str(row[0])

    def finish_run(
        self,
        run_id: str,
        *,
        status: Literal["SUCCESS", "PARTIAL", "FAILED"],
        rows_processed: int,
        rows_quarantined: int,
        watermark_value: str | None,
    ) -> None:
        query = """
            UPDATE pipeline_runs
            SET status = %s,
                rows_processed = %s,
                rows_quarantined = %s,
                watermark_value = %s,
                finished_at = NOW()
            WHERE run_id = %s;
        """
        with psycopg.connect(self._database_url, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                query,
                (status, rows_processed, rows_quarantined, watermark_value, run_id),
            )


class BronzeParquetWriter:
    """Idempotent Hive-partitioned Bronze Parquet writer for B2 / R2 / MinIO."""

    def __init__(
        self,
        settings: R2StorageSettings | None = None,
        s3_client: S3ReadWriteProtocol | None = None,
    ) -> None:
        self._settings = settings or R2StorageSettings()
        self._s3_client = s3_client

    @property
    def bucket(self) -> str:
        """Return target Lakehouse S3 bucket name."""
        return self._settings.r2_bucket

    def _get_client(self) -> S3ReadWriteProtocol:
        if self._s3_client is None:
            boto_cfg = BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
            )
            client: S3ReadWriteProtocol = boto3.client(
                "s3",
                endpoint_url=self._settings.r2_endpoint_url,
                aws_access_key_id=self._settings.r2_access_key_id,
                aws_secret_access_key=self._settings.r2_secret_access_key,
                region_name=self._settings.r2_region,
                config=boto_cfg,
            )
            self._s3_client = client
        return self._s3_client

    def _read_existing_parquet(self, key: str) -> pd.DataFrame | None:
        try:
            obj = self._get_client().get_object(Bucket=self.bucket, Key=key)
            body_bytes: bytes = obj["Body"].read()
            return pd.read_parquet(io.BytesIO(body_bytes), engine="pyarrow")
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        except KeyError:
            return None

    def upsert_partition(
        self,
        df: pd.DataFrame,
        *,
        key: str,
        dedup_cols: Sequence[str],
    ) -> str:
        """Idempotently merge ``df`` into ``key``, deduplicating on ``dedup_cols``."""
        existing_df = self._read_existing_parquet(key)
        if existing_df is not None and not existing_df.empty:
            merged = pd.concat([existing_df, df], ignore_index=True)
        else:
            merged = df.reset_index(drop=True)

        present_keys = [c for c in dedup_cols if c in merged.columns]
        if present_keys:
            merged = merged.drop_duplicates(subset=present_keys, keep="last")
            merged = merged.sort_values(by=present_keys).reset_index(drop=True)

        buffer = io.BytesIO()
        merged.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        payload = buffer.getvalue()

        self._get_client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType="application/vnd.apache.parquet",
        )
        logger.info(
            "bronze_writer.partition_upserted",
            bucket=self.bucket,
            key=key,
            rows_total_in_partition=len(merged),
            bytes_written=len(payload),
        )
        return key

    def write_ohlcv(self, valid_df: pd.DataFrame) -> tuple[str, ...]:
        """Write ``bronze/idx_ohlcv/dt=YYYY-MM-DD/part-000.parquet`` partitions."""
        if valid_df.empty:
            return ()
        work = valid_df.copy()
        work["_dt"] = pd.to_datetime(work["trade_date"]).dt.strftime("%Y-%m-%d")
        keys: list[str] = []
        for dt_str, group in work.groupby("_dt", sort=True):
            part = group.drop(columns=["_dt"])
            key = f"bronze/idx_ohlcv/dt={dt_str}/part-000.parquet"
            keys.append(self.upsert_partition(part, key=key, dedup_cols=("ticker", "trade_date")))
        return tuple(keys)

    def write_financials(self, valid_df: pd.DataFrame) -> tuple[str, ...]:
        """Write ``bronze/idx_financials_raw/fiscal_year=YYYY/fiscal_quarter=Q*/...``."""
        if valid_df.empty:
            return ()
        keys: list[str] = []
        for (fy, fq), group in valid_df.groupby(["fiscal_year", "fiscal_quarter"], sort=True):
            fy_int = int(str(fy))
            fq_int = int(str(fq))
            prefix = f"bronze/idx_financials_raw/fiscal_year={fy_int}"
            key = f"{prefix}/fiscal_quarter=Q{fq_int}/part-000.parquet"
            keys.append(
                self.upsert_partition(
                    group,
                    key=key,
                    dedup_cols=("ticker", "fiscal_year", "fiscal_quarter"),
                )
            )
        return tuple(keys)

    def write_dividends(self, valid_df: pd.DataFrame) -> tuple[str, ...]:
        """Write ``bronze/idx_dividends_raw/year=YYYY/part-000.parquet`` partitions."""
        if valid_df.empty:
            return ()
        work = valid_df.copy()
        work["_year"] = pd.to_datetime(work["cum_date"]).dt.year
        keys: list[str] = []
        for yr, group in work.groupby("_year", sort=True):
            part = group.drop(columns=["_year"])
            yr_int = int(str(yr))
            key = f"bronze/idx_dividends_raw/year={yr_int}/part-000.parquet"
            keys.append(
                self.upsert_partition(
                    part,
                    key=key,
                    dedup_cols=("ticker", "cum_date", "action_type"),
                )
            )
        return tuple(keys)

    def write_disclosures(self, valid_df: pd.DataFrame) -> tuple[str, ...]:
        """Write ``bronze/idx_disclosures_raw/dt=YYYY-MM-DD/part-000.parquet`` partitions."""
        if valid_df.empty:
            return ()
        work = valid_df.copy()
        work["_dt"] = pd.to_datetime(work["published_at"]).dt.strftime("%Y-%m-%d")
        keys: list[str] = []
        for dt_str, group in work.groupby("_dt", sort=True):
            part = group.drop(columns=["_dt"])
            key = f"bronze/idx_disclosures_raw/dt={dt_str}/part-000.parquet"
            keys.append(self.upsert_partition(part, key=key, dedup_cols=("disclosure_id",)))
        return tuple(keys)

    def write_special_monitoring(self, valid_df: pd.DataFrame) -> tuple[str, ...]:
        """Write ``bronze/idx_special_monitoring/dt=YYYY-MM-DD/part-000.parquet`` partitions."""
        if valid_df.empty:
            return ()
        work = valid_df.copy()
        work["_dt"] = pd.to_datetime(work["as_of_date"]).dt.strftime("%Y-%m-%d")
        keys: list[str] = []
        for dt_str, group in work.groupby("_dt", sort=True):
            part = group.drop(columns=["_dt"])
            key = f"bronze/idx_special_monitoring/dt={dt_str}/part-000.parquet"
            keys.append(self.upsert_partition(part, key=key, dedup_cols=("ticker", "as_of_date")))
        return tuple(keys)


class IngestionRunner:
    """Coordinates multi-source collection, Pandera contracts, Bronze writes, and watermarks."""

    def __init__(
        self,
        *,
        run_store: PipelineRunStoreProtocol,
        bronze_writer: BronzeParquetWriter,
        quarantine_writer: QuarantineWriter,
        market_collector: MarketCollector | None = None,
        idx_collector: IdxCollector | None = None,
    ) -> None:
        self._run_store = run_store
        self._bronze_writer = bronze_writer
        self._quarantine_writer = quarantine_writer
        self._market = market_collector or MarketCollector()
        self._idx = idx_collector or IdxCollector()

    def _ingest_dataframe(
        self,
        df: pd.DataFrame,
        *,
        pipeline_name: str,
        schema: pa.DataFrameSchema,
        partition_date: date,
        watermark_value: str,
        write_fn: Any,
    ) -> SourceIngestionSummary:
        run_id = self._run_store.start_run(pipeline_name)
        try:
            validation = validate_and_quarantine(
                df,
                schema,
                source_name=pipeline_name,
                run_id=run_id,
                partition_date=partition_date,
                quarantine_writer=self._quarantine_writer,
            )
            bronze_keys: tuple[str, ...] = write_fn(validation.valid_df)
            status: Literal["SUCCESS", "PARTIAL", "FAILED"] = (
                "PARTIAL" if validation.rows_quarantined > 0 else "SUCCESS"
            )
            wm = watermark_value if validation.rows_valid > 0 else None
            self._run_store.finish_run(
                run_id,
                status=status,
                rows_processed=validation.rows_processed,
                rows_quarantined=validation.rows_quarantined,
                watermark_value=wm,
            )
            return SourceIngestionSummary(
                pipeline_name=pipeline_name,
                run_id=run_id,
                status=status,
                rows_processed=validation.rows_processed,
                rows_valid=validation.rows_valid,
                rows_quarantined=validation.rows_quarantined,
                bronze_keys=bronze_keys,
                quarantine_key=validation.quarantine_key,
                watermark_value=wm,
            )
        except Exception as exc:
            logger.error(
                "ingestion_runner.pipeline_failed",
                pipeline_name=pipeline_name,
                run_id=run_id,
                error=str(exc),
            )
            self._run_store.finish_run(
                run_id,
                status="FAILED",
                rows_processed=len(df),
                rows_quarantined=0,
                watermark_value=None,
            )
            raise

    def run_all(
        self,
        *,
        tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
        as_of_date: date | None = None,
        fiscal_year: int | None = None,
        fiscal_quarter: int | None = None,
        mode: Literal["auto", "live", "fixtures"] = "auto",
    ) -> list[SourceIngestionSummary]:
        """Run all 5 Bronze ingestion pipelines with watermark tracking."""
        resolved_date = as_of_date or datetime.now(tz=UTC).date()
        resolved_year = fiscal_year or resolved_date.year
        resolved_quarter = fiscal_quarter or max(1, min(4, (resolved_date.month - 1) // 3 + 1))
        norm_tickers = [normalize_idx_ticker(t) for t in tickers if normalize_idx_ticker(t)]

        ohlcv_wm = self._run_store.get_latest_watermark("idx_ohlcv")
        start_dt = (
            date.fromisoformat(ohlcv_wm[:10])
            if ohlcv_wm and len(ohlcv_wm) >= 10
            else (resolved_date - timedelta(days=7))
        )

        if mode == "fixtures":
            ohlcv_df = build_fixture_ohlcv(norm_tickers, as_of_date=resolved_date)
            div_df = build_fixture_dividends(norm_tickers, year=resolved_year)
            fin_df = build_fixture_financials(
                norm_tickers, fiscal_year=resolved_year, fiscal_quarter=resolved_quarter
            )
            disc_df = build_fixture_disclosures(norm_tickers, as_of_date=resolved_date)
            mon_df = build_fixture_special_monitoring(norm_tickers, as_of_date=resolved_date)
        elif mode == "live":
            ohlcv_df = self._market.collect_ohlcv(
                norm_tickers, start_date=start_dt, end_date=resolved_date
            )
            div_df = self._market.collect_dividends_and_splits(
                norm_tickers, start_date=date(resolved_year, 1, 1)
            )
            fin_df = self._idx.collect_quarterly_financials(
                norm_tickers, fiscal_year=resolved_year, fiscal_quarter=resolved_quarter
            )
            disc_df = self._idx.collect_disclosures(
                norm_tickers, from_date=start_dt, to_date=resolved_date
            )
            mon_df = self._idx.collect_special_monitoring(as_of_date=resolved_date)
        else:
            # auto mode: try live collection per source, falling back cleanly to fixtures
            # when running in an offline local sandbox.
            ohlcv_df = _try_or_fallback(
                "idx_ohlcv",
                lambda: self._market.collect_ohlcv(
                    norm_tickers, start_date=start_dt, end_date=resolved_date
                ),
                lambda: build_fixture_ohlcv(norm_tickers, as_of_date=resolved_date),
            )
            div_df = _try_or_fallback(
                "idx_dividends_raw",
                lambda: self._market.collect_dividends_and_splits(
                    norm_tickers, start_date=date(resolved_year, 1, 1)
                ),
                lambda: build_fixture_dividends(norm_tickers, year=resolved_year),
            )
            fin_df = _try_or_fallback(
                "idx_financials_raw",
                lambda: self._idx.collect_quarterly_financials(
                    norm_tickers, fiscal_year=resolved_year, fiscal_quarter=resolved_quarter
                ),
                lambda: build_fixture_financials(
                    norm_tickers, fiscal_year=resolved_year, fiscal_quarter=resolved_quarter
                ),
            )
            disc_df = _try_or_fallback(
                "idx_disclosures_raw",
                lambda: self._idx.collect_disclosures(
                    norm_tickers, from_date=start_dt, to_date=resolved_date
                ),
                lambda: build_fixture_disclosures(norm_tickers, as_of_date=resolved_date),
            )
            mon_df = _try_or_fallback(
                "idx_special_monitoring",
                lambda: self._idx.collect_special_monitoring(as_of_date=resolved_date),
                lambda: build_fixture_special_monitoring(norm_tickers, as_of_date=resolved_date),
            )

        iso_wm = resolved_date.isoformat()
        fin_wm = f"{resolved_year}-Q{resolved_quarter}"

        summaries = [
            self._ingest_dataframe(
                ohlcv_df,
                pipeline_name="idx_ohlcv",
                schema=PriceSchema,
                partition_date=resolved_date,
                watermark_value=iso_wm,
                write_fn=self._bronze_writer.write_ohlcv,
            ),
            self._ingest_dataframe(
                fin_df,
                pipeline_name="idx_financials_raw",
                schema=FinancialReportSchema,
                partition_date=resolved_date,
                watermark_value=fin_wm,
                write_fn=self._bronze_writer.write_financials,
            ),
            self._ingest_dataframe(
                div_df,
                pipeline_name="idx_dividends_raw",
                schema=CorporateActionSchema,
                partition_date=resolved_date,
                watermark_value=str(resolved_year),
                write_fn=self._bronze_writer.write_dividends,
            ),
            self._ingest_dataframe(
                disc_df,
                pipeline_name="idx_disclosures_raw",
                schema=DisclosureSchema,
                partition_date=resolved_date,
                watermark_value=iso_wm,
                write_fn=self._bronze_writer.write_disclosures,
            ),
            self._ingest_dataframe(
                mon_df,
                pipeline_name="idx_special_monitoring",
                schema=SpecialMonitoringSchema,
                partition_date=resolved_date,
                watermark_value=iso_wm,
                write_fn=self._bronze_writer.write_special_monitoring,
            ),
        ]
        for s in summaries:
            logger.info(
                "ingestion_runner.source_completed",
                pipeline_name=s.pipeline_name,
                run_id=s.run_id,
                status=s.status,
                rows_processed=s.rows_processed,
                rows_valid=s.rows_valid,
                rows_quarantined=s.rows_quarantined,
                bronze_keys=list(s.bronze_keys),
                watermark_value=s.watermark_value,
            )
        return summaries


def _try_or_fallback(
    source_name: str,
    live_fn: Any,
    fallback_fn: Any,
) -> pd.DataFrame:
    try:
        df: pd.DataFrame = live_fn()
        if not df.empty:
            return df
        logger.warning("ingestion_runner.empty_live_using_fixtures", source_name=source_name)
    except Exception as exc:
        logger.warning(
            "ingestion_runner.live_unreachable_using_fixtures",
            source_name=source_name,
            error=str(exc),
        )
    fallback_df: pd.DataFrame = fallback_fn()
    return fallback_df


def load_ingestion_settings(env_mode: Literal["local", "prod"]) -> IngestionSettings:
    """Load settings for ``local`` (``.env``) or ``prod`` (``.env.local`` + ``.env.prod``)."""
    if env_mode == "prod":
        overrides: dict[str, Any] = {}
        for candidate in (Path(".env.local"), Path(".env.prod")):
            if candidate.exists():
                for key, val in dotenv_values(candidate).items():
                    if val is not None and val != "":
                        overrides[key] = val
        if overrides:
            return IngestionSettings.model_validate(overrides)
    return IngestionSettings()


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for ``python -m pipelines.ingestion.runner``."""
    parser = argparse.ArgumentParser(
        description="Run Arthala ID multi-source IDX & market Bronze ingestion."
    )
    parser.add_argument(
        "--env",
        choices=("local", "prod"),
        default="local",
        help="Target environment ('local' enforces localhost safety guardrails).",
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "live", "fixtures"),
        default=None,
        help="Collection mode (defaults to 'auto' in local and 'live' in prod).",
    )
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_IDX_TICKERS),
        help="Comma-separated IDX tickers (e.g. BBCA,BBRI,ASII,TLKM).",
    )
    args = parser.parse_args(argv)

    env_mode: Literal["local", "prod"] = args.env
    settings = load_ingestion_settings(env_mode)
    if env_mode == "local":
        assert_local_endpoints(settings)

    resolved_mode: Literal["auto", "live", "fixtures"] = (
        args.mode if args.mode is not None else ("live" if env_mode == "prod" else "auto")
    )
    tickers = [t.strip() for t in str(args.tickers).split(",") if t.strip()]

    r2_settings = settings.to_r2_storage_settings()
    runner = IngestionRunner(
        run_store=PostgresPipelineRunStore(settings.database_url),
        bronze_writer=BronzeParquetWriter(settings=r2_settings),
        quarantine_writer=QuarantineWriter(settings=r2_settings),
    )
    runner.run_all(tickers=tickers, mode=resolved_mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
