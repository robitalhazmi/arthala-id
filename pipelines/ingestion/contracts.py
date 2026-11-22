"""Pandera data quality contracts and Cloudflare R2 Quarantine/DLQ writer.

Implements Section 5.1 & 5.2 of ``docs/Implementation_Plan.md``:
- ``PriceSchema`` (daily IDX OHLCV market rows)
- ``FinancialReportSchema`` (quarterly IDX fundamental financial statements)
- ``CorporateActionSchema`` (dividend & corporate action calendar rows)
- ``QuarantineWriter`` & ``validate_and_quarantine`` (routes malformed rows to
  ``r2://arthala-lakehouse/quarantine/{source}/dt=YYYY-MM-DD/run_{run_id}_errors.parquet``
  with failure metadata and increments ``ingestion_quarantine_rows_total``).
"""

from __future__ import annotations

import io
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

import boto3
import pandas as pd
import pandera.pandas as pa
import structlog
from prometheus_client import Counter
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

IDX_TICKER_REGEX = r"^[A-Z]{4}$"

INGESTION_QUARANTINE_ROWS_TOTAL = Counter(
    "ingestion_quarantine_rows_total",
    "Total malformed ingestion rows routed to Cloudflare R2 Quarantine/DLQ",
    labelnames=("source", "schema"),
)


class R2StorageSettings(BaseSettings):
    """Cloudflare R2 / local MinIO S3-compatible object storage configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    r2_endpoint_url: str = Field(default="http://localhost:9000", alias="R2_ENDPOINT_URL")
    r2_access_key_id: str = Field(default="arthala", alias="R2_ACCESS_KEY_ID")
    r2_secret_access_key: str = Field(default="password", alias="R2_SECRET_ACCESS_KEY")
    r2_bucket: str = Field(default="arthala-lakehouse", alias="R2_BUCKET")
    r2_region: str = Field(default="auto", alias="R2_REGION")


class S3ClientProtocol(Protocol):
    """Minimal S3 client protocol for uploading Parquet objects to R2/MinIO."""

    def put_object(
        self,
        *,
        Bucket: str,  # noqa: N803
        Key: str,  # noqa: N803
        Body: bytes,  # noqa: N803
        ContentType: str = ...,  # noqa: N803
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class ContractValidationResult:
    """Outcome of validating a raw ingestion batch against a Pandera contract."""

    source_name: str
    schema_name: str
    run_id: str
    valid_df: pd.DataFrame
    quarantine_df: pd.DataFrame
    rows_processed: int
    rows_valid: int
    rows_quarantined: int
    quarantine_key: str | None
    quarantine_uri: str | None


def _check_ohlc_bounds(df: pd.DataFrame) -> pd.Series[bool]:
    """Verify high >= max(open, close, low) and low <= min(open, close, high)."""
    required = {"open_price", "high_price", "low_price", "close_price"}
    if not required.issubset(df.columns):
        return pd.Series(False, index=df.index, dtype=bool)

    open_p = pd.to_numeric(df["open_price"], errors="coerce")
    high_p = pd.to_numeric(df["high_price"], errors="coerce")
    low_p = pd.to_numeric(df["low_price"], errors="coerce")
    close_p = pd.to_numeric(df["close_price"], errors="coerce")

    any_na = open_p.isna() | high_p.isna() | low_p.isna() | close_p.isna()
    bounds_ok = (
        (high_p >= low_p)
        & (high_p >= open_p)
        & (high_p >= close_p)
        & (low_p <= open_p)
        & (low_p <= close_p)
    )
    return (bounds_ok | any_na).astype(bool)


def _check_dividend_date_order(df: pd.DataFrame) -> pd.Series[bool]:
    """Verify ex_date >= cum_date and payment_date >= cum_date when present."""
    if "cum_date" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)

    cum = pd.to_datetime(df["cum_date"], errors="coerce")
    ok = pd.Series(True, index=df.index, dtype=bool)

    if "ex_date" in df.columns:
        ex = pd.to_datetime(df["ex_date"], errors="coerce")
        ok = ok & ((ex >= cum) | ex.isna() | cum.isna())

    if "recording_date" in df.columns:
        rec = pd.to_datetime(df["recording_date"], errors="coerce")
        ok = ok & ((rec >= cum) | rec.isna() | cum.isna())

    if "payment_date" in df.columns:
        pay = pd.to_datetime(df["payment_date"], errors="coerce")
        ok = ok & ((pay >= cum) | pay.isna() | cum.isna())

    return ok.astype(bool)


PriceSchema = pa.DataFrameSchema(
    name="PriceSchema",
    columns={
        "ticker": pa.Column(
            str,
            checks=pa.Check.str_matches(IDX_TICKER_REGEX),
            nullable=False,
            coerce=True,
        ),
        "trade_date": pa.Column(
            pa.DateTime,
            nullable=False,
            coerce=True,
        ),
        "open_price": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "high_price": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "low_price": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "close_price": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "volume": pa.Column(
            int,
            checks=pa.Check.ge(0),
            nullable=False,
            coerce=True,
        ),
        "adj_close": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "source": pa.Column(
            str,
            checks=pa.Check.isin(["yfinance", "idx", "replay"]),
            nullable=True,
            required=False,
            coerce=True,
        ),
    },
    checks=[
        pa.Check(
            _check_ohlc_bounds,
            name="ohlc_bounds_valid",
            error="high_price must be >= open/close/low and low_price must be <= open/close/high",
        )
    ],
    strict=False,
)


FinancialReportSchema = pa.DataFrameSchema(
    name="FinancialReportSchema",
    columns={
        "ticker": pa.Column(
            str,
            checks=pa.Check.str_matches(IDX_TICKER_REGEX),
            nullable=False,
            coerce=True,
        ),
        "fiscal_year": pa.Column(
            int,
            checks=pa.Check.in_range(2000, 2100),
            nullable=False,
            coerce=True,
        ),
        "fiscal_quarter": pa.Column(
            int,
            checks=pa.Check.isin([1, 2, 3, 4]),
            nullable=False,
            coerce=True,
        ),
        "filing_date": pa.Column(
            pa.DateTime,
            nullable=False,
            coerce=True,
        ),
        "revenue_idr": pa.Column(
            float,
            checks=pa.Check.ge(0),
            nullable=False,
            coerce=True,
        ),
        "net_income_idr": pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        "operating_cf_idr": pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        "total_assets_idr": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "total_liabilities_idr": pa.Column(
            float,
            checks=pa.Check.ge(0),
            nullable=False,
            coerce=True,
        ),
        "total_equity_idr": pa.Column(
            float,
            nullable=False,
            coerce=True,
        ),
        "short_term_debt_idr": pa.Column(
            float,
            checks=pa.Check.ge(0),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "cash_and_equivalents_idr": pa.Column(
            float,
            checks=pa.Check.ge(0),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "retained_earnings_idr": pa.Column(
            float,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "ebit_idr": pa.Column(
            float,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "working_capital_idr": pa.Column(
            float,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "eps": pa.Column(
            float,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "currency": pa.Column(
            str,
            checks=pa.Check.isin(["IDR", "USD"]),
            nullable=True,
            required=False,
            coerce=True,
        ),
    },
    strict=False,
)


CorporateActionSchema = pa.DataFrameSchema(
    name="CorporateActionSchema",
    columns={
        "ticker": pa.Column(
            str,
            checks=pa.Check.str_matches(IDX_TICKER_REGEX),
            nullable=False,
            coerce=True,
        ),
        "action_type": pa.Column(
            str,
            checks=pa.Check.isin(
                [
                    "CASH_DIVIDEND",
                    "STOCK_SPLIT",
                    "REVERSE_SPLIT",
                    "RIGHTS_ISSUE",
                    "BONUS_SHARE",
                ]
            ),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "fiscal_year": pa.Column(
            float,
            checks=pa.Check.in_range(2000, 2100),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "dividend_per_share": pa.Column(
            float,
            checks=pa.Check.gt(0),
            nullable=False,
            coerce=True,
        ),
        "currency": pa.Column(
            str,
            checks=pa.Check.isin(["IDR", "USD"]),
            nullable=True,
            required=False,
            coerce=True,
        ),
        "cum_date": pa.Column(
            pa.DateTime,
            nullable=False,
            coerce=True,
        ),
        "ex_date": pa.Column(
            pa.DateTime,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "recording_date": pa.Column(
            pa.DateTime,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "payment_date": pa.Column(
            pa.DateTime,
            nullable=True,
            required=False,
            coerce=True,
        ),
        "payout_ratio_pct": pa.Column(
            float,
            checks=pa.Check.in_range(0.0, 1000.0),
            nullable=True,
            required=False,
            coerce=True,
        ),
    },
    checks=[
        pa.Check(
            _check_dividend_date_order,
            name="dividend_date_order_valid",
            error="ex_date, recording_date, and payment_date must be >= cum_date",
        )
    ],
    strict=False,
)


class QuarantineWriter:
    """Writes quarantined/DLQ rows as Parquet to Cloudflare R2 / local MinIO."""

    def __init__(
        self,
        settings: R2StorageSettings | None = None,
        s3_client: S3ClientProtocol | None = None,
    ) -> None:
        self._settings = settings or R2StorageSettings()
        self._s3_client = s3_client

    @property
    def bucket(self) -> str:
        """Return target R2/MinIO bucket name."""
        return self._settings.r2_bucket

    def _get_client(self) -> S3ClientProtocol:
        if self._s3_client is None:
            client: S3ClientProtocol = boto3.client(
                "s3",
                endpoint_url=self._settings.r2_endpoint_url,
                aws_access_key_id=self._settings.r2_access_key_id,
                aws_secret_access_key=self._settings.r2_secret_access_key,
                region_name=self._settings.r2_region,
            )
            self._s3_client = client
        return self._s3_client

    @staticmethod
    def build_quarantine_key(source_name: str, partition_date: date, run_id: str) -> str:
        """Construct the canonical R2 quarantine Parquet key (§5.2)."""
        clean_source = source_name.strip("/").replace(" ", "_")
        dt_part = partition_date.isoformat()
        return f"quarantine/{clean_source}/dt={dt_part}/run_{run_id}_errors.parquet"

    def write_quarantine_dataframe(
        self,
        quarantine_df: pd.DataFrame,
        *,
        source_name: str,
        partition_date: date,
        run_id: str,
    ) -> tuple[str, str]:
        """Serialize ``quarantine_df`` to Parquet and upload to R2/MinIO.

        Returns ``(quarantine_key, quarantine_uri)``.
        """
        key = self.build_quarantine_key(
            source_name=source_name,
            partition_date=partition_date,
            run_id=run_id,
        )
        uri = f"r2://{self.bucket}/{key}"

        buffer = io.BytesIO()
        quarantine_df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        payload = buffer.getvalue()

        self._get_client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType="application/vnd.apache.parquet",
        )
        logger.warning(
            "ingestion.quarantine_written",
            source_name=source_name,
            run_id=run_id,
            rows_quarantined=len(quarantine_df),
            quarantine_key=key,
            quarantine_uri=uri,
            bytes_written=len(payload),
        )
        return key, uri


def _json_default(value: Any) -> Any:
    """Safe JSON serializer fallback for dates, timestamps, and NaNs."""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return str(value)


def _collect_failures_by_index(
    df: pd.DataFrame,
    failure_cases: pd.DataFrame,
) -> tuple[dict[Any, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Group Pandera failure cases into row-specific errors and schema-level errors."""
    per_row: dict[Any, list[dict[str, Any]]] = {}
    schema_wide: list[dict[str, Any]] = []
    valid_index_set = set(df.index)

    for record in failure_cases.to_dict(orient="records"):
        raw_idx = record.get("index")
        entry = {
            "schema_context": str(record.get("schema_context", "")),
            "column": None if pd.isna(record.get("column")) else str(record.get("column")),
            "check": None if pd.isna(record.get("check")) else str(record.get("check")),
            "failure_case": (
                None if pd.isna(record.get("failure_case")) else str(record.get("failure_case"))
            ),
        }
        if raw_idx is not None and not pd.isna(raw_idx) and raw_idx in valid_index_set:
            per_row.setdefault(raw_idx, []).append(entry)
        else:
            schema_wide.append(entry)

    return per_row, schema_wide


def _build_quarantine_df(
    raw_rows: pd.DataFrame,
    *,
    failures_by_index: dict[Any, list[dict[str, Any]]],
    default_failures: list[dict[str, Any]],
    source_name: str,
    schema_name: str,
    run_id: str,
    quarantined_at: str,
) -> pd.DataFrame:
    """Create a Parquet-safe quarantine DataFrame with failure metadata and raw JSON payload."""
    records: list[dict[str, Any]] = []
    for idx, row in raw_rows.iterrows():
        row_dict = {str(col): (None if pd.isna(val) else val) for col, val in row.to_dict().items()}
        reasons = failures_by_index.get(idx, default_failures)
        records.append(
            {
                "run_id": run_id,
                "source_name": source_name,
                "schema_name": schema_name,
                "quarantined_at": quarantined_at,
                "row_index": str(idx),
                "failure_reasons": json.dumps(reasons, default=_json_default, sort_keys=True),
                "raw_payload": json.dumps(row_dict, default=_json_default, sort_keys=True),
            }
        )
    return pd.DataFrame(
        records,
        columns=[
            "run_id",
            "source_name",
            "schema_name",
            "quarantined_at",
            "row_index",
            "failure_reasons",
            "raw_payload",
        ],
    )


def validate_and_quarantine(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    *,
    source_name: str,
    run_id: str | None = None,
    partition_date: date | None = None,
    quarantine_writer: QuarantineWriter | None = None,
) -> ContractValidationResult:
    """Validate ``df`` against ``schema`` and route invalid rows to Quarantine/DLQ.

    Guarantees:
    - No row is ever silently dropped: ``rows_valid + rows_quarantined == rows_processed``.
    - ``valid_df`` has passed ``schema.validate(..., lazy=True)`` with coerced types.
    - When ``rows_quarantined > 0``, ``ingestion_quarantine_rows_total`` is incremented
      and (when ``quarantine_writer`` is provided) ``quarantine_df`` is written to
      ``r2://arthala-lakehouse/quarantine/{source_name}/dt=YYYY-MM-DD/run_{run_id}_errors.parquet``.
    """
    resolved_run_id = run_id or uuid.uuid4().hex
    resolved_date = partition_date or datetime.now(tz=UTC).date()
    schema_name = schema.name or "DataFrameSchema"
    quarantined_at = datetime.now(tz=UTC).isoformat()

    if df.empty:
        empty_quarantine = _build_quarantine_df(
            df,
            failures_by_index={},
            default_failures=[],
            source_name=source_name,
            schema_name=schema_name,
            run_id=resolved_run_id,
            quarantined_at=quarantined_at,
        )
        return ContractValidationResult(
            source_name=source_name,
            schema_name=schema_name,
            run_id=resolved_run_id,
            valid_df=df.copy(),
            quarantine_df=empty_quarantine,
            rows_processed=0,
            rows_valid=0,
            rows_quarantined=0,
            quarantine_key=None,
            quarantine_uri=None,
        )

    all_failures_by_index: dict[Any, list[dict[str, Any]]] = {}
    schema_wide_failures: list[dict[str, Any]] = []
    candidate_df = df.copy()
    valid_df: pd.DataFrame | None = None

    for _ in range(3):
        if candidate_df.empty:
            valid_df = candidate_df
            break
        try:
            valid_df = schema.validate(candidate_df, lazy=True)
            break
        except pa.errors.SchemaErrors as exc:
            per_row, schema_wide = _collect_failures_by_index(candidate_df, exc.failure_cases)
            if not per_row:
                schema_wide_failures.extend(schema_wide)
                for idx in candidate_df.index:
                    all_failures_by_index.setdefault(idx, []).extend(schema_wide)
                valid_df = candidate_df.iloc[0:0].copy()
                break

            for idx, reasons in per_row.items():
                all_failures_by_index.setdefault(idx, []).extend(reasons)
            candidate_df = candidate_df.drop(index=list(per_row.keys()))

    if valid_df is None:
        for idx in candidate_df.index:
            all_failures_by_index.setdefault(idx, []).extend(schema_wide_failures)
        valid_df = candidate_df.iloc[0:0].copy()

    bad_index_list = [idx for idx in df.index if idx in all_failures_by_index]
    raw_bad_rows = df.loc[bad_index_list]
    quarantine_df = _build_quarantine_df(
        raw_bad_rows,
        failures_by_index=all_failures_by_index,
        default_failures=schema_wide_failures,
        source_name=source_name,
        schema_name=schema_name,
        run_id=resolved_run_id,
        quarantined_at=quarantined_at,
    )

    rows_processed = len(df)
    rows_valid = len(valid_df)
    rows_quarantined = len(quarantine_df)
    quarantine_key: str | None = None
    quarantine_uri: str | None = None

    if rows_quarantined > 0:
        INGESTION_QUARANTINE_ROWS_TOTAL.labels(source=source_name, schema=schema_name).inc(
            rows_quarantined
        )
        if quarantine_writer is not None:
            quarantine_key, quarantine_uri = quarantine_writer.write_quarantine_dataframe(
                quarantine_df,
                source_name=source_name,
                partition_date=resolved_date,
                run_id=resolved_run_id,
            )
        else:
            logger.warning(
                "ingestion.rows_quarantined",
                source_name=source_name,
                schema_name=schema_name,
                run_id=resolved_run_id,
                rows_valid=rows_valid,
                rows_quarantined=rows_quarantined,
            )
    else:
        logger.info(
            "ingestion.contract_valid",
            source_name=source_name,
            schema_name=schema_name,
            run_id=resolved_run_id,
            rows_valid=rows_valid,
        )

    return ContractValidationResult(
        source_name=source_name,
        schema_name=schema_name,
        run_id=resolved_run_id,
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        rows_processed=rows_processed,
        rows_valid=rows_valid,
        rows_quarantined=rows_quarantined,
        quarantine_key=quarantine_key,
        quarantine_uri=quarantine_uri,
    )
