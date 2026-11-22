"""Unit tests for Pandera ingestion contracts and R2 Quarantine/DLQ writer."""

from __future__ import annotations

import io
import json
from datetime import date
from typing import Any

import boto3
import pandas as pd
import pytest

from pipelines.ingestion.contracts import (
    INGESTION_QUARANTINE_ROWS_TOTAL,
    CorporateActionSchema,
    FinancialReportSchema,
    PriceSchema,
    QuarantineWriter,
    R2StorageSettings,
    _check_dividend_date_order,
    _json_default,
    validate_and_quarantine,
)


class FakeS3Client:
    """In-memory S3/R2 client recorder for offline unit tests."""

    def __init__(self) -> None:
        self.uploads: list[dict[str, Any]] = []

    def put_object(
        self,
        *,
        Bucket: str,  # noqa: N803
        Key: str,  # noqa: N803
        Body: bytes,  # noqa: N803
        ContentType: str = "application/octet-stream",  # noqa: N803
    ) -> dict[str, Any]:
        record = {
            "Bucket": Bucket,
            "Key": Key,
            "Body": Body,
            "ContentType": ContentType,
        }
        self.uploads.append(record)
        return {"ETag": '"fake-etag"'}


def _counter_value(source: str, schema: str) -> float:
    return float(INGESTION_QUARANTINE_ROWS_TOTAL.labels(source=source, schema=schema)._value.get())


def test_price_schema_all_valid_rows() -> None:
    df = pd.DataFrame(
        [
            {
                "ticker": "BBCA",
                "trade_date": "2026-11-20",
                "open_price": 9800.0,
                "high_price": 9950.0,
                "low_price": 9775.0,
                "close_price": 9900.0,
                "volume": 45_200_000,
                "adj_close": 9900.0,
                "source": "yfinance",
            },
            {
                "ticker": "BBRI",
                "trade_date": "2026-11-20",
                "open_price": 4600.0,
                "high_price": 4680.0,
                "low_price": 4580.0,
                "close_price": 4650.0,
                "volume": 88_100_000,
                "adj_close": 4650.0,
                "source": "idx",
            },
        ]
    )
    fake_s3 = FakeS3Client()
    writer = QuarantineWriter(
        settings=R2StorageSettings(R2_BUCKET="arthala-lakehouse"),
        s3_client=fake_s3,
    )

    result = validate_and_quarantine(
        df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="run001",
        partition_date=date(2026, 11, 20),
        quarantine_writer=writer,
    )

    assert result.rows_processed == 2
    assert result.rows_valid == 2
    assert result.rows_quarantined == 0
    assert result.quarantine_key is None
    assert result.quarantine_uri is None
    assert fake_s3.uploads == []
    assert list(result.valid_df["ticker"]) == ["BBCA", "BBRI"]


def test_price_schema_mixed_batch_splits_and_writes_quarantine_parquet() -> None:
    df = pd.DataFrame(
        [
            {
                "ticker": "BBCA",
                "trade_date": "2026-11-20",
                "open_price": 9800.0,
                "high_price": 9950.0,
                "low_price": 9775.0,
                "close_price": 9900.0,
                "volume": 45_200_000,
                "source": "yfinance",
            },
            {
                # Invalid lowercase ticker with .JK suffix
                "ticker": "bbri.jk",
                "trade_date": "2026-11-20",
                "open_price": 4600.0,
                "high_price": 4680.0,
                "low_price": 4580.0,
                "close_price": 4650.0,
                "volume": 88_100_000,
                "source": "yfinance",
            },
            {
                # Violates OHLC bounds: high_price (4800) < low_price (5000)
                "ticker": "ASII",
                "trade_date": "2026-11-20",
                "open_price": 4900.0,
                "high_price": 4800.0,
                "low_price": 5000.0,
                "close_price": 4850.0,
                "volume": 12_000_000,
                "source": "idx",
            },
            {
                # Uncoercible string in numeric close_price must not poison row 0
                "ticker": "TLKM",
                "trade_date": "2026-11-20",
                "open_price": 2700.0,
                "high_price": 2750.0,
                "low_price": 2680.0,
                "close_price": "N/A",
                "volume": 30_000_000,
                "source": "idx",
            },
        ]
    )
    fake_s3 = FakeS3Client()
    writer = QuarantineWriter(
        settings=R2StorageSettings(R2_BUCKET="arthala-lakehouse"),
        s3_client=fake_s3,
    )
    before_metric = _counter_value("idx_ohlcv", "PriceSchema")

    result = validate_and_quarantine(
        df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="run002",
        partition_date=date(2026, 11, 20),
        quarantine_writer=writer,
    )

    assert result.rows_processed == 4
    assert result.rows_valid == 1
    assert result.rows_quarantined == 3
    assert result.rows_valid + result.rows_quarantined == result.rows_processed
    assert list(result.valid_df["ticker"]) == ["BBCA"]

    expected_key = "quarantine/idx_ohlcv/dt=2026-11-20/run_run002_errors.parquet"
    assert result.quarantine_key == expected_key
    assert result.quarantine_uri == f"r2://arthala-lakehouse/{expected_key}"
    assert _counter_value("idx_ohlcv", "PriceSchema") == before_metric + 3.0

    assert len(fake_s3.uploads) == 1
    upload = fake_s3.uploads[0]
    assert upload["Bucket"] == "arthala-lakehouse"
    assert upload["Key"] == expected_key
    assert upload["ContentType"] == "application/vnd.apache.parquet"

    roundtrip_df = pd.read_parquet(io.BytesIO(upload["Body"]))
    assert len(roundtrip_df) == 3
    assert set(roundtrip_df["row_index"]) == {"1", "2", "3"}
    payloads = [json.loads(raw) for raw in roundtrip_df["raw_payload"]]
    assert [p["ticker"] for p in payloads] == ["bbri.jk", "ASII", "TLKM"]
    reasons = [json.loads(r) for r in roundtrip_df["failure_reasons"]]
    assert all(len(r) >= 1 for r in reasons)


def test_financial_report_schema_allows_distressed_negative_equity_and_quarantines_invalid() -> (
    None
):
    df = pd.DataFrame(
        [
            {
                # Valid distressed issuer (Notasi Khusus 'E' negative equity)
                "ticker": "WSKT",
                "fiscal_year": 2026,
                "fiscal_quarter": 3,
                "filing_date": date(2026, 10, 31),
                "revenue_idr": 8_500_000_000_000.0,
                "net_income_idr": -2_100_000_000_000.0,
                "operating_cf_idr": -950_000_000_000.0,
                "total_assets_idr": 85_000_000_000_000.0,
                "total_liabilities_idr": 91_000_000_000_000.0,
                "total_equity_idr": -6_000_000_000_000.0,
                "short_term_debt_idr": 15_000_000_000_000.0,
                "eps": -73.0,
                "currency": "IDR",
            },
            {
                # Invalid: fiscal_quarter=5 and total_assets_idr=0 (must be > 0)
                "ticker": "ASII",
                "fiscal_year": 2026,
                "fiscal_quarter": 5,
                "filing_date": date(2026, 10, 31),
                "revenue_idr": 100_000_000_000.0,
                "net_income_idr": 10_000_000_000.0,
                "operating_cf_idr": 12_000_000_000.0,
                "total_assets_idr": 0.0,
                "total_liabilities_idr": 50_000_000_000.0,
                "total_equity_idr": 50_000_000_000.0,
                "short_term_debt_idr": 5_000_000_000.0,
                "eps": 120.0,
                "currency": "IDR",
            },
        ]
    )

    result = validate_and_quarantine(
        df,
        FinancialReportSchema,
        source_name="idx_financials_raw",
        run_id="fin001",
        partition_date=date(2026, 11, 20),
    )

    assert result.rows_processed == 2
    assert result.rows_valid == 1
    assert result.rows_quarantined == 1
    assert list(result.valid_df["ticker"]) == ["WSKT"]
    assert result.quarantine_key is None  # no writer passed


def test_corporate_action_schema_validates_dividend_dates_and_amounts() -> None:
    df = pd.DataFrame(
        [
            {
                "ticker": "BBCA",
                "action_type": "CASH_DIVIDEND",
                "fiscal_year": 2026,
                "dividend_per_share": 250.0,
                "currency": "IDR",
                "cum_date": "2026-11-10",
                "ex_date": "2026-11-11",
                "recording_date": "2026-11-12",
                "payment_date": "2026-11-28",
                "payout_ratio_pct": 62.5,
            },
            {
                # Invalid: payment_date (2026-11-01) is before cum_date (2026-11-10)
                "ticker": "BBRI",
                "action_type": "CASH_DIVIDEND",
                "fiscal_year": 2026,
                "dividend_per_share": 310.0,
                "currency": "IDR",
                "cum_date": "2026-11-10",
                "ex_date": "2026-11-11",
                "recording_date": "2026-11-12",
                "payment_date": "2026-11-01",
                "payout_ratio_pct": 80.0,
            },
            {
                # Invalid: dividend_per_share <= 0
                "ticker": "TLKM",
                "action_type": "CASH_DIVIDEND",
                "fiscal_year": 2026,
                "dividend_per_share": 0.0,
                "currency": "IDR",
                "cum_date": "2026-11-10",
                "ex_date": "2026-11-11",
                "recording_date": "2026-11-12",
                "payment_date": "2026-11-25",
                "payout_ratio_pct": 70.0,
            },
        ]
    )

    result = validate_and_quarantine(
        df,
        CorporateActionSchema,
        source_name="idx_dividends_raw",
        run_id="div001",
        partition_date=date(2026, 11, 20),
    )

    assert result.rows_processed == 3
    assert result.rows_valid == 1
    assert result.rows_quarantined == 2
    assert list(result.valid_df["ticker"]) == ["BBCA"]


def test_missing_required_column_quarantines_entire_batch_without_dropping_rows() -> None:
    # Missing required 'close_price' and 'volume' columns
    df = pd.DataFrame(
        [
            {"ticker": "BBCA", "trade_date": "2026-11-20", "open_price": 9800.0},
            {"ticker": "BBRI", "trade_date": "2026-11-20", "open_price": 4600.0},
        ]
    )
    fake_s3 = FakeS3Client()
    writer = QuarantineWriter(s3_client=fake_s3)

    result = validate_and_quarantine(
        df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="missing_col",
        partition_date=date(2026, 11, 20),
        quarantine_writer=writer,
    )

    assert result.rows_processed == 2
    assert result.rows_valid == 0
    assert result.rows_quarantined == 2
    assert len(fake_s3.uploads) == 1


def test_empty_dataframe_returns_zero_counts_and_skips_quarantine_write() -> None:
    empty_df = pd.DataFrame(
        columns=[
            "ticker",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
        ]
    )
    fake_s3 = FakeS3Client()
    writer = QuarantineWriter(s3_client=fake_s3)

    result = validate_and_quarantine(
        empty_df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="empty001",
        partition_date=date(2026, 11, 20),
        quarantine_writer=writer,
    )

    assert result.rows_processed == 0
    assert result.rows_valid == 0
    assert result.rows_quarantined == 0
    assert result.quarantine_key is None
    assert fake_s3.uploads == []


def test_quarantine_writer_lazy_boto3_client_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_s3 = FakeS3Client()
    captured_kwargs: dict[str, Any] = {}

    def _fake_boto3_client(service_name: str, **kwargs: Any) -> FakeS3Client:
        captured_kwargs["service_name"] = service_name
        captured_kwargs.update(kwargs)
        return fake_s3

    monkeypatch.setattr(boto3, "client", _fake_boto3_client)

    settings = R2StorageSettings(
        R2_ENDPOINT_URL="http://localhost:9000",
        R2_BUCKET="arthala-lakehouse",
        R2_REGION="auto",
    )
    writer = QuarantineWriter(settings=settings)
    qdf = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "source_name": "idx_ohlcv",
                "schema_name": "PriceSchema",
                "quarantined_at": "2026-11-20T00:00:00+00:00",
                "row_index": "0",
                "failure_reasons": "[]",
                "raw_payload": "{}",
            }
        ]
    )
    key, uri = writer.write_quarantine_dataframe(
        qdf,
        source_name="idx_ohlcv",
        partition_date=date(2026, 11, 20),
        run_id="r1",
    )

    assert key == "quarantine/idx_ohlcv/dt=2026-11-20/run_r1_errors.parquet"
    assert uri == "r2://arthala-lakehouse/quarantine/idx_ohlcv/dt=2026-11-20/run_r1_errors.parquet"
    assert captured_kwargs["service_name"] == "s3"
    assert captured_kwargs["endpoint_url"] == "http://localhost:9000"
    assert len(fake_s3.uploads) == 1


def test_all_rows_invalid_by_row_check_and_optional_dividend_columns() -> None:
    # Minimal CorporateActionSchema batch without optional ex_date/recording_date/payment_date
    valid_min_df = pd.DataFrame(
        [
            {
                "ticker": "ASII",
                "dividend_per_share": 150.0,
                "cum_date": "2026-11-10",
            }
        ]
    )
    res_valid = validate_and_quarantine(
        valid_min_df,
        CorporateActionSchema,
        source_name="idx_dividends_raw",
        run_id="div_min",
    )
    assert res_valid.rows_valid == 1
    assert res_valid.rows_quarantined == 0

    # Batch where 100% of rows fail row-level checks so candidate_df becomes empty
    all_bad_df = pd.DataFrame(
        [
            {"ticker": "bad1", "dividend_per_share": -5.0, "cum_date": "2026-11-10"},
            {"ticker": "bad2", "dividend_per_share": 0.0, "cum_date": "2026-11-10"},
        ]
    )
    res_bad = validate_and_quarantine(
        all_bad_df,
        CorporateActionSchema,
        source_name="idx_dividends_raw",
        run_id="div_all_bad",
    )
    assert res_bad.rows_valid == 0
    assert res_bad.rows_quarantined == 2

    # Direct edge-case helpers
    no_cum_df = pd.DataFrame([{"ticker": "BBCA"}])
    assert list(_check_dividend_date_order(no_cum_df)) == [False]
    assert _json_default(float("nan")) is None
    assert _json_default(object()).startswith("<object")
