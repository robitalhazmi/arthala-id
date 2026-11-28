"""Unit tests for Pandera ingestion contracts and automatic R2/B2 Quarantine/DLQ writer."""

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
    _check_corporate_action_amount,
    _check_dividend_date_order,
    _check_filing_date_chronology,
    _check_ohlc_bounds,
    _json_default,
    validate_and_quarantine,
)


class FakeS3Client:
    """In-memory S3/R2/B2 client recorder for offline unit tests."""

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


def test_automatic_default_quarantine_writer_when_not_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling validate_and_quarantine without quarantine_writer writes automatically to R2/B2."""
    fake_s3 = FakeS3Client()
    captured_kwargs: dict[str, Any] = {}

    def _fake_boto3_client(service_name: str, **kwargs: Any) -> FakeS3Client:
        captured_kwargs["service_name"] = service_name
        captured_kwargs.update(kwargs)
        return fake_s3

    monkeypatch.setattr(boto3, "client", _fake_boto3_client)

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
            },
            {
                "ticker": "bad!",
                "trade_date": "2026-11-20",
                "open_price": 4600.0,
                "high_price": 4680.0,
                "low_price": 4580.0,
                "close_price": 4650.0,
                "volume": 88_100_000,
            },
        ]
    )

    result = validate_and_quarantine(
        df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="auto001",
        partition_date=date(2026, 11, 20),
    )

    expected_key = "quarantine/idx_ohlcv/dt=2026-11-20/run_auto001_errors.parquet"
    assert result.rows_valid == 1
    assert result.rows_quarantined == 1
    assert result.quarantine_key == expected_key
    assert result.quarantine_uri == f"r2://arthala-lakehouse/{expected_key}"
    assert len(fake_s3.uploads) == 1
    assert captured_kwargs["service_name"] == "s3"
    assert captured_kwargs["config"].signature_version == "s3v4"


def test_price_schema_mixed_batch_infinity_and_multi_column_coercion() -> None:
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
                # Uncoercible col 1
                "ticker": "BBRI",
                "trade_date": "2026-11-20",
                "open_price": "N/A",
                "high_price": 4680.0,
                "low_price": 4580.0,
                "close_price": 4650.0,
                "volume": 88_100_000,
                "source": "yfinance",
            },
            {
                # Uncoercible col 2
                "ticker": "BMRI",
                "trade_date": "2026-11-20",
                "open_price": 6000.0,
                "high_price": "ERR",
                "low_price": 5950.0,
                "close_price": 6050.0,
                "volume": 30_000_000,
                "source": "idx",
            },
            {
                # Uncoercible col 3
                "ticker": "BBNI",
                "trade_date": "2026-11-20",
                "open_price": 5000.0,
                "high_price": 5100.0,
                "low_price": "BAD",
                "close_price": 5050.0,
                "volume": 20_000_000,
                "source": "idx",
            },
            {
                # Uncoercible col 4 (tests >3 column coercion loop)
                "ticker": "TLKM",
                "trade_date": "2026-11-20",
                "open_price": 2700.0,
                "high_price": 2750.0,
                "low_price": 2680.0,
                "close_price": "N/A",
                "volume": 30_000_000,
                "source": "idx",
            },
            {
                # +Infinity must be rejected despite inf > 0
                "ticker": "ASII",
                "trade_date": "2026-11-20",
                "open_price": 4900.0,
                "high_price": float("inf"),
                "low_price": 4850.0,
                "close_price": 4920.0,
                "volume": 12_000_000,
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

    assert result.rows_processed == 6
    assert result.rows_valid == 1
    assert result.rows_quarantined == 5
    assert result.rows_valid + result.rows_quarantined == result.rows_processed
    assert list(result.valid_df["ticker"]) == ["BBCA"]
    assert _counter_value("idx_ohlcv", "PriceSchema") == before_metric + 5.0

    roundtrip_df = pd.read_parquet(io.BytesIO(fake_s3.uploads[0]["Body"]))
    assert len(roundtrip_df) == 5
    payloads = [json.loads(raw) for raw in roundtrip_df["raw_payload"]]
    assert payloads[-1]["ticker"] == "ASII"
    assert payloads[-1]["high_price"] == "inf"


def test_duplicate_dataframe_indices_preserve_row_conservation() -> None:
    """Concatenated DataFrames with duplicate index labels must never drop valid rows."""
    page1 = pd.DataFrame(
        [
            {
                "ticker": "BBCA",
                "trade_date": "2026-11-20",
                "open_price": 9800.0,
                "high_price": 9950.0,
                "low_price": 9775.0,
                "close_price": 9900.0,
                "volume": 45_200_000,
            }
        ]
    )
    page2 = pd.DataFrame(
        [
            {
                # Same index 0, but invalid OHLC (high < low)
                "ticker": "ASII",
                "trade_date": "2026-11-20",
                "open_price": 4900.0,
                "high_price": 4700.0,
                "low_price": 5000.0,
                "close_price": 4850.0,
                "volume": 12_000_000,
            }
        ]
    )
    concat_df = pd.concat([page1, page2], ignore_index=False)
    assert list(concat_df.index) == [0, 0]

    result = validate_and_quarantine(
        concat_df,
        PriceSchema,
        source_name="idx_ohlcv",
        run_id="dup_idx",
        auto_write_quarantine=False,
    )

    assert result.rows_processed == 2
    assert result.rows_valid == 1
    assert result.rows_quarantined == 1
    assert list(result.valid_df["ticker"]) == ["BBCA"]
    assert result.quarantine_key is None


def test_financial_report_schema_chronology_and_distressed_negative_equity() -> None:
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
                # Invalid: filing_date (2021) is before fiscal_year (2026)
                "ticker": "TLKM",
                "fiscal_year": 2026,
                "fiscal_quarter": 2,
                "filing_date": date(2021, 7, 31),
                "revenue_idr": 75_000_000_000_000.0,
                "net_income_idr": 12_000_000_000_000.0,
                "operating_cf_idr": 28_000_000_000_000.0,
                "total_assets_idr": 280_000_000_000_000.0,
                "total_liabilities_idr": 130_000_000_000_000.0,
                "total_equity_idr": 150_000_000_000_000.0,
                "currency": "IDR",
            },
            {
                # Invalid: fiscal_quarter=5 and total_assets_idr=0
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
        auto_write_quarantine=False,
    )

    assert result.rows_processed == 3
    assert result.rows_valid == 1
    assert result.rows_quarantined == 2
    assert list(result.valid_df["ticker"]) == ["WSKT"]


def test_corporate_action_schema_cash_dividend_and_stock_split() -> None:
    df = pd.DataFrame(
        [
            {
                "ticker": "BBCA",
                "action_type": "CASH_DIVIDEND",
                "fiscal_year": 2026,
                "dividend_per_share": 250.0,
                "split_ratio": None,
                "currency": "IDR",
                "cum_date": "2026-11-10",
                "ex_date": "2026-11-11",
                "recording_date": "2026-11-12",
                "payment_date": "2026-11-28",
                "payout_ratio_pct": 62.5,
            },
            {
                # Valid STOCK_SPLIT without cash dividend_per_share
                "ticker": "BMRI",
                "action_type": "STOCK_SPLIT",
                "fiscal_year": 2026,
                "dividend_per_share": None,
                "split_ratio": 2.0,
                "currency": "IDR",
                "cum_date": "2026-11-15",
                "ex_date": "2026-11-16",
                "recording_date": "2026-11-17",
                "payment_date": None,
                "payout_ratio_pct": None,
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
                # Invalid: CASH_DIVIDEND with dividend_per_share <= 0
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
        auto_write_quarantine=False,
    )

    assert result.rows_processed == 4
    assert result.rows_valid == 2
    assert result.rows_quarantined == 2
    assert list(result.valid_df["ticker"]) == ["BBCA", "BMRI"]


def test_missing_required_column_quarantines_entire_batch_without_dropping_rows() -> None:
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


def test_edge_case_helpers_and_all_rows_invalid() -> None:
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
        auto_write_quarantine=False,
    )
    assert res_valid.rows_valid == 1
    assert res_valid.rows_quarantined == 0

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
        auto_write_quarantine=False,
    )
    assert res_bad.rows_valid == 0
    assert res_bad.rows_quarantined == 2

    minimal = pd.DataFrame([{"ticker": "BBCA"}])
    assert list(_check_ohlc_bounds(minimal)) == [False]
    assert list(_check_filing_date_chronology(minimal)) == [False]
    assert list(_check_dividend_date_order(minimal)) == [False]
    assert list(_check_corporate_action_amount(minimal)) == [False]
    assert _json_default(date(2026, 11, 20)) == "2026-11-20"
    assert _json_default(float("nan")) is None
    assert _json_default(object()).startswith("<object")
