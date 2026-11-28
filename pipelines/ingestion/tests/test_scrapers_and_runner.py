"""Offline unit & integration tests for Task 1.2 scrapers, Bronze writer, and runner."""

from __future__ import annotations

import io
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any, Literal

import pandas as pd
import psycopg
import pytest
from botocore.exceptions import ClientError

from pipelines.ingestion.contracts import QuarantineWriter, R2StorageSettings
from pipelines.ingestion.idx_collector import (
    BotChallengeError,
    CurlCffiTransport,
    IdxCollector,
    classify_disclosure_category,
    compute_filing_hash,
)
from pipelines.ingestion.market_collector import (
    MarketCollector,
    YFinanceMarketProvider,
    chunk_tickers,
    normalize_idx_ticker,
    to_yahoo_symbol,
)
from pipelines.ingestion.runner import (
    BronzeParquetWriter,
    IngestionRunner,
    IngestionSettings,
    PostgresPipelineRunStore,
    assert_local_endpoints,
    load_ingestion_settings,
)
from pipelines.ingestion.runner import (
    main as runner_main,
)


class InMemoryS3Bucket:
    """In-memory S3/B2/R2 object store supporting get_object and put_object."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        data = self.objects.get((Bucket, Key))
        if data is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
                "GetObject",
            )
        return {"Body": io.BytesIO(data)}

    def put_object(
        self,
        *,
        Bucket: str,  # noqa: N803
        Key: str,  # noqa: N803
        Body: bytes,  # noqa: N803
        ContentType: str = "application/octet-stream",  # noqa: N803
    ) -> dict[str, Any]:
        assert ContentType
        self.objects[(Bucket, Key)] = Body
        return {"ETag": '"mem-etag"'}


class InMemoryRunStore:
    """In-memory implementation of ``PipelineRunStoreProtocol`` for offline tests."""

    def __init__(self) -> None:
        self.runs: list[dict[str, Any]] = []
        self._counter = 0

    def get_latest_watermark(self, pipeline_name: str) -> str | None:
        for rec in reversed(self.runs):
            if (
                rec["pipeline_name"] == pipeline_name
                and rec["status"] in {"SUCCESS", "PARTIAL"}
                and rec["watermark_value"] is not None
            ):
                return str(rec["watermark_value"])
        return None

    def start_run(self, pipeline_name: str) -> str:
        self._counter += 1
        run_id = f"run-{self._counter:04d}"
        self.runs.append(
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "status": "RUNNING",
                "rows_processed": 0,
                "rows_quarantined": 0,
                "watermark_value": None,
            }
        )
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: Literal["SUCCESS", "PARTIAL", "FAILED"],
        rows_processed: int,
        rows_quarantined: int,
        watermark_value: str | None,
    ) -> None:
        for rec in self.runs:
            if rec["run_id"] == run_id:
                rec["status"] = status
                rec["rows_processed"] = rows_processed
                rec["rows_quarantined"] = rows_quarantined
                rec["watermark_value"] = watermark_value
                return


class FakeMarketProvider:
    """Offline fake ``yfinance`` provider testing batching, retries, dividends, and splits."""

    def __init__(self, *, fail_first_ohlcv: bool = False) -> None:
        self.fail_first_ohlcv = fail_first_ohlcv
        self.ohlcv_calls: list[list[str]] = []
        self._failed_once = False

    def fetch_ohlcv_batch(
        self,
        yahoo_symbols: Sequence[str],
        *,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        assert start_date <= end_date
        self.ohlcv_calls.append(list(yahoo_symbols))
        if self.fail_first_ohlcv and not self._failed_once:
            self._failed_once = True
            raise TimeoutError("simulated transient Yahoo Finance timeout")

        idx = pd.DatetimeIndex(["2026-11-26", "2026-11-27"])
        if len(yahoo_symbols) == 1:
            return pd.DataFrame(
                {
                    "Open": [9800.0, 9850.0],
                    "High": [9900.0, 9950.0],
                    "Low": [9750.0, 9800.0],
                    "Close": [9850.0, 9925.0],
                    "Adj Close": [9850.0, 9925.0],
                    "Volume": [40_000_000, 45_000_000],
                },
                index=idx,
            )

        frames: dict[tuple[str, str], list[float]] = {}
        for sym in yahoo_symbols:
            frames[(sym, "Open")] = [4600.0, 4620.0]
            frames[(sym, "High")] = [4680.0, 4700.0]
            frames[(sym, "Low")] = [4580.0, 4600.0]
            frames[(sym, "Close")] = [4650.0, 4675.0]
            frames[(sym, "Adj Close")] = [4650.0, 4675.0]
            frames[(sym, "Volume")] = [80_000_000.0, 85_000_000.0]
        return pd.DataFrame(frames, index=idx)

    def fetch_ticker_actions(
        self,
        yahoo_symbol: str,
        *,
        start_date: date,
    ) -> tuple[pd.Series[float], pd.Series[float]]:
        assert start_date
        if yahoo_symbol == "BBCA.JK":
            divs = pd.Series([270.0], index=pd.DatetimeIndex(["2026-11-11"]))
            splits = pd.Series(dtype=float)
            return divs, splits
        if yahoo_symbol == "BMRI.JK":
            divs = pd.Series(dtype=float)
            splits = pd.Series([2.0], index=pd.DatetimeIndex(["2026-11-16"]))
            return divs, splits
        return pd.Series(dtype=float), pd.Series(dtype=float)


def test_market_collector_batching_retry_and_actions() -> None:
    assert normalize_idx_ticker(" bbca.jk ") == "BBCA"
    assert to_yahoo_symbol("bbri") == "BBRI.JK"
    assert chunk_tickers(["BBCA", "bbca.jk", "BBRI", "BMRI"], batch_size=2) == [
        ["BBCA", "BBRI"],
        ["BMRI"],
    ]
    with pytest.raises(ValueError, match="batch_size"):
        chunk_tickers(["BBCA"], batch_size=0)

    sleeps: list[float] = []
    provider = FakeMarketProvider(fail_first_ohlcv=True)
    collector = MarketCollector(
        provider=provider,
        batch_size=2,
        max_retries=3,
        base_backoff_seconds=0.5,
        sleep_fn=sleeps.append,
    )

    ohlcv_df = collector.collect_ohlcv(
        ["BBCA", "BBRI", "BMRI"],
        start_date=date(2026, 11, 20),
        end_date=date(2026, 11, 27),
    )
    # Batch 1 (BBCA, BBRI) failed once then succeeded; Batch 2 (BMRI) single-ticker frame succeeded
    assert len(provider.ohlcv_calls) == 3
    assert len(sleeps) == 1
    assert sleeps[0] >= 0.5
    assert len(ohlcv_df) == 6
    assert set(ohlcv_df["ticker"]) == {"BBCA", "BBRI", "BMRI"}

    actions_df = collector.collect_dividends_and_splits(
        ["BBCA", "BMRI", "ASII"],
        start_date=date(2026, 1, 1),
    )
    assert len(actions_df) == 2
    assert set(actions_df["action_type"]) == {"CASH_DIVIDEND", "STOCK_SPLIT"}


def test_idx_collector_primary_and_playwright_fallback() -> None:
    class _BlockedPrimaryTransport:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_json(
            self,
            url: str,
            *,
            params: Mapping[str, str | int] | None = None,
            timeout_seconds: float = 20.0,
        ) -> dict[str, Any]:
            assert timeout_seconds > 0
            self.calls += 1
            if "GetFinancialReport" in url:
                raise BotChallengeError("Cloudflare 403 challenge")
            if "GetAllAnnouncement" in url:
                raise RuntimeError("502 gateway timeout")
            return {
                "data": [
                    {
                        "KodeEmiten": "WSKT",
                        "NamaEmiten": "PT Waskita Karya Tbk",
                        "PapanPencatatan": "Pemantauan Khusus",
                        "NotasiKhusus": "E,L,X",
                        "Sektor": "Infrastructures",
                        "SubSektor": "Construction",
                    },
                    {
                        "KodeEmiten": "BBCA",
                        "NamaEmiten": "PT Bank Central Asia Tbk",
                        "PapanPencatatan": "Utama",
                        "NotasiKhusus": [],
                        "Sektor": "Financials",
                        "SubSektor": "Banks",
                    },
                ]
            }

    class _WorkingPlaywrightFallback:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_json(
            self,
            url: str,
            *,
            params: Mapping[str, str | int] | None = None,
            timeout_seconds: float = 20.0,
        ) -> dict[str, Any]:
            assert timeout_seconds > 0
            self.calls += 1
            if "GetFinancialReport" in url:
                ticker = str((params or {}).get("kodeEmiten", "BBCA"))
                return {
                    "Results": [
                        {
                            "KodeEmiten": ticker,
                            "Report_Year": 2026,
                            "File_Modified": "2026-10-30",
                            "SalesAndRevenue": 50_000_000_000_000.0,
                            "ProfitLoss": 20_000_000_000_000.0,
                            "CashFlowsFromOperatingActivities": 22_000_000_000_000.0,
                            "Assets": 900_000_000_000_000.0,
                            "Liabilities": 750_000_000_000_000.0,
                            "Equity": 150_000_000_000_000.0,
                            "EarningsPerShare": 210.5,
                        }
                    ]
                }
            return {
                "Replies": [
                    {
                        "pengumuman": {
                            "Id": "ANN-001",
                            "Kode_Emiten": "WSKT",
                            "TglPengumuman": "2026-11-27T17:30:00",
                            "JudulPengumuman": "Informasi Permohonan PKPU Perseroan",
                        },
                        "attachments": [{"FullSavePath": "https://www.idx.co.id/doc/ann001.pdf"}],
                    }
                ]
            }

    primary = _BlockedPrimaryTransport()
    fallback = _WorkingPlaywrightFallback()
    collector = IdxCollector(
        primary_transport=primary,
        fallback_transport=fallback,
        max_retries=2,
        base_backoff_seconds=0.01,
        sleep_fn=lambda _: None,
    )

    fin_df = collector.collect_quarterly_financials(["BBCA"], fiscal_year=2026, fiscal_quarter=3)
    assert len(fin_df) == 1
    assert fin_df.iloc[0]["ticker"] == "BBCA"
    assert len(str(fin_df.iloc[0]["filing_hash"])) == 64

    disc_df = collector.collect_disclosures(
        ["WSKT"], from_date=date(2026, 11, 20), to_date=date(2026, 11, 28)
    )
    assert len(disc_df) == 1
    assert disc_df.iloc[0]["category"] == "PKPU_LITIGATION"
    assert disc_df.iloc[0]["source"] == "playwright"
    assert fallback.calls == 2

    mon_df = collector.collect_special_monitoring(as_of_date=date(2026, 11, 28))
    assert len(mon_df) == 2
    wskt_row = mon_df[mon_df["ticker"] == "WSKT"].iloc[0]
    assert bool(wskt_row["is_fca_monitoring"]) is True
    assert wskt_row["listing_board"] == "Watchlist"

    assert classify_disclosure_category("Pembagian Dividen Interim") == "DIVIDEND"
    assert classify_disclosure_category("Penyampaian Laporan Keuangan") == "FINANCIAL_REPORT"
    assert classify_disclosure_category("Informasi atau Fakta Material") == "MATERIAL_INFORMATION"
    assert classify_disclosure_category("Panggilan RUPS Tahunan") == "CORPORATE_ACTION"
    assert classify_disclosure_category("Perubahan Sekretaris Perusahaan") == "OTHER"
    assert len(compute_filing_hash("BBCA", "2026", "Q3")) == 64


def test_ingestion_runner_idempotent_bronze_writes_and_watermarks() -> None:
    mem_s3 = InMemoryS3Bucket()
    run_store = InMemoryRunStore()
    r2_cfg = R2StorageSettings(R2_BUCKET="arthala-lakehouse")
    bronze_writer = BronzeParquetWriter(settings=r2_cfg, s3_client=mem_s3)
    quarantine_writer = QuarantineWriter(settings=r2_cfg, s3_client=mem_s3)

    runner = IngestionRunner(
        run_store=run_store,
        bronze_writer=bronze_writer,
        quarantine_writer=quarantine_writer,
    )

    # First run (fixtures mode)
    first_summaries = runner.run_all(
        tickers=["BBCA", "BBRI", "ASII", "WSKT"],
        as_of_date=date(2026, 11, 28),
        fiscal_year=2026,
        fiscal_quarter=3,
        mode="fixtures",
    )
    assert len(first_summaries) == 5
    assert all(s.status == "SUCCESS" for s in first_summaries)
    assert run_store.get_latest_watermark("idx_ohlcv") == "2026-11-28"
    assert run_store.get_latest_watermark("idx_financials_raw") == "2026-Q3"

    # Verify exact canonical Bronze Parquet partition keys exist
    written_keys = {key for (_, key) in mem_s3.objects}
    assert "bronze/idx_ohlcv/dt=2026-11-28/part-000.parquet" in written_keys
    assert (
        "bronze/idx_financials_raw/fiscal_year=2026/fiscal_quarter=Q3/part-000.parquet"
        in written_keys
    )
    assert "bronze/idx_dividends_raw/year=2026/part-000.parquet" in written_keys
    assert "bronze/idx_disclosures_raw/dt=2026-11-28/part-000.parquet" in written_keys
    assert "bronze/idx_special_monitoring/dt=2026-11-28/part-000.parquet" in written_keys

    counts_after_run1 = {
        key: len(pd.read_parquet(io.BytesIO(payload)))
        for (_, key), payload in mem_s3.objects.items()
    }

    # Second run on the exact same date/quarter MUST be 100% idempotent (zero duplicate rows!)
    second_summaries = runner.run_all(
        tickers=["BBCA", "BBRI", "ASII", "WSKT"],
        as_of_date=date(2026, 11, 28),
        fiscal_year=2026,
        fiscal_quarter=3,
        mode="fixtures",
    )
    assert all(s.status == "SUCCESS" for s in second_summaries)

    counts_after_run2 = {
        key: len(pd.read_parquet(io.BytesIO(payload)))
        for (_, key), payload in mem_s3.objects.items()
    }
    assert counts_after_run2 == counts_after_run1


def test_local_endpoint_safety_guardrail_and_postgres_run_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    safe_cfg = IngestionSettings(
        DATABASE_URL="postgresql://arthala:password@localhost:5432/arthala",
        R2_ENDPOINT_URL="http://localhost:9000",
    )
    assert_local_endpoints(safe_cfg)

    unsafe_db_cfg = IngestionSettings(
        DATABASE_URL="postgresql://u@ep-arthala.aws.neon.tech/neondb",
        R2_ENDPOINT_URL="http://localhost:9000",
    )
    with pytest.raises(RuntimeError, match="DATABASE_URL points to a non-local host"):
        assert_local_endpoints(unsafe_db_cfg)

    unsafe_r2_cfg = IngestionSettings(
        DATABASE_URL="postgresql://arthala:password@localhost:5432/arthala",
        R2_ENDPOINT_URL="https://s3.us-east-005.backblazeb2.com",
    )
    with pytest.raises(RuntimeError, match="R2_ENDPOINT_URL points to a non-local host"):
        assert_local_endpoints(unsafe_r2_cfg)

    # Test PostgresPipelineRunStore SQL calls via monkeypatched psycopg.connect
    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _FakeCur:
        def __enter__(self) -> _FakeCur:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            executed.append((sql, params))

        def fetchone(self) -> tuple[str]:
            if len(executed) == 1:
                return ("2026-11-27",)
            return ("uuid-run-1",)

    class _FakeConn:
        def __enter__(self) -> _FakeConn:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> _FakeCur:
            return _FakeCur()

    monkeypatch.setattr(psycopg, "connect", lambda *a, **kw: _FakeConn())
    store = PostgresPipelineRunStore(safe_cfg.database_url)
    assert store.get_latest_watermark("idx_ohlcv") == "2026-11-27"
    rid = store.start_run("idx_ohlcv")
    assert rid == "uuid-run-1"
    store.finish_run(
        rid,
        status="SUCCESS",
        rows_processed=10,
        rows_quarantined=0,
        watermark_value="2026-11-28",
    )
    assert len(executed) == 3


def test_curl_cffi_transport_and_yfinance_provider_adapters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import curl_cffi.requests as curl_requests
    import yfinance as yf

    class _FakeCurlResp:
        def __init__(self, status_code: int, text: str) -> None:
            self.status_code = status_code
            self.text = text

    responses = [
        _FakeCurlResp(200, '{"Results": [{"KodeEmiten": "BBCA"}]}'),
        _FakeCurlResp(403, "Forbidden"),
        _FakeCurlResp(200, "<!DOCTYPE html><html>challenge</html>"),
        _FakeCurlResp(500, "Internal Error"),
    ]

    def _fake_get(*args: Any, **kwargs: Any) -> _FakeCurlResp:
        return responses.pop(0)

    monkeypatch.setattr(curl_requests, "get", _fake_get)
    transport = CurlCffiTransport()

    ok_json = transport.fetch_json("https://www.idx.co.id/primary/test", params={"a": 1})
    assert ok_json["Results"][0]["KodeEmiten"] == "BBCA"

    with pytest.raises(BotChallengeError, match="403"):
        transport.fetch_json("https://www.idx.co.id/primary/test")

    with pytest.raises(BotChallengeError, match="HTML challenge"):
        transport.fetch_json("https://www.idx.co.id/primary/test")

    with pytest.raises(RuntimeError, match="HTTP 500"):
        transport.fetch_json("https://www.idx.co.id/primary/test")

    # YFinanceMarketProvider adapter test
    idx = pd.DatetimeIndex(["2026-11-27"])
    sample_dl = pd.DataFrame(
        {
            "Open": [9800.0],
            "High": [9900.0],
            "Low": [9750.0],
            "Close": [9850.0],
            "Volume": [10_000_000],
        },
        index=idx,
    )
    monkeypatch.setattr(yf, "download", lambda **kw: sample_dl)

    class _FakeTicker:
        def __init__(self, sym: str) -> None:
            assert sym.endswith(".JK")
            self.dividends = pd.Series([250.0], index=pd.DatetimeIndex(["2026-11-10"]))
            self.splits = pd.Series([2.0], index=pd.DatetimeIndex(["2026-11-15"]))

    monkeypatch.setattr(yf, "Ticker", _FakeTicker)

    yf_provider = YFinanceMarketProvider()
    dl_df = yf_provider.fetch_ohlcv_batch(
        ["BBCA.JK"], start_date=date(2026, 11, 20), end_date=date(2026, 11, 27)
    )
    assert len(dl_df) == 1
    divs, splits = yf_provider.fetch_ticker_actions("BBCA.JK", start_date=date(2026, 1, 1))
    assert len(divs) == 1
    assert len(splits) == 1


def test_runner_auto_fallback_partial_quarantine_and_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mem_s3 = InMemoryS3Bucket()
    run_store = InMemoryRunStore()
    r2_cfg = R2StorageSettings(R2_BUCKET="arthala-lakehouse")
    bronze_writer = BronzeParquetWriter(settings=r2_cfg, s3_client=mem_s3)
    quarantine_writer = QuarantineWriter(settings=r2_cfg, s3_client=mem_s3)

    class _FailingMarket(MarketCollector):
        def collect_ohlcv(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            raise ConnectionError("offline sandbox")

        def collect_dividends_and_splits(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            return pd.DataFrame()

    class _FailingIdx(IdxCollector):
        def collect_quarterly_financials(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            raise ConnectionError("offline sandbox")

        def collect_disclosures(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            raise ConnectionError("offline sandbox")

        def collect_special_monitoring(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
            # Return 1 valid row and 1 malformed row to verify PARTIAL + quarantine write
            return pd.DataFrame(
                [
                    {
                        "ticker": "BBCA",
                        "company_name": "PT Bank Central Asia Tbk",
                        "as_of_date": "2026-11-28",
                        "listing_board": "Main",
                        "special_notations": "[]",
                        "is_fca_monitoring": False,
                        "sector": "Financials",
                        "sub_sector": "Banks",
                    },
                    {
                        "ticker": "bad!",
                        "company_name": "",
                        "as_of_date": "2026-11-28",
                        "listing_board": "InvalidBoard",
                        "special_notations": "[]",
                        "is_fca_monitoring": False,
                        "sector": None,
                        "sub_sector": None,
                    },
                ]
            )

    runner = IngestionRunner(
        run_store=run_store,
        bronze_writer=bronze_writer,
        quarantine_writer=quarantine_writer,
        market_collector=_FailingMarket(),
        idx_collector=_FailingIdx(),
    )
    summaries = runner.run_all(
        tickers=["BBCA", "WSKT"],
        as_of_date=date(2026, 11, 28),
        fiscal_year=2026,
        fiscal_quarter=3,
        mode="auto",
    )
    mon_summary = next(s for s in summaries if s.pipeline_name == "idx_special_monitoring")
    assert mon_summary.status == "PARTIAL"
    assert mon_summary.rows_valid == 1
    assert mon_summary.rows_quarantined == 1
    assert mon_summary.quarantine_key is not None

    # Test CLI main() wired with fake run store and fake S3
    monkeypatch.setattr(
        "pipelines.ingestion.runner.PostgresPipelineRunStore",
        lambda db_url: run_store,
    )
    monkeypatch.setattr(
        "pipelines.ingestion.runner.BronzeParquetWriter",
        lambda settings: bronze_writer,
    )
    monkeypatch.setattr(
        "pipelines.ingestion.runner.QuarantineWriter",
        lambda settings: quarantine_writer,
    )
    assert load_ingestion_settings("local").app_env in {"local", "ci"}
    exit_code = runner_main(["--env", "local", "--mode", "fixtures", "--tickers", "BBCA,ASII"])
    assert exit_code == 0
