"""EOD OHLCV and dividend/split collector for IDX (.JK) tickers via yfinance.

Implements Task 1.2a (``docs/Task.md``):
- Normalizes 4-letter IDX symbols (``BBCA`` <-> ``BBCA.JK``).
- Batches ticker downloads to avoid rate-limit spikes and oversized requests.
- Retries transient ``yfinance`` / network failures with exponential jittered backoff.
- Emits DataFrames shaped for ``PriceSchema`` and ``CorporateActionSchema``.
"""

from __future__ import annotations

import functools
import secrets
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol

import pandas as pd
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

DEFAULT_IDX_TICKERS: tuple[str, ...] = (
    "BBCA",
    "BBRI",
    "BMRI",
    "BBNI",
    "ASII",
    "TLKM",
    "UNVR",
    "WSKT",
)


def normalize_idx_ticker(raw_symbol: str) -> str:
    """Normalize ``'bbca.jk'`` or ``' BBCA '`` into canonical 4-letter ``'BBCA'``."""
    cleaned = raw_symbol.strip().upper()
    if cleaned.endswith(".JK"):
        cleaned = cleaned[:-3]
    return cleaned


def to_yahoo_symbol(idx_ticker: str) -> str:
    """Convert canonical ``'BBCA'`` into Yahoo Finance ``'BBCA.JK'``."""
    return f"{normalize_idx_ticker(idx_ticker)}.JK"


def chunk_tickers(tickers: Sequence[str], batch_size: int) -> list[list[str]]:
    """Split normalized unique tickers into fixed-size batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    seen: set[str] = set()
    ordered: list[str] = []
    for t in tickers:
        norm = normalize_idx_ticker(t)
        if norm and norm not in seen:
            seen.add(norm)
            ordered.append(norm)
    return [ordered[i : i + batch_size] for i in range(0, len(ordered), batch_size)]


class MarketDataProviderProtocol(Protocol):
    """Injectable market data provider interface for ``yfinance`` and offline tests."""

    def fetch_ohlcv_batch(
        self,
        yahoo_symbols: Sequence[str],
        *,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame: ...

    def fetch_ticker_actions(
        self,
        yahoo_symbol: str,
        *,
        start_date: date,
    ) -> tuple[pd.Series[float], pd.Series[float]]: ...


class YFinanceMarketProvider:
    """Production ``yfinance`` adapter for ``.JK`` OHLCV and corporate actions."""

    def fetch_ohlcv_batch(
        self,
        yahoo_symbols: Sequence[str],
        *,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        import yfinance as yf

        # yfinance end date is exclusive, so add +1 day to include end_date EOD bar
        inclusive_end = (end_date + timedelta(days=1)).isoformat()
        raw = yf.download(
            tickers=list(yahoo_symbols),
            start=start_date.isoformat(),
            end=inclusive_end,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()

    def fetch_ticker_actions(
        self,
        yahoo_symbol: str,
        *,
        start_date: date,
    ) -> tuple[pd.Series[float], pd.Series[float]]:
        import yfinance as yf

        ticker_obj = yf.Ticker(yahoo_symbol)
        dividends = ticker_obj.dividends
        splits = ticker_obj.splits

        start_ts = pd.Timestamp(start_date.isoformat())
        if isinstance(dividends, pd.Series) and not dividends.empty:
            div_idx = pd.to_datetime(dividends.index).tz_localize(None)
            dividends = pd.Series(dividends.to_numpy(dtype=float), index=div_idx)
            dividends = dividends[dividends.index >= start_ts]
        else:
            dividends = pd.Series(dtype=float)

        if isinstance(splits, pd.Series) and not splits.empty:
            split_idx = pd.to_datetime(splits.index).tz_localize(None)
            splits = pd.Series(splits.to_numpy(dtype=float), index=split_idx)
            splits = splits[splits.index >= start_ts]
        else:
            splits = pd.Series(dtype=float)

        return dividends, splits


def _jitter_fraction() -> float:
    """Cryptographically safe jitter fraction in [0.0, 0.5) (avoids S311 bandit warning)."""
    return (secrets.randbelow(500)) / 1000.0


class MarketCollector:
    """Batched EOD OHLCV & corporate actions collector with jittered retry backoff."""

    def __init__(
        self,
        provider: MarketDataProviderProtocol | None = None,
        *,
        batch_size: int = 20,
        max_retries: int = 3,
        base_backoff_seconds: float = 1.0,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._provider = provider or YFinanceMarketProvider()
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._base_backoff_seconds = base_backoff_seconds
        self._sleep_fn = sleep_fn

    def _call_with_backoff(self, operation_name: str, fn: Callable[[], Any]) -> Any:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                if attempt + 1 >= self._max_retries:
                    break
                delay = self._base_backoff_seconds * (2**attempt) + _jitter_fraction()
                logger.warning(
                    "market_collector.retry_backoff",
                    operation=operation_name,
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    backoff_seconds=round(delay, 3),
                    error=str(exc),
                )
                self._sleep_fn(delay)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Operation {operation_name} failed without exception")

    def collect_ohlcv(
        self,
        tickers: Sequence[str],
        *,
        start_date: date,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV rows across batched ``.JK`` tickers."""
        resolved_end = end_date or datetime.now(tz=UTC).date()
        batches = chunk_tickers(tickers, self._batch_size)
        records: list[dict[str, Any]] = []

        for batch in batches:
            yahoo_batch = [to_yahoo_symbol(t) for t in batch]
            raw_df: pd.DataFrame = self._call_with_backoff(
                f"fetch_ohlcv_batch({','.join(batch)})",
                functools.partial(
                    self._provider.fetch_ohlcv_batch,
                    yahoo_batch,
                    start_date=start_date,
                    end_date=resolved_end,
                ),
            )
            if raw_df.empty:
                continue
            records.extend(_normalize_yfinance_ohlcv_frame(raw_df, batch))

        columns = [
            "ticker",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "adj_close",
            "source",
        ]
        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records, columns=columns)

    def collect_dividends_and_splits(
        self,
        tickers: Sequence[str],
        *,
        start_date: date,
    ) -> pd.DataFrame:
        """Fetch cash dividends and stock splits for ``.JK`` tickers."""
        records: list[dict[str, Any]] = []
        for batch in chunk_tickers(tickers, self._batch_size):
            for ticker in batch:
                yahoo_sym = to_yahoo_symbol(ticker)
                dividends, splits = self._call_with_backoff(
                    f"fetch_ticker_actions({ticker})",
                    functools.partial(
                        self._provider.fetch_ticker_actions,
                        yahoo_sym,
                        start_date=start_date,
                    ),
                )
                for ts, amount in dividends.items():
                    ex_dt = pd.Timestamp(str(ts)).date()
                    # On IDX, ex_date is T+1 trading day after cum_date; when yfinance
                    # provides the ex-dividend timestamp, record cum_date <= ex_date.
                    cum_dt = ex_dt - timedelta(days=1)
                    records.append(
                        {
                            "ticker": ticker,
                            "action_type": "CASH_DIVIDEND",
                            "fiscal_year": float(cum_dt.year),
                            "dividend_per_share": float(amount),
                            "split_ratio": None,
                            "currency": "IDR",
                            "cum_date": cum_dt.isoformat(),
                            "ex_date": ex_dt.isoformat(),
                            "recording_date": (ex_dt + timedelta(days=1)).isoformat(),
                            "payment_date": (ex_dt + timedelta(days=18)).isoformat(),
                            "payout_ratio_pct": None,
                        }
                    )
                for ts, ratio in splits.items():
                    if float(ratio) <= 0:
                        continue
                    ex_dt = pd.Timestamp(str(ts)).date()
                    act_type = "STOCK_SPLIT" if float(ratio) >= 1.0 else "REVERSE_SPLIT"
                    records.append(
                        {
                            "ticker": ticker,
                            "action_type": act_type,
                            "fiscal_year": float(ex_dt.year),
                            "dividend_per_share": None,
                            "split_ratio": float(ratio),
                            "currency": "IDR",
                            "cum_date": ex_dt.isoformat(),
                            "ex_date": ex_dt.isoformat(),
                            "recording_date": None,
                            "payment_date": None,
                            "payout_ratio_pct": None,
                        }
                    )

        columns = [
            "ticker",
            "action_type",
            "fiscal_year",
            "dividend_per_share",
            "split_ratio",
            "currency",
            "cum_date",
            "ex_date",
            "recording_date",
            "payment_date",
            "payout_ratio_pct",
        ]
        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records, columns=columns)


def _normalize_yfinance_ohlcv_frame(
    raw_df: pd.DataFrame,
    batch_tickers: Sequence[str],
) -> list[dict[str, Any]]:
    """Extract normalized OHLCV rows from either MultiIndex or flat yfinance frames."""
    rows: list[dict[str, Any]] = []
    if isinstance(raw_df.columns, pd.MultiIndex):
        top_levels = {str(x) for x in raw_df.columns.get_level_values(0)}
        for ticker in batch_tickers:
            yahoo_sym = to_yahoo_symbol(ticker)
            key = yahoo_sym if yahoo_sym in top_levels else (ticker if ticker in top_levels else "")
            if not key:
                continue
            sub_obj: Any = raw_df[key]
            if isinstance(sub_obj, pd.DataFrame):
                rows.extend(_extract_single_ticker_ohlcv(sub_obj, ticker))
    elif len(batch_tickers) == 1:
        rows.extend(_extract_single_ticker_ohlcv(raw_df, batch_tickers[0]))
    return rows


def _extract_single_ticker_ohlcv(sub_df: pd.DataFrame, ticker: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    col_map: dict[str, str] = {str(c).lower(): str(c) for c in sub_df.columns}
    open_col = col_map.get("open")
    high_col = col_map.get("high")
    low_col = col_map.get("low")
    close_col = col_map.get("close")
    vol_col = col_map.get("volume")
    adj_col = col_map.get("adj close") or col_map.get("adj_close")

    if (
        open_col is None
        or high_col is None
        or low_col is None
        or close_col is None
        or vol_col is None
    ):
        return out

    for idx_val, row in sub_df.iterrows():
        close_val = row[close_col]
        if pd.isna(close_val):
            continue
        trade_dt = pd.Timestamp(str(idx_val)).date().isoformat()
        adj_val = row[adj_col] if adj_col is not None and not pd.isna(row[adj_col]) else close_val
        vol_raw = row[vol_col]
        out.append(
            {
                "ticker": ticker,
                "trade_date": trade_dt,
                "open_price": float(row[open_col]),
                "high_price": float(row[high_col]),
                "low_price": float(row[low_col]),
                "close_price": float(close_val),
                "volume": 0 if pd.isna(vol_raw) else int(float(vol_raw)),
                "adj_close": float(adj_val),
                "source": "yfinance",
            }
        )
    return out
