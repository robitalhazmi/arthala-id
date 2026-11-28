"""Deterministic offline IDX seed fixtures for local sandbox runs and CI tests.

Covers Indonesian banking stocks (``BBCA``, ``BBRI``, ``BMRI``, ``BBNI``),
non-bank blue chips (``ASII``, ``TLKM``, ``UNVR``), and a distressed Notasi
Khusus / FCA issuer (``WSKT``) with multi-quarter history so Silver/Gold dbt
models and Phase 2 ML training work 100% offline.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

import pandas as pd

from pipelines.ingestion.idx_collector import (
    classify_disclosure_category,
    compute_filing_hash,
)
from pipelines.ingestion.market_collector import (
    DEFAULT_IDX_TICKERS,
    normalize_idx_ticker,
)

_TICKER_META: dict[str, dict[str, Any]] = {
    "BBCA": {
        "company_name": "PT Bank Central Asia Tbk",
        "sector": "Financials",
        "sub_sector": "Banks",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 9850.0,
        "volume": 52_400_000,
        "dividend_dps": 270.0,
    },
    "BBRI": {
        "company_name": "PT Bank Rakyat Indonesia (Persero) Tbk",
        "sector": "Financials",
        "sub_sector": "Banks",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 4620.0,
        "volume": 115_800_000,
        "dividend_dps": 315.0,
    },
    "BMRI": {
        "company_name": "PT Bank Mandiri (Persero) Tbk",
        "sector": "Financials",
        "sub_sector": "Banks",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 6150.0,
        "volume": 64_100_000,
        "dividend_dps": 350.0,
    },
    "BBNI": {
        "company_name": "PT Bank Negara Indonesia (Persero) Tbk",
        "sector": "Financials",
        "sub_sector": "Banks",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 4980.0,
        "volume": 38_500_000,
        "dividend_dps": 280.0,
    },
    "ASII": {
        "company_name": "PT Astra International Tbk",
        "sector": "Industrials",
        "sub_sector": "Multi-sector Holdings",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 5075.0,
        "volume": 41_200_000,
        "dividend_dps": 420.0,
    },
    "TLKM": {
        "company_name": "PT Telkom Indonesia (Persero) Tbk",
        "sector": "Infrastructures",
        "sub_sector": "Telecommunication",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 2780.0,
        "volume": 79_300_000,
        "dividend_dps": 178.5,
    },
    "UNVR": {
        "company_name": "PT Unilever Indonesia Tbk",
        "sector": "Consumer Non-Cyclicals",
        "sub_sector": "Personal Care Products",
        "listing_board": "Main",
        "special_notations": [],
        "base_price": 2150.0,
        "volume": 19_600_000,
        "dividend_dps": 130.0,
    },
    "WSKT": {
        "company_name": "PT Waskita Karya (Persero) Tbk",
        "sector": "Infrastructures",
        "sub_sector": "Heavy Constructions & Civil Engineering",
        "listing_board": "Watchlist",
        "special_notations": ["E", "L", "X"],
        "base_price": 202.0,
        "volume": 4_100_000,
        "dividend_dps": 0.0,
    },
}


def build_fixture_ohlcv(
    tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
    *,
    as_of_date: date,
    days: int = 5,
) -> pd.DataFrame:
    """Generate deterministic daily OHLCV rows ending at ``as_of_date``."""
    records: list[dict[str, Any]] = []
    for raw in tickers:
        ticker = normalize_idx_ticker(raw)
        meta = _TICKER_META.get(ticker, _TICKER_META["ASII"])
        base = float(meta["base_price"])
        vol_base = int(meta["volume"])

        for offset in range(days - 1, -1, -1):
            dt = as_of_date - timedelta(days=offset)
            step = (days - 1 - offset) * 10.0
            open_p = round(base + step - 15.0, 2)
            close_p = round(base + step, 2)
            high_p = round(max(open_p, close_p) + 25.0, 2)
            low_p = round(max(50.0, min(open_p, close_p) - 20.0), 2)
            records.append(
                {
                    "ticker": ticker,
                    "trade_date": dt.isoformat(),
                    "open_price": open_p,
                    "high_price": high_p,
                    "low_price": low_p,
                    "close_price": close_p,
                    "volume": vol_base + offset * 250_000,
                    "adj_close": close_p,
                    "source": "idx",
                }
            )
    return pd.DataFrame(records)


def build_fixture_financials(
    tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
    *,
    fiscal_year: int = 2026,
    fiscal_quarter: int = 3,
) -> pd.DataFrame:
    """Generate deterministic quarterly financial statement rows."""
    records: list[dict[str, Any]] = []
    filing_dt = date(fiscal_year, min(12, fiscal_quarter * 3 + 1), 28).isoformat()

    for raw in tickers:
        ticker = normalize_idx_ticker(raw)
        if ticker == "WSKT":
            rev = 7_800_000_000_000.0
            ni = -2_450_000_000_000.0
            ocf = -1_150_000_000_000.0
            assets = 84_000_000_000_000.0
            liab = 90_500_000_000_000.0
            eq = -6_500_000_000_000.0
            st_debt = 18_200_000_000_000.0
            cash = 1_100_000_000_000.0
            ret_earn = -14_000_000_000_000.0
            ebit = -900_000_000_000.0
            wc = -9_500_000_000_000.0
            eps = -85.0
        elif ticker in {"BBCA", "BBRI", "BMRI", "BBNI"}:
            rev = 78_000_000_000_000.0
            ni = 41_200_000_000_000.0
            ocf = 46_500_000_000_000.0
            assets = 1_450_000_000_000_000.0
            liab = 1_190_000_000_000_000.0
            eq = 260_000_000_000_000.0
            st_debt = 35_000_000_000_000.0
            cash = 95_000_000_000_000.0
            ret_earn = 195_000_000_000_000.0
            ebit = 52_000_000_000_000.0
            wc = 60_000_000_000_000.0
            eps = 335.0
        else:
            rev = 245_000_000_000_000.0
            ni = 25_800_000_000_000.0
            ocf = 31_400_000_000_000.0
            assets = 445_000_000_000_000.0
            liab = 195_000_000_000_000.0
            eq = 250_000_000_000_000.0
            st_debt = 28_000_000_000_000.0
            cash = 48_000_000_000_000.0
            ret_earn = 180_000_000_000_000.0
            ebit = 36_000_000_000_000.0
            wc = 54_000_000_000_000.0
            eps = 637.0

        f_hash = compute_filing_hash(
            ticker, str(fiscal_year), f"Q{fiscal_quarter}", filing_dt, str(assets), str(ni)
        )
        records.append(
            {
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "filing_date": filing_dt,
                "revenue_idr": rev,
                "net_income_idr": ni,
                "operating_cf_idr": ocf,
                "total_assets_idr": assets,
                "total_liabilities_idr": liab,
                "total_equity_idr": eq,
                "short_term_debt_idr": st_debt,
                "cash_and_equivalents_idr": cash,
                "retained_earnings_idr": ret_earn,
                "ebit_idr": ebit,
                "working_capital_idr": wc,
                "eps": eps,
                "currency": "IDR",
                "filing_hash": f_hash,
            }
        )
    return pd.DataFrame(records)


def build_fixture_dividends(
    tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
    *,
    year: int = 2026,
) -> pd.DataFrame:
    """Generate deterministic cash dividend calendar rows."""
    records: list[dict[str, Any]] = []
    for raw in tickers:
        ticker = normalize_idx_ticker(raw)
        meta = _TICKER_META.get(ticker, _TICKER_META["ASII"])
        dps = float(meta["dividend_dps"])
        if dps <= 0:
            continue
        cum_dt = date(year, 11, 10)
        ex_dt = date(year, 11, 11)
        rec_dt = date(year, 11, 12)
        pay_dt = date(year, 11, 28)
        records.append(
            {
                "ticker": ticker,
                "action_type": "CASH_DIVIDEND",
                "fiscal_year": float(year),
                "dividend_per_share": dps,
                "split_ratio": None,
                "currency": "IDR",
                "cum_date": cum_dt.isoformat(),
                "ex_date": ex_dt.isoformat(),
                "recording_date": rec_dt.isoformat(),
                "payment_date": pay_dt.isoformat(),
                "payout_ratio_pct": 55.0,
            }
        )
    return pd.DataFrame(records)


def build_fixture_disclosures(
    tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
    *,
    as_of_date: date,
) -> pd.DataFrame:
    """Generate deterministic IDX announcement/disclosure rows."""
    records: list[dict[str, Any]] = []
    pub_iso = f"{as_of_date.isoformat()}T17:15:00+07:00"
    for raw in tickers:
        ticker = normalize_idx_ticker(raw)
        if ticker == "WSKT":
            title = f"Informasi Permohonan PKPU dan Restrukturisasi Utang {ticker}"
        else:
            title = f"Penyampaian Laporan Keuangan Interim Kuartal III {ticker}"
        disc_id = f"IDX-{as_of_date.strftime('%Y%m%d')}-{ticker}"
        f_hash = compute_filing_hash(disc_id, ticker, pub_iso, title)
        records.append(
            {
                "disclosure_id": disc_id,
                "ticker": ticker,
                "published_at": pub_iso,
                "title": title,
                "category": classify_disclosure_category(title),
                "attachment_url": f"https://www.idx.co.id/StaticData/News/{disc_id}.pdf",
                "filing_hash": f_hash,
                "source": "fixtures",
            }
        )
    return pd.DataFrame(records)


def build_fixture_special_monitoring(
    tickers: Sequence[str] = DEFAULT_IDX_TICKERS,
    *,
    as_of_date: date,
) -> pd.DataFrame:
    """Generate deterministic IDX company board & Notasi Khusus / FCA rows."""
    records: list[dict[str, Any]] = []
    for raw in tickers:
        ticker = normalize_idx_ticker(raw)
        meta = _TICKER_META.get(
            ticker,
            {
                "company_name": f"PT {ticker} Tbk",
                "sector": "Industrials",
                "sub_sector": "General",
                "listing_board": "Main",
                "special_notations": [],
            },
        )
        notations: list[str] = list(meta["special_notations"])
        board = str(meta["listing_board"])
        records.append(
            {
                "ticker": ticker,
                "company_name": str(meta["company_name"]),
                "as_of_date": as_of_date.isoformat(),
                "listing_board": board,
                "special_notations": json.dumps(notations),
                "is_fca_monitoring": board == "Watchlist" or ("X" in notations),
                "sector": str(meta["sector"]),
                "sub_sector": str(meta["sub_sector"]),
            }
        )
    return pd.DataFrame(records)
