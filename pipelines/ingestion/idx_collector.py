"""IDX quarterly financials, disclosures & Notasi Khusus (FCA) collector.

Implements Task 1.2b (``docs/Task.md``) and ``AGENTS.md`` §5.2 scraper hygiene:
- Primary transport: ``curl_cffi`` (``impersonate="chrome"``) with polite jittered
  rate limiting, timeouts, and exponential retry backoff.
- Fallback transport: headless ``Playwright`` browser session triggered automatically
  when IDX / Cloudflare blocks direct HTTP calls (HTTP 403/503 or HTML challenge).
- Deterministic SHA-256 ``filing_hash`` computation for watermark deduplication.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol
from urllib.parse import urlencode

import pandas as pd
import structlog

from pipelines.ingestion.market_collector import normalize_idx_ticker

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

IDX_BASE_URL = "https://www.idx.co.id/primary"
IDX_FINANCIAL_REPORT_PATH = "/ListedCompany/GetFinancialReport"
IDX_ANNOUNCEMENT_PATH = "/NewsAnnouncement/GetAllAnnouncement"
IDX_SPECIAL_NOTATION_PATH = "/ListedCompany/GetCompanyProfiles"

ROTATING_ACCEPT_LANGUAGES: tuple[str, ...] = (
    "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,id-ID;q=0.8,id;q=0.7",
)


class BotChallengeError(RuntimeError):
    """Raised when an IDX endpoint returns a Cloudflare/WAF block or HTML challenge."""


class JsonTransportProtocol(Protocol):
    """Injectable JSON HTTP transport for primary ``curl_cffi`` and Playwright fallback."""

    def fetch_json(
        self,
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        timeout_seconds: float = 20.0,
    ) -> dict[str, Any]: ...


def compute_filing_hash(*parts: str) -> str:
    """Compute a deterministic 64-char SHA-256 hex digest over canonical filing parts."""
    canonical = "|".join(p.strip() for p in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def classify_disclosure_category(title: str) -> str:
    """Classify an Indonesian/English IDX disclosure headline into an alert category."""
    lowered = title.lower()
    if any(k in lowered for k in ("pkpu", "pailit", "gugatan", "suspensi", "bankrupt")):
        return "PKPU_LITIGATION"
    if any(k in lowered for k in ("dividen", "dividend")):
        return "DIVIDEND"
    if any(k in lowered for k in ("laporan keuangan", "financial statement", "audit", "interim")):
        return "FINANCIAL_REPORT"
    if any(
        k in lowered for k in ("fakta material", "informasi atau fakta", "material information")
    ):
        return "MATERIAL_INFORMATION"
    if any(k in lowered for k in ("rups", "stock split", "rights issue", "hmetd", "buyback")):
        return "CORPORATE_ACTION"
    return "OTHER"


def _build_url(url: str, params: Mapping[str, str | int] | None) -> str:
    if not params:
        return url
    query = urlencode({k: str(v) for k, v in params.items()})
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{query}"


def _polite_jitter(min_ms: int = 150, max_ms: int = 450) -> float:
    span = max(1, max_ms - min_ms)
    return (min_ms + secrets.randbelow(span)) / 1000.0


class CurlCffiTransport:
    """Primary IDX JSON fetcher using ``curl_cffi`` Chrome TLS impersonation."""

    def __init__(self, impersonate: str = "chrome") -> None:
        self._impersonate = impersonate

    def fetch_json(
        self,
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        timeout_seconds: float = 20.0,
    ) -> dict[str, Any]:
        from curl_cffi import requests as curl_requests

        lang = ROTATING_ACCEPT_LANGUAGES[secrets.randbelow(len(ROTATING_ACCEPT_LANGUAGES))]
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": lang,
            "Referer": "https://www.idx.co.id/",
        }
        full_url = _build_url(url, params)
        resp = curl_requests.get(
            full_url,
            headers=headers,
            impersonate=self._impersonate,
            timeout=timeout_seconds,
        )
        status = int(resp.status_code)
        if status in {403, 503}:
            raise BotChallengeError(f"IDX HTTP {status} bot challenge on {url}")
        if status >= 400:
            raise RuntimeError(f"IDX HTTP {status} error on {url}")

        body_text = resp.text.strip()
        if body_text.startswith("<!DOCTYPE") or body_text.startswith("<html"):
            raise BotChallengeError(f"Received HTML challenge instead of JSON on {url}")

        parsed = json.loads(body_text)
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected JSON object from {url}, got {type(parsed).__name__}")
        return parsed


class PlaywrightFallbackTransport:
    """Headless Playwright Chromium fallback when IDX blocks direct ``curl_cffi`` HTTP calls."""

    def fetch_json(
        self,
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        timeout_seconds: float = 20.0,
    ) -> dict[str, Any]:
        from playwright.sync_api import sync_playwright

        full_url = _build_url(url, params)
        timeout_ms = int(timeout_seconds * 1000)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(locale="id-ID")
                page = context.new_page()
                page.goto(
                    "https://www.idx.co.id/id",
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                api_resp = context.request.get(
                    full_url,
                    headers={
                        "Accept": "application/json, text/plain, */*",
                        "Referer": "https://www.idx.co.id/id",
                    },
                    timeout=timeout_ms,
                )
                if not api_resp.ok:
                    raise RuntimeError(f"Playwright fallback HTTP {api_resp.status} on {url}")
                parsed = api_resp.json()
                if not isinstance(parsed, dict):
                    raise ValueError("Expected JSON object from Playwright IDX response")
                return parsed
            finally:
                browser.close()


class IdxCollector:
    """Multi-transport IDX fundamentals, disclosures, and Notasi Khusus collector."""

    def __init__(
        self,
        primary_transport: JsonTransportProtocol | None = None,
        fallback_transport: JsonTransportProtocol | None = None,
        *,
        base_url: str = IDX_BASE_URL,
        max_retries: int = 3,
        base_backoff_seconds: float = 1.0,
        timeout_seconds: float = 20.0,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self._primary = primary_transport or CurlCffiTransport()
        self._fallback = fallback_transport or PlaywrightFallbackTransport()
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._base_backoff_seconds = base_backoff_seconds
        self._timeout_seconds = timeout_seconds
        self._sleep_fn = sleep_fn

    def _fetch_with_resilience(
        self,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
    ) -> tuple[dict[str, Any], str]:
        """Fetch JSON via primary ``curl_cffi`` with retries, falling back to Playwright."""
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None

        for attempt in range(self._max_retries):
            self._sleep_fn(_polite_jitter())
            try:
                payload = self._primary.fetch_json(
                    url, params=params, timeout_seconds=self._timeout_seconds
                )
                return payload, "idx"
            except BotChallengeError as challenge_exc:
                logger.warning(
                    "idx_collector.bot_challenge_fallback",
                    url=url,
                    attempt=attempt + 1,
                    error=str(challenge_exc),
                )
                payload = self._fallback.fetch_json(
                    url, params=params, timeout_seconds=self._timeout_seconds
                )
                return payload, "playwright"
            except Exception as exc:
                last_exc = exc
                if attempt + 1 >= self._max_retries:
                    break
                delay = self._base_backoff_seconds * (2**attempt) + _polite_jitter()
                logger.warning(
                    "idx_collector.retry_backoff",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    backoff_seconds=round(delay, 3),
                    error=str(exc),
                )
                self._sleep_fn(delay)

        logger.warning(
            "idx_collector.primary_exhausted_trying_playwright",
            url=url,
            error=str(last_exc),
        )
        payload = self._fallback.fetch_json(
            url, params=params, timeout_seconds=self._timeout_seconds
        )
        return payload, "playwright"

    def collect_quarterly_financials(
        self,
        tickers: Sequence[str],
        *,
        fiscal_year: int,
        fiscal_quarter: int,
    ) -> pd.DataFrame:
        """Fetch IDX quarterly fundamental statements for ``tickers``."""
        period_map = {1: "tw1", 2: "tw2", 3: "tw3", 4: "audit"}
        period_code = period_map.get(fiscal_quarter)
        if period_code is None:
            raise ValueError(f"fiscal_quarter must be in 1..4, got {fiscal_quarter}")

        records: list[dict[str, Any]] = []
        for raw_symbol in tickers:
            ticker = normalize_idx_ticker(raw_symbol)
            payload, _ = self._fetch_with_resilience(
                IDX_FINANCIAL_REPORT_PATH,
                params={
                    "indexFrom": 1,
                    "pageSize": 20,
                    "year": fiscal_year,
                    "reportType": "rdf",
                    "periode": period_code,
                    "kodeEmiten": ticker,
                },
            )
            items = payload.get("Results") or payload.get("results") or []
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict):
                    parsed = _parse_financial_item(
                        item,
                        fallback_ticker=ticker,
                        fallback_year=fiscal_year,
                        fallback_quarter=fiscal_quarter,
                    )
                    if parsed is not None:
                        records.append(parsed)

        columns = [
            "ticker",
            "fiscal_year",
            "fiscal_quarter",
            "filing_date",
            "revenue_idr",
            "net_income_idr",
            "operating_cf_idr",
            "total_assets_idr",
            "total_liabilities_idr",
            "total_equity_idr",
            "short_term_debt_idr",
            "cash_and_equivalents_idr",
            "retained_earnings_idr",
            "ebit_idr",
            "working_capital_idr",
            "eps",
            "currency",
            "filing_hash",
        ]
        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records, columns=columns)

    def collect_disclosures(
        self,
        tickers: Sequence[str],
        *,
        from_date: date,
        to_date: date | None = None,
    ) -> pd.DataFrame:
        """Fetch IDX corporate announcements / disclosures across ``tickers``."""
        resolved_to = to_date or datetime.now(tz=UTC).date()
        records: list[dict[str, Any]] = []

        for raw_symbol in tickers:
            ticker = normalize_idx_ticker(raw_symbol)
            payload, transport_source = self._fetch_with_resilience(
                IDX_ANNOUNCEMENT_PATH,
                params={
                    "kodeEmiten": ticker,
                    "indexFrom": 0,
                    "pageSize": 50,
                    "dateFrom": from_date.strftime("%Y%m%d"),
                    "dateTo": resolved_to.strftime("%Y%m%d"),
                    "lang": "id",
                },
            )
            items = payload.get("Replies") or payload.get("Results") or []
            if not isinstance(items, list):
                continue
            for entry in items:
                if not isinstance(entry, dict):
                    continue
                raw_peng = entry.get("pengumuman")
                ann: dict[str, Any] = raw_peng if isinstance(raw_peng, dict) else entry
                title = str(ann.get("JudulPengumuman") or ann.get("title") or "").strip()
                pub_raw = str(ann.get("TglPengumuman") or ann.get("published_at") or "").strip()
                disc_id = str(
                    ann.get("Id") or ann.get("NoPengumuman") or ann.get("disclosure_id") or ""
                ).strip()
                if not title or not pub_raw:
                    continue
                if not disc_id:
                    disc_id = compute_filing_hash(ticker, pub_raw, title)[:24]

                attachments = entry.get("attachments")
                att_url: str | None = None
                if (
                    isinstance(attachments, list)
                    and attachments
                    and isinstance(attachments[0], dict)
                ):
                    att_url = str(attachments[0].get("FullSavePath") or "") or None
                elif ann.get("attachment_url"):
                    att_url = str(ann["attachment_url"])

                f_hash = compute_filing_hash(disc_id, ticker, pub_raw, title)
                records.append(
                    {
                        "disclosure_id": disc_id,
                        "ticker": normalize_idx_ticker(
                            str(ann.get("Kode_Emiten") or ann.get("ticker") or ticker)
                        ),
                        "published_at": pub_raw,
                        "title": title,
                        "category": classify_disclosure_category(title),
                        "attachment_url": att_url,
                        "filing_hash": f_hash,
                        "source": transport_source,
                    }
                )

        columns = [
            "disclosure_id",
            "ticker",
            "published_at",
            "title",
            "category",
            "attachment_url",
            "filing_hash",
            "source",
        ]
        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records, columns=columns)

    def collect_special_monitoring(
        self,
        *,
        as_of_date: date | None = None,
    ) -> pd.DataFrame:
        """Fetch IDX company profiles, listing boards, and Notasi Khusus / FCA notations."""
        resolved_date = as_of_date or datetime.now(tz=UTC).date()
        payload, _ = self._fetch_with_resilience(
            IDX_SPECIAL_NOTATION_PATH,
            params={"start": 0, "length": 1000},
        )
        items = payload.get("data") or payload.get("Results") or []
        records: list[dict[str, Any]] = []
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                ticker = normalize_idx_ticker(
                    str(item.get("KodeEmiten") or item.get("ticker") or "")
                )
                company_name = str(item.get("NamaEmiten") or item.get("company_name") or "").strip()
                if not ticker or not company_name:
                    continue

                board_raw = str(item.get("PapanPencatatan") or item.get("listing_board") or "Main")
                board = _normalize_listing_board(board_raw)

                raw_notations = item.get("NotasiKhusus") or item.get("special_notations") or []
                if isinstance(raw_notations, str):
                    notations = [
                        code.strip().upper() for code in raw_notations.split(",") if code.strip()
                    ]
                elif isinstance(raw_notations, list):
                    notations = [
                        str(code).strip().upper() for code in raw_notations if str(code).strip()
                    ]
                else:
                    notations = []

                is_fca = board == "Watchlist" or ("X" in notations)
                records.append(
                    {
                        "ticker": ticker,
                        "company_name": company_name,
                        "as_of_date": resolved_date.isoformat(),
                        "listing_board": board,
                        "special_notations": json.dumps(sorted(set(notations))),
                        "is_fca_monitoring": is_fca,
                        "sector": str(item.get("Sektor") or item.get("sector") or "") or None,
                        "sub_sector": str(item.get("SubSektor") or item.get("sub_sector") or "")
                        or None,
                    }
                )

        columns = [
            "ticker",
            "company_name",
            "as_of_date",
            "listing_board",
            "special_notations",
            "is_fca_monitoring",
            "sector",
            "sub_sector",
        ]
        if not records:
            return pd.DataFrame(columns=columns)
        return pd.DataFrame(records, columns=columns)


def _normalize_listing_board(raw_board: str) -> str:
    lowered = raw_board.strip().lower()
    if any(k in lowered for k in ("pemantauan khusus", "watchlist", "fca")):
        return "Watchlist"
    if any(k in lowered for k in ("pengembangan", "development")):
        return "Development"
    if any(k in lowered for k in ("akselerasi", "acceleration")):
        return "Acceleration"
    return "Main"


def _parse_financial_item(
    item: dict[str, Any],
    *,
    fallback_ticker: str,
    fallback_year: int,
    fallback_quarter: int,
) -> dict[str, Any] | None:
    ticker = normalize_idx_ticker(
        str(item.get("KodeEmiten") or item.get("ticker") or fallback_ticker)
    )
    filing_raw = str(
        item.get("File_Modified") or item.get("filing_date") or f"{fallback_year}-11-01"
    ).strip()
    fy = int(item.get("Report_Year") or item.get("fiscal_year") or fallback_year)
    fq = int(item.get("fiscal_quarter") or fallback_quarter)

    # Support both normalized snake_case JSON/XBRL dicts and IDX camelCase keys
    rev = item.get("revenue_idr", item.get("SalesAndRevenue"))
    ni = item.get("net_income_idr", item.get("ProfitLoss"))
    ocf = item.get("operating_cf_idr", item.get("CashFlowsFromOperatingActivities"))
    assets = item.get("total_assets_idr", item.get("Assets"))
    liab = item.get("total_liabilities_idr", item.get("Liabilities"))
    eq = item.get("total_equity_idr", item.get("Equity"))

    if rev is None or ni is None or ocf is None or assets is None or liab is None or eq is None:
        return None

    currency = str(item.get("currency") or item.get("Currency") or "IDR").upper()
    f_hash = compute_filing_hash(
        ticker,
        str(fy),
        f"Q{fq}",
        filing_raw,
        str(assets),
        str(ni),
    )
    return {
        "ticker": ticker,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "filing_date": filing_raw,
        "revenue_idr": float(rev),
        "net_income_idr": float(ni),
        "operating_cf_idr": float(ocf),
        "total_assets_idr": float(assets),
        "total_liabilities_idr": float(liab),
        "total_equity_idr": float(eq),
        "short_term_debt_idr": _opt_float(
            item.get("short_term_debt_idr", item.get("ShortTermBankLoans"))
        ),
        "cash_and_equivalents_idr": _opt_float(
            item.get("cash_and_equivalents_idr", item.get("CashAndCashEquivalents"))
        ),
        "retained_earnings_idr": _opt_float(
            item.get("retained_earnings_idr", item.get("RetainedEarnings"))
        ),
        "ebit_idr": _opt_float(item.get("ebit_idr", item.get("OperatingProfit"))),
        "working_capital_idr": _opt_float(item.get("working_capital_idr")),
        "eps": _opt_float(item.get("eps", item.get("EarningsPerShare"))),
        "currency": currency,
        "filing_hash": f_hash,
    }


def _opt_float(val: Any) -> float | None:
    if val is None or pd.isna(val):
        return None
    return float(val)
