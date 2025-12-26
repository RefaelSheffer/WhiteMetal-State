from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

from engine.utils.http import get_with_retry

DEFAULT_CACHE_DIR = Path("public/data/raw")


class OhlcvFetchError(RuntimeError):
    """Raised when price history cannot be fetched from any source."""


_DEF_NORMALIZE_MAP = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    columns = {c.lower(): c for c in df.columns}
    rename_map = {}
    for target, canonical in _DEF_NORMALIZE_MAP.items():
        if target in columns:
            rename_map[columns[target]] = canonical
        elif target.capitalize() in columns:
            rename_map[columns[target.capitalize()]] = canonical
    if not rename_map:
        raise ValueError("DataFrame does not contain OHLCV columns")

    df = df.rename(columns=rename_map)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype("int64")
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def _stooq_url(symbol: str) -> str:
    return f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"


def _fetch_stooq(symbol: str) -> pd.DataFrame:
    url = _stooq_url(symbol)
    headers = {"User-Agent": "WhiteMetalBot/1.0 (data-fetcher)"}
    resp = get_with_retry(url, headers=headers, timeout=60, max_attempts=4)
    return pd.read_csv(io.StringIO(resp.text))


def _fetch_yahoo(symbol: str, start_date: str, end_date: str | None) -> pd.DataFrame:
    start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        if end_date
        else datetime.now(timezone.utc)
    )
    period1 = int(start_dt.timestamp())
    period2 = int(end_dt.timestamp())
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
        f"?period1={period1}&period2={period2}&interval=1d&events=history&includeAdjustedClose=true"
    )
    headers = {"User-Agent": "WhiteMetalBot/1.0 (data-fetcher)"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _load_cache(cache_file: Path) -> list[dict] | None:
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text())
        except Exception:
            return None
    return None


def fetch_ohlcv(
    *,
    symbol: str,
    start_date: str = "2008-01-01",
    end_date: str | None = None,
    cache_path: str | Path | None = None,
    sources: Iterable[str] = ("stooq", "yahoo"),
    refresh: bool = False,
) -> list[dict]:
    """Fetch daily OHLCV data for ``symbol`` and write them to JSON.

    The first available source in ``sources`` is used. Results are cached to
    ``cache_path`` (default: ``public/data/raw/{symbol.lower()}_daily.json``).
    """

    cache_file = Path(cache_path) if cache_path else DEFAULT_CACHE_DIR / f"{symbol.lower()}_daily.json"

    if not refresh:
        cached = _load_cache(cache_file)
        if cached:
            return cached

    last_error: Exception | None = None
    for source in sources:
        try:
            if source == "stooq":
                df = _fetch_stooq(symbol)
            elif source == "yahoo":
                df = _fetch_yahoo(symbol, start_date, end_date)
            else:
                raise ValueError(f"Unsupported source '{source}'")
            df = _normalize(df)
            df = df[df["date"] >= start_date]
            if end_date:
                df = df[df["date"] <= end_date]
            records = df.to_dict(orient="records")
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(records, indent=2), encoding="utf-8")
            return records
        except Exception as exc:  # noqa: PERF203
            last_error = exc
            continue

    if cache_file.exists():
        cached = _load_cache(cache_file)
        if cached:
            return cached

    raise OhlcvFetchError(f"Unable to fetch {symbol} OHLCV; last error: {last_error}")


def _cache_mtime_iso(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat()


def fetch_ohlcv_with_status(
    *,
    symbol: str,
    start_date: str = "2008-01-01",
    end_date: str | None = None,
    cache_path: str | Path | None = None,
    sources: Iterable[str] = ("stooq", "yahoo"),
    refresh: bool = False,
) -> tuple[list[dict], dict]:
    cache_file = Path(cache_path) if cache_path else DEFAULT_CACHE_DIR / f"{symbol.lower()}_daily.json"

    if not refresh:
        cached = _load_cache(cache_file)
        if cached:
            return cached, {
                "fetched_at_utc": _cache_mtime_iso(cache_file),
                "source_status": "cached",
                "error_reason": None,
                "source": None,
            }

    last_error: Exception | None = None
    for source in sources:
        try:
            if source == "stooq":
                df = _fetch_stooq(symbol)
            elif source == "yahoo":
                df = _fetch_yahoo(symbol, start_date, end_date)
            else:
                raise ValueError(f"Unsupported source '{source}'")
            df = _normalize(df)
            df = df[df["date"] >= start_date]
            if end_date:
                df = df[df["date"] <= end_date]
            records = df.to_dict(orient="records")
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(records, indent=2), encoding="utf-8")
            return records, {
                "fetched_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "source_status": "live",
                "error_reason": None,
                "source": source,
            }
        except Exception as exc:  # noqa: PERF203
            last_error = exc
            continue

    if cache_file.exists():
        cached = _load_cache(cache_file)
        if cached:
            return cached, {
                "fetched_at_utc": _cache_mtime_iso(cache_file),
                "source_status": "cached",
                "error_reason": str(last_error) if last_error else None,
                "source": None,
            }

    raise OhlcvFetchError(f"Unable to fetch {symbol} OHLCV; last error: {last_error}")
