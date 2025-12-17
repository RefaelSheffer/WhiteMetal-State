from __future__ import annotations

from engine.fetchers.ohlcv import fetch_ohlcv


def fetch_slv_ohlcv(
    start_date: str = "2008-01-01",
    end_date: str | None = None,
    cache_path: str = "public/data/raw/slv_daily.json",
    source: str | None = None,
    refresh: bool = False,
) -> list[dict]:
    """Fetch SLV OHLCV data, persisting the results to JSON."""

    sources = (source,) if source else ("stooq", "yahoo")
    return fetch_ohlcv(
        symbol="SLV",
        start_date=start_date,
        end_date=end_date,
        cache_path=cache_path,
        sources=sources,
        refresh=refresh,
    )
