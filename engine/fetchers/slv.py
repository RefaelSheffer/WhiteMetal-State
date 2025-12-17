"""SLV data generator that prefers real market data.

The legacy implementation emitted synthetic random data so the pipeline could
run without API keys. Now we default to fetching real daily OHLCV prices from
the :mod:`engine.fetchers.slv_real` module, falling back to deterministic
synthetic data only if the network and cache are both unavailable.
"""
from __future__ import annotations

import random
from datetime import date, timedelta
from typing import List, Mapping

from engine.fetchers.slv_real import fetch_slv_ohlcv


def generate_slv_series(
    days: int = 180,
    cache_path: str = "public/data/raw/slv_daily.json",
    *,
    allow_synthetic_fallback: bool = True,
) -> List[Mapping]:
    """Return the most recent ``days`` of SLV OHLCV data.

    We first attempt to load real prices (using the cached JSON if present).
    If that fails and ``allow_synthetic_fallback`` is true, we emit a
    deterministic synthetic series so downstream components can continue to
    operate.
    """

    try:
        records = fetch_slv_ohlcv(cache_path=cache_path)
    except Exception:
        if not allow_synthetic_fallback:
            raise
        records = _generate_synthetic_series(days)
    else:
        records = records[-days:]

    return records


def _generate_synthetic_series(days: int) -> List[Mapping]:
    rng = random.Random(42)
    today = date.today()
    base_price = 22.0
    records = []

    for offset in range(days, 0, -1):
        session_date = today - timedelta(days=offset)
        drift = rng.uniform(-0.35, 0.45)
        base_price = max(15.0, base_price + drift)
        close = round(base_price, 2)
        open_price = round(close - rng.uniform(-0.25, 0.25), 2)
        high = round(max(open_price, close) + rng.uniform(0, 0.4), 2)
        low = round(min(open_price, close) - rng.uniform(0, 0.4), 2)
        volume = rng.randint(1_000_000, 2_500_000)

        records.append(
            {
                "date": session_date.isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )

    return records
