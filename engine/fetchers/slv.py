"""Simple SLV data generator.

In production this would call a market data API. For the MVP we emit a
synthetic but stable time series so the rest of the pipeline can run in
GitHub Actions without external credentials.
"""
from __future__ import annotations

import random
from datetime import date, timedelta
from typing import List, Mapping


def generate_slv_series(days: int = 180) -> List[Mapping]:
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
