from __future__ import annotations

from datetime import datetime
from typing import Iterable


def _require_key(row: dict, key: str, idx: int) -> float:
    if key not in row:
        raise ValueError(f"Row {idx} missing required key '{key}'")
    value = row[key]
    if value is None:
        raise ValueError(f"Row {idx} has null value for '{key}'")
    return value


def validate_ohlcv(rows: Iterable[dict]) -> None:
    """Validate basic OHLCV sanity constraints.

    The checks ensure:
    - Dates are strictly increasing with no duplicates.
    - close > 0
    - high >= max(open, close)
    - low <= min(open, close)
    - volume >= 0
    """

    prev_date = None
    seen_dates: set[datetime] = set()

    for idx, row in enumerate(rows):
        date_raw = _require_key(row, "date", idx)
        try:
            current_date = datetime.strptime(str(date_raw), "%Y-%m-%d")
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Row {idx} has invalid date format: {date_raw}") from exc

        if prev_date and current_date <= prev_date:
            raise ValueError(f"Dates must be strictly increasing; found {current_date.date()} after {prev_date.date()}")
        if current_date in seen_dates:
            raise ValueError(f"Duplicate date detected: {current_date.date()}")

        open_px = float(_require_key(row, "open", idx))
        high_px = float(_require_key(row, "high", idx))
        low_px = float(_require_key(row, "low", idx))
        close_px = float(_require_key(row, "close", idx))
        volume = _require_key(row, "volume", idx)

        if close_px <= 0:
            raise ValueError(f"Close must be positive at {current_date.date()}")
        if high_px < max(open_px, close_px):
            raise ValueError(f"High below open/close at {current_date.date()}")
        if low_px > min(open_px, close_px):
            raise ValueError(f"Low above open/close at {current_date.date()}")
        if volume is None or float(volume) < 0:
            raise ValueError(f"Volume negative at {current_date.date()}")

        prev_date = current_date
        seen_dates.add(current_date)

    if prev_date is None:
        raise ValueError("No OHLCV rows to validate")
