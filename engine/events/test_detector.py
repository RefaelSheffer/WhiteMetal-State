from __future__ import annotations

from engine.events.detector import detect_events


def _build_prices(closes: list[float]):
    return [
        {
            "date": f"2025-01-{idx+1:02d}",
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000_000,
        }
        for idx, close in enumerate(closes)
    ]


def _event_names(prices: list[dict]) -> set[str]:
    return {event.name for event in detect_events(prices)}


def test_reclaim_requires_volatility_buffer():
    base_window = [100 + (i % 5) for i in range(20)]

    muted_breakout = _build_prices(base_window + [104.5])
    assert "RECLAIM" not in _event_names(muted_breakout)

    strong_breakout = _build_prices(base_window + [105.5])
    assert "RECLAIM" in _event_names(strong_breakout)


def test_distribution_and_shakeout_need_sigma_break():
    base = [100, 101, 102, 103, 104] * 3

    shallow_pullback = _build_prices(base + [99.6])
    names = _event_names(shallow_pullback)
    assert "DISTRIBUTION_RISK" not in names
    assert "SHAKEOUT" not in names

    deep_break = _build_prices(base + [98])
    names = _event_names(deep_break)
    assert "DISTRIBUTION_RISK" in names
    assert "SHAKEOUT" in names
