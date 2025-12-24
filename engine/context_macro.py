from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from typing import Sequence

from engine.backtest.performance import compute_atr, compute_rsi
from engine.context import (
    BASE_CONTEXT_DIR,
    DEFAULT_MIN_OCCURRENCES,
    ContextComputationError,
    _align_series_with_ffill,
    _build_indicator,
    _to_close_series,
)
from engine.fetchers.ohlcv import fetch_ohlcv
from engine.utils.io import ensure_parent, write_json


def _volume_flow_ratio(volumes: Sequence[float], *, short: int = 5, long: int = 60) -> list[float | None]:
    ratios: list[float | None] = []
    for idx in range(len(volumes)):
        if idx + 1 < long:
            ratios.append(None)
            continue
        short_avg = mean(volumes[idx - short + 1 : idx + 1])
        long_avg = mean(volumes[idx - long + 1 : idx + 1])
        if long_avg <= 0:
            ratios.append(None)
            continue
        ratios.append(short_avg / long_avg)
    return ratios


def _liquidity_coverage(
    atr: Sequence[float | None], closes: Sequence[float], volumes: Sequence[float], *, window: int = 20
) -> list[float | None]:
    coverage: list[float | None] = []
    for idx, atr_val in enumerate(atr):
        if atr_val is None or idx + 1 < window:
            coverage.append(None)
            continue
        avg_volume = mean(volumes[idx - window + 1 : idx + 1])
        if avg_volume <= 0 or closes[idx] <= 0:
            coverage.append(None)
            continue
        # Higher values imply more volatility per unit of typical volume (harder to cover shorts).
        normalized_atr_pct = atr_val / closes[idx]
        coverage.append(normalized_atr_pct / (avg_volume / 1_000_000))
    return coverage


def _filter_series(dates: Sequence[str], values: Sequence[float | None]) -> tuple[list[str], list[float]]:
    filtered_dates: list[str] = []
    filtered_values: list[float] = []
    for date, value in zip(dates, values):
        if value is None:
            continue
        filtered_dates.append(date)
        filtered_values.append(float(value))
    return filtered_dates, filtered_values


def fetch_macro_assets(
    start_date: str,
    *,
    source: str | None = None,
    refresh: bool = False,
) -> tuple[dict[str, list[dict]], dict]:
    sources = (source,) if source else ("yahoo", "stooq")

    def fetch(symbols: list[str], cache_name: str) -> tuple[str, list[dict]]:
        last_exc: Exception | None = None
        for sym in symbols:
            try:
                rows = fetch_ohlcv(
                    symbol=sym,
                    start_date=start_date,
                    cache_path=cache_name,
                    sources=sources,
                    refresh=refresh,
                )
                return sym, rows
            except Exception as exc:  # noqa: PERF203
                last_exc = exc
                continue
        raise ContextComputationError(f"Unable to fetch {symbols}: {last_exc}")

    vix_symbol, vix_rows = fetch(
        ["^VIX", "VIX", "VIXY", "VXX"], cache_name="public/data/raw/vix_daily.json"
    )
    tip_symbol, tip_rows = fetch(["TIP", "VTIP"], cache_name="public/data/raw/tip_daily.json")

    now_source = source or "stooq/yahoo"
    meta = {
        "preferred_source": now_source,
        "symbols": {
            "VIX": vix_symbol,
            "TIP": tip_symbol,
        },
    }

    return {"VIX": vix_rows, "TIP": tip_rows}, meta


def build_macro_enrichment(
    slv_rows: list[dict],
    *,
    vix_rows: list[dict],
    tip_rows: list[dict],
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
) -> dict:
    if not slv_rows:
        raise ContextComputationError("SLV history is required for macro enrichment")

    slv_series = _to_close_series(slv_rows)
    vix_series = _to_close_series(vix_rows)
    tip_series = _to_close_series(tip_rows)

    vix_dates, slv_for_vix, vix_vals = _align_series_with_ffill(slv_series, vix_series)
    tip_dates, slv_for_tip, tip_vals = _align_series_with_ffill(slv_series, tip_series)

    closes = [row["close"] for row in slv_rows]
    highs = [row["high"] for row in slv_rows]
    lows = [row["low"] for row in slv_rows]
    volumes = [float(row.get("volume", 0) or 0) for row in slv_rows]
    dates = [row["date"] for row in slv_rows]

    vix_indicator = _build_indicator(
        "vix",
        "Volatility (VIX)",
        vix_vals,
        vix_dates,
        slv_for_vix,
        min_occurrences=min_occurrences,
    )
    inflation_indicator = _build_indicator(
        "inflation",
        "Inflation Expectations (TIP proxy)",
        tip_vals,
        tip_dates,
        slv_for_tip,
        min_occurrences=min_occurrences,
    )

    flow_dates, flow_values = _filter_series(dates, _volume_flow_ratio(volumes))
    sentiment_dates, sentiment_values = _filter_series(dates, compute_rsi(closes))
    short_interest_dates, short_interest_values = _filter_series(
        dates, _liquidity_coverage(compute_atr(highs, lows, closes), closes, volumes)
    )

    flow_indicator = _build_indicator(
        "flows",
        "ETF Flow Proxy",
        flow_values,
        flow_dates,
        closes[-len(flow_values) :] if flow_values else closes,
        min_occurrences=min_occurrences,
    )
    sentiment_indicator = _build_indicator(
        "sentiment",
        "Investor Sentiment (RSI proxy)",
        sentiment_values,
        sentiment_dates,
        closes[-len(sentiment_values) :] if sentiment_values else closes,
        min_occurrences=min_occurrences,
    )
    short_interest_indicator = _build_indicator(
        "short_interest",
        "Short Interest Coverage (liquidity proxy)",
        short_interest_values,
        short_interest_dates,
        closes[-len(short_interest_values) :] if short_interest_values else closes,
        min_occurrences=min_occurrences,
    )

    baseline = {
        "window_z": 252,
        "window_pct": 2520,
        "min_occurrences": min_occurrences,
    }

    indicators = [
        vix_indicator,
        inflation_indicator,
        flow_indicator,
        sentiment_indicator,
        short_interest_indicator,
    ]

    notes = [
        "Macro enrichment uses historical z-scores and percentiles to contextualize events.",
        "Flow, sentiment, and short-interest items are proxies derived from volume, RSI, and liquidity coverage respectively.",
    ]

    return {
        "asof": dates[-1],
        "baseline": baseline,
        "items": indicators,
        "notes": notes,
    }


def write_macro_outputs(payload: dict, meta: dict | None = None, *, source: str | None = None) -> None:
    ensure_parent(BASE_CONTEXT_DIR / "placeholder")
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    meta_payload = {
        "last_updated_utc": now,
        "source": source or meta.get("preferred_source") if meta else source,
        "symbols": meta.get("symbols") if meta else {},
        "notes": payload.get("notes", []),
    }

    write_json(BASE_CONTEXT_DIR / "macro_enrichment.json", payload)
    write_json(BASE_CONTEXT_DIR / "macro_enrichment_meta.json", meta_payload)
