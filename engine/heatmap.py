from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence

import numpy as np
import pandas as pd

from engine.backtest.performance import compute_atr


@dataclass
class BandThresholds:
    extreme_high: float = 2.5
    high: float = 1.5
    low: float = -1.5
    extreme_low: float = -2.5


def _ema(values: Sequence[float], window: int) -> List[float | None]:
    if window <= 0:
        raise ValueError("window must be positive")

    if not values:
        return []

    alpha = 2 / (window + 1)
    ema: float | None = None
    result: List[float | None] = []

    for idx, value in enumerate(values):
        if idx + 1 < window:
            result.append(None)
            continue

        if ema is None:
            ema = sum(values[:window]) / window
        else:
            ema = alpha * value + (1 - alpha) * ema

        result.append(round(float(ema), 4))

    return result


def _classify_band(z: float | None, pct: float | None, thresholds: BandThresholds) -> str | None:
    if z is None and pct is None:
        return None

    if z is not None:
        if z >= thresholds.extreme_high:
            return "EXTREME_HIGH"
        if z >= thresholds.high:
            return "HIGH"
        if z <= thresholds.extreme_low:
            return "EXTREME_LOW"
        if z <= thresholds.low:
            return "LOW"
        return "NEUTRAL"

    if pct is None:
        return None

    if pct >= 98:
        return "EXTREME_HIGH"
    if pct >= 85:
        return "HIGH"
    if pct <= 2:
        return "EXTREME_LOW"
    if pct <= 15:
        return "LOW"
    return "NEUTRAL"


def compute_deviation_heatmap(
    closes: Sequence[float],
    dates: Sequence[str],
    baseline_window: int = 200,
    std_window: int = 200,
    percentile_window: int = 2520,
    baseline_type: str = "EMA",
    thresholds: BandThresholds | None = None,
) -> Mapping:
    if len(closes) != len(dates):
        raise ValueError("closes and dates must align")

    thresholds = thresholds or BandThresholds()
    baseline = _ema(closes, baseline_window)
    deviation = [
        None if base is None else round(close - base, 4)
        for close, base in zip(closes, baseline)
    ]

    dev_series = pd.Series(deviation, dtype="float64")
    std_series = dev_series.rolling(std_window, min_periods=std_window).std()

    def _percentile_rank(arr: Iterable[float]) -> float:
        series = pd.Series(arr)
        last = series.iloc[-1]
        if pd.isna(last):
            return float("nan")
        rank = (series <= last).sum() / len(series)
        return float(rank * 100)

    pct_series = dev_series.rolling(percentile_window, min_periods=percentile_window).apply(
        _percentile_rank, raw=False
    )

    z_series = dev_series / std_series

    bands: List[str | None] = []
    rows: List[Mapping] = []
    latest_idx = None

    for idx, date in enumerate(dates):
        z_val = z_series.iloc[idx] if idx < len(z_series) else np.nan
        pct_val = pct_series.iloc[idx] if idx < len(pct_series) else np.nan

        z_clean = None if pd.isna(z_val) or z_val == float("inf") else round(float(z_val), 2)
        pct_clean = None if pd.isna(pct_val) else round(float(pct_val), 1)
        band = _classify_band(z_clean, pct_clean, thresholds)
        bands.append(band)

        row: dict = {
            "date": date,
            "close": round(float(closes[idx]), 4),
        }
        if baseline[idx] is not None:
            row["baseline"] = baseline[idx]
        if deviation[idx] is not None:
            row["deviation"] = deviation[idx]
        if z_clean is not None:
            row["z"] = z_clean
        if pct_clean is not None:
            row["pct"] = pct_clean
        if band is not None:
            row["band"] = band
            latest_idx = idx
        rows.append(row)

    latest: Mapping | None = None
    if latest_idx is not None:
        latest = {
            "date": dates[latest_idx],
            "z": rows[latest_idx].get("z"),
            "pct": rows[latest_idx].get("pct"),
            "band": bands[latest_idx],
        }

    return {
        "symbol": "SLV",
        "baseline": {"type": baseline_type, "window": baseline_window},
        "std_window": std_window,
        "percentile_window": percentile_window,
        "rows": rows,
        "latest": latest,
        "bands": bands,
    }


def compute_volatility_heatmap(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    dates: Sequence[str],
    atr_window: int = 14,
    percentile_low: float = 20,
    percentile_high: float = 80,
) -> Mapping:
    atr_raw = compute_atr(highs, lows, closes, window=atr_window)
    length = len(closes)
    atr_values: List[float | None] = [None] * length
    for entry in atr_raw:
        idx = entry.get("index")
        if idx is None or idx >= length:
            continue
        atr_values[idx] = entry.get("atr")

    atr_pct = [
        None if atr is None or close == 0 else round(atr / close, 6)
        for atr, close in zip(atr_values, closes)
    ]

    clean = [v for v in atr_pct if v is not None]
    low_th = float(np.percentile(clean, percentile_low)) if clean else 0.0
    high_th = float(np.percentile(clean, percentile_high)) if clean else 0.0

    rows: List[Mapping] = []
    for idx, date in enumerate(dates):
        value = atr_pct[idx]
        if value is None:
            bucket = "UNKNOWN"
        elif value >= high_th:
            bucket = "HIGH"
        elif value <= low_th:
            bucket = "LOW"
        else:
            bucket = "NORMAL"

        row = {
            "date": date,
            "atr": atr_values[idx],
            "atr_pct": value,
            "vol_bucket": bucket,
        }
        rows.append(row)

    return {
        "symbol": "SLV",
        "atr_window": atr_window,
        "rows": rows,
        "thresholds": {"low": low_th, "high": high_th},
    }


def compute_momentum_heatmap(
    closes: Sequence[float],
    dates: Sequence[str],
    roc_window: int = 20,
    percentile_bear: float = 20,
    percentile_bull: float = 80,
) -> Mapping:
    roc_values: List[float | None] = []
    for idx, close in enumerate(closes):
        if idx < roc_window:
            roc_values.append(None)
            continue
        prev = closes[idx - roc_window]
        roc = (close / prev - 1) if prev else None
        roc_values.append(round(roc, 6) if roc is not None else None)

    clean = [v for v in roc_values if v is not None]
    bear_th = float(np.percentile(clean, percentile_bear)) if clean else -0.02
    bull_th = float(np.percentile(clean, percentile_bull)) if clean else 0.02

    rows: List[Mapping] = []
    for date, value in zip(dates, roc_values):
        if value is None:
            bucket = "UNKNOWN"
        elif value >= bull_th:
            bucket = "BULLISH"
        elif value <= bear_th:
            bucket = "BEARISH"
        else:
            bucket = "NEUTRAL"
        rows.append({"date": date, "roc_20": value, "mom_bucket": bucket})

    return {
        "symbol": "SLV",
        "momentum": {"type": "ROC", "window": roc_window},
        "rows": rows,
        "thresholds": {"bear": bear_th, "bull": bull_th},
    }


def compute_stats_by_band(
    closes: Sequence[float], bands: Sequence[str | None], horizons: Sequence[int] = (5, 10)
) -> Mapping:
    if len(closes) != len(bands):
        raise ValueError("closes and bands length mismatch")

    band_names = ["EXTREME_HIGH", "HIGH", "NEUTRAL", "LOW", "EXTREME_LOW"]
    stats: dict[str, Mapping] = {}

    for band in band_names:
        indices = [idx for idx, b in enumerate(bands) if b == band]
        band_stats: dict[str, float | int | None] = {"n": len(indices)}

        for horizon in horizons:
            returns: List[float] = []
            for idx in indices:
                if idx + horizon >= len(closes):
                    continue
                start = closes[idx]
                end = closes[idx + horizon]
                if start:
                    returns.append(end / start - 1)

            key_prefix = f"{horizon}d"
            if returns:
                arr = np.array(returns)
                band_stats[f"p_up_{key_prefix}"] = round(float((arr > 0).mean()), 3)
                band_stats[f"median_{key_prefix}"] = round(float(np.median(arr)), 4)
                band_stats[f"p10_{key_prefix}"] = round(float(np.percentile(arr, 10)), 4)
                band_stats[f"p90_{key_prefix}"] = round(float(np.percentile(arr, 90)), 4)
            else:
                band_stats[f"p_up_{key_prefix}"] = None
                band_stats[f"median_{key_prefix}"] = None
                band_stats[f"p10_{key_prefix}"] = None
                band_stats[f"p90_{key_prefix}"] = None

        stats[band] = band_stats

    return {"symbol": "SLV", "horizons": list(horizons), "bands": stats}
