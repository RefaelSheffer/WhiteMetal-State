from __future__ import annotations

from dataclasses import dataclass
from math import atan
from statistics import mean, pstdev
from typing import Iterable, List, Mapping, Sequence

from engine.events.cycles import CycleSegment

import pandas as pd
from statsmodels.tsa.seasonal import STL


@dataclass
class PerformanceSummary:
    hit_rate: float
    avg_return_5d: float
    avg_return_10d: float
    max_drawdown: float

    def to_dict(self) -> Mapping:
        return {
            "hit_rate": round(self.hit_rate, 2),
            "avg_return_5d": round(self.avg_return_5d, 3),
            "avg_return_10d": round(self.avg_return_10d, 3),
            "max_drawdown": round(self.max_drawdown, 3),
        }


@dataclass
class AlgorithmScore:
    composite: float
    hit_rate: float
    sharpe_ratio: float
    cycle_capture_rate: float

    def to_dict(self) -> Mapping:
        return {
            "composite": round(self.composite, 2),
            "hit_rate": round(self.hit_rate, 3),
            "sharpe_ratio": round(self.sharpe_ratio, 3),
            "cycle_capture_rate": round(self.cycle_capture_rate, 3),
        }


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


def compute_equity_curve(closes: Sequence[float]) -> List[Mapping]:
    equity = 1.0
    curve: List[Mapping] = []

    if not closes:
        return curve

    curve.append({"index": 0, "equity": round(equity, 4)})

    for idx in range(1, len(closes)):
        ret = (closes[idx] - closes[idx - 1]) / closes[idx - 1]
        equity *= 1 + ret
        curve.append({"index": idx, "equity": round(equity, 4)})

    return curve


def compute_buy_and_hold_equity(closes: Sequence[float]) -> List[Mapping]:
    if not closes:
        return []

    base = closes[0]
    if base == 0:
        return []

    return [
        {"index": idx, "equity": round(close / base, 4)} for idx, close in enumerate(closes)
    ]


def summarize_returns(closes: Sequence[float]) -> PerformanceSummary:
    if len(closes) < 12:
        return PerformanceSummary(hit_rate=0.5, avg_return_5d=0.0, avg_return_10d=0.0, max_drawdown=0.0)

    changes = [
        (closes[i + 1] - closes[i]) / closes[i]
        for i in range(len(closes) - 1)
    ]

    positives = [c for c in changes if c > 0]
    hit_rate = len(positives) / len(changes)

    horizon5 = [
        (closes[i + 5] - closes[i]) / closes[i]
        for i in range(len(closes) - 5)
    ]
    horizon10 = [
        (closes[i + 10] - closes[i]) / closes[i]
        for i in range(len(closes) - 10)
    ]

    max_drawdown = min(horizon10) if horizon10 else min(changes)

    return PerformanceSummary(
        hit_rate=hit_rate,
        avg_return_5d=sum(horizon5) / len(horizon5),
        avg_return_10d=sum(horizon10) / len(horizon10),
        max_drawdown=max_drawdown,
    )


def _normalized_sharpe(mean_ret: float, stddev: float) -> float:
    """Map a raw Sharpe-style ratio into [0, 1] for scoring.

    The arctangent keeps extreme values bounded without clipping positive surprise.
    """

    if stddev == 0:
        return 0.0

    raw_ratio = mean_ret / stddev
    # atan returns (-pi/2, pi/2); rescale to [0, 1]
    return _clamp(0.5 + (atan(raw_ratio) / 3.14159))


def _cycle_capture_rate(closes: Sequence[float], cycles: Sequence[CycleSegment]) -> float:
    """Estimate how much of each detected cycle move was captured.

    We assume a realistic entry a third of the way into the cycle and an exit
    just before the turning point. The ratio compares that captured move to the
    full amplitude so that late/early exits are penalized.
    """

    if not closes or not cycles:
        return 0.0

    ratios: List[float] = []
    last_idx = len(closes) - 1

    for cycle in cycles:
        if cycle.start_idx >= len(closes) or cycle.end_idx > last_idx:
            continue

        amplitude = cycle.amplitude
        if amplitude == 0:
            continue

        entry_idx = min(cycle.end_idx - 1, cycle.start_idx + max(1, cycle.length // 3))
        exit_idx = cycle.end_idx

        if entry_idx >= exit_idx:
            entry_idx = cycle.start_idx
        if entry_idx >= exit_idx or exit_idx >= len(closes):
            continue

        entry = closes[entry_idx]
        exit_price = closes[exit_idx]

        captured = (exit_price - entry) / entry
        ratios.append(_clamp(abs(captured) / abs(amplitude)))

    if not ratios:
        return 0.0

    return sum(ratios) / len(ratios)


DEFAULT_SCORE_WEIGHTS = {
    "hit_rate": 0.4,
    "sharpe_ratio": 0.35,
    "cycle_capture_rate": 0.25,
}


def _normalize_score_weights(weights: Mapping[str, float] | None) -> Mapping[str, float]:
    base = {**DEFAULT_SCORE_WEIGHTS}
    if weights:
        for key in DEFAULT_SCORE_WEIGHTS:
            if weights.get(key) is not None:
                base[key] = weights[key]

    total = sum(value for value in base.values() if value > 0)
    if total <= 0:
        return DEFAULT_SCORE_WEIGHTS

    return {key: value / total for key, value in base.items()}


def compute_algorithm_score(
    closes: Sequence[float],
    cycles: Sequence[CycleSegment],
    weights: Mapping[str, float] | None = None,
) -> AlgorithmScore:
    """Build a weighted scorecard combining hit rate, Sharpe, and cycle capture.

    Custom `weights` can be supplied to stress-test the sensitivity of the
    composite score during backtests. Missing keys fall back to the defaults
    (40% hit rate, 35% Sharpe, 25% cycle capture), and weights are normalized
    to sum to 1 so relative importance is preserved even if raw inputs do not
    add up to 100%.
    """

    if len(closes) < 3:
        return AlgorithmScore(composite=0.0, hit_rate=0.0, sharpe_ratio=0.0, cycle_capture_rate=0.0)

    changes = [
        (closes[i + 1] - closes[i]) / closes[i]
        for i in range(len(closes) - 1)
    ]

    mean_ret = mean(changes) if changes else 0.0
    stddev = pstdev(changes) if len(changes) > 1 else 0.0
    hit_rate = len([c for c in changes if c > 0]) / len(changes) if changes else 0.0
    sharpe_ratio = mean_ret / stddev if stddev != 0 else 0.0
    cycle_capture = _cycle_capture_rate(closes, cycles)

    normalized_sharpe = _normalized_sharpe(mean_ret, stddev)
    weights = _normalize_score_weights(weights)
    composite = (
        (hit_rate * weights["hit_rate"])
        + (normalized_sharpe * weights["sharpe_ratio"])
        + (cycle_capture * weights["cycle_capture_rate"])
    )

    return AlgorithmScore(
        composite=round(composite * 100, 2),
        hit_rate=hit_rate,
        sharpe_ratio=sharpe_ratio,
        cycle_capture_rate=cycle_capture,
    )


def _max_drawdown(equity_values: Sequence[float]) -> float:
    if not equity_values:
        return 0.0

    peak = equity_values[0]
    max_dd = 0.0

    for value in equity_values:
        if value > peak:
            peak = value
        drawdown = (value - peak) / peak if peak != 0 else 0.0
        max_dd = min(max_dd, drawdown)

    return max_dd


def compute_performance_stats(curve: Sequence[Mapping[str, float]]) -> Mapping[str, float]:
    if len(curve) < 2:
        return {
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
            "sortino_ratio": 0.0,
        }

    equity_values = [point["equity"] for point in curve]
    total_return = (equity_values[-1] / equity_values[0]) - 1 if equity_values[0] else 0.0

    returns = [
        (equity_values[i] - equity_values[i - 1]) / equity_values[i - 1]
        for i in range(1, len(equity_values))
        if equity_values[i - 1] != 0
    ]

    if not returns:
        return {
            "total_return": round(total_return, 4),
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
            "sortino_ratio": 0.0,
        }

    mean_ret = mean(returns)
    stddev = pstdev(returns) if len(returns) > 1 else 0.0
    sharpe_ratio = mean_ret / stddev if stddev != 0 else 0.0

    downside = [r for r in returns if r < 0]
    downside_dev = pstdev(downside) if len(downside) > 1 else (abs(downside[0]) if downside else 0.0)
    sortino_ratio = mean_ret / downside_dev if downside_dev != 0 else 0.0

    return {
        "total_return": round(total_return, 4),
        "max_drawdown": round(_max_drawdown(equity_values), 4),
        "sharpe_ratio": round(sharpe_ratio, 4),
        "sortino_ratio": round(sortino_ratio, 4),
    }


def event_breakdown(events: Iterable[str], closes: Sequence[float]) -> List[Mapping]:
    breakdown: List[Mapping] = []
    if not events:
        return breakdown

    last_event = events[-1]
    ret5 = (closes[-1] - closes[-5]) / closes[-5] if len(closes) >= 6 else 0.0
    ret10 = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 11 else 0.0

    breakdown.append(
        {
            "event": last_event,
            "avg_return_5d": round(ret5, 3),
            "avg_return_10d": round(ret10, 3),
            "count": 1,
        }
    )

    return breakdown


def compute_rsi(closes: Sequence[float], period: int = 14) -> List[Mapping]:
    """Compute a rolling Relative Strength Index (RSI).

    Returns a list of {"index": idx, "rsi": value} starting once enough
    closes are available for the initial window.
    """

    if len(closes) <= period:
        return []

    gains: List[float] = []
    losses: List[float] = []
    for idx in range(1, period + 1):
        delta = closes[idx] - closes[idx - 1]
        if delta >= 0:
            gains.append(delta)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-delta)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    rsi_series: List[Mapping] = []
    rs = avg_gain / avg_loss if avg_loss != 0 else float("inf")
    rsi = 100 - (100 / (1 + rs)) if rs != float("inf") else 100.0
    rsi_series.append({"index": period, "rsi": round(rsi, 2)})

    for idx in range(period + 1, len(closes)):
        delta = closes[idx] - closes[idx - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        rsi_series.append({"index": idx, "rsi": round(rsi, 2)})

    return rsi_series


def compute_rolling_stddev(closes: Sequence[float], window: int = 20) -> List[Mapping]:
    """Compute a simple rolling standard deviation over closes."""

    if len(closes) < window:
        return []

    stddev_series: List[Mapping] = []
    for idx in range(window - 1, len(closes)):
        window_values = closes[idx - window + 1 : idx + 1]
        mean = sum(window_values) / window
        variance = sum((v - mean) ** 2 for v in window_values) / window
        stddev_series.append({"index": idx, "stddev": round(variance ** 0.5, 4)})

    return stddev_series


def compute_macd(
    closes: Sequence[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
) -> List[Mapping]:
    """Compute MACD line, signal line, and histogram.

    Uses standard EMA smoothing. Returns entries only once enough values exist for
    the slow EMA and signal line.
    """

    if len(closes) < slow_period + signal_period - 1:
        return []

    def ema(values: Sequence[float], period: int) -> List[float]:
        if len(values) < period:
            return []

        k = 2 / (period + 1)
        ema_values: List[float] = []
        current = sum(values[:period]) / period
        ema_values.append(current)
        for value in values[period:]:
            current = (value - current) * k + current
            ema_values.append(current)
        return ema_values

    fast_ema = ema(closes, fast_period)
    slow_ema = ema(closes, slow_period)

    # Align fast EMA with slow EMA positions
    offset = slow_period - fast_period
    macd_values = [
        fast_ema[i + offset] - slow
        for i, slow in enumerate(slow_ema)
        if i + offset < len(fast_ema)
    ]

    signal_ema = ema(macd_values, signal_period)

    start_index = slow_period + signal_period - 2
    macd_series: List[Mapping] = []

    for idx, signal_value in enumerate(signal_ema):
        macd_idx = idx + signal_period - 1
        if macd_idx >= len(macd_values):
            break
        price_index = start_index + idx
        macd_value = macd_values[macd_idx]
        hist_value = macd_value - signal_value
        macd_series.append(
            {
                "index": price_index,
                "macd": round(macd_value, 4),
                "signal": round(signal_value, 4),
                "hist": round(hist_value, 4),
            }
        )

    return macd_series


def compute_bollinger_bands(
    closes: Sequence[float], window: int = 20, num_stddev: float = 2.0
) -> List[Mapping]:
    """Compute Bollinger Bands (upper, middle, lower)."""

    if len(closes) < window:
        return []

    bands: List[Mapping] = []
    for idx in range(window - 1, len(closes)):
        window_values = closes[idx - window + 1 : idx + 1]
        mean = sum(window_values) / window
        variance = sum((v - mean) ** 2 for v in window_values) / window
        stddev = variance ** 0.5
        bands.append(
            {
                "index": idx,
                "middle": round(mean, 4),
                "upper": round(mean + num_stddev * stddev, 4),
                "lower": round(mean - num_stddev * stddev, 4),
            }
        )

    return bands


def compute_obv(closes: Sequence[float], volumes: Sequence[float]) -> List[Mapping]:
    """Compute On-Balance Volume (OBV)."""

    if not closes or not volumes or len(closes) != len(volumes):
        return []

    obv_series: List[Mapping] = []
    obv_value = volumes[0]
    obv_series.append({"index": 0, "obv": obv_value})

    for idx in range(1, len(closes)):
        if closes[idx] > closes[idx - 1]:
            obv_value += volumes[idx]
        elif closes[idx] < closes[idx - 1]:
            obv_value -= volumes[idx]
        # unchanged price keeps OBV flat
        obv_series.append({"index": idx, "obv": obv_value})

    return obv_series


def compute_moving_average(closes: Sequence[float], window: int) -> List[Mapping]:
    """Compute a simple moving average."""

    if window <= 0:
        raise ValueError("window must be positive")

    if len(closes) < window:
        return []

    ma_series: List[Mapping] = []
    for idx in range(window - 1, len(closes)):
        window_values = closes[idx - window + 1 : idx + 1]
        ma = sum(window_values) / window
        ma_series.append({"index": idx, "ma": round(ma, 4)})

    return ma_series


def attach_dates(series: List[Mapping], dates: Sequence[str]) -> List[Mapping]:
    """Attach ISO dates to indicator series that track price indices."""

    dated: List[Mapping] = []
    for entry in series:
        idx = entry.get("index")
        if idx is None or idx >= len(dates):
            continue
        dated.append({**entry, "date": dates[idx]})
    return dated


def decompose_closes(
    closes: Sequence[float], period: int = 14, robust: bool = True
) -> Mapping[str, List[Mapping]]:
    """Decompose close prices into trend/seasonal/residual components.

    Returns lists of mappings with the index to align with the input closes.
    """

    if len(closes) < period:
        return {"trend": [], "seasonal": [], "resid": []}

    series = pd.Series(closes)
    decomposition = STL(series, period=period, robust=robust).fit()

    def to_list(values, key: str) -> List[Mapping]:
        return [
            {"index": idx, key: round(float(value), 4)}
            for idx, value in enumerate(values)
        ]

    return {
        "trend": to_list(decomposition.trend, "trend"),
        "seasonal": to_list(decomposition.seasonal, "seasonal"),
        "resid": to_list(decomposition.resid, "resid"),
    }
