from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence


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


def compute_equity_curve(closes: Sequence[float]) -> List[Mapping]:
    equity = 1.0
    curve: List[Mapping] = []

    for idx in range(1, len(closes)):
        ret = (closes[idx] - closes[idx - 1]) / closes[idx - 1]
        equity *= 1 + ret
        curve.append({"index": idx, "equity": round(equity, 4)})

    return curve


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
