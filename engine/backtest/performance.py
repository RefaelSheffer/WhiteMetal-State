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
