from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, pstdev
from typing import List, Mapping, Sequence


@dataclass
class Event:
    name: str
    confidence: float
    rationale: str

    def to_dict(self) -> Mapping:
        return {
            "name": self.name,
            "confidence": round(self.confidence, 2),
            "rationale": self.rationale,
        }


def detect_events(prices: Sequence[Mapping]) -> List[Event]:
    closes = [row["close"] for row in prices]
    latest_close = closes[-1]
    window5 = closes[-5:]
    window20 = closes[-20:]

    trailing5 = closes[:-1][-5:]
    trailing20 = closes[:-1][-20:]

    baseline_low5 = min(trailing5) if trailing5 else min(window5)
    baseline_high20 = max(trailing20) if trailing20 else max(window20)

    volatility5 = pstdev(trailing5) if len(trailing5) > 1 else 0.0
    volatility20 = pstdev(trailing20) if len(trailing20) > 1 else 0.0

    events: List[Event] = []

    if latest_close <= baseline_low5 - 0.5 * volatility5:
        events.append(
            Event(
                name="SHAKEOUT",
                confidence=0.62,
                rationale="Close undercut the 5-day low by more than a 0.5σ volatility filter, a common shakeout signature.",
            )
        )

    if latest_close < baseline_low5 - volatility5:
        events.append(
            Event(
                name="DISTRIBUTION_RISK",
                confidence=0.55,
                rationale="Close slipped more than 1σ below the 5-day low, reinforcing distribution risk.",
            )
        )

    if latest_close > baseline_high20 + 0.5 * volatility20:
        events.append(
            Event(
                name="RECLAIM",
                confidence=0.68,
                rationale="Close cleared the 20-day high by over 0.5σ of rolling volatility, confirming reclaim momentum.",
            )
        )

    if latest_close > mean(window20) and latest_close > min(window5):
        events.append(
            Event(
                name="RANGE_ACCUMULATION",
                confidence=0.5,
                rationale="Staying above the 20-day mean while basing near highs.",
            )
        )

    if not events:
        events.append(
            Event(
                name="NEUTRAL",
                confidence=0.4,
                rationale="No clear pattern detected in the latest window.",
            )
        )

    return events
