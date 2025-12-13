from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
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

    events: List[Event] = []

    if latest_close == min(window5):
        events.append(
            Event(
                name="SHAKEOUT",
                confidence=0.62,
                rationale="Price tagged a 5-day low, often a shakeout signature.",
            )
        )

    if latest_close < max(window5) * 0.985:
        events.append(
            Event(
                name="DISTRIBUTION_RISK",
                confidence=0.55,
                rationale="Pullback from recent local high suggests distribution risk.",
            )
        )

    if latest_close > max(window20):
        events.append(
            Event(
                name="RECLAIM",
                confidence=0.68,
                rationale="Closing above 20-day high indicates reclaim momentum.",
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
