from __future__ import annotations

from datetime import datetime
from typing import List, Mapping, Sequence

from engine.events.detector import Event

ACTION_BY_EVENT = {
    "SHAKEOUT": "BUY",
    "RECLAIM": "ADD",
    "RANGE_ACCUMULATION": "HOLD",
    "DISTRIBUTION_RISK": "REDUCE",
    "NEUTRAL": "WAIT",
}


def select_action(events: Sequence[Event]) -> Mapping:
    prioritized = sorted(events, key=lambda e: e.confidence, reverse=True)
    primary = prioritized[0]
    action = ACTION_BY_EVENT.get(primary.name, "WAIT")

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "market_state": primary.name,
        "confidence": round(primary.confidence, 2),
        "rationale": primary.rationale,
        "active_events": [event.to_dict() for event in prioritized],
    }
