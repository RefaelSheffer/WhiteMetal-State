from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import List, Mapping, Sequence

from engine.events.detector import Event
from engine.events.cycles import CycleSegment, filter_cycles

ACTIONS = {"BUY", "ADD", "HOLD", "REDUCE", "WAIT"}

ACTION_BY_EVENT = {
    "SHAKEOUT": "BUY",
    "RECLAIM": "ADD",
    "RANGE_ACCUMULATION": "HOLD",
    "DISTRIBUTION_RISK": "REDUCE",
    "NEUTRAL": "WAIT",
}


def select_action(
    events: Sequence[Event],
    cycles: Sequence[CycleSegment],
    indicator_context: Mapping | None = None,
) -> Mapping:
    prioritized = sorted(events, key=lambda e: e.confidence, reverse=True)
    primary = prioritized[0]
    base_action = ACTION_BY_EVENT.get(primary.name, "WAIT")

    filtered_cycles = filter_cycles(cycles, min_length=2)
    cycle_context = _analyze_cycle_context(filtered_cycles, raw_cycle_count=len(cycles))
    action, rationale = _apply_indicator_filters(
        base_action, primary, indicator_context
    )
    action, rationale = _apply_cycle_bias(action, rationale, cycle_context)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "action": action,
        "market_state": primary.name,
        "confidence": round(primary.confidence, 2),
        "rationale": rationale,
        "cycle_context": cycle_context,
        "indicator_context": indicator_context or {},
        "active_events": [event.to_dict() for event in prioritized],
    }


def _analyze_cycle_context(
    cycles: Sequence[CycleSegment], raw_cycle_count: int | None = None
) -> Mapping:
    """Summarize the latest cycle to bias event-based actions."""

    context: Mapping[str, object]

    if not cycles:
        return {
            "bias": "neutral",
            "note": "No completed cycles detected after filtering; keep event-driven stance.",
            "filtered_cycle_count": 0,
            "raw_cycle_count": raw_cycle_count if raw_cycle_count is not None else 0,
            "min_cycle_length": 2,
        }

    latest = cycles[-1]
    up_cycles = [cycle for cycle in cycles if cycle.direction == "upswing"]
    down_cycles = [cycle for cycle in cycles if cycle.direction == "downswing"]

    def _avg(values: Sequence[float], fallback: float = 0.0) -> float:
        return round(mean(values), 4) if values else fallback

    avg_up_length = _avg([cycle.length for cycle in up_cycles], fallback=latest.length)
    avg_up_amplitude = _avg([cycle.amplitude for cycle in up_cycles], fallback=latest.amplitude)
    avg_down_length = _avg([cycle.length for cycle in down_cycles], fallback=latest.length)
    avg_down_amplitude = _avg([abs(cycle.amplitude) for cycle in down_cycles], fallback=abs(latest.amplitude))
    avg_magnitude = _avg([abs(cycle.amplitude) for cycle in cycles], fallback=abs(latest.amplitude))
    reference_magnitude = max(min(avg_magnitude, 0.01), 0.0047)

    low_amplitude_cutoff = round(max(0.002, reference_magnitude * 0.6), 4)
    high_amplitude_cutoff = round(max(reference_magnitude * 1.4, 0.007), 4)
    amplitude_flag = "normal"
    if abs(latest.amplitude) < low_amplitude_cutoff:
        amplitude_flag = "low"
    elif abs(latest.amplitude) > high_amplitude_cutoff:
        amplitude_flag = "high"

    context = {
        "bias": "neutral",
        "latest_direction": latest.direction,
        "latest_length": latest.length,
        "latest_amplitude": round(latest.amplitude, 4),
        "avg_up_length": avg_up_length,
        "avg_up_amplitude": avg_up_amplitude,
        "avg_down_length": avg_down_length,
        "avg_down_amplitude": avg_down_amplitude,
        "avg_magnitude": avg_magnitude,
        "reference_magnitude": reference_magnitude,
        "low_amplitude_cutoff": low_amplitude_cutoff,
        "high_amplitude_cutoff": high_amplitude_cutoff,
        "amplitude_flag": amplitude_flag,
        "filtered_cycle_count": len(cycles),
        "raw_cycle_count": raw_cycle_count if raw_cycle_count is not None else len(cycles),
        "min_cycle_length": 2,
    }

    matured_upswing = (
        latest.direction == "upswing"
        and latest.length >= avg_up_length * 1.2
        and latest.amplitude >= avg_up_amplitude * 1.1
    )

    shallow_downswing = (
        latest.direction == "downswing"
        and latest.length <= max(2, avg_down_length * 0.8)
        and abs(latest.amplitude) <= max(avg_down_amplitude * 0.75, 0.01)
    )

    notes: List[str] = []

    if matured_upswing:
        context["bias"] = "mature_upswing"
        notes.append(
            "Late-stage upswing with extended length/amplitude; favor taking risk off."
        )
    elif shallow_downswing:
        context["bias"] = "shallow_downswing"
        notes.append(
            "Downswing is young and shallow; avoid reactionary reductions."
        )
    else:
        notes.append("Cycle posture is neutral; follow event guidance.")

    if amplitude_flag == "low":
        notes.append(
            "Latest cycle amplitude is muted; stand down until volatility expands beyond the low cutoff."
        )
    elif amplitude_flag == "high":
        notes.append(
            "Latest cycle amplitude is elevated versus average; conviction trades can be sized up."
        )

    context["note"] = " ".join(notes)

    return context


def _apply_indicator_filters(
    action: str, primary: Event, indicator_context: Mapping | None
) -> tuple[str, str]:
    """Gate event-driven actions behind simple RSI/MACD confirmations."""

    if action not in ACTIONS:
        return "WAIT", f"Unrecognized base action; defaulting to WAIT. {primary.rationale}"

    if indicator_context is None:
        return action, f"{primary.rationale} Technical filters unavailable; keeping event action."

    notes: List[str] = [primary.rationale]
    rsi = indicator_context.get("latest_rsi")
    macd = indicator_context.get("latest_macd")
    macd_hist = indicator_context.get("latest_macd_hist")
    macd_improving = indicator_context.get("macd_improving")

    if primary.name == "SHAKEOUT" and action == "BUY":
        oversold = rsi is not None and rsi < 30
        if oversold or macd_improving:
            qualifier = "RSI oversold" if oversold else "MACD momentum improving"
            notes.append(f"BUY confirmed by {qualifier} filter.")
        else:
            action = "WAIT"
            notes.append(
                "BUY gated until RSI < 30 or MACD momentum improves to reduce false triggers."
            )

    if primary.name == "DISTRIBUTION_RISK" and action == "REDUCE":
        overbought = rsi is not None and rsi > 70
        macd_negative = (macd is not None and macd < 0) or (
            macd_hist is not None and macd_hist < 0
        )
        if overbought or macd_negative:
            qualifier = "RSI overbought" if overbought else "MACD negative bias"
            notes.append(f"SELL/REDUCE confirmed by {qualifier} filter.")
        else:
            action = "HOLD"
            notes.append(
                "SELL/REDUCE deferred until RSI > 70 or MACD turns negative to avoid whipsaws."
            )

    return action, " ".join(note for note in notes if note)


def _apply_cycle_bias(action: str, rationale: str, cycle_context: Mapping) -> tuple[str, str]:
    """Blend event actions with cycle-aware adjustments."""

    if action not in ACTIONS:
        return "WAIT", f"Unrecognized base action; defaulting to WAIT. {rationale}"

    bias = cycle_context.get("bias")
    notes: List[str] = [rationale]

    if bias == "mature_upswing" and action in {"BUY", "ADD", "HOLD", "WAIT"}:
        action = "REDUCE"
        notes.append(
            "Cycle maturity suggests tightening risk, elevating reduce/sell threshold."
        )
    elif bias == "shallow_downswing" and action == "REDUCE":
        action = "HOLD"
        notes.append(
            "Early shallow downswing detected; pausing reductions to confirm trend."
        )
    else:
        notes.append(cycle_context.get("note", ""))

    amplitude_flag = cycle_context.get("amplitude_flag")
    latest_amplitude = cycle_context.get("latest_amplitude")
    latest_direction = cycle_context.get("latest_direction")
    low_cutoff = cycle_context.get("low_amplitude_cutoff")
    high_cutoff = cycle_context.get("high_amplitude_cutoff")

    if amplitude_flag == "low":
        action = "WAIT"
        notes.append(
            f"Cycle amplitude ({latest_amplitude}) is below the trade filter ({low_cutoff}); deferring trades until swings expand."
        )
    elif amplitude_flag == "high" and latest_direction == "upswing" and action in {"BUY", "ADD", "HOLD"}:
        action = "ADD"
        notes.append(
            f"High-amplitude upswing ({latest_amplitude}) above {high_cutoff}; leaning into long exposure with a larger add."
        )

    return action, " ".join(note for note in notes if note)


def build_indicator_context(
    rsi_series: Sequence[Mapping], macd_series: Sequence[Mapping]
) -> Mapping:
    """Extract the latest indicator readings and slope cues for gating actions."""

    context: Mapping[str, float | bool | None]
    latest_rsi = rsi_series[-1]["rsi"] if rsi_series else None
    latest_macd_entry = macd_series[-1] if macd_series else None
    prev_macd_entry = macd_series[-2] if len(macd_series) > 1 else None

    macd_improving = False
    if latest_macd_entry and prev_macd_entry:
        macd_improving = (
            latest_macd_entry.get("macd", 0) > prev_macd_entry.get("macd", 0)
            and latest_macd_entry.get("hist", 0) >= prev_macd_entry.get("hist", 0)
        )

    context = {
        "latest_rsi": latest_rsi,
        "latest_macd": latest_macd_entry.get("macd") if latest_macd_entry else None,
        "latest_macd_hist": latest_macd_entry.get("hist") if latest_macd_entry else None,
        "macd_improving": macd_improving,
    }

    return context
