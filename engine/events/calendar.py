from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean
from typing import Iterable, Mapping, Sequence

PathLike = str | Path


@dataclass
class KnownEvent:
    date: date
    event: str
    category: str
    priority: str
    known_time: str | None = None
    description: str | None = None

    @classmethod
    def from_dict(cls, payload: Mapping) -> "KnownEvent":
        return cls(
            date=datetime.fromisoformat(str(payload["date"]).strip()).date(),
            event=str(payload["event"]),
            category=str(payload.get("category", "")),
            priority=str(payload.get("priority", "")),
            known_time=payload.get("known_time"),
            description=payload.get("description"),
        )

    def to_dict(self) -> Mapping:
        return {
            "date": self.date.isoformat(),
            "event": self.event,
            "category": self.category,
            "priority": self.priority,
            "known_time": self.known_time,
            "description": self.description,
        }


@dataclass
class EventOccurrence:
    event: KnownEvent
    index: int
    trading_date: date


def load_events_calendar(path: PathLike) -> list[KnownEvent]:
    path_obj = Path(path)
    if not path_obj.exists():
        return []
    payload = json.loads(path_obj.read_text())
    return [KnownEvent.from_dict(item) for item in payload]


def align_events_to_history(
    events: Iterable[KnownEvent], price_dates: Sequence[str]
) -> list[EventOccurrence]:
    parsed_dates = [datetime.fromisoformat(d).date() for d in price_dates]
    occurrences: list[EventOccurrence] = []
    for event in events:
        idx = next((i for i, d in enumerate(parsed_dates) if d >= event.date), None)
        if idx is None:
            continue
        occurrences.append(EventOccurrence(event=event, index=idx, trading_date=parsed_dates[idx]))
    return occurrences


def _safe_mean(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return mean(clean)


def _confidence_from_samples(n: int) -> str:
    if n >= 12:
        return "high"
    if n >= 6:
        return "medium"
    if n > 0:
        return "low"
    return "none"


def compute_event_impact_stats(
    closes: Sequence[float],
    atr_series: Sequence[float | None],
    occurrences: Sequence[EventOccurrence],
    *,
    sample_threshold: int = 3,
) -> Mapping:
    grouped: dict[str, list[Mapping[str, float | int | str | None]]] = {}
    for occ in occurrences:
        idx = occ.index
        if idx <= 0 or idx >= len(closes):
            continue
        ret_1d = ((closes[idx] - closes[idx - 1]) / closes[idx - 1]) * 100
        forward_idx = min(idx + 5, len(closes) - 1)
        ret_5d = ((closes[forward_idx] - closes[idx]) / closes[idx]) * 100
        vol_change = None
        prev_atr = atr_series[idx - 1] if idx - 1 < len(atr_series) else None
        curr_atr = atr_series[idx] if idx < len(atr_series) else None
        if prev_atr and prev_atr != 0 and curr_atr is not None:
            vol_change = ((curr_atr - prev_atr) / prev_atr) * 100

        grouped.setdefault(occ.event.event, []).append(
            {
                "ret_1d": ret_1d,
                "ret_5d": ret_5d,
                "vol_change": vol_change,
            }
        )

    stats: dict[str, Mapping] = {"updated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z"}
    for name, samples in grouped.items():
        n = len(samples)
        avg_1d = _safe_mean([s["ret_1d"] for s in samples])
        avg_5d = _safe_mean([s["ret_5d"] for s in samples])
        avg_vol = _safe_mean([s["vol_change"] for s in samples])
        up_prob = sum(1 for s in samples if s["ret_1d"] is not None and s["ret_1d"] > 0) / n
        down_prob = sum(1 for s in samples if s["ret_1d"] is not None and s["ret_1d"] < 0) / n
        bias = "neutral"
        if up_prob > 0.55:
            bias = "up"
        elif down_prob > 0.55:
            bias = "down"

        confidence = _confidence_from_samples(n)
        stats[name] = {
            "sample_size": n,
            "confidence": confidence,
            "low_confidence": n < sample_threshold,
            "event_day": {
                "avg_return_1d": avg_1d,
                "volatility_change_pct": avg_vol,
                "directional_bias": bias,
                "up_probability": up_prob,
                "down_probability": down_prob,
            },
            "post_5d": {
                "mean_return": avg_5d,
                "up_probability": sum(
                    1 for s in samples if s["ret_5d"] is not None and s["ret_5d"] > 0
                )
                / n,
                "down_probability": sum(
                    1 for s in samples if s["ret_5d"] is not None and s["ret_5d"] < 0
                )
                / n,
            },
        }

    return stats


def build_event_context(
    occurrences: Sequence[EventOccurrence],
    stats: Mapping,
    *,
    as_of: str,
    pre_window: int = 3,
    post_window: int = 5,
) -> Mapping:
    as_of_date = datetime.fromisoformat(as_of).date()
    sorted_events = sorted(occurrences, key=lambda e: e.trading_date)

    def _describe(event_name: str) -> tuple[str, str, int]:
        impact = stats.get(event_name, {})
        event_day = impact.get("event_day", {}) if isinstance(impact, Mapping) else {}
        sample_size = impact.get("sample_size") if isinstance(impact, Mapping) else None
        vol_change = event_day.get("volatility_change_pct")
        bias = event_day.get("directional_bias")
        down_prob = event_day.get("down_probability")
        up_prob = event_day.get("up_probability")
        vol_note = (
            f"Volatility (ATR) tends to shift by {vol_change:.1f}% on event days" if isinstance(vol_change, (int, float)) else "Volatility impact based on historical ATR shifts"
        )
        bias_note = "Directional bias has been mixed historically"
        if isinstance(bias, str):
            if bias == "up" and isinstance(up_prob, float):
                bias_note = f"Event days finished higher {up_prob:.0%} of the time"
            elif bias == "down" and isinstance(down_prob, float):
                bias_note = f"Event days finished lower {down_prob:.0%} of the time"
        confidence = impact.get("confidence", "none") if isinstance(impact, Mapping) else "none"
        return vol_note, bias_note, sample_size or 0, confidence

    context: Mapping | None = None
    next_event = None
    for occ in sorted_events:
        delta_days = (occ.trading_date - as_of_date).days
        status = None
        if -post_window <= delta_days < 0:
            status = "POST_EVENT"
        elif delta_days == 0:
            status = "EVENT_DAY"
        elif 0 < delta_days <= pre_window:
            status = "PRE_EVENT"
        if status:
            vol_note, bias_note, sample_size, confidence = _describe(occ.event.event)
            context = {
                "status": status,
                "event": occ.event.event,
                "category": occ.event.category,
                "priority": occ.event.priority,
                "calendar_date": occ.event.date.isoformat(),
                "trading_date": occ.trading_date.isoformat(),
                "days_to_event": delta_days,
                "known_time": occ.event.known_time,
                "description": occ.event.description,
                "historical_note": vol_note,
                "directional_note": bias_note,
                "sample_size": sample_size,
                "confidence": confidence,
                "window": {"pre_days": pre_window, "post_days": post_window},
                "as_of": as_of,
            }
            break
        if delta_days > 0 and next_event is None:
            next_event = (delta_days, occ)

    if context:
        return context

    if next_event:
        delta, occ = next_event
        vol_note, bias_note, sample_size, confidence = _describe(occ.event.event)
        return {
            "status": "UPCOMING",
            "event": occ.event.event,
            "category": occ.event.category,
            "priority": occ.event.priority,
            "calendar_date": occ.event.date.isoformat(),
            "trading_date": occ.trading_date.isoformat(),
            "days_to_event": delta,
            "known_time": occ.event.known_time,
            "description": occ.event.description,
            "historical_note": vol_note,
            "directional_note": bias_note,
            "sample_size": sample_size,
            "confidence": confidence,
            "window": {"pre_days": pre_window, "post_days": post_window},
            "as_of": as_of,
        }

    return {
        "status": "NO_EVENT",
        "as_of": as_of,
        "window": {"pre_days": pre_window, "post_days": post_window},
        "message": "No known calendar events in range.",
    }
