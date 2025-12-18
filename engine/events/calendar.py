from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Mapping, Sequence

PathLike = str | Path


@dataclass
class KnownEvent:
    date: date
    event_type: str
    category: str
    priority: str
    time_utc: str | None = None
    title: str | None = None
    source: str | None = None
    notes: str | None = None

    @classmethod
    def from_dict(cls, payload: Mapping) -> "KnownEvent":
        event_type = payload.get("type") or payload.get("event")
        return cls(
            date=datetime.fromisoformat(str(payload["date"]).strip()).date(),
            event_type=str(event_type),
            category=str(payload.get("category", "")),
            priority=str(payload.get("priority", "")),
            time_utc=payload.get("time_utc") or payload.get("known_time"),
            title=payload.get("title"),
            source=payload.get("source"),
            notes=payload.get("notes") or payload.get("description"),
        )

    def to_dict(self) -> Mapping:
        return {
            "date": self.date.isoformat(),
            "type": self.event_type,
            "category": self.category,
            "priority": self.priority,
            "time_utc": self.time_utc,
            "title": self.title,
            "source": self.source,
            "notes": self.notes,
        }


@dataclass
class EventOccurrence:
    event: KnownEvent
    index: int
    trading_date: date


def load_events_calendar(path: PathLike) -> tuple[list[KnownEvent], Mapping[str, str]]:
    path_obj = Path(path)
    if not path_obj.exists():
        return [], {"version": "", "timezone": "UTC"}

    payload = json.loads(path_obj.read_text())
    meta = {"version": "", "timezone": "UTC"}
    if isinstance(payload, Mapping):
        meta["version"] = str(payload.get("version") or meta["version"])
        meta["timezone"] = str(payload.get("timezone") or meta["timezone"])
        events_payload = payload.get("events", [])
    else:
        events_payload = payload

    return [KnownEvent.from_dict(item) for item in events_payload], meta


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


def _safe_median(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return median(clean)


def _percentile(values: list[float | None], q: float) -> float | None:
    clean = sorted(v for v in values if v is not None)
    if not clean or not 0 <= q <= 1:
        return None
    k = (len(clean) - 1) * q
    f = int(k)
    c = f + 1
    if c >= len(clean):
        return clean[-1]
    d0 = clean[f] * (c - k)
    d1 = clean[c] * (k - f)
    return d0 + d1


def _confidence_from_samples(n: int) -> str:
    if n > 120:
        return "high"
    if n >= 60:
        return "medium"
    if n > 0:
        return "low"
    return "none"


def _pct_return(closes: Sequence[float], start_idx: int, end_idx: int) -> float | None:
    if start_idx < 0 or end_idx >= len(closes):
        return None
    start = closes[start_idx]
    end = closes[end_idx]
    if start == 0:
        return None
    return (end - start) / start


def _true_range_series(
    highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]
) -> list[float | None]:
    tr: list[float | None] = []
    for idx in range(len(closes)):
        if idx == 0:
            tr.append(None)
            continue
        high = highs[idx]
        low = lows[idx]
        prev_close = closes[idx - 1]
        tr.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return tr


def _rolling_median(series: Sequence[float | None], window: int) -> list[float | None]:
    medians: list[float | None] = []
    for idx in range(len(series)):
        start = max(0, idx - window + 1)
        window_vals = [v for v in series[start : idx + 1] if v is not None]
        medians.append(median(window_vals) if window_vals else None)
    return medians


def _extract_numeric_series(series: Sequence[float | None | Mapping]) -> list[float | None]:
    values: list[float | None] = []
    for item in series:
        if isinstance(item, Mapping):
            if "atr" in item:
                values.append(item.get("atr"))
            elif "value" in item:
                values.append(item.get("value"))
            else:
                values.append(None)
        else:
            values.append(item)
    return values


def compute_event_impact_stats(
    closes: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    atr_series: Sequence[float | None],
    occurrences: Sequence[EventOccurrence],
    *,
    windows: Mapping[str, Sequence[int]] | None = None,
    as_of: str | None = None,
    atr_baseline_window: int = 252,
    asset: str = "SLV",
) -> Mapping:
    window_config = windows or {"pre": (-3, -1), "event_day": (0, 0), "post": (1, 5)}
    tr_series = _true_range_series(highs, lows, closes)
    atr_values = _extract_numeric_series(atr_series)
    atr_baseline = _rolling_median(atr_values, window=atr_baseline_window)
    atr_change = [
        (atr / baseline - 1) if atr is not None and baseline not in (None, 0) else None
        for atr, baseline in zip(atr_values, atr_baseline)
    ]

    grouped: dict[str, list[Mapping[str, float | int | str | None]]] = {}
    for occ in occurrences:
        idx = occ.index
        if idx <= 0 or idx >= len(closes):
            continue
        samples: dict[str, float | None] = {
            "ret_1d": _pct_return(closes, idx - 1, idx),
            "ret_5d": _pct_return(closes, idx, min(idx + 5, len(closes) - 1)),
            "ret_10d": _pct_return(closes, idx, min(idx + 10, len(closes) - 1)),
            "atr_change": atr_change[idx] if idx < len(atr_change) else None,
            "tr": tr_series[idx] if idx < len(tr_series) else None,
            "close": closes[idx],
        }
        grouped.setdefault(occ.event.event_type, []).append(samples)

    stats: dict[str, Mapping] = {
        "meta": {
            "asof": as_of or datetime.utcnow().date().isoformat(),
            "asset": asset,
            "windows": {k: list(v) for k, v in window_config.items()},
        }
    }

    for name, samples in grouped.items():
        n = len(samples)
        confidence = _confidence_from_samples(n)
        ret_1d = [s["ret_1d"] for s in samples]
        ret_5d = [s["ret_5d"] for s in samples]
        ret_10d = [s["ret_10d"] for s in samples]
        atr_changes = [s["atr_change"] for s in samples]
        tr_relative = [
            (s["tr"] / s["close"]) if s.get("tr") is not None and s.get("close") else None
            for s in samples
        ]

        stats[name] = {
            "confidence": confidence,
            "event_day": {
                "n": n,
                "p_up_1d": sum(1 for r in ret_1d if r is not None and r > 0) / n if n else None,
                "median_1d": _safe_median(ret_1d),
                "atr_change_median": _safe_median(atr_changes),
                "tr_p90": _percentile(tr_relative, 0.9),
            },
            "post_5d": {
                "n": n,
                "p_up_5d": sum(1 for r in ret_5d if r is not None and r > 0) / n if n else None,
                "median_5d": _safe_median(ret_5d),
                "p10_5d": _percentile(ret_5d, 0.1),
                "p90_5d": _percentile(ret_5d, 0.9),
            },
            "post_10d": {
                "n": n,
                "p_up_10d": sum(1 for r in ret_10d if r is not None and r > 0) / n if n else None,
                "median_10d": _safe_median(ret_10d),
                "p10_10d": _percentile(ret_10d, 0.1),
                "p90_10d": _percentile(ret_10d, 0.9),
            },
        }

    return stats


def compute_current_event_context(
    *,
    occurrences: Sequence[EventOccurrence],
    stats: Mapping,
    as_of: str,
    pre_window: int = 3,
    post_window: int = 5,
) -> Mapping:
    as_of_date = datetime.fromisoformat(as_of).date()
    sorted_events = sorted(occurrences, key=lambda e: e.trading_date)

    def _nearest_event() -> EventOccurrence | None:
        nearest: tuple[int, EventOccurrence] | None = None
        for occ in sorted_events:
            delta = (occ.trading_date - as_of_date).days
            if -post_window <= delta <= pre_window:
                return occ
            if delta >= 0 and (nearest is None or delta < nearest[0]):
                nearest = (delta, occ)
        return nearest[1] if nearest else None

    def _phase(event_date: date) -> tuple[str, int]:
        delta = (event_date - as_of_date).days
        if -post_window <= delta < 0:
            return "POST_EVENT", delta
        if delta == 0:
            return "EVENT_DAY", delta
        if 0 < delta <= pre_window:
            return "PRE_EVENT", delta
        return "UPCOMING", delta

    occ = _nearest_event()
    if not occ:
        return {
            "asof": as_of,
            "window": {"pre": [-pre_window, -1], "post": [1, post_window]},
            "framing": {"headline": "No events loaded", "note": "No known calendar events in range."},
            "nearest": None,
        }

    phase, delta_days = _phase(occ.trading_date)
    event_stats = stats.get(occ.event.event_type, {}) if isinstance(stats, Mapping) else {}
    event_day = event_stats.get("event_day", {}) if isinstance(event_stats, Mapping) else {}
    post_5d = event_stats.get("post_5d", {}) if isinstance(event_stats, Mapping) else {}
    confidence = event_stats.get("confidence", "none") if isinstance(event_stats, Mapping) else "none"

    atr_change = event_day.get("atr_change_median")
    vol_label = "Volatility context unavailable"
    if isinstance(atr_change, (int, float)):
        if atr_change > 0:
            vol_label = "Higher volatility is common"
        elif atr_change < 0:
            vol_label = "Volatility often softens"
        else:
            vol_label = "Volatility similar to baseline"

    p_up_1d = event_day.get("p_up_1d")
    bias_label = "No consistent same-day directional bias"
    if isinstance(p_up_1d, float):
        if p_up_1d > 0.55:
            bias_label = "Historically more up closes on event day"
        elif p_up_1d < 0.45:
            bias_label = "Historically more down closes on event day"

    return {
        "asof": as_of,
        "window": {"pre": [-pre_window, -1], "post": [1, post_window]},
        "nearest": {
            "type": occ.event.event_type,
            "date": occ.event.date.isoformat(),
            "days_to_event": delta_days,
            "phase": phase,
            "priority": occ.event.priority,
            "category": occ.event.category,
            "time_utc": occ.event.time_utc,
            "title": occ.event.title or occ.event.event_type,
        },
        "framing": {
            "headline": "Known macro event window ahead" if phase in {"PRE_EVENT", "UPCOMING"} else "Event window in progress",
            "note": "Historical context only; not a promise.",
        },
        "expected_effects": {
            "volatility": {
                "label": vol_label,
                "atr_change_median": atr_change,
                "confidence": confidence,
            },
            "direction": {
                "label": bias_label,
                "p_up_1d": p_up_1d,
                "p_up_5d": post_5d.get("p_up_5d"),
                "median_5d": post_5d.get("median_5d"),
                "confidence": confidence,
            },
        },
    }


def build_event_context(
    occurrences: Sequence[EventOccurrence],
    stats: Mapping,
    *,
    as_of: str,
    pre_window: int = 3,
    post_window: int = 5,
) -> Mapping:
    return compute_current_event_context(
        occurrences=occurrences,
        stats=stats,
        as_of=as_of,
        pre_window=pre_window,
        post_window=post_window,
    )
