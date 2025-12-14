from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import List, Mapping, Sequence, Tuple


@dataclass
class CycleSegment:
    start_idx: int
    end_idx: int
    start_date: str
    end_date: str
    direction: str  # "upswing" or "downswing"
    length: int
    amplitude: float
    start_close: float
    end_close: float

    def to_dict(self) -> Mapping:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "direction": self.direction,
            "length": self.length,
            "amplitude": round(self.amplitude, 4),
            "start_close": round(self.start_close, 2),
            "end_close": round(self.end_close, 2),
        }


@dataclass
class TurningPoint:
    index: int
    kind: str  # "peak" or "trough"


def detect_cycles(prices: Sequence[Mapping]) -> Tuple[List[CycleSegment], List[TurningPoint]]:
    """Identify peak/trough-based cycles using sign flips in daily returns."""

    if len(prices) < 3:
        return [], []

    closes = [row["close"] for row in prices]
    dates = [row["date"] for row in prices]

    turning_points = _find_turning_points(closes)
    segments: List[CycleSegment] = []

    for prev, curr in zip(turning_points, turning_points[1:]):
        if prev.kind == curr.kind:
            # Skip duplicate consecutive peaks/troughs; need an alternation to form a cycle.
            continue

        start_idx, end_idx = prev.index, curr.index
        start_close, end_close = closes[start_idx], closes[end_idx]
        amplitude = (end_close - start_close) / start_close
        direction = "upswing" if prev.kind == "trough" and curr.kind == "peak" else "downswing"

        segments.append(
            CycleSegment(
                start_idx=start_idx,
                end_idx=end_idx,
                start_date=dates[start_idx],
                end_date=dates[end_idx],
                direction=direction,
                length=end_idx - start_idx,
                amplitude=amplitude,
                start_close=start_close,
                end_close=end_close,
            )
        )

    return segments, turning_points


def summarize_cycles(cycles: Sequence[CycleSegment]) -> Mapping:
    if not cycles:
        return {
            "cycle_count": 0,
            "avg_length": 0,
            "avg_amplitude": 0,
            "avg_magnitude": 0,
            "avg_up_length": 0,
            "avg_down_length": 0,
            "avg_up_amplitude": 0,
            "avg_down_amplitude": 0,
        }

    lengths = [cycle.length for cycle in cycles]
    amplitudes = [cycle.amplitude for cycle in cycles]

    up_cycles = [cycle for cycle in cycles if cycle.direction == "upswing"]
    down_cycles = [cycle for cycle in cycles if cycle.direction == "downswing"]

    def _avg(values: Sequence[float]) -> float:
        return round(mean(values), 4) if values else 0

    return {
        "cycle_count": len(cycles),
        "avg_length": round(mean(lengths), 2),
        "avg_amplitude": _avg(amplitudes),
        "avg_magnitude": _avg([abs(a) for a in amplitudes]),
        "avg_up_length": round(mean([cycle.length for cycle in up_cycles]), 2) if up_cycles else 0,
        "avg_down_length": round(mean([cycle.length for cycle in down_cycles]), 2) if down_cycles else 0,
        "avg_up_amplitude": _avg([cycle.amplitude for cycle in up_cycles]),
        "avg_down_amplitude": _avg([cycle.amplitude for cycle in down_cycles]),
    }


def turning_points_to_records(
    turning_points: Sequence[TurningPoint], dates: Sequence[str], closes: Sequence[float]
) -> List[Mapping]:
    records: List[Mapping] = []

    for tp in turning_points:
        if tp.index < 0 or tp.index >= len(dates) or tp.index >= len(closes):
            continue

        records.append(
            {
                "index": tp.index,
                "kind": tp.kind,
                "date": dates[tp.index],
                "close": round(closes[tp.index], 2),
            }
        )

    return records


def _find_turning_points(closes: Sequence[float]) -> List[TurningPoint]:
    """Return indices where the slope changes sign (local peaks/troughs)."""

    turning_points: List[TurningPoint] = []
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

    # Initialize with the first non-zero delta to know the starting slope.
    prev_delta = None
    for delta in deltas:
        if delta != 0:
            prev_delta = delta
            break

    if prev_delta is None:
        return turning_points

    for idx, delta in enumerate(deltas[1:], start=1):
        if delta == 0:
            continue

        if prev_delta > 0 and delta < 0:
            turning_points.append(TurningPoint(index=idx, kind="peak"))
        elif prev_delta < 0 and delta > 0:
            turning_points.append(TurningPoint(index=idx, kind="trough"))

        prev_delta = delta

    return turning_points
