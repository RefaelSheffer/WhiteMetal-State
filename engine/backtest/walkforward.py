from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import List, Mapping, Sequence

from engine.backtest.performance import PerformanceSummary, summarize_returns


@dataclass
class WalkForwardWindow:
    """Index boundaries for a single walk-forward evaluation."""

    train_start: int
    train_end: int
    test_start: int
    test_end: int

    def to_dict(self, dates: Sequence[str] | None = None) -> Mapping:
        payload = {
            "train_start": self.train_start,
            "train_end": self.train_end,
            "test_start": self.test_start,
            "test_end": self.test_end,
        }

        if dates and all(idx < len(dates) for idx in payload.values()):
            payload.update(
                {
                    "train_start_date": dates[self.train_start],
                    "train_end_date": dates[self.train_end],
                    "test_start_date": dates[self.test_start],
                    "test_end_date": dates[self.test_end],
                }
            )

        return payload


@dataclass
class WalkForwardRun:
    """Train/test split with performance summaries for each half."""

    window: WalkForwardWindow
    train: PerformanceSummary
    test: PerformanceSummary

    def to_dict(self, dates: Sequence[str] | None = None) -> Mapping:
        return {
            "window": self.window.to_dict(dates),
            "train": self.train.to_dict(),
            "test": self.test.to_dict(),
        }


def walk_forward_validate(
    closes: Sequence[float],
    *,
    train_size: int,
    test_size: int,
    step: int | None = None,
    dates: Sequence[str] | None = None,
) -> Mapping[str, List[Mapping]]:
    """Roll a fixed train/test window forward and summarize each slice.

    The train slice feeds any parameter tuning, while the immediately
    following test slice is treated as out-of-sample. `step` controls how
    far to advance the anchor for the next window; by default it matches the
    test size for non-overlapping evaluations.
    """

    if train_size <= 1:
        raise ValueError("train_size must be at least 2 sessions")
    if test_size <= 1:
        raise ValueError("test_size must be at least 2 sessions")

    step = step or test_size
    runs: List[WalkForwardRun] = []

    total = len(closes)
    boundary = total - train_size - test_size + 1
    if boundary <= 0:
        return {"windows": [], "summary": {}}

    for anchor in range(0, boundary, step):
        train_start = anchor
        train_end = anchor + train_size - 1
        test_start = train_end + 1
        test_end = test_start + test_size - 1

        if test_end >= total:
            break

        train_slice = closes[train_start : train_end + 1]
        test_slice = closes[test_start : test_end + 1]

        runs.append(
            WalkForwardRun(
                window=WalkForwardWindow(
                    train_start=train_start,
                    train_end=train_end,
                    test_start=test_start,
                    test_end=test_end,
                ),
                train=summarize_returns(train_slice),
                test=summarize_returns(test_slice),
            )
        )

    return {"windows": [run.to_dict(dates) for run in runs], "summary": _summarize_runs(runs)}


def _summarize_runs(runs: Sequence[WalkForwardRun]) -> Mapping[str, float]:
    if not runs:
        return {}

    test_hit_rates = [run.test.hit_rate for run in runs]
    test_avg_5d = [run.test.avg_return_5d for run in runs]
    test_avg_10d = [run.test.avg_return_10d for run in runs]
    test_drawdowns = [run.test.max_drawdown for run in runs]

    return {
        "windows": len(runs),
        "avg_test_hit_rate": round(mean(test_hit_rates), 3),
        "avg_test_return_5d": round(mean(test_avg_5d), 4),
        "avg_test_return_10d": round(mean(test_avg_10d), 4),
        "worst_test_drawdown": round(min(test_drawdowns), 4),
    }
