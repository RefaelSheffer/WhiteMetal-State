import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from engine.backtest.walkforward import WalkForwardWindow, walk_forward_validate


class TestWalkForwardWindow:
    def test_to_dict_adds_dates_when_available(self):
        window = WalkForwardWindow(train_start=0, train_end=2, test_start=3, test_end=4)
        dates = ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]

        result = window.to_dict(dates)

        assert result["train_start_date"] == "2020-01-01"
        assert result["test_end_date"] == "2020-01-05"

    def test_to_dict_skips_dates_when_out_of_range(self):
        window = WalkForwardWindow(train_start=0, train_end=2, test_start=3, test_end=6)
        dates = ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]

        result = window.to_dict(dates)

        assert "train_start_date" not in result


class TestWalkForwardValidate:
    def test_generates_non_overlapping_windows(self):
        closes = list(range(1, 21))
        dates = [f"2020-01-{idx:02d}" for idx in range(1, 21)]

        result = walk_forward_validate(closes, train_size=5, test_size=3, dates=dates)

        assert len(result["windows"]) == 5  # 5 train + 3 test, stepping 3 at a time
        first_window = result["windows"][0]["window"]
        assert first_window["train_start"] == 0
        assert first_window["test_end_date"] == "2020-01-08"

    def test_raises_for_tiny_windows(self):
        with pytest.raises(ValueError):
            walk_forward_validate([1, 2, 3], train_size=1, test_size=2)

        with pytest.raises(ValueError):
            walk_forward_validate([1, 2, 3], train_size=2, test_size=1)

    def test_returns_empty_when_not_enough_history(self):
        result = walk_forward_validate([1, 2, 3], train_size=3, test_size=3)

        assert result == {"windows": [], "summary": {}}
