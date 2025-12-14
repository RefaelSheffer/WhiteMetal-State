import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from engine.backtest.performance import (
    compute_rsi,
    compute_rolling_stddev,
    decompose_closes,
)


class TestComputeRSI:
    def test_returns_known_values_for_sample_data(self):
        closes = [
            44,
            47,
            45,
            48,
            50,
            49,
            51,
            52,
            53,
            54,
            55,
            56,
            57,
            58,
            59,
            60,
            58,
            57,
            59,
            61,
        ]

        rsi = compute_rsi(closes, period=14)

        assert rsi == [
            {"index": 14, "rsi": 85.71},
            {"index": 15, "rsi": 86.41},
            {"index": 16, "rsi": 78.2},
            {"index": 17, "rsi": 74.39},
            {"index": 18, "rsi": 76.82},
            {"index": 19, "rsi": 78.97},
        ]

    def test_returns_empty_for_insufficient_data(self):
        assert compute_rsi([1, 2, 3], period=5) == []
        assert compute_rsi([], period=14) == []


class TestComputeRollingStddev:
    def test_matches_simple_window_statistics(self):
        closes = [10, 12, 11, 13, 12, 14, 13]

        stddev = compute_rolling_stddev(closes, window=3)

        expected = [
            {"index": 2, "stddev": 0.8165},
            {"index": 3, "stddev": 0.8165},
            {"index": 4, "stddev": 0.8165},
            {"index": 5, "stddev": 0.8165},
            {"index": 6, "stddev": 0.8165},
        ]
        assert stddev == expected

    def test_handles_window_larger_than_series(self):
        assert compute_rolling_stddev([1, 2], window=5) == []


class TestDecomposeCloses:
    def test_constant_series_has_flat_trend_and_zero_noise(self):
        closes = [10.0] * 14

        components = decompose_closes(closes, period=7)

        assert len(components["trend"]) == len(closes)
        assert all(entry["trend"] == 10.0 for entry in components["trend"])
        assert all(math.isclose(entry["seasonal"], 0.0, abs_tol=1e-8) for entry in components["seasonal"])
        assert all(math.isclose(entry["resid"], 0.0, abs_tol=1e-8) for entry in components["resid"])

    def test_returns_empty_components_for_short_series(self):
        components = decompose_closes([1, 2, 3], period=14)

        assert components == {"trend": [], "seasonal": [], "resid": []}
