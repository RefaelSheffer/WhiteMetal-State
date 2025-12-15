import math
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from engine.backtest.performance import (
    attach_dates,
    compute_equity_curve,
    compute_algorithm_score,
    compute_bollinger_bands,
    compute_macd,
    compute_moving_average,
    compute_obv,
    compute_rsi,
    compute_rolling_stddev,
    TradingCosts,
    decompose_closes,
)
from engine.events.cycles import CycleSegment


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


class TestMacd:
    def test_macd_matches_known_values(self):
        closes = [10, 11, 12, 11, 13, 14, 15, 14]

        macd = compute_macd(closes, fast_period=3, slow_period=6, signal_period=3)

        assert macd == [
            {"index": 7, "macd": 0.9014, "signal": 1.11, "hist": -0.2086}
        ]

    def test_macd_requires_enough_history(self):
        assert compute_macd([1, 2, 3], fast_period=3, slow_period=6, signal_period=3) == []


class TestBollingerBands:
    def test_computes_upper_and_lower_bands(self):
        closes = [10, 11, 12, 11, 13, 14, 15, 14]

        bands = compute_bollinger_bands(closes, window=3, num_stddev=2)

        assert bands[0] == {"index": 2, "middle": 11.0, "upper": 12.633, "lower": 9.367}
        assert bands[-1] == {
            "index": 7,
            "middle": 14.3333,
            "upper": 15.2761,
            "lower": 13.3905,
        }

    def test_bollinger_handles_short_series(self):
        assert compute_bollinger_bands([1, 2], window=3) == []


class TestOnBalanceVolume:
    def test_obv_accumulates_directionally(self):
        closes = [10, 11, 12, 11, 13, 14, 15, 14]
        volumes = [100, 110, 120, 130, 140, 150, 160, 170]

        obv = compute_obv(closes, volumes)

        assert obv == [
            {"index": 0, "obv": 100},
            {"index": 1, "obv": 210},
            {"index": 2, "obv": 330},
            {"index": 3, "obv": 200},
            {"index": 4, "obv": 340},
            {"index": 5, "obv": 490},
            {"index": 6, "obv": 650},
            {"index": 7, "obv": 480},
        ]

    def test_obv_requires_matching_lengths(self):
        assert compute_obv([1, 2, 3], [100, 200]) == []
        assert compute_obv([], []) == []


class TestMovingAverage:
    def test_ma_computes_simple_average(self):
        closes = [10, 11, 12, 11, 13, 14, 15, 14]

        ma = compute_moving_average(closes, window=3)

        assert ma == [
            {"index": 2, "ma": 11.0},
            {"index": 3, "ma": 11.3333},
            {"index": 4, "ma": 12.0},
            {"index": 5, "ma": 12.6667},
            {"index": 6, "ma": 14.0},
            {"index": 7, "ma": 14.3333},
        ]

    def test_ma_handles_short_series(self):
        assert compute_moving_average([1, 2], window=3) == []

    def test_ma_rejects_non_positive_window(self):
        try:
            compute_moving_average([1, 2, 3], window=0)
        except ValueError as exc:
            assert "window must be positive" in str(exc)
        else:
            assert False, "Expected ValueError for non-positive window"


class TestAttachDates:
    def test_attach_dates_adds_date_field(self):
        series = [{"index": 1, "value": 10.0}, {"index": 3, "value": 12.0}]
        dates = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]

        dated = attach_dates(series, dates)

        assert dated == [
            {"index": 1, "value": 10.0, "date": "2024-01-02"},
            {"index": 3, "value": 12.0, "date": "2024-01-04"},
        ]

    def test_attach_dates_skips_out_of_range(self):
        series = [{"index": 5, "value": 1.0}]
        dates = ["2024-01-01", "2024-01-02"]

        assert attach_dates(series, dates) == []


class TestComputeAlgorithmScore:
    def test_blends_hit_rate_sharpe_and_cycle_capture(self):
        closes = [10, 11, 10.5, 12, 12.5, 11.5]
        cycles = [
            CycleSegment(
                start_idx=0,
                end_idx=3,
                start_date="",
                end_date="",
                direction="upswing",
                length=3,
                amplitude=(12 - 10) / 10,
                start_close=10,
                end_close=12,
            ),
            CycleSegment(
                start_idx=3,
                end_idx=5,
                start_date="",
                end_date="",
                direction="downswing",
                length=2,
                amplitude=(11.5 - 12) / 12,
                start_close=12,
                end_close=11.5,
            ),
        ]

        score = compute_algorithm_score(closes, cycles)

        assert score.hit_rate == pytest.approx(0.6)
        assert score.sharpe_ratio == pytest.approx(0.3773, rel=1e-3)
        assert score.cycle_capture_rate == pytest.approx(0.7273, rel=1e-3)
        assert 50 <= score.composite <= 80  # weighted blend should land mid-range

    def test_accepts_custom_weightings(self):
        closes = [10, 11, 10.5, 12, 12.5, 11.5]
        cycles = [
            CycleSegment(
                start_idx=0,
                end_idx=3,
                start_date="",
                end_date="",
                direction="upswing",
                length=3,
                amplitude=(12 - 10) / 10,
                start_close=10,
                end_close=12,
            ),
            CycleSegment(
                start_idx=3,
                end_idx=5,
                start_date="",
                end_date="",
                direction="downswing",
                length=2,
                amplitude=(11.5 - 12) / 12,
                start_close=12,
                end_close=11.5,
            ),
        ]

        weights = {"hit_rate": 0.7, "sharpe_ratio": 0.2, "cycle_capture_rate": 0.1}
        score = compute_algorithm_score(closes, cycles, weights=weights)

        assert score.hit_rate == pytest.approx(0.6)
        assert 60 <= score.composite <= 62  # higher bias to hit rate pulls composite down slightly


class TestEquityCurveWithCosts:
    def test_applies_round_trip_friction(self):
        closes = [100.0, 102.0, 104.0]
        opens = [99.0, 100.0, 102.0]

        base_curve = compute_equity_curve(closes, opens=opens, costs=TradingCosts())
        cost_curve = compute_equity_curve(
            closes, opens=opens, costs=TradingCosts(commission_pct=0.001, slippage_pct=0.0005)
        )

        assert base_curve[-1]["equity"] == pytest.approx(1.04, rel=1e-3)
        assert cost_curve[-1]["equity"] == pytest.approx(1.0342, rel=1e-3)
        assert cost_curve[-1]["equity"] < base_curve[-1]["equity"]

    def test_prefers_next_open_to_close_returns_when_available(self):
        closes = [10.0, 11.0, 12.0]
        opens = [10.0, 20.0, 5.0]

        curve_with_opens = compute_equity_curve(closes, opens=opens, costs=TradingCosts())
        curve_with_closes = compute_equity_curve(closes, costs=TradingCosts())

        assert curve_with_opens[-1]["equity"] == pytest.approx(1.32, rel=1e-3)
        assert curve_with_closes[-1]["equity"] == pytest.approx(1.2, rel=1e-3)
        assert curve_with_opens[-1]["equity"] != curve_with_closes[-1]["equity"]
