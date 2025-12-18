import math

from engine.diagnostics.decomposition import DecompositionConfig, compute_decomposition


def test_computes_components_with_valid_history():
    dates = [f"2024-01-{day:02d}" for day in range(1, 31)]
    closes = [20 + 0.1 * day for day in range(len(dates))]

    result = compute_decomposition(dates, closes, DecompositionConfig(period=7, robust=True))

    assert result.meta["status"] == "ok"
    assert len(result.trend["rows"]) == len(dates)
    assert len(result.seasonal["rows"]) == len(dates)
    assert len(result.residual["rows"]) == len(dates)
    assert result.trend["key"] == "trend"
    assert all("date" in row and math.isfinite(row["value"]) for row in result.trend["rows"][:3])


def test_sets_error_metadata_when_series_too_short():
    dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
    closes = [10.0, 10.1, 10.2]

    result = compute_decomposition(dates, closes, DecompositionConfig(period=7))

    assert result.meta["status"] == "error"
    assert result.meta["error"]
    assert result.trend["rows"] == []
    assert result.residual["rows"] == []
    assert result.seasonal["rows"] == []
