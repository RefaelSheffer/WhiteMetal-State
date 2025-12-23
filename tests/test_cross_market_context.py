import json
from pathlib import Path

from engine.cross_market_context import generate_cross_market_context


def test_generate_writes_and_sanitizes(tmp_path: Path):
    output = tmp_path / "cross_market_context.json"
    fetchers = {
        "gold": lambda: [
            {"date": "2024-12-20", "value": "25.1"},
            {"date": "2024-12-21", "value": 25.3},
        ],
        "silver": lambda: [
            {"date": "2024-12-21", "value": 24.0},
            {"date": "2024-12-22", "value": "24.5"},
        ],
        "usd": lambda: [
            {"date": "2024-12-22", "value": None},
            {"date": "2024-12-23", "value": "nan"},
        ],
        "us10y": lambda: [
            {"date": "2024-12-22", "value": 3.85},
            {"date": "2024-12-23", "value": 3.9},
        ],
    }

    updated = generate_cross_market_context(output_path=output, fetchers=fetchers, history_days=10)
    assert updated is True

    payload = json.loads(output.read_text())
    assert payload["series"]["gold"]["last_value"] == 25.3
    assert payload["series"]["silver"]["history"][-1]["date"] == "2024-12-22"
    assert payload["series"]["usd"]["history"] == []
    assert payload["series"]["us10y"]["last_date"] == "2024-12-23"


def test_skip_when_last_dates_unchanged(tmp_path: Path):
    output = tmp_path / "cross_market_context.json"
    initial_fetchers = {
        "gold": lambda: [{"date": "2024-01-02", "value": 10.0}],
        "silver": lambda: [{"date": "2024-01-02", "value": 20.0}],
        "usd": lambda: [{"date": "2024-01-02", "value": 100.0}],
        "us10y": lambda: [{"date": "2024-01-02", "value": 4.0}],
    }
    generate_cross_market_context(output_path=output, fetchers=initial_fetchers, history_days=5)
    baseline = output.read_text()

    same_date_fetchers = {
        "gold": lambda: [{"date": "2024-01-02", "value": 11.0}],
        "silver": lambda: [{"date": "2024-01-02", "value": 21.0}],
        "usd": lambda: [{"date": "2024-01-02", "value": 101.0}],
        "us10y": lambda: [{"date": "2024-01-02", "value": 4.1}],
    }

    updated = generate_cross_market_context(output_path=output, fetchers=same_date_fetchers, history_days=5)

    assert updated is False
    assert output.read_text() == baseline
