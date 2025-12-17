from engine.decision.engine import select_action
from engine.events.cycles import CycleSegment
from engine.events.detector import Event


def test_select_action_filters_short_cycles_from_context():
    events = [Event(name="NEUTRAL", confidence=0.5, rationale="neutral state")]
    cycles = [
        CycleSegment(
            start_idx=0,
            end_idx=1,
            start_date="2024-01-01",
            end_date="2024-01-02",
            direction="upswing",
            length=1,
            amplitude=0.002,
            start_close=10,
            end_close=10.02,
        ),
        CycleSegment(
            start_idx=1,
            end_idx=4,
            start_date="2024-01-02",
            end_date="2024-01-05",
            direction="upswing",
            length=3,
            amplitude=0.01,
            start_close=10.02,
            end_close=10.12,
        ),
    ]

    result = select_action(events, cycles)

    context = result["cycle_context"]
    assert context["filtered_cycle_count"] == 1
    assert context["raw_cycle_count"] == 2
    assert context["latest_length"] == 3


def test_low_amplitude_cycles_gate_trading_to_wait():
    events = [Event(name="SHAKEOUT", confidence=0.9, rationale="deep pullback")]
    cycles = [
        CycleSegment(
            start_idx=0,
            end_idx=3,
            start_date="2024-02-01",
            end_date="2024-02-04",
            direction="upswing",
            length=3,
            amplitude=0.001,
            start_close=20.0,
            end_close=20.02,
        )
    ]

    result = select_action(events, cycles)

    assert result["action"] == "WAIT"
    assert "below the trade filter" in result["rationale"]


def test_high_amplitude_upswing_allows_sizing_up_adds():
    events = [Event(name="SHAKEOUT", confidence=0.9, rationale="momentum turn")]
    cycles = [
        CycleSegment(
            start_idx=0,
            end_idx=4,
            start_date="2024-03-01",
            end_date="2024-03-05",
            direction="upswing",
            length=4,
            amplitude=0.05,
            start_close=15.0,
            end_close=15.75,
        )
    ]

    result = select_action(events, cycles)

    assert result["action"] == "ADD"
    assert "High-amplitude upswing" in result["rationale"]


def test_uptrend_regime_relaxes_buy_filter_after_shakeout():
    events = [Event(name="SHAKEOUT", confidence=0.8, rationale="pullback into trend")]
    cycles: list[CycleSegment] = []
    indicator_context = {
        "latest_rsi": 44,
        "latest_macd": 0.2,
        "latest_macd_hist": 0.05,
        "macd_improving": True,
        "regime": "uptrend",
        "regime_note": "ADX strong uptrend",
    }

    result = select_action(events, cycles, indicator_context=indicator_context)

    assert result["action"] == "BUY"
    assert "uptrend" in result["rationale"]


def test_downtrend_regime_demands_stricter_buy_confirmation():
    events = [Event(name="SHAKEOUT", confidence=0.85, rationale="volatility spike")]
    cycles: list[CycleSegment] = []
    indicator_context = {
        "latest_rsi": 31,
        "latest_macd": -0.05,
        "latest_macd_hist": -0.01,
        "macd_improving": False,
        "regime": "downtrend",
        "regime_note": "ADX strong downtrend",
    }

    result = select_action(events, cycles, indicator_context=indicator_context)

    assert result["action"] == "WAIT"
    assert "downtrend" in result["rationale"]
