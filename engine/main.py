from __future__ import annotations

from datetime import datetime
from pathlib import Path

from engine.backtest.performance import (
    DEFAULT_TRADING_COSTS,
    attach_dates,
    compute_bollinger_bands,
    compute_buy_and_hold_equity,
    compute_equity_curve,
    compute_macd,
    compute_moving_average,
    compute_obv,
    compute_performance_stats,
    compute_rolling_stddev,
    compute_rsi,
    compute_algorithm_score,
    decompose_closes,
    event_breakdown,
    summarize_returns,
)
from engine.decision.engine import build_indicator_context, select_action
from engine.events.cycles import (
    detect_cycles,
    filter_cycles,
    summarize_cycles,
    turning_points_to_records,
)
from engine.events.detector import detect_events
from engine.fetchers.slv_real import fetch_slv_ohlcv
from engine.utils.io import ensure_parent, write_json

BASE_PATH = Path("public/data")


def run_pipeline() -> None:
    raw_data = fetch_slv_ohlcv(
        start_date="2008-01-01",
        cache_path=str(BASE_PATH / "raw/slv_daily.json"),
        source="stooq",
    )
    closes = [row["close"] for row in raw_data]
    opens = [row["open"] for row in raw_data]
    volumes = [row["volume"] for row in raw_data]
    dates = [row["date"] for row in raw_data]

    latest_events = detect_events(raw_data)
    cycles, turning_points = detect_cycles(raw_data)
    filtered_cycles = filter_cycles(cycles, min_length=2)
    cycle_stats = summarize_cycles(filtered_cycles)

    perf_summary = summarize_returns(closes)
    algo_score = compute_algorithm_score(closes, filtered_cycles)
    equity_curve = compute_equity_curve(
        closes, opens=opens, costs=DEFAULT_TRADING_COSTS, turnover=1.0
    )
    buy_and_hold_curve = compute_buy_and_hold_equity(closes)
    strategy_stats = compute_performance_stats(equity_curve)
    buy_and_hold_stats = compute_performance_stats(buy_and_hold_curve)
    breakdown = event_breakdown([event.name for event in latest_events], closes)
    rsi_raw = compute_rsi(closes)
    macd_raw = compute_macd(closes)
    bollinger_raw = compute_bollinger_bands(closes)
    obv_raw = compute_obv(closes, volumes)
    rsi_series = attach_dates(rsi_raw, dates)
    stddev_series = attach_dates(compute_rolling_stddev(closes), dates)
    macd_series = attach_dates(macd_raw, dates)
    bollinger_series = attach_dates(bollinger_raw, dates)
    obv_series = attach_dates(obv_raw, dates)
    ma1000_series = attach_dates(compute_moving_average(closes, window=1000), dates)
    decomposition = decompose_closes(closes, period=30)

    indicator_context = build_indicator_context(rsi_raw, macd_raw)
    signal = select_action(
        latest_events, filtered_cycles, indicator_context=indicator_context
    )
    now = datetime.utcnow().isoformat()
    history_record = {"timestamp": now, **signal}

    write_json(BASE_PATH / "events/latest.json", {"as_of": now, "events": [e.to_dict() for e in latest_events]})
    write_json(
        BASE_PATH / "events/cycle_stats.json",
        {
            "updated_at": now,
            "cycles": [cycle.to_dict() for cycle in filtered_cycles],
            "stats": cycle_stats,
            "turning_points": turning_points_to_records(
                turning_points, dates=[row["date"] for row in raw_data], closes=closes
            ),
        },
    )
    write_json(BASE_PATH / "signals/latest_signal.json", signal)

    events_history_path = BASE_PATH / "events/history.jsonl"
    signal_history_path = BASE_PATH / "signals/signal_history.jsonl"

    append_jsonl(events_history_path, {"timestamp": now, "events": [e.to_dict() for e in latest_events]})
    append_jsonl(signal_history_path, history_record)

    write_json(
        BASE_PATH / "perf/summary.json",
        {"updated_at": now, **perf_summary.to_dict(), "algo_score": algo_score.to_dict()},
    )
    write_json(
        BASE_PATH / "perf/equity_curve.json",
        {
            "updated_at": now,
            "equity_curve": equity_curve,
            "buy_and_hold_curve": buy_and_hold_curve,
            "performance": {
                "strategy": strategy_stats,
                "buy_and_hold": buy_and_hold_stats,
            },
            "trading_costs": {
                "commission_pct": DEFAULT_TRADING_COSTS.commission_pct,
                "slippage_pct": DEFAULT_TRADING_COSTS.slippage_pct,
                "turnover": 1.0,
                "execution": "next_open_to_close",
            },
        },
    )
    write_json(BASE_PATH / "perf/by_event.json", {"updated_at": now, "breakdown": breakdown})
    write_json(
        BASE_PATH / "perf/rsi.json",
        {"updated_at": now, "period": 14, "rsi": rsi_series},
    )
    write_json(
        BASE_PATH / "perf/stddev.json",
        {"updated_at": now, "window": 20, "stddev": stddev_series},
    )
    write_json(
        BASE_PATH / "perf/macd.json",
        {
            "updated_at": now,
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
            "macd": macd_series,
        },
    )
    write_json(
        BASE_PATH / "perf/bollinger.json",
        {
            "updated_at": now,
            "window": 20,
            "num_stddev": 2.0,
            "bands": bollinger_series,
        },
    )
    write_json(
        BASE_PATH / "perf/obv.json",
        {"updated_at": now, "obv": obv_series},
    )
    write_json(
        BASE_PATH / "perf/ma1000.json",
        {"updated_at": now, "window": 1000, "ma": ma1000_series},
    )
    write_json(
        BASE_PATH / "perf/decomposition.json",
        {
            "updated_at": now,
            "period": 30,
            "trend": decomposition["trend"],
            "seasonal": decomposition["seasonal"],
            "resid": decomposition["resid"],
        },
    )


def append_jsonl(path: Path, record: dict) -> None:
    ensure_parent(path)
    existing = []
    if path.exists():
        existing = [line for line in path.read_text().splitlines() if line.strip()]
    existing.append(json_dumps(record))
    path.write_text("\n".join(existing))


def json_dumps(record: dict) -> str:
    import json

    return json.dumps(record)


if __name__ == "__main__":
    run_pipeline()
