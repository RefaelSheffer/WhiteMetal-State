from __future__ import annotations

from datetime import datetime
from pathlib import Path

from engine.backtest.performance import (
    attach_dates,
    compute_bollinger_bands,
    compute_equity_curve,
    compute_macd,
    compute_moving_average,
    compute_obv,
    compute_rolling_stddev,
    compute_rsi,
    compute_algorithm_score,
    decompose_closes,
    event_breakdown,
    summarize_returns,
)
from engine.decision.engine import select_action
from engine.events.cycles import (
    detect_cycles,
    summarize_cycles,
    turning_points_to_records,
)
from engine.events.detector import detect_events
from engine.fetchers.slv import generate_slv_series
from engine.utils.io import ensure_parent, write_json

BASE_PATH = Path("public/data")


def run_pipeline() -> None:
    raw_data = generate_slv_series()
    closes = [row["close"] for row in raw_data]
    volumes = [row["volume"] for row in raw_data]
    dates = [row["date"] for row in raw_data]

    latest_events = detect_events(raw_data)
    cycles, turning_points = detect_cycles(raw_data)
    cycle_stats = summarize_cycles(cycles)
    signal = select_action(latest_events, cycles)

    now = datetime.utcnow().isoformat()
    history_record = {"timestamp": now, **signal}

    perf_summary = summarize_returns(closes)
    algo_score = compute_algorithm_score(closes, cycles)
    equity_curve = compute_equity_curve(closes)
    breakdown = event_breakdown([event.name for event in latest_events], closes)
    rsi_series = attach_dates(compute_rsi(closes), dates)
    stddev_series = attach_dates(compute_rolling_stddev(closes), dates)
    macd_series = attach_dates(compute_macd(closes), dates)
    bollinger_series = attach_dates(compute_bollinger_bands(closes), dates)
    obv_series = attach_dates(compute_obv(closes, volumes), dates)
    ma1000_series = attach_dates(compute_moving_average(closes, window=1000), dates)
    decomposition = decompose_closes(closes, period=30)

    write_json(BASE_PATH / "raw/slv_daily.json", {"symbol": "SLV", "data": raw_data})
    write_json(BASE_PATH / "events/latest.json", {"as_of": now, "events": [e.to_dict() for e in latest_events]})
    write_json(
        BASE_PATH / "events/cycle_stats.json",
        {
            "updated_at": now,
            "cycles": [cycle.to_dict() for cycle in cycles],
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
    write_json(BASE_PATH / "perf/equity_curve.json", {"updated_at": now, "equity_curve": equity_curve})
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
