from __future__ import annotations

from datetime import datetime
from pathlib import Path

from engine.backtest.performance import (
    DEFAULT_TRADING_COSTS,
    RiskManagementConfig,
    attach_dates,
    compute_adx,
    compute_algorithm_score,
    compute_atr,
    compute_bollinger_bands,
    compute_buy_and_hold_equity,
    compute_equity_curve,
    compute_macd,
    compute_moving_average,
    compute_obv,
    compute_performance_stats,
    compute_risk_managed_equity,
    compute_rolling_stddev,
    compute_rsi,
    decompose_closes,
    event_breakdown,
    summarize_returns,
)
from engine.anomalies.detector import compute_regime, detect_anomalies
from engine.context import fetch_context_assets, write_context_outputs
from engine.backtest.trade_engine import (
    TradeSettings,
    trade_engine_cycle_basic,
    write_backtest_outputs,
)
from engine.diagnostics.decomposition import DecompositionConfig, write_decomposition_outputs
from engine.events.calendar import (
    align_events_to_history,
    compute_current_event_context,
    compute_event_impact_stats,
    load_events_calendar,
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
from engine.heatmap import (
    compute_deviation_heatmap,
    compute_momentum_heatmap,
    compute_stats_by_band,
    compute_volatility_heatmap,
)
from engine.probabilistic import build_probabilistic_signal
from engine.utils.io import ensure_parent, write_json
from engine.validation.sanity import validate_ohlcv

BASE_PATH = Path("public/data")


def run_pipeline() -> None:
    source = "stooq"
    raw_data = fetch_slv_ohlcv(
        start_date="2008-01-01",
        cache_path=str(BASE_PATH / "raw/slv_daily.json"),
        source=source,
    )
    aux_assets, context_meta = fetch_context_assets(start_date="2008-01-01", source=source)
    validate_ohlcv(raw_data)
    closes = [row["close"] for row in raw_data]
    opens = [row["open"] for row in raw_data]
    highs = [row["high"] for row in raw_data]
    lows = [row["low"] for row in raw_data]
    volumes = [row["volume"] for row in raw_data]
    dates = [row["date"] for row in raw_data]

    write_context_outputs(
        raw_data,
        gld_rows=aux_assets["GLD"],
        dxy_rows=aux_assets["DXY"],
        us10y_rows=aux_assets["US10Y"],
        source=source,
        meta=context_meta,
    )

    calendar_path = Path("data/events_calendar.json")
    known_events, calendar_meta = load_events_calendar(calendar_path)
    aligned_events = align_events_to_history(known_events, dates)

    event_timeline = []
    for idx in range(len(raw_data)):
        daily_events = detect_events(raw_data[: idx + 1])
        for event in daily_events:
            event_timeline.append({"name": event.name, "index": idx})

    latest_events = [event for event in detect_events(raw_data)]
    cycles, turning_points = detect_cycles(raw_data)
    filtered_cycles = filter_cycles(cycles, min_length=2)
    cycle_stats = summarize_cycles(filtered_cycles)

    perf_summary = summarize_returns(closes)
    algo_score = compute_algorithm_score(closes, filtered_cycles)
    equity_curve = compute_equity_curve(
        closes, opens=opens, costs=DEFAULT_TRADING_COSTS, turnover=1.0
    )
    buy_and_hold_curve = compute_buy_and_hold_equity(closes)
    risk_config = RiskManagementConfig()
    atr_raw = compute_atr(highs, lows, closes, window=risk_config.atr_window)
    risk_managed_curve = compute_risk_managed_equity(
        closes,
        highs,
        lows,
        opens=opens,
        costs=DEFAULT_TRADING_COSTS,
        config=risk_config,
    )
    strategy_stats = compute_performance_stats(equity_curve)
    risk_managed_stats = compute_performance_stats(risk_managed_curve)
    buy_and_hold_stats = compute_performance_stats(buy_and_hold_curve)
    breakdown = event_breakdown(event_timeline, closes)
    rsi_raw = compute_rsi(closes)
    macd_raw = compute_macd(closes)
    adx_raw = compute_adx(highs, lows, closes)
    turning_point_records = [
        {"index": tp.index, "kind": tp.kind} for tp in turning_points
    ]
    trade_settings = TradeSettings(strategy_id="cycle_basic")
    trade_outputs = trade_engine_cycle_basic(
        raw_data,
        turning_point_records,
        atr_raw,
        adx_raw,
        settings=trade_settings,
    )
    bollinger_raw = compute_bollinger_bands(closes)
    obv_raw = compute_obv(closes, volumes)
    rsi_series = attach_dates(rsi_raw, dates)
    stddev_series = attach_dates(compute_rolling_stddev(closes), dates)
    atr_series = attach_dates(atr_raw, dates)
    macd_series = attach_dates(macd_raw, dates)
    bollinger_series = attach_dates(bollinger_raw, dates)
    obv_series = attach_dates(obv_raw, dates)
    adx_series = attach_dates(adx_raw, dates)
    ma1000_series = attach_dates(compute_moving_average(closes, window=1000), dates)
    decomposition = decompose_closes(closes, period=21)

    indicator_context = build_indicator_context(rsi_raw, macd_raw, adx_raw)
    signal = select_action(
        latest_events, filtered_cycles, indicator_context=indicator_context
    )

    regime = compute_regime(closes, adx_raw, atr_raw)
    anomalies = detect_anomalies(
        closes,
        bollinger_raw,
        atr_raw,
        adx_raw,
        regime,
        dates=dates,
    )
    probabilistic_signal = build_probabilistic_signal(
        closes,
        dates,
        regime,
        bollinger_raw,
        rsi_raw,
        macd_raw,
        adx_raw,
        atr_raw,
        symbol="SLV",
    )
    event_impact_stats = compute_event_impact_stats(
        closes,
        highs,
        lows,
        atr_raw,
        aligned_events,
        as_of=dates[-1],
        asset="SLV",
    )
    event_context = compute_current_event_context(
        occurrences=aligned_events, stats=event_impact_stats, as_of=dates[-1]
    )
    now = datetime.utcnow().isoformat()
    last_updated = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    history_record = {"timestamp": now, **signal}

    deviation_payload = compute_deviation_heatmap(closes, dates)
    volatility_payload = compute_volatility_heatmap(highs, lows, closes, dates)
    momentum_payload = compute_momentum_heatmap(closes, dates)
    stats_by_band = compute_stats_by_band(closes, deviation_payload["bands"])

    write_json(
        BASE_PATH / "meta.json",
        {
            "source": source,
            "symbol": "SLV",
            "start": raw_data[0]["date"],
            "end": raw_data[-1]["date"],
            "rows": len(raw_data),
            "last_updated_utc": last_updated,
        },
    )

    write_json(BASE_PATH / "anomalies/latest.json", anomalies)

    write_json(BASE_PATH / "events/latest.json", {"as_of": now, "events": [e.to_dict() for e in latest_events]})
    write_json(
        BASE_PATH / "events/calendar.json",
        {
            "updated_at": now,
            "version": calendar_meta.get("version"),
            "timezone": calendar_meta.get("timezone", "UTC"),
            "events": [e.to_dict() for e in known_events],
        },
    )
    write_json(BASE_PATH / "events/event_impact_stats.json", event_impact_stats)
    write_json(BASE_PATH / "events/current_event_context.json", event_context)
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
    write_json(BASE_PATH / "signals/probabilistic.json", probabilistic_signal)

    events_history_path = BASE_PATH / "events/history.jsonl"
    anomaly_history_path = BASE_PATH / "anomalies/history.jsonl"
    signal_history_path = BASE_PATH / "signals/signal_history.jsonl"

    append_jsonl(events_history_path, {"timestamp": now, "events": [e.to_dict() for e in latest_events]})
    append_jsonl(anomaly_history_path, {"timestamp": now, **anomalies})
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
            "risk_managed_curve": risk_managed_curve,
            "buy_and_hold_curve": buy_and_hold_curve,
            "performance": {
                "strategy": strategy_stats,
                "risk_managed": risk_managed_stats,
                "buy_and_hold": buy_and_hold_stats,
            },
            "trading_costs": {
                "commission_pct": DEFAULT_TRADING_COSTS.commission_pct,
                "slippage_pct": DEFAULT_TRADING_COSTS.slippage_pct,
                "turnover": 1.0,
                "execution": "next_open_to_close",
            },
            "risk_management": {
                "atr_window": risk_config.atr_window,
                "stop_loss_atr_multiple": risk_config.stop_loss_atr_multiple,
                "take_profit_atr_multiple": risk_config.take_profit_atr_multiple,
                "risk_fraction_per_trade": risk_config.risk_fraction_per_trade,
                "max_position": risk_config.max_position,
            },
        },
    )
    write_json(BASE_PATH / "perf/by_event.json", {"updated_at": now, "breakdown": breakdown})
    write_json(
        BASE_PATH / "perf/rsi.json",
        {"updated_at": now, "period": 14, "rsi": rsi_series},
    )
    write_json(
        BASE_PATH / "perf/atr.json",
        {"updated_at": now, "window": risk_config.atr_window, "atr": atr_series},
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
        BASE_PATH / "perf/adx.json",
        {"updated_at": now, "period": 14, "adx": adx_series},
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
            "period": 21,
            "trend": decomposition["trend"],
            "seasonal": decomposition["seasonal"],
            "resid": decomposition["resid"],
        },
    )
    write_json(BASE_PATH / "heatmap/deviation.json", deviation_payload)
    write_json(BASE_PATH / "heatmap/volatility.json", volatility_payload)
    write_json(BASE_PATH / "heatmap/momentum.json", momentum_payload)
    write_json(BASE_PATH / "heatmap/stats_by_band.json", stats_by_band)
    write_backtest_outputs(BASE_PATH / "backtest", trade_outputs)

    write_decomposition_outputs(
        dates,
        closes,
        output_dir=BASE_PATH / "diagnostics",
        config=DecompositionConfig(period_mode="monthly", robust=True),
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
