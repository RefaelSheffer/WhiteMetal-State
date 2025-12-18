from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Mapping, Sequence

from engine.utils.io import ensure_parent


@dataclass
class TradeSettings:
    strategy_id: str = "cycle_basic"
    cost_bps: int = 10
    slippage_bps: int = 5
    max_positions: int = 1
    cooldown_days: int = 3
    adx_min_soft: float = 12.0
    atr_pct_min_soft: float = 0.006
    target_daily_vol: float = 0.01
    min_position: float = 0.25
    max_position: float = 1.0
    stop_atr_multiple: float = 2.5
    time_stop_days: int = 45


def _indicator_lookup(series: Iterable[Mapping], key: str) -> Mapping[int, float]:
    return {entry["index"]: float(entry.get(key, 0.0)) for entry in series if entry.get(key) is not None}


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _soft_position_size(adx: float | None, atr: float | None, close: float, settings: TradeSettings) -> float:
    size = settings.max_position

    if adx is not None and adx < settings.adx_min_soft:
        size *= 0.5

    atr_pct = (atr / close) if atr and close else None
    if atr_pct is not None and atr_pct < settings.atr_pct_min_soft:
        size *= 0.5

    if atr_pct and atr_pct > settings.target_daily_vol:
        size *= 0.9

    return _clamp(size, settings.min_position, settings.max_position)


def _max_drawdown(equity_path: Sequence[float]) -> float:
    peak = equity_path[0] if equity_path else 1.0
    max_dd = 0.0
    for value in equity_path:
        if value > peak:
            peak = value
        drawdown = (value - peak) / peak if peak else 0.0
        max_dd = min(max_dd, drawdown)
    return max_dd


def _compute_ulcer_index(equity: Sequence[float]) -> float:
    if not equity:
        return 0.0
    peaks = []
    max_seen = equity[0]
    for value in equity:
        if value > max_seen:
            max_seen = value
        drawdown = max(0.0, (max_seen - value) / max_seen if max_seen else 0.0)
        peaks.append(drawdown ** 2)
    return (sum(peaks) / len(peaks)) ** 0.5 if peaks else 0.0


def _year_fraction(start: str, end: str) -> float:
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    delta_days = max((end_dt - start_dt).days, 1)
    return delta_days / 365.25


def trade_engine_cycle_basic(
    prices: Sequence[Mapping],
    turning_points: Sequence[Mapping],
    atr_series: Sequence[Mapping],
    adx_series: Sequence[Mapping],
    settings: TradeSettings | None = None,
) -> Mapping:
    if settings is None:
        settings = TradeSettings()

    if not prices:
        return {
            "meta": {},
            "trades": [],
            "equity_curves": [],
            "risk_metrics": {},
            "diagnostics": {},
        }

    closes = [row["close"] for row in prices]
    dates = [row["date"] for row in prices]

    atr_lookup = _indicator_lookup(atr_series, "atr")
    adx_lookup = _indicator_lookup(adx_series, "adx")

    cycle_events = {}
    for tp in turning_points:
        idx = tp.get("index") if isinstance(tp, Mapping) else getattr(tp, "index", None)
        kind = tp.get("kind") if isinstance(tp, Mapping) else getattr(tp, "kind", None)
        if idx is None or kind is None:
            continue
        cycle_events[idx] = str(kind).upper()

    trades: List[Mapping] = []
    diagnostics = {
        "cycle_events": {"trough_confirmed": 0, "peak_confirmed": 0},
        "blocked": {"cooldown": 0, "already_in_position": 0, "missing_exit": 0},
        "notes": ["No hard filters enabled (soft sizing only)."],
        "last_detected_trough": None,
        "last_detected_peak": None,
    }

    equity_strategy_gross = 100.0
    equity_strategy_net = 100.0
    equity_risk = 100.0
    buy_hold_base = closes[0] if closes else 1.0

    equity_rows: List[Mapping] = []

    in_position = False
    entry_idx = None
    entry_price = 0.0
    position_size = 0.0
    cooldown_until = -1
    cost_rate = (settings.cost_bps + settings.slippage_bps) / 10000

    max_close = 0.0
    min_close = 0.0
    equity_path: List[float] = []
    intraday_equity = 1.0

    for idx, close in enumerate(closes):
        event = cycle_events.get(idx)
        if event == "TROUGH":
            diagnostics["cycle_events"]["trough_confirmed"] += 1
            diagnostics["last_detected_trough"] = dates[idx]
        elif event == "PEAK":
            diagnostics["cycle_events"]["peak_confirmed"] += 1
            diagnostics["last_detected_peak"] = dates[idx]

        daily_change = 0.0
        if idx > 0 and closes[idx - 1]:
            daily_change = (close - closes[idx - 1]) / closes[idx - 1]

        if in_position:
            equity_strategy_gross *= 1 + (daily_change * position_size)
            equity_strategy_net *= 1 + (daily_change * position_size)
            equity_risk *= 1 + (daily_change * position_size)

            if close > max_close:
                max_close = close
            if close < min_close:
                min_close = close

            intraday_equity *= 1 + (daily_change * position_size)
            equity_path.append(intraday_equity)

            exit_reason = None
            if event == "PEAK":
                exit_reason = "PEAK_CONFIRMED"
            elif idx - entry_idx >= settings.time_stop_days:
                exit_reason = "TIME_STOP"
            elif atr_lookup.get(entry_idx) and close <= entry_price - (settings.stop_atr_multiple * atr_lookup[entry_idx]):
                exit_reason = "STOP_ATR"

            if exit_reason:
                gross_return = (close - entry_price) / entry_price if entry_price else 0.0
                net_return = (gross_return * position_size) - (cost_rate * 2 * position_size)
                hold_days = idx - entry_idx
                mfe = (max_close - entry_price) / entry_price if entry_price else 0.0
                mae = (min_close - entry_price) / entry_price if entry_price else 0.0
                max_dd_trade = _max_drawdown(equity_path)

                equity_strategy_net *= 1 - (cost_rate * position_size)
                equity_risk *= 1 - (cost_rate * position_size)

                trades.append(
                    {
                        "id": len(trades) + 1,
                        "entry_date": dates[entry_idx],
                        "entry_price": round(entry_price, 4),
                        "entry_reason": "TROUGH_CONFIRMED",
                        "size": round(position_size, 3),
                        "exit_date": dates[idx],
                        "exit_price": round(close, 4),
                        "exit_reason": exit_reason,
                        "gross_return": round(gross_return, 4),
                        "net_return": round(net_return, 4),
                        "hold_days": hold_days,
                        "mfe": round(mfe, 4),
                        "mae": round(mae, 4),
                        "max_drawdown_trade": round(max_dd_trade, 4),
                    }
                )

                in_position = False
                entry_idx = None
                position_size = 0.0
                cooldown_until = idx + settings.cooldown_days
                max_close = 0.0
                min_close = 0.0
                equity_path = []
                intraday_equity = 1.0

        if not in_position and event == "TROUGH":
            if idx < cooldown_until:
                diagnostics["blocked"]["cooldown"] += 1
            else:
                adx_value = adx_lookup.get(idx)
                atr_value = atr_lookup.get(idx)
                size = _soft_position_size(adx_value, atr_value, close, settings)
                in_position = True
                entry_idx = idx
                entry_price = close
                position_size = size
                max_close = close
                min_close = close
                intraday_equity = 1.0
                equity_strategy_net *= 1 - (cost_rate * position_size)
                equity_risk *= 1 - (cost_rate * position_size)
                equity_path = [1.0]
        elif in_position and event == "TROUGH":
            diagnostics["blocked"]["already_in_position"] += 1

        buy_hold_value = 100.0 * (close / buy_hold_base) if buy_hold_base else 100.0
        equity_rows.append(
            {
                "date": dates[idx],
                "buy_hold": round(buy_hold_value, 4),
                "strategy_gross": round(equity_strategy_gross, 4),
                "strategy_net": round(equity_strategy_net, 4),
                "risk_managed": round(equity_risk, 4),
            }
        )

    if in_position and entry_idx is not None:
        diagnostics["blocked"]["missing_exit"] += 1
        gross_return = (closes[-1] - entry_price) / entry_price if entry_price else 0.0
        net_return = (gross_return * position_size) - (cost_rate * 2 * position_size)
        hold_days = len(closes) - entry_idx - 1
        mfe = (max_close - entry_price) / entry_price if entry_price else 0.0
        mae = (min_close - entry_price) / entry_price if entry_price else 0.0
        max_dd_trade = _max_drawdown(equity_path)

        trades.append(
            {
                "id": len(trades) + 1,
                "entry_date": dates[entry_idx],
                "entry_price": round(entry_price, 4),
                "entry_reason": "TROUGH_CONFIRMED",
                "size": round(position_size, 3),
                "exit_date": dates[-1],
                "exit_price": round(closes[-1], 4),
                "exit_reason": "FORCED_EXIT_END_OF_DATA",
                "gross_return": round(gross_return, 4),
                "net_return": round(net_return, 4),
                "hold_days": hold_days,
                "mfe": round(mfe, 4),
                "mae": round(mae, 4),
                "max_drawdown_trade": round(max_dd_trade, 4),
            }
        )

    equity_values = [row["strategy_net"] for row in equity_rows]
    total_return_net = (equity_values[-1] / equity_values[0] - 1) if equity_values else 0.0
    years = _year_fraction(dates[0], dates[-1]) if dates else 1.0
    cagr = ((equity_values[-1] / equity_values[0]) ** (1 / years) - 1) if equity_values else 0.0

    returns = []
    for idx in range(1, len(equity_values)):
        prior = equity_values[idx - 1]
        if prior:
            returns.append((equity_values[idx] - prior) / prior)

    sharpe = 0.0
    if returns:
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        stddev = variance ** 0.5
        sharpe = mean_ret / stddev if stddev else 0.0

    risk_metrics = {
        "total_return_net": round(total_return_net, 4),
        "cagr_net": round(cagr, 4),
        "max_drawdown": round(_max_drawdown(equity_values), 4),
        "sharpe": round(sharpe, 4),
        "ulcer_index": round(_compute_ulcer_index(equity_values), 4),
        "win_rate": round(
            len([t for t in trades if t.get("net_return", 0) > 0]) / len(trades),
            4,
        ) if trades else 0.0,
        "avg_trade_net": round(sum(t.get("net_return", 0) for t in trades) / len(trades), 4)
        if trades
        else 0.0,
        "num_trades": len(trades),
        "avg_hold_days": round(sum(t.get("hold_days", 0) for t in trades) / len(trades), 2)
        if trades
        else 0.0,
    }

    by_year: List[Mapping] = []
    if trades:
        yearly: dict[int, List[float]] = {}
        for trade in trades:
            year = datetime.fromisoformat(trade["exit_date"]).year
            yearly.setdefault(year, []).append(trade.get("net_return", 0.0))
        for year in sorted(yearly.keys()):
            compounded = 1.0
            for ret in yearly[year]:
                compounded *= 1 + ret
            by_year.append(
                {
                    "year": year,
                    "return_net": round(compounded - 1, 4),
                    "max_dd": 0.0,
                }
            )

    equity_payload = {
        "meta": {"asof": dates[-1], "base": "100", "net": "includes costs"},
        "rows": equity_rows,
    }

    risk_payload = {
        "meta": {"asof": dates[-1], "strategy": settings.strategy_id},
        "summary": risk_metrics,
        "by_year": by_year,
    }

    trade_log = {
        "meta": {
            "asof": dates[-1],
            "asset": "SLV",
            "strategy": settings.strategy_id,
            "cost_bps": settings.cost_bps,
            "slippage_bps": settings.slippage_bps,
            "cooldown_days": settings.cooldown_days,
        },
        "trades": trades,
    }

    diagnostics["range"] = {"start": dates[0], "end": dates[-1]}

    diagnostics_payload = {"asof": dates[-1], **diagnostics}

    return {
        "trade_log": trade_log,
        "equity_curves": equity_payload,
        "risk_metrics": risk_payload,
        "diagnostics": diagnostics_payload,
    }


def write_backtest_outputs(base_path, outputs: Mapping) -> None:
    ensure_parent(base_path / "trade_log.json").write_text(
        __import__("json").dumps(outputs["trade_log"], indent=2)
    )
    ensure_parent(base_path / "equity_curves.json").write_text(
        __import__("json").dumps(outputs["equity_curves"], indent=2)
    )
    ensure_parent(base_path / "risk_metrics.json").write_text(
        __import__("json").dumps(outputs["risk_metrics"], indent=2)
    )
    ensure_parent(base_path / "trade_diagnostics.json").write_text(
        __import__("json").dumps(outputs["diagnostics"], indent=2)
    )
