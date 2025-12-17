from __future__ import annotations

from dataclasses import dataclass
from typing import List, Mapping, Sequence

import numpy as np

from engine.anomalies.detector import Regime
from engine.backtest.performance import compute_moving_average


def _series_to_array(series: Sequence[Mapping], key: str, length: int) -> List[float | None]:
    values: List[float | None] = [None] * length
    for row in series:
        idx = row.get("index")
        if idx is None or idx >= length:
            continue
        values[idx] = row.get(key)
    return values


def _volatility_state(atr_values: Sequence[float | None], idx: int) -> str:
    if idx >= len(atr_values) or atr_values[idx] is None:
        return "NORMAL"

    start = max(0, idx - 99)
    window = [v for v in atr_values[start : idx + 1] if v is not None]
    if not window:
        return "NORMAL"

    avg_atr = sum(window) / len(window)
    ratio = atr_values[idx] / avg_atr if avg_atr else 0.0
    if ratio >= 1.5:
        return "HIGH"
    if ratio <= 0.8:
        return "LOW"
    return "NORMAL"


def _regime_state(adx: float | None, close: float, ma50: float | None, ma200: float | None) -> str:
    if adx is None:
        return "MIXED"

    regime = "MIXED"
    if adx >= 25 and ma50 is not None and ma200 is not None:
        regime = "TREND"
    elif adx < 15:
        regime = "RANGE"

    if ma50 is not None and ma200 is not None:
        if close > ma50 > ma200:
            regime = "TREND"
        elif close < ma50 < ma200:
            regime = "RANGE"

    return regime


def _bias_from_mas(close: float, ma50: float | None, ma200: float | None) -> str:
    if ma50 is None or ma200 is None:
        return "NEUTRAL"
    if close > ma50 > ma200:
        return "BULLISH"
    if close < ma50 < ma200:
        return "BEARISH"
    return "NEUTRAL"


def _bollinger_position(close: float, upper: float | None, lower: float | None) -> str:
    if upper is None or lower is None:
        return "INSIDE"
    if close > upper:
        return "ABOVE_UPPER"
    if close < lower:
        return "BELOW_LOWER"
    return "INSIDE"


def _rsi_bucket(rsi: float | None) -> str:
    if rsi is None:
        return "RSI_UNKNOWN"
    if rsi < 30:
        return "RSI_LT_30"
    if rsi > 70:
        return "RSI_GT_70"
    return "RSI_30_70"


def _macd_momentum(hist_values: Sequence[float | None], idx: int) -> str:
    if idx >= len(hist_values) or hist_values[idx] is None:
        return "FLAT"

    current = hist_values[idx]
    prev = None
    for back in range(1, 4):
        if idx - back >= 0 and hist_values[idx - back] is not None:
            prev = hist_values[idx - back]
            break

    if prev is None:
        prev = current

    delta = current - prev
    if abs(delta) < 0.001:
        return "FLAT"
    if current >= 0 and delta > 0:
        return "IMPROVING"
    if current <= 0 and delta < 0:
        return "WORSENING"
    return "FLAT"


def build_scenario_id(context: Mapping[str, str]) -> str:
    parts = [
        context.get("regime", "UNKNOWN"),
        context.get("volatility", "UNKNOWN"),
        context.get("bollinger", "UNKNOWN"),
        context.get("rsi", "UNKNOWN"),
        context.get("macd", "UNKNOWN"),
    ]
    return "|".join(parts)


@dataclass
class ScenarioFrame:
    closes: List[float]
    dates: List[str]
    scenario_ids: List[str]
    ma50: List[float | None]
    ma200: List[float | None]


def _prepare_scenarios(
    closes: Sequence[float],
    dates: Sequence[str],
    bollinger_raw: Sequence[Mapping],
    rsi_raw: Sequence[Mapping],
    macd_raw: Sequence[Mapping],
    adx_raw: Sequence[Mapping],
    atr_raw: Sequence[Mapping],
) -> ScenarioFrame:
    length = len(closes)
    ma50_series = compute_moving_average(closes, window=50)
    ma200_series = compute_moving_average(closes, window=200)

    boll_upper = _series_to_array(bollinger_raw, "upper", length)
    boll_lower = _series_to_array(bollinger_raw, "lower", length)
    rsi_series = _series_to_array(rsi_raw, "rsi", length)
    macd_hist = _series_to_array(macd_raw, "hist", length)
    adx_series = _series_to_array(adx_raw, "adx", length)
    atr_series = _series_to_array(atr_raw, "atr", length)
    ma50 = _series_to_array(ma50_series, "ma", length)
    ma200 = _series_to_array(ma200_series, "ma", length)

    scenario_ids: List[str] = []
    for idx, close in enumerate(closes):
        vol_state = _volatility_state(atr_series, idx)
        regime_state = _regime_state(adx_series[idx], close, ma50[idx], ma200[idx])
        boll_pos = _bollinger_position(close, boll_upper[idx], boll_lower[idx])
        rsi_bucket = _rsi_bucket(rsi_series[idx])
        macd_momentum = _macd_momentum(macd_hist, idx)

        context = {
            "regime": regime_state,
            "volatility": vol_state,
            "bollinger": boll_pos,
            "rsi": rsi_bucket,
            "macd": macd_momentum,
        }
        scenario_ids.append(build_scenario_id(context))

    return ScenarioFrame(list(closes), list(dates), scenario_ids, ma50, ma200)


def compute_historical_outcomes(
    closes: Sequence[float], scenario_ids: Sequence[str], target_scenario_id: str, horizon: int = 5
) -> Mapping:
    forward_returns = []
    indices: List[int] = []

    for idx in range(len(closes)):
        if scenario_ids[idx] != target_scenario_id:
            continue
        if idx + horizon >= len(closes):
            continue
        start = closes[idx]
        end = closes[idx + horizon]
        if start:
            indices.append(idx)
            forward_returns.append(end / start - 1)

    if not forward_returns:
        return {
            "horizon_days": horizon,
            "occurrences": 0,
            "p_up": None,
            "p_down": None,
            "median_return": None,
            "p10_return": None,
            "p90_return": None,
            "matches": [],
        }

    returns_array = np.array(forward_returns)
    p_up = float((returns_array > 0).mean())
    return {
        "horizon_days": horizon,
        "occurrences": len(forward_returns),
        "p_up": round(p_up, 2),
        "p_down": round(1 - p_up, 2),
        "median_return": round(float(np.median(returns_array)), 4),
        "p10_return": round(float(np.percentile(returns_array, 10)), 4),
        "p90_return": round(float(np.percentile(returns_array, 90)), 4),
        "matches": indices,
    }


def phrase_from_outcomes(outcomes: Mapping, regime: Regime) -> str:
    p_up = outcomes.get("p_up")
    median = outcomes.get("median_return")
    vol_state = regime.vol_state if hasattr(regime, "vol_state") else None

    if p_up is None or median is None:
        return "נתונים היסטוריים מוגבלים; לא זוהה דפוס חזק"

    if p_up >= 0.65 and median > 0:
        base = "סביר שמתחיל גל עליות קצר-טווח"
    elif p_up <= 0.35 and median < 0:
        base = "קיים סיכוי מוגבר להמשך ירידה קצר-טווח"
    else:
        base = "השוק במצב נייטרלי עם פיזור תוצאות רחב"

    if vol_state == "HIGH":
        base += "; בתנודתיות גבוהה התוצאות מפוזרות יותר מהרגיל"
    elif vol_state == "LOW":
        base += "; התנודתיות דחוסה מהרגיל"

    return base


def build_probabilistic_signal(
    closes: Sequence[float],
    dates: Sequence[str],
    regime: Regime,
    bollinger_raw: Sequence[Mapping],
    rsi_raw: Sequence[Mapping],
    macd_raw: Sequence[Mapping],
    adx_raw: Sequence[Mapping],
    atr_raw: Sequence[Mapping],
    symbol: str = "SLV",
) -> Mapping:
    scenarios = _prepare_scenarios(closes, dates, bollinger_raw, rsi_raw, macd_raw, adx_raw, atr_raw)
    latest_idx = len(closes) - 1
    latest_scenario_id = scenarios.scenario_ids[latest_idx]

    outcomes = compute_historical_outcomes(
        scenarios.closes, scenarios.scenario_ids, latest_scenario_id, horizon=5
    )
    headline = phrase_from_outcomes(outcomes, regime)
    occurrences = outcomes.get("occurrences") or 0
    confidence = int(round((outcomes.get("p_up") or 0.5) * 100))
    if occurrences < 30:
        confidence = max(0, confidence - 15)
    if regime.vol_state == "HIGH":
        confidence = max(0, confidence - 10)

    analogies = []
    for idx in outcomes.get("matches", [])[-5:]:
        if idx + 10 >= len(scenarios.closes):
            continue
        ret5 = scenarios.closes[idx + 5] / scenarios.closes[idx] - 1 if scenarios.closes[idx] else None
        ret10 = scenarios.closes[idx + 10] / scenarios.closes[idx] - 1 if scenarios.closes[idx] else None
        analogies.append(
            {
                "start_date": dates[idx],
                "similarity": 1.0,
                "forward_5d": round(ret5, 4) if ret5 is not None else None,
                "forward_10d": round(ret10, 4) if ret10 is not None else None,
            }
        )

    boll_row = bollinger_raw[-1] if bollinger_raw else {}
    evidence = []
    if boll_row:
        mid = boll_row.get("middle")
        upper = boll_row.get("upper")
        lower = boll_row.get("lower")
        std_est = (upper - mid) / 2 if mid and upper else None
        z = (closes[-1] - mid) / std_est if std_est else None
        if upper and closes[-1] > upper:
            evidence.append({"type": "BOLLINGER", "text": f"Close מעל Upper band (z={z:.1f})" if z else "Close מעל Upper band"})
        elif lower and closes[-1] < lower:
            evidence.append({"type": "BOLLINGER", "text": f"Close מתחת Lower band (z={z:.1f})" if z else "Close מתחת Lower band"})
    if adx_raw:
        adx_val = adx_raw[-1].get("adx")
        if adx_val is not None:
            evidence.append({"type": "ADX", "text": f"ADX {adx_val:.1f} (Regime {regime.trend_state})"})
    if atr_raw:
        evidence.append({"type": "ATR", "text": "ATR גבוה מהממוצע" if regime.vol_state == "HIGH" else "ATR בתחום נורמלי"})

    disclaimer_level = "HIGH" if occurrences < 30 else "STANDARD"
    subtext_suffix = "מספר מקרים היסטוריים דומה נמוך; ביטחון מוגבל." if disclaimer_level == "HIGH" else ""
    subtext = f"מבוסס על {occurrences} מקרים היסטוריים דומים מאז {dates[0][:4]}."
    if subtext_suffix:
        subtext = f"{subtext} {subtext_suffix}".strip()

    return {
        "asof": dates[-1],
        "symbol": symbol,
        "scenario_id": latest_scenario_id,
        "state": {
            "bias": _bias_from_mas(closes[-1], scenarios.ma50[-1], scenarios.ma200[-1]),
            "volatility": regime.vol_state,
            "regime": regime.trend_state,
        },
        "confidence": confidence,
        "message": {"headline": headline, "subtext": subtext},
        "historical_outcomes": outcomes,
        "evidence": evidence,
        "disclaimer_level": disclaimer_level,
        "analogs": analogies,
    }

