from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Iterable, List, Mapping, Sequence

from engine.backtest.performance import compute_moving_average


@dataclass
class Regime:
    trend_state: str
    vol_state: str
    adx: float
    atr: float
    ma20: float | None
    ma50: float | None
    ma200: float | None

    def to_dict(self) -> Mapping:
        return {
            "trend_state": self.trend_state,
            "vol_state": self.vol_state,
            "adx": round(self.adx, 2) if self.adx is not None else None,
            "atr": round(self.atr, 4) if self.atr is not None else None,
            "ma20": round(self.ma20, 4) if self.ma20 is not None else None,
            "ma50": round(self.ma50, 4) if self.ma50 is not None else None,
            "ma200": round(self.ma200, 4) if self.ma200 is not None else None,
        }


def compute_regime(closes: Sequence[float], adx_raw: Sequence[Mapping], atr_raw: Sequence[Mapping]) -> Regime:
    if not closes:
        raise ValueError("Cannot compute regime without closes")

    ma20 = compute_moving_average(closes, window=20)
    ma50 = compute_moving_average(closes, window=50)
    ma200 = compute_moving_average(closes, window=200)

    latest_close = closes[-1]
    latest_ma20 = ma20[-1]["ma"] if ma20 else None
    latest_ma50 = ma50[-1]["ma"] if ma50 else None
    latest_ma200 = ma200[-1]["ma"] if ma200 else None

    latest_adx = adx_raw[-1]["adx"] if adx_raw else 0.0
    latest_atr = atr_raw[-1]["atr"] if atr_raw else 0.0

    trend_state = "MIXED"
    if latest_adx >= 25 and latest_ma50 is not None and latest_ma200 is not None:
        trend_state = "TREND"
    elif latest_adx < 15:
        trend_state = "RANGE"

    vol_state = "NORMAL"
    if atr_raw:
        tail = [row["atr"] for row in atr_raw[-100:] if row.get("atr") is not None]
        avg_atr = sum(tail) / len(tail) if tail else latest_atr or 0.0
        ratio = latest_atr / avg_atr if avg_atr else 0.0
        if ratio >= 1.5:
            vol_state = "HIGH"
        elif ratio <= 0.8:
            vol_state = "LOW"

    if latest_ma50 is not None and latest_ma200 is not None:
        if latest_close < latest_ma50 < latest_ma200:
            trend_state = "DOWN_BIAS"
        elif latest_close > latest_ma50 > latest_ma200:
            trend_state = "UP_BIAS"

    return Regime(trend_state, vol_state, latest_adx, latest_atr, latest_ma20, latest_ma50, latest_ma200)


def _forward_return_stats(indices: Iterable[int], closes: Sequence[float], horizon: int = 5) -> Mapping:
    returns: List[float] = []
    for idx in indices:
        if idx + horizon < len(closes):
            start = closes[idx]
            end = closes[idx + horizon]
            if start:
                returns.append(end / start - 1)
    if not returns:
        return {"window_days": horizon, "occurrences": 0, "median_forward_return": None, "p_positive": None}

    positives = len([r for r in returns if r > 0])
    return {
        "window_days": horizon,
        "occurrences": len(returns),
        "median_forward_return": round(median(returns), 4),
        "p_positive": round(positives / len(returns), 2),
    }


def detect_anomalies(
    closes: Sequence[float],
    bollinger_raw: Sequence[Mapping],
    atr_raw: Sequence[Mapping],
    adx_raw: Sequence[Mapping],
    regime: Regime,
    dates: Sequence[str] | None = None,
) -> Mapping:
    alerts: List[Mapping] = []
    anomaly_score = 0
    latest_close = closes[-1]
    latest_date = dates[-1] if dates else None

    # Bollinger breakout
    if bollinger_raw:
        last_bb = bollinger_raw[-1]
        mid = last_bb.get("middle")
        upper = last_bb.get("upper")
        lower = last_bb.get("lower")
        if mid and upper and lower:
            std_est = (upper - mid) / 2
            z = (latest_close - mid) / std_est if std_est else 0.0
            severity = None
            if abs(z) >= 3:
                severity = "CRITICAL"
                anomaly_score += 35
            elif abs(z) >= 2:
                severity = "WARN"
                anomaly_score += 20
            if severity:
                direction = "UP" if z > 0 else "DOWN"
                zscores = []
                bb_indices = []
                for idx, band in enumerate(bollinger_raw[:-1]):
                    mid_i = band.get("middle")
                    upper_i = band.get("upper")
                    if not mid_i or not upper_i:
                        continue
                    std_i = (upper_i - mid_i) / 2
                    if std_i:
                        zscores.append((closes[idx] - mid_i) / std_i)
                        bb_indices.append(idx)
                context_stats = _forward_return_stats(
                    [i for i, zval in zip(bb_indices, zscores) if abs(zval) >= 2], closes, horizon=5
                )
                alerts.append(
                    {
                        "id": "BOLL_BREAKOUT_UP" if direction == "UP" else "BOLL_BREAKOUT_DOWN",
                        "severity": severity,
                        "direction": direction,
                        "why": "Close moved outside Bollinger band (20,2).",
                        "evidence": {
                            "close": round(latest_close, 4),
                            "mid": round(mid, 4),
                            "upper": round(upper, 4),
                            "lower": round(lower, 4),
                            "z": round(z, 2),
                        },
                        "historical_context": {
                            **context_stats,
                            "label": "Bollinger breakout z>=2",
                        },
                    }
                )

    # ATR spike
    if atr_raw:
        latest_atr = atr_raw[-1]["atr"]
        tail = [row["atr"] for row in atr_raw[-30:] if row.get("atr") is not None]
        avg_atr = sum(tail) / len(tail) if tail else latest_atr or 0.0
        ratio = latest_atr / avg_atr if avg_atr else 0.0
        severity = None
        if ratio >= 2:
            severity = "CRITICAL"
            anomaly_score += 30
        elif ratio >= 1.6:
            severity = "WARN"
            anomaly_score += 18
        elif ratio >= 1.3:
            severity = "INFO"
            anomaly_score += 10
        if severity:
            alerts.append(
                {
                    "id": "ATR_SPIKE",
                    "severity": severity,
                    "direction": "NEUTRAL",
                    "why": "Daily volatility jumped versus 30-day average (ATR).",
                    "evidence": {
                        "atr": round(latest_atr, 4),
                        "atr_avg": round(avg_atr, 4),
                        "ratio": round(ratio, 2),
                    },
                    "historical_context": _forward_return_stats(
                        [i for i, row in enumerate(atr_raw[:-1]) if row.get("atr") and avg_atr and row["atr"] / avg_atr >= 1.6],
                        closes,
                        horizon=5,
                    ),
                }
            )

    # ADX regime shift
    if len(adx_raw) >= 2:
        prev_adx = adx_raw[-2]["adx"]
        latest_adx = adx_raw[-1]["adx"]
        crossed = prev_adx < 20 <= latest_adx
        slope = latest_adx - prev_adx
        if crossed or slope >= 5:
            severity = "INFO" if latest_adx < 25 else "WARN"
            anomaly_score += 12 if severity == "INFO" else 18
            alerts.append(
                {
                    "id": "ADX_REGIME_SHIFT",
                    "severity": severity,
                    "direction": "NEUTRAL",
                    "why": "ADX inflected higher, indicating a trend regime change.",
                    "evidence": {"previous": round(prev_adx, 2), "current": round(latest_adx, 2), "delta": round(slope, 2)},
                    "historical_context": _forward_return_stats(
                        [i for i in range(1, len(adx_raw) - 1) if adx_raw[i - 1]["adx"] < 20 <= adx_raw[i]["adx"]],
                        closes,
                        horizon=10,
                    ),
                }
            )

    # Bollinger width squeeze
    if bollinger_raw:
        widths: List[float] = []
        for band in bollinger_raw:
            mid = band.get("middle")
            upper = band.get("upper")
            lower = band.get("lower")
            if mid:
                width = (upper - lower) / mid
                widths.append(width)
        if widths:
            latest_width = widths[-1]
            sorted_widths = sorted(widths[:-1] or widths)
            rank = sum(1 for w in sorted_widths if w <= latest_width) / len(sorted_widths)
            if rank <= 0.1:
                alerts.append(
                    {
                        "id": "BOLL_SQUEEZE",
                        "severity": "INFO",
                        "direction": "NEUTRAL",
                        "why": "Bollinger band width is in the bottom decile (volatility squeeze).",
                        "evidence": {
                            "band_width": round(latest_width, 4),
                            "percentile": round(rank * 100, 1),
                        },
                        "historical_context": _forward_return_stats(
                            [i for i, w in enumerate(widths[:-1]) if w <= sorted_widths[int(0.1 * len(sorted_widths))]],
                            closes,
                            horizon=20,
                        ),
                    }
                )
                anomaly_score += 8

    anomaly_score = min(100, anomaly_score)

    return {
        "asof": latest_date,
        "symbol": "SLV",
        "regime": regime.to_dict(),
        "alerts": alerts,
        "anomaly_score": anomaly_score,
        "price": {
            "last_close": round(latest_close, 4),
            "ma20": regime.ma20,
            "ma50": regime.ma50,
            "ma200": regime.ma200,
        },
    }
