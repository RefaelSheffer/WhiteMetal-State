from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Iterable, Sequence

from engine.fetchers.ohlcv import fetch_ohlcv
from engine.utils.io import ensure_parent, write_json

BASE_CONTEXT_DIR = Path("public/data/context")


class ContextComputationError(RuntimeError):
    """Raised when required context assets cannot be fetched."""


def _percentile_rank(window: Sequence[float], value: float) -> float | None:
    if not window:
        return None
    count = sum(1 for v in window if v <= value)
    return (count / len(window)) * 100.0


def rolling_zscores(values: Sequence[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        window_vals = values[start : idx + 1]
        if len(window_vals) < max(30, window // 2):
            out.append(None)
            continue
        std = pstdev(window_vals)
        if std == 0:
            out.append(None)
            continue
        out.append((values[idx] - mean(window_vals)) / std)
    return out


def rolling_percentiles(values: Sequence[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        window_vals = values[start : idx + 1]
        if len(window_vals) < max(60, window // 2):
            out.append(None)
            continue
        pct = _percentile_rank(window_vals, values[idx])
        out.append(pct)
    return out


def context_bucket(pct: float | None, z: float | None) -> str | None:
    if pct is not None:
        if pct >= 98:
            return "EXTREME_HIGH"
        if pct >= 85:
            return "HIGH"
        if pct <= 2:
            return "EXTREME_LOW"
        if pct <= 15:
            return "LOW"
        return "NEUTRAL"
    if z is not None:
        if z >= 2.5:
            return "EXTREME_HIGH"
        if z >= 1.0:
            return "HIGH"
        if z <= -2.5:
            return "EXTREME_LOW"
        if z <= -1.0:
            return "LOW"
        return "NEUTRAL"
    return None


def _align_series(primary: list[tuple[str, float]], secondary: dict[str, float]) -> tuple[list[str], list[float], list[float]]:
    dates: list[str] = []
    primary_vals: list[float] = []
    secondary_vals: list[float] = []
    for date, pval in primary:
        if date in secondary:
            dates.append(date)
            primary_vals.append(pval)
            secondary_vals.append(secondary[date])
    return dates, primary_vals, secondary_vals


def _to_close_series(rows: Iterable[dict]) -> list[tuple[str, float]]:
    series = []
    for row in rows:
        if "close" not in row or "date" not in row:
            continue
        try:
            close = float(row["close"])
        except (TypeError, ValueError):
            continue
        series.append((row["date"], close))
    series.sort(key=lambda x: x[0])
    return series


def _write_raw_payload(symbol: str, rows: list[dict], source: str) -> None:
    if not rows:
        return
    ensure_parent(BASE_CONTEXT_DIR.parent / "raw" / f"{symbol}.json")
    payload = {
        "symbol": symbol,
        "source": source,
        "start": rows[0]["date"],
        "end": rows[-1]["date"],
        "last_updated_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "rows": rows,
    }
    write_json(BASE_CONTEXT_DIR.parent / "raw" / f"{symbol}.json", payload)


def _bucket_note(indicator: str, bucket: str | None) -> str:
    if indicator == "gsr":
        if bucket in {"HIGH", "EXTREME_HIGH"}:
            return "Relative: silver cheaper vs gold than usual"
        if bucket in {"LOW", "EXTREME_LOW"}:
            return "Relative: silver richer vs gold than usual"
        return "Gold/silver ratio is near its typical zone"
    if indicator == "dxy":
        if bucket in {"HIGH", "EXTREME_HIGH"}:
            return "Dollar strength often coincides with pressure on metals"
        if bucket in {"LOW", "EXTREME_LOW"}:
            return "Dollar softness is often a tailwind for metals"
        return "Dollar index sits in a neutral regime vs history"
    if indicator == "us10y":
        if bucket in {"HIGH", "EXTREME_HIGH"}:
            return "Higher yields can pressure precious metals"
        if bucket in {"LOW", "EXTREME_LOW"}:
            return "Lower yields often ease rate pressure on metals"
        return "Rates context is neutral relative to its history"
    return "Context only; not a trading instruction"


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    lower = int(k)
    upper = min(lower + 1, len(sorted_vals) - 1)
    weight = k - lower
    return sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight


def compute_conditional_stats(
    buckets: Sequence[str | None],
    closes: Sequence[float],
    *,
    horizons: Sequence[int] = (5, 10),
    min_occurrences: int = 50,
) -> dict[str, dict]:
    forward_returns: dict[int, list[float]] = {h: [] for h in horizons}
    for horizon in horizons:
        for idx in range(len(closes) - horizon):
            start = closes[idx]
            end = closes[idx + horizon]
            forward_returns[horizon].append((end - start) / start)

    grouped: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for idx, bucket in enumerate(buckets):
        if not bucket:
            continue
        for horizon in horizons:
            if idx + horizon >= len(closes):
                continue
            ret = forward_returns[horizon][idx]
            grouped[bucket][horizon].append(ret)

    summary: dict[str, dict] = {}
    for bucket, horizon_map in grouped.items():
        if not horizon_map:
            continue
        n = min(len(vals) for vals in horizon_map.values())
        if n < min_occurrences:
            continue
        bucket_stats: dict[str, float | int] = {"n": n}
        for horizon, values in horizon_map.items():
            bucket_stats[f"p_up_{horizon}d"] = sum(1 for v in values if v > 0) / len(values)
            bucket_stats[f"median_{horizon}d"] = median(values)
            bucket_stats[f"p10_{horizon}d"] = _percentile(values, 10)
            bucket_stats[f"p90_{horizon}d"] = _percentile(values, 90)
        summary[bucket] = bucket_stats
    return summary


def _build_indicator(
    key: str,
    name: str,
    values: list[float],
    dates: list[str],
    slv_closes: list[float],
    *,
    min_occurrences: int = 50,
) -> dict:
    if not values or not dates:
        raise ContextComputationError(f"No data available for {name}")
    z = rolling_zscores(values, window=252)
    pct = rolling_percentiles(values, window=2520)
    buckets = [context_bucket(p, z_val) for p, z_val in zip(pct, z)]
    stats = compute_conditional_stats(buckets, slv_closes, min_occurrences=min_occurrences)
    latest_idx = len(values) - 1
    latest_bucket = buckets[latest_idx]
    return {
        "key": key,
        "name": name,
        "dates": dates,
        "values": values,
        "z": z,
        "pct": pct,
        "buckets": buckets,
        "stats": stats,
        "current": {
            "value": values[latest_idx],
            "z": z[latest_idx],
            "pct": pct[latest_idx],
            "bucket": latest_bucket,
            "as_of": dates[latest_idx],
            "note": _bucket_note(key, latest_bucket),
        },
    }


def _values_map(series: list[tuple[str, float]]) -> dict[str, float]:
    return {d: v for d, v in series}


def build_context_payloads(
    slv_rows: list[dict],
    *,
    gld_rows: list[dict],
    dxy_rows: list[dict],
    us10y_rows: list[dict],
    min_occurrences: int = 50,
) -> tuple[dict, dict]:
    slv_series = _to_close_series(slv_rows)
    gld_series = _to_close_series(gld_rows)
    dxy_series = _to_close_series(dxy_rows)
    us10y_series = _to_close_series(us10y_rows)

    slv_map = _values_map(slv_series)
    gld_map = _values_map(gld_series)
    dxy_map = _values_map(dxy_series)
    us10y_map = _values_map(us10y_series)

    gsr_dates_raw, slv_for_gsr_raw, gld_vals_raw = _align_series(slv_series, gld_map)
    gsr_dates: list[str] = []
    gsr_values: list[float] = []
    slv_for_gsr: list[float] = []
    for date, g_val, s_val in zip(gsr_dates_raw, gld_vals_raw, slv_for_gsr_raw):
        if s_val:
            gsr_dates.append(date)
            gsr_values.append(g_val / s_val)
            slv_for_gsr.append(s_val)

    dxy_dates, slv_for_dxy, dxy_vals = _align_series(slv_series, dxy_map)
    us10y_dates, slv_for_us10y, us10y_vals = _align_series(slv_series, us10y_map)

    if not (gsr_values and dxy_vals and us10y_vals):
        raise ContextComputationError("Insufficient data to compute context indicators")

    gsr = _build_indicator(
        "gsr",
        "Gold/Silver Ratio",
        gsr_values,
        gsr_dates,
        slv_for_gsr,
        min_occurrences=min_occurrences,
    )
    dxy = _build_indicator(
        "dxy",
        "Dollar Index (proxy)",
        dxy_vals,
        dxy_dates,
        slv_for_dxy,
        min_occurrences=min_occurrences,
    )
    us10y = _build_indicator(
        "us10y",
        "US 10Y (proxy)",
        us10y_vals,
        us10y_dates,
        slv_for_us10y,
        min_occurrences=min_occurrences,
    )

    conditional_stats = {
        "gsr": {k: v for k, v in gsr["stats"].items() if v.get("n", 0) >= min_occurrences},
        "dxy": {k: v for k, v in dxy["stats"].items() if v.get("n", 0) >= min_occurrences},
        "us10y": {k: v for k, v in us10y["stats"].items() if v.get("n", 0) >= min_occurrences},
    }

    current_context = {
        "asof": slv_rows[-1]["date"],
        "items": [
            {"key": "GSR", "name": gsr["name"], **gsr["current"], "stats": gsr["stats"].get(gsr["current"]["bucket"])} if gsr else None,
            {"key": "DXY", "name": dxy["name"], **dxy["current"], "stats": dxy["stats"].get(dxy["current"]["bucket"])} if dxy else None,
            {"key": "US10Y", "name": us10y["name"], **us10y["current"], "stats": us10y["stats"].get(us10y["current"]["bucket"])} if us10y else None,
        ],
    }
    current_context["items"] = [item for item in current_context["items"] if item]

    return current_context, conditional_stats


def fetch_context_assets(start_date: str, *, source: str | None = None) -> dict[str, list[dict]]:
    sources = (source,) if source else ("stooq", "yahoo")

    def fetch_with_fallback(symbols: list[str], cache_name: str) -> tuple[str, list[dict]]:
        last_exc: Exception | None = None
        for sym in symbols:
            try:
                rows = fetch_ohlcv(
                    symbol=sym,
                    start_date=start_date,
                    cache_path=cache_name,
                    sources=sources,
                )
                return sym, rows
            except Exception as exc:  # noqa: PERF203
                last_exc = exc
                continue
        raise ContextComputationError(f"Unable to fetch {symbols}: {last_exc}")

    gld_symbol, gld_rows = fetch_with_fallback(["GLD"], cache_name="public/data/raw/gld_daily.json")
    dxy_symbol, dxy_rows = fetch_with_fallback(["DXY", "UUP"], cache_name="public/data/raw/dxy_daily.json")
    us10y_symbol, us10y_rows = fetch_with_fallback(["US10Y", "IEF", "TLT"], cache_name="public/data/raw/us10y_daily.json")

    now_source = source or "stooq/yahoo"
    _write_raw_payload("GLD", gld_rows, f"{now_source} ({gld_symbol})")
    _write_raw_payload("DXY", dxy_rows, f"{now_source} ({dxy_symbol})")
    _write_raw_payload("US10Y", us10y_rows, f"{now_source} ({us10y_symbol})")

    return {"GLD": gld_rows, "DXY": dxy_rows, "US10Y": us10y_rows}


def write_context_outputs(
    slv_rows: list[dict],
    *,
    gld_rows: list[dict],
    dxy_rows: list[dict],
    us10y_rows: list[dict],
    min_occurrences: int = 50,
) -> None:
    ensure_parent(BASE_CONTEXT_DIR / "placeholder")
    current, stats = build_context_payloads(
        slv_rows,
        gld_rows=gld_rows,
        dxy_rows=dxy_rows,
        us10y_rows=us10y_rows,
        min_occurrences=min_occurrences,
    )
    write_json(BASE_CONTEXT_DIR / "current_context.json", current)
    write_json(BASE_CONTEXT_DIR / "conditional_stats.json", stats)

