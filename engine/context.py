from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Iterable, Sequence

from engine.fetchers.ohlcv import fetch_ohlcv
from engine.utils.io import ensure_parent, write_json

BASE_CONTEXT_DIR = Path("public/data/context")
Z_WINDOW = 252
PERCENTILE_WINDOW = 2520
MAX_FFILL_DAYS = 3
DEFAULT_MIN_OCCURRENCES = 80


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


def _align_series_with_ffill(
    primary: list[tuple[str, float]],
    secondary: list[tuple[str, float]],
    *,
    max_ffill_days: int = MAX_FFILL_DAYS,
) -> tuple[list[str], list[float], list[float]]:
    dates: list[str] = []
    primary_vals: list[float] = []
    secondary_vals: list[float] = []

    secondary_sorted = sorted(secondary, key=lambda x: x[0])
    sec_idx = 0
    last_date: datetime | None = None
    last_val: float | None = None

    for date_str, pval in primary:
        date_dt = datetime.fromisoformat(date_str)
        while sec_idx < len(secondary_sorted) and secondary_sorted[sec_idx][0] <= date_str:
            last_date = datetime.fromisoformat(secondary_sorted[sec_idx][0])
            last_val = secondary_sorted[sec_idx][1]
            sec_idx += 1

        if last_date is None or last_val is None:
            continue

        if last_val <= 0 or pval <= 0:
            continue

        if date_dt - last_date <= timedelta(days=max_ffill_days):
            dates.append(date_str)
            primary_vals.append(pval)
            secondary_vals.append(last_val)
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


def _confidence_from_samples(n: int, min_occurrences: int) -> str:
    if n < min_occurrences:
        return "low"
    if n < min_occurrences * 2:
        return "medium"
    return "high"


def compute_conditional_stats(
    buckets: Sequence[str | None],
    closes: Sequence[float],
    *,
    horizons: Sequence[int] = (5, 10),
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
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
        bucket_stats: dict[str, float | int | str] = {
            "n": n,
            "confidence": _confidence_from_samples(n, min_occurrences),
        }
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
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
) -> dict:
    if not values or not dates:
        raise ContextComputationError(f"No data available for {name}")
    z = rolling_zscores(values, window=Z_WINDOW)
    pct = rolling_percentiles(values, window=PERCENTILE_WINDOW)
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


def build_context_payloads(
    slv_rows: list[dict],
    *,
    gld_rows: list[dict],
    dxy_rows: list[dict],
    us10y_rows: list[dict],
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
) -> tuple[dict, dict]:
    slv_series = _to_close_series(slv_rows)
    gld_series = _to_close_series(gld_rows)
    dxy_series = _to_close_series(dxy_rows)
    us10y_series = _to_close_series(us10y_rows)

    gsr_dates_raw, slv_for_gsr_raw, gld_vals_raw = _align_series_with_ffill(slv_series, gld_series)
    gsr_dates: list[str] = []
    gsr_values: list[float] = []
    slv_for_gsr: list[float] = []
    for date, g_val, s_val in zip(gsr_dates_raw, gld_vals_raw, slv_for_gsr_raw):
        if s_val:
            gsr_dates.append(date)
            gsr_values.append(g_val / s_val)
            slv_for_gsr.append(s_val)

    dxy_dates, slv_for_dxy, dxy_vals = _align_series_with_ffill(slv_series, dxy_series)
    us10y_dates, slv_for_us10y, us10y_vals = _align_series_with_ffill(slv_series, us10y_series)

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
        "GSR": gsr["stats"],
        "DXY": dxy["stats"],
        "US10Y": us10y["stats"],
    }

    baseline = {
        "window_z": Z_WINDOW,
        "window_pct": PERCENTILE_WINDOW,
        "min_occurrences": min_occurrences,
    }

    current_context = {
        "asof": slv_rows[-1]["date"],
        "baseline": baseline,
        "items": [],
        "note": "Historical context only; not a promise.",
    }

    for indicator in (gsr, dxy, us10y):
        bucket = indicator["current"]["bucket"]
        band_stats = indicator["stats"].get(bucket, {}) if bucket else {}
        current_context["items"].append(
            {
                "key": indicator["key"].upper(),
                "name": indicator["name"],
                "value": indicator["current"]["value"],
                "z": indicator["current"]["z"],
                "pct": indicator["current"]["pct"],
                "band": bucket,
                "n_band": band_stats.get("n", 0),
                "p_up_5d": band_stats.get("p_up_5d"),
                "median_5d": band_stats.get("median_5d"),
                "p_up_10d": band_stats.get("p_up_10d"),
                "median_10d": band_stats.get("median_10d"),
                "confidence": band_stats.get("confidence", _confidence_from_samples(0, min_occurrences)),
                "note": indicator["current"].get("note") or "Historical context only; not a promise.",
                "as_of": indicator["current"].get("as_of"),
            }
        )

    return current_context, {"baseline": baseline, **conditional_stats}


def fetch_context_assets(
    start_date: str,
    *,
    source: str | None = None,
    refresh: bool = False,
) -> tuple[dict[str, list[dict]], dict]:
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
                    refresh=refresh,
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

    meta = {
        "preferred_source": now_source,
        "symbols": {
            "GLD": gld_symbol,
            "DXY": dxy_symbol,
            "US10Y": us10y_symbol,
        },
    }

    return {"GLD": gld_rows, "DXY": dxy_rows, "US10Y": us10y_rows}, meta


def write_context_outputs(
    slv_rows: list[dict],
    *,
    gld_rows: list[dict],
    dxy_rows: list[dict],
    us10y_rows: list[dict],
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
    source: str | None = None,
    meta: dict | None = None,
) -> None:
    ensure_parent(BASE_CONTEXT_DIR / "placeholder")
    current, stats = build_context_payloads(
        slv_rows,
        gld_rows=gld_rows,
        dxy_rows=dxy_rows,
        us10y_rows=us10y_rows,
        min_occurrences=min_occurrences,
    )
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    assets_meta = meta or {}
    assets_meta.update(
        {
            "SLV": {
                "start": slv_rows[0]["date"] if slv_rows else None,
                "end": slv_rows[-1]["date"] if slv_rows else None,
                "rows": len(slv_rows),
                "source": source or "unknown",
            },
            "GLD": {
                "start": gld_rows[0]["date"] if gld_rows else None,
                "end": gld_rows[-1]["date"] if gld_rows else None,
                "rows": len(gld_rows),
                "source": meta.get("preferred_source") if meta else source,
                "fetched_symbol": meta.get("symbols", {}).get("GLD") if meta else None,
            },
            "DXY": {
                "start": dxy_rows[0]["date"] if dxy_rows else None,
                "end": dxy_rows[-1]["date"] if dxy_rows else None,
                "rows": len(dxy_rows),
                "source": meta.get("preferred_source") if meta else source,
                "fetched_symbol": meta.get("symbols", {}).get("DXY") if meta else None,
            },
            "US10Y": {
                "start": us10y_rows[0]["date"] if us10y_rows else None,
                "end": us10y_rows[-1]["date"] if us10y_rows else None,
                "rows": len(us10y_rows),
                "source": meta.get("preferred_source") if meta else source,
                "fetched_symbol": meta.get("symbols", {}).get("US10Y") if meta else None,
            },
        }
    )
    meta_payload = {
        "asof": slv_rows[-1]["date"] if slv_rows else None,
        "last_updated_utc": now,
        "baseline": stats.get("baseline"),
        "assets": assets_meta,
        "notes": [
            "Historical context only; not a promise.",
            "Conditional stats on SLV based on similar cross-market regimes.",
        ],
    }

    write_json(BASE_CONTEXT_DIR / "cross_market_current.json", current)
    write_json(BASE_CONTEXT_DIR / "cross_market_conditional.json", stats)
    write_json(BASE_CONTEXT_DIR / "cross_market_meta.json", meta_payload)

    # Legacy paths for backward compatibility
    write_json(BASE_CONTEXT_DIR / "current_context.json", current)
    write_json(BASE_CONTEXT_DIR / "conditional_stats.json", stats)
