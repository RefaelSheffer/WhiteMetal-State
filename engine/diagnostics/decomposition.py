from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Sequence

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

from engine.utils.io import ensure_parent, write_json


PERIOD_BY_MODE = {"weekly": 5, "monthly": 21, "yearly": 252}


@dataclass
class DecompositionConfig:
    asset: str = "SLV"
    input: str = "log_close"
    period_mode: str = "monthly"
    robust: bool = True
    period: int | None = None

    def resolved_period(self) -> int:
        if self.period is not None:
            return self.period
        return PERIOD_BY_MODE.get(self.period_mode, PERIOD_BY_MODE["monthly"])


@dataclass
class DecompositionResult:
    meta: Mapping
    trend: Mapping
    seasonal: Mapping
    residual: Mapping
    reconstruction: Mapping | None


class DecompositionError(Exception):
    pass


def _validate_inputs(dates: Sequence[str], closes: Sequence[float], period: int) -> None:
    if len(dates) != len(closes):
        raise DecompositionError("dates and closes length mismatch")
    if not dates or not closes:
        raise DecompositionError("no data available")
    min_len = max(3 * period, period + 1)
    if len(closes) < min_len:
        raise DecompositionError(f"insufficient data for period={period}; need >= {min_len}")
    if any(c is None for c in closes):
        raise DecompositionError("missing close values")
    if any(c <= 0 for c in closes):
        raise DecompositionError("non-positive close values cannot be logged")


def _sanitize_series(values: Sequence[float]) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if np.isnan(arr).any() or np.isinf(arr).any():
        raise DecompositionError("NaNs or inf values in series")
    return arr


def compute_decomposition(
    dates: Sequence[str], closes: Sequence[float], config: DecompositionConfig | None = None
) -> DecompositionResult:
    cfg = config or DecompositionConfig()
    period = cfg.resolved_period()

    meta: Dict[str, object] = {
        "asof": dates[-1] if dates else None,
        "asset": cfg.asset,
        "input": cfg.input,
        "stl": {"period_mode": cfg.period_mode, "period": period, "robust": cfg.robust},
        "range": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
            "n": len(dates),
        },
        "status": "ok",
        "error": None,
    }

    try:
        _validate_inputs(dates, closes, period)
        y = _sanitize_series(np.log(closes) if cfg.input == "log_close" else closes)
        series = pd.Series(y)
        decomposition = STL(series, period=period, robust=cfg.robust).fit()
    except Exception as err:  # pragma: no cover - captured as metadata
        meta["status"] = "error"
        meta["error"] = str(err)
        empty = {"key": "", "units": cfg.input, "rows": []}
        return DecompositionResult(meta=meta, trend=empty, seasonal=empty, residual=empty, reconstruction=None)

    def build_rows(values: Sequence[float], key: str) -> Mapping:
        rows = []
        for date, value in zip(dates, values):
            if not np.isfinite(value):
                continue
            rows.append({"date": date, "value": round(float(value), 4)})
        return {"key": key, "units": "log" if cfg.input.startswith("log") else "raw", "rows": rows}

    trend = build_rows(decomposition.trend, "trend")
    seasonal = build_rows(decomposition.seasonal, "seasonal")
    residual = build_rows(decomposition.resid, "residual")

    recon_rows = []
    if len(trend["rows"]) == len(seasonal["rows"]) == len(residual["rows"]):
        recon_values = np.array([row["value"] for row in trend["rows"]])
        recon_values += np.array([row["value"] for row in seasonal["rows"]])
        recon_values += np.array([row["value"] for row in residual["rows"]])
        y_values = np.array([round(float(v), 4) for v in y[: len(recon_values)]])
        diffs = recon_values - y_values
        recon_rows = [
            {"date": date, "y": float(y_val), "recon": round(float(rec), 4), "err": round(float(err), 4)}
            for date, y_val, rec, err in zip(dates[: len(recon_values)], y_values, recon_values, diffs)
        ]
    reconstruction = None
    if recon_rows:
        abs_err = np.abs([row["err"] for row in recon_rows])
        reconstruction = {
            "key": "reconstruction",
            "rows": recon_rows,
            "summary": {
                "mae": round(float(abs_err.mean()), 6),
                "max_abs": round(float(abs_err.max()), 6),
            },
        }

    return DecompositionResult(
        meta=meta,
        trend=trend,
        seasonal=seasonal,
        residual=residual,
        reconstruction=reconstruction,
    )


def write_decomposition_outputs(
    dates: Sequence[str], closes: Sequence[float], output_dir: Path, config: DecompositionConfig | None = None
) -> DecompositionResult:
    cfg = config or DecompositionConfig()
    result = compute_decomposition(dates, closes, cfg)

    ensure_parent(output_dir / "placeholder")

    write_json(output_dir / "decomposition_meta.json", result.meta)
    if result.meta.get("status") != "ok":
        return result

    write_json(output_dir / "decomposition_trend.json", result.trend)
    write_json(output_dir / "decomposition_seasonal.json", result.seasonal)
    write_json(output_dir / "decomposition_residual.json", result.residual)
    if result.reconstruction:
        write_json(output_dir / "decomposition_recon.json", result.reconstruction)

    return result
