from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import requests

from engine.utils.http import get_with_retry
from engine.utils.io import sanitize_for_json, write_json

STOOQ_BASE_URL = "https://stooq.com/q/d/l/"
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"

DEFAULT_HISTORY_DAYS = 365
DEFAULT_OUTPUT = Path("public/data/context/cross_market_context.json")

STOOQ_SYMBOLS = {
    "gold": "GC.C",
    "silver": "SI.C",
}

FRED_SERIES = {
    "usd": "DTWEXBGS",
    "us10y": "DGS10",
}

HEADERS = {"User-Agent": "WhiteMetalBot/1.0 (cross-market-context)"}


@dataclass
class SeriesResult:
    name: str
    last_date: str | None
    last_value: float | None
    history: list[dict]


class CrossMarketFetchError(RuntimeError):
    """Raised when a specific series cannot be refreshed and no fallback is available."""


class CrossMarketContextGenerator:
    def __init__(
        self,
        *,
        output_path: Path = DEFAULT_OUTPUT,
        history_days: int = DEFAULT_HISTORY_DAYS,
        fred_api_key: str | None = None,
        fetchers: dict[str, Callable[[], Iterable[dict]]] | None = None,
    ) -> None:
        self.output_path = Path(output_path)
        self.history_days = history_days
        self.fred_api_key = fred_api_key or os.getenv("FRED_API_KEY")
        self.fetchers = fetchers or {
            "gold": self._fetch_gold,
            "silver": self._fetch_silver,
            "usd": self._fetch_usd,
            "us10y": self._fetch_us10y,
        }

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"

    def _fetch_stooq(self, symbol: str) -> list[dict]:
        url = f"{STOOQ_BASE_URL}?s={symbol}&i=d"
        resp = get_with_retry(url, headers=HEADERS, timeout=60, max_attempts=4)
        df = pd.read_csv(io.BytesIO(resp.content))
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
        return [
            {"date": row["Date"].date().isoformat(), "value": float(row["Close"])}
            for _, row in df.iterrows()
        ]

    def _fetch_gold(self) -> list[dict]:
        return self._fetch_stooq(STOOQ_SYMBOLS["gold"])

    def _fetch_silver(self) -> list[dict]:
        return self._fetch_stooq(STOOQ_SYMBOLS["silver"])

    def _fetch_fred_series(self, series_id: str) -> list[dict]:
        if not self.fred_api_key:
            raise CrossMarketFetchError("FRED API key missing; set FRED_API_KEY")

        params = {
            "series_id": series_id,
            "api_key": self.fred_api_key,
            "file_type": "json",
            "sort_order": "asc",
        }
        resp = requests.get(FRED_OBSERVATIONS_URL, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        observations = payload.get("observations", [])
        rows: list[dict] = []
        for obs in observations:
            value = obs.get("value")
            try:
                as_float = float(value)
            except (TypeError, ValueError):
                continue
            rows.append({"date": obs.get("date"), "value": as_float})
        return rows

    def _fetch_usd(self) -> list[dict]:
        return self._fetch_fred_series(FRED_SERIES["usd"])

    def _fetch_us10y(self) -> list[dict]:
        return self._fetch_fred_series(FRED_SERIES["us10y"])

    def _load_existing(self) -> dict:
        if not self.output_path.exists():
            return {}
        try:
            return json.loads(self.output_path.read_text())
        except Exception:
            return {}

    @staticmethod
    def _trim_history(history: list[dict], *, days: int) -> list[dict]:
        if not history:
            return []
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
        filtered = []
        for point in history:
            date_str = point.get("date")
            if not date_str:
                continue
            try:
                dt = datetime.fromisoformat(str(date_str)).date()
            except ValueError:
                continue
            if dt >= cutoff:
                filtered.append({"date": dt.isoformat(), "value": _safe_float(point.get("value"))})
        sanitized = [p for p in filtered if p["value"] is not None]
        if sanitized:
            return sanitized
        # If everything was filtered out (e.g., all older than cutoff), keep the latest points
        history_sorted = sorted(
            [p for p in history if p.get("date") and _safe_float(p.get("value")) is not None],
            key=lambda p: p["date"],
        )
        return history_sorted[-days:]

    def _build_series(self, name: str, raw_history: list[dict], fallback: dict | None = None) -> SeriesResult:
        history = self._trim_history(raw_history, days=self.history_days)
        if not history and fallback:
            history = fallback.get("history", [])
        last_date = history[-1]["date"] if history else fallback.get("last_date") if fallback else None
        last_value = history[-1]["value"] if history else fallback.get("last_value") if fallback else None
        return SeriesResult(name=name, last_date=last_date, last_value=_safe_float(last_value), history=history)

    def generate(self) -> bool:
        existing = self._load_existing()
        existing_series = existing.get("series", {}) if isinstance(existing, dict) else {}
        warnings: list[str] = []
        series_payload: dict[str, dict] = {}

        for name, fetcher in self.fetchers.items():
            raw_history: list[dict] = []
            try:
                raw_history = list(fetcher())
            except Exception as exc:  # noqa: PERF203
                warnings.append(f"{name} fetch failed: {exc}")
            series_result = self._build_series(name, raw_history, fallback=existing_series.get(name, {}))
            series_payload[name] = {
                "last_date": series_result.last_date,
                "last_value": series_result.last_value,
                "history": series_result.history,
            }

        if existing_series:
            if all(
                series_payload.get(key, {}).get("last_date") == (existing_series.get(key) or {}).get("last_date")
                for key in series_payload
            ):
                return False

        payload = {
            "meta": {
                "generated_at_utc": self._utc_now_iso(),
                "sources": {
                    "gold": f"stooq:{STOOQ_SYMBOLS['gold']}",
                    "silver": f"stooq:{STOOQ_SYMBOLS['silver']}",
                    "usd": f"fred:{FRED_SERIES['usd']}",
                    "us10y": f"fred:{FRED_SERIES['us10y']}",
                },
            },
            "series": series_payload,
        }
        if warnings:
            payload["meta"]["warnings"] = warnings

        write_json(self.output_path, sanitize_for_json(payload))
        return True


def _safe_float(val) -> float | None:
    try:
        if val is None:
            return None
        if isinstance(val, str) and val.strip() == "":
            return None
        out = float(val)
        if pd.isna(out):
            return None
        return float(out)
    except Exception:
        return None


def generate_cross_market_context(
    *,
    output_path: Path | str = DEFAULT_OUTPUT,
    history_days: int = DEFAULT_HISTORY_DAYS,
    fred_api_key: str | None = None,
    fetchers: dict[str, Callable[[], Iterable[dict]]] | None = None,
) -> bool:
    generator = CrossMarketContextGenerator(
        output_path=Path(output_path),
        history_days=history_days,
        fred_api_key=fred_api_key,
        fetchers=fetchers,
    )
    return generator.generate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate cross-market context JSON")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path to write cross_market_context.json")
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--fred-api-key", default=None, help="FRED API key (falls back to FRED_API_KEY env var)")
    args = parser.parse_args()

    updated = generate_cross_market_context(
        output_path=Path(args.output), history_days=args.history_days, fred_api_key=args.fred_api_key
    )
    status = "written" if updated else "skipped (no new data)"
    print(f"cross_market_context.json {status} at {args.output}")
