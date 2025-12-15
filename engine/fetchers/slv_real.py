from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

STOOQ_URL = "https://stooq.com/q/d/l/?s=slv.us&i=d"


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0).astype("int64")
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    return df


def fetch_slv_ohlcv(
    start_date: str = "2008-01-01",
    end_date: str | None = None,
    cache_path: str = "public/data/raw/slv_daily.json",
    source: str = "stooq",
) -> list[dict]:
    if source != "stooq":
        raise ValueError("Only stooq supported in this fetcher")

    try:
        df = pd.read_csv(STOOQ_URL)
    except Exception:
        cache_file = Path(cache_path)
        if cache_file.exists():
            return json.loads(cache_file.read_text())
        raise

    df = _normalize(df)

    df = df[df["date"] >= start_date]
    if end_date:
        df = df[df["date"] <= end_date]

    records = df.to_dict(orient="records")

    path = Path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    return records
