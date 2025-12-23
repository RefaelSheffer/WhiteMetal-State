import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

CFTC_LEGACY_FUTURES_ONLY_ZIP = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"
TARGET_MARKET_KEYWORDS = ["SILVER", "COMMODITY EXCHANGE"]
OUTPUT_DIR = Path("public/data/cot")
SAMPLE_FIXTURE = Path(__file__).with_name("sample_cot_silver.csv")


def normalize_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    return None


def load_local_fixture() -> pd.DataFrame:
    if SAMPLE_FIXTURE.exists():
        return pd.read_csv(SAMPLE_FIXTURE)

    today = datetime.now(timezone.utc).date()
    dates = pd.date_range(end=today, periods=120, freq="W-FRI").date
    base_oi = 150_000
    rows = []
    comm_long = 60_000
    comm_short = 45_000
    nonc_long = 55_000
    nonc_short = 40_000
    for idx, d in enumerate(dates):
        drift = math.sin(idx / 10) * 500
        comm_long_step = comm_long + drift
        comm_short_step = comm_short - drift * 0.6
        nonc_long_step = nonc_long - drift * 0.5
        nonc_short_step = nonc_short + drift * 0.4
        rows.append(
            {
                "Market_and_Exchange_Names": "SILVER - COMMODITY EXCHANGE INC.",
                "Report_Date_as_YYYY-MM-DD": d.isoformat(),
                "Open_Interest_All": base_oi + idx * 20 + drift,
                "Commercial_Positions_Long_All": comm_long_step,
                "Commercial_Positions_Short_All": comm_short_step,
                "Noncommercial_Positions_Long_All": nonc_long_step,
                "Noncommercial_Positions_Short_All": nonc_short_step,
                "Nonreportable_Positions_Long_All": 20_000 + drift * 0.3,
                "Nonreportable_Positions_Short_All": 18_000 - drift * 0.2,
            }
        )
    return pd.DataFrame(rows)


def download_zip(url: str) -> bytes:
    headers = {"User-Agent": "WhiteMetalBot/1.0 (cot-fetcher)"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.content


def read_first_csv_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise RuntimeError("Empty ZIP from CFTC")
        with zf.open(names[0]) as f:
            return pd.read_csv(f, low_memory=False)


def fetch_cot_history(start_year: int, end_year: int | None = None) -> pd.DataFrame:
    end_year = end_year or datetime.now(timezone.utc).year
    frames: list[pd.DataFrame] = []
    for year in range(start_year, end_year + 1):
        url = CFTC_LEGACY_FUTURES_ONLY_ZIP.format(year=year)
        try:
            zip_bytes = download_zip(url)
            dfy = read_first_csv_from_zip(zip_bytes)
            dfy["__year"] = year
            frames.append(dfy)
            print(f"[cot] fetched {year}")
        except Exception as exc:  # noqa: PERF203
            print(f"[WARN] failed {year}: {exc}")
    if frames:
        return pd.concat(frames, ignore_index=True)

    print("[cot] Falling back to bundled sample (network unreachable)")
    return load_local_fixture()


def filter_silver(df: pd.DataFrame) -> pd.DataFrame:
    col_market = normalize_col(df, ["Market_and_Exchange_Names", "Market and Exchange Names"])
    if not col_market:
        raise RuntimeError("Missing market names column in COT file")
    m = df[col_market].astype(str)
    silver = df[m.str.contains("SILVER", case=False, na=False)].copy()
    if silver.empty:
        raise RuntimeError("No SILVER rows found in COT history")
    comex = silver[m.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if not comex.empty:
        silver = comex
    return silver


def normalize_silver(df: pd.DataFrame) -> pd.DataFrame:
    col_date = normalize_col(df, ["Report_Date_as_YYYY-MM-DD", "Report Date as YYYY-MM-DD", "Report_Date"])
    col_oi = normalize_col(df, ["Open_Interest_All", "Open Interest (All)", "Open_Interest"])
    col_comm_long = normalize_col(df, ["Commercial_Positions_Long_All", "Commercial Long All", "Comm_long_All"])
    col_comm_short = normalize_col(df, ["Commercial_Positions_Short_All", "Commercial Short All", "Comm_short_All"])
    col_nc_long = normalize_col(df, ["Noncommercial_Positions_Long_All", "Noncommercial_Long_All", "Noncommercial Long"])
    col_nc_short = normalize_col(df, ["Noncommercial_Positions_Short_All", "Noncommercial_Short_All", "Noncommercial Short"])
    col_nr_long = normalize_col(df, ["Nonreportable_Positions_Long_All", "Nonreportable_Long_All", "Nonreportable Long"])
    col_nr_short = normalize_col(df, ["Nonreportable_Positions_Short_All", "Nonreportable_Short_All", "Nonreportable Short"])

    required = [col_date, col_oi, col_comm_long, col_comm_short, col_nc_long, col_nc_short]
    if not all(required):
        raise RuntimeError("Missing expected COT columns in history")

    out = pd.DataFrame()
    out["as_of"] = pd.to_datetime(df[col_date], errors="coerce")
    out["open_interest"] = pd.to_numeric(df[col_oi], errors="coerce")
    out["commercial_long"] = pd.to_numeric(df[col_comm_long], errors="coerce")
    out["commercial_short"] = pd.to_numeric(df[col_comm_short], errors="coerce")
    out["noncommercial_long"] = pd.to_numeric(df[col_nc_long], errors="coerce")
    out["noncommercial_short"] = pd.to_numeric(df[col_nc_short], errors="coerce")
    out["nonreportable_long"] = pd.to_numeric(df[col_nr_long], errors="coerce") if col_nr_long else math.nan
    out["nonreportable_short"] = pd.to_numeric(df[col_nr_short], errors="coerce") if col_nr_short else math.nan

    out = out.dropna(subset=["as_of", "commercial_long", "commercial_short", "noncommercial_long", "noncommercial_short"])
    out = out.sort_values("as_of").reset_index(drop=True)

    out["commercial_net"] = out["commercial_long"] - out["commercial_short"]
    out["noncommercial_net"] = out["noncommercial_long"] - out["noncommercial_short"]
    out["nonreportable_net"] = out["nonreportable_long"] - out["nonreportable_short"]

    out["commercial_net_change_1w"] = out["commercial_net"].diff()
    out["commercial_net_change_2w"] = out["commercial_net"] - out["commercial_net"].shift(2)
    out["noncommercial_net_change_1w"] = out["noncommercial_net"].diff()
    out["noncommercial_net_change_2w"] = out["noncommercial_net"] - out["noncommercial_net"].shift(2)
    out["open_interest_change_1w"] = out["open_interest"].diff()
    out["open_interest_change_4w"] = out["open_interest"] - out["open_interest"].shift(4)

    out["commercial_net_pct52"] = rolling_percentile(out["commercial_net"], 52)
    out["noncommercial_net_pct52"] = rolling_percentile(out["noncommercial_net"], 52)
    out["commercial_net_z52"] = rolling_zscore(out["commercial_net"], 52)
    out["noncommercial_net_z52"] = rolling_zscore(out["noncommercial_net"], 52)

    return out


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    values = series.tolist()
    out: list[float | None] = []
    for i, v in enumerate(values):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            out.append(None)
            continue
        start = max(0, i - window + 1)
        window_vals = [x for x in values[start : i + 1] if x is not None and not (isinstance(x, float) and math.isnan(x))]
        if not window_vals:
            out.append(None)
            continue
        rank = sum(1 for x in window_vals if x <= v)
        out.append(rank / len(window_vals))
    return pd.Series(out, index=series.index)


def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    out: list[float | None] = []
    values = series.tolist()
    for i, v in enumerate(values):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            out.append(None)
            continue
        start = max(0, i - window + 1)
        window_vals = [x for x in values[start : i + 1] if x is not None and not (isinstance(x, float) and math.isnan(x))]
        if len(window_vals) < 5:
            out.append(None)
            continue
        mean = sum(window_vals) / len(window_vals)
        variance = sum((x - mean) ** 2 for x in window_vals) / len(window_vals)
        std = math.sqrt(variance)
        out.append((v - mean) / std if std else 0.0)
    return pd.Series(out, index=series.index)


def cot_signal_from_latest(latest: pd.Series) -> dict:
    bias = "neutral"
    reasons: list[str] = []
    conf = "low"

    comm_pct = latest.get("commercial_net_pct52")
    comm_z = latest.get("commercial_net_z52")
    nonc_pct = latest.get("noncommercial_net_pct52")
    nonc_z = latest.get("noncommercial_net_z52")

    if comm_pct is not None and comm_pct <= 0.1:
        bias = "bullish"
        reasons.append("commercial_net_extreme_long")
    elif comm_pct is not None and comm_pct >= 0.9:
        bias = "bearish"
        reasons.append("commercial_net_extreme_short")

    if nonc_pct is not None and nonc_pct >= 0.9:
        bias = "bearish"
        reasons.append("noncommercial_crowded_long")
    elif nonc_pct is not None and nonc_pct <= 0.1:
        bias = "bullish"
        reasons.append("noncommercial_washed_out")

    max_abs_z = max(abs(comm_z or 0), abs(nonc_z or 0))
    if max_abs_z >= 2:
        conf = "high"
    elif max_abs_z >= 1:
        conf = "med"

    if not reasons:
        reasons.append("no_extreme_detected")

    return {"cot_bias": bias, "confidence": conf, "reason": reasons}


def latest_payload(df: pd.DataFrame) -> dict:
    last = df.iloc[-1]
    signals = cot_signal_from_latest(last)
    return {
        "as_of": last["as_of"].date().isoformat(),
        "report_type": "legacy_futures_only",
        "market": "COMEX Silver",
        "open_interest": int(last["open_interest"]) if not pd.isna(last["open_interest"]) else None,
        "groups": {
            "commercial": {
                "long": int(last["commercial_long"]),
                "short": int(last["commercial_short"]),
                "net": int(last["commercial_net"]),
                "net_change_1w": _safe_float(last.get("commercial_net_change_1w")),
                "net_change_2w": _safe_float(last.get("commercial_net_change_2w")),
                "z_52w": _safe_float(last.get("commercial_net_z52")),
                "pct_52w": _safe_float(last.get("commercial_net_pct52")),
            },
            "noncommercial": {
                "long": int(last["noncommercial_long"]),
                "short": int(last["noncommercial_short"]),
                "net": int(last["noncommercial_net"]),
                "net_change_1w": _safe_float(last.get("noncommercial_net_change_1w")),
                "net_change_2w": _safe_float(last.get("noncommercial_net_change_2w")),
                "z_52w": _safe_float(last.get("noncommercial_net_z52")),
                "pct_52w": _safe_float(last.get("noncommercial_net_pct52")),
            },
        },
        "signals": signals,
        "open_interest_change_4w": _safe_float(last.get("open_interest_change_4w")),
        "last_updated_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": {
            "provider": "CFTC",
            "dataset": "COT",
            "url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm",
        },
    }


def history_payload(df: pd.DataFrame) -> dict:
    series = []
    for _, row in df.iterrows():
        series.append(
            {
                "as_of": row["as_of"].date().isoformat(),
                "open_interest": _safe_int(row.get("open_interest")),
                "commercial_net": _safe_int(row.get("commercial_net")),
                "noncommercial_net": _safe_int(row.get("noncommercial_net")),
                "commercial_net_z52": _safe_float(row.get("commercial_net_z52")),
                "noncommercial_net_z52": _safe_float(row.get("noncommercial_net_z52")),
                "commercial_net_pct52": _safe_float(row.get("commercial_net_pct52")),
                "noncommercial_net_pct52": _safe_float(row.get("noncommercial_net_pct52")),
            }
        )
    return {"report_type": "legacy_futures_only", "market": "COMEX Silver", "series": series}


def _safe_int(value):
    if pd.isna(value):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value):
    if pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"[cot] wrote {path}")


def adjust_signal_with_cot(signal_path: Path, cot_latest: dict) -> None:
    if not signal_path.exists():
        return
    try:
        data = json.loads(signal_path.read_text())
    except Exception:
        return

    breakdown = data.get("scoreBreakdown") or data.get("score_breakdown") or []
    cot_entry = None
    others = []
    for item in breakdown:
        if isinstance(item, dict) and str(item.get("name", "")).lower() == "cot":
            cot_entry = item
        else:
            others.append(item)
    bias = cot_latest.get("signals", {}).get("cot_bias", "neutral")
    reason_list = cot_latest.get("signals", {}).get("reason", [])
    pct = cot_latest.get("groups", {}).get("commercial", {}).get("pct_52w")
    score_delta = 0
    summary_parts = []
    if bias == "bullish":
        score_delta += 15
        summary_parts.append("Commercials stretched long (bullish contrarian read).")
    elif bias == "bearish":
        score_delta -= 15
        summary_parts.append("Noncommercials crowded long / commercials hedged (bearish risk).")
    else:
        summary_parts.append("COT balanced; neutral weight.")
    if pct is not None:
        summary_parts.append(f"Commercial net at {pct*100:.1f}th percentile (52w).")

    cot_entry = cot_entry or {"name": "COT"}
    cot_entry.update({"points": score_delta, "summary": " ".join(summary_parts)})
    breakdown = others + [cot_entry]

    base_total = data.get("scoreTotal") or data.get("score_total") or 0
    data["scoreBreakdown"] = breakdown
    data["scoreTotal"] = round(base_total + score_delta, 2)
    data["confidence"] = data.get("confidence") or cot_latest.get("signals", {}).get("confidence", "MED")
    data["explain"] = data.get("explain") or {}
    bullets = [b for b in (data["explain"].get("bullets") or []) if "cot" not in str(b).lower()]
    bullets.append(f"COT bias: {bias} ({', '.join(reason_list)})")
    data["explain"]["bullets"] = bullets
    if bias != "neutral":
        data["explain"]["headline"] = data["explain"].get("headline") or f"Signal adjusted by COT ({bias})"

    if bias == "bullish" and data.get("action", "").upper().startswith("HOLD"):
        data["action"] = "BUY / ADD"
    elif bias == "bearish" and "BUY" in str(data.get("action", "")).upper():
        data["action"] = "HOLD"

    write_json(signal_path, data)


def main():
    parser = argparse.ArgumentParser(description="Fetch COT (Legacy Futures Only) for Silver and write cache JSON.")
    parser.add_argument("--start-year", type=int, default=2008, help="First year of COT history to download")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR, help="Directory to write COT JSON payloads")
    args = parser.parse_args()

    raw = fetch_cot_history(args.start_year)
    silver = filter_silver(raw)
    normalized = normalize_silver(silver)

    latest = latest_payload(normalized)
    history = history_payload(normalized)
    metadata = {
        "report_type": "legacy_futures_only",
        "market": "COMEX Silver",
        "start": history["series"][0]["as_of"],
        "end": history["series"][-1]["as_of"],
        "rows": len(history["series"]),
        "last_updated_utc": latest["last_updated_utc"],
        "source": latest["source"],
    }

    write_json(args.output_dir / "silver_cot_latest.json", latest)
    write_json(args.output_dir / "silver_cot_history.json", history)
    write_json(args.output_dir / "metadata.json", metadata)

    signal_path = Path("public/data/signal_latest.json")
    adjust_signal_with_cot(signal_path, latest)


if __name__ == "__main__":
    main()
