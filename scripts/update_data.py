import argparse
import io
import json
import os
import shutil
import time
import zipfile
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests


DATA_DIR = "data"
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
HISTORICAL_START_YEAR = 2008

STOOQ_URL = "https://stooq.com/q/d/l/?s=slv.us&i=d"  # Daily OHLCV
CIK = "0001330568"  # iShares Silver Trust
EDGAR_SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"

# CFTC Historical Compressed ZIPs (Legacy Futures Only, by year)
# NOTE: These URLs are linked from CFTC "Historical Compressed" pages (by year). :contentReference[oaicite:1]{index=1}
CFTC_LEGACY_FUTURES_ONLY_ZIP = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def year_range(start_year: int, end_year: int | None = None) -> list[int]:
    end = end_year or datetime.now(timezone.utc).year
    if start_year > end:
        return []
    return list(range(start_year, end + 1))


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def fetch_prices_stooq() -> pd.DataFrame:
    headers = {"User-Agent": "WhiteMetalBot/1.0 (whitemetal@example.com)"}
    resp = requests.get(STOOQ_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(io.BytesIO(resp.content))
    # Stooq columns: Date, Open, High, Low, Close, Volume
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")
    return df


def calc_price_score(df: pd.DataFrame) -> dict:
    # Basic trend/momentum scoring on daily closes
    closes = df["Close"].astype(float)
    last_close = float(closes.iloc[-1])

    ma20 = closes.rolling(20).mean()
    ma50 = closes.rolling(50).mean()
    ma200 = closes.rolling(200).mean()

    last_ma20 = safe_float(ma20.iloc[-1])
    last_ma50 = safe_float(ma50.iloc[-1])
    last_ma200 = safe_float(ma200.iloc[-1])

    score = 0

    # Trend bias
    if last_ma200 is not None and last_close > last_ma200:
        score += 20
    if last_ma50 is not None and last_ma200 is not None and last_ma50 > last_ma200:
        score += 10

    # Short-term momentum
    if last_ma20 is not None and last_ma50 is not None and last_ma20 > last_ma50:
        score += 10

    # 1M momentum
    if len(closes) >= 22:
        mom_1m = (last_close / float(closes.iloc[-22]) - 1.0) * 100.0
        if mom_1m > 2:
            score += 10
        elif mom_1m < -2:
            score -= 10
    else:
        mom_1m = None

    return {
        "score_price": score,
        "last_close": last_close,
        "ma20": last_ma20,
        "ma50": last_ma50,
        "ma200": last_ma200,
        "mom_1m_pct": mom_1m,
    }


def download_zip(url: str, headers: dict | None = None) -> bytes:
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.content


def read_first_csv_from_zip(zip_bytes: bytes) -> pd.DataFrame:
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    # choose first non-directory file
    names = [n for n in zf.namelist() if not n.endswith("/")]
    if not names:
        raise RuntimeError("ZIP has no files")
    with zf.open(names[0]) as f:
        # Many CFTC files are comma-delimited text; pandas can read it.
        df = pd.read_csv(f, low_memory=False)
    return df


def fetch_cot_legacy_futures_only(years: list[int]) -> pd.DataFrame:
    # Some environments may occasionally fail downloading; we'll degrade gracefully.
    dfs = []
    for y in years:
        url = CFTC_LEGACY_FUTURES_ONLY_ZIP.format(year=y)
        try:
            zip_bytes = download_zip(url)
            dfy = read_first_csv_from_zip(zip_bytes)
            dfy["__year"] = y
            dfs.append(dfy)
        except Exception as e:
            print(f"[WARN] COT download/parse failed for {y}: {e}")
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    return df


def normalize_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols:
            return cols[key]
    return None


def calc_cot_score(cot_df: pd.DataFrame) -> dict:
    """
    We use Legacy Futures Only, filter SILVER - COMMODITY EXCHANGE INC.
    Compute Noncommercial Net = Long - Short, and 4-week delta.
    """
    if cot_df.empty:
        return {"score_cot": 0, "cot_available": False}

    col_market = normalize_col(cot_df, ["Market_and_Exchange_Names", "Market and Exchange Names"])
    col_date = normalize_col(cot_df, ["Report_Date_as_YYYY-MM-DD", "Report Date as YYYY-MM-DD", "Report_Date"])
    col_nc_long = normalize_col(cot_df, ["Noncommercial_Long_All", "Noncommercial Long All", "Noncommercial Long"])
    col_nc_short = normalize_col(cot_df, ["Noncommercial_Short_All", "Noncommercial Short All", "Noncommercial Short"])

    if not all([col_market, col_date, col_nc_long, col_nc_short]):
        # Column names can vary; degrade gracefully.
        print("[WARN] Missing expected COT columns. Available columns:", list(cot_df.columns)[:30])
        return {"score_cot": 0, "cot_available": False}

    df = cot_df.copy()
    df[col_date] = pd.to_datetime(df[col_date], errors="coerce")
    df = df.dropna(subset=[col_date])

    # Try to match “SILVER” market line
    m = df[col_market].astype(str)
    df = df[m.str.contains("SILVER", case=False, na=False)]

    # Prefer COMEX line if present
    df_comex = df[m.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if not df_comex.empty:
        df = df_comex

    df = df.sort_values(col_date)
    df["nc_net"] = pd.to_numeric(df[col_nc_long], errors="coerce") - pd.to_numeric(df[col_nc_short], errors="coerce")
    df = df.dropna(subset=["nc_net"])

    if df.empty:
        return {"score_cot": 0, "cot_available": False}

    last = df.iloc[-1]
    last_net = float(last["nc_net"])
    last_date = last[col_date].date().isoformat()

    # 4-week delta
    if len(df) >= 5:
        net_4w_ago = float(df.iloc[-5]["nc_net"])
        delta_4w = last_net - net_4w_ago
    else:
        delta_4w = 0.0

    # Score rule (simple MVP):
    # - If net increased over 4 weeks => bullish +15
    # - If net decreased over 4 weeks => bearish -15
    score = 0
    if delta_4w > 0:
        score += 15
    elif delta_4w < 0:
        score -= 15

    # Mild crowding penalty/bonus via percentile within available sample
    pct = float((df["nc_net"].rank(pct=True).iloc[-1]) * 100.0)
    if pct >= 85:
        score -= 5  # crowded long
    elif pct <= 15:
        score += 5  # washed out

    return {
        "score_cot": score,
        "cot_available": True,
        "cot_report_date": last_date,
        "nc_net": last_net,
        "nc_net_delta_4w": float(delta_4w),
        "nc_net_percentile": pct,
    }


def fetch_edgar_latest() -> dict:
    # SEC asks for identifying User-Agent; put your email here (required for automation).
    headers = {
        "User-Agent": "WhiteMetalBot/1.0 (whitemetal@example.com)",
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }
    try:
        r = requests.get(EDGAR_SUBMISSIONS_URL, headers=headers, timeout=60)
        r.raise_for_status()
        j = r.json()
        recent = j.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        acc = recent.get("accessionNumber", [])

        if not forms or not dates:
            return {"edgar_available": True, "latest_filing": None}

        latest = {
            "form": forms[0],
            "filingDate": dates[0],
            "accessionNumber": acc[0] if acc else None,
        }
        # Small "event score" if a new filing was in last 14 days
        latest_dt = pd.to_datetime(latest["filingDate"], errors="coerce")
        score = 0
        if pd.notna(latest_dt):
            days = (pd.Timestamp.utcnow().normalize() - latest_dt.normalize()).days
            if days <= 14:
                score = 5

        latest["days_ago"] = int(days) if pd.notna(latest_dt) else None
        return {"edgar_available": True, "latest_filing": latest, "score_events": score}

    except Exception as e:
        print(f"[WARN] EDGAR fetch failed: {e}")
        return {"edgar_available": False, "latest_filing": None, "score_events": 0}


DEFAULT_BUY_THRESHOLD = 30.0
DEFAULT_SELL_THRESHOLD = -30.0
DEFAULT_BULLISH_HOLD_THRESHOLD = 10.0
DEFAULT_BEARISH_HOLD_THRESHOLD = -10.0


def decide_action(
    score_total: float,
    *,
    buy_threshold: float = DEFAULT_BUY_THRESHOLD,
    sell_threshold: float = DEFAULT_SELL_THRESHOLD,
    bullish_hold_threshold: float = DEFAULT_BULLISH_HOLD_THRESHOLD,
    bearish_hold_threshold: float = DEFAULT_BEARISH_HOLD_THRESHOLD,
) -> tuple[str, str]:
    """Map a composite score into an action bucket with configurable thresholds."""

    if score_total >= buy_threshold:
        return "BUY / ADD", "HIGH"
    if score_total <= sell_threshold:
        return "REDUCE / HEDGE", "HIGH"
    if score_total >= bullish_hold_threshold:
        return "HOLD (Bullish bias)", "MED"
    if score_total <= bearish_hold_threshold:
        return "HOLD (Bearish bias)", "MED"
    return "HOLD / WAIT", "LOW"


def backup_json_outputs(paths: list[str], timestamp: str | None = None) -> str | None:
    if not paths:
        return None

    ensure_dir(BACKUP_DIR)
    ts = timestamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = os.path.join(BACKUP_DIR, ts)
    ensure_dir(backup_dir)

    for p in paths:
        if os.path.exists(p):
            shutil.copy2(p, os.path.join(backup_dir, os.path.basename(p)))
    return backup_dir


def run_continuously(
    start_year: int,
    enable_backup: bool,
    interval_hours: float,
    *,
    buy_threshold: float = DEFAULT_BUY_THRESHOLD,
    sell_threshold: float = DEFAULT_SELL_THRESHOLD,
    bullish_hold_threshold: float = DEFAULT_BULLISH_HOLD_THRESHOLD,
    bearish_hold_threshold: float = DEFAULT_BEARISH_HOLD_THRESHOLD,
):
    interval = max(interval_hours, 1.0)
    while True:
        run_once(
            start_year=start_year,
            enable_backup=enable_backup,
            buy_threshold=buy_threshold,
            sell_threshold=sell_threshold,
            bullish_hold_threshold=bullish_hold_threshold,
            bearish_hold_threshold=bearish_hold_threshold,
        )
        print(f"Sleeping for {interval} hours before next refresh...")
        time.sleep(interval * 3600)




def run_once(
    start_year: int = HISTORICAL_START_YEAR,
    enable_backup: bool = True,
    *,
    buy_threshold: float = DEFAULT_BUY_THRESHOLD,
    sell_threshold: float = DEFAULT_SELL_THRESHOLD,
    bullish_hold_threshold: float = DEFAULT_BULLISH_HOLD_THRESHOLD,
    bearish_hold_threshold: float = DEFAULT_BEARISH_HOLD_THRESHOLD,
):
    ensure_dir(DATA_DIR)

    # ---- Prices ----
    prices_df = fetch_prices_stooq()
    price_info = calc_price_score(prices_df)

    # Save prices JSON for dashboard
    prices_out = {
        "updated_at_utc": utc_now_iso(),
        "symbol": "SLV",
        "dates": prices_df["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "close": prices_df["Close"].astype(float).round(4).tolist(),
        "volume": prices_df["Volume"].fillna(0).astype(float).tolist(),
    }
    prices_path = os.path.join(DATA_DIR, "slv_prices.json")
    with open(prices_path, "w", encoding="utf-8") as f:
        json.dump(prices_out, f, ensure_ascii=False)

    # ---- COT ----
    years = year_range(start_year)
    cot_raw = fetch_cot_legacy_futures_only(years)
    cot_info = calc_cot_score(cot_raw)
    cot_path = os.path.join(DATA_DIR, "cot_silver.json")
    with open(cot_path, "w", encoding="utf-8") as f:
        json.dump({"updated_at_utc": utc_now_iso(), **cot_info}, f, ensure_ascii=False)

    # ---- EDGAR ----
    edgar_info = fetch_edgar_latest()
    edgar_path = os.path.join(DATA_DIR, "edgar_latest.json")
    with open(edgar_path, "w", encoding="utf-8") as f:
        json.dump({"updated_at_utc": utc_now_iso(), **edgar_info}, f, ensure_ascii=False)

    # ---- Final Signal ----
    score_total = float(price_info["score_price"]) + float(cot_info.get("score_cot", 0)) + float(edgar_info.get("score_events", 0))
    action, confidence = decide_action(
        score_total,
        buy_threshold=buy_threshold,
        sell_threshold=sell_threshold,
        bullish_hold_threshold=bullish_hold_threshold,
        bearish_hold_threshold=bearish_hold_threshold,
    )

    signal = {
        "updated_at_utc": utc_now_iso(),
        "symbol": "SLV",
        "score_total": score_total,
        "action": action,
        "confidence": confidence,
        "breakdown": {
            "score_price": price_info["score_price"],
            "score_cot": cot_info.get("score_cot", 0),
            "score_events": edgar_info.get("score_events", 0),
        },
        "price": {
            "last_close": price_info["last_close"],
            "ma20": price_info["ma20"],
            "ma50": price_info["ma50"],
            "ma200": price_info["ma200"],
            "mom_1m_pct": price_info["mom_1m_pct"],
        },
        "cot": cot_info,
        "edgar": edgar_info,
        "disclaimer": "Not financial advice. For research/dashboard only.",
    }

    signal_path = os.path.join(DATA_DIR, "signal_latest.json")
    with open(signal_path, "w", encoding="utf-8") as f:
        json.dump(signal, f, ensure_ascii=False)

    written = [prices_path, cot_path, edgar_path, signal_path]
    if enable_backup:
        backup_dir = backup_json_outputs(written)
        print(f"Backed up outputs to {backup_dir}")

    print("OK: wrote data/*.json")


def main():
    parser = argparse.ArgumentParser(description="Fetch SLV pricing, COT, and EDGAR data.")
    parser.add_argument("--start-year", type=int, default=HISTORICAL_START_YEAR, help="First year to request CFTC history (default: 2008)")
    parser.add_argument("--no-backup", action="store_true", help="Skip copying outputs to data/backups/<timestamp>")
    parser.add_argument("--loop-daily", action="store_true", help="Run continuously with a daily refresh interval")
    parser.add_argument("--interval-hours", type=float, default=24.0, help="Refresh cadence in hours when using --loop-daily")
    parser.add_argument(
        "--buy-threshold",
        type=float,
        default=DEFAULT_BUY_THRESHOLD,
        help="Score needed to trigger BUY / ADD",
    )
    parser.add_argument(
        "--sell-threshold",
        type=float,
        default=DEFAULT_SELL_THRESHOLD,
        help="Score needed to trigger REDUCE / HEDGE",
    )
    parser.add_argument(
        "--bullish-hold-threshold",
        type=float,
        default=DEFAULT_BULLISH_HOLD_THRESHOLD,
        help="Score that biases HOLD toward bullish stance",
    )
    parser.add_argument(
        "--bearish-hold-threshold",
        type=float,
        default=DEFAULT_BEARISH_HOLD_THRESHOLD,
        help="Score that biases HOLD toward bearish stance",
    )
    args = parser.parse_args()

    enable_backup = not args.no_backup
    if args.loop_daily:
        run_continuously(
            start_year=args.start_year,
            enable_backup=enable_backup,
            interval_hours=args.interval_hours,
            buy_threshold=args.buy_threshold,
            sell_threshold=args.sell_threshold,
            bullish_hold_threshold=args.bullish_hold_threshold,
            bearish_hold_threshold=args.bearish_hold_threshold,
        )
    else:
        run_once(
            start_year=args.start_year,
            enable_backup=enable_backup,
            buy_threshold=args.buy_threshold,
            sell_threshold=args.sell_threshold,
            bullish_hold_threshold=args.bullish_hold_threshold,
            bearish_hold_threshold=args.bearish_hold_threshold,
        )


if __name__ == "__main__":
    main()
