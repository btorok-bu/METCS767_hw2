"""
This code is largely AI generated and was generated as quickly as possible
in order to get additional snapshot data for the assignment 2 dataset.
Changes to this code are minimal and future implementations should be done
to improve the performance and output. However, it is functional for what is
currently needed.
"""

import os
import sys
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

# API config
BASE = "https://api.massive.com"
TIMESPAN = "day"
MULTIPLIER = 1
LIMIT = 50000
ADJUSTED = True
SORT = "asc"
DEFAULT_WORKERS = 20

# Anchors to write
ANCHORS = {
    "4y": relativedelta(years=4),
    "2y": relativedelta(years=2),
    "1y": relativedelta(years=1),
    "6m": relativedelta(months=6),
    "3m": relativedelta(months=3),
    "1m": relativedelta(months=1),
}

FIELDS = ["date", "open", "high", "low", "close", "volume", "vwap"]  # written per anchor

def iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def human_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def pick_on_or_before(sorted_bars, anchor_dt_utc: datetime):
    """Return last bar whose timestamp <= end of anchor day (UTC)."""
    anchor_ms = int(anchor_dt_utc.replace(hour=23, minute=59, second=59, microsecond=0).timestamp() * 1000)
    lo, hi = 0, len(sorted_bars) - 1
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        t = sorted_bars[mid]["t"]
        if t <= anchor_ms:
            best = sorted_bars[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def fetch_aggs(api_key: str, ticker: str, start_iso: str, end_iso: str, retry=3, backoff=1.5):
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/{MULTIPLIER}/{TIMESPAN}/{start_iso}/{end_iso}"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"adjusted": str(ADJUSTED).lower(), "sort": SORT, "limit": LIMIT}

    last_err = None
    for attempt in range(1, retry + 1):
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        # retry burst/edge outages
        if r.status_code in (429, 502, 503, 504):
            import time
            time.sleep((backoff ** (attempt - 1)) + 0.05 * attempt)
            last_err = f"HTTP {r.status_code}: {r.text}"
            continue
        r.raise_for_status()
    raise requests.HTTPError(last_err or "Failed after retries")


def per_ticker_job(api_key: str, ticker: str, ref_dt_utc: datetime):
    """Fetch last 4y of daily bars; compute snapshot fields for each anchor."""
    start = ref_dt_utc - relativedelta(years=4)
    start_iso, end_iso = iso(start), iso(ref_dt_utc)

    out = {"symbol": ticker}
    try:
        data = fetch_aggs(api_key, ticker, start_iso, end_iso)
        bars = data.get("results") or []
        bars.sort(key=lambda b: b["t"])
        for label, delta in ANCHORS.items():
            bar = pick_on_or_before(bars, ref_dt_utc - delta)
            if bar:
                out[f"{label}_date"] = human_date(bar["t"])
                out[f"{label}_open"] = bar.get("o")
                out[f"{label}_high"] = bar.get("h")
                out[f"{label}_low"] = bar.get("l")
                out[f"{label}_close"] = bar.get("c")
                out[f"{label}_volume"] = bar.get("v")
                out[f"{label}_vwap"] = bar.get("vw")
            else:
                for f in FIELDS:
                    out[f"{label}_{f}"] = None
        return out
    except Exception as e:
        # keep schema; mark as error
        for label in ANCHORS.keys():
            for f in FIELDS:
                out[f"{label}_{f}"] = None
        out["massive_error"] = repr(e)
        return out

def main():
    ap = argparse.ArgumentParser(description="Write Massive snapshot anchors (4y,2y,1y,6m,3m,1m) into CSV (in-place).")
    ap.add_argument("--csv", default="sp1500_company_info.csv", help="Input CSV with a 'symbol' column")
    ap.add_argument("--symbol-col", default="symbol", help="Ticker column name")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Parallel workers (threads)")
    ap.add_argument("--ref-date", default=None, help="Reference date (YYYY-MM-DD UTC). Default: today UTC")
    ap.add_argument("--verbose", action="store_true", help="Print light progress")
    args = ap.parse_args()

    api_key = os.getenv("MASSIVE_API_KEY")
    if not api_key:
        print("ERROR: set MASSIVE_API_KEY in your environment.")
        sys.exit(1)

    if not os.path.exists(args.csv):
        print(f"ERROR: file not found: {args.csv}")
        sys.exit(1)

    df = pd.read_csv(args.csv)
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # find symbol column (case-insensitive fallback)
    sym_col = args.symbol_col
    if sym_col not in df.columns:
        lowmap = {c.lower(): c for c in df.columns}
        if sym_col.lower() in lowmap:
            sym_col = lowmap[sym_col.lower()]
        else:
            print(f"ERROR: symbol column '{args.symbol_col}' not found. Columns: {list(df.columns)}")
            sys.exit(1)

    # ensure output columns exist
    for label in ANCHORS.keys():
        for f in FIELDS:
            col = f"{label}_{f}"
            if col not in df.columns:
                df[col] = pd.NA
    if "massive_error" not in df.columns:
        df["massive_error"] = pd.NA

    symbols = (
        df[sym_col].astype(str).str.strip().str.upper().dropna().unique().tolist()
    )
    if not symbols:
        df.to_csv(args.csv, index=False)
        print(f"Wrote (no symbols found): {args.csv}")
        sys.exit(0)

    ref_dt_utc = (
        datetime.strptime(args.ref_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.ref_date else datetime.now(timezone.utc)
    )

    if args.verbose:
        print(f"Symbols: {len(symbols)} | workers: {args.workers} | ref: {iso(ref_dt_utc)}")

    # fetch & assemble
    results = {}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(per_ticker_job, api_key, sym, ref_dt_utc): sym for sym in symbols}
        for fut in as_completed(futs):
            sym = futs[fut]
            results[sym] = fut.result()

    # build merge frame
    rows = []
    for sym, payload in results.items():
        row = {"_sym_upper": sym}
        for label in ANCHORS.keys():
            for f in FIELDS:
                row[f"{label}_{f}"] = payload.get(f"{label}_{f}")
        row["massive_error"] = payload.get("massive_error")
        rows.append(row)
    res_df = pd.DataFrame(rows)

    # join back, write
    df["_sym_upper"] = df[sym_col].astype(str).str.strip().str.upper()
    if not res_df.empty:
        overwrite_cols = [c for c in res_df.columns if c != "_sym_upper"]
        df = df.drop(columns=[c for c in overwrite_cols if c in df.columns], errors="ignore") \
               .merge(res_df, on="_sym_upper", how="left")
    df = df.drop(columns=["_sym_upper"], errors="ignore")
    df.to_csv(args.csv, index=False)

    print(f"Wrote snapshots to {args.csv}")

if __name__ == "__main__":
    main()