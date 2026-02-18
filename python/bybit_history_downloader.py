#!/usr/bin/env python3
"""
Bybit Historical Tick Data Downloader and OHLCV Aggregator

Downloads tick data for DELISTED symbols from public.bybit.com and aggregates
it to OHLCV candles.  Active symbols are handled by the C++ downloader.

Delisted symbol list is fetched from the Bybit REST API:
  GET /v5/market/instruments-info  (status=Closed)

Public tick data sources:
  Futures : https://public.bybit.com/trading/{SYMBOL}/{SYMBOL}YYYY-MM-DD.csv.gz
  Spot    : https://public.bybit.com/spot/{SYMBOL}/{SYMBOL}_YYYY-MM-DD.csv.gz

Output format matches crypto_data_downloader:
  Futures : {outdir}/csvFut/{interval}/SYMBOL.csv
  Spot    : {outdir}/csvSpot/{interval}/SYMBOL.csv
  Columns : open_time,open,high,low,close,volume,turnover
  - open_time : milliseconds since UTC epoch
  - volume    : base asset quantity
  - turnover  : USDT value
"""

import argparse
import gzip
import io
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import NamedTuple

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BYBIT_API = "https://api.bybit.com"

FUTURES_PUBLIC_URL = "https://public.bybit.com/trading/"
SPOT_PUBLIC_URL    = "https://public.bybit.com/spot/"

# Bybit interval string → (csvFut/csvSpot subdirectory, pandas resample freq)
INTERVAL_MAP: dict[str, tuple[str, str]] = {
    "1":  ("1m", "1min"),
    "60": ("1h", "60min"),
}


class MarketConfig(NamedTuple):
    category:    str   # Bybit API category: "linear" | "spot"
    public_url:  str   # base URL for public tick data
    csv_dir:     str   # output subdirectory: "csvFut" | "csvSpot"
    # timestamp unit in tick CSVs: "s" (futures) | "ms" (spot)
    ts_unit:     str


FUTURES_CONFIG = MarketConfig(
    category="linear",
    public_url=FUTURES_PUBLIC_URL,
    csv_dir="csvFut",
    ts_unit="s",
)
SPOT_CONFIG = MarketConfig(
    category="spot",
    public_url=SPOT_PUBLIC_URL,
    csv_dir="csvSpot",
    ts_unit="ms",
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-data-downloader/1.0"})
PRINT_LOCK = Lock()


def log(*args, **kwargs) -> None:
    """Thread-safe print."""
    with PRINT_LOCK:
        print(*args, **kwargs)


# ---------------------------------------------------------------------------
# Bybit REST API — delisted symbol discovery
# ---------------------------------------------------------------------------

def get_delisted_symbols(cfg: MarketConfig) -> set[str]:
    """
    Fetch all symbols with status=Closed from the Bybit REST API.
    For futures: only USDT-margined linear perpetuals.
    """
    symbols: set[str] = set()
    cursor = ""

    while True:
        params: dict[str, str | int] = {
            "category": cfg.category,
            "status":   "Closed",
            "limit":    1000,
        }
        if cursor:
            params["cursor"] = cursor

        resp = SESSION.get(
            f"{BYBIT_API}/v5/market/instruments-info",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()

        if body.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {body.get('retMsg')}")

        result = body["result"]
        for item in result.get("list", []):
            if cfg.category == "linear":
                # Only USDT-margined perpetuals (exclude quarterly futures)
                if (item.get("quoteCoin") == "USDT"
                        and item.get("contractType") == "LinearPerpetual"):
                    symbols.add(item["symbol"])
            else:
                symbols.add(item["symbol"])

        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break

    return symbols


# ---------------------------------------------------------------------------
# Public bybit.com — directory listing
# ---------------------------------------------------------------------------

def fetch_html(url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc
            wait = 2 ** attempt
            log(f"  Retry {attempt + 1}/{retries} after {wait}s: {exc}")
            time.sleep(wait)
    assert False, "unreachable"


def fetch_bytes(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=120)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Failed to download {url}: {exc}") from exc
            wait = 2 ** attempt
            log(f"  Retry {attempt + 1}/{retries} after {wait}s: {exc}")
            time.sleep(wait)
    assert False, "unreachable"


def parse_hrefs(html: str) -> list[str]:
    return re.findall(r'href="([^"]+)"', html)


def list_available_symbols(cfg: MarketConfig) -> set[str]:
    """List all symbol directories present on public.bybit.com."""
    html = fetch_html(cfg.public_url)
    symbols: set[str] = set()
    for href in parse_hrefs(html):
        if href.endswith('/') and not href.startswith(('?', '/', '.')):
            symbols.add(href.rstrip('/'))
    return symbols


def list_symbol_files(symbol: str, cfg: MarketConfig) -> list[tuple[str, str]]:
    """
    Return sorted list of (filename, date_str 'YYYY-MM-DD') for daily .csv.gz
    files available for a symbol.  Monthly aggregate files are skipped.
    """
    html = fetch_html(f"{cfg.public_url}{symbol}/")
    result: list[tuple[str, str]] = []
    for href in parse_hrefs(html):
        fname = href.split('/')[-1]
        if not fname.endswith('.csv.gz'):
            continue
        # Spot monthly files look like BTCUSDT-2022-11.csv.gz — skip them
        # Daily files: futures=BTCUSDT2024-01-01.csv.gz  spot=BTCUSDT_2024-01-01.csv.gz
        m = re.search(r'(\d{4}-\d{2}-\d{2})', fname)
        if m:
            result.append((fname, m.group(1)))
    return sorted(result, key=lambda x: x[1])


# ---------------------------------------------------------------------------
# Tick data download and OHLCV aggregation
# ---------------------------------------------------------------------------

def download_ticks(symbol: str, filename: str, cfg: MarketConfig) -> pd.DataFrame:
    """Download, decompress and parse one tick data .csv.gz file."""
    url = f"{cfg.public_url}{symbol}/{filename}"
    raw = fetch_bytes(url)

    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
        raw_df = pd.read_csv(gz)

    if cfg.category == "linear":
        # Futures columns: timestamp(s), price, homeNotional, foreignNotional
        df = raw_df[['timestamp', 'price', 'homeNotional', 'foreignNotional']].copy()
        df = df.apply(pd.to_numeric, errors='coerce').dropna()
        # Convert seconds → milliseconds for uniform internal representation
        df['timestamp'] = df['timestamp'] * 1000
        df.rename(columns={'homeNotional': 'volume', 'foreignNotional': 'turnover'},
                  inplace=True)
    else:
        # Spot columns: id, timestamp(ms), price, volume, side
        df = raw_df[['timestamp', 'price', 'volume']].copy()
        df = df.apply(pd.to_numeric, errors='coerce').dropna()
        # Compute turnover from price × volume
        df['turnover'] = df['price'] * df['volume']

    return df.sort_values('timestamp').reset_index(drop=True)


def aggregate_ticks(df: pd.DataFrame, bybit_interval: str) -> pd.DataFrame:
    """
    Aggregate tick DataFrame to OHLCV candles.

    Input `df` must have columns: timestamp (ms), price, volume, turnover.

    Returns DataFrame: open_time (ms), open, high, low, close, volume, turnover.
    """
    _, freq = INTERVAL_MAP[bybit_interval]

    # timestamp is in ms internally
    idx = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    price   = pd.Series(df['price'].values,    index=idx, name='price')
    volume  = pd.Series(df['volume'].values,   index=idx, name='volume')
    turnover = pd.Series(df['turnover'].values, index=idx, name='turnover')

    rs_kw = dict(closed='left', label='left')
    ohlcv = pd.DataFrame({
        'open':     price.resample(freq, **rs_kw).first(),
        'high':     price.resample(freq, **rs_kw).max(),
        'low':      price.resample(freq, **rs_kw).min(),
        'close':    price.resample(freq, **rs_kw).last(),
        'volume':   volume.resample(freq, **rs_kw).sum(),
        'turnover': turnover.resample(freq, **rs_kw).sum(),
    })

    ohlcv.dropna(subset=['open'], inplace=True)
    ohlcv = ohlcv[ohlcv['volume'] > 0].copy()

    # DatetimeIndex (nanoseconds) → milliseconds
    ohlcv.index = (ohlcv.index.astype('int64') // 1_000_000)
    ohlcv.index.name = 'open_time'
    return ohlcv.reset_index()


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def read_last_open_time(csv_path: Path) -> int | None:
    """Return the last open_time (ms) from an existing CSV, or None."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return None
    try:
        df = pd.read_csv(csv_path, usecols=['open_time'])
        return int(df['open_time'].max()) if not df.empty else None
    except Exception:
        return None


def ms_to_date_str(ms: int) -> str:
    """Convert millisecond timestamp to 'YYYY-MM-DD' (UTC)."""
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d')


# ---------------------------------------------------------------------------
# Per-symbol update
# ---------------------------------------------------------------------------

def update_symbol(
    symbol: str,
    intervals: list[str],
    outdir: Path,
    cfg: MarketConfig,
    include_today: bool = False,
) -> None:
    prefix = f"[{symbol}]"
    try:
        all_files = list_symbol_files(symbol, cfg)
    except Exception as exc:
        log(f"{prefix} ERROR listing files: {exc}")
        return

    if not all_files:
        log(f"{prefix} No files available on public.bybit.com.")
        return

    today = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')
    if not include_today:
        all_files = [(f, d) for f, d in all_files if d < today]

    if not all_files:
        log(f"{prefix} No completed data files.")
        return

    # Per-interval: determine the last processed date and which new files are needed
    last_open_times: dict[str, int | None] = {}
    interval_start_dates: dict[str, str] = {}

    for iv in intervals:
        dir_name, _ = INTERVAL_MAP[iv]
        csv_path = outdir / cfg.csv_dir / dir_name / f"{symbol}.csv"
        last_ms = read_last_open_time(csv_path)
        last_open_times[iv] = last_ms

        if last_ms is not None:
            # Past-day files are immutable — skip up to and including last date
            interval_start_dates[iv] = ms_to_date_str(last_ms)
        else:
            interval_start_dates[iv] = all_files[0][1]

    # Union of dates needed across all intervals (strictly newer than last processed)
    needed_dates: set[str] = set()
    for iv in intervals:
        start = interval_start_dates[iv]
        needed_dates.update(d for _, d in all_files if d > start)

    files_to_get = [(f, d) for f, d in all_files if d in needed_dates]

    if not files_to_get:
        log(f"{prefix} Already up to date.")
        return

    log(f"{prefix} Downloading {len(files_to_get)} file(s)...")
    tick_frames: list[pd.DataFrame] = []
    for fname, _date in files_to_get:
        try:
            ticks = download_ticks(symbol, fname, cfg)
            log(f"{prefix}   {fname}: {len(ticks):,} ticks")
            tick_frames.append(ticks)
        except Exception as exc:
            log(f"{prefix}   {fname}: ERROR: {exc}")

    if not tick_frames:
        return

    new_ticks = (pd.concat(tick_frames, ignore_index=True)
                 .sort_values('timestamp')
                 .reset_index(drop=True))

    # Aggregate and save for each interval
    for iv in intervals:
        dir_name, _ = INTERVAL_MAP[iv]
        csv_path = outdir / cfg.csv_dir / dir_name / f"{symbol}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        last_ms = last_open_times[iv]
        start_date = interval_start_dates[iv]

        # Filter: only ticks strictly after last processed date
        cutoff_ms = (pd.Timestamp(start_date, tz='UTC').timestamp() + 86400) * 1000
        iv_ticks = new_ticks[new_ticks['timestamp'] >= cutoff_ms]
        if iv_ticks.empty:
            continue

        new_candles = aggregate_ticks(iv_ticks, iv)
        if new_candles.empty:
            continue

        if csv_path.exists() and last_ms is not None:
            existing = pd.read_csv(csv_path)
            combined = pd.concat([existing, new_candles], ignore_index=True)
        else:
            combined = new_candles

        combined = (combined
                    .sort_values('open_time')
                    .drop_duplicates(subset='open_time', keep='last')
                    .reset_index(drop=True))
        combined.to_csv(csv_path, index=False)
        rel = csv_path.relative_to(outdir)
        log(f"{prefix}   [{iv:>3}] {len(combined):>8,} candles → {rel}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download Bybit historical tick data for DELISTED symbols and "
            "aggregate to OHLCV candles."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Available intervals: " + ", ".join(INTERVAL_MAP.keys()) + "\n\n"
            "Examples:\n"
            "  # All delisted futures, all intervals\n"
            "  %(prog)s -o /data/crypto --type futures\n\n"
            "  # All delisted spot, 1h and 1d only\n"
            "  %(prog)s -o /data/crypto --type spot -i 60 D\n\n"
            "  # Specific symbols (override API lookup)\n"
            "  %(prog)s -o /data/crypto --type futures -s LUNA2USDT FTMUSDT\n"
        ),
    )
    parser.add_argument(
        '-o', '--outdir', required=True,
        help="Output base directory (same as C++ downloader's working directory)",
    )
    parser.add_argument(
        '--type', choices=['futures', 'spot'], default='futures',
        help="Market type to download (default: futures)",
    )
    parser.add_argument(
        '-i', '--interval', nargs='+', default=list(INTERVAL_MAP.keys()),
        choices=list(INTERVAL_MAP.keys()), metavar='INTERVAL',
        help="Interval(s) to generate (default: all)",
    )
    parser.add_argument(
        '-s', '--symbol', nargs='*',
        help="Specific symbol(s) to download (bypasses API lookup)",
    )
    parser.add_argument(
        '--include-today', action='store_true',
        help="Include today's potentially incomplete data file",
    )
    parser.add_argument(
        '-j', '--jobs', type=int, default=4,
        help="Number of parallel download threads (default: 4)",
    )
    args = parser.parse_args()

    cfg = FUTURES_CONFIG if args.type == 'futures' else SPOT_CONFIG
    outdir = Path(args.outdir).resolve()

    if args.symbol:
        symbols = list(args.symbol)
        log(f"Processing {len(symbols)} specified {args.type} symbol(s).")
    else:
        log(f"Fetching delisted {args.type} symbols from Bybit API...")
        delisted = get_delisted_symbols(cfg)
        log(f"  {len(delisted)} delisted symbols found.")

        log(f"Fetching available symbols from public.bybit.com...")
        available = list_available_symbols(cfg)
        log(f"  {len(available)} symbols available for download.")

        symbols = sorted(delisted & available)
        log(f"  {len(symbols)} delisted symbols with public data → processing.")

    total = len(symbols)
    if total == 0:
        log("Nothing to do.")
        return

    completed = 0

    def run(sym: str) -> None:
        update_symbol(sym, args.interval, outdir, cfg, args.include_today)

    with ThreadPoolExecutor(max_workers=args.jobs) as executor:
        futures = {executor.submit(run, sym): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            completed += 1
            try:
                future.result()
            except Exception as exc:
                log(f"[{sym}] FAILED: {exc}")
            log(f"Progress: {completed}/{total}")

    log("\nFinished.")


if __name__ == '__main__':
    main()
