# coc_sources.py — hardened loader/resampler for CoC
# - Intraday (1h/60m) from Yahoo capped to 730d
# - Robust column normalization (handles MultiIndex, Adj Close, Date/Datetime)
# - Safe resample -> 4H with missing-column guards
# - Chooses first non-empty source per TF; falls back 1D -> 4H if needed

import pandas as pd
import yfinance as yf
from binance.client import Client

# ---------- helpers ----------

_COL_MAP = {
    "Open": "open", "High": "high", "Low": "low", "Close": "close", "Adj Close": "close",
    "Volume": "volume", "Datetime": "timestamp", "Date": "timestamp",
    "open": "open", "high": "high", "low": "low", "close": "close", "adj close": "close",
    "volume": "volume", "timestamp": "timestamp", "index": "timestamp",
}

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a frame with ['timestamp','open','high','low','close','volume'] (lowercase),
    timestamp TZ-aware UTC, numeric OHLCV; ok to return empty with those columns."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    # yfinance can return MultiIndex columns (e.g., ('Open','BTC-USD')); drop to first level
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Make timestamp a column
    if df.index.name and "timestamp" not in df.columns:
        df = df.reset_index()

    # Standardize names (case-insensitive)
    df = df.rename(columns=lambda c: _COL_MAP.get(str(c), str(c).lower()))

    # Keep / create required columns
    want = ["timestamp", "open", "high", "low", "close", "volume"]
    have = [c for c in want if c in df.columns]
    df = df[have]
    for c in want:
        if c not in df.columns:
            df[c] = pd.NA

    # Types
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Drop rows without a timestamp
    df = df.dropna(subset=["timestamp"])
    # Ensure column order
    return df[want]


def _finalize(df: pd.DataFrame) -> pd.DataFrame:
    return _normalize_columns(df)


def _resample_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Safe OHLCV resample with guards; returns normalized frame."""
    df = _finalize(df)
    if df.empty:
        return df

    # Ensure required numeric cols exist
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            df[c] = pd.NA

    g = (
        df.set_index("timestamp")
          .resample(rule, label="right", closed="right")
          .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
          .dropna(how="all")
          .reset_index()
    )
    return _finalize(g)


# ---------- data fetchers ----------

def fetch_binance(symbol: str = "BTCUSDT", interval: str = "1h", lookback: str = "5y") -> pd.DataFrame:
    """Binance klines -> normalized DataFrame. Returns empty normalized frame on error."""
    try:
        client = Client()
        kl = client.get_historical_klines(symbol, interval, lookback)
        if not kl:
            return _finalize(None)
        df = pd.DataFrame(
            kl,
            columns=["ts", "o", "h", "l", "c", "v", "x", "q", "n", "takerb", "takerv", "ig"],
        )[["ts", "o", "h", "l", "c", "v"]]
        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return _finalize(df)
    except Exception:
        return _finalize(None)


def fetch_yahoo(ticker: str = "^GSPC", interval: str = "1h", period: str = "5y") -> pd.DataFrame:
    """Yahoo fetch respecting intraday limits (60m/1h max ~730d)."""
    try:
        per = "730d" if interval in ("1h", "60m") else period
        df = yf.download(
            ticker, interval=interval, period=per, progress=False, auto_adjust=False
        )
        if df is None or df.empty:
            return _finalize(None)
        # Ensure timestamp column exists then normalize
        df = df.reset_index()
        return _finalize(df)
    except Exception:
        return _finalize(None)


# ---------- public API ----------

def load_basket_ohlcv(years: int = 5, macro_years: int = 15) -> dict:
    """Return {asset: {tf: DataFrame}} for BTC, ETH (Binance) and SPX, DXY, GOLD, US10Y (Yahoo).
    TFs: 1h, 4h, 1d. Chooses first non-empty source per TF."""
    assets = {
        "BTC": {"binance": "BTCUSDT"},
        "ETH": {"binance": "ETHUSDT"},
        "SPX": {"yahoo": "^GSPC"},
        "DXY": {"yahoo": "UUP"},   # ETF proxy for DXY (intraday more reliable)
        "GOLD": {"yahoo": "GC=F"},
        "US10Y": {"yahoo": "^TNX"},
    }

    data = {}
    for asset, sources in assets.items():
        per_tf = {"1h": pd.DataFrame(), "4h": pd.DataFrame(), "1d": pd.DataFrame()}

        # Binance (native 1h/4h/1d)
        if "binance" in sources:
            try:
                per_tf["1h"] = fetch_binance(sources["binance"], "1h", f"{years}y")
                per_tf["4h"] = fetch_binance(sources["binance"], "4h", f"{years}y")
                per_tf["1d"] = fetch_binance(sources["binance"], "1d", f"{years}y")
            except Exception:
                pass

        # Yahoo (intraday 60m -> resample 4H; 1d is multi-year)
        if "yahoo" in sources:
            try:
                h1 = fetch_yahoo(sources["yahoo"], "1h", f"{years}y")     # internally 730d
                m60 = fetch_yahoo(sources["yahoo"], "60m", f"{years}y")   # 730d
                d1 = fetch_yahoo(sources["yahoo"], "1d", f"{years}y")

                # Use first non-empty
                if per_tf["1h"].empty: per_tf["1h"] = h1
                if per_tf["4h"].empty: per_tf["4h"] = _resample_ohlc(m60, "4H")
                if per_tf["1d"].empty: per_tf["1d"] = d1

                # Fallback: if 4h still empty but 1d exists, coarse resample
                if (per_tf["4h"].empty) and (not per_tf["1d"].empty):
                    per_tf["4h"] = _resample_ohlc(per_tf["1d"], "4H")
            except Exception:
                pass

        # Final normalization (ensures column set even if empty)
        for tf in ("1h", "4h", "1d"):
            per_tf[tf] = _finalize(per_tf.get(tf))

        data[asset] = per_tf

    return data
