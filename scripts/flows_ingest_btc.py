#!/usr/bin/env python3
# flows_ingest_btc_closed.py
# Robust BTC flows ingest with LAST-CLOSED candle volume and futures enrichment.
# - Prefers ccxt (Binance spot OHLCV) and uses the **last closed** 1h bar.
# - Falls back to yfinance; also uses the second-to-last (closed) bar.
# - Enriches with Binance futures funding + OI; infers OI direction from state if needed.

import sys, json, os, math
from statistics import mean
from datetime import datetime, timezone

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def write_json(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def using_ccxt_closed(symbol: str = "BTC/USDT", limit: int = 250):
    try:
        import ccxt  # type: ignore
    except Exception:
        return None
    try:
        ex = ccxt.binance({"enableRateLimit": True})
        ohlcv = ex.fetch_ohlcv(symbol, timeframe="1h", limit=limit)
        if not ohlcv or len(ohlcv) < 22:
            return None
        closes = [c[4] for c in ohlcv]
        vols   = [c[5] for c in ohlcv]
        # Use the last **closed** candle at index -2
        vol_curr = float(vols[-2])
        # Average the previous 20 closed candles
        vol_avg20 = float(mean(vols[-22:-2]))
        ratio = (vol_curr / vol_avg20) if vol_avg20 > 0 else None
        price = float(closes[-2])  # price of last closed bar
        return {
            "source": "ccxt.binance",
            "ts_utc": now_iso_utc(),
            "price": price,
            "vol_1h_current": vol_curr,
            "vol_avg20": vol_avg20,
            "volume_ratio": ratio,
            "oi": None,
            "funding": None,
            "liq_skew": None
        }
    except Exception:
        return None

def using_yf_closed(ticker: str = "BTC-USD"):
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return None
    try:
        df = yf.download(ticker, period="4d", interval="1h", progress=False, auto_adjust=False)
        if df is None or df.empty or len(df) < 22 or "Volume" not in df.columns or "Close" not in df.columns:
            return None
        # Use second-to-last row as last closed
        vol_curr = float(df["Volume"].iloc[-2])
        vol_avg20 = float(df["Volume"].iloc[-22:-2].mean())
        ratio = (vol_curr / vol_avg20) if vol_avg20 > 0 else None
        price = float(df["Close"].iloc[-2])
        return {
            "source": "yfinance",
            "ts_utc": now_iso_utc(),
            "price": price,
            "vol_1h_current": vol_curr,
            "vol_avg20": vol_avg20,
            "volume_ratio": ratio,
            "oi": None,
            "funding": None,
            "liq_skew": None
        }
    except Exception:
        return None

def enrich_futures_metrics(data: dict) -> dict:
    try:
        import ccxt  # type: ignore
    except Exception:
        return data

    try:
        fx = ccxt.binance({"options": {"defaultType": "future"}, "enableRateLimit": True})

        funding = None
        for sym in ("BTC/USDT", "BTC/USDT:USDT", "BTC/USDT-PERP"):
            try:
                fr = fx.fetchFundingRate(sym) if hasattr(fx, "fetchFundingRate") else fx.fetch_funding_rate(sym)
                if fr is None: 
                    continue
                val = fr.get("fundingRate")
                if val is None and isinstance(fr.get("info"), dict):
                    val = fr["info"].get("lastFundingRate") or fr["info"].get("fundingRate")
                if val is not None:
                    funding = float(val)
                    break
            except Exception:
                continue

        oi_value = None
        oi_prev = None
        for fn_name in ("fapiPublicGetOpenInterestHist", "fapiV2PublicGetOpenInterestHist"):
            try:
                fn = getattr(fx, fn_name)
                hist = fn({"symbol": "BTCUSDT", "period": "5m", "limit": 2})
                if isinstance(hist, list) and len(hist) >= 2:
                    def _sf(x):
                        try: return float(x)
                        except Exception: return None
                    oi_prev = _sf(hist[-2].get("sumOpenInterest"))
                    oi_value = _sf(hist[-1].get("sumOpenInterest"))
                    break
            except Exception:
                pass

        if oi_value is None:
            for fn_name in ("fapiPublicGetOpenInterest", "fapiV2PublicGetOpenInterest"):
                try:
                    fn = getattr(fx, fn_name)
                    snap = fn({"symbol": "BTCUSDT"})
                    if isinstance(snap, dict) and snap.get("openInterest") is not None:
                        oi_value = float(snap["openInterest"])
                        break
                except Exception:
                    pass

        oi = None
        if oi_value is not None and oi_prev is not None:
            up_th = oi_prev * 1.001
            down_th = oi_prev * 0.999
            if oi_value > up_th: oi = "up"
            elif oi_value < down_th: oi = "down"
            else: oi = "flat"

        liq_skew = None
        if isinstance(funding, (int, float)):
            liq_skew = "short" if funding > 0 else "long"

        data.update({"funding": funding, "oi_value": oi_value, "oi": oi, "liq_skew": liq_skew})
    except Exception:
        pass
    return data

def infer_oi_direction_stateful(data: dict, state_path: str):
    try:
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        prev = None
        try:
            with open(state_path, "r", encoding="utf-8") as sf:
                prev = float(sf.read().strip())
        except Exception:
            prev = None
        cur = data.get("oi_value")
        if isinstance(cur, (int, float)):
            with open(state_path, "w", encoding="utf-8") as sf:
                sf.write(str(cur))
            # If we still don't have 'oi' but we have a previous OI, infer direction with a small threshold
            if data.get("oi") is None and isinstance(prev, (int, float)):
                up_th   = prev * 1.0005
                down_th = prev * 0.9995
                if cur > up_th: data["oi"] = "up"
                elif cur < down_th: data["oi"] = "down"
                else: data["oi"] = "flat"
    except Exception:
        pass
    return data


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="C:/OPRT/data/flows_btc.json")
    ap.add_argument("--last_price", default="C:/OPRT/data/last_price.txt")
    ap.add_argument("--log", default="C:/OPRT/logs/flows_ingest.log")
    ap.add_argument("--oi_state", default="C:/OPRT/data/oi_prev.txt")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    def log(msg: str):
        with open(args.log, "a", encoding="utf-8") as fp:
            fp.write(f"{now_iso_utc()} {msg}\n")

    data = using_ccxt_closed() or using_yf_closed()
    if not data:
        msg = "ERROR: Neither ccxt nor yfinance ingest succeeded (closed bar)."
        log(msg); print(msg, file=sys.stderr); sys.exit(2)

    data = enrich_futures_metrics(data)
    data = infer_oi_direction_stateful(data, args.oi_state)

    # --- classify volume ratio; never fail the pipeline on outliers ---
    ratio = safe_float(data.get("volume_ratio"))
    if ratio is None or not math.isfinite(ratio):
        ratio = None

    if ratio is None:
        ratio_flag = "nan"
    elif ratio < 0.05:
        ratio_flag = "low_outlier"
    elif ratio > 8.0:
        ratio_flag = "high_outlier"
    else:
        ratio_flag = "ok"

    data["ratio_flag"] = ratio_flag

    # Persist outputs
    write_json(args.out, data)
    if data.get("price") is not None:
        write_text(args.last_price, f"{data['price']:.2f}")

    log(f"flows_btc.json written: src={data.get('source')} vol_ratio={ratio} flag={ratio_flag} funding={data.get('funding')} oi={data.get('oi')}")
    print(json.dumps(data, ensure_ascii=False))

    # Exit 0: data written successfully (engine will handle gates/risk)
    sys.exit(0)

if __name__ == "__main__":
    main()
