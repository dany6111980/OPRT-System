#!/usr/bin/env python3
r"""
OPRT Agent Builder with LIVE technicals (RSI/MACD/EMA) and
DETERMINISTIC tf_alignment for H4 + H1.

- Fetches OHLCV from Binance (ccxt), yfinance fallback
- Computes RSI(14), MACD(12/26/9), EMA 50/200, slope signs
- Writes indicators into each agent JSON
- Sets tf_alignment using rules (2-of-3 majority, slope tiebreakers)

Requires:
  pip install openai>=1.0 ccxt yfinance numpy
  OPENAI_API_KEY in env or set via C:\\OPRT\\config\\openai_key then export.
"""
import os, json, time, random, math, warnings
from datetime import datetime, timezone

# Silence only *very specific* yfinance chatter we already address explicitly.
warnings.filterwarnings(
    "ignore",
    message=".*auto_adjust default to True.*",
    category=FutureWarning,
    module="yfinance"
)

# ---------------- OpenAI client ----------------
try:
    from openai import OpenAI
except Exception as e:
    raise SystemExit(f"[agents_build_openai] OpenAI package missing: {e}")

# ---------------- Data sources ----------------
try:
    import ccxt
except Exception:
    ccxt = None

try:
    import yfinance as yf
except Exception:
    yf = None

import numpy as np

# ---------------- Config ----------------
ASSETS = ["BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y"]
# Which assets get LIVE crypto indicators (others skip)
INDICATOR_ASSETS = set(os.getenv("OPRT_INDICATORS_ASSETS", "BTC,ETH,SOL").split(","))
OUTDIR = r"C:\OPRT\agents"
os.makedirs(OUTDIR, exist_ok=True)

MODEL = os.getenv("OPRT_OPENAI_MODEL", "gpt-4o-mini")

PROMPT_TMPL = """You are Agent {side} for {asset}. Return ONLY a JSON with keys:
tf_alignment {{H4,H1 in ["bull","bear","neutral"]}},
indicators {{rsi{{H4,H1,slope{{H4,H1 in ["+","-"]}}}}, macd{{H4,H1 in ["pos","neg"], hist_slope{{H4,H1 in ["+","-"]}}}}, ema{{H4,H1 in ["50>200","50<200"], slope{{H4,H1 in ["+","-"]}}}}}},
volume {{ratio_1h_to_avg20: number}},
leaders {{ETH: "+"|"-", SOL: "+"|"-", breadth: "risk_on"|"risk_off"}},
flows {{oi:"up"|"down"|"flat", funding:"pos"|"neg"|"flat", liq_skew:"short"|"long"|"flat"}},
sentiment_index (int -3..+3),
levels {{resistance:[], support:[], invalidation:null}},
scenarios:[],
phase_vector (array of 5 floats; mean ~{mean_hint}).
STRICT JSON. No commentary, no code fences."""

# ---------------- Indicator math ----------------
def ema(arr, period):
    arr = np.asarray(arr, dtype=float)
    if len(arr) < period:
        return None
    k = 2.0/(period+1.0)
    e = arr[0]
    out = [e]
    for x in arr[1:]:
        e = x*k + e*(1.0-k)
        out.append(e)
    return np.array(out)

def rsi(arr, period=14):
    arr = np.asarray(arr, dtype=float)
    if len(arr) < period+1:
        return None
    diffs = np.diff(arr)
    gains = np.where(diffs>0, diffs, 0.0)
    losses = np.where(diffs<0, -diffs, 0.0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    rsis = []
    for i in range(period, len(diffs)):
        avg_gain = (avg_gain*(period-1) + gains[i]) / period
        avg_loss = (avg_loss*(period-1) + losses[i]) / period
        if avg_loss == 0:
            rsis.append(100.0)
        else:
            rs = avg_gain/avg_loss
            rsis.append(100.0 - 100.0/(1.0+rs))
    pad = [np.nan]*(len(arr) - len(rsis))
    return np.array(pad + rsis, dtype=float)

def macd(arr, fast=12, slow=26, signal=9):
    arr = np.asarray(arr, dtype=float)
    if len(arr) < slow+signal+5:  # some cushion
        return None, None, None
    ema_fast = ema(arr, fast)
    ema_slow = ema(arr, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

# ---------------- OHLCV fetch ----------------
def fetch_closes_ccxt(asset: str, timeframe: str, limit: int = 300):
    if ccxt is None:
        return None
    sym_map = {"BTC":"BTC/USDT", "ETH":"ETH/USDT", "SOL":"SOL/USDT"}
    symbol = sym_map.get(asset)
    if not symbol:
        return None
    try:
        ex = ccxt.binance({"enableRateLimit": True})
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return [c[4] for c in ohlcv]
    except Exception:
        return None

def fetch_closes_yf(asset: str, interval: str, period: str = "120d"):
    if yf is None:
        return None
    tmap = {"BTC":"BTC-USD", "ETH":"ETH-USD", "SOL":"SOL-USD"}
    ticker = tmap.get(asset)
    if not ticker:
        return None
    try:
        # Explicitly set auto_adjust to avoid FutureWarning and to keep raw closes
        dl = yf.download(
            ticker,
            interval=interval,
            period=period,
            progress=False,
            auto_adjust=False  # keep behavior stable
        )
        if dl is None or dl.empty:
            return None
        return dl["Close"].astype(float).tolist()
    except Exception:
        return None

def get_closes(asset: str, tf_label: str):
    if tf_label == "H1":
        closes = fetch_closes_ccxt(asset, "1h", 300)
        if closes is None:
            closes = fetch_closes_yf(asset, "60m", "180d")
        return closes
    if tf_label == "H4":
        closes = fetch_closes_ccxt(asset, "4h", 300)
        if closes is None:
            closes = fetch_closes_yf(asset, "240m", "720d")
        return closes
    if tf_label == "D1":
        closes = fetch_closes_yf(asset, "1d", "5y")
        return closes
    return None

def compute_indicator_pack(closes):
    if not closes or len(closes) < 210:
        return None
    arr = np.array(closes, dtype=float)

    # RSI
    rsi_vals = rsi(arr, 14)
    rsi_last = float(rsi_vals[-1]) if rsi_vals is not None else float("nan")
    rsi_prev = float(rsi_vals[-2]) if rsi_vals is not None else float("nan")
    rsi_slope = "+" if (not math.isnan(rsi_last) and not math.isnan(rsi_prev) and rsi_last >= rsi_prev) else "-"

    # MACD
    macd_line, signal_line, hist = macd(arr, 12, 26, 9)
    if macd_line is not None and signal_line is not None and hist is not None:
        macd_sign = "pos" if macd_line[-1] >= signal_line[-1] else "neg"
        macd_hist_slope = "+" if hist[-1] >= hist[-2] else "-"
    else:
        macd_sign, macd_hist_slope = "neg", "-"

    # EMA cross & slope
    ema50 = ema(arr, 50)
    ema200 = ema(arr, 200)
    if ema50 is None or ema200 is None:
        ema_cross = "50<200"
        ema50_slope = "-"
    else:
        ema_cross = "50>200" if ema50[-1] >= ema200[-1] else "50<200"
        ema50_slope = "+" if ema50[-1] >= ema50[-2] else "-"

    return {
        "rsi": round(rsi_last, 2) if not math.isnan(rsi_last) else None,
        "rsi_slope": rsi_slope,
        "macd_sign": macd_sign,
        "macd_hist_slope": macd_hist_slope,
        "ema_cross": ema_cross,
        "ema50_slope": ema50_slope,
    }

def build_indicator_block(asset: str):
    # Only for crypto majors listed in INDICATOR_ASSETS
    if asset not in INDICATOR_ASSETS:
        return None

    # include D1 so TF gate (H4=H1=D1) can pass under high CoC profiles
    tfs = ("H4", "H1", "D1")
    out = {
        "rsi":  {"H4": None, "H1": None, "D1": None, "slope": {"H4": "-", "H1": "-", "D1": "-"}},
        "macd": {"H4": "neg", "H1": "neg", "D1": "neg",
                 "hist_slope": {"H4": "-", "H1": "-", "D1": "-"}},
        "ema":  {"H4": "50<200", "H1": "50<200", "D1": "50<200",
                 "slope": {"H4": "-", "H1": "-", "D1": "-"}}
    }

    for tf in tfs:
        closes = get_closes(asset, tf)
        pack = compute_indicator_pack(closes) if closes else None
        if pack:
            out["rsi"][tf] = pack["rsi"]
            out["rsi"]["slope"][tf] = pack["rsi_slope"]
            out["macd"][tf] = pack["macd_sign"]
            out["macd"]["hist_slope"][tf] = pack["macd_hist_slope"]
            out["ema"][tf] = pack["ema_cross"]
            out["ema"]["slope"][tf] = pack["ema50_slope"]
    return out


# -------- Deterministic tf_alignment from indicators --------
def decide_alignment_for_tf(ind, tf: str) -> str:
    """
    Majority rules (2-of-3):
      Bull if RSI>55, MACD pos, EMA 50>200 (>=2 true)
      Bear if RSI<45, MACD neg, EMA 50<200 (>=2 true)
      Else neutral, with slope tiebreakers:
         any '+' slope among RSI or EMA50 tilts bull,
         any '-' slope tilts bear; otherwise neutral.
    """
    rsi_val = ind["rsi"].get(tf)
    macd_sign = ind["macd"].get(tf)
    ema_cross = ind["ema"].get(tf)
    rsi_s = ind["rsi"]["slope"].get(tf, "-")
    ema_s = ind["ema"]["slope"].get(tf, "-")

    bull_votes = 0
    bear_votes = 0
    if rsi_val is not None:
        if rsi_val > 55: bull_votes += 1
        elif rsi_val < 45: bear_votes += 1
    if macd_sign == "pos": bull_votes += 1
    elif macd_sign == "neg": bear_votes += 1
    if ema_cross == "50>200": bull_votes += 1
    elif ema_cross == "50<200": bear_votes += 1

    if bull_votes >= 2: return "bull"
    if bear_votes >= 2: return "bear"

    # Tiebreaker via slopes
    if rsi_s == "+" or ema_s == "+": return "bull"
    if rsi_s == "-" or ema_s == "-": return "bear"
    return "neutral"

def decide_tf_alignment(ind):
    if not ind:  # non-crypto assets
        return {"H4":"neutral","H1":"neutral","D1":"neutral"}
    return {
        "H4": decide_alignment_for_tf(ind, "H4"),
        "H1": decide_alignment_for_tf(ind, "H1"),
        "D1": decide_alignment_for_tf(ind, "D1"),
    }

# ---------------- OpenAI interaction ----------------
def ask(side: str, asset: str, mean_hint: float, client: OpenAI):
    msg = PROMPT_TMPL.format(side=side, asset=asset, mean_hint=mean_hint)
    r = client.chat.completions.create(
        model=MODEL,
        messages=[{"role":"system","content":"Follow the schema precisely and return ONLY JSON."},
                  {"role":"user","content":msg}],
        temperature=0.2
    )
    txt = r.choices[0].message.content.strip()
    if txt.startswith("```"):
        txt = txt.strip("`")
        txt = txt.split("\n",1)[1] if "\n" in txt else txt
    return json.loads(txt)

def ensure_phase(v, bias_mean):
    if not isinstance(v, list) or len(v) != 5:
        base = bias_mean
        return [round(base + random.uniform(-0.05,0.05),4) for _ in range(5)]
    return [float(x) for x in v]

# ---------------- Main ----------------
def main():
    client = OpenAI()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[agents_build_openai] start {ts}, model={MODEL}")

    for asset in ASSETS:
        # 1) ask model for base skeletons (narrative + placeholders)
        A = ask("A (Bull)", asset, 1.08, client)
        B = ask("B (Bear)", asset, 0.92, client)
        A["phase_vector"] = ensure_phase(A.get("phase_vector"), 1.08)
        B["phase_vector"] = ensure_phase(B.get("phase_vector"), 0.92)

        # 2) live indicators (for crypto) and deterministic alignment
        indicators = build_indicator_block(asset)
        if indicators:
            A["indicators"] = indicators
            B["indicators"] = indicators
            tf = decide_tf_alignment(indicators)
            A["tf_alignment"] = tf
            B["tf_alignment"] = tf
        else:
            # non-crypto: keep model's indicators if any, but enforce neutral safety
            A.setdefault("tf_alignment", {"H4":"neutral","H1":"neutral"})
            B.setdefault("tf_alignment", {"H4":"neutral","H1":"neutral"})

        # 3) write files
        pa = os.path.join(OUTDIR, f"{asset}_A.json")
        pb = os.path.join(OUTDIR, f"{asset}_B.json")
        with open(pa,"w",encoding="utf-8") as fa: json.dump(A, fa, ensure_ascii=False, indent=2)
        with open(pb,"w",encoding="utf-8") as fb: json.dump(B, fb, ensure_ascii=False, indent=2)
        print(f"  wrote {pa}  &  {pb}")
        time.sleep(0.25)

    print("[agents_build_openai] done")

if __name__ == "__main__":
    main()
