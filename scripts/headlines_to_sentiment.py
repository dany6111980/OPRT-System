#!/usr/bin/env python3
# headlines_to_sentiment_hdr.py
# Robust scorer for your *headerless* headlines.csv format:
#   2025-08-29T19:34:06+00:00,How CME crypto futures records reflect broader altcoin demand â€“ Blockworks,0,news.google.com
# Parses the first ISO timestamp, takes everything after the first comma as text,
# strips common trailing ",0,<domain>" parts, computes a 24h-decayed sentiment index,
# and writes C:\OPRT\data\sentiment_index.txt (+ snapshot JSON & log).
import os, json, math, sys, re
from datetime import datetime, timezone

DATA_DIR = "C:/OPRT/data"
LOG_DIR  = "C:/OPRT/logs"
HEADLINES = os.path.join(DATA_DIR, "headlines.csv")
OUT_TXT   = os.path.join(DATA_DIR, "sentiment_index.txt")
OUT_JSON  = os.path.join(DATA_DIR, "sentiment_snapshot.json")
LOG_FILE  = os.path.join(LOG_DIR, "headlines_ingest.log")

ISO_RE = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})?')
TRAIL_RE = re.compile(r'(?:,\s*0\s*,\s*[A-Za-z0-9\.\-]+(?:\.[A-Za-z]{2,})+)\s*$')

BULL = [
    "etf inflow","net inflow","approval","approves","approved","buyback","partnership",
    "surge","rally","breakout","accumulate","adoption","blackrock","fidelity","inflows",
    "upgrade","bullish","longs increase","institutional","allocation","treasury buy",
    "supply shock","halving","short covering","reversal higher","broke above","record high",
    "demand rises","strong demand","beats","tops","growth accelerates"
]
BEAR = [
    "hack","exploit","outflow","net outflow","ban","lawsuit","investigation","shutdown",
    "default","liquidation","selloff","dump","crackdown","restriction","bankruptcy",
    "sec sues","delist","miner capitulation","rug","downgrade","bearish","longs liquidated",
    "reversal lower","broke below","record low","slide","losses","slump","plunge","misses"
]

def now_iso_utc(): return datetime.now(timezone.utc).isoformat()

def parse_ts(s: str):
    s = s.strip()
    if s.endswith("Z"): s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def score_text(t: str) -> int:
    txt = (t or "").lower()
    bull = any(k in txt for k in BULL)
    bear = any(k in txt for k in BEAR)
    if bull and not bear: return +1
    if bear and not bull: return -1
    return 0

def read_headerless_lines(path: str):
    rows = []
    if not os.path.exists(path): return rows
    with open(path, "r", encoding="utf-8", newline="") as f:
        for raw in f:
            line = raw.strip()
            if not line: continue
            # split only on the first comma (timestamp delimiter)
            parts = line.split(",", 1)
            if len(parts) < 2: continue
            ts_str, text = parts[0].strip(), parts[1].strip()
            if not ISO_RE.match(ts_str):
                # try to find iso-like token at start anyway
                m = ISO_RE.search(line)
                if not m: continue
                ts_str = m.group(0)
                text = line[m.end():].lstrip(", ")
            # strip trailing ",0,domain" if present
            text = TRAIL_RE.sub("", text)
            # light normalization
            text = text.replace("â€“", " ").replace("â€”", " ").replace(";", " ").strip()
            rows.append({"ts": parse_ts(ts_str), "text": text})
    return rows

def compute_index(rows, half_life_h=24.0, default_age_h=12.0):
    if not rows: return 0.0, []
    lam = math.log(2.0) / half_life_h
    now = datetime.now(timezone.utc)
    num=0.0; den=0.0; items=[]
    for r in rows:
        s = score_text(r["text"])
        if s == 0: continue
        age_h = ((now - r["ts"]).total_seconds()/3600.0) if r["ts"] else default_age_h
        w = math.exp(-lam * age_h)
        num += w * s; den += w
        items.append({"text": r["text"][:180], "score": s, "age_h": round(age_h,2), "w": round(w,4)})
    idx = (num/den) if den>0 else 0.0
    return max(-1.0, min(1.0, idx)), items

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    rows = read_headerless_lines(HEADLINES)
    idx, items = compute_index(rows)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_TXT, "w", encoding="utf-8") as f: f.write(f"{idx:.3f}")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"ts_utc": now_iso_utc(), "count": len(rows), "index": idx, "sample": items[:12]}, f, ensure_ascii=False, indent=2)
    with open(LOG_FILE, "a", encoding="utf-8") as fp:
        fp.write(f"{now_iso_utc()} headlines={len(rows)} sentiment_index={idx:.3f} format=headerless\n")
    print(json.dumps({"sentiment_index": round(idx,3), "headlines": len(rows)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
