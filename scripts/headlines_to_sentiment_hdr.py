import os, json, math, re
from datetime import datetime, timezone

DATA_DIR   = r"C:\OPRT\data"
LOG_DIR    = r"C:\OPRT\logs"
HEADLINES  = os.path.join(DATA_DIR, "headlines.csv")     # headerless
OUT_TXT    = os.path.join(DATA_DIR, "sentiment_index.txt")
OUT_JSON   = os.path.join(DATA_DIR, "sentiment_index.json")
LOG_FILE   = os.path.join(LOG_DIR,  "headlines_to_sentiment_hdr.log")

ISO_RE   = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}")
TRAIL_RE = re.compile(r",\s*\d+(?:\.\d+)?,[^,\s]+\s*$")  # ",0,domain"

def now_iso_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_ts(s):
    try:
        s = s.replace(" ", "T")
        if not s.endswith("Z"):
            s += "Z"
        return datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        return None

def score_text(t: str) -> int:
    text = t.lower()
    pos_kw = ("approve","approval","wins","adopt","growth","bull","support","upgrade","record","inflow","build","surge")
    neg_kw = ("reject","ban","hack","exploit","outage","selloff","bear","lawsuit","downgrade","delay","outflow","crash")
    score = 0
    for k in pos_kw:
        if k in text: score += 1
    for k in neg_kw:
        if k in text: score -= 1
    if "etf" in text: score += 1  # institutional weight
    return max(-1, min(1, score))

def read_headerless_lines(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            # expected: "<ts>,<headline>[,extra..]"
            if "," in line:
                ts_str, text = line.split(",", 1)
            else:
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
    num = 0.0; den = 0.0; items = []
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

    # persist main TXT in [-1..+1]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_TXT, "w", encoding="utf-8") as f:
        f.write(f"{idx:.3f}")

    # provide a normalized [-3..+3] variant for analytics/engine if desired
    idx_norm3 = max(-3.0, min(3.0, idx * 3.0))

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"ts_utc": now_iso_utc(), "count": len(rows), "index": idx, "index_norm3": idx_norm3, "sample": items[:12]}, f, ensure_ascii=False, indent=2)

    with open(LOG_FILE, "a", encoding="utf-8") as fp:
        fp.write(f"{now_iso_utc()} headlines={len(rows)} sentiment_index={idx:.3f} index_norm3={idx_norm3:.2f} format=headerless\n")

    # stdout for runner
    print(json.dumps({"sentiment_index": round(idx,3), "index_norm3": round(idx_norm3,2), "headlines": len(rows)}, ensure_ascii=False))

    with open(os.path.join(DATA_DIR, "sentiment_index_norm3.txt"), "w", encoding="utf-8") as f:
        f.write(f"{idx_norm3:.2f}")

if __name__ == "__main__":
    main()
