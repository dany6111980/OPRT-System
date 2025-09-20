#!/usr/bin/env python3
"""
Headlines â†’ sentiment_index for OPRT.
- Pulls from several RSS feeds (crypto + macro).
- Filters to last 24h, basic de-duplication.
- Heuristic sentiment: +1 bullish, -1 bearish, 0 neutral (keyword buckets).
- Writes:
    C:\OPRT\data\headlines.csv  (ISO8601,title,score,source)
    C:\OPRT\data\sentiment_index.txt  (single float/integer)
"""
import os, re, time, math, json
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import urllib.request

ROOT   = r"C:\OPRT"
DATA   = os.path.join(ROOT, "data")
os.makedirs(DATA, exist_ok=True)
OUT_CSV = os.path.join(DATA, "headlines.csv")
OUT_SI  = os.path.join(DATA, "sentiment_index.txt")

# â€”â€”â€” feeds (feel free to add/remove) â€”â€”â€”
FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss",
    "https://news.google.com/rss/search?q=bitcoin+OR+crypto&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=stocks+futures+OR+nasdaq+OR+sp500&hl=en-US&gl=US&ceid=US:en",
]

BULL = re.compile(r"\b(etf inflow|spot etf buys|approval|adopt|accumulate|longs rise|risk-on|rally|breaks out|bullish|tops inflow|buyback|cuts rates|rate cut|eases policy|institutional buy)\b", re.I)
BEAR = re.compile(r"\b(outflow|ban|restrict|probe|hack|exploit|selloff|liquidations|risk-off|bearish|rate hike|tighten policy|defaults|bankruptcy|lawsuit)\b", re.I)

def fetch(url, timeout=15):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()

def parse_rss(xml_bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    ns = {"dc":"http://purl.org/dc/elements/1.1/"}  # sometimes used for date
    items = []
    for it in root.findall(".//item"):
        title = (it.findtext("title") or "").strip()
        pub = it.findtext("pubDate") or it.findtext("dc:date", namespaces=ns) or ""
        link = (it.findtext("link") or "").strip()
        items.append((title, pub, link))
    return items

def score_title(title: str) -> int:
    t = title.lower()
    if BULL.search(t) and not BEAR.search(t): return +1
    if BEAR.search(t) and not BULL.search(t): return -1
    return 0

def to_iso(pub_text: str) -> str:
    # best-effort; if parsing fails, use now
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub_text)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

def within_24h(iso_str: str) -> bool:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - dt <= timedelta(hours=24)
    except Exception:
        return True

def main():
    rows = []
    seen = set()
    for url in FEEDS:
        try:
            xmlb = fetch(url)
            for title, pub, link in parse_rss(xmlb):
                if not title: continue
                iso = to_iso(pub)
                if not within_24h(iso): continue
                key = title.lower()
                if key in seen: continue
                seen.add(key)
                score = score_title(title)
                src = urlparse(url).netloc.split(":")[0]
                rows.append((iso, title.replace("\n"," ").strip(), score, src))
        except Exception as e:
            # soft-fail; keep going
            pass

    # keep latest 100
    rows.sort(key=lambda r: r[0], reverse=True)
    rows = rows[:100]

    # write CSV
    with open(OUT_CSV, "w", encoding="utf-8") as f:
        for iso, title, score, src in rows:
            # iso,title,score,source
            title = title.replace('"','').replace(",",";")
            f.write(f"{iso},{title},{score},{src}\n")

    # aggregate sentiment index
    if rows:
        s = sum(r[2] for r in rows)
        # clamp to [-6, +6] for stability
        s = max(-6, min(6, s))
    else:
        s = 0

    with open(OUT_SI, "w", encoding="utf-8") as f:
        f.write(str(s))

    print(f"[headlines_ingest] wrote {OUT_CSV} ({len(rows)} rows), sentiment_index={s}")

if __name__ == "__main__":
    main()
