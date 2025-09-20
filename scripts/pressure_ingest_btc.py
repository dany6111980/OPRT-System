#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
OPRT :: pressure_ingest_btc.py
Computes intrahour "pressure" P \in [-1, 1] and writes to C:\OPRT\data\pressure_btc.json.

Inputs (best-effort, all optional):
- C:\OPRT\data\flows_btc.json  (expected keys used if present):
    {
      "liq_skew": "short" | "long" | "neutral",
      "funding": <float>,                # positive -> longs pay shorts (bull-biased)
      "vol_lh_current": <float>,         # last hour volume (optional)
      "vol_avg20": <float>,              # 20h average volume (optional)
      "price": <float>                   # for display only
    }
- C:\OPRT\data\sentiment_index.txt      # single float value (can be negative/small)

Outputs:
- C:\OPRT\data\pressure_btc.json:
  {
    "pressure": P,
    "components": {
        "sentiment_score": ...,
        "funding_score": ...,
        "skew_score": ...,
        "volume_gate": ...,
        "vol_ratio": ...
    },
    "source": {... minimal echo of flows/sent}
  }

CLI (optional):
  python C:\OPRT\scripts\pressure_ingest_btc.py --root C:\OPRT
"""

import argparse
import json
import math
from pathlib import Path


def _sigmoid(x: float, k: float = 4.0):
    try:
        return 1.0 / (1.0 + math.exp(-k * x))
    except OverflowError:
        return 1.0 if x > 0 else 0.0


def _read_json(p: Path):
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_sentiment(p: Path):
    if not p.exists():
        return 0.0
    try:
        return float(p.read_text(encoding="utf-8").strip())
    except Exception:
        return 0.0


def compute_pressure(ROOT: Path):
    DATA = ROOT / "data"
    OUTF = DATA / "pressure_btc.json"
    FLOWS = DATA / "flows_btc.json"
    SENTF = DATA / "sentiment_index.txt"

    flows = _read_json(FLOWS)
    sent_val = _read_sentiment(SENTF)

    # --- Components ---------------------------------------------------------
    # Sentiment: assume ~[-3..+3] typical, softly squash to [0..1]
    sent_score = _sigmoid(sent_val / 3.0, k=3.0)  # 0.5 is neutral

    # Funding to [0..1], stronger tail if extreme
    funding = float(flows.get("funding", 0.0) or 0.0)
    funding_score = _sigmoid(funding, k=3.0)     # 0.5 neutral

    # Liquidity skew â†’ longs under pressure if "short" (bullish)
    liq_skew = str(flows.get("liq_skew", "neutral") or "neutral").lower()
    skew_score = {
        "short": 0.75,   # short skew â†’ bull tilt
        "neutral": 0.50,
        "flat": 0.50,
        "long": 0.25     # long skew â†’ bear tilt
    }.get(liq_skew, 0.50)

        # Optional: volume gate (discourage strong P if volume is poor)
    # Accept both 'vol_lh_current' and 'vol_1h_current'; prefer direct 'volume_ratio' if present.
    vol = float((flows.get("vol_lh_current") or flows.get("vol_1h_current") or 0.0) or 0.0)
    v20 = float(flows.get("vol_avg20", 0.0) or 0.0)
    vol_ratio = flows.get("volume_ratio")
    try:
        vol_ratio = float(vol_ratio)
    except Exception:
        vol_ratio = (vol / v20) if (vol > 0 and v20 > 0) else 1.0

    if vol_ratio >= 1.3:
        volume_gate = 1.00
    elif vol_ratio >= 1.2:
        volume_gate = 0.95
    elif vol_ratio >= 1.0:
        volume_gate = 0.80
    else:
        volume_gate = 0.65


    # --- Blend to "bull energy" in [0..1] -----------------------------------
    # Weighting: sentiment 0.4, skew 0.3, funding 0.3
    bull_energy = 0.4 * sent_score + 0.3 * skew_score + 0.3 * funding_score
    # Convert to pressure in [-1,1]: P = 2*bull - 1
    P = 2.0 * bull_energy - 1.0
    # Apply volume gate softly to avoid overread during quiet hours
    P_eff = max(-1.0, min(1.0, P * volume_gate))

    out = {
        "pressure": round(P_eff, 6),
        "components": {
            "sentiment_score": round(sent_score, 6),
            "funding_score": round(funding_score, 6),
            "skew_score": round(skew_score, 6),
            "volume_gate": round(volume_gate, 6),
            "vol_ratio": round(vol_ratio, 6),
        },
        "source": {
            "liq_skew": liq_skew,
            "funding": funding,
            "sentiment_index": sent_val,
            "price": flows.get("price", None),
        },
    }
    OUTF.parent.mkdir(parents=True, exist_ok=True)
    OUTF.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))  # keep stdout JSON for caller visibility


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"C:\OPRT")
    args = ap.parse_args()
    ROOT = Path(str(args.root).replace("\\", "/"))
    compute_pressure(ROOT)


if __name__ == "__main__":
    main()
