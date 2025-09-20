#!/usr/bin/env python3
# OPRT Sweet-Spot Finder — targets 65–75% over last H hours (default 48h)
import json, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd
import argparse

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"C:\OPRT")
    ap.add_argument("--hours", type=int, default=48, help="lookback hours (default 48)")
    ap.add_argument("--friction_bps", type=float, default=5.0)
    ap.add_argument("--min_labelled", type=int, default=20, help="min rows required to accept a config")
    ap.add_argument("--experiment_id", default=None, help="optional filter to a specific variant id")
    ap.add_argument("--outname", default="best_params_48h.json")
    return ap.parse_args()

def load_jsonl(p: Path) -> pd.DataFrame:
    rows=[]
    if not p.exists(): return pd.DataFrame()
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s=line.strip()
            if not s: continue
            try: rows.append(json.loads(s))
            except: pass
    return pd.DataFrame(rows)

def prep_df(root: Path, hours: int) -> pd.DataFrame:
    logs = root / "logs"
    dec  = logs / "mirror_loop_unified_decisions.jsonl"
    csvp = logs / "mirror_loop_unified_run.csv"

    df = load_jsonl(dec)
    if df.empty: return df
    df = df.copy()
    df["ts"] = pd.to_datetime(df.get("timestamp_utc"), errors="coerce", utc=True)
    end = datetime.now(timezone.utc); start = end - timedelta(hours=hours)
    df = df[(df["ts"]>=start) & (df["ts"]<end)].copy()

    # DEDUP per snapshot (prevents multi-writes inflating counts)
    if not df.empty:
        df = df.drop_duplicates(subset=[
            'timestamp_utc','signal','mode','size_band','price','C_eff','phase_angle_deg'
        ], keep='last')

    # Fill price from CSV if missing
    if "price" not in df.columns and csvp.exists():
        try:
            csv_df = pd.read_csv(csvp)
            csv_df["ts"] = pd.to_datetime(csv_df["timestamp_utc"], errors="coerce", utc=True)
            keep = csv_df[["ts","price"]].dropna()
            df = df.merge(keep, on="ts", how="left", suffixes=("","_csv"))
            df["price"] = df["price"].fillna(df["price_csv"])
            df.drop(columns=["price_csv"], inplace=True, errors="ignore")
        except Exception:
            pass

    df = df.sort_values("ts").reset_index(drop=True)
    for h in (1,4,12):
        df[f"price_next_{h}h"] = df["price"].shift(-h)
    return df

def within(x, lo, hi):
    try: 
        x = float(x)
        return (x>=float(lo)) and (x<=float(hi))
    except: 
        return False

def label_hit(row, friction_bps: float=5.0) -> Tuple[float, float]:
    """Return (pnl_1h_bps, hit_1h) where hit_1h is 1/0/np.nan."""
    sig = row.get("signal")
    p0, p1 = row.get("price"), row.get("price_next_1h")
    try:
        p0f=float(p0); p1f=float(p1)
        if not np.isfinite(p0f) or not np.isfinite(p1f) or p0f==0.0:
            return (np.nan, np.nan)
    except Exception:
        return (np.nan, np.nan)

    raw = ((p1f/p0f)-1.0)*1e4 - float(friction_bps)
    if sig == "BUY":
        pnl = raw
        hit = float(1.0 if pnl>0 else 0.0)
    elif sig == "SELL":
        pnl = raw  # SELL wins if pnl>0 after sign flip in sim below (we’ll flip earlier)
        # For symmetry, treat SELL as positive when price went down; we already flip later.
        hit = float(1.0 if pnl>0 else 0.0)
    else:
        return (np.nan, np.nan)
    return (pnl, hit)

def row_full_thr(r, default_active=66.0, default_quiet=70.0) -> float:
    # Prefer the engine's own threshold if present
    try:
        thr = r.get("checks_values", {}).get("ceff", {}).get("thr")
        if thr is not None: return float(thr)
    except Exception: pass
    vr = float(r.get("volume_ratio", 1.0) or 1.0)
    return float(default_active if vr >= 1.0 else default_quiet)

def simulate(df: pd.DataFrame, params: dict, friction_bps: float) -> Tuple[pd.DataFrame, float]:
    """Return (labelled_df, hit_1h_pct) for a param set applied to the *existing directions*."""
    if df.empty: return pd.DataFrame(), None
    s = df.copy()

    # SIDE (direction) taken from engine signal (BUY/SELL only)
    side_ok = s["signal"].isin(["BUY","SELL"])

    # STRONG mask: replicate policy gates with candidate thresholds
    phase_ok = s["phase_angle_deg"].apply(lambda a: within(a, params["angle_min"], params["angle_max"]))
    trap_ok  = ~( (s.get("trap_T",1.0).astype(float) > params["trap_cutoff"]) & (~s.get("herald_ok",False).astype(bool)) )

    # C_eff entry uses either row-aware thr or candidate full_cut (pick stricter of the two to avoid overfit)
    ce_row = s.get("C_eff",-1).astype(float)
    ce_thr_row = s.apply(lambda r: row_full_thr(r), axis=1).astype(float)
    ce_thr_use = np.maximum(ce_thr_row.values, float(params["full_cut"]))
    ce_ok  = ce_row.values >= ce_thr_use

    strong_mask = side_ok & phase_ok & trap_ok & ce_ok

    # LITE rescue (Half) — only when strong failed
    lite_angle_ok = s["phase_angle_deg"].apply(lambda a: within(a, params["lite_angle_min"], params["lite_angle_max"]))
    lite_vol_ok   = s.get("volume_ratio",0).astype(float) >= params["lite_vol"]
    lite_herald   = (s.get("herald_ok",False).astype(bool)) | (s.get("leaders_ok",False).astype(bool)) | (s.get("flows_ok",False).astype(bool))
    lite_mask     = (~strong_mask) & side_ok & lite_angle_ok & lite_vol_ok & lite_herald & trap_ok

    selected = s[strong_mask | lite_mask].copy()
    if selected.empty: 
        return selected, None

    # Label hits (direction already decided by engine’s signal)
    pnls, hits = [], []
    for _, r in selected.iterrows():
        pnl, hit = label_hit(r, friction_bps=friction_bps)
        # Flip sign for SELL so that positive means "correct"
        if r.get("signal")=="SELL" and np.isfinite(pnl):
            pnl = abs(pnl)
        pnls.append(pnl); hits.append(hit)
    selected["pnl_1h_bps"] = pnls
    selected["hit_1h"] = hits

    lab = selected[selected["hit_1h"].notna()]
    hit = float(lab["hit_1h"].mean()*100) if len(lab)>0 else None
    return lab, hit

def pick_config(results: List[Dict[str,Any]], target=(65.0,75.0), min_n=20):
    lo, hi = target
    # prefer inside 65–75 band with n>=min_n, closest to 70, then larger N
    in_band = [r for r in results if (r["hit"] is not None and lo<=r["hit"]<=hi and r["n"]>=min_n)]
    if in_band:
        in_band.sort(key=lambda r: (abs(r["hit"]-70.0), -r["n"]))
        return in_band[0]
    # else closest to 70 with n>=min_n
    cand = [r for r in results if r["hit"] is not None and r["n"]>=min_n]
    if not cand: return None
    cand.sort(key=lambda r: (abs(r["hit"]-70.0), -r["n"]))
    return cand[0]

def main():
    args = parse_args()
    ROOT = Path(str(args.root).replace("\\","/"))
    OUTD = ROOT / "tuning"
    OUTD.mkdir(parents=True, exist_ok=True)

    df = prep_df(ROOT, args.hours)
    if df.empty:
        out = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "hours": args.hours, "status":"no_rows_in_window"}
        (OUTD/args.outname).write_text(json.dumps(out, indent=2), encoding="utf-8")
        print(json.dumps(out, indent=2)); return

    if args.experiment_id:
        df = df[df.get("experiment_id")==args.experiment_id].copy()
        if df.empty:
            out = {"generated_at": datetime.now(timezone.utc).isoformat(),
                   "hours": args.hours, "experiment_id": args.experiment_id,
                   "status":"no_rows_for_experiment"}
            (OUTD/args.outname).write_text(json.dumps(out, indent=2), encoding="utf-8")
            print(json.dumps(out, indent=2)); return

    # Parameter grid (tight, practical)
    grid=[]
    for full_cut in (64.0, 66.0, 68.0, 70.0):
        for ang_min in (12.0, 15.0):
            for ang_max in (30.0, 35.0, 40.0):
                for trap_cut in (0.75, 0.80):
                    for lite_ang_min in (12.0, 15.0):
                        for lite_vol in (0.85, 0.90, 1.00):
                            grid.append({
                                "full_cut": full_cut,
                                "angle_min": ang_min, "angle_max": ang_max,
                                "trap_cutoff": trap_cut,
                                "lite_angle_min": lite_ang_min, "lite_angle_max": 45.0,
                                "lite_vol": lite_vol
                            })

    results=[]
    for p in grid:
        lab, hit = simulate(df, p, friction_bps=args.friction_bps)
        results.append({"params":p, "n":int(lab.shape[0]), "hit":hit})

    # Save full grid for audit
    grid_df = pd.DataFrame([{
        **r["params"], "labelled_n": r["n"], "hit_1h_pct": r["hit"]
    } for r in results])
    grid_path = OUTD / f"sweetspot_grid_{args.hours}h.csv"
    grid_df.to_csv(grid_path, index=False, encoding="utf-8")

    best = pick_config(results, target=(65.0,75.0), min_n=args.min_labelled)
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hours": args.hours,
        "experiment_id": args.experiment_id,
        "target_band": [65.0, 75.0],
        "min_labelled": args.min_labelled,
        "best": best,
        "grid_csv": str(grid_path)
    }
    (OUTD/args.outname).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))

if __name__=="__main__":
    main()
