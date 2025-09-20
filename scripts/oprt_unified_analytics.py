#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
OPRT Unified Analytics — ENGINE-ALIGNED & ROBUST (v3.5)
- Always writes debug_counts.json (root-cause when "no data")
- Dedup per snapshot (prevents inflated counts)
- Auto-widen window once if empty (even when --since_hours is set)
- variant_summary: hit_1h_pct + meets_sweet_spot flag
"""
from __future__ import annotations
import argparse, json, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Tuple, List, Dict, Any
import numpy as np
import pandas as pd
from collections import Counter

# ---------------- CLI ----------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"C:\OPRT")
    ap.add_argument("--day_utc", default=None, help="YYYY-MM-DD; if omitted, uses today")
    ap.add_argument("--tz", default="Europe/Brussels")
    ap.add_argument("--since_hours", type=int, default=None, help="Use last N hours instead of full day")
    ap.add_argument("--friction_bps", type=float, default=5.0)
    ap.add_argument("--list_last", type=int, default=25)
    ap.add_argument("--trap_cutoff_default", type=float, default=0.80)
    ap.add_argument("--experiment_id", default=None)
    ap.add_argument("--use_skipped_as_cover", action="store_true", default=True)
    return ap.parse_args()

# ---------------- IO helpers ----------------
def load_jsonl(p: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if not p.exists():
        return pd.DataFrame()
    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s=line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                pass
    return pd.DataFrame(rows)

def compute_next_prices(df: pd.DataFrame, steps=(1,4,12)) -> pd.DataFrame:
    if df.empty: return df
    df = df.copy()
    if "timestamp_utc" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
        df = df.sort_values("ts").reset_index(drop=True)
    if "price" not in df.columns:
        df["price"] = pd.NA
    for h in steps:
        df[f"price_next_{h}h"] = df["price"].shift(-h)
    return df

def window_bounds(day_utc: str|None, tz_name: str, since_hours: int|None):
    if since_hours is not None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=since_hours)
        return start, end
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name) if tz_name and tz_name.upper()!="UTC" else timezone.utc
    except Exception:
        tz = timezone.utc
    now_utc = datetime.now(timezone.utc)
    if day_utc:
        y,m,d = map(int, day_utc.split("-"))
        start_local = datetime(y,m,d,0,0,0, tzinfo=tz)
    else:
        now_local = now_utc.astimezone(tz)
        start_local = datetime(now_local.year, now_local.month, now_local.day, 0,0,0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)

# ---------------- Row-aware helpers ----------------
def row_full_thr(r, default_active=66.0, default_quiet=70.0) -> float:
    try:
        thr = r.get("checks_values", {}).get("ceff", {}).get("thr")
        if thr is not None: return float(thr)
    except Exception: pass
    vr = float(r.get("volume_ratio", 1.0) or 1.0)
    return float(default_active if vr >= 1.0 else default_quiet)

def row_angle_minmax(r, default_min=12.0, default_max=35.0) -> Tuple[float,float]:
    try:
        pm = r.get("checks_values", {}).get("phase", {})
        amin = pm.get("thr_min"); amax = pm.get("thr_max")
        if amin is not None and amax is not None:
            return float(amin), float(amax)
    except Exception: pass
    return float(default_min), float(default_max)

def row_trap_cutoff(r, default_cut=0.80) -> float:
    return float(default_cut)

def within_angle(a, amin, amax) -> bool:
    try: x = float(a); return (x >= float(amin)) and (x <= float(amax))
    except Exception: return False

# ---------------- Metrics helpers ----------------
def dir_hit(sig, p0, p1):
    if sig not in ("BUY","SELL") or pd.isna(p0) or pd.isna(p1): return None
    return 1 if ((sig=="BUY" and p1>p0) or (sig=="SELL" and p1<p0)) else 0

def safe_ret_bps(p0, p1, friction_bps=5.0, sig=None):
    try:
        p0f=float(p0); p1f=float(p1)
        if not np.isfinite(p0f) or not np.isfinite(p1f) or p0f==0.0: return np.nan
        ret=((p1f/p0f)-1.0)*1e4 - float(friction_bps)
        return ret if sig=="BUY" else (-ret if sig=="SELL" else ret)
    except Exception: return np.nan

def eff_bucket(c):
    if pd.isna(c): return None
    c=float(c)
    if c<35: return "<35"
    lo = int((max(35,c)//5)*5); hi=lo+5
    lo=max(lo,35); hi=min(hi,100)
    return f"{lo}-{hi}"

def ang_bucket(a):
    if pd.isna(a): return None
    a=float(a)
    if a<=15: return "≤15°"
    if a<=30: return "15–30°"
    if a<=45: return "30–45°"
    return ">45°"

def vol_bucket(v):
    if pd.isna(v): return None
    v=float(v)
    if v<0.8: return "<0.8"
    if v<1.0: return "0.8–1.0"
    if v<1.2: return "1.0–1.2"
    return "≥1.2"

# ---------------- Tables / masks ----------------
def overlay_league(df: pd.DataFrame, trap_default=0.80) -> pd.DataFrame:
    base = df.copy()
    base["active"] = base["signal"].isin(["BUY","SELL"])
    base["hit1"] = [dir_hit(s,p,n) for s,p,n in zip(base["signal"], base["price"], base.get("price_next_1h"))]

    amin=[]; amax=[]; full_thr=[]; trap_thr=[]
    for _, r in base.iterrows():
        mi, ma = row_angle_minmax(r); amin.append(mi); amax.append(ma)
        full_thr.append(row_full_thr(r))
        trap_thr.append(row_trap_cutoff(r, trap_default))
    base["_amin"]=amin; base["_amax"]=amax; base["_full"]=full_thr; base["_trap_thr"]=trap_thr

    m_engine = (
        base["active"]
        & (base.get("mode","")=="strong")
        & (base.get("size_band","")=="Full")
        & (base["phase_angle_deg"].astype(float).between(base["_amin"], base["_amax"], inclusive="both"))
        & (~((base.get("trap_T",0).astype(float) > base["_trap_thr"]) & (~base.get("herald_ok", False).astype(bool))))
        & (base.get("C_eff",-1).astype(float) >= base["_full"])
    )

    overlays = {
        "EngineStrong": m_engine,
        "OPRT_loose(≤45°)": base["active"] & (base.get("phase_angle_deg",999).astype(float) <= 45),
        "VOL_≥1.2":         base["active"] & (base.get("volume_ratio",0).astype(float) >= 1.2),
        "HERALD_only":      base["active"] & (base.get("herald_ok",False).astype(bool)),
        "FLOWS_only":       base["active"] & (base.get("flows_ok",False).astype(bool)),
    }

    rows=[]
    for name, m in overlays.items():
        sub=base[m]
        lab=sub[sub["hit1"].notna()]
        tot=int(sub.shape[0]); labn=int(lab.shape[0])
        hit=float(lab["hit1"].mean()*100) if labn>0 else float("nan")
        rows.append({
            "strategy": name,
            "count_labelled": labn,
            "active_total": tot,
            "labelled_%": round((labn/tot*100),1) if tot>0 else None,
            "hit_1h_%": round(hit,1) if labn>0 else None
        })
    return pd.DataFrame(rows).sort_values(["hit_1h_%","count_labelled"], ascending=[False,False])

def gate_funnel(df: pd.DataFrame, trap_default=0.80) -> pd.DataFrame:
    rows=[]
    total=int(df.shape[0]); rows.append(("all_rows", total))

    m_active = df["signal"].isin(["BUY","SELL"])
    rows.append(("active(BUY/SELL)", int(m_active.sum())))

    m_mode = m_active & (df.get("mode","")=="strong") & (df.get("size_band","")=="Full")
    rows.append(("mode=strong & size=Full", int(m_mode.sum())))

    amin=[]; amax=[]; trap_thr=[]; full_thr=[]
    for _, r in df.iterrows():
        mi, ma = row_angle_minmax(r); amin.append(mi); amax.append(ma)
        trap_thr.append(row_trap_cutoff(r, trap_default))
        full_thr.append(row_full_thr(r))
    _amin=pd.Series(amin, index=df.index); _amax=pd.Series(amax, index=df.index)
    _trap=pd.Series(trap_thr, index=df.index); _full=pd.Series(full_thr, index=df.index)

    m_ang = m_mode & (df.get("phase_angle_deg",999).astype(float).between(_amin, _amax, inclusive="both"))
    rows.append(("phase row-window", int(m_ang.sum())))

    m_trap = m_ang & (~((df.get("trap_T",0).astype(float)>_trap) & (~df.get("herald_ok",False).astype(bool))))
    rows.append(("trap veto (ok)", int(m_trap.sum())))

    m_ce = m_trap & (df.get("C_eff",-1).astype(float) >= _full)
    rows.append(("C_eff >= full_thr(row)", int(m_ce.sum())))

    out = pd.DataFrame(rows, columns=["gate","count"])
    out["survival_%"] = (out["count"]/max(1,total)*100).round(1)
    return out

# ---------------- Performance & buckets ----------------
def passes_policy_row(r, trap_default=0.80) -> bool:
    if r.get("signal") == "WATCH": return False
    if str(r.get("mode","")).lower() != "strong": return False
    if str(r.get("size_band","")) != "Full": return False
    amin, amax = row_angle_minmax(r)
    if not within_angle(r.get("phase_angle_deg"), amin, amax): return False
    trap_thr = row_trap_cutoff(r, trap_default)
    trap = float(r.get("trap_T", 0.0) or 0.0)
    herald = bool(r.get("herald_ok", False))
    if (trap > trap_thr) and (not herald):
        return False
    try:
        ce = float(r.get("C_eff"))
        return bool(ce >= row_full_thr(r))
    except Exception:
        return False

def perf_and_buckets(df: pd.DataFrame, friction_bps=5.0, trap_default=0.80):
    mask=[passes_policy_row(r, trap_default) for _, r in df.iterrows()]
    s = df[pd.Series(mask, index=df.index)].copy()
    if s.empty:
        return (pd.DataFrame(), {}, s)
    for H in (1,4,12):
        s[f"hit_{H}h"] = [dir_hit(sig, p0, pH) for sig,p0,pH in zip(s["signal"], s["price"], s.get(f"price_next_{H}h"))]
        s[f"pnl_{H}h_bps"] = [safe_ret_bps(p0, pH, friction_bps, sig) for sig,p0,pH in zip(s["signal"], s["price"], s.get(f"price_next_{H}h"))]
    s["C_bucket5"] = s["C_eff"].apply(eff_bucket)
    s["ang_bin"]   = s["phase_angle_deg"].apply(ang_bucket)
    s["vol_bin"]   = s["volume_ratio"].apply(vol_bucket)
    s["herald"]    = s.get("herald_ok", False).astype(bool).map({True:"Herald=On", False:"Herald=Off"})
    s["trap_hi"]   = s.get("trap_T",0).astype(float).apply(lambda x: "Trap>0.8" if (pd.notna(x) and x>0.8) else "Trap≤0.8")
    s["flows"]     = s.get("flows_ok", False).astype(bool).map({True:"Flows=On", False:"Flows=Off"})
    s["leaders"]   = s.get("leaders_ok", False).astype(bool).map({True:"Leaders=On", False:"Leaders=Off"})

    def agg_hit(df_in, col, H=1):
        g=(df_in.groupby(col)[f"hit_{H}h"].agg(["size","mean"]).rename(columns={"size":"count","mean":"hit_%"}))
        g["hit_%"]=(g["hit_%"]*100).round(1)
        return g

    buckets={
        "by_signal": agg_hit(s, "signal", 1),
        "by_Ceff":   agg_hit(s, "C_bucket5", 1).sort_index(),
        "by_angle":  agg_hit(s, "ang_bin", 1).reindex(["≤15°","15–30°","30–45°",">45°"]),
        "by_volume": agg_hit(s, "vol_bin", 1).reindex(["<0.8","0.8–1.0","1.0–1.2","≥1.2"]),
        "by_herald": agg_hit(s, "herald", 1),
        "by_trap":   agg_hit(s, "trap_hi", 1),
        "by_flows":  agg_hit(s, "flows", 1),
        "by_leaders":agg_hit(s, "leaders", 1),
    }

    pnl_rows=[]
    for H in (1,4,12):
        pnl = s[f"pnl_{H}h_bps"].astype(float)
        pnl = pnl[np.isfinite(pnl)]
        if len(pnl)==0:
            pnl_rows.append({"horizon_h":H,"n":0,"pnl_mean_bps":None,"pnl_std_bps":None,"sharpe":None})
        else:
            mu=float(np.mean(pnl)); sd=float(np.std(pnl, ddof=1)) if len(pnl)>1 else 0.0
            pnl_rows.append({"horizon_h":H,"n":int(len(pnl)),"pnl_mean_bps":round(mu,2),
                             "pnl_std_bps":round(sd,2),"sharpe":round(mu/sd,2) if sd>1e-9 else None})
    pnl_tbl=pd.DataFrame(pnl_rows)

    # simple calibration
    def prob_map(row):
        try:
            ce=float(row.get("C_eff")); thr=row_full_thr(row)
            return max(0.0, min(1.0, (ce - thr)/34.0))
        except Exception:
            return 0.5
    s["p_est"]=s.apply(prob_map, axis=1)
    s["y1"]=s["hit_1h"].astype(float)
    brier=float(((s["p_est"]-s["y1"])**2).mean()) if len(s)>0 else float("nan")

    try:
        s["C_bin"]=pd.qcut(s["C_eff"].astype(float), q=min(7, max(2, int(np.sqrt(max(1,len(s))/2)))), duplicates="drop")
    except Exception:
        s["C_bin"]=pd.cut(s["C_eff"].astype(float), bins=[35,50,60,66,70,80,90,100], right=True, include_lowest=True)
    calib=(s.groupby("C_bin")["y1"].agg(["size","mean"]).rename(columns={"size":"count","mean":"hit_%"}))
    calib["hit_%"]=(calib["hit_%"]*100).round(1)

    return (pnl_tbl, {"buckets":buckets, "brier":brier, "n":int(s.shape[0]), "calib":calib}, s)

# ---------------- Failures (skipped) ----------------
def failure_table(skipped_df: pd.DataFrame, topn=50) -> pd.DataFrame:
    if skipped_df.empty:
        return pd.DataFrame(columns=["reason|size|signal","count"])
    keys=[]
    for _, r in skipped_df.iterrows():
        hard=r.get("hard_gate_reason")
        if pd.isna(hard) or hard is None:
            fc=r.get("failed_checks")
            hard=(fc[0] if isinstance(fc,(list,tuple)) and len(fc)>0 else None)
        keys.append(f"{hard}|{r.get('size_band')}|{r.get('signal')}")
    cnt=Counter(keys)
    rows=[{"reason|size|signal":k,"count":n} for k,n in cnt.most_common(topn)]
    return pd.DataFrame(rows)

# ---------------- ToD & rolling ----------------
def tod_heatmap(engine_df: pd.DataFrame, tz_name: str) -> pd.DataFrame:
    if engine_df.empty:
        return pd.DataFrame(columns=["hour_local","count","hit_%"])
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name) if tz_name.upper()!="UTC" else timezone.utc
    except Exception:
        tz = timezone.utc
    e = engine_df.copy()
    e["ts"] = pd.to_datetime(e["timestamp_utc"], errors="coerce", utc=True)
    e["hour_local"] = e["ts"].dt.tz_convert(tz).dt.hour
    e["hit1"] = e["hit_1h"]
    g = e.groupby("hour_local")["hit1"].agg(["size","mean"]).rename(columns={"size":"count","mean":"hit_%"})
    g["hit_%"]=(g["hit_%"]*100).round(1)
    return g.reset_index()

def rolling_accuracy(engine_df: pd.DataFrame) -> pd.DataFrame:
    if engine_df.empty:
        return pd.DataFrame(columns=["date","n","hit_%"])
    e=engine_df.copy()
    e["date"]=pd.to_datetime(e["timestamp_utc"], errors="coerce", utc=True).dt.date
    e["hit1"]=e["hit_1h"]
    g=(e.groupby("date")["hit1"].agg(["size","mean"]).rename(columns={"size":"n","mean":"hit_%"}))
    g["hit_%"]=(g["hit_%"]*100).round(1)
    return g.reset_index()

# ---------------- Main ----------------
def main():
    args=parse_args()

    ROOT = Path(str(args.root).replace("\\","/"))
    LOGS = ROOT / "logs"
    decisions_path = LOGS / "mirror_loop_unified_decisions.jsonl"
    skipped_path   = LOGS / "mirror_loop_unified_decisions_skipped.jsonl"
    csv_path       = LOGS / "mirror_loop_unified_run.csv"

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    OUT = ROOT / "reports" / "unified" / stamp
    OUT.mkdir(parents=True, exist_ok=True)

    # -------- load raw ----------
    df_dec = load_jsonl(decisions_path)
    df_skp = load_jsonl(skipped_path)

    # coverage fallback
    used_skipped_as_cover = False
    df = df_dec.copy()
    if df.empty and args.use_skipped_as_cover and not df_skp.empty:
        df = df_skp.copy()
        used_skipped_as_cover = True

    # price fill from CSV
    if not df.empty and "price" not in df.columns and csv_path.exists():
        try:
            csv_df = pd.read_csv(csv_path)
            if "timestamp_utc" in csv_df.columns and "ts" not in csv_df.columns:
                csv_df["ts"] = pd.to_datetime(csv_df["timestamp_utc"], errors="coerce", utc=True)
            else:
                csv_df["ts"] = pd.to_datetime(csv_df.get("ts"), errors="coerce", utc=True)
            csv_df["price"] = pd.to_numeric(csv_df.get("price"), errors="coerce")
            keep = csv_df[["ts","price"]].dropna()
            df["ts"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
            df = df.merge(keep, on="ts", how="left", suffixes=("", "_csv"))
            df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(df["price_csv"])
            df.drop(columns=["price_csv"], inplace=True, errors="ignore")
        except Exception as e:
            print("[WARN] CSV merge:", e)

    # ---- compute window ----
    start_utc, end_utc = window_bounds(args.day_utc, args.tz, args.since_hours)

    # ---- initial bounds for debug ----
    def _ts_bounds(x: pd.DataFrame) -> Dict[str, Any]:
        if x.empty or "timestamp_utc" not in x.columns: return {"min": None, "max": None}
        ts = pd.to_datetime(x["timestamp_utc"], errors="coerce", utc=True)
        return {"min": str(ts.min()), "max": str(ts.max())}

    raw_dec_bounds = _ts_bounds(df_dec)
    raw_skp_bounds = _ts_bounds(df_skp)

    # ---- filter by time window ----
    df_all = df.copy()
    if not df.empty and "timestamp_utc" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
        df = df[(df["ts"]>=start_utc) & (df["ts"]<end_utc)].copy()

    # Auto-widen by +2h (min total 8h) once if empty
    window_widened_hours = 0
    if df.empty and not df_all.empty:
        end_f = datetime.now(timezone.utc)
        base = max(args.since_hours or 6, 6)
        widen = max(base+2, 8)
        start_f = end_f - timedelta(hours=widen)
        df = df_all[(pd.to_datetime(df_all["timestamp_utc"], errors="coerce", utc=True)>=start_f) &
                    (pd.to_datetime(df_all["timestamp_utc"], errors="coerce", utc=True)<end_f)].copy()
        window_widened_hours = widen

    # ---- experiment filter ----
    df_pre_exp_ct = int(df.shape[0])
    if args.experiment_id and not df.empty:
        df = df[df.get("experiment_id")==args.experiment_id].copy()

    # ---- DEDUP per snapshot ----
    if not df.empty:
        df = df.drop_duplicates(
            subset=['timestamp_utc','signal','mode','size_band','price','C_eff','phase_angle_deg'],
            keep='last'
        )

    # ---- DEBUG COUNTS (always write) ----
    dbg = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "args": {
            "day_utc": args.day_utc,
            "since_hours": args.since_hours,
            "tz": args.tz,
            "experiment_id": args.experiment_id,
            "trap_cutoff_default": args.trap_cutoff_default
        },
        "files_exist": {
            "decisions": decisions_path.exists(),
            "skipped": skipped_path.exists(),
            "csv": csv_path.exists()
        },
        "raw_counts": {
            "decisions_rows": int(df_dec.shape[0]),
            "skipped_rows": int(df_skp.shape[0]),
            "decisions_min_ts": raw_dec_bounds["min"],
            "decisions_max_ts": raw_dec_bounds["max"],
            "skipped_min_ts": raw_skp_bounds["min"],
            "skipped_max_ts": raw_skp_bounds["max"]
        },
        "window_bounds_utc": {
            "start_utc": start_utc.isoformat(),
            "end_utc": end_utc.isoformat(),
            "window_widened_by_hours": window_widened_hours
        },
        "post_window_counts": {
            "rows_after_window": int(df_pre_exp_ct)
        },
        "experiment_filter": {
            "applied": bool(args.experiment_id),
            "rows_after_experiment": int(df.shape[0]) if not df.empty else 0,
            "raw_experiment_histogram_top5": (
                pd.Series(df_all.get("experiment_id")).value_counts().head(5).to_dict()
                if not df_all.empty and "experiment_id" in df_all.columns else {}
            )
        },
        "used_skipped_as_cover": used_skipped_as_cover
    }
    OUT.joinpath("debug_counts.json").write_text(json.dumps(dbg, indent=2), encoding="utf-8")

    # ---- Next prices + ensure 'ts' + prune (FIX) ----
    df = compute_next_prices(df, steps=(1,4,12))
    if "ts" not in df.columns:
        df["ts"] = pd.to_datetime(df.get("timestamp_utc"), errors="coerce", utc=True)
    df = df[df["ts"].notna()].copy()

    # 1) Overlays & funnel
    overlays = overlay_league(df.copy(), trap_default=args.trap_cutoff_default) if not df.empty else pd.DataFrame()
    funnel   = gate_funnel(df.copy(), trap_default=args.trap_cutoff_default) if not df.empty else pd.DataFrame(columns=["gate","count","survival_%"])
    overlays.to_csv(OUT/"overlay_league.csv", index=False, encoding="utf-8")
    funnel.to_csv(OUT/"gate_funnel.csv", index=False, encoding="utf-8")

    # 2) EngineStrong perf & buckets
    pnl_tbl, stats, engine_df = perf_and_buckets(df.copy(), friction_bps=args.friction_bps, trap_default=args.trap_cutoff_default)
    pnl_tbl.to_csv(OUT/"pnl_by_horizon.csv", index=False, encoding="utf-8")

    # 3) Buckets → CSV
    buckets = stats.get("buckets", {})
    for name, tbl in buckets.items():
        if isinstance(tbl, pd.DataFrame):
            tbl.to_csv(OUT/f"{name}.csv", index=True, encoding="utf-8")

    # 4) Calibration
    calib = stats.get("calib", pd.DataFrame())
    calib.to_csv(OUT/"calibration_by_Ceff.csv", index=True, encoding="utf-8")

    # 5) ToD & rolling accuracy
    tod = tod_heatmap(engine_df, args.tz)
    roll = rolling_accuracy(engine_df)
    tod.to_csv(OUT/"time_of_day.csv", index=False, encoding="utf-8")
    roll.to_csv(OUT/"rolling_accuracy_14d.csv", index=False, encoding="utf-8")

    # 6) Failure combos
    if not df_skp.empty:
        df_skp["ts"] = pd.to_datetime(df_skp.get("timestamp_utc"), errors="coerce", utc=True)
    fail_tbl = failure_table(df_skp[(df_skp.get("ts")>=start_utc) & (df_skp.get("ts")<end_utc)] if "ts" in df_skp.columns else df_skp, topn=50)
    fail_tbl.to_csv(OUT/"skipped_failure_top50.csv", index=False, encoding="utf-8")

    # 7) Last N rows
    lastN = engine_df.tail(int(args.list_last)).copy()
    cols = ["timestamp_utc","signal","size_band","price","phase_angle_deg","C_eff","volume_ratio","trap_T","herald_ok","leaders_ok","flows_ok","assets_present"]
    keep_cols=[c for c in cols if c in lastN.columns]
    lastN[keep_cols].to_csv(OUT/"last_engine_strong.csv", index=False, encoding="utf-8")

    # 8) best_params.json — medians (decisions-only if available)
    src_for_medians = df_dec.copy() if not df_dec.empty else pd.DataFrame()
    def _med_or_nan(values): return float(pd.Series(values).median()) if len(values)>0 else float("nan")
    med_full = _med_or_nan([row_full_thr(r) for _,r in src_for_medians.iterrows()])
    med_amin = _med_or_nan([row_angle_minmax(r)[0] for _,r in src_for_medians.iterrows()])
    med_amax = _med_or_nan([row_angle_minmax(r)[1] for _,r in src_for_medians.iterrows()])
    params_obj={
        "version": "engine-strong-row-aware",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds_median_for_window": {
            "full_cut": med_full, "angle_min": med_amin, "angle_max": med_amax,
            "trap_cutoff": float(args.trap_cutoff_default)
        },
        "note": "Row-aware: medians computed from decisions.jsonl for the selected window; skipped not used."
    }
    OUT.joinpath("best_params.json").write_text(json.dumps(params_obj, indent=2), encoding="utf-8")

    # 9) Markdown report
    exp_label = args.experiment_id if args.experiment_id else "(all)"
    md=[]
    md.append("# OPRT Unified Analytics — Engine-aligned & Detailed")
    md.append(f"- generated_at: {datetime.now(timezone.utc).isoformat()}")
    md.append(f"- window: {start_utc.strftime('%Y-%m-%d %H:%M')} → {end_utc.strftime('%Y-%m-%d %H:%M')} UTC")
    if window_widened_hours:
        md.append(f"- window_widened_by_hours: {window_widened_hours} (auto widen safeguard)")
    md.append(f"- experiment_id: {exp_label}")
    md.append(f"- outdir: {OUT}")

    if not math.isnan(med_full) and not math.isnan(med_amin) and not math.isnan(med_amax):
        md.append(f"- medians(decisions): FullCut≈{med_full:.1f}, Angle≈{med_amin:.1f}–{med_amax:.1f}, TrapCut={float(args.trap_cutoff_default):.2f}")

    if used_skipped_as_cover and df_dec.empty and not df.empty:
        md.append("- note: decisions.jsonl empty for window; using *_skipped.jsonl* for coverage only (EngineStrong requires BUY/SELL).")

    md.append("\n## Overlay League (context)\n" + (overlays.to_string(index=False) if not overlays.empty else "(no data)"))
    md.append("\n## Gate Funnel (survival)\n" + (funnel.to_string(index=False) if not funnel.empty else "(no data)"))

    md.append("\n## EngineStrong — Performance")
    if not pnl_tbl.empty:
        md.append(pnl_tbl.to_string(index=False))
        md.append(f"\n- labelled_n: {stats.get('n',0)}")
        brier=stats.get("brier", float('nan'))
        if not (isinstance(brier,float) and math.isnan(brier)):
            md.append(f"- Brier (1h): {brier:.4f}")
    else:
        md.append("(no EngineStrong rows in window)")

    def add_csv_table(title, path):
        p=OUT/path
        if p.exists():
            try:
                t=pd.read_csv(p)
                md.append(f"\n### {title}\n" + t.to_string(index=False))
            except Exception:
                md.append(f"\n### {title}\n(see file: {p})")
        else:
            md.append(f"\n### {title}\n(no data)")

    for title, file in [
        ("Buckets — by Signal","by_signal.csv"),
        ("Buckets — by C_eff (5pt)","by_Ceff.csv"),
        ("Buckets — by Phase Angle","by_angle.csv"),
        ("Buckets — by Volume","by_volume.csv"),
        ("Buckets — Herald effect","by_herald.csv"),
        ("Buckets — Trap effect","by_trap.csv"),
        ("Buckets — Flows effect","by_flows.csv"),
        ("Buckets — Leaders effect","by_leaders.csv"),
        ("Calibration by C_eff","calibration_by_Ceff.csv"),
        ("Time-of-day (local)","time_of_day.csv"),
        ("Rolling accuracy (daily)","rolling_accuracy_14d.csv"),
        ("Top 50 failure combos (skipped)","skipped_failure_top50.csv"),
        (f"Last {args.list_last} EngineStrong","last_engine_strong.csv")
    ]:
        add_csv_table(title, file)

    OUT.joinpath("unified_report.md").write_text("\n".join(md), encoding="utf-8")

    # 10) Variant summary
    labelled_n = int(stats.get("n",0) or 0)
    hit_1h_pct = None
    try:
        if labelled_n>0 and "hit_1h" in engine_df.columns:
            hit_1h_pct = float(round(engine_df["hit_1h"].mean()*100, 1))
    except Exception: pass
    variant_obj = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": args.experiment_id,
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
        "labelled_n": labelled_n,
        "hit_1h_pct": hit_1h_pct,
        "coverage_pct": 0.0 if df.empty else 100.0,
        "brier_1h": stats.get("brier"),
        "pnl_mean_bps_1h": (float(pnl_tbl.loc[pnl_tbl["horizon_h"]==1, "pnl_mean_bps"].iloc[0])
                            if not pnl_tbl.empty and (pnl_tbl["horizon_h"]==1).any() else None),
        "meets_sweet_spot": (hit_1h_pct is not None and 65.0 <= hit_1h_pct <= 75.0 and labelled_n >= 20)
    }
    OUT.joinpath("variant_summary.json").write_text(json.dumps(variant_obj, indent=2), encoding="utf-8")

    print("============================================")
    print("Unified Analytics — outputs written to:")
    print(OUT)
    for f in ["overlay_league.csv","gate_funnel.csv","pnl_by_horizon.csv","by_signal.csv","by_Ceff.csv","by_angle.csv",
              "by_volume.csv","by_herald.csv","by_trap.csv","by_flows.csv","by_leaders.csv",
              "calibration_by_Ceff.csv","time_of_day.csv","rolling_accuracy_14d.csv",
              "skipped_failure_top50.csv","last_engine_strong.csv","best_params.json","debug_counts.json","unified_report.md","variant_summary.json"]:
        print(" -", f)
    print("============================================")

if __name__ == "__main__":
    main()
