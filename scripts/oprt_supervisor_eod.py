# -*- coding: utf-8 -*-
"""
OPRT EOD Supervisor — v1.20 (ENGINE-ALIGNED + DIAGNOSTICS)
- Aligns with engine & unified analytics strong-zone:
  * Dynamic Full cut: C_eff =66 (active, vol>=1.0) else =70 (quiet)
  * Phase 15–45°, volume =1.00
  * Trap veto ONLY when trap>=0.60 AND herald==False
  * Pressure OPTIONAL (gate only if present and |P|<0.20)
  * tech_coh is DIAGNOSTIC (not a hard fail)
- Adds diagnostics for strong-file rows:
  * strong_fail_leaderboard.csv, strong_fail_details.csv, strong_pass_details.csv
- Writes daily pack to C:\OPRT\reports\daily\<UTC_STAMP>\
- Optional LLM daily synthesis with engine thresholds baked in.
"""

from __future__ import annotations
from pathlib import Path
import argparse, csv, json, sys, math
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

# --------------- CLI ---------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agents_dir", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--jsonl", required=True)
    ap.add_argument("--jsonl_skipped")
    ap.add_argument("--data_dir", required=True)
    ap.add_argument("--flows")
    ap.add_argument("--pressure")
    ap.add_argument("--sentiment_index")
    ap.add_argument("--out")
    ap.add_argument("--out_root", default=r"C:\OPRT\reports\daily")
    ap.add_argument("--lookback_reports", type=int, default=5)
    # time slicing
    ap.add_argument("--day_utc", default=None, help="YYYY-MM-DD; default=today")
    ap.add_argument("--tz", default="Europe/Brussels", help="Pivot timezone")
    # LLM
    ap.add_argument("--with_llm", action="store_true")
    ap.add_argument("--llm_model", default="gpt-4o-mini")
    ap.add_argument("--llm_max_tokens", type=int, default=900)
    ap.add_argument("--llm_pattern_labels", action="store_true")
    # heartbeat
    ap.add_argument("--heartbeat", default=r"C:\OPRT\logs\engine_heartbeat.txt")
    return ap.parse_args()

# --------------- IO helpers ---------------
def _p(p): return Path(p)

def read_jsonl(fp: Path, n=200000) -> list[dict]:
    if not fp or not fp.exists(): return []
    out=[]
    with fp.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            s=line.strip()
            if not s: continue
            try: out.append(json.loads(s))
            except: pass
    return out[-n:]

def load_optional_json(fp: Path):
    if not fp or not fp.exists(): return None
    try: return json.loads(fp.read_text(encoding="utf-8", errors="replace"))
    except Exception: return None

def path_age_minutes(fp: Path):
    try:
        if not fp or not fp.exists(): return None
        now = datetime.now(timezone.utc).timestamp()
        return round((now - fp.stat().st_mtime)/60.0, 1)
    except Exception: return None

def to_epoch(ts):
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            pass
    try:
        return float(s)
    except Exception:
        return None

def day_bounds_utc(day_str: str | None, tz_name: str):
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo(tz_name) if tz_name and tz_name.upper()!="UTC" else timezone.utc
    except Exception:
        tz = timezone.utc
    now_utc = datetime.now(timezone.utc)
    if day_str:
        y,m,d = map(int, day_str.split("-"))
        start_local = datetime(y,m,d,0,0,0,tzinfo=tz)
    else:
        now_local = now_utc.astimezone(tz)
        start_local = datetime(now_local.year, now_local.month, now_local.day, 0,0,0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc).timestamp(), end_local.astimezone(timezone.utc).timestamp()

def f(x, default=None):
    try: return float(x)
    except Exception: return default

def to_bool(x):
    if isinstance(x,bool): return x
    return str(x).strip().lower() in ("1","true","yes","y","t")

def compute_p(r: dict):
    for k in ("P","pressure","p"):
        if k in r:
            try:
                pv = float(r.get(k))
                return max(-1.0, min(1.0, pv))
            except: pass
    return None

# ---------------- ENGINE-ALIGNED GATES ----------------
def strong_zone_ok(r: dict, pv: float | None):
    """
    Engine-aligned strong-zone:
      - Dynamic full_cut: 66 if vol>=1.0 else 70
      - Phase 15–45°
      - Volume >= 1.00
      - Trap veto only when trap>=0.60 AND herald==False
      - Pressure OPTIONAL: only gate when pv is provided and |pv|<0.20
      - tech_coh is DIAGNOSTIC ONLY
    """
    ce=f(r.get("C_eff")); ang=f(r.get("phase_angle_deg")); vr=f(r.get("volume_ratio"))
    trap=f(r.get("trap_T"))
    herald = to_bool(r.get("herald_ok")) if "herald_ok" in r else (to_bool(r.get("leaders_ok")) or to_bool(r.get("flows_ok")))
    if None in (ce,ang,vr): return False

    full_cut = 66.0 if (vr is not None and vr >= 1.0) else 70.0
    if ce < full_cut: return False
    if ang < 15 or ang > 45: return False
    if vr < 1.00: return False

    # Engine-style trap veto (no generic herald requirement)
    if (trap is not None and trap >= 0.60) and (not herald):
        return False

    # Pressure optional
    if pv is not None and abs(pv) < 0.20:
        return False

    return True

def gate_fail_tags(r: dict, pv: float | None):
    """
    Diagnostic tags aligned to engine policy (dynamic C_eff and vol>=1.00).
    """
    tags=[]
    ce=f(r.get("C_eff")); ang=f(r.get("phase_angle_deg")); vr=f(r.get("volume_ratio"))
    tech=f(r.get("tech_coh")); trap=f(r.get("trap_T"))
    herald = to_bool(r.get("herald_ok")) if "herald_ok" in r else (to_bool(r.get("leaders_ok")) or to_bool(r.get("flows_ok")))

    # Dynamic C_eff threshold by volume regime
    full_cut = 66.0 if (vr is not None and vr >= 1.0) else 70.0
    if ce is None or ce < full_cut: tags.append(f"C_eff<{int(full_cut)}")

    if ang is None or ang < 15 or ang > 45: tags.append("angle_out")

    # Volume gate aligned to engine enter
    if vr is None or vr < 1.00: tags.append("vol<1.00")

    # Herald/Trap diagnostics
    if not herald: tags.append("herald_off")
    if tech is not None and tech <= 0.60: tags.append("tech_coh_low")

    # Pressure only tagged as fail when provided and small
    if pv is not None and abs(pv) < 0.20: tags.append("|P|<0.20")

    # Trap info + veto documentation
    if trap is not None and trap >= 0.60:
        if not herald: tags.append("trap_veto")
        else: tags.append("trap_hi")

    return tags

# --------------- LLM ---------------
def try_llm(out_dir: Path, kpis: dict, patterns, buckets, ingest, model: str, max_tokens: int):
    import os, json as _json, traceback
    # API key from env OR fallback file
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        sec = Path(r"C:\OPRT\secrets\openai_key.txt")
        if sec.exists():
            key = sec.read_text(encoding="utf-8", errors="ignore").strip()
    if not key:
        (out_dir/"llm_advice.md").write_text(
            "LLM disabled: no OPENAI_API_KEY and no C:\\OPRT\\secrets\\openai_key.txt", encoding="utf-8"
        )
        return
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)

        thresholds = {
            "phase_deg": [15,45],
            "full_cut_active": 66.0,
            "full_cut_quiet": 70.0,
            "volume_ratio_min": 1.00,
            "trap_veto_rule": "trap>=0.60 AND herald==False",
            "pressure_optional_abs_min": 0.20
        }
        compact = {
            "thresholds": thresholds,
            "kpis": kpis,
            "ingest": ingest,
            "top_patterns": patterns[:12],
            "top_buckets": buckets[:12]
        }
        prompt = (
            "You are the EOD supervisor for an OPRT mirror loop.\n"
            "Follow THESE engine thresholds exactly (do NOT propose angles <15°, or C_eff below Full cuts):\n"
            f"{_json.dumps(thresholds)}\n\n"
            "Using the JSON context, produce:\n"
            "1) DAILY SYNTHESIS (3–6 sentences) about C_eff, |P|, volume_ratio, trap, Strong-Zone coverage, ingests.\n"
            "2) STRATEGIC TASKING (1–3 actions) to improve accuracy tomorrow.\n"
            "3) MIRROR LOOP COORDINATION: Bull/Bear prompts for the next 1H/4H with volume & angle checks.\n"
            "4) RISK NOTE: anomalies/data gaps.\n\n"
            f"Context:\n{_json.dumps(compact)}"
        )
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role":"user","content":prompt}],
            temperature=0.2, max_tokens=max_tokens
        )
        text = resp.choices[0].message.content if resp and resp.choices else "LLM returned no content."
        (out_dir/"llm_advice.md").write_text(text, encoding="utf-8")
    except Exception as e:
        (out_dir/"llm_advice.md").write_text(f"LLM error: {e}\n{traceback.format_exc()}", encoding="utf-8")

# --------------- sweet-spot ---------------
def sweet_spot_scan(rows):
    """
    Scans coverage across engine-like grids (dynamic full threshold regime).
    """
    import itertools
    Cgrid=[66,70]                 # engine dynamic full cuts
    Agrid=[(15,45),(12,48)]
    Vgrid=[1.00,1.15,1.20,1.30]   # include engine enter at 1.00
    Pgrid=[0.20,0.30]
    Tgrid=[0.60]
    out=[]
    for Cmin,(Amin,Amax),Vmin,Pmin,Tmin in itertools.product(Cgrid,Agrid,Vgrid,Pgrid,Tgrid):
        keep=N=0
        for r in rows:
            ce=f(r.get("C_eff")); ang=f(r.get("phase_angle_deg")); vr=f(r.get("volume_ratio")); tech=f(r.get("tech_coh"))
            herald = to_bool(r.get("herald_ok")) if "herald_ok" in r else (to_bool(r.get("leaders_ok")) or to_bool(r.get("flows_ok")))
            trap=f(r.get("trap_T")); pv=compute_p(r)
            if None in (ce,ang,vr): continue
            N+=1
            # Engine-style veto & optional pressure
            trap_ok = not ((trap is not None and trap>=0.60) and (not herald))
            p_ok = True if pv is None else (abs(pv) >= Pmin)
            if ce>=Cmin and Amin<=ang<=Amax and vr>=Vmin and trap_ok and (tech is None or tech>=Tmin) and p_ok:
                keep+=1
        if N:
            out.append({"Cmin":Cmin,"A":[Amin,Amax],"Vmin":Vmin,"Pmin":Pmin,"Tmin":Tmin,"coverage":keep/N})
    out.sort(key=lambda d:(-d["coverage"], d["Cmin"], -d["Pmin"]))
    return out[:10]

# --------------- main ---------------
def main():
    ns = parse_args()

    root_out = _p(ns.out_root)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    out_dir = _p(ns.out) if ns.out else (root_out / stamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    strong_all = read_jsonl(_p(ns.jsonl))
    weak_all   = read_jsonl(_p(ns.jsonl_skipped)) if ns.jsonl_skipped else []

    # Daily Brussels slice
    start_ts, end_ts = day_bounds_utc(ns.day_utc, ns.tz)
    def in_window(r):
        ep = to_epoch(r.get("timestamp_utc") or r.get("ts_utc") or r.get("timestamp"))
        return (ep is not None) and (ep >= start_ts) and (ep < end_ts)

    strong = [r for r in strong_all if in_window(r)]
    weak   = [r for r in weak_all   if in_window(r)]

    # Auto-widen if empty
    widened = False
    if (len(strong) + len(weak)) == 0:
        widened = True
        end_ts = datetime.now(timezone.utc).timestamp()
        start_ts = end_ts - 48*3600.0
        def in_window2(r):
            ep = to_epoch(r.get("timestamp_utc") or r.get("ts_utc") or r.get("timestamp"))
            return (ep is not None) and (ep >= start_ts) and (ep < end_ts)
        strong = [r for r in strong_all if in_window2(r)]
        weak   = [r for r in weak_all   if in_window2(r)]

    data_dir = _p(ns.data_dir)
    flows_fp    = _p(ns.flows) if ns.flows else (data_dir/"flows_btc.json")
    pressure_fp = _p(ns.pressure) if ns.pressure else (data_dir/"pressure_btc.json")
    senti_fp    = _p(ns.sentiment_index) if ns.sentiment_index else (data_dir/"sentiment_index.txt")
    headlines_fp= data_dir/"headlines.csv"
    heartbeat_fp= _p(ns.heartbeat)
    coc_summary_fp = _p(ns.data_dir).parent / "derived" / "coc_summary.json"
    coc = load_optional_json(coc_summary_fp)

    flows   = load_optional_json(flows_fp)
    p_live  = load_optional_json(pressure_fp)
    senti_v = None
    try:
        if senti_fp.exists(): senti_v = senti_fp.read_text(encoding="utf-8", errors="replace").strip()
    except: pass

    hb_line = None
    try:
        if heartbeat_fp.exists():
            hb_line = heartbeat_fp.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-1]
    except: pass

    ingest = {
        "headlines_csv_age_mins": path_age_minutes(headlines_fp),
        "sentiment_index_age_mins": path_age_minutes(senti_fp),
        "flows_btc_age_mins": path_age_minutes(flows_fp),
        "pressure_btc_age_mins": path_age_minutes(pressure_fp),
        "sentiment_index_value": senti_v,
        "engine_heartbeat": hb_line
    }

    # aggregate
    def metrics(rows):
        agg=defaultdict(float); cnt=defaultdict(int)
        buckets=Counter(); patterns=Counter(); strong_hits=0
        size_mix=Counter(); wcode_mix=Counter()
        for r in rows:
            ce=f(r.get("C_eff")); ang=f(r.get("phase_angle_deg")); vr=f(r.get("volume_ratio"))
            trap=f(r.get("trap_T")); tech=f(r.get("tech_coh")); pv=compute_p(r)
            herald = to_bool(r.get("herald_ok")) if "herald_ok" in r else (to_bool(r.get("leaders_ok")) or to_bool(r.get("flows_ok")))
            size_mix[str(r.get("size_band",""))]+=1
            wcode_mix[str(r.get("w_code",""))]+=1
            if ce is not None:   agg["C_eff_sum"]+=ce; cnt["C_eff"]+=1
            if ang is not None:  agg["angle_sum"]+=ang; cnt["angle"]+=1
            if vr is not None:   agg["vr_sum"]+=vr;    cnt["vr"]+=1
            if trap is not None: agg["trap_sum"]+=trap;cnt["trap"]+=1
            if tech is not None: agg["tech_sum"]+=tech;cnt["tech"]+=1
            if pv is not None:   agg["P_sum"]+=pv;     cnt["P"]+=1
            def b_ce(c):
                if c is None: return "Ceff:NA"
                c=float(c);
                return "Ceff:>=90" if c>=90 else "Ceff:[70,90)" if c>=70 else "Ceff:[55,70)" if c>=55 else "Ceff:[35,55)" if c>=35 else "Ceff:<35"
            def b_ang(a):
                if a is None: return "Ang:NA"
                a=abs(float(a));
                return "Ang:<15" if a<15 else "Ang:15-45" if a<=45 else "Ang:45-90" if a<=90 else "Ang:>90"
            def b_vr(v):
                if v is None: return "Vol:NA"
                v=float(v);
                return "Vol:>=1.30" if v>=1.30 else "Vol:1.20-1.29" if v>=1.20 else "Vol:1.00-1.19" if v>=1.00 else "Vol:<1.00"
            def b_tr(t):
                if t is None: return "Trap:NA"
                t=float(t);
                return "Trap:>=0.60" if t>=0.60 else "Trap:0.30-0.59" if t>=0.30 else "Trap:<0.30"
            def b_p(p):
                if p is None: return "P:NA"
                ap=abs(float(p));
                return "P:>=0.50" if ap>=0.50 else "P:[0.30,0.50)" if ap>=0.30 else "P:[0.20,0.30)" if ap>=0.20 else "P:[0.10,0.20)" if ap>=0.10 else "P:<0.10"
            buckets[b_ce(ce)] += 1; buckets[b_ang(ang)] += 1; buckets[b_vr(vr)] += 1; buckets[b_tr(trap)] += 1; buckets[b_p(pv)] += 1
            buckets["Herald:" + ("Y" if herald else "N")] += 1
            patt=[b_ce(ce), b_ang(ang), b_vr(vr), b_p(pv), "Herald:"+("Y" if herald else "N"),
                  ("TrapHi" if (trap is not None and trap>=0.60) else "TrapLo")]
            patterns[" | ".join(patt)] += 1
            if strong_zone_ok(r, pv): strong_hits += 1
        return {"agg":agg,"cnt":cnt,"buckets":buckets,"patterns":patterns,"strong_hits":strong_hits,"n":len(rows),
                "size_mix":size_mix,"wcode_mix":wcode_mix}

    S = metrics(strong)
    W = metrics(weak)

    # Weak fail tags (legacy)
    GF = Counter()
    for r in weak:
        for t in gate_fail_tags(r, compute_p(r)):
            GF[t] += 1

    # Strong rows audit (new)
    strong_fail = []
    strong_pass = []
    SF = Counter()
    for r in strong:
        pv = compute_p(r)
        ok = strong_zone_ok(r, pv)
        if ok:
            strong_pass.append(r)
        else:
            reasons = gate_fail_tags(r, pv)
            for t in reasons: SF[t] += 1
            rcopy = {k:r.get(k) for k in ("timestamp_utc","signal","size_band","price","phase_angle_deg","C_eff","volume_ratio","trap_T","herald_ok","leaders_ok","flows_ok","tech_coh")}
            rcopy["reasons"] = reasons
            strong_fail.append(rcopy)

    # KPIs
    def avg(sumk,cntk,M):
        c=M["cnt"].get(cntk,0);
        return (M["agg"].get(sumk,0.0)/c) if c else None

    kpis = {
        "samples_strong": S["n"], "samples_weak": W["n"],
        "C_eff_avg_strong": avg("C_eff_sum","C_eff", S),
        "P_avg_strong":     avg("P_sum","P", S),
        "angle_avg_strong": avg("angle_sum","angle", S),
        "vr_avg_strong":    avg("vr_sum","vr", S),
        "trap_avg_strong":  avg("trap_sum","trap", S),
        "tech_avg_strong":  avg("tech_sum","tech", S),
        "strong_zone_coverage": (len(strong_pass)/S["n"]) if S["n"] else None,
        # Size counts
        "count_full":    S["size_mix"].get("Full",0),
        "count_half":    S["size_mix"].get("Half",0),
        "count_quarter": S["size_mix"].get("Quarter",0),
        "window_widened": (len(strong)+len(weak)==0),
    }

    # outputs
    (out_dir/"kpis_daily.csv").write_text(
        "\n".join([ "metric,value" ] + [f"{k},{kpis[k]}" for k in kpis]), encoding="utf-8")

    with (out_dir/"ingest_freshness.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["artifact","age_mins","notes"])
        w.writerow(["headlines.csv", path_age_minutes(_p(ns.data_dir)/"headlines.csv"), ""])
        w.writerow(["sentiment_index.txt", path_age_minutes(_p(ns.sentiment_index) if ns.sentiment_index else _p(ns.data_dir)/"sentiment_index.txt"), ""])
        w.writerow(["flows_btc.json", path_age_minutes(_p(ns.flows) if ns.flows else _p(ns.data_dir)/"flows_btc.json"), ""])
        w.writerow(["pressure_btc.json", path_age_minutes(_p(ns.pressure) if ns.pressure else _p(ns.data_dir)/"pressure_btc.json"), ""])
        w.writerow(["engine_heartbeat.txt", path_age_minutes(_p(ns.heartbeat)), ""])

    with (out_dir/"strong_vs_weak.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["bucket","strong_count","weak_count"])
        allk=set(S["buckets"])|set(W["buckets"])
        for k in sorted(allk):
            w.writerow([k, S["buckets"].get(k,0), W["buckets"].get(k,0)])

    # Mixes
    with (out_dir/"size_mix.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["size_band","count"])
        for k,cnt in S["size_mix"].most_common(): w.writerow([k,cnt])
    with (out_dir/"w_code_mix.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["w_code","count"])
        for k,cnt in S["wcode_mix"].most_common(): w.writerow([k,cnt])

    # Buckets & patterns
    with (out_dir/"bucket_stats.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["bucket","count"])
        for k,cnt in S["buckets"].most_common(): w.writerow([k,cnt])
    with (out_dir/"pattern_counts.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["pattern","count"])
        for k,cnt in S["patterns"].most_common(): w.writerow([k,cnt])

    # Weak gate fails (legacy) + Strong gate fails (new)
    with (out_dir/"gate_fail_leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["tag","count"])
        for tag,cnt in Counter(GF).most_common(): w.writerow([tag,cnt])
    with (out_dir/"strong_fail_leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["tag","count"])
        for tag,cnt in Counter(SF).most_common(): w.writerow([tag,cnt])

    # Strong details
    def write_details(fp, rows):
        if not rows:
            Path(fp).write_text("", encoding="utf-8"); return
        cols = ["timestamp_utc","signal","size_band","price","phase_angle_deg","C_eff","volume_ratio","trap_T","herald_ok","leaders_ok","flows_ok","tech_coh","reasons"]
        with open(fp, "w", encoding="utf-8", newline="") as fh:
            w=csv.writer(fh); w.writerow(cols)
            for r in rows:
                w.writerow([r.get(c) if c!="reasons" else "|".join(r.get("reasons",[])) for c in cols])

    write_details(out_dir/"strong_fail_details.csv", strong_fail)
    # For passes, record without "reasons"
    if strong_pass:
        cols = ["timestamp_utc","signal","size_band","price","phase_angle_deg","C_eff","volume_ratio","trap_T","herald_ok","leaders_ok","flows_ok","tech_coh"]
        with open(out_dir/"strong_pass_details.csv", "w", encoding="utf-8", newline="") as fh:
            w=csv.writer(fh); w.writerow(cols)
            for r in strong_pass:
                w.writerow([r.get(c) for c in cols])

    sweet = sweet_spot_scan(strong+weak)
    with (out_dir/"sweet_spot_candidates.csv").open("w", encoding="utf-8", newline="") as fh:
        w=csv.writer(fh); w.writerow(["Cmin","Amin","Amax","Vmin","Pmin","Tmin","coverage"])
        for d in sweet:
            w.writerow([d["Cmin"], d["A"][0], d["A"][1], d["Vmin"], d["Pmin"], d["Tmin"], d["coverage"]])

    best = {
        "strong_zone": {
            "C_eff_min":        sweet[0]["Cmin"] if sweet else 66,
            "angle_min":        sweet[0]["A"][0] if sweet else 15,
            "angle_max":        sweet[0]["A"][1] if sweet else 45,
            "volume_ratio_min": sweet[0]["Vmin"] if sweet else 1.00,
            "tech_coh_min":     sweet[0]["Tmin"] if sweet else 0.60,
            "absP_min":         sweet[0]["Pmin"] if sweet else 0.20,
            "trap_veto":        "trap>=0.60 AND !herald"
        },
        "notes": "Auto-proposal from today's coverage sweet-spot scan (engine-aligned)."
    }
    (out_dir/"best_params.json").write_text(json.dumps(best, indent=2), encoding="utf-8")

    with (out_dir/"signal_audits.jsonl").open("w", encoding="utf-8") as fh:
        for r in strong[-100:]:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Markdown summary
    (out_dir/"unified_report.md").write_text(
        "# OPRT EOD Report ({} UTC)\n\nWindow: {} ? {}\n\n{}\n\n## Daily KPIs\n"
        "- samples_strong: {}\n- samples_weak: {}\n"
        "- C_eff_avg_strong: {}\n- P_avg_strong: {}\n"
        "- strong_zone_coverage: {}  (passes/strong = {}/{})\n"
        "- size_mix: Full={} Half={} Quarter={}\n\n"
        "## Ingest Freshness (minutes)\n"
        "- headlines.csv age: {} min\n- sentiment_index.txt age: {} min (value={})\n"
        "- flows_btc.json age: {} min\n- pressure_btc.json age: {} min\n"
        "- engine_heartbeat: {}\n".format(
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            datetime.fromtimestamp(end_ts,   tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            ("**[Auto-widened to last 48h due to empty day window]**\n" if (len(strong)+len(weak)==0) else ""),
            kpis["samples_strong"], kpis["samples_weak"],
            kpis["C_eff_avg_strong"], kpis["P_avg_strong"],
            (round(100.0* (len(strong_pass)/S["n"]),1) if S["n"] else None),
            len(strong_pass), S["n"],
            kpis["count_full"], kpis["count_half"], kpis["count_quarter"],
            path_age_minutes(_p(ns.data_dir)/"headlines.csv"),
            path_age_minutes(_p(ns.sentiment_index) if ns.sentiment_index else _p(ns.data_dir)/"sentiment_index.txt"), (senti_v or ""),
            path_age_minutes(_p(ns.flows) if ns.flows else _p(ns.data_dir)/"flows_btc.json"),
            path_age_minutes(_p(ns.pressure) if ns.pressure else _p(ns.data_dir)/"pressure_btc.json"),
            (hb_line or "n/a")
        ),
        encoding="utf-8"
    )

    if ns.with_llm:
        try_llm(out_dir, kpis, list(S["patterns"].most_common()), list(S["buckets"].most_common()),
                ingest, ns.llm_model, ns.llm_max_tokens)

    print("[OK] EOD wrote:", out_dir)

if __name__ == "__main__":
    sys.exit(main())
