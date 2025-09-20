#!/usr/bin/env python3
# OPRT mirror loop v0.3+ (compat v3) — SWEET-SPOT TUNED (v0.3.2)
# - Fastgate: size_band='Watch' (clear WATCH labeling)
# - LITE: new --lite_rescue_min_vol (default 0.85) instead of hard 0.75
# - Defaults aligned to strong-zone policy: C_eff enter 66/70, angles 12–35
from __future__ import annotations
import os, json, math, argparse, csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
import numpy as np

ASSETS = ["BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y"]

def now_iso_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ensure_dir(path:str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def load_json(path:str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

_rng = np.random.default_rng(40)
def _mock_vec(bias=1.0):
    return (bias + _rng.normal(0,0.05,size=5)).astype(float)

def mock_agent_json(bull=True):
    sign = 'bull' if bull else 'bear'
    inv  = '50>200' if bull else '50<200'
    return {
        'tf_alignment': {'H4':sign,'H1':sign},
        'indicators': {
            'rsi': {'H4': 62.0 if bull else 42.0, 'H1': 60.0 if bull else 45.0,
                    'slope': {'H4': '+' if bull else '-', 'H1': '+' if bull else '-'}},
            'macd': {'H4':'pos' if bull else 'neg','H1':'pos' if bull else 'neg',
                     'hist_slope': {'H4': '+' if bull else '-', 'H1': '+' if bull else '-'}},
            'ema': {'H4':inv,'H1':inv,'slope': {'H4': '+' if bull else '-', 'H1': '+' if bull else '-'}}
        },
        'volume': {'ratio_1h_to_avg20': 1.05 if bull else 0.95},
        'leaders': {'ETH': '+' if bull else '-', 'SOL': '+' if bull else '-', 'breadth': 'risk_on' if bull else 'risk_off'},
        'flows': {'oi':'up' if bull else 'down','funding':'flat','liq_skew':'short' if bull else 'long'},
        'sentiment_index': 1 if bull else -1,
        'levels': {}, 'scenarios': [],
        'phase_vector': _mock_vec(1.04 if bull else 0.96).round(4).tolist(),
    }

@dataclass
class AgentOut:
    tf_alignment: Dict[str,str]
    indicators: Dict[str,Any]
    volume_ratio: float
    leaders: Dict[str,Any]
    flows: Dict[str,str]
    sentiment_index: float
    levels: Dict[str,Any]
    scenarios: List[Dict[str,Any]]
    phase_vector: np.ndarray
    @staticmethod
    def from_json(d:Dict[str,Any])->'AgentOut':
        return AgentOut(
            tf_alignment=d.get('tf_alignment',{'H4':'neutral','H1':'neutral'}),
            indicators=d.get('indicators',{}),
            volume_ratio=float(d.get('volume',{}).get('ratio_1h_to_avg20',1.0)),
            leaders=d.get('leaders',{}),
            flows=d.get('flows',{}),
            sentiment_index=float(d.get('sentiment_index',0.0)),
            levels=d.get('levels',{}), scenarios=d.get('scenarios',[]),
            phase_vector=np.array(d.get('phase_vector',[1,1,1,1,1]), dtype=float),
        )

def c_raw_from_delta(delta_phi:np.ndarray, kappa:float=25.0)->float:
    std = float(np.std(delta_phi))
    return 100.0 / (1.0 + kappa*std)

def global_vector_and_metrics(delta_by_asset:Dict[str,np.ndarray])->Tuple[np.ndarray,float]:
    others=[v for a,v in delta_by_asset.items() if a!='BTC']
    if not others:
        return np.zeros(5), 0.0
    mat = np.vstack(others)
    return np.mean(mat, axis=0), 1.0/float(np.std(mat))

def phase_angle_deg(v1:np.ndarray,v2:np.ndarray)->float:
    v1 = v1 / (np.linalg.norm(v1)+1e-9)
    v2 = v2 / (np.linalg.norm(v2)+1e-9)
    dot = float(np.clip(np.dot(v1,v2), -1.0, 1.0))
    return math.degrees(math.acos(dot))

def apply_global_alignment(c_local:float, angle:float):
    if angle <= 10.0: return c_local*1.00, "Tight alignment (no boost)"
    if angle <= 35.0: return c_local*1.15, "Sweet lane (12–35°) +15%"
    if angle <= 45.0: return c_local*1.00, "Loose alignment (no change)"
    return c_local*0.70, "Divergence (-30% C)"

def gate_volume(r:float)->float:
    r=float(r)
    if r>=1.30: return 1.00
    if r>=1.15: return 0.98
    if r>=1.00: return 0.92
    return 0.85

def trap_probability(v:float)->float:
    v=float(v)
    if v>=1.30: return 0.10
    if v>=1.15: return 0.20
    if v>=1.05: return 0.30
    if v>=0.95: return 0.50
    return 0.70

def tech_bias_sign_from_tf(tf:Dict[str,str])->int:
    if tf.get('H4')=='bull' and tf.get('H1')=='bull': return +1
    if tf.get('H4')=='bear' and tf.get('H1')=='bear': return -1
    return 0

def compute_tech_detail(A:AgentOut, price:float):
    def norm_trend(ema): return (+1 if '50>200' in str(ema) else -1)*0.7
    def norm_momo(tf):  return +1.0 if A.indicators.get('macd',{}).get('hist_slope',{}).get(tf,'+')=='+' else -1.0
    def norm_rsi(v):
        try: r = float(v)
        except Exception: r = 50.0
        # clamp to [-1, +1]
        return max(-1.0, min(1.0, (r - 50.0) / 25.0))
    S_H4=(norm_trend(A.indicators.get('ema',{}).get('H4','50>200'))+norm_momo('H4')+norm_rsi(A.indicators.get('rsi',{}).get('H4',50)))/3.0
    S_H1=(norm_trend(A.indicators.get('ema',{}).get('H1','50>200'))+norm_momo('H1')+norm_rsi(A.indicators.get('rsi',{}).get('H1',50)))/3.0
    coh=float(max(0.0,1.0-abs(S_H4-S_H1))); S_dir=(S_H4+S_H1)/2.0
    g=0.85+0.15*max(0.0, S_dir if S_dir>=0 else -S_dir)*coh
    return g, {'S_H4':round(S_H4,3),'S_H1':round(S_H1,3),'coh':round(coh,3),'S_dir':round(S_dir,3)}

def gate_tf(tech_sign:int)->float:
    return 1.00 if tech_sign!=0 else 0.50

def gate_sentiment_conflict(si:float, tech_sign:int, thr:float, mult:float)->float:
    try: v=float(si)
    except: v=0.0
    v=max(-3.0, min(3.0, v))
    conflict=(v>0 and tech_sign<0) or (v<0 and tech_sign>0)
    if abs(v) >= thr and conflict:
        return max(0.50, 1.0 - float(mult))
    return 1.00

def gate_flows(flows:Dict[str,str], up_short_mult:float, down_long_mult:float)->float:
    oi=(flows or {}).get('oi','').lower()
    liq=(flows or {}).get('liq_skew','').lower()
    if oi=='up' and liq=='short':  return 1.0 + float(up_short_mult)
    if oi=='down' and liq=='long': return max(0.80, 1.0 - float(down_long_mult))
    return 1.00

def size_band_from_ce(C_eff:float, full:float)->str:
    if C_eff>=full: return 'Full'
    if C_eff>=max(32.0, full-20.0): return 'Half'
    return 'Watch'

def read_state(data_dir:str)->dict:
    try: return json.load(open(os.path.join(data_dir,'loop_state.json'),'r',encoding='utf-8'))
    except Exception: return {}
def write_state(data_dir:str, state:dict):
    try: open(os.path.join(data_dir,'loop_state.json'),'w',encoding='utf-8').write(json.dumps(state))
    except Exception: pass

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--agents_dir',default='C:/OPRT/agents')
    ap.add_argument('--csv',default='C:/OPRT/logs/mirror_loop_unified_run.csv')
    ap.add_argument('--jsonl',default='C:/OPRT/logs/mirror_loop_unified_decisions.jsonl')
    ap.add_argument('--data_dir',default='C:/OPRT/data')
    ap.add_argument('--heartbeat',default=r'C:\\OPRT\\logs\\engine_heartbeat.txt')
    ap.add_argument('--price',type=float,default=float('nan'))
    ap.add_argument('--kappa',type=float,default=20.0)
    ap.add_argument('--sentiment_index',type=float,default=None)
    ap.add_argument('--volume_ratio',type=float,default=None)
    ap.add_argument('--flows',type=str,default=None)
    ap.add_argument('--pressure_gate',choices=['on','off'],default='off')
    ap.add_argument('--herald_mode',choices=['all','any'],default='any')
    ap.add_argument('--experiment_id',type=str,default=None)

    # Strong-zone defaults aligned to target
    ap.add_argument('--strong_angle_min',type=float,default=12.0)
    ap.add_argument('--strong_angle_max',type=float,default=35.0)
    ap.add_argument('--strong_ceff_enter_active',type=float,default=66.0)
    ap.add_argument('--strong_ceff_enter_quiet',type=float,default=70.0)
    ap.add_argument('--coh_enter',type=float,default=0.50)
    ap.add_argument('--vol_enter',type=float,default=1.00)

    # LITE
    ap.add_argument('--lite_enable',action='store_true',default=True)
    ap.add_argument('--lite_angle_min',type=float,default=12.0)   # was 10.0
    ap.add_argument('--lite_angle_max',type=float,default=45.0)
    ap.add_argument('--lite_ceff_enter',type=float,default=48.0)
    ap.add_argument('--lite_coh_enter',type=float,default=0.35)
    ap.add_argument('--lite_vol_enter',type=float,default=0.95)
    ap.add_argument('--lite_rescue_min_vol',type=float,default=0.85)  # NEW (was hard 0.75)
    ap.add_argument('--lite_pabs_enter',type=float,default=0.0)

    ap.add_argument('--flows_file',type=str,default=None)
    ap.add_argument('--pressure_file',type=str,default=None)
    ap.add_argument('--pressure_mode',choices=['on','off'],default=None)
    ap.add_argument('--si_conflict_threshold',type=float,default=2.0)
    ap.add_argument('--si_conflict_mult',type=float,default=None)
    ap.add_argument('--si_conflict_multiplier',type=float,default=None)
    ap.add_argument('--flows_up_short_mult',type=float,default=None)
    ap.add_argument('--flows_down_long_mult',type=float,default=None)
    ap.add_argument('--trap_cutoff',type=float,default=0.80)
    ap.add_argument('--log_all',action='store_true',default=False)
    ap.add_argument('--lite_starve_cycles',type=int,default=None)
    args=ap.parse_args()

    if args.pressure_mode is not None:
        args.pressure_gate = args.pressure_mode

    up_mult   = 0.05 if args.flows_up_short_mult   is None else float(args.flows_up_short_mult)
    down_mult = 0.05 if args.flows_down_long_mult is None else float(args.flows_down_long_mult)
    si_mult   = 0.25 if args.si_conflict_mult is None else float(args.si_conflict_mult)
    if args.si_conflict_multiplier is not None:
        si_mult = float(args.si_conflict_multiplier)

    # Agents load (mock if missing)
    agentA={}; agentB={}
    for asset in ASSETS:
        a=os.path.join(args.agents_dir,f'{asset}_A.json'); b=os.path.join(args.agents_dir,f'{asset}_B.json')
        if os.path.exists(a) and os.path.exists(b):
            aj=load_json(a); bj=load_json(b)
        else:
            aj=mock_agent_json(True); bj=mock_agent_json(False)
        agentA[asset]=AgentOut.from_json(aj); agentB[asset]=AgentOut.from_json(bj)

    A=agentA['BTC']
    if args.sentiment_index is not None: A.sentiment_index=args.sentiment_index
    if args.volume_ratio is not None: A.volume_ratio=args.volume_ratio
    if args.flows:
        try: A.flows.update(json.loads(args.flows))
        except Exception: pass
    if args.flows_file and os.path.exists(args.flows_file):
        try: A.flows.update(json.load(open(args.flows_file,'r',encoding='utf-8')))
        except Exception: pass

    # Coherence + global alignment
    delta_by_asset={a:(agentA[a].phase_vector-agentB[a].phase_vector) for a in ASSETS}
    btc_delta=delta_by_asset['BTC']
    C_raw=c_raw_from_delta(btc_delta,kappa=args.kappa)
    global_vec,_Cglob=global_vector_and_metrics(delta_by_asset)
    angle=phase_angle_deg(btc_delta,global_vec)
    C_loc,align_note=apply_global_alignment(C_raw,angle)

    leaders_ok = (A.leaders.get("ETH")=="+") or (A.leaders.get("SOL")=="+")
    flows_ok   = (A.flows.get("oi","").lower()=="up") and (A.flows.get("liq_skew","").lower()=="short")
    herald_ok  = bool(leaders_ok or flows_ok)
    trap_T_fg  = float(round(trap_probability(A.volume_ratio), 3))

    # FASTGATE (volume low) -> explicit WATCH
    if float(A.volume_ratio) < 0.80:
        ensure_dir(args.csv); new_csv = not os.path.exists(args.csv)
        with open(args.csv,'a',newline='') as fp:
            w=csv.writer(fp)
            if new_csv:
                w.writerow(['timestamp_utc','asset','price','C_eff','phase_angle_deg','volume_ratio','signal','size_band','mode','trap_T'])
            w.writerow([now_iso_utc(),'BTC',None,round(C_loc,3),round(angle,2),float(A.volume_ratio),
                        'WATCH','Watch','baseline',trap_T_fg])

        out = {
            'timestamp_utc': now_iso_utc(),
            'asset': 'BTC',
            'signal': 'WATCH',
            'size_band': 'Watch',           # <— NEW explicit
            'mode': 'baseline',             # <— explicit
            'reason': 'volume_low_fastgate',
            'is_watch': True,
            'volume_ratio': float(A.volume_ratio),
            'phase_angle_deg': round(angle, 2),
            'trap_T': trap_T_fg,
            'herald_ok': bool(herald_ok),
            'leaders_ok': bool(leaders_ok),
            'flows_ok': bool(flows_ok),
            'lane': (args.experiment_id or 'baseline'),
            'experiment_id': (args.experiment_id or 'baseline'),
            'checks_values': {
                'ceff':  {'thr': (args.strong_ceff_enter_quiet if A.volume_ratio < 1.0 else args.strong_ceff_enter_active)},
                'phase': {'thr_min': args.strong_angle_min, 'thr_max': args.strong_angle_max},
                'volume':{'thr': args.vol_enter}
            },
            'failed_checks': ['volume'],
            'hard_gate_reason': 'volume',
            'conditions_ready': False,
            'gate_note': 'watch_fastgate'
        }
        ensure_dir(args.jsonl)
        open(args.jsonl,'a',encoding='utf-8').write(json.dumps(out)+'\n')
        open(args.jsonl.replace('.jsonl','_skipped.jsonl'),'a',encoding='utf-8').write(json.dumps(out)+'\n')
        print(f'[WHY] fastgate volume_low | vol={A.volume_ratio:.3f}')
        return

    # Tech & gates
    tech_sign=tech_bias_sign_from_tf(A.tf_alignment)
    g_tech, tech_detail=compute_tech_detail(A, args.price if math.isfinite(args.price) else float('nan'))
    g_volume=gate_volume(A.volume_ratio)
    g_tf=gate_tf(tech_sign)
    g_sent=gate_sentiment_conflict(A.sentiment_index, tech_sign, args.si_conflict_threshold, si_mult)
    g_flow=gate_flows(A.flows, up_short_mult=up_mult, down_long_mult=down_mult)
    trap_T=float(round(trap_probability(A.volume_ratio),3))
    C_eff=C_loc*g_volume*g_tf*g_sent*g_flow*g_tech

    thr_full = args.strong_ceff_enter_quiet if A.volume_ratio < 1.0 else args.strong_ceff_enter_active
    amin=args.strong_angle_min; amax=args.strong_angle_max

    # regime-aware angle tweaks (optional)
    try:
        cstats_path=os.path.join(args.data_dir,'coc_stats.json')
        if os.path.exists(cstats_path):
            decile=int(json.load(open(cstats_path,'r',encoding='utf-8')).get('decile'))
            if decile>=7: amax=min(amax,35.0)
            elif decile<=2: amax=min(60.0, amax+10.0); amin=max(0.0, amin-5.0)
    except Exception:
        pass

    leaders_ok = (A.leaders.get("ETH")=="+") or (A.leaders.get("SOL")=="+")
    flows_ok   = (A.flows.get("oi","").lower()=="up") and (A.flows.get("liq_skew","").lower()=="short")
    herald_ok  = bool(leaders_ok or flows_ok)
    tf_ok      = (tech_sign!=0)

    phase_strong_ok = (angle <= amax)  # min angle covered in checks
    checks={
        'ceff':   (C_eff>=thr_full),
        'phase':  bool(phase_strong_ok),
        'volume': (A.volume_ratio>=args.vol_enter),
        'coh':    (tech_detail['coh']>args.coh_enter),
        'tf':     bool(tf_ok),
        'herald': bool(herald_ok)
    }
    trap_veto=(trap_T > float(args.trap_cutoff)) and (not herald_ok)
    if trap_veto:
        checks['trap_veto']=False

    strong_ok=all(checks.values())
    signal=('BUY' if tech_sign>=0 else 'SELL') if strong_ok else 'WATCH'
    w_code='strong' if strong_ok else 'weak_zone'; mode='strong' if strong_ok else 'baseline'
    gate_note = 'strong_full' if strong_ok else 'watch_checks'

    # -------- LITE rescue (tightened) --------
    lite_ok=False
    state = read_state(args.data_dir)
    starve_cnt = int(state.get('starve_cnt', 0)) if isinstance(state, dict) else 0
    lite_guard_ok = True if args.lite_starve_cycles is None else (starve_cnt >= int(args.lite_starve_cycles))

    if (not strong_ok) and bool(args.lite_enable):
        amin_l=args.lite_angle_min; amax_l=args.lite_angle_max
        try:
            if 'decile' in locals() and decile>=7: amax_l=min(amax_l,35.0)
        except Exception:
            pass

        ang_l_ok = (angle <= amax_l)  # low-angle capture
        # Tightened rescue: require at least lite_rescue_min_vol with Herald & TF
        vol_rescue_ok = (A.volume_ratio >= float(args.lite_rescue_min_vol)) and herald_ok and tf_ok
        vol_l_ok = (A.volume_ratio >= args.lite_vol_enter) or vol_rescue_ok
        ceff_l_thr = args.lite_ceff_enter if (A.volume_ratio >= args.lite_vol_enter) else max(35.0, args.lite_ceff_enter - 6.0)

        checks_l={
            'ceff': (C_eff>=ceff_l_thr),
            'phase': bool(ang_l_ok),
            'volume': bool(vol_l_ok),
            'coh': (tech_detail['coh']>args.lite_coh_enter),
            'tf': bool(tf_ok),
            'herald': bool(herald_ok),
            'starve_guard': bool(lite_guard_ok)
        }
        lite_ok=(not trap_veto) and all(checks_l.values())
        if lite_ok:
            signal=('BUY' if tech_sign>=0 else 'SELL'); w_code='lite'; mode='lite'
            gate_note='lite_rescue' if vol_rescue_ok else 'lite_default'

    size_band = 'Half' if mode=='lite' else size_band_from_ce(C_eff, thr_full)

    print(f"[EXP] {args.experiment_id or 'baseline'} | mode={mode} | vol={A.volume_ratio:.3f} | angle={angle:.2f} | Ceff={C_eff:.2f} | gates(vol={g_volume:.2f},tech={g_tech:.2f},sent={g_sent:.2f},flow={g_flow:.2f}) | TFok={tf_ok} Herald={herald_ok} | trapT={trap_T} | align={align_note} | {gate_note}")

    failed_checks=[k for k,v in checks.items() if v is False]
    hard_gate_reason = None
    if trap_veto: hard_gate_reason='trap_veto'
    elif failed_checks:
        for k in ('phase','ceff','volume','tf','coh','herald'):
            if k in failed_checks: hard_gate_reason=k; break

    summary={
        'timestamp_utc': now_iso_utc(),
        'asset':'BTC',
        'kappa': float(args.kappa),
        'experiment_id': (args.experiment_id or 'baseline'),
        'lane': (args.experiment_id or 'baseline'),
        'herald_mode':'OR',
        'leaders_ok': bool(leaders_ok),
        'flows_ok': bool(flows_ok),
        'herald_ok': bool(herald_ok),
        'signal': signal,
        'w_code': w_code,
        'size_band': size_band,
        'mode': mode,
        'C_eff': round(C_eff,3),
        'phase_angle_deg': round(angle,2),
        'volume_ratio': float(A.volume_ratio),
        'price': (float(args.price) if math.isfinite(args.price) else None),
        'regime': ('quiet' if A.volume_ratio<1.0 else 'active'),
        'trap_T': trap_T,
        'tech_coh': float(tech_detail['coh']),
        'tech_sdir': float(tech_detail['S_dir']),
        'checks_values': {
            'ceff': {'actual': round(C_eff,3),'thr': thr_full},
            'phase': {'actual': round(angle,2),'thr_min': amin,'thr_max': amax},
            'volume': {'actual': round(A.volume_ratio,3),'thr': args.vol_enter},
            'coh': {'actual': round(tech_detail['coh'],3),'thr': args.coh_enter}
        },
        'assets_present': ",".join(ASSETS),
        'starve_cnt': starve_cnt,
        'lite_guard_ok': bool(lite_guard_ok),
        'gate_note': gate_note
    }
    summary['conditions_ready']=bool((A.volume_ratio>=1.0) and (trap_T <= float(args.trap_cutoff)) and herald_ok and tf_ok)
    if signal=='WATCH':
        summary['failed_checks']= failed_checks
        if hard_gate_reason: summary['hard_gate_reason']= hard_gate_reason

    # CSV + JSONL write
    ensure_dir(args.csv)
    csv_exists=os.path.exists(args.csv)
    with open(args.csv,'a' if csv_exists else 'w',newline='') as fp:
        w=csv.writer(fp)
        if not csv_exists:
            w.writerow(['timestamp_utc','asset','price','C_eff','phase_angle_deg','volume_ratio','signal','size_band','mode','trap_T'])
        w.writerow([summary['timestamp_utc'],'BTC',summary['price'],summary['C_eff'],
                    summary['phase_angle_deg'],summary['volume_ratio'],summary['signal'],
                    summary['size_band'],summary['mode'],summary['trap_T']])

    ensure_dir(args.jsonl)
    open(args.jsonl,'a',encoding='utf-8').write(json.dumps(summary,ensure_ascii=False)+'\n')
    if summary.get('signal')=='WATCH':
        open(args.jsonl.replace('.jsonl','_skipped.jsonl'),'a',encoding='utf-8').write(json.dumps(summary,ensure_ascii=False)+'\n')

    # state & heartbeat
    try:
        state_next={'last_strong': (signal!='WATCH'), 'starve_cnt': 0 if signal!='WATCH' else min(1000, (summary.get('starve_cnt',0)+1))}
        write_state(args.data_dir, state_next)
    except Exception: pass
    try:
        hb=json.dumps({'ts':now_iso_utc(),'strong': 1 if signal!='WATCH' else 0,'exp':args.experiment_id,'C_eff':summary['C_eff']})
        ensure_dir(args.heartbeat); open(args.heartbeat,'a',encoding='utf-8').write(hb+'\n')
    except Exception: pass

if __name__=='__main__':
    main()
