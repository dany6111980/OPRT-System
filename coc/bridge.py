# C:\OPRT\coc\bridge.py
from __future__ import annotations
from typing import Dict, Any
import math

try:
    # your real implementation should import from coc.core.coc
    from coc.core import coc as _coc
except Exception:
    _coc = None  # engine will handle None and skip

AssetVec = Dict[str, Dict[str, float]]

def unit(vx: float, vy: float) -> tuple[float,float]:
    mag = math.hypot(vx, vy)
    return (0.0, 0.0) if mag == 0 else (vx/mag, vy/mag)

def phase_deg(vx: float, vy: float) -> float:
    """Return 0–180 absolute phase angle (deg) from vector."""
    return abs(math.degrees(math.atan2(vy, vx)))

def run_local_phases() -> AssetVec | None:
    """
    Return per-asset coherence vectors from CoC in a stable format:
      { "BTC": {"vx": .., "vy": .., "phase_deg": .., "c_local": ..}, ... }
    """
    if _coc is None:
        return None
    # >>> REPLACE the stub below with real outputs from your CoC <<<
    # Example expected from CoC:
    #   results = _coc.compute_all(["BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y"])
    #   where each entry has a 2D vector or (phase, coherence) info
    results = getattr(_coc, "compute_all", None)
    if results is None:
        return None
    raw = results(["BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y"])
    out: AssetVec = {}
    for k, r in raw.items():
        vx, vy = r.get("vx", 0.0), r.get("vy", 0.0)
        ux, uy = unit(vx, vy)
        out[k] = {
            "vx": ux, "vy": uy,
            "phase_deg": phase_deg(ux, uy),
            "c_local": float(r.get("coh", 0.0))
        }
    return out

def compute_c_global(av: AssetVec) -> dict | None:
    """Compute global vector, coherence, and per-asset angle vs C_global."""
    if not av: return None
    vx = sum(d["vx"] for d in av.values())
    vy = sum(d["vy"] for d in av.values())
    ux, uy = unit(vx, vy)
    global_phase = phase_deg(ux, uy)
    # angle diff per asset (0–180)
    deltas = {k: abs(av[k]["phase_deg"] - global_phase) for k in av}
    # crude coherence as average |dot| with global direction
    dots = [abs(av[k]["vx"]*ux + av[k]["vy"]*uy) for k in av]
    c_global = sum(dots)/len(dots) if dots else 0.0
    return {"ux": ux, "uy": uy, "phase_deg": global_phase, "C_global": c_global, "deltas": deltas}
