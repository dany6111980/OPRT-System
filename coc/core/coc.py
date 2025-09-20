from dataclasses import dataclass
from pathlib import Path
import json
import pandas as pd
from .coc_sources import load_basket_ohlcv
from .coc_metrics import compute_beta, compute_Dphi, compute_Hdir, combine_coc, normalize_vec

@dataclass
class CoCResult:
    coc_basket: float
    coc_ref: float
    delta: float
    decile: int

def compute_and_persist(output_dir: Path, years: int = 5, macro_years: int = 15) -> CoCResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    data = load_basket_ohlcv(years=years, macro_years=macro_years)

    rows = []
    for asset, per_tf in data.items():
        beta = compute_beta(per_tf)
        dphi = compute_Dphi(per_tf)
        hdir = compute_Hdir(per_tf)
        rows.append({"asset": asset, "beta": beta, "Dphi": dphi, "Hdir": hdir})
    df = pd.DataFrame(rows).set_index("asset")

    # If everything is NaN/empty, write neutral CoC and exit gracefully
    if df.empty or df[["beta","Dphi","Hdir"]].isna().all().all():
        coc_basket = 0.50
        coc_ref = 0.50
        delta = 0.0
        decile = 5
        # persist minimal artifacts
        (output_dir / "coc_assets_metrics.csv").write_text(df.to_csv())
        hist = pd.DataFrame([{"timestamp": pd.Timestamp.utcnow().isoformat(), "coc_basket": coc_basket}])
        hist.to_csv(output_dir / "coc_time_series.csv", index=False)
        summary = {"coc_basket": coc_basket, "coc_ref": coc_ref, "delta": delta, "coc_decile": decile}
        (output_dir / "coc_summary.json").write_text(json.dumps(summary, indent=2))
        (output_dir / "coc_delta.json").write_text(json.dumps({"delta": delta}, indent=2))
        return CoCResult(coc_basket, coc_ref, delta, decile)

    # Normalize and combine
    df_norm = df.apply(normalize_vec)
    df["CoC_asset"] = df_norm.apply(lambda r: combine_coc(r["beta"], r["Dphi"], r["Hdir"]), axis=1)
    coc_basket = float(df["CoC_asset"].median())

    # History
    ts_path = output_dir / "coc_time_series.csv"
    hist = pd.read_csv(ts_path) if ts_path.exists() else pd.DataFrame(columns=["timestamp","coc_basket"])
    hist = pd.concat([hist, pd.DataFrame([{"timestamp": pd.Timestamp.utcnow().isoformat(), "coc_basket": coc_basket}])], ignore_index=True)

    # Reference (bootstrap if short)
    coc_ref = float(hist["coc_basket"].median()) if len(hist) >= 10 else 0.50
    delta = coc_basket - coc_ref

    # Decile (guard for empty/NaN)
    series = hist["coc_basket"].dropna()
    if series.empty:
        decile = 5
    else:
        q = series.quantile([i/10 for i in range(11)])
        # find how many quantile thresholds current value exceeds
        decile = int((series.iloc[-1] >= q.values).sum() - 1)

    # Save outputs
    df.to_csv(output_dir / "coc_assets_metrics.csv")
    hist.to_csv(ts_path, index=False)
    summary = {"coc_basket": coc_basket, "coc_ref": coc_ref, "delta": delta, "coc_decile": decile}
    (output_dir / "coc_summary.json").write_text(json.dumps(summary, indent=2))
    (output_dir / "coc_delta.json").write_text(json.dumps({"delta": delta}, indent=2))

    return CoCResult(coc_basket, coc_ref, delta, decile)

def read_delta(path: Path = Path("C:/OPRT/data/derived/coc_delta.json")) -> float:
    try:
        return json.loads(path.read_text()).get("delta", 0.0) if path.exists() else 0.0
    except Exception:
        return 0.0

def read_coc_stats(path: Path = Path("C:/OPRT/data/derived/coc_summary.json")) -> dict:
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}
