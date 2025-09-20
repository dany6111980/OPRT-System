from __future__ import annotations
from pprint import pprint

# 1) verify the core API is present
import importlib
m = importlib.import_module("coc.core.coc")
print("core import OK:", m.__name__, "has read_delta:", hasattr(m, "read_delta"))

# 2) exercise the bridge (this assumes you dropped C:\OPRT\coc\bridge.py as we discussed)
try:
    from coc.bridge import run_local_phases, compute_c_global
except Exception as e:
    print("bridge import failed:", e)
    raise SystemExit(1)

assets = run_local_phases()
print("assets from CoC:")
pprint(assets)

if assets and "BTC" in assets:
    cg = compute_c_global(assets)
    print("\nC_global pack:")
    pprint(cg)
    if cg:
        delta = cg["deltas"].get("BTC", 180.0)
        # this is the exact rule the engine will apply
        if   delta <= 15.0: mult = 1.20
        elif delta > 45.0:  mult = 0.70
        else:               mult = 1.00
        print(f"\nÎ”Ï•(BTC vs C_global)={delta:.1f}Â°  â†’ multiplier {mult:.2f}")
else:
    print("\n(run_local_phases returned nothing; wire your CoC outputs into bridge.run_local_phases())")
