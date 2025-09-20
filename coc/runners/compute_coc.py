from pathlib import Path
import sys
from importlib import import_module

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CANDIDATES = ("coc.coc", "coc.core.coc")
_last_err = None
compute_and_persist = None
for modname in CANDIDATES:
    try:
        mod = import_module(modname)
        compute_and_persist = getattr(mod, "compute_and_persist")
        break
    except Exception as e:
        _last_err = e

if compute_and_persist is None:
    raise ImportError(f"Could not find compute_and_persist in {CANDIDATES}: {_last_err}")

def main():
    pkg_dir = Path(__file__).resolve().parents[1]
    out = pkg_dir / "outputs"
    out.mkdir(parents=True, exist_ok=True)
    compute_and_persist(output_dir=out)

if __name__ == "__main__":
    main()
