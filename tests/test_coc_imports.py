import importlib, pytest

def test_import_coc_modules():
    # core modules that should import without heavy deps
    for name in [
        "coc.bridge",
        "coc.core.coc_metrics",
    ]:
        importlib.import_module(name)

def test_import_sources_if_yfinance_present():
    # Only run this import when yfinance is installed
    pytest.importorskip("yfinance")
    importlib.import_module("coc.core.coc_sources")

def test_compute_coc_has_entrypoint():
    m = importlib.import_module("coc.runners.compute_coc")
    # accept common entrypoint names
    assert any(hasattr(m, attr) for attr in ("compute_and_persist", "main", "run"))
