import importlib as _imp

try:
    m = _imp.import_module('coc.core.coherence')
except ModuleNotFoundError:
    m = _imp.import_module('coc.coherence')

def test_api_surface_exists():
    assert hasattr(m, 'c_raw_from_sigma_delta_phi')
    assert hasattr(m, 'phase_weight')

def test_c_raw_monotonic():
    f = m.c_raw_from_sigma_delta_phi
    assert f(0.0) == 100.0
    assert f(0.01) > f(0.05) > f(0.1)

def test_phase_weight_bands():
    w = m.phase_weight
    assert w(10) == 1.20
    assert w(30) == 1.00
    assert w(60) == 0.70
