def c_raw_from_sigma_delta_phi(sigma_delta_phi: float, kappa: float = 25.0) -> float:
    return 100.0 / (1.0 + kappa * float(sigma_delta_phi))

def phase_weight(deg: float) -> float:
    if deg <= 15: return 1.20
    if deg <= 45: return 1.00
    return 0.70
