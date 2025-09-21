from __future__ import annotations
from typing import Iterable
from contracts.signal import Signal

def hit_rate_from_realized(signals: Iterable[Signal], realized_moves: dict[str, float]) -> float:
    hits = total = 0
    for s in signals:
        key = s.key()
        if key not in realized_moves:
            continue
        r = realized_moves[key]
        pred_up = (s.side == "long")  and (s.strength > 0.5)
        pred_dn = (s.side == "short") and (s.strength > 0.5)
        if (pred_up and r > 0) or (pred_dn and r < 0) or (s.side == "flat" and abs(r) < 1e-6):
            hits += 1
        total += 1
    return hits / total if total else 0.0
