from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from contracts.signal import Signal
from analytics.signals_io import write_signals_jsonl, load_signals_jsonl
from analytics.metrics import hit_rate_from_realized

UTC = timezone.utc

def _utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)

def test_engine_to_analytics_roundtrip(tmp_path: Path):
    s1 = Signal(ts_src=_utc("2025-09-19T14:00:00"), ts_effective=_utc("2025-09-19T14:05:00"),
                symbol="BTC", horizon_min=5, side="long", strength=0.9, price_ref=60000.0)
    s2 = Signal(ts_src=_utc("2025-09-19T14:00:00"), ts_effective=_utc("2025-09-19T14:05:00"),
                symbol="ETH", horizon_min=5, side="short", strength=0.8, price_ref=2300.0)

    out_file = write_signals_jsonl([s1, s2], tmp_path)
    loaded = load_signals_jsonl(out_file)
    assert len(loaded) == 2
    realized = { s1.key(): +0.012, s2.key(): -0.007 }
    assert hit_rate_from_realized(loaded, realized) == 1.0
