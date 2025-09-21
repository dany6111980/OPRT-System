from __future__ import annotations
from pathlib import Path
from typing import Iterable, List
import json
from contracts.signal import Signal

def write_signals_jsonl(signals: Iterable[Signal], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "signals.jsonl"
    with path.open("a", encoding="utf-8", newline="\n") as f:
        for s in signals:
            f.write(s.model_dump_json() + "\n")
    return path

def load_signals_jsonl(path: Path) -> List[Signal]:
    out: List[Signal] = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(Signal.model_validate_json(line))
    return out
