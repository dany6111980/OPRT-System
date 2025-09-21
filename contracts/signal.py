from __future__ import annotations
from pydantic import BaseModel, Field, conint, confloat
from datetime import datetime
from typing import Dict, Any, Optional
import uuid

class Signal(BaseModel):
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    ts_src: datetime                 # timestamp of the bar used to make the signal
    ts_effective: datetime           # when the signal should be acted on/evaluated
    symbol: str
    horizon_min: conint(ge=1)
    side: str                        # "long" | "short" | "flat"
    strength: confloat(ge=0, le=1)   # 0..1
    price_ref: Optional[float] = None
    gates: Dict[str, Any] = {}
    features: Dict[str, float] = {}
    source_commit: Optional[str] = None

    class Config:
        json_encoders = {datetime: lambda dt: dt.replace(microsecond=0).isoformat()}

    def key(self) -> str:
        return f"{self.symbol}|{self.horizon_min}|{self.ts_src.isoformat()}"
