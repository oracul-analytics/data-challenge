from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence


@dataclass(frozen=True, slots=True)
class Anomaly:
    detector: str
    score: float
    severity: float
    description: str
    window_start: datetime
    window_end: datetime


@dataclass(frozen=True, slots=True)
class AnomalyReport:
    generated_at: datetime
    window_start: datetime
    window_end: datetime
    anomalies: Sequence[Anomaly]

    def highest_severity(self) -> float:
        if not self.anomalies:
            return 0.0
        return max(item.severity for item in self.anomalies)
