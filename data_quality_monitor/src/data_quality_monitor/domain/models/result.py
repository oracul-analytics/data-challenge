from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Tuple

from data_quality_monitor.domain.models.rule import TableRule


@dataclass
class RuleResult:
    rule: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    table: str
    generated_at: datetime
    results: Tuple[RuleResult, ...] = field(default_factory=tuple)
