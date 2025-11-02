from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


@dataclass
class Expectation:
    """
    Описывает одно правило проверки данных.
    Например: Expectation("completeness", {"column": "value", "threshold": 1.0})
    """
    type: str
    params: Dict[str, Any]


@dataclass
class RuleResult:
    """
    Результат проверки одного Expectation.
    """
    expectation: Expectation
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableRule:
    """
    Правила, применяемые к таблице (DataFrame).
    """
    table: str
    expectations: Tuple[Expectation, ...]

    def run(self, engine, df) -> "RuleReport":
        results = [engine.evaluate_expectation(e, df) for e in self.expectations]
        return RuleReport(self, results)


@dataclass
class RuleReport:
    """
    Отчёт по проверкам таблицы.
    """
    rule: TableRule
    results: List[RuleResult]
