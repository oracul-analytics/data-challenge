from __future__ import annotations

from datetime import datetime
from typing import Callable, Mapping

import pandas as pd

from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.domain.models.rule import Expectation, TableRule

Evaluator = Callable[[pd.DataFrame, Expectation], RuleResult]


def completeness(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    column = expectation.params["column"]
    threshold = float(expectation.params.get("threshold", 1.0))
    ratio = frame[column].notna().mean() if column in frame.columns else 0.0
    return RuleResult(
        rule=f"completeness:{column}",
        passed=ratio >= threshold,
        details={"ratio": ratio, "threshold": threshold},
    )


def uniqueness(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    column = expectation.params["column"]
    threshold = float(expectation.params.get("threshold", 1.0))
    ratio = frame[column].nunique() / len(frame) if column in frame.columns and len(frame) else 0.0
    return RuleResult(
        rule=f"uniqueness:{column}",
        passed=ratio >= threshold,
        details={"ratio": ratio, "threshold": threshold},
    )


def range_check(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    column = expectation.params["column"]
    min_value = expectation.params.get("min")
    max_value = expectation.params.get("max")
    violations = frame[(frame[column] < min_value) | (frame[column] > max_value)] if column in frame.columns else frame
    passed = len(violations) == 0
    return RuleResult(
        rule=f"range:{column}",
        passed=passed,
        details={"violations": int(len(violations)), "min": min_value, "max": max_value},
    )


def schema(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    expected = expectation.params.get("columns", {})
    missing = [col for col in expected if col not in frame.columns]
    extra = [col for col in frame.columns if col not in expected]
    passed = not missing and not extra
    return RuleResult(
        rule="schema",
        passed=passed,
        details={"missing": missing, "extra": extra},
    )


REGISTRY: Mapping[str, Evaluator] = {
    "completeness": completeness,
    "uniqueness": uniqueness,
    "range": range_check,
    "schema": schema,
}


def evaluate(table_rule: TableRule, frame: pd.DataFrame) -> QualityReport:
    results: list[RuleResult] = []
    for expectation in table_rule.expectations:
        evaluator = REGISTRY.get(expectation.type)
        if evaluator is None:
            raise ValueError(f"unknown expectation {expectation.type}")
        results.append(evaluator(frame, expectation))
    return QualityReport(
        table=table_rule.table,
        generated_at=datetime.utcnow(),
        results=tuple(results),
    )
