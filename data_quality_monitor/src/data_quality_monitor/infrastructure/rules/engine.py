from __future__ import annotations

from datetime import datetime
from typing import Callable, Mapping

import pandas as pd

from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.domain.models.rule import Expectation, TableRule
from datetime import datetime, timezone

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
    ratio = (
        frame[column].nunique() / len(frame)
        if column in frame.columns and len(frame)
        else 0.0
    )
    return RuleResult(
        rule=f"uniqueness:{column}",
        passed=ratio >= threshold,
        details={"ratio": ratio, "threshold": threshold},
    )


def range_check(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    column = expectation.params["column"]
    min_value = expectation.params.get("min")
    max_value = expectation.params.get("max")
    violations = (
        frame[(frame[column] < min_value) | (frame[column] > max_value)]
        if column in frame.columns
        else frame
    )
    passed = len(violations) == 0
    return RuleResult(
        rule=f"range:{column}",
        passed=passed,
        details={
            "violations": int(len(violations)),
            "min": min_value,
            "max": max_value,
        },
    )


def schema(frame: pd.DataFrame, expectation: Expectation) -> RuleResult:
    expected = expectation.params.get("columns", {})
    actual_schema = expectation.params.get("_actual_schema", {})

    if not actual_schema:
        missing = [col for col in expected if col not in frame.columns]
        extra = [col for col in frame.columns if col not in expected]
        return RuleResult(
            rule="schema",
            passed=not missing and not extra,
            details={
                "missing": missing,
                "extra": extra,
                "warning": "Type checking skipped - schema not provided",
            },
        )

    missing = [col for col in expected if col not in actual_schema]
    extra = [col for col in actual_schema if col not in expected]

    type_mismatches = []
    for col, expected_type in expected.items():
        if col in actual_schema:
            actual_type = actual_schema[col]
            normalized = actual_type
            for wrapper in ["Nullable(", "LowCardinality("]:
                if wrapper in normalized:
                    normalized = normalized.replace(wrapper, "").rstrip(")")

            if normalized != expected_type:
                type_mismatches.append(
                    {"column": col, "expected": expected_type, "actual": actual_type}
                )

    passed = not missing and not extra and not type_mismatches
    return RuleResult(
        rule="schema",
        passed=passed,
        details={
            "missing": missing,
            "extra": extra,
            "type_mismatches": type_mismatches,
        },
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
        generated_at=datetime.now(timezone.utc),
        results=tuple(results),
    )
