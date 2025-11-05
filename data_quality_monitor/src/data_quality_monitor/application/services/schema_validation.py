from datetime import datetime, timezone
from loguru import logger
from data_quality_monitor.domain.models.result import RuleResult, QualityReport
from data_quality_monitor.domain.models.rule import TableRule
from data_quality_monitor.infrastructure.adapters.redpanda_producer import (
    RedpandaProducer,
)


class SchemaValidator:
    def __init__(self, repository):
        self.repository = repository

    def _normalize_type(self, clickhouse_type: str) -> str:
        if clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")"):
            clickhouse_type = clickhouse_type[9:-1]
        return clickhouse_type

    def _types_match(self, expected_type: str, actual_type: str) -> bool:
        return self._normalize_type(expected_type) == self._normalize_type(actual_type)

    def validate(self, rule: TableRule, producer: RedpandaProducer) -> tuple[bool, int]:
        expectations = [e for e in rule.expectations if e.type == "schema"]
        if not expectations:
            return True, 0

        actual_schema = self.repository.get_table_schema(rule.table)
        expected_columns = expectations[0].params.get("columns", {})

        missing = [col for col in expected_columns if col not in actual_schema]
        extra = [col for col in actual_schema if col not in expected_columns]

        type_mismatches = [
            {
                "column": col,
                "expected": expected_columns[col],
                "actual": actual_schema[col],
            }
            for col in expected_columns
            if col in actual_schema
            and not self._types_match(expected_columns[col], actual_schema[col])
        ]

        passed = not missing and not extra and not type_mismatches

        details = {"missing": missing, "extra": extra}
        if type_mismatches:
            details["type_mismatches"] = type_mismatches

        result = RuleResult(rule="schema", passed=passed, details=details)

        report = QualityReport(
            table=rule.table, generated_at=datetime.now(timezone.utc), results=(result,)
        )
        producer.send_report(report)
        logger.info(
            f"Schema validation for table '{rule.table}': {'passed' if passed else 'failed'}"
        )

        return passed, 1
