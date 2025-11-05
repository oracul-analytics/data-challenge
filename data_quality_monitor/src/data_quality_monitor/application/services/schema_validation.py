from datetime import datetime, timezone
from loguru import logger

from data_quality_monitor.domain.models.result import RuleResult, QualityReport
from data_quality_monitor.domain.models.rule import TableRule
from data_quality_monitor.infrastructure.adapters.redpanda_producer import RedpandaProducer


class SchemaValidator:
    def __init__(self, repository):
        self.repository = repository

    def validate(self, rule: TableRule, producer: RedpandaProducer) -> tuple[bool, int]:
        schema_expectations = self._get_schema_expectations(rule)
        if not schema_expectations:
            return True, 0

        actual_schema = self.repository.get_table_schema(rule.table)
        expected_columns = schema_expectations[0].params.get("columns", {})

        validation_result = self._compare_schemas(expected_columns, actual_schema)

        self._send_validation_report(rule.table, validation_result, producer)
        self._log_validation_result(rule.table, validation_result.passed)

        return validation_result.passed, 1

    def _get_schema_expectations(self, rule: TableRule) -> list:
        return [e for e in rule.expectations if e.type == "schema"]

    def _compare_schemas(self, expected_columns: dict[str, str], actual_schema: dict[str, str]) -> RuleResult:
        missing_columns = self._find_missing_columns(expected_columns, actual_schema)
        extra_columns = self._find_extra_columns(expected_columns, actual_schema)
        type_mismatches = self._find_type_mismatches(expected_columns, actual_schema)

        passed = not (missing_columns or extra_columns or type_mismatches)

        details = {
            "missing": missing_columns,
            "extra": extra_columns,
        }

        if type_mismatches:
            details["type_mismatches"] = type_mismatches

        return RuleResult(rule="schema", passed=passed, details=details)

    def _find_missing_columns(self, expected_columns: dict[str, str], actual_schema: dict[str, str]) -> list[str]:
        return [col for col in expected_columns if col not in actual_schema]

    def _find_extra_columns(self, expected_columns: dict[str, str], actual_schema: dict[str, str]) -> list[str]:
        return [col for col in actual_schema if col not in expected_columns]

    def _find_type_mismatches(self, expected_columns: dict[str, str], actual_schema: dict[str, str]) -> list[dict]:
        mismatches = []

        for col_name, expected_type in expected_columns.items():
            if col_name not in actual_schema:
                continue
            actual_type = actual_schema[col_name]
            if not self._types_match(expected_type, actual_type):
                mismatches.append(
                    {
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_type,
                    }
                )

        return mismatches

    def _normalize_type(self, clickhouse_type: str) -> str:
        if clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")"):
            return clickhouse_type[9:-1]
        return clickhouse_type

    def _types_match(self, expected_type: str, actual_type: str) -> bool:
        normalized_expected = self._normalize_type(expected_type)
        normalized_actual = self._normalize_type(actual_type)
        return normalized_expected == normalized_actual

    def _send_validation_report(self, table_name: str, validation_result: RuleResult, producer: RedpandaProducer) -> None:
        report = QualityReport(table=table_name, generated_at=datetime.now(timezone.utc), results=(validation_result,))
        producer.send_report(report)

    def _log_validation_result(self, table_name: str, passed: bool) -> None:
        status = "passed" if passed else "failed"
        logger.info(f"Schema validation for table '{table_name}': {status}")
