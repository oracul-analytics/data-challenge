from data_quality_monitor.domain.models.rules.rule import TableRule
from data_quality_monitor.domain.models.rules.result import RuleResult


class SchemaValidator:
    def __init__(self, repository):
        self.repository = repository

    def validate(self, rule: TableRule) -> RuleResult:
        schema_expectations = [e for e in rule.expectations if e.type == "schema"]
        if not schema_expectations:
            return RuleResult(rule="schema", passed=True, details={})

        actual_schema = self.repository.get_table_schema(rule.table)
        expected_columns = schema_expectations[0].params.get("columns", {})

        missing_columns = [c for c in expected_columns if c not in actual_schema]
        extra_columns = [c for c in actual_schema if c not in expected_columns]
        type_mismatches = [
            {"column": col, "expected": expected_columns[col], "actual": actual_schema[col]}
            for col in expected_columns
            if col in actual_schema and self._normalize_type(expected_columns[col]) != self._normalize_type(actual_schema[col])
        ]

        passed = not (missing_columns or extra_columns or type_mismatches)
        details = {"missing": missing_columns, "extra": extra_columns}
        if type_mismatches:
            details["type_mismatches"] = type_mismatches

        return RuleResult(rule="schema", passed=passed, details=details)

    def _normalize_type(self, clickhouse_type: str) -> str:
        return clickhouse_type[9:-1] if clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")") else clickhouse_type
