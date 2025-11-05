from datetime import datetime, timezone
from loguru import logger

from data_quality_monitor.domain.models.rule import TableRule
from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.infrastructure.adapters.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.application.services.schema_validation import SchemaValidator
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class RuleEvaluator:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def evaluate(self, rule: TableRule, schema_valid: bool, producer: RedpandaProducer) -> int:
        non_schema_expectations = [e for e in rule.expectations if e.type != "schema"]
        if not non_schema_expectations:
            return 0

        if not schema_valid:
            report = self._build_skipped_report(rule.table, non_schema_expectations)
        else:
            table_data = self.repository.fetch_table(rule.table)
            table_rule = TableRule(table=rule.table, expectations=tuple(non_schema_expectations))
            report = engine.evaluate(table_rule, table_data)

        producer.send_report(report)
        return len(report.results)

    def _build_skipped_report(self, table_name: str, expectations: list) -> QualityReport:
        results = tuple(
            RuleResult(rule=f"{e.type}:{e.params.get('column', '')}", passed=False, details={"skipped_due_to_schema_failure": True})
            for e in expectations
        )
        return QualityReport(
            table=table_name,
            generated_at=datetime.now(timezone.utc),
            results=results,
        )


class ProcessRulesUseCase:
    def __init__(self, repository: ClickHouseRepository):
        self.schema_validator = SchemaValidator(repository)
        self.rule_evaluator = RuleEvaluator(repository)

    def execute(self, rules: list[TableRule], producer: RedpandaProducer) -> int:
        total = 0
        for rule in rules:
            has_schema = any(e.type == "schema" for e in rule.expectations)
            schema_valid, schema_count = self.schema_validator.validate(rule, producer) if has_schema else (True, 0)

            other_count = self.rule_evaluator.evaluate(rule, schema_valid, producer)

            if not schema_valid:
                logger.warning(f"Schema validation failed for table '{rule.table}'. All other rules marked as failed without execution.")

            total += schema_count + other_count

        return total
