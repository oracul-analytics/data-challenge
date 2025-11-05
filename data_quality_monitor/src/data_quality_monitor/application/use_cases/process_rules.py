from datetime import datetime, timezone
from loguru import logger

from data_quality_monitor.domain.models.rule import TableRule
from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.infrastructure.adapters.redpanda_producer import (
    RedpandaProducer,
)
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.application.services.schema_validation import SchemaValidator
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)


class RuleEvaluator:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def evaluate(self, rule: TableRule, schema_valid: bool, producer: RedpandaProducer) -> int:
        expectations = [e for e in rule.expectations if e.type != "schema"]
        if not expectations:
            return 0

        report = self._create_report(rule, expectations, schema_valid)
        producer.send_report(report)
        return len(report.results)

    def _create_report(self, rule: TableRule, expectations: list, schema_valid: bool) -> QualityReport:
        if not schema_valid:
            return QualityReport(
                table=rule.table,
                generated_at=datetime.now(timezone.utc),
                results=tuple(
                    RuleResult(
                        rule=f"{e.type}:{e.params.get('column', '')}",
                        passed=False,
                        details={"skipped_due_to_schema_failure": True},
                    )
                    for e in expectations
                ),
            )

        return engine.evaluate(
            TableRule(table=rule.table, expectations=tuple(expectations)),
            self.repository.fetch_table(rule.table),
        )


class ProcessRulesUseCase:
    def __init__(self, repository: ClickHouseRepository):
        self.schema_validator = SchemaValidator(repository)
        self.rule_evaluator = RuleEvaluator(repository)

    def execute(self, rules: list[TableRule], producer: RedpandaProducer) -> int:
        total_messages = 0

        for rule in rules:
            schema_valid, schema_msg_count = self._validate_schema(rule, producer)
            total_messages += schema_msg_count

            other_msg_count = self.rule_evaluator.evaluate(rule, schema_valid, producer)
            total_messages += other_msg_count

            if not schema_valid:
                logger.warning(f"Schema validation failed for table '{rule.table}'. All other rules marked as failed without execution.")

        return total_messages

    def _validate_schema(self, rule: TableRule, producer: RedpandaProducer) -> tuple[bool, int]:
        if any(e.type == "schema" for e in rule.expectations):
            return self.schema_validator.validate(rule, producer)
        return True, 0
