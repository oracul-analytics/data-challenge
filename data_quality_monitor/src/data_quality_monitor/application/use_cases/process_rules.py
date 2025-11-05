from datetime import datetime, timezone
from data_quality_monitor.domain.models.rules.rule import TableRule
from data_quality_monitor.domain.models.rules.result import QualityReport, RuleResult
from data_quality_monitor.infrastructure.adapters.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.application.services.schema_validation import SchemaValidator
from data_quality_monitor.infrastructure.serializers.kafka.payload_serializer import PayloadOutputSerializer


class RuleEvaluator:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def evaluate(self, rule: TableRule, schema_valid: bool) -> QualityReport:
        non_schema_expectations = [e for e in rule.expectations if e.type != "schema"]
        results = []

        if schema_valid:
            table_data = self.repository.fetch_table(rule.table)
            report = engine.evaluate(TableRule(table=rule.table, expectations=tuple(non_schema_expectations)), table_data)
            results.extend(report.results)
        else:
            results.extend(
                RuleResult(rule=f"{e.type}:{e.params.get('column', '')}", passed=False, details={"skipped_due_to_schema_failure": True})
                for e in non_schema_expectations
            )

        return QualityReport(table=rule.table, generated_at=datetime.now(timezone.utc), results=tuple(results))


class ProcessRulesUseCase:
    def __init__(self, repository: ClickHouseRepository):
        self.schema_validator = SchemaValidator(repository)
        self.rule_evaluator = RuleEvaluator(repository)

    def execute(self, rules: list[TableRule], producer: RedpandaProducer) -> int:
        total_messages = 0
        for rule in rules:
            schema_result = self.schema_validator.validate(rule)
            report = self.rule_evaluator.evaluate(rule, schema_result.passed)
            self._send_results(report, schema_result, producer)
            total_messages += len(report.results) + 1
        return total_messages

    def _send_results(self, report: QualityReport, schema_result: RuleResult, producer: RedpandaProducer):
        for r in report.results + (schema_result,):
            msg = {
                "table_name": report.table,
                "rule": r.rule,
                "passed": int(r.passed),
                "details": str(r.details),
                "generated_at": report.generated_at.isoformat(),
            }
            producer.send_payload(key=report.table, payload=PayloadOutputSerializer.to_json(msg))
