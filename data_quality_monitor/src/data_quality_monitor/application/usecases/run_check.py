import uuid
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.adapters.redpanda_producer import (
    RedpandaProducer,
)
from data_quality_monitor.domain.models.rule import Expectation
from data_quality_monitor.infrastructure.adapters.redpanda_consumer import (
    RedpandaConsumer,
)
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.rules.engine import (
    QualityReport,
    TableRule,
    RuleResult,
)


@dataclass
class KafkaRuntimeConfig:
    topic_name: str
    group_id: str

    @classmethod
    def create_with_random_names(cls) -> "KafkaRuntimeConfig":
        return cls(
            topic_name=f"events_{uuid.uuid4().hex}",
            group_id=f"dq_monitor_{uuid.uuid4().hex}",
        )


class TopicManager:
    def __init__(self, bootstrap_servers: str) -> None:
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def create_topic(self, topic_name: str) -> bool:
        try:
            fs = self.admin_client.create_topics([NewTopic(topic_name, 1, 1)])
            for _, f in fs.items():
                f.result(timeout=10)
            logger.info(f"Topic created: {topic_name}")
            return True
        except Exception as e:
            logger.warning(f"Topic creation skipped: {e}")
            return False

    def delete_topic(self, topic_name: str) -> bool:
        try:
            fs = self.admin_client.delete_topics([topic_name], operation_timeout=30)
            for _, f in fs.items():
                f.result()
            logger.info(f"Topic deleted: {topic_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic_name}': {e}")
            return False


class SchemaValidator:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def validate(self, rule: TableRule, producer: RedpandaProducer) -> tuple[bool, int]:
        expectations = [e for e in rule.expectations if e.type == "schema"]
        if not expectations:
            return True, 0

        actual_schema = self.repository.get_table_schema(rule.table)

        enriched_expectations = []
        for exp in expectations:
            enriched_params = {**exp.params, "_actual_schema": actual_schema}
            enriched_expectations.append(
                Expectation(type=exp.type, params=enriched_params)
            )

        report = engine.evaluate(
            TableRule(table=rule.table, expectations=tuple(enriched_expectations)),
            self.repository.fetch_table(rule.table),
        )
        producer.send_report(report)
        return all(r.passed for r in report.results), len(report.results)


class RuleEvaluator:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def evaluate(
        self, rule: TableRule, schema_valid: bool, producer: RedpandaProducer
    ) -> int:
        expectations = [e for e in rule.expectations if e.type != "schema"]
        if not expectations:
            return 0

        report = self._create_report(rule, expectations, schema_valid)
        producer.send_report(report)
        return len(report.results)

    def _create_report(
        self, rule: TableRule, expectations: list, schema_valid: bool
    ) -> QualityReport:
        if schema_valid:
            return engine.evaluate(
                TableRule(table=rule.table, expectations=expectations),
                self.repository.fetch_table(rule.table),
            )

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


class RuleProcessor:
    def __init__(self, repository: ClickHouseRepository):
        self.schema_validator = SchemaValidator(repository)
        self.rule_evaluator = RuleEvaluator(repository)

    def process(self, rules: list[TableRule], producer: RedpandaProducer) -> int:
        schema_valid = self._validate_schemas(rules, producer)
        return self._evaluate_rules(rules, schema_valid, producer)

    def _validate_schemas(
        self, rules: list[TableRule], producer: RedpandaProducer
    ) -> bool:
        total_messages = 0
        all_valid = True

        for rule in rules:
            if any(e.type == "schema" for e in rule.expectations):
                valid, count = self.schema_validator.validate(rule, producer)
                total_messages += count
                all_valid &= valid

        return all_valid

    def _evaluate_rules(
        self, rules: list[TableRule], schema_valid: bool, producer: RedpandaProducer
    ) -> int:
        return sum(
            self.rule_evaluator.evaluate(rule, schema_valid, producer) for rule in rules
        )


class MessageConsumer:
    def __init__(self, bootstrap_servers: str, topic_config: KafkaRuntimeConfig):
        self.bootstrap_servers = bootstrap_servers
        self.topic_config = topic_config

    def consume(self, repository: ClickHouseRepository, total_messages: int) -> None:
        consumer = RedpandaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_config.topic_name,
            group_id=self.topic_config.group_id,
        )
        try:
            consumer.consume(
                callback=repository.save_from_message,
                max_messages=total_messages,
                timeout=1.0,
            )
            logger.info(f"Successfully consumed {total_messages} messages")
        finally:
            consumer.close()


class KafkaService:
    def __init__(self, bootstrap_servers: str, topic_config: KafkaRuntimeConfig):
        self.bootstrap_servers = bootstrap_servers
        self.topic_config = topic_config
        self.topic_manager = TopicManager(bootstrap_servers)
        self.consumer = MessageConsumer(bootstrap_servers, topic_config)

    def setup(self) -> None:
        self.topic_manager.create_topic(self.topic_config.topic_name)

    def cleanup(self) -> None:
        self.topic_manager.delete_topic(self.topic_config.topic_name)

    def create_producer(self) -> RedpandaProducer:
        return RedpandaProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_config.topic_name,
        )

    def consume_messages(
        self, repository: ClickHouseRepository, total_messages: int
    ) -> None:
        self.consumer.consume(repository, total_messages)


class RunProcess:
    def __init__(self, infra_path: Path, rules_path: Path) -> None:
        self.rule_config = RuleConfig.load(infra_path, rules_path)
        self.kafka_config = KafkaRuntimeConfig.create_with_random_names()

        factory = ClickHouseFactory(self.rule_config.clickhouse)
        self.repository = ClickHouseRepository(
            factory=factory, rule_config=self.rule_config
        )
        self.repository.ensure_schema_output()

        self.rule_processor = RuleProcessor(self.repository)
        self.kafka_service = KafkaService(
            self.rule_config.kafka.bootstrap_servers,
            self.kafka_config,
        )

    def execute(self) -> None:
        try:
            self.kafka_service.setup()
            producer = self.kafka_service.create_producer()

            total_messages = self.rule_processor.process(
                self.rule_config.rules, producer
            )
            logger.info(f"Sent {total_messages} quality check results")

            self.kafka_service.consume_messages(self.repository, total_messages)
            logger.info("Pipeline completed successfully")
        finally:
            self.kafka_service.cleanup()
            logger.info("Cleanup completed")
