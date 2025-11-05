import uuid
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger
import pandas as pd

from data_quality_monitor.infrastructure.adapters.redpanda_producer import (
    RedpandaProducer,
)
from data_quality_monitor.domain.models.rule import Expectation, TableRule
from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.infrastructure.adapters.redpanda_consumer import (
    RedpandaConsumer,
)
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.application.services.schema_validation import SchemaValidator


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
        # Если схема невалидна - все правила автоматически failed БЕЗ чтения данных
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

        # Если схема валидна - выполняем проверки
        return engine.evaluate(
            TableRule(table=rule.table, expectations=tuple(expectations)),
            self.repository.fetch_table(rule.table),
        )


class RuleProcessor:
    def __init__(self, repository: ClickHouseRepository):
        self.schema_validator = SchemaValidator(repository)
        self.rule_evaluator = RuleEvaluator(repository)

    def process(self, rules: list[TableRule], producer: RedpandaProducer) -> int:
        total_messages = 0

        for rule in rules:
            # Шаг 1: Проверяем schema (если есть)
            schema_valid, schema_msg_count = self._validate_schema_for_rule(
                rule, producer
            )
            total_messages += schema_msg_count

            # Шаг 2: Обрабатываем остальные правила
            # Если schema failed (schema_valid=False), то все правила будут автоматически failed
            other_msg_count = self.rule_evaluator.evaluate(rule, schema_valid, producer)
            total_messages += other_msg_count

            if not schema_valid:
                logger.warning(
                    f"Schema validation failed for table '{rule.table}'. "
                    f"All other rules marked as failed without execution."
                )

        return total_messages

    def _validate_schema_for_rule(
        self, rule: TableRule, producer: RedpandaProducer
    ) -> tuple[bool, int]:
        """Проверяет schema для правила. Возвращает (валидна ли схема, количество сообщений)"""
        if any(e.type == "schema" for e in rule.expectations):
            return self.schema_validator.validate(rule, producer)
        # Если нет проверки schema - считаем что схема валидна
        return True, 0


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
            bootstrap_servers=self.bootstrap_servers, topic=self.topic_config.topic_name
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
            self.rule_config.kafka.bootstrap_servers, self.kafka_config
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
