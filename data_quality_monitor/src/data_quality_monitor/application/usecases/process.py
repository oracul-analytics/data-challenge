import time
import uuid
from pathlib import Path
from dataclasses import dataclass
from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.redpanda_producer import (
    RedpandaProducer,
)
from data_quality_monitor.infrastructure.repositories.redpanda_consumer import (
    RedpandaConsumer,
)
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig


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
    def __init__(self, bootstrap_servers: str):
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ) -> bool:
        try:
            new_topic = NewTopic(topic_name, num_partitions, replication_factor)
            fs = self.admin_client.create_topics([new_topic])
            for _, f in fs.items():
                f.result(timeout=10)
            logger.info("Topic created: {}", topic_name)
            return True
        except Exception as e:
            logger.warning("Topic creation skipped: {}", e)
            return False

    def delete_topic(
        self, topic_name: str, retries: int = 3, delay: float = 1.0
    ) -> bool:
        for attempt in range(1, retries + 1):
            try:
                fs = self.admin_client.delete_topics([topic_name], operation_timeout=30)
                for _, f in fs.items():
                    f.result()
                logger.info("Topic deleted: {}", topic_name)
                return True
            except Exception as e:
                logger.warning("Attempt {}: Topic deletion failed: {}", attempt, e)
                if attempt < retries:
                    time.sleep(delay)

        logger.error(
            "Failed to delete topic '{}' after {} attempts", topic_name, retries
        )
        return False


class QualityCheckProcessor:
    def __init__(self, repository: ClickHouseRepository, config: RuleConfig):
        self.repository = repository
        self.config = config

    def process_and_send(self, producer: RedpandaProducer) -> int:
        total_messages = 0

        for rule in self.config.rules:
            frame = self.repository.fetch_table(rule.table)
            report = engine.evaluate(rule, frame)
            producer.send_report(report)
            total_messages += len(report.results)

        logger.info("Sent {} quality check results", total_messages)
        return total_messages


class ReportConsumer:
    def __init__(self, repository: ClickHouseRepository):
        self.repository = repository

    def consume_and_save(
        self, consumer: RedpandaConsumer, expected_messages: int, timeout: float = 1.0
    ):
        try:
            consumer.consume(
                callback=self.repository.save_from_message,
                max_messages=expected_messages,
                timeout=timeout,
            )
            logger.info("Successfully consumed {} messages", expected_messages)
        finally:
            consumer.close()


class RunProcess:
    def __init__(self, infra_path: Path, rules_path: Path):
        self.rule_config = RuleConfig.load(infra_path, rules_path)
        self.kafka_config = KafkaRuntimeConfig.create_with_random_names()

        self.bootstrap_servers = self.rule_config.kafka.bootstrap_servers
        self.topic_manager = TopicManager(self.bootstrap_servers)
        self._setup_repository()

        self.processor = QualityCheckProcessor(self.repository, self.rule_config)
        self.consumer_handler = ReportConsumer(self.repository)

    def _setup_repository(self):
        factory = ClickHouseFactory(self.rule_config.clickhouse)
        self.repository = ClickHouseRepository(factory=factory)
        self.repository.ensure_schema()

    def setup(self):
        self.topic_manager.create_topic(self.kafka_config.topic_name)

    def run(self):
        producer = RedpandaProducer(
            bootstrap_servers=self.bootstrap_servers, topic=self.kafka_config.topic_name
        )
        total_messages = self.processor.process_and_send(producer)

        consumer = RedpandaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.kafka_config.topic_name,
            group_id=self.kafka_config.group_id,
        )
        self.consumer_handler.consume_and_save(consumer, total_messages)

        logger.info("Pipeline completed successfully")

    def cleanup(self):
        self.topic_manager.delete_topic(self.kafka_config.topic_name)
        logger.info("Cleanup completed")

    def execute(self):
        try:
            self.setup()
            self.run()
        finally:
            self.cleanup()
