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
    def create_with_random_names(cls):
        return cls(
            topic_name=f"events_{uuid.uuid4().hex}",
            group_id=f"dq_monitor_{uuid.uuid4().hex}",
        )


class TopicManager:
    def __init__(self, bootstrap_servers: str):
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def create_topic(self, topic_name: str):
        try:
            new_topic = NewTopic(topic_name, 1, 1)
            fs = self.admin_client.create_topics([new_topic])
            for _, f in fs.items():
                f.result(timeout=10)
            logger.info(f"Topic created: {topic_name}")
            return True
        except Exception as e:
            logger.warning(f"Topic creation skipped: {e}")
            return False

    def delete_topic(self, topic_name: str):
        try:
            fs = self.admin_client.delete_topics([topic_name], operation_timeout=30)
            for _, f in fs.items():
                f.result()
            logger.info(f"Topic deleted: {topic_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic_name}': {e}")
            return False


class RunProcess:
    def __init__(self, infra_path: Path, rules_path: Path):
        self.rule_config = RuleConfig.load(infra_path, rules_path)
        self.kafka_config = KafkaRuntimeConfig.create_with_random_names()
        self.bootstrap_servers = self.rule_config.kafka.bootstrap_servers
        self.topic_manager = TopicManager(self.bootstrap_servers)

        factory = ClickHouseFactory(self.rule_config.clickhouse)
        self.repository = ClickHouseRepository(factory=factory)
        self.repository.ensure_schema()

    def execute(self):
        try:
            self.topic_manager.create_topic(self.kafka_config.topic_name)

            producer = RedpandaProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.kafka_config.topic_name,
            )

            total_messages = 0
            for rule in self.rule_config.rules:
                frame = self.repository.fetch_table(rule.table)
                report = engine.evaluate(rule, frame)
                producer.send_report(report)
                total_messages += len(report.results)

            logger.info(f"Sent {total_messages} quality check results")

            consumer = RedpandaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                topic=self.kafka_config.topic_name,
                group_id=self.kafka_config.group_id,
            )

            try:
                consumer.consume(
                    callback=self.repository.save_from_message,
                    max_messages=total_messages,
                    timeout=1.0,
                )
                logger.info(f"Successfully consumed {total_messages} messages")
            finally:
                consumer.close()

            logger.info("Pipeline completed successfully")
        finally:
            self.topic_manager.delete_topic(self.kafka_config.topic_name)
            logger.info("Cleanup completed")
