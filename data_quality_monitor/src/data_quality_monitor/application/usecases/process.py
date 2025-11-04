import time
import uuid
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.repositories.redpanda_consumer import RedpandaConsumer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from loguru import logger


class RunProcess:
    def __init__(self, config_path: Path, bootstrap_servers: str = "localhost:39092"):
        self.bootstrap_servers = bootstrap_servers
        self.config_path = config_path
        self.topic_name = f"events_{uuid.uuid4().hex}"
        self.group_id = f"dq_monitor_{uuid.uuid4().hex}"

        self.config = RuleConfig.load(self.config_path)
        factory = ClickHouseFactory(self.config.clickhouse)
        self.repo = ClickHouseRepository(factory=factory)
        self.repo.ensure_schema()

    def create_topic(self):
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        try:
            new_topic = NewTopic(self.topic_name, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for _, f in fs.items():
                f.result(timeout=10)
            logger.info("Topic created: {}", self.topic_name)
        except Exception as e:
            logger.warning("Topic creation skipped: {}", e)

    def delete_topic(self, retries: int = 3, delay: float = 1.0):
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})

        for attempt in range(1, retries + 1):
            try:
                fs = admin_client.delete_topics([self.topic_name], operation_timeout=30)
                for _, f in fs.items():
                    f.result()
                logger.info("Topic deleted: {}", self.topic_name)
                return
            except Exception as e:
                logger.warning("Attempt {}: Topic deletion failed: {}", attempt, e)
                time.sleep(delay)

        logger.error("Failed to delete topic '{}' after {} attempts", self.topic_name, retries)


    def setup(self):
        self.create_topic()
        time.sleep(0.5)

    def run(self):
        producer = RedpandaProducer(bootstrap_servers=self.bootstrap_servers, topic=self.topic_name)

        total_messages = 0
        for rule in self.config.rules:
            frame = self.repo.fetch_table(rule.table)
            report = engine.evaluate(rule, frame)
            producer.send_report(report)
            total_messages += len(report.results)

        logger.info("Sent {} reports to topic '{}'", total_messages, self.topic_name)
        time.sleep(1)

        consumer = RedpandaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_name,
            group_id=self.group_id
        )

        try:
            consumer.consume(
                callback=self.repo.save_from_message,
                max_messages=total_messages,
                timeout=1.0
            )
        finally:
            consumer.close()

        logger.info("RunProcess completed successfully")

    def cleanup(self):
        self.delete_topic()
        time.sleep(0.3)
        logger.info("Cleanup completed")
