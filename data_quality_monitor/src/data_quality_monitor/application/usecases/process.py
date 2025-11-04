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


class RunCheck:
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
        except Exception as e:
            logger.warning(f"Topic creation skipped: {e}")

    def delete_topic(self):
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        try:
            fs = admin_client.delete_topics([self.topic_name], operation_timeout=30)
            for _, f in fs.items():
                f.result()
        except Exception as e:
            logger.warning(f"Topic deletion skipped: {e}")

    def setup(self):
        self.create_topic()
        time.sleep(0.5)

    def run(self):
        producer = RedpandaProducer(bootstrap_servers=self.bootstrap_servers, topic=self.topic_name)
        total_expectations = 0

        for rule in self.config.rules:
            frame = self.repo.fetch_table(rule.table)
            report = engine.evaluate(rule, frame)

            total_expectations += len(report.results)
            producer.send_report(report)

        time.sleep(1.5)

        consumer = RedpandaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_name,
            group_id=self.group_id
        )

        try:
            consumer.consume(callback=self.repo.save_from_message, max_messages=total_expectations)
            time.sleep(1.0)
            reports = self.repo.list_reports()
        finally:
            consumer.close()

        assert len(reports) == total_expectations, f"Expected {total_expectations}, got {len(reports)}"
        assert all(reports["passed"] == 1), "Some rules failed"
        logger.info(f"✓ All {total_expectations} rules passed successfully")

    def cleanup(self):
        self.delete_topic()
        time.sleep(0.3)
        logger.info("Cleanup completed")

