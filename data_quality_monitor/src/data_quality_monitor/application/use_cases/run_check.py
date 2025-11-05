import uuid
from pathlib import Path
from dataclasses import dataclass

from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from data_quality_monitor.infrastructure.adapters.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.adapters.redpanda_consumer import RedpandaConsumer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig, KafkaTopicConfig, KafkaConsumerConfig, KafkaConfig
from data_quality_monitor.application.use_cases.process_rules import ProcessRulesUseCase, RuleEvaluator


@dataclass
class KafkaRuntimeConfig:
    topic_name: str
    group_id: str

    @classmethod
    def random(cls) -> "KafkaRuntimeConfig":
        uid = uuid.uuid4().hex
        return cls(f"events_{uid}", f"dq_monitor_{uid}")


class TopicManager:
    def __init__(self, bootstrap_servers: str, topic_config):
        self.admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
        self.topic_config = topic_config

    def create(self, topic_name: str):
        topic = NewTopic(topic_name, num_partitions=self.topic_config.partitions, replication_factor=self.topic_config.replication_factor)
        for _, future in self.admin_client.create_topics([topic]).items():
            future.result(timeout=self.topic_config.create_timeout_seconds)
        logger.info(f"Topic created: {topic_name}")

    def delete(self, topic_name: str):
        for _, future in self.admin_client.delete_topics([topic_name], operation_timeout=self.topic_config.delete_timeout_seconds).items():
            future.result()
        logger.info(f"Topic deleted: {topic_name}")


class KafkaService:
    def __init__(self, config: KafkaConfig, runtime: KafkaRuntimeConfig):
        self.bootstrap_servers = config.bootstrap_servers
        self.runtime = runtime
        self.topic_manager = TopicManager(self.bootstrap_servers, config.topic)
        self.consumer_config = config.consumer

    def setup(self):
        self.topic_manager.create(self.runtime.topic_name)

    def cleanup(self):
        self.topic_manager.delete(self.runtime.topic_name)

    def producer(self):
        return RedpandaProducer(bootstrap_servers=self.bootstrap_servers, topic=self.runtime.topic_name)

    def consume(self, repository: ClickHouseRepository, total_messages: int):
        consumer = RedpandaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topic=self.runtime.topic_name,
            group_id=self.runtime.group_id,
            timeout_seconds=self.consumer_config.default_timeout_seconds,
        )
        consumer.consume(callback=repository.save_from_message, max_messages=total_messages)
        consumer.close()


class RunProcess:
    def __init__(self, infra_path: Path, rules_path: Path):
        self.config = RuleConfig.load(infra_path, rules_path)
        self.runtime = KafkaRuntimeConfig.random()
        self.repository = self._setup_repository()
        self.kafka_service = KafkaService(self.config.kafka, self.runtime)
        self.rules_use_case = ProcessRulesUseCase(self.repository)

    def _setup_repository(self):
        factory = ClickHouseFactory(self.config.clickhouse)
        repo = ClickHouseRepository(factory=factory, rule_config=self.config)
        repo.ensure_schema_output()
        return repo

    def execute(self):
        try:
            self.kafka_service.setup()
            producer = self.kafka_service.producer()
            total_messages = self.rules_use_case.execute(self.config.rules, producer)
            self.kafka_service.consume(self.repository, total_messages)
            logger.debug("Pipeline completed successfully")
        finally:
            self.kafka_service.cleanup()
            logger.debug("Cleanup completed")
