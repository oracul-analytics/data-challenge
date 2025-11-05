import uuid
from pathlib import Path
from dataclasses import dataclass

from confluent_kafka.admin import AdminClient, NewTopic
from loguru import logger

from data_quality_monitor.infrastructure.adapters.redpanda_producer import RedpandaProducer, ProducerConfig
from data_quality_monitor.infrastructure.adapters.redpanda_consumer import RedpandaConsumer, ConsumerConfig
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig, KafkaConfig
from data_quality_monitor.application.use_cases.process_rules import ProcessRulesUseCase


@dataclass
class KafkaRuntimeConfig:
    topic_name: str
    group_id: str

    @classmethod
    def random(cls) -> "KafkaRuntimeConfig":
        uid = uuid.uuid4().hex
        return cls(f"events_{uid}", f"dq_monitor_{uid}")


class TopicManager:
    def __init__(self, config: KafkaConfig):
        self.admin_client = AdminClient({"bootstrap.servers": config.bootstrap_servers})
        self.partitions = config.topic.partitions
        self.replication_factor = config.topic.replication_factor
        self.create_timeout_seconds = config.topic.create_timeout_seconds
        self.delete_timeout_seconds = config.topic.delete_timeout_seconds

    def create(self, topic_name: str):
        topic = NewTopic(topic_name, num_partitions=self.partitions, replication_factor=self.replication_factor)
        for _, future in self.admin_client.create_topics([topic]).items():
            future.result(timeout=self.create_timeout_seconds)
        logger.info(f"Topic created: {topic_name}")

    def delete(self, topic_name: str):
        for _, future in self.admin_client.delete_topics([topic_name], operation_timeout=self.delete_timeout_seconds).items():
            future.result()
        logger.info(f"Topic deleted: {topic_name}")


class KafkaService:
    def __init__(self, config: KafkaConfig, runtime: KafkaRuntimeConfig, producer_profile: str = "high_throughput"):
        self.config = config
        self.runtime = runtime
        self.producer_profile = producer_profile
        self.topic_manager = TopicManager(config)

    def setup(self):
        self.topic_manager.create(self.runtime.topic_name)

    def cleanup(self):
        self.topic_manager.delete(self.runtime.topic_name)

    def producer(self) -> RedpandaProducer:
        profile_cfg = self.config.producer_profiles[self.producer_profile]
        config = ProducerConfig(
            bootstrap_servers=self.config.bootstrap_servers,
            topic=self.runtime.topic_name,
            linger_ms=profile_cfg.linger_ms,
            batch_size=profile_cfg.batch_size,
            compression_type=profile_cfg.compression_type,
            acks=profile_cfg.acks,
            max_in_flight=profile_cfg.max_in_flight,
            queue_buffering_max_messages=profile_cfg.queue_buffering_max_messages,
            queue_buffering_max_kbytes=profile_cfg.queue_buffering_max_kbytes,
        )
        return RedpandaProducer(config=config, auto_flush_interval=config.linger_ms)

    def consume(self, repository: ClickHouseRepository, total_messages: int, consumer_profile_name: str = "low_latency"):
        profile = self.config.consumer_profiles[consumer_profile_name]
        consumer_config = ConsumerConfig(
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.runtime.group_id,
            topic=self.runtime.topic_name,
            timeout_seconds=profile.timeout_seconds,
            auto_offset_reset=profile.auto_offset_reset,
            enable_auto_commit=profile.enable_auto_commit,
        )
        consumer = RedpandaConsumer(config=consumer_config)
        consumer.consume(callback=repository.save_from_message, max_messages=total_messages)
        consumer.close()


class RunProcess:
    def __init__(self, infra_path: Path, rules_path: Path, consumer_profile: str = "low_latency"):
        self.config = RuleConfig.load(infra_path, rules_path)
        self.runtime = KafkaRuntimeConfig.random()
        self.repository = self._setup_repository()
        self.kafka_service = KafkaService(self.config.kafka, self.runtime, producer_profile="high_throughput")
        self.rules_use_case = ProcessRulesUseCase(self.repository)
        self.consumer_profile_name = consumer_profile

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
            self.kafka_service.consume(self.repository, total_messages, consumer_profile_name=self.consumer_profile_name)
            logger.debug("Pipeline completed successfully")
        finally:
            self.kafka_service.cleanup()
            logger.debug("Cleanup completed")
