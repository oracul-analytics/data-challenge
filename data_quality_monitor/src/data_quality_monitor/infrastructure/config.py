from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import yaml

from data_quality_monitor.domain.models.rule import Expectation, TableRule


@dataclass(slots=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass(slots=True)
class KafkaTopicConfig:
    partitions: int
    replication_factor: int
    create_timeout_seconds: int
    delete_timeout_seconds: int


@dataclass(slots=True)
class KafkaConsumerConfig:
    default_timeout_seconds: float


@dataclass(slots=True)
class ProducerProfileConfig:
    linger_ms: int
    batch_size: int
    compression_type: str
    acks: int
    max_in_flight: int
    queue_buffering_max_messages: int
    queue_buffering_max_kbytes: int


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: KafkaTopicConfig
    consumer: KafkaConsumerConfig
    producer_profiles: Dict[str, ProducerProfileConfig]


@dataclass(slots=True)
class RuleConfig:
    clickhouse: ClickHouseConfig
    kafka: KafkaConfig
    rules: tuple[TableRule, ...]

    @classmethod
    def load(cls, infra_path: Path, rules_path: Path) -> "RuleConfig":
        with infra_path.open("r", encoding="utf-8") as f:
            infra_raw = yaml.safe_load(f)

        clickhouse_config = ClickHouseConfig(**infra_raw["clickhouse"])
        kafka_raw = infra_raw["kafka"]
        kafka_topic_config = KafkaTopicConfig(**kafka_raw["topic"])
        kafka_consumer_config = KafkaConsumerConfig(**kafka_raw["consumer"])
        producer_profiles_raw = kafka_raw.get("producer_profiles", {})
        producer_profiles = {name: ProducerProfileConfig(**profile) for name, profile in producer_profiles_raw.items()}
        kafka_config = KafkaConfig(
            bootstrap_servers=kafka_raw["bootstrap_servers"],
            topic=kafka_topic_config,
            consumer=kafka_consumer_config,
            producer_profiles=producer_profiles,
        )

        with rules_path.open("r", encoding="utf-8") as f:
            rules_raw = yaml.safe_load(f)

        rules = []
        for item in rules_raw["rules"]:
            expectations = [Expectation(type=exp.pop("type"), params=exp) for exp in item["expectations"]]
            rules.append(TableRule(table=item["table"], expectations=tuple(expectations)))

        return cls(
            clickhouse=clickhouse_config,
            kafka=kafka_config,
            rules=tuple(rules),
        )
