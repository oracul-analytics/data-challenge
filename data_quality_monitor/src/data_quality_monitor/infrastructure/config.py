from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import tomli
from data_quality_monitor.domain.models.rules.rule import Expectation, TableRule


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
class ConsumerProfileConfig:
    timeout_seconds: float
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True


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
    consumer_profiles: Dict[str, ConsumerProfileConfig]
    producer_profiles: Dict[str, ProducerProfileConfig]


@dataclass(slots=True)
class RuleConfig:
    clickhouse: ClickHouseConfig
    kafka: KafkaConfig
    rules: tuple[TableRule, ...]

    @classmethod
    def load(cls, infra_path: Path, rules_path: Path) -> "RuleConfig":
        with infra_path.open("rb") as f:
            infra_raw = tomli.load(f)

        clickhouse_config = ClickHouseConfig(**infra_raw["clickhouse"])

        kafka_raw = infra_raw["kafka"]
        kafka_topic_config = KafkaTopicConfig(**kafka_raw["topic"])

        consumer_profiles_raw = kafka_raw.get("consumer_profiles", {})
        consumer_profiles = {name: ConsumerProfileConfig(**profile) for name, profile in consumer_profiles_raw.items()}

        producer_profiles_raw = kafka_raw.get("producer_profiles", {})
        producer_profiles = {name: ProducerProfileConfig(**profile) for name, profile in producer_profiles_raw.items()}

        kafka_config = KafkaConfig(
            bootstrap_servers=kafka_raw["bootstrap_servers"],
            topic=kafka_topic_config,
            consumer_profiles=consumer_profiles,
            producer_profiles=producer_profiles,
        )

        with rules_path.open("rb") as f:
            rules_raw = tomli.load(f)

        rules = []
        for table_data in rules_raw["tables"]:
            expectations = []
            for exp in table_data["expectations"]:
                exp_type = exp.pop("type")
                expectations.append(Expectation(type=exp_type, params=exp))
            rules.append(TableRule(table=table_data["name"], expectations=tuple(expectations)))

        return cls(
            clickhouse=clickhouse_config,
            kafka=kafka_config,
            rules=tuple(rules),
        )
