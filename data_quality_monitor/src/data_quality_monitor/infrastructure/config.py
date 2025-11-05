from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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
class KafkaConfig:
    bootstrap_servers: str
    topic: KafkaTopicConfig
    consumer: KafkaConsumerConfig


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
        kafka_config = KafkaConfig(
            bootstrap_servers=kafka_raw["bootstrap_servers"],
            topic=kafka_topic_config,
            consumer=kafka_consumer_config,
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
