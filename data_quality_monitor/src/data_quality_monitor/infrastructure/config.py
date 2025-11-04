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
class KafkaConfig:
    bootstrap_servers: str


@dataclass(slots=True)
class RuleConfig:
    clickhouse: ClickHouseConfig
    kafka: KafkaConfig
    rules: tuple[TableRule, ...]

    @classmethod
    def load(cls, path: Path) -> "RuleConfig":
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
        rules = []
        for item in raw["rules"]:
            expectations = [
                Expectation(type=exp.pop("type"), params=exp)
                for exp in item["expectations"]
            ]
            rules.append(
                TableRule(table=item["table"], expectations=tuple(expectations))
            )
        return cls(
            clickhouse=ClickHouseConfig(**raw["clickhouse"]),
            kafka=KafkaConfig(**raw["kafka"]),
            rules=tuple(rules),
        )
