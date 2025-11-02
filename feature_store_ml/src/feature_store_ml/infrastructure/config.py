from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(slots=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass(slots=True)
class FeatureConfig:
    lookback_hours: int
    bucket_minutes: int
    min_records: int


@dataclass(slots=True)
class TrainingConfig:
    test_size: float
    random_state: int
    num_boost_round: int
    learning_rate: float
    max_depth: int


@dataclass(slots=True)
class ServingConfig:
    batch_size: int
    threshold: float


@dataclass(slots=True)
class StoreConfig:
    clickhouse: ClickHouseConfig
    features: FeatureConfig
    training: TrainingConfig
    serving: ServingConfig

    @classmethod
    def load(cls, path: Path) -> "StoreConfig":
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
        return cls(
            clickhouse=ClickHouseConfig(**raw["clickhouse"]),
            features=FeatureConfig(**raw["features"]),
            training=TrainingConfig(**raw["training"]),
            serving=ServingConfig(**raw["serving"]),
        )
