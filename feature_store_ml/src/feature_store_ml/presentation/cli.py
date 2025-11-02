from __future__ import annotations

from pathlib import Path

import typer

from feature_store_ml.application.use_cases.materialize_features import MaterializeFeatures
from feature_store_ml.application.use_cases.serve_predictions import ServePredictions
from feature_store_ml.application.use_cases.train_model import TrainModel
from feature_store_ml.infrastructure.clients.clickhouse import ClickHouseFactory
from feature_store_ml.infrastructure.config import StoreConfig
from feature_store_ml.infrastructure.datasets.builder import DatasetBuilder
from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository

app = typer.Typer()


def _bootstrap(config_path: Path) -> tuple[StoreConfig, ClickHouseRepository, FeatureRegistry, LightGBMTrainer]:
    cfg = StoreConfig.load(config_path)
    repository = ClickHouseRepository(factory=ClickHouseFactory(cfg.clickhouse))
    registry = FeatureRegistry.default()
    trainer = LightGBMTrainer(config=cfg.training, artifacts_dir=config_path.parent / "artifacts")
    return cfg, repository, registry, trainer


@app.command()
def materialize(config: Path = typer.Option(..., exists=True)) -> None:
    cfg, repository, registry, trainer = _bootstrap(config)
    use_case = MaterializeFeatures(repository=repository, registry=registry)
    use_case.execute(lookback_hours=cfg.features.lookback_hours)


@app.command()
def train(config: Path = typer.Option(..., exists=True)) -> None:
    cfg, repository, registry, trainer = _bootstrap(config)
    dataset_builder = DatasetBuilder(registry=registry)
    use_case = TrainModel(
        repository=repository,
        dataset_builder=dataset_builder,
        trainer=trainer,
    )
    use_case.execute(lookback_hours=cfg.features.lookback_hours)


@app.command()
def serve(config: Path = typer.Option(..., exists=True)) -> None:
    cfg, repository, registry, trainer = _bootstrap(config)
    use_case = ServePredictions(
        repository=repository,
        trainer=trainer,
        threshold=cfg.serving.threshold,
    )
    use_case.execute()


if __name__ == "__main__":
    app()
