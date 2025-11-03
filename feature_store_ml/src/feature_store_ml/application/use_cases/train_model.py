from __future__ import annotations

from loguru import logger

from feature_store_ml.infrastructure.datasets.builder import DatasetBuilder
from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class TrainModel:
    
    def __init__(
        self,
        repository: ClickHouseRepository,
        dataset_builder: DatasetBuilder,
        trainer: LightGBMTrainer,
    ) -> None:
        self._repository = repository
        self._dataset_builder = dataset_builder
        self._trainer = trainer
    
    def execute(self, lookback_hours: int = 720) -> None:
        logger.info(f"Starting model training with lookback={lookback_hours}h")
        
        logger.info("Loading training data from ClickHouse...")
        raw_data = self._repository.fetch_training_data(lookback_hours=lookback_hours)
        logger.info(f"Loaded {len(raw_data)} rows")
        
        logger.info("Building feature dataset...")
        dataset = self._dataset_builder.build(raw_data)
        logger.info(f"Built dataset: {dataset}")
        
        logger.info("Training LightGBM model...")
        artifact = self._trainer.train(dataset)
        logger.info(f"Training complete: {artifact}")
        logger.info(f"Model saved to: {artifact.model_path}")
        
        _, metadata = self._trainer.load()
        logger.success(f"Model AUC: {metadata['auc']:.4f}")
        logger.info(f"Features used: {len(metadata['features'])}")
