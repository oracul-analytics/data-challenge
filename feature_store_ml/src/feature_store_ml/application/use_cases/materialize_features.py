from __future__ import annotations

from loguru import logger

from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class MaterializeFeatures:
    
    def __init__(
        self,
        repository: ClickHouseRepository,
        registry: FeatureRegistry,
    ) -> None:
        self._repository = repository
        self._registry = registry
    
    def execute(self, lookback_hours: int = 168) -> None:
        logger.info(f"Starting feature materialization with lookback={lookback_hours}h")
        
        logger.info("Loading raw data from ClickHouse...")
        raw_data = self._repository.fetch_raw_events(lookback_hours=lookback_hours)
        logger.info(f"Loaded {len(raw_data)} raw events")
        
        if raw_data.empty:
            logger.warning("No raw data to process")
            return
        
        logger.info(f"Computing {len(self._registry.feature_names)} features...")
        features = self._registry.compute(raw_data)
        logger.info(f"Computed features for {len(features)} entities")
        
        logger.info("Writing features to ClickHouse...")
        self._repository.write_features(features)
        logger.success(f"Materialized {len(features)} feature rows")
