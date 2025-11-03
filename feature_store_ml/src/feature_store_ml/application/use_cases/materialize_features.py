from __future__ import annotations

import pickle
from pathlib import Path

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class MaterializeFeatures:
    
    def __init__(
        self,
        repository: ClickHouseRepository,
        registry: FeatureRegistry,
        model_path: Path = Path("config/artifacts/lightgbm_model.pkl"),
        metadata_path: Path = Path("config/artifacts/metadata.pkl"),
    ) -> None:
        self._repository = repository
        self._registry = registry
        self._model_path = model_path
        self._metadata_path = metadata_path
        self._model = None
        self._metadata = None
        
    def _load_model(self) -> None:
        if not self._model_path.exists():
            raise FileNotFoundError(f"Model not found at {self._model_path}")
        if not self._metadata_path.exists():
            raise FileNotFoundError(f"Metadata not found at {self._metadata_path}")
        
        with open(self._model_path, "rb") as f:
            self._model = pickle.load(f)
        
        with open(self._metadata_path, "rb") as f:
            self._metadata = pickle.load(f)
        
        logger.info(f"Loaded model from {self._model_path}")
        logger.info(f"Metadata keys: {list(self._metadata.keys())}")
        logger.info(f"Model metadata: {self._metadata}")
    
    def _get_feature_names(self) -> list[str]:
        if self._metadata is None:
            self._load_model()
        
        possible_keys = ['feature_names', 'features', 'feature_cols', 'columns']
        
        for key in possible_keys:
            if key in self._metadata and self._metadata[key]:
                logger.info(f"Using feature names from metadata['{key}']: {self._metadata[key]}")
                return self._metadata[key]
        
        feature_names = [name for name in self._registry.feature_names 
                        if name not in ['entity_id', 'event_time']]
        
        logger.warning(
            f"Feature names not found in metadata. "
            f"Using all computed features from registry: {feature_names}"
        )
        
        return feature_names
    
    def _predict_artifacts(self, features: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            self._load_model()
        
        feature_cols = self._get_feature_names()
        
        if not feature_cols:
            raise ValueError(
                "No feature names found in metadata and registry. "
                "Cannot perform predictions."
            )
        
        logger.info(f"Using {len(feature_cols)} features for prediction: {feature_cols}")
        logger.info(f"Model type: {type(self._model).__name__}")
        
        missing_cols = set(feature_cols) - set(features.columns)
        if missing_cols:
            raise ValueError(
                f"Missing required features for prediction: {missing_cols}. "
                f"Available features: {list(features.columns)}"
            )
        
        X = features[feature_cols].copy()
        
        X = X.fillna(0)
        
        logger.info(f"Predicting on {len(X)} records with shape {X.shape}")
        
        import lightgbm as lgb
        
        if isinstance(self._model, lgb.Booster):
            logger.info("Using Booster.predict() for raw predictions")
            raw_predictions = self._model.predict(X)
            
            threshold = 0.5
            predictions = (raw_predictions > threshold).astype(int)
            prediction_scores = raw_predictions
            
        elif hasattr(self._model, 'predict_proba'):
            logger.info("Using predict_proba() for probability predictions")
            predictions = self._model.predict(X)
            prediction_scores = self._model.predict_proba(X)[:, 1]
            
        else:
            raise ValueError(f"Unsupported model type: {type(self._model)}")
        
        logger.info(f"Prediction stats - Class 0: {(predictions == 0).sum()}, "
                    f"Class 1: {(predictions == 1).sum()}")
        logger.info(f"Score range: [{prediction_scores.min():.4f}, {prediction_scores.max():.4f}]")
        
        features_with_predictions = features.copy()
        features_with_predictions['prediction_label'] = predictions
        features_with_predictions['prediction_score'] = prediction_scores
        
        return features_with_predictions

    def execute(self, lookback_hours: int = 168, output_table: str = "results") -> None:
        logger.info(f"Starting feature materialization with lookback={lookback_hours}h")
        
        logger.info("Loading raw events from feature_store.inputs...")
        raw_data = self._repository.fetch_raw_events(
            lookback_hours=lookback_hours,
            table="inputs"
        )
        logger.info(f"Loaded {len(raw_data)} raw events")
        
        if raw_data.empty:
            logger.warning("No raw data to process")
            return
        
        logger.info(f"Computing {len(self._registry.feature_names)} features...")
        features = self._registry.compute(raw_data)
        logger.info(f"Computed features for {len(features)} entities")
        logger.info(f"Feature columns: {list(features.columns)}")
        
        if features.empty:
            logger.warning("No features computed")
            return
        
        logger.info("Running artifact detection with trained model...")
        features_with_predictions = self._predict_artifacts(features)
        
        clean_features = features_with_predictions[
            features_with_predictions['prediction_label'] == 0
        ].copy()
        
        artifact_count = len(features_with_predictions) - len(clean_features)
        logger.info(f"Detected {artifact_count} artifacts, keeping {len(clean_features)} clean records")
        
        if clean_features.empty:
            logger.warning("All records were classified as artifacts - nothing to write")
            return
        
        columns_to_write = [col for col in clean_features.columns 
                           if col not in ['prediction_label', 'prediction_score']]
        clean_features_final = clean_features[columns_to_write]
        
        logger.info(f"Writing clean features to feature_store.{output_table}...")
        self._repository.write_features(clean_features_final, table_name=output_table)
        logger.success(
            f"Materialized {len(clean_features_final)} clean feature rows "
            f"(filtered out {artifact_count} artifacts)"
        )
