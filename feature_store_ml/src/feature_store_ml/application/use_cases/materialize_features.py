from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)


class MaterializeFeatures:
    def __init__(
        self,
        repository: ClickHouseRepository,
        registry: FeatureRegistry,
        model_path: Path = Path("config/artifacts/ensemble_model.pkl"),
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

        self._model = joblib.load(self._model_path)
        self._metadata = joblib.load(self._metadata_path)

        logger.info(f"Loaded model from {self._model_path}")
        logger.info(f"Metadata keys: {list(self._metadata.keys())}")

    def _get_feature_names(self) -> list[str]:
        if self._metadata is None:
            self._load_model()

        for key in ["feature_names", "features", "feature_cols", "columns"]:
            if key in self._metadata and self._metadata[key]:
                logger.info(f"Using feature names from metadata['{key}']")
                return self._metadata[key]

        feature_names = [
            name
            for name in self._registry.feature_names
            if name not in ["entity_id", "event_time"]
        ]
        logger.warning(f"Using all computed features from registry: {feature_names}")
        return feature_names

    def _predict_artifacts(self, features: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            self._load_model()

        feature_cols = self._get_feature_names()
        X = features[feature_cols].fillna(0)

        if hasattr(self._model, "predict_proba"):
            proba = self._model.predict_proba(X)
            # Проверяем размерность
            if proba.ndim == 1:
                prediction_scores = proba
            else:
                prediction_scores = proba[:, 1]
            predictions = (prediction_scores >= 0.5).astype(int)
        elif isinstance(self._model, lgb.Booster):
            prediction_scores = self._model.predict(X)
            predictions = (prediction_scores >= 0.5).astype(int)
        else:
            raise ValueError(f"Unsupported model type: {type(self._model)}")

        features_with_predictions = features.copy()
        features_with_predictions["prediction_label"] = predictions
        features_with_predictions["prediction_score"] = prediction_scores

        return features_with_predictions

    def execute(
        self,
        lookback_hours: int = 16800000000,
        output_table: str = "materialized_features",
    ) -> None:
        raw_data = self._repository.fetch_raw_events(
            lookback_hours=lookback_hours, table="inputs"
        )
        if raw_data.empty:
            logger.warning("No raw data to process")
            return

        required_features = [
            "value_mean",
            "value_std",
            "value_count",
            "value_p95",
            "attribute_mean",
        ]
        has_precomputed = all(col in raw_data.columns for col in required_features)

        if has_precomputed:
            features = raw_data[
                ["entity_id", "event_time", "value"] + required_features
            ].copy()
        else:
            features = self._registry.compute(raw_data)

        features = features.dropna(subset=required_features)

        features = (
            features.sort_values("event_time", ascending=False)
            .groupby("entity_id", as_index=False)
            .first()
        )

        if features.empty:
            return

        features_with_predictions = self._predict_artifacts(features)

        clean_features = features_with_predictions[
            features_with_predictions["prediction_label"] == 0
        ].copy()

        if clean_features.empty:
            return

        columns_to_write = [
            col
            for col in clean_features.columns
            if col not in ["prediction_label", "prediction_score"]
        ]
        clean_features_final = clean_features[columns_to_write]

        self._repository.write_features(clean_features_final, table_name=output_table)
        logger.success(f"Materialized {len(clean_features_final)} clean feature rows")
