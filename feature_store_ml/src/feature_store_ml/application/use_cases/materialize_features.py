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

        if "class_distribution" in self._metadata:
            logger.info(
                f"Training class distribution: {self._metadata['class_distribution']}"
            )

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
            prediction_scores = proba[:, 1] if proba.ndim > 1 else proba
            predictions = (prediction_scores >= 0.5).astype(int)
        else:
            raise ValueError(f"Unsupported model type: {type(self._model)}")

        logger.info(
            f"Prediction scores distribution:\n{pd.Series(prediction_scores).describe()}"
        )
        logger.info(
            f"Prediction labels distribution:\n{pd.Series(predictions).value_counts()}"
        )
        logger.info(
            f"Score stats - Min: {prediction_scores.min():.4f}, "
            f"Max: {prediction_scores.max():.4f}, "
            f"Mean: {prediction_scores.mean():.4f}, "
            f"Median: {pd.Series(prediction_scores).median():.4f}"
        )

        if (prediction_scores >= 0.5).all():
            logger.warning(
                "⚠️ ALL predictions >= 0.5! Model predicting all as artifacts."
            )
        if (prediction_scores < 0.5).all():
            logger.warning("⚠️ ALL predictions < 0.5! Model predicting all as clean.")

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

        features = (
            raw_data[["entity_id", "event_time", "value"] + required_features].copy()
            if has_precomputed
            else self._registry.compute(raw_data)
        )

        features_before_dropna = len(features)
        features = features.dropna(subset=required_features)
        dropped_na = features_before_dropna - len(features)
        if dropped_na > 0:
            logger.warning(
                f"Dropped {dropped_na} rows with NULL values in required features"
            )
        if features.empty:
            logger.warning("No features left after preprocessing")
            return

        logger.info("Feature statistics before prediction:")
        for col in required_features:
            logger.info(
                f"  {col}: min={features[col].min():.4f}, max={features[col].max():.4f}, mean={features[col].mean():.4f}"
            )

        features_with_predictions = self._predict_artifacts(features)

        total_records = len(features_with_predictions)
        artifact_features = features_with_predictions[
            features_with_predictions["prediction_label"] == 1
        ]
        clean_features = features_with_predictions[
            features_with_predictions["prediction_label"] == 0
        ]

        logger.info(f"Total records: {total_records}")
        logger.info(
            f"Artifacts detected (label=1): {len(artifact_features)} ({len(artifact_features) / total_records * 100:.2f}%)"
        )
        logger.info(
            f"Clean records (label=0): {len(clean_features)} ({len(clean_features) / total_records * 100:.2f}%)"
        )

        if clean_features.empty:
            logger.warning(
                "All records classified as artifacts (label=1) - nothing to write."
            )
            return

        columns_to_write = [
            col
            for col in clean_features.columns
            if col not in ["prediction_label", "prediction_score"]
        ]
        clean_features_final = clean_features[columns_to_write]

        self._repository.write_features(clean_features_final, table_name=output_table)
        logger.success(
            f"Materialized {len(clean_features_final)} clean feature rows (out of {total_records}, filtered out {len(artifact_features)} artifacts)"
        )
