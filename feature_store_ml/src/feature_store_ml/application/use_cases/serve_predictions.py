from __future__ import annotations

from datetime import datetime

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)


class ServePredictions:
    def __init__(
        self,
        repository: ClickHouseRepository,
        trainer: LightGBMTrainer,
        threshold: float = 0.5,
        features_table: str = "materialized_features",
    ) -> None:
        self._repository = repository
        self._trainer = trainer
        self._threshold = threshold
        self._features_table = features_table

    def _align_features(self, features: pd.DataFrame) -> pd.DataFrame:
        model, meta = self._trainer.load()
        expected_features = meta["features"]

        metadata_columns = ["entity_id", "feature_timestamp", "event_time"]
        features = features.drop(
            columns=[col for col in metadata_columns if col in features.columns]
        )

        for col in expected_features:
            if col not in features.columns:
                features[col] = 0

        return features[expected_features]

    def execute(self) -> pd.DataFrame:
        logger.info("Starting prediction serving...")

        self._load_model()
        features = self._fetch_features()

        if features.empty:
            logger.warning("No features available for prediction")
            return pd.DataFrame()

        logger.info(f"Fetched features for {len(features)} entities")

        X = self._align_features(features.copy())
        predictions = self._make_predictions(X)
        results = self._create_results_dataframe(features, predictions)

        self._log_prediction_stats(results, predictions)
        self._write_predictions(results)

        return results

    def _load_model(self) -> None:
        logger.info("Loading trained model...")
        try:
            self._trainer.load()
            logger.success("Model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

    def _fetch_features(self) -> pd.DataFrame:
        logger.info(
            f"Fetching latest features from feature_store.{self._features_table}..."
        )
        return self._repository.fetch_latest_features(table=self._features_table)

    def _make_predictions(self, X: pd.DataFrame) -> pd.Series:
        logger.info(f"Making predictions on {len(X)} samples...")
        return self._trainer.predict_proba(X)

    def _create_results_dataframe(
        self, features: pd.DataFrame, predictions: pd.Series
    ) -> pd.DataFrame:
        now = datetime.now()
        prediction_labels = (predictions >= self._threshold).astype(int)

        return pd.DataFrame(
            {
                "entity_id": features["entity_id"],
                "event_time": now,
                "prediction_timestamp": now,
                "prediction_score": predictions,
                "prediction_label": prediction_labels,
                "is_anomaly": prediction_labels,
            }
        )

    def _log_prediction_stats(
        self, results: pd.DataFrame, predictions: pd.Series
    ) -> None:
        n_anomalies = results["is_anomaly"].sum()
        anomaly_rate = n_anomalies / len(results) * 100

        logger.info(
            f"Predicted {n_anomalies} anomalies out of {len(results)} ({anomaly_rate:.2f}%)"
        )
        logger.info(f"Average anomaly score: {predictions.mean():.4f}")
        logger.info(f"Max anomaly score: {predictions.max():.4f}")

    def _write_predictions(self, results: pd.DataFrame) -> None:
        logger.info("Writing predictions to ClickHouse...")
        self._repository.write_predictions(results)
        logger.success(f"Served {len(results)} predictions")

    def predict_single(self, entity_id: str) -> dict[str, float | bool]:
        logger.info(f"Making prediction for entity: {entity_id}")

        self._ensure_model_loaded()
        features = self._fetch_entity_features(entity_id)

        if features.empty:
            logger.warning(f"No features found for entity: {entity_id}")
            return {"anomaly_score": 0.0, "is_anomaly": False}

        latest_features = self._get_latest_features(features)
        score = self._predict_score(latest_features)
        result = self._format_prediction_result(entity_id, score)

        logger.info(
            f"Prediction for {entity_id}: score={score:.4f}, anomaly={result['is_anomaly']}"
        )

        return result

    def _ensure_model_loaded(self) -> None:
        if not hasattr(self._trainer, "_model") or self._trainer._model is None:
            self._trainer.load()

    def _fetch_entity_features(self, entity_id: str) -> pd.DataFrame:
        return self._repository.fetch_latest_features(
            table=self._features_table, entity_ids=[entity_id]
        )

    def _get_latest_features(self, features: pd.DataFrame) -> pd.DataFrame:
        return features.sort_values("feature_timestamp", ascending=False).iloc[0]

    def _predict_score(self, latest_features: pd.Series) -> float:
        X = self._align_features(latest_features.to_frame().T)
        return self._trainer.predict_proba(X)[0]

    def _format_prediction_result(self, entity_id: str, score: float) -> dict:
        return {
            "entity_id": entity_id,
            "anomaly_score": float(score),
            "is_anomaly": bool(score >= self._threshold),
            "prediction_timestamp": datetime.now().isoformat(),
        }

    def get_top_anomalies(self, top_k: int = 10) -> pd.DataFrame:
        logger.info(f"Fetching top {top_k} anomalies...")

        results = self.execute()

        if results.empty:
            return pd.DataFrame()

        top_anomalies = results.sort_values("prediction_score", ascending=False).head(
            top_k
        )

        logger.info(
            f"Top anomaly score: {top_anomalies['prediction_score'].iloc[0]:.4f}"
        )

        return top_anomalies
