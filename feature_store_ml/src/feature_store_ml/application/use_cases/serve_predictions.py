from __future__ import annotations

from datetime import datetime

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class ServePredictions:
    
    def __init__(
        self,
        repository: ClickHouseRepository,
        trainer: LightGBMTrainer,
        threshold: float = 0.5,
    ) -> None:
        self._repository = repository
        self._trainer = trainer
        self._threshold = threshold
    
    def execute(self) -> pd.DataFrame:
        logger.info("Starting prediction serving...")
        
        logger.info("Loading trained model...")
        try:
            self._trainer.load()
            logger.success("Model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
        
        logger.info("Fetching latest features from feature store...")
        features = self._repository.fetch_latest_features()
        
        if features.empty:
            logger.warning("No features available for prediction")
            return pd.DataFrame()
        
        logger.info(f"Fetched features for {len(features)} entities")
        
        feature_columns = [col for col in features.columns 
                          if col not in ["entity_id", "feature_timestamp", "event_time"]]
        
        if not feature_columns:
            logger.error("No feature columns found in data")
            raise ValueError("No valid feature columns for prediction")
        
        X = features[feature_columns]
        
        logger.info(f"Making predictions on {len(X)} samples...")
        predictions = self._trainer.predict_proba(X)
        
        results = pd.DataFrame({
            "entity_id": features["entity_id"],
            "prediction_timestamp": datetime.now(),
            "anomaly_score": predictions,
            "is_anomaly": predictions >= self._threshold,
        })
        
        n_anomalies = results["is_anomaly"].sum()
        anomaly_rate = n_anomalies / len(results) * 100
        logger.info(f"Predicted {n_anomalies} anomalies out of {len(results)} ({anomaly_rate:.2f}%)")
        logger.info(f"Average anomaly score: {predictions.mean():.4f}")
        logger.info(f"Max anomaly score: {predictions.max():.4f}")
        
        logger.info("Writing predictions to ClickHouse...")
        self._repository.write_predictions(results)
        logger.success(f"Served {len(results)} predictions")
        
        return results
    
    def predict_single(self, entity_id: str) -> dict[str, float | bool]:
        logger.info(f"Making prediction for entity: {entity_id}")
        
        if not hasattr(self._trainer, "_model") or self._trainer._model is None:
            self._trainer.load()
        
        features = self._repository.fetch_features(entity_ids=[entity_id])
        
        if features.empty:
            logger.warning(f"No features found for entity: {entity_id}")
            return {"anomaly_score": 0.0, "is_anomaly": False}
        
        latest = features.sort_values("feature_timestamp", ascending=False).iloc[0]
        
        feature_columns = [col for col in features.columns 
                          if col not in ["entity_id", "feature_timestamp", "event_time"]]
        X = latest[feature_columns].to_frame().T
        
        score = self._trainer.predict_proba(X)[0]
        
        result = {
            "entity_id": entity_id,
            "anomaly_score": float(score),
            "is_anomaly": bool(score >= self._threshold),
            "prediction_timestamp": datetime.now().isoformat(),
        }
        
        logger.info(f"Prediction for {entity_id}: score={score:.4f}, anomaly={result['is_anomaly']}")
        
        return result
    
    def get_top_anomalies(self, top_k: int = 10) -> pd.DataFrame:
        logger.info(f"Fetching top {top_k} anomalies...")
        
        results = self.execute()
        
        if results.empty:
            return pd.DataFrame()
        
        top_anomalies = (
            results
            .sort_values("anomaly_score", ascending=False)
            .head(top_k)
        )
        
        logger.info(f"Top anomaly score: {top_anomalies['anomaly_score'].iloc[0]:.4f}")
        
        return top_anomalies
