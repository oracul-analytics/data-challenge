from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.clients.clickhouse import ClickHouseFactory


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self.client = factory.create()
        self.database = factory.database

    def fetch_training_data(
        self,
        lookback_hours: int,
        table: str = "events",
        limit: int | None = None,
    ) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)

        query = f"""
        SELECT 
            timestamp,
            entity_id,
            event_time,
            value,
            attribute,
            event_type,
            session_id,
            label
        FROM {self.database}.{table}
        WHERE timestamp >= '{cutoff_time.strftime("%Y-%m-%d %H:%M:%S")}'
        AND label IS NOT NULL
        ORDER BY entity_id, event_time
        """

        if limit:
            query += f" LIMIT {limit}"

        return self.client.query_df(query)

    def fetch_raw_events(
        self,
        lookback_hours: int,
        table: str = "results",
        limit: int | None = None,
    ) -> pd.DataFrame:
        cutoff_time = datetime.now() - timedelta(hours=lookback_hours)

        query = f"""
        SELECT 
            timestamp,
            entity_id,
            event_time,
            value,
            attribute,
            event_type,
            session_id
        FROM {self.database}.{table}
        WHERE timestamp >= '{cutoff_time.strftime("%Y-%m-%d %H:%M:%S")}'
        ORDER BY entity_id, event_time
        """

        if limit:
            query += f" LIMIT {limit}"

        return self.client.query_df(query)

    def write_features(
        self, features: pd.DataFrame, table_name: str = "features"
    ) -> None:
        if features.empty:
            logger.warning("No features to write")
            return

        features_with_timestamp = features.copy()
        if "feature_timestamp" not in features_with_timestamp.columns:
            features_with_timestamp["feature_timestamp"] = datetime.now()

        self.client.insert_df(
            table=f"{self.database}.{table_name}",
            df=features_with_timestamp,
        )
        logger.info(
            f"Wrote {len(features_with_timestamp)} feature rows to {table_name}"
        )

    def clear_results(self) -> None:
        query = f"TRUNCATE TABLE IF EXISTS {self.database}.results"

        self.client.command(query)
        logger.info("Cleared feature_store.results table")

    def write_predictions(self, predictions: pd.DataFrame) -> None:
        if predictions.empty:
            logger.warning("No predictions to write")
            return

        results = predictions.copy()
        results["materialized_at"] = datetime.now()

        if "is_anomaly" not in results.columns:
            results["is_anomaly"] = (results["prediction_label"] == 1).astype(int)

        key_columns = [
            "entity_id",
            "prediction_score",
            "prediction_label",
            "is_anomaly",
            "materialized_at",
        ]

        feature_cols = [col for col in results.columns if col not in key_columns]

        results = results[key_columns + feature_cols]

        self.client.insert_df(
            table=f"{self.database}.results",
            df=results,
        )
        logger.info(f"Wrote {len(results)} prediction rows to results")

    def read_latest_predictions(
        self, entity_ids: list[str] | None = None, limit: int | None = None
    ) -> pd.DataFrame:
        query = f"""
        SELECT *
        FROM {self.database}.results
        """

        if entity_ids:
            entity_list = "','".join(entity_ids)
            query += f" WHERE entity_id IN ('{entity_list}')"

        query += " ORDER BY materialized_at DESC"

        if limit:
            query += f" LIMIT {limit}"

        return self.client.query_df(query)

    def get_prediction_stats(self) -> pd.DataFrame:
        query = f"""
        SELECT
            prediction_label,
            COUNT(*) as count,
            AVG(prediction_score) as avg_score,
            MIN(prediction_score) as min_score,
            MAX(prediction_score) as max_score,
            STDDEV(prediction_score) as std_score
        FROM {self.database}.results
        GROUP BY prediction_label
        ORDER BY prediction_label
        """

        return self.client.query_df(query)

    def fetch_latest_features(
        self,
        table: str = "results",
        entity_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        if entity_ids:
            entity_list = "','".join(entity_ids)
            where = f"WHERE entity_id IN ('{entity_list}')"
        else:
            where = ""

        query = f"""
        SELECT 
            entity_id,
            event_time,
            value,
            value_mean,
            value_std,
            value_count,
            value_p95,
            attribute_mean,
            feature_timestamp
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY feature_timestamp DESC) as rn
            FROM {self.database}.{table}
            {where}
        ) ranked
        WHERE rn = 1
        """

        df = self.client.query_df(query)

        logger.info(f"Fetched {len(df)} latest feature rows from {table}")
        return df
