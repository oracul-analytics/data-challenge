from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

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
        SELECT *
        FROM {self.database}.{table}
        WHERE timestamp >= '{cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        return self.client.query_df(query)
    
    def fetch_raw_events(
        self,
        lookback_hours: int,
        table: str = "events",
        limit: int | None = None,
    ) -> pd.DataFrame:
        return self.fetch_training_data(
            lookback_hours=lookback_hours,
            table=table,
            limit=limit,
        )
    
    def write_features(self, features: pd.DataFrame) -> None:
        if features.empty:
            return
        
        features_with_timestamp = features.copy()
        if "feature_timestamp" not in features_with_timestamp.columns:
            features_with_timestamp["feature_timestamp"] = datetime.now()
        
        self.client.insert_df(
            table=f"{self.database}.features",
            df=features_with_timestamp,
        )
