from __future__ import annotations

import pandas as pd
from clickhouse_connect import get_client
from loguru import logger


class ClickHouseRepository:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "dq",
    ) -> None:
        self.client = get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )
        self.database = database

    def ensure_schema(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.dq_reports (
                table_name String,
                rule String,
                passed UInt8,
                details String,
                generated_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY generated_at
        """)
        logger.info("Schema ensured")

    def save_from_message(self, message: dict) -> None:
        try:
            row = {
                "table_name": str(message["table_name"]),
                "rule": str(message["rule"]),
                "passed": int(message["passed"]),
                "details": str(message["details"]) if isinstance(message["details"], str) else str(message["details"]),
                "generated_at": pd.Timestamp(message["generated_at"]),
            }
            
            df = pd.DataFrame([row])
            self.client.insert(f"{self.database}.dq_reports", df)
            logger.info("Saved: table={}, rule={}", row["table_name"], row["rule"])
            
        except Exception as e:
            logger.error("Failed to save: {}", e)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        try:
            return self.client.query_df(f"SELECT * FROM {self.database}.{table_name}")
        except Exception as e:
            logger.error("Failed to fetch table {}: {}", table_name, e)
            return pd.DataFrame()

    def list_reports(self, limit: int = 100) -> pd.DataFrame:
        try:
            return self.client.query_df(f"SELECT * FROM {self.database}.dq_reports ORDER BY generated_at DESC LIMIT {limit}")
        except Exception as e:
            logger.error("Failed to list reports: {}", e)
            return pd.DataFrame()
