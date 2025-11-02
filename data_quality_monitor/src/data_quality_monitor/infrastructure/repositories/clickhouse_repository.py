from __future__ import annotations

import pandas as pd
from clickhouse_connect import get_client
from loguru import logger

from data_quality_monitor.domain.models.result import QualityReport


class ClickHouseRepository:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8123,  # HTTP порт по умолчанию
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
        logger.info("Ensured database {}", self.database)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        try:
            df = self.client.query_df(f"SELECT * FROM {self.database}.{table_name}")
            return df
        except Exception as e:
            logger.error("Failed to fetch table {}: {}", table_name, e)
            return pd.DataFrame()

    def save_report(self, report: QualityReport) -> None:
        for result in report.results:
            logger.info(
                "Table: {}, Rule: {}, Passed: {}, Details: {}",
                getattr(report, "table", "unknown"),
                result.rule,
                result.passed,
                result.details,
            )

    def list_reports(self) -> pd.DataFrame:
        try:
            df = self.client.query_df(f"SELECT * FROM {self.database}.dq_reports")
            return df
        except Exception as e:
            logger.error("Failed to list reports: {}", e)
            return pd.DataFrame()
