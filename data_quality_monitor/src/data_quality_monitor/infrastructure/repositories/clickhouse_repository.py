from __future__ import annotations

import pandas as pd
from loguru import logger

from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory
from data_quality_monitor.domain.models.result import QualityReport

class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self._factory = factory
        self.client = factory.create()
        self.database = factory._config.database

    def ensure_schema(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        
        # Таблица events - для бизнес-данных (которые мы проверяем)
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.events (
                event_id UInt64,
                value Float64,
                ts DateTime
            ) ENGINE = MergeTree()
            ORDER BY ts
        """)
        
        # Таблица reports - для ВСЕХ результатов проверок (из /run и из Kafka)
        self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.reports (
                table_name String,
                rule String,
                passed UInt8,
                details String,
                generated_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY generated_at
        """)
        
        logger.info("Schema ensured (events and reports tables created)")

    def save_report(self, report: QualityReport) -> None:
        """Save a single quality report to the reports table."""
        try:
            rows = []
            for result in report.results:
                row = {
                    "table_name": report.table,
                    "rule": result.rule,
                    "passed": int(result.passed),
                    "details": str(result.details),
                    "generated_at": report.generated_at,
                }
                rows.append(row)
            
            if rows:
                df = pd.DataFrame(rows)
                self.client.insert(f"{self.database}.reports", df)
                logger.info("Saved report for table={} with {} results", report.table, len(rows))
                
        except Exception as e:
            logger.error("Failed to save report for table={}: {}", report.table, e)

    def save_reports(self, reports: list[QualityReport]) -> None:
        """Save multiple quality reports to the reports table in a batch."""
        try:
            all_rows = []
            for report in reports:
                for result in report.results:
                    row = {
                        "table_name": report.table,
                        "rule": result.rule,
                        "passed": int(result.passed),
                        "details": str(result.details),
                        "generated_at": report.generated_at,
                    }
                    all_rows.append(row)
            
            if all_rows:
                df = pd.DataFrame(all_rows)
                self.client.insert(f"{self.database}.reports", df)
                logger.info("Saved {} reports with {} total results", len(reports), len(all_rows))
            else:
                logger.warning("No results to save")
                
        except Exception as e:
            logger.error("Failed to save reports: {}", e)

    def save_from_message(self, message: dict) -> None:
        """Save quality check result from Kafka/Redpanda message to reports table."""
        try:
            row = {
                "table_name": str(message["table_name"]),
                "rule": str(message["rule"]),
                "passed": int(message["passed"]),
                "details": str(message["details"]) if isinstance(message["details"], str) else str(message["details"]),
                "generated_at": pd.Timestamp(message["generated_at"]),
            }
            
            df = pd.DataFrame([row])
            self.client.insert(f"{self.database}.reports", df)
            logger.info("Saved from Kafka: table={}, rule={}", row["table_name"], row["rule"])
            
        except Exception as e:
            logger.error("Failed to save from Kafka: {}", e)

    def insert_events(self, events: pd.DataFrame) -> None:
        """Insert business events into events table."""
        try:
            self.client.insert(f"{self.database}.events", events)
            logger.info("Inserted {} events", len(events))
        except Exception as e:
            logger.error("Failed to insert events: {}", e)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        try:
            if '.' in table_name:
                full_table_name = table_name
            else:
                full_table_name = f"{self.database}.{table_name}"
            
            return self.client.query_df(f"SELECT * FROM {full_table_name}")
        except Exception as e:
            logger.error("Failed to fetch table {}: {}", table_name, e)
            return pd.DataFrame()

    def list_reports(self) -> pd.DataFrame:
        """List all quality check reports (from both /run endpoint and Kafka)."""
        try:
            query = f"SELECT * FROM {self.database}.reports ORDER BY generated_at DESC"
            return self.client.query_df(query)
        except Exception as e:
            logger.error(f"Failed to list reports: {e}")
            return pd.DataFrame()
