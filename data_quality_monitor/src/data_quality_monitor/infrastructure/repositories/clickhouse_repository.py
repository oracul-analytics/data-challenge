from __future__ import annotations

import pandas as pd
from loguru import logger

from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.domain.models.result import QualityReport
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.domain.models.rule import Expectation
from typing import Dict


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory, rule_config: RuleConfig) -> None:
        self.client = factory.create()
        self.database = factory._config.database
        self.rule_config = rule_config

    def ensure_schema_input(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        for table_rule in self.rule_config.rules:
            table_name = table_rule.table.split(".")[-1]
            schema_columns = self._extract_schema_columns(table_rule.expectations)
            if schema_columns:
                columns_ddl = ",\n".join(
                    f"{name} {dtype}" for name, dtype in schema_columns.items()
                )
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                    {columns_ddl}
                ) ENGINE = MergeTree()
                ORDER BY ts
                """
                self.client.command(ddl)
                logger.info(f"Table schema ensured: {table_name}")

    @staticmethod
    def _extract_schema_columns(
        expectations: tuple[Expectation, ...],
    ) -> Dict[str, str]:
        for exp in expectations:
            if exp.type == "schema":
                return exp.params.get("columns", {})
        return {}

    def ensure_schema_output(self) -> None:
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
        logger.info("Output schema ensured (reports table created)")

    def ensure_schema(self) -> None:
        self.ensure_schema_input()
        self.ensure_schema_output()
        logger.info("Schema ensured (events and reports tables created)")

    def _insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning("No rows to insert into {}", table)
            return
        self.client.insert(f"{self.database}.{table}", df)
        logger.info("Inserted {} rows into {}", len(df), table)

    def _prepare_report_rows(self, reports: list[QualityReport]) -> pd.DataFrame:
        rows = [
            {
                "table_name": r.table,
                "rule": res.rule,
                "passed": int(res.passed),
                "details": str(res.details),
                "generated_at": r.generated_at,
            }
            for r in reports
            for res in r.results
        ]
        return pd.DataFrame(rows)

    def save_report(self, report: QualityReport) -> None:
        self.save_reports([report])

    def save_reports(self, reports: list[QualityReport]) -> None:
        try:
            df = self._prepare_report_rows(reports)
            self._insert_dataframe("reports", df)
        except Exception as e:
            logger.error("Failed to save reports: {}", e)
            raise

    def save_from_message(self, message: dict) -> None:
        try:
            df = pd.DataFrame(
                [
                    {
                        "table_name": str(message["table_name"]),
                        "rule": str(message["rule"]),
                        "passed": int(message["passed"]),
                        "details": str(message.get("details", "")),
                        "generated_at": pd.Timestamp(message["generated_at"]),
                    }
                ]
            )
            self._insert_dataframe("reports", df)
            logger.debug(
                "Saved from Kafka: table={}, rule={}",
                message["table_name"],
                message["rule"],
            )
        except Exception as e:
            logger.error("Failed to save from Kafka: {}", e)
            raise

    def insert_events(self, events: pd.DataFrame) -> None:
        try:
            df = events.copy()
            if "value" in df.columns:
                df["value"] = df["value"].astype("Float64")
            self._insert_dataframe("events", df)
            null_count = df["value"].isna().sum() if "value" in df.columns else 0
            logger.info(
                "✓ Inserted {} events ({} with NULL values)", len(df), null_count
            )
        except Exception as e:
            logger.error("❌ Failed to insert events: {}", e)
            raise

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        full_table_name = (
            table_name if "." in table_name else f"{self.database}.{table_name}"
        )
        try:
            df = self.client.query_df(f"SELECT * FROM {full_table_name}")
            logger.debug("Fetched {} rows from {}", len(df), full_table_name)
            return df
        except Exception as e:
            logger.error("Failed to fetch table {}: {}", table_name, e)
            return pd.DataFrame()

    def list_reports(self) -> pd.DataFrame:
        try:
            return self.client.query_df(
                f"SELECT * FROM {self.database}.reports ORDER BY generated_at DESC"
            )
        except Exception as e:
            logger.error("Failed to list reports: {}", e)
            return pd.DataFrame()

    def get_table_schema(self, table: str) -> dict[str, str]:
        table_name = table.split(".")[-1]
        database = table.split(".")[0] if "." in table else self.database

        query = f"""
            SELECT name, type
            FROM system.columns
            WHERE database = '{database}' AND table = '{table_name}'
            ORDER BY name
        """
        result = self.client.query(query)
        schema = {row[0]: row[1] for row in result.result_rows}
        logger.debug("Fetched schema for {}: {}", table, schema)
        return schema
