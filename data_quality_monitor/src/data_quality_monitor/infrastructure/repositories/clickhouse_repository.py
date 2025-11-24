from __future__ import annotations
import pandas as pd
from data_quality_monitor.domain.factories.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.domain.repository.clickhouse import ClickHouseRepositoryDomain


class ClickHouseRepository(ClickHouseRepositoryDomain):
    def __init__(self, factory: ClickHouseFactory, rule_config: RuleConfig) -> None:
        client = factory.create()
        database = factory._config.database
        super().__init__(client, database)
        self.rule_config = rule_config

    def ensure_schema_input(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        for table_rule in self.rule_config.rules:
            table_name = table_rule.table.split(".")[-1]
            schema_columns = self._extract_schema_columns(table_rule.expectations)
            if schema_columns:
                columns_ddl = ",\n".join(f"{name} {dtype}" for name, dtype in schema_columns.items())
                ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{table_name} (
                    {columns_ddl}
                ) ENGINE = MergeTree()
                ORDER BY ts
                """
                self.client.command(ddl)

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

    def ensure_schema(self) -> None:
        self.ensure_schema_input()
        self.ensure_schema_output()

    def list_reports(self) -> pd.DataFrame:
        return self.client.query_df(f"SELECT * FROM {self.database}.reports ORDER BY generated_at DESC")
