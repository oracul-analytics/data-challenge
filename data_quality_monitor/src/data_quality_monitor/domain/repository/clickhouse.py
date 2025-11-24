from loguru import logger
import pandas as pd
from data_quality_monitor.domain.models.rules.result import QualityReport
from data_quality_monitor.domain.models.rules.rule import Expectation
from typing import Dict


class ClickHouseRepositoryDomain:
    def __init__(self, client, database: str) -> None:
        self.client = client
        self.database = database

    def _insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        if not df.empty:
            self.client.insert_df(f"{self.database}.{table}", df)

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

    @staticmethod
    def _extract_schema_columns(expectations: tuple[Expectation, ...]) -> Dict[str, str]:
        for exp in expectations:
            if exp.type == "schema":
                return exp.params.get("columns", {})
        return {}

    def drop_table(self, table_name: str) -> None:
        full_table_name = table_name if "." in table_name else f"{self.database}.{table_name}"
        try:
            self.client.command(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info("✓ Dropped table {}", full_table_name)
        except Exception as e:
            logger.error("❌ Failed to drop table {}: {}", full_table_name, e)
            raise

    def insert_table(self, table_name: str, df: pd.DataFrame) -> None:
        df_copy = df.copy()

        if "id" in df_copy.columns:
            df_copy["id"] = df_copy["id"].astype("uint64")
        if "field_a" in df_copy.columns:
            df_copy["field_a"] = df_copy["field_a"].astype("Float64")
        if "ts" in df_copy.columns:
            df_copy["ts"] = pd.to_datetime(df_copy["ts"])

        full_table_name = table_name if "." in table_name else f"{self.database}.{table_name}"
        self.client.insert_df(full_table_name, df_copy)
        logger.info("✓ Inserted {} rows into table '{}'", len(df_copy), full_table_name)

    def save_report(self, report: QualityReport) -> None:
        self.save_reports([report])

    def save_reports(self, reports: list[QualityReport]) -> None:
        df = self._prepare_report_rows(reports)
        self._insert_dataframe("reports", df)

    def save_from_message(self, message: dict) -> None:
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

    def insert_events(self, events: pd.DataFrame) -> None:
        df = events.copy()
        if "event_id" in df.columns:
            df["event_id"] = df["event_id"].astype("uint64")
        if "value" in df.columns:
            df["value"] = df["value"].astype("Float64")
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"])
        self.client.insert_df(f"{self.database}.events", df)

    def fetch_table(self, table_name: str) -> pd.DataFrame:
        full_table_name = table_name if "." in table_name else f"{self.database}.{table_name}"
        return self.client.query_df(f"SELECT * FROM {full_table_name}")

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
        return {row[0]: row[1] for row in result.result_rows}
