from __future__ import annotations
from datetime import datetime
import pandas as pd

from pipeline_anomaly.domain.models.aggregate import AggregateCollection
from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.infrastructure.clients.clickhouse import ClickHouseFactory


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self._factory = factory

    def ensure_schema(self) -> None:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS events (
                event_time DateTime,
                entity_id UInt64,
                value Float64,
                attribute Float64
            ) ENGINE = MergeTree ORDER BY (event_time, entity_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS aggregates (
                metric String,
                value Float64,
                window_start DateTime,
                window_end DateTime,
                extra Map(String, Float64)
            ) ENGINE = MergeTree ORDER BY (window_start, metric)
            """,
            """
            CREATE TABLE IF NOT EXISTS anomaly_reports (
                generated_at DateTime,
                window_start DateTime,
                window_end DateTime,
                detector String,
                score Float64,
                severity Float64,
                description String
            ) ENGINE = MergeTree ORDER BY (generated_at, detector)
            """,
        ]
        with self._factory.connect() as client:
            for ddl in ddl_statements:
                client.command(ddl)

    def ingest_batch(self, batch: RecordBatch) -> None:
        df = batch.dataframe.copy()
        df["event_time"] = pd.to_datetime(df["event_time"]).apply(
            lambda x: x.to_pydatetime()
        )
        df["entity_id"] = df["entity_id"].astype(int)
        df["value"] = df["value"].astype(float)
        df["attribute"] = df["attribute"].astype(float)
        data = df.values.tolist()

        with self._factory.connect() as client:
            client.insert(
                table="events",
                data=data,
                column_names=["event_time", "entity_id", "value", "attribute"],
            )

    def persist_aggregates(self, aggregates: AggregateCollection) -> None:
        rows = []
        for agg in aggregates.aggregates:
            window_start = self._to_datetime(agg.window_start)
            window_end = self._to_datetime(agg.window_end)
            extra_map = {k: float(v) for k, v in (agg.extra or {}).items()}
            rows.append(
                [str(agg.metric), float(agg.value), window_start, window_end, extra_map]
            )

        with self._factory.connect() as client:
            client.insert(
                table="aggregates",
                column_names=["metric", "value", "window_start", "window_end", "extra"],
                data=rows,
            )

    @staticmethod
    def _to_datetime(val):
        if isinstance(val, datetime):
            return val
        if hasattr(val, "to_pydatetime"):
            return val.to_pydatetime()
        return pd.to_datetime(val).to_pydatetime()

    def persist_report(self, report: AnomalyReport) -> None:
        rows = [
            {
                "generated_at": self._to_datetime(report.generated_at),
                "window_start": self._to_datetime(anomaly.window_start),
                "window_end": self._to_datetime(anomaly.window_end),
                "detector": str(anomaly.detector),
                "score": float(anomaly.score),
                "severity": float(anomaly.severity),
                "description": str(anomaly.description),
            }
            for anomaly in report.anomalies
        ]

        with self._factory.connect() as client:
            # Для словарей column_names указывать не нужно
            client.insert(
                table="anomaly_reports",
                data=rows,
            )

    def read_latest_window(self) -> pd.DataFrame:
        query = """
        SELECT
            count(*) AS count,
            avg(value) AS mean_value,
            stddevPop(value) AS std_value,
            min(event_time) AS window_start,
            max(event_time) AS window_end
        FROM events
        WHERE event_time >= now() - INTERVAL 1 DAY
        """
        with self._factory.connect() as client:
            result = client.query_df(query)

        if result.empty:
            raise RuntimeError("No events in the last 24 hours")

        return result
