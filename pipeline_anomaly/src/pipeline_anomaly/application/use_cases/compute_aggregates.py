from __future__ import annotations
from datetime import datetime

from pipeline_anomaly.domain.models.aggregate import Aggregate, AggregateCollection
from pipeline_anomaly.domain.services.interfaces import ClickHouseWriter


class ComputeAggregates:
    def __init__(self, writer: ClickHouseWriter) -> None:
        self._writer = writer

    def execute(self) -> AggregateCollection:
        """
        Вычисляет агрегаты за последние 24 часа через ClickHouse
        без загрузки всех событий в память.
        """
        # Читаем агрегаты напрямую из ClickHouse
        df = self._writer.read_latest_window()

        if df.empty:
            raise RuntimeError("No events in the last 24 hours to compute aggregates")

        row = df.iloc[0]

        window_start = (
            row["window_start"].to_pydatetime()
            if hasattr(row["window_start"], "to_pydatetime")
            else row["window_start"]
        )
        window_end = (
            row["window_end"].to_pydatetime()
            if hasattr(row["window_end"], "to_pydatetime")
            else row["window_end"]
        )

        aggregates = AggregateCollection(
            aggregates=(
                Aggregate("count", float(row["count"]), window_start, window_end),
                Aggregate(
                    "mean_value", float(row["mean_value"]), window_start, window_end
                ),
                Aggregate(
                    "std_value", float(row["std_value"]), window_start, window_end
                ),
            )
        )

        # Сохраняем агрегаты обратно в ClickHouse
        self._writer.persist_aggregates(aggregates)

        return aggregates
