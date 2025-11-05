from __future__ import annotations

from loguru import logger

from pipeline_anomaly.domain.services.interfaces import (
    ClickHouseWriter,
    DatasetGenerator,
)


class LoadSyntheticDataset:
    def __init__(self, generator: DatasetGenerator, writer: ClickHouseWriter) -> None:
        self._generator = generator
        self._writer = writer

    def execute(self) -> None:
        logger.info("ensure schema")
        self._writer.ensure_schema()
        for idx, batch in enumerate(self._generator.batches(), start=1):
            logger.info("ingesting batch {}/{} rows", idx, batch.size)

            # ===== Отладочный вывод =====
            logger.debug(
                "Batch dataframe preview:\n{}", batch.dataframe.head(10)
            )  # первые 10 строк
            logger.debug("Batch dataframe dtypes:\n{}", batch.dataframe.dtypes)
            logger.debug("Batch dataframe shape: {}", batch.dataframe.shape)
            # ===========================

            self._writer.ingest_batch(batch)
