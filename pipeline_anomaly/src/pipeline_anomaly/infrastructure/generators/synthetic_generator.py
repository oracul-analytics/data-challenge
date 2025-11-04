from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterator

import numpy as np
import pandas as pd

from pipeline_anomaly.domain.models.batch import RecordBatch


@dataclass(slots=True)
class SyntheticDatasetConfig:
    row_count: int
    batch_size: int
    anomaly_ratio: float
    seed: int


class SyntheticDatasetGenerator:
    def __init__(self, config: SyntheticDatasetConfig) -> None:
        self._config = config
        self._random = np.random.default_rng(config.seed)

    def batches(self) -> list[RecordBatch]:
        return list(self._iter_batches())

    def _iter_batches(self) -> Iterator[RecordBatch]:
        remaining = self._config.row_count
        current_time = datetime.utcnow()
        while remaining > 0:
            batch_size = min(self._config.batch_size, remaining)
            timestamps = [
                current_time - timedelta(seconds=i) for i in range(batch_size)
            ]
            entity_ids = self._random.integers(1, 1_000_000, size=batch_size)
            base_values = self._random.normal(loc=100.0, scale=15.0, size=batch_size)
            attribute = self._random.uniform(0, 1, size=batch_size)

            anomalies_count = max(1, int(batch_size * self._config.anomaly_ratio))
            anomaly_indices = self._random.choice(
                batch_size, size=anomalies_count, replace=False
            )
            base_values[anomaly_indices] *= self._random.uniform(
                2, 5, size=anomalies_count
            )

            dataframe = pd.DataFrame(
                {
                    "event_time": timestamps,
                    "entity_id": entity_ids,
                    "value": base_values,
                    "attribute": attribute,
                }
            )
            yield RecordBatch(dataframe=dataframe)
            remaining -= batch_size
            current_time -= timedelta(seconds=batch_size)
