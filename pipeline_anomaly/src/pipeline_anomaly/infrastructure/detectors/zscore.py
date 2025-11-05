from __future__ import annotations

import pandas as pd
from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class ZScoreDetector(PandasDetector):
    def __init__(self, threshold: float) -> None:
        super().__init__(name="zscore")
        self._threshold = threshold

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        """
        Вычисляет Z-оценки на основе агрегированных данных:
        mean_value - среднее значение за окно
        std_value  - стандартное отклонение за окно
        """
        mean = dataframe["mean_value"].mean()
        std = dataframe["std_value"].mean()

        z_scores = (dataframe["mean_value"] - mean) / std

        return (z_scores.abs() > self._threshold).astype(int)
