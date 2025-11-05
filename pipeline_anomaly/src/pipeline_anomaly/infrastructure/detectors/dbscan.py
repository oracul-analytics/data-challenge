from __future__ import annotations

import pandas as pd
from sklearn.cluster import DBSCAN
from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class DBSCANDetector(PandasDetector):
    def __init__(self, eps: float, min_samples: int) -> None:
        super().__init__(name="dbscan")
        self._model = DBSCAN(eps=eps, min_samples=min_samples)

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        """
        Используем агрегаты mean_value и std_value как признаки для детекции аномалий.
        """
        required_columns = ["mean_value", "std_value"]
        missing = [col for col in required_columns if col not in dataframe.columns]
        if missing:
            raise KeyError(
                f"Columns not found in dataframe: {missing}. Available columns: {list(dataframe.columns)}"
            )

        features = dataframe[["mean_value", "std_value"]].copy()
        predictions = self._model.fit_predict(features)
        # -1 обозначает аномалию, приводим к 0/1
        return pd.Series((predictions == -1).astype(int), index=dataframe.index)
