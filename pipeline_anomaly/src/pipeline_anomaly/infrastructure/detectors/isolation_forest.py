from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest
from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class IsolationForestDetector(PandasDetector):
    def __init__(self, contamination: float, random_state: int | None = None) -> None:
        super().__init__(name="isolation_forest")
        self._model = IsolationForest(
            contamination=contamination, random_state=random_state
        )

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        """
        Используем агрегаты mean_value и std_value как признаки для детекции аномалий.
        """
        # Проверяем наличие необходимых колонок
        missing = [
            col for col in ["mean_value", "std_value"] if col not in dataframe.columns
        ]
        if missing:
            raise KeyError(
                f"Columns not found in dataframe: {missing}. Available columns: {list(dataframe.columns)}"
            )

        features = dataframe[["mean_value", "std_value"]].copy()
        predictions = self._model.fit_predict(features)
        return pd.Series((predictions == -1).astype(int), index=dataframe.index)
