from __future__ import annotations

from typing import Sequence

import pandas as pd

from feature_store_ml.domain.models.feature import Feature


class FeatureRegistry:
    def __init__(self, features: Sequence[Feature]) -> None:
        self._features = tuple(features)

    @classmethod
    def default(cls) -> "FeatureRegistry":
        return cls(
            features=(
                Feature("value_mean", lambda df: df.groupby("entity_id")["value"].transform("mean")),
                Feature("value_std", lambda df: df.groupby("entity_id")["value"].transform("std")),
                Feature("value_count", lambda df: df.groupby("entity_id")["value"].transform("count")),
                Feature(
                    "value_p95",
                    lambda df: df.groupby("entity_id")["value"].transform(lambda s: s.quantile(0.95)),
                ),
                Feature(
                    "attribute_mean",
                    lambda df: df.groupby("entity_id")["attribute"].transform("mean"),
                ),
            )
        )

    def compute(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        result = dataframe[["entity_id", "event_time", "value"]].copy()
        for feature in self._features:
            result[feature.name] = feature.compute(dataframe)
        return result

    @property
    def feature_names(self) -> tuple[str, ...]:
        return tuple(feature.name for feature in self._features)
