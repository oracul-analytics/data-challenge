from __future__ import annotations

import pandas as pd

from feature_store_ml.domain.models.dataset import FeatureDataset
from feature_store_ml.infrastructure.registry import FeatureRegistry


class DatasetBuilder:
    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def build(self, frame: pd.DataFrame) -> FeatureDataset:
        features = self._registry.compute(frame)
        target = (frame["label"] > 0).astype(int)
        return FeatureDataset(features=features[self._registry.feature_names], target=target)
