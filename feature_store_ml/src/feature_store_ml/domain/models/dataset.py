from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd


@dataclass(frozen=True)
class FeatureDataset:
    features: pd.DataFrame
    labels: pd.Series | None = None
    feature_names: Sequence[str] | None = None

    def __post_init__(self) -> None:
        if self.labels is not None and len(self.features) != len(self.labels):
            raise ValueError(
                f"Features and labels length mismatch: {len(self.features)} != {len(self.labels)}"
            )

        if self.feature_names is not None:
            missing_features = set(self.feature_names) - set(self.features.columns)
            if missing_features:
                raise ValueError(f"Missing features in dataframe: {missing_features}")

    @property
    def X(self) -> pd.DataFrame:
        if self.feature_names is not None:
            return self.features[list(self.feature_names)]
        return self.features

    @property
    def y(self) -> pd.Series | None:
        return self.labels

    @property
    def shape(self) -> tuple[int, int]:
        return self.X.shape

    def __len__(self) -> int:
        return len(self.features)

    def split(self, train_ratio: float = 0.8) -> tuple[FeatureDataset, FeatureDataset]:
        if not 0 < train_ratio < 1:
            raise ValueError(f"train_ratio must be between 0 and 1, got {train_ratio}")

        n_train = int(len(self) * train_ratio)

        train_features = self.features.iloc[:n_train]
        test_features = self.features.iloc[n_train:]

        train_labels = self.labels.iloc[:n_train] if self.labels is not None else None
        test_labels = self.labels.iloc[n_train:] if self.labels is not None else None

        train_dataset = FeatureDataset(
            features=train_features,
            labels=train_labels,
            feature_names=self.feature_names,
        )

        test_dataset = FeatureDataset(
            features=test_features,
            labels=test_labels,
            feature_names=self.feature_names,
        )

        return train_dataset, test_dataset

    def sample(
        self,
        n: int | None = None,
        frac: float | None = None,
        random_state: int | None = None,
    ) -> FeatureDataset:
        sampled_features = self.features.sample(
            n=n, frac=frac, random_state=random_state
        )
        sampled_labels = (
            self.labels.loc[sampled_features.index] if self.labels is not None else None
        )

        return FeatureDataset(
            features=sampled_features,
            labels=sampled_labels,
            feature_names=self.feature_names,
        )

    def filter_features(self, feature_names: Sequence[str]) -> FeatureDataset:
        return FeatureDataset(
            features=self.features,
            labels=self.labels,
            feature_names=feature_names,
        )

    def add_labels(self, labels: pd.Series) -> FeatureDataset:
        if len(labels) != len(self.features):
            raise ValueError(
                f"Labels length {len(labels)} doesn't match features length {len(self.features)}"
            )

        return FeatureDataset(
            features=self.features,
            labels=labels,
            feature_names=self.feature_names,
        )

    def describe(self) -> dict[str, any]:
        stats = {
            "n_samples": len(self),
            "n_features": self.X.shape[1],
            "feature_names": list(self.X.columns),
            "has_labels": self.labels is not None,
        }

        if self.labels is not None:
            stats["label_distribution"] = self.labels.value_counts().to_dict()
            stats["label_balance"] = {
                "positive": (self.labels == 1).sum(),
                "negative": (self.labels == 0).sum(),
            }

        return stats

    def to_dict(self) -> dict[str, any]:
        result = {
            "features": self.X.to_dict(orient="records"),
        }

        if self.labels is not None:
            result["labels"] = self.labels.tolist()

        return result

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> FeatureDataset:
        features = pd.DataFrame(data["features"])
        labels = pd.Series(data["labels"]) if "labels" in data else None

        return cls(features=features, labels=labels)
