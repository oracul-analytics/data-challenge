from __future__ import annotations

from pathlib import Path

import joblib
import lightgbm as lgb
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from feature_store_ml.domain.models.dataset import FeatureDataset
from feature_store_ml.domain.models.model_artifact import ModelArtifact
from feature_store_ml.infrastructure.config import TrainingConfig


class LightGBMTrainer:
    def __init__(self, config: TrainingConfig, artifacts_dir: Path) -> None:
        self._config = config
        self._artifacts_dir = artifacts_dir
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)

    def train(self, dataset: FeatureDataset) -> ModelArtifact:
        X_train, X_test, y_train, y_test = train_test_split(
            dataset.features,
            dataset.target,
            test_size=self._config.test_size,
            random_state=self._config.random_state,
            stratify=dataset.target,
        )
        train_set = lgb.Dataset(X_train, label=y_train)
        valid_set = lgb.Dataset(X_test, label=y_test)
        params = {
            "objective": "binary",
            "learning_rate": self._config.learning_rate,
            "max_depth": self._config.max_depth,
            "metric": ["auc"],
        }
        booster = lgb.train(
            params=params,
            train_set=train_set,
            num_boost_round=self._config.num_boost_round,
            valid_sets=[valid_set],
            verbose_eval=False,
        )
        predictions = booster.predict(X_test)
        auc = roc_auc_score(y_test, predictions)
        model_path = self._artifacts_dir / "lightgbm_model.pkl"
        joblib.dump(booster, model_path)
        metadata = {
            "auc": auc,
            "features": list(dataset.features.columns),
        }
        joblib.dump(metadata, self._artifacts_dir / "metadata.pkl")
        return ModelArtifact(model_path=model_path, feature_names=tuple(dataset.features.columns))

    def load(self) -> tuple[lgb.Booster, dict[str, object]]:
        model = joblib.load(self._artifacts_dir / "lightgbm_model.pkl")
        metadata = joblib.load(self._artifacts_dir / "metadata.pkl")
        return model, metadata
