from __future__ import annotations

from pathlib import Path
from typing import Tuple

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from feature_store_ml.domain.models.dataset import FeatureDataset
from feature_store_ml.domain.models.model_artifact import ModelArtifact
from feature_store_ml.infrastructure.config import TrainingConfig


class TrainingDataSplitter:
    def __init__(self, test_size: float, random_state: int):
        self._test_size = test_size
        self._random_state = random_state

    def split(
        self, X: pd.DataFrame, y: pd.Series
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        return train_test_split(
            X,
            y,
            test_size=self._test_size,
            random_state=self._random_state,
            stratify=y,
        )


class LightGBMModelBuilder:
    def __init__(self, learning_rate: float, max_depth: int, num_boost_round: int):
        self._learning_rate = learning_rate
        self._max_depth = max_depth
        self._num_boost_round = num_boost_round

    def build_params(self) -> dict:
        return {
            "objective": "binary",
            "learning_rate": self._learning_rate,
            "max_depth": self._max_depth,
            "metric": "auc",
            "verbosity": -1,
        }

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: pd.DataFrame,
        y_valid: pd.Series,
    ) -> lgb.Booster:
        train_set = lgb.Dataset(X_train, label=y_train)
        valid_set = lgb.Dataset(X_valid, label=y_valid, reference=train_set)

        booster = lgb.train(
            params=self.build_params(),
            train_set=train_set,
            num_boost_round=self._num_boost_round,
            valid_sets=[valid_set],
            callbacks=[lgb.log_evaluation(period=0)],
        )

        return booster


class ModelEvaluator:
    @staticmethod
    def calculate_auc(y_true: pd.Series, y_pred: np.ndarray) -> float:
        return roc_auc_score(y_true, y_pred)


class ModelPersistence:
    def __init__(self, artifacts_dir: Path):
        self._artifacts_dir = artifacts_dir
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)

    @property
    def model_path(self) -> Path:
        return self._artifacts_dir / "lightgbm_model.pkl"

    @property
    def metadata_path(self) -> Path:
        return self._artifacts_dir / "metadata.pkl"

    def save_model(self, model: lgb.Booster) -> None:
        joblib.dump(model, self.model_path)

    def save_metadata(self, metadata: dict) -> None:
        joblib.dump(metadata, self.metadata_path)

    def load_model(self) -> lgb.Booster:
        return joblib.load(self.model_path)

    def load_metadata(self) -> dict:
        return joblib.load(self.metadata_path)


class TrainingMetadataBuilder:
    @staticmethod
    def build(
        auc: float,
        feature_names: list[str],
        num_train: int,
        num_test: int,
    ) -> dict:
        return {
            "auc": auc,
            "features": feature_names,
            "num_train": num_train,
            "num_test": num_test,
        }


class ModelPredictor:
    def __init__(self, model: lgb.Booster, feature_names: list[str]):
        self._model = model
        self._feature_names = feature_names

    def _validate_features(self, X: pd.DataFrame) -> None:
        missing_features = set(self._feature_names) - set(X.columns)
        if missing_features:
            raise ValueError(f"Missing required features: {missing_features}")

    def _prepare_features(self, X: pd.DataFrame) -> pd.DataFrame:
        self._validate_features(X)
        return X[self._feature_names]

    def _convert_to_probabilities(self, predictions: np.ndarray) -> np.ndarray:
        if np.all((predictions >= 0) & (predictions <= 1)):
            return predictions
        return 1 / (1 + np.exp(-predictions))

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X_prepared = self._prepare_features(X)
        predictions = self._model.predict(X_prepared)
        return self._convert_to_probabilities(predictions)

    def predict(self, X: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        probabilities = self.predict_proba(X)
        return (probabilities >= threshold).astype(int)


class LightGBMTrainer:
    def __init__(self, config: TrainingConfig, artifacts_dir: Path) -> None:
        self._config = config
        self._persistence = ModelPersistence(artifacts_dir)
        self._splitter = TrainingDataSplitter(config.test_size, config.random_state)
        self._builder = LightGBMModelBuilder(
            config.learning_rate, config.max_depth, config.num_boost_round
        )
        self._evaluator = ModelEvaluator()
        self._predictor = None

    def train(self, dataset: FeatureDataset) -> ModelArtifact:
        X_train, X_test, y_train, y_test = self._splitter.split(dataset.X, dataset.y)

        model = self._builder.train(X_train, y_train, X_test, y_test)

        predictions = model.predict(X_test)
        auc = self._evaluator.calculate_auc(y_test, predictions)

        feature_names = list(dataset.X.columns)
        metadata = TrainingMetadataBuilder.build(
            auc=auc,
            feature_names=feature_names,
            num_train=len(X_train),
            num_test=len(X_test),
        )

        self._persistence.save_model(model)
        self._persistence.save_metadata(metadata)

        self._predictor = ModelPredictor(model, feature_names)

        return ModelArtifact(
            model_path=self._persistence.model_path,
            feature_names=tuple(feature_names),
        )

    def load(self) -> Tuple[lgb.Booster, dict]:
        model = self._persistence.load_model()
        metadata = self._persistence.load_metadata()
        self._predictor = ModelPredictor(model, metadata["features"])
        return model, metadata

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self._predictor is None:
            raise RuntimeError("Model is not loaded. Call load() before predict().")
        return self._predictor.predict_proba(X)

    def predict(self, X: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        if self._predictor is None:
            raise RuntimeError("Model is not loaded. Call load() before predict().")
        return self._predictor.predict(X, threshold)
