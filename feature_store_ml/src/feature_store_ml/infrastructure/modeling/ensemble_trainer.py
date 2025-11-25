from __future__ import annotations

from pathlib import Path
from typing import Tuple, List, Dict, Any
import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.linear_model import LogisticRegression
import xgboost as xgb
import catboost as cb
from loguru import logger

from feature_store_ml.domain.models.dataset import FeatureDataset
from feature_store_ml.domain.models.model_artifact import ModelArtifact
from feature_store_ml.infrastructure.config import TrainingConfig


FEATURE_NAMES: List[str] = [
    "value",
    "value_mean",
    "value_std",
    "value_count",
    "value_p95",
    "attribute_mean",
]


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


class XGBoostModelBuilder:
    def __init__(
        self, learning_rate: float, max_depth: int, n_estimators: int, random_state: int
    ):
        self._learning_rate = learning_rate
        self._max_depth = max_depth
        self._n_estimators = n_estimators
        self._random_state = random_state

    def build_params(self) -> dict:
        return {
            "objective": "binary:logistic",
            "learning_rate": self._learning_rate,
            "max_depth": self._max_depth,
            "n_estimators": self._n_estimators,
            "eval_metric": "auc",
            "tree_method": "hist",
            "random_state": self._random_state,
            "verbosity": 0,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_alpha": 0.1,
            "reg_lambda": 1.0,
        }

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: pd.DataFrame,
        y_valid: pd.Series,
    ) -> xgb.XGBClassifier:
        model = xgb.XGBClassifier(**self.build_params())
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_valid, y_valid)],
            verbose=False,
        )
        return model


class CatBoostModelBuilder:
    def __init__(
        self, learning_rate: float, max_depth: int, n_estimators: int, random_state: int
    ):
        self._learning_rate = learning_rate
        self._max_depth = max_depth
        self._n_estimators = n_estimators
        self._random_state = random_state

    def build_params(self) -> dict:
        return {
            "loss_function": "Logloss",
            "eval_metric": "AUC",
            "learning_rate": self._learning_rate,
            "depth": self._max_depth,
            "iterations": self._n_estimators,
            "random_state": self._random_state,
            "verbose": False,
            "l2_leaf_reg": 3.0,
            "bagging_temperature": 1.0,
            "random_strength": 1.0,
        }

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_valid: pd.DataFrame,
        y_valid: pd.Series,
    ) -> cb.CatBoostClassifier:
        model = cb.CatBoostClassifier(**self.build_params())
        model.fit(
            X_train,
            y_train,
            eval_set=(X_valid, y_valid),
            verbose=False,
        )
        return model


class StackingEnsemble:
    def __init__(self, random_state: int):
        self._models: List[Any] = []
        self._meta_model = LogisticRegression(random_state=random_state, max_iter=1000)
        self._random_state = random_state

    def add_model(self, model: Any) -> None:
        self._models.append(model)

    def _get_oof_predictions(
        self, X: pd.DataFrame, y: pd.Series, n_folds: int = 5
    ) -> np.ndarray:
        oof_preds = np.zeros((len(X), len(self._models)))
        skf = StratifiedKFold(
            n_splits=n_folds, shuffle=True, random_state=self._random_state
        )

        for model_idx, model in enumerate(self._models):
            for fold, (train_idx, val_idx) in enumerate(skf.split(X, y)):
                X_fold_train = X.iloc[train_idx]
                y_fold_train = y.iloc[train_idx]
                X_fold_val = X.iloc[val_idx]

                model_clone = self._clone_model(model)
                model_clone.fit(X_fold_train, y_fold_train, verbose=False)

                oof_preds[val_idx, model_idx] = model_clone.predict_proba(X_fold_val)[
                    :, 1
                ]

        return oof_preds

    def _clone_model(self, model: Any) -> Any:
        if isinstance(model, xgb.XGBClassifier):
            return xgb.XGBClassifier(**model.get_params())
        elif isinstance(model, cb.CatBoostClassifier):
            return cb.CatBoostClassifier(**model.get_params())
        else:
            raise ValueError(f"Unknown model type: {type(model)}")

    def fit_meta_model(self, X: pd.DataFrame, y: pd.Series) -> None:
        oof_predictions = self._get_oof_predictions(X, y)
        self._meta_model.fit(oof_predictions, y)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        base_predictions = np.column_stack(
            [model.predict_proba(X)[:, 1] for model in self._models]
        )
        return self._meta_model.predict_proba(base_predictions)[:, 1]

    def predict(self, X: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        probabilities = self.predict_proba(X)
        return (probabilities >= threshold).astype(int)


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
        return self._artifacts_dir / "ensemble_model.pkl"

    @property
    def metadata_path(self) -> Path:
        return self._artifacts_dir / "metadata.pkl"

    def save_model(self, model: StackingEnsemble) -> None:
        joblib.dump(model, self.model_path)

    def save_metadata(self, metadata: dict) -> None:
        joblib.dump(metadata, self.metadata_path)

    def load_model(self) -> StackingEnsemble:
        return joblib.load(self.model_path)

    def load_metadata(self) -> dict:
        return joblib.load(self.metadata_path)


class TrainingMetadataBuilder:
    @staticmethod
    def build(
        auc: float,
        feature_names: List[str],
        num_train: int,
        num_test: int,
        model_type: str,
        class_distribution: dict,
    ) -> dict:
        return {
            "auc": auc,
            "features": feature_names,
            "num_train": num_train,
            "num_test": num_test,
            "model_type": model_type,
            "class_distribution": class_distribution,
        }


class AdvancedEnsembleTrainer:
    def __init__(self, config: TrainingConfig, artifacts_dir: Path) -> None:
        self._config = config
        self._persistence = ModelPersistence(artifacts_dir)
        self._splitter = TrainingDataSplitter(config.test_size, config.random_state)
        self._xgb_builder = XGBoostModelBuilder(
            config.learning_rate,
            config.max_depth,
            config.num_boost_round,
            config.random_state,
        )
        self._catboost_builder = CatBoostModelBuilder(
            config.learning_rate,
            config.max_depth,
            config.num_boost_round,
            config.random_state,
        )
        self._evaluator = ModelEvaluator()
        self._ensemble = None

    def train(self, dataset: FeatureDataset) -> ModelArtifact:
        X = dataset.features[FEATURE_NAMES]
        y = dataset.labels

        class_counts = y.value_counts().to_dict()
        logger.info(f"Training data class distribution: {class_counts}")
        logger.info(f"Label 0 (clean): {class_counts.get(0, 0)} samples")
        logger.info(f"Label 1 (artifact): {class_counts.get(1, 0)} samples")

        X_train, X_test, y_train, y_test = self._splitter.split(X, y)

        logger.info("Training XGBoost model...")
        xgb_model = self._xgb_builder.train(X_train, y_train, X_test, y_test)

        logger.info("Training CatBoost model...")
        catboost_model = self._catboost_builder.train(X_train, y_train, X_test, y_test)

        logger.info("Building stacking ensemble...")
        self._ensemble = StackingEnsemble(self._config.random_state)
        self._ensemble.add_model(xgb_model)
        self._ensemble.add_model(catboost_model)

        logger.info("Training meta-model...")
        self._ensemble.fit_meta_model(X_train, y_train)

        predictions = self._ensemble.predict_proba(X_test)
        auc = self._evaluator.calculate_auc(y_test, predictions)

        metadata = TrainingMetadataBuilder.build(
            auc=auc,
            feature_names=FEATURE_NAMES,
            num_train=len(X_train),
            num_test=len(X_test),
            model_type="XGBoost+CatBoost Stacking Ensemble",
            class_distribution=class_counts,
        )

        self._persistence.save_model(self._ensemble)
        self._persistence.save_metadata(metadata)

        logger.info(f"Ensemble AUC: {auc:.4f}")

        return ModelArtifact(
            model_path=self._persistence.model_path,
            feature_names=tuple(FEATURE_NAMES),
        )

    def load(self) -> Tuple[StackingEnsemble, dict]:
        model = self._persistence.load_model()
        metadata = self._persistence.load_metadata()
        self._ensemble = model
        return model, metadata

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self._ensemble is None:
            raise RuntimeError("Model is not loaded. Call load() before predict().")
        return self._ensemble.predict_proba(X)

    def predict(self, X: pd.DataFrame, threshold: float = 0.5) -> np.ndarray:
        if self._ensemble is None:
            raise RuntimeError("Model is not loaded. Call load() before predict().")
        return self._ensemble.predict(X, threshold)
