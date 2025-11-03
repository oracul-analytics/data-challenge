# domain/models/dataset.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

import pandas as pd


@dataclass(frozen=True)
class DatasetMetadata:
    """Метаданные датасета для обучения/инференса."""
    
    name: str
    feature_names: Sequence[str]
    created_at: datetime
    num_rows: int
    num_features: int
    target_column: str | None = None
    
    @classmethod
    def from_dataframe(
        cls,
        name: str,
        df: pd.DataFrame,
        feature_names: Sequence[str],
        target_column: str | None = None,
    ) -> "DatasetMetadata":
        return cls(
            name=name,
            feature_names=tuple(feature_names),
            created_at=datetime.now(),
            num_rows=len(df),
            num_features=len(feature_names),
            target_column=target_column,
        )


@dataclass(frozen=True)
class Dataset:
    """Датасет с фичами и опциональным таргетом."""
    
    metadata: DatasetMetadata
    data: pd.DataFrame
    
    @classmethod
    def create(
        cls,
        name: str,
        data: pd.DataFrame,
        feature_names: Sequence[str],
        target_column: str | None = None,
    ) -> "Dataset":
        """Создать датасет с валидацией."""
        missing_cols = set(feature_names) - set(data.columns)
        if missing_cols:
            raise ValueError(f"Missing feature columns: {missing_cols}")
        
        if target_column and target_column not in data.columns:
            raise ValueError(f"Target column '{target_column}' not found in data")
        
        metadata = DatasetMetadata.from_dataframe(
            name=name,
            df=data,
            feature_names=feature_names,
            target_column=target_column,
        )
        
        return cls(metadata=metadata, data=data)
    
    @property
    def features(self) -> pd.DataFrame:
        """Вернуть только фичи."""
        return self.data[list(self.metadata.feature_names)]
    
    @property
    def target(self) -> pd.Series | None:
        """Вернуть таргет, если есть."""
        if self.metadata.target_column:
            return self.data[self.metadata.target_column]
        return None
    
    def split(self, test_size: float = 0.2, random_state: int = 42) -> tuple[Dataset, Dataset]:
        """Сплит на train/test."""
        from sklearn.model_selection import train_test_split
        
        train_data, test_data = train_test_split(
            self.data,
            test_size=test_size,
            random_state=random_state,
        )
        
        train_dataset = Dataset(
            metadata=DatasetMetadata.from_dataframe(
                name=f"{self.metadata.name}_train",
                df=train_data,
                feature_names=self.metadata.feature_names,
                target_column=self.metadata.target_column,
            ),
            data=train_data,
        )
        
        test_dataset = Dataset(
            metadata=DatasetMetadata.from_dataframe(
                name=f"{self.metadata.name}_test",
                df=test_data,
                feature_names=self.metadata.feature_names,
                target_column=self.metadata.target_column,
            ),
            data=test_data,
        )
        
        return train_dataset, test_dataset
    
    def __len__(self) -> int:
        return self.metadata.num_rows


@dataclass(frozen=True)
class TrainingDataset(Dataset):
    """Датасет для обучения (обязательно с таргетом)."""
    
    @classmethod
    def create(
        cls,
        name: str,
        data: pd.DataFrame,
        feature_names: Sequence[str],
        target_column: str,
    ) -> "TrainingDataset":
        """Создать training датасет с валидацией таргета."""
        if target_column not in data.columns:
            raise ValueError(f"Target column '{target_column}' is required for training")
        
        if data[target_column].isna().any():
            raise ValueError(f"Target column '{target_column}' contains NaN values")
        
        base_dataset = Dataset.create(
            name=name,
            data=data,
            feature_names=feature_names,
            target_column=target_column,
        )
        
        return cls(metadata=base_dataset.metadata, data=base_dataset.data)
    
    @property
    def target(self) -> pd.Series:
        """Таргет всегда присутствует в TrainingDataset."""
        return self.data[self.metadata.target_column]


@dataclass(frozen=True)
class InferenceDataset(Dataset):
    
    @classmethod
    def create(
        cls,
        name: str,
        data: pd.DataFrame,
        feature_names: Sequence[str],
    ) -> "InferenceDataset":
        """Создать inference датасет без таргета."""
        base_dataset = Dataset.create(
            name=name,
            data=data,
            feature_names=feature_names,
            target_column=None,
        )
        
        return cls(metadata=base_dataset.metadata, data=base_dataset.data)
