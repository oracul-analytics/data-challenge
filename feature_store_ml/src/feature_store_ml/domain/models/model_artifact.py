from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence


@dataclass(frozen=True)
class ModelArtifact:
    
    model_path: Path
    feature_names: Sequence[str]
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self) -> None:
        if not isinstance(self.model_path, Path):
            object.__setattr__(self, "model_path", Path(self.model_path))
        
        if not isinstance(self.feature_names, tuple):
            object.__setattr__(self, "feature_names", tuple(self.feature_names))
    
    @property
    def exists(self) -> bool:
        """Check if model file exists."""
        return self.model_path.exists()
    
    @property
    def model_name(self) -> str:
        """Get model file name."""
        return self.model_path.name
    
    @property
    def n_features(self) -> int:
        """Get number of features."""
        return len(self.feature_names)
    
    def get_metric(self, name: str, default: Any = None) -> Any:
        return self.metadata.get(name, default)
    
    def with_metadata(self, **kwargs: Any) -> ModelArtifact:
        new_metadata = {**self.metadata, **kwargs}
        return ModelArtifact(
            model_path=self.model_path,
            feature_names=self.feature_names,
            metadata=new_metadata,
            created_at=self.created_at,
        )
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "model_path": str(self.model_path),
            "feature_names": list(self.feature_names),
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelArtifact:
        return cls(
            model_path=Path(data["model_path"]),
            feature_names=tuple(data["feature_names"]),
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(),
        )
    
    def __repr__(self) -> str:
        metrics_str = ", ".join(f"{k}={v:.4f}" if isinstance(v, float) else f"{k}={v}" 
                               for k, v in self.metadata.items())
        return (
            f"ModelArtifact(model={self.model_name}, "
            f"n_features={self.n_features}, "
            f"{metrics_str})"
        )


@dataclass(frozen=True)
class ModelMetrics:
    
    auc: float
    accuracy: float | None = None
    precision: float | None = None
    recall: float | None = None
    f1_score: float | None = None
    
    def to_dict(self) -> dict[str, float]:
        return {
            k: v for k, v in {
                "auc": self.auc,
                "accuracy": self.accuracy,
                "precision": self.precision,
                "recall": self.recall,
                "f1_score": self.f1_score,
            }.items() if v is not None
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, float]) -> ModelMetrics:
        return cls(
            auc=data["auc"],
            accuracy=data.get("accuracy"),
            precision=data.get("precision"),
            recall=data.get("recall"),
            f1_score=data.get("f1_score"),
        )
    
    def __repr__(self) -> str:
        metrics = [f"AUC={self.auc:.4f}"]
        if self.accuracy is not None:
            metrics.append(f"Acc={self.accuracy:.4f}")
        if self.precision is not None:
            metrics.append(f"Prec={self.precision:.4f}")
        if self.recall is not None:
            metrics.append(f"Rec={self.recall:.4f}")
        if self.f1_score is not None:
            metrics.append(f"F1={self.f1_score:.4f}")
        return f"ModelMetrics({', '.join(metrics)})"
