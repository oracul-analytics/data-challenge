import pandas as pd

from feature_store_ml.infrastructure.registry import FeatureRegistry


def test_registry_computes_features():
    data = pd.DataFrame(
        {
            "entity_id": [1, 1, 2],
            "event_time": pd.date_range("2024-01-01", periods=3, freq="h"),
            "value": [1.0, 2.0, 3.0],
            "attribute": [0.1, 0.2, 0.3],
        }
    )
    registry = FeatureRegistry.default()
    features = registry.compute(data)
    assert "value_mean" in features.columns
