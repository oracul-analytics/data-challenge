import pandas as pd

from data_quality_monitor.domain.models.rule import Expectation, TableRule
from data_quality_monitor.infrastructure.rules import engine


def test_completeness_passes():
    frame = pd.DataFrame({"value": [1, 2, 3]})
    rule = TableRule(table="t", expectations=(Expectation("completeness", {"column": "value", "threshold": 1.0}),))
    report = engine.evaluate(rule, frame)
    
    assert report.results[0].passed
