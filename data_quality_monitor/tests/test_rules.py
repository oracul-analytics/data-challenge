import pandas as pd
import time
from data_quality_monitor.domain.models.rule import Expectation, TableRule
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.redpanda_producer import RedpandaProducer
from data_quality_monitor.infrastructure.repositories.redpanda_consumer import RedpandaConsumer
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


def test_completeness_passes():
    frame = pd.DataFrame({"value": [1, 2, 3]})
    rule = TableRule(table="tx", expectations=(Expectation("completeness", {"column": "value", "threshold": 1.0}),))
    report = engine.evaluate(rule, frame)
    
    assert report.results[0].passed

    producer = RedpandaProducer()
    producer.send_report(report)
    
    repo = ClickHouseRepository()
    repo.ensure_schema()
    
    consumer = RedpandaConsumer()
    consumer.consume(callback=repo.save_from_message, max_messages=1)
    
    time.sleep(0.5)
    reports = repo.list_reports(limit=1)
    assert len(reports) > 0
    assert reports.iloc[0]["table_name"] == "tx"
