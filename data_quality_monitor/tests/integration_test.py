import pandas as pd
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.application.usecases.process import RunProcess


CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "rules.yaml"


def test_rules_yaml_via_kafka():
    logger.info("=== Starting integration test for rules.yaml ===")

    config = RuleConfig.load(CONFIG_PATH)
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory)
    repo.ensure_schema()

    num_rows = 1000
    start_time = datetime(2025, 11, 4, 0, 0, 0)

    events_data = pd.DataFrame(
        {
            "event_id": range(1, num_rows + 1),
            "value": [float(i % 1000) for i in range(num_rows)],
            "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)],
        }
    )

    repo.insert_events(events_data)
    logger.info("✓ Inserted {} events into dq.events", len(events_data))

    tester = RunProcess(CONFIG_PATH)
    tester.setup()
    tester.run()
    tester.cleanup()

    logger.info("=== Integration test completed successfully ===")


if __name__ == "__main__":
    test_rules_yaml_via_kafka()
