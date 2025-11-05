import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.application.usecases.run_check import RunProcess

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.yaml"
RULES_PATH = CONFIG_DIR / "rules.yaml"


def test_rules_yaml_via_kafka():
    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    logger.info("✓ Loaded config from {} and {}", INFRA_PATH, RULES_PATH)
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory, rule_config=config)

    try:
        repo.client.command("TRUNCATE TABLE dq.events")
        repo.client.command("TRUNCATE TABLE dq.reports")
        logger.info("✓ Cleared events and reports tables")
    except Exception as e:
        logger.warning("Could not truncate tables: {}", e)

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

    tester = RunProcess(INFRA_PATH, RULES_PATH)
    tester.execute()

    logger.info("=== Integration test completed successfully ===")


if __name__ == "__main__":
    test_rules_yaml_via_kafka()
