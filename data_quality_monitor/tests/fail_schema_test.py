import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
from loguru import logger

from data_quality_monitor.application.usecases.run_check import RunProcess
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)

logger.remove()
logger.add(sys.stderr, level="INFO")

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.yaml"
RULES_PATH = CONFIG_DIR / "rules.yaml"


def test_all_rules_fail_in_reports():
    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    logger.info("✓ Loaded config from {} and {}", INFRA_PATH, RULES_PATH)
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory, rule_config=config)
    repo.ensure_schema_output()
    try:
        repo.client.command("TRUNCATE TABLE dq.events")
        repo.client.command("TRUNCATE TABLE dq.reports")
        logger.info("✓ Cleared events and reports tables")
    except Exception as e:
        logger.warning("Could not truncate tables: {}", e)

    repo.client.command(f"""
        CREATE TABLE IF NOT EXISTS {repo.database}.events (
            event_id String,           -- намеренно неверный тип
            value Nullable(Float64),
            ts DateTime
        ) ENGINE = MergeTree()
        ORDER BY ts
    """)
    logger.info("✗ Created events table with intentionally faulty schema")

    num_rows = 100
    start_time = datetime(2025, 11, 4, 0, 0, 0)
    events_data = pd.DataFrame(
        {
            "event_id": [str(i) for i in range(num_rows)],  # String вместо Int
            "value": [None if i % 10 == 0 else float(i) for i in range(num_rows)],
            "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)],
        }
    )
    repo.insert_events(events_data)
    logger.info("✓ Inserted {} test events", num_rows)

    run_process = RunProcess(INFRA_PATH, RULES_PATH)
    run_process.execute()

    reports = repo.list_reports()
    logger.info("Total reports saved: {}", len(reports))

    failed = reports[reports["passed"] != 0]
    if not failed.empty:
        logger.error("Some rules unexpectedly passed!")
        for idx, row in failed.iterrows():
            logger.error(
                "Rule '{}' unexpectedly passed (passed={})", row["rule"], row["passed"]
            )
        raise AssertionError("Expected all rules to fail (passed == 0).")

    logger.info("✓ All rules failed as expected in reports.")


if __name__ == "__main__":
    test_all_rules_fail_in_reports()
