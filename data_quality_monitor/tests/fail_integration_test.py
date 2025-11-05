import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
import sys

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


def test_rules_yaml_failures_via_usecases():
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
            "event_id": [i % 100 for i in range(num_rows)],
            "value": [
                None if i % 50 == 0 else (i % 1200) - 100 for i in range(num_rows)
            ],
            "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)],
        }
    )
    repo.insert_events(events_data)
    logger.info(
        "✓ Inserted {} faulty events (with intentional rule violations)", num_rows
    )

    run_process = RunProcess(INFRA_PATH, RULES_PATH)
    run_process.execute()

    reports = repo.list_reports()
    logger.info("\n=== Saved Reports ===")
    logger.info("Total reports: {}", len(reports))
    for idx, row in reports.iterrows():
        status = "✓" if row["passed"] == 1 else "✗"
        logger.info(
            "{:<2} {:<25} (table={}, passed={})",
            status,
            row["rule"],
            row["table_name"],
            row["passed"],
        )

    non_schema_passed = reports[
        (reports["passed"] == 1) & (reports["rule"] != "schema")
    ]
    if not non_schema_passed.empty:
        logger.error("\nSome rules (excluding schema) passed, test failed!")
        for idx, row in non_schema_passed.iterrows():
            logger.error(
                "Rule '{}' passed on table '{}'", row["rule"], row["table_name"]
            )
        raise AssertionError(
            "Expected all rules (except schema) to fail, but some passed!"
        )

    logger.info("\nAll rules failed as expected (excluding schema). Test successful.")


if __name__ == "__main__":
    test_rules_yaml_failures_via_usecases()
