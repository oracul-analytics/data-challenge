import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.yaml"
RULES_PATH = CONFIG_DIR / "rules.yaml"


def test_rules_yaml_failures():
    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    logger.info("✓ Loaded config from {} and {}", INFRA_PATH, RULES_PATH)

    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory)
    repo.ensure_schema()

    try:
        repo.client.command("TRUNCATE TABLE dq.events")
        repo.client.command("TRUNCATE TABLE dq.reports")
        logger.info("✓ Cleared events and reports tables")
    except Exception as e:
        logger.warning("Could not truncate tables: {}", e)

    # Insert faulty events to trigger rule violations
    num_rows = 1000
    start_time = datetime(2025, 11, 4, 0, 0, 0)

    events_data = pd.DataFrame(
        {
            "event_id": [i % 100 for i in range(num_rows)],
            "value": [
                None if i % 200 == 0 else (i % 1200) - 100 for i in range(num_rows)
            ],
            "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)],
        }
    )

    repo.insert_events(events_data)
    logger.info(
        "✓ Inserted {} faulty events (with intentional rule violations)", num_rows
    )

    total_checks = 0
    any_failed = False

    for rule in config.rules:
        frame = repo.fetch_table(rule.table)
        report = engine.evaluate(rule, frame)

        logger.info("\n=== Report for {} ===", rule.table)
        for result in report.results:
            total_checks += 1
            status = "✓ PASSED" if result.passed else "✗ FAILED"
            logger.info("{} - {}", status, result.rule)
            logger.info("  Details: {}", result.details)

            if not result.passed:
                any_failed = True

        repo.save_report(report)

    reports = repo.list_reports()
    logger.info("\n=== Saved Reports ===")
    logger.info("Total reports: {}", len(reports))

    for idx, row in reports.iterrows():
        status = "✓" if row["passed"] == 1 else "✗"
        logger.info(
            "{} {} (table={}, passed={})",
            status,
            row["rule"],
            row["table_name"],
            row["passed"],
        )

    assert any_failed, "Expected at least one rule to fail, but all passed!"
    assert not all(reports["passed"] == 1), (
        "All reports show passed, expected failures!"
    )

    logger.info("\n=== Test Summary ===")
    logger.info("✗ Some checks failed as expected (negative test successful)")


if __name__ == "__main__":
    test_rules_yaml_failures()
