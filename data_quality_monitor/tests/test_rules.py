import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.domain.factories.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from loguru import logger
import sys

logger.remove()
logger.add(sys.stderr, level="INFO")

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.toml"
RULES_PATH = CONFIG_DIR / "rules.toml"


def test_rules_yaml_direct():
    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    logger.info("✓ Loaded config from {} and {}", INFRA_PATH, RULES_PATH)
    logger.info("  Database: {}", config.clickhouse.database)
    logger.info("  Rules: {}", len(config.rules))

    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory, rule_config=config)
    repo.ensure_schema()
    repo.drop_table("dq.test2")
    repo.drop_table("dq.events")
    repo.drop_table("dq.reports")

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
    logger.info("✓ Inserted {} events", num_rows)

    test2_data = pd.DataFrame(
        {
            "id": range(1, num_rows + 1),
            "field_a": [float(i % 500) for i in range(num_rows)],
            "ts": [start_time + timedelta(seconds=i) for i in range(num_rows)],
        }
    )
    repo.insert_table("dq.test2", test2_data)
    logger.info("✓ Inserted {} rows into dq.test2", num_rows)

    all_passed = True
    total_checks = 0

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
                all_passed = False

        repo.save_report(report)

    reports = repo.list_reports()
    logger.info("\n=== Saved Reports ===")
    logger.info("Total reports in database: {}", len(reports))

    for idx, row in reports.iterrows():
        status = "✓" if row["passed"] == 1 else "✗"
        logger.info(
            "{} {} (table={}, passed={})",
            status,
            row["rule"],
            row["table_name"],
            row["passed"],
        )

    assert len(reports) == total_checks, f"Expected {total_checks} reports, got {len(reports)}"
    assert all(reports["passed"] == 1), "Some checks failed"
    assert all_passed, "Not all checks passed"

    logger.info("\n=== Test Summary ===")
    logger.info("✓ All {} checks passed!", total_checks)
    logger.info("✓ Rules from rules.yaml validated successfully")

    unique_rules = reports["rule"].unique()
    for rule_name in unique_rules:
        logger.info("  - {}: PASSED", rule_name)


if __name__ == "__main__":
    test_rules_yaml_direct()
