import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys
from loguru import logger
from data_quality_monitor.application.use_cases.run_check import RunProcess
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.domain.factories.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.domain.repository.clickhouse import ClickHouseRepositoryDomain


logger.remove()
logger.add(sys.stderr, level="INFO")

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.toml"
RULES_PATH = CONFIG_DIR / "rules.toml"


def test_all_rules_fail_in_reports():
    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory, rule_config=config)
    repo.ensure_schema_output()

    repo_domain = ClickHouseRepositoryDomain(client=repo.client, database=repo.database)
    repo_domain.drop_table("dq.events")
    repo_domain.drop_table("dq.reports")
    repo_domain.drop_table("dq.test2")
    logger.info("✓ Dropped events and reports tables")

    repo.client.command(f"""
        CREATE TABLE {repo.database}.events (
            event_id String,
            value Nullable(Float64),
            ts DateTime
        ) ENGINE = MergeTree()
        ORDER BY ts
    """)

    num_rows = 100
    start_time = datetime(2025, 11, 4, 0, 0, 0)
    data = [
        [
            f"id_{i}",
            None if i % 10 == 0 else float(i),
            start_time + timedelta(seconds=i),
        ]
        for i in range(num_rows)
    ]

    repo.client.insert(
        f"{repo.database}.events",
        data,
        column_names=["event_id", "value", "ts"],
    )

    run_process = RunProcess(INFRA_PATH, RULES_PATH)
    run_process.execute()

    reports = repo.list_reports()
    failed = reports[reports["passed"] != 0]

    if not failed.empty:
        for idx, row in failed.iterrows():
            logger.error("Rule '{}' unexpectedly passed (passed={})", row["rule"], row["passed"])
        raise AssertionError("Expected all rules to fail (passed == 0).")


if __name__ == "__main__":
    test_all_rules_fail_in_reports()
