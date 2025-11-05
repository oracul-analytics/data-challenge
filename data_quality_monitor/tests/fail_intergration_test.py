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
from data_quality_monitor.domain.repository.clickhouse import ClickHouseRepositoryDomain


CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.yaml"
RULES_PATH = CONFIG_DIR / "rules.yaml"


def test_rules_yaml_via_kafka():
    logger.info("=== Starting integration test for rules.yaml ===")

    config = RuleConfig.load(INFRA_PATH, RULES_PATH)
    factory = ClickHouseFactory(config.clickhouse)
    repo = ClickHouseRepository(factory=factory)
    domain_repo = ClickHouseRepositoryDomain(repo.client, repo.database)

    try:
        repo.client.command(f"""
            CREATE TABLE IF NOT EXISTS {repo.database}.events (
                event_id String,
                value Nullable(Float64),
                ts DateTime
            ) ENGINE = MergeTree()
            ORDER BY ts
        """)
        repo.client.command(f"""
            CREATE TABLE IF NOT EXISTS {repo.database}.reports (
                table_name String,
                rule String,
                passed UInt8,
                details String,
                generated_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY generated_at
        """)
        domain_repo.truncate_table(f"{repo.database}.events")
        domain_repo.truncate_table(f"{repo.database}.reports")
        logger.info(
            "✗ Created intentionally faulty schema (event_id as String) and reports table"
        )
    except Exception as e:
        logger.error("Failed to create tables: {}", e)
        raise

    tester = RunProcess(INFRA_PATH, RULES_PATH)
    tester.execute()

    logger.info("=== Integration test completed successfully ===")


if __name__ == "__main__":
    test_rules_yaml_via_kafka()
