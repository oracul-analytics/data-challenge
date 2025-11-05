import pandas as pd
from datetime import datetime
from pathlib import Path
from data_quality_monitor.infrastructure.rules import engine
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
import pytest
from datetime import datetime, timezone
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.domain.models.result import QualityReport, RuleResult
from data_quality_monitor.domain.repository.clickhouse import ClickHouseRepositoryDomain
import sys
from loguru import logger

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

        domain_repo.truncate_table(f"{repo.database}.events")
        domain_repo.truncate_table(f"{repo.database}.reports")
        logger.info("✗ Created intentionally faulty schema (event_id as String)")
    except Exception as e:
        logger.error("Failed to create faulty schema: {}", e)
        raise

    # Здесь мы говорим pytest, что ожидаем AssertionError
    with pytest.raises(AssertionError, match="Schema validation failed"):
        for rule in config.rules:
            schema_expectation = next(
                (e for e in rule.expectations if e.type == "schema"), None
            )
            if schema_expectation:
                frame = repo.fetch_table(rule.table)
                schema_result = engine.schema(frame, schema_expectation)

                if not schema_result.passed:
                    logger.error(
                        "✗ Schema FAILED for table '{}'. All other rules automatically failed.",
                        rule.table,
                    )
                    failed_results = [
                        RuleResult(
                            rule=f"{e.type}:{e.params.get('column', '')}",
                            passed=False,
                            details={"reason": "Skipped due to schema failure"},
                        )
                        for e in rule.expectations
                        if e.type != "schema"
                    ]

                    report = QualityReport(
                        table=rule.table,
                        generated_at=datetime.now(timezone.utc),
                        results=(schema_result, *failed_results),
                    )

                    repo.save_report(report)
                    raise AssertionError(
                        f"Schema validation failed for table '{rule.table}'. All rules marked as FAILED."
                    )


if __name__ == "__main__":
    test_rules_yaml_failures()
