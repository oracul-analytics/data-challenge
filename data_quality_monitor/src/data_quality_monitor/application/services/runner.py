from __future__ import annotations

from loguru import logger

from data_quality_monitor.domain.models.result import QualityReport
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository
from data_quality_monitor.infrastructure.rules.engine import evaluate
from data_quality_monitor.domain.models.rule import TableRule


class QualityRunner:
    def __init__(self, repository: ClickHouseRepository) -> None:
        self._repository = repository

    def run(self, rules: tuple[TableRule, ...]) -> list[QualityReport]:
        self._repository.ensure_schema()
        reports: list[QualityReport] = []
        for rule in rules:
            logger.info("running dq rule for table {}", rule.table)
            frame = self._repository.fetch_table(rule.table)
            if frame.empty:
                logger.warning("table {} empty, skipping", rule.table)
                continue
            report = evaluate(rule, frame)
            self._repository.save_report(report)
            reports.append(report)
        return reports
