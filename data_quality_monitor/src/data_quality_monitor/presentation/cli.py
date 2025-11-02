from __future__ import annotations

from pathlib import Path

import typer

from data_quality_monitor.application.services.runner import QualityRunner
from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository

app = typer.Typer()


@app.command()
def run(config: Path = typer.Option(..., exists=True)) -> None:
    cfg = RuleConfig.load(config)
    repository = ClickHouseRepository(factory=ClickHouseFactory(cfg.clickhouse))
    runner = QualityRunner(repository=repository)
    runner.run(cfg.rules)


if __name__ == "__main__":
    app()
