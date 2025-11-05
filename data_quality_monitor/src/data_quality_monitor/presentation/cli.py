from __future__ import annotations

from pathlib import Path
import typer

from data_quality_monitor.application.services.runner import QualityRunner
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)

app = typer.Typer()


@app.command()
def run(
    infra_config: Path = typer.Option(..., exists=True, help="Path to infrastructure.yaml"),
    rules_config: Path = typer.Option(..., exists=True, help="Path to rules.yaml"),
) -> None:
    cfg = RuleConfig.load(infra_config, rules_config)

    repository = ClickHouseRepository(factory=ClickHouseFactory(cfg.clickhouse))
    runner = QualityRunner(repository=repository)

    runner.run(cfg.rules)
    typer.echo(f"✓ Successfully executed {len(cfg.rules)} rules")


if __name__ == "__main__":
    app()
