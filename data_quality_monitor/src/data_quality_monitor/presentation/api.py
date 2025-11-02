from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from prometheus_client import CollectorRegistry, Counter, generate_latest
from starlette.responses import PlainTextResponse

from data_quality_monitor.application.services.runner import QualityRunner
from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import ClickHouseRepository

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "rules.yaml"

app = FastAPI(title="Data Quality Monitor")
registry = CollectorRegistry()
run_counter = Counter("dq_runs_total", "DQ runs", registry=registry)


@app.on_event("startup")
def bootstrap() -> None:
    app.state.config = RuleConfig.load(CONFIG_PATH)
    repository = ClickHouseRepository(factory=ClickHouseFactory(app.state.config.clickhouse))
    app.state.runner = QualityRunner(repository)


@app.post("/run")
def run_checks() -> dict[str, int]:
    reports = app.state.runner.run(app.state.config.rules)
    run_counter.inc()
    return {"reports": len(reports)}


@app.get("/reports")
def list_reports() -> list[dict[str, object]]:
    repository: ClickHouseRepository = app.state.runner._repository  # noqa: SLF001
    frame = repository.list_reports()
    return frame.to_dict(orient="records")


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest(registry), media_type="text/plain; version=0.0.4")
