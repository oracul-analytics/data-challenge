from fastapi import FastAPI
from starlette.responses import PlainTextResponse
from pathlib import Path

from data_quality_monitor.infrastructure.adapters.metrics import (
    registry,
    run_counter,
    PrometheusMiddleware,
)
from data_quality_monitor.infrastructure.config import RuleConfig
from data_quality_monitor.infrastructure.factory.clickhouse import ClickHouseFactory
from data_quality_monitor.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)
from data_quality_monitor.application.services.runner import QualityRunner
from data_quality_monitor.application.usecases.process import RunProcess

CONFIG_DIR = Path(__file__).resolve().parent.parent.parent.parent / "config"
INFRA_PATH = CONFIG_DIR / "infrastructure.yaml"
RULES_PATH = CONFIG_DIR / "rules.yaml"

app = FastAPI(title="Data Quality Monitor")

app.add_middleware(PrometheusMiddleware)


@app.on_event("startup")
def bootstrap() -> None:
    app.state.config = RuleConfig.load(INFRA_PATH, RULES_PATH)

    factory = ClickHouseFactory(app.state.config.clickhouse)
    repository = ClickHouseRepository(factory=factory)
    repository.ensure_schema()
    app.state.runner = QualityRunner(repository)


@app.post("/run")
def run_checks() -> dict[str, int]:
    reports = app.state.runner.run(app.state.config.rules)
    run_counter.inc()
    executor = RunProcess(INFRA_PATH, RULES_PATH)
    executor.execute()
    return {"reports": len(reports)}


@app.get("/reports")
def list_reports() -> list[dict[str, object]]:
    repository: ClickHouseRepository = app.state.runner._repository
    frame = repository.list_reports()
    return frame.to_dict(orient="records")


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    from prometheus_client import generate_latest

    return PlainTextResponse(
        generate_latest(registry), media_type="text/plain; version=0.0.4"
    )
