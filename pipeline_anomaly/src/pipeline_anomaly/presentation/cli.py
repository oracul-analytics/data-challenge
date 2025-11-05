from __future__ import annotations

from pathlib import Path

import typer

from pipeline_anomaly.application.use_cases.compute_aggregates import ComputeAggregates
from pipeline_anomaly.application.use_cases.detect_anomalies import DetectAnomalies
from pipeline_anomaly.application.use_cases.load_dataset import LoadSyntheticDataset
from pipeline_anomaly.application.use_cases.run_pipeline import RunPipeline
from pipeline_anomaly.infrastructure.alerting.stdout_sink import StdOutAlertSink
from pipeline_anomaly.infrastructure.clients.clickhouse import ClickHouseFactory
from pipeline_anomaly.infrastructure.config import PipelineConfig
from pipeline_anomaly.infrastructure.detectors.dbscan import DBSCANDetector
from pipeline_anomaly.infrastructure.detectors.isolation_forest import (
    IsolationForestDetector,
)
from pipeline_anomaly.infrastructure.detectors.zscore import ZScoreDetector
from pipeline_anomaly.infrastructure.generators.synthetic_generator import (
    SyntheticDatasetGenerator,
)
from pipeline_anomaly.infrastructure.repositories.clickhouse_repository import (
    ClickHouseRepository,
)

app = typer.Typer()


@app.command()
def run(config: Path = typer.Option(..., exists=True, readable=True)) -> None:
    cfg = PipelineConfig.load(config)

    factory = ClickHouseFactory(
        host=cfg.clickhouse.host,
        port=cfg.clickhouse.port,
        username=cfg.clickhouse.username,
        password=cfg.clickhouse.password,
        database=cfg.clickhouse.database,
    )
    repository = ClickHouseRepository(factory=factory)

    generator = SyntheticDatasetGenerator(config=cfg.dataset)
    loader = LoadSyntheticDataset(generator=generator, writer=repository)
    aggregator = ComputeAggregates(writer=repository)

    detectors = [
        ZScoreDetector(threshold=cfg.anomaly_detection.zscore_threshold),
        IsolationForestDetector(
            contamination=cfg.anomaly_detection.isolation_forest.contamination,
            random_state=cfg.anomaly_detection.isolation_forest.random_state,
        ),
        DBSCANDetector(
            eps=cfg.anomaly_detection.dbscan.eps,
            min_samples=cfg.anomaly_detection.dbscan.min_samples,
        ),
    ]
    detector = DetectAnomalies(
        writer=repository,
        detectors=detectors,
        threshold=cfg.alerting.threshold_score,
    )

    sink = StdOutAlertSink()
    pipeline = RunPipeline(
        loader=loader,
        aggregator=aggregator,
        detector=detector,
        alert_sink=sink,
        alerts_enabled=cfg.alerting.enabled,
    )
    pipeline.execute()


if __name__ == "__main__":
    app()
