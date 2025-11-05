from __future__ import annotations
from datetime import datetime

from pipeline_anomaly.domain.models.anomaly import Anomaly, AnomalyReport
from pipeline_anomaly.domain.services.interfaces import (
    AnomalyDetector,
    ClickHouseWriter,
)


class DetectAnomalies:
    def __init__(
        self,
        writer: ClickHouseWriter,
        detectors: list[AnomalyDetector],
        threshold: float,
    ) -> None:
        self._writer = writer
        self._detectors = detectors
        self._threshold = threshold

    def execute(self) -> AnomalyReport:
        """
        Генерирует отчет по аномалиям на основе последних агрегатов.
        """
        # Получаем агрегаты за последний день
        dataframe = self._writer.read_latest_window()
        if dataframe.empty:
            raise RuntimeError("No aggregates available to detect anomalies")

        # Берем минимальное и максимальное время из агрегатов
        window_start = dataframe["window_start"].min()
        window_end = dataframe["window_end"].max()

        # Детектим аномалии по каждому детектору
        anomalies: list[Anomaly] = []
        for detector in self._detectors:
            # В детекторах ожидается dataframe с колонками 'count', 'mean_value', 'std_value'
            scores = detector.fit_predict(dataframe)
            severity = detector.severity(scores)
            anomalies.append(
                Anomaly(
                    detector=detector.name,
                    score=float(scores.mean()),
                    severity=float(severity),
                    description=f"{detector.name} severity={severity:.3f}",
                    window_start=window_start,  # добавляем временные рамки
                    window_end=window_end,
                )
            )

        # Формируем отчет
        report = AnomalyReport(
            generated_at=datetime.utcnow(),
            window_start=window_start,
            window_end=window_end,
            anomalies=tuple(anomalies),
        )

        # Сохраняем отчет в ClickHouse
        self._writer.persist_report(report)
        return report

    def is_alert(self, report: AnomalyReport) -> bool:
        """
        Проверяет, превышает ли самая высокая severity детектора порог для тревоги.
        """
        return report.highest_severity() >= self._threshold
