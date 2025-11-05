from data_quality_monitor.domain.models.result import QualityReport


class KafkaPayloadBuilder:
    @staticmethod
    def build(report: QualityReport, result) -> dict:
        return {
            "table_name": report.table,
            "rule": result.rule,
            "passed": bool(result.passed),
            "details": result.details,
            "generated_at": report.generated_at.isoformat(),
        }
