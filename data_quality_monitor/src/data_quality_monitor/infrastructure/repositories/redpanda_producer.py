from confluent_kafka import Producer
import json
from loguru import logger
from data_quality_monitor.domain.models.result import QualityReport

class RedpandaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:39092", topic: str = "dq_reports"):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic

    def delivery_report(self, err, msg):
        if err:
            logger.error("Message delivery failed: {}", err)
        else:
            logger.info("Message delivered to {} [{}]", msg.topic(), msg.partition())

    def send_report(self, report: QualityReport):
        if not report.results:
            logger.warning("No results to send for report: {}", getattr(report, "table", "unknown"))
            return

        # Отправляем каждый результат как отдельное сообщение
        for result in report.results:
            payload = {
                "table_name": getattr(report, "table", "unknown"),
                "rule": result.rule,
                "passed": result.passed,
                "details": result.details,
                "generated_at": report.generated_at.isoformat(),
            }
            self.producer.produce(
                self.topic,
                key=str(payload["table_name"]),
                value=json.dumps(payload),
                callback=self.delivery_report
            )
        self.producer.flush()
