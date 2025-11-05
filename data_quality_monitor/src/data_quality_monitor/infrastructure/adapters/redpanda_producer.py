from confluent_kafka import Producer
import json
from loguru import logger
from data_quality_monitor.domain.models.result import QualityReport
import numpy as np


class RedpandaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic
        logger.info("Producer initialized with topic: {}", self.topic)

    def delivery_report(self, err, msg):
        if err:
            logger.error("Message delivery failed: {}", err)
        else:
            logger.info("Message delivered to {} [{}]", msg.topic(), msg.partition())

    @staticmethod
    def _to_serializable(obj):
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj)
        return str(obj)

    def send_report(self, report: QualityReport):
        if not report.results:
            logger.warning("No results to send for report")
            return

        table_name = report.table

        for result in report.results:
            payload = {
                "table_name": table_name,
                "rule": result.rule,
                "passed": bool(result.passed),
                "details": result.details,
                "generated_at": report.generated_at.isoformat(),
            }

            try:
                self.producer.produce(
                    self.topic,
                    key=str(table_name),
                    value=json.dumps(payload, default=self._to_serializable),
                    callback=self.delivery_report,
                )
            except Exception as e:
                logger.error("Failed to produce message to Redpanda: {}", e)

        self.producer.flush()
        logger.info(
            "Report sent to Redpanda for table {} on topic {}", table_name, self.topic
        )
