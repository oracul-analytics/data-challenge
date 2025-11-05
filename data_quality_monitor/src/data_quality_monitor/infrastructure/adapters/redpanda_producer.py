from confluent_kafka import Producer
import json
from loguru import logger
import numpy as np
from data_quality_monitor.domain.models.result import QualityReport
from data_quality_monitor.domain.payloads.kafka.build_payload import KafkaPayloadBuilder


class _KafkaProducerWrapper:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.topic = topic

    def produce(self, key: str, value: str, callback=None):
        try:
            self.producer.produce(self.topic, key=key, value=value, callback=callback)
        except Exception as e:
            logger.error("Failed to produce message: {}", e)

    def flush(self):
        self.producer.flush()


class _PayloadSerializer:
    @staticmethod
    def serialize(obj):
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj)
        return str(obj)


class RedpandaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self._wrapper = _KafkaProducerWrapper(bootstrap_servers, topic)
        logger.info("Producer initialized for topic '{}'", topic)

    def _delivery_report(self, err, msg):
        if err:
            logger.error("Message delivery failed: {}", err)

    def send_report(self, report: QualityReport):
        if not report.results:
            logger.warning("No results to send for report")
            return

        for result in report.results:
            payload = KafkaPayloadBuilder.build(report, result)
            serialized = json.dumps(payload, default=_PayloadSerializer.serialize)
            self._wrapper.produce(key=str(report.table), value=serialized, callback=self._delivery_report)

        self._wrapper.flush()
        logger.info(
            "Sent {} results for table '{}' to topic '{}'",
            len(report.results),
            report.table,
            self._wrapper.topic,
        )
