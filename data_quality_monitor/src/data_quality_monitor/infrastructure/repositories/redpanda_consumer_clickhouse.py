from confluent_kafka import Consumer, KafkaException
import json
from loguru import logger
from clickhouse_repository import ClickHouseRepository
from data_quality_monitor.domain.models.result import QualityReport, QualityResult
from datetime import datetime

clickhouse_repo = ClickHouseRepository()
clickhouse_repo.ensure_schema()

consumer_conf = {
    "bootstrap.servers": "localhost:39092",
    "group.id": "dq_group",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)
consumer.subscribe(["dq_reports"])

def consume_and_save():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))

            result = QualityResult(
                rule=payload["rule"],
                passed=payload["passed"],
                details=payload["details"]
            )
            report = QualityReport(
                table=payload["table_name"],
                generated_at=datetime.fromisoformat(payload["generated_at"]),
                results=[result]
            )

            clickhouse_repo.save_report(report)
            consumer.commit()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_save()
