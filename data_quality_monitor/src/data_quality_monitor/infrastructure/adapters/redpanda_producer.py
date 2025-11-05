from confluent_kafka import Producer
from loguru import logger
from typing import Optional
import threading


class ProducerConfig:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        linger_ms: int,
        batch_size: int,
        compression_type: str,
        acks: int,
        max_in_flight: int,
        queue_buffering_max_messages: int,
        queue_buffering_max_kbytes: int,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.linger_ms = linger_ms
        self.batch_size = batch_size
        self.compression_type = compression_type
        self.acks = acks
        self.max_in_flight = max_in_flight
        self.queue_buffering_max_messages = queue_buffering_max_messages
        self.queue_buffering_max_kbytes = queue_buffering_max_kbytes


class _KafkaProducerWrapper:
    def __init__(self, config: ProducerConfig):
        self.topic = config.topic
        producer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "linger.ms": config.linger_ms,
            "batch.size": config.batch_size,
            "compression.type": config.compression_type,
            "acks": config.acks,
            "max.in.flight.requests.per.connection": config.max_in_flight,
            "queue.buffering.max.messages": config.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": config.queue_buffering_max_kbytes,
        }
        self.producer = Producer(producer_config)
        logger.debug("Producer config: {}", producer_config)

    def produce(self, key: str, value, callback=None):
        try:
            self.producer.produce(
                self.topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value.encode("utf-8") if isinstance(value, str) else value,
                callback=callback,
            )
            self.producer.poll(0)
        except BufferError:
            self.producer.flush()
            self.producer.produce(self.topic, key=key, value=value, callback=callback)
        except Exception as e:
            logger.error("Failed to produce message: {}", e)
            raise

    def flush(self, timeout: Optional[float] = None):
        return self.producer.flush(timeout=timeout)


class RedpandaProducer:
    def __init__(self, config: ProducerConfig, auto_flush_interval: int):
        self._wrapper = _KafkaProducerWrapper(config)
        self._auto_flush_interval = auto_flush_interval
        self._message_count = 0
        self._delivery_success = 0
        self._delivery_failed = 0
        logger.info("Producer initialized for topic '{}' with profile", config.topic)

    def _delivery_report(self, err, msg):
        if err:
            self._delivery_failed += 1
            logger.error("Message delivery failed: {}", err)
        else:
            self._delivery_success += 1

    def send_payload(self, key: str, payload: str):
        self._wrapper.produce(key=key, value=payload, callback=self._delivery_report)
        self._message_count += 1
        if self._auto_flush_interval and self._message_count % self._auto_flush_interval == 0:
            self.flush()

    def flush(self, timeout: Optional[float] = None):
        return self._wrapper.flush(timeout=timeout)

    def get_stats(self) -> dict:
        return {
            "total_sent": self._message_count,
            "delivered": self._delivery_success,
            "failed": self._delivery_failed,
            "pending": self._message_count - self._delivery_success - self._delivery_failed,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
