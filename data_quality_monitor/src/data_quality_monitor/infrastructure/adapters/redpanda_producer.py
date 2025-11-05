from confluent_kafka import Producer
from loguru import logger
from typing import Optional
from dataclasses import dataclass


@dataclass
class ProducerConfig:
    linger_ms: int
    batch_size: int
    compression_type: str
    acks: int
    max_in_flight: int
    queue_buffering_max_messages: int
    queue_buffering_max_kbytes: int

    @classmethod
    def high_throughput(cls, profile) -> "ProducerConfig":
        return cls(
            linger_ms=profile.linger_ms,
            batch_size=profile.batch_size,
            compression_type=profile.compression_type,
            acks=profile.acks,
            max_in_flight=profile.max_in_flight,
            queue_buffering_max_messages=profile.queue_buffering_max_messages,
            queue_buffering_max_kbytes=profile.queue_buffering_max_kbytes,
        )


class _KafkaProducerWrapper:
    def __init__(self, bootstrap_servers: str, topic: str, config: ProducerConfig):
        self.topic = topic
        self.config = config
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": self.config.linger_ms,
            "batch.size": self.config.batch_size,
            "compression.type": self.config.compression_type,
            "acks": self.config.acks,
            "max.in.flight.requests.per.connection": self.config.max_in_flight,
            "queue.buffering.max.messages": self.config.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.config.queue_buffering_max_kbytes,
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
    def __init__(self, bootstrap_servers: str, topic: str, config: ProducerConfig, auto_flush_interval: int):
        self._wrapper = _KafkaProducerWrapper(bootstrap_servers, topic, config)
        self._auto_flush_interval = auto_flush_interval
        self._message_count = 0
        self._delivery_success = 0
        self._delivery_failed = 0
        logger.info("Producer initialized for topic '{}' with profile", topic)

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
        timeout = timeout
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
