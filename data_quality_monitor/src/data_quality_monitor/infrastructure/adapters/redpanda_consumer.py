from confluent_kafka import Consumer, KafkaException
import json
from loguru import logger
from typing import Callable, Optional
import signal
import threading


class _KafkaConsumerWrapper:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.topic = topic
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

    def subscribe(self):
        self.consumer.subscribe([self.topic])

    def poll(self, timeout: float):
        return self.consumer.poll(timeout=timeout)

    def close(self):
        try:
            self.consumer.close()
            logger.info("Consumer closed")
        except Exception as e:
            logger.warning("Error closing consumer: {}", e)


class _MessageProcessor:
    def __init__(self, consumer: _KafkaConsumerWrapper, timeout_seconds):
        self.consumer = consumer
        self.timeout_seconds = timeout_seconds
        self.running = True

    def stop(self):
        self.running = False

    def run(self, callback: Callable[[dict], None], max_messages: Optional[int] = None):
        messages_processed = 0
        while self.running:
            if max_messages and messages_processed >= max_messages:
                break

            msg = self.consumer.poll(self.timeout_seconds)
            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: {}", msg.error())
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
                callback(value)
                messages_processed += 1
            except Exception as e:
                logger.error("Failed to process message: {}", e)


class _SignalHandler:
    @staticmethod
    def register(processor: _MessageProcessor):
        if threading.current_thread() is not threading.main_thread():
            return
        signal.signal(signal.SIGINT, lambda s, f: processor.stop())
        signal.signal(signal.SIGTERM, lambda s, f: processor.stop())


class RedpandaConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, timeout_seconds):
        self._consumer_wrapper = _KafkaConsumerWrapper(bootstrap_servers, group_id, topic)
        self._processor = _MessageProcessor(self._consumer_wrapper, timeout_seconds)
        _SignalHandler.register(self._processor)

    def consume(self, callback: Callable[[dict], None], max_messages: Optional[int] = None):
        self._consumer_wrapper.subscribe()
        self._processor.run(callback, max_messages)
        self.close()

    def close(self):
        self._processor.stop()
        self._consumer_wrapper.close()
