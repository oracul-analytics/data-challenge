from confluent_kafka import Consumer, KafkaException
import json
from loguru import logger
from typing import Callable, Optional
import signal


class RedpandaConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:39092",
        topic: str = "dq_reports",
        group_id: str = "dq_monitor_group",
    ):
        self.config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        self.consumer = Consumer(self.config)
        self.topic = topic
        self.running = True
        
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutting down consumer...")
        self.running = False

    def consume(self, callback: Callable[[dict], None], timeout: float = 1.0, max_messages: Optional[int] = None):
        try:
            self.consumer.subscribe([self.topic])
            logger.info("Subscribed to topic: {}", self.topic)
            
            messages_processed = 0
            
            while self.running:
                if max_messages and messages_processed >= max_messages:
                    break
                
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error("Consumer error: {}", msg.error())
                    continue
                
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    callback(value)
                    messages_processed += 1
                    
                except json.JSONDecodeError as e:
                    logger.error("Failed to decode message: {}", e)
                except Exception as e:
                    logger.error("Error processing message: {}", e)
        
        except KafkaException as e:
            logger.error("Kafka exception: {}", e)
        finally:
            self.consumer.close()

    def close(self):
        self.running = False
        self.consumer.close()
